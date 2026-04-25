#!/usr/bin/env python3
"""
Project Neo - V1 Trend Learning Module v1.0
============================================

Daily autopsy + weekly proposal generation for V1 Trend strategy.
No LLM calls. Never auto-applies proposals.

Each daily cycle (23:00 UTC):
  1. Finds closed V1 Trend trades with no autopsy.
  2. Writes trade_autopsies rows (strategy-agnostic engine).
  3. Computes per-pair, per-session, per-ADX-bucket accuracy.
  4. Tracks trail exit vs regime exit vs time stop distribution.

Each weekly cycle (Sunday 23:00 UTC):
  1. Generates proposals — pair removal, ADX threshold tuning,
     session restriction, re-entry block duration adjustment.
  2. Writes to proposals table with strategy=v1_trend.
  3. Never auto-applies.

Build Date: April 2026
"""

import sys
import json
import uuid
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, "/root/Project_Neo_Damon")
from shared.market_hours import get_market_state
from v1_swing_parameters import V1_SWING_PAIRS
from shared.v1_trend_parameters import (
    STRATEGY_NAME,
    LM_MIN_TRADES_FOR_PROPOSAL,
    LM_WIN_RATE_REMOVAL_THRESHOLD,
    ADX_GATE,
    TIME_STOP_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

AGENT_NAME = "v1_trend_learning"


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

class DatabaseConnection:
    AWS_REGION = "eu-west-2"

    def __init__(self):
        self.ssm     = boto3.client("ssm",            region_name=self.AWS_REGION)
        self.secrets = boto3.client("secretsmanager", region_name=self.AWS_REGION)
        self.conn    = None
        self._connect()

    def _connect(self):
        endpoint = self.ssm.get_parameter(
            Name="/platform/config/rds-endpoint", WithDecryption=True
        )["Parameter"]["Value"]
        creds = json.loads(
            self.secrets.get_secret_value(SecretId="platform/rds/credentials")["SecretString"]
        )
        self.conn = psycopg2.connect(
            host=endpoint, database="postgres",
            user=creds["username"], password=creds["password"],
            port=5432, options="-c search_path=forex_network,shared,public",
        )
        self.conn.autocommit = False
        logger.info("DB connection established")

    def reconnect_if_needed(self):
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:
            logger.warning("DB reconnecting")
            try:
                self.conn.close()
            except Exception:
                pass
            self._connect()

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Autopsy engine
# ---------------------------------------------------------------------------

class V1TrendAutopsyEngine:
    """
    Writes trade_autopsies for closed V1 Trend trades.
    Strategy-agnostic core — keyed on trade_id.
    """

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db      = db
        self.user_id = user_id

    def _fetch_unautopsied_trades(self) -> List[Dict]:
        self.db.reconnect_if_needed()
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT t.*
                FROM forex_network.trades t
                LEFT JOIN forex_network.trade_autopsies ta ON ta.trade_id = t.id
                WHERE t.user_id = %s
                  AND t.strategy = %s
                  AND t.exit_time IS NOT NULL
                  AND ta.trade_id IS NULL
                ORDER BY t.exit_time ASC
                LIMIT 100
            """, (self.user_id, STRATEGY_NAME))
            return [dict(r) for r in cur.fetchall()]

    def _classify_failure_mode(self, trade: Dict) -> str:
        exit_reason = trade.get("exit_reason", "")
        pnl         = float(trade.get("pnl") or 0)
        if exit_reason == "time_stop":
            return "time_stop_no_momentum"
        if exit_reason == "regime_exit":
            return "regime_reversal"
        if exit_reason == "stop" and pnl < 0:
            return "stop_loss"
        if exit_reason == "trailing_stop" and pnl > 0:
            return "trail_exit_profit"
        if exit_reason == "trailing_stop" and pnl <= 0:
            return "trail_exit_breakeven_or_loss"
        if exit_reason == "daily_halt":
            return "halt_forced_close"
        return "unknown"

    def _classify_signal_quality(self, trade: Dict) -> str:
        pnl = float(trade.get("pnl") or 0)
        t1  = trade.get("target_1_hit")
        if pnl > 0 and t1:
            return "good"
        if pnl > 0 and not t1:
            return "marginal"
        if pnl <= 0:
            return "poor"
        return "unknown"

    def write_autopsy(self, trade: Dict) -> bool:
        failure_mode   = self._classify_failure_mode(trade)
        signal_quality = self._classify_signal_quality(trade)
        hold_days      = None
        if trade.get("entry_time") and trade.get("exit_time"):
            hold_days = (trade["exit_time"] - trade["entry_time"]).days

        factors = {
            "adx_at_entry":      trade.get("adx_at_entry"),
            "rsi_at_entry":      trade.get("rsi_at_entry"),
            "ema50_at_entry":    trade.get("ema50_at_entry"),
            "ema200_at_entry":   trade.get("ema200_at_entry"),
            "macd_at_entry":     trade.get("macd_at_entry"),
            "session_at_entry":  trade.get("session_at_entry"),
            "exit_reason":       trade.get("exit_reason"),
            "regime_exit":       trade.get("regime_exit"),
            "time_stop_exit":    trade.get("time_stop_exit"),
            "hold_days":         hold_days,
            "target_1_hit":      trade.get("target_1_hit"),
            "pnl":               float(trade.get("pnl") or 0),
            "strategy":          STRATEGY_NAME,
        }

        try:
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.trade_autopsies
                        (trade_id, failure_mode, signal_quality, contributing_factors,
                         learning_flags, diagnosis)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    trade["id"],
                    failure_mode,
                    signal_quality,
                    json.dumps(factors, default=str),
                    f"strategy={STRATEGY_NAME}",
                    f"V1 Trend autopsy: {failure_mode} | quality={signal_quality} | hold={hold_days}d",
                ))
            self.db.conn.commit()
            return True
        except Exception as e:
            logger.error(f"write_autopsy failed for trade {trade.get('id')}: {e}")
            try:
                self.db.conn.rollback()
            except Exception:
                pass
            return False

    def process_unautopsied(self) -> int:
        trades  = self._fetch_unautopsied_trades()
        written = 0
        for trade in trades:
            if self.write_autopsy(trade):
                written += 1
        logger.info(f"Autopsies written: {written} of {len(trades)}")
        return written


# ---------------------------------------------------------------------------
# Accuracy tracker
# ---------------------------------------------------------------------------

class V1TrendAccuracyTracker:
    """
    Computes per-pair, per-session, per-ADX-bucket win rates.
    Tracks trail exit vs regime exit vs time stop distribution.
    """

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db      = db
        self.user_id = user_id

    def compute_pair_accuracy(self) -> Dict[str, Dict]:
        """Win rate per pair over all closed V1 Trend trades."""
        self.db.reconnect_if_needed()
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    instrument,
                    COUNT(*) AS total,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                    AVG(pnl) AS avg_pnl,
                    AVG(CASE WHEN pnl > 0 THEN pnl END) AS avg_win,
                    AVG(CASE WHEN pnl <= 0 THEN pnl END) AS avg_loss
                FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NOT NULL
                GROUP BY instrument
                ORDER BY instrument
            """, (self.user_id, STRATEGY_NAME))
            rows = cur.fetchall()

        result = {}
        for r in rows:
            total = int(r["total"])
            wins  = int(r["wins"] or 0)
            result[r["instrument"]] = {
                "total":      total,
                "wins":       wins,
                "win_rate":   round(wins / total, 4) if total > 0 else 0,
                "avg_pnl":    round(float(r["avg_pnl"] or 0), 4),
                "avg_win":    round(float(r["avg_win"] or 0), 4),
                "avg_loss":   round(float(r["avg_loss"] or 0), 4),
            }
        return result

    def compute_session_accuracy(self) -> Dict[str, Dict]:
        """Win rate per session."""
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    session_at_entry AS session,
                    COUNT(*) AS total,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
                FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NOT NULL
                  AND session_at_entry IS NOT NULL
                GROUP BY session_at_entry
            """, (self.user_id, STRATEGY_NAME))
            rows = cur.fetchall()

        result = {}
        for r in rows:
            total = int(r["total"])
            wins  = int(r["wins"] or 0)
            result[r["session"]] = {
                "total":    total,
                "wins":     wins,
                "win_rate": round(wins / total, 4) if total > 0 else 0,
            }
        return result

    def compute_adx_bucket_accuracy(self) -> Dict[str, Dict]:
        """Win rate by ADX bucket: 25-35, 35-50, 50+."""
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    CASE
                        WHEN adx_at_entry < 35 THEN '25-35'
                        WHEN adx_at_entry < 50 THEN '35-50'
                        ELSE '50+'
                    END AS bucket,
                    COUNT(*) AS total,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
                FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NOT NULL
                  AND adx_at_entry IS NOT NULL
                GROUP BY bucket
            """, (self.user_id, STRATEGY_NAME))
            rows = cur.fetchall()

        result = {}
        for r in rows:
            total = int(r["total"])
            wins  = int(r["wins"] or 0)
            result[r["bucket"]] = {
                "total":    total,
                "wins":     wins,
                "win_rate": round(wins / total, 4) if total > 0 else 0,
            }
        return result

    def compute_exit_distribution(self) -> Dict[str, int]:
        """Count of trail_exit, regime_exit, time_stop, stop_loss."""
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT exit_reason, COUNT(*) AS cnt
                FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NOT NULL
                GROUP BY exit_reason
            """, (self.user_id, STRATEGY_NAME))
            rows = cur.fetchall()
        return {r["exit_reason"]: int(r["cnt"]) for r in rows}

    def compute_hold_duration(self) -> Dict:
        """Average hold duration and T1 hit rate."""
        with self.db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    AVG(EXTRACT(EPOCH FROM (exit_time - entry_time)) / 86400) AS avg_days,
                    SUM(CASE WHEN target_1_hit THEN 1 ELSE 0 END) AS t1_hits,
                    COUNT(*) AS total
                FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NOT NULL
            """, (self.user_id, STRATEGY_NAME))
            row = cur.fetchone()

        total = int(row["total"] or 0)
        return {
            "avg_hold_days": round(float(row["avg_days"] or 0), 1),
            "t1_hit_rate":   round(int(row["t1_hits"] or 0) / total, 4) if total > 0 else 0,
            "total_trades":  total,
        }


# ---------------------------------------------------------------------------
# Proposal generator
# ---------------------------------------------------------------------------

class V1TrendProposalGenerator:
    """
    Generates weekly proposals. Never auto-applies.
    Writes to proposals table with strategy=v1_trend.
    """

    def __init__(self, db: DatabaseConnection, user_id: str,
                 accuracy_tracker: V1TrendAccuracyTracker):
        self.db       = db
        self.user_id  = user_id
        self.tracker  = accuracy_tracker

    def _write_proposal(self, proposal_type: str, pair: Optional[str],
                        description: str, data: Dict) -> bool:
        try:
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.proposals
                        (user_id, strategy, proposal_type, pair, description,
                         data, created_at, applied)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), FALSE)
                """, (
                    self.user_id,
                    STRATEGY_NAME,
                    proposal_type,
                    pair,
                    description,
                    json.dumps(data, default=str),
                ))
            self.db.conn.commit()
            logger.info(f"Proposal written: {proposal_type} | {pair or 'global'} | {description[:60]}")
            return True
        except Exception as e:
            logger.error(f"_write_proposal failed: {e}")
            try:
                self.db.conn.rollback()
            except Exception:
                pass
            return False

    def generate_pair_removal_proposals(self, pair_accuracy: Dict[str, Dict]) -> int:
        """Propose removing pairs with win_rate < threshold over min sample."""
        written = 0
        for pair, stats in pair_accuracy.items():
            if (stats["total"] >= LM_MIN_TRADES_FOR_PROPOSAL and
                    stats["win_rate"] < LM_WIN_RATE_REMOVAL_THRESHOLD):
                written += int(self._write_proposal(
                    proposal_type="pair_removal",
                    pair=pair,
                    description=(
                        f"Win rate {stats['win_rate']:.1%} below threshold "
                        f"{LM_WIN_RATE_REMOVAL_THRESHOLD:.1%} over {stats['total']} trades"
                    ),
                    data=stats,
                ))
        return written

    def generate_adx_threshold_proposals(self, adx_accuracy: Dict[str, Dict]) -> int:
        """Propose raising ADX gate if 25-35 bucket underperforms."""
        written = 0
        low_bucket = adx_accuracy.get("25-35", {})
        high_bucket = adx_accuracy.get("35-50", {})
        if (low_bucket.get("total", 0) >= 15 and
                high_bucket.get("total", 0) >= 15 and
                low_bucket.get("win_rate", 0) < 0.35 and
                high_bucket.get("win_rate", 0) > 0.50):
            written += int(self._write_proposal(
                proposal_type="adx_threshold_raise",
                pair=None,
                description=(
                    f"ADX 25-35 bucket win rate {low_bucket['win_rate']:.1%} "
                    f"vs 35-50 bucket {high_bucket['win_rate']:.1%} — consider raising gate to 35"
                ),
                data={"low_bucket": low_bucket, "high_bucket": high_bucket,
                      "current_gate": ADX_GATE, "proposed_gate": 35},
            ))
        return written

    def generate_session_restriction_proposals(self, session_accuracy: Dict[str, Dict]) -> int:
        """Propose restricting sessions with consistently poor performance."""
        written = 0
        for session, stats in session_accuracy.items():
            if stats["total"] >= 10 and stats["win_rate"] < 0.30:
                written += int(self._write_proposal(
                    proposal_type="session_restriction",
                    pair=None,
                    description=(
                        f"Session '{session}' win rate {stats['win_rate']:.1%} "
                        f"over {stats['total']} trades — consider restricting"
                    ),
                    data={"session": session, **stats},
                ))
        return written

    def generate_time_stop_proposals(self, hold_data: Dict) -> int:
        """Propose adjusting time stop if avg hold is near limit."""
        written = 0
        avg_days = hold_data.get("avg_hold_days", 0)
        t1_rate  = hold_data.get("t1_hit_rate", 0)
        if hold_data.get("total_trades", 0) >= LM_MIN_TRADES_FOR_PROPOSAL:
            if avg_days > TIME_STOP_DAYS * 0.8 and t1_rate < 0.40:
                written += int(self._write_proposal(
                    proposal_type="time_stop_reduction",
                    pair=None,
                    description=(
                        f"Avg hold {avg_days:.1f}d approaching {TIME_STOP_DAYS}d limit, "
                        f"T1 hit rate only {t1_rate:.1%} — consider reducing time stop"
                    ),
                    data={**hold_data, "current_time_stop": TIME_STOP_DAYS},
                ))
        return written

    def generate_all_proposals(self) -> int:
        logger.info("Generating V1 Trend weekly proposals...")
        pair_accuracy    = self.tracker.compute_pair_accuracy()
        session_accuracy = self.tracker.compute_session_accuracy()
        adx_accuracy     = self.tracker.compute_adx_bucket_accuracy()
        hold_data        = self.tracker.compute_hold_duration()

        total = 0
        total += self.generate_pair_removal_proposals(pair_accuracy)
        total += self.generate_adx_threshold_proposals(adx_accuracy)
        total += self.generate_session_restriction_proposals(session_accuracy)
        total += self.generate_time_stop_proposals(hold_data)

        logger.info(f"Proposals generated: {total}")
        return total


# ---------------------------------------------------------------------------
# Main Learning Module
# ---------------------------------------------------------------------------

class V1TrendLearningModule:

    DAILY_RUN_HOUR  = 23   # UTC
    WEEKLY_RUN_DAY  = 6    # Sunday

    def __init__(self, user_id: str, dry_run: bool = False):
        self.user_id  = user_id
        self.dry_run  = dry_run
        self.db       = DatabaseConnection()

        self.autopsy_engine = V1TrendAutopsyEngine(self.db, user_id)
        self.tracker        = V1TrendAccuracyTracker(self.db, user_id)
        self.proposal_gen   = V1TrendProposalGenerator(self.db, user_id, self.tracker)

        logger.info(f"V1TrendLearningModule initialised — user={user_id}")

    def update_heartbeat(self):
        try:
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.agent_heartbeats
                        (agent_name, user_id, session_id, last_seen, status, cycle_count)
                    VALUES (%s, %s, %s, NOW(), 'active', 0)
                    ON CONFLICT (agent_name, user_id) DO UPDATE SET
                        last_seen  = NOW(),
                        status     = 'active'
                """, (AGENT_NAME, self.user_id, str(uuid.uuid4())))
            self.db.conn.commit()
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")
            try:
                self.db.conn.rollback()
            except Exception:
                pass

    def run_daily_cycle(self):
        logger.info("Running daily V1 Trend learning cycle")
        self.update_heartbeat()
        autopsied = self.autopsy_engine.process_unautopsied()

        # Log current accuracy snapshot
        pair_accuracy = self.tracker.compute_pair_accuracy()
        exit_dist     = self.tracker.compute_exit_distribution()
        hold_data     = self.tracker.compute_hold_duration()

        logger.info(f"Accuracy snapshot — {len(pair_accuracy)} pairs tracked")
        logger.info(f"Exit distribution: {exit_dist}")
        logger.info(f"Hold duration: {hold_data}")
        self.update_heartbeat()

    def run_weekly_cycle(self):
        logger.info("Running weekly V1 Trend proposal generation")
        if self.dry_run:
            logger.info("[dry-run] Skipping proposal writes")
            return
        self.proposal_gen.generate_all_proposals()
        self.update_heartbeat()

    def close(self):
        self.db.close()
        logger.info("V1TrendLearningModule resources cleaned up")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_v1_trend_user_ids(region: str = "eu-west-2") -> List[str]:
    ssm = boto3.client("ssm", region_name=region)
    sm  = boto3.client("secretsmanager", region_name=region)
    endpoint = ssm.get_parameter(
        Name="/platform/config/rds-endpoint", WithDecryption=True
    )["Parameter"]["Value"]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT user_id FROM forex_network.risk_parameters
        WHERE paper_mode = TRUE ORDER BY user_id
    """)
    user_ids = [str(r["user_id"]) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return user_ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="V1 Trend Learning Module")
    parser.add_argument("--user",    default=None,        help="Run for single user only")
    parser.add_argument("--single",  action="store_true", help="Single daily cycle then exit")
    parser.add_argument("--weekly",  action="store_true", help="Run weekly proposals then exit")
    parser.add_argument("--test",    action="store_true", help="Config test only")
    parser.add_argument("--dry-run", action="store_true", help="Do not write proposals")
    args = parser.parse_args()

    user_ids = [args.user] if args.user else _get_v1_trend_user_ids()
    if not user_ids:
        logger.error("No users found")
        sys.exit(1)

    logger.info(f"{len(user_ids)} user(s): {user_ids}")

    if args.test:
        for uid in user_ids:
            try:
                lm = V1TrendLearningModule(user_id=uid, dry_run=True)
                logger.info(f"  PASS: {uid}")
                lm.close()
            except Exception as e:
                logger.error(f"  FAIL {uid}: {e}")
                sys.exit(1)
        sys.exit(0)

    modules = {}
    for uid in user_ids:
        try:
            modules[uid] = V1TrendLearningModule(
                user_id=uid, dry_run=getattr(args, "dry_run", False)
            )
        except Exception as e:
            logger.error(f"Failed to init LM for {uid}: {e}")

    if not modules:
        logger.error("No modules initialised")
        sys.exit(1)

    if args.single:
        for uid, lm in modules.items():
            try:
                lm.run_daily_cycle()
            except Exception as e:
                logger.error(f"Daily cycle failed for {uid}: {e}")
            finally:
                lm.close()
        sys.exit(0)

    if args.weekly:
        for uid, lm in modules.items():
            try:
                lm.run_weekly_cycle()
            except Exception as e:
                logger.error(f"Weekly cycle failed for {uid}: {e}")
            finally:
                lm.close()
        sys.exit(0)

    # Continuous mode — poll every 30 min, run daily at 23:00, weekly on Sunday
    logger.info("Starting continuous operation")
    last_daily_date  = None
    last_weekly_date = None

    try:
        while True:
            now     = datetime.now(timezone.utc)
            today   = now.date()
            weekday = now.weekday()

            for uid, lm in modules.items():
                try:
                    lm.update_heartbeat()

                    # Daily cycle at 23:00 UTC
                    if now.hour == V1TrendLearningModule.DAILY_RUN_HOUR and last_daily_date != today:
                        lm.run_daily_cycle()
                        last_daily_date = today

                        # Weekly proposals on Sunday after daily cycle
                        if weekday == V1TrendLearningModule.WEEKLY_RUN_DAY and last_weekly_date != today:
                            lm.run_weekly_cycle()
                            last_weekly_date = today

                except Exception as e:
                    logger.error(f"Cycle failed for {uid}: {e}")
                    try:
                        lm.update_heartbeat()
                    except Exception:
                        pass

            time.sleep(1800)  # 30-minute poll

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for lm in modules.values():
            try:
                lm.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
