#!/usr/bin/env python3
"""
Project Neo - V1 Trend Orchestrator v1.0
==========================================

Binary AND gate. No convergence scoring. No LLM calls.

Each cycle:
  1. Reads latest macro (ema_regime) and technical signals from agent_signals.
  2. Applies AND gate: tech_score != 0 AND macro_direction != neutral AND directions agree.
  3. Checks re-entry block table — skips pair if block active.
  4. Writes approved proposals to agent_signals as trade_approval (strategy=v1_trend).
  5. Risk Guardian picks up and processes.

Build Date: April 2026
"""

import sys
import json
import uuid
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, "/root/Project_Neo_Damon")
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, AGENT_SCOPE_USER_ID
from shared.schema_validator import validate_schema
from v1_swing_parameters import V1_SWING_PAIRS
from shared.v1_trend_parameters import (
    STRATEGY_NAME,
    RISK_PER_TRADE_PCT,
    ATR_STOP_MULTIPLIER,
    ATR_T1_MULTIPLIER,
    ATR_TRAIL_MULTIPLIER,
    T1_CLOSE_PCT,
    TIME_STOP_DAYS,
    MIN_RR,
    MAX_CONCURRENT_POSITIONS,
    SESSION_WINDOWS_UTC,
    ASIA_SESSION_PAIRS,
)

EXPECTED_TABLES = {
    "forex_network.agent_signals":           ["agent_name", "instrument", "signal_type",
                                              "score", "bias", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats":        ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.trades":                  ["user_id", "instrument", "direction", "exit_time"],
    "forex_network.v1_trend_reentry_blocks": ["pair", "direction", "blocked_until"],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _get_current_session(utc_hour: int, pair: str) -> Optional[str]:
    for session, (start, end) in SESSION_WINDOWS_UTC.items():
        if session == "dead_zone":
            continue
        if start <= utc_hour < end:
            if session == "asia" and pair not in ASIA_SESSION_PAIRS:
                return None
            return session
    return None


class V1TrendOrchestrator:
    """
    Binary AND gate orchestrator for V1 Trend strategy.
    Reads macro + technical signals, emits trade_approval proposals to RG.
    """

    AGENT_NAME             = "v1_trend_orchestrator"
    CYCLE_INTERVAL_SECONDS = 300
    SIGNAL_EXPIRY_MINUTES  = 25
    PROPOSAL_EXPIRY_MINUTES = 20
    AWS_REGION             = "eu-west-2"
    PAIRS                  = V1_SWING_PAIRS

    def __init__(self, user_id: str, dry_run: bool = False):
        self.session_id  = str(uuid.uuid4())
        self.cycle_count = 0
        self.user_id     = user_id
        self.dry_run     = dry_run

        self.ssm_client     = boto3.client("ssm",            region_name=self.AWS_REGION)
        self.secrets_client = boto3.client("secretsmanager", region_name=self.AWS_REGION)

        self._load_configuration()
        self._init_database()

        saved = load_state(self.AGENT_NAME, AGENT_SCOPE_USER_ID, "cycle_count", default=0)
        if isinstance(saved, int) and saved > 0:
            self.cycle_count = saved
        logger.info(f"V1TrendOrchestrator initialised — user={user_id} session={self.session_id}")

    # ------------------------------------------------------------------
    # Configuration / DB
    # ------------------------------------------------------------------

    def _load_configuration(self):
        self.rds_endpoint    = self._get_parameter("/platform/config/rds-endpoint")
        self.rds_credentials = self._get_secret("platform/rds/credentials")

    def _get_parameter(self, name: str) -> str:
        try:
            return self.ssm_client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        except ClientError as e:
            logger.error(f"SSM get failed {name}: {e}")
            raise

    def _get_secret(self, name: str) -> Dict[str, Any]:
        try:
            return json.loads(self.secrets_client.get_secret_value(SecretId=name)["SecretString"])
        except ClientError as e:
            logger.error(f"Secret get failed {name}: {e}")
            raise

    def _init_database(self):
        self.db_conn = psycopg2.connect(
            host=self.rds_endpoint, database="postgres",
            user=self.rds_credentials["username"],
            password=self.rds_credentials["password"],
            port=5432, options="-c search_path=forex_network,shared,public",
        )
        self.db_conn.autocommit = False
        validate_schema(self.db_conn, EXPECTED_TABLES)
        logger.info("Database connection established")

    def _reconnect_db_if_needed(self):
        try:
            self.db_conn.cursor().execute("SELECT 1")
        except Exception:
            logger.warning("DB reconnecting")
            try:
                self.db_conn.close()
            except Exception:
                pass
            self._init_database()

    def check_kill_switch(self) -> bool:
        try:
            val = self._get_parameter("/platform/config/kill-switch")
            if val == "active":
                logger.warning("Kill switch ACTIVE")
                return True
            return False
        except Exception as e:
            logger.error(f"Kill switch check failed: {e}")
            return True

    # ------------------------------------------------------------------
    # Signal reading
    # ------------------------------------------------------------------

    def _read_latest_signals(self) -> Dict[str, Dict]:
        """
        Read latest unexpired macro and technical signals for all pairs.
        Returns dict: pair -> {macro: {...}, technical: {...}}
        """
        self._reconnect_db_if_needed()
        result = {pair: {"macro": None, "technical": None} for pair in self.PAIRS}

        with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (instrument, agent_name)
                    instrument, agent_name, score, bias, payload, created_at
                FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND agent_name IN ('v1_trend_macro', 'v1_trend_technical')
                  AND expires_at > NOW()
                  AND instrument = ANY(%s)
                ORDER BY instrument, agent_name, created_at DESC
            """, (self.user_id, list(self.PAIRS)))
            rows = cur.fetchall()

        for row in rows:
            pair     = row["instrument"]
            agent    = row["agent_name"]
            payload  = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"] or "{}")
            if pair not in result:
                continue
            if agent == "v1_trend_macro":
                result[pair]["macro"] = {
                    "direction": payload.get("direction", "neutral"),
                    "score":     float(row["score"] or 0),
                    "ema50":     payload.get("ema50"),
                    "ema200":    payload.get("ema200"),
                    "bias":      row["bias"],
                }
            elif agent == "v1_trend_technical":
                result[pair]["technical"] = {
                    "direction":          payload.get("direction"),
                    "setup_type":         payload.get("setup_type"),
                    "score":              float(row["score"] or 0),
                    "adx":                payload.get("adx"),
                    "rsi":                payload.get("rsi"),
                    "macd_line":          payload.get("macd_line"),
                    "macd_signal":        payload.get("macd_signal"),
                    "macd_histogram":     payload.get("macd_histogram"),
                    "histogram_expanding": payload.get("histogram_expanding"),
                    "crossover_bars_ago": payload.get("crossover_bars_ago"),
                    "current_price":      payload.get("current_price"),
                    "atr_daily":          payload.get("atr_daily"),
                    "atr_stop_loss":      payload.get("atr_stop_loss"),
                    "gate_failures":      payload.get("gate_failures", []),
                }

        macro_count = sum(1 for v in result.values() if v["macro"] is not None)
        tech_count  = sum(1 for v in result.values() if v["technical"] is not None)
        logger.info(f"Signals read: {macro_count} macro, {tech_count} technical")
        return result

    def _read_open_positions(self) -> List[Dict]:
        """Read open V1 Trend positions for this user."""
        with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT instrument, direction FROM forex_network.trades
                WHERE user_id = %s AND strategy = %s AND exit_time IS NULL
            """, (self.user_id, STRATEGY_NAME))
            return [dict(r) for r in cur.fetchall()]

    def _is_reentry_blocked(self, pair: str, direction: str) -> bool:
        """Check if re-entry block is active for this pair/direction."""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM forex_network.v1_trend_reentry_blocks
                WHERE pair = %s AND direction = %s AND blocked_until > NOW()
                LIMIT 1
            """, (pair, direction))
            return cur.fetchone() is not None

    def _has_pending_proposal(self, pair: str, direction: str) -> bool:
        """Check if an unexpired trade_approval proposal already exists for this pair/direction."""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND agent_name = %s
                  AND instrument = %s
                  AND signal_type = 'trade_approval'
                  AND expires_at > NOW()
                  AND payload->>'direction' = %s
                LIMIT 1
            """, (self.user_id, self.AGENT_NAME, pair, direction))
            return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # AND gate evaluation
    # ------------------------------------------------------------------

    def _evaluate_pair(self, pair: str, signals: Dict, open_positions: List[Dict],
                       utc_hour: int) -> Optional[Dict]:
        """
        Apply V1 Trend binary AND gate to one pair.
        Returns proposal dict if all gates pass, None otherwise.
        """
        macro = signals.get("macro")
        tech  = signals.get("technical")

        # Gate 1: both signals present
        if macro is None:
            logger.debug(f"{pair}: no macro signal")
            return None
        if tech is None:
            logger.debug(f"{pair}: no technical signal")
            return None

        # Gate 2: technical score non-zero (all tech gates passed)
        if tech["score"] == 0:
            logger.debug(f"{pair}: tech score=0, gate_failures={tech['gate_failures']}")
            return None

        # Gate 3: macro direction not neutral
        macro_direction = macro["direction"]
        if macro_direction == "neutral":
            logger.debug(f"{pair}: macro neutral")
            return None

        # Gate 4: directions agree
        tech_direction = tech["direction"]
        if tech_direction != macro_direction:
            logger.debug(f"{pair}: direction mismatch macro={macro_direction} tech={tech_direction}")
            return None

        direction = macro_direction  # confirmed: long or short

        # Gate 5: session eligibility
        session = _get_current_session(utc_hour, pair)
        if session is None:
            logger.debug(f"{pair}: session ineligible at UTC hour {utc_hour}")
            return None

        # Gate 6: max concurrent positions
        if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
            logger.debug(f"{pair}: max concurrent positions reached ({MAX_CONCURRENT_POSITIONS})")
            return None

        # Gate 7: no existing open position on this pair
        if any(p["instrument"] == pair for p in open_positions):
            logger.debug(f"{pair}: position already open")
            return None

        # Gate 8: re-entry block
        if self._is_reentry_blocked(pair, direction):
            logger.debug(f"{pair}: re-entry block active for {direction}")
            return None

        # Gate 9: no duplicate pending proposal
        if self._has_pending_proposal(pair, direction):
            logger.debug(f"{pair}: pending proposal already exists")
            return None

        # All gates passed — build proposal
        current_price = tech.get("current_price")
        atr_daily     = tech.get("atr_daily")
        atr_stop_loss = tech.get("atr_stop_loss")

        if not current_price or not atr_daily:
            logger.warning(f"{pair}: missing price/ATR data in technical payload")
            return None

        proposal = {
            "strategy":           STRATEGY_NAME,
            "pair":               pair,
            "direction":          direction,
            "setup_type":         tech.get("setup_type"),
            "session":            session,

            # Sizing inputs for RG
            "risk_pct":           RISK_PER_TRADE_PCT,
            "current_price":      current_price,
            "atr_daily":          atr_daily,
            "atr_stop_loss":      atr_stop_loss,

            # Exit config passed through to EA
            "stop_atr_mult":      ATR_STOP_MULTIPLIER,
            "t1_atr_mult":        ATR_T1_MULTIPLIER,
            "t1_close_pct":       T1_CLOSE_PCT,
            "trail_atr_mult":     ATR_TRAIL_MULTIPLIER,
            "time_stop_days":     TIME_STOP_DAYS,
            "min_rr":             MIN_RR,

            # Context at decision time
            "ema50_at_decision":  macro.get("ema50"),
            "ema200_at_decision": macro.get("ema200"),
            "macd_at_decision":   tech.get("macd_line"),
            "adx_at_decision":    tech.get("adx"),
            "rsi_at_decision":    tech.get("rsi"),

            "decided_at":         datetime.now(timezone.utc).isoformat(),
        }

        logger.info(f"✅ {pair} {direction} — all gates passed, emitting proposal")
        return proposal

    # ------------------------------------------------------------------
    # Proposal writing
    # ------------------------------------------------------------------

    def _write_proposals(self, proposals: List[Dict]) -> bool:
        if self.dry_run:
            logger.info(f"[dry-run] Would write {len(proposals)} proposals")
            return True
        if not proposals:
            return True
        try:
            with self.db_conn.cursor() as cur:
                expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.PROPOSAL_EXPIRY_MINUTES)
                for proposal in proposals:
                    cur.execute("""
                        INSERT INTO forex_network.agent_signals
                            (agent_name, user_id, instrument, signal_type,
                             score, bias, confidence, payload, expires_at, strategy)
                        VALUES (%s, %s, %s, 'trade_approval', 1.0, %s, 1.0, %s, %s, %s)
                    """, (
                        self.AGENT_NAME,
                        self.user_id,
                        proposal["pair"],
                        proposal["direction"],
                        json.dumps(proposal, default=str),
                        expires_at,
                        STRATEGY_NAME,
                    ))
            self.db_conn.commit()
            logger.info(f"Wrote {len(proposals)} proposals to agent_signals")
            return True
        except Exception as e:
            logger.error(f"_write_proposals failed: {e}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            return False

    def update_heartbeat(self) -> bool:
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.agent_heartbeats
                        (agent_name, user_id, session_id, last_seen, status, cycle_count)
                    VALUES (%s, %s, %s, NOW(), 'active', %s)
                    ON CONFLICT (agent_name, user_id) DO UPDATE SET
                        last_seen   = NOW(),
                        status      = 'active',
                        cycle_count = EXCLUDED.cycle_count,
                        session_id  = EXCLUDED.session_id
                """, (self.AGENT_NAME, self.user_id, self.session_id, self.cycle_count))
            self.db_conn.commit()
            return True
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            return False

    # ------------------------------------------------------------------
    # Cycle
    # ------------------------------------------------------------------

    def run_cycle(self) -> bool:
        cycle_start = time.time()
        self.cycle_count += 1
        logger.info(f"Cycle #{self.cycle_count} starting")

        try:
            if self.check_kill_switch():
                return False

            self.update_heartbeat()

            utc_hour       = datetime.now(timezone.utc).hour
            signals        = self._read_latest_signals()
            open_positions = self._read_open_positions()

            proposals = []
            for pair in self.PAIRS:
                try:
                    proposal = self._evaluate_pair(
                        pair, signals.get(pair, {}), open_positions, utc_hour
                    )
                    if proposal:
                        proposals.append(proposal)
                except Exception as e:
                    logger.error(f"Pair evaluation failed {pair}: {e}")

            logger.info(
                f"Cycle #{self.cycle_count}: {len(proposals)} proposals from {len(self.PAIRS)} pairs"
            )
            success = self._write_proposals(proposals)
            self.update_heartbeat()

            save_state(self.AGENT_NAME, AGENT_SCOPE_USER_ID, "cycle_count", self.cycle_count)
            logger.info(f"Cycle #{self.cycle_count} done in {time.time()-cycle_start:.2f}s")
            return success

        except Exception as e:
            logger.error(f"Cycle #{self.cycle_count} failed: {e}", exc_info=True)
            try:
                self.update_heartbeat()
            except Exception:
                pass
            return False

    def close(self):
        if hasattr(self, "db_conn"):
            try:
                self.db_conn.close()
            except Exception:
                pass
        logger.info("V1TrendOrchestrator resources cleaned up")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LAST_KNOWN_MARKET_STATE = None


def _interruptible_sleep(total_seconds: int, check_interval: int = 30) -> bool:
    global _LAST_KNOWN_MARKET_STATE
    start = time.time()
    while time.time() - start < total_seconds:
        remaining = total_seconds - (time.time() - start)
        time.sleep(min(check_interval, max(0, remaining)))
        try:
            current_state = get_market_state().get("state")
            if _LAST_KNOWN_MARKET_STATE is None:
                _LAST_KNOWN_MARKET_STATE = current_state
            elif current_state != _LAST_KNOWN_MARKET_STATE:
                logger.info(f"Market state: {_LAST_KNOWN_MARKET_STATE} → {current_state}")
                _LAST_KNOWN_MARKET_STATE = current_state
                return True
        except Exception as e:
            logger.warning(f"Market state check failed: {e}")
    return False


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
        WHERE paper_mode = TRUE
        ORDER BY user_id
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
    parser = argparse.ArgumentParser(description="V1 Trend Orchestrator")
    parser.add_argument("--user",    default=None,        help="Run for single user only")
    parser.add_argument("--single",  action="store_true", help="Single cycle then exit")
    parser.add_argument("--test",    action="store_true", help="Config test only")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()

    user_ids = [args.user] if args.user else _get_v1_trend_user_ids()
    if not user_ids:
        logger.error("No users found")
        sys.exit(1)

    logger.info(f"{len(user_ids)} user(s): {user_ids}")

    if args.test:
        for uid in user_ids:
            try:
                agent = V1TrendOrchestrator(user_id=uid, dry_run=True)
                logger.info(f"  PASS: {uid}")
                agent.close()
            except Exception as e:
                logger.error(f"  FAIL {uid}: {e}")
                sys.exit(1)
        sys.exit(0)

    agents = {}
    for uid in user_ids:
        try:
            agents[uid] = V1TrendOrchestrator(user_id=uid, dry_run=getattr(args, "dry_run", False))
        except Exception as e:
            logger.error(f"Failed to init orchestrator for {uid}: {e}")

    if not agents:
        logger.error("No orchestrators initialised")
        sys.exit(1)

    agent_list = list(agents.values())

    try:
        if args.single:
            for uid, agent in agents.items():
                logger.info(f"--- {uid} ---")
                try:
                    agent.run_cycle()
                except Exception as e:
                    logger.error(f"Cycle failed for {uid}: {e}")
            sys.exit(0)

        logger.info("Starting continuous operation")
        while True:
            market = get_market_state()
            if market["state"] in ("closed", "pre_open"):
                logger.info(f"STANDBY — {market['reason']}")
                for agent in agent_list:
                    try:
                        agent.update_heartbeat()
                    except Exception:
                        pass
                time.sleep(300)
                continue

            try:
                for agent in agent_list:
                    agent.run_cycle()
            except Exception as e:
                logger.error(f"Cycle failed: {e}", exc_info=True)
                for agent in agent_list:
                    try:
                        agent.update_heartbeat()
                    except Exception:
                        pass

            _interruptible_sleep(V1TrendOrchestrator.CYCLE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for agent in agents.values():
            try:
                agent.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
