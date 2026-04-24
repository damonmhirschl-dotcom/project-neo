#!/usr/bin/env python3
"""
Project Neo - Macro Agent v2.0 (Deterministic)
===============================================

Deterministic scoring agent — no LLM calls.

Each cycle:
  1. Reads latest composite_score per currency from macro_signals_history.
  2. Derives pair score = base_composite - quote_composite.
  3. Applies tanh soft-clamp (scale 1.5) so extreme differences compress gracefully.
  4. Applies 1.2× amplifier for cross pairs (not USD_PAIRS) then hard-clips to [-1, 1].
  5. Bias threshold: |score| > 0.05 → bullish/bearish, else neutral.
  6. Writes 20 signals to agent_signals + signal_persistence.

Keeps:  heartbeat, kill-switch, service structure, multi-user, quiet-hours sleep.
Remove: Anthropic client, MCP servers, REST API calls, all LLM logic.

Build Date: April 23, 2026
"""

import os
import sys
import json
import uuid
import time
import math
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, AGENT_SCOPE_USER_ID
from shared.schema_validator import validate_schema
from shared.system_events import log_event
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.agent_signals":    ["agent_name", "instrument", "signal_type", "score",
                                       "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats": ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.macro_signals_history": ["currency", "signal_date", "composite_score",
                                             "yield_score", "cpi_surprise_score",
                                             "employment_surprise_score", "signal_count"],
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MacroAgent:
    """
    Deterministic macro scoring agent for Project Neo.

    Reads macro_signals_history, derives 20 pair scores via tanh, writes signals.
    """

    AGENT_NAME             = "macro"
    CYCLE_INTERVAL_MINUTES = 15
    SIGNAL_EXPIRY_MINUTES  = 25
    AWS_REGION             = "eu-west-2"

    PAIRS = [
        # USD pairs
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
        # Cross pairs
        "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
        "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
        "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
    ]

    CURRENCIES = ['EUR', 'GBP', 'USD', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD']

    PAIR_DERIVATION = {
        'EURUSD': ('EUR', 'USD'), 'GBPUSD': ('GBP', 'USD'),
        'USDJPY': ('USD', 'JPY'), 'USDCHF': ('USD', 'CHF'),
        'AUDUSD': ('AUD', 'USD'), 'USDCAD': ('USD', 'CAD'),
        'NZDUSD': ('NZD', 'USD'),
        'EURGBP': ('EUR', 'GBP'), 'EURJPY': ('EUR', 'JPY'),
        'GBPJPY': ('GBP', 'JPY'), 'EURCHF': ('EUR', 'CHF'),
        'GBPCHF': ('GBP', 'CHF'), 'EURAUD': ('EUR', 'AUD'),
        'GBPAUD': ('GBP', 'AUD'), 'EURCAD': ('EUR', 'CAD'),
        'GBPCAD': ('GBP', 'CAD'), 'AUDNZD': ('AUD', 'NZD'),
        'AUDJPY': ('AUD', 'JPY'), 'CADJPY': ('CAD', 'JPY'),
        'NZDJPY': ('NZD', 'JPY'),
    }

    USD_PAIRS = {
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    }

    def __init__(self, user_id: str = "neo_user_002", dry_run: bool = False):
        self.session_id  = str(uuid.uuid4())
        self.cycle_count = 0
        self.user_id     = user_id
        self.dry_run     = dry_run

        self.ssm_client     = boto3.client('ssm',           region_name=self.AWS_REGION)
        self.secrets_client = boto3.client('secretsmanager', region_name=self.AWS_REGION)

        self._load_configuration()
        self._init_database()

        saved_count = load_state('macro', AGENT_SCOPE_USER_ID, 'cycle_count', default=0)
        if isinstance(saved_count, int) and saved_count > 0:
            self.cycle_count = saved_count
            logger.info(f"[state] Restored cycle_count={self.cycle_count}")

        logger.info(f"Macro agent (deterministic) initialised — session {self.session_id}")

    # ------------------------------------------------------------------
    # Configuration / DB
    # ------------------------------------------------------------------

    def _load_configuration(self):
        try:
            self.rds_endpoint   = self._get_parameter('/platform/config/rds-endpoint')
            self.kill_switch    = self._get_parameter('/platform/config/kill-switch')
            self.rds_credentials = self._get_secret('platform/rds/credentials')
            logger.info("Configuration loaded")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def _get_parameter(self, name: str) -> str:
        try:
            return self.ssm_client.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']
        except ClientError as e:
            logger.error(f"Failed to get parameter {name}: {e}")
            raise

    def _get_secret(self, name: str) -> Dict[str, Any]:
        try:
            return json.loads(self.secrets_client.get_secret_value(SecretId=name)['SecretString'])
        except ClientError as e:
            logger.error(f"Failed to get secret {name}: {e}")
            raise

    def _init_database(self):
        try:
            self.db_conn = psycopg2.connect(
                host=self.rds_endpoint,
                database='postgres',
                user=self.rds_credentials['username'],
                password=self.rds_credentials['password'],
                port=5432,
                options='-c search_path=forex_network,shared,public',
            )
            self.db_conn.autocommit = False
            logger.info("Database connection established")
            validate_schema(self.db_conn, EXPECTED_TABLES)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _reconnect_db_if_needed(self):
        """Reconnect if the DB connection has gone away."""
        try:
            self.db_conn.cursor().execute("SELECT 1")
        except Exception:
            logger.warning("DB connection lost — reconnecting")
            try:
                self.db_conn.close()
            except Exception:
                pass
            self._init_database()

    # ------------------------------------------------------------------
    # Kill switch
    # ------------------------------------------------------------------

    def check_kill_switch(self) -> bool:
        try:
            val = self._get_parameter('/platform/config/kill-switch')
            if val == 'active':
                logger.warning("Kill switch ACTIVE — skipping cycle")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to check kill switch: {e}")
            return True  # conservative

    # ------------------------------------------------------------------
    # Core scoring
    # ------------------------------------------------------------------

    def _get_cot_adjustment(self, currency: str) -> float:
        """
        Returns a conviction multiplier based on COT speculator positioning.
        >p90 (crowded long) → 0.5 (reduce conviction — priced in)
        <p10 (crowded short) → 1.5 (increase conviction — underpositioned)
        Otherwise → 1.0 (neutral)

        Uses pct_signed_52w (signed 52-week percentile rank) from shared.cot_positioning.
        For USD-base pairs (USDJPY, USDCAD, USDCHF), the sign is inverted so the
        percentile reflects the non-USD currency's positioning.
        """
        CURRENCY_TO_COT = {
            'EUR': 'EURUSD',
            'GBP': 'GBPUSD',
            'AUD': 'AUDUSD',
            'JPY': 'USDJPY',
            'CAD': 'USDCAD',
            'CHF': 'USDCHF',
            'USD': None,  # USD is the base — no direct COT adjustment
            'NZD': None,  # NZD not covered by COT data
        }
        USD_BASE_PAIRS = {'USDJPY', 'USDCAD', 'USDCHF'}

        pair_code = CURRENCY_TO_COT.get(currency)
        if not pair_code:
            return 1.0

        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT pct_signed_52w
                    FROM shared.cot_positioning
                    WHERE pair_code = %s
                    ORDER BY report_date DESC LIMIT 1
                """, (pair_code,))
                row = cur.fetchone()
                if not row or row[0] is None:
                    return 1.0

            pct = float(row[0])
            # Invert for USD-base pairs (e.g. USDJPY high pct = long USD = short JPY)
            if pair_code in USD_BASE_PAIRS:
                pct = 100.0 - pct

            if pct >= 90:
                logger.debug(
                    f"COT crowded long {currency} ({pct:.0f}th pctile) — reducing conviction 0.5x"
                )
                return 0.5
            elif pct <= 10:
                logger.debug(
                    f"COT crowded short {currency} ({pct:.0f}th pctile) — increasing conviction 1.5x"
                )
                return 1.5
            else:
                return 1.0
        except Exception as e:
            logger.warning(f"COT adjustment failed for {currency}: {e}")
            return 1.0

    def _compute_scores(self) -> List[Dict]:
        """
        Read latest composite_score per currency, derive 20 pair signals.

        score = tanh(raw_diff * 1.5)
        cross pairs: score = clamp(score * 1.2, -1, 1)
        bias: score >  0.05 → 'bullish'
              score < -0.05 → 'bearish'
              else          → 'neutral'
        """
        self._reconnect_db_if_needed()

        cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT DISTINCT ON (currency)
                currency,
                composite_score,
                yield_score,
                cpi_surprise_score,
                pmi_surprise_score,
                employment_surprise_score,
                signal_date,
                signal_count
            FROM forex_network.macro_signals_history
            WHERE signal_date <= CURRENT_DATE
            ORDER BY currency, signal_date DESC
        """)
        rows = cur.fetchall()

        if not rows:
            logger.warning("macro_signals_history returned no rows — emitting neutral signals")
            return self._neutral_signals("no macro data")

        currency_scores: Dict[str, dict] = {r['currency']: dict(r) for r in rows}

        missing = [c for c in self.CURRENCIES if c not in currency_scores]
        if missing:
            logger.warning(f"Missing currency scores for: {missing} — affected pairs → neutral")

        signals = []
        for pair in self.PAIRS:
            base, quote = self.PAIR_DERIVATION[pair]
            base_row  = currency_scores.get(base)
            quote_row = currency_scores.get(quote)

            if base_row is None or quote_row is None:
                signals.append(self._neutral_pair_signal(pair, f"missing score for {base if base_row is None else quote}"))
                continue

            b_comp = float(base_row['composite_score'] or 0)
            q_comp = float(quote_row['composite_score'] or 0)
            raw_diff = b_comp - q_comp

            score = math.tanh(raw_diff * 1.5)
            if pair not in self.USD_PAIRS:
                score = max(-1.0, min(1.0, score * 1.2))

            score = round(score, 4)

            if score > 0.05:
                bias = 'bullish'
            elif score < -0.05:
                bias = 'bearish'
            else:
                bias = 'neutral'

            # Confidence: how many signal components underpinned the base/quote rows
            avg_sig_count = (
                (int(base_row.get('signal_count') or 1) +
                 int(quote_row.get('signal_count') or 1)) / 2
            )
            confidence = round(min(1.0, avg_sig_count / 4.0), 3)

            payload = {
                'base_currency':        base,
                'quote_currency':       quote,
                'base_composite':       round(b_comp, 4),
                'quote_composite':      round(q_comp, 4),
                'raw_diff':             round(raw_diff, 4),
                'cross_amplifier':      1.2 if pair not in self.USD_PAIRS else 1.0,
                'base_yield_score':     round(float(base_row.get('yield_score') or 0), 4),
                'base_cpi_score':       round(float(base_row.get('cpi_surprise_score') or 0), 4),
                'base_pmi_score':       round(float(base_row.get('pmi_surprise_score') or 0), 4),
                'base_emp_score':       round(float(base_row.get('employment_surprise_score') or 0), 4),
                'quote_yield_score':    round(float(quote_row.get('yield_score') or 0), 4),
                'quote_cpi_score':      round(float(quote_row.get('cpi_surprise_score') or 0), 4),
                'quote_pmi_score':      round(float(quote_row.get('pmi_surprise_score') or 0), 4),
                'quote_emp_score':      round(float(quote_row.get('employment_surprise_score') or 0), 4),
                'base_signal_date':     str(base_row.get('signal_date', '')),
                'quote_signal_date':    str(quote_row.get('signal_date', '')),
                'method':               'deterministic_macro_v2',
                # signal_contract stubs — required by shared/signal_validator.py
                'reasoning':            (
                    f'Deterministic macro: pair_score={score:.3f} '
                    f'(base={b_comp:.3f}, quote={q_comp:.3f})'
                ),
            }

            signals.append({
                'agent_name':   self.AGENT_NAME,
                'instrument':   pair,
                'signal_type':  'macro_bias',
                'score':        score,
                'bias':         bias,
                'confidence':   confidence,
                'payload':      payload,
            })

        logger.info(
            f"_compute_scores: {len(signals)} signals — "
            f"bull={sum(1 for s in signals if s['bias']=='bullish')}, "
            f"bear={sum(1 for s in signals if s['bias']=='bearish')}, "
            f"neutral={sum(1 for s in signals if s['bias']=='neutral')}"
        )
        return signals

    def _neutral_pair_signal(self, pair: str, reason: str) -> Dict:
        return {
            'agent_name':  self.AGENT_NAME,
            'instrument':  pair,
            'signal_type': 'macro_bias',
            'score':       0.0,
            'bias':        'neutral',
            'confidence':  0.0,
            'payload':     {'reason': reason, 'method': 'deterministic_macro_v2'},
        }

    def _neutral_signals(self, reason: str) -> List[Dict]:
        return [self._neutral_pair_signal(p, reason) for p in self.PAIRS]

    # ------------------------------------------------------------------
    # DB writes
    # ------------------------------------------------------------------

    def write_signals_to_database(self, signals: List[Dict]) -> bool:
        if self.dry_run:
            logger.info(f"[dry-run] Would write {len(signals)} signals")
            return True
        try:
            with self.db_conn.cursor() as cur:
                expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.SIGNAL_EXPIRY_MINUTES)

                for signal in signals:
                    cur.execute("""
                        INSERT INTO forex_network.agent_signals
                            (agent_name, user_id, instrument, signal_type,
                             score, bias, confidence, payload, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        signal.get('agent_name', self.AGENT_NAME),
                        self.user_id,
                        signal.get('instrument'),
                        signal.get('signal_type', 'macro_bias'),
                        signal.get('score', 0.0),
                        signal.get('bias', 'neutral'),
                        signal.get('confidence', 0.0),
                        json.dumps(signal.get('payload', {})),
                        expires_at,
                    ))

                for signal in signals:
                    instrument = signal.get('instrument')
                    if not instrument:
                        continue
                    cur.execute("""
                        INSERT INTO forex_network.signal_persistence
                            (user_id, agent_name, instrument, current_bias,
                             consecutive_cycles, first_seen_at, last_updated)
                        VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                        ON CONFLICT (user_id, agent_name, instrument)
                        DO UPDATE SET
                            consecutive_cycles = CASE
                                WHEN signal_persistence.current_bias = EXCLUDED.current_bias
                                THEN LEAST(signal_persistence.consecutive_cycles + 1, 20)
                                ELSE 1
                            END,
                            current_bias = EXCLUDED.current_bias,
                            first_seen_at = CASE
                                WHEN signal_persistence.current_bias = EXCLUDED.current_bias
                                THEN signal_persistence.first_seen_at
                                ELSE NOW()
                            END,
                            last_updated = NOW()
                    """, (self.user_id, self.AGENT_NAME, instrument, signal.get('bias', 'neutral')))

            self.db_conn.commit()
            logger.info(f"Wrote {len(signals)} signals to database for {self.user_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to write signals: {e}")
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
                    ON CONFLICT (agent_name, user_id)
                    DO UPDATE SET
                        last_seen    = NOW(),
                        status       = 'active',
                        cycle_count  = EXCLUDED.cycle_count,
                        session_id   = EXCLUDED.session_id
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
        logger.info(f"Starting macro agent cycle #{self.cycle_count}")

        try:
            if self.check_kill_switch():
                return False

            self.update_heartbeat()
            signals = self._compute_scores()
            success = self.write_signals_to_database(signals)
            self.update_heartbeat()

            duration = time.time() - cycle_start
            logger.info(f"Cycle #{self.cycle_count} done in {duration:.2f}s — success={success}")
            return success

        except Exception as e:
            logger.error(f"Cycle #{self.cycle_count} failed: {e}", exc_info=True)
            try:
                self.update_heartbeat()
            except Exception:
                pass
            return False

    def close(self):
        if hasattr(self, 'db_conn'):
            try:
                self.db_conn.close()
            except Exception:
                pass
        logger.info("Macro agent resources cleaned up")


# ---------------------------------------------------------------------------
# Module-level sleep with market-state transition detection
# ---------------------------------------------------------------------------

_LAST_KNOWN_MARKET_STATE = None


def _interruptible_sleep(total_seconds: int, check_interval: int = 30) -> bool:
    """Sleep total_seconds; return True early if market state changes."""
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
                logger.info(
                    f"Market state transition: {_LAST_KNOWN_MARKET_STATE} → {current_state}. "
                    f"Breaking sleep early."
                )
                _LAST_KNOWN_MARKET_STATE = current_state
                return True
        except Exception as e:
            logger.warning(f"Market state check failed: {e}")
    return False


def _get_active_user_ids(region: str = "eu-west-2") -> List[str]:
    ssm = boto3.client("ssm", region_name=region)
    sm  = boto3.client("secretsmanager", region_name=region)
    endpoint = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    creds    = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
    user_ids = [str(r["user_id"]) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return user_ids


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Macro Agent (deterministic)")
    parser.add_argument("--user",    default=None, help="Run for a single user only")
    parser.add_argument("--single",  action="store_true", help="Single cycle then exit")
    parser.add_argument("--test",    action="store_true", help="Config test only")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()

    if args.user:
        user_ids = [args.user]
    else:
        try:
            user_ids = _get_active_user_ids()
        except Exception as e:
            logger.error(f"Failed to query active users: {e}")
            sys.exit(1)

    if not user_ids:
        logger.error("No active users found in risk_parameters")
        sys.exit(1)

    logger.info(f"{len(user_ids)} active user(s): {user_ids}")

    if args.test:
        for uid in user_ids:
            try:
                agent = MacroAgent(user_id=uid, dry_run=True)
                logger.info(f"  PASS: {uid}")
                agent.close()
            except Exception as e:
                logger.error(f"  FAIL {uid}: {e}")
                sys.exit(1)
        logger.info("All users configured successfully")
        sys.exit(0)

    agents: Dict[str, MacroAgent] = {}
    for uid in user_ids:
        try:
            agents[uid] = MacroAgent(user_id=uid, dry_run=getattr(args, 'dry_run', False))
            logger.info(f"Initialised Macro Agent for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialise Macro Agent for {uid}: {e}")

    if not agents:
        logger.error("No agents initialised — exiting")
        sys.exit(1)

    agent_list = list(agents.values())
    primary    = agent_list[0]

    try:
        if args.single:
            for uid, agent in agents.items():
                logger.info(f"--- {uid} ---")
                try:
                    agent.run_cycle()
                except Exception as e:
                    logger.error(f"Cycle failed for {uid}: {e}")
            logger.info("Single cycle complete")
            sys.exit(0)

        # Continuous mode — scores are identical for all users (read from shared table)
        logger.info("Starting continuous operation")
        while True:
            market = get_market_state()

            if market['state'] in ('closed', 'pre_open'):
                logger.info(f"STANDBY — {market['reason']}, skipping cycle")
                for agent in agent_list:
                    try:
                        agent.update_heartbeat()
                    except Exception:
                        pass
                time.sleep(300)
                continue

            if market['state'] == 'quiet':
                logger.info(f"QUIET HOURS — {market['reason']}, 60-min reduced cycle")

            try:
                if primary.check_kill_switch():
                    logger.warning("Kill switch active — skipping cycle")
                    time.sleep(60)
                    continue

                for agent in agent_list:
                    agent.cycle_count += 1
                    agent.update_heartbeat()
                save_state('macro', AGENT_SCOPE_USER_ID, 'cycle_count', primary.cycle_count)

                # Scores are purely from macro_signals_history — compute once
                signals = primary._compute_scores()

                for uid, agent in agents.items():
                    # Each user gets their own signal rows (different user_id)
                    agent.write_signals_to_database(signals)
                    agent.update_heartbeat()

            except Exception as e:
                logger.error(f"Cycle failed: {e}", exc_info=True)
                for agent in agent_list:
                    try:
                        agent.update_heartbeat()
                    except Exception:
                        pass

            # Sleep
            if market['state'] == 'quiet':
                _sleep_start  = time.time()
                _sleep_total  = 3600
                while time.time() - _sleep_start < _sleep_total:
                    time.sleep(30)
                    for _agent in agent_list:
                        try:
                            _agent.update_heartbeat()
                        except Exception:
                            pass
                    import datetime as _dt
                    _now = _dt.datetime.now(_dt.timezone.utc)
                    _hr, _min = _now.hour, _now.minute
                    _pre_open = (
                        (_hr == 6  and _min >= 40) or
                        (_hr == 12 and _min >= 40)
                    )
                    if _pre_open:
                        logger.info(f"Pre-open wake at {_hr:02d}:{_min:02d} UTC — firing cycle")
                        break
                    try:
                        if get_market_state().get('state') != 'quiet':
                            logger.info("Market state left quiet — breaking sleep")
                            break
                    except Exception:
                        pass
            else:
                _interruptible_sleep(primary.CYCLE_INTERVAL_MINUTES * 60)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for uid, agent in agents.items():
            try:
                agent.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
