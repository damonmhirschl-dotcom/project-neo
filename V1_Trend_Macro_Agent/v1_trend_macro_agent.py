#!/usr/bin/env python3
"""
Project Neo - V1 Trend Macro Agent v1.0 (Deterministic)
=======================================================

EMA50/EMA200 regime classifier — no macro scoring, no COT, no tanh.

Each cycle:
  1. Fetches last 200 daily closes per pair from historical_prices.
  2. Computes EMA-50 and EMA-200 from closes.
  3. Classifies regime:
       EMA50 > EMA200 → bullish  (score = +spread)
       EMA50 < EMA200 → bearish  (score = -spread)
       spread < threshold → neutral
  4. Writes 22 signals to agent_signals + signal_persistence.

Keeps:  heartbeat, kill-switch, service structure, multi-user, quiet-hours sleep.
No:     macro_signals_history, COT, tanh, cross-pair amplifier.

Build Date: April 25, 2026
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
from typing import Dict, List, Optional, Any
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, AGENT_SCOPE_USER_ID
from shared.schema_validator import validate_schema
from shared.system_events import log_event
from shared.warn_log import warn
from v1_swing_parameters import V1_SWING_PAIRS
from shared.schemas.v1_swing_payloads import validate_macro_payload

EXPECTED_TABLES = {
    "forex_network.agent_signals":    ["agent_name", "instrument", "signal_type", "score",
                                       "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats": ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.historical_prices": ["instrument", "timeframe", "ts", "close"],
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _ema(closes: List[float], period: int) -> float:
    """Compute EMA over a list of closes (oldest-first). Returns final EMA value."""
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period          # seed with SMA
    for price in closes[period:]:
        ema = price * k + ema * (1.0 - k)
    return ema


class V1TrendMacroAgent:
    """
    EMA50/EMA200 regime agent for Project Neo V1 Trend strategy.

    Reads daily closes from historical_prices, computes EMA regime, writes signals.
    """

    AGENT_NAME             = "v1_trend_macro"
    CYCLE_INTERVAL_MINUTES = 15
    SIGNAL_EXPIRY_MINUTES  = 60
    AWS_REGION             = "eu-west-2"

    PAIRS = V1_SWING_PAIRS          # canonical 22-pair universe

    EMA_FAST              = 50
    EMA_SLOW              = 200
    LOOKBACK_DAYS         = 300     # fetch enough candles for EMA-200 warm-up
    NEUTRAL_THRESHOLD_PCT = 0.001   # |spread| < 0.1% of price → neutral

    def __init__(self, user_id: str = "neo_user_002", dry_run: bool = False):
        self.session_id  = str(uuid.uuid4())
        self.cycle_count = 0
        self.user_id     = user_id
        self.dry_run     = dry_run

        self.ssm_client     = boto3.client('ssm',           region_name=self.AWS_REGION)
        self.secrets_client = boto3.client('secretsmanager', region_name=self.AWS_REGION)

        self._load_configuration()
        self._init_database()

        saved_count = load_state('v1_trend_macro', AGENT_SCOPE_USER_ID, 'cycle_count', default=0)
        if isinstance(saved_count, int) and saved_count > 0:
            self.cycle_count = saved_count
            logger.info(f"[state] Restored cycle_count={self.cycle_count}")

        logger.info(f"V1 Trend Macro agent initialised — session {self.session_id}")

    # ------------------------------------------------------------------
    # Configuration / DB
    # ------------------------------------------------------------------

    def _load_configuration(self):
        try:
            self.rds_endpoint    = self._get_parameter('/platform/config/rds-endpoint')
            self.kill_switch     = self._get_parameter('/platform/config/kill-switch')
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
    # Core scoring — EMA regime
    # ------------------------------------------------------------------

    def _fetch_daily_closes(self) -> Dict[str, List[float]]:
        """Fetch last LOOKBACK_DAYS daily closes per pair, oldest-first."""
        self._reconnect_db_if_needed()

        cur = self.db_conn.cursor()
        cur.execute("""
            SELECT instrument, ts, close
            FROM forex_network.historical_prices
            WHERE timeframe = '1D'
              AND instrument = ANY(%s)
              AND ts >= NOW() - INTERVAL '%s days'
            ORDER BY instrument, ts ASC
        """ % ('%s', self.LOOKBACK_DAYS), (self.PAIRS,))

        closes: Dict[str, List[float]] = {p: [] for p in self.PAIRS}
        for instrument, ts, close_price in cur.fetchall():
            if instrument in closes:
                closes[instrument].append(float(close_price))

        return closes

    def _compute_scores(self) -> List[Dict]:
        """
        Compute EMA-50 / EMA-200 regime for each pair.

        score  = (ema50 - ema200) / ema200   (normalised spread)
        regime = bullish if spread > threshold, bearish if < -threshold, else neutral
        direction = long / short / neutral
        confidence = min(1.0, |spread| / 0.02)  — saturates at 2% spread
        """
        closes_by_pair = self._fetch_daily_closes()

        signals = []
        for pair in self.PAIRS:
            closes = closes_by_pair.get(pair, [])

            if len(closes) < self.EMA_SLOW:
                logger.warning(f"{pair}: only {len(closes)} daily candles, need {self.EMA_SLOW} — neutral")
                signals.append(self._neutral_pair_signal(pair, f"insufficient data ({len(closes)} candles)"))
                continue

            ema_fast = _ema(closes, self.EMA_FAST)
            ema_slow = _ema(closes, self.EMA_SLOW)
            current_price = closes[-1]

            if ema_slow == 0:
                signals.append(self._neutral_pair_signal(pair, "ema200 is zero"))
                continue

            spread = (ema_fast - ema_slow) / ema_slow
            score  = round(spread, 6)

            if abs(spread) < self.NEUTRAL_THRESHOLD_PCT:
                bias = 'neutral'
                direction = 'neutral'
            elif spread > 0:
                bias = 'bullish'
                direction = 'long'
            else:
                bias = 'bearish'
                direction = 'short'

            confidence = round(min(1.0, abs(spread) / 0.02), 3)

            payload = {
                'score':         score,
                'direction':     direction,
                'ema_fast':      round(ema_fast, 6),
                'ema_slow':      round(ema_slow, 6),
                'spread':        score,
                'current_price': round(current_price, 6),
                'candle_count':  len(closes),
                'method':        'ema_regime_v1',
                'reasoning':     (
                    f"EMA{self.EMA_FAST}={ema_fast:.5f} vs EMA{self.EMA_SLOW}={ema_slow:.5f} "
                    f"spread={spread:+.4%} → {direction}"
                ),
            }

            validate_macro_payload(payload)
            signals.append({
                'agent_name':  self.AGENT_NAME,
                'instrument':  pair,
                'signal_type': 'macro_bias',
                'score':       score,
                'bias':        bias,
                'confidence':  confidence,
                'payload':     payload,
            })

        logger.info(
            f"_compute_scores: {len(signals)} signals — "
            f"bull={sum(1 for s in signals if s['bias'] == 'bullish')}, "
            f"bear={sum(1 for s in signals if s['bias'] == 'bearish')}, "
            f"neutral={sum(1 for s in signals if s['bias'] == 'neutral')}"
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
            'payload':     {'reason': reason, 'method': 'ema_regime_v1'},
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

                _instruments = [s.get('instrument') for s in signals if s.get('instrument')]
                if _instruments:
                    cur.execute("""
                        DELETE FROM forex_network.agent_signals
                        WHERE user_id = %s AND agent_name = %s AND instrument = ANY(%s)
                    """, (self.user_id, self.AGENT_NAME, _instruments))

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
        logger.info(f"Starting V1 Trend Macro agent cycle #{self.cycle_count}")

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
        logger.info("V1 Trend Macro agent resources cleaned up")


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

    parser = argparse.ArgumentParser(description="Project Neo V1 Trend Macro Agent (EMA regime)")
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
                agent = V1TrendMacroAgent(user_id=uid, dry_run=True)
                logger.info(f"  PASS: {uid}")
                agent.close()
            except Exception as e:
                logger.error(f"  FAIL {uid}: {e}")
                sys.exit(1)
        logger.info("All users configured successfully")
        sys.exit(0)

    agents: Dict[str, V1TrendMacroAgent] = {}
    for uid in user_ids:
        try:
            agents[uid] = V1TrendMacroAgent(user_id=uid, dry_run=getattr(args, 'dry_run', False))
            logger.info(f"Initialised V1 Trend Macro Agent for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialise V1 Trend Macro Agent for {uid}: {e}")

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

        # Continuous mode — EMA regime is identical for all users
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
                save_state('v1_trend_macro', AGENT_SCOPE_USER_ID, 'cycle_count', primary.cycle_count)

                signals = primary._compute_scores()

                for uid, agent in agents.items():
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
