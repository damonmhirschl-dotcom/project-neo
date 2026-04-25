#!/usr/bin/env python3
"""
Project Neo - V1 Trend Technical Agent v1.0
=============================================

Pure technical signal agent for V1 Trend strategy. No LLM calls.

Each cycle:
  1. Fetches 1H bars, resamples to 4H.
  2. Computes ADX(14, Wilder), MACD(12,26,9), RSI(14) on 4H.
  3. Reads ATR(14) daily from price_metrics.
  4. Applies binary AND gate:
       ADX > 25
       AND MACD crossover within last 2 bars
       AND MACD histogram expanding
       AND RSI > 50 (long) or < 50 (short)
  5. Emits setup signal per pair tagged strategy=v1_trend.

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
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, "/root/Project_Neo_Damon")
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, AGENT_SCOPE_USER_ID
from shared.schema_validator import validate_schema
from v1_swing_parameters import V1_SWING_PAIRS
from shared.v1_trend_parameters import (
    STRATEGY_NAME,
    ADX_PERIOD,
    ADX_GATE,
    MACD_FAST,
    MACD_SLOW,
    MACD_SIGNAL,
    MACD_LOOKBACK_BARS,
    RSI_PERIOD,
    RSI_LONG_GATE,
    RSI_SHORT_GATE,
    ATR_PERIOD,
    ATR_STOP_MULTIPLIER,
    SESSION_WINDOWS_UTC,
    ASIA_SESSION_PAIRS,
)

EXPECTED_TABLES = {
    "forex_network.agent_signals":     ["agent_name", "instrument", "signal_type",
                                        "score", "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats":  ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.historical_prices": ["instrument", "timeframe", "ts", "open", "high", "low", "close"],
    "forex_network.price_metrics":     ["instrument", "timeframe", "ts", "atr_14"],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Indicator computations (pure Python)
# ---------------------------------------------------------------------------

def _compute_ema(closes: List[float], period: int) -> List[Optional[float]]:
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1)
    result = [None] * (period - 1)
    ema = sum(closes[:period]) / period
    result.append(ema)
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
        result.append(ema)
    return result


def _compute_macd(closes: List[float], fast: int, slow: int, signal: int
                  ) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    """Returns (macd_line, signal_line, histogram) — all same length as closes."""
    ema_fast = _compute_ema(closes, fast)
    ema_slow = _compute_ema(closes, slow)

    macd_line = []
    for f, s in zip(ema_fast, ema_slow):
        macd_line.append(round(f - s, 8) if f is not None and s is not None else None)

    # Signal line = EMA of MACD line (using only non-None values)
    valid_start = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line = [None] * len(macd_line)
    if valid_start is not None:
        valid_macd = [v for v in macd_line if v is not None]
        if len(valid_macd) >= signal:
            sig_ema = _compute_ema(valid_macd, signal)
            for i, val in enumerate(sig_ema):
                signal_line[valid_start + i] = val

    histogram = []
    for m, s in zip(macd_line, signal_line):
        histogram.append(round(m - s, 8) if m is not None and s is not None else None)

    return macd_line, signal_line, histogram


def _compute_rsi(closes: List[float], period: int) -> List[Optional[float]]:
    if len(closes) < period + 1:
        return [None] * len(closes)
    result = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    result.append(round(100 - 100 / (1 + rs), 4))
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(diff, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        result.append(round(100 - 100 / (1 + rs), 4))
    return result


def _compute_adx_wilder(highs: List[float], lows: List[float], closes: List[float],
                         period: int) -> List[Optional[float]]:
    """Wilder-smoothed ADX. Returns list same length as input."""
    n = len(closes)
    if n < period * 2:
        return [None] * n

    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        up   = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        tr_list.append(tr)

    def wilder_smooth(data, p):
        result = [None] * (p - 1)
        s = sum(data[:p])
        result.append(s)
        for v in data[p:]:
            s = s - s / p + v
            result.append(s)
        return result

    atr_w   = wilder_smooth(tr_list, period)
    plus_w  = wilder_smooth(plus_dm, period)
    minus_w = wilder_smooth(minus_dm, period)

    di_plus, di_minus, dx_list = [], [], []
    for a, p, m in zip(atr_w, plus_w, minus_w):
        if a is None or a == 0:
            di_plus.append(None)
            di_minus.append(None)
            dx_list.append(None)
        else:
            dip = 100 * p / a
            dim = 100 * m / a
            di_plus.append(dip)
            di_minus.append(dim)
            diff = abs(dip - dim)
            summ = dip + dim
            dx_list.append(100 * diff / summ if summ != 0 else 0)

    # ADX = Wilder smooth of DX
    valid_dx = [(i, v) for i, v in enumerate(dx_list) if v is not None]
    adx_result = [None] * (n)  # offset by 1 because tr starts at index 1
    if len(valid_dx) >= period:
        start_idx, _ = valid_dx[0]
        dx_values = [v for _, v in valid_dx]
        adx_smooth = wilder_smooth(dx_values, period)
        for i, val in enumerate(adx_smooth):
            result_idx = start_idx + i + 1  # +1 for the offset
            if result_idx < n and val is not None:
                adx_result[result_idx] = round(val, 4)

    return adx_result


def _resample_1h_to_4h(bars_1h: List[Dict]) -> List[Dict]:
    """Aggregate 1H OHLCV bars into 4H bars. Groups by floor(hour/4)."""
    if not bars_1h:
        return []
    buckets: Dict[datetime, Dict] = {}
    for bar in bars_1h:
        ts = bar["ts"]
        bucket_ts = ts.replace(
            hour=(ts.hour // 4) * 4, minute=0, second=0, microsecond=0
        )
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "ts": bucket_ts,
                "open":  bar["open"],
                "high":  bar["high"],
                "low":   bar["low"],
                "close": bar["close"],
                "volume": bar.get("volume") or 0,
            }
        else:
            b = buckets[bucket_ts]
            b["high"]   = max(b["high"], bar["high"])
            b["low"]    = min(b["low"],  bar["low"])
            b["close"]  = bar["close"]
            b["volume"] = (b["volume"] or 0) + (bar.get("volume") or 0)
    return sorted(buckets.values(), key=lambda x: x["ts"])


def _get_current_session(utc_hour: int, pair: str) -> Optional[str]:
    """Return session name or None if dead zone / pair not eligible."""
    for session, (start, end) in SESSION_WINDOWS_UTC.items():
        if session == "dead_zone":
            continue
        if start <= utc_hour < end:
            if session == "asia" and pair not in ASIA_SESSION_PAIRS:
                return None
            return session
    return None  # dead zone


# ---------------------------------------------------------------------------
# Main agent class
# ---------------------------------------------------------------------------

class V1TrendTechnicalAgent:
    """
    Technical signal agent for V1 Trend strategy.
    Computes MACD, ADX, RSI on 4H bars. Binary gate — trade or don't.
    """

    AGENT_NAME             = "v1_trend_technical"
    CYCLE_INTERVAL_MINUTES = 5
    SIGNAL_EXPIRY_MINUTES  = 25
    AWS_REGION             = "eu-west-2"
    PAIRS                  = V1_SWING_PAIRS
    BARS_1H_NEEDED         = 400   # ~17 days of 1H — enough for 4H MACD(12,26,9) + ADX(14)

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
        logger.info(f"V1TrendTechnicalAgent initialised — user={user_id} session={self.session_id}")

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
    # Data fetching
    # ------------------------------------------------------------------

    def _fetch_1h_bars(self, pair: str) -> List[Dict]:
        self._reconnect_db_if_needed()
        with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ts, open, high, low, close, volume
                FROM forex_network.historical_prices
                WHERE instrument = %s AND timeframe = '1H'
                ORDER BY ts DESC LIMIT %s
            """, (pair, self.BARS_1H_NEEDED))
            rows = cur.fetchall()
        bars = [dict(r) for r in reversed(rows)]
        return bars

    def _fetch_daily_atr(self, pair: str) -> Optional[float]:
        with self.db_conn.cursor() as cur:
            cur.execute("""
                SELECT atr_14 FROM forex_network.price_metrics
                WHERE instrument = %s AND timeframe = '1D' AND atr_14 IS NOT NULL
                ORDER BY ts DESC LIMIT 1
            """, (pair,))
            row = cur.fetchone()
        return float(row[0]) if row else None

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def _generate_signal(self, pair: str, utc_hour: int) -> Dict:
        """
        Evaluate V1 Trend entry gates for one pair.
        Returns signal dict with score=1.0 (pass) or 0.0 (fail).
        """
        gate_failures = []
        now = datetime.now(timezone.utc)

        # Session check
        session = _get_current_session(utc_hour, pair)
        if session is None:
            return self._no_signal(pair, ["dead_zone_or_session_ineligible"], now)

        # Fetch and resample
        bars_1h = self._fetch_1h_bars(pair)
        if len(bars_1h) < 100:
            return self._no_signal(pair, [f"insufficient_1h_bars:{len(bars_1h)}"], now)

        bars_4h = _resample_1h_to_4h(bars_1h)
        if len(bars_4h) < 50:
            return self._no_signal(pair, [f"insufficient_4h_bars:{len(bars_4h)}"], now)

        closes_4h = [b["close"] for b in bars_4h]
        highs_4h  = [b["high"]  for b in bars_4h]
        lows_4h   = [b["low"]   for b in bars_4h]

        # --- ADX ---
        adx_series = _compute_adx_wilder(highs_4h, lows_4h, closes_4h, ADX_PERIOD)
        adx_now = next((v for v in reversed(adx_series) if v is not None), None)
        if adx_now is None:
            return self._no_signal(pair, ["adx_computation_failed"], now)
        if adx_now <= ADX_GATE:
            gate_failures.append(f"adx_below_gate:{adx_now:.1f}<={ADX_GATE}")

        # --- MACD ---
        macd_line, signal_line, histogram = _compute_macd(
            closes_4h, MACD_FAST, MACD_SLOW, MACD_SIGNAL
        )
        # Get last N valid histogram values
        valid_hist = [(i, v) for i, v in enumerate(histogram) if v is not None]
        if len(valid_hist) < 3:
            return self._no_signal(pair, ["macd_computation_failed"], now)

        macd_now      = next((v for v in reversed(macd_line)    if v is not None), None)
        signal_now    = next((v for v in reversed(signal_line)  if v is not None), None)
        hist_now      = valid_hist[-1][1]
        hist_prev     = valid_hist[-2][1]
        hist_prev2    = valid_hist[-3][1]

        # Crossover detection: MACD crossed signal within last MACD_LOOKBACK_BARS bars
        crossover_bars_ago = None
        crossover_direction = None
        for lookback in range(1, MACD_LOOKBACK_BARS + 2):
            if len(valid_hist) < lookback + 1:
                break
            h_curr = valid_hist[-lookback][1]
            h_prev = valid_hist[-lookback - 1][1]
            if h_prev < 0 and h_curr >= 0:
                crossover_bars_ago  = lookback
                crossover_direction = "bullish"
                break
            elif h_prev > 0 and h_curr <= 0:
                crossover_bars_ago  = lookback
                crossover_direction = "bearish"
                break

        if crossover_bars_ago is None or crossover_bars_ago > MACD_LOOKBACK_BARS:
            gate_failures.append(f"no_macd_crossover_within_{MACD_LOOKBACK_BARS}_bars")

        # Histogram expanding
        histogram_expanding_long  = abs(hist_now) > abs(hist_prev) and hist_now > 0
        histogram_expanding_short = abs(hist_now) > abs(hist_prev) and hist_now < 0
        histogram_expanding = histogram_expanding_long or histogram_expanding_short

        if not histogram_expanding:
            gate_failures.append("histogram_not_expanding")

        # --- RSI ---
        rsi_series = _compute_rsi(closes_4h, RSI_PERIOD)
        rsi_now = next((v for v in reversed(rsi_series) if v is not None), None)
        if rsi_now is None:
            return self._no_signal(pair, ["rsi_computation_failed"], now)

        # Determine attempted direction from MACD crossover
        if crossover_direction == "bullish":
            attempted_direction = "long"
            if rsi_now <= RSI_LONG_GATE:
                gate_failures.append(f"rsi_not_above_{RSI_LONG_GATE}:{rsi_now:.1f}")
        elif crossover_direction == "bearish":
            attempted_direction = "short"
            if rsi_now >= RSI_SHORT_GATE:
                gate_failures.append(f"rsi_not_below_{RSI_SHORT_GATE}:{rsi_now:.1f}")
        else:
            attempted_direction = None

        # --- ATR ---
        atr_daily = self._fetch_daily_atr(pair)
        if atr_daily is None:
            return self._no_signal(pair, ["atr_daily_missing"], now)

        current_price = closes_4h[-1]
        atr_stop_loss = (
            current_price - ATR_STOP_MULTIPLIER * atr_daily
            if attempted_direction == "long"
            else current_price + ATR_STOP_MULTIPLIER * atr_daily
            if attempted_direction == "short"
            else current_price - ATR_STOP_MULTIPLIER * atr_daily  # fallback
        )

        # --- Build payload ---
        all_gates_passed = len(gate_failures) == 0 and attempted_direction is not None

        setup_type = None
        direction  = None
        score      = 0.0

        if all_gates_passed:
            direction  = attempted_direction
            setup_type = "trend_long" if direction == "long" else "trend_short"
            score      = 1.0

        payload = {
            "strategy":            STRATEGY_NAME,
            "pair":                pair,
            "direction":           direction,
            "setup_type":          setup_type,
            "score":               score,
            "adx":                 round(adx_now, 4),
            "rsi":                 round(rsi_now, 4),
            "macd_line":           round(macd_now, 8) if macd_now else 0.0,
            "macd_signal":         round(signal_now, 8) if signal_now else 0.0,
            "macd_histogram":      round(hist_now, 8),
            "macd_histogram_prev": round(hist_prev, 8),
            "histogram_expanding": histogram_expanding,
            "crossover_bars_ago":  crossover_bars_ago,
            "current_price":       round(current_price, 6),
            "atr_daily":           round(atr_daily, 8),
            "atr_stop_loss":       round(atr_stop_loss, 6),
            "gate_failures":       gate_failures,
            "computed_at":         now.isoformat(),
        }

        return {
            "agent_name":  self.AGENT_NAME,
            "instrument":  pair,
            "signal_type": "technical_signal",
            "score":       score,
            "bias":        "bullish" if direction == "long" else "bearish" if direction == "short" else "neutral",
            "confidence":  1.0 if all_gates_passed else 0.0,
            "strategy":    STRATEGY_NAME,
            "payload":     payload,
        }

    def _no_signal(self, pair: str, gate_failures: List[str], now: datetime) -> Dict:
        payload = {
            "strategy":            STRATEGY_NAME,
            "pair":                pair,
            "direction":           None,
            "setup_type":          None,
            "score":               0.0,
            "adx":                 0.0,
            "rsi":                 0.0,
            "macd_line":           0.0,
            "macd_signal":         0.0,
            "macd_histogram":      0.0,
            "macd_histogram_prev": 0.0,
            "histogram_expanding": False,
            "crossover_bars_ago":  None,
            "current_price":       0.0,
            "atr_daily":           0.0,
            "atr_stop_loss":       0.0,
            "gate_failures":       gate_failures,
            "computed_at":         now.isoformat(),
        }
        return {
            "agent_name":  self.AGENT_NAME,
            "instrument":  pair,
            "signal_type": "technical_signal",
            "score":       0.0,
            "bias":        "neutral",
            "confidence":  0.0,
            "strategy":    STRATEGY_NAME,
            "payload":     payload,
        }

    def generate_all_signals(self) -> List[Dict]:
        utc_hour = datetime.now(timezone.utc).hour
        signals  = []
        for pair in self.PAIRS:
            try:
                signals.append(self._generate_signal(pair, utc_hour))
            except Exception as e:
                logger.error(f"Signal generation failed for {pair}: {e}")
                signals.append(self._no_signal(
                    pair, [f"exception:{e}"], datetime.now(timezone.utc)
                ))

        pass_count = sum(1 for s in signals if s["score"] > 0)
        logger.info(f"Technical signals: {len(signals)} pairs, {pass_count} passed all gates")
        return signals

    # ------------------------------------------------------------------
    # DB writes
    # ------------------------------------------------------------------

    def write_signals_to_database(self, signals: List[Dict]) -> bool:
        if self.dry_run:
            logger.info(f"[dry-run] Would write {len(signals)} signals")
            return True
        try:
            with self.db_conn.cursor() as cur:
                expires_at  = datetime.now(timezone.utc) + timedelta(minutes=self.SIGNAL_EXPIRY_MINUTES)
                instruments = [s["instrument"] for s in signals]
                if instruments:
                    cur.execute("""
                        DELETE FROM forex_network.agent_signals
                        WHERE user_id = %s AND agent_name = %s AND instrument = ANY(%s)
                    """, (self.user_id, self.AGENT_NAME, instruments))

                for signal in signals:
                    cur.execute("""
                        INSERT INTO forex_network.agent_signals
                            (agent_name, user_id, instrument, signal_type,
                             score, bias, confidence, payload, expires_at, strategy)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        self.AGENT_NAME,
                        self.user_id,
                        signal["instrument"],
                        signal["signal_type"],
                        signal["score"],
                        signal["bias"],
                        signal["confidence"],
                        json.dumps(signal["payload"], default=str),
                        expires_at,
                        STRATEGY_NAME,
                    ))
            self.db_conn.commit()
            logger.info(f"Wrote {len(signals)} technical signals for user={self.user_id}")
            return True
        except Exception as e:
            logger.error(f"write_signals_to_database failed: {e}")
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
            signals = self.generate_all_signals()
            success = self.write_signals_to_database(signals)
            self.update_heartbeat()
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
        logger.info("V1TrendTechnicalAgent resources cleaned up")


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
                logger.info(f"Market state: {_LAST_KNOWN_MARKET_STATE} -> {current_state}")
                _LAST_KNOWN_MARKET_STATE = current_state
                return True
        except Exception as e:
            logger.warning(f"Market state check failed: {e}")
    return False


def _get_v1_trend_user_ids(region: str = "eu-west-2") -> List[str]:
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
    parser = argparse.ArgumentParser(description="V1 Trend Technical Agent")
    parser.add_argument("--user",    default=None,        help="Run for single user only")
    parser.add_argument("--single",  action="store_true", help="Single cycle then exit")
    parser.add_argument("--test",    action="store_true", help="Config test only")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()

    user_ids = [args.user] if args.user else _get_v1_trend_user_ids()
    if not user_ids:
        logger.error("No V1 Trend users found")
        sys.exit(1)

    logger.info(f"{len(user_ids)} V1 Trend user(s): {user_ids}")

    if args.test:
        for uid in user_ids:
            try:
                agent = V1TrendTechnicalAgent(user_id=uid, dry_run=True)
                logger.info(f"  PASS: {uid}")
                agent.close()
            except Exception as e:
                logger.error(f"  FAIL {uid}: {e}")
                sys.exit(1)
        sys.exit(0)

    agents = {}
    for uid in user_ids:
        try:
            agents[uid] = V1TrendTechnicalAgent(user_id=uid, dry_run=getattr(args, "dry_run", False))
        except Exception as e:
            logger.error(f"Failed to init agent for {uid}: {e}")

    if not agents:
        logger.error("No agents initialised")
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
                if primary.check_kill_switch():
                    time.sleep(60)
                    continue

                for agent in agent_list:
                    agent.cycle_count += 1
                    agent.update_heartbeat()
                save_state(primary.AGENT_NAME, AGENT_SCOPE_USER_ID, "cycle_count", primary.cycle_count)

                signals = primary.generate_all_signals()
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

            _interruptible_sleep(primary.CYCLE_INTERVAL_MINUTES * 60)

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
