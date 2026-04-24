#!/usr/bin/env python3
"""
Project Neo — Regime Agent v1.0
================================
Classifies market regime, computes system stress score (0-100),
determines session context, and writes signals for the orchestrator.

Decision rules embedded: R1-R5
Adversarial defence rules: 9, 10 (cross-provider price sanity, spread sanity)
Gap C: Regime transition confidence
Gap D: Data staleness detection

Cycle: Every 15 minutes
IAM Role: platform-forex-role-dev
Signal expiry: 20 minutes

Build spec: 3424d2ec-e676-81ed-984c-d9459810f03b
Decision rules: 3444d2ec-e676-81d1-8bde-e2da050ef260
Stress score spec: 3444d2ec-e676-81d4-b748-c1a92c670ee1
"""

import json
import logging
import os
import sys
import time
import uuid
import random
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Dict, List, Tuple, Any

import boto3
import psycopg2
import psycopg2.extras
import requests

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, log_loaded_state_summary
from shared.score_trajectory import get_recent_trajectory, get_recent_trajectory_batch, analyse_trajectory
from shared.schema_validator import validate_schema
from shared.system_events import log_event
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.agent_signals":        ["agent_name", "instrument", "signal_type", "score",
                                           "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats":     ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.system_alerts":        ["alert_type", "severity", "title", "detail",
                                           "acknowledged", "created_at"],
    "shared.market_context_snapshots":    ["system_stress_score", "stress_state",
                                           "stress_components", "snapshot_time"],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("regime_agent")


def log_api_call(db_conn, provider, endpoint, agent_name, success,
                 response_time_ms, error_type=None, pairs_returned=0,
                 data_age_seconds=0):
    try:
        cur = db_conn.cursor()
        cur.execute("""
            INSERT INTO forex_network.api_call_log
                (provider, endpoint, agent_name, success, response_time_ms,
                 error_type, pairs_returned, data_age_seconds, called_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (provider, endpoint, agent_name, success, response_time_ms,
              error_type, pairs_returned, data_age_seconds))
        db_conn.commit()
    except Exception:
        try:
            db_conn.rollback()
        except Exception:
            pass

# Module-level cache for Finnhub general news sentiment (1-hour TTL)
_finnhub_news_cache: tuple = (None, 0.0)  # (score: Optional[float], timestamp: float)
_FINNHUB_NEWS_CACHE_TTL = 3600  # seconds


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REGION = "eu-west-2"
DB_NAME = "postgres"
SCHEMA_FOREX = "forex_network"
SCHEMA_SHARED = "shared"

PAIRS = [
    # USD pairs
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Cross pairs — confirmed on IG demo 2026-04-22
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
    "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
    "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
]

AGENT_NAME = "regime"
SIGNAL_EXPIRY_MINUTES = 20
HEARTBEAT_INTERVAL_SECONDS = 60
CYCLE_INTERVAL_SECONDS = 900  # 15 minutes

# Stress score component weights (must sum to 1.0)
# order_flow_anomaly wired 2026-04-21: MyFXBook SSI + IG client sentiment
# Weights: correlation_breakdown 0.17→0.14, cot_extremes 0.06→0.03, order_flow_anomaly +0.06
STRESS_WEIGHTS = {
    "vix_level_trend":         0.17,
    "yield_curve_slope":       0.17,
    "realised_vol_divergence": 0.21,
    "correlation_breakdown":   0.14,  # reduced from 0.17 to fund order_flow_anomaly
    "cot_extremes":            0.03,  # reduced from 0.06 to fund order_flow_anomaly
    "geopolitical_index":      0.11,
    "cross_asset_risk":        0.11,  # equity/gold/oil/DXY risk-off detection
    "order_flow_anomaly":      0.06,  # MyFXBook SSI + IG client sentiment extremes
    # weights sum: 0.17+0.17+0.21+0.14+0.03+0.11+0.11+0.06 = 1.00
}

# Stress state thresholds
STRESS_THRESHOLDS = [
    (0,  30, "normal"),
    (30, 50, "elevated"),
    (50, 70, "high"),
    (70, 85, "pre_crisis"),
    (85, 100, "crisis"),
]

# ADX regime thresholds
ADX_TRENDING = 25
ADX_RANGING = 20

# Volatility regime thresholds (relative to 30-day average)
VOL_HIGH_MULTIPLIER = 1.5
VOL_LOW_MULTIPLIER = 0.5

# R1: Boundary oscillation detection
R1_BOUNDARY_TOLERANCE = 3       # ±3 points detection window
R1_OSCILLATION_CYCLES = 4       # 4+ consecutive cycles before escalation
R1_RECOVERY_MARGIN    = 1       # Score must be 1 pt below boundary to start recovery
R1_RECOVERY_CYCLES    = 3       # Consecutive recovery cycles required
R1_ESCALATION_BUFFER  = 3       # Score must exceed boundary+3 before cycles count
                                 #   elevated→high: score must be >33 (boundary=30+3)
                                 #   high→elevated: score must be <29 (boundary=30-1)

# R2: VIX spike detection
R2_VIX_SPIKE_PCT = 0.50         # 50% increase
R2_MIN_FLOOR_CYCLES = 4         # Minimum 4 cycles (1 hour)
R2_RETRACEMENT_PCT = 0.25       # Must retrace to within 25%
R2_STRESS_FLOOR = 75            # Pre-crisis floor

# R3: Prolonged ADX transitional zone
R3_TRANSITIONAL_CYCLES = 6     # 6+ consecutive cycles in 20-25
R3_RECOVERY_CYCLES = 2          # 2+ cycles decisively outside 20-25

# R5: Stress component null handling
R5_STALE_CYCLE_THRESHOLD_1 = 2  # Cycles 1-2: use last known
R5_STALE_CYCLE_THRESHOLD_2 = 4  # Cycles 3-4: use neutral midpoint
R5_STALE_CYCLE_THRESHOLD_3 = 5  # Cycles 5+: write proposal
R5_CONVERGENCE_BUFFER_PER_COMPONENT = 0.03
R5_SIMULTANEOUS_FAILURE_COUNT = 3

# Gap D: Data staleness thresholds (seconds)
STALENESS_THRESHOLDS = {
    "london":   300,   # 5 minutes
    "overlap":  300,   # 5 minutes
    "newyork":  300,   # 5 minutes
    "asian":    1800,  # 30 minutes
    "off_hours": 3600, # 60 minutes
}

# GDELT API
GDELT_API_BASE = "https://api.gdeltproject.org/api/v2"

# Kill switch
KILL_SWITCH_PARAM = "/platform/config/kill-switch"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class Regime(Enum):
    TRENDING = "trending"
    RANGING = "ranging"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"


class Session(Enum):
    LONDON = "london"
    OVERLAP = "overlap"
    NEWYORK = "newyork"
    ASIAN = "asian"
    OFF_HOURS = "off_hours"


class StressState(Enum):
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    PRE_CRISIS = "pre_crisis"
    CRISIS = "crisis"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class StressComponent:
    """Individual stress score component with null-handling state."""
    name: str
    weight: float
    raw_value: Optional[float] = None
    normalised_score: float = 0.0  # 0-100 contribution before weighting
    source: str = "live"           # live | stale | substituted
    stale_cycles: int = 0
    neutral_midpoint: float = 50.0  # Calibrated at init from 30-day data
    last_known_value: Optional[float] = None
    last_updated: Optional[datetime] = None


@dataclass
class PairRegime:
    """Regime classification for a single pair."""
    instrument: str
    regime: Regime
    adx: Optional[float] = None
    realised_vol_14: Optional[float] = None
    realised_vol_30: Optional[float] = None
    vol_ratio: Optional[float] = None  # realised_vol_14 / realised_vol_30
    regime_confidence: str = "high"    # high | low (Gap C)
    adx_transitional_cycles: int = 0   # R3 tracking
    timeframe_alignment: str = "unknown"


@dataclass
class SessionContext:
    """Current session classification."""
    session: Session
    day_of_week: int               # 1=Monday through 5=Friday
    stop_hunt_window: bool = False  # London AND UTC hour < 07:30
    ny_close_window: bool = False   # UTC hour >= 20:00
    utc_hour: int = 0


@dataclass
class VixSpikeState:
    """R2: VIX spike floor tracking."""
    active: bool = False
    baseline_vix: Optional[float] = None
    spike_vix: Optional[float] = None
    cycles_remaining: int = 0
    triggered_at: Optional[datetime] = None


@dataclass
class BoundaryOscillation:
    """R1: Stress score boundary oscillation tracking."""
    boundary_value: Optional[int] = None
    oscillation_cycles: int = 0
    higher_state_applied: bool = False
    recovery_cycles_below: int = 0
    consecutive_above: int = 0  # cycles score stayed above boundary while escalated


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------
def get_secret(secret_name: str, region: str = REGION) -> dict:
    """Retrieve a secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def get_parameter(param_name: str, region: str = REGION) -> str:
    """Retrieve a parameter from AWS Systems Manager Parameter Store."""
    client = boto3.client("ssm", region_name=region)
    resp = client.get_parameter(Name=param_name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def get_db_connection():
    """Create a psycopg2 connection to RDS."""
    creds = get_secret("platform/rds/credentials")
    endpoint = get_parameter("/platform/config/rds-endpoint")
    conn = psycopg2.connect(
        host=endpoint,
        port=5432,
        dbname=DB_NAME,
        user=creds["username"],
        password=creds["password"],
        options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Session classification
# ---------------------------------------------------------------------------
def classify_session(now_utc: Optional[datetime] = None) -> SessionContext:
    """
    Classify the current trading session based on UTC time.

    Sessions:
    - Asian:    00:00-07:00 UTC
    - London:   07:00-12:00 UTC (before NY opens)
    - Overlap:  12:00-16:00 UTC (London + NY)
    - New York: 16:00-20:00 UTC (after London closes)
    - Off hours: 20:00-00:00 UTC
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    hour = now_utc.hour
    minute = now_utc.minute
    dow = now_utc.isoweekday()  # 1=Mon, 7=Sun

    # Weekend = off hours
    if dow in (6, 7):
        return SessionContext(
            session=Session.OFF_HOURS,
            day_of_week=dow,
            stop_hunt_window=False,
            ny_close_window=False,
            utc_hour=hour,
        )

    if 0 <= hour < 7:
        session = Session.ASIAN
    elif 7 <= hour < 12:
        session = Session.LONDON
    elif 12 <= hour < 16:
        session = Session.OVERLAP
    elif 16 <= hour < 20:
        session = Session.NEWYORK
    else:
        session = Session.OFF_HOURS

    stop_hunt = (session == Session.LONDON and hour == 7 and minute < 30)
    ny_close = (hour >= 20)

    return SessionContext(
        session=session,
        day_of_week=dow,
        stop_hunt_window=stop_hunt,
        ny_close_window=ny_close,
        utc_hour=hour,
    )


# ---------------------------------------------------------------------------
# Data retrieval — RDS
# ---------------------------------------------------------------------------
def fetch_price_metrics(conn, pair: str, timeframe: str = "1H", limit: int = 50) -> List[dict]:
    """Fetch recent price metrics for a pair from RDS."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ts, atr_14, realised_vol_14, realised_vol_30
                FROM forex_network.price_metrics
                WHERE instrument = %s AND timeframe = %s
                ORDER BY ts DESC LIMIT %s
            """, (pair, timeframe, limit))
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Price metrics fetch failed for {pair}: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_historical_prices(conn, pair: str, timeframe: str = "1H", limit: int = 200) -> List[dict]:
    """Fetch recent OHLCV bars for indicator calculation."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ts, open, high, low, close, volume, session
                FROM forex_network.historical_prices
                WHERE instrument = %s AND timeframe = %s
                ORDER BY ts DESC LIMIT %s
            """, (pair, timeframe, limit))
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Historical prices fetch failed for {pair}: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_portfolio_correlations(conn) -> List[dict]:
    """Fetch the latest rolling correlations from shared.portfolio_correlation."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # First check actual column names on the table
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'shared' AND table_name = 'portfolio_correlation'
            """)
            cols = {row["column_name"] for row in cur.fetchall()}

            # Try common column name patterns
            if "pair_a" in cols:
                pair_col_a, pair_col_b, corr_col = "pair_a", "pair_b", "correlation_30d"
            elif "instrument_a" in cols:
                pair_col_a, pair_col_b, corr_col = "instrument_a", "instrument_b", "correlation_30d"
            else:
                logger.warning(f"portfolio_correlation columns: {cols} — unknown schema, skipping")
                return []

            if corr_col not in cols:
                corr_col = "correlation" if "correlation" in cols else list(cols - {pair_col_a, pair_col_b, "updated_at", "id"})[0] if cols else "correlation_30d"

            cur.execute(f"""
                SELECT {pair_col_a} AS pair_a, {pair_col_b} AS pair_b,
                       {corr_col} AS correlation_30d, updated_at
                FROM shared.portfolio_correlation
                ORDER BY updated_at DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Correlation fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_latest_stress_snapshot(conn) -> Optional[dict]:
    """Fetch the most recent market context snapshot.
    Gracefully handles missing columns that may not exist until migrations are applied."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # First check which columns exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'shared' AND table_name = 'market_context_snapshots'
        """)
        existing_cols = {row["column_name"] for row in cur.fetchall()}

        # Build SELECT with only existing columns
        wanted = [
            "snapshot_time", "system_stress_score", "stress_state",
            "stress_components", "vix_value", "degraded_agents",
            "kill_switch_active", "session", "regime_per_pair",
            "metadata",  # carries yield_differentials, cross_asset_prices
        ]
        select_cols = [c for c in wanted if c in existing_cols]
        if not select_cols:
            return None

        cur.execute(f"""
            SELECT {', '.join(select_cols)}
            FROM shared.market_context_snapshots
            ORDER BY snapshot_time DESC LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None


def fetch_ssi_data(conn) -> List[dict]:
    """Fetch recent MyFXBook SSI data for order flow anomaly detection.

    Window is 26 hours (not 20 minutes) to cover:
      - Weekday hourly ingest: any single missed run still falls within window
      - Weekends: no ingest scheduled (MON-FRI only), last known snapshot stays visible
    Data is deduped by instrument so widening the window always returns the latest row per pair.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (instrument)
                       instrument, long_pct, short_pct, contrarian_score,
                       avg_long_price, avg_short_price, ts
                FROM forex_network.sentiment_ssi
                WHERE ts >= NOW() - INTERVAL '26 hours'
                ORDER BY instrument, ts DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"SSI fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_ig_ssi_data(conn) -> List[dict]:
    """Fetch latest IG Client Sentiment for order flow anomaly (second broker confirmation).

    Uses same 26-hour window as fetch_ssi_data so weekend coverage is consistent.
    Column aliases map long_percentage / short_percentage to the same long_pct / short_pct
    keys used by the anomaly scoring block.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (instrument)
                       instrument,
                       long_percentage  AS long_pct,
                       short_percentage AS short_pct
                FROM forex_network.ig_client_sentiment
                WHERE ts >= NOW() - INTERVAL '26 hours'
                  AND instrument IN ('EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','NZDUSD')
                ORDER BY instrument, ts DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"IG SSI fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_order_flow_data(conn) -> List[dict]:
    """Fetch recent TraderMade order flow data."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT instrument, buy_volume, sell_volume, net_flow, flow_score, ts
                FROM forex_network.sentiment_order_flow
                WHERE ts >= NOW() - INTERVAL '20 minutes'
                ORDER BY instrument, ts DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Order flow fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


def fetch_fred_yield_data(conn) -> Optional[dict]:
    """
    Fetch US Treasury yield data from FRED economic releases for yield curve slope.
    Uses 10Y minus 2Y as the slope indicator.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT indicator, actual, release_time
                FROM forex_network.economic_releases
                WHERE indicator IN ('UST_10Y', 'UST_2Y', 'GS10', 'GS2',
                                    'DGS10', 'DGS2', 'T10Y2Y')
                ORDER BY release_time DESC LIMIT 10
            """)
            rows = cur.fetchall()
            if not rows:
                return None

            # Try direct spread first (T10Y2Y)
            for row in rows:
                if row["indicator"] == "T10Y2Y" and row["actual"] is not None:
                    return {
                        "spread": float(row["actual"]),
                        "timestamp": row["release_time"],
                        "source": "T10Y2Y_direct",
                    }

            # Fallback: compute from 10Y and 2Y
            ten_y = None
            two_y = None
            for row in rows:
                if row["indicator"] in ("UST_10Y", "GS10", "DGS10") and ten_y is None:
                    ten_y = float(row["actual"]) if row["actual"] else None
                if row["indicator"] in ("UST_2Y", "GS2", "DGS2") and two_y is None:
                    two_y = float(row["actual"]) if row["actual"] else None

            if ten_y is not None and two_y is not None:
                return {
                    "spread": ten_y - two_y,
                    "timestamp": rows[0]["release_time"],
                    "source": "computed_10Y_minus_2Y",
                }

            return None
    except Exception as e:
        logger.warning(f"FRED yield fetch failed: {e}")
        warn("regime_agent", "API_FAIL", "FRED fetch failed", source="FRED", error=str(e)[:120])
        try:
            conn.rollback()
        except:
            pass
        return None


def fetch_eodhd_yield_data(conn) -> Optional[dict]:
    """
    Fetch bond yield data from cross_asset_prices (populated by neo-ingest-bond-yields-dev).
    Returns a dict with:
        spread           - US 10Y-2Y (primary stress signal)
        us10y/us2y       - raw yield values
        us_uk_spread     - US10Y minus UK10Y (positive = USD yield premium vs GBP)
        us_de_spread     - US10Y minus DE10Y (positive = USD yield premium vs EUR)
        us_jp_spread     - US10Y minus JP10Y (positive = USD yield premium vs JPY)
    All cross-country spreads affect relative FX pair bias:
        positive us_uk_spread -> USD supported vs GBP -> GBPUSD bearish pressure
        positive us_de_spread -> USD supported vs EUR -> EURUSD bearish pressure
        positive us_jp_spread -> USD supported vs JPY -> USDJPY bullish pressure
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (instrument)
                    instrument, close, bar_time
                FROM forex_network.cross_asset_prices
                WHERE instrument IN ('US10Y', 'US2Y', 'UK10Y', 'DE10Y', 'JP10Y')
                  AND timeframe = '1D'
                  AND close IS NOT NULL
                ORDER BY instrument, bar_time DESC
            """)
            rows = cur.fetchall()

        latest = {row['instrument']: row for row in rows}

        if 'US10Y' not in latest or 'US2Y' not in latest:
            return None

        us10y = float(latest['US10Y']['close'])
        us2y  = float(latest['US2Y']['close'])
        spread = us10y - us2y
        ts = min(latest['US10Y']['bar_time'], latest['US2Y']['bar_time'])

        result = {
            'spread':    spread,
            'timestamp': ts,
            'source':    'eodhd_cross_asset',
            'us10y':     us10y,
            'us2y':      us2y,
        }

        # Cross-country differentials for per-pair FX bias signals
        if 'UK10Y' in latest:
            result['us_uk_spread'] = round(us10y - float(latest['UK10Y']['close']), 4)
        if 'DE10Y' in latest:
            result['us_de_spread'] = round(us10y - float(latest['DE10Y']['close']), 4)
        if 'JP10Y' in latest:
            result['us_jp_spread'] = round(us10y - float(latest['JP10Y']['close']), 4)

        return result
    except Exception as e:
        logger.warning(f"EODHD yield fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return None


def fetch_agent_heartbeats(conn) -> List[dict]:
    """Fetch current agent heartbeat statuses for degradation detection."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT agent_name, last_seen, status, absent_cycles,
                       degradation_mode, convergence_boost
                FROM forex_network.agent_heartbeats
            """)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Heartbeat fetch failed: {e}")
        try:
            conn.rollback()
        except:
            pass
        return []


# ---------------------------------------------------------------------------
# Data retrieval — GDELT (REST)
# ---------------------------------------------------------------------------

# Module-level cache: (value, fetched_at). Shared across all users in a cycle
# so only one HTTP request is made per cycle regardless of user count.
# fetched_at is pushed forward during failure-backoff windows.
_gdelt_cache: tuple[Optional[float], float] = (None, 0.0)
_GDELT_CACHE_TTL = 14400   # 4 hours — tone from 24h window barely changes in <4h
_GDELT_RETRY_AFTER = 14400  # 4 hours between retries after a 429 / failure


def _gdelt_db_load(conn) -> Optional[tuple]:
    """Return (value, epoch) from shared.api_cache if the row exists, else None."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT num_value, EXTRACT(EPOCH FROM fetched_at) "
            "FROM shared.api_cache WHERE cache_key = 'gdelt_geopolitical_tone'"
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return float(row[0]), float(row[1])
    except Exception as e:
        logger.warning(f"GDELT DB load failed: {e}")
    return None


def _gdelt_db_last_attempt(conn) -> Optional[float]:
    """Return epoch of last GDELT HTTP attempt, or None if no record exists."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM fetched_at) "
            "FROM shared.api_cache WHERE cache_key = 'gdelt_last_attempt'"
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return float(row[0])
    except Exception as e:
        logger.warning(f"GDELT DB attempt load failed: {e}")
    return None


def _gdelt_db_record_attempt(conn) -> None:
    """Stamp the current time as the last GDELT HTTP attempt (success or failure).
    This allows cross-restart backoff — the in-memory backoff window is lost on
    restart, so without this every restart immediately retries the rate-limited API."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO shared.api_cache (cache_key, num_value, fetched_at)
            VALUES ('gdelt_last_attempt', 0.0, NOW())
            ON CONFLICT (cache_key) DO UPDATE
                SET num_value = 0.0,
                    fetched_at = NOW()
            """
        )
        cur.close()
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"GDELT DB attempt record failed: {e}")


def _gdelt_db_save(conn, value: float) -> None:
    """Upsert the latest GDELT tone value into shared.api_cache."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO shared.api_cache (cache_key, num_value, fetched_at)
            VALUES ('gdelt_geopolitical_tone', %s, NOW())
            ON CONFLICT (cache_key) DO UPDATE
                SET num_value = EXCLUDED.num_value,
                    fetched_at = EXCLUDED.fetched_at
            """,
            (value,),
        )
        cur.close()
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"GDELT DB save failed: {e}")


def fetch_gdelt_geopolitical_tone(conn=None) -> Optional[float]:
    """
    GDELT disabled — returns None. Geopolitical stress uses fixed neutral fallback (50).

    GDELT was generating noise (persistent 429 rate-limits, unreliable tone signal)
    and has been removed from the active data pipeline. The DB helper functions
    (_gdelt_db_load etc.) and this stub are retained for future replacement
    with NewsAPI.ai or another geopolitical data provider.
    """
    return None

    # ---- original implementation preserved below for reference ----
    # (unreachable — remove when NewsAPI.ai replacement is wired in)
    global _gdelt_cache
    cached_value, fetched_at = _gdelt_cache

    # STEP 1: DB backoff check — runs FIRST, before in-memory cache.
    # Prevents an HTTP call on every cycle restart when _gdelt_cache resets to
    # (None, 0.0) and the in-memory window is lost. The gdelt_last_attempt row
    # in shared.api_cache survives restarts and is the authoritative gate.
    if conn is not None:
        last_attempt = _gdelt_db_last_attempt(conn)
        if last_attempt is not None:
            attempt_age = time.time() - last_attempt
            if attempt_age < _GDELT_RETRY_AFTER:
                # Best available tone: in-memory first, then DB, then None
                if cached_value is None:
                    db_row = _gdelt_db_load(conn)
                    if db_row is not None:
                        cached_value = db_row[0]
                # Warm in-memory for the remaining backoff window so repeated
                # cycles don't each hit the DB for the backoff check
                _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + (_GDELT_RETRY_AFTER - attempt_age))
                logger.info(
                    f"GDELT: DB backoff active ({attempt_age:.0f}s / {_GDELT_RETRY_AFTER}s) — "
                    f"skipping HTTP, value={'N/A' if cached_value is None else f'{cached_value:.2f}'}"
                )
                return cached_value

    # STEP 2: In-memory cache hit
    cache_age = time.time() - fetched_at
    if cache_age < _GDELT_CACHE_TTL:
        if cached_value is not None:
            logger.info(f"GDELT cache hit (age {cache_age:.0f}s): {cached_value:.2f}")
            if conn is not None:
                log_api_call(conn, 'gdelt', '/api/v2/doc/doc (cache_hit)', 'regime',
                             True, 0, data_age_seconds=int(cache_age))
        return cached_value

    # STEP 3: DB tone value — try before making an HTTP call
    if conn is not None:
        db_row = _gdelt_db_load(conn)
        if db_row is not None:
            db_value, db_epoch = db_row
            db_age = time.time() - db_epoch
            if db_age < _GDELT_CACHE_TTL:
                # Fresh enough — warm in-memory cache and skip the HTTP call
                _gdelt_cache = (db_value, db_epoch)
                logger.info(f"GDELT DB cache hit (age {db_age:.0f}s): {db_value:.2f}")
                log_api_call(conn, 'gdelt', '/api/v2/doc/doc (db_cache_hit)', 'regime',
                             True, 0, data_age_seconds=int(db_age))
                return db_value
            # Stale DB value — keep as fallback in case HTTP also fails
            cached_value = db_value

    try:
        url = (
            f"{GDELT_API_BASE}/doc/doc"
            f"?query=geopolitical conflict sanctions"
            f"&mode=tonechart"
            f"&format=json"
            f"&timespan=24h"
        )
        # Record that we are about to make an HTTP call — this timestamp is used
        # for cross-restart backoff even if the call fails.
        if conn is not None:
            _gdelt_db_record_attempt(conn)

        _t0 = time.time()
        resp = requests.get(url, timeout=10,
                            headers={"User-Agent": "ProjectNeo-RegimeAgent/1.0"})
        _ms = int((time.time() - _t0) * 1000)
        if resp.status_code != 200:
            if conn is not None:
                log_api_call(conn, 'gdelt', '/api/v2/doc/doc', 'regime',
                             False, _ms, error_type=f'http_{resp.status_code}')
            logger.warning(f"GDELT API returned {resp.status_code}")
            _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + _GDELT_RETRY_AFTER)
            if conn is not None and resp.status_code in (429, 503, 504, 500):
                try:
                    import json as _j
                    with conn.cursor() as _cur:
                        _cur.execute("""
                            INSERT INTO forex_network.audit_log
                              (user_id, event_type, description, metadata, source, created_at)
                            VALUES (NULL, 'api_provider_degraded',
                                    %s, %s::jsonb, 'regime_agent', NOW())
                        """, (
                            f"GDELT API returned HTTP {resp.status_code} — geopolitical_index unavailable",
                            _j.dumps({"provider": "gdelt", "status_code": resp.status_code,
                                      "retry_after_s": _GDELT_RETRY_AFTER}),
                        ))
                    conn.commit()
                except Exception:
                    try: conn.rollback()
                    except: pass
            return cached_value

        try:
            data = resp.json()
        except (ValueError, Exception) as _je:
            # GDELT sometimes returns 200 with a plain-text rate-limit message
            # ("Please limit requests to one every 5 seconds…") instead of JSON.
            if conn is not None:
                log_api_call(conn, 'gdelt', '/api/v2/doc/doc', 'regime',
                             False, _ms, error_type='rate_limit_200_text')
            logger.warning(f"GDELT 200 but non-JSON body (rate-limit text?): {resp.text[:120]}")
            _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + _GDELT_RETRY_AFTER)
            return cached_value
        if not data:
            _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + _GDELT_RETRY_AFTER)
            return cached_value

        # tonechart mode returns {"tonechart": [{"date": "...", "tonavg": X, "articles": N}, ...]}
        chart = data.get("tonechart") if isinstance(data, dict) else None
        if chart and isinstance(chart, list):
            tones = [item["tonavg"] for item in chart if "tonavg" in item]
            if tones:
                result = sum(tones) / len(tones)
                _gdelt_cache = (result, time.time())
                if conn is not None:
                    _gdelt_db_save(conn, result)
                    log_api_call(conn, 'gdelt', '/api/v2/doc/doc', 'regime',
                                 True, _ms)
                logger.info(f"GDELT fresh fetch: tone={result:.2f}")
                return result

        logger.warning(f"GDELT: unexpected response shape: {str(data)[:200]}")
        _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + _GDELT_RETRY_AFTER)
        return cached_value

    except Exception as e:
        if conn is not None:
            _t0_ms = int((time.time() - _t0) * 1000) if '_t0' in dir() else 0
            log_api_call(conn, 'gdelt', '/api/v2/doc/doc', 'regime',
                         False, _t0_ms, error_type=type(e).__name__)
        logger.warning(f"GDELT fetch failed: {e}")
        _gdelt_cache = (cached_value, time.time() - _GDELT_CACHE_TTL + _GDELT_RETRY_AFTER)
        if conn is not None:
            try:
                import json as _j
                with conn.cursor() as _cur:
                    _cur.execute("""
                        INSERT INTO forex_network.audit_log
                          (user_id, event_type, description, metadata, source, created_at)
                        VALUES (NULL, 'api_provider_degraded',
                                %s, %s::jsonb, 'regime_agent', NOW())
                    """, (
                        f"GDELT fetch exception: {type(e).__name__} — geopolitical_index unavailable",
                        _j.dumps({"provider": "gdelt", "error": str(e)[:200],
                                  "retry_after_s": _GDELT_RETRY_AFTER}),
                    ))
                conn.commit()
            except Exception:
                try: conn.rollback()
                except: pass
        if cached_value is not None:
            logger.info(f"GDELT using stale value (age {cache_age:.0f}s): {cached_value:.2f}")
        return cached_value


# ---------------------------------------------------------------------------
# ADX calculation (from OHLCV bars)
# ---------------------------------------------------------------------------
def compute_adx(bars: List[dict], period: int = 14) -> Optional[float]:
    """
    Compute ADX(14) from OHLCV bars.
    Bars must be in chronological order (oldest first).
    Returns the most recent ADX value.
    """
    if len(bars) < period * 2 + 1:
        return None

    # Reverse to chronological if needed (our queries return DESC)
    if bars[0]["ts"] > bars[-1]["ts"]:
        bars = list(reversed(bars))

    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = [float(b["close"]) for b in bars]

    n = len(bars)
    tr_list = []
    plus_dm_list = []
    minus_dm_list = []

    for i in range(1, n):
        high_diff = highs[i] - highs[i - 1]
        low_diff = lows[i - 1] - lows[i]

        plus_dm = max(high_diff, 0) if high_diff > low_diff else 0
        minus_dm = max(low_diff, 0) if low_diff > high_diff else 0

        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )

        tr_list.append(tr)
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)

    # Wilder smoothing
    def wilder_smooth(values, period):
        if len(values) < period:
            return []
        smoothed = [sum(values[:period])]
        for v in values[period:]:
            smoothed.append(smoothed[-1] - (smoothed[-1] / period) + v)
        return smoothed

    smoothed_tr = wilder_smooth(tr_list, period)
    smoothed_plus_dm = wilder_smooth(plus_dm_list, period)
    smoothed_minus_dm = wilder_smooth(minus_dm_list, period)

    if not smoothed_tr or not smoothed_plus_dm or not smoothed_minus_dm:
        return None

    min_len = min(len(smoothed_tr), len(smoothed_plus_dm), len(smoothed_minus_dm))
    dx_list = []

    for i in range(min_len):
        if smoothed_tr[i] == 0:
            continue
        plus_di = 100 * smoothed_plus_dm[i] / smoothed_tr[i]
        minus_di = 100 * smoothed_minus_dm[i] / smoothed_tr[i]
        di_sum = plus_di + minus_di
        if di_sum == 0:
            continue
        dx = 100 * abs(plus_di - minus_di) / di_sum
        dx_list.append(dx)

    if len(dx_list) < period:
        return None

    # ADX smoothing: initial value is average of first `period` DX values,
    # subsequent values add DX/period (not full DX).
    # wilder_smooth() uses sum as initial and adds full value — correct for
    # TR/DM but wrong here, producing values ~14x too high.
    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = adx - adx / period + dx / period

    return adx


# ---------------------------------------------------------------------------
# Stress score computation
# ---------------------------------------------------------------------------
class StressScoreEngine:
    """
    Computes the system stress score (0-100) from 7 weighted components.
    Implements R1 (boundary oscillation), R2 (VIX spike), R5 (null handling).
    """

    def __init__(self, conn, previous_snapshot: Optional[dict] = None):
        self.conn = conn
        self.previous_snapshot = previous_snapshot
        self.components: Dict[str, StressComponent] = {}
        self.proposals: List[dict] = []
        self.substitution_log: List[str] = []
        self._stress_score_confidence = "high"

        # Initialise components
        for name, weight in STRESS_WEIGHTS.items():
            self.components[name] = StressComponent(name=name, weight=weight)

        # Restore stale cycle counts from previous snapshot
        if previous_snapshot and previous_snapshot.get("stress_components"):
            prev_components = previous_snapshot["stress_components"]
            if isinstance(prev_components, str):
                prev_components = json.loads(prev_components)
            for name, comp in self.components.items():
                prev = prev_components.get(name, {})
                comp.stale_cycles = prev.get("stale_cycles", 0)
                comp.last_known_value = prev.get("last_known_value")
                if prev.get("last_updated"):
                    try:
                        comp.last_updated = datetime.fromisoformat(
                            str(prev["last_updated"])
                        )
                    except (ValueError, TypeError):
                        pass

        # VIX spike state (R2)
        self.vix_spike = VixSpikeState()
        self._restore_vix_spike_state()

        # Boundary oscillation state (R1)
        self.boundary_state = BoundaryOscillation()
        self._restore_boundary_state()

        # Cross-country yield differentials (populated by gather_component_data)
        self._yield_data: Optional[dict] = None
        # Cross-asset CFD prices for current cycle (persisted to metadata for next cycle)
        self._cross_asset_prices_current: Optional[dict] = None

    def _restore_vix_spike_state(self):
        """Restore VIX spike floor state from previous snapshot."""
        if not self.previous_snapshot:
            return
        prev_components = self.previous_snapshot.get("stress_components", {})
        if isinstance(prev_components, str):
            prev_components = json.loads(prev_components)
        vix_meta = prev_components.get("_vix_spike_state", {})
        if vix_meta.get("active"):
            self.vix_spike.active = True
            self.vix_spike.baseline_vix = vix_meta.get("baseline_vix")
            self.vix_spike.spike_vix = vix_meta.get("spike_vix")
            self.vix_spike.cycles_remaining = vix_meta.get("cycles_remaining", 0)

    def _restore_boundary_state(self):
        """Restore boundary oscillation state from previous snapshot."""
        if not self.previous_snapshot:
            return
        prev_components = self.previous_snapshot.get("stress_components", {})
        if isinstance(prev_components, str):
            prev_components = json.loads(prev_components)
        boundary_meta = prev_components.get("_boundary_state", {})
        if boundary_meta:
            self.boundary_state.boundary_value = boundary_meta.get("boundary_value")
            self.boundary_state.oscillation_cycles = boundary_meta.get("oscillation_cycles", 0)
            self.boundary_state.higher_state_applied = boundary_meta.get("higher_state_applied", False)
            self.boundary_state.recovery_cycles_below = boundary_meta.get("recovery_cycles_below", 0)
            self.boundary_state.consecutive_above = boundary_meta.get("consecutive_above", 0)

    def calibrate_neutral_midpoints(self):
        """
        R5: Calibrate neutral midpoint values from trailing 30-day data.
        Called once at agent initialisation.
        """
        logger.info("Calibrating neutral midpoints from 30-day historical data")

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT system_stress_score, stress_components
                    FROM shared.market_context_snapshots
                    WHERE snapshot_time >= NOW() - INTERVAL '30 days'
                    ORDER BY snapshot_time DESC
                """)
                snapshots = cur.fetchall()
                if snapshots:
                    vix_values = []
                    for s in snapshots:
                        comp = s.get("stress_components", {})
                        if isinstance(comp, str):
                            comp = json.loads(comp)
                        vix_data = comp.get("vix_level_trend", {})
                        if vix_data.get("raw_value") is not None:
                            vix_values.append(vix_data["raw_value"])
                    if vix_values:
                        vix_values.sort()
                        mid = len(vix_values) // 2
                        self.components["vix_level_trend"].neutral_midpoint = vix_values[mid]
                        logger.info(f"VIX neutral midpoint calibrated: {vix_values[mid]:.2f}")
        except Exception as e:
            logger.warning(f"Failed to calibrate VIX midpoint: {e}")

        # Other components use sensible defaults (already set to 50.0)
        # These will be refined as historical snapshots accumulate

    # --- Normalisation functions (raw value → 0-100 stress) ---

    def _normalise_vix(self, vix_value: float) -> float:
        """VIX 10-15=low, 15-25=moderate, 25-40=high, 40+=extreme."""
        if vix_value <= 12:
            return 0
        elif vix_value <= 15:
            return (vix_value - 12) / 3 * 20
        elif vix_value <= 25:
            return 20 + (vix_value - 15) / 10 * 30
        elif vix_value <= 40:
            return 50 + (vix_value - 25) / 15 * 30
        else:
            return min(80 + (vix_value - 40) / 20 * 20, 100)

    def _normalise_yield_curve(self, spread: float) -> float:
        """
        Map US 10Y-2Y yield spread to a stress score.
        Calibration (user spec):
            spread >= 2.0  -> stress  10  (steep curve, minimal stress)
            spread == 0.0  -> stress  50  (flat curve, moderate stress)
            spread <= -0.5 -> stress  90  (inverted, high stress)
        Linear interpolation between breakpoints; clamped at extremes.
        """
        if spread >= 2.0:
            return 10.0
        elif spread >= 0.0:
            # 0..2  maps to  50..10  (slope = -20 per unit)
            return 50.0 - (spread / 2.0) * 40.0
        elif spread >= -0.5:
            # -0.5..0  maps to  90..50  (slope = -80 per unit)
            return 50.0 + (abs(spread) / 0.5) * 40.0
        else:
            return 90.0

    def _normalise_vol_divergence(self, divergence: float) -> float:
        """High positive divergence = short-term vol spiking = stress."""
        abs_div = abs(divergence)
        if abs_div < 0.1:
            return 10
        elif abs_div < 0.3:
            return 10 + (abs_div - 0.1) / 0.2 * 30
        elif abs_div < 0.6:
            return 40 + (abs_div - 0.3) / 0.3 * 30
        else:
            return min(70 + (abs_div - 0.6) / 0.4 * 30, 100)

    def _normalise_correlation_breakdown(self, breakdown_score: float) -> float:
        """Higher deviation from baseline correlations = higher stress."""
        if breakdown_score < 0.05:
            return 5
        elif breakdown_score < 0.15:
            return 5 + (breakdown_score - 0.05) / 0.10 * 30
        elif breakdown_score < 0.30:
            return 35 + (breakdown_score - 0.15) / 0.15 * 35
        else:
            return min(70 + (breakdown_score - 0.30) / 0.20 * 30, 100)

    def _normalise_cot(self, cot_score: float) -> float:
        """COT extremity 0-100 maps directly to stress."""
        return min(max(cot_score, 0), 100)

    def _normalise_order_flow_anomaly(self, avg_contrarian: float) -> float:
        """Map average contrarian score (0–1) to stress contribution (0–100).

        Calibration:
          < 0.6 (normal retail noise):  subdued — max ~18 at threshold
          >= 0.6 (crowded positioning): full weight — 60–100
        The step-up at 0.6 is intentional: only genuine extremes raise stress.
        """
        if avg_contrarian < 0.6:
            return round(avg_contrarian * 0.3 * 100, 2)
        else:
            return round(min(avg_contrarian * 100.0, 100.0), 2)

    def _normalise_geopolitical(self, tone: float) -> float:
        """GDELT tone: more negative = more conflict = higher stress."""
        if tone >= 2:
            return 10
        elif tone >= 0:
            return 10 + (2 - tone) / 2 * 20
        elif tone >= -3:
            return 30 + abs(tone) / 3 * 25
        elif tone >= -6:
            return 55 + (abs(tone) - 3) / 3 * 25
        else:
            return min(80 + (abs(tone) - 6) / 4 * 20, 100)

    # --- R5 null handling ---

    def _apply_r5_null_handling(self, component: StressComponent) -> float:
        """
        R5: Handle null/stale stress components.
        Returns the effective normalised score (0-100) to use.
        """
        if component.raw_value is not None:
            component.stale_cycles = 0
            component.last_known_value = component.raw_value
            component.last_updated = datetime.now(timezone.utc)
            component.source = "live"
            return component.normalised_score

        # KNOWN GAP: stale_cycles is in-memory only — resets to 0 on every service restart.
        # During extended API outages with multiple restarts, the agent may use last_known_value
        # indefinitely rather than escalating to neutral_midpoint or raising a proposal.
        # Fix: persist stale_cycles and last_updated to shared.api_cache table across restarts.
        # Priority: medium — only matters if VIX spikes during a combined outage + restart event.
        # Data is null — increment stale counter
        component.stale_cycles += 1
        warn("regime_agent", "STALE_DATA", f"{component.name} stale — using substituted value",
             stale_cycles=component.stale_cycles, component=component.name)

        if component.stale_cycles <= R5_STALE_CYCLE_THRESHOLD_1:
            # Cycles 1-2: use last known value
            if component.last_known_value is not None:
                component.source = "stale"
                self.substitution_log.append(
                    f"stress_component_stale: {component.name} "
                    f"(cycle {component.stale_cycles}, using last known)"
                )
                return component.normalised_score
            else:
                component.source = "substituted"
                return component.neutral_midpoint

        elif component.stale_cycles <= R5_STALE_CYCLE_THRESHOLD_2:
            # Cycles 3-4: neutral midpoint + convergence buffer
            component.source = "substituted"
            self.substitution_log.append(
                f"stress_component_substituted: {component.name} "
                f"(cycle {component.stale_cycles}, neutral midpoint)"
            )
            return component.neutral_midpoint

        else:
            # Cycles 5+: neutral midpoint + proposal
            component.source = "substituted"
            self.substitution_log.append(
                f"stress_component_substituted: {component.name} "
                f"(cycle {component.stale_cycles}, extended outage)"
            )
            self.proposals.append({
                "type": "risk_budget",
                "priority": "medium",
                "title": (
                    f"Stress component '{component.name}' unavailable "
                    f"for {component.stale_cycles} cycles"
                ),
                "reasoning": (
                    f"The {component.name} data source has returned null for "
                    f"{component.stale_cycles} consecutive cycles. Neutral midpoint "
                    f"substituted. Stress score accuracy degraded."
                ),
                "data_supporting": (
                    f"stale_cycles={component.stale_cycles}, "
                    f"neutral_midpoint={component.neutral_midpoint}"
                ),
                "suggested_action": (
                    f"Check data source for {component.name}. "
                    f"Verify provider connectivity and API health."
                ),
                "expected_impact": (
                    f"Stress score accuracy degraded by ~{component.weight * 100:.0f}%"
                ),
            })
            return component.neutral_midpoint

    # --- Data gathering ---

    def set_vix_data(self, vix_value: Optional[float], vix_trend: Optional[float] = None):
        """Set VIX data from EODHD MCP call."""
        comp = self.components["vix_level_trend"]
        if vix_value is not None:
            comp.raw_value = vix_value
            comp.normalised_score = self._normalise_vix(vix_value)
            if vix_trend is not None and vix_trend > 0:
                trend_bonus = min(vix_trend * 5, 15)
                comp.normalised_score = min(comp.normalised_score + trend_bonus, 100)
            logger.info(f"VIX: {vix_value:.2f} (trend: {vix_trend}) → "
                       f"stress: {comp.normalised_score:.1f}")
        else:
            comp.raw_value = None

    def set_cot_data(self, cot_extremity: Optional[float]):
        """Set COT extremes data from EODHD MCP call."""
        comp = self.components["cot_extremes"]
        if cot_extremity is not None:
            comp.raw_value = cot_extremity
            comp.normalised_score = self._normalise_cot(cot_extremity)
            logger.info(f"COT extremity: {cot_extremity:.1f} → "
                       f"stress: {comp.normalised_score:.1f}")
        else:
            comp.raw_value = None

    def _fetch_ig_equity_sentiment(self) -> dict:
        """Read IG retail %long/%short for equity indices from ig_client_sentiment.

        Returns dict keyed by instrument (SPX500/GER30/UK100) → {long_pct, short_pct}.
        Uses 26-hour window to cover weekends (same as order flow anomaly).
        Returns {} on failure so callers degrade gracefully.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT ON (instrument)
                           instrument,
                           long_percentage  AS long_pct,
                           short_percentage AS short_pct
                    FROM forex_network.ig_client_sentiment
                    WHERE instrument IN ('SPX500', 'GER30', 'UK100')
                      AND ts >= NOW() - INTERVAL '26 hours'
                    ORDER BY instrument, ts DESC
                """)
                rows = cur.fetchall()
            return {row['instrument']: {
                'long_pct':  float(row['long_pct']),
                'short_pct': float(row['short_pct']),
            } for row in rows}
        except Exception as e:
            logger.warning(f'IG equity sentiment fetch failed: {e}')
            return {}

    def fetch_cross_asset_prices(self) -> dict:
        """Fetch live CFD prices from TraderMade for risk-off detection, plus DXY from DB.

        Instruments (TraderMade live): SPX500, GER30, UK100, XAUUSD, UKOIL
        DXY: latest 1D close from forex_network.cross_asset_prices (IG source).
        Returns dict keyed by instrument → price.
        Returns {} on failure so caller degrades gracefully.
        """
        CFD_INSTRUMENTS = ['SPX500', 'GER30', 'UK100', 'XAUUSD', 'UKOIL']
        _t0 = time.time()
        try:
            tradermade_key = get_secret('platform/tradermade/api-key')['api_key']
            url = (
                'https://marketdata.tradermade.com/api/v1/live'
                '?currency=' + ','.join(CFD_INSTRUMENTS) +
                '&api_key=' + tradermade_key
            )
            resp = requests.get(url, timeout=10)
            _ms = int((time.time() - _t0) * 1000)
            resp.raise_for_status()
            data = resp.json()
            quotes = data.get('quotes', [])
            prices = {}
            for q in quotes:
                # CFD instruments use 'instrument' key; FX pairs use base+quote
                inst = q.get('instrument') or (
                    q.get('base_currency', '') + q.get('quote_currency', '')
                )
                mid = q.get('mid')
                if inst in CFD_INSTRUMENTS and mid is not None:
                    prices[inst] = float(mid)
            log_api_call(
                self.conn, 'tradermade', '/live?cfd', 'regime',
                bool(prices), _ms,
                pairs_returned=len(prices),
            )
            if prices:
                logger.info(f'Cross-asset CFD prices: {prices}')
            else:
                logger.warning('TraderMade CFD: empty quotes response')
        except Exception as e:
            _ms = int((time.time() - _t0) * 1000)
            log_api_call(
                self.conn, 'tradermade', '/live?cfd', 'regime',
                False, _ms, error_type=type(e).__name__,
            )
            logger.warning(f'Cross-asset CFD fetch failed: {e}')
            prices = {}

        # Append latest DXY daily close from DB (IG source, already scaled to index points)
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT close FROM forex_network.cross_asset_prices
                    WHERE instrument = 'DXY' AND timeframe = '1D'
                    ORDER BY bar_time DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    prices['DXY'] = float(row[0])
                    logger.info(f'DXY daily close from DB: {prices["DXY"]:.3f}')
        except Exception as e:
            logger.warning(f'DXY DB fetch failed: {e}')

        return prices

    def _compute_cross_asset_stress(
        self, current: dict, previous: dict
    ) -> float:
        """Score risk-off pressure from equities, gold, oil, and DXY.

        Weights (sum = 1.00):
          SPX500  0.30 (falling = risk-off)
          GER30   0.20 (falling = risk-off)
          UK100   0.10 (falling = risk-off)
          XAUUSD  0.20 (rising  = risk-off / safe-haven demand)
          UKOIL   0.05 (ambiguous — contributes neutral 50)
          DXY     0.15 (rising  = USD strength = risk-off)

        DXY comes from the DB daily bar so it contributes neutrally intra-day and
        fires a signal on the cycle after each new daily bar is written.
        Returns 10-95 (50 = neutral). First cycle (no previous) → 50.0.
        """
        WEIGHTS = {
            'SPX500': (0.30, 'equity'),
            'GER30':  (0.20, 'equity'),
            'UK100':  (0.10, 'equity'),
            'XAUUSD': (0.20, 'safe_haven'),
            'UKOIL':  (0.05, 'neutral'),
            'DXY':    (0.15, 'usd_strength'),
        }
        if not previous:
            return 50.0

        weighted_sum = 0.0
        total_weight = 0.0
        for inst, (w, kind) in WEIGHTS.items():
            cur_price = current.get(inst)
            prev_price = previous.get(inst)
            if kind == 'neutral' or cur_price is None or prev_price is None or prev_price == 0:
                weighted_sum += w * 50.0
            else:
                pct_change = (cur_price - prev_price) / prev_price  # e.g. -0.01 = -1%
                if kind == 'equity':
                    # Falling equities = risk-off → higher score
                    # -3% → ~95, 0% → 50, +3% → ~10
                    raw = 50.0 - pct_change * 3000.0
                elif kind == 'safe_haven':
                    # Rising gold = risk-off → higher score
                    # +3% → ~95, 0% → 50, -3% → ~10
                    raw = 50.0 + pct_change * 3000.0
                elif kind == 'usd_strength':
                    # Rising DXY = USD strength = risk-off → higher score
                    # Daily DXY moves are small (~0.5-1%); same scale keeps signal proportionate
                    raw = 50.0 + pct_change * 3000.0
                else:
                    raw = 50.0
                weighted_sum += w * max(10.0, min(95.0, raw))
            total_weight += w

        if total_weight == 0:
            return 50.0
        return round(max(10.0, min(95.0, weighted_sum / total_weight)), 2)

    def set_cross_asset_data(self, current: dict, previous: dict, ig_equity: dict = None):
        """Set cross_asset_risk component from CFD prices with IG equity sentiment overlay.

        ig_equity: optional dict {instrument: {long_pct, short_pct}} for SPX500/GER30/UK100.
        When provided, applies a contrarian adjustment (±3 pts per instrument, normalised):
          price falling + retail >60% long  → nudge stress UP   (crowd buying dip = more downside)
          price falling + retail <40% long  → nudge stress DOWN  (crowd short = contrarian bounce)
        """
        comp = self.components['cross_asset_risk']
        if not current:
            comp.raw_value = None
            return
        score = self._compute_cross_asset_stress(current, previous)

        # IG equity sentiment overlay
        if ig_equity and previous:
            adj     = 0.0
            checked = 0
            for inst in ('SPX500', 'GER30', 'UK100'):
                s      = ig_equity.get(inst)
                cur_p  = current.get(inst)
                prev_p = previous.get(inst)
                if not s or cur_p is None or prev_p is None or prev_p == 0:
                    continue
                price_falling = cur_p < prev_p
                long_pct = s['long_pct']
                checked += 1
                if price_falling:
                    if long_pct > 60:
                        adj += 3.0   # crowd buying the dip: contrarian → more downside
                    elif long_pct < 40:
                        adj -= 3.0   # crowd piling short: contrarian → bounce likely
            if checked > 0:
                adj   = adj / checked
                score = max(10.0, min(95.0, score + adj))
                logger.info(
                    f'IG equity overlay: adj={adj:+.1f} → score={score:.1f}'
                    f' ({len(ig_equity)} instruments)'
                )

        comp.raw_value = score
        comp.normalised_score = score  # already on 0-100 scale
        logger.info(
            f'Cross-asset risk: score={score:.1f}'
            f' (prev_avail={bool(previous)}, instruments={list(current.keys())})'
        )

    def _compute_correlation_from_prices(self):
        """Compute rolling 30-day pair correlation breakdown from historical_prices.

        Runs PostgreSQL corr() on 1D bars for 3 key pairs:
          AUDUSD/NZDUSD  expected ~+0.90
          EURUSD/GBPUSD  expected ~+0.85
          EURUSD/USDCHF  expected ~-0.95

        Returns average absolute deviation from expected norms.
        Deviation 0.0 = all pairs at historical norms (low stress).
        Deviation 0.4+ = significant breakdown (high stress).
        Returns None if insufficient data.
        """
        NORMS = {
            ('AUDUSD', 'NZDUSD'):  0.90,
            ('EURUSD', 'GBPUSD'):  0.85,
            ('EURUSD', 'USDCHF'): -0.95,
        }
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    WITH daily_returns AS (
                        SELECT instrument,
                               ts::date AS dt,
                               (close - LAG(close) OVER (
                                   PARTITION BY instrument ORDER BY ts
                               )) / NULLIF(
                                   LAG(close) OVER (
                                       PARTITION BY instrument ORDER BY ts
                                   ), 0
                               ) AS ret
                        FROM forex_network.historical_prices
                        WHERE instrument IN (
                            'AUDUSD', 'NZDUSD', 'EURUSD', 'GBPUSD', 'USDCHF'
                        )
                          AND timeframe = '1D'
                          AND ts >= NOW() - INTERVAL '32 days'
                    )
                    SELECT
                        a.instrument AS pair_a,
                        b.instrument AS pair_b,
                        corr(a.ret, b.ret) AS correlation_30d
                    FROM daily_returns a
                    JOIN daily_returns b
                         ON a.dt = b.dt AND a.instrument < b.instrument
                    WHERE (a.instrument, b.instrument) IN (
                        ('AUDUSD', 'NZDUSD'),
                        ('EURUSD', 'GBPUSD'),
                        ('EURUSD', 'USDCHF')
                    )
                    GROUP BY a.instrument, b.instrument
                """)
                rows = cur.fetchall()

            if not rows:
                logger.warning('Correlation query returned no rows — insufficient 1D price data')
                return None

            deviations = []
            for row in rows:
                key = (row['pair_a'], row['pair_b'])
                corr_val = row['correlation_30d']
                if corr_val is None:
                    continue
                norm = NORMS.get(key)
                if norm is not None:
                    deviations.append(abs(float(corr_val) - norm))
                    logger.debug(
                        f'Corr {key[0]}/{key[1]}: {corr_val:.3f} '
                        f'(norm {norm:+.2f}, dev {abs(float(corr_val)-norm):.3f})'
                    )

            if not deviations:
                return None

            avg_dev = sum(deviations) / len(deviations)
            logger.info(
                f'Correlation breakdown: avg_deviation={avg_dev:.3f} '
                f'from {len(deviations)} pairs '
                f'-> stress: {self._normalise_correlation_breakdown(avg_dev):.1f}'
            )
            return avg_dev

        except Exception as e:
            logger.warning(f'Correlation from prices failed: {e}')
            try:
                self.conn.rollback()
            except Exception:
                pass
            return None


    def fetch_news_geopolitical_tone(self) -> Optional[float]:
        """Fetch Finnhub general news and compute a sentiment-based stress score (0-100).

        Returns a float in [0, 100] where 0=very calm, 100=extreme stress,
        or None on failure. Result is cached for _FINNHUB_NEWS_CACHE_TTL seconds.
        """
        global _finnhub_news_cache
        now = time.time()
        cached_score, cache_ts = _finnhub_news_cache
        if cached_score is not None and (now - cache_ts) < _FINNHUB_NEWS_CACHE_TTL:
            logger.info(f"Finnhub news sentiment: returning cached score {cached_score:.1f}")
            return cached_score

        try:
            finnhub_key = get_secret("platform/finnhub/api-key")["api_key"]
            _t0 = time.time()
            resp = requests.get(
                "https://finnhub.io/api/v1/news",
                params={"category": "general", "token": finnhub_key},
                timeout=10,
            )
            _ms = int((time.time() - _t0) * 1000)

            if resp.status_code != 200:
                log_api_call(self.conn, "finnhub", "/news?category=general", "regime",
                             False, _ms, error_type=f"http_{resp.status_code}")
                logger.warning(f"Finnhub general news HTTP {resp.status_code}")
                return None

            articles = resp.json()[:30]
            log_api_call(self.conn, "finnhub", "/news?category=general", "regime",
                         True, _ms, pairs_returned=len(articles))

            # Keyword-based scoring: risk keywords push toward 100, calm keywords toward 0
            risk_keywords = [
                "war", "conflict", "crisis", "sanction", "attack", "terror",
                "recession", "default", "collapse", "crash", "surge", "shock",
                "escalat", "invasion", "strike", "tariff", "ban", "emergency",
            ]
            calm_keywords = [
                "recovery", "stability", "growth", "agreement", "deal", "ceasefire",
                "rally", "rebound", "easing", "resolution", "cooperation",
            ]

            risk_hits = 0
            calm_hits = 0
            for article in articles:
                text = (article.get("headline", "") + " " + article.get("summary", "")).lower()
                for kw in risk_keywords:
                    if kw in text:
                        risk_hits += 1
                        break
                for kw in calm_keywords:
                    if kw in text:
                        calm_hits += 1
                        break

            total = len(articles) if articles else 1
            # Net stress ratio: risk proportion pulls toward 100, calm toward 0, neutral at 50
            net_ratio = (risk_hits - calm_hits) / total  # range [-1, +1]
            score = round(50.0 + net_ratio * 50.0, 1)
            score = max(0.0, min(100.0, score))

            _finnhub_news_cache = (score, now)
            logger.info(
                f"Finnhub news sentiment: {len(articles)} articles, "
                f"risk={risk_hits}, calm={calm_hits} → score={score:.1f}"
            )
            return score

        except Exception as e:
            logger.warning(f"Finnhub news geopolitical tone fetch failed: {e}")
            return None

    def gather_component_data(self, session_ctx: SessionContext):
        """Gather raw data for all 7 stress components from RDS and REST."""

        # 1. VIX — already set via set_vix_data() from MCP

        # 2. Yield curve slope (EODHD cross_asset_prices, FRED fallback)
        try:
            yield_data = fetch_eodhd_yield_data(self.conn)
            if yield_data is None:
                yield_data = fetch_fred_yield_data(self.conn)
                if yield_data:
                    logger.info("Yield curve: EODHD unavailable, using FRED fallback")
            if yield_data and yield_data["spread"] is not None:
                spread = yield_data["spread"]
                comp = self.components["yield_curve_slope"]
                comp.raw_value = spread
                comp.normalised_score = self._normalise_yield_curve(spread)
                src = yield_data.get("source", "unknown")
                extra = ""
                if "us10y" in yield_data:
                    extra = (f" (US10Y={yield_data['us10y']:.3f},"
                             f" US2Y={yield_data['us2y']:.3f})")
                cross = ""
                if "us_uk_spread" in yield_data:
                    cross += f" US-UK={yield_data['us_uk_spread']:+.3f}"
                if "us_de_spread" in yield_data:
                    cross += f" US-DE={yield_data['us_de_spread']:+.3f}"
                if "us_jp_spread" in yield_data:
                    cross += f" US-JP={yield_data['us_jp_spread']:+.3f}"
                if cross:
                    cross = f" [{cross.strip()}]"
                src_label = yield_data.get("source", "unknown")
                logger.info(f"Yield curve spread [{src_label}]: {spread:.3f}{extra}{cross} -> "
                            f"stress: {comp.normalised_score:.1f}")
                self._yield_data = yield_data
            else:
                self.components["yield_curve_slope"].raw_value = None
                self._yield_data = None
        except Exception as e:
            logger.warning(f"Yield curve fetch failed: {e}")
            self.components["yield_curve_slope"].raw_value = None
            self._yield_data = None

        # 3. Realised vol divergence (RDS price_metrics)
        try:
            divergences = []
            for pair in PAIRS:
                metrics = fetch_price_metrics(self.conn, pair, "1H", limit=5)
                if (metrics and
                    metrics[0].get("realised_vol_14") and
                    metrics[0].get("realised_vol_30")):
                    rv14 = float(metrics[0]["realised_vol_14"])
                    rv30 = float(metrics[0]["realised_vol_30"])
                    if rv30 > 0:
                        divergences.append(rv14 / rv30 - 1.0)

            if divergences:
                avg_div = sum(divergences) / len(divergences)
                comp = self.components["realised_vol_divergence"]
                comp.raw_value = avg_div
                comp.normalised_score = self._normalise_vol_divergence(avg_div)
                logger.info(f"Realised vol divergence: {avg_div:.3f} → "
                           f"stress: {comp.normalised_score:.1f}")
            else:
                self.components["realised_vol_divergence"].raw_value = None
        except Exception as e:
            logger.warning(f"Vol divergence fetch failed: {e}")
            self.components["realised_vol_divergence"].raw_value = None

        # 4. Correlation breakdown — live corr() from historical_prices 1D bars
        try:
            avg_deviation = self._compute_correlation_from_prices()
            comp = self.components["correlation_breakdown"]
            if avg_deviation is not None:
                comp.raw_value = avg_deviation
                comp.normalised_score = self._normalise_correlation_breakdown(avg_deviation)
            else:
                comp.raw_value = None
        except Exception as e:
            logger.warning(f"Correlation breakdown failed: {e}")
            self.components["correlation_breakdown"].raw_value = None

        # 5. COT extremes — already set via set_cot_data() from MCP

        # 6. Geopolitical index — Finnhub general news sentiment
        # GDELT disabled (persistent 429s). Now using Finnhub /news?category=general
        # with keyword-based scoring. Falls back to neutral 50 if fetch fails.
        comp = self.components["geopolitical_index"]
        news_tone = self.fetch_news_geopolitical_tone()
        if news_tone is not None:
            comp.raw_value = news_tone
            comp.normalised_score = news_tone  # Already in 0-100 range
            logger.info(f"Geopolitical stress (Finnhub news): {news_tone:.1f}")
        else:
            comp.raw_value = 50.0
            comp.normalised_score = 50.0
            logger.info("Geopolitical stress: Finnhub fetch failed, using neutral fallback 50.0")

        # 7. Cross-asset risk (TraderMade live CFD + DXY DB + IG equity sentiment overlay)
        try:
            prev_prices: dict = {}
            if self.previous_snapshot:
                prev_meta = self.previous_snapshot.get("metadata") or {}
                if isinstance(prev_meta, str):
                    import json as _jm
                    prev_meta = _jm.loads(prev_meta)
                prev_prices = prev_meta.get("cross_asset_prices", {})
            current_prices = self.fetch_cross_asset_prices()   # now includes DXY from DB
            ig_equity      = self._fetch_ig_equity_sentiment()
            self._cross_asset_prices_current = current_prices or None
            self.set_cross_asset_data(current_prices, prev_prices, ig_equity)
        except Exception as e:
            logger.warning(f"Cross-asset risk failed: {e}")
            self.components["cross_asset_risk"].raw_value = None
            self._cross_asset_prices_current = None

        # 8. Order flow anomaly (MyFXBook SSI + IG client sentiment extremes)
        try:
            ssi_rows    = fetch_ssi_data(self.conn)
            ig_ssi_rows = fetch_ig_ssi_data(self.conn)

            contrarian_scores = []
            # MyFXBook: contrarian_score already computed (0–1 range)
            for row in (ssi_rows or []):
                cs = row.get("contrarian_score")
                if cs is not None:
                    contrarian_scores.append(abs(float(cs)))
            # IG SSI: derive contrarian score from long/short split
            for row in (ig_ssi_rows or []):
                lp = float(row.get("long_pct") or 50)
                sp = float(row.get("short_pct") or 50)
                contrarian_scores.append(abs(lp - sp) / 100.0)

            comp = self.components["order_flow_anomaly"]
            if contrarian_scores:
                avg_contrarian = sum(contrarian_scores) / len(contrarian_scores)
                comp.raw_value = round(avg_contrarian, 4)
                comp.normalised_score = self._normalise_order_flow_anomaly(avg_contrarian)
                pair_count = len(ssi_rows or []) + len(ig_ssi_rows or [])
                logger.info(
                    f"Order flow anomaly: avg_contrarian={avg_contrarian:.3f} "
                    f"({pair_count} pairs) → stress: {comp.normalised_score:.1f}"
                )
            else:
                comp.raw_value = None
                logger.warning("Order flow anomaly: no SSI data available")
        except Exception as e:
            logger.warning(f"Order flow anomaly failed: {e}")
            self.components["order_flow_anomaly"].raw_value = None

    # --- Composite computation ---

    def compute(self) -> Tuple[float, StressState, float]:
        """
        Compute final stress score with R1, R2, R5 applied.
        Returns (stress_score, stress_state, convergence_boost).
        """
        weighted_scores = []
        null_count = 0
        convergence_boost = 0.0

        # R5 weekend exemption: on Sat/Sun, data gaps are structural (COT not
        # published, no trades → no correlation), not failures. Suppress boost.
        _is_weekend = datetime.now(timezone.utc).isoweekday() >= 6
        _weekend_exempt = {"cot_extremes", "correlation_breakdown", "geopolitical_index"}

        for name, comp in self.components.items():
            effective_score = self._apply_r5_null_handling(comp)
            weighted_scores.append(effective_score * comp.weight)
            if comp.raw_value is None:
                null_count += 1
                if comp.stale_cycles > R5_STALE_CYCLE_THRESHOLD_1:
                    if not (_is_weekend and name in _weekend_exempt):
                        convergence_boost += R5_CONVERGENCE_BUFFER_PER_COMPONENT

        # R5: simultaneous failure check
        self._stress_score_confidence = "high"
        if null_count >= R5_SIMULTANEOUS_FAILURE_COUNT:
            self._stress_score_confidence = "low"
            self.proposals.append({
                "type": "risk_budget",
                "priority": "high",
                "title": f"{null_count} stress components simultaneously unavailable",
                "reasoning": (
                    f"{null_count}/7 stress components returning null. "
                    f"Score unreliable. Elevated tier applied as floor."
                ),
                "data_supporting": ", ".join(
                    f"{c.name}={c.source}" for c in self.components.values()
                    if c.raw_value is None
                ),
                "suggested_action": "Check all data provider connectivity.",
                "expected_impact": "Convergence threshold raised, position size reduced.",
            })

        raw_score = max(0, min(100, sum(weighted_scores)))

        # R2: VIX spike floor
        score_after_vix = self._apply_r2_vix_spike(raw_score)

        # Classify state
        state = self._classify_stress_state(score_after_vix)

        # R1: boundary oscillation
        final_state = self._apply_r1_boundary_oscillation(score_after_vix, state)

        # R5: low confidence → Elevated floor
        if self._stress_score_confidence == "low":
            if final_state == StressState.NORMAL:
                final_state = StressState.ELEVATED
                convergence_boost = max(convergence_boost, 0.05)

        logger.info(
            f"Stress score: {score_after_vix:.1f} | State: {final_state.value} | "
            f"Confidence: {self._stress_score_confidence} | "
            f"Boost: {convergence_boost:.3f}"
        )
        return score_after_vix, final_state, convergence_boost

    def _apply_r2_vix_spike(self, raw_score: float) -> float:
        """R2: VIX spike detection with baseline anchor and 4-cycle decay."""
        vix_comp = self.components["vix_level_trend"]
        current_vix = vix_comp.raw_value

        # Active floor — check decay
        if self.vix_spike.active:
            self.vix_spike.cycles_remaining -= 1

            if self.vix_spike.cycles_remaining <= 0:
                if (current_vix is not None and
                    self.vix_spike.baseline_vix is not None):
                    retracement_threshold = (
                        self.vix_spike.baseline_vix *
                        (1 + R2_VIX_SPIKE_PCT * R2_RETRACEMENT_PCT)
                    )
                    if current_vix <= retracement_threshold:
                        logger.info(f"R2: VIX spike floor released — "
                                   f"VIX {current_vix:.2f} <= {retracement_threshold:.2f}")
                        self.vix_spike.active = False
                        return raw_score
                    else:
                        self.vix_spike.cycles_remaining = 1  # Re-check next cycle
                else:
                    self.vix_spike.cycles_remaining = 1

            if self.vix_spike.active:
                return max(raw_score, R2_STRESS_FLOOR)

        # Check for new spike — baseline anchor is last snapshot VIX
        if current_vix is not None:
            baseline_vix = None
            if self.previous_snapshot and self.previous_snapshot.get("vix_value"):
                baseline_vix = float(self.previous_snapshot["vix_value"])

            if baseline_vix is not None and baseline_vix > 0:
                pct_change = (current_vix - baseline_vix) / baseline_vix
                if pct_change >= R2_VIX_SPIKE_PCT:
                    logger.warning(
                        f"R2: VIX SPIKE — {baseline_vix:.2f} → "
                        f"{current_vix:.2f} ({pct_change*100:.1f}%)"
                    )
                    self.vix_spike.active = True
                    self.vix_spike.baseline_vix = baseline_vix
                    self.vix_spike.spike_vix = current_vix
                    self.vix_spike.cycles_remaining = R2_MIN_FLOOR_CYCLES
                    self.vix_spike.triggered_at = datetime.now(timezone.utc)

                    self.proposals.append({
                        "type": "risk_budget",
                        "priority": "high",
                        "title": f"VIX spiked {pct_change*100:.1f}% in one session",
                        "reasoning": (
                            f"VIX from {baseline_vix:.2f} to {current_vix:.2f}. "
                            f"Pre-crisis rules applied. Floor={R2_STRESS_FLOOR}. "
                            f"Review open positions manually."
                        ),
                        "data_supporting": (
                            f"baseline={baseline_vix:.2f}, current={current_vix:.2f}, "
                            f"change={pct_change*100:.1f}%"
                        ),
                        "suggested_action": (
                            "Review all open positions. Tighten stops. "
                            "Monitor for further deterioration."
                        ),
                        "expected_impact": (
                            "Trailing stops tightened to 70%. "
                            "Pre-crisis convergence (+0.15). Size 0.25×."
                        ),
                    })
                    return max(raw_score, R2_STRESS_FLOOR)

        return raw_score

    def _classify_stress_state(self, score: float) -> StressState:
        """Map stress score to state."""
        for low, high, state_name in STRESS_THRESHOLDS:
            if low <= score < high:
                return StressState(state_name)
        return StressState.CRISIS

    def _apply_r1_boundary_oscillation(
        self, score: float, state: StressState
    ) -> StressState:
        """R1: Boundary oscillation → apply higher state after 4 cycles."""
        boundaries = [30, 50, 70, 85]
        nearest_boundary = None
        min_distance = float("inf")

        for b in boundaries:
            dist = abs(score - b)
            if dist < min_distance:
                min_distance = dist
                nearest_boundary = b

        if min_distance <= R1_BOUNDARY_TOLERANCE:
            if self.boundary_state.boundary_value == nearest_boundary:
                # Hysteresis: only accumulate escalation cycles when score is
                # meaningfully above the boundary — prevents false escalation when
                # score merely grazes it (e.g. 31.0 at boundary=30 → no cycles;
                # needs score ≥ boundary+R1_ESCALATION_BUFFER = 33).
                if score >= nearest_boundary + R1_ESCALATION_BUFFER:
                    self.boundary_state.oscillation_cycles += 1
                elif not self.boundary_state.higher_state_applied:
                    # Bleed off any accumulated cycles while score is in the buffer zone
                    self.boundary_state.oscillation_cycles = max(
                        0, self.boundary_state.oscillation_cycles - 1
                    )
            else:
                self.boundary_state.boundary_value = nearest_boundary
                self.boundary_state.oscillation_cycles = 1
                self.boundary_state.higher_state_applied = False
                self.boundary_state.consecutive_above = 0

            # R1 genuine-oscillation guard: if escalated but score is consistently
            # ABOVE the boundary, the score isn't oscillating — it's stably elevated.
            # Reset after R1_RECOVERY_CYCLES consecutive cycles above boundary.
            if self.boundary_state.higher_state_applied:
                if score >= nearest_boundary:
                    self.boundary_state.consecutive_above += 1
                    if self.boundary_state.consecutive_above >= R1_RECOVERY_CYCLES:
                        logger.info(
                            f"R1: Score {score:.1f} stably above {nearest_boundary} for "
                            f"{self.boundary_state.consecutive_above} cycles "
                            f"— escalation reset (not genuine oscillation)"
                        )
                        self.boundary_state.higher_state_applied = False
                        self.boundary_state.oscillation_cycles = 0
                        self.boundary_state.consecutive_above = 0
                        return state
                else:
                    self.boundary_state.consecutive_above = 0

            if self.boundary_state.oscillation_cycles >= R1_OSCILLATION_CYCLES:
                higher_state = self._get_higher_state(state)
                self.boundary_state.higher_state_applied = True
                self.boundary_state.recovery_cycles_below = 0
                # Do NOT reset consecutive_above here — it must accumulate
                # across cycles so the stable-above guard can trigger a reset.
                logger.info(
                    f"R1: Oscillation at {nearest_boundary} for "
                    f"{self.boundary_state.oscillation_cycles} cycles → "
                    f"{higher_state.value}"
                )
                return higher_state
        else:
            if self.boundary_state.higher_state_applied:
                if (self.boundary_state.boundary_value is not None and
                    score < self.boundary_state.boundary_value - R1_RECOVERY_MARGIN):
                    self.boundary_state.recovery_cycles_below += 1
                    if self.boundary_state.recovery_cycles_below >= R1_RECOVERY_CYCLES:
                        logger.info(f"R1: Recovery confirmed at {score:.1f}")
                        self.boundary_state.higher_state_applied = False
                        self.boundary_state.oscillation_cycles = 0
                        self.boundary_state.boundary_value = None
                    else:
                        return self._get_higher_state(state)
                else:
                    self.boundary_state.recovery_cycles_below = 0
                    if self.boundary_state.boundary_value is not None:
                        return self._get_higher_state(state)
            else:
                self.boundary_state.oscillation_cycles = 0

        return state

    def _get_higher_state(self, current: StressState) -> StressState:
        """Get the next higher stress state."""
        order = [
            StressState.NORMAL, StressState.ELEVATED, StressState.HIGH,
            StressState.PRE_CRISIS, StressState.CRISIS,
        ]
        idx = order.index(current)
        return order[idx + 1] if idx < len(order) - 1 else current

    def get_serialisable_state(self) -> dict:
        """Serialise all component state for market_context_snapshots JSONB."""
        result = {}
        for name, comp in self.components.items():
            result[name] = {
                "raw_value": comp.raw_value,
                "normalised_score": comp.normalised_score,
                "weight": comp.weight,
                "source": comp.source,
                "stale_cycles": comp.stale_cycles,
                "last_known_value": comp.last_known_value,
                "neutral_midpoint": comp.neutral_midpoint,
                "last_updated": (
                    comp.last_updated.isoformat() if comp.last_updated else None
                ),
            }
        result["_vix_spike_state"] = {
            "active": self.vix_spike.active,
            "baseline_vix": self.vix_spike.baseline_vix,
            "spike_vix": self.vix_spike.spike_vix,
            "cycles_remaining": self.vix_spike.cycles_remaining,
        }
        result["_boundary_state"] = {
            "boundary_value": self.boundary_state.boundary_value,
            "oscillation_cycles": self.boundary_state.oscillation_cycles,
            "higher_state_applied": self.boundary_state.higher_state_applied,
            "recovery_cycles_below": self.boundary_state.recovery_cycles_below,
            "consecutive_above": self.boundary_state.consecutive_above,
        }
        return result


# ---------------------------------------------------------------------------
# Regime classification per pair
# ---------------------------------------------------------------------------
class RegimeClassifier:
    """
    Classifies regime per pair using ADX and volatility.
    Implements R3 (prolonged transitional zone).
    """

    def __init__(self, conn, user_id: str, previous_snapshot: Optional[dict] = None):
        self.conn = conn
        self.previous_snapshot = previous_snapshot
        self.adx_transitional_counts: Dict[str, int] = {}
        persisted = load_state('regime', user_id, 'adx_transitional_counts',
                               default=None, max_age_minutes=120)
        if persisted and isinstance(persisted, dict):
            self.adx_transitional_counts = {k: int(v) for k, v in persisted.items()}
            logger.info(f"[state] Restored adx_transitional_counts from agent_state: "
                        f"{self.adx_transitional_counts}")
        else:
            self._restore_transitional_counts()

    def _restore_transitional_counts(self):
        """Restore R3 transitional cycle counts from previous snapshot."""
        if not self.previous_snapshot:
            return
        regime_data = self.previous_snapshot.get("regime_per_pair", {})
        if isinstance(regime_data, str):
            regime_data = json.loads(regime_data)
        for pair, data in regime_data.items():
            if isinstance(data, dict):
                self.adx_transitional_counts[pair] = data.get(
                    "adx_transitional_cycles", 0
                )

    def classify_pair(self, pair: str) -> PairRegime:
        """Classify regime for a single pair."""
        bars = fetch_historical_prices(self.conn, pair, "1H", limit=200)
        metrics = fetch_price_metrics(self.conn, pair, "1H", limit=5)

        adx = compute_adx(bars) if bars else None

        rv14 = None
        rv30 = None
        vol_ratio = None
        if (metrics and
            metrics[0].get("realised_vol_14") and
            metrics[0].get("realised_vol_30")):
            rv14 = float(metrics[0]["realised_vol_14"])
            rv30 = float(metrics[0]["realised_vol_30"])
            if rv30 > 0:
                vol_ratio = rv14 / rv30

        regime = Regime.RANGING
        regime_confidence = "high"

        # Volatility regime overrides ADX
        if vol_ratio is not None:
            if vol_ratio > VOL_HIGH_MULTIPLIER:
                regime = Regime.HIGH_VOLATILITY
            elif vol_ratio < VOL_LOW_MULTIPLIER:
                regime = Regime.LOW_VOLATILITY
            elif adx is not None:
                if adx > ADX_TRENDING:
                    regime = Regime.TRENDING
                elif adx < ADX_RANGING:
                    regime = Regime.RANGING
                else:
                    # Gap C: transitional zone 20-25
                    regime_confidence = "low"
                    prev_count = self.adx_transitional_counts.get(pair, 0)
                    new_count = prev_count + 1
                    self.adx_transitional_counts[pair] = new_count

                    if new_count >= R3_TRANSITIONAL_CYCLES:
                        regime = Regime.RANGING
                        logger.info(f"R3: {pair} transitional for {new_count} cycles → RANGING")
                    else:
                        regime = Regime.RANGING
        elif adx is not None:
            if adx > ADX_TRENDING:
                regime = Regime.TRENDING
            elif adx < ADX_RANGING:
                regime = Regime.RANGING
            else:
                regime_confidence = "low"
                regime = Regime.RANGING

        # R3: Reset transitional count when ADX moves out
        if adx is not None and (adx > ADX_TRENDING or adx < ADX_RANGING):
            self.adx_transitional_counts[pair] = 0

        return PairRegime(
            instrument=pair,
            regime=regime,
            adx=adx,
            realised_vol_14=rv14,
            realised_vol_30=rv30,
            vol_ratio=vol_ratio,
            regime_confidence=regime_confidence,
            adx_transitional_cycles=self.adx_transitional_counts.get(pair, 0),
        )

    def classify_all_pairs(self) -> Dict[str, PairRegime]:
        """Classify regime for all 7 pairs."""
        results = {}
        for pair in PAIRS:
            try:
                results[pair] = self.classify_pair(pair)
                logger.info(
                    f"{pair}: {results[pair].regime.value}, "
                    f"ADX={f'{results[pair].adx:.1f}' if results[pair].adx else 'N/A'}, "
                    f"conf={results[pair].regime_confidence}"
                )
            except Exception as e:
                logger.error(f"Failed to classify {pair}: {e}")
                results[pair] = PairRegime(
                    instrument=pair,
                    regime=Regime.RANGING,
                    regime_confidence="low",
                )
        return results


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------
def write_market_context_snapshot(
    conn,
    session_ctx: SessionContext,
    stress_score: float,
    stress_state: StressState,
    stress_engine: StressScoreEngine,
    pair_regimes: Dict[str, PairRegime],
    degraded_agents: List[str],
    kill_switch_active: bool,
    convergence_boost: float,
):
    """Write full market context snapshot to shared.market_context_snapshots."""
    vix_value = stress_engine.components["vix_level_trend"].raw_value

    # Build metadata payload — yield differentials from gather_component_data
    yd = getattr(stress_engine, '_yield_data', None)
    metadata_payload: dict = {}
    if yd:
        metadata_payload['yield_differentials'] = {
            'us_uk_spread': yd.get('us_uk_spread'),
            'us_de_spread': yd.get('us_de_spread'),
            'us_jp_spread': yd.get('us_jp_spread'),
            'us10y':        yd.get('us10y'),
            'us2y':         yd.get('us2y'),
            'source':       yd.get('source'),
        }
    ca = getattr(stress_engine, '_cross_asset_prices_current', None)
    if ca:
        metadata_payload['cross_asset_prices'] = ca

    regime_data = {}
    for pair, pr in pair_regimes.items():
        regime_data[pair] = {
            "regime": pr.regime.value,
            "adx": pr.adx,
            "realised_vol_14": pr.realised_vol_14,
            "realised_vol_30": pr.realised_vol_30,
            "vol_ratio": pr.vol_ratio,
            "regime_confidence": pr.regime_confidence,
            "adx_transitional_cycles": pr.adx_transitional_cycles,
        }

    with conn.cursor() as cur:
        # Check which columns exist on the table
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'shared' AND table_name = 'market_context_snapshots'
        """)
        existing_cols = {row[0] for row in cur.fetchall()}

        # Build column list and values dynamically
        col_val_pairs = [
            ("snapshot_time", "NOW()"),
        ]
        params = []

        # Compute dominant regime across all pairs (≥60% consensus → named, else "mixed")
        regime_counts: dict = {}
        for pr in pair_regimes.values():
            r = pr.regime.value
            regime_counts[r] = regime_counts.get(r, 0) + 1
        if regime_counts:
            dominant = max(regime_counts, key=regime_counts.get)
            regime_global = dominant if regime_counts[dominant] >= len(pair_regimes) * 0.6 else "mixed"
        else:
            regime_global = None

        current_session_val = session_ctx.session.value

        optional_cols = {
            "current_session": current_session_val,
            "active_sessions": json.dumps([current_session_val]),
            "regime_global": regime_global,
            "day_of_week": session_ctx.day_of_week,
            "stop_hunt_window": session_ctx.stop_hunt_window,
            "ny_close_window": session_ctx.ny_close_window,
            "system_stress_score": stress_score,
            "stress_state": stress_state.value,
            "stress_components": json.dumps(stress_engine.get_serialisable_state()),
            "stress_score_confidence": 1.0 if stress_engine._stress_score_confidence == "high" else 0.5,
            "vix_value": vix_value,
            "vix_spike_floor_active": stress_engine.vix_spike.active,
            "vix_spike_cycles_remaining": stress_engine.vix_spike.cycles_remaining,
            "regime_per_pair": json.dumps(regime_data),
            "degraded_agents": json.dumps(degraded_agents),
            "kill_switch_active": kill_switch_active,
            "convergence_boost": convergence_boost,
            "metadata": json.dumps(metadata_payload),
        }

        for col, val in optional_cols.items():
            if col in existing_cols:
                col_val_pairs.append((col, "%s"))
                params.append(val)

        cols_str = ", ".join(c for c, _ in col_val_pairs)
        vals_str = ", ".join(v for _, v in col_val_pairs)

        cur.execute(
            f"INSERT INTO shared.market_context_snapshots ({cols_str}) VALUES ({vals_str})",
            params,
        )
    conn.commit()
    logger.info("Market context snapshot written")


def write_agent_signal(
    conn,
    user_id: str,
    stress_score: float,
    stress_state: StressState,
    pair_regimes: Dict[str, PairRegime],
    session_ctx: SessionContext,
    payload: dict,
    trajectory_modifiers: dict = None,
):
    """Write regime agent signals to forex_network.agent_signals."""
    with conn.cursor() as cur:
        # Global regime signal
        cur.execute("""
            INSERT INTO forex_network.agent_signals (
                agent_name, user_id, instrument, signal_type,
                score, bias, confidence, payload, expires_at
            ) VALUES (
                %s, %s, NULL, 'regime_classification',
                %s, %s, %s, %s, NOW() + INTERVAL '%s minutes'
            )
        """, (
            AGENT_NAME,
            user_id,
            max(-1.0, min(1.0, (50 - stress_score) / 50)),
            ("neutral" if stress_state in (StressState.NORMAL, StressState.ELEVATED)
             else "bearish"),
            (1.0 if payload.get("stress_score_confidence") == "high" else 0.7),
            json.dumps(payload),
            SIGNAL_EXPIRY_MINUTES,
        ))

        # Per-pair regime signals — enriched payload so the admin dashboard's
        # Agent Observation tab has reasoning / key_factors / decision /
        # data_sources / session_context (same keys macro and technical emit).
        for pair, pr in pair_regimes.items():
            confidence = 1.0 if pr.regime_confidence == "high" else 0.7
            _tm = trajectory_modifiers.get(pair) if trajectory_modifiers else None
            if _tm is not None:
                confidence = _tm['adjusted_confidence']
            def adx_to_score(adx_value):
                if adx_value >= 40:
                    return 0.50  # strong trend
                elif adx_value >= 25:
                    return 0.25 + (adx_value - 25) * (0.25 / 15)  # 0.25-0.50 scaled
                elif adx_value >= 15:
                    return 0.10 + (adx_value - 15) * (0.15 / 10)  # 0.10-0.25 scaled
                else:
                    return 0.10  # floor — even weak ranging contributes something

            if pr.regime in (Regime.TRENDING, Regime.RANGING):
                pair_score = adx_to_score(pr.adx) if pr.adx is not None else (
                    0.50 if pr.regime == Regime.TRENDING else 0.10
                )
            elif pr.regime == Regime.HIGH_VOLATILITY:
                pair_score = -0.3
            elif pr.regime == Regime.LOW_VOLATILITY:
                pair_score = 0.1
            else:
                pair_score = 0.10

            adx_val = pr.adx if pr.adx is not None else float("nan")
            vol_val = pr.vol_ratio if pr.vol_ratio is not None else float("nan")
            adx_interp = (
                "strong trend" if pr.regime == Regime.TRENDING
                else "range-bound (ADX<25)" if pr.regime == Regime.RANGING
                else "elevated volatility" if pr.regime == Regime.HIGH_VOLATILITY
                else "compressed volatility" if pr.regime == Regime.LOW_VOLATILITY
                else "transitional"
            )
            reasoning_lines = [
                f"{pair} classified as {pr.regime.value.upper()} "
                f"(ADX={adx_val:.1f} → {adx_interp}; vol ratio={vol_val:.2f}; "
                f"confidence={pr.regime_confidence}).",
            ]
            if pr.adx_transitional_cycles:
                reasoning_lines.append(
                    f"R3: {pr.adx_transitional_cycles} transitional cycle(s) pending "
                    f"before regime change confirmed."
                )
            reasoning_lines.append(
                f"System stress: {stress_score:.1f} ({stress_state.value}). "
                f"Session: {session_ctx.session.value} (day {session_ctx.day_of_week})."
            )

            enriched_payload = {
                # Preserve original per-pair fields for backward compatibility
                "regime": pr.regime.value,
                "adx": pr.adx,
                "vol_ratio": pr.vol_ratio,
                "regime_confidence": pr.regime_confidence,
                "adx_transitional_cycles": pr.adx_transitional_cycles,
                # Enriched fields for the observation panel
                "reasoning": " ".join(reasoning_lines),
                "decision": pr.regime.value,
                "key_factors": {
                    "ADX_14": pr.adx,
                    "ADX_interpretation": adx_interp,
                    "vol_ratio": pr.vol_ratio,
                    "regime_confidence": pr.regime_confidence,
                    "transitional_cycles": pr.adx_transitional_cycles,
                    "system_stress_score": stress_score,
                    "stress_state": stress_state.value,
                },
                "data_sources": {
                    "price_metrics": "forex_network.price_metrics",
                    "historical_prices": "forex_network.historical_prices",
                    "stress_snapshot": "shared.market_context_snapshots",
                },
                "session_context": {
                    "current_session": session_ctx.session.value,
                    "day_of_week": session_ctx.day_of_week,
                    "utc_hour": session_ctx.utc_hour,
                },
                "system_context": {
                    "stress_score": stress_score,
                    "stress_state": stress_state.value,
                    "stress_state_confidence": payload.get("stress_score_confidence"),
                    "degraded_agents": payload.get("degraded_agents", []),
                    "kill_switch_active": payload.get("kill_switch_active", False),
                    "convergence_boost": payload.get("convergence_boost"),
                    "vix_spike_floor_active": payload.get("vix_spike_floor_active", False),
                },
                "counter_argument": (
                    f"Regime classification depends on ADX and vol_ratio stability. "
                    f"A single cycle read of ADX={adx_val:.1f} can flip if price "
                    f"breaks out in the next hour; R3 requires several transitional "
                    f"cycles before acting on the change."
                ),
                "proposals": [],
            }
            if _tm is not None:
                enriched_payload['trajectory'] = _tm['features']
                enriched_payload['confidence_adjustment_from_trajectory'] = _tm['adj']

            cur.execute("""
                INSERT INTO forex_network.agent_signals (
                    agent_name, user_id, instrument, signal_type,
                    score, bias, confidence, payload, expires_at
                ) VALUES (
                    %s, %s, %s, 'regime_classification',
                    %s, %s, %s, %s, NOW() + INTERVAL '%s minutes'
                )
            """, (
                AGENT_NAME,
                user_id,
                pair,
                pair_score,
                "neutral",
                confidence,
                json.dumps(enriched_payload),
                SIGNAL_EXPIRY_MINUTES,
            ))

        # Feature 1: Signal persistence tracking — per-pair regime bias
        for pair, pr in pair_regimes.items():
            bias = "neutral"  # Regime always emits neutral directional bias
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
            """, (user_id, AGENT_NAME, pair, bias))

    conn.commit()
    logger.info(f"Agent signals written for user {user_id}")


def write_heartbeat(conn, user_id: str, session_id: str, cycle_count: int,
                    degradation_mode: Optional[str] = None,
                    convergence_boost: float = 0.0):
    """Upsert heartbeat to forex_network.agent_heartbeats."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.agent_heartbeats
                (agent_name, user_id, session_id, last_seen, status, cycle_count)
            VALUES (%s, %s, %s, NOW(), 'active', %s)
            ON CONFLICT (agent_name, user_id)
            DO UPDATE SET
                last_seen = NOW(),
                cycle_count = EXCLUDED.cycle_count,
                status = 'active',
                session_id = EXCLUDED.session_id,
                absent_cycles = 0,
                degradation_mode = %s,
                convergence_boost = %s
        """, (AGENT_NAME, user_id, session_id, cycle_count,
              degradation_mode, convergence_boost))
    conn.commit()


def write_audit_log(conn, event_type: str, description: str,
                    metadata: dict, user_id: str = None,
                    source: str = "regime_agent") -> None:
    """Write a single row to forex_network.audit_log."""
    import json as _json
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forex_network.audit_log
                  (user_id, event_type, description, metadata, source, created_at)
                VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
            """, (
                user_id,
                event_type,
                description,
                _json.dumps(metadata),
                source,
            ))
        conn.commit()
    except Exception as e:
        logger.warning(f"audit_log write failed ({event_type}): {e}")
        try:
            conn.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# MCP interaction via Anthropic API
# ---------------------------------------------------------------------------
def _fetch_cot_extremity_from_db() -> Optional[float]:
    """Return the highest abs-positioning percentile (52w) across the 6 FX
    contracts for the most recent CFTC report. Values 0–100; higher = more
    stretched speculator positioning on at least one major pair.

    Returns None on any error — regime agent's stress engine handles null COT
    via its existing substitution logic.
    """
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(pct_abs_52w) AS extremity
                    FROM shared.cot_positioning
                    WHERE report_date = (
                        SELECT MAX(report_date) FROM shared.cot_positioning
                    )
                      AND pct_abs_52w IS NOT NULL
                    """
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"COT extremity DB fetch failed: {e}")
    return None



def _read_vix_from_cache(conn):
    """Return cached VIX value if fresher than 48 hours, else None."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT num_value, fetched_at FROM shared.api_cache "
                "WHERE cache_key = 'vix_last_known'"
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            import datetime as _dt
            fetched = row[1]
            if fetched.tzinfo is None:
                fetched = fetched.replace(tzinfo=_dt.timezone.utc)
            age_h = (_dt.datetime.now(_dt.timezone.utc) - fetched).total_seconds() / 3600
            if age_h < 48:
                return float(row[0])
    except Exception as _e:
        logger.debug(f"VIX cache read failed: {_e}")
    return None


def _write_vix_to_cache(conn, vix: float) -> None:
    """Upsert the latest VIX value into shared.api_cache."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared.api_cache (cache_key, num_value, fetched_at)
                VALUES ('vix_last_known', %s, NOW())
                ON CONFLICT (cache_key) DO UPDATE
                    SET num_value = EXCLUDED.num_value, fetched_at = NOW()
                """,
                (vix,)
            )
        conn.commit()
    except Exception as _e:
        logger.debug(f"VIX cache write failed: {_e}")


def run_mcp_data_gathering(conn=None) -> dict:
    """
    Gather VIX (FRED VIXCLS REST) + COT (shared.cot_positioning DB) data.
    No LLM or MCP calls.
    """

    cot_extremity = _fetch_cot_extremity_from_db()
    if cot_extremity is not None:
        logger.info(f"COT extremity (max pct_abs_52w across 6 FX contracts): {cot_extremity:.2f}")

    try:
        fred_key = get_secret("platform/fred/api-key")["api_key"]
    except Exception as e:
        logger.warning(f"FRED key fetch failed, skipping VIX pull: {e}")
        return {
            "vix_value": None,
            "vix_previous_close": None,
            "vix_trend": None,
            "cot_extremity": cot_extremity,
            "data_notes": f"FRED key unavailable: {e}",
        }

    _t0 = time.time()
    try:
        resp = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": "VIXCLS",
                "api_key": fred_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,  # pull a few in case the most recent is missing
            },
            timeout=10,
        )
        _ms = int((time.time() - _t0) * 1000)
        resp.raise_for_status()
        observations = resp.json().get("observations", [])
        # FRED marks missing days with value "."; filter to real numbers
        values = []
        for o in observations:
            v = o.get("value")
            if v and v != ".":
                try:
                    values.append(float(v))
                except ValueError:
                    continue
        if len(values) < 2:
            if conn is not None:
                log_api_call(conn, 'fred', '/fred/series/observations', 'regime',
                             False, _ms, error_type='insufficient_observations')
            logger.warning(
                f"FRED VIXCLS returned <2 usable observations ({len(values)})"
            )
            return {
                "vix_value": values[0] if values else None,
                "vix_previous_close": None,
                "vix_trend": None,
                "cot_extremity": cot_extremity,
                "data_notes": "FRED VIXCLS: insufficient observations",
            }
        if conn is not None:
            log_api_call(conn, 'fred', '/fred/series/observations', 'regime',
                         True, _ms)
        current, previous = values[0], values[1]
        trend = (current - previous) / previous if previous else None
        logger.info(
            f"FRED VIX: {current:.2f} (prev {previous:.2f}, trend {trend:+.4f}); COT: null"
        )
        if conn is not None:
            _write_vix_to_cache(conn, current)
        return {
            "vix_value": current,
            "vix_previous_close": previous,
            "vix_trend": trend,
            "cot_extremity": cot_extremity,
            "data_notes": "VIX from FRED VIXCLS; COT from shared.cot_positioning (CFTC Legacy FX)",
        }
    except Exception as e:
        _ms = int((time.time() - _t0) * 1000)
        if conn is not None:
            log_api_call(conn, 'fred', '/fred/series/observations', 'regime',
                         False, _ms, error_type=type(e).__name__)
        logger.warning(f"FRED VIX fetch failed: {e}")
        _cached_vix = _read_vix_from_cache(conn) if conn is not None else None
        if _cached_vix is not None:
            logger.info(f"VIX: using cached value {_cached_vix:.2f} (FRED failed)")
            return {
                "vix_value": _cached_vix,
                "vix_previous_close": None,
                "vix_trend": None,
                "cot_extremity": cot_extremity,
                "data_notes": f"VIX from api_cache (FRED failed: {e})",
            }
        logger.warning("VIX: no cache available, using neutral fallback 20.0")
        return {
            "vix_value": 20.0,
            "vix_previous_close": None,
            "vix_trend": None,
            "cot_extremity": cot_extremity,
            "data_notes": f"VIX: neutral fallback 20.0 (FRED failed, no cache: {e})",
        }


# ---------------------------------------------------------------------------
# Kill switch & degradation detection
# ---------------------------------------------------------------------------
def check_kill_switch() -> bool:
    """Check Parameter Store kill switch."""
    try:
        value = get_parameter(KILL_SWITCH_PARAM)
        return value.lower() == "active"
    except Exception as e:
        logger.warning(f"Kill switch check failed: {e}")
        return False  # Fail open


DEGRADATION_STALENESS_SECONDS = 1200  # 20 min — longer than slowest 15-min cycle


def get_degraded_agents(conn) -> List[str]:
    """Return a deduped list of agents whose heartbeat is stale or flagged.

    An agent is one logical producer (macro, technical, …) and has one
    heartbeat row per user, so we dedupe by agent_name before returning.
    Staleness threshold is 1200s (20 min) — agents cycle every ~15 min, so
    1200s gives ~33% headroom before we start flagging transient gaps as
    degradation.
    """
    heartbeats = fetch_agent_heartbeats(conn)
    degraded = set()
    now = datetime.now(timezone.utc)
    for hb in heartbeats:
        name = hb.get("agent_name")
        if not name:
            continue
        if hb.get("degradation_mode") in ("degraded", "halted"):
            degraded.add(name)
            continue
        last_seen = hb.get("last_seen")
        if not last_seen:
            continue
        if hasattr(last_seen, "tzinfo") and last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        if (now - last_seen).total_seconds() > DEGRADATION_STALENESS_SECONDS:
            degraded.add(name)
    return sorted(degraded)


# ---------------------------------------------------------------------------
# Main cycle
# ---------------------------------------------------------------------------
def run_cycle(
    conn,
    user_id: str,
    session_id: str,
    cycle_count: int,
) -> dict:
    """Execute one regime agent cycle. Returns the signal payload."""
    cycle_start = time.time()
    logger.info(f"=== Regime Agent Cycle {cycle_count} ===")

    # 1. Session
    session_ctx = classify_session()
    logger.info(f"Session: {session_ctx.session.value} | Day: {session_ctx.day_of_week}")

    # 2. Kill switch
    kill_switch_active = check_kill_switch()
    if kill_switch_active:
        logger.warning("KILL SWITCH ACTIVE")

    # 3. Previous snapshot for state continuity
    previous_snapshot = fetch_latest_stress_snapshot(conn)

    # 4. Stress score engine
    stress_engine = StressScoreEngine(conn, previous_snapshot)
    stress_engine.calibrate_neutral_midpoints()

    # 5. MCP data (VIX, COT)
    mcp_data = run_mcp_data_gathering(conn=conn)
    stress_engine.set_vix_data(
        mcp_data.get("vix_value"), mcp_data.get("vix_trend")
    )
    stress_engine.set_cot_data(mcp_data.get("cot_extremity"))

    # 6. RDS + REST components
    stress_engine.gather_component_data(session_ctx)

    # 7. Compute (R1, R2, R5)
    stress_score, stress_state, convergence_boost = stress_engine.compute()

    # Load previous classifications state (persists last known regime per pair)
    _prev_classifications = load_state(
        'regime', user_id, 'classifications', default={})

    # 8. Regime per pair (R3)
    classifier = RegimeClassifier(conn, user_id, previous_snapshot)
    pair_regimes = classifier.classify_all_pairs()

    # 8b. Log regime changes vs previous cycle
    for _pair, _pr in pair_regimes.items():
        _prev_regime = _prev_classifications.get(_pair, {}).get('regime')
        if _prev_regime and _prev_regime != _pr.regime.value:
            log_event('REGIME_CHANGE',
                f'{_pair} changed {_prev_regime} → {_pr.regime.value}' +
                (f' ADX={_pr.adx:.1f}' if _pr.adx else ''),
                category='REGIME', agent='regime', instrument=_pair,
                payload={'old': _prev_regime, 'new': _pr.regime.value,
                         'adx': round(_pr.adx, 1) if _pr.adx else None})

    # Compute trajectory modifiers before write_agent_signal (must run before
    # entering the write_agent_signal transaction to avoid rollback conflicts)
    # Single batch query for all pairs
    _traj_batch = get_recent_trajectory_batch(
        conn, 'regime', list(PAIRS),
        lookback_minutes=120, max_cycles_per_pair=8,
    )
    trajectory_modifiers = {}
    for _pair in PAIRS:
        try:
            _traj = _traj_batch.get(_pair, [])
            _features = analyse_trajectory(_traj)
            logger.info(f"Regime trajectory for {_pair}: {_features}")
            _base = 1.0 if pair_regimes[_pair].regime_confidence == "high" else 0.7
            _adj = 0.0
            if _features['persistence'] >= 5 and _features['direction'] in ('strengthening', 'stable'):
                _adj += 0.05
            if _features['volatility'] > 0.20:
                _adj -= 0.10
            if _features['direction'] == 'reversing' and _features['persistence'] <= 2:
                _adj -= 0.05
            # Regime-specific: short persistence + non-trivial volatility = transitional regime
            if _features['persistence'] <= 2 and _features['volatility'] > 0.10:
                _adj -= 0.10
            _adj = max(-0.10, min(0.10, _adj))
            trajectory_modifiers[_pair] = {
                'adjusted_confidence': max(0.10, min(1.0, _base + _adj)),
                'adj': round(_adj, 3),
                'features': _features,
            }
        except Exception as _te:
            logger.warning(f"Regime trajectory failed for {_pair}: {_te}")

    # Checkpoint transitional counts to agent_state (survives restarts)
    try:
        save_state('regime', user_id, 'adx_transitional_counts',
                   classifier.adx_transitional_counts)
        # Persist current regime classifications (pair → regime/adx/confidence)
        save_state('regime', user_id, 'classifications', {
            pair: {
                'instrument': pr.instrument,
                'regime': pr.regime.value,
                'adx': pr.adx,
                'realised_vol_14': pr.realised_vol_14,
                'realised_vol_30': pr.realised_vol_30,
                'vol_ratio': pr.vol_ratio,
                'regime_confidence': pr.regime_confidence,
                'adx_transitional_cycles': pr.adx_transitional_cycles,
                'timeframe_alignment': pr.timeframe_alignment,
            }
            for pair, pr in pair_regimes.items()
        })
    except Exception as _e:
        logger.warning(f"State checkpoint failed (non-fatal): {_e}")

    # 9. Degraded agents
    degraded_agents = get_degraded_agents(conn)

    # 10. Full payload
    payload = {
        "regime_per_pair": {
            pair: {
                "regime": pr.regime.value,
                "adx": pr.adx,
                "vol_ratio": pr.vol_ratio,
                "regime_confidence": pr.regime_confidence,
                "adx_transitional_cycles": pr.adx_transitional_cycles,
            }
            for pair, pr in pair_regimes.items()
        },
        "session_context": {
            "session": session_ctx.session.value,
            "day_of_week": session_ctx.day_of_week,
            "stop_hunt_window": session_ctx.stop_hunt_window,
            "ny_close_window": session_ctx.ny_close_window,
            "utc_hour": session_ctx.utc_hour,
        },
        "system_stress_score": stress_score,
        "stress_state": stress_state.value,
        "stress_components": stress_engine.get_serialisable_state(),
        "stress_score_confidence": stress_engine._stress_score_confidence,
        "vix_spike_floor_active": stress_engine.vix_spike.active,
        "vix_spike_cycles_remaining": stress_engine.vix_spike.cycles_remaining,
        "boundary_oscillation": {
            "boundary_value": stress_engine.boundary_state.boundary_value,
            "oscillation_cycles": stress_engine.boundary_state.oscillation_cycles,
            "higher_state_applied": stress_engine.boundary_state.higher_state_applied,
        },
        "degraded_agents": degraded_agents,
        "kill_switch_active": kill_switch_active,
        "proposals": stress_engine.proposals,
        "data_staleness_flags": {
            c.name: c.source for c in stress_engine.components.values()
        },
        "component_substitution_log": stress_engine.substitution_log,
        "convergence_boost": convergence_boost,
    }

    # 11. Write snapshot
    write_market_context_snapshot(
        conn, session_ctx, stress_score, stress_state,
        stress_engine, pair_regimes, degraded_agents,
        kill_switch_active, convergence_boost,
    )

    # 12. Write signals
    write_agent_signal(
        conn, user_id, stress_score, stress_state,
        pair_regimes, session_ctx, payload,
        trajectory_modifiers=trajectory_modifiers,
    )

    # 12b. Audit: stress threshold crossed
    if previous_snapshot:
        prev_state_val = previous_snapshot.get("stress_state")
        if prev_state_val and prev_state_val != stress_state.value:
            try:
                write_audit_log(
                    conn,
                    event_type="stress_threshold_crossed",
                    description=(
                        f"Market stress transitioned from {prev_state_val} → {stress_state.value} "
                        f"(score: {stress_score:.1f})"
                    ),
                    metadata={
                        "previous_state": prev_state_val,
                        "new_state": stress_state.value,
                        "stress_score": round(stress_score, 2),
                        "confidence": stress_engine._stress_score_confidence,
                        "components": {
                            k: {"value": v.normalised_score, "source": v.source}
                            for k, v in stress_engine.components.items()
                            if v.raw_value is not None
                        },
                    },
                    user_id=user_id,
                )
            except Exception as _ae:
                logger.warning(f"audit stress_threshold_crossed failed: {_ae}")
            log_event('STRESS_ELEVATED',
                f'Stress score {stress_score:.1f} transitioning {prev_state_val} → {stress_state.value}',
                category='SYSTEM', agent='regime', severity='WARN', user_id=user_id,
                payload={'score': round(stress_score, 2), 'band': stress_state.value,
                         'prev_band': prev_state_val})

    # 13. Heartbeat
    _deg_mode = (
        'halted'  if kill_switch_active
        else 'degraded' if stress_engine._stress_score_confidence == 'low'
        else 'normal'
    )
    write_heartbeat(conn, user_id, session_id, cycle_count,
                    degradation_mode=_deg_mode,
                    convergence_boost=convergence_boost)

    elapsed = time.time() - cycle_start
    logger.info(
        f"Cycle {cycle_count} done in {elapsed:.1f}s | "
        f"Stress: {stress_score:.1f} ({stress_state.value}) | "
        f"Kill: {kill_switch_active}"
    )
    return payload


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    """Run the regime agent in a continuous loop for all paper users."""
    import sys
    if "--no-delay" not in sys.argv:
        logger.info("Startup delay 600s (stagger vs macro/technical agents)")
        time.sleep(600)
    logger.info("=" * 60)
    logger.info("Project Neo — Regime Agent v2.0 (deterministic, no LLM)")
    logger.info("=" * 60)
    logger.info("Geopolitical stress: Finnhub news-based scoring (GDELT replaced)")

    conn = get_db_connection()
    validate_schema(conn, EXPECTED_TABLES)
    session_id = str(uuid.uuid4())

    # Resolve paper user IDs
    user_ids = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id
                FROM forex_network.risk_parameters
                WHERE paper_mode = TRUE
            """)
            for row in cur.fetchall():
                uid = str(row["user_id"])
                user_ids[uid] = uid
        conn.commit()  # Clear any transaction state
    except Exception as e:
        logger.warning(f"Could not resolve user IDs: {e}")
        try:
            conn.rollback()
        except:
            pass
        for u in ["neo_user_001", "neo_user_002", "neo_user_003"]:
            user_ids[u] = u

    logger.info(f"Users: {list(user_ids.keys())}")

    import signal as _signal
    def _sigterm_handler(signum, frame):
        logger.info("[state] SIGTERM received — checkpointed state already saved, exiting")
        sys.exit(0)
    _signal.signal(_signal.SIGTERM, _sigterm_handler)

    log_loaded_state_summary('regime')

    cycle_count = 0
    while True:
        market = get_market_state()

        # Standby during weekend and pre-open window
        if market['state'] in ('closed', 'pre_open'):
            logger.info(f"STANDBY — {market['reason']}, skipping cycle")
            for user_label, user_id in user_ids.items():
                try:
                    write_heartbeat(conn, user_id, session_id, cycle_count,
                                     degradation_mode='normal')
                except Exception:
                    pass
            time.sleep(300)  # re-check every 5 min
            continue

        if market['state'] == 'quiet':
            logger.info(f"QUIET HOURS — {market['reason']}, 60-min reduced cycle")

        cycle_count += 1
        for user_label, user_id in user_ids.items():
            try:
                logger.info(f"--- {user_label} ({user_id}) ---")
                run_cycle(
                    conn, user_id, session_id, cycle_count,
                )
            except psycopg2.OperationalError as e:
                logger.error(f"DB connection lost: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_db_connection()
            except Exception as e:
                logger.error(f"Cycle failed for {user_label}: {e}", exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Heartbeat between cycles; quiet hours: 60-min cycle, active: 15-min
        # Wakes early on: state transition OR 06:40/12:40 UTC pre-open window
        interval = 3600 if market['state'] == 'quiet' else CYCLE_INTERVAL_SECONDS
        _prior_state = market['state']
        next_cycle = time.time() + interval
        while time.time() < next_cycle:
            for user_label, user_id in user_ids.items():
                try:
                    write_heartbeat(conn, user_id, session_id, cycle_count,
                                     degradation_mode='normal')
                except Exception as e:
                    logger.warning(f"Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            if _prior_state == 'quiet':
                import datetime as _dt
                _now = _dt.datetime.now(_dt.timezone.utc)
                _hr, _min = _now.hour, _now.minute
                if (_hr == 6 and _min >= 40) or (_hr == 12 and _min >= 40):
                    logger.info(
                        f"Pre-open wake at {_hr:02d}:{_min:02d} UTC -- "
                        f"firing early cycle before market open"
                    )
                    break
            try:
                current_state = get_market_state().get("state")
                if current_state != _prior_state:
                    logger.info(
                        f"Market state transition detected: "
                        f"{_prior_state} -> {current_state}. "
                        f"Breaking sleep early to fire immediate cycle."
                    )
                    break
            except Exception as e:
                logger.warning(f"Market state check during sleep failed: {e}")


if __name__ == "__main__":
    main()

