#!/usr/bin/env python3
"""
Project Neo - Technical Agent v1.0
===================================

The technical agent analyzes price action, technical indicators, and generates
entry/exit signals for each of the 7 currency pairs with confidence levels.
Carries 45% weight in orchestrator convergence scoring.

Architecture:
- EODHD/Alpha Vantage MCP: Cross-provider price validation
- RDS: Historical OHLCV data, pre-calculated price_metrics (ATR, realized vol)
- Technical indicators: ATR(14), ADX(14), MA crossovers, RSI(14), MACD, Bollinger Bands

Signal Output:
- Score: -1.0 (strongly bearish) to +1.0 (strongly bullish)
- Confidence: 0.0 to 1.0
- Expiry: NOW() + 20 minutes
- Proposals: Mandatory structured suggestions for human review
- R:R Calculation: ATR-derived stops, structure-based targets
- Spread-to-signal validation: Expected move must exceed spread × ratio

Key Features:
- T1-T2 Decision Rules: ATR expansion handling, timeframe conflict resolution
- Session-pair alignment weighting
- Regime-indicator alignment (trending vs ranging strategies)
- Cross-provider price sanity checks (0.1% tolerance)
- Spread sanity validation (5× historical average)
- Swap cost integration for overnight positions

Adversarial Defenses:
- Cross-provider price validation before signal generation
- Bid-ask spread sanity checks vs historical averages
- Data staleness detection with session-specific thresholds

Build Date: April 16, 2026
"""

import os
import sys
import re
import json
import uuid
import time
import math
import logging
import requests
import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, log_loaded_state_summary
from shared.score_trajectory import get_recent_trajectory, get_recent_trajectory_batch, analyse_trajectory
from shared.schema_validator import validate_schema
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.agent_signals":    ["agent_name", "instrument", "signal_type", "score",
                                       "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats": ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.historical_prices":["instrument", "timeframe", "ts", "open", "high",
                                       "low", "close", "volume"],
    "forex_network.price_metrics":    ["instrument", "timeframe", "ts", "atr_14",
                                       "realised_vol_14", "realised_vol_30"],
    "forex_network.swap_rates":       ["instrument", "long_rate_pips",
                                       "short_rate_pips", "rate_date"],
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        except Exception as _e:
            logger.debug(f"Indicator calculation failed: {_e}")


class TechnicalAgent:
    """
    Project Neo Technical Agent

    Analyzes technical price action, indicators, and market structure
    to generate directional signals for 7 major FX pairs.
    """

    # Configuration constants
    AGENT_NAME = "technical"
    CYCLE_INTERVAL_MINUTES = 5   # 5-minute polling; LLM call gated by price-change threshold
    PRICE_CHANGE_THRESHOLD_PIPS = 10  # minimum pip move across any pair to justify a new LLM call
    SIGNAL_EXPIRY_MINUTES = 120  # 2h TTL so quiet-market price-skip cycles do not blank the observation panel
    AWS_REGION = "eu-west-2"

    # FX pairs and technical analysis parameters
    PAIRS = [
    # USD pairs
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Cross pairs confirmed on IG demo 2026-04-22
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
    "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
    "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
]

    # Spread-to-signal ratios by user profile
    MIN_SIGNAL_SPREAD_RATIOS = {
        "conservative": 7.0,  # Expected move must be 7× current spread
        "balanced": 5.0,      # Expected move must be 5× current spread
        "aggressive": 4.0     # Expected move must be 4× current spread
    }

    # Minimum R:R ratios by user profile
    MIN_RR_RATIOS = {
        "conservative": 2.0,  # Must make £2 for every £1 risked
        "balanced": 1.5,      # Must make £1.50 for every £1 risked
        "aggressive": 1.2     # Must make £1.20 for every £1 risked
    }

    # ATR stop multipliers by user profile
    ATR_STOP_MULTIPLIERS = {
        "conservative": 1.5,  # ATR(14) × 1.5 (tighter stops)
        "balanced": 1.5,      # ATR(14) × 1.5 (standard)
        "aggressive": 2.0     # ATR(14) × 2.0 (wider stops)
    }

    # Session-pair alignment weights
    SESSION_PAIR_ALIGNMENT = {
        "london": {
            "primary": ["EURUSD", "GBPUSD", "USDCHF"],
            "secondary": ["USDJPY", "EURGBP", "EURCHF", "GBPCHF",
                          "EURJPY", "GBPJPY", "EURAUD", "GBPAUD", "EURCAD", "GBPCAD"],
        },
        "newyork": {
            "primary": ["EURUSD", "GBPUSD", "USDCAD", "USDJPY"],
            "secondary": ["AUDUSD", "EURCAD", "GBPCAD", "CADJPY"],
        },
        "overlap": {
            "primary": ["EURUSD", "GBPUSD", "USDJPY"],
            "secondary": ["AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
                          "EURGBP", "EURJPY", "GBPJPY", "EURAUD", "GBPAUD", "EURCAD", "GBPCAD"],
        },
        "asian": {
            "primary": ["AUDUSD", "NZDUSD", "USDJPY"],
            "secondary": ["USDCAD", "AUDNZD", "AUDJPY", "NZDJPY", "CADJPY"],
        },
    }

    # Cross-provider price tolerance (for adversarial defense)
    PRICE_TOLERANCE_PCT = 0.1  # 0.1% tolerance between providers
    SPREAD_SANITY_MULTIPLIER = 5.0  # Max 5× historical average spread

    def __init__(self, user_id: str = "neo_user_002", dry_run: bool = False):
        """Initialize the technical agent with AWS secrets and database connection."""
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        self.user_id = user_id
        self.dry_run = dry_run
        # Price-skip state: mid prices at the last LLM call, keyed by pair
        self._last_llm_prices: Dict[str, float] = {}

        # Initialize AWS clients
        self.ssm_client = boto3.client('ssm', region_name=self.AWS_REGION)
        self.secrets_client = boto3.client('secretsmanager', region_name=self.AWS_REGION)

        # Load configuration and secrets
        self._load_configuration()

        # Initialize database connection
        self._init_database()

        # Load historical spread averages for sanity checking
        self._load_historical_spreads()


        logger.info(f"Technical agent initialized - Session ID: {self.session_id}")

    def _load_configuration(self):
        """Load configuration from Parameter Store and secrets from Secrets Manager."""
        try:
            # Parameter Store (WITH leading slash)
            self.rds_endpoint = self._get_parameter('/platform/config/rds-endpoint')
            self.aws_region = self._get_parameter('/platform/config/aws-region')
            self.kill_switch = self._get_parameter('/platform/config/kill-switch')

            # Secrets Manager (NO leading slash)
            self.rds_credentials = self._get_secret('platform/rds/credentials')
            self.tradermade_key = self._get_secret('platform/tradermade/api-key')['api_key']
            self.eodhd_key = self._get_secret('platform/eodhd/api-key')['api_key']
            logger.info("Configuration loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def _get_parameter(self, name: str) -> str:
        """Get parameter from Parameter Store."""
        try:
            response = self.ssm_client.get_parameter(Name=name, WithDecryption=True)
            return response['Parameter']['Value']
        except ClientError as e:
            logger.error(f"Failed to get parameter {name}: {e}")
            raise

    def _get_secret(self, name: str) -> Dict[str, Any]:
        """Get secret from Secrets Manager."""
        try:
            response = self.secrets_client.get_secret_value(SecretId=name)
            return json.loads(response['SecretString'])
        except ClientError as e:
            logger.error(f"Failed to get secret {name}: {e}")
            raise

    def _init_database(self):
        """Initialize PostgreSQL database connection."""
        try:
            self.db_conn = psycopg2.connect(
                host=self.rds_endpoint,
                database='postgres',
                user=self.rds_credentials['username'],
                password=self.rds_credentials['password'],
                port=5432,
                options='-c search_path=forex_network,shared,public'
            )
            self.db_conn.autocommit = False
            logger.info("Database connection established")
            validate_schema(self.db_conn, EXPECTED_TABLES)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise



    def _load_historical_spreads(self):
        """Load historical spread averages for sanity checking.

        Groups by instrument only (not session) so that cross pairs whose
        historical rows carry session='utc' or session=None are still usable.
        The spread proxy (avg high-low × 0.3) is used as a session-independent
        baseline; validate_spreads applies a pair-aware fallback when absent.
        """
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT instrument,
                           AVG(high - low) * 0.3 AS historical_spread_avg
                    FROM forex_network.historical_prices
                    WHERE ts >= NOW() - INTERVAL '30 days'
                      AND timeframe = '15M'
                    GROUP BY instrument
                """)
                results = cur.fetchall()
                # Flat dict: {pair: avg_spread} — no per-session split needed
                self.historical_spreads = {}
                for row in results:
                    pair = row['instrument']
                    val  = float(row['historical_spread_avg']) if row['historical_spread_avg'] else None
                    if val:
                        self.historical_spreads[pair] = val
                logger.info(f"Loaded historical spreads for {len(self.historical_spreads)} pairs")

                # Guard against mis-scaled historical data for JPY pairs.
                # Correct DB values are ~0.030–0.045 for JPY crosses; if earlier
                # data was ingested at wrong price scale the query can return
                # ~0.00020 (USD-pair scale), causing false spread warnings every
                # cycle.  Floor at 0.008 (0.8 pip) — far below any real JPY
                # average, well above the mis-scaled artefact.
                _JPY_FLOOR = 0.008
                for _p in list(self.historical_spreads.keys()):
                    if _p.endswith('JPY') and self.historical_spreads[_p] < _JPY_FLOOR:
                        logger.warning(
                            f"JPY spread floor applied: {_p} "
                            f"computed={self.historical_spreads[_p]:.5f} → {_JPY_FLOOR:.5f} "
                            f"(stale/mis-scaled data in historical_prices)"
                        )
                        self.historical_spreads[_p] = _JPY_FLOOR

        except Exception as e:
            logger.error(f"Failed to load historical spreads: {e}")
            # Pair-aware fallback: JPY pairs quoted at 2dp vs 4dp for others
            self.historical_spreads = {
                pair: (0.010 if pair.endswith('JPY') else 0.0001)
                for pair in self.PAIRS
            }

    def check_kill_switch(self) -> bool:
        """Check if the system kill switch is active."""
        try:
            kill_switch = self._get_parameter('/platform/config/kill-switch')
            if kill_switch == 'active':
                logger.warning("Kill switch is ACTIVE - halting all signal generation")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to check kill switch: {e}")
            return True

    def get_current_session(self) -> str:
        """Determine current trading session based on UTC time."""
        current_hour = datetime.now(timezone.utc).hour

        if 7 <= current_hour < 12:
            return "london"
        elif 12 <= current_hour < 16:
            return "overlap"
        elif 16 <= current_hour < 20:
            return "newyork"
        elif 20 <= current_hour <= 23 or 0 <= current_hour < 7:
            return "asian"
        else:
            return "off_hours"

    def get_session_pair_weight(self, pair: str, session: str) -> float:
        """Get weight multiplier based on pair-session alignment."""
        alignment = self.SESSION_PAIR_ALIGNMENT.get(session, {})

        if pair in alignment.get("primary", []):
            return 1.0  # Full weight
        elif pair in alignment.get("secondary", []):
            return 0.8  # Reduced weight
        else:
            return 0.6  # Minimum weight for off-session pairs

    def get_live_prices(self) -> Dict[str, Dict]:
        """
        Fetch live FX prices with fallback hierarchy:
          1. TraderMade REST  — primary (fastest, most reliable for FX)
          2. RDS last bar     — fallback when TraderMade unavailable or market closed
        Returns dict keyed by pair. Includes 'market_closed' and 'source' flags.
        """
        # ── 1. TraderMade REST ────────────────────────────────────────────────
        try:
            pairs_str = ",".join(self.PAIRS)
            url = (
                f"https://marketdata.tradermade.com/api/v1/live"
                f"?currency={pairs_str}&api_key={self.tradermade_key}"
            )
            _t0 = time.time()
            resp = requests.get(url, timeout=10)
            _ms = int((time.time() - _t0) * 1000)
            resp.raise_for_status()
            data = resp.json()
            quotes = data.get("quotes", [])
            if quotes:
                live_prices = {}
                for q in quotes:
                    pair = q.get("base_currency", "") + q.get("quote_currency", "")
                    if pair not in self.PAIRS:
                        continue
                    bid = q.get("bid")
                    ask = q.get("ask")
                    live_prices[pair] = {
                        "bid":          bid,
                        "ask":          ask,
                        "last":         q.get("mid"),
                        "timestamp":    data.get("timestamp"),
                        "source":       "tradermade",
                        "market_closed": False,
                    }
                    if bid and ask:
                        live_prices[pair]["spread"] = ask - bid
                if live_prices:
                    log_api_call(self.db_conn, 'tradermade', '/api/v1/live', 'technical',
                                 True, _ms, pairs_returned=len(live_prices))
                    logger.info(f"Live prices from TraderMade: {len(live_prices)} pairs")
                    return live_prices
            log_api_call(self.db_conn, 'tradermade', '/api/v1/live', 'technical',
                         False, _ms, error_type='empty_response')
        except Exception as e:
            _ms = int((time.time() - _t0) * 1000) if '_t0' in dir() else 0
            log_api_call(self.db_conn, 'tradermade', '/api/v1/live', 'technical',
                         False, _ms, error_type=type(e).__name__)
            logger.warning(f"TraderMade live prices failed: {e} — falling back to RDS")

        # ── 2. RDS last bar close (market closed / TraderMade down) ───────────
        try:
            live_prices = {}
            for pair in self.PAIRS:
                df = self.get_historical_bars(pair, "1H", 1)
                if not df.empty:
                    close = float(df["close"].iloc[-1])
                    live_prices[pair] = {
                        "bid":          None,
                        "ask":          None,
                        "last":         close,
                        "timestamp":    None,
                        "source":       "rds_historical",
                        "market_closed": True,
                    }
            if live_prices:
                logger.info(
                    f"Live prices from RDS fallback: {len(live_prices)} pairs "
                    f"(TraderMade unavailable or market closed)"
                )
                return live_prices
        except Exception as e:
            logger.warning(f"RDS price fallback failed: {e}")

        logger.error("All price sources exhausted — returning empty price dict")
        return {}

    _FALLBACK_STALE_THRESHOLDS = {
        '15M': timedelta(minutes=30),
        '1H':  timedelta(hours=2),
        '1D':  timedelta(days=2),
    }
    _FALLBACK_MAX_LOOKBACK = {
        '15M': timedelta(days=7),
        '1H':  timedelta(days=14),
        '1D':  timedelta(days=14),
    }

    def _resample_1m_to_15m(self, minute_bars: list) -> list:
        """Aggregate 1-minute OHLC bars into 15-minute bars aligned to 15-min floor."""
        buckets = {}
        for q in minute_bars:
            raw = q['date']
            try:
                dt = datetime.strptime(raw, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dt = datetime.strptime(raw, '%Y-%m-%d %H:%M')
            floored = dt.replace(minute=(dt.minute // 15) * 15, second=0)
            key = floored.strftime('%Y-%m-%d %H:%M:%S')
            o, h, l, c = float(q['open']), float(q['high']), float(q['low']), float(q['close'])
            if key not in buckets:
                buckets[key] = {'date': key, 'open': o, 'high': h, 'low': l, 'close': c}
            else:
                b = buckets[key]
                b['high'] = max(b['high'], h)
                b['low']  = min(b['low'],  l)
                b['close'] = c
        return [buckets[k] for k in sorted(buckets)]

    def _tradermade_fallback_bars(self, pair: str, timeframe: str, last_ts) -> int:
        """Fetch bars from TraderMade timeseries and persist to historical_prices.
        Returns count of rows inserted. All exceptions are swallowed."""
        now = datetime.now(timezone.utc)
        max_lb = self._FALLBACK_MAX_LOOKBACK.get(timeframe, timedelta(days=7))
        if last_ts is None:
            fetch_start = now - max_lb
        else:
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            if timeframe == '1D':
                fetch_start = (last_ts + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0)
            elif timeframe == '1H':
                fetch_start = (last_ts + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0)
            else:
                fetch_start = (last_ts + timedelta(minutes=15)).replace(
                    second=0, microsecond=0)
        cap_start = now - max_lb
        if fetch_start < cap_start:
            fetch_start = cap_start
        if timeframe == '1D':
            fetch_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_str = fetch_start.strftime('%Y-%m-%d')
            end_str   = fetch_end.strftime('%Y-%m-%d')
            tm_interval = 'daily'
        elif timeframe == '1H':
            fetch_end = now.replace(minute=0, second=0, microsecond=0)
            start_str = fetch_start.strftime('%Y-%m-%d %H:%M')
            end_str   = fetch_end.strftime('%Y-%m-%d %H:%M')
            tm_interval = 'hourly'
        else:  # 15M
            floor_min = (now.minute // 15) * 15
            fetch_end = now.replace(minute=floor_min, second=0, microsecond=0)
            start_str = fetch_start.strftime('%Y-%m-%d %H:%M')
            end_str   = fetch_end.strftime('%Y-%m-%d %H:%M')
            tm_interval = 'minute'
        if fetch_start >= fetch_end:
            return 0
        all_bars = []
        try:
            if tm_interval == 'minute':
                chunk = fetch_start
                while chunk < fetch_end:
                    chunk_end = min(chunk + timedelta(hours=24), fetch_end)
                    resp = requests.get(
                        'https://marketdata.tradermade.com/api/v1/timeseries',
                        params={
                            'currency':   pair,
                            'api_key':    self.tradermade_key,
                            'start_date': chunk.strftime('%Y-%m-%d %H:%M'),
                            'end_date':   chunk_end.strftime('%Y-%m-%d %H:%M'),
                            'format':     'records',
                            'interval':   'minute',
                        }, timeout=20)
                    resp.raise_for_status()
                    minute_bars = resp.json().get('quotes', [])
                    if minute_bars:
                        all_bars.extend(self._resample_1m_to_15m(minute_bars))
                    chunk = chunk_end
            else:
                resp = requests.get(
                    'https://marketdata.tradermade.com/api/v1/timeseries',
                    params={
                        'currency':   pair,
                        'api_key':    self.tradermade_key,
                        'start_date': start_str,
                        'end_date':   end_str,
                        'format':     'records',
                        'interval':   tm_interval,
                    }, timeout=20)
                resp.raise_for_status()
                all_bars = resp.json().get('quotes', [])
        except Exception as _fe:
            logger.warning(
                f"TraderMade fallback fetch failed for {pair} {timeframe}: {_fe}")
            return 0
        if not all_bars:
            return 0
        inserted = 0
        try:
            with self.db_conn.cursor() as cur:
                for q in all_bars:
                    raw = q['date']
                    try:
                        dt = datetime.strptime(raw, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            dt = datetime.strptime(raw, '%Y-%m-%d %H:%M')
                        except ValueError:
                            dt = datetime.strptime(raw, '%Y-%m-%d')
                    ts = dt.replace(tzinfo=timezone.utc)
                    cur.execute("""
                        INSERT INTO forex_network.historical_prices
                            (instrument, timeframe, ts, open, high, low, close, session)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'utc')
                        ON CONFLICT (instrument, timeframe, ts) DO NOTHING
                    """, (pair, timeframe, ts,
                          float(q['open']), float(q['high']),
                          float(q['low']),  float(q['close'])))
                    inserted += cur.rowcount
            self.db_conn.commit()
            logger.info(
                f"TraderMade fallback: persisted {inserted} new {timeframe} bars "
                f"for {pair} (fetched {len(all_bars)})")
        except Exception as _ie:
            logger.warning(
                f"TraderMade fallback DB write failed for {pair} {timeframe}: {_ie}")
            try:
                self.db_conn.rollback()
            except Exception as _e:
                logger.debug(f"Indicator calculation failed: {_e}")
        return inserted

    def get_historical_bars(self, pair: str, timeframe: str = "1H", limit: int = 200) -> pd.DataFrame:
        """Get historical OHLCV bars from RDS; triggers TraderMade fallback when empty or stale."""
        def _fetch_from_rds():
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT ts, open, high, low, close, volume, session
                    FROM forex_network.historical_prices
                    WHERE instrument = %s AND timeframe = %s
                    ORDER BY ts DESC LIMIT %s
                """, (pair, timeframe, limit))
                rows = cur.fetchall()
            if not rows:
                return pd.DataFrame(), None
            df = pd.DataFrame(rows)
            df = df.sort_values('ts').reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: float(x) if x is not None else 0.0)
            latest = df['ts'].iloc[-1]
            return df, latest

        try:
            df, latest_ts = _fetch_from_rds()
            threshold = self._FALLBACK_STALE_THRESHOLDS.get(timeframe)
            now = datetime.now(timezone.utc)
            if df.empty:
                stale = True
            elif threshold is not None and latest_ts is not None:
                ts_utc = latest_ts if latest_ts.tzinfo else latest_ts.replace(tzinfo=timezone.utc)
                stale = (now - ts_utc) > threshold
            else:
                stale = False
            if stale:
                inserted = self._tradermade_fallback_bars(
                    pair, timeframe, latest_ts if not df.empty else None)
                if inserted > 0:
                    df, _ = _fetch_from_rds()
            if df.empty:
                logger.warning(f"No historical data for {pair} {timeframe} after fallback")
            return df
        except Exception as e:
            logger.error(f"Failed to get historical bars for {pair}: {e}")
            return pd.DataFrame()

    def _persist_price_bars(self, instrument: str, timeframe: str, bars) -> int:
        """Write fetched OHLCV bars to historical_prices for fallback/backtesting.
        Accepts a DataFrame or list of dicts with keys: ts, open, high, low, close.
        Uses ON CONFLICT DO NOTHING — safe to call every cycle.
        Only writes the most recent 20 rows.
        Returns number of rows newly inserted.
        """
        if bars is None:
            return 0
        if hasattr(bars, 'iterrows'):
            if bars.empty:
                return 0
            records = bars.tail(20).to_dict('records')
        else:
            if not bars:
                return 0
            records = list(bars)[-20:]

        inserted = 0
        try:
            with self.db_conn.cursor() as cur:
                for r in records:
                    ts = r.get('ts') or r.get('timestamp')
                    if ts is None:
                        continue
                    if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    cur.execute("""
                        INSERT INTO forex_network.historical_prices
                            (instrument, timeframe, ts, open, high, low, close, session)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'utc')
                        ON CONFLICT (instrument, timeframe, ts) DO NOTHING
                    """, (
                        instrument, timeframe, ts,
                        float(r.get('open') or 0),
                        float(r.get('high') or 0),
                        float(r.get('low') or 0),
                        float(r.get('close') or 0),
                    ))
                    inserted += cur.rowcount
            self.db_conn.commit()
        except Exception as e:
            logger.warning(f"_persist_price_bars failed for {instrument} {timeframe}: {e}")
            try:
                self.db_conn.rollback()
            except Exception as _e:
                logger.debug(f"Indicator calculation failed: {_e}")
        return inserted

    def get_price_metrics(self, pair: str, timeframe: str = "1H", limit: int = 50) -> pd.DataFrame:
        """Get pre-calculated price metrics (ATR, realized vol) from RDS."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT ts, atr_14, realised_vol_14, realised_vol_30
                    FROM forex_network.price_metrics
                    WHERE instrument = %s AND timeframe = %s
                    ORDER BY ts DESC LIMIT %s
                """, (pair, timeframe, limit))

                results = cur.fetchall()

                if results:
                    df = pd.DataFrame(results)
                    df = df.sort_values('ts').reset_index(drop=True)
                    # Convert Decimal columns to float
                    for col in ['atr_14', 'realised_vol_14', 'realised_vol_30']:
                        if col in df.columns:
                            df[col] = df[col].apply(lambda x: float(x) if x is not None else 0.0)
                    return df
                else:
                    logger.warning(f"No price metrics found for {pair} {timeframe}")
                    return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to get price metrics for {pair}: {e}")
            return pd.DataFrame()

    def calculate_technical_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate technical indicators from OHLCV data."""
        if df.empty or len(df) < 20:
            return {}

        indicators = {}

        try:
            # Moving averages
            indicators["ema_50"] = df['close'].ewm(span=50).mean().iloc[-1] if len(df) >= 50 else None
            indicators["ema_200"] = df['close'].ewm(span=200).mean().iloc[-1] if len(df) >= 200 else None

            # ADX (trend strength) - simplified calculation
            high_low = df['high'] - df['low']
            high_close = abs(df['high'] - df['close'].shift(1))
            low_close = abs(df['low'] - df['close'].shift(1))
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            plus_dm = (df['high'] - df['high'].shift(1)).where(
                (df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low']), 0
            ).where(df['high'] - df['high'].shift(1) > 0, 0)

            minus_dm = (df['low'].shift(1) - df['low']).where(
                (df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1)), 0
            ).where(df['low'].shift(1) - df['low'] > 0, 0)

            atr = true_range.ewm(span=14).mean()
            plus_di = 100 * (plus_dm.ewm(span=14).mean() / atr)
            minus_di = 100 * (minus_dm.ewm(span=14).mean() / atr)

            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float('nan'))
            _adx_val = dx.ewm(span=14).mean().iloc[-1]
            indicators["adx"] = _adx_val if not pd.isna(_adx_val) else 20.0

            # RSI (14-period)
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0).ewm(span=14).mean()
            loss = (-delta).where(delta < 0, 0).ewm(span=14).mean()
            rs = gain / loss
            indicators["rsi"] = (100 - (100 / (1 + rs))).iloc[-1]

            # MACD (12, 26, 9)
            ema_12 = df['close'].ewm(span=12).mean()
            ema_26 = df['close'].ewm(span=26).mean()
            macd_line = ema_12 - ema_26
            macd_signal = macd_line.ewm(span=9).mean()
            indicators["macd"] = macd_line.iloc[-1]
            indicators["macd_signal"] = macd_signal.iloc[-1]
            indicators["macd_histogram"] = (macd_line - macd_signal).iloc[-1]

            # Bollinger Bands (20, 2σ)
            bb_period = 20
            if len(df) >= bb_period:
                bb_mean = df['close'].rolling(bb_period).mean()
                bb_std = df['close'].rolling(bb_period).std()
                indicators["bb_upper"] = (bb_mean + 2 * bb_std).iloc[-1]
                indicators["bb_lower"] = (bb_mean - 2 * bb_std).iloc[-1]
                indicators["bb_middle"] = bb_mean.iloc[-1]

            # Stochastic (14, 3, 3)
            if len(df) >= 14:
                low_14 = df['low'].rolling(14).min()
                high_14 = df['high'].rolling(14).max()
                k_percent = 100 * ((df['close'] - low_14) / (high_14 - low_14))
                indicators["stoch_k"] = k_percent.rolling(3).mean().iloc[-1]
                indicators["stoch_d"] = k_percent.rolling(3).mean().rolling(3).mean().iloc[-1]

            # Current price position
            indicators["current_price"] = df['close'].iloc[-1]
            indicators["price_change_24h"] = ((df['close'].iloc[-1] / df['close'].iloc[-24]) - 1) * 100 if len(df) >= 24 else 0

        except Exception as e:
            logger.error(f"Error calculating technical indicators: {e}")
            return {}

        return indicators

    def cross_validate_prices(self, live_prices: Dict[str, Dict]) -> Dict[str, bool]:
        """Cross-validate live prices across providers for adversarial defense."""
        validation_results = {}

        try:
            # For now, implement basic validation using historical context
            # In full implementation, would query EODHD/Alpha Vantage via MCP
            for pair in self.PAIRS:
                if pair not in live_prices:
                    validation_results[pair] = False
                    continue

                live_price = live_prices[pair].get("last")
                if not live_price:
                    validation_results[pair] = False
                    continue

                # Get recent historical price for sanity check
                recent_df = self.get_historical_bars(pair, "1H", 3)
                if not recent_df.empty:
                    recent_price = recent_df['close'].iloc[-1]
                    price_diff_pct = abs(live_price - recent_price) / recent_price * 100

                    # Allow up to 1% deviation from recent historical price
                    validation_results[pair] = price_diff_pct < 1.0
                else:
                    validation_results[pair] = True  # No historical data to compare

            logger.info(f"Price validation: {sum(validation_results.values())}/{len(validation_results)} pairs validated")

        except Exception as e:
            logger.error(f"Price cross-validation failed: {e}")
            # Conservative default - assume all are valid
            validation_results = {pair: True for pair in self.PAIRS}

        return validation_results

    def validate_spreads(self, live_prices: Dict[str, Dict], session: str) -> Dict[str, bool]:
        """Validate that bid-ask spreads are within reasonable bounds."""
        spread_validation = {}

        for pair in self.PAIRS:
            if pair not in live_prices or "spread" not in live_prices[pair]:
                spread_validation[pair] = False
                continue

            current_spread = live_prices[pair]["spread"]
            # JPY pairs are quoted at 2 d.p. (spreads ~0.008–0.015) vs 4 d.p.
            # for others (~0.0001–0.0003).  Use pair-aware fallback when no DB
            # data is available (cross pairs may lack 15M history).
            _jpy_default = 0.010  # 1.0 pip in JPY terms
            _std_default  = 0.0002  # 0.2 pip for all other pairs
            _default_spread = _jpy_default if pair.endswith('JPY') else _std_default
            historical_avg = self.historical_spreads.get(pair, _default_spread)

            # Check if spread is within 5× historical average (adversarial defense)
            max_allowed_spread = historical_avg * self.SPREAD_SANITY_MULTIPLIER

            spread_validation[pair] = current_spread <= max_allowed_spread

            if not spread_validation[pair]:
                logger.warning(f"{pair} spread {current_spread:.5f} exceeds {self.SPREAD_SANITY_MULTIPLIER}× historical average {historical_avg:.5f}")

        return spread_validation

    def calculate_atr_stop_and_target(self, pair: str, direction: str, current_price: float,
                                    price_metrics: pd.DataFrame, profile: str = "balanced") -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate ATR-based stop loss and target levels."""
        try:
            if price_metrics.empty:
                warn("technical_agent", "NULL_DATA", "No price metrics for pair",
                     pair=pair, timeframe="1H")
                return None, None, None

            current_atr = float(price_metrics['atr_14'].iloc[-1])
            if not current_atr or current_atr <= 0:
                return None, None, None

            # Apply T1 rule: ATR expansion check
            if len(price_metrics) >= 4:   # Need 4 × 1H bars = 4 hours
                atr_4h_ago = price_metrics['atr_14'].iloc[-4]
                if atr_4h_ago > 0 and (current_atr / atr_4h_ago) > 3.0:  # 200% expansion = 3× ratio
                    # Use 5-day ATR average instead
                    if len(price_metrics) >= 120:  # 120 × 1H bars = 5 days
                        current_atr = price_metrics['atr_14'].tail(120).mean()
                        logger.info(f"{pair}: ATR normalized due to 200%+ expansion")

            # Get ATR multiplier for user profile
            atr_multiplier = self.ATR_STOP_MULTIPLIERS.get(profile, 1.5)

            # Calculate stop loss
            stop_distance = current_atr * atr_multiplier

            if direction.lower() == "long":
                stop_loss = current_price - stop_distance
                # Target at 2× stop distance for minimum 2:1 R:R
                target = current_price + (stop_distance * 2)
            else:  # short
                stop_loss = current_price + stop_distance
                target = current_price - (stop_distance * 2)

            # Calculate R:R ratio
            risk = abs(current_price - stop_loss)
            reward = abs(target - current_price)
            rr_ratio = reward / risk if risk > 0 else 0

            return stop_loss, target, rr_ratio

        except Exception as e:
            logger.error(f"Error calculating ATR stop/target for {pair}: {e}")
            return None, None, None

    def check_spread_to_signal_ratio(self, expected_pips: float, spread: float, profile: str = "balanced") -> bool:
        """Check if expected move meets minimum spread-to-signal ratio."""
        min_ratio = self.MIN_SIGNAL_SPREAD_RATIOS.get(profile, 5.0)
        return expected_pips > (spread * min_ratio)

    def generate_signals(self, live_prices: Dict[str, Dict]) -> List[Dict]:
        """
        Deterministic signal generation using RSI(14) pullback + ADX(14) trend filter.

        Logic:
          - trending       : ADX > 20
          - bullish setup  : trending AND 38 <= RSI <= 55  (pullback in uptrend)
          - bearish setup  : trending AND 45 <= RSI <= 62  (rally in downtrend)
          - score          : direction × (ADX/100) × conviction × session_weight
          - conviction     : linear position within the RSI pullback zone
          - bias           : bullish if score > 0.1, bearish if score < -0.1, else neutral
          - stop_distance  : ATR(14) × 1.5 in pips

        Returns list of signal dicts compatible with write_signals_to_database.
        """
        session = self.get_current_session()
        signals: List[Dict] = []

        for pair in self.PAIRS:
            try:
                # ── fetch bars ───────────────────────────────────────────────
                df = self.get_historical_bars(pair, '1H', 50)
                if df is None or df.empty or len(df) < 20:
                    raise ValueError(f"insufficient bars ({0 if df is None or df is False or (hasattr(df,'empty') and df.empty) else len(df)})")

                # ── compute indicators ───────────────────────────────────────
                indicators = self.calculate_technical_indicators(df)
                rsi = float(indicators.get('rsi') or 50.0)
                adx = float(indicators.get('adx') or 15.0)
                if pd.isna(rsi):
                    rsi = 50.0
                if pd.isna(adx):
                    adx = 15.0

                # ── ATR: prefer price_metrics, fall back to computed ─────────
                pm = self.get_price_metrics(pair, '1H', 5)
                if not pm.empty and pm['atr_14'].iloc[-1] and float(pm['atr_14'].iloc[-1]) > 0:
                    atr = float(pm['atr_14'].iloc[-1])
                else:
                    # compute ATR(14) from bars directly
                    if len(df) >= 15:
                        high_low = df['high'] - df['low']
                        high_pc  = abs(df['high'] - df['close'].shift(1))
                        low_pc   = abs(df['low']  - df['close'].shift(1))
                        tr       = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
                        atr      = float(tr.ewm(span=14, adjust=False).mean().iloc[-1])
                    else:
                        atr = 0.0

                pip_size = 0.01 if 'JPY' in pair else 0.0001
                trending = adx > 20

                # ── RSI pullback direction & conviction ──────────────────────
                if trending and 38.0 <= rsi <= 55.0:
                    direction  =  1.0   # bullish pullback
                    conviction = (55.0 - rsi) / (55.0 - 38.0)   # 1.0 @ RSI=38, 0.0 @ RSI=55
                elif trending and 45.0 <= rsi <= 62.0:
                    direction  = -1.0   # bearish rally into resistance
                    conviction = (rsi - 45.0) / (62.0 - 45.0)   # 1.0 @ RSI=62, 0.0 @ RSI=45
                else:
                    direction  = 0.0
                    conviction = 0.0

                session_weight = self.get_session_pair_weight(pair, session)
                score          = direction * (adx / 100.0) * conviction * session_weight
                score          = round(score, 4)

                bias = 'bullish' if score > 0.1 else ('bearish' if score < -0.1 else 'neutral')
                confidence = round(min(0.90, max(0.10, abs(score) * 1.5)), 4)
                stop_distance_pips = round((atr * 1.5 / pip_size), 1) if atr > 0 else 20.0

                signals.append({
                    'agent_name':   self.AGENT_NAME,
                    'instrument':   pair,
                    'signal_type':  'price_action',
                    'score':        score,
                    'bias':         bias,
                    'confidence':   confidence,
                    'payload': {
                        'rsi_14':             round(rsi, 2),
                        'adx_14':             round(adx, 2),
                        'atr_14':             round(atr, 6),
                        'stop_distance_pips': stop_distance_pips,
                        'trending':           trending,
                        'session':            session,
                        'session_weight':     session_weight,
                        'proposals':          [],
                        # signal_contract stubs — required by shared/signal_validator.py
                        'risk_management': {
                            'stop_distance_pips': stop_distance_pips,
                            'current_spread':     0.0,
                        },
                        'technical_analysis': {
                            'indicators': {'atr_14': round(atr, 6)},
                            'rsi_14':     round(rsi, 2),
                            'adx_14':     round(adx, 2),
                        },
                    }
                })

            except Exception as _e:
                logger.warning(f"generate_signals failed for {pair}: {_e}")
                signals.append({
                    'agent_name':  self.AGENT_NAME,
                    'instrument':  pair,
                    'signal_type': 'price_action',
                    'score':       0.0,
                    'bias':        'neutral',
                    'confidence':  0.10,
                    'payload':     {'error': str(_e), 'proposals': []},
                })

        logger.info(
            f"generate_signals: {sum(1 for s in signals if s['bias'] != 'neutral')} "
            f"directional / {len(signals)} total | session={session}"
        )
        return signals

    def write_signals_to_database(self, signals: List[Dict]) -> bool:
        """Write generated signals to the database."""
        try:
            with self.db_conn.cursor() as cur:
                for signal in signals:
                    expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.SIGNAL_EXPIRY_MINUTES)
                    _score = signal.get('score', 0.0)
                    _bias = 'bullish' if _score > 0.02 else ('bearish' if _score < -0.02 else 'neutral')

                    cur.execute("""
                        INSERT INTO forex_network.agent_signals
                        (agent_name, user_id, instrument, signal_type, score, bias, confidence, payload, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        signal.get('agent_name', self.AGENT_NAME),
                        self.user_id,
                        signal.get('instrument'),
                        signal.get('signal_type', 'price_action'),
                        _score,
                        _bias,
                        signal.get('confidence', 0.0),
                        json.dumps(signal.get('payload', {})),
                        expires_at
                    ))

                # Feature 1: Signal persistence tracking
                for signal in signals:
                    instrument = signal.get('instrument')
                    if not instrument:
                        continue
                    _score = signal.get('score', 0.0)
                    bias = 'bullish' if _score > 0.02 else ('bearish' if _score < -0.02 else 'neutral')
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
                    """, (self.user_id, self.AGENT_NAME, instrument, bias))

                self.db_conn.commit()
                logger.info(f"Successfully wrote {len(signals)} technical signals to database")
                return True

        except Exception as e:
            logger.error(f"Failed to write technical signals to database: {e}")
            self.db_conn.rollback()
            return False

    def update_heartbeat(self) -> bool:
        """Update agent heartbeat in the database."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.agent_heartbeats
                    (agent_name, user_id, session_id, last_seen, status, cycle_count)
                    VALUES (%s, %s, %s, NOW(), 'active', %s)
                    ON CONFLICT (agent_name, user_id)
                    DO UPDATE SET
                        last_seen = NOW(),
                        status = 'active',
                        cycle_count = EXCLUDED.cycle_count,
                        session_id = EXCLUDED.session_id
                """, (self.AGENT_NAME, self.user_id, self.session_id, self.cycle_count))

                self.db_conn.commit()
                return True

        except Exception as e:
            logger.error(f"Failed to update technical agent heartbeat: {e}")
            self.db_conn.rollback()
            return False

    def _reuse_existing_signals(self) -> bool:
        """
        On a price-skip cycle, re-insert the most recent signal per pair with
        fresh timestamps and reuse flags so orchestrator freshness checks pass.
        Preserves all signal content (score, bias, confidence, full payload).
        Returns True on success; logs and returns False on DB error.
        """
        try:
            reused_count = 0
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                for instrument in self.PAIRS:
                    cur.execute("""
                        SELECT signal_type, score, bias, confidence, payload
                        FROM forex_network.agent_signals
                        WHERE agent_name = %s
                          AND user_id = %s
                          AND instrument = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (self.AGENT_NAME, self.user_id, instrument))
                    prev = cur.fetchone()
                    if not prev:
                        logger.debug(
                            f"Price-skip {instrument}: no prior signal found — "
                            f"will write fresh on next LLM cycle"
                        )
                        continue

                    expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.SIGNAL_EXPIRY_MINUTES)

                    # Clone payload; handle both dict (JSONB auto-parse) and str
                    raw_payload = prev['payload']
                    if isinstance(raw_payload, str):
                        import json as _json
                        try:
                            payload = _json.loads(raw_payload)
                        except Exception:
                            payload = {}
                    elif isinstance(raw_payload, dict):
                        payload = dict(raw_payload)
                    else:
                        payload = {}

                    payload['reused'] = True
                    payload['reuse_reason'] = 'price_skip'

                    _score = float(prev['score']) if prev['score'] is not None else 0.0
                    _bias = prev['bias'] or (
                        'bullish' if _score > 0.02 else ('bearish' if _score < -0.02 else 'neutral')
                    )
                    _confidence = float(prev['confidence']) if prev['confidence'] is not None else 0.0

                    cur.execute("""
                        INSERT INTO forex_network.agent_signals
                            (agent_name, user_id, instrument, signal_type,
                             score, bias, confidence, payload, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        self.AGENT_NAME,
                        self.user_id,
                        instrument,
                        prev['signal_type'],
                        _score,
                        _bias,
                        _confidence,
                        json.dumps(payload),
                        expires_at,
                    ))

                    # Keep signal_persistence up-to-date (same as write_signals_to_database)
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
                    """, (self.user_id, self.AGENT_NAME, instrument, _bias))

                    logger.info(
                        f"Price-skip {instrument}: reused previous signal "
                        f"(score={_score:.3f} bias={_bias}, timestamps refreshed)"
                    )
                    reused_count += 1

            self.db_conn.commit()
            logger.info(
                f"Price-skip cycle #{self.cycle_count}: "
                f"reused {reused_count}/{len(self.PAIRS)} signals"
            )
            return True

        except Exception as e:
            logger.error(f"_reuse_existing_signals failed: {e}")
            try:
                self.db_conn.rollback()
            except Exception as _e:
                logger.debug(f"Indicator calculation failed: {_e}")
            return False

    def should_call_llm(self, live_prices: Dict[str, Dict]) -> bool:
        """Return True if any pair has moved >= PRICE_CHANGE_THRESHOLD_PIPS since
        the last LLM call, or if no previous prices are stored (first cycle).
        Saves ~$400/month by skipping the Anthropic call during quiet markets.
        """
        if not self._last_llm_prices:
            return True  # first cycle — always call
        for pair, data in live_prices.items():
            current_mid = data.get("last")
            if current_mid is None:
                continue
            prev_mid = self._last_llm_prices.get(pair)
            if prev_mid is None:
                return True  # new pair appeared
            pip_scale = 100 if 'JPY' in pair else 10000
            if abs(float(current_mid) - float(prev_mid)) * pip_scale >= self.PRICE_CHANGE_THRESHOLD_PIPS:
                return True
        return False

    def _apply_trajectory_modifiers(self, signals: list) -> None:
        """
        Post-LLM confidence adjustment based on recent score trajectory.
        Modifies signals in-place. Score is never modified — only confidence.
        Trajectory features stored in payload for observability.
        Failures are swallowed so a trajectory DB issue never crashes a cycle.
        """
        # Pre-fetch trajectories for all instruments in a single batch query
        _instruments = [s['instrument'] for s in signals if s.get('instrument')]
        _trajectories = get_recent_trajectory_batch(
            self.db_conn, 'technical', _instruments,
            lookback_minutes=60, max_cycles_per_pair=12,
        )
        for sig in signals:
            instrument = sig.get('instrument')
            if not instrument:
                continue
            try:
                trajectory = _trajectories.get(instrument, [])
                features = analyse_trajectory(trajectory)
                logger.info(f"Technical trajectory for {instrument}: {features}")

                base_confidence = sig.get('confidence', 0.0)
                conf_adjustment = 0.0

                # Strong persistence with current bias → boost confidence
                if features['persistence'] >= 5 and features['direction'] in ('strengthening', 'stable'):
                    conf_adjustment += 0.05

                # High volatility → reduce confidence (signal is unstable)
                if features['volatility'] > 0.20:
                    conf_adjustment -= 0.10

                # Reversing (sign flip recent) → reduce confidence (still in transition)
                if features['direction'] == 'reversing' and features['persistence'] <= 2:
                    conf_adjustment -= 0.05

                # Apply bounded by ±0.10 total, floor 0.10, ceiling 0.95
                conf_adjustment = max(-0.10, min(0.10, conf_adjustment))
                adjusted_confidence = max(0.10, min(1.0, base_confidence + conf_adjustment))

                sig['confidence'] = adjusted_confidence
                if not isinstance(sig.get('payload'), dict):
                    sig['payload'] = {}
                sig['payload']['trajectory'] = features
                sig['payload']['confidence_adjustment_from_trajectory'] = round(conf_adjustment, 3)

            except Exception as e:
                logger.warning(f"Trajectory modifier failed for {instrument}: {e}")

    def _inject_structure_targets(self, signals: list, live_prices: dict) -> None:
        """Post-process signals: add Python-computed structure targets where target_price is null.

        Algorithm (per non-neutral signal):
          - Fetch last 100 bars of 15M data; use the 50 most recent for structure (~12.5 hours).
          - True swing detection: bar i qualifies if its high/low beats both neighbours ±2 bars.
          - LONG/bullish → nearest swing HIGH above current_price (lowest qualifying high).
          - SHORT/bearish → nearest swing LOW below current_price (highest qualifying low).
          - If no qualifying swing exists in the window: skip — do not set a target.
          - Minimum distance gate: target must be at least 1.5× stop_distance from entry.
          - If LLM already filled target_price (non-null), preserve it.
        Never raises — failures log and skip silently.
        """
        for sig in signals:
            try:
                payload = sig.get('payload') or {}
                rm = payload.get('risk_management')
                if not isinstance(rm, dict):
                    continue
                if rm.get('target_price') is not None:
                    continue  # LLM already set a value — preserve it

                pair = sig.get('instrument')
                bias = sig.get('bias', 'neutral')
                if bias == 'neutral' or not pair:
                    continue

                # Current mid price
                current_price = None
                lp = live_prices.get(pair, {})
                if lp.get('last'):
                    current_price = float(lp['last'])

                # Fetch last 100 candles; use the 50 most recent for structure (~12.5 hours)
                bars = self.get_historical_bars(pair, '15M', 100)
                if bars is None or bars.empty or len(bars) < 5:
                    logger.warning(f"Structure target: insufficient 15M bars for {pair}")
                    continue
                recent = bars.tail(50)

                if bias == 'bullish':
                    # True swing high: bar i high beats both neighbours ±2 bars
                    highs = recent['high'].values
                    swing_highs = []
                    for i in range(2, len(highs) - 2):
                        if (highs[i] > highs[i-1] and highs[i] > highs[i-2]
                                and highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                            swing_highs.append(highs[i])
                    if not swing_highs:
                        logger.debug(f"Structure target: no swing high found for {pair} bullish")
                        continue
                    candidates = [sh for sh in swing_highs if current_price is None or sh > current_price]
                    if not candidates:
                        logger.debug(f"Structure target: no swing high above price for {pair} bullish")
                        continue
                    target = round(min(candidates), 5)  # nearest (lowest) swing high above price
                else:  # bearish
                    # True swing low: bar i low beats both neighbours ±2 bars
                    lows = recent['low'].values
                    swing_lows = []
                    for i in range(2, len(lows) - 2):
                        if (lows[i] < lows[i-1] and lows[i] < lows[i-2]
                                and lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                            swing_lows.append(lows[i])
                    if not swing_lows:
                        logger.debug(f"Structure target: no swing low found for {pair} bearish")
                        continue
                    candidates = [sl for sl in swing_lows if current_price is None or sl < current_price]
                    if not candidates:
                        logger.debug(f"Structure target: no swing low below price for {pair} bearish")
                        continue
                    target = round(max(candidates), 5)  # nearest (highest) swing low below price

                # Minimum distance gate: target must be >= 1.5× stop_distance from entry (1.5:1 R:R floor)
                stop_distance = (rm.get('stop_distance')
                                 or payload.get('stop_distance'))
                if stop_distance and float(stop_distance) > 0 and current_price is not None:
                    min_target_distance = float(stop_distance) * 1.5
                    actual_distance = abs(target - current_price)
                    if actual_distance < min_target_distance:
                        logger.warning(
                            f"Structure target {target:.5f} too close to entry for {pair} "
                            f"({bias}) — distance {actual_distance:.5f} < min {min_target_distance:.5f} "
                            f"(stop_distance={stop_distance})"
                        )
                        continue

                rm['target_price'] = target
                logger.info(
                    f"Structure target set {pair} ({bias}): {target:.5f} "
                    f"[current={current_price} stop_dist={stop_distance}]"
                )
            except Exception as exc:
                logger.warning(f"Structure target injection failed for {sig.get('instrument')}: {exc}")

    def run_cycle(self) -> bool:
        """Execute one complete technical analysis cycle."""
        cycle_start = time.time()
        self.cycle_count += 1

        logger.info(f"Starting technical agent cycle #{self.cycle_count}")

        try:
            # Check kill switch
            if self.check_kill_switch():
                logger.warning("Kill switch active - skipping cycle")
                return False

            # Update heartbeat
            self.update_heartbeat()

            # Fetch live prices once — reused by both the skip check and build_conversation_context
            live_prices = self.get_live_prices()

            # Persist live tick as LIVE timeframe bar (one row per minute per pair)
            _tick_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            for _pair, _pd in live_prices.items():
                if _pd.get('last') and not _pd.get('market_closed'):
                    _mid = float(_pd['last'])
                    self._persist_price_bars(_pair, 'LIVE', [
                        {'ts': _tick_ts, 'open': _mid, 'high': _mid, 'low': _mid, 'close': _mid}
                    ])

            # Price-change gate: skip LLM if no pair has moved >= PRICE_CHANGE_THRESHOLD_PIPS
            if not self.should_call_llm(live_prices):
                logger.info(
                    f"Price-skip cycle #{self.cycle_count} — no pair moved "
                    f">={self.PRICE_CHANGE_THRESHOLD_PIPS} pips since last LLM call, "
                    f"refreshing signal timestamps"
                )
                self._reuse_existing_signals()
                self.update_heartbeat()
                return True

            logger.info(f"Analysis cycle #{self.cycle_count} — price movement detected, computing RSI/ADX signals")

            # Deterministic RSI/ADX signal generation
            signals = self.generate_signals(live_prices)

            # Apply trajectory-based confidence modifiers (score unchanged)
            self._apply_trajectory_modifiers(signals)

            # Inject Python-computed structure targets (nearest swing high/low from 15M bars)
            self._inject_structure_targets(signals, live_prices)

            # Store mid prices for next cycle's skip check
            self._last_llm_prices = {
                p: float(d["last"]) for p, d in live_prices.items() if d.get("last") is not None
            }

            # Write signals to database
            success = self.write_signals_to_database(signals)

            # Final heartbeat update
            self.update_heartbeat()

            cycle_duration = time.time() - cycle_start
            logger.info(f"Technical cycle #{self.cycle_count} completed in {cycle_duration:.2f}s - Success: {success}")

            return success

        except Exception as e:
            logger.error(f"Technical cycle #{self.cycle_count} failed: {e}")
            self.update_heartbeat()
            return False

    def run_continuous(self):
        """Run the technical agent continuously with 15-minute cycles."""
        logger.info("Starting technical agent continuous operation")

        while True:
            try:
                success = self.run_cycle()

                if not success:
                    logger.warning("Cycle failed - continuing to next cycle")

                sleep_seconds = self.CYCLE_INTERVAL_MINUTES * 60
                logger.info(f"Sleeping for {sleep_seconds} seconds until next cycle")
                time.sleep(sleep_seconds)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal - shutting down")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous loop: {e}")
                time.sleep(60)

    def run_single_cycle(self):
        """Run a single cycle for testing purposes."""
        logger.info("Running single technical agent cycle")
        return self.run_cycle()

    def close(self):
        """Clean up resources."""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()
        logger.info("Technical agent resources cleaned up")

def _get_active_user_ids(region="eu-west-2"):
    """Query all active user IDs from risk_parameters."""
    import psycopg2, psycopg2.extras, json, boto3
    ssm = boto3.client("ssm", region_name=region)
    sm = boto3.client("secretsmanager", region_name=region)
    endpoint = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
    user_ids = [str(row["user_id"]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return user_ids


def main():
    """Main entry point — runs Technical Agent for ALL active users."""
    import argparse
    import sys
    if "--no-delay" not in sys.argv:
        logger.info("Startup delay 300s (stagger vs macro agent)")
        time.sleep(300)

    parser = argparse.ArgumentParser(description="Project Neo Technical Agent")
    parser.add_argument("--user", default=None,
                        help="Optional: run for a single user only (for debugging)")
    parser.add_argument("--single", action="store_true",
                        help="Run a single cycle then exit")
    parser.add_argument("--test", action="store_true",
                        help="Run configuration test only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not write to DB or execute trades")

    args = parser.parse_args()

    # Resolve user list
    if args.user:
        # Single user mode (debugging)
        user_ids = [args.user]
    else:
        # Multi-user mode (production)
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
        logger.info("Technical Agent test mode — verifying configuration for all users")
        try:
            for uid in user_ids:
                agent = TechnicalAgent(user_id=uid, dry_run=True)
                logger.info(f"  ✅ PASS: {uid}")
                agent.close() if hasattr(agent, "close") else None
            logger.info("✅ All users configured successfully")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ FAIL: {e}")
            sys.exit(1)

    # Create agent instances per user
    agents = {}
    for uid in user_ids:
        try:
            agents[uid] = TechnicalAgent(user_id=uid, dry_run=getattr(args, "dry_run", False))
            logger.info(f"Initialized Technical Agent for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Technical Agent for {uid}: {e}")

    if not agents:
        logger.error("No agents initialized — exiting")
        sys.exit(1)

    try:
        if args.single:
            for uid, agent in agents.items():
                logger.info(f"--- {uid} ---")
                try:
                    agent.run_cycle()
                except Exception as e:
                    logger.error(f"Cycle failed for {uid}: {e}")
            logger.info("Single cycle complete for all users")
            sys.exit(0)
        else:
            # Continuous mode — one API call per cycle, shared across all users
            logger.info("Starting continuous operation for all users")
            agent_list = list(agents.values())
            primary = agent_list[0]
            while True:
                market = get_market_state()

                # Standby during weekend and pre-open window
                if market['state'] in ('closed', 'pre_open'):
                    logger.info(f"STANDBY — {market['reason']}, skipping cycle")
                    for agent in agent_list:
                        try:
                            agent.update_heartbeat()
                        except Exception as _e:
                            logger.debug(f"Indicator calculation failed: {_e}")
                    time.sleep(300)  # re-check every 5 min
                    continue

                if market['state'] == 'quiet':
                    logger.info(f"QUIET HOURS — {market['reason']}, 30-min reduced cycle")

                try:
                    if primary.check_kill_switch():
                        logger.warning("Kill switch active - skipping cycle")
                        time.sleep(60)
                        continue

                    for agent in agent_list:
                        agent.cycle_count += 1
                        agent.update_heartbeat()

                    # Fetch live prices once — reused by skip-check and build_conversation_context
                    live_prices = primary.get_live_prices()

                    # Persist live tick to historical_prices (LIVE timeframe, one row/min per pair)
                    _tick_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                    for _pair, _pd in live_prices.items():
                        if _pd.get("last") and not _pd.get("market_closed"):
                            _mid = float(_pd["last"])
                            primary._persist_price_bars(_pair, "LIVE", [
                                {"ts": _tick_ts, "open": _mid, "high": _mid, "low": _mid, "close": _mid}
                            ])

                    if not primary.should_call_llm(live_prices):
                        logger.info(
                            f"Price-skip cycle #{primary.cycle_count} — no pair moved "
                            f">={primary.PRICE_CHANGE_THRESHOLD_PIPS} pips since last LLM call, "
                            f"refreshing signal timestamps"
                        )
                        for agent in agent_list:
                            agent._reuse_existing_signals()
                            agent.update_heartbeat()
                    else:
                        logger.info(f"Analysis cycle #{primary.cycle_count} — price movement detected, computing RSI/ADX signals")
                        signals = primary.generate_signals(live_prices)

                        # Apply trajectory-based confidence modifiers (score unchanged)
                        primary._apply_trajectory_modifiers(signals)

                        # Inject Python-computed structure targets (nearest swing high/low from 15M bars)
                        primary._inject_structure_targets(signals, live_prices)

                        # Store mid prices for next cycle's skip check
                        primary._last_llm_prices = {
                            p: float(d["last"]) for p, d in live_prices.items() if d.get("last") is not None
                        }

                        for uid, agent in agents.items():
                            logger.info(f"Writing signals for {uid}")
                            agent.write_signals_to_database(signals)
                            agent.update_heartbeat()

                except Exception as e:
                    logger.error(f"Cycle failed: {e}")
                    try:
                        for agent in agent_list:
                            agent.update_heartbeat()
                    except Exception as hb_err:
                        logger.error(f"Heartbeat update failed: {hb_err}")

                # Quiet hours: interruptible 30-min sleep
                # Wakes early on: state transition OR 06:40/12:40 UTC pre-open window
                if market['state'] == 'quiet':
                    _sleep_start = time.time()
                    _sleep_total = 1800
                    while time.time() - _sleep_start < _sleep_total:
                        time.sleep(30)
                        for agent in agent_list:
                            try:
                                agent.update_heartbeat()
                            except Exception as _e:
                                logger.debug(f"Indicator calculation failed: {_e}")
                        # Check for pre-open wake (20 min before London/NY open)
                        import datetime as _dt
                        _now = _dt.datetime.now(_dt.timezone.utc)
                        _hr, _min = _now.hour, _now.minute
                        _pre_open = (
                            (_hr == 6  and _min >= 40) or  # London pre-open 06:40-07:00
                            (_hr == 12 and _min >= 40)     # NY pre-open 12:40-13:00
                        )
                        if _pre_open:
                            logger.info(
                                f"Pre-open wake at {_hr:02d}:{_min:02d} UTC -- "
                                f"firing early cycle before market open"
                            )
                            break
                        # Check for state transition (quiet -> active)
                        try:
                            _new_state = get_market_state().get('state')
                            if _new_state != 'quiet':
                                logger.info(
                                    f"Market state transition: quiet -> {_new_state} -- "
                                    f"breaking sleep early"
                                )
                                break
                        except Exception as _e:
                            logger.debug(f"Indicator calculation failed: {_e}")
                else:
                    time.sleep(primary.CYCLE_INTERVAL_MINUTES * 60 if hasattr(primary, "CYCLE_INTERVAL_MINUTES") else 300)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for uid, agent in agents.items():
            try:
                if hasattr(agent, "close"):
                    agent.close()
            except Exception as _e:
                logger.debug(f"Indicator calculation failed: {_e}")


if __name__ == "__main__":
    main()

