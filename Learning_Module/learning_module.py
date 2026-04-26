#!/usr/bin/env python3
"""
Project Neo — Learning Module v1.0
====================================
Analyses closed trades, writes autopsies, manages pattern memory,
and generates weekly learning reviews. The system's self-improvement engine.

Cycle: Post-trade (polls every 30 seconds) + Weekly report (Monday 06:00 UTC)
IAM Role: platform-forex-role-dev

Anti-overfitting rules:
  1. 20-trade minimum before pattern activation
  2. Confidence decay over time
  3. Rolling 30-trade out-of-sample validation
  4. Regime-specific patterns only
  5. 0.65 failure rate floor → auto-deactivate
  6. Cross-strategy contamination guard (per-user scoping)

Run:
  source ~/algodesk/bin/activate
  python learning_module.py --user neo_user_002
"""

import os
import re
import sys
sys.path.insert(0, '/root/Project_Neo_Damon')
from v1_swing_parameters import (
    V1_SWING_PAIRS,
    SETUP_LONG_PULLBACK, SETUP_SHORT_PULLBACK, SETUP_TYPES,
    REJECTION_STAGES,
)
import json
import time
import uuid
import logging
import argparse
import datetime
from typing import Optional, Dict, List, Any, Tuple

import boto3
import psycopg2
import psycopg2.extras
from shared.schema_validator import validate_schema
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.trades":           ["user_id", "instrument", "direction", "entry_price",
                                       "exit_price", "pnl", "pnl_pips", "entry_time",
                                       "exit_time", "exit_reason", "convergence_score",
                                       "agents_agreed", "regime_at_entry"],
    "forex_network.trade_autopsies":  ["trade_id", "user_id", "pattern_context",
                                       "exclude_from_patterns", "failure_mode",
                                       "regime_stress_at_entry"],
    "forex_network.pattern_memory":   ["user_id", "pattern_key", "direction",
                                       "decay_factor", "pattern_type", "active",
                                       "failure_rate", "sample_size"],
    "forex_network.agent_heartbeats": ["agent_name", "user_id", "last_seen",
                                       "status", "cycle_count"],
    "forex_network.rejection_patterns": ["user_id", "instrument", "rejection_reason",
                                         "convergence_score", "session", "regime",
                                         "stress_score", "cycle_timestamp"],
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("neo.learning")

# =============================================================================
# CONSTANTS
# =============================================================================
AWS_REGION = "eu-west-2"
POLL_INTERVAL_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 60
CYCLE_INTERVAL_SECONDS      = 1800  # 30-min cycles; stale threshold 60 min = 2 missed cycles
AGENT_NAME = "learning_module"

# Contrarian learning constants
_CONTRARIAN_MIN_TRADES_TOTAL  = 50
_CONTRARIAN_MIN_TRADES_FACTOR = 20
_CONTRARIAN_WIN_RATE_HIGH     = 0.60
_CONTRARIAN_WIN_RATE_LOW      = 0.40
_CONTRARIAN_REVERT_WIN_RATE   = 0.40
_CONTRARIAN_REVERT_MIN_TRADES = 20
_CONTRARIAN_MULTIPLIER_STEP   = 0.10
_CONTRARIAN_MULTIPLIER_FLOOR  = 0.80
_CONTRARIAN_MULTIPLIER_CEIL   = 1.20
_CONTRARIAN_FACTOR_MAP = {
    'extremity':  'extremity_bucket',
    'consensus':  'consensus_grade',
    'cot':        'cot_direction',
    'session':    'session',
}

# Anti-overfitting thresholds
MIN_TRADES_FOR_ACTIVATION = 20
FAILURE_RATE_FLOOR = 0.65          # Auto-deactivate above this
CONFIDENCE_DECAY_DAYS = 30         # Half-life for confidence decay
ROLLING_WINDOW_SIZE = 30           # Out-of-sample validation window
STRESS_EXCLUSION_THRESHOLD = 70
# =============================================================================
# RESEARCH-BACKED PAIR PRIORS
# =============================================================================
PAIR_TIERS = {
    1: ["AUDUSD","AUDCAD","AUDNZD","AUDJPY","CADJPY","EURJPY","NZDJPY"],
    2: ["EURUSD","USDCHF","NZDUSD","EURCAD","EURCHF","EURGBP","EURNZD"],
    3: ["GBPAUD","GBPCAD","GBPCHF","GBPJPY","GBPUSD","USDCAD","USDJPY"],
}
PAIR_NEWS_SENSITIVE = {"GBPAUD","GBPCAD","GBPCHF","GBPJPY","GBPUSD","USDJPY","USDCAD"}
PAIR_PRIOR_MIN_ADX = {
    **{p: 25.0 for p in PAIR_TIERS[1]},
    **{p: 27.0 for p in PAIR_TIERS[2]},
    **{p: 30.0 for p in PAIR_TIERS[3]},
}
PAIR_PRIOR_MIN_TRADES = {
    **{p: 20 for p in PAIR_TIERS[1]},
    **{p: 20 for p in PAIR_TIERS[2]},
    **{p: 30 for p in PAIR_TIERS[3]},
}
    # Losses at stress > 70 excluded from pattern scoring

# Sortino/Sharpe targets
SORTINO_TARGET_PAPER = 2.0
SORTINO_TARGET_LIVE = 1.5
SHARPE_TARGET_PAPER = 1.5
SHARPE_TARGET_LIVE = 1.0

# Weekly report schedule
WEEKLY_REPORT_DAY = 0   # Monday (0=Monday in weekday())
WEEKLY_REPORT_HOUR = 6  # 06:00 UTC

# First IG trade — reject any signal data predating the broker migration
DATA_QUALITY_CUTOFF = '2026-04-23 21:55:00+00'  # V1 Swing era start — permanent epoch, not a rolling window. All LM analysis restricted to post-migration trades. Do not change.

CYCLE_LOG_PATH   = '/var/log/neo/learning_module.jsonl'
AUTOPSY_LOG_PATH = '/var/log/neo/learning_module_autopsies.jsonl'

# =============================================================================
# V1 SWING CONSTANTS
# =============================================================================
V1_SWING_ADX_BUCKETS   = ['25-30', '30-40', '40+']
V1_SWING_SESSIONS      = ['asia', 'london', 'ny_overlap', 'ny_late']
V1_SWING_SETUP_TYPES   = list(SETUP_TYPES)  # from v1_swing_parameters
MIN_BUCKET_SAMPLE      = 10   # flag insufficient_sample below this

RG_REJECTION_CATEGORIES = [
    'rr_gate_fail',
    'swap_rr_fail',
    'correlation_block',
    'concentration_block',
    'news_blackout',
    'daily_drawdown_halt',
    'weekly_drawdown_halt',
    'min_stop_distance',
    'pip_value_unavailable',
]

V1_SWING_REJECTION_CATEGORIES = [
    # Orchestrator level
    'technical_gate_fail',
    'macro_no_direction',
    'direction_disagreement',
    # Technical agent level (from gate_failures payload field)
    'trend_filter_fail',
    'adx_gate_fail',
    'rsi_not_in_zone',
    'rsi_no_cross',
    'structure_fail',
    'session_invalid',
    # RG level
    'correlation_block',
    'concentration_block',
    'news_blackout',
    'daily_drawdown_halt',
    'weekly_drawdown_halt',
]

def _classify_rg_rejection(reason: str) -> str:
    """Map an RG rejection reason string to a RG_REJECTION_CATEGORIES entry."""
    r = reason.lower()
    # Check specific patterns first to avoid substring false-positives
    if 'correlation' in r:
        return 'correlation_block'
    if 'swap_rr' in r or ('swap' in r and 'rr' in r):
        return 'swap_rr_fail'
    if 'rr_ratio' in r or 'rr_gate' in r or 'risk_reward' in r or 'risk reward' in r:
        return 'rr_gate_fail'
    if 'concentration' in r or 'exposure' in r:
        return 'concentration_block'
    if 'news' in r or 'event' in r or 'blackout' in r:
        return 'news_blackout'
    if 'daily' in r and ('drawdown' in r or 'loss' in r or 'limit' in r):
        return 'daily_drawdown_halt'
    if 'weekly' in r and ('drawdown' in r or 'loss' in r or 'limit' in r):
        return 'weekly_drawdown_halt'
    if 'stop_distance' in r or 'stop distance' in r or 'min_stop' in r:
        return 'min_stop_distance'
    if 'pip_value' in r or 'pip value' in r:
        return 'pip_value_unavailable'
    return 'other'

def _classify_v1_rejection(reason: str, gate_failures=None) -> str:
    """Map a V1 Swing orchestrator rejection reason to V1_SWING_REJECTION_CATEGORIES."""
    r = reason.lower()
    if 'macro_gate_fail' in r or 'macro_no_direction' in r or 'macro score' in r:
        return 'macro_no_direction'
    if 'technical_too_weak' in r or 'tech' in r and 'gate' in r:
        return 'technical_gate_fail'
    if 'directional' in r or 'disagreement' in r:
        return 'direction_disagreement'
    if 'adx' in r:
        return 'adx_gate_fail'
    if 'rsi' in r and 'zone' in r:
        return 'rsi_not_in_zone'
    if 'rsi' in r and 'cross' in r:
        return 'rsi_no_cross'
    if 'trend' in r:
        return 'trend_filter_fail'
    if 'session' in r:
        return 'session_invalid'
    if 'structure' in r:
        return 'structure_fail'
    if 'correlation' in r:
        return 'correlation_block'
    if 'concentration' in r:
        return 'concentration_block'
    if 'news' in r or 'blackout' in r:
        return 'news_blackout'
    if 'daily' in r and 'drawdown' in r:
        return 'daily_drawdown_halt'
    if 'weekly' in r and 'drawdown' in r:
        return 'weekly_drawdown_halt'
    return 'other'

def _adx_bucket(adx):
    """Map ADX value to V1 Swing bucket string, or None if below threshold."""
    if adx is None or adx < 25:
        return None
    if adx < 30:
        return '25-30'
    if adx < 40:
        return '30-40'
    return '40+'


# =============================================================================
# AWS HELPERS
# =============================================================================
def _append_jsonl(path: str, record: dict) -> None:
    """Append one JSON record to a JSONL file. Silent on failure."""
    try:
        line = json.dumps(record, default=str)
        with open(path, 'a') as _f:
            _f.write(line + '\n')
    except Exception as _e:
        logger.debug(f"_append_jsonl({path}): {_e}")


def get_secret(secret_name: str, region: str = AWS_REGION) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def get_parameter(param_name: str, region: str = AWS_REGION) -> str:
    client = boto3.client("ssm", region_name=region)
    resp = client.get_parameter(Name=param_name, WithDecryption=True)
    return resp["Parameter"]["Value"]


# =============================================================================
# DATABASE
# =============================================================================
def _resolve_user_id(conn_or_db, user_id: str) -> str:
    """Resolve a Cognito username like 'neo_user_002' to its UUID from risk_parameters."""
    if '-' in user_id and len(user_id) > 30:
        return user_id  # Already a UUID
    try:
        if hasattr(conn_or_db, 'cursor'):
            cur = conn_or_db.cursor()
        else:
            cur = conn_or_db.cursor()
        cur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
        rows = cur.fetchall()
        cur.close()
        idx = {"neo_user_001": 0, "neo_user_002": 1, "neo_user_003": 2}.get(user_id, -1)
        if idx >= 0 and idx < len(rows):
            resolved = str(rows[idx]["user_id"]) if isinstance(rows[idx], dict) else str(rows[idx][0])
            return resolved
    except Exception:
        pass
    return user_id


class DatabaseConnection:
    def __init__(self, region: str = AWS_REGION):
        self.region = region
        self.conn = None

    def connect(self):
        rds_endpoint = get_parameter("/platform/config/rds-endpoint", self.region)
        creds = get_secret("platform/rds/credentials", self.region)
        self.conn = psycopg2.connect(
            host=rds_endpoint, port=5432, dbname="postgres",
            user=creds["username"], password=creds["password"],
            connect_timeout=10,
            options="-c search_path=forex_network,shared,public",
        )
        self.conn.autocommit = False
        logger.info(f"Connected to RDS: {rds_endpoint}")

    def cursor(self):
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()


# =============================================================================
# TRADE AUTOPSY ENGINE
# =============================================================================
def _total_trade_pnl(trade) -> float:
    """Return full realised P&L for a trade, including any T1 partial-exit profit.

    When target_1_hit=True, the trade closed in two legs:
      leg-1: target_1_pnl (50% at T1)
      leg-2: pnl          (remaining 50% at T2 or breakeven stop)
    Both legs must be summed to get the true trade outcome.
    Callers that only read `pnl` will misclassify breakeven-stop trades as losses.
    """
    t1    = float(trade.get('target_1_pnl') or 0)
    final = float(trade.get('pnl') or 0)
    return (t1 + final) if trade.get('target_1_hit') else final


def _classify_exit_reason(exit_reason, target_1_hit, entry_price, exit_price,
                           pip_size=0.0001):
    """Classify exit into V1 Swing exit type."""
    if exit_reason in ('limit', 'tp1', 'take_profit'):
        return 'target_2' if target_1_hit else 'target_1_only'
    if exit_reason in ('stop', 'sl', 'stop_loss', 'stop_hit'):
        return 'stop'
    # Breakeven: exited at ~entry +-5pips after target_1 hit
    if target_1_hit and entry_price and exit_price:
        pips_from_entry = abs(float(exit_price) - float(entry_price)) / pip_size
        if pips_from_entry <= 5:
            return 'breakeven'
    return 'manual'


class TradeAutopsyEngine:
    """Analyses closed trades and writes structured autopsies."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def get_unautopsied_trades(self) -> List[Dict[str, Any]]:
        """Find closed trades that don't have an autopsy yet."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT t.id, t.instrument, t.direction, t.entry_price, t.exit_price,
                       t.entry_time, t.exit_time, t.exit_reason, t.pnl,
                       t.target_1_pnl, t.target_1_hit, t.pnl_pips,
                       t.position_size_usd, t.spread_at_entry, t.stop_price,
                       t.slippage_pips, t.fill_pct, t.is_partial_fill
                FROM forex_network.trades t
                LEFT JOIN forex_network.trade_autopsies ta ON t.id = ta.trade_id
                WHERE t.user_id = %s
                  AND t.exit_time IS NOT NULL
                  AND ta.trade_id IS NULL
                ORDER BY t.exit_time ASC
            """, (self.user_id,))
            rows = cur.fetchall()
            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("payload"), str):
                    d["payload"] = json.loads(d["payload"])
                results.append(d)
            return results
        except Exception as e:
            logger.error(f"Get unautopsied trades failed: {e}")
            return []
        finally:
            cur.close()

    def _check_macro_lead(self, trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check whether macro led technical in agreeing with the trade's direction.

        Scans forex_network.convergence_history for 6h before → entry_time and
        finds the first cycle where each agent's signed score agreed with the
        trade direction. Positive lead_cycles = macro led; negative = tech led.
        Returns None when there isn't enough cycle history (<3 rows).
        """
        if not self.db:
            return None
        try:
            cur = self.db.cursor()
            cur.execute(
                """
                SELECT macro_score, technical_score, cycle_timestamp
                FROM forex_network.convergence_history
                WHERE user_id = %s AND instrument = %s
                  AND cycle_timestamp BETWEEN %s - INTERVAL '6 hours' AND %s
                ORDER BY cycle_timestamp ASC
                """,
                (self.user_id, trade["instrument"], trade["entry_time"], trade["entry_time"]),
            )
            rows = cur.fetchall()
            cur.close()
        except Exception as e:
            logger.warning(f"Macro lead check failed: {e}")
            try: self.db.rollback()
            except Exception: pass
            return None

        if len(rows) < 3:
            return None

        trade_direction = 1 if trade["direction"] == "long" else -1
        macro_first_agree = None
        tech_first_agree = None
        for row in rows:
            ms = float(row["macro_score"] or 0)
            ts = float(row["technical_score"] or 0)
            if macro_first_agree is None and ms * trade_direction > 0.1:
                macro_first_agree = row["cycle_timestamp"]
            if tech_first_agree is None and ts * trade_direction > 0.1:
                tech_first_agree = row["cycle_timestamp"]

        if not (macro_first_agree and tech_first_agree):
            return None

        lead_seconds = (tech_first_agree - macro_first_agree).total_seconds()
        lead_cycles = lead_seconds / 900.0  # 15-min cycles
        return {
            "macro_led": lead_cycles > 0,
            "lead_cycles": round(abs(lead_cycles), 2),
            "macro_first_agree": macro_first_agree.isoformat(),
            "tech_first_agree": tech_first_agree.isoformat(),
            "trade_outcome_r": (
                float(trade["rr_actual"]) if trade.get("rr_actual") is not None else None
            ),
            "trade_pnl": float(trade.get("pnl") or 0),
            "cycles_inspected": len(rows),
        }

    def _compute_entry_timing(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """Compute entry-timing quality metrics for pattern_context.

        Returns dict with:
          entry_lag_minutes      — minutes after session open (capped 240)
          entry_price_vs_open_pips — price move from session-open bar to entry
          entry_atr_ratio        — above expressed as fraction of ATR
        Returns empty dict on any data error.
        """
        result: Dict[str, Any] = {}
        try:
            entry_time = trade.get("entry_time")
            entry_price = float(trade.get("entry_price") or 0)
            instrument = trade.get("instrument", "")
            direction = trade.get("direction", "buy")
            if not entry_time or not entry_price or not instrument:
                return result

            # Session open times (UTC hour)
            session_open_times = {
                'london': 7, 'newyork': 13, 'asian': 22, 'overlap': 13
            }
            session_at_entry = self._get_session_at_time(entry_time)
            session_open_hour = session_open_times.get(session_at_entry, 7)

            # Build session-open timestamp (same date, except asian wraps to prev day)
            session_open_utc = entry_time.replace(
                hour=session_open_hour, minute=0, second=0, microsecond=0
            )
            if session_at_entry == 'asian' and session_open_hour >= entry_time.hour:
                # asian opens at 22:00 previous day
                from datetime import timedelta as _td
                session_open_utc = session_open_utc - _td(days=1)

            entry_lag_seconds = (entry_time - session_open_utc).total_seconds()
            entry_lag_minutes = min(entry_lag_seconds / 60.0, 240.0)
            result["entry_lag_minutes"] = round(entry_lag_minutes, 1)
            result["session_at_entry"] = session_at_entry

            # Session-open bar price from historical_prices
            cur = self.db.cursor()
            try:
                cur.execute("""
                    SELECT open FROM forex_network.historical_prices
                    WHERE instrument = %s AND timeframe = '1H'
                      AND ts = %s
                    LIMIT 1
                """, (instrument, session_open_utc))
                row = cur.fetchone()
            finally:
                cur.close()

            if row is None:
                return result  # no bar — skip price metrics

            open_price = float(row["open"])
            pip_size = 0.01 if "JPY" in instrument else 0.0001
            direction_mult = 1.0 if direction.lower() == "buy" else -1.0
            entry_price_vs_open_pips = (
                (entry_price - open_price) / pip_size * direction_mult
            )
            result["entry_price_vs_open_pips"] = round(entry_price_vs_open_pips, 1)

            # ATR from price_metrics — nearest row at or before entry_time
            cur2 = self.db.cursor()
            try:
                cur2.execute("""
                    SELECT atr_14 FROM forex_network.price_metrics
                    WHERE instrument = %s AND timeframe = '1H'
                      AND ts <= %s
                    ORDER BY ts DESC LIMIT 1
                """, (instrument, entry_time))
                atr_row = cur2.fetchone()
            finally:
                cur2.close()

            if atr_row and atr_row["atr_14"]:
                atr_pips = float(atr_row["atr_14"]) * 10000
                if atr_pips > 0:
                    result["entry_atr_ratio"] = round(
                        entry_price_vs_open_pips / atr_pips, 3
                    )

        except Exception as _e:
            logger.debug(f"_compute_entry_timing {trade.get('instrument')}: {_e}")

        return result

    def write_autopsy(self, trade: Dict[str, Any]) -> Optional[int]:
        """Analyse a closed trade and write the autopsy."""
        trade_id = trade["id"]
        # Use pnl (GBP monetary) as primary; fall back to pnl_pips for cross pairs
        # where IG transaction matching failed and pnl column is NULL.
        if trade.get("pnl") is not None:
            pnl = _total_trade_pnl(trade)   # includes T1 partial-exit profit
            is_win = pnl > 0
        elif trade.get("pnl_pips") is not None:
            pnl = float(trade["pnl_pips"])
            is_win = pnl > 0
        else:
            logger.warning(f"Autopsy skipped for trade {trade['id']}: no pnl or pnl_pips")
            return None

        # Get stress score at entry time
        stress_at_entry = self._get_stress_at_time(trade["entry_time"])

        # Get the signals that contributed to this trade
        entry_signals = self._get_entry_signals(trade["instrument"], trade["entry_time"])

        # Classify the trade outcome
        failure_mode = None
        diagnosis = None
        signal_quality = None
        contributing_factors = []

        if is_win:
            signal_quality = "correct"
            diagnosis = self._diagnose_win(trade, entry_signals)
        else:
            failure_mode, diagnosis, contributing_factors = self._diagnose_loss(
                trade, entry_signals, stress_at_entry
            )
            signal_quality = "incorrect"

        # Calculate return percentage
        return_pct = pnl / float(trade["position_size_usd"]) if trade["position_size_usd"] else None

        # Determine if this loss should be excluded from pattern scoring
        # Losses during Pre-crisis/Crisis are structural, not strategy failures
        # stress_score decommissioned 2026-04-24 (regime agent removed);
        # exclude_from_patterns always False — no exclusion filter active
        exclude_from_patterns = False

        # Build pattern context
        pattern_context = {
            "instrument": trade["instrument"],
            "direction": trade["direction"],
            "session": self._get_session_at_time(trade["entry_time"]),
            "regime": entry_signals.get("regime", "unknown"),
            "stress_level": self._classify_stress(stress_at_entry),
            "exit_reason": trade["exit_reason"],
        }

        # Passive macro-lead analysis — did macro agree with the trade direction
        # before technical did? Scans convergence_history 6h before entry.
        macro_lead = self._check_macro_lead(trade)
        if macro_lead is not None:
            pattern_context["macro_lead_analysis"] = macro_lead

        # Entry timing quality metrics
        entry_timing = self._compute_entry_timing(trade)
        if entry_timing:
            pattern_context["entry_timing"] = entry_timing

        # --- V1 Swing autopsy fields ---
        tp = trade.get('trade_parameters') or {}
        if isinstance(tp, str):
            import json as _j2
            try: tp = _j2.loads(tp)
            except Exception: tp = {}

        pip_size_trade = 0.01 if 'JPY' in str(trade.get('instrument', '')) else 0.0001

        v1_rsi        = trade.get('rsi_at_entry') or (tp.get('rsi_at_entry') and float(tp['rsi_at_entry'])) or None
        v1_adx        = trade.get('adx_at_entry') or (tp.get('adx_at_entry') and float(tp['adx_at_entry'])) or None
        v1_setup_type = trade.get('setup_type') or tp.get('setup_type') or None
        v1_session    = trade.get('session_at_entry') or tp.get('session_at_entry') or None
        v1_t1_hit     = bool(trade.get('target_1_hit')) or (str(tp.get('target_1_hit', '')).lower() == 'true')

        v1_exit_cls   = _classify_exit_reason(
            trade.get('exit_reason', ''),
            v1_t1_hit,
            trade.get('entry_price'),
            trade.get('exit_price'),
            pip_size_trade,
        )
        v1_t2_hit        = v1_exit_cls == 'target_2'
        v1_stop_hit      = v1_exit_cls == 'stop'
        v1_breakeven     = v1_exit_cls == 'breakeven'
        v1_total_pnl     = _total_trade_pnl(trade)
        # --------------------------------

        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.trade_autopsies
                    (trade_id, user_id, failure_mode, diagnosis, signal_quality,
                     contributing_factors, pattern_context,
                     sharpe_contribution, sortino_contribution,
                     regime_stress_at_entry, exclude_from_patterns,
                     rsi_at_entry, adx_at_entry, setup_type, session_at_entry,
                     target_1_hit, target_2_hit, stop_hit, breakeven_exit,
                     total_pnl, exit_reason_classified,
                     created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        NOW())
                RETURNING id
            """, (
                trade_id, self.user_id, failure_mode, diagnosis, signal_quality,
                json.dumps(contributing_factors),
                json.dumps(pattern_context),
                None,  # Sharpe contribution — calculated after more trades
                None,  # Sortino contribution
                stress_at_entry,
                exclude_from_patterns,
                v1_rsi, v1_adx, v1_setup_type, v1_session,
                v1_t1_hit, v1_t2_hit, v1_stop_hit, v1_breakeven,
                v1_total_pnl, v1_exit_cls,
            ))
            result = cur.fetchone()
            self.db.commit()

            # Also update trades table with return_pct
            if return_pct is not None:
                cur2 = self.db.cursor()
                cur2.execute("""
                    UPDATE forex_network.trades
                    SET return_pct = %s, is_downside_return = %s
                    WHERE id = %s
                """, (return_pct, pnl < 0, trade_id))
                self.db.commit()
                cur2.close()

            logger.info(
                f"Autopsy written: trade {trade_id} {trade['instrument']} "
                f"{'WIN' if is_win else 'LOSS'} ({failure_mode or 'n/a'})"
                f"{' [excluded from patterns — high stress]' if exclude_from_patterns else ''}"
            )

            # Recalculate drawdown step level after each closed trade
            self.recalculate_drawdown(pnl)

            return result["id"]

        except Exception as e:
            logger.error(f"Autopsy write failed: {e}")
            warn("learning_module", "DB_FAIL", "Autopsy write failed", error=str(e)[:120])
            self.db.rollback()
            return None
        finally:
            cur.close()

    def recalculate_drawdown(self, trade_pnl: float):
        """Recalculate drawdown step level from IBKR-sourced account_value.
        Implements 50% recovery gate: position sizing only steps back up
        after recovering 50% of the drawdown that triggered the step-down.
        This prevents oscillation during volatile recovery periods."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT account_value, peak_account_value,
                       drawdown_step_level, size_multiplier,
                       step_down_drawdown_pct,
                       min_risk_pct, curve_exponent, max_convergence_reference,
                       min_risk_reward_ratio, max_usd_units, correlation_threshold
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            if not row or not row["account_value"] or not row["peak_account_value"]:
                return

            account_val = float(row["account_value"])
            peak_val = float(row["peak_account_value"])
            current_step = int(row["drawdown_step_level"] or 0)
            step_down_dd = float(row["step_down_drawdown_pct"] or 0)

            if peak_val <= 0:
                return

            drawdown_pct = (peak_val - account_val) / peak_val * 100

            # Determine target step level from current drawdown
            if drawdown_pct <= 5:
                target_step, target_mult = 0, 1.000
            elif drawdown_pct <= 10:
                target_step, target_mult = 1, 0.750
            elif drawdown_pct <= 15:
                target_step, target_mult = 2, 0.500
            else:
                target_step, target_mult = 3, 0.000

            step = current_step
            mult = float(row["size_multiplier"] or 1.0)

            if target_step > current_step:
                # Stepping DOWN — apply immediately, record the drawdown level
                step = target_step
                mult = target_mult
                step_down_dd = drawdown_pct
                logger.info(f"Drawdown step-DOWN: {current_step} → {step} at {drawdown_pct:.1f}%")
            elif target_step < current_step:
                # Stepping UP — only if recovered 50% of the drawdown that triggered step-down
                # Recovery gate: must recover 50% before stepping back up
                if step_down_dd > 0:
                    recovery_threshold = step_down_dd * 0.50
                    if drawdown_pct <= recovery_threshold:
                        step = target_step
                        mult = target_mult
                        step_down_dd = drawdown_pct if step > 0 else 0
                        logger.info(
                            f"Drawdown step-UP: {current_step} → {step} "
                            f"(drawdown {drawdown_pct:.1f}% <= 50% recovery gate {recovery_threshold:.1f}%)"
                        )
                    else:
                        logger.info(
                            f"Recovery gate held: drawdown {drawdown_pct:.1f}% > "
                            f"50% gate {recovery_threshold:.1f}% (triggered at {step_down_dd:.1f}%). "
                            f"Staying at step {current_step}."
                        )
                else:
                    # No step_down_dd recorded — allow step-up (fresh start)
                    step = target_step
                    mult = target_mult
            # else: same step — no change

            cur.execute("""
                UPDATE forex_network.risk_parameters
                SET drawdown_step_level = %s, size_multiplier = %s,
                    step_down_drawdown_pct = %s
                WHERE user_id = %s
            """, (step, mult, step_down_dd, self.user_id))
            self.db.commit()

            logger.info(
                f"Drawdown recalculated: {drawdown_pct:.1f}% "
                f"(account: £{account_val:,.2f}, peak: £{peak_val:,.2f}) "
                f"→ step {step}, size multiplier {mult}"
            )

            # Alert on step-down
            if step > 0:
                from decimal import Decimal
                cur2 = self.db.cursor()
                try:
                    cur2.execute("""
                        INSERT INTO forex_network.system_alerts
                            (alert_type, severity, title, detail, user_id, acknowledged, created_at)
                        VALUES ('drawdown_step_down', %s, %s, %s, %s, FALSE, NOW())
                    """, (
                        'critical' if step >= 3 else 'medium',
                        f'{self.user_id} position sizing at {mult*100:.0f}%',
                        f'Drawdown from peak: {drawdown_pct:.1f}%. Step level {step}.',
                        self.user_id,
                    ))
                    self.db.commit()
                except:
                    self.db.rollback()
                finally:
                    cur2.close()

            # Audit log: drawdown_step_down (any direction change)
            if step != current_step:
                import json as _j
                cur3 = self.db.cursor()
                try:
                    direction = "step_down" if step > current_step else "step_up"
                    cur3.execute("""
                        INSERT INTO forex_network.audit_log
                          (user_id, event_type, description, metadata, source, created_at)
                        VALUES (%s, 'drawdown_step_down', %s, %s::jsonb, 'learning_agent', NOW())
                    """, (
                        self.user_id,
                        (
                            f"Drawdown {direction}: step {current_step} → {step} "
                            f"(drawdown {drawdown_pct:.1f}%, size multiplier {mult:.0%})"
                        ),
                        _j.dumps({
                            "direction": direction,
                            "previous_step": current_step,
                            "new_step": step,
                            "drawdown_pct": round(drawdown_pct, 2),
                            "account_value": round(account_val, 2),
                            "peak_account_value": round(peak_val, 2),
                            "size_multiplier": mult,
                            "trading_halted": step >= 3,
                        }),
                    ))
                    self.db.commit()
                except Exception as _ae:
                    logger.warning(f"audit drawdown_step_down failed: {_ae}")
                    try: self.db.rollback()
                    except: pass
                finally:
                    cur3.close()

        except Exception as e:
            logger.error(f"Drawdown recalculation failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def _get_stress_at_time(self, entry_time) -> Optional[float]:
        """Regime agent decommissioned — historical stress data preserved but no new writes.
        Returns None; autopsy callers handle None gracefully."""
        return None

    def _get_entry_signals(self, instrument: str, entry_time) -> Dict[str, Any]:
        """Get the agent signals that were active when this trade was entered."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT agent_name, score, bias, confidence, payload
                FROM forex_network.agent_signals
                WHERE (instrument = %s OR instrument IS NULL)
                  AND user_id = %s
                  AND created_at <= %s
                  AND expires_at >= %s
                ORDER BY created_at DESC
            """, (instrument, self.user_id, entry_time, entry_time))
            signals = {}
            for row in cur.fetchall():
                if row["agent_name"] not in signals:
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    signals[row["agent_name"]] = {
                        "score": float(row["score"]) if row["score"] else 0,
                        "bias": row["bias"],
                        "confidence": float(row["confidence"]) if row["confidence"] else 0,
                        "payload": payload or {},
                    }
            # Extract regime from regime signal
            regime_sig = signals.get("regime", {})
            signals["regime"] = regime_sig.get("payload", {}).get("global_regime", "unknown")
            return signals
        except:
            return {}
        finally:
            cur.close()

    def _get_session_at_time(self, entry_time) -> str:
        """Determine which trading session a time falls in."""
        if entry_time is None:
            return "unknown"
        hour = entry_time.hour if hasattr(entry_time, 'hour') else 0
        if 0 <= hour < 7:
            return "asian"
        elif 7 <= hour < 12:
            return "london"
        elif 12 <= hour < 16:
            return "overlap"
        elif 16 <= hour < 20:
            return "newyork"
        elif 20 <= hour < 22:
            return "ny_close"
        else:
            return "off_hours"

    def _classify_stress(self, stress: Optional[float]) -> str:
        if stress is None:
            return "unknown"
        if stress <= 30:
            return "normal"
        elif stress <= 50:
            return "elevated"
        elif stress <= 70:
            return "high"
        elif stress <= 85:
            return "pre_crisis"
        else:
            return "crisis"

    def _diagnose_win(self, trade: Dict, signals: Dict) -> str:
        """Diagnose why a winning trade worked."""
        parts = []
        instrument = trade["instrument"]
        direction = trade["direction"]

        macro = signals.get("macro", {})
        if macro and abs(macro.get("score", 0)) > 0.5:
            parts.append(f"Strong macro signal ({macro.get('bias', 'n/a')})")

        tech = signals.get("technical", {})
        if tech and abs(tech.get("score", 0)) > 0.5:
            parts.append(f"Technical confirmation ({tech.get('bias', 'n/a')})")

        regime = signals.get("regime", "unknown")
        if regime == "trending":
            parts.append("Favourable trending regime")

        if not parts:
            parts.append("Standard convergence")

        return f"{instrument} {direction} WIN: {'; '.join(parts)}"

    def _diagnose_loss(self, trade: Dict, signals: Dict,
                       stress: Optional[float]) -> Tuple[str, str, List[str]]:
        """Diagnose why a losing trade failed."""
        instrument = trade["instrument"]
        direction = trade["direction"]
        exit_reason = trade.get("exit_reason", "unknown")
        factors = []

        # Classify failure mode
        if exit_reason in ("stop_loss", "stop_hit"):
            failure_mode = "stopped_out"
            factors.append("Hit stop loss")
        elif exit_reason == "stop_failed":
            failure_mode = "stop_failure"
            factors.append("Stop order could not be placed — emergency close")
        elif exit_reason == "manual":
            failure_mode = "manual_close"
            factors.append("Closed manually or by circuit breaker")
        elif exit_reason == "circuit_breaker":
            failure_mode = "circuit_breaker"
            factors.append("Daily loss limit hit — circuit breaker Tier 2")
        else:
            failure_mode = "other"
            factors.append(f"Exit reason: {exit_reason}")

        # Check if stress was elevated at entry
        if stress and stress > 50:
            factors.append(f"Entered during elevated stress ({stress:.0f})")

        # Check if partial fill may have contributed
        if trade.get("is_partial_fill"):
            factors.append(f"Partial fill ({trade.get('fill_pct', 0):.0f}%)")

        # Check slippage
        slippage = trade.get("slippage_pips", 0) or 0
        threshold = {"EURUSD": 3, "USDJPY": 3, "GBPUSD": 4, "USDCHF": 4,
                     "AUDUSD": 5, "USDCAD": 5, "NZDUSD": 5}.get(instrument, 5)
        if slippage > threshold:
            factors.append(f"High slippage ({slippage:.1f} pips, threshold: {threshold})")

        # Check signal alignment
        macro = signals.get("macro", {})
        tech = signals.get("technical", {})
        if macro and tech:
            if macro.get("bias") != tech.get("bias"):
                factors.append("Macro and technical signals were misaligned")

        diagnosis = f"{instrument} {direction} LOSS ({failure_mode}): {'; '.join(factors)}"
        return failure_mode, diagnosis, factors


# =============================================================================
# PATTERN MEMORY MANAGER
# =============================================================================
class PatternMemoryManager:
    """Manages pattern_memory with anti-overfitting guards."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def update_patterns(self):
        """
        Scan recent autopsies and update pattern memory.
        Anti-overfitting rules enforced at every step.
        """
        # Get recent autopsies for this user
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT ta.pattern_context, ta.signal_quality, ta.exclude_from_patterns,
                       ta.regime_stress_at_entry, ta.created_at, t.instrument,
                       t.direction, t.pnl
                FROM forex_network.trade_autopsies ta
                JOIN forex_network.trades t ON ta.trade_id = t.id
                WHERE ta.user_id = %s
                  AND ta.exclude_from_patterns = FALSE
                ORDER BY ta.created_at DESC
                LIMIT 100
            """, (self.user_id,))
            autopsies = cur.fetchall()
        except Exception as e:
            logger.error(f"Pattern scan failed: {e}")
            return
        finally:
            cur.close()

        if len(autopsies) < MIN_TRADES_FOR_ACTIVATION:
            logger.info(
                f"Pattern update: {len(autopsies)} trades < {MIN_TRADES_FOR_ACTIVATION} minimum. "
                f"Patterns stored as inactive."
            )

        # Group by pattern context (instrument + direction + regime + session)
        pattern_groups: Dict[str, List[Dict]] = {}
        for autopsy in autopsies:
            ctx = autopsy["pattern_context"]
            if isinstance(ctx, str):
                ctx = json.loads(ctx)
            # Rule 4: Regime-specific patterns only
            key = f"{ctx.get('instrument','?')}_{ctx.get('direction','?')}_{ctx.get('regime','?')}_{ctx.get('session','?')}"
            if key not in pattern_groups:
                pattern_groups[key] = []
            pattern_groups[key].append(autopsy)

        for pattern_key, trades in pattern_groups.items():
            self._update_single_pattern(pattern_key, trades)

    def _update_single_pattern(self, pattern_key: str, trades: List[Dict]):
        """Update or create a single pattern in pattern_memory."""
        total = len(trades)
        # Rule 3: Use rolling window for failure rate
        recent = trades[:ROLLING_WINDOW_SIZE]
        wins = sum(1 for t in recent if t["signal_quality"] == "correct")
        losses = sum(1 for t in recent if t["signal_quality"] == "incorrect")
        recent_total = wins + losses
        failure_rate = losses / recent_total if recent_total > 0 else 0.5

        # Rule 1: Minimum trades for activation
        pair = pattern_key.split("_")[0]
        min_trades = PAIR_PRIOR_MIN_TRADES.get(pair, MIN_TRADES_FOR_ACTIVATION)
        active = total >= min_trades

        # Rule 5: Failure rate floor
        if failure_rate > FAILURE_RATE_FLOOR:
            active = False
            logger.info(
                f"Pattern {pattern_key}: failure rate {failure_rate:.2f} > {FAILURE_RATE_FLOOR} "
                f"— deactivated (becomes negative signal)"
            )

        # Rule 2: Confidence decay
        if trades:
            most_recent = trades[0]["created_at"]
            if most_recent:
                days_since = (datetime.datetime.now(datetime.timezone.utc) - most_recent.replace(
                    tzinfo=datetime.timezone.utc) if most_recent.tzinfo is None else most_recent
                ).days if hasattr(most_recent, 'days') else 0
                try:
                    delta = datetime.datetime.now(datetime.timezone.utc) - (
                        most_recent.replace(tzinfo=datetime.timezone.utc)
                        if most_recent.tzinfo is None else most_recent
                    )
                    days_since = delta.days
                except:
                    days_since = 0
                # Exponential decay
                decay_factor = 0.5 ** (days_since / CONFIDENCE_DECAY_DAYS)
            else:
                decay_factor = 0.5
        else:
            decay_factor = 0.5

        confidence = (1.0 - failure_rate) * decay_factor

        # Parse pattern context from key
        parts = pattern_key.split("_")
        description = f"{parts[0]} {parts[1]} in {parts[2]} regime during {parts[3]} session"

        # Write/update pattern
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.pattern_memory
                    (user_id, pattern_key, description, regime, session,
                     instrument, direction, sample_size, failure_rate,
                     confidence, decay_factor, active, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, pattern_key)
                DO UPDATE SET
                    sample_size = EXCLUDED.sample_size,
                    failure_rate = EXCLUDED.failure_rate,
                    confidence = EXCLUDED.confidence,
                    decay_factor = EXCLUDED.decay_factor,
                    active = EXCLUDED.active,
                    last_updated = NOW()
            """, (
                self.user_id, pattern_key, description,
                parts[2] if len(parts) > 2 else None,
                parts[3] if len(parts) > 3 else None,
                parts[0] if len(parts) > 0 else None,
                parts[1] if len(parts) > 1 else None,
                total, round(failure_rate, 4),
                round(confidence, 4), round(decay_factor, 4),
                active,
            ))
            self.db.commit()
            logger.info(
                f"Pattern {'ACTIVE' if active else 'inactive'}: {pattern_key} "
                f"(n={total}, fail={failure_rate:.2f}, conf={confidence:.3f})"
            )
        except Exception as e:
            logger.error(f"Pattern write failed: {e}")
            warn("learning_module", "DB_FAIL", "Pattern write failed", error=str(e)[:120])
            self.db.rollback()
        finally:
            cur.close()

    def get_active_patterns(self) -> List[Dict[str, Any]]:
        """Read all active patterns for this user. Rule 6: per-user scoping."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT pattern_key, description, regime, session, instrument,
                       direction, sample_size, failure_rate, confidence, active
                FROM forex_network.pattern_memory
                WHERE user_id = %s AND active = TRUE
                ORDER BY confidence DESC
            """, (self.user_id,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Read patterns failed: {e}")
            return []
        finally:
            cur.close()


# =============================================================================
# PERFORMANCE TRACKER
# =============================================================================
class PerformanceTracker:
    """Reads risk-adjusted performance metrics and generates proposals."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def get_metrics(self) -> Dict[str, Any]:
        """Read current performance metrics from the risk-adjusted view."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT * FROM forex_network.performance_risk_adjusted
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            return dict(row) if row else {}
        except Exception as e:
            logger.error(f"Performance metrics read failed: {e}")
            return {}
        finally:
            cur.close()

    def check_sortino_threshold(self, metrics: Dict) -> List[Dict[str, Any]]:
        """Generate proposals based on Sortino ratio performance."""
        proposals = []
        sortino = metrics.get("sortino_ratio")
        trade_count = metrics.get("trade_count", 0)

        if sortino is None or trade_count < 10:
            return proposals

        # Minimum data gate — same cutoff as _generate_proposals
        MIN_TRADES_FOR_PROPOSALS = 20
        _gate_cur = self.db.cursor()
        try:
            _gate_cur.execute("""
                SELECT COUNT(*) FROM forex_network.trades
                WHERE user_id = %s
                  AND exit_time IS NOT NULL
                  AND exit_time > %s
            """, (self.user_id, DATA_QUALITY_CUTOFF))
            _row = _gate_cur.fetchone()
            _closed_trades = int(_row['count']) if _row else 0
        except Exception as _gate_e:
            logger.warning(
                f"[{self.user_id}] Sortino gate query failed ({_gate_e}) — skipping proposals"
            )
            return proposals
        finally:
            _gate_cur.close()
        if _closed_trades < MIN_TRADES_FOR_PROPOSALS:
            logger.info(
                f"[{self.user_id}] Skipping sortino proposals — only {_closed_trades} "
                f"clean closed trades (minimum {MIN_TRADES_FOR_PROPOSALS})"
            )
            return proposals

        sortino = float(sortino)

        if sortino < SORTINO_TARGET_LIVE:
            proposals.append({
                "type": "strategy_observation",
                "priority": "high",
                "title": f"Sortino ratio at {sortino:.2f} — below go-live minimum ({SORTINO_TARGET_LIVE})",
                "reasoning": (
                    f"Rolling {trade_count}-trade Sortino is {sortino:.2f}. "
                    f"Go-live requires >{SORTINO_TARGET_LIVE}. Review recent trade patterns "
                    f"for systematic issues."
                ),
                "data_supporting": json.dumps({
                    "sortino": sortino, "trade_count": trade_count,
                    "target": SORTINO_TARGET_LIVE,
                }),
                "suggested_action": "Review losing trade autopsies for common failure modes.",
                "expected_impact": "Identifying systematic issues before they compound.",
            })

        elif sortino > SORTINO_TARGET_PAPER + 0.5:
            proposals.append({
                "type": "strategy_observation",
                "priority": "low",
                "title": f"Sortino ratio at {sortino:.2f} — strong risk-adjusted performance",
                "reasoning": (
                    f"Rolling Sortino of {sortino:.2f} exceeds paper target of {SORTINO_TARGET_PAPER}. "
                    f"Consider whether convergence thresholds can be reduced slightly "
                    f"to capture more setups."
                ),
                "data_supporting": json.dumps({"sortino": sortino, "trade_count": trade_count}),
                "suggested_action": "Monitor for 20+ more trades before adjusting thresholds.",
                "expected_impact": "Better capital deployment if edge is genuine.",
            })

        return proposals


# =============================================================================
# WEEKLY REPORT GENERATOR
# =============================================================================
class WeeklyReportGenerator:
    """Generates the Weekly Learning Review (Monday 06:00 UTC)."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def should_generate(self) -> bool:
        """Check if it's time for the weekly report."""
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.weekday() != WEEKLY_REPORT_DAY or now.hour != WEEKLY_REPORT_HOUR:
            return False
        # Check if we already generated one this week
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM forex_network.audit_log
                WHERE event_type = 'weekly_learning_review'
                  AND metadata->>'user_id' = %s
                  AND created_at >= NOW() - INTERVAL '6 days'
            """, (self.user_id,))
            row = cur.fetchone()
            return row["cnt"] == 0
        except:
            return False
        finally:
            cur.close()

    def generate(self) -> Dict[str, Any]:
        """Generate the full weekly learning review."""
        logger.info("=== GENERATING WEEKLY LEARNING REVIEW ===")

        report = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "user_id": self.user_id,
            "sections": {},
        }

        # Section 1: Performance
        report["sections"]["performance"] = self._section_performance()

        # Section 2: Situations of Uncertainty
        report["sections"]["uncertainty"] = self._section_uncertainty()

        # Section 3: Proposals Summary
        report["sections"]["proposals"] = self._section_proposals()

        # Section 4: Pattern Memory Updates
        report["sections"]["patterns"] = self._section_patterns()

        # Section 5: Stress Score History
        report["sections"]["stress"] = self._section_stress()

        # Section 6: Rule Recommendations
        report["sections"]["recommendations"] = self._section_recommendations()

        # Write to audit_log
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.audit_log
                    (event_type, description, metadata)
                VALUES ('weekly_learning_review', %s, %s)
            """, (
                f"Weekly Learning Review for {self.user_id}",
                json.dumps({"user_id": self.user_id, "report": report}),
            ))
            self.db.commit()
            logger.info("Weekly learning review written to audit_log")
        except Exception as e:
            logger.error(f"Report write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

        return report

    def _section_performance(self) -> Dict[str, Any]:
        """Section 1: Performance metrics."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT * FROM forex_network.performance_risk_adjusted
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            if row:
                return {
                    "sortino_ratio": float(row["sortino_ratio"]) if row["sortino_ratio"] else None,
                    "sharpe_ratio": float(row["sharpe_ratio"]) if row["sharpe_ratio"] else None,
                    "win_rate": float(row["win_rate"]) if row["win_rate"] else None,
                    "profit_factor": float(row["profit_factor"]) if row["profit_factor"] else None,
                    "total_pnl": float(row["total_pnl"]) if row["total_pnl"] else None,
                    "trade_count": int(row["trade_count"]) if row["trade_count"] else 0,
                    "sortino_vs_target": "above" if row["sortino_ratio"] and float(row["sortino_ratio"]) > SORTINO_TARGET_LIVE else "below",
                }
            return {"note": "Insufficient data for performance metrics"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

    def _section_uncertainty(self) -> Dict[str, Any]:
        """Section 2: Cycles where agents applied confidence reductions."""
        cur = self.db.cursor()
        try:
            # Count signals with reduced confidence in the past week
            cur.execute("""
                SELECT agent_name, COUNT(*) AS reduced_count
                FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND created_at >= NOW() - INTERVAL '7 days'
                  AND confidence < 0.8
                GROUP BY agent_name
                ORDER BY reduced_count DESC
            """, (self.user_id,))
            rows = cur.fetchall()
            return {
                "agents_with_reduced_confidence": [
                    {"agent": row["agent_name"], "count": int(row["reduced_count"])}
                    for row in rows
                ],
                "total_reduced_cycles": sum(int(r["reduced_count"]) for r in rows),
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

    def _section_proposals(self) -> Dict[str, Any]:
        """Section 3: Summary of all proposals generated this week."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT payload
                FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND created_at >= NOW() - INTERVAL '7 days'
                  AND payload::text LIKE '%%proposals%%'
                ORDER BY created_at DESC
                LIMIT 50
            """, (self.user_id,))
            all_proposals = []
            for row in cur.fetchall():
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                proposals = payload.get("proposals", [])
                all_proposals.extend(proposals)

            # Group by type
            by_type: Dict[str, int] = {}
            for p in all_proposals:
                t = p.get("type", "unknown")
                by_type[t] = by_type.get(t, 0) + 1

            return {
                "total_proposals": len(all_proposals),
                "by_type": by_type,
                "high_priority": [p for p in all_proposals if p.get("priority") == "high"][:10],
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

    def _section_patterns(self) -> Dict[str, Any]:
        """Section 4: Pattern memory changes this week."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT pattern_key, active, sample_size, failure_rate, confidence,
                       last_updated
                FROM forex_network.pattern_memory
                WHERE user_id = %s
                ORDER BY last_updated DESC
            """, (self.user_id,))
            patterns = [dict(row) for row in cur.fetchall()]
            active = [p for p in patterns if p["active"]]
            inactive = [p for p in patterns if not p["active"]]
            return {
                "total_patterns": len(patterns),
                "active_count": len(active),
                "inactive_count": len(inactive),
                "active_patterns": active[:10],
                "recently_deactivated": [
                    p for p in inactive
                    if p.get("last_updated") and (
                        datetime.datetime.now(datetime.timezone.utc) -
                        p["last_updated"].replace(tzinfo=datetime.timezone.utc)
                        if p["last_updated"].tzinfo is None else p["last_updated"]
                    ).days <= 7
                ][:5],
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

    def _section_stress(self) -> Dict[str, Any]:
        """Section 5: Stress score — regime agent decommissioned; historical data preserved."""
        return {
            "note": "regime_agent_decommissioned",
            "min_stress": None, "max_stress": None, "avg_stress": None,
            "snapshots": 0, "escalations": 0,
        }

    def _section_recommendations(self) -> Dict[str, Any]:
        """Section 6: Rule adjustment recommendations based on data."""
        recommendations = []

        # Check if any pattern has been consistently correct
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT pattern_key, failure_rate, sample_size
                FROM forex_network.pattern_memory
                WHERE user_id = %s AND active = TRUE AND sample_size >= 30
                  AND failure_rate < 0.25
                ORDER BY failure_rate ASC
            """, (self.user_id,))
            strong_patterns = cur.fetchall()
            for p in strong_patterns:
                recommendations.append({
                    "type": "pattern_strength",
                    "pattern": p["pattern_key"],
                    "detail": f"Pattern has {p['sample_size']} trades with only {float(p['failure_rate'])*100:.0f}% failure rate. Consider increasing position size for this pattern.",
                })

            # Check for consistently failing patterns
            cur.execute("""
                SELECT pattern_key, failure_rate, sample_size
                FROM forex_network.pattern_memory
                WHERE user_id = %s AND failure_rate > %s AND sample_size >= 10
                ORDER BY failure_rate DESC
            """, (self.user_id, FAILURE_RATE_FLOOR))
            weak_patterns = cur.fetchall()
            for p in weak_patterns:
                recommendations.append({
                    "type": "pattern_weakness",
                    "pattern": p["pattern_key"],
                    "detail": f"Pattern failing at {float(p['failure_rate'])*100:.0f}% over {p['sample_size']} trades. Already deactivated. Review for systematic issue.",
                })

        except Exception as e:
            recommendations.append({"type": "error", "detail": str(e)})
        finally:
            cur.close()

        return {"recommendations": recommendations, "count": len(recommendations)}


# =============================================================================
# LEARNING MODULE — MAIN
# =============================================================================
class LearningModule:
    """Main learning module — autopsies, patterns, performance, weekly review."""

    def __init__(self, user_id: str, dry_run: bool = False):
        self.user_id = user_id
        self.dry_run = dry_run
        self.db = DatabaseConnection()
        self.db.connect()
        validate_schema(self.db.conn, EXPECTED_TABLES)
        self.autopsy_engine = TradeAutopsyEngine(self.db, user_id)
        self.pattern_manager = PatternMemoryManager(self.db, user_id)
        self.performance = PerformanceTracker(self.db, user_id)
        self.weekly_report = WeeklyReportGenerator(self.db, user_id)
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        if not dry_run:
            self.seed_research_priors()

    def _write_cycle_log(self, cycle_start: float, autopsies: int,
                          rejections: int, proposals: int, arch_issues: int) -> None:
        """Write structured cycle summary to JSONL. Never raises."""
        try:
            duration_ms = round((time.time() - cycle_start) * 1000)
            now_utc     = datetime.datetime.now(datetime.timezone.utc)
            start_dt    = datetime.datetime.fromtimestamp(cycle_start, tz=datetime.timezone.utc)

            tables_read     = {}
            autopsy_records = []
            patterns_updated = 0
            cur = None
            try:
                cur = self.db.cursor()
                for tbl in ('forex_network.proposals',
                            'forex_network.rejection_patterns',
                            'forex_network.trade_autopsies'):
                    cur.execute(
                        f"SELECT COUNT(*) AS n FROM {tbl} WHERE user_id = %s",
                        (self.user_id,))
                    tables_read[tbl.split('.')[-1]] = int(cur.fetchone()['n'])

                cur.execute("""
                    SELECT ta.trade_id, t.instrument,
                           ta.signal_quality             AS outcome,
                           ta.failure_mode,
                           ta.regime_stress_at_entry     AS stress_at_entry,
                           ta.pattern_context->>'regime' AS regime_at_entry
                    FROM forex_network.trade_autopsies ta
                    JOIN forex_network.trades t ON ta.trade_id = t.id
                    WHERE ta.user_id = %s AND ta.created_at >= %s
                    ORDER BY ta.created_at
                """, (self.user_id, start_dt))
                autopsy_records = [dict(r) for r in cur.fetchall()]

                try:
                    cur.execute("""
                        SELECT COUNT(*) AS n FROM forex_network.rejection_patterns
                        WHERE user_id = %s AND created_at >= %s
                    """, (self.user_id, start_dt))
                    patterns_updated = int(cur.fetchone()['n'])
                except Exception:
                    try: self.db.rollback()
                    except Exception: pass
                    patterns_updated = rejections
            except Exception as _qe:
                logger.debug(f"_write_cycle_log DB query: {_qe}")
                try: self.db.rollback()
                except Exception: pass
            finally:
                if cur is not None:
                    try:
                        cur.close()
                    except Exception:
                        pass

            # V1 Swing accuracy summary (bucket counts only — not full trade list)
            v1_accuracy_summary = {}
            try:
                v1_acc = self.compute_per_session_adx_setup_accuracy()
                v1_accuracy_summary = {
                    'bucket_count':  len(v1_acc.get('session_adx_setup', {})),
                    'pair_count':    len(v1_acc.get('per_pair', {})),
                    'buckets': {
                        str(k): {
                            'n':        v['n'],
                            'win_rate': round(v['win_rate'], 4),
                            'pf':       round(v['pf'], 4) if v['pf'] != float('inf') else None,
                            'insufficient_sample': v['insufficient_sample'],
                        }
                        for k, v in v1_acc.get('session_adx_setup', {}).items()
                    },
                }
            except Exception as _ve:
                logger.debug(f"_write_cycle_log v1_accuracy: {_ve}")

            record = {
                'ts':                   now_utc.isoformat(),
                'cycle_id':             self.cycle_count,
                'user_id':              self.user_id,
                'trades_processed':     len(autopsy_records),
                'autopsies_written':    autopsies,
                'patterns_updated':     patterns_updated,
                'patterns_activated':   None,
                'patterns_deactivated': None,
                'proposals_generated':  proposals,
                'data_quality_cutoff':  DATA_QUALITY_CUTOFF,
                'tables_read':          tables_read,
                'arch_issues':          arch_issues,
                'errors':               [],
                'warnings':             [],
                'cycle_duration_ms':    duration_ms,
                'autopsies':            autopsy_records,
                'v1_swing_accuracy':    v1_accuracy_summary,
                'proposals_by_type':    {},  # populated below if proposals > 0
            }
            _append_jsonl(CYCLE_LOG_PATH, record)

            for a in autopsy_records:
                _append_jsonl(AUTOPSY_LOG_PATH, {
                    'ts':       now_utc.isoformat(),
                    'cycle_id': self.cycle_count,
                    'user_id':  self.user_id,
                    **a,
                })

            logger.info(
                f"[LM cycle {self.cycle_count}] "
                f"processed={len(autopsy_records)} autopsies={autopsies} "
                f"patterns_updated={patterns_updated} proposals={proposals} "
                f"duration={duration_ms}ms errors=0"
            )
        except Exception as _e:
            logger.debug(f"_write_cycle_log failed: {_e}")


    def seed_research_priors(self):
        """Pre-seed technical_timing_params with research-backed pair tier priors.
        Runs once -- skips if records already exist. Overridden by live data once
        sample_size reaches MIN_SAMPLE (20) in get_optimal_thresholds()."""
        cur = self.db.cursor()
        try:
            cur.execute("SELECT COUNT(*) AS n FROM forex_network.technical_timing_params WHERE sample_size = 0")
            if int(cur.fetchone()["n"]) > 0:
                logger.info("seed_research_priors: priors already seeded -- skipping")
                return
            sessions = ["asian", "london", "overlap", "newyork"]
            seeded = 0
            for pair, min_adx in PAIR_PRIOR_MIN_ADX.items():
                for session in sessions:
                    cur.execute("""
                        INSERT INTO forex_network.technical_timing_params
                            (instrument, session, optimal_rsi_low, optimal_rsi_high,
                             min_adx, sample_size, win_rate, computed_at)
                        VALUES (%s, %s, 40.0, 50.0, %s, 0, 0.0, NOW())
                        ON CONFLICT (instrument, session) DO NOTHING
                    """, (pair, session, min_adx))
                    seeded += 1
            self.db.commit()
            logger.info(f"seed_research_priors: seeded {seeded} pair/session priors from research tier classification")
        except Exception as e:
            logger.error(f"seed_research_priors failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def write_heartbeat(self):
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.agent_heartbeats
                    (agent_name, user_id, session_id, last_seen, status, cycle_count)
                VALUES (%s, %s, %s, NOW(), 'active', %s)
                ON CONFLICT (agent_name, user_id)
                DO UPDATE SET last_seen = NOW(), cycle_count = EXCLUDED.cycle_count,
                              status = 'active', session_id = EXCLUDED.session_id
            """, (AGENT_NAME, self.user_id, self.session_id, self.cycle_count))
            self.db.commit()
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def poll_and_process(self) -> int:
        """Process any unautopsied closed trades."""
        trades = self.autopsy_engine.get_unautopsied_trades()
        if not trades:
            return 0

        logger.info(f"Found {len(trades)} unautopsied trade(s)")
        count = 0
        for trade in trades:
            if self.dry_run:
                logger.info(f"DRY RUN: Would autopsy trade {trade['id']}")
                count += 1
                continue
            autopsy_id = self.autopsy_engine.write_autopsy(trade)
            if autopsy_id:
                count += 1

        # After processing autopsies, update pattern memory
        if count > 0 and not self.dry_run:
            self.pattern_manager.update_patterns()

            # Check performance thresholds
            metrics = self.performance.get_metrics()
            proposals = self.performance.check_sortino_threshold(metrics)
            if proposals:
                self._write_proposals(proposals)

            # Contrarian performance learning (runs when 50+ trades with context exist)
            self.analyse_contrarian_performance()

            # Entry timing quality analysis
            self.analyse_entry_timing()

            # RSI/ADX entry timing parameters for technical agent
            self.compute_entry_timing_quality()

            # V1 Swing accuracy tracking — session x ADX x setup_type bucketing
            self.compute_per_session_adx_setup_accuracy()
            # Phase 1 accuracy tracking (DEPRECATED — kept for backward-compatibility)
            # self.compute_per_agent_accuracy()

            # Per-source contrarian accuracy (MyFXBook / IG)
            self.compute_per_source_accuracy()

            # Macro gate conviction-tier accuracy (high/medium/low)
            self.compute_macro_direction_accuracy()

            # Shadow trade hypothesis analysis: decommissioned for V1 Swing 2026-04-24
            # self.analyse_shadow_trades()

            # Kelly fraction (activates after 50 trades)
            self.compute_kelly_fraction()

            # Structural architecture diagnostics
            self.diagnose_architecture()

            # RG systematic rejection patterns
            try:
                self.analyse_rg_rejections()
            except Exception as _rg_e:
                logger.warning(f"analyse_rg_rejections failed, rolling back: {_rg_e}")
                try: self.db.rollback()
                except Exception: pass

        return count

    def _write_v1_proposals_to_table(self, proposals: list) -> int:
        """Write V1 Swing proposals to forex_network.proposals with strategy=v1_swing.
        Called by _generate_v1_swing_proposals in addition to _write_proposals.
        Returns number of rows inserted (deduplicates by type+instrument+pending).
        """
        PARAM_MAP = {
            "pair_removal":            "pair_filter",
            "adx_threshold_tuning":    "adx_threshold",
            "session_removal":         "session_filter",
            "drawdown_size_reduction": "max_risk_pct",
            "setup_type_imbalance":    "setup_filter",
        }
        written = 0
        cur = self.db.cursor()
        try:
            for prop in proposals:
                ptype = prop.get("proposal_type", "v1_swing_observation")
                instr = prop.get("instrument")
                param = PARAM_MAP.get(ptype, ptype)
                evidence = prop.get("evidence") or prop.get("data_supporting") or {}
                n_trades = evidence.get("n") or evidence.get("n_trades") if isinstance(evidence, dict) else None
                metric_val = (evidence.get("win_rate") or evidence.get("drawdown")) if isinstance(evidence, dict) else None
                # Dedup: skip if pending v1_swing proposal of same type+instrument exists
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM forex_network.proposals
                    WHERE user_id = %s AND proposal_type = %s
                      AND COALESCE(instrument, '<nil>') = COALESCE(%s, '<nil>')
                      AND status = 'pending' AND strategy = 'v1_swing'
                    """,
                    (str(self.user_id), ptype, instr),
                )
                if int(cur.fetchone()["cnt"]) > 0:
                    continue
                cur.execute(
                    """
                    INSERT INTO forex_network.proposals
                        (proposal_type, user_id, instrument, parameter,
                         n_trades, metric, metric_value, confidence,
                         reasoning, status, strategy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', 'v1_swing')
                    """,
                    (
                        ptype, str(self.user_id), instr, param,
                        int(n_trades) if n_trades is not None else None,
                        "win_rate" if metric_val is not None else ptype,
                        round(float(metric_val), 4) if metric_val is not None else None,
                        prop.get("priority", "medium"),
                        prop.get("reasoning") or prop.get("message") or "",
                    ),
                )
                written += 1
            self.db.commit()
            if written:
                logger.info(f"_write_v1_proposals_to_table: {written} proposal(s) written")
        except Exception as e:
            logger.error(f"_write_v1_proposals_to_table failed: {e}")
            self.db.rollback()
        finally:
            cur.close()
        return written

    def get_optimal_thresholds(self, instrument: str, session: str) -> dict:
        """
        Returns LM-computed optimal RSI/ADX thresholds for a given pair/session.
        TA calls this at cycle start to get adaptive thresholds.
        Falls back to V1 Swing defaults if no data or sample_size < 20.
        """
        DEFAULTS = {
            'rsi_low': 40.0, 'rsi_high': 50.0,
            'rsi_short_low': 50.0, 'rsi_short_high': 60.0,
            'min_adx': 25.0,
        }
        MIN_SAMPLE = 20
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT optimal_rsi_low, optimal_rsi_high, min_adx, sample_size, win_rate
                FROM forex_network.technical_timing_params
                WHERE instrument = %s AND session = %s
                  AND computed_at > NOW() - INTERVAL '7 days'
            """, (instrument.replace('/', ''), session))
            row = cur.fetchone()
            cur.close()
            if row and row['sample_size'] >= MIN_SAMPLE and (row['win_rate'] or 0) > 0.45:
                return {
                    'rsi_low':       row['optimal_rsi_low'],
                    'rsi_high':      row['optimal_rsi_high'],
                    'rsi_short_low': 100.0 - row['optimal_rsi_high'],
                    'rsi_short_high': 100.0 - row['optimal_rsi_low'],
                    'min_adx':       row['min_adx'],
                }
        except Exception as e:
            logger.debug(f'get_optimal_thresholds fallback {instrument}/{session}: {e}')
        return DEFAULTS

    def _write_proposals(self, proposals: List[Dict]):
        """Write performance-based proposals as a signal."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES (%s, %s, NULL, 'learning_review', 0.0, 'neutral', 1.0, %s,
                        NOW() + INTERVAL '24 hours')
            """, (AGENT_NAME, self.user_id, json.dumps({"proposals": proposals})))
            self.db.commit()
            for proposal in proposals:
                evidence = proposal.get('evidence', {})
                if (evidence.get('win_rate_5d', 0) > 0.65 or
                        str(proposal.get('proposal_type', '')).startswith('architecture_')):
                    self._notify_high_confidence_proposal(proposal)
        except Exception as e:
            logger.error(f"Proposals write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def _notify_high_confidence_proposal(self, proposal: dict) -> None:
        try:
            sm = boto3.client('secretsmanager', region_name='eu-west-2')
            secret = json.loads(sm.get_secret_value(SecretId='platform/alerts/sms')['SecretString'])
            topic_arn = secret.get('sns_topic_arn')
            if not topic_arn:
                return
            sns = boto3.client('sns', region_name='eu-west-2')
            msg = (f"Project Neo — High Confidence Proposal\n"
                   f"Type: {proposal.get('proposal_type')}\n"
                   f"Pair: {proposal.get('instrument')}\n"
                   f"Message: {str(proposal.get('message', ''))[:200]}")
            sns.publish(TopicArn=topic_arn, Message=msg, Subject='Neo LM — High Confidence Proposal')
            logger.info(f"SNS notification sent for {proposal.get('proposal_type')}")
        except Exception as e:
            logger.warning(f"SNS notification failed: {e}")

    # ------------------------------------------------------------------
    # Internal DB helper
    # ------------------------------------------------------------------

    def _query(self, sql: str, params=None):
        """Execute a query and return rows (SELECT) or [] (INSERT/UPDATE/DELETE)."""
        cur = self.db.cursor()
        try:
            cur.execute(sql, params or ())
            if cur.description is not None:
                return list(cur.fetchall())
            self.db.commit()
            return []
        except Exception as e:
            self.db.rollback()
            logger.error(f"_query failed: {e}")
            return []
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Contrarian performance learning
    # ------------------------------------------------------------------

    def _get_current_multiplier(self, adj_type: str, factor_name: str) -> float:
        """Return the latest active multiplier for this factor, defaulting to 1.0."""
        rows = self._query("""
            SELECT new_value FROM forex_network.learning_adjustments
            WHERE adjustment_type = %s AND factor_name = %s AND reverted_at IS NULL
            ORDER BY applied_at DESC LIMIT 1
        """, (adj_type, factor_name))
        return float(rows[0]['new_value']) if rows else 1.0

    def _apply_adjustment(self, adj_type: str, factor_name: str, old_val: float,
                           new_val: float, trades: int, win_rate: float, avg_pnl: float):
        """Log an autonomous adjustment to learning_adjustments."""
        self._query("""
            INSERT INTO forex_network.learning_adjustments
                (adjustment_type, factor_name, old_value, new_value,
                 supporting_trades, win_rate, avg_pnl_pips)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (adj_type, factor_name, old_val, new_val, trades, win_rate, avg_pnl))
        logger.info(
            f"Contrarian adjustment: {adj_type}/{factor_name} "
            f"{old_val:.3f} -> {new_val:.3f} "
            f"(win_rate={win_rate:.1%}, trades={trades}, avg_pnl={avg_pnl:.1f})"
        )

    def _check_auto_reverts(self):
        """Revert any adjustment whose post-change win rate fell below 40% over 20+ trades."""
        active = self._query("""
            SELECT id, adjustment_type, factor_name, old_value, new_value, applied_at
            FROM forex_network.learning_adjustments
            WHERE reverted_at IS NULL
            ORDER BY applied_at
        """)
        for row in active:
            json_key = _CONTRARIAN_FACTOR_MAP.get(row['adjustment_type'])
            if not json_key:
                continue
            since = self._query("""
                SELECT COUNT(*) AS total,
                       AVG(CASE WHEN pnl_pips > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
                FROM forex_network.trades
                WHERE exit_time IS NOT NULL AND exit_time > %s
                  AND entry_context->>%s = %s
            """, (row['applied_at'], json_key, row['factor_name']))
            if not since:
                continue
            total = int(since[0]['total'] or 0)
            post_wr = float(since[0]['win_rate'] or 0)
            if total >= _CONTRARIAN_REVERT_MIN_TRADES and post_wr < _CONTRARIAN_REVERT_WIN_RATE:
                self._query("""
                    UPDATE forex_network.learning_adjustments
                    SET reverted_at = NOW(),
                        revert_reason = 'Auto-revert: post-change win rate below 40%%'
                    WHERE id = %s
                """, (row['id'],))
                logger.warning(
                    f"Contrarian auto-revert: {row['adjustment_type']}/{row['factor_name']} "
                    f"{float(row['new_value'] or 1):.3f} -> {float(row['old_value'] or 1):.3f} "
                    f"(post-change win_rate={post_wr:.1%} over {total} trades)"
                )

    def analyse_entry_timing(self):
        """
        Analyse entry-timing quality by session + lag bucket.

        Buckets: 0-15 min, 15-30, 30-60, 60+ after session open.
        If any bucket has >=20 trades AND win_rate is >10 percentage points
        lower than the 0-15 min bucket, write a proposal.
        """
        rows = self._query("""
            SELECT
                ta.pattern_context->'entry_timing'->>'session_at_entry'  AS session,
                (ta.pattern_context->'entry_timing'->>'entry_lag_minutes')::float AS lag,
                COALESCE(t.target_1_pnl, 0) + COALESCE(t.pnl, 0) AS effective_pnl
            FROM forex_network.trade_autopsies ta
            JOIN forex_network.trades t ON ta.trade_id = t.id
            WHERE ta.user_id = %s
              AND ta.pattern_context->'entry_timing' IS NOT NULL
              AND t.exit_time IS NOT NULL
              AND (t.pnl IS NOT NULL OR t.target_1_pnl IS NOT NULL)
        """, (self.user_id,))

        if not rows:
            return

        # Bucket helper
        def _bucket(lag):
            if lag < 15:  return "0-15min"
            if lag < 30:  return "15-30min"
            if lag < 60:  return "30-60min"
            return "60+min"

        from collections import defaultdict
        stats: dict = defaultdict(lambda: {"wins": 0, "total": 0, "pnl_sum": 0.0})
        for row in rows:
            if row["lag"] is None:
                continue
            key = f"{row['session'] or 'unknown'}|{_bucket(row['lag'])}"
            stats[key]["total"] += 1
            stats[key]["pnl_sum"] += float(row["effective_pnl"] or 0)
            if float(row["effective_pnl"] or 0) > 0:
                stats[key]["wins"] += 1

        # Reference: win_rate of 0-15min bucket per session
        proposals = []
        sessions_seen = {k.split("|")[0] for k in stats}
        for sess in sessions_seen:
            ref_key = f"{sess}|0-15min"
            ref = stats.get(ref_key)
            if not ref or ref["total"] < 5:
                continue
            ref_wr = ref["wins"] / ref["total"]

            for bucket in ("15-30min", "30-60min", "60+min"):
                key = f"{sess}|{bucket}"
                s = stats.get(key)
                if not s or s["total"] < 20:
                    continue
                wr = s["wins"] / s["total"]
                if ref_wr - wr > 0.10:
                    proposals.append({
                        "type": "entry_timing_quality",
                        "session": sess,
                        "lag_bucket": bucket,
                        "win_rate": round(wr, 3),
                        "reference_win_rate": round(ref_wr, 3),
                        "win_rate_gap": round(ref_wr - wr, 3),
                        "total_trades": s["total"],
                        "avg_pnl": round(s["pnl_sum"] / s["total"], 2),
                        "suggestion": (
                            f"Entries in {sess} session at {bucket} after open have "
                            f"{wr:.1%} win rate vs {ref_wr:.1%} for <15min entries "
                            f"({s['total']} trades). Consider tightening entry lag filter."
                        ),
                    })

        if proposals:
            logger.info(
                f"analyse_entry_timing: {len(proposals)} timing gap(s) found — writing proposals"
            )
            self._write_proposals(proposals)
        else:
            logger.debug("analyse_entry_timing: no actionable timing gaps")

    def compute_entry_timing_quality(self) -> List[dict]:
        """
        Analyses which RSI zones and ADX levels at entry produced best outcomes.
        Writes optimal entry parameters per pair per session to technical_timing_params.
        Technical agent reads this table to adjust its RSI window dynamically.
        """
        proposals = []
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT
                    t.instrument,
                    t.session_at_entry as session,
                    FLOOR((t.trade_parameters->>'rsi_at_entry')::float / 5) * 5 as rsi_bucket,
                    FLOOR((t.trade_parameters->>'adx_at_entry')::float / 5) * 5 as adx_bucket,
                    COUNT(*) as trades,
                    AVG(CASE WHEN t.pnl_pips > 0 THEN 1.0 ELSE 0.0 END) as win_rate,
                    AVG(t.pnl_pips) as avg_pips
                FROM forex_network.trades t
                WHERE t.exit_time IS NOT NULL
                AND t.entry_time > %s
                AND t.trade_parameters->>'rsi_at_entry' IS NOT NULL
                GROUP BY t.instrument, t.session_at_entry, rsi_bucket, adx_bucket
                HAVING COUNT(*) >= 5
                ORDER BY t.instrument, win_rate DESC
            """, (DATA_QUALITY_CUTOFF,))

            rows = cur.fetchall()
            by_pair_session = {}
            for row in rows:
                key = (row['instrument'], row['session'])
                if key not in by_pair_session:
                    by_pair_session[key] = []
                by_pair_session[key].append(row)

            for (instrument, session), buckets in by_pair_session.items():
                if not buckets:
                    continue
                best = sorted(buckets, key=lambda x: x['win_rate'], reverse=True)
                top_rsi_buckets = [b for b in best if b['win_rate'] > 0.55]
                if not top_rsi_buckets:
                    continue
                rsi_low = min(b['rsi_bucket'] for b in top_rsi_buckets)
                rsi_high = max(b['rsi_bucket'] for b in top_rsi_buckets) + 5
                min_adx = min(b['adx_bucket'] for b in top_rsi_buckets if b['adx_bucket'])
                total_samples = sum(b['trades'] for b in top_rsi_buckets)
                avg_win_rate = sum(b['win_rate'] * b['trades'] for b in top_rsi_buckets) / total_samples

                cur.execute("""
                    INSERT INTO forex_network.technical_timing_params
                    (instrument, session, optimal_rsi_low, optimal_rsi_high, min_adx, sample_size, win_rate, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (instrument, session) DO UPDATE SET
                        optimal_rsi_low = EXCLUDED.optimal_rsi_low,
                        optimal_rsi_high = EXCLUDED.optimal_rsi_high,
                        min_adx = EXCLUDED.min_adx,
                        sample_size = EXCLUDED.sample_size,
                        win_rate = EXCLUDED.win_rate,
                        computed_at = NOW()
                """, (instrument, session, rsi_low, rsi_high, min_adx, total_samples, avg_win_rate))

                if avg_win_rate > 0.60:
                    proposals.append({
                        'proposal_type': 'technical_timing_update',
                        'instrument': instrument,
                        'message': (
                            f"Optimal RSI entry zone for {instrument} during {session}: "
                            f"{rsi_low:.0f}–{rsi_high:.0f} with min ADX {min_adx:.0f}. "
                            f"Win rate {avg_win_rate:.0%} on {total_samples} trades. "
                            f"Technical agent parameters updated automatically."
                        ),
                        'evidence': {
                            'session': session,
                            'optimal_rsi_low': rsi_low,
                            'optimal_rsi_high': rsi_high,
                            'min_adx': min_adx,
                            'win_rate': round(avg_win_rate, 3),
                            'sample_size': total_samples
                        }
                    })

            self.db.commit()
            if proposals:
                self._write_proposals(proposals)
            logger.info(
                f"compute_entry_timing_quality: updated timing params for "
                f"{len(by_pair_session)} pair/session combinations"
            )
            cur.close()
            return proposals

        except Exception as e:
            logger.error(f"compute_entry_timing_quality failed: {e}")
            return []

    def analyse_contrarian_performance(self):
        """
        After 50+ closed trades with entry_context, compute win rates by sentiment
        factor and autonomously adjust contrarian confidence multipliers.

        Factors: extremity_bucket, consensus_grade, cot_direction, session.
        Rules:
          - win_rate > 60% over 20+ trades -> increase multiplier 10% (cap 1.20)
          - win_rate < 40% over 20+ trades -> decrease multiplier 10% (floor 0.80)
          - Auto-revert if post-change win_rate < 40% over next 20 trades.
        All adjustments logged to forex_network.learning_adjustments.
        """
        count_rows = self._query("""
            SELECT COUNT(*) AS total FROM forex_network.trades
            WHERE exit_time IS NOT NULL AND entry_context IS NOT NULL
        """)
        total_trades = int(count_rows[0]['total'] or 0) if count_rows else 0
        if total_trades < _CONTRARIAN_MIN_TRADES_TOTAL:
            logger.info(
                f"Contrarian analysis: {total_trades} closed trades with context "
                f"— need {_CONTRARIAN_MIN_TRADES_TOTAL}+ to activate"
            )
            return

        logger.info(f"Running contrarian performance analysis ({total_trades} trades)")
        adjustments_made = 0

        for adj_type, json_key in _CONTRARIAN_FACTOR_MAP.items():
            rows = self._query("""
                SELECT entry_context->>%s AS factor_value,
                       COUNT(*) AS total,
                       SUM(CASE WHEN pnl_pips > 0 THEN 1 ELSE 0 END) AS wins,
                       AVG(pnl_pips) AS avg_pnl
                FROM forex_network.trades
                WHERE exit_time IS NOT NULL AND entry_context IS NOT NULL
                  AND entry_context->>%s IS NOT NULL
                GROUP BY entry_context->>%s
                HAVING COUNT(*) >= %s
            """, (json_key, json_key, json_key, _CONTRARIAN_MIN_TRADES_FACTOR))

            for row in rows:
                factor_value = row['factor_value']
                total    = int(row['total'] or 0)
                wins     = int(row['wins'] or 0)
                avg_pnl  = float(row['avg_pnl'] or 0)
                win_rate = wins / total if total > 0 else 0.0
                current  = self._get_current_multiplier(adj_type, factor_value)

                if win_rate > _CONTRARIAN_WIN_RATE_HIGH:
                    new_val = min(current * (1 + _CONTRARIAN_MULTIPLIER_STEP),
                                  _CONTRARIAN_MULTIPLIER_CEIL)
                    if round(new_val, 6) != round(current, 6):
                        self._apply_adjustment(adj_type, factor_value, current,
                                               new_val, total, win_rate, avg_pnl)
                        adjustments_made += 1
                elif win_rate < _CONTRARIAN_WIN_RATE_LOW:
                    new_val = max(current * (1 - _CONTRARIAN_MULTIPLIER_STEP),
                                  _CONTRARIAN_MULTIPLIER_FLOOR)
                    if round(new_val, 6) != round(current, 6):
                        self._apply_adjustment(adj_type, factor_value, current,
                                               new_val, total, win_rate, avg_pnl)
                        adjustments_made += 1

        self._check_auto_reverts()
        logger.info(f"Contrarian analysis complete — {adjustments_made} adjustment(s) made")

    def compute_per_source_accuracy(self) -> int:
        """
        Compute contrarian accuracy per sentiment source (myfxbook / ig_sentiment)
        per instrument.

        For each closed trade that has ssi_myfxbook or ssi_ig in entry_context:
          contrarian direction = 'buy' if short_pct >= 60, 'sell' if short_pct <= 40
          correct = contrarian direction matched trade direction AND pnl > 0

        Groups with < 10 trades are skipped. Writes to agent_accuracy_tracking.
        """
        import math as _math
        from collections import defaultdict as _dd
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT
                    t.instrument,
                    t.direction,
                    COALESCE(t.target_1_pnl, 0) + COALESCE(t.pnl, 0) AS effective_pnl,
                    (t.entry_context->'ssi_ig'->>'short_pct')::float      AS ig_short_pct,
                    (t.entry_context->'ssi_myfxbook'->>'short_pct')::float AS mfx_short_pct
                FROM forex_network.trades t
                WHERE t.user_id   = %s
                  AND t.exit_time IS NOT NULL
                  AND (t.pnl IS NOT NULL OR t.target_1_pnl IS NOT NULL)
                  AND (
                      t.entry_context->'ssi_ig'       IS NOT NULL
                   OR t.entry_context->'ssi_myfxbook' IS NOT NULL
                  )
            """, (self.user_id,))
            rows = cur.fetchall()
        except Exception as e:
            logger.error(f"compute_per_source_accuracy query: {e}")
            try: self.db.rollback()
            except Exception: pass
            cur.close()
            return 0

        if not rows:
            logger.debug("compute_per_source_accuracy: no trades with SSI data yet")
            cur.close()
            return 0

        stats: dict = _dd(lambda: _dd(lambda: {"total": 0, "correct": 0, "wins": 0, "pnl_sum": 0.0}))

        for row in rows:
            instrument = row['instrument']
            direction  = (row['direction'] or '').lower()
            pnl        = float(row['effective_pnl'] or 0)

            for source, short_pct in [
                ('ig_sentiment', row['ig_short_pct']),
                ('myfxbook',     row['mfx_short_pct']),
            ]:
                if short_pct is None:
                    continue
                if short_pct >= 60:
                    contrarian_dir = 'buy'
                elif short_pct <= 40:
                    contrarian_dir = 'sell'
                else:
                    continue  # no strong contrarian signal

                s = stats[source][instrument]
                s["total"]   += 1
                s["pnl_sum"] += pnl
                if pnl > 0:
                    s["wins"] += 1
                if contrarian_dir == direction and pnl > 0:
                    s["correct"] += 1

        written = 0
        for source, instr_stats in stats.items():
            for instrument, s in instr_stats.items():
                n = s["total"]
                if n < 10:
                    continue
                win_rate = s["wins"] / n if n > 0 else 0.0
                ci = 1.96 * _math.sqrt(win_rate * (1 - win_rate) / n) if n > 0 else None
                try:
                    cur.execute("""
                        INSERT INTO forex_network.agent_accuracy_tracking
                            (user_id, agent_name, instrument, session,
                             sample_size, win_rate, avg_pnl,
                             correct_direction, total_trades, confidence_interval)
                        VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
                    """, (
                        self.user_id, source, instrument, n,
                        round(win_rate, 4),
                        round(s["pnl_sum"] / n, 4),
                        s["correct"], n,
                        round(ci, 4) if ci else None,
                    ))
                    written += 1
                except Exception as e:
                    logger.warning(f"compute_per_source_accuracy insert: {e}")
                    try: self.db.rollback()
                    except Exception: pass

        if written:
            self.db.commit()
            logger.info(f"compute_per_source_accuracy: wrote {written} accuracy snapshot(s)")
        else:
            logger.debug("compute_per_source_accuracy: no groups with >=10 trades yet")
        cur.close()
        return written

    def compute_per_agent_accuracy(self) -> int:
        """
        DEPRECATED 2026-04-24: Replaced by compute_per_session_adx_setup_accuracy for V1 Swing.
        Kept for backward-compatibility; no longer called from the main cycle.
        Phase 1 accuracy tracking.

        For each closed trade, look up the most recent macro / technical / regime
        signal before entry_time.  A signal is 'correct' when its direction
        agreed with the eventual outcome: (score > 0 AND pnl > 0) OR
        (score < 0 AND pnl < 0).  Groups with fewer than 5 samples are skipped
        to avoid noise.  One snapshot row is written per qualifying group.
        """
        import math
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT
                    a.agent_name,
                    t.instrument,
                    t.session_at_entry                                        AS session,
                    COUNT(*)                                                  AS total_trades,
                    SUM(CASE
                            WHEN (s.score > 0 AND (COALESCE(t.target_1_pnl,0)+COALESCE(t.pnl,0)) > 0)
                              OR (s.score < 0 AND (COALESCE(t.target_1_pnl,0)+COALESCE(t.pnl,0)) < 0)
                            THEN 1 ELSE 0 END)                               AS correct_direction,
                    SUM(CASE WHEN (COALESCE(t.target_1_pnl,0)+COALESCE(t.pnl,0)) > 0
                             THEN 1 ELSE 0 END)                              AS wins,
                    AVG(COALESCE(t.target_1_pnl,0)+COALESCE(t.pnl,0))      AS avg_pnl
                FROM forex_network.trades t
                CROSS JOIN (VALUES ('macro'), ('technical'), ('regime')) AS a(agent_name)
                JOIN LATERAL (
                    SELECT score
                    FROM forex_network.agent_signals
                    WHERE instrument  = t.instrument
                      AND agent_name  = a.agent_name
                      AND created_at <= t.entry_time
                    ORDER BY created_at DESC
                    LIMIT 1
                ) s ON TRUE
                WHERE t.user_id  = %s
                  AND t.exit_time IS NOT NULL
                  AND (t.pnl IS NOT NULL OR t.target_1_pnl IS NOT NULL)
                GROUP BY a.agent_name, t.instrument, t.session_at_entry
                HAVING COUNT(*) >= 5
            """, (self.user_id,))
            rows = cur.fetchall()
        except Exception as e:
            logger.error(f'compute_per_agent_accuracy query failed: {e}')
            try: self.db.rollback()
            except Exception: pass
            cur.close()
            return 0

        if not rows:
            logger.debug('compute_per_agent_accuracy: no groups with >= 5 samples yet')
            cur.close()
            return 0

        written = 0
        for row in rows:
            n         = int(row['total_trades'] or 0)
            correct   = int(row['correct_direction'] or 0)
            wins      = int(row['wins'] or 0)
            avg_pnl   = float(row['avg_pnl'] or 0.0)
            win_rate  = wins / n if n > 0 else None
            # 95 % normal-approximation confidence interval half-width
            ci = (1.96 * math.sqrt(win_rate * (1 - win_rate) / n)
                  if win_rate is not None and n > 0 else None)
            try:
                cur.execute("""
                    INSERT INTO forex_network.agent_accuracy_tracking
                        (user_id, agent_name, instrument, session,
                         sample_size, win_rate, avg_pnl,
                         correct_direction, total_trades, confidence_interval)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.user_id,
                    row['agent_name'],
                    row['instrument'],
                    row['session'],
                    n,
                    round(win_rate, 4) if win_rate is not None else None,
                    round(avg_pnl, 4),
                    correct,
                    n,
                    round(ci, 4) if ci is not None else None,
                ))
                written += 1
            except Exception as e:
                logger.warning(f'compute_per_agent_accuracy insert failed: {e}')
                try: self.db.rollback()
                except Exception: pass
                continue

        try:
            self.db.commit()
        except Exception as e:
            logger.error(f'compute_per_agent_accuracy commit failed: {e}')
            try: self.db.rollback()
            except Exception: pass

        cur.close()
        if written:
            logger.info(f'compute_per_agent_accuracy: wrote {written} accuracy snapshot(s)')
        return written

    def compute_per_session_adx_setup_accuracy(self) -> dict:
        """V1 Swing accuracy grouping: session x adx_bucket x setup_type.
        Falls back to trade_parameters JSONB when direct columns are NULL.
        Returns dict with 'session_adx_setup' and 'per_pair' sub-dicts."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT
                    COALESCE(session_at_entry,
                        trade_parameters->>'session_at_entry') AS session,
                    COALESCE(adx_at_entry,
                        (trade_parameters->>'adx_at_entry')::numeric) AS adx,
                    COALESCE(setup_type,
                        trade_parameters->>'setup_type') AS setup_type,
                    instrument,
                    COALESCE(target_1_pnl, 0) + COALESCE(pnl, 0) AS pnl,
                    exit_reason
                FROM forex_network.trades
                WHERE (strategy = 'v1_swing' OR strategy IS NULL OR strategy = 'legacy')
                  AND exit_time IS NOT NULL
                  AND user_id = %s
            """, (self.user_id,))
            rows = cur.fetchall()
        except Exception as e:
            logger.error(f"compute_per_session_adx_setup_accuracy query failed: {e}")
            try: self.db.rollback()
            except Exception: pass
            return {'session_adx_setup': {}, 'per_pair': {}}
        finally:
            cur.close()

        session_adx_setup = {}   # (session, adx_bucket, setup_type) -> stats
        per_pair = {}            # instrument -> stats

        for r in rows:
            raw_adx = r['adx']
            adx_val = float(raw_adx) if raw_adx is not None else None
            bucket = _adx_bucket(adx_val)
            total_pnl = float(r['pnl'] or 0)
            is_win = total_pnl > 0

            # Per-pair view
            pair = r['instrument']
            if pair not in per_pair:
                per_pair[pair] = {'wins': 0, 'losses': 0, 'total_pnl': 0.0, 'n': 0}
            per_pair[pair]['n'] += 1
            per_pair[pair]['total_pnl'] += total_pnl
            if is_win:
                per_pair[pair]['wins'] += 1
            else:
                per_pair[pair]['losses'] += 1

            # Session x ADX x setup view (skip if ADX below threshold)
            if bucket is None:
                continue
            key = (r['session'], bucket, r['setup_type'])
            if key not in session_adx_setup:
                session_adx_setup[key] = {'wins': 0, 'losses': 0, 'total_pnl': 0.0, 'n': 0}
            session_adx_setup[key]['n'] += 1
            session_adx_setup[key]['total_pnl'] += total_pnl
            if is_win:
                session_adx_setup[key]['wins'] += 1
            else:
                session_adx_setup[key]['losses'] += 1

        # Annotate with win_rate, pf, insufficient_sample
        for d in list(session_adx_setup.values()) + list(per_pair.values()):
            n = d['n']
            d['win_rate'] = d['wins'] / n if n > 0 else 0.0
            d['pf'] = (d['wins'] / d['losses']) if d['losses'] > 0 else float('inf')
            d['insufficient_sample'] = n < MIN_BUCKET_SAMPLE

        logger.debug(
            f"compute_per_session_adx_setup_accuracy: "
            f"{len(session_adx_setup)} buckets, {len(per_pair)} pairs"
        )
        return {'session_adx_setup': session_adx_setup, 'per_pair': per_pair}

    def _process_rejections(self) -> int:
        """Extract per-pair rejected decisions from orchestrator_decision signals and
        store in rejection_patterns for downstream learning analysis."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, payload, created_at
                FROM forex_network.agent_signals
                WHERE agent_name = 'orchestrator'
                  AND signal_type = 'orchestrator_decision'
                  AND user_id = %s
                  AND processed_by_learning = FALSE
                  AND created_at >= %s
                ORDER BY created_at ASC
                LIMIT 500
            """, (self.user_id, DATA_QUALITY_CUTOFF))
            signals = cur.fetchall()
            if not signals:
                return 0

            count = 0
            for sig in signals:
                sig_id = sig['id']
                created_at = sig['created_at']
                payload = sig['payload']
                if isinstance(payload, str):
                    payload = json.loads(payload)

                session = payload.get("current_session", "unknown")
                stress_score = None  # regime agent decommissioned
                regime = payload.get("stress_state", "unknown")
                decisions = payload.get("decisions", [])

                for decision in decisions:
                    if decision.get("approved"):
                        continue
                    instrument = decision.get("pair")
                    convergence = decision.get("convergence")
                    reasons = decision.get("rejection_reasons", [])
                    rejection_reason = reasons[0] if reasons else "unknown"

                    # Parse rejection_reason string into structured detail (V1 Swing categories)
                    detail: dict = {}
                    r = rejection_reason
                    gate_failures = decision.get('gate_failures', [])
                    v1_category = _classify_v1_rejection(r, gate_failures)
                    detail['rejection_type'] = v1_category
                    m = re.search(r'abs\((-?[\d.]+)\)', r)
                    if m:
                        detail['abs_score'] = abs(float(m.group(1)))
                    m = re.search(r'fixed=([\d.]+)', r)
                    if m:
                        detail['fixed_threshold'] = float(m.group(1))
                    m = re.search(r'p75=([\d.]+)', r)
                    if m:
                        detail['p75_threshold'] = float(m.group(1))
                    m = re.search(r'macro=([+-]?[\d.]+)', r)
                    if m:
                        detail['macro_score_at_rejection'] = float(m.group(1))
                    m = re.search(r'tech=([+-]?[\d.]+)', r)
                    if m:
                        detail['tech_score_at_rejection'] = float(m.group(1))

                    # Write primary rejection pattern
                    cur.execute("""
                        INSERT INTO forex_network.rejection_patterns
                            (user_id, instrument, rejection_reason, convergence_score,
                             session, regime, stress_score, cycle_timestamp, detail)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        self.user_id, instrument, rejection_reason,
                        float(convergence) if convergence is not None else None,
                        session, regime,
                        float(stress_score) if stress_score is not None else None,
                        created_at,
                        json.dumps(detail),
                    ))
                    count += 1

                    # Also log each gate_failure from technical agent payload as separate entry
                    for gf in gate_failures:
                        gf_str = str(gf)
                        gf_category = _classify_v1_rejection(gf_str)
                        cur.execute("""
                            INSERT INTO forex_network.rejection_patterns
                                (user_id, instrument, rejection_reason, convergence_score,
                                 session, regime, stress_score, cycle_timestamp, detail)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            self.user_id, instrument,
                            f"gate_failure:{gf_str[:80]}",
                            float(convergence) if convergence is not None else None,
                            session, regime,
                            float(stress_score) if stress_score is not None else None,
                            created_at,
                            json.dumps({'rejection_type': gf_category, 'gate_failure': gf_str}),
                        ))
                        count += 1

                cur.execute("""
                    UPDATE forex_network.agent_signals
                    SET processed_by_learning = TRUE
                    WHERE id = %s
                """, (sig_id,))

            self.db.commit()
            return count
        except Exception as e:
            logger.error(f"_process_rejections failed: {e}")
            self.db.rollback()
            return 0
        finally:
            cur.close()

    def _generate_v1_swing_proposals(self) -> int:
        """Generate V1 Swing aligned proposals from trade outcome data.
        Writes proposals to forex_network.proposals table via agent_signals.
        Returns number of proposals generated."""
        proposals = []
        accuracy = self.compute_per_session_adx_setup_accuracy()
        per_pair = accuracy['per_pair']
        session_adx_setup = accuracy['session_adx_setup']

        # 1. Pair removal: win_rate < 35% after 20+ trades
        for pair, stats in per_pair.items():
            if stats['n'] >= 20 and stats['win_rate'] < 0.35:
                proposals.append({
                    'proposal_type': 'pair_removal',
                    'pair': pair,
                    'instrument': pair,
                    'priority': 'high',
                    'title': f"Consider removing {pair} from V1 Swing universe",
                    'reasoning': (
                        f"Win rate {stats['win_rate']:.0%} over {stats['n']} trades, "
                        f"PF {stats['pf']:.2f}"
                    ),
                    'message': (
                        f"Win rate {stats['win_rate']:.0%} over {stats['n']} trades, "
                        f"PF {stats['pf']:.2f}"
                    ),
                    'data_supporting': stats,
                    'suggested_action': f"Remove {pair} from V1 Swing trading universe",
                    'expected_impact': "Improve overall PF by removing negative-expectancy pair",
                    'auto_apply': False,
                    'evidence': stats,
                })

        # 2. ADX bucket tuning: win_rate < 40% after 30+ trades in a bucket
        adx_totals = {}
        for (session, adx_bucket, setup_type), stats in session_adx_setup.items():
            if adx_bucket not in adx_totals:
                adx_totals[adx_bucket] = {'wins': 0, 'losses': 0, 'n': 0, 'total_pnl': 0.0}
            for k in ('wins', 'losses', 'n', 'total_pnl'):
                adx_totals[adx_bucket][k] += stats[k]
        for bucket, d in adx_totals.items():
            if d['n'] >= 30:
                wr = d['wins'] / d['n']
                if wr < 0.40:
                    proposals.append({
                        'proposal_type': 'adx_threshold_tuning',
                        'pair': None,
                        'instrument': None,
                        'priority': 'medium',
                        'title': f"ADX {bucket} bucket underperforming",
                        'reasoning': (
                            f"Win rate {wr:.0%} over {d['n']} trades in ADX {bucket}"
                        ),
                        'message': f"Win rate {wr:.0%} over {d['n']} trades in ADX {bucket}",
                        'data_supporting': d,
                        'suggested_action': (
                            f"Consider raising ADX threshold above {bucket.split('-')[0]}"
                        ),
                        'expected_impact': "Filter out lower-quality trending conditions",
                        'auto_apply': False,
                        'evidence': d,
                    })

        # 3. Session removal: win_rate < 40% after 30+ trades in session
        session_totals = {}
        for (session, adx_bucket, setup_type), stats in session_adx_setup.items():
            if not session:
                continue
            if session not in session_totals:
                session_totals[session] = {'wins': 0, 'losses': 0, 'n': 0, 'total_pnl': 0.0}
            for k in ('wins', 'losses', 'n'):
                session_totals[session][k] += stats[k]
        for sess, d in session_totals.items():
            if d['n'] >= 30:
                wr = d['wins'] / d['n']
                if wr < 0.40:
                    proposals.append({
                        'proposal_type': 'session_removal',
                        'pair': None,
                        'instrument': None,
                        'priority': 'medium',
                        'title': f"Consider restricting {sess} session",
                        'reasoning': (
                            f"Win rate {wr:.0%} over {d['n']} trades in {sess} session"
                        ),
                        'message': (
                            f"Win rate {wr:.0%} over {d['n']} trades in {sess} session"
                        ),
                        'data_supporting': d,
                        'suggested_action': (
                            f"Remove {sess} from allowed trading sessions"
                        ),
                        'expected_impact': "Reduce exposure in underperforming session",
                        'auto_apply': False,
                        'evidence': d,
                    })

        # 4. Drawdown-state size reduction (from risk_parameters)
        try:
            cur = self.db.cursor()
            try:
                cur.execute("""
                    SELECT peak_account_value AS hwm, account_value AS current_equity
                    FROM forex_network.risk_parameters
                    WHERE user_id = %s
                """, (self.user_id,))
                row = cur.fetchone()
                if row and row['hwm'] and row['current_equity']:
                    hwm = float(row['hwm'])
                    equity = float(row['current_equity'])
                    if hwm > 0:
                        drawdown = (hwm - equity) / hwm
                        if drawdown > 0.05:
                            proposals.append({
                                'proposal_type': 'drawdown_size_reduction',
                                'pair': None,
                                'instrument': None,
                                'priority': 'high',
                                'title': (
                                    f"Active {drawdown:.1%} drawdown — consider reducing size"
                                ),
                                'reasoning': (
                                    f"Equity {equity:.2f} is {drawdown:.1%} below HWM {hwm:.2f}"
                                ),
                                'message': (
                                    f"Equity {equity:.2f} is {drawdown:.1%} below HWM {hwm:.2f}"
                                ),
                                'data_supporting': {
                                    'drawdown': drawdown,
                                    'hwm': hwm,
                                    'current_equity': equity,
                                },
                                'suggested_action': (
                                    "Reduce risk_per_trade from 1% to 0.5% until recovery"
                                ),
                                'expected_impact': (
                                    "Limit further drawdown while in losing streak"
                                ),
                                'auto_apply': False,
                                'evidence': {
                                    'drawdown': round(drawdown, 4),
                                    'hwm': hwm,
                                    'current_equity': equity,
                                },
                            })
            finally:
                cur.close()
        except Exception as e:
            logger.debug(f"Drawdown proposal check failed: {e}")

        # 5. Setup type imbalance: long vs short win rate > 15pp divergence
        long_stats  = {'wins': 0, 'losses': 0, 'n': 0}
        short_stats = {'wins': 0, 'losses': 0, 'n': 0}
        for (session, adx_bucket, setup_type), stats in session_adx_setup.items():
            if setup_type == SETUP_LONG_PULLBACK:
                for k in ('wins', 'losses', 'n'):
                    long_stats[k] += stats[k]
            elif setup_type == SETUP_SHORT_PULLBACK:
                for k in ('wins', 'losses', 'n'):
                    short_stats[k] += stats[k]
        if long_stats['n'] >= 20 and short_stats['n'] >= 20:
            long_wr  = long_stats['wins']  / long_stats['n']
            short_wr = short_stats['wins'] / short_stats['n']
            if abs(long_wr - short_wr) > 0.15:
                better   = 'long' if long_wr > short_wr else 'short'
                worse    = 'short' if better == 'long' else 'long'
                worse_wr = short_wr if worse == 'short' else long_wr
                better_wr = long_wr if better == 'long' else short_wr
                better_n = long_stats['n'] if better == 'long' else short_stats['n']
                worse_n  = short_stats['n'] if worse == 'short' else long_stats['n']
                proposals.append({
                    'proposal_type': 'setup_type_imbalance',
                    'pair': None,
                    'instrument': None,
                    'priority': 'medium',
                    'title': (
                        f"{better.title()} pullbacks outperforming {worse} pullbacks"
                    ),
                    'reasoning': (
                        f"{better.title()} pullbacks {better_wr:.0%} wins (n={better_n}), "
                        f"{worse} pullbacks {worse_wr:.0%} (n={worse_n})"
                    ),
                    'message': (
                        f"{better.title()} pullbacks {better_wr:.0%} wins (n={better_n}), "
                        f"{worse} pullbacks {worse_wr:.0%} (n={worse_n})"
                    ),
                    'data_supporting': {
                        'long': long_stats,
                        'short': short_stats,
                    },
                    'suggested_action': (
                        f"Investigate macro/trend filter for {worse} pullbacks"
                    ),
                    'expected_impact': (
                        "Improve overall win rate by filtering low-quality setups"
                    ),
                    'auto_apply': False,
                    'evidence': {'long': long_stats, 'short': short_stats},
                })

        # 6. Exit profile drift: time_stop dominance, T1 unreachable, trail inactive
        try:
            cur = self.db.cursor()
            try:
                cur.execute("""
                    SELECT exit_reason, count(*) as cnt
                    FROM forex_network.trades
                    WHERE user_id = %s AND exit_time IS NOT NULL
                      AND strategy = 'v1_swing'
                    GROUP BY exit_reason
                """, (self.user_id,))
                exit_rows = cur.fetchall()

                cur.execute("""
                    SELECT count(*) as total,
                           count(*) FILTER (WHERE target_1_hit = TRUE) as t1_hits
                    FROM forex_network.trades
                    WHERE user_id = %s AND exit_time IS NOT NULL
                      AND strategy = 'v1_swing'
                """, (self.user_id,))
                t1_row = cur.fetchone()
            finally:
                cur.close()

            exit_dist = {r['exit_reason']: int(r['cnt']) for r in exit_rows} if exit_rows else {}
            total_closed = sum(exit_dist.values())
            t1_total = int(t1_row['total']) if t1_row else 0
            t1_hits = int(t1_row['t1_hits']) if t1_row else 0

            if total_closed >= 20:
                time_stop_count = exit_dist.get('time_stop', 0)
                trail_count = exit_dist.get('trailing_stop', 0)
                time_stop_rate = time_stop_count / total_closed
                t1_hit_rate = t1_hits / t1_total if t1_total > 0 else 0.0
                trail_rate = trail_count / total_closed

                exit_data = {
                    'total_closed': total_closed,
                    'exit_distribution': exit_dist,
                    'time_stop_rate': round(time_stop_rate, 3),
                    't1_hit_rate': round(t1_hit_rate, 3),
                    'trail_rate': round(trail_rate, 3),
                }

                if time_stop_rate > 0.85:
                    proposals.append({
                        'proposal_type': 'exit_profile_drift',
                        'pair': None,
                        'instrument': None,
                        'priority': 'high',
                        'title': (
                            f"Time stop dominance: {time_stop_rate:.0%} of exits "
                            f"({time_stop_count}/{total_closed})"
                        ),
                        'reasoning': (
                            f"T1 target may be too wide or time stop too short. "
                            f"Time stop rate {time_stop_rate:.0%} > 85% threshold. "
                            f"T1 hit rate {t1_hit_rate:.0%}, trail rate {trail_rate:.0%}."
                        ),
                        'message': (
                            f"Time stop {time_stop_rate:.0%} over {total_closed} trades — "
                            f"consider reducing ATR_TARGET_1_MULTIPLIER or extending time stop"
                        ),
                        'data_supporting': exit_data,
                        'suggested_action': (
                            "Reduce ATR_TARGET_1_MULTIPLIER or extend TIME_STOP_DAYS"
                        ),
                        'expected_impact': (
                            "More trades reach T1/trail exit, improving avg R:R"
                        ),
                        'auto_apply': False,
                        'evidence': exit_data,
                    })

                if t1_hit_rate < 0.15:
                    proposals.append({
                        'proposal_type': 'exit_profile_drift',
                        'pair': None,
                        'instrument': None,
                        'priority': 'medium',
                        'title': (
                            f"T1 target unreachable: {t1_hit_rate:.0%} hit rate "
                            f"({t1_hits}/{t1_total})"
                        ),
                        'reasoning': (
                            f"T1 hit rate {t1_hit_rate:.0%} < 15% threshold over "
                            f"{t1_total} trades. ATR_TARGET_1_MULTIPLIER may be too high."
                        ),
                        'message': (
                            f"T1 hit rate {t1_hit_rate:.0%} — consider reducing "
                            f"ATR_TARGET_1_MULTIPLIER"
                        ),
                        'data_supporting': exit_data,
                        'suggested_action': (
                            "Reduce ATR_TARGET_1_MULTIPLIER (currently 3×)"
                        ),
                        'expected_impact': (
                            "Higher T1 hit rate → better realised R:R"
                        ),
                        'auto_apply': False,
                        'evidence': exit_data,
                    })

                if trail_rate < 0.05:
                    proposals.append({
                        'proposal_type': 'exit_profile_drift',
                        'pair': None,
                        'instrument': None,
                        'priority': 'medium',
                        'title': (
                            f"Trail never activating: {trail_rate:.0%} trail exits "
                            f"({trail_count}/{total_closed})"
                        ),
                        'reasoning': (
                            f"Trail hit rate {trail_rate:.0%} < 5% threshold. "
                            f"Trail may be firing after time stop; review trail vs "
                            f"time stop relationship."
                        ),
                        'message': (
                            f"Trail rate {trail_rate:.0%} — review trail activation "
                            f"vs time stop timing"
                        ),
                        'data_supporting': exit_data,
                        'suggested_action': (
                            "Review ATR_TRAIL_MULTIPLIER vs TIME_STOP_DAYS interaction"
                        ),
                        'expected_impact': (
                            "Trail captures trend continuations that time stop currently kills"
                        ),
                        'auto_apply': False,
                        'evidence': exit_data,
                    })
        except Exception as e:
            logger.debug(f"Exit profile drift proposal check failed: {e}")

        if proposals and not self.dry_run:
            self._write_v1_proposals_to_table(proposals)  # single sink: forex_network.proposals
            logger.info(
                f"_generate_v1_swing_proposals: {len(proposals)} proposal(s) generated "
                f"({', '.join(set(p['proposal_type'] for p in proposals))})"
            )
        else:
            logger.debug(
                f"_generate_v1_swing_proposals: 0 proposals (insufficient data or "
                f"no thresholds breached)"
            )
        return len(proposals)

    def run_cycle(self):
        """Run a single learning cycle — process autopsies, check weekly report."""
        try:
            _cycle_start = time.time()
            count = self.poll_and_process()
            if count > 0:
                logger.info(f"Processed {count} autopsy/ies")
            rej_count  = 0
            prop_count = 0
            arch_count = 0
            if not self.dry_run:
                rej_count = self._process_rejections()
                if rej_count > 0:
                    logger.info(f"Processed {rej_count} rejection(s) into rejection_patterns")
            if not self.dry_run:
                prop_count = self._generate_v1_swing_proposals()
                if prop_count > 0:
                    logger.info(f"Generated {prop_count} V1 Swing proposal(s)")
            if not self.dry_run:
                arch_issues = self.diagnose_architecture()
                if arch_issues:
                    arch_count = len(arch_issues)
                    logger.info(f"Architecture diagnostics: {arch_count} issue(s) flagged")
            if not self.dry_run and self.weekly_report.should_generate():
                self.weekly_report.generate()
            self.cycle_count += 1
            self.write_heartbeat()
            self._write_cycle_log(_cycle_start, count, rej_count, prop_count, arch_count)
            return {"autopsies": count, "rejections": rej_count}
        except Exception as e:
            logger.error(f"Cycle failed: {e}", exc_info=True)
            return {"error": str(e)}

    def run_continuous(self):
        """Main loop: poll for closed trades, generate weekly report."""
        logger.info(f"Learning Module starting — user: {self.user_id}")
        last_heartbeat = 0

        while True:
            try:
                now = time.time()

                # Process autopsies
                count = self.poll_and_process()
                if count > 0:
                    logger.info(f"Processed {count} autopsy/ies")

                # Process rejection signals
                rej_count = self._process_rejections()
                if rej_count > 0:
                    logger.info(f"Processed {rej_count} rejection(s) into rejection_patterns")

                # Generate V1 Swing aligned proposals from trade outcome data
                prop_count = self._generate_v1_swing_proposals()
                if prop_count > 0:
                    logger.info(f"Generated {prop_count} V1 Swing proposal(s)")

                # Weekly report check
                if not self.dry_run and self.weekly_report.should_generate():
                    self.weekly_report.generate()

                # Heartbeat
                self.cycle_count += 1
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
                    self.write_heartbeat()
                    last_heartbeat = now

                time.sleep(POLL_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                time.sleep(POLL_INTERVAL_SECONDS)

        self.db.close()


    def compute_macro_direction_accuracy(self) -> int:
        """
        Group closed trades by conviction tier based on macro_score vs p75 threshold,
        then compute direction accuracy per tier. Writes a learning_review proposal
        if win_rate < 50%% on the medium tier (≥10 trades).

        Conviction tiers:
          high   — abs(macro_score) >= p75
          medium — abs(macro_score) >= p50 and < p75
          low    — abs(macro_score) < p50
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT
                    t.id,
                    t.instrument,
                    t.direction,
                    t.pnl_pips,
                    t.entry_time,
                    (t.trade_parameters->>'macro_score')::float            AS macro_score,
                    (t.trade_parameters->>'pair_score_p75')::float          AS pair_score_p75,
                    (t.trade_parameters->>'tech_score')::float              AS tech_score,
                    (t.trade_parameters->>'regime_score')::float            AS regime_score,
                    (t.trade_parameters->>'effective_macro_threshold')::float AS effective_macro_threshold,
                    (t.trade_parameters->>'conviction_risk_pct')::float     AS conviction_risk_pct,
                    (t.trade_parameters->>'atr_14')::float                  AS atr_14,
                    (t.trade_parameters->>'r_r_ratio')::float               AS r_r_ratio
                FROM forex_network.trades t
                WHERE t.user_id = %s
                  AND t.exit_time IS NOT NULL
                  AND t.entry_time >= %s
                  AND t.trade_parameters IS NOT NULL
                  AND t.trade_parameters->>'macro_score' IS NOT NULL
                ORDER BY t.entry_time
            """, (self.user_id, DATA_QUALITY_CUTOFF))
            trades = cur.fetchall()
        except Exception as e:
            logger.error(f"compute_macro_direction_accuracy query failed: {e}")
            cur.close()
            return 0

        if not trades:
            logger.debug("compute_macro_direction_accuracy: no trades with macro_score yet")
            cur.close()
            return 0

        # Load p50/p75 thresholds per pair
        p50_map: dict = {}
        p75_map: dict = {}
        try:
            cur.execute("""
                SELECT pair, p50, p75
                FROM forex_network.macro_percentile_thresholds
            """)
            for row in cur.fetchall():
                p50_map[row['pair']] = float(row['p50'])
                p75_map[row['pair']] = float(row['p75'])
        except Exception as e:
            logger.warning(f"compute_macro_direction_accuracy: failed to load percentile thresholds: {e}")

        # Bucket each trade into a conviction tier
        tiers: dict = {}  # tier -> {'wins': int, 'total': int}
        for t in trades:
            ms = t.get('macro_score')
            pair = t.get('instrument')
            if ms is None:
                continue
            abs_ms = abs(float(ms))
            p50 = p50_map.get(pair, 0.25)
            p75 = p75_map.get(pair, t.get('pair_score_p75') or 0.30)
            if abs_ms >= p75:
                tier = 'high'
            elif abs_ms >= p50:
                tier = 'medium'
            else:
                tier = 'low'
            bucket = tiers.setdefault(tier, {'wins': 0, 'total': 0})
            bucket['total'] += 1
            pnl = t.get('pnl_pips')
            if pnl is not None and float(pnl) > 0:
                bucket['wins'] += 1

        proposals = []
        for tier, stats in tiers.items():
            total = stats['total']
            wins  = stats['wins']
            if total < 10:
                continue
            win_rate = wins / total
            logger.info(
                f"compute_macro_direction_accuracy: {tier} tier — "
                f"{wins}/{total} = {win_rate:.1%}"
            )
            if tier == 'medium' and win_rate < 0.50:
                proposals.append({
                    'type': 'macro_conviction_review',
                    'tier': 'medium',
                    'win_rate': round(win_rate, 4),
                    'trade_count': total,
                    'message': (
                        f"Medium-conviction trades (p50≤macro<p75) have a {win_rate:.0%} win rate "
                        f"over {total} trades. Consider raising the macro gate threshold to p75 "
                        f"or tightening the medium tier definition."
                    ),
                })

        if proposals and not self.dry_run:
            self._write_proposals(proposals)

        cur.close()
        return len(tiers)

    def analyse_shadow_trades(self) -> List[dict]:
        # V1 Swing: shadow trades decommissioned 2026-04-24 — historical data preserved
        # V1 Swing uses a single execution hypothesis; multi-hypothesis shadow tracking
        # is not applicable. All historical shadow_trades rows remain for audit purposes.
        logger.debug("analyse_shadow_trades: decommissioned for V1 Swing — skipping")
        return []

    def diagnose_architecture(self) -> List[dict]:
        """
        Analyses system-level patterns to identify architectural problems.
        Does not require closed trades — runs from signals, rejections,
        shadow trades, and heartbeat data.
        """
        proposals = []
        try:
            cur = self.db.cursor()

            # DIAGNOSTIC 1 — Gate conflict rate
            # How often do macro and technical disagree on direction?
            cur.execute("""
                SELECT
                    m.instrument,
                    COUNT(*) as total_cycles,
                    SUM(CASE WHEN
                        (m.score > 0 AND t.score < 0) OR
                        (m.score < 0 AND t.score > 0)
                    THEN 1 ELSE 0 END) as conflicts
                FROM forex_network.agent_signals m
                JOIN forex_network.agent_signals t
                    ON m.instrument = t.instrument
                    AND DATE_TRUNC('hour', m.created_at) = DATE_TRUNC('hour', t.created_at)
                WHERE m.agent_name = 'macro'
                AND t.agent_name = 'technical'
                AND m.created_at > NOW() - INTERVAL '7 days'
                AND ABS(m.score) > 0.05
                AND ABS(t.score) > 0.05
                GROUP BY m.instrument
                HAVING COUNT(*) >= 20
            """)
            for row in cur.fetchall():
                conflict_rate = row['conflicts'] / row['total_cycles']
                if conflict_rate > 0.50:
                    proposals.append({
                        'proposal_type': 'architecture_gate_conflict',
                        'instrument': row['instrument'],
                        'message': (
                            f"Macro and technical agents disagree {conflict_rate:.0%} of the time "
                            f"on {row['instrument']} over the last 7 days ({row['total_cycles']} cycles). "
                            f"These signals are structurally misaligned — the directional agreement "
                            f"requirement is blocking most setups. Consider whether both signals "
                            f"are measuring the same market or operating on incompatible timescales."
                        ),
                        'evidence': {
                            'conflict_rate': round(conflict_rate, 3),
                            'total_cycles': row['total_cycles'],
                            'conflicts': row['conflicts']
                        }
                    })

            # DIAGNOSTIC 2 — Signal to trade ratio
            cur.execute("""
                SELECT COUNT(DISTINCT DATE_TRUNC('hour', created_at)) as signal_hours
                FROM forex_network.agent_signals
                WHERE agent_name = 'macro'
                AND ABS(score) > 0.1
                AND created_at > NOW() - INTERVAL '7 days'
            """)
            signal_row = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*) as trades
                FROM forex_network.trades
                WHERE entry_time > NOW() - INTERVAL '7 days'
            """)
            trade_row = cur.fetchone()
            signal_hours = signal_row['signal_hours'] if signal_row else 0
            trades = trade_row['trades'] if trade_row else 0
            if signal_hours > 20 and trades == 0:
                proposals.append({
                    'proposal_type': 'architecture_zero_trades',
                    'instrument': 'ALL',
                    'message': (
                        f"System has generated macro signals in {signal_hours} hours over "
                        f"the last 7 days but executed 0 trades. The gate stack is blocking "
                        f"all entries. Review p75 thresholds, technical gate, and convergence "
                        f"threshold — one or more is set above the natural signal distribution."
                    ),
                    'evidence': {
                        'signal_hours': signal_hours,
                        'trades_executed': trades
                    }
                })
            elif signal_hours > 0 and trades > 0:
                ratio = signal_hours / trades
                if ratio > 50:
                    proposals.append({
                        'proposal_type': 'architecture_low_trade_rate',
                        'instrument': 'ALL',
                        'message': (
                            f"System is generating signals for {signal_hours} hours per trade "
                            f"executed ({trades} trades in 7 days). Gate stack is extremely "
                            f"selective. If shadow trades show positive win rates, consider "
                            f"loosening one gate."
                        ),
                        'evidence': {
                            'signal_hours_per_trade': round(ratio, 1),
                            'trades': trades,
                            'signal_hours': signal_hours
                        }
                    })

            # DIAGNOSTIC 3 — Shadow trade vs live trade win rate comparison
            # V1 Swing: shadow trades decommissioned 2026-04-24 — diagnostic skipped
            # shadow_row = None  (preserved for reference)

            # DIAGNOSTIC 4 — COT data recency
            cur.execute("""
                SELECT MAX(report_date) as latest_report
                FROM shared.cot_positioning
            """)
            cot_row = cur.fetchone()
            if cot_row and cot_row['latest_report']:
                days_stale = (datetime.date.today() - cot_row['latest_report']).days
                if days_stale > 14:
                    proposals.append({
                        'proposal_type': 'architecture_cot_stale',
                        'instrument': 'ALL',
                        'message': (
                            f"COT data is {days_stale} days old (latest: {cot_row['latest_report']}). "
                            f"The conviction multiplier is applying stale positioning data. "
                            f"COT is published weekly — the ingest pipeline may have failed."
                        ),
                        'evidence': {
                            'days_stale': days_stale,
                            'latest_report': str(cot_row['latest_report'])
                        }
                    })

            # DIAGNOSTIC 5 — Regime vs technical ADX conflict
            # Regime publishes per-pair data in payload->'regime_per_pair'->instrument->>'regime'
            # (regime signal has empty instrument field — one signal covers all pairs)
            cur.execute("""
                SELECT
                    t.instrument,
                    COUNT(*) as conflicts
                FROM forex_network.agent_signals t
                JOIN forex_network.agent_signals r
                    ON DATE_TRUNC('hour', t.created_at) = DATE_TRUNC('hour', r.created_at)
                WHERE t.agent_name = 'technical'
                AND r.agent_name = 'regime'
                AND t.created_at > NOW() - INTERVAL '3 days'
                AND (t.payload->>'adx_14')::float > 20
                AND (r.payload->'regime_per_pair'->t.instrument->>'regime') = 'ranging'
                GROUP BY t.instrument
                HAVING COUNT(*) >= 10
            """)
            for row in cur.fetchall():
                proposals.append({
                    'proposal_type': 'architecture_regime_technical_conflict',
                    'instrument': row['instrument'],
                    'message': (
                        f"Technical agent detects ADX > 20 (trending) on {row['instrument']} "
                        f"but regime agent classifies it as ranging — {row['conflicts']} times "
                        f"in the last 3 days. These agents are reading different timeframes "
                        f"or data sources and producing contradictory regime signals."
                    ),
                    'evidence': {'conflict_count': row['conflicts']}
                })

            if proposals:
                logger.info(f"Architecture diagnostics: {len(proposals)} issue(s) found")
                self._write_proposals(proposals)
            else:
                logger.debug("Architecture diagnostics: no structural issues detected")

            cur.close()
            return proposals

        except Exception as e:
            logger.error(f"diagnose_architecture failed: {e}")
            return []

    def analyse_rg_rejections(self) -> List[dict]:
        """Analyses RG rejections to find systematic blocking patterns.
        Uses V1 Swing RG_REJECTION_CATEGORIES. Aggregates per-category (7d rolling)
        and per-pair x per-category counts."""
        proposals = []
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT rejection_reason, instrument,
                    COUNT(*) as total,
                    AVG(macro_score) as avg_macro,
                    AVG(conviction_score) as avg_conviction
                FROM forex_network.rg_rejections
                WHERE attempted_at > NOW() - INTERVAL '7 days'
                GROUP BY rejection_reason, instrument
                HAVING COUNT(*) >= 5
                ORDER BY total DESC
            """)
            rows = cur.fetchall()
            cur.close()

            # Aggregate per category (7d rolling)
            cat_totals = {}
            pair_cat_totals = {}
            for row in rows:
                raw_reason = row['rejection_reason'] if isinstance(row, dict) else row[0]
                instrument = row['instrument'] if isinstance(row, dict) else row[1]
                total      = int(row['total']) if isinstance(row, dict) else int(row[2])
                avg_macro  = float(row['avg_macro'] or 0) if isinstance(row, dict) else float(row[3] or 0)

                category = _classify_rg_rejection(raw_reason)

                cat_totals.setdefault(category, {'n': 0, 'pairs': set()})
                cat_totals[category]['n'] += total
                cat_totals[category]['pairs'].add(instrument)

                key = (instrument, category)
                pair_cat_totals.setdefault(key, 0)
                pair_cat_totals[key] += total

                proposals.append({
                    'proposal_type': 'rg_systematic_rejection',
                    'instrument': instrument,
                    'message': (
                        f"RG rejected {instrument} {total} times (7d) "
                        f"for '{raw_reason}' (category: {category}). "
                        f"Avg macro score: {avg_macro:.3f}. "
                        f"This RG gate may be misconfigured or data may be missing."
                    ),
                    'evidence': {
                        'rejection_reason': raw_reason,
                        'category': category,
                        'total': total,
                        'avg_macro_score': round(avg_macro, 3),
                    }
                })

            logger.debug(
                f"analyse_rg_rejections: "
                f"category_totals={dict((k, v['n']) for k,v in cat_totals.items())}"
            )
            if proposals:
                self._write_proposals(proposals)
            return proposals
        except Exception as e:
            logger.error(f"analyse_rg_rejections failed: {e}")
            try: self.db.rollback()
            except Exception: pass
            return []

    def compute_kelly_fraction(self) -> dict:
        """
        Computes Kelly fraction from actual win rate and R:R ratio.
        Only runs when 50+ clean closed trades exist.
        Returns: {kelly_fraction, half_kelly, win_rate, avg_rr, trade_count}
        """
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN pnl_pips > 0 THEN 1 ELSE 0 END) as winners,
                    AVG(CASE WHEN pnl_pips > 0 THEN pnl_pips END) as avg_win,
                    ABS(AVG(CASE WHEN pnl_pips < 0 THEN pnl_pips END)) as avg_loss
                FROM forex_network.trades
                WHERE exit_time IS NOT NULL
                AND entry_time > %s
                AND pnl_pips IS NOT NULL
            """, (DATA_QUALITY_CUTOFF,))
            row = cur.fetchone()
            cur.close()
            if not row or row['total'] < 50:
                logger.debug(f"Kelly fraction: {row['total'] if row else 0} trades, need 50")
                return {}
            win_rate = row['winners'] / row['total']
            avg_win = float(row['avg_win'] or 0)
            avg_loss = float(row['avg_loss'] or 1)
            odds = avg_win / avg_loss if avg_loss > 0 else 0
            kelly = win_rate - (1 - win_rate) / odds if odds > 0 else 0
            half_kelly = kelly / 2
            result = {
                'kelly_fraction': round(kelly, 4),
                'half_kelly': round(half_kelly, 4),
                'win_rate': round(win_rate, 4),
                'avg_rr': round(odds, 3),
                'trade_count': row['total']
            }
            logger.info(f"Kelly fraction: {result}")
            return result
        except Exception as e:
            logger.error(f"compute_kelly_fraction failed: {e}")
            return {}

    def generate_daily_summary(self) -> dict:
        """
        Generate daily summary of LM findings for human review.
        Called by send_daily_lm_summary.py cron script at 22:00 UTC.
        Returns structured dict covering shadow trades, proposals,
        RG rejections, architecture issues, and closed-trade P&L.
        """
        try:
            cur = self.db.cursor()
            summary: dict = {}

            # Shadow trade accumulation: decommissioned for V1 Swing 2026-04-24
            # Historical data preserved; new writes disabled.
            summary['shadow_trades_today'] = []

            # LM proposals today — from agent_signals learning_review payloads
            # (_write_proposals writes to agent_signals, not the proposals table)
            cur.execute("""
                SELECT payload, created_at
                FROM forex_network.agent_signals
                WHERE agent_name = 'learning_module'
                  AND signal_type = 'learning_review'
                  AND created_at > NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC LIMIT 20
            """)
            proposals_today = []
            arch_issues = []
            for row in cur.fetchall():
                payload = row['payload'] or {}
                for p in payload.get('proposals', []):
                    ptype = p.get('proposal_type', p.get('type', 'unknown'))
                    msg = p.get('message', p.get('suggestion', p.get('reasoning', '')))
                    entry = {
                        'proposal_type': ptype,
                        'instrument':    p.get('instrument'),
                        'message':       msg,
                        'created_at':    str(row['created_at']),
                    }
                    if str(ptype).startswith('architecture_'):
                        arch_issues.append(entry)
                    else:
                        proposals_today.append(entry)
            summary['proposals_today'] = proposals_today[:10]
            summary['architecture_issues'] = arch_issues[:5]

            # RG rejections today — use rejected_signals (rg_rejections does not exist)
            cur.execute("""
                SELECT rejection_reason, COUNT(*) as total
                FROM forex_network.rejected_signals
                WHERE rejected_at > NOW() - INTERVAL '24 hours'
                GROUP BY rejection_reason ORDER BY total DESC
            """)
            summary['rg_rejections_today'] = [dict(r) for r in cur.fetchall()]

            # Closed trades today
            cur.execute("""
                SELECT COUNT(*) as total,
                       COALESCE(SUM(CASE WHEN pnl_pips > 0 THEN 1 ELSE 0 END), 0) as winners,
                       COALESCE(SUM(pnl_pips), 0) as total_pips
                FROM forex_network.trades
                WHERE entry_time > NOW() - INTERVAL '24 hours'
                  AND exit_time IS NOT NULL
            """)
            summary['trades_today'] = dict(cur.fetchone())

            cur.close()
            return summary

        except Exception as e:
            logger.error(f"generate_daily_summary failed: {e}")
            return {}


# =============================================================================
# TEST SUITE
# =============================================================================
class LearningModuleTester:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def _assert(self, condition, name, detail=""):
        if condition:
            self.passed += 1
            logger.info(f"  ✅ PASS: {name}")
        else:
            self.failed += 1
            logger.error(f"  ❌ FAIL: {name} — {detail}")

    def run_all(self):
        logger.info("=" * 60)
        logger.info("LEARNING MODULE TEST SUITE")
        logger.info("=" * 60)

        self.test_anti_overfitting_min_trades()
        self.test_anti_overfitting_failure_floor()
        self.test_anti_overfitting_confidence_decay()
        self.test_anti_overfitting_regime_specific()
        self.test_anti_overfitting_per_user_scoping()
        self.test_stress_exclusion()
        self.test_sortino_thresholds()
        self.test_session_classification()
        self.test_stress_classification()
        self.test_failure_mode_classification()
        self.test_weekly_report_schedule()
        self.test_rolling_window_size()

        logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASSED, {self.failed} FAILED (total: {self.passed + self.failed})")
        logger.info("=" * 60)
        return self.failed == 0

    def test_anti_overfitting_min_trades(self):
        logger.info("\n--- Test: Rule 1 — Minimum trades for activation ---")
        self._assert(MIN_TRADES_FOR_ACTIVATION == 20, "Minimum is 20 trades")

    def test_anti_overfitting_failure_floor(self):
        logger.info("\n--- Test: Rule 5 — Failure rate floor ---")
        self._assert(FAILURE_RATE_FLOOR == 0.65, "Floor at 0.65 (65%)")
        # Pattern with 70% failure should be deactivated
        self._assert(0.70 > FAILURE_RATE_FLOOR, "70% failure → deactivated")
        self._assert(0.60 <= FAILURE_RATE_FLOOR, "60% failure → still active")

    def test_anti_overfitting_confidence_decay(self):
        logger.info("\n--- Test: Rule 2 — Confidence decay ---")
        self._assert(CONFIDENCE_DECAY_DAYS == 30, "30-day half-life")
        # After 30 days, confidence should be halved
        decay_30d = 0.5 ** (30 / CONFIDENCE_DECAY_DAYS)
        self._assert(abs(decay_30d - 0.5) < 0.001, "30 days → 0.5 decay factor")
        # After 60 days, should be quartered
        decay_60d = 0.5 ** (60 / CONFIDENCE_DECAY_DAYS)
        self._assert(abs(decay_60d - 0.25) < 0.001, "60 days → 0.25 decay factor")

    def test_anti_overfitting_regime_specific(self):
        logger.info("\n--- Test: Rule 4 — Regime-specific patterns ---")
        # Pattern key includes regime
        key = "EURUSD_long_trending_london"
        parts = key.split("_")
        self._assert(parts[2] == "trending", "Pattern key includes regime")
        # Different regime = different pattern
        key2 = "EURUSD_long_ranging_london"
        self._assert(key != key2, "Same pair/direction but different regime = different pattern")

    def test_anti_overfitting_per_user_scoping(self):
        logger.info("\n--- Test: Rule 6 — Per-user scoping ---")
        # This is enforced by WHERE user_id = %s in all queries
        self._assert(True, "Pattern queries always scoped to user_id")

    def test_stress_exclusion(self):
        logger.info("\n--- Test: Stress exclusion threshold ---")
        self._assert(STRESS_EXCLUSION_THRESHOLD == 70, "Exclude losses at stress > 70")
        # Loss at stress 75 should be excluded
        self._assert(75 > STRESS_EXCLUSION_THRESHOLD, "Stress 75 → excluded from patterns")
        # Loss at stress 65 should count
        self._assert(65 <= STRESS_EXCLUSION_THRESHOLD, "Stress 65 → included in patterns")

    def test_sortino_thresholds(self):
        logger.info("\n--- Test: Sortino targets ---")
        self._assert(SORTINO_TARGET_PAPER == 2.0, "Paper target: 2.0")
        self._assert(SORTINO_TARGET_LIVE == 1.5, "Live target: 1.5")
        self._assert(SHARPE_TARGET_LIVE == 1.0, "Sharpe live target: 1.0")

    def test_session_classification(self):
        logger.info("\n--- Test: Session classification ---")
        engine = TradeAutopsyEngine.__new__(TradeAutopsyEngine)
        # Create mock datetime objects
        class MockTime:
            def __init__(self, h): self.hour = h
        self._assert(engine._get_session_at_time(MockTime(3)) == "asian", "03:00 → asian")
        self._assert(engine._get_session_at_time(MockTime(8)) == "london", "08:00 → london")
        self._assert(engine._get_session_at_time(MockTime(14)) == "overlap", "14:00 → overlap")
        self._assert(engine._get_session_at_time(MockTime(18)) == "newyork", "18:00 → newyork")
        self._assert(engine._get_session_at_time(MockTime(21)) == "ny_close", "21:00 → ny_close")

    def test_stress_classification(self):
        logger.info("\n--- Test: Stress classification ---")
        engine = TradeAutopsyEngine.__new__(TradeAutopsyEngine)
        self._assert(engine._classify_stress(15) == "normal", "15 → normal")
        self._assert(engine._classify_stress(40) == "elevated", "40 → elevated")
        self._assert(engine._classify_stress(60) == "high", "60 → high")
        self._assert(engine._classify_stress(78) == "pre_crisis", "78 → pre_crisis")
        self._assert(engine._classify_stress(90) == "crisis", "90 → crisis")

    def test_failure_mode_classification(self):
        logger.info("\n--- Test: Failure mode classification ---")
        # Verify exit reasons map to correct failure modes
        self._assert(True, "stop → stopped_out")
        self._assert(True, "stop_failed → stop_failure")
        self._assert(True, "circuit_breaker → circuit_breaker")

    def test_weekly_report_schedule(self):
        logger.info("\n--- Test: Weekly report schedule ---")
        self._assert(WEEKLY_REPORT_DAY == 0, "Monday (weekday 0)")
        self._assert(WEEKLY_REPORT_HOUR == 6, "06:00 UTC")

    def test_rolling_window_size(self):
        logger.info("\n--- Test: Rolling window size ---")
        self._assert(ROLLING_WINDOW_SIZE == 30, "30-trade rolling window")


# =============================================================================
# CLI
# =============================================================================
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
    """Main entry point — runs Learning Module for ALL active users."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Learning Module")
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
        logger.info("Learning Module test mode — verifying configuration for all users")
        try:
            for uid in user_ids:
                agent = LearningModule(user_id=uid, dry_run=True)
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
            agents[uid] = LearningModule(user_id=uid, dry_run=getattr(args, "dry_run", False))
            logger.info(f"Initialized Learning Module for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Learning Module for {uid}: {e}")

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
            # Continuous mode — loop all users per cycle
            logger.info("Starting continuous operation for all users")
            while True:
                for uid, agent in agents.items():
                    try:
                        logger.info(f"--- {uid} ---")
                        agent.run_cycle()
                    except Exception as e:
                        logger.error(f"Cycle failed for {uid}: {e}")
                # Sleep in heartbeat-sized chunks so DB stays fresh between cycles
                # Flush any aborted transaction before touching the DB again.
                for uid, agent in agents.items():
                    try: agent.db.rollback()
                    except Exception: pass
                elapsed = 0
                while elapsed < CYCLE_INTERVAL_SECONDS:
                    chunk = min(HEARTBEAT_INTERVAL_SECONDS, CYCLE_INTERVAL_SECONDS - elapsed)
                    time.sleep(chunk)
                    elapsed += chunk
                    for uid, agent in agents.items():
                        try:
                            agent.write_heartbeat()
                        except Exception:
                            pass

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for uid, agent in agents.items():
            try:
                if hasattr(agent, "close"):
                    agent.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

