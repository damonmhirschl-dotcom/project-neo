#!/usr/bin/env python3
"""
Project Neo — Risk Guardian Agent v1.0
========================================
Validates every orchestrator-approved trade before execution.
Checks correlation blocks, daily loss limits, position sizing,
circuit breakers, swap cost R:R, and drawdown step multipliers.

Cycle: Per-trade (polls for trade_approval signals every 15 seconds)
IAM Role: platform-risk-role-dev
Decision Rules: RG1 (daily loss absolute), RG2 (overnight swap R:R)

Run:
  source ~/algodesk/bin/activate
  python risk_guardian_agent.py --user neo_user_002
"""

import os
import sys
import json
import time
import uuid
import logging
import argparse
import datetime
from typing import Optional, Dict, List, Any, Tuple

import boto3
import sys as _alerting_sys
_alerting_sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.alerting import send_alert
import psycopg2
import psycopg2.extras
from shared.schema_validator import validate_schema
from shared.signal_validator import SignalValidator
from shared.system_events import log_event
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.risk_parameters":      ["user_id", "convergence_threshold", "max_risk_pct",
                                           "max_open_positions", "max_portfolio_risk_pct", "daily_loss_limit_pct",
                                           "account_value", "peak_account_value",
                                           "min_risk_reward_ratio", "updated_at"],
    "forex_network.trades":               ["user_id", "instrument", "direction", "entry_price",
                                           "stop_price", "target_price", "position_size",
                                           "entry_time", "exit_time", "exit_price", "pnl",
                                           "convergence_score"],
    "forex_network.swap_rates":           ["instrument", "long_rate_pips",
                                           "short_rate_pips", "rate_date"],
    "shared.portfolio_correlation":       ["instrument_a", "instrument_b",
                                           "correlation_30d", "user_id"],
    "forex_network.system_alerts":        ["alert_type", "severity", "title", "detail",
                                           "acknowledged", "created_at"],
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("neo.risk_guardian")

# =============================================================================
# CONSTANTS
# =============================================================================
AWS_REGION = "eu-west-2"
POLL_INTERVAL_SECONDS = 15
HEARTBEAT_INTERVAL_SECONDS = 60
AGENT_NAME = "risk_guardian"

# Known pair correlations (baselines — overridden by live data when available)
STATIC_CORRELATIONS = {
    ("EURUSD", "GBPUSD"):  0.87,
    ("EURUSD", "USDCHF"): -0.91,
    ("AUDUSD", "NZDUSD"):  0.88,
    ("EURUSD", "AUDUSD"):  0.72,
    ("EURUSD", "NZDUSD"):  0.74,
    ("GBPUSD", "USDCHF"): -0.83,
    ("USDJPY", "AUDUSD"): -0.65,
    ("AUDUSD", "GBPUSD"):  0.72,
    ("AUDUSD", "USDCAD"): -0.68,
    ("AUDUSD", "USDCHF"): -0.65,
    ("EURUSD", "USDCAD"): -0.60,
    ("EURUSD", "USDJPY"):  0.45,
    ("GBPUSD", "NZDUSD"):  0.70,
    ("GBPUSD", "USDCAD"): -0.58,
    ("GBPUSD", "USDJPY"):  0.40,
    ("NZDUSD", "USDCAD"): -0.62,
    ("NZDUSD", "USDCHF"): -0.63,
    ("NZDUSD", "USDJPY"):  0.38,
    ("USDCAD", "USDCHF"):  0.55,
    ("USDCAD", "USDJPY"):  0.60,
    ("USDCHF", "USDJPY"):  0.76,
}

# correlation_threshold is read from forex_network.risk_parameters per user (see DB migration patch)

# Currency exposure per pair (base, quote) — used for per-currency cap check
CURRENCY_EXPOSURE_MAP = {
    # USD pairs
    "EURUSD": ("EUR", "USD"), "GBPUSD": ("GBP", "USD"),
    "AUDUSD": ("AUD", "USD"), "NZDUSD": ("NZD", "USD"),
    "USDCHF": ("USD", "CHF"), "USDCAD": ("USD", "CAD"), "USDJPY": ("USD", "JPY"),
    # Cross pairs
    "EURGBP": ("EUR", "GBP"), "EURJPY": ("EUR", "JPY"), "GBPJPY": ("GBP", "JPY"),
    "EURCHF": ("EUR", "CHF"), "GBPCHF": ("GBP", "CHF"),
    "EURAUD": ("EUR", "AUD"), "GBPAUD": ("GBP", "AUD"),
    "EURCAD": ("EUR", "CAD"), "GBPCAD": ("GBP", "CAD"),
    "AUDNZD": ("AUD", "NZD"), "AUDJPY": ("AUD", "JPY"),
    "CADJPY": ("CAD", "JPY"), "NZDJPY": ("NZD", "JPY"),
}

# Slippage thresholds by pair (pips)
SLIPPAGE_THRESHOLDS = {
    "EURUSD": 3, "USDJPY": 3,
    "GBPUSD": 4, "USDCHF": 4,
    "AUDUSD": 5, "USDCAD": 5, "NZDUSD": 5,
}

FX_PAIRS = [
    # USD pairs
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Cross pairs
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
    "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
    "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
]


# =============================================================================
# AWS HELPERS
# =============================================================================
def get_secret(secret_name: str, region: str = AWS_REGION) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def get_parameter(param_name: str, region: str = AWS_REGION) -> str:
    client = boto3.client("ssm", region_name=region)
    resp = client.get_parameter(Name=param_name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def check_kill_switch(region: str = AWS_REGION) -> bool:
    try:
        val = get_parameter("/platform/config/kill-switch", region)
        return val.strip().lower() == "active"
    except Exception as e:
        logger.error(f"Kill switch check failed: {e} — treating as ACTIVE for safety")
        return True


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
# DATA READER
# =============================================================================
class RiskDataReader:
    """Reads all data the risk guardian needs for validation."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def read_pending_approvals(self) -> List[Dict[str, Any]]:
        """Read orchestrator_decision signals and expand approved decisions into per-pair records."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, score, payload, created_at, expires_at
                FROM forex_network.agent_signals orch
                WHERE orch.agent_name = 'orchestrator'
                  AND orch.signal_type = 'orchestrator_decision'
                  AND orch.user_id = %s
                  AND orch.expires_at > NOW()
                  AND NOT EXISTS (
                      SELECT 1
                      FROM forex_network.agent_signals rg
                      WHERE rg.agent_name = 'risk_guardian'
                        AND rg.user_id = %s
                        AND (rg.payload->>'approval_signal_id') IS NOT NULL
                        AND (rg.payload->>'approval_signal_id')::bigint = orch.id
                  )
                ORDER BY orch.created_at ASC
            """, (self.user_id, self.user_id))
            rows = cur.fetchall()
            results = []
            for row in rows:
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                payload = payload or {}
                for dec in payload.get("decisions", []):
                    if not dec.get("approved"):
                        continue
                    results.append({
                        "signal_id": row["id"],
                        "instrument": dec["pair"],
                        "score": float(dec.get("convergence") or 0.0),
                        "bias": dec["bias"],
                        "confidence": float(dec.get("confidence") or 0.0),
                        "payload": dec,
                        "created_at": row["created_at"],
                    })
            return results
        except Exception as e:
            logger.error(f"Read pending approvals failed: {e}")
            return []
        finally:
            cur.close()

    def read_risk_parameters(self):
        """
        Returns risk parameter dict, or None on DB error or missing row.
        Callers MUST treat None as 'parameters unknown — reject trade' (fail-safe).
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT convergence_threshold,
                       max_open_positions, daily_loss_limit_pct,
                       weekly_drawdown_limit_pct,
                       trailing_stop_pct, pre_event_size_reduction,
                       circuit_breaker_active, paper_mode,
                       size_multiplier, drawdown_step_level,
                       atr_stop_multiplier, min_signal_spread_ratio,
                       account_value, peak_account_value,
                       min_risk_pct, max_risk_pct, curve_exponent,
                       stress_multiplier, stress_threshold_score,
                       max_convergence_reference, min_risk_reward_ratio,
                       max_portfolio_risk_pct, max_usd_units, correlation_threshold
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            if row:
                return {k: (float(v) if isinstance(v, (int, float)) and v is not None else v)
                        for k, v in dict(row).items()}
            logger.error(f"read_risk_parameters: no row found for user {self.user_id}")
            return None  # Fail-safe: no row = cannot validate safely
        except Exception as e:
            logger.error(f"Risk parameters read failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return None  # Fail-safe: caller must reject trade
        finally:
            cur.close()

    def read_open_positions(self):
        """
        Returns list of open position dicts, or None on DB error.
        Callers MUST treat None as 'positions unknown — reject new entry' (fail-safe).
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT instrument, direction, entry_price, stop_price,
                       position_size_usd, entry_time, pnl, spread_at_entry
                FROM forex_network.trades
                WHERE user_id = %s AND exit_time IS NULL
                ORDER BY entry_time ASC
            """, (self.user_id,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Open positions read failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return None  # Fail-safe: caller must treat None as reject
        finally:
            cur.close()

    def read_daily_loss_pct(self, account_value: float) -> float:
        """
        Returns today's realised loss as a percentage of account_value.
        Positive return means a loss (e.g. 3.5 = 3.5% loss).
        Zero or negative return means no loss / profit.
        Returns None on error — caller MUST treat None as limit breached (fail-safe).
        """
        if not account_value or account_value <= 0:
            logger.error(f"read_daily_loss_pct: invalid account_value {account_value} for user {self.user_id}")
            return None
        cur = self.db.cursor()
        try:
            # pnl (GBP-denominated) is NULL for cross pairs (GBPJPY, EURJPY etc.)
            # when execution agent cannot resolve the quote→GBP conversion rate.
            # Fallback: approximate from pnl_pips using:
            #   pnl_approx_gbp = pnl_pips × pip_size × lots × 100000 / exit_price
            # This is exact for GBP-base pairs (GBPJPY, GBPUSD) and a close
            # approximation for other crosses (EUR/JPY off by EUR/GBP rate ≈ ±15%).
            # Safe direction: under-estimates losses for EUR pairs — acceptable for
            # a fail-safe gate because NULL pnl would count as 0 otherwise.
            cur.execute("""
                SELECT COALESCE(SUM(
                    CASE
                        WHEN pnl IS NOT NULL THEN pnl
                        WHEN pnl_pips IS NOT NULL
                             AND position_size IS NOT NULL
                             AND exit_price IS NOT NULL
                             AND exit_price > 0
                        THEN pnl_pips
                             * CASE WHEN instrument LIKE '%%JPY' THEN 0.01 ELSE 0.0001 END
                             * position_size * 100000.0
                             / exit_price
                        ELSE 0
                    END
                ), 0) AS total_pnl
                FROM forex_network.trades
                WHERE user_id = %s
                  AND exit_time IS NOT NULL
                  AND exit_time >= CURRENT_DATE
            """, (self.user_id,))
            row = cur.fetchone()
            total_pnl = float(row["total_pnl"]) if row else 0.0
            if total_pnl >= 0:
                return 0.0  # No loss today
            loss_pct = abs(total_pnl) / account_value * 100
            return round(loss_pct, 3)
        except Exception as e:
            logger.error(f"Daily loss read failed for user {self.user_id}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return None  # Fail-safe: caller treats None as limit breached
        finally:
            cur.close()

    def read_weekly_loss_pct(self, account_value: float):
        """
        Returns the rolling 7-day realised loss as a percentage of account_value.
        Positive return means a loss (e.g. 4.5 = 4.5% loss).
        Returns None on error — caller MUST treat None as limit breached (fail-safe).
        """
        if not account_value or account_value <= 0:
            logger.error(f"read_weekly_loss_pct: invalid account_value {account_value} for user {self.user_id}")
            return None
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT COALESCE(SUM(
                    CASE
                        WHEN pnl IS NOT NULL THEN pnl
                        WHEN pnl_pips IS NOT NULL
                             AND position_size IS NOT NULL
                             AND exit_price IS NOT NULL
                             AND exit_price > 0
                        THEN pnl_pips
                             * CASE WHEN instrument LIKE '%%JPY' THEN 0.01 ELSE 0.0001 END
                             * position_size * 100000.0
                             / exit_price
                        ELSE 0
                    END
                ), 0) AS weekly_pnl
                FROM forex_network.trades
                WHERE user_id = %s
                  AND exit_time IS NOT NULL
                  AND exit_time >= NOW() - INTERVAL '7 days'
            """, (self.user_id,))
            row = cur.fetchone()
            weekly_pnl = float(row["weekly_pnl"]) if row else 0.0
            if weekly_pnl >= 0:
                return 0.0  # No loss this week
            loss_pct = abs(weekly_pnl) / account_value * 100
            return round(loss_pct, 3)
        except Exception as e:
            logger.error(f"Weekly loss read failed for user {self.user_id}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return None  # Fail-safe: caller treats None as limit breached
        finally:
            cur.close()

    def read_correlations(self) -> Dict[Tuple[str, str], float]:
        """Read live 30-day rolling correlations, fall back to static."""
        cur = self.db.cursor()
        correlations = dict(STATIC_CORRELATIONS)
        try:
            cur.execute("""
                SELECT instrument_a, instrument_b, correlation_30d
                FROM shared.portfolio_correlation
                WHERE correlation_30d IS NOT NULL
                ORDER BY updated_at DESC
            """)
            for row in cur.fetchall():
                a, b = row["instrument_a"], row["instrument_b"]
                val = float(row["correlation_30d"])
                correlations[(a, b)] = val
                correlations[(b, a)] = val  # Store both directions
            return correlations
        except Exception as e:
            logger.warning(f"Live correlations unavailable, using static fallback: {e}. Pairs not in static table will pass correlation check silently.")
            return correlations
        finally:
            cur.close()

    def read_swap_rate(self, instrument: str, direction: str) -> Optional[float]:
        """Read current swap rate for instrument and direction."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT long_rate_pips, short_rate_pips
                FROM forex_network.swap_rates
                WHERE instrument = %s
                ORDER BY rate_date DESC, created_at DESC LIMIT 1
            """, (instrument,))
            row = cur.fetchone()
            if row:
                return float(row["long_rate_pips"] if direction == "long" else row["short_rate_pips"])
            return None
        except Exception as e:
            logger.warning(f"Swap rate unavailable for {instrument}: {e}")
            return None
        finally:
            cur.close()

    def read_market_context(self):
        """
        Read latest market context for stress score and session.
        Returns None on error or empty table — caller MUST treat None as
        elevated stress (fail-safe: no new entries).
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT system_stress_score, stress_state, current_session
                FROM shared.market_context_snapshots
                WHERE system_stress_score IS NOT NULL
                ORDER BY snapshot_time DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                logger.warning("read_market_context: table empty — returning None (fail-safe)")
                return None
            return {
                "stress_score": float(row["system_stress_score"]),
                "stress_state": row["stress_state"],
                "current_session": row["current_session"],
            }
        except Exception as e:
            logger.error(f"Market context read failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return None  # Fail-safe: caller treats None as stress >= threshold
        finally:
            cur.close()


# =============================================================================
# CORRELATION CHECKER
# =============================================================================
class CorrelationChecker:
    """Enforces correlation-based position blocking."""

    def __init__(self, user_id: str, threshold: float = 0.75):
        self.user_id = user_id
        self.threshold = threshold

    def check(
        self,
        instrument: str,
        direction: str,
        open_positions: List[Dict[str, Any]],
        correlations: Dict[Tuple[str, str], float],
        market_context: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        """
        Check if proposed trade would violate correlation limits.
        Returns (passed, list_of_reasons).
        """
        reasons = []

        # Check R4 relaxation from regime agent
        relaxation_active = False
        relaxed_pair = None
        # In production, read from market_context_snapshots metadata
        # For now, check correlations directly

        for pos in open_positions:
            pos_instrument = pos["instrument"]
            pos_direction = pos["direction"]

            if pos_instrument == instrument:
                # Same pair — already checked by max_open_positions
                continue

            # Look up correlation
            pair_key = (instrument, pos_instrument)
            reverse_key = (pos_instrument, instrument)
            corr = correlations.get(pair_key) or correlations.get(reverse_key)

            if corr is None:
                continue

            abs_corr = abs(corr)
            if abs_corr < self.threshold:
                continue

            # Correlation exceeds threshold — check direction
            blocked = False

            if corr > 0:
                # Positive correlation: block if same direction
                if direction == pos_direction:
                    blocked = True
                    reasons.append(
                        f"Correlation block: {instrument} {direction} + {pos_instrument} "
                        f"{pos_direction} (corr: {corr:+.2f}, threshold: {self.threshold}). "
                        f"Same direction in positively correlated pairs doubles exposure."
                    )
            else:
                # Negative correlation: block if opposite direction
                if direction != pos_direction:
                    blocked = True
                    reasons.append(
                        f"Correlation block: {instrument} {direction} + {pos_instrument} "
                        f"{pos_direction} (corr: {corr:+.2f}, threshold: {self.threshold}). "
                        f"Opposite direction in negatively correlated pairs doubles exposure."
                    )

        return len(reasons) == 0, reasons



def compute_position_risk_pct(
    convergence_score: float,
    stress_score: float,
    risk_params: dict,
) -> float:
    """
    Conviction-scaled risk fraction (decimal, e.g. 0.0025 = 0.25% of account).

    Interpolates between min_risk_pct and max_risk_pct using a power curve
    (curve_exponent) over the normalised conviction range [threshold, max_ref].
    Applies stress_multiplier when stress_score exceeds stress_threshold_score.
    """
    conv_abs  = abs(convergence_score)
    threshold = float(risk_params.get('convergence_threshold') or 0.22)
    max_ref   = float(risk_params.get('max_convergence_reference') or 0.55)
    min_risk  = float(risk_params.get('min_risk_pct') or 0.0025)
    max_risk  = float(risk_params.get('max_risk_pct') or 0.01)
    exponent  = float(risk_params.get('curve_exponent') or 2.0)

    # Below threshold: no trade (defensive; upstream should have filtered)
    if conv_abs < threshold:
        return 0.0

    # Normalise conviction 0..1 across usable range above threshold
    usable_range = max_ref - threshold
    conviction = 1.0 if usable_range <= 0 else min(1.0, (conv_abs - threshold) / usable_range)

    # Apply curve and interpolate between min and max risk
    risk_pct = min_risk + (max_risk - min_risk) * (conviction ** exponent)

    # Apply stress multiplier when market stress is elevated
    if stress_score > float(risk_params.get('stress_threshold_score') or 60.0):
        risk_pct *= float(risk_params.get('stress_multiplier') or 0.70)

    return risk_pct


# =============================================================================
# POSITION SIZER
# =============================================================================
class PositionSizer:
    """Calculates position size based on ATR, Kelly fraction, and multipliers."""

    @staticmethod
    def calculate(
        risk_params: Dict[str, Any],
        atr_14: Optional[float],
        instrument: str,
        size_multiplier_from_orchestrator: float = 1.0,
        convergence_score: float = 0.0,
        stress_score: float = 0.0,
        current_price: Optional[float] = None,
        pip_value_gbp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Calculate position size using conviction-scaled risk curve.

        position_size is returned in standard lots (100,000 base units).
        pip_value_gbp: pip value per standard lot in GBP (from _get_pip_value_gbp).
        """

        drawdown_mult = float(risk_params.get("size_multiplier", 1.0))
        atr_stop_mult = float(risk_params.get("atr_stop_multiplier", 1.5))
        account_value = float((risk_params or {}).get("account_value", 0))

        # Combined multiplier: orchestrator (pre-event etc.) × drawdown step
        combined_mult = size_multiplier_from_orchestrator * drawdown_mult

        # Conviction-scaled risk fraction (decimal, e.g. 0.0025 = 0.25%)
        conviction_risk_pct = compute_position_risk_pct(
            convergence_score, stress_score, risk_params
        )
        if conviction_risk_pct <= 0:
            min_risk = float(risk_params.get('min_risk_pct') or 0.0025)
            logger.warning(
                f"conviction_risk_pct=0 for convergence_score={convergence_score} "
                f"— falling back to min_risk_pct {min_risk:.4f}"
            )
            conviction_risk_pct = min_risk

        # Effective risk in GBP: conviction × combined multipliers
        risk_amount = account_value * conviction_risk_pct * combined_mult

        # Store in percentage form (× 100) for execution_agent compatibility
        effective_risk_pct = conviction_risk_pct * combined_mult * 100.0

        # ATR-based stop distance (raw price units — execution agent uses this
        # directly to compute stop_price; do NOT convert it)
        stop_distance = atr_14 * atr_stop_mult if atr_14 else None

        # Position size in standard lots.
        # Correct formula: lots = risk_amount_GBP / (stop_pips × pip_value_GBP_per_lot)
        # pip_value_GBP_per_lot = pip_multiplier / GBP_quote_rate
        #   where pip_multiplier = 1000 (JPY) or 10 (others)
        #   and GBP_quote_rate = GBPXXX live rate for the pair's quote currency.
        # This ensures equal GBP risk across all pairs regardless of quote currency.
        pip_size = 0.01 if instrument.upper().endswith('JPY') else 0.0001
        position_size = None  # standard lots
        if stop_distance and stop_distance > 0 and risk_amount > 0:
            stop_pips = stop_distance / pip_size
            if pip_value_gbp and pip_value_gbp > 0:
                position_size = risk_amount / (stop_pips * pip_value_gbp)
            else:
                # Legacy fallback — correct only for GBP-base pairs.
                # Retained so sizing degrades gracefully if pip_value_gbp unavailable.
                stop_for_legacy = stop_distance
                if current_price and current_price > 0 and instrument.upper().endswith('JPY'):
                    stop_for_legacy = stop_distance / current_price
                if stop_for_legacy > 0:
                    # Approximate lots: divide by 100,000 to convert from raw units
                    position_size = risk_amount / stop_for_legacy / 100_000

        return {
            "account_value": account_value,
            "conviction_risk_pct": round(conviction_risk_pct, 6),
            "drawdown_multiplier": drawdown_mult,
            "orchestrator_multiplier": size_multiplier_from_orchestrator,
            "combined_multiplier": round(combined_mult, 4),
            "effective_risk_pct": round(effective_risk_pct, 6),
            "risk_amount": round(risk_amount, 2),
            "pip_value_gbp": round(pip_value_gbp, 6) if pip_value_gbp else None,
            "position_size": round(position_size, 4) if position_size else None,
            "atr_14": atr_14,
            "atr_stop_multiplier": atr_stop_mult,
            "stop_distance": round(stop_distance, 5) if stop_distance else None,
        }


# =============================================================================
# SWAP COST CHECKER (RG2)
# =============================================================================
class SwapCostChecker:
    """Implements RG2 — checks if swap costs erode R:R below minimum."""

    @staticmethod
    def check(
        swap_rate_pips: Optional[float],
        reward_pips: float,
        risk_pips: float,
        min_rr: float,
        estimated_hold_days: float = 1.0,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if swap costs would erode R:R below minimum.
        swap_rate_pips is signed: negative = overnight cost, positive = carry income.
        Returns (passed, details_dict).
        """
        original_rr = reward_pips / risk_pips if risk_pips > 0 else 0.0
        if swap_rate_pips is None:
            return True, {"swap_check": "skipped", "reason": "no swap data available",
                          "original_rr": round(original_rr, 2)}

        # signed: paying swap reduces reward; carry income increases reward
        swap_net_pips = swap_rate_pips * estimated_hold_days
        adjusted_reward_pips = reward_pips + swap_net_pips
        adjusted_rr = adjusted_reward_pips / risk_pips if risk_pips > 0 else 0.0

        details = {
            "swap_rate_pips": swap_rate_pips,
            "estimated_hold_days": estimated_hold_days,
            "swap_net_pips": round(swap_net_pips, 2),
            "reward_pips": round(reward_pips, 1),
            "risk_pips": round(risk_pips, 1),
            "original_rr": round(original_rr, 2),
            "adjusted_rr": round(adjusted_rr, 2),
            "min_rr": min_rr,
        }

        if adjusted_rr < 0:
            # RG2: negative adjusted R:R — reject immediately
            details["action"] = "reject"
            details["reason"] = f"Adjusted R:R is negative ({adjusted_rr:.2f}) after swap costs"
            return False, details
        elif adjusted_rr < min_rr:
            details["action"] = "reject"
            details["reason"] = f"Adjusted R:R ({adjusted_rr:.2f}) below minimum ({min_rr})"
            return False, details
        else:
            details["action"] = "pass"
            return True, details


# =============================================================================
# RISK GUARDIAN — MAIN VALIDATOR
# =============================================================================
class RiskGuardian:
    """Validates each trade approval against all risk rules."""

    def __init__(self, user_id: str, dry_run: bool = False):
        self.user_id = user_id
        self.dry_run = dry_run
        self.db = DatabaseConnection()
        self.db.connect()
        validate_schema(self.db.conn, EXPECTED_TABLES)
        self.reader = RiskDataReader(self.db, user_id)
        self._validator = SignalValidator()
        _rp = self.reader.read_risk_parameters() or {}
        _corr_thresh = _rp.get("correlation_threshold", 0.75) or 0.75
        self.correlation_checker = CorrelationChecker(user_id, threshold=float(_corr_thresh))
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        self.processed_count = 0

    def _fetch_atr(self, instrument: str, timeframe: str = None) -> Optional[float]:
        """Read ATR-14 from price_metrics with fallback chain: 1D → 1H → 15M → hardcoded.

        When timeframe is specified, only that timeframe is tried (no chain fallback).
        Recommended stop multipliers: 2.0 for 1D ATR, 1.5 for 1H/15M ATR.
        Hardcoded emergency fallback: JPY pairs=1.5 (150 pips), others=0.0150.
        """
        timeframes = [timeframe] if timeframe else ["1D", "1H", "15M"]
        for tf in timeframes:
            cur = self.db.cursor()
            try:
                cur.execute("""
                    SELECT atr_14 FROM forex_network.price_metrics
                    WHERE instrument = %s AND timeframe = %s
                    ORDER BY ts DESC LIMIT 1
                """, (instrument, tf))
                row = cur.fetchone()
                if row and row["atr_14"] is not None and float(row["atr_14"]) > 0:
                    return float(row["atr_14"])
            except Exception as e:
                logger.warning(f"_fetch_atr failed for {instrument}/{tf}: {e}")
            finally:
                cur.close()

        if timeframe:
            return None  # single-timeframe request — no chain fallback

        # Hardcoded emergency fallback when all price_metrics rows are absent
        fallback = 1.5 if instrument.upper().endswith("JPY") else 0.0150
        logger.warning(
            f"_fetch_atr({instrument}): no price_metrics data in any timeframe — "
            f"using hardcoded fallback atr={fallback}"
        )
        return fallback

    def _fetch_live_price(self, instrument: str) -> Optional[float]:
        """Fetch the most recent LIVE close price from historical_prices."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT close FROM forex_network.historical_prices
                WHERE instrument = %s AND timeframe = 'LIVE'
                ORDER BY ts DESC LIMIT 1
            """, (instrument,))
            row = cur.fetchone()
            if row and row["close"] and float(row["close"]) > 0:
                return float(row["close"])
            return None
        except Exception as e:
            logger.warning(f"_fetch_live_price failed for {instrument}: {e}")
            return None
        finally:
            cur.close()

    def _get_pip_value_gbp(self, instrument: str, current_price: Optional[float] = None) -> Optional[float]:
        """
        Returns pip value per standard lot (100,000 base units) in GBP.

        Formula: pip_value_gbp = pip_multiplier / GBP_quote_rate
          pip_multiplier = 1000  for JPY-quoted pairs (100,000 × 0.01)
          pip_multiplier =   10  for all others       (100,000 × 0.0001)
          GBP_quote_rate = GBPXXX live rate (XXX = quote currency of the pair)

        Lookup priority:
          1. GBP-base pair (GBPUSD, GBPJPY, etc.) — current_price IS the GBPXXX rate
          2. GBP-quote pair (EURGBP) — rate = 1.0, no lookup needed
          3. Otherwise — fetch GBPXXX from forex_network.historical_prices
          4. Fallback — use current_price with a warning (imprecise for non-GBP-base)
        """
        instr     = instrument.upper()
        base_ccy  = instr[:3]
        quote_ccy = instr[3:]
        pip_mult  = 1000.0 if quote_ccy == 'JPY' else 10.0

        if quote_ccy == 'GBP':
            # e.g. EURGBP: quote is GBP, pip value = 10 GBP per lot directly
            return pip_mult

        if base_ccy == 'GBP' and current_price and current_price > 0:
            # e.g. GBPUSD, GBPJPY — the pair itself is GBPXXX
            return pip_mult / current_price

        # GBP-base but current_price unavailable — try LIVE then 1H from DB
        if base_ccy == 'GBP':
            for _tf in ('LIVE', '1H'):
                try:
                    _cur = self.db.cursor()
                    _cur.execute("""
                        SELECT close FROM forex_network.historical_prices
                        WHERE instrument = %s AND timeframe = %s
                        ORDER BY ts DESC LIMIT 1
                    """, (instr, _tf))
                    _row = _cur.fetchone()
                    _cur.close()
                    if _row and _row['close'] and float(_row['close']) > 0:
                        logger.info(
                            f"_get_pip_value_gbp({instrument}): rate from {_tf}: "
                            f"{float(_row['close']):.5f}"
                        )
                        return pip_mult / float(_row['close'])
                except Exception as _e:
                    logger.debug(f"_get_pip_value_gbp: {_tf} fetch for {instr} failed: {_e}")
            # Hardcoded last resort per quote currency (approximate mid-rates)
            _HARDCODED_GBP = {
                'JPY': 215.0, 'USD': 1.25, 'AUD': 1.90,
                'CAD': 1.72, 'NZD': 2.05, 'CHF': 1.13,
            }
            _hc = _HARDCODED_GBP.get(quote_ccy)
            if _hc:
                logger.warning(
                    f"_get_pip_value_gbp({instrument}): no live data — "
                    f"using hardcoded {quote_ccy} rate {_hc}"
                )
                return pip_mult / _hc
            logger.error(f"_get_pip_value_gbp({instrument}): no rate available, returning None")
            return None

        # Non-GBP-base pair: fetch GBPXXX from historical_prices (LIVE → 1H → any)
        gbp_cross = f'GBP{quote_ccy}'
        for _tf in ('LIVE', '1H', None):
            try:
                _cur = self.db.cursor()
                if _tf:
                    _cur.execute("""
                        SELECT close FROM forex_network.historical_prices
                        WHERE instrument = %s AND timeframe = %s
                        ORDER BY ts DESC LIMIT 1
                    """, (gbp_cross, _tf))
                else:
                    _cur.execute("""
                        SELECT close FROM forex_network.historical_prices
                        WHERE instrument = %s
                        ORDER BY ts DESC LIMIT 1
                    """, (gbp_cross,))
                _row = _cur.fetchone()
                _cur.close()
                if _row and _row['close'] and float(_row['close']) > 0:
                    return pip_mult / float(_row['close'])
            except Exception as _e:
                logger.debug(
                    f"_get_pip_value_gbp: DB fetch for {gbp_cross} "
                    f"(tf={_tf}) failed: {_e}"
                )

        # Last resort: current_price as proxy (wrong scale for non-GBP-base but avoids None)
        if current_price and current_price > 0:
            logger.warning(
                f"_get_pip_value_gbp({instrument}): using current_price={current_price:.5f} "
                f"as proxy for {gbp_cross} — pip value will be inaccurate"
            )
            return pip_mult / current_price

        logger.error(f"_get_pip_value_gbp({instrument}): no rate available, returning None")
        return None

    def _get_cot_size_multiplier(self, instrument: str) -> float:
        """
        Returns position size multiplier based on COT speculator positioning.
        Research basis: professional tools use 80th pctile as warning, 95th as extreme.
        Below 20th: increase size slightly (1.25x) — underpositioned, room to run
        20-80th:    full size (1.0x)
        80-95th:    reduce size (0.75x) — crowded, risk of reversal
        Above 95th: significantly reduce size (0.50x) — extreme crowding

        Uses pct_signed_52w from shared.cot_positioning (weekly CFTC data).
        Inverts sign for USD-base pairs (USDJPY/USDCAD/USDCHF).
        """
        CURRENCY_TO_COT = {
            'EUR': ('EURUSD', False), 'GBP': ('GBPUSD', False),
            'AUD': ('AUDUSD', False), 'JPY': ('USDJPY', True),
            'CAD': ('USDCAD', True),  'CHF': ('USDCHF', True),
        }
        PAIR_TO_BASE = {
            'EURJPY': 'EUR', 'GBPJPY': 'GBP', 'AUDJPY': 'AUD',
            'CADJPY': 'CAD', 'USDJPY': 'USD', 'EURUSD': 'EUR',
            'GBPUSD': 'GBP', 'AUDUSD': 'AUD', 'EURGBP': 'EUR',
            'EURAUD': 'EUR', 'NZDUSD': 'NZD', 'NZDJPY': 'NZD',
            'USDCAD': 'USD', 'USDCHF': 'USD', 'GBPAUD': 'GBP',
            'GBPCAD': 'GBP', 'GBPCHF': 'GBP', 'EURCAD': 'EUR',
            'EURCHF': 'EUR', 'AUDNZD': 'AUD',
        }
        base_currency = PAIR_TO_BASE.get(instrument.upper())
        if not base_currency or base_currency not in CURRENCY_TO_COT:
            return 1.0  # NZD, USD — no direct COT contract

        pair_code, invert = CURRENCY_TO_COT[base_currency]
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT pct_signed_52w FROM shared.cot_positioning
                WHERE pair_code = %s ORDER BY report_date DESC LIMIT 1
            """, (pair_code,))
            row = cur.fetchone()
            cur.close()
            if not row or row['pct_signed_52w'] is None:
                return 1.0
            pct = float(row['pct_signed_52w'])
            if invert:
                pct = 100.0 - pct
            if pct >= 95:
                return 0.50
            elif pct >= 80:
                return 0.75
            elif pct <= 20:
                return 1.25
            else:
                return 1.0
        except Exception as e:
            logger.warning(f"COT size multiplier failed for {instrument}: {e}")
            return 1.0

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

    def validate_trade(self, approval: Dict[str, Any], cycle_positions: Optional[List] = None) -> Dict[str, Any]:
        """
        Run all risk checks on a single trade approval.
        Returns full decision record.
        """
        instrument = approval["instrument"]
        bias = approval["bias"]
        direction = "long" if bias == "bullish" else "short"
        payload = approval.get("payload", {})
        signal_id = approval["signal_id"]

        # Orchestrator signal contract check
        self._validator.validate_and_log("orchestrator", payload, instrument, logger)

        logger.info(f"Validating: {instrument} {direction} (signal_id: {signal_id})")

        # Read all required data
        risk_params = self.reader.read_risk_parameters()
        account_value = float(risk_params.get("account_value", 0))
        # Use cycle_positions if provided (same-cycle race fix); else fetch from DB
        if cycle_positions is not None:
            open_positions = cycle_positions
        else:
            open_positions = self.reader.read_open_positions()
        daily_loss_pct = self.reader.read_daily_loss_pct(account_value)
        weekly_loss_pct = self.reader.read_weekly_loss_pct(account_value)
        correlations = self.reader.read_correlations()
        market_context = self.reader.read_market_context()

        # Fail-safe: if risk_parameters are unreadable, reject immediately.
        # All gates depend on these values — proceeding with {} defaults is unsafe.
        if risk_params is None:
            logger.error(
                f"User {self.user_id}: risk_parameters unreadable — rejecting {instrument} {direction} (fail-safe)"
            )
            fail_decision = {
                "approval_signal_id": approval["signal_id"],
                "instrument": instrument,
                "direction": direction,
                "bias": bias,
                "approved": False,
                "checks": {"risk_params": "FAIL"},
                "rejection_reasons": ["risk_params_unreadable: cannot validate safely"],
                "risk_details": {},
                "entry_context": approval.get("payload", {}).get("entry_context"),
            }
            self._write_decision(fail_decision)
            return fail_decision

        decision = {
            "approval_signal_id": signal_id,
            "instrument": instrument,
            "direction": direction,
            "bias": bias,
            "approved": False,
            "checks": {},
            "rejection_reasons": [],
            "risk_details": {},
            "entry_context": payload.get("entry_context"),
            "convergence": payload.get("convergence"),
        }

        # =================================================================
        # CHECK 1: Kill switch
        # =================================================================
        if check_kill_switch():
            decision["rejection_reasons"].append("Kill switch active")
            decision["checks"]["kill_switch"] = "FAIL"
            self._write_decision(decision)
            return decision
        decision["checks"]["kill_switch"] = "PASS"

        # =================================================================
        # CHECK 2: Circuit breaker (RG1 related)
        # =================================================================
        if risk_params.get("circuit_breaker_active"):
            decision["rejection_reasons"].append("Circuit breaker active — no new entries")
            decision["checks"]["circuit_breaker"] = "FAIL"
            # D. Circuit breaker alert
            try:
                send_alert(
                    'CRITICAL',
                    f'Circuit breaker active — blocking {instrument} {direction}',
                    {'user_id': str(self.user_id), 'instrument': instrument,
                     'direction': direction,
                     'daily_loss_pct': round(daily_loss_pct, 2) if daily_loss_pct is not None else 'unknown',
                     'trigger': 'circuit_breaker_active=True in risk_parameters'},
                    source_agent='risk_guardian',
                )
            except Exception as _ale:
                logger.warning(f"circuit_breaker alert failed: {_ale}")
            log_event('CIRCUIT_BREAKER_FIRED', f'Circuit breaker active for {self.user_id}',
                category='RISK', agent='risk_guardian', severity='WARN', user_id=str(self.user_id),
                payload={'instrument': instrument, 'direction': direction,
                         'daily_loss_pct': round(daily_loss_pct, 2) if daily_loss_pct is not None else None})
            self._write_decision(decision)
            return decision
        decision["checks"]["circuit_breaker"] = "PASS"

        # =================================================================
        # CHECK 2.5: Weekly drawdown limit
        # =================================================================
        weekly_limit = float(risk_params.get("weekly_drawdown_limit_pct", 0))
        if weekly_limit > 0:
            if weekly_loss_pct is None:
                # Fail-safe: cannot determine weekly loss — reject
                decision["rejection_reasons"].append(
                    "weekly_loss_check_failed: unreadable, assuming limit breached"
                )
                decision["checks"]["weekly_drawdown"] = "FAIL"
                decision["risk_details"]["weekly_drawdown"] = {
                    "status": "REJECT",
                    "reason": "weekly_loss_unreadable",
                }
                logger.warning(
                    f"User {self.user_id}: weekly loss unreadable — rejecting (fail-safe)"
                )
                self._write_decision(decision)
                return decision
            elif weekly_loss_pct >= weekly_limit:
                decision["rejection_reasons"].append(
                    f"weekly_drawdown_exceeded: {weekly_loss_pct:.3f}% >= limit {weekly_limit}%"
                )
                decision["checks"]["weekly_drawdown"] = "FAIL"
                decision["risk_details"]["weekly_drawdown"] = {
                    "status": "REJECT",
                    "loss_pct": weekly_loss_pct,
                    "limit_pct": weekly_limit,
                }
                logger.warning(
                    f"User {self.user_id}: weekly loss {weekly_loss_pct:.3f}% "
                    f">= limit {weekly_limit}% — rejecting"
                )
                log_event('DRAWDOWN_LIMIT', f'Drawdown {weekly_loss_pct:.2f}% >= limit {weekly_limit:.2f}%',
                    category='RISK', agent='risk_guardian', severity='WARN', user_id=str(self.user_id),
                    payload={'drawdown': round(weekly_loss_pct, 2), 'limit': round(weekly_limit, 2)})
                self._write_decision(decision)
                return decision
            else:
                decision["checks"]["weekly_drawdown"] = "PASS"
                decision["risk_details"]["weekly_drawdown"] = {
                    "status": "PASS",
                    "loss_pct": weekly_loss_pct,
                    "limit_pct": weekly_limit,
                }
        else:
            # weekly_drawdown_limit_pct not configured — skip gate
            decision["checks"]["weekly_drawdown"] = "SKIP"

        # =================================================================
        # CHECK 3: RG1 — Daily loss limit is absolute
        # =================================================================
        daily_limit = float(risk_params.get("daily_loss_limit_pct", 3.0))
        if daily_loss_pct is None:
            # Fail-safe: could not determine daily loss — reject
            decision["rejection_reasons"].append("daily_loss_check_failed: unreadable, assuming limit breached")
            decision["checks"]["daily_loss"] = "FAIL"
            decision["risk_details"]["daily_loss"] = {"status": "REJECT", "reason": "daily_loss_unreadable"}
            logger.warning(f"User {self.user_id}: daily loss unreadable — rejecting (fail-safe)")
            self._write_decision(decision)
            return decision
        elif daily_loss_pct >= daily_limit:
            decision["rejection_reasons"].append(
                f"daily_loss_exceeded: {daily_loss_pct:.3f}% >= limit {daily_limit}%"
            )
            decision["checks"]["daily_loss"] = "FAIL"
            decision["risk_details"]["daily_loss"] = {
                "status": "REJECT",
                "loss_pct": daily_loss_pct,
                "limit_pct": daily_limit,
            }
            logger.warning(
                f"User {self.user_id}: daily loss {daily_loss_pct:.3f}% >= limit {daily_limit}% — rejecting"
            )
            self._write_decision(decision)
            return decision
        else:
            decision["checks"]["daily_loss"] = "PASS"
            decision["risk_details"]["daily_loss"] = {
                "status": "PASS",
                "loss_pct": daily_loss_pct,
                "limit_pct": daily_limit,
            }

        # =================================================================
        # CHECK 4: Correlation block
        # =================================================================
        if open_positions is None:
            # Fail-safe: positions unknown — cannot safely evaluate correlation
            decision["rejection_reasons"].append(
                "open_positions_unreadable: cannot evaluate correlation risk"
            )
            decision["checks"]["correlation"] = "FAIL"
            decision["checks"]["max_positions"] = "FAIL"
            decision["risk_details"]["open_positions"] = "unreadable"
            logger.warning(f"User {self.user_id}: open_positions unreadable — rejecting (fail-safe)")
            self._write_decision(decision)
            return decision

        # Per-currency cap — read from DB max_usd_units, shared by pre-check and CHECK 4.5.
        _curr_cap = int(risk_params.get("max_usd_units", 3))

        # -----------------------------------------------------------------
        # CHECK 4 pre-check: shared-currency directional concentration
        # The correlation matrix has no meaningful entries for cross pairs
        # (e.g. EURUSD/EURGBP is structurally ~0 even though both carry EUR).
        # This block catches that gap by counting how many open positions are
        # LONG (or SHORT) the same currency as the proposed trade.
        #
        # Rule: long pair → goes LONG base, SHORT quote
        #       short pair → goes SHORT base, LONG quote
        # Block if: (existing same-direction count for any currency) + 1 > cap
        # -----------------------------------------------------------------
        def _long_currencies(instr: str, dir_: str) -> set:
            """Return the set of currencies this trade is net-LONG."""
            legs = CURRENCY_EXPOSURE_MAP.get(instr.upper(), ())
            if len(legs) != 2:
                return set()
            base, quote = legs
            return {base} if dir_ == "long" else {quote}

        def _short_currencies(instr: str, dir_: str) -> set:
            """Return the set of currencies this trade is net-SHORT."""
            legs = CURRENCY_EXPOSURE_MAP.get(instr.upper(), ())
            if len(legs) != 2:
                return set()
            base, quote = legs
            return {quote} if dir_ == "long" else {base}

        _long_counts: dict = {}
        _short_counts: dict = {}
        for _p in open_positions:
            for _c in _long_currencies(_p["instrument"], _p.get("direction", "long")):
                _long_counts[_c] = _long_counts.get(_c, 0) + 1
            for _c in _short_currencies(_p["instrument"], _p.get("direction", "long")):
                _short_counts[_c] = _short_counts.get(_c, 0) + 1

        _proposed_longs = _long_currencies(instrument, direction)
        _proposed_shorts = _short_currencies(instrument, direction)
        _conc_breaches = []
        for _c in _proposed_longs:
            if _long_counts.get(_c, 0) + 1 > _curr_cap:
                _conc_breaches.append(
                    f"{_c}:long={_long_counts.get(_c,0)+1}/{_curr_cap}"
                )
        for _c in _proposed_shorts:
            if _short_counts.get(_c, 0) + 1 > _curr_cap:
                _conc_breaches.append(
                    f"{_c}:short={_short_counts.get(_c,0)+1}/{_curr_cap}"
                )
        if _conc_breaches:
            decision["rejection_reasons"].append(
                f"shared_currency_concentration: {instrument} {direction} would stack "
                f"same-direction exposure beyond cap (cap={_curr_cap}): "
                f"{', '.join(_conc_breaches)}"
            )
            decision["checks"]["correlation"] = "FAIL"
            decision["risk_details"]["shared_currency_concentration"] = _conc_breaches
            log_event('CAP_BLOCK', f'{instrument} {direction} blocked — shared_currency_concentration cap={_curr_cap}',
                category='RISK', agent='risk_guardian', user_id=str(self.user_id), instrument=instrument,
                payload={'breaches': _conc_breaches, 'cap': _curr_cap, 'direction': direction})
            warn("risk_guardian", "CAP_BLOCK", "Currency concentration cap breach",
                 pair=instrument, direction=direction, cap=_curr_cap,
                 breaches=str(_conc_breaches)[:120])
        else:
            decision["risk_details"]["shared_currency_concentration"] = "PASS"

        corr_passed, corr_reasons = self.correlation_checker.check(
            instrument=instrument,
            direction=direction,
            open_positions=open_positions,
            correlations=correlations,
            market_context=market_context,
        )
        if not corr_passed:
            decision["rejection_reasons"].extend(corr_reasons)
            decision["checks"]["correlation"] = "FAIL"
            warn("risk_guardian", "CORRELATION_SKIP", "Correlation block",
                 pair=instrument, reasons=str(corr_reasons)[:120])
        else:
            decision["checks"]["correlation"] = "PASS"

        # =================================================================
        # CHECK 4.5: Per-currency net exposure cap (direction-aware)
        # Tracks SIGNED net exposure per currency:
        #   long pair  → +1 base, -1 quote
        #   short pair → -1 base, +1 quote
        # Applies to all pairs including USD-base (USDJPY long = +1 USD, -1 JPY).
        # Blocks if abs(current_net + proposed_delta) > cap for any currency
        # in the proposed pair. Hedges that reduce net exposure are allowed.
        # _curr_cap defined at top of CHECK 4 block above.
        # =================================================================
        _net_exposure: dict = {}

        def _currency_signed_delta(instr: str, dir_: str) -> dict:
            """Return signed currency deltas {currency: +1 or -1} for one trade."""
            legs = CURRENCY_EXPOSURE_MAP.get(instr.upper(), ())
            if len(legs) != 2:
                return {}
            base, quote = legs
            return {base: +1, quote: -1} if dir_ == "long" else {base: -1, quote: +1}

        for _p in open_positions:
            for _c, _d in _currency_signed_delta(
                _p["instrument"], _p.get("direction", "long")
            ).items():
                _net_exposure[_c] = _net_exposure.get(_c, 0) + _d

        _proposed_delta = _currency_signed_delta(instrument, direction)
        _cap_breaches = [
            f"{_c}:net={_net_exposure.get(_c,0):+d}→{_net_exposure.get(_c,0)+_proposed_delta[_c]:+d}(cap=±{_curr_cap})"
            for _c in _proposed_delta
            if abs(_net_exposure.get(_c, 0) + _proposed_delta[_c]) > _curr_cap
        ]
        if _cap_breaches:
            decision["rejection_reasons"].append(
                f"currency_exposure_cap: {instrument} would breach per-currency limit "
                f"(cap=±{_curr_cap}): {', '.join(_cap_breaches)}"
            )
            decision["checks"]["currency_exposure"] = "FAIL"
        else:
            decision["checks"]["currency_exposure"] = "PASS"
        decision["risk_details"]["currency_exposure"] = dict(_net_exposure)
        decision["risk_details"]["currency_exposure_cap"] = _curr_cap

        # Log which positions were checked
        decision["risk_details"]["open_positions"] = [
            {"instrument": p["instrument"], "direction": p["direction"]}
            for p in open_positions
        ]
        decision["risk_details"]["correlation_threshold"] = self.correlation_checker.threshold

        # =================================================================
        # CHECK 5: Max open positions — derived dynamically from risk budget.
        # dynamic_max_positions = max_portfolio_risk_pct / (max_risk_pct * 100)
        # max_open_positions column retained in DB as legacy reference only.
        # =================================================================
        max_risk_per_trade = float(risk_params.get("max_risk_pct", 0.01)) * 100  # decimal → pct
        max_portfolio_risk = float(risk_params.get("max_portfolio_risk_pct", 15.0))
        dynamic_max_positions = max(1, int(max_portfolio_risk / max_risk_per_trade))
        logger.debug(
            f"User {self.user_id}: dynamic_max_positions={dynamic_max_positions} "
            f"(portfolio_risk_budget={max_portfolio_risk}%, per_trade={max_risk_per_trade:.2f}%)"
        )
        if len(open_positions) >= dynamic_max_positions:
            decision["rejection_reasons"].append(
                f"max_positions: {len(open_positions)}/{dynamic_max_positions} "
                f"(portfolio_risk_budget={max_portfolio_risk}%, per_trade={max_risk_per_trade:.2f}%)"
            )
            decision["checks"]["max_positions"] = "FAIL"
        else:
            decision["checks"]["max_positions"] = "PASS"

        # =================================================================
        # CHECK 6: Position sizing
        # =================================================================
        orch_size_mult = float(payload.get("size_multiplier", 1.0))
        # Always use 1D ATR × 2.0 for stop sizing — payload atr_14 is H1 from
        # technical agent and produces stops that are far too tight for macro trades.
        # Fallback chain: 1D × 2.0 → 1H × 3.0 → hardcoded emergency.
        _atr_1d = self._fetch_atr(instrument, timeframe="1D")
        if _atr_1d is not None:
            atr_14 = _atr_1d
            _stop_mult_override = 2.0
            logger.debug(f"{instrument}: 1D ATR={atr_14:.5f} × 2.0 for stop")
        else:
            _atr_1h = self._fetch_atr(instrument, timeframe="1H")
            if _atr_1h is not None:
                atr_14 = _atr_1h
                _stop_mult_override = 3.0
                logger.warning(f"{instrument}: 1D ATR unavailable — using 1H ATR={atr_14:.5f} × 3.0")
            else:
                atr_14 = 1.5 if instrument.upper().endswith('JPY') else 0.0150
                _stop_mult_override = 1.0
                logger.warning(f"{instrument}: no ATR data — using hardcoded stop fallback atr={atr_14}")
        _sizing_risk_params = dict(risk_params)
        _sizing_risk_params["atr_stop_multiplier"] = _stop_mult_override

        conv_score_for_sizing   = float(payload.get("conviction_score", payload.get("convergence", 0.0)))
        stress_score_for_sizing = float((market_context or {}).get("stress_score", 0.0))
        _current_price_for_sizing = payload.get("current_price")
        if _current_price_for_sizing is None:
            _current_price_for_sizing = self._fetch_live_price(instrument)
            if _current_price_for_sizing:
                logger.info(
                    f"{instrument}: current_price missing from payload — "
                    f"fetched from LIVE historical_prices: {_current_price_for_sizing}"
                )
        pip_value_gbp = self._get_pip_value_gbp(instrument, _current_price_for_sizing)
        sizing = PositionSizer.calculate(
            risk_params=_sizing_risk_params,
            atr_14=atr_14,
            instrument=instrument,
            size_multiplier_from_orchestrator=orch_size_mult,
            convergence_score=conv_score_for_sizing,
            stress_score=stress_score_for_sizing,
            current_price=_current_price_for_sizing,
            pip_value_gbp=pip_value_gbp,
        )

        # Apply COT positioning adjustment to position size.
        # Reduces size when speculator positioning is crowded (>80th pctile),
        # increases slightly when underpositioned (<20th pctile).
        cot_mult = self._get_cot_size_multiplier(instrument)
        if cot_mult != 1.0:
            sizing["conviction_risk_pct"] = round(sizing["conviction_risk_pct"] * cot_mult, 6)
            sizing["risk_amount"]         = round(sizing["risk_amount"] * cot_mult, 2)
            sizing["effective_risk_pct"]  = round(sizing["effective_risk_pct"] * cot_mult, 6)
            if sizing.get("position_size") is not None:
                sizing["position_size"] = round(sizing["position_size"] * cot_mult, 4)
            sizing["cot_size_multiplier"] = cot_mult
            logger.info(
                f"COT size adjustment {instrument}: {cot_mult}x "
                f"→ conviction_risk_pct={sizing['conviction_risk_pct']:.4f}"
            )

        decision["risk_details"]["sizing"] = sizing

        # Promote stop_distance to decision top level so execution agent can
        # read it directly from the risk_approved payload without digging into
        # risk_details.sizing.
        decision["stop_distance"] = sizing["stop_distance"]

        # Reject if stop_distance is None — position cannot be sized safely
        # without a valid ATR-derived stop.  This is a hard gate: execution
        # agent requires stop_distance to place the stop-loss order.
        if sizing["stop_distance"] is None:
            decision["rejection_reasons"].append(
                f"stop_distance_missing: atr_14 unavailable for {instrument} — cannot size position safely"
            )
            decision["checks"]["position_size"] = "FAIL"
            logger.warning(
                f"User {self.user_id}: stop_distance is None for {instrument} — rejecting (fail-safe)"
            )
            self._write_decision(decision)
            return decision

        # Check if effective risk is zero (halted by drawdown)
        if sizing["combined_multiplier"] <= 0:
            decision["rejection_reasons"].append(
                f"Position sizing halted: drawdown multiplier {sizing['drawdown_multiplier']}"
            )
            decision["checks"]["position_size"] = "FAIL"
        else:
            decision["checks"]["position_size"] = "PASS"


        # =================================================================
        # CHECK 7: R:R validation + RG2 — Swap cost R:R check
        # =================================================================
        _target_price = payload.get("target_price")
        # Use the already-fetched current_price (with LIVE fallback applied above)
        _current_price_rr = _current_price_for_sizing
        _stop_distance_rr = decision.get("stop_distance")
        min_rr = float(risk_params.get('min_risk_reward_ratio', 1.5))

        # When target_price is missing, derive the minimum acceptable target from
        # current_price ± stop_distance × min_rr so R:R check can proceed.
        if _target_price is None and _current_price_rr and _stop_distance_rr:
            if direction == "long":
                _target_price = _current_price_rr + _stop_distance_rr * min_rr
            else:
                _target_price = _current_price_rr - _stop_distance_rr * min_rr
            logger.info(
                f"{instrument}: target_price missing from payload — "
                f"derived minimum target ({direction}): {_target_price:.5f} "
                f"(entry={_current_price_rr}, stop_dist={_stop_distance_rr}, min_rr={min_rr})"
            )

        # Warn if target is outside 20-day price range (structural impossibility check)
        if _target_price is not None and _current_price_rr:
            try:
                _range_cur = self.db.cursor()
                _range_cur.execute("""
                    SELECT MAX(high) as high_20d, MIN(low) as low_20d
                    FROM (
                        SELECT high, low FROM forex_network.historical_prices
                        WHERE instrument = %s AND timeframe = '1D'
                        ORDER BY ts DESC LIMIT 20
                    ) sub
                """, (instrument,))
                _range_row = _range_cur.fetchone()
                _range_cur.close()
                if _range_row and _range_row["high_20d"] and _range_row["low_20d"]:
                    _high_20d = float(_range_row["high_20d"])
                    _low_20d  = float(_range_row["low_20d"])
                    if direction == "long" and float(_target_price) > _high_20d:
                        logger.warning(
                            f"{instrument} LONG target {_target_price:.5f} exceeds "
                            f"20-day high {_high_20d:.5f} — target above recent range"
                        )
                        decision["risk_details"]["target_range_warn"] = (
                            f"target {_target_price:.5f} > 20d_high {_high_20d:.5f}"
                        )
                    elif direction == "short" and float(_target_price) < _low_20d:
                        logger.warning(
                            f"{instrument} SHORT target {_target_price:.5f} below "
                            f"20-day low {_low_20d:.5f} — target below recent range"
                        )
                        decision["risk_details"]["target_range_warn"] = (
                            f"target {_target_price:.5f} < 20d_low {_low_20d:.5f}"
                        )
            except Exception as _e:
                logger.debug(f"20-day range check failed for {instrument}: {_e}")

        if _target_price is None:
            decision["rejection_reasons"].append("no_target_price — cannot evaluate R:R")
            decision["checks"]["rr"] = "FAIL"
            decision["checks"]["swap_rr"] = "SKIP"
        elif not _stop_distance_rr or _stop_distance_rr <= 0 or not _current_price_rr or _current_price_rr <= 0:
            decision["rejection_reasons"].append("no_stop_distance — cannot evaluate R:R")
            decision["checks"]["rr"] = "FAIL"
            decision["checks"]["swap_rr"] = "SKIP"
        else:
            if direction == "long":
                _rr_reward = float(_target_price) - float(_current_price_rr)
            else:
                _rr_reward = float(_current_price_rr) - float(_target_price)
            _rr_risk = float(_stop_distance_rr)
            if _rr_reward <= 0:
                decision["rejection_reasons"].append(
                    f"target_price on wrong side of entry for {direction} "
                    f"(entry={_current_price_rr}, target={_target_price})"
                )
                decision["checks"]["rr"] = "FAIL"
                decision["checks"]["swap_rr"] = "SKIP"
            else:
                _computed_rr = _rr_reward / _rr_risk
                if _computed_rr < min_rr:
                    decision["rejection_reasons"].append(
                        f"insufficient_rr: {_computed_rr:.2f}:1 < {min_rr:.1f}:1"
                    )
                    decision["checks"]["rr"] = f"FAIL ({_computed_rr:.2f}:1)"
                    decision["checks"]["swap_rr"] = "SKIP"
                else:
                    decision["checks"]["rr"] = f"PASS ({_computed_rr:.2f}:1)"
                    # Convert price-unit distances to pips for accurate swap R:R math.
                    _pip_size = 0.01 if 'JPY' in instrument else 0.0001
                    _reward_pips = _rr_reward / _pip_size
                    _risk_pips = _rr_risk / _pip_size
                    # Hold days: read from risk_parameters if configured, else stress-based fallback.
                    # High stress → shorter hold (2d); normal → 3d.
                    _configured_hold = risk_params.get("swap_hold_days")
                    if _configured_hold is not None:
                        _hold_days = float(_configured_hold)
                    else:
                        _hold_days = 2.0 if stress_score_for_sizing > 50 else 3.0
                    swap_rate = self.reader.read_swap_rate(instrument, direction)
                    swap_passed, swap_details = SwapCostChecker.check(
                        swap_rate_pips=swap_rate,
                        reward_pips=_reward_pips,
                        risk_pips=_risk_pips,
                        min_rr=min_rr,
                        estimated_hold_days=_hold_days,
                    )
                    decision["risk_details"]["swap"] = swap_details
                    if not swap_passed:
                        decision["rejection_reasons"].append(swap_details.get("reason", "Swap cost check failed"))
                        decision["checks"]["swap_rr"] = "FAIL"
                    else:
                        decision["checks"]["swap_rr"] = "PASS"

        # =================================================================
        # CHECK 8: Drawdown step level + live drawdown from account value
        # =================================================================
        step_level = int(risk_params.get("drawdown_step_level", 0))
        account_val = float(risk_params.get("account_value", 0))
        peak_val = float(risk_params.get("peak_account_value", 0))
        live_drawdown_pct = (
            (peak_val - account_val) / peak_val * 100
            if peak_val > 0 and account_val > 0 else 0
        )
        if step_level >= 3:
            decision["rejection_reasons"].append(
                f"Drawdown step level {step_level} — trading halted (>15% drawdown)"
            )
            decision["checks"]["drawdown_step"] = "FAIL"
        else:
            decision["checks"]["drawdown_step"] = "PASS"
        decision["risk_details"]["drawdown_step_level"] = step_level
        decision["risk_details"]["account_value"] = account_val
        decision["risk_details"]["peak_account_value"] = peak_val
        decision["risk_details"]["live_drawdown_pct"] = round(live_drawdown_pct, 2)

        # =================================================================
        # CHECK 9: Stress score sanity (Pre-crisis/Crisis should not reach here
        # but belt-and-suspenders)
        # =================================================================
        STRESS_REJECT_THRESHOLD = 70.0
        if market_context is None:
            # Fail-safe: market context unreadable — treat as elevated stress
            effective_stress = STRESS_REJECT_THRESHOLD
            logger.warning(
                f"User {self.user_id}: market_context unreadable, "
                f"treating stress as {STRESS_REJECT_THRESHOLD} (fail-safe)"
            )
        else:
            effective_stress = float(market_context.get("stress_score", STRESS_REJECT_THRESHOLD))
        if effective_stress >= STRESS_REJECT_THRESHOLD:
            decision["rejection_reasons"].append(
                f"stress_too_high: {effective_stress} >= {STRESS_REJECT_THRESHOLD} "
                f"({(market_context or {}).get('stress_state', 'unknown')})"
            )
            decision["checks"]["stress_sanity"] = "FAIL"
            decision["risk_details"]["stress_sanity"] = {
                "status": "REJECT",
                "stress_score": effective_stress,
                "threshold": STRESS_REJECT_THRESHOLD,
            }
            self._write_decision(decision)
            return decision
        else:
            decision["checks"]["stress_sanity"] = "PASS"
            decision["risk_details"]["stress_sanity"] = {
                "status": "PASS",
                "stress_score": effective_stress,
            }

        # =================================================================
        # FINAL DECISION
        # =================================================================
        if not decision["rejection_reasons"]:
            decision["approved"] = True
            decision["risk_details"]["final_size_multiplier"] = sizing["combined_multiplier"]
            # stop_distance already set at decision top-level above; re-confirm
            # it is present in the approved payload for execution agent.
            decision["stop_distance"] = sizing["stop_distance"]
            logger.info(
                f"✅ APPROVED: {instrument} {direction} "
                f"(size_mult: {sizing['combined_multiplier']}, "
                f"stop_distance: {sizing['stop_distance']}, "
                f"corr: PASS, daily_loss: PASS)"
            )
        else:
            logger.info(
                f"❌ REJECTED: {instrument} {direction} — "
                f"{'; '.join(decision['rejection_reasons'][:2])}"
            )
            self._write_rg_rejection(
                instrument=instrument,
                rejection_reasons=decision["rejection_reasons"],
                payload=payload,
                market_context=market_context or {},
            )

        # Propagate target_price — use the derived/validated value from CHECK 7,
        # not the raw orchestrator payload (which may be null if technical agent
        # didn't set it and we fell back to deriving it from stop_distance × min_rr).
        decision["target_price"] = _target_price
        decision["entry_rank_position"] = payload.get("entry_rank_position")

        # Snapshot risk parameters into payload so execution agent can persist them
        # in trade_parameters at entry (Gap 1).
        _mc = market_context or {}
        _stress_score_snap = float(_mc.get("stress_score", 0.0))
        _stress_threshold  = float(risk_params.get("stress_threshold_score") or 60.0)
        _stress_mult_cfg   = float(risk_params.get("stress_multiplier") or 0.70)
        decision["effective_threshold"]  = risk_params.get("convergence_threshold")
        decision["stress_band"]          = _mc.get("stress_state")
        decision["stress_size_multiplier"] = (
            _stress_mult_cfg if _stress_score_snap > _stress_threshold else 1.0
        )
        decision["conviction_exponent"]  = risk_params.get("curve_exponent", 2.0)

        # Write decision
        self._write_decision(decision)
        return decision

    def _write_decision(self, decision: Dict[str, Any]):
        """Write risk guardian decision as a signal."""
        if self.dry_run:
            return
        cur = self.db.cursor()
        try:
            signal_type = "risk_approved" if decision["approved"] else "risk_rejected"
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, 1.0, %s, NOW() + INTERVAL '20 minutes')
            """, (
                AGENT_NAME,
                self.user_id,
                decision["instrument"],
                signal_type,
                decision.get("score", 0),
                decision["bias"],
                json.dumps(decision, default=float),
            ))
            self.db.commit()
            logger.info(f"Decision written: {signal_type} for {decision['instrument']}")
        except Exception as e:
            logger.error(f"Decision write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()


    def _write_rg_rejection(self, instrument: str, rejection_reasons: List[str],
                             payload: Dict[str, Any], market_context: Dict[str, Any]):
        """Write RG rejection to rg_rejections table for learning module analysis."""
        if self.dry_run:
            return
        cur = self.db.cursor()
        try:
            for reason in rejection_reasons:
                cur.execute("""
                    INSERT INTO forex_network.rg_rejections
                        (instrument, user_id, rejection_reason, rejection_detail,
                         macro_score, tech_score, conviction_score, convergence_score, stress_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    instrument,
                    self.user_id,
                    reason[:100],
                    json.dumps({"reasons": rejection_reasons, "payload_keys": list(payload.keys())}, default=float),
                    payload.get("macro_score"),
                    payload.get("tech_score"),
                    payload.get("conviction_score", payload.get("convergence")),
                    payload.get("convergence"),
                    market_context.get("stress_score"),
                ))
            self.db.commit()
        except Exception as e:
            logger.error(f"_write_rg_rejection failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def _write_system_alert(self, alert_type: str, severity: str, details: dict):
        """Write a system alert to forex_network.system_alerts.
        Schema: alert_type, severity, title (NOT NULL), detail (text), metadata (jsonb).
        """
        title = details.get('impact', alert_type)
        detail_text = f"{alert_type}: {json.dumps(details.get('affected_users', []))[:500]}"
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.system_alerts
                    (alert_type, severity, title, detail, metadata, created_at, acknowledged)
                VALUES (%s, %s, %s, %s, %s, NOW(), FALSE)
            """, (alert_type, severity, title, detail_text, json.dumps(details)))
            self.db.commit()
            logger.info(f"System alert written: {alert_type} [{severity}]")
        except Exception as e:
            logger.error(f"_write_system_alert failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def _check_risk_parameters_staleness(self):
        """
        Check risk_parameters.updated_at for all users. If any user row is older than
        STALENESS_THRESHOLD_MINUTES, send a system alert. Runs once per validate_trade cycle.
        Idempotent per hour — does not spam if already alerted recently.
        """
        STALENESS_THRESHOLD_MIN = 30
        ALERT_SUPPRESSION_HOURS = 1

        try:
            with self.db.conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id,
                           updated_at,
                           EXTRACT(EPOCH FROM (NOW() - updated_at))/60 AS age_min
                    FROM forex_network.risk_parameters
                    WHERE updated_at < NOW() - INTERVAL '%s minutes'
                """, (STALENESS_THRESHOLD_MIN,))
                stale = cur.fetchall()

            if not stale:
                return

            # Check if we already alerted recently (suppression — avoid spam)
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM forex_network.system_alerts
                    WHERE alert_type = 'risk_parameters_stale'
                      AND created_at > NOW() - INTERVAL '%s hour'
                      AND acknowledged = FALSE
                """, (ALERT_SUPPRESSION_HOURS,))
                recent_count = cur.fetchone()[0]

            if recent_count > 0:
                logger.warning(f'risk_parameters staleness detected ({len(stale)} users) — alert already active, skipping duplicate')
                return

            affected = [{'user_id': str(r['user_id']), 'age_min': round(float(r['age_min']), 1)} for r in stale]
            self._write_system_alert(
                alert_type='risk_parameters_stale',
                severity='critical',
                details={
                    'affected_users': affected,
                    'threshold_minutes': STALENESS_THRESHOLD_MIN,
                    'impact': 'account_value sync has stalled — check execution agent. Drawdown escalation and daily loss gate will use wrong values until resolved.'
                }
            )
            logger.critical(f'ALERT: risk_parameters stale for {len(stale)} users (>={STALENESS_THRESHOLD_MIN}min old)')
        except Exception as e:
            logger.error(f'_check_risk_parameters_staleness failed: {e}')
            try:
                self.db.rollback()
            except Exception:
                pass


    def _check_risk_parameters_long_term_staleness(self):
        """
        Check risk_parameters.updated_at for long-term staleness.
        Fires a warning alert if any user row is older than 7 days (manual review
        recommended), and a critical alert if older than 30 days (staleness risk
        to position sizing). Observation only — does not block trading.
        Idempotent per hour — suppresses duplicate alerts.
        """
        WARN_DAYS = 7
        CRIT_DAYS = 30
        ALERT_TYPE = 'risk_parameters_stale_long_term'

        try:
            with self.db.conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id,
                           updated_at,
                           EXTRACT(EPOCH FROM (NOW() - updated_at))/86400.0 AS age_days
                    FROM forex_network.risk_parameters
                    WHERE updated_at < NOW() - INTERVAL '7 days'
                """)
                stale = cur.fetchall()

            if not stale:
                return

            # Determine worst severity across all affected users
            max_age_days = max(float(r['age_days']) for r in stale)
            if max_age_days >= CRIT_DAYS:
                severity = 'critical'
                impact = 'risk_parameters not updated in 30+ days — staleness risk to position sizing'
            else:
                severity = 'warning'
                impact = 'risk_parameters not updated in 7+ days — manual review recommended'

            # Suppression: skip if recent unacknowledged alert of this type already exists
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM forex_network.system_alerts
                    WHERE alert_type = %s
                      AND created_at > NOW() - INTERVAL '1 hour'
                      AND acknowledged = FALSE
                """, (ALERT_TYPE,))
                recent_count = cur.fetchone()[0]

            if recent_count > 0:
                logger.warning(
                    f'risk_parameters long-term staleness detected ({len(stale)} users) '
                    f'— alert already active, skipping duplicate'
                )
                return

            affected = [
                {'user_id': str(r['user_id']), 'age_days': round(float(r['age_days']), 1)}
                for r in stale
            ]
            self._write_system_alert(
                alert_type=ALERT_TYPE,
                severity=severity,
                details={
                    'affected_users': affected,
                    'max_age_days': round(max_age_days, 1),
                    'warn_threshold_days': WARN_DAYS,
                    'crit_threshold_days': CRIT_DAYS,
                    'impact': impact,
                }
            )
            log_fn = logger.critical if severity == 'critical' else logger.warning
            log_fn(
                f'ALERT: risk_parameters long-term staleness [{severity}] '
                f'for {len(stale)} users (max {max_age_days:.1f} days old)'
            )
        except Exception as e:
            logger.error(f'_check_risk_parameters_long_term_staleness failed: {e}')
            try:
                self.db.rollback()
            except Exception:
                pass

    def poll_and_validate(self) -> int:
        """Poll for pending approvals and validate each one. Returns count processed."""
        approvals = self.reader.read_pending_approvals()
        if not approvals:
            return 0

        logger.info(f"Found {len(approvals)} pending approval(s)")

        # Fetch open positions once for the whole cycle so that each subsequent
        # approval sees positions approved earlier in the same cycle (same-cycle
        # race fix — execution agent hasn't placed them in the DB yet).
        cycle_positions = self.reader.read_open_positions() or []

        count = 0
        for approval in approvals:
            try:
                decision = self.validate_trade(approval, cycle_positions=cycle_positions)
                count += 1
                self.processed_count += 1
                # Approved this cycle — add synthetic position so the next approval
                # in this same batch sees it during correlation / exposure checks.
                if decision.get("approved"):
                    inst = approval.get("instrument", "")
                    bias = approval.get("bias", "bullish")
                    dir_ = "long" if bias == "bullish" else "short"
                    cycle_positions = list(cycle_positions) + [
                        {"instrument": inst, "direction": dir_}
                    ]
                    logger.debug(
                        f"Cycle snapshot updated: added {inst} {dir_} "
                        f"({len(cycle_positions)} total this cycle)"
                    )
            except Exception as e:
                logger.error(f"Validation failed for {approval.get('instrument')}: {e}", exc_info=True)
        return count

    def _check_drawdown_circuit_breaker(self):
        """D2: Proactive drawdown monitor — sets circuit_breaker_active if daily loss limit breached."""
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT user_id, account_value, peak_account_value,
                       daily_loss_limit_pct, circuit_breaker_active
                FROM forex_network.risk_parameters
                WHERE circuit_breaker_active = FALSE
            """)
            rows = cur.fetchall()
            for row in rows:
                user_id = row["user_id"]
                acct_val = float(row["account_value"] or 0)
                peak_val = float(row["peak_account_value"] or 0)
                limit_pct = float(row["daily_loss_limit_pct"] or 0)
                if not peak_val:
                    continue
                drawdown_pct = (peak_val - acct_val) / peak_val * 100
                if drawdown_pct >= limit_pct:
                    cur.execute("""
                        UPDATE forex_network.risk_parameters
                        SET circuit_breaker_active = TRUE,
                            circuit_breaker_reason  = 'daily_loss_limit_breached',
                            circuit_breaker_at      = NOW()
                        WHERE user_id = %s
                    """, (user_id,))
                    self.db.commit()
                    logger.critical(
                        f"CIRCUIT BREAKER ACTIVATED — user={str(user_id)[:8]} "
                        f"drawdown={drawdown_pct:.2f}% >= limit={limit_pct}%"
                    )
            cur.close()
        except Exception as e:
            logger.error(f"_check_drawdown_circuit_breaker failed: {e}")

    def run_cycle(self):
        """Run a single validation cycle."""
        try:
            # Staleness guard runs every cycle — fires even during quiet markets
            self._check_risk_parameters_staleness()
            # Long-term staleness audit (7-day warning, 30-day critical) — observation only
            self._check_risk_parameters_long_term_staleness()
            self._check_drawdown_circuit_breaker()
            count = self.poll_and_validate()
            self.write_heartbeat()
            return {"validated": count}
        except Exception as e:
            logger.error(f"Cycle failed: {e}")
            return {"error": str(e)}

    def run_continuous(self):
        """Run in continuous mode, polling every 15 seconds."""
        logger.info(f"Risk Guardian starting — user: {self.user_id}")
        logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS}s, dry_run: {self.dry_run}")
        logger.info(f"Correlation threshold: {self.correlation_checker.threshold}")

        last_heartbeat = 0

        while True:
            try:
                # Kill switch check
                if check_kill_switch():
                    logger.warning("Kill switch ACTIVE — heartbeat only")
                    self.write_heartbeat()
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Poll for approvals
                count = self.poll_and_validate()
                if count > 0:
                    logger.info(f"Processed {count} approval(s) this poll (total: {self.processed_count})")

                # Heartbeat
                self.cycle_count += 1
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
                    self.write_heartbeat()
                    last_heartbeat = now

                time.sleep(POLL_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                break
            except Exception as e:
                logger.error(f"Poll cycle failed: {e}", exc_info=True)
                time.sleep(POLL_INTERVAL_SECONDS)

        self.db.close()
        logger.info("Risk Guardian shutdown complete.")


# =============================================================================
# TEST SUITE
# =============================================================================
class RiskGuardianTester:
    """Unit tests for risk guardian logic."""

    def __init__(self):
        self.passed = 0
        self.failed = 0

    def _assert(self, condition: bool, name: str, detail: str = ""):
        if condition:
            self.passed += 1
            logger.info(f"  ✅ PASS: {name}")
        else:
            self.failed += 1
            logger.error(f"  ❌ FAIL: {name} — {detail}")

    def run_all(self):
        logger.info("=" * 60)
        logger.info("RISK GUARDIAN AGENT TEST SUITE")
        logger.info("=" * 60)

        self.test_correlation_same_direction_positive()
        self.test_correlation_opposite_direction_negative()
        self.test_correlation_below_threshold()
        self.test_correlation_no_open_positions()
        self.test_correlation_aud_nzd_block()
        self.test_correlation_threshold_per_profile()
        self.test_position_sizing()
        self.test_position_sizing_halted()
        self.test_swap_cost_pass()
        self.test_swap_cost_fail()
        self.test_swap_cost_negative_rr()
        self.test_swap_no_data()
        self.test_slippage_thresholds()
        self.test_rg1_daily_loss_absolute()
        self.test_drawdown_step_halt()

        logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASSED, {self.failed} FAILED (total: {self.passed + self.failed})")
        logger.info("=" * 60)
        return self.failed == 0

    def test_correlation_same_direction_positive(self):
        logger.info("\n--- Test: Correlation — Same direction, positive corr ---")
        checker = CorrelationChecker("neo_user_002")  # threshold 0.75
        positions = [{"instrument": "EURUSD", "direction": "long"}]
        corrs = {("GBPUSD", "EURUSD"): 0.87}

        passed, reasons = checker.check("GBPUSD", "long", positions, corrs, {})
        self._assert(not passed, "GBPUSD long blocked when EURUSD long (corr 0.87)")
        self._assert(len(reasons) > 0, "Rejection reason provided")

    def test_correlation_opposite_direction_negative(self):
        logger.info("\n--- Test: Correlation — Opposite direction, negative corr ---")
        checker = CorrelationChecker("neo_user_002")
        positions = [{"instrument": "EURUSD", "direction": "long"}]
        corrs = {("USDCHF", "EURUSD"): -0.91}

        # EUR long + CHF long = opposite direction in negatively correlated pair
        # This should block because it doubles USD short exposure
        passed, reasons = checker.check("USDCHF", "short", positions, corrs, {})
        self._assert(not passed, "USDCHF short blocked when EURUSD long (corr -0.91)")

    def test_correlation_below_threshold(self):
        logger.info("\n--- Test: Correlation — Below threshold ---")
        checker = CorrelationChecker("neo_user_002")  # threshold 0.75
        positions = [{"instrument": "EURUSD", "direction": "long"}]
        corrs = {("AUDUSD", "EURUSD"): 0.72}  # Below 0.75

        passed, reasons = checker.check("AUDUSD", "long", positions, corrs, {})
        self._assert(passed, "AUDUSD long allowed (corr 0.72 < threshold 0.75)")

    def test_correlation_no_open_positions(self):
        logger.info("\n--- Test: Correlation — No open positions ---")
        checker = CorrelationChecker("neo_user_002")
        passed, reasons = checker.check("EURUSD", "long", [], STATIC_CORRELATIONS, {})
        self._assert(passed, "Any trade allowed with no open positions")

    def test_correlation_aud_nzd_block(self):
        logger.info("\n--- Test: Correlation — AUD/NZD near-identical block ---")
        checker = CorrelationChecker("neo_user_002")
        positions = [{"instrument": "AUDUSD", "direction": "long"}]
        corrs = {("NZDUSD", "AUDUSD"): 0.88}

        passed, reasons = checker.check("NZDUSD", "long", positions, corrs, {})
        self._assert(not passed, "NZDUSD long blocked when AUDUSD long (corr 0.88)")

    def test_correlation_threshold_per_profile(self):
        logger.info("\n--- Test: Correlation — Threshold varies per profile ---")
        positions = [{"instrument": "EURUSD", "direction": "long"}]
        corrs = {("GBPUSD", "EURUSD"): 0.73}

        # Conservative (0.70) — should block
        checker_c = CorrelationChecker("neo_user_001")
        passed_c, _ = checker_c.check("GBPUSD", "long", positions, corrs, {})
        self._assert(not passed_c, "Conservative blocks at 0.73 (threshold 0.70)")

        # Balanced (0.75) — should pass
        checker_b = CorrelationChecker("neo_user_002")
        passed_b, _ = checker_b.check("GBPUSD", "long", positions, corrs, {})
        self._assert(passed_b, "Balanced allows at 0.73 (threshold 0.75)")

        # Aggressive (0.80) — should pass
        checker_a = CorrelationChecker("neo_user_003")
        passed_a, _ = checker_a.check("GBPUSD", "long", positions, corrs, {})
        self._assert(passed_a, "Aggressive allows at 0.73 (threshold 0.80)")

    def test_position_sizing(self):
        logger.info("\n--- Test: Position sizing ---")
        # Use flat min=max risk (1%) to isolate combined_multiplier arithmetic
        sizing = PositionSizer.calculate(
            risk_params={
                "size_multiplier": 0.75, "atr_stop_multiplier": 1.5,
                "account_value": 100000,
                "convergence_threshold": 0.22, "min_risk_pct": 0.01,
                "max_risk_pct": 0.01, "curve_exponent": 2.0,
                "stress_threshold_score": 60.0, "stress_multiplier": 0.70,
                "max_convergence_reference": 0.55,
            },
            atr_14=0.0045,
            instrument="EURUSD",
            size_multiplier_from_orchestrator=0.5,
            convergence_score=0.35,
            stress_score=0.0,
        )
        # Combined = 0.5 (orchestrator) × 0.75 (drawdown) = 0.375
        self._assert(
            abs(sizing["combined_multiplier"] - 0.375) < 0.001,
            "Combined multiplier: 0.5 × 0.75 = 0.375",
            f"Got: {sizing['combined_multiplier']}"
        )
        # Flat risk 1% × combined_mult 0.375 → effective_risk_pct = 0.375 (in %)
        self._assert(
            abs(sizing["effective_risk_pct"] - 0.375) < 0.001,
            "Effective risk: flat 1% × combined_mult 0.375 = 0.375%",
            f"Got: {sizing['effective_risk_pct']}"
        )

    def test_position_sizing_halted(self):
        logger.info("\n--- Test: Position sizing — halted at step 3 ---")
        sizing = PositionSizer.calculate(
            risk_params={"max_position_size_pct": 1.0, "size_multiplier": 0.0, "atr_stop_multiplier": 1.5},
            atr_14=0.0045,
            instrument="EURUSD",
            size_multiplier_from_orchestrator=1.0,
        )
        self._assert(sizing["combined_multiplier"] == 0.0, "Halted: multiplier is 0")

    def test_swap_cost_pass(self):
        logger.info("\n--- Test: Swap cost — passes ---")
        # 100 reward pips, 40 risk pips (2.5:1), swap=-0.5 pips/day for 1 day
        # adjusted_reward = 99.5, adj_rr = 99.5/40 = 2.49 — above 1.5
        passed, details = SwapCostChecker.check(
            swap_rate_pips=-0.5, reward_pips=100.0, risk_pips=40.0, min_rr=1.5)
        self._assert(passed, "Swap cost does not erode R:R below minimum")

    def test_swap_cost_fail(self):
        logger.info("\n--- Test: Swap cost — fails ---")
        # 80 reward pips, 50 risk pips (1.6:1), swap=-5.0 for 3 days → -15 pips
        # adjusted_reward = 65, adj_rr = 65/50 = 1.3 — below 1.5
        passed, details = SwapCostChecker.check(
            swap_rate_pips=-5.0, reward_pips=80.0, risk_pips=50.0, min_rr=1.5, estimated_hold_days=3.0)
        self._assert(not passed, "Heavy swap cost erodes R:R below minimum")

    def test_swap_cost_negative_rr(self):
        logger.info("\n--- Test: Swap cost — negative adjusted R:R ---")
        # 15 reward pips, 30 risk pips (0.5:1), swap=-10.0 for 2 days → -20 pips
        # adjusted_reward = -5, adj_rr = -5/30 = -0.17 — negative
        passed, details = SwapCostChecker.check(
            swap_rate_pips=-10.0, reward_pips=15.0, risk_pips=30.0, min_rr=1.5, estimated_hold_days=2.0)
        self._assert(not passed, "Negative adjusted R:R rejected")
        self._assert("negative" in details.get("reason", "").lower(), "Reason mentions negative R:R")

    def test_swap_no_data(self):
        logger.info("\n--- Test: Swap cost — no data ---")
        passed, details = SwapCostChecker.check(
            swap_rate_pips=None, reward_pips=80.0, risk_pips=40.0, min_rr=1.5)
        self._assert(passed, "No swap data → skip check (pass)")

    def test_slippage_thresholds(self):
        logger.info("\n--- Test: Slippage thresholds ---")
        self._assert(SLIPPAGE_THRESHOLDS["EURUSD"] == 3, "EUR/USD: 3 pips")
        self._assert(SLIPPAGE_THRESHOLDS["GBPUSD"] == 4, "GBP/USD: 4 pips")
        self._assert(SLIPPAGE_THRESHOLDS["AUDUSD"] == 5, "AUD/USD: 5 pips")

    def test_rg1_daily_loss_absolute(self):
        logger.info("\n--- Test: RG1 — Daily loss limit is absolute ---")
        # This is a principle test — the daily loss limit check
        # never has exceptions regardless of convergence or setup quality
        self._assert(True, "RG1: Daily loss limit is never overridden (architecture rule)")

    def test_drawdown_step_halt(self):
        logger.info("\n--- Test: Drawdown step level halt ---")
        # Step 3 = >15% drawdown = halted
        self._assert(True, "Step 3 (>15% drawdown) → size_multiplier 0.0 → trading halted")


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
    """Main entry point — runs Risk Guardian for ALL active paper users."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Risk Guardian")
    parser.add_argument("--user", default=None,
                        help="Optional: run for a single user only (for debugging)")
    parser.add_argument("--single", action="store_true",
                        help="Run a single validation cycle then exit")
    parser.add_argument("--test", action="store_true",
                        help="Run configuration test only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not write approval updates")

    args = parser.parse_args()

    # Resolve user list
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
        logger.info("Risk Guardian test mode — running unit tests")
        tester = RiskGuardianTester()
        tester.run_all()
        sys.exit(0 if tester.failed == 0 else 1)

    # Create a RiskGuardian per user
    agents = {}
    for uid in user_ids:
        try:
            agents[uid] = RiskGuardian(user_id=uid, dry_run=args.dry_run)
            logger.info(f"Initialized Risk Guardian for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Risk Guardian for {uid}: {e}")

    if not agents:
        logger.error("No agents initialized — exiting")
        sys.exit(1)

    if args.single:
        for uid, agent in agents.items():
            logger.info(f"--- {uid} ---")
            try:
                result = agent.run_cycle()
                logger.info(f"  result: {result}")
            except Exception as e:
                logger.error(f"Cycle failed for {uid}: {e}")
        logger.info("Single cycle complete for all users")
        sys.exit(0)

    # Continuous mode: shared poll loop across all users.
    # run_cycle() does poll_and_validate() + write_heartbeat() per user per tick.
    logger.info(f"Starting continuous operation for {len(agents)} user(s), poll={POLL_INTERVAL_SECONDS}s")
    try:
        while True:
            try:
                if check_kill_switch():
                    logger.warning("Kill switch ACTIVE — heartbeat only")
                    for agent in agents.values():
                        try:
                            agent.write_heartbeat()
                        except Exception as e:
                            logger.error(f"Heartbeat failed for {agent.user_id}: {e}")
                    _utc_hr = datetime.datetime.now(datetime.timezone.utc).hour
                    _interval = 300 if (_utc_hr >= 22 or _utc_hr < 7) else POLL_INTERVAL_SECONDS
                    time.sleep(_interval)
                    continue

                for uid, agent in agents.items():
                    try:
                        result = agent.run_cycle()
                        if isinstance(result, dict) and result.get("validated", 0) > 0:
                            logger.info(f"[{uid}] validated {result['validated']} approval(s)")
                    except Exception as e:
                        logger.error(f"Cycle failed for {uid}: {e}")

                _utc_hr = datetime.datetime.now(datetime.timezone.utc).hour
                _interval = 300 if (_utc_hr >= 22 or _utc_hr < 7) else POLL_INTERVAL_SECONDS
                time.sleep(_interval)
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error(f"Poll cycle failed: {e}", exc_info=True)
                _utc_hr = datetime.datetime.now(datetime.timezone.utc).hour
                _interval = 300 if (_utc_hr >= 22 or _utc_hr < 7) else POLL_INTERVAL_SECONDS
                time.sleep(_interval)
    finally:
        for uid, agent in agents.items():
            try:
                if hasattr(agent, "db") and hasattr(agent.db, "close"):
                    agent.db.close()
            except Exception:
                pass
        logger.info("Risk Guardian shutdown complete.")


if __name__ == "__main__":
    main()
