#!/usr/bin/env python3
"""
Project Neo — Orchestrator Agent v1.0
======================================
Reads signals from Macro (35%), Technical (45%), and Regime (20%) agents.
Computes weighted convergence score per pair per user.
Approves trades only when convergence exceeds effective threshold
and all pre-conditions are met.

Weight: N/A (decision layer, not scored)
Cycle: Every 15 minutes
IAM Role: platform-orchestrator-role-dev
Decision Rules: O1 (convergence threshold is a hard floor)

Run:
  source ~/algodesk/bin/activate
  python orchestrator_agent.py --user neo_user_002
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
import psycopg2
import psycopg2.extras

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.alerting import send_alert
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, log_loaded_state_summary, AGENT_SCOPE_USER_ID
from shared.system_events import log_event
from shared.schema_validator import validate_schema
from shared.signal_validator import SignalValidator
from shared.warn_log import warn

EXPECTED_TABLES = {
    "forex_network.agent_signals":        ["agent_name", "instrument", "signal_type", "score",
                                           "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats":     ["agent_name", "user_id", "last_seen",
                                           "status", "cycle_count", "degradation_mode",
                                           "convergence_boost"],
    "forex_network.convergence_history":  ["instrument", "user_id", "convergence_score",
                                           "cycle_timestamp"],
    "forex_network.risk_parameters":      ["user_id", "convergence_threshold", "max_risk_pct",
                                           "max_open_positions", "account_value",
                                           "peak_account_value", "updated_at"],
    "shared.market_context_snapshots":    ["system_stress_score", "stress_state", "snapshot_time"],
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("neo.orchestrator")

# =============================================================================
# CONSTANTS
# =============================================================================
AWS_REGION = "eu-west-2"
CYCLE_INTERVAL_SECONDS = 300  # 5 minutes — matches technical agent polling cadence
SIGNAL_EXPIRY_MINUTES = 20
# Market-state-aware freshness thresholds (minutes).
# Technical: 5-min cycle regardless of session.
# Macro: 15-min active / 60-min quiet — threshold must exceed longest normal cycle.
# Regime: 15-min active / 15-min quiet.
SIGNAL_MAX_AGE_MINUTES = {
    'active': {'macro': 25, 'technical': 20, 'regime': 20},
    'quiet':  {'macro': 65, 'technical': 20, 'regime': 20},
}
_SIGNAL_MAX_AGE_DEFAULT = {'macro': 25, 'technical': 20, 'regime': 20}
REGIME_SNAPSHOT_MAX_AGE_MINUTES = 55  # regime quiet-hours sleep = 60s; 55 avoids false-stale before next write
HEARTBEAT_INTERVAL_SECONDS = 60
AGENT_NAME = "orchestrator"

# Convergence weights
CONVERGENCE_WEIGHTS = {
    "macro":     0.40,
    "technical": 0.40,
    "regime":    0.20,
}

# User profile defaults (read from risk_parameters at runtime)
# Keyed by Cognito sub (UUID) — the value main() passes as user_id.
# Prior bug: keyed by "neo_user_00X" so USER_PROFILES.get(uuid, ...) always
# returned the default (Balanced), silently mis-profiling all three users.
USER_PROFILES = {
    # neo_user_001 Conservative
    "e61202e4-30d1-70f8-9927-30b8a439e042": {"name": "Conservative", "base_threshold": 0.80, "min_rr": 2.0, "min_spread_ratio": 7.0, "session_filter": ["london", "overlap"], "day_filter": [2, 3, 4]},
    # neo_user_002 Balanced
    "76829264-20e1-7023-1e31-37b7a37a1274": {"name": "Balanced",     "base_threshold": 0.65, "min_rr": 1.5, "min_spread_ratio": 5.0, "session_filter": None, "day_filter": [1, 2, 3, 4, 5]},
    # neo_user_003 Aggressive
    "d6c272e4-a031-7053-af8e-ade000f0d0d5": {"name": "Aggressive",   "base_threshold": 0.55, "min_rr": 1.2, "min_spread_ratio": 4.0, "session_filter": None, "day_filter": [1, 2, 3, 4, 5]},
}

FX_PAIRS = [
    # USD pairs
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Cross pairs confirmed on IG demo 2026-04-22
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
    "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
    "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
]

# Session constraints for cross pairs — these pairs have lower liquidity
# outside their primary session; enforced in evaluate_pair CHECK 6.7.
CROSS_PAIR_SESSIONS = {
    'EURGBP': ['london'],
    'EURJPY': ['london', 'overlap'],
    'GBPJPY': ['london', 'overlap'],
    'EURCHF': ['london'],
    'GBPCHF': ['london'],
    'EURAUD': ['london', 'overlap'],
    'GBPAUD': ['london', 'overlap'],
    'EURCAD': ['overlap', 'newyork'],
    'GBPCAD': ['overlap', 'newyork'],
    'AUDNZD': ['asian', 'london'],
    'AUDJPY': ['asian', 'london'],
    'CADJPY': ['newyork', 'overlap'],
    'NZDJPY': ['asian'],
}

# Per-cycle convergence cache for collapse-alert delta comparison
_prev_convergence_scores: dict = {}

# Stress score → orchestrator behaviour
STRESS_ADJUSTMENTS = [
    # (max_score, threshold_add, size_multiplier, session_restriction, new_entries_allowed)
    (30,  0.00, 1.00, None,                     True),   # Normal
    (50,  0.05, 0.75, None,                     True),   # Elevated
    (70,  0.10, 0.50, ["london", "overlap"],    True),   # High
    (85,  0.15, 0.25, None,                     False),  # Pre-crisis
    (100, 0.00, 0.00, None,                     False),  # Crisis
]

# Pre-event protocol
PRE_EVENT_WINDOW_MINUTES = 60
PRE_EVENT_THRESHOLD_ADD = 0.05


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


def check_maintenance_mode(region: str = AWS_REGION) -> bool:
    """Return True if maintenance mode is active (e.g. during rolling agent restarts).
    Unlike the kill switch, maintenance mode keeps the orchestrator cycling and writing
    heartbeats — it only suppresses trade proposal evaluation.
    Fails safe to False (not in maintenance) so a missing parameter never blocks trading.
    """
    try:
        val = get_parameter("/platform/config/maintenance-mode", region)
        return val.strip().lower() == "true"
    except Exception:
        return False  # parameter absent or error — default: not in maintenance


# =============================================================================
# DATABASE
# =============================================================================
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
# SIGNAL READER
# =============================================================================
class SignalReader:
    """Reads latest unexpired signals from analysis agents."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def read_latest_signals(self) -> Dict[str, List[Dict[str, Any]]]:
        """Read most-recent signal per (agent_name, instrument) with no TTL filter.

        Freshness filtering removed: the most recent signal IS the current analysis
        until replaced. Agent liveness tracked separately via agent_heartbeats.
        signal_age_min included per signal for audit/observability.
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT ON (agent_name, instrument)
                       agent_name, instrument, score, bias, confidence, payload,
                       created_at, expires_at,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) / 60 AS signal_age_min
                FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND agent_name IN ('macro', 'technical', 'regime')
                ORDER BY agent_name, instrument, created_at DESC
            """, (self.user_id,))
            rows = cur.fetchall()

            _now_utc = datetime.datetime.now(datetime.timezone.utc)
            signals: Dict[str, List[Dict]] = {"macro": [], "technical": [], "regime": []}
            for row in rows:
                agent = row["agent_name"]
                if agent in signals:
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    _expires = row["expires_at"]
                    if _expires is not None and _expires.tzinfo is None:
                        _expires = _expires.replace(tzinfo=datetime.timezone.utc)
                    _is_stale = _expires is not None and _expires < _now_utc
                    signals[agent].append({
                        "agent_name": agent,
                        "instrument": row["instrument"],
                        "score": float(row["score"]) if row["score"] is not None else 0.0,
                        "bias": row["bias"],
                        "confidence": float(row["confidence"]) if row["confidence"] is not None else 0.0,
                        "payload": payload or {},
                        "created_at": row["created_at"],
                        "expires_at": _expires,
                        "is_stale": _is_stale,
                        "signal_age_min": float(row["signal_age_min"] or 0),
                    })
            return signals
        except Exception as e:
            logger.error(f"Signal read failed: {e}")
            return {"macro": [], "technical": [], "regime": []}
        finally:
            cur.close()

    def read_market_context(self) -> Dict[str, Any]:
        """Read latest market context snapshot (from regime agent).
        If the snapshot is older than REGIME_SNAPSHOT_MAX_AGE_MINUTES, the regime
        agent has likely crashed. Return conservative defaults (elevated stress, low
        confidence) so the orchestrator doesn't trade on stale regime data.
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT system_stress_score, stress_state, stress_components,
                       current_session, day_of_week, stop_hunt_window,
                       ny_close_window, regime_global AS global_regime, metadata,
                       snapshot_time
                FROM shared.market_context_snapshots
                WHERE system_stress_score IS NOT NULL
                ORDER BY snapshot_time DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                # Staleness guard — regime agent down or crashed
                snapshot_time = row["snapshot_time"]
                if snapshot_time:
                    if snapshot_time.tzinfo is None:
                        snapshot_time = snapshot_time.replace(tzinfo=datetime.timezone.utc)
                    age_minutes = (
                        datetime.datetime.now(datetime.timezone.utc) - snapshot_time
                    ).total_seconds() / 60
                    if age_minutes > REGIME_SNAPSHOT_MAX_AGE_MINUTES:
                        logger.warning(
                            f"Regime snapshot is {age_minutes:.0f}min old "
                            f"(max: {REGIME_SNAPSHOT_MAX_AGE_MINUTES}min) — "
                            f"using conservative defaults to avoid trading on stale stress data"
                        )
                        return {
                            "stress_score": 50.0,
                            "stress_state": "elevated",
                            "stress_components": {},
                            "current_session": "unknown",
                            "day_of_week": None,
                            "stop_hunt_window": False,
                            "ny_close_window": False,
                            "global_regime": "ranging",
                            "metadata": {},
                            "kill_switch_active": False,
                            "stress_score_confidence": "low",
                            "convergence_caution_buffer": 0.05,
                            "vix_spike_floor_active": False,
                            "regime_snapshot_stale": True,
                            "regime_snapshot_age_minutes": round(age_minutes, 1),
                        }

                metadata = row["metadata"]
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                return {
                    "stress_score": float(row["system_stress_score"]),
                    "stress_state": row["stress_state"],
                    "stress_components": row["stress_components"],
                    "current_session": row["current_session"],
                    "day_of_week": int(row["day_of_week"]) if row["day_of_week"] else None,
                    "stop_hunt_window": row["stop_hunt_window"],
                    "ny_close_window": row["ny_close_window"],
                    "global_regime": row["global_regime"],
                    "metadata": metadata or {},
                    "kill_switch_active": (metadata or {}).get("kill_switch_active", False),
                    "stress_score_confidence": (metadata or {}).get("stress_score_confidence", "high"),
                    "convergence_caution_buffer": (metadata or {}).get("convergence_caution_buffer", 0.0),
                    "vix_spike_floor_active": (metadata or {}).get("vix_spike_floor_active", False),
                }
            return {}
        except Exception as e:
            logger.error(f"Market context read failed: {e}")
            return {}
        finally:
            cur.close()

    def read_risk_parameters(self) -> Dict[str, Any]:
        """Read risk parameters for this user."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT convergence_threshold,
                       max_open_positions, daily_loss_limit_pct,
                       trailing_stop_pct, pre_event_size_reduction,
                       circuit_breaker_active, paper_mode,
                       size_multiplier, drawdown_step_level,
                       allowed_instruments
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            if row:
                return {k: (float(v) if isinstance(v, (int, float)) and v is not None else v)
                        for k, v in dict(row).items()}
            return {}
        except Exception as e:
            logger.error(f"Risk parameters read failed: {e}")
            return {}
        finally:
            cur.close()

    def read_degradation_state(self) -> Dict[str, Dict[str, Any]]:
        """Read agent degradation state from heartbeats."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT agent_name, status, last_seen, absent_cycles,
                       degradation_mode, convergence_boost
                FROM forex_network.agent_heartbeats
                WHERE user_id = %s
                  AND agent_name IN ('macro', 'technical', 'regime')
            """, (self.user_id,))
            rows = cur.fetchall()
            return {
                row["agent_name"]: {
                    "status": row["status"],
                    "last_seen": row["last_seen"],
                    "absent_cycles": int(row["absent_cycles"] or 0),
                    "degradation_mode": row["degradation_mode"],
                    "convergence_boost": float(row["convergence_boost"] or 0.0),
                }
                for row in rows
            }
        except Exception as e:
            logger.error(f"Degradation state read failed: {e}")
            return {}
        finally:
            cur.close()

    def read_open_positions(self) -> List[Dict[str, Any]]:
        """Read currently open positions for this user."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT instrument, direction, entry_price, stop_price,
                       position_size_usd, entry_time, pnl
                FROM forex_network.trades
                WHERE user_id = %s AND exit_time IS NULL
            """, (self.user_id,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Open positions read failed: {e}")
            return []
        finally:
            cur.close()

    def read_daily_loss(self) -> float:
        """Read today's total P&L for this user."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT COALESCE(SUM(pnl), 0) AS daily_pnl
                FROM forex_network.trades
                WHERE user_id = %s
                  AND exit_time IS NOT NULL
                  AND exit_time >= CURRENT_DATE
            """, (self.user_id,))
            row = cur.fetchone()
            return float(row["daily_pnl"]) if row else 0.0
        except Exception as e:
            logger.error(f"Daily loss read failed: {e}")
            return 0.0
        finally:
            cur.close()

    def check_upcoming_events(self) -> List[Dict[str, Any]]:
        """Check for tier-1 events within the pre-event window."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT country, indicator, scheduled_time, forecast, previous
                FROM forex_network.economic_calendar
                WHERE importance = 3 AND resolved = FALSE
                  AND scheduled_time BETWEEN NOW()
                      AND NOW() + INTERVAL '%s minutes'
                ORDER BY scheduled_time ASC
            """, (PRE_EVENT_WINDOW_MINUTES,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Event check failed: {e}")
            return []
        finally:
            cur.close()


# =============================================================================
# CONVERGENCE CALCULATOR
# =============================================================================
class ConvergenceCalculator:
    """
    Computes weighted convergence score per pair and determines
    effective threshold after all adjustments.
    """

    def __init__(self, user_id: str, db: "DatabaseConnection" = None):
        self.user_id = user_id
        self.db = db
        if user_id not in USER_PROFILES:
            raise ValueError(
                f"Unknown user_id {user_id!r} — not found in USER_PROFILES. "
                f"Refusing to silently fall back to a default profile."
            )
        self.profile = USER_PROFILES[user_id]

    # -------------------------------------------------------------------------
    # Feature 1: Signal Persistence Score
    # -------------------------------------------------------------------------
    def get_persistence_bonus(self, instrument: str) -> float:
        """Signals persisting 3+ cycles (non-neutral) get a convergence bonus."""
        if not self.db:
            return 0.0
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT agent_name, consecutive_cycles, current_bias
                FROM forex_network.signal_persistence
                WHERE user_id = %s AND instrument = %s
            """, (self.user_id, instrument))
            rows = cur.fetchall()
            cur.close()
        except Exception as e:
            logger.warning(f"persistence_bonus query failed for {instrument}: {e}")
            try: self.db.rollback()
            except Exception: pass
            return 0.0

        if not rows:
            return 0.0

        aligned_persistent = 0
        for row in rows:
            bias = (row["current_bias"] or "neutral").lower()
            if row["consecutive_cycles"] >= 3 and bias not in ("neutral", ""):
                aligned_persistent += 1

        bonus_map = {0: 0.0, 1: 0.02, 2: 0.05, 3: 0.08}
        return bonus_map.get(aligned_persistent, 0.08)

    # -------------------------------------------------------------------------
    # Feature: Directional-conflict classification
    # -------------------------------------------------------------------------
    def _get_persistence_cycles(self, instrument: str, agent_name: str) -> int:
        """Read consecutive_cycles for one (instrument, agent) from signal_persistence."""
        if not self.db or not instrument:
            return 0
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT consecutive_cycles
                FROM forex_network.signal_persistence
                WHERE user_id = %s AND instrument = %s AND agent_name = %s
            """, (self.user_id, instrument, agent_name))
            row = cur.fetchone()
            cur.close()
            return int(row["consecutive_cycles"]) if row else 0
        except Exception as e:
            logger.warning(f"persistence cycles read failed ({agent_name} {instrument}): {e}")
            try: self.db.rollback()
            except Exception: pass
            return 0

    def classify_conflict(
        self,
        macro_signal: Dict[str, Any],
        tech_signal: Dict[str, Any],
        stress_score: float,
    ) -> str:
        """Classify the type of fundamental-technical conflict.

        Returns one of:
          crisis_divergence              — stress > 70
          noise_conflict                 — both low-confidence or both low-magnitude
          fundamental_shift_macro_leading    — macro changed recently, tech persistent
          fundamental_shift_technical_leading — tech changed recently, macro persistent
          fundamental_exhaustion         — tech persistent + macro flagging contrarian extremes
          unclassified_conflict          — fallback
        """
        try:
            macro_score = abs(float(macro_signal.get("score", 0) or 0))
            tech_score = abs(float(tech_signal.get("score", 0) or 0))
            macro_conf = float(macro_signal.get("confidence", 0) or 0)
            tech_conf = float(tech_signal.get("confidence", 0) or 0)
        except (TypeError, ValueError):
            return "unclassified_conflict"

        instrument = macro_signal.get("instrument") or tech_signal.get("instrument")
        stress_score = float(stress_score or 0)

        # Category 4: Crisis divergence — market-wide stress overrides classification
        if stress_score > 70:
            return "crisis_divergence"

        # Category 3: Noise conflict — both agents weak on magnitude or confidence
        if (macro_conf < 0.40 and tech_conf < 0.40) or \
           (macro_score < 0.15 and tech_score < 0.15):
            return "noise_conflict"

        # Persistence from signal_persistence (consecutive non-neutral cycles)
        macro_persistence = self._get_persistence_cycles(instrument, "macro")
        tech_persistence = self._get_persistence_cycles(instrument, "technical")

        # Category 1: Fundamental shift — one side changed recently, other is persistent
        if macro_persistence <= 3 and tech_persistence >= 5:
            return "fundamental_shift_macro_leading"
        if tech_persistence <= 3 and macro_persistence >= 5:
            return "fundamental_shift_technical_leading"

        # Category 2: Fundamental exhaustion — long-persistent technical conflicts with
        # macro that is flagging structural contrarian extremes (COT or sentiment velocity)
        if tech_persistence >= 5 and macro_conf > 0.40:
            payload = macro_signal.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (ValueError, TypeError):
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            cot_extreme = bool(payload.get("cot_extreme"))
            sentiment_extreme = bool(payload.get("sentiment_velocity_flag"))
            if cot_extreme or sentiment_extreme:
                return "fundamental_exhaustion"

        return "unclassified_conflict"

    def _write_rejection(
        self,
        instrument: str,
        reason: str,
        category: str,
        macro_signal: Dict[str, Any],
        tech_signal: Dict[str, Any],
        stress_score: float,
    ) -> None:
        """Persist one rejection row for learning-module consumption."""
        if not self.db or not instrument:
            return
        try:
            cur = self.db.cursor()
            cur.execute("""
                INSERT INTO forex_network.rejection_log
                  (user_id, instrument, reason, conflict_category,
                   macro_bias, macro_score, macro_confidence,
                   tech_bias, tech_score, tech_confidence,
                   stress_score, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.user_id,
                instrument,
                reason,
                category,
                macro_signal.get("bias"),
                float(macro_signal.get("score") or 0),
                float(macro_signal.get("confidence") or 0),
                tech_signal.get("bias"),
                float(tech_signal.get("score") or 0),
                float(tech_signal.get("confidence") or 0),
                float(stress_score or 0),
                json.dumps({
                    "macro_persistence": self._get_persistence_cycles(instrument, "macro"),
                    "tech_persistence": self._get_persistence_cycles(instrument, "technical"),
                }),
            ))
            cur.close()
            if hasattr(self.db, "commit"):
                self.db.commit()
        except Exception as e:
            logger.warning(f"rejection_log insert failed for {instrument}: {e}")
            try: self.db.rollback()
            except Exception: pass

    # -------------------------------------------------------------------------
    # Feature 2: Convergence Trend Tracking
    # -------------------------------------------------------------------------
    def get_convergence_trend(self, instrument: str, lookback: int = 6) -> Dict[str, Any]:
        """Analyse whether convergence is building or oscillating."""
        if not self.db:
            return {"trend": "insufficient_data", "modifier": 0.0}
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT convergence_score
                FROM forex_network.convergence_history
                WHERE user_id = %s AND instrument = %s
                ORDER BY cycle_timestamp DESC LIMIT %s
            """, (self.user_id, instrument, lookback))
            scores = [abs(float(row["convergence_score"])) for row in cur.fetchall()]  # trend tracks magnitude
            cur.close()
        except Exception as e:
            logger.warning(f"convergence_trend query failed for {instrument}: {e}")
            return {"trend": "insufficient_data", "modifier": 0.0}

        if len(scores) < 3:
            return {"trend": "insufficient_data", "modifier": 0.0}

        scores.reverse()  # oldest first
        increases = sum(1 for i in range(1, len(scores)) if scores[i] > scores[i - 1])
        trend_ratio = increases / (len(scores) - 1)

        mean_score = sum(scores) / len(scores)
        variance = sum((s - mean_score) ** 2 for s in scores) / len(scores)

        if trend_ratio >= 0.66 and variance < 0.02:
            return {"trend": "building", "modifier": +0.03}
        elif trend_ratio <= 0.33 and variance < 0.02:
            return {"trend": "declining", "modifier": -0.03}
        elif variance > 0.05:
            return {"trend": "oscillating", "modifier": -0.02}
        else:
            return {"trend": "stable", "modifier": 0.0}

    def write_convergence_history(
        self, instrument: str, convergence_score: float,
        macro_score: float, technical_score: float, regime_score: float,
        bias: str = "neutral",
    ) -> None:
        """Persist convergence score for trend analysis. Auto-prunes rows >7 days."""
        if not self.db:
            return
        try:
            cur = self.db.cursor()
            cur.execute("""
                INSERT INTO forex_network.convergence_history
                    (user_id, instrument, convergence_score,
                     macro_score, technical_score, regime_score, bias)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (self.user_id, instrument, convergence_score,
                  macro_score, technical_score, regime_score, bias))
            # Prune history older than 7 days
            cur.execute("""
                DELETE FROM forex_network.convergence_history
                WHERE user_id = %s AND instrument = %s
                  AND cycle_timestamp < NOW() - INTERVAL '7 days'
            """, (self.user_id, instrument))
            self.db.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"convergence_history write failed for {instrument}: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # Feature 3: Pattern Memory Integration
    # -------------------------------------------------------------------------
    def get_pattern_match_bonus(
        self, instrument: str, regime: str, session: str, bias: str,
    ) -> float:
        """Check if current setup matches a known winning pattern."""
        if not self.db:
            return 0.0
        try:
            cur = self.db.cursor()
            cur.execute("""
                SELECT failure_rate, sample_size, confidence
                FROM forex_network.pattern_memory
                WHERE user_id = %s
                  AND instrument = %s
                  AND regime = %s
                  AND active = TRUE
                  AND sample_size >= 20
                  AND failure_rate < 0.65
                ORDER BY confidence DESC LIMIT 1
            """, (self.user_id, instrument, regime))
            pattern = cur.fetchone()
            cur.close()
        except Exception as e:
            logger.warning(f"pattern_match query failed for {instrument}: {e}")
            return 0.0

        if not pattern:
            return 0.0

        success_rate = 1.0 - float(pattern["failure_rate"])
        sample_size = int(pattern["sample_size"])
        confidence = float(pattern["confidence"])

        if success_rate < 0.55 or sample_size < 20:
            return 0.0

        if success_rate >= 0.75:
            base_bonus = 0.06
        elif success_rate >= 0.65:
            base_bonus = 0.04
        else:
            base_bonus = 0.02

        return base_bonus * confidence

    # -------------------------------------------------------------------------
    # Integration: Final convergence with historical bonuses
    # -------------------------------------------------------------------------
    def compute_final_convergence(
        self, instrument: str, base_convergence: float,
        regime: str, session: str, bias: str,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Final convergence = base + persistence_bonus + trend_modifier + pattern_bonus
        Total historical bonus capped at +0.12.
        """
        persistence_bonus = self.get_persistence_bonus(instrument)
        trend = self.get_convergence_trend(instrument)
        pattern_bonus = self.get_pattern_match_bonus(instrument, regime, session, bias)

        total_historical_bonus = persistence_bonus + trend["modifier"] + pattern_bonus
        total_historical_bonus = min(total_historical_bonus, 0.12)

        # Fix: bonus must reinforce the base direction, not always add positive magnitude.
        if base_convergence != 0:
            direction = 1 if base_convergence > 0 else -1
            final_convergence = base_convergence + (total_historical_bonus * direction)
        else:
            final_convergence = base_convergence

        logger.info(
            f"{instrument}: base={base_convergence:.3f} + "
            f"persistence={persistence_bonus:.3f} + "
            f"trend={trend['modifier']:+.3f} ({trend['trend']}) + "
            f"pattern={pattern_bonus:.3f} = "
            f"final={final_convergence:.3f}"
        )

        return final_convergence, {
            "persistence_bonus": persistence_bonus,
            "trend": trend,
            "pattern_bonus": pattern_bonus,
            "total_historical_bonus": total_historical_bonus,
        }

    # -------------------------------------------------------------------------
    # Directional Consensus Gate
    # -------------------------------------------------------------------------
    def check_directional_consensus(
        self,
        macro_signal: Dict[str, Any],
        technical_signal: Dict[str, Any],
    ) -> Tuple[Optional[str], bool, str]:
        """
        Check whether macro and technical agents agree on direction.

        Returns (consensus_direction, should_proceed, reason).
          - consensus_direction: 'bullish' | 'bearish' | None
          - should_proceed: False means reject this pair immediately
          - reason: human-readable log string
        """
        macro_raw = (macro_signal.get("bias") or "neutral").lower()
        tech_raw = (technical_signal.get("bias") or "neutral").lower()

        # Normalise compound labels (e.g. "neutral_bullish" → "bullish")
        macro_dir = "bullish" if "bullish" in macro_raw else "bearish" if "bearish" in macro_raw else "neutral"
        tech_dir = "bullish" if "bullish" in tech_raw else "bearish" if "bearish" in tech_raw else "neutral"

        # Both neutral → no signal, reject
        if macro_dir == "neutral" and tech_dir == "neutral":
            return None, False, "no_directional_signal: macro=neutral, technical=neutral"

        # Direct conflict → reject
        if (macro_dir == "bullish" and tech_dir == "bearish") or \
           (macro_dir == "bearish" and tech_dir == "bullish"):
            return None, False, f"directional_conflict: macro={macro_dir}, technical={tech_dir}"

        # One neutral, one directional → proceed with reduced conviction
        if macro_dir == "neutral":
            return tech_dir, True, f"single_agent_direction: technical={tech_dir}"
        if tech_dir == "neutral":
            return macro_dir, True, f"single_agent_direction: macro={macro_dir}"

        # Both agree → full conviction
        return macro_dir, True, f"aligned: macro={macro_dir}, technical={tech_dir}"

    def compute_effective_threshold(
        self,
        risk_params: Dict[str, Any],
        market_context: Dict[str, Any],
        degradation: Dict[str, Dict[str, Any]],
        upcoming_events: List[Dict[str, Any]],
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate effective convergence threshold after ALL adjustments.
        O1: This threshold is a HARD FLOOR — never reduced.

        Returns (effective_threshold, breakdown_dict).
        """
        breakdown: Dict[str, Any] = {}

        # 1. Base threshold from risk_parameters (or profile default)
        base = float(risk_params.get("convergence_threshold", self.profile["base_threshold"]))
        breakdown["base_threshold"] = base
        effective = base

        # 2. Stress score adjustment
        stress_score = market_context.get("stress_score", 0)
        stress_add = 0.0
        stress_size_mult = 1.0
        stress_session_restriction = None
        stress_new_entries = True

        for max_score, t_add, s_mult, s_restrict, new_ok in STRESS_ADJUSTMENTS:
            if stress_score <= max_score:
                stress_add = t_add
                stress_size_mult = s_mult
                stress_session_restriction = s_restrict
                stress_new_entries = new_ok
                break

        effective += stress_add
        breakdown["stress_adjustment"] = stress_add
        breakdown["stress_size_multiplier"] = stress_size_mult
        breakdown["stress_session_restriction"] = stress_session_restriction
        breakdown["stress_new_entries_allowed"] = stress_new_entries

        # 3. R5: Stress score confidence caution buffer
        caution_buffer = market_context.get("convergence_caution_buffer", 0.0)
        if caution_buffer > 0:
            effective += caution_buffer
            breakdown["r5_caution_buffer"] = caution_buffer

        # R5: If stress_score_confidence is 'low', apply Elevated floor
        if market_context.get("stress_score_confidence") == "low":
            elevated_floor_add = 0.05
            elevated_floor_mult = 0.75
            if stress_add < elevated_floor_add:
                effective += (elevated_floor_add - stress_add)
                breakdown["r5_elevated_floor_applied"] = True
            stress_size_mult = min(stress_size_mult, elevated_floor_mult)
            breakdown["stress_size_multiplier"] = stress_size_mult

        # 4. Pre-event protocol
        if upcoming_events:
            effective += PRE_EVENT_THRESHOLD_ADD
            breakdown["pre_event_adjustment"] = PRE_EVENT_THRESHOLD_ADD
            breakdown["pre_event_triggers"] = [
                {"country": e["country"], "indicator": e["indicator"],
                 "time": str(e["scheduled_time"])}
                for e in upcoming_events[:3]
            ]

        # 5. Agent degradation boosts
        total_degradation_boost = 0.0
        for agent_name, state in degradation.items():
            boost = state.get("convergence_boost", 0.0)
            if boost > 0:
                total_degradation_boost += boost
                breakdown[f"degradation_boost_{agent_name}"] = boost

        if total_degradation_boost > 0:
            effective += total_degradation_boost
            breakdown["total_degradation_boost"] = total_degradation_boost

        # 6. Gap C: Regime confidence — low regime confidence adds +0.05
        regime_confidence = market_context.get("metadata", {}).get("regime_confidence")
        # Also check per-pair from regime signal
        # This is applied per-pair in the convergence scoring, not globally
        breakdown["regime_confidence_global"] = regime_confidence

        # 7. Drawdown step multiplier (from risk guardian)
        drawdown_mult = float(risk_params.get("size_multiplier", 1.0))
        breakdown["drawdown_size_multiplier"] = drawdown_mult

        # Combined size multiplier
        combined_size_mult = stress_size_mult * drawdown_mult
        breakdown["combined_size_multiplier"] = combined_size_mult

        # O1: Threshold is a HARD FLOOR — cap at minimum of base
        # (adjustments can only increase, never decrease)
        effective = max(effective, base)
        breakdown["effective_threshold"] = round(effective, 4)

        logger.info(
            f"Effective threshold: {effective:.4f} "
            f"(base={base}, stress=+{stress_add}, "
            f"caution=+{caution_buffer:.3f}, "
            f"pre_event=+{breakdown.get('pre_event_adjustment', 0)}, "
            f"degradation=+{total_degradation_boost})"
        )

        return effective, breakdown

    def compute_pair_convergence(
        self,
        pair: str,
        signals: Dict[str, List[Dict[str, Any]]],
        regime_payload: Dict[str, Any],
    ) -> Tuple[float, str, float, Dict[str, Any]]:
        """
        Compute convergence score for a single pair.

        Formula (matches spec):
            convergence = Σ(agent_score × agent_weight) over reporting agents
                          whose confidence ≥ MIN_AGENT_CONFIDENCE

        Deliberate choices:
          1. Confidence is NOT multiplied into the score. Confidence is a gate
             (drop the signal if below the floor) and reported separately as
             avg_confidence. Previously `score × confidence × weight` squashed
             convergence below reachable thresholds when the LLM returned
             modest confidences (0.3–0.5).
          2. No division by total_weight. Prior code rescaled surviving
             contributions when an agent was missing, silently boosting
             convergence the wrong way. Degradation is handled on the
             threshold side via compute_effective_threshold().
          3. Bias strings from the LLM can be compound ("neutral_bullish",
             "neutral_bearish"). We normalise by substring match so the
             directional consensus counts them correctly.
          4. Regime is excluded from the directional consensus. Regime emits
             a constant `bias = "neutral"` string and its score sign doesn't
             reliably indicate trend direction (TRENDING = +0.5 regardless of
             the actual trend). Its weight still contributes to the magnitude
             of the convergence score.

        Returns (convergence_score, bias, confidence, detail_dict).
        """
        MIN_AGENT_CONFIDENCE = 0.20

        detail: Dict[str, Any] = {"pair": pair}
        directional_sum = 0.0        # macro + technical weighted contributions only
        regime_score_captured = None  # captured separately; applied as multiplier
        weighted_confidence = 0.0
        agents_present: List[str] = []
        directional_biases: List[str] = []  # macro + technical only

        for agent_name, weight in CONVERGENCE_WEIGHTS.items():
            agent_signals = signals.get(agent_name, [])

            # Find the most recent signal for this pair.
            # Regime: prefer per-pair signal; fall back to GLOBAL (instrument IS NULL)
            # if no per-pair signal exists or per-pair score is exactly 0.00.
            pair_signal = None
            for sig in agent_signals:
                if agent_name == "regime":
                    if sig["instrument"] == pair:
                        pair_signal = sig
                        break
                else:
                    if sig["instrument"] == pair:
                        pair_signal = sig
                        break

            if agent_name == "regime":
                if pair_signal is None or float(pair_signal.get("score") or 0) == 0.0:
                    global_sig = None
                    for sig in agent_signals:
                        if sig["instrument"] is None:
                            global_sig = sig
                            break
                    if global_sig is not None:
                        pair_signal = global_sig
                        detail["regime_used_global_fallback"] = True

            if not pair_signal:
                detail[f"{agent_name}_missing"] = True
                logger.warning(f"No signal from {agent_name} for {pair}")
                continue

            # Staleness decay: exponential step-down based on signal age vs per-agent TTL.
            # TTL comes from SIGNAL_MAX_AGE_MINUTES[market_state][agent_name].
            # Within TTL            → 1.0   (full weight)
            # TTL + 0-10 min        → 0.85  (recently stale, probably just lagged)
            # TTL + 10-20 min       → 0.65  (noticeably stale)
            # > TTL + 20 min        → 0.0   (drop from convergence)
            stale_decay = 1.0
            signal_age_min = float(pair_signal.get("signal_age_min") or 0)
            if signal_age_min > 0:
                detail[f"{agent_name}_signal_age_min"] = round(signal_age_min, 1)
            _sig_created_at = pair_signal.get("created_at")
            if _sig_created_at is not None:
                _now_decay = datetime.datetime.now(datetime.timezone.utc)
                _mkt_st = get_market_state().get('state', 'active')
                _ttl = SIGNAL_MAX_AGE_MINUTES.get(_mkt_st, _SIGNAL_MAX_AGE_DEFAULT).get(
                    agent_name, 20
                )
                signal_age_minutes = (_now_decay - _sig_created_at).total_seconds() / 60
                if signal_age_minutes <= _ttl:
                    stale_decay = 1.0
                elif signal_age_minutes <= _ttl + 10:
                    stale_decay = 0.85
                elif signal_age_minutes <= _ttl + 20:
                    stale_decay = 0.65
                else:
                    stale_decay = 0.0
                if stale_decay < 1.0:
                    detail[f"{agent_name}_stale_decay"] = stale_decay
                    detail[f"{agent_name}_signal_age_min"] = round(signal_age_minutes, 1)

            score = float(pair_signal["score"] or 0) * stale_decay
            confidence = float(pair_signal["confidence"] or 0)
            raw_bias = (pair_signal["bias"] or "neutral").lower()

            # Confidence gate.
            if confidence < MIN_AGENT_CONFIDENCE:
                detail[f"{agent_name}_dropped_low_conf"] = round(confidence, 4)
                continue

            # Normalise compound bias labels.
            if "bullish" in raw_bias:
                norm_bias = "bullish"
            elif "bearish" in raw_bias:
                norm_bias = "bearish"
            else:
                norm_bias = "neutral"

            # Regime is a multiplier, not an additive component — capture score separately.
            if agent_name == "regime":
                regime_score_captured = score
            else:
                directional_sum += score * weight
            weighted_confidence += confidence * weight
            agents_present.append(agent_name)

            if agent_name != "regime":
                directional_biases.append(norm_bias)

            detail[f"{agent_name}_score"] = round(score, 4)
            detail[f"{agent_name}_confidence"] = round(confidence, 4)
            detail[f"{agent_name}_bias"] = pair_signal["bias"]
            detail[f"{agent_name}_bias_normalized"] = norm_bias

        if not agents_present:
            return 0.0, "neutral", 0.0, detail

        # Regime contributes additively to convergence (weight 0.20).
        # Additive avoids cascading collapse when only one directional agent clears
        # the confidence gate — regime's informational value is independent of
        # how many agents are contributing to directional_sum.
        # Score 0.0 (ranging)  -> +0.000 regime contribution.
        # Score 0.50 (neutral) -> +0.100 regime contribution.
        # Score 1.0 (trending) -> +0.200 regime contribution.
        _regime_score = regime_score_captured if regime_score_captured is not None else 0.50
        regime_additive = _regime_score * 0.20
        convergence = directional_sum + regime_additive
        detail["regime_score_used"] = round(_regime_score, 4)
        detail["regime_additive"] = round(regime_additive, 4)
        detail["directional_sum"] = round(directional_sum, 4)

        # Consensus bias from directional agents only (regime excluded).
        bullish = directional_biases.count("bullish")
        bearish = directional_biases.count("bearish")
        if bullish > bearish:
            consensus_bias = "bullish"
        elif bearish > bullish:
            consensus_bias = "bearish"
        else:
            consensus_bias = "neutral"

        # avg_confidence uses the weights of agents that actually reported.
        present_weight_sum = sum(CONVERGENCE_WEIGHTS[a] for a in agents_present)
        avg_confidence = (
            weighted_confidence / present_weight_sum if present_weight_sum > 0 else 0.0
        )

        # Gap C: per-pair regime confidence note (threshold boost applied elsewhere).
        pair_regime = regime_payload.get("pair_regimes", {}).get(pair, {})
        if pair_regime.get("regime_confidence") == "low":
            detail["regime_confidence_low"] = True

        detail["convergence_score"] = round(convergence, 4)
        detail["consensus_bias"] = consensus_bias
        detail["avg_confidence"] = round(avg_confidence, 4)
        detail["agents_reporting"] = len(agents_present)
        detail["min_confidence_gate"] = MIN_AGENT_CONFIDENCE

        return convergence, consensus_bias, avg_confidence, detail

    def evaluate_pair(
        self,
        pair: str,
        convergence: float,
        bias: str,
        confidence: float,
        effective_threshold: float,
        threshold_breakdown: Dict[str, Any],
        market_context: Dict[str, Any],
        risk_params: Dict[str, Any],
        open_positions: List[Dict[str, Any]],
        upcoming_events: List[Dict[str, Any]],
        technical_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Evaluate whether a pair should be approved for trading.
        Returns full decision record.
        """
        decision = {
            "pair": pair,
            "convergence": round(convergence, 4),
            "bias": bias,
            "confidence": round(confidence, 4),
            "effective_threshold": round(effective_threshold, 4),
            "approved": False,
            "rejection_reasons": [],
            "checks": {},
        }

        # === CHECK 1: Convergence vs threshold (O1 — hard floor) ===
        # convergence is now signed (negative = bearish); compare magnitude vs threshold.
        if abs(convergence) < effective_threshold:
            decision["rejection_reasons"].append(
                f"O1: Convergence {abs(convergence):.4f} below threshold {effective_threshold:.4f}"
            )
            decision["checks"]["convergence"] = "FAIL"
            warn("orchestrator_agent", "THRESHOLD", "Pair below convergence threshold",
                 pair=decision.get("pair", "unknown"), score=round(abs(convergence), 4),
                 threshold=round(effective_threshold, 4))
        else:
            decision["checks"]["convergence"] = "PASS"

        # === CHECK 2: Stress new entries allowed ===
        if not threshold_breakdown.get("stress_new_entries_allowed", True):
            decision["rejection_reasons"].append(
                f"Stress state {market_context.get('stress_state', 'unknown')} — "
                f"no new entries (score: {market_context.get('stress_score', 0)})"
            )
            decision["checks"]["stress_entries"] = "FAIL"
        else:
            decision["checks"]["stress_entries"] = "PASS"

        # === CHECK 3: Kill switch ===
        if market_context.get("kill_switch_active"):
            decision["rejection_reasons"].append("Kill switch active — no new entries")
            decision["checks"]["kill_switch"] = "FAIL"
        else:
            decision["checks"]["kill_switch"] = "PASS"

        # === CHECK 4: Session filter ===
        current_session = market_context.get("current_session", "unknown")
        session_filter = self.profile.get("session_filter")
        stress_session_restriction = threshold_breakdown.get("stress_session_restriction")

        # Apply most restrictive session filter
        allowed_sessions = None
        if session_filter and stress_session_restriction:
            allowed_sessions = list(set(session_filter) & set(stress_session_restriction))
        elif session_filter:
            allowed_sessions = session_filter
        elif stress_session_restriction:
            allowed_sessions = stress_session_restriction

        if allowed_sessions and current_session not in allowed_sessions:
            decision["rejection_reasons"].append(
                f"Session filter: {current_session} not in {allowed_sessions}"
            )
            decision["checks"]["session"] = "FAIL"
        else:
            decision["checks"]["session"] = "PASS"

        # === CHECK 5: Day filter ===
        day_of_week = market_context.get("day_of_week")
        day_filter = self.profile.get("day_filter")
        if day_of_week and day_filter and day_of_week not in day_filter:
            decision["rejection_reasons"].append(
                f"Day filter: day {day_of_week} not in {day_filter}"
            )
            decision["checks"]["day_filter"] = "FAIL"
        else:
            decision["checks"]["day_filter"] = "PASS"

        # === CHECK 6: NY close window — no new entries ===
        if market_context.get("ny_close_window"):
            decision["rejection_reasons"].append("NY close window (20:00-22:00 UTC) — no new entries")
            decision["checks"]["ny_close"] = "FAIL"
        else:
            decision["checks"]["ny_close"] = "PASS"

        # === CHECK 6.5: Off-hours hard block — no new entries 22:00–07:00 UTC ===
        # Covers dead hours between NY close and London open, regardless of profile.
        # Existing positions are managed normally — this only blocks new entries.
        _off_hours_utc = datetime.datetime.now(datetime.timezone.utc)
        if _off_hours_utc.hour >= 22 or _off_hours_utc.hour < 7:
            decision["rejection_reasons"].append(
                "off_hours_hard_block: no new entries 22:00–07:00 UTC"
            )
            decision["checks"]["off_hours"] = "FAIL"
        else:
            decision["checks"]["off_hours"] = "PASS"

        # === CHECK 6.6: Stop-hunt window — no new entries 07:00–07:30 UTC ===
        # London open first 30 min: retail stops hunted; spread widens; signals unreliable.
        if market_context.get("stop_hunt_window"):
            decision["rejection_reasons"].append(
                "stop_hunt_window: no entries during 07:00-07:30 UTC London open"
            )
            decision["checks"]["stop_hunt_window"] = "FAIL"
        else:
            decision["checks"]["stop_hunt_window"] = "PASS"

        # === CHECK 6.7: Cross-pair session filter ===
        # Cross pairs have specific liquidity windows; block outside those windows.
        _cross_sessions = CROSS_PAIR_SESSIONS.get(pair)
        _cur_session = market_context.get("current_session", "")
        if _cross_sessions and _cur_session not in _cross_sessions:
            decision["rejection_reasons"].append(
                f"cross_pair_session: {pair} only tradeable in {_cross_sessions}, "
                f"current={_cur_session}"
            )
            decision["checks"]["cross_pair_session"] = "FAIL"
        else:
            decision["checks"]["cross_pair_session"] = "PASS"

        # === CHECK 7: Friday cutoff — no new positions after 16:00 UTC Friday ===
        if day_of_week == 5:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if now_utc.hour >= 16:
                decision["rejection_reasons"].append("Friday after 16:00 UTC — no new positions")
                decision["checks"]["friday_cutoff"] = "FAIL"
            else:
                decision["checks"]["friday_cutoff"] = "PASS"
        else:
            decision["checks"]["friday_cutoff"] = "PASS"

        # === CHECK 8: Max open positions ===
        max_positions = int(risk_params.get("max_open_positions", 3))
        current_open = len(open_positions)
        if current_open >= max_positions:
            decision["rejection_reasons"].append(
                f"Max positions: {current_open}/{max_positions} slots used"
            )
            decision["checks"]["max_positions"] = "FAIL"
        else:
            decision["checks"]["max_positions"] = "PASS"

        # === CHECK 9: Circuit breaker ===
        if risk_params.get("circuit_breaker_active"):
            decision["rejection_reasons"].append("Circuit breaker active — no new entries")
            decision["checks"]["circuit_breaker"] = "FAIL"
        else:
            decision["checks"]["circuit_breaker"] = "PASS"

        # === CHECK 10: Spread-to-signal ratio ===
        # Fields live in technical_payload["risk_management"] sub-dict
        _risk_mgmt = technical_payload.get("risk_management", {})
        _tech_analysis = technical_payload.get("technical_analysis", {})
        min_spread_ratio = self.profile.get("min_spread_ratio", 5.0)
        # current_spread is the canonical field name from technical agent
        spread_at_entry = float(_risk_mgmt.get("current_spread") or
                                technical_payload.get("spread_pips") or 0)
        expected_pips = float(_risk_mgmt.get("expected_pips") or
                              technical_payload.get("expected_pips") or 0)

        if spread_at_entry > 0 and expected_pips > 0:
            actual_ratio = expected_pips / spread_at_entry
            if actual_ratio < min_spread_ratio:
                decision["rejection_reasons"].append(
                    f"Spread ratio: {actual_ratio:.1f}× < minimum {min_spread_ratio}× "
                    f"(spread: {spread_at_entry}, expected: {expected_pips})"
                )
                decision["checks"]["spread_ratio"] = "FAIL"
            else:
                decision["checks"]["spread_ratio"] = "PASS"
            decision["spread_ratio"] = round(actual_ratio, 2)
        else:
            decision["checks"]["spread_ratio"] = "SKIP"

        # === CHECK 11: Minimum R:R ===
        min_rr = self.profile.get("min_rr", 1.5)
        rr_ratio = float(_risk_mgmt.get("rr_ratio") or 0)

        if rr_ratio > 0:
            if rr_ratio < min_rr:
                decision["rejection_reasons"].append(
                    f"R:R ratio: {rr_ratio:.2f} < minimum {min_rr}"
                )
                decision["checks"]["min_rr"] = "FAIL"
            else:
                decision["checks"]["min_rr"] = "PASS"
            decision["rr_ratio"] = round(rr_ratio, 2)
        else:
            decision["checks"]["min_rr"] = "SKIP"

        # === CHECK 12: Bias is not neutral ===
        if bias == "neutral":
            decision["rejection_reasons"].append("No directional consensus — all agents neutral")
            decision["checks"]["directional_bias"] = "FAIL"
        else:
            decision["checks"]["directional_bias"] = "PASS"

        # === Propagate price / ATR fields for risk_guardian downstream use ===
        # These come from the technical agent payload; risk_guardian reads them
        # directly from the decision dict instead of doing a separate DB lookup.
        decision["current_price"] = _tech_analysis.get("current_price")
        decision["target_price"] = _risk_mgmt.get("target_price")
        decision["stop_price"] = _risk_mgmt.get("atr_stop_loss")
        decision["atr_14"] = (_tech_analysis.get("indicators") or {}).get("atr_14")

        # === FINAL DECISION ===
        if not decision["rejection_reasons"]:
            decision["approved"] = True
            # Calculate position size multiplier
            combined_mult = threshold_breakdown.get("combined_size_multiplier", 1.0)
            pre_event_reduction = 1.0
            if upcoming_events:
                reduction_pct = float(risk_params.get("pre_event_size_reduction", 50))
                pre_event_reduction = 1.0 - (reduction_pct / 100.0)
            decision["size_multiplier"] = round(combined_mult * pre_event_reduction, 4)

        return decision


# =============================================================================
# RISK BUDGET ANALYSER
# =============================================================================
class RiskBudgetAnalyser:
    """Analyses risk budget deployment for proposal generation."""

    @staticmethod
    def analyse(
        risk_params: Dict[str, Any],
        open_positions: List[Dict[str, Any]],
        daily_loss: float,
        market_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        daily_limit = float(risk_params.get("daily_loss_limit_pct", 3.0))
        max_positions = int(risk_params.get("max_open_positions", 3))
        current_open = len(open_positions)

        # Daily loss usage (approximate — assumes percentage of some account base)
        daily_loss_usage = abs(daily_loss / daily_limit * 100) if daily_limit > 0 else 0

        return {
            "daily_loss_limit_pct": daily_limit,
            "daily_loss_current": round(daily_loss, 2),
            "daily_loss_usage_pct": round(daily_loss_usage, 1),
            "max_positions": max_positions,
            "current_open": current_open,
            "positions_available": max_positions - current_open,
            "budget_underdeployed": (
                daily_loss_usage < 10
                and current_open < max_positions * 0.5
                and market_context.get("stress_score", 100) < 30
            ),
        }


# =============================================================================
# PROPOSAL GENERATOR
# =============================================================================
class OrchestratorProposalGenerator:
    """Generates proposals from orchestrator analysis."""

    @staticmethod
    def generate(
        decisions: List[Dict[str, Any]],
        risk_budget: Dict[str, Any],
        market_context: Dict[str, Any],
        threshold_breakdown: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        proposals = []

        # Risk budget underdeployed
        if risk_budget.get("budget_underdeployed"):
            session = market_context.get("current_session", "unknown")
            proposals.append({
                "type": "opportunity_flag",
                "priority": "medium",
                "title": "Risk budget significantly underdeployed",
                "reasoning": (
                    f"Daily loss usage at {risk_budget['daily_loss_usage_pct']:.0f}%, "
                    f"only {risk_budget['current_open']}/{risk_budget['max_positions']} "
                    f"positions open during {session} session with Normal stress. "
                    f"Strong setups are being left on the table."
                ),
                "data_supporting": json.dumps(risk_budget),
                "suggested_action": "Review convergence threshold — may be too restrictive for current conditions.",
                "expected_impact": "Better capital efficiency during favourable conditions.",
            })

        # Near-miss trades (within 0.05 of threshold)
        near_misses = [
            d for d in decisions
            if not d["approved"]
            and d["convergence"] >= d["effective_threshold"] - 0.05
            and d["convergence"] < d["effective_threshold"]
            and len(d["rejection_reasons"]) == 1
            and "O1" in d["rejection_reasons"][0]
        ]
        if near_misses:
            pairs = [d["pair"] for d in near_misses]
            proposals.append({
                "type": "strategy_observation",
                "priority": "low",
                "title": f"Near-miss setups: {', '.join(pairs)}",
                "reasoning": (
                    f"{len(near_misses)} pair(s) scored within 0.05 of threshold but were "
                    f"rejected by O1. These may represent viable setups that the threshold "
                    f"is filtering out."
                ),
                "data_supporting": json.dumps([
                    {"pair": d["pair"], "score": d["convergence"],
                     "threshold": d["effective_threshold"]}
                    for d in near_misses
                ]),
                "suggested_action": "Monitor these pairs — if they frequently near-miss and resolve profitably, threshold may need review.",
                "expected_impact": "Identifying systematic threshold over-filtering.",
            })

        # High degradation impacting threshold
        total_deg = threshold_breakdown.get("total_degradation_boost", 0)
        if total_deg >= 0.10:
            proposals.append({
                "type": "risk_budget",
                "priority": "high",
                "title": f"Agent degradation adding +{total_deg:.2f} to threshold",
                "reasoning": (
                    f"One or more analysis agents are degraded, adding {total_deg:.2f} "
                    f"to the convergence threshold. This significantly reduces trading activity."
                ),
                "data_supporting": json.dumps({
                    k: v for k, v in threshold_breakdown.items()
                    if "degradation" in k
                }),
                "suggested_action": "Check agent health in admin dashboard. Restart degraded agents.",
                "expected_impact": "Restoring agents removes threshold penalty.",
            })

        # All pairs rejected
        if decisions and all(not d["approved"] for d in decisions):
            common_reasons = {}
            for d in decisions:
                for r in d["rejection_reasons"]:
                    key = r.split(":")[0].strip()
                    common_reasons[key] = common_reasons.get(key, 0) + 1
            most_common = max(common_reasons, key=common_reasons.get) if common_reasons else "unknown"
            proposals.append({
                "type": "strategy_observation",
                "priority": "low",
                "title": f"All {len(decisions)} pairs rejected — dominant: {most_common}",
                "reasoning": (
                    f"No pairs met approval criteria this cycle. Most common rejection: "
                    f"{most_common} ({common_reasons.get(most_common, 0)}/{len(decisions)} pairs)."
                ),
                "data_supporting": json.dumps(common_reasons),
                "suggested_action": "No action needed if market conditions warrant caution.",
                "expected_impact": "Informational — helps identify patterns of inactivity.",
            })

        return proposals


# =============================================================================
# SIGNAL WRITER
# =============================================================================
class OrchestratorSignalWriter:
    """Writes orchestrator signals, heartbeats, and approved trades."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        # Restore cycle_count across restarts
        saved_count = load_state('orchestrator', AGENT_SCOPE_USER_ID, 'cycle_count', default=0)
        if isinstance(saved_count, int) and saved_count > 0:
            self.cycle_count = saved_count
            logger.info(f"[state] Restored orchestrator cycle_count={self.cycle_count}")

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
            logger.error(f"Heartbeat write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def write_orchestrator_signal(self, payload: Dict[str, Any]):
        """Write the orchestrator's full cycle output as a signal."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES (%s, %s, NULL, 'orchestrator_decision', 0.0, 'neutral',
                        1.0, %s, NOW() + INTERVAL '20 minutes')
            """, (AGENT_NAME, self.user_id, json.dumps(payload)))
            self.db.commit()
        except Exception as e:
            logger.error(f"Orchestrator signal write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def write_trade_approval(self, decision: Dict[str, Any]):
        """Write an approved trade signal for the Risk Guardian to pick up."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES ('orchestrator', %s, %s, 'trade_approval', %s, %s,
                        %s, %s, NOW() + INTERVAL '20 minutes')
            """, (
                self.user_id,
                decision["pair"],
                decision["convergence"],
                decision["bias"],
                decision["confidence"],
                json.dumps(decision),
            ))
            self.db.commit()
            logger.info(
                f"✅ TRADE APPROVED: {decision['pair']} {decision['bias']} "
                f"(convergence: {decision['convergence']:.4f}, "
                f"size_mult: {decision.get('size_multiplier', 1.0)})"
            )
        except Exception as e:
            logger.error(f"Trade approval write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()


# =============================================================================
# ORCHESTRATOR AGENT — MAIN CYCLE
# =============================================================================
class OrchestratorAgent:
    """Main orchestrator — reads signals, computes convergence, approves trades."""

    def __init__(self, user_id: str, dry_run: bool = False):
        self.user_id = user_id
        self.dry_run = dry_run
        self.db = DatabaseConnection()
        self.db.connect()
        validate_schema(self.db.conn, EXPECTED_TABLES)

        # Resolve Cognito username to UUID if needed
        self.user_id = self._resolve_user_id(user_id)

        self.signal_reader = SignalReader(self.db, self.user_id)
        self._validator = SignalValidator()
        self.convergence_calc = ConvergenceCalculator(self.user_id, self.db)
        self.signal_writer = OrchestratorSignalWriter(self.db, self.user_id)
        # (user_id, instrument) -> cycles_remaining; raised threshold +0.05 per cycle
        self._stop_loss_penalties: dict = {}

    def _resolve_user_id(self, user_id: str) -> str:
        """Resolve a Cognito username like 'neo_user_002' to its UUID from risk_parameters."""
        if '-' in user_id and len(user_id) > 30:
            return user_id  # Already a UUID
        try:
            cur = self.db.cursor()
            cur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
            rows = cur.fetchall()
            cur.close()
            idx = {"neo_user_001": 0, "neo_user_002": 1, "neo_user_003": 2}.get(user_id, -1)
            if idx >= 0 and idx < len(rows):
                resolved = str(rows[idx]["user_id"]) if isinstance(rows[idx], dict) else str(rows[idx][0])
                logger.info(f"Resolved {user_id} → {resolved}")
                return resolved
        except Exception as e:
            logger.warning(f"UUID resolution failed: {e}")
        return user_id

    def _write_rejected_signal(
        self, pair: str, reason: str, convergence: float, threshold: float,
        bias: str = None, macro_score: float = None, tech_score: float = None,
        regime_score: float = None, current_price: float = None,
        payload: dict = None,
    ) -> None:
        """Persist one row to forex_network.rejected_signals."""
        if not self.db or not pair:
            return
        r_lower = reason.lower()
        if "missing_signal" in r_lower:
            stage = "signal_gate"
        elif "macro_gate_fail" in r_lower:
            stage = "macro_gate"
        elif "technical_too_weak" in r_lower:
            stage = "technical_gate"
        elif "consensus" in r_lower or "directional" in r_lower or "conflict" in r_lower:
            stage = "directional"
        elif "threshold" in r_lower or "below" in r_lower:
            stage = "threshold"
        elif "session" in r_lower or "stop_hunt" in r_lower or "ny_close" in r_lower or "friday" in r_lower or "off_hours" in r_lower:
            stage = "session_gate"
        elif "spread" in r_lower:
            stage = "spread_gate"
        elif "rr" in r_lower or "risk_reward" in r_lower:
            stage = "rr_gate"
        elif "daily_loss" in r_lower or "drawdown" in r_lower or "circuit" in r_lower or "kill" in r_lower:
            stage = "risk_gate"
        elif "portfolio" in r_lower or "max_position" in r_lower or "max positions" in r_lower:
            stage = "capacity"
        else:
            stage = "other"
        try:
            cur = self.db.cursor()
            cur.execute(
                """
                INSERT INTO forex_network.rejected_signals
                    (user_id, instrument, direction, rejection_stage, rejection_reason,
                     macro_score, technical_score, regime_score,
                     convergence_score, convergence_threshold,
                     price_at_rejection, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(self.user_id), pair, (bias or "")[:5], stage, reason[:200],
                    float(macro_score) if macro_score is not None else None,
                    float(tech_score) if tech_score is not None else None,
                    float(regime_score) if regime_score is not None else None,
                    float(convergence), float(threshold),
                    float(current_price) if current_price is not None else None,
                    json.dumps(payload) if payload else None,
                ),
            )
            cur.close()
            self.db.commit()
        except Exception as _e:
            logger.warning(f"rejected_signals insert failed for {pair}: {_e}")
            try:
                self.db.rollback()
            except Exception:
                pass

    def _write_shadow_trade(
        self, pair: str, bias: str,
        macro_score: float, tech_score: float, regime_score: float,
        rejection_reason: str,
        hypothesis: str = None,
        macro_gate_passed: bool = None,
        tech_gate_passed: bool = None,
        regime_agrees: bool = None,
        cot_multiplier: float = None,
        p75_threshold: float = None,
        effective_threshold: float = None,
        would_have_traded: bool = None,
    ) -> None:
        """Record a shadow trade hypothesis row.
        hypothesis maps the 5 learning hypotheses (macro_only, tech_threshold_0.10,
        macro_regime_no_tech, p75_relaxed, directional_disagreement).
        price_at_signal/pips populated nightly by backfill_shadow_trades.py."""
        if not self.db or not pair:
            return
        direction = (bias or "")[:5]  # 'bulli' or 'beari'
        hyp = (hypothesis or rejection_reason or "unknown")[:50]
        try:
            cur = self.db.cursor()
            cur.execute(
                """
                INSERT INTO forex_network.shadow_trades
                    (instrument, direction, signal_time,
                     macro_score, tech_score, regime_score,
                     rejection_reason, user_id,
                     hypothesis, macro_gate_passed, tech_gate_passed,
                     regime_agrees, cot_multiplier,
                     p75_threshold, effective_threshold, would_have_traded)
                VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pair, direction,
                    float(macro_score) if macro_score is not None else None,
                    float(tech_score) if tech_score is not None else None,
                    float(regime_score) if regime_score is not None else None,
                    rejection_reason[:50] if rejection_reason else None,
                    str(self.user_id),
                    hyp,
                    macro_gate_passed,
                    tech_gate_passed,
                    regime_agrees,
                    float(cot_multiplier) if cot_multiplier is not None else None,
                    float(p75_threshold) if p75_threshold is not None else None,
                    float(effective_threshold) if effective_threshold is not None else None,
                    would_have_traded,
                ),
            )
            cur.close()
            self.db.commit()
        except Exception as _e:
            logger.warning(f"shadow_trade insert failed for {pair}: {_e}")
            try:
                self.db.rollback()
            except Exception:
                pass

    def build_entry_context(self, instrument: str, convergence_score: float,
                             bias: str, market_context: Dict[str, Any],
                             signals: Dict[str, Any]) -> Dict[str, Any]:
        """
        Snapshot the current sentiment and market state at trade entry.
        Used by the learning module to correlate entry conditions with outcomes.
        """
        context: Dict[str, Any] = {}

        cur = self.db.cursor()
        try:
            # MyFXBook SSI (sentiment_ssi columns: long_pct, short_pct)
            try:
                cur.execute("""
                    SELECT long_pct, short_pct
                    FROM forex_network.sentiment_ssi
                    WHERE instrument = %s AND ts >= NOW() - INTERVAL '26 hours'
                    ORDER BY ts DESC LIMIT 1
                """, (instrument,))
                row = cur.fetchone()
                if row:
                    context['ssi_myfxbook'] = {
                        'long_pct':  float(row['long_pct']),
                        'short_pct': float(row['short_pct']),
                    }
            except Exception:
                pass

            # IG Client Sentiment
            try:
                cur.execute("""
                    SELECT long_percentage, short_percentage
                    FROM forex_network.ig_client_sentiment
                    WHERE instrument = %s AND ts >= NOW() - INTERVAL '26 hours'
                    ORDER BY ts DESC LIMIT 1
                """, (instrument,))
                row = cur.fetchone()
                if row:
                    context['ssi_ig'] = {
                        'long_pct': float(row['long_percentage']),
                        'short_pct': float(row['short_percentage']),
                    }
            except Exception:
                pass

            # Consensus grade
            if 'ssi_myfxbook' in context and 'ssi_ig' in context:
                mfx_short = context['ssi_myfxbook']['short_pct']
                ig_short = context['ssi_ig']['short_pct']
                both_same_side = (mfx_short > 50 and ig_short > 50) or                                   (mfx_short < 50 and ig_short < 50)
                mfx_extreme = max(mfx_short, 100 - mfx_short)
                ig_extreme = max(ig_short, 100 - ig_short)
                if both_same_side and mfx_extreme > 75 and ig_extreme > 75:
                    context['consensus_grade'] = 'HIGH'
                elif both_same_side and max(mfx_extreme, ig_extreme) > 60:
                    context['consensus_grade'] = 'MODERATE'
                else:
                    context['consensus_grade'] = 'MIXED'

            # Extremity bucket
            max_extreme = 0.0
            for src in ('ssi_myfxbook', 'ssi_ig'):
                if src in context:
                    short_pct = context[src]['short_pct']
                    max_extreme = max(max_extreme, max(short_pct, 100 - short_pct))
            if max_extreme >= 90:
                context['extremity_bucket'] = '90+'
            elif max_extreme >= 80:
                context['extremity_bucket'] = '80-90'
            elif max_extreme >= 70:
                context['extremity_bucket'] = '70-80'
            elif max_extreme >= 60:
                context['extremity_bucket'] = '60-70'
            else:
                context['extremity_bucket'] = '<60'

            # COT direction
            try:
                cur.execute("""
                    SELECT net_positions, pct_signed_52w
                    FROM shared.cot_positioning
                    WHERE instrument = %s ORDER BY report_date DESC LIMIT 1
                """, (instrument,))
                row = cur.fetchone()
                if row:
                    cot_net = float(row['net_positions'] or 0)
                    context['cot_net_position'] = cot_net
                    if bias == 'bullish':
                        context['cot_direction'] = 'confirming' if cot_net > 0 else 'opposing'
                    elif bias == 'bearish':
                        context['cot_direction'] = 'confirming' if cot_net < 0 else 'opposing'
                    else:
                        context['cot_direction'] = 'neutral'
            except Exception:
                pass

        finally:
            cur.close()
            # Reset any aborted-transaction state left by failed inner queries
            # (psycopg2 marks connection as aborted on any uncaught query error;
            # swallowed exceptions don't reset it — rollback does).
            try:
                self.db.rollback()
            except Exception:
                pass

        # Session and convergence from live context
        context['session'] = market_context.get('current_session', 'unknown')
        context['convergence_score'] = round(convergence_score, 4)
        stress = market_context.get('stress_score')
        if stress is not None:
            context['stress_score'] = round(float(stress), 2)

        # Macro and tech confidence from signals
        macro_sigs = signals.get('macro', [])
        tech_sigs = signals.get('technical', [])
        macro_sig = next((s for s in macro_sigs if s.get('instrument') == instrument), {})
        tech_sig = next((s for s in tech_sigs if s.get('instrument') == instrument), {})
        if macro_sig:
            context['macro_confidence'] = round(float(macro_sig.get('confidence') or 0), 3)
        if tech_sig:
            context['tech_confidence'] = round(float(tech_sig.get('confidence') or 0), 3)

        # ── NEW: per-agent raw scores ──────────────────────────────────────────
        context['macro_score'] = float(macro_sig.get('score') or 0) if macro_sig else None
        context['tech_score']  = float(tech_sig.get('score')  or 0) if tech_sig  else None

        # Per-instrument regime signal + global regime payload for pair data
        _regime_sigs = signals.get('regime', [])
        _regime_sig  = next((s for s in _regime_sigs if s.get('instrument') == instrument), {})
        _regime_global = next((s for s in _regime_sigs if s.get('instrument') is None), {})
        _pair_regime = (
            (_regime_global.get('payload') or {})
            .get('pair_regimes', {})
            .get(instrument, {})
        )

        context['regime_score'] = float(_regime_sig.get('score') or 0) if _regime_sig else None

        # ── NEW: trajectory features from each agent's payload ────────────────
        def _safe_payload(sig):
            p = (sig or {}).get('payload') or {}
            if isinstance(p, str):
                try:    return json.loads(p)
                except: return {}
            return p if isinstance(p, dict) else {}

        _mpay = _safe_payload(macro_sig)
        _tpay = _safe_payload(tech_sig)
        _rpay = _safe_payload(_regime_sig)

        context['macro_trajectory']  = _mpay.get('trajectory')
        context['tech_trajectory']   = _tpay.get('trajectory')
        context['regime_trajectory'] = _rpay.get('trajectory')

        # ── NEW: per-pair regime classification ───────────────────────────────
        context['regime_classification'] = (
            _pair_regime.get('regime') if isinstance(_pair_regime, dict) else None
        )
        context['regime_adx'] = (
            float(_pair_regime['adx']) if isinstance(_pair_regime, dict)
                                          and _pair_regime.get('adx') is not None else None
        )

        # ── NEW: agents_agreed ────────────────────────────────────────────────
        _agreed = []
        if (macro_sig or {}).get('bias') == bias: _agreed.append('macro')
        if (tech_sig  or {}).get('bias') == bias: _agreed.append('tech')
        if (_regime_sig or {}).get('bias') == bias: _agreed.append('regime')
        context['agents_agreed'] = '+'.join(_agreed) if _agreed else 'none'

        # ── NEW: geopolitical tension + persistence ───────────────────────────
        context['geopolitical_tension'] = _mpay.get('geopolitical_tension')
        context['macro_persistence']    = _mpay.get('persistence_count')
        context['tech_persistence']     = _tpay.get('persistence_count')

        return context

    # -------------------------------------------------------------------------
    # Restart safety helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _is_signal_fresh(signal: Dict[str, Any], agent_name: str = 'technical',
                         market_state: str = 'active') -> bool:
        """Return False if the signal's created_at is older than the per-agent,
        per-market-state threshold. Quiet-session macro cycles run every 60 min
        so the threshold is relaxed to 65 min to avoid false-absent flags.
        """
        if not signal or not signal.get("created_at"):
            return False
        created = signal["created_at"]
        if created.tzinfo is None:
            created = created.replace(tzinfo=datetime.timezone.utc)
        age_minutes = (
            datetime.datetime.now(datetime.timezone.utc) - created
        ).total_seconds() / 60
        thresholds = SIGNAL_MAX_AGE_MINUTES.get(market_state, _SIGNAL_MAX_AGE_DEFAULT)
        max_age = thresholds.get(agent_name, 25)
        return age_minutes <= max_age

    def _expire_signal(self, signal_id: int) -> None:
        """Immediately expire a signal by setting expires_at = NOW()."""
        cur = self.db.cursor()
        try:
            cur.execute(
                "UPDATE forex_network.agent_signals SET expires_at = NOW() WHERE id = %s",
                (signal_id,),
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"Signal expiry failed for id={signal_id}: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def validate_pending_proposals(self, signals: Dict[str, List[Dict[str, Any]]]) -> int:
        """Cancel any pending trade_approval signals whose direction is contradicted
        by the current macro or technical signals.

        Called at the start of each cycle (after signals are read) so that if the
        macro agent restarted and flipped direction, existing proposals are cancelled
        before the Risk Guardian picks them up.

        Returns the number of proposals cancelled.
        """
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, instrument, bias
                FROM forex_network.agent_signals
                WHERE user_id = %s
                  AND signal_type = 'trade_approval'
                  AND expires_at > NOW()
            """, (self.user_id,))
            pending = cur.fetchall()
        except Exception as e:
            logger.error(f"Pending proposal read failed: {e}")
            return 0
        finally:
            cur.close()

        if not pending:
            return 0

        cancelled = 0
        for row in pending:
            signal_id = row["id"]
            pair = row["instrument"]
            proposed_bias = (row["bias"] or "neutral").lower()

            if proposed_bias == "neutral":
                continue  # non-directional — nothing to validate

            current_macro = next(
                (s for s in signals.get("macro", []) if s.get("instrument") == pair), None
            )
            current_tech = next(
                (s for s in signals.get("technical", []) if s.get("instrument") == pair), None
            )

            def _norm_bias(sig: Optional[Dict]) -> Optional[str]:
                if not sig:
                    return None
                raw = (sig.get("bias") or "neutral").lower()
                if "bullish" in raw:
                    return "bullish"
                if "bearish" in raw:
                    return "bearish"
                return None  # neutral — not a definitive direction

            macro_bias = _norm_bias(current_macro)
            tech_bias = _norm_bias(current_tech)

            cancel_reason = None
            if macro_bias and macro_bias != proposed_bias:
                cancel_reason = f"macro flipped to {current_macro.get('bias')}"
            elif tech_bias and tech_bias != proposed_bias:
                cancel_reason = f"technical flipped to {current_tech.get('bias')}"

            if cancel_reason:
                logger.warning(
                    f"CANCELLING {pair} {proposed_bias} proposal (id={signal_id}) — {cancel_reason}"
                )
                self._expire_signal(signal_id)
                cancelled += 1

        if cancelled:
            logger.info(f"Cancelled {cancelled} stale proposal(s) due to signal direction change")
        return cancelled

    def _check_stop_loss_penalties(self) -> None:
        """
        Decrement active stop-loss convergence penalties and register new ones.
        Called once per cycle before pair evaluation.
        A stop-loss exit raises that instrument's effective threshold by +0.05
        for the next 2 orchestrator cycles (~10 minutes).
        """
        # Decrement existing counters; remove expired
        for key in list(self._stop_loss_penalties):
            self._stop_loss_penalties[key] -= 1
            if self._stop_loss_penalties[key] <= 0:
                del self._stop_loss_penalties[key]
                logger.info(
                    f"Stop loss penalty expired: {key[1]} for user {key[0][:8]}"
                )

        # Register new stop losses from last 6 minutes
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT user_id, instrument
                FROM forex_network.trades
                WHERE exit_reason IN (
                    'stop_loss', 'stop_hit', 'trailing_stop',
                    'max_hold_exceeded', 'stop'
                )
                AND exit_time > NOW() - INTERVAL '6 minutes'
            """)
            for row in cur.fetchall():
                key = (str(row["user_id"]), row["instrument"])
                self._stop_loss_penalties[key] = 2
                logger.info(
                    f"Stop loss penalty activated: {row['instrument']} "
                    f"for user {str(row['user_id'])[:8]} — "
                    f"threshold +0.05 for next 2 cycles"
                )
        except Exception as e:
            logger.warning(f"Stop loss penalty check failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
        finally:
            cur.close()

    def run_cycle(self) -> Dict[str, Any]:
        cycle_start = time.time()
        self.signal_writer.cycle_count += 1
        save_state('orchestrator', AGENT_SCOPE_USER_ID, 'cycle_count', self.signal_writer.cycle_count)
        logger.info(f"=== ORCHESTRATOR CYCLE {self.signal_writer.cycle_count} START === (5-min interval, checking for fresh signals)")

        # 0. Kill switch
        if check_kill_switch():
            logger.warning("Kill switch ACTIVE — heartbeat only.")
            self.signal_writer.write_heartbeat()
            return {"status": "kill_switch_active"}

        # 0a. Maintenance mode — skip trade evaluation during rolling agent restarts
        if check_maintenance_mode():
            logger.info("MAINTENANCE MODE — skipping trade evaluation this cycle.")
            self.signal_writer.write_heartbeat()
            return {"status": "maintenance_mode"}

        # 1. Heartbeat
        self.signal_writer.write_heartbeat()

        # 2. Read all inputs
        signals = self.signal_reader.read_latest_signals()
        market_context = self.signal_reader.read_market_context()
        risk_params = self.signal_reader.read_risk_parameters()
        degradation = self.signal_reader.read_degradation_state()

        # 2b. Cancel any in-flight proposals whose direction has been contradicted by fresh signals
        self.validate_pending_proposals(signals)

        # E. Agent heartbeat stale alert
        # Per-agent thresholds: macro/regime 30 min (15-min cycles, 2 missed),
        # technical 10 min (5-min cycles, 2 missed)
        _HEARTBEAT_STALE_MIN = {
            "macro":     30,
            "technical": 10,
            "regime":    30,
        }
        for _agent_name, _agent_state in degradation.items():
            _last_seen = _agent_state.get("last_seen")
            if _last_seen:
                _mins_ago = (
                    datetime.datetime.now(datetime.timezone.utc) - _last_seen
                ).total_seconds() / 60
                _stale_min = _HEARTBEAT_STALE_MIN.get(_agent_name, 30)
                if _mins_ago > _stale_min:
                    send_alert(
                        'CRITICAL',
                        f'Agent {_agent_name} heartbeat stale ({_mins_ago:.0f}min)',
                        {'agent': _agent_name, 'last_heartbeat_minutes_ago': round(_mins_ago, 1)},
                        source_agent='orchestrator',
                    )

        open_positions = self.signal_reader.read_open_positions()
        daily_loss = self.signal_reader.read_daily_loss()
        upcoming_events = self.signal_reader.check_upcoming_events()

        logger.info(
            f"Signals: macro={len(signals['macro'])}, "
            f"technical={len(signals['technical'])}, regime={len(signals['regime'])}"
        )
        logger.info(
            f"Context: stress={market_context.get('stress_score', 'N/A')}, "
            f"session={market_context.get('current_session', 'N/A')}, "
            f"open_positions={len(open_positions)}"
        )

        # 3. Check for technical agent halt (degradation)
        tech_state = degradation.get("technical", {})
        if tech_state.get("degradation_mode") == "halted":
            logger.warning("Technical agent HALTED — no entry signals possible.")
            self.signal_writer.write_heartbeat()
            if not self.dry_run:
                self.signal_writer.write_orchestrator_signal({
                    "status": "technical_agent_halted",
                    "cycle": self.signal_writer.cycle_count,
                })
            return {"status": "technical_agent_halted"}

        # 4. Compute effective threshold
        effective_threshold, threshold_breakdown = self.convergence_calc.compute_effective_threshold(
            risk_params=risk_params,
            market_context=market_context,
            degradation=degradation,
            upcoming_events=upcoming_events,
        )

        # 5. Get regime payload for per-pair context
        regime_payload = {}
        for sig in signals.get("regime", []):
            if sig["instrument"] is None:
                regime_payload = sig.get("payload", {})
                break

        # Regime signal contract check (once per cycle)
        if regime_payload:
            self._validator.validate_and_log("regime", regime_payload, None, logger)

        # 6. Evaluate each pair
        # Check for recent stop losses and update per-pair convergence penalties
        self._check_stop_loss_penalties()
        current_session = market_context.get("current_session", "unknown")
        _mkt_state = get_market_state().get('state', 'active')
        decisions = []
        approved_this_cycle = set()  # tracks instruments approved this cycle to prevent stale-read duplicates

        # ── Pass 1: score every pair ──────────────────────────────────────────
        # Convergence history writes happen here, in original FX_PAIRS order.
        _allowed = risk_params.get("allowed_instruments")  # None = all pairs
        _scored = []
        for pair in FX_PAIRS:
            if _allowed is not None and pair not in _allowed:
                continue  # instrument filtered by risk_parameters.allowed_instruments
            base_convergence, bias, confidence, detail = self.convergence_calc.compute_pair_convergence(
                pair=pair,
                signals=signals,
                regime_payload=regime_payload,
            )

            # Write base convergence to history (Feature 2)
            if not self.dry_run:
                self.convergence_calc.write_convergence_history(
                    instrument=pair,
                    convergence_score=base_convergence,
                    macro_score=detail.get("macro_score", 0.0),
                    technical_score=detail.get("technical_score", 0.0),
                    regime_score=detail.get("regime_score", 0.0),
                    bias=bias,
                )

            # Apply historical bonuses (Features 1, 2, 3)
            pair_regime = regime_payload.get("pair_regimes", {}).get(pair, {})
            regime_str = pair_regime.get("regime", "ranging") if isinstance(pair_regime, dict) else "ranging"
            final_convergence, historical_detail = self.convergence_calc.compute_final_convergence(
                instrument=pair,
                base_convergence=base_convergence,
                regime=regime_str,
                session=current_session,
                bias=bias,
            )
            detail["historical_bonus"] = historical_detail
            _scored.append((pair, final_convergence, bias, confidence, detail))

        # ── Pass 2: evaluate in top-conviction order ──────────────────────────
        # Sort by absolute convergence descending — highest-conviction pairs
        # get capacity slots first regardless of their position in FX_PAIRS.
        # All gates, checks, and side-effects below are unchanged.
        _scored.sort(key=lambda x: abs(x[1]), reverse=True)

        # Load p50/p75 pair-spread thresholds once — replaces per-pair PERCENTILE_CONT queries.
        # Populated weekly by scripts/compute_macro_percentiles.py.
        # Key: pair (e.g. 'EURJPY'), value: percentile of abs(base.composite - quote.composite) over 25yr.
        _macro_p75: Dict[str, float] = {}
        _macro_p50: Dict[str, float] = {}
        try:
            _pct_cur = self.db.cursor()
            _pct_cur.execute(
                "SELECT pair, p50, p75 FROM forex_network.macro_percentile_thresholds"
            )
            for _row in _pct_cur.fetchall():
                _k = _row['pair'].strip()
                _macro_p75[_k] = float(_row['p75'])
                _macro_p50[_k] = float(_row['p50'])
            _pct_cur.close()
            if _macro_p75:
                logger.debug(f"Loaded p50/p75 macro thresholds for {len(_macro_p75)} pairs")
        except Exception as _pct_e:
            logger.warning(f"macro_percentile_thresholds unavailable: {_pct_e} — fixed threshold only")

        for pair, final_convergence, bias, confidence, detail in _scored:
            # >>> DIRECTIONAL GATE — runs BEFORE threshold comparison <<<
            macro_sig = next(
                (s for s in signals.get("macro", []) if s.get("instrument") == pair), None
            )
            tech_sig = next(
                (s for s in signals.get("technical", []) if s.get("instrument") == pair), None
            )

            # Signal freshness gate — treat near-expiry signals as absent so we don't
            # act on data from the restart/cycle-gap window.
            # Thresholds are market-state-aware: quiet-session macro cycles every 60 min.
            if macro_sig and not self._is_signal_fresh(macro_sig, 'macro', _mkt_state):
                _age = (
                    datetime.datetime.now(datetime.timezone.utc) - macro_sig["created_at"]
                ).total_seconds() / 60
                logger.warning(f"{pair}: macro signal stale ({_age:.0f}min old, {_mkt_state}) — treating as absent")
                macro_sig = None
            if tech_sig and not self._is_signal_fresh(tech_sig, 'technical', _mkt_state):
                _age = (
                    datetime.datetime.now(datetime.timezone.utc) - tech_sig["created_at"]
                ).total_seconds() / 60
                logger.warning(f"{pair}: technical signal stale ({_age:.0f}min old, {_mkt_state}) — treating as absent")
                tech_sig = None

            # Signal contract validation (diagnostic only — never blocks)
            if macro_sig:
                self._validator.validate_and_log("macro", macro_sig, pair, logger)
            if tech_sig:
                self._validator.validate_and_log("technical", tech_sig, pair, logger)

            if not macro_sig or not tech_sig:
                dc_reason = f"missing_signal: macro={'present' if macro_sig else 'absent'}, technical={'present' if tech_sig else 'absent'}"
                logger.info(f"{pair}: REJECTED — {dc_reason}")
                warn("orchestrator_agent", "MISSING_SIGNAL", "Pair rejected — signal absent",
                     pair=pair, macro="present" if macro_sig else "absent",
                     technical="present" if tech_sig else "absent")
                decisions.append({
                    "pair": pair,
                    "convergence": round(final_convergence, 4),
                    "bias": bias,
                    "confidence": round(confidence, 4),
                    "effective_threshold": round(effective_threshold, 4),
                    "approved": False,
                    "rejection_reasons": [dc_reason],
                    "checks": {"directional_gate": "FAIL"},
                    "convergence_detail": detail,
                })
                self._write_rejected_signal(
                    pair=pair, reason=dc_reason,
                    convergence=final_convergence, threshold=effective_threshold,
                    bias=bias,
                    macro_score=float(macro_sig.get("score") or 0) if macro_sig else None,
                    tech_score=float(tech_sig.get("score") or 0) if tech_sig else None,
                    regime_score=detail.get("regime_score"),
                    payload={"rejection_reasons": [dc_reason]},
                )
                continue

            # ── HIERARCHICAL SIGNAL GATE ─────────────────────────────────────
            # Layer 1 — macro gate
            MACRO_THRESHOLD    = float(risk_params.get('convergence_threshold', 0.30))
            TECH_MIN_THRESHOLD = 0.10

            _macro_score = float(macro_sig.get('score') or 0)
            _tech_score  = float(tech_sig.get('score') or 0)

            # Percentile gate: p75 of abs(base - quote) spread from 25yr history.
            # effective = max(fixed_threshold, p75) — tighter in rangy markets,
            # never looser than convergence_threshold risk parameter.
            # Falls back to fixed threshold if table not yet populated for this pair.
            _p75 = _macro_p75.get(pair)
            _effective_macro_threshold = max(MACRO_THRESHOLD, _p75) if _p75 else MACRO_THRESHOLD
            detail["effective_macro_threshold"] = round(_effective_macro_threshold, 4)
            detail["p75_macro_threshold"]       = round(_p75, 4) if _p75 is not None else None

            def _gate_reject(_reason, _pair=pair, _ms=macro_sig, _ts=tech_sig):
                _stress_val = float(market_context.get("stress_score", 0) or 0)
                _cat = self.convergence_calc.classify_conflict(_ms, _ts, _stress_val)
                logger.info(
                    f"{_pair}: REJECTED — {_reason} "
                    f"(macro={float(_ms.get('score') or 0):+.3f}, "
                    f"tech={float(_ts.get('score') or 0):+.3f}, "
                    f"convergence={final_convergence:.3f})"
                )
                self.convergence_calc._write_rejection(
                    _pair, _reason, _cat, _ms, _ts, _stress_val,
                )
                decisions.append({
                    "pair": _pair,
                    "convergence": round(final_convergence, 4),
                    "bias": bias,
                    "confidence": round(confidence, 4),
                    "effective_threshold": round(effective_threshold, 4),
                    "approved": False,
                    "rejection_reasons": [_reason],
                    "conflict_category": _cat,
                    "checks": {"directional_gate": "FAIL"},
                    "convergence_detail": detail,
                })
                self._write_rejected_signal(
                    pair=_pair, reason=_reason,
                    convergence=final_convergence, threshold=effective_threshold,
                    bias=bias,
                    macro_score=float(_ms.get("score") or 0),
                    tech_score=float(_ts.get("score") or 0),
                    regime_score=detail.get("regime_score"),
                    payload={"rejection_reasons": [_reason], "conflict_category": _cat},
                )

            # ── Common shadow-hypothesis context ─────────────────────────────
            _p50 = _macro_p50.get(pair)
            _regime_score_val = float(detail.get("regime_score") or detail.get("regime_score_used") or 0)
            _regime_direction = 'long' if _regime_score_val > 0 else 'short'
            _macro_direction_pre = 'long' if _macro_score > 0 else 'short'
            _regime_agrees_pre = (_regime_direction == _macro_direction_pre)

            if abs(_macro_score) < _effective_macro_threshold:
                _p75_info = f"p75={_p75:.3f}" if _p75 is not None else "no p75 data"
                _gate_reject(
                    f"macro_gate_fail: abs({_macro_score:.3f}) < {_effective_macro_threshold:.3f} "
                    f"(fixed={MACRO_THRESHOLD:.3f}, {_p75_info})"
                )
                # Hypothesis D — p75_relaxed: above p50 but below p75 effective threshold
                if _p50 is not None and abs(_macro_score) >= _p50:
                    self._write_shadow_trade(
                        pair, bias, _macro_score, _tech_score,
                        _regime_score_val, 'macro_gate_fail',
                        hypothesis='p75_relaxed',
                        macro_gate_passed=False,
                        tech_gate_passed=(abs(_tech_score) >= TECH_MIN_THRESHOLD),
                        regime_agrees=_regime_agrees_pre,
                        p75_threshold=_p75,
                        effective_threshold=_effective_macro_threshold,
                        would_have_traded=True,
                    )
                # Full-cycle observation — macro gate blocked
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'macro_gate_fail',
                    hypothesis='full_cycle',
                    macro_gate_passed=False,
                    tech_gate_passed=None,
                    regime_agrees=_regime_agrees_pre,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=False,
                )
                continue

            _macro_direction = 'long' if _macro_score > 0 else 'short'
            _regime_agrees = (_regime_direction == _macro_direction)

            # Layer 2 — technical trigger
            if abs(_tech_score) < TECH_MIN_THRESHOLD:
                _gate_reject(
                    f"technical_too_weak: abs({_tech_score:.3f}) < {TECH_MIN_THRESHOLD:.3f}"
                )
                # Hypothesis A — macro_only: macro passed, tech too weak
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'technical_too_weak',
                    hypothesis='macro_only',
                    macro_gate_passed=True,
                    tech_gate_passed=False,
                    regime_agrees=_regime_agrees,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=True,
                )
                # Hypothesis C — macro_regime_no_tech: macro+regime agree, tech absent
                if _regime_agrees:
                    self._write_shadow_trade(
                        pair, bias, _macro_score, _tech_score,
                        _regime_score_val, 'technical_too_weak',
                        hypothesis='macro_regime_no_tech',
                        macro_gate_passed=True,
                        tech_gate_passed=False,
                        regime_agrees=True,
                        p75_threshold=_p75,
                        effective_threshold=_effective_macro_threshold,
                        would_have_traded=True,
                    )
                # Full-cycle observation — tech gate blocked
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'technical_too_weak',
                    hypothesis='full_cycle',
                    macro_gate_passed=True,
                    tech_gate_passed=False,
                    regime_agrees=_regime_agrees,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=False,
                )
                continue

            _tech_direction = 'long' if _tech_score > 0 else 'short'
            # Hypothesis B — tech_threshold_0.10: tech in 0.10-0.20 range (sub-old-threshold)
            if 0.10 <= abs(_tech_score) < 0.20:
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'tech_in_0.10_0.20_range',
                    hypothesis='tech_threshold_0.10',
                    macro_gate_passed=True,
                    tech_gate_passed=True,
                    regime_agrees=_regime_agrees,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=(_tech_direction == _macro_direction),
                )

            if _tech_direction != _macro_direction:
                _gate_reject(
                    f"directional_disagreement: macro={_macro_direction} ({_macro_score:+.3f}) "
                    f"vs tech={_tech_direction} ({_tech_score:+.3f})"
                )
                # Hypothesis E — directional_disagreement: both convicted, opposite directions
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'directional_disagreement',
                    hypothesis='directional_disagreement',
                    macro_gate_passed=True,
                    tech_gate_passed=True,
                    regime_agrees=_regime_agrees,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=False,
                )
                # Hypothesis C — macro_regime_no_tech: macro+regime agree despite tech opposing
                if _regime_agrees:
                    self._write_shadow_trade(
                        pair, bias, _macro_score, _tech_score,
                        _regime_score_val, 'directional_disagreement',
                        hypothesis='macro_regime_no_tech',
                        macro_gate_passed=True,
                        tech_gate_passed=True,
                        regime_agrees=True,
                        p75_threshold=_p75,
                        effective_threshold=_effective_macro_threshold,
                        would_have_traded=True,
                    )
                # Full-cycle observation — directional gate blocked
                self._write_shadow_trade(
                    pair, bias, _macro_score, _tech_score,
                    _regime_score_val, 'directional_disagreement',
                    hypothesis='full_cycle',
                    macro_gate_passed=True,
                    tech_gate_passed=True,
                    regime_agrees=_regime_agrees,
                    p75_threshold=_p75,
                    effective_threshold=_effective_macro_threshold,
                    would_have_traded=False,
                )
                continue

            # Layer 3 — all existing session, stress, R:R, spread checks in evaluate_pair
            # Full-cycle observation — all three hierarchical gates cleared
            self._write_shadow_trade(
                pair, bias, _macro_score, _tech_score,
                _regime_score_val, None,
                hypothesis='full_cycle',
                macro_gate_passed=True,
                tech_gate_passed=True,
                regime_agrees=_regime_agrees,
                p75_threshold=_p75,
                effective_threshold=_effective_macro_threshold,
                would_have_traded=True,
            )
            direction  = 'bullish' if _macro_direction == 'long' else 'bearish'
            dc_reason  = (
                f"aligned: macro={_macro_direction}({_macro_score:+.3f}) "
                f"tech={_tech_direction}({_tech_score:+.3f})"
            )
            detail["directional_consensus"] = {
                "direction": direction,
                "reason": dc_reason,
                "macro_bias": (macro_sig.get("bias") or "neutral").lower(),
                "technical_bias": (tech_sig.get("bias") or "neutral").lower(),
                "macro_gate": round(_macro_score, 4),
                "tech_gate":  round(_tech_score, 4),
            }

            # Convergence for display — 70/20/10 (macro dominates; tech = confirmation strength)
            _regime_s_display = float(detail.get("regime_score_used") or 0.50)
            detail["hierarchical_convergence"] = round(
                abs(_macro_score) * 0.70 + abs(_tech_score) * 0.20 + _regime_s_display * 0.10, 4
            )

            logger.info(f"{pair}: {dc_reason} (convergence={final_convergence:.3f})")

            # CHECK 8b — already approved this cycle (stale-read race condition guard)
            if pair in approved_this_cycle:
                decisions.append({
                    "pair": pair, "approved": False,
                    "rejection_reasons": [f"already_approved_this_cycle: {pair} already approved in this cycle — preventing duplicate"],
                    "checks": {"same_instrument": "FAIL"},
                    "convergence": round(final_convergence, 4), "bias": bias,
                    "effective_threshold": round(effective_threshold, 4),
                    "convergence_detail": detail,
                })
                logger.info(f"❌ {pair}: REJECTED (already_approved_this_cycle)")
                continue

            # CHECK 8c — same instrument already open for this user
            _same_open = [p for p in open_positions if p["instrument"] == pair]
            if _same_open:
                decisions.append({
                    "pair": pair, "approved": False,
                    "rejection_reasons": [f"same_instrument_open: {pair} already has {len(_same_open)} open position(s) for this user"],
                    "checks": {"same_instrument": "FAIL"},
                    "convergence": round(final_convergence, 4), "bias": bias,
                    "effective_threshold": round(effective_threshold, 4),
                    "convergence_detail": detail,
                })
                logger.info(f"❌ {pair}: REJECTED (same_instrument_open — {len(_same_open)} open)")
                continue

            # Get technical payload for spread/R:R checks
            tech_payload = tech_sig.get("payload", {})

            # Apply stop loss convergence penalty for this pair (+0.05 for 2 cycles)
            _penalty_cycles = self._stop_loss_penalties.get((self.user_id, pair), 0)
            _stop_penalty = 0.05 if _penalty_cycles > 0 else 0.0
            _pair_threshold = effective_threshold + _stop_penalty
            _pair_breakdown = dict(threshold_breakdown)
            if _stop_penalty > 0:
                _pair_breakdown["stop_loss_penalty"] = _stop_penalty
                _pair_breakdown["stop_loss_penalty_cycles_remaining"] = _penalty_cycles
                logger.info(
                    f"{pair}: stop_loss_penalty +{_stop_penalty:.2f} "
                    f"({_penalty_cycles} cycle(s) remaining), "
                    f"threshold {effective_threshold:.4f} -> {_pair_threshold:.4f}"
                )
            decision = self.convergence_calc.evaluate_pair(
                pair=pair,
                convergence=final_convergence,
                bias=direction or bias,  # use gate-confirmed direction
                confidence=confidence,
                effective_threshold=_pair_threshold,
                threshold_breakdown=_pair_breakdown,
                market_context=market_context,
                risk_params=risk_params,
                open_positions=open_positions,
                upcoming_events=upcoming_events,
                technical_payload=tech_payload,
            )
            decision["convergence_detail"] = detail
            decision["conviction_score"] = round(abs(_macro_score), 4)
            decision["convergence_score"] = round(final_convergence, 4)  # display only
            decision["macro_score"]              = round(_macro_score, 4)
            decision["tech_score"]               = round(_tech_score, 4)
            decision["effective_macro_threshold"] = round(_effective_macro_threshold, 4)
            decision["p75_threshold"]             = round(_p75, 4) if _p75 is not None else None
            decision["stress_score"]              = market_context.get("stress_score")
            decision["stress_band"]               = market_context.get("stress_state")

            # B. Bias-score inconsistency alert
            _bias_lower = (decision.get("bias") or "").lower()
            _conv_val = decision.get("convergence", 0)
            _signed_sum = decision.get("convergence_detail", {}).get("directional_sum", 0)
            # Genuine contradiction: signed directional_sum disagrees with stated bias.
            # convergence is now signed (negative = bearish); directional_sum is the authoritative check.
            if (_signed_sum > 0.05 and "bearish" in _bias_lower) or \
               (_signed_sum < -0.05 and "bullish" in _bias_lower):
                send_alert(
                    'CRITICAL',
                    f'{pair} bias-score contradiction: bias={_bias_lower} directional_sum={_signed_sum:.3f} (convergence={_conv_val:.3f})',
                    {'pair': pair, 'bias': _bias_lower, 'score': round(_conv_val, 4), 'directional_sum': round(_signed_sum, 4)},
                    source_agent='orchestrator',
                )

            decisions.append(decision)

            if decision["approved"]:
                approved_this_cycle.add(pair)
                open_positions.append({"instrument": pair, "direction": direction})  # same-cycle race fix
                logger.info(f"✅ {pair}: APPROVED (convergence={final_convergence:.4f}, bias={direction or bias})")
                log_event('TRADE_APPROVED', f'{pair} approved conv={final_convergence:.3f} bias={direction or bias}',
                    category='APPROVAL', agent='orchestrator', user_id=str(self.user_id), instrument=pair,
                    payload={'convergence': round(final_convergence, 4),
                             'threshold': round(decision.get('effective_threshold', 0), 4),
                             'stress': market_context.get('stress_score', 0)})
            else:
                reasons = "; ".join(decision["rejection_reasons"][:2])
                logger.info(f"❌ {pair}: REJECTED ({reasons})")
                log_event('TRADE_REJECTED', f'{pair} rejected: {reasons}',
                    category='REJECTION', agent='orchestrator', user_id=str(self.user_id), instrument=pair,
                    payload={'reason': reasons, 'convergence': round(final_convergence, 4),
                             'threshold': round(decision.get('effective_threshold', 0), 4)})
                self._write_rejected_signal(
                    pair=pair,
                    reason=decision["rejection_reasons"][0] if decision["rejection_reasons"] else "unknown",
                    convergence=final_convergence,
                    threshold=decision.get("effective_threshold", effective_threshold),
                    bias=decision.get("bias") or bias,
                    macro_score=detail.get("macro_score"),
                    tech_score=detail.get("technical_score"),
                    regime_score=detail.get("regime_score"),
                    current_price=decision.get("current_price"),
                    payload={"rejection_reasons": decision.get("rejection_reasons", []),
                             "checks": decision.get("checks", {})},
                )

        # A. Convergence collapse / spike alert
        _current_conv_scores = {d["pair"]: d["convergence"] for d in decisions}
        _collapse_pairs = []
        _spike_pairs    = []
        for _pair, _cur in _current_conv_scores.items():
            _prev = _prev_convergence_scores.get(_pair)
            if _prev is not None and abs(_prev) > 0.10:
                _mac = next((s for s in signals.get("macro", []) if s.get("instrument") == _pair), {})
                _tec = next((s for s in signals.get("technical", []) if s.get("instrument") == _pair), {})
                _pair_detail = {
                    'pair': _pair, 'previous': round(_prev, 4), 'current': round(_cur, 4),
                    'macro_score': float(_mac.get('score') or 0),
                    'macro_conf': float(_mac.get('confidence') or 0),
                    'tech_score': float(_tec.get('score') or 0),
                    'tech_conf': float(_tec.get('confidence') or 0),
                }
                _drop_pct  = (_prev - _cur) / max(abs(_prev), 0.01)
                _spike_pct = (_cur - _prev) / max(abs(_prev), 0.01)
                if _drop_pct > 0.80:
                    _collapse_pairs.append((_pair, _drop_pct, _pair_detail))
                elif _spike_pct > 1.00:
                    _spike_pairs.append((_pair, _spike_pct, _pair_detail))

        # Aggregate collapses → single email per cycle with 2-hour dedup
        if _collapse_pairs:
            _suppress_collapse = False
            try:
                _dedup_cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
                _dcur = self.db.cursor()
                _dcur.execute(
                    "SELECT 1 FROM forex_network.system_events "
                    "WHERE event_type = 'CONVERGENCE_COLLAPSE' "
                    "  AND event_time > %s LIMIT 1",
                    (_dedup_cutoff,),
                )
                if _dcur.fetchone():
                    _suppress_collapse = True
                _dcur.close()
            except Exception as _de:
                logger.warning('Collapse dedup DB check failed: %s', _de)

            if not _suppress_collapse:
                _pairs_summary = ', '.join(
                    f'{p} -{d:.0%}' for p, d, _ in _collapse_pairs
                )
                send_alert(
                    'CRITICAL',
                    f'CONVERGENCE COLLAPSE — {len(_collapse_pairs)} pair(s): {_pairs_summary}',
                    {'collapses': [{**det, 'drop_pct': round(dp, 4)} for _, dp, det in _collapse_pairs]},
                    source_agent='orchestrator',
                )
                # Record dedup sentinel
                try:
                    _scur = self.db.cursor()
                    _scur.execute(
                        "INSERT INTO forex_network.system_events "
                        "  (event_type, severity, category, agent, message, payload) "
                        "VALUES ('CONVERGENCE_COLLAPSE', 'CRITICAL', 'ALERT', 'orchestrator', %s, %s)",
                        (
                            f'Collapse: {_pairs_summary}',
                            json.dumps({'pairs': [p for p, _, _ in _collapse_pairs]}),
                        ),
                    )
                    self.db.commit()
                    _scur.close()
                except Exception as _se:
                    logger.warning('Collapse sentinel write failed: %s', _se)
                    try: self.db.rollback()
                    except Exception: pass
            else:
                warn('orchestrator', 'THRESHOLD',
                     'Convergence collapse suppressed by 2-hour dedup',
                     pairs=str([p for p, _, _ in _collapse_pairs]))

        # Aggregate spikes → single WARNING per cycle
        if _spike_pairs:
            _spikes_summary = ', '.join(
                f'{p} +{s:.0%}' for p, s, _ in _spike_pairs
            )
            send_alert(
                'WARNING',
                f'CONVERGENCE SPIKE — {len(_spike_pairs)} pair(s): {_spikes_summary}',
                {'spikes': [{**det, 'spike_pct': round(sp, 4)} for _, sp, det in _spike_pairs]},
                source_agent='orchestrator',
            )

        _prev_convergence_scores.update(_current_conv_scores)
        _low_count = sum(1 for s in _current_conv_scores.values() if abs(s) < 0.05)
        if _low_count >= 5:
            send_alert(
                'CRITICAL',
                f'Systemic convergence collapse: {_low_count}/20 pairs near zero',
                {'scores': {k: round(v, 4) for k, v in _current_conv_scores.items()}},
                source_agent='orchestrator',
            )

        # 7. Risk budget analysis
        risk_budget = RiskBudgetAnalyser.analyse(
            risk_params=risk_params,
            open_positions=open_positions,
            daily_loss=daily_loss,
            market_context=market_context,
        )

        # 8. Generate proposals
        proposals = OrchestratorProposalGenerator.generate(
            decisions=decisions,
            risk_budget=risk_budget,
            market_context=market_context,
            threshold_breakdown=threshold_breakdown,
        )

        # 9. Write outputs
        approved_count = sum(1 for d in decisions if d["approved"])

        full_payload = {
            "cycle": self.signal_writer.cycle_count,
            "effective_threshold": round(effective_threshold, 4),
            "threshold_breakdown": threshold_breakdown,
            "stress_score": market_context.get("stress_score"),
            "stress_state": market_context.get("stress_state"),
            "current_session": market_context.get("current_session"),
            "day_of_week": market_context.get("day_of_week"),
            "open_positions": len(open_positions),
            "approved_count": approved_count,
            "rejected_count": len(decisions) - approved_count,
            "risk_budget": risk_budget,
            "decisions": decisions,
            "proposals": proposals,
        }

        if not self.dry_run:
            # Build entry_context before writing orchestrator_decision so it is
            # embedded in the decisions array and flows: orchestrator_decision →
            # risk_guardian decision → risk_approved → execution → trades
            for decision in decisions:
                if decision["approved"]:
                    decision["entry_context"] = self.build_entry_context(
                        decision["pair"], decision["convergence"],
                        decision["bias"], market_context, signals,
                    )
                    # Propagate p75 threshold into entry_context for downstream trade_parameters persistence
                    _p75_val = (decision.get("convergence_detail") or {}).get("p75_macro_threshold")
                    if _p75_val is not None:
                        decision["entry_context"]["pair_score_p75"] = _p75_val

            self.signal_writer.write_orchestrator_signal(full_payload)

            # Write individual trade approvals for Risk Guardian
            # Rank approved decisions by convergence score descending (1 = highest conviction).
            # entry_rank_position flows through the payload to the trades table.
            _approved = sorted(
                [d for d in decisions if d["approved"]],
                key=lambda d: d["convergence"],
                reverse=True,
            )
            for _rank, decision in enumerate(_approved, start=1):
                decision["entry_rank_position"] = _rank
                self.signal_writer.write_trade_approval(decision)

        cycle_duration = time.time() - cycle_start
        logger.info(
            f"=== ORCHESTRATOR CYCLE {self.signal_writer.cycle_count} COMPLETE "
            f"({cycle_duration:.1f}s) — Approved: {approved_count}/{len(decisions)}, "
            f"Threshold: {effective_threshold:.4f} ==="
        )

        return {
            "status": "complete",
            "cycle": self.signal_writer.cycle_count,
            "approved": approved_count,
            "rejected": len(decisions) - approved_count,
            "effective_threshold": round(effective_threshold, 4),
            "stress_score": market_context.get("stress_score"),
            "proposals_count": len(proposals),
            "duration_seconds": round(cycle_duration, 1),
        }

    def run_continuous(self):
        logger.info(f"Orchestrator starting — user: {self.user_id}")
        while True:
            try:
                result = self.run_cycle()
                logger.info(f"Cycle result: {json.dumps(result)}")
                sleep_time = CYCLE_INTERVAL_SECONDS - result.get("duration_seconds", 0)
                if sleep_time > 0:
                    elapsed = 0
                    while elapsed < sleep_time:
                        chunk = min(HEARTBEAT_INTERVAL_SECONDS, sleep_time - elapsed)
                        time.sleep(chunk)
                        elapsed += chunk
                        if not self.dry_run:
                            self.signal_writer.write_heartbeat()
            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                break
            except Exception as e:
                logger.error(f"Cycle failed: {e}", exc_info=True)
                time.sleep(30)
        self.db.close()


# =============================================================================
# TEST SUITE
# =============================================================================
class OrchestratorTester:
    """Unit tests for orchestrator logic."""

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
        logger.info("ORCHESTRATOR AGENT TEST SUITE")
        logger.info("=" * 60)

        self.test_convergence_weights()
        self.test_threshold_base_values()
        self.test_o1_hard_floor()
        self.test_stress_threshold_adjustments()
        self.test_r5_confidence_low_floor()
        self.test_pre_event_adjustment()
        self.test_degradation_boost()
        self.test_combined_threshold_stacking()
        self.test_session_filter()
        self.test_day_filter()
        self.test_ny_close_rejection()
        self.test_friday_cutoff()
        self.test_max_positions()
        self.test_circuit_breaker()
        self.test_neutral_bias_rejection()
        self.test_spread_ratio_check()
        self.test_min_rr_check()
        self.test_approval_flow()
        self.test_size_multiplier_stacking()
        self.test_risk_budget_analysis()
        self.test_near_miss_proposals()
        self.test_crisis_no_entries()
        self.test_worst_case_threshold()

        logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASSED, {self.failed} FAILED (total: {self.passed + self.failed})")
        logger.info("=" * 60)
        return self.failed == 0

    def test_convergence_weights(self):
        logger.info("\n--- Test: Convergence Weights ---")
        total = sum(CONVERGENCE_WEIGHTS.values())
        self._assert(abs(total - 1.0) < 0.001, "Weights sum to 1.0", f"Got: {total}")
        self._assert(CONVERGENCE_WEIGHTS["macro"] == 0.40, "Macro weight is 0.40")
        self._assert(CONVERGENCE_WEIGHTS["technical"] == 0.40, "Technical weight is 0.40")
        self._assert(CONVERGENCE_WEIGHTS["regime"] == 0.20, "Regime weight is 0.20")

    def test_threshold_base_values(self):
        logger.info("\n--- Test: Threshold Base Values ---")
        self._assert(USER_PROFILES["e61202e4-30d1-70f8-9927-30b8a439e042"]["base_threshold"] == 0.80, "Conservative base: 0.80")
        self._assert(USER_PROFILES["76829264-20e1-7023-1e31-37b7a37a1274"]["base_threshold"] == 0.65, "Balanced base: 0.65")
        self._assert(USER_PROFILES["d6c272e4-a031-7053-af8e-ade000f0d0d5"]["base_threshold"] == 0.55, "Aggressive base: 0.55")

    def test_o1_hard_floor(self):
        logger.info("\n--- Test: O1 Hard Floor ---")
        calc = ConvergenceCalculator("neo_user_002")
        # Threshold should never go below base
        threshold, _ = calc.compute_effective_threshold(
            risk_params={"convergence_threshold": 0.65},
            market_context={"stress_score": 0, "stress_state": "Normal"},
            degradation={},
            upcoming_events=[],
        )
        self._assert(threshold >= 0.65, "O1: Threshold never below base 0.65", f"Got: {threshold}")

    def test_stress_threshold_adjustments(self):
        logger.info("\n--- Test: Stress Threshold Adjustments ---")
        calc = ConvergenceCalculator("neo_user_002")

        # Normal (0-30): no adjustment
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 15}, {}, [])
        self._assert(t == 0.65, "Normal stress: no adjustment", f"Got: {t}")

        # Elevated (30-50): +0.05
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 40}, {}, [])
        self._assert(abs(t - 0.70) < 0.001, "Elevated stress: +0.05", f"Got: {t}")

        # High (50-70): +0.10
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 60}, {}, [])
        self._assert(t == 0.75, "High stress: +0.10", f"Got: {t}")

        # Pre-crisis (70-85): +0.15, no new entries
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 78}, {}, [])
        self._assert(t == 0.80, "Pre-crisis: +0.15", f"Got: {t}")
        self._assert(not b["stress_new_entries_allowed"], "Pre-crisis: no new entries")

        # Crisis (85+): no new entries
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 92}, {}, [])
        self._assert(not b["stress_new_entries_allowed"], "Crisis: no new entries")
        self._assert(b["stress_size_multiplier"] == 0.0, "Crisis: size multiplier 0")

    def test_r5_confidence_low_floor(self):
        logger.info("\n--- Test: R5 Confidence Low Floor ---")
        calc = ConvergenceCalculator("neo_user_002")

        # Normal stress but low confidence → Elevated floor
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65},
            {"stress_score": 15, "stress_score_confidence": "low",
             "convergence_caution_buffer": 0.06},
            {}, [])
        self._assert(t >= 0.76, "R5 low confidence: threshold includes caution + Elevated floor", f"Got: {t}")
        self._assert(b.get("stress_size_multiplier", 1.0) <= 0.75, "R5 low confidence: size ≤ 0.75×")

    def test_pre_event_adjustment(self):
        logger.info("\n--- Test: Pre-event Adjustment ---")
        calc = ConvergenceCalculator("neo_user_002")
        events = [{"country": "US", "indicator": "NFP", "scheduled_time": "2026-04-16T13:30:00Z"}]
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 15}, {}, events)
        self._assert(abs(t - 0.70) < 0.001, "Pre-event: +0.05", f"Got: {t}")

    def test_degradation_boost(self):
        logger.info("\n--- Test: Degradation Boost ---")
        calc = ConvergenceCalculator("neo_user_002")
        degradation = {
            "macro": {"convergence_boost": 0.10, "degradation_mode": "degraded"},
            "regime": {"convergence_boost": 0.15, "degradation_mode": "degraded"},
        }
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65}, {"stress_score": 15}, degradation, [])
        self._assert(t == 0.90, "Degradation: +0.10 +0.15 = +0.25", f"Got: {t}")

    def test_combined_threshold_stacking(self):
        logger.info("\n--- Test: Combined Threshold Stacking ---")
        calc = ConvergenceCalculator("neo_user_003")  # Aggressive base 0.55
        # High stress (+0.10) + pre-event (+0.05) + macro degraded (+0.10)
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.55},
            {"stress_score": 60},
            {"macro": {"convergence_boost": 0.10, "degradation_mode": "degraded"}},
            [{"country": "US", "indicator": "CPI", "scheduled_time": "2026-04-16T13:30:00Z"}],
        )
        expected = 0.55 + 0.10 + 0.05 + 0.10  # = 0.80
        self._assert(
            abs(t - expected) < 0.001,
            f"Stacked: aggressive becomes 0.80",
            f"Got: {t}, expected: {expected}"
        )

    def test_session_filter(self):
        logger.info("\n--- Test: Session Filter ---")
        calc = ConvergenceCalculator("neo_user_001")  # Conservative: london + overlap only
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.80,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "asian", "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Conservative rejected in Asian session")
        self._assert(any("Session" in r for r in decision["rejection_reasons"]), "Reason is session filter")

    def test_day_filter(self):
        logger.info("\n--- Test: Day Filter ---")
        calc = ConvergenceCalculator("neo_user_001")  # Conservative: Tue-Thu only
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.80,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 1},  # Monday
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Conservative rejected on Monday")

    def test_ny_close_rejection(self):
        logger.info("\n--- Test: NY Close Rejection ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "ny_close", "ny_close_window": True, "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Rejected during NY close window")

    def test_friday_cutoff(self):
        logger.info("\n--- Test: Friday Cutoff ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "newyork", "day_of_week": 5, "ny_close_window": False},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        # This test depends on current time — it checks the Friday 16:00 rule
        # Just verify the check exists
        self._assert("friday_cutoff" in decision["checks"], "Friday cutoff check exists")

    def test_max_positions(self):
        logger.info("\n--- Test: Max Positions ---")
        calc = ConvergenceCalculator("neo_user_002")
        fake_positions = [{"instrument": "GBPUSD"}, {"instrument": "USDJPY"}, {"instrument": "AUDUSD"}]
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=fake_positions, upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Rejected when max positions reached")
        self._assert(any("Max positions" in r for r in decision["rejection_reasons"]), "Reason is max positions")

    def test_circuit_breaker(self):
        logger.info("\n--- Test: Circuit Breaker ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3, "circuit_breaker_active": True},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Rejected when circuit breaker active")

    def test_neutral_bias_rejection(self):
        logger.info("\n--- Test: Neutral Bias Rejection ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="neutral", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Rejected when bias is neutral")

    def test_spread_ratio_check(self):
        logger.info("\n--- Test: Spread Ratio Check ---")
        calc = ConvergenceCalculator("neo_user_002")  # min_spread_ratio = 5.0
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[],
            technical_payload={"spread_pips": 2.0, "expected_pips": 6.0},  # ratio 3.0 < 5.0
        )
        self._assert(not decision["approved"], "Rejected when spread ratio too low")
        self._assert(any("Spread" in r for r in decision["rejection_reasons"]), "Reason is spread ratio")

    def test_min_rr_check(self):
        logger.info("\n--- Test: Min R:R Check ---")
        calc = ConvergenceCalculator("neo_user_002")  # min_rr = 1.5
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.90, bias="bullish", confidence=0.85,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[],
            technical_payload={"rr_ratio": 1.1},  # below 1.5
        )
        self._assert(not decision["approved"], "Rejected when R:R below minimum")
        self._assert(any("R:R" in r for r in decision["rejection_reasons"]), "Reason is R:R")

    def test_approval_flow(self):
        logger.info("\n--- Test: Approval Flow ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.85, bias="bullish", confidence=0.90,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": True, "combined_size_multiplier": 0.75},
            market_context={"current_session": "london", "day_of_week": 3},
            risk_params={"max_open_positions": 3, "pre_event_size_reduction": 50},
            open_positions=[], upcoming_events=[],
            technical_payload={"spread_pips": 1.0, "expected_pips": 15.0, "rr_ratio": 2.5},
        )
        self._assert(decision["approved"], "Clean setup approved", f"Reasons: {decision['rejection_reasons']}")
        self._assert(
            decision.get("size_multiplier", 0) > 0,
            "Size multiplier set on approval",
            f"Got: {decision.get('size_multiplier')}"
        )

    def test_size_multiplier_stacking(self):
        logger.info("\n--- Test: Size Multiplier Stacking ---")
        calc = ConvergenceCalculator("neo_user_002")
        # High stress (0.50×) + drawdown step 1 (0.75×) + pre-event (0.50×)
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.65, "size_multiplier": 0.75},
            {"stress_score": 60},
            {}, [{"country": "US", "indicator": "NFP", "scheduled_time": "2026-04-16T13:30:00Z"}],
        )
        combined = b["combined_size_multiplier"]
        self._assert(
            abs(combined - 0.375) < 0.01,
            "Stress 0.50 × drawdown 0.75 = 0.375",
            f"Got: {combined}"
        )

    def test_risk_budget_analysis(self):
        logger.info("\n--- Test: Risk Budget Analysis ---")
        result = RiskBudgetAnalyser.analyse(
            risk_params={"daily_loss_limit_pct": 3.0, "max_open_positions": 3},
            open_positions=[],
            daily_loss=0.0,
            market_context={"stress_score": 15},
        )
        self._assert(result["budget_underdeployed"], "Empty book in Normal stress = underdeployed")
        self._assert(result["positions_available"] == 3, "3 positions available")

    def test_near_miss_proposals(self):
        logger.info("\n--- Test: Near-miss Proposals ---")
        decisions = [{
            "pair": "EURUSD", "approved": False, "convergence": 0.62,
            "effective_threshold": 0.65,
            "rejection_reasons": ["O1: Convergence 0.6200 below threshold 0.6500"],
        }]
        proposals = OrchestratorProposalGenerator.generate(
            decisions=decisions,
            risk_budget={"budget_underdeployed": False},
            market_context={},
            threshold_breakdown={},
        )
        near_miss = [p for p in proposals if "near-miss" in p.get("title", "").lower()]
        self._assert(len(near_miss) > 0, "Near-miss proposal generated")

    def test_crisis_no_entries(self):
        logger.info("\n--- Test: Crisis No Entries ---")
        calc = ConvergenceCalculator("neo_user_002")
        decision = calc.evaluate_pair(
            pair="EURUSD", convergence=0.99, bias="bullish", confidence=0.99,
            effective_threshold=0.65,
            threshold_breakdown={"stress_new_entries_allowed": False},
            market_context={"current_session": "london", "stress_state": "Crisis",
                            "stress_score": 92, "day_of_week": 3},
            risk_params={"max_open_positions": 3},
            open_positions=[], upcoming_events=[], technical_payload={},
        )
        self._assert(not decision["approved"], "Crisis: even perfect setup rejected")

    def test_worst_case_threshold(self):
        logger.info("\n--- Test: Worst-case Threshold ---")
        calc = ConvergenceCalculator("neo_user_003")  # Aggressive 0.55
        # High stress (+0.10) + pre-event (+0.05) + macro degraded (+0.10) + regime degraded (+0.15)
        t, b = calc.compute_effective_threshold(
            {"convergence_threshold": 0.55},
            {"stress_score": 60},
            {
                "macro": {"convergence_boost": 0.10, "degradation_mode": "degraded"},
                "regime": {"convergence_boost": 0.15, "degradation_mode": "degraded"},
            },
            [{"country": "US", "indicator": "NFP", "scheduled_time": "2026-04-16T13:30:00Z"}],
        )
        expected = 0.55 + 0.10 + 0.05 + 0.10 + 0.15  # = 0.95
        self._assert(
            abs(t - expected) < 0.001,
            f"Worst-case aggressive → {expected}",
            f"Got: {t}"
        )


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
    """Main entry point — runs Orchestrator for ALL active users."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Orchestrator")
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
        logger.info("Orchestrator test mode — verifying configuration for all users")
        try:
            for uid in user_ids:
                agent = OrchestratorAgent(user_id=uid, dry_run=True)
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
            agents[uid] = OrchestratorAgent(user_id=uid, dry_run=getattr(args, "dry_run", False))
            logger.info(f"Initialized Orchestrator for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Orchestrator for {uid}: {e}")

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
                market = get_market_state()

                # Standby during weekend and pre-open window
                if market['state'] in ('closed', 'pre_open'):
                    logger.info(f"STANDBY — {market['reason']}, skipping cycle")
                    for uid, agent in agents.items():
                        try:
                            agent.update_heartbeat()
                        except Exception:
                            pass
                    time.sleep(300)  # re-check every 5 min
                    continue

                if market['state'] == 'quiet':
                    logger.info(f"QUIET HOURS — {market['reason']}")

                for uid, agent in agents.items():
                    try:
                        logger.info(f"--- {uid} ---")
                        agent.run_cycle()
                    except Exception as e:
                        logger.error(f"Cycle failed for {uid}: {e}")
                _utc_hr = datetime.datetime.now(datetime.timezone.utc).hour
                _interval = 600 if (_utc_hr >= 22 or _utc_hr < 7) else CYCLE_INTERVAL_SECONDS
                time.sleep(_interval)

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
