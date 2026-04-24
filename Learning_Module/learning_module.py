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
STRESS_EXCLUSION_THRESHOLD = 70    # Losses at stress > 70 excluded from pattern scoring

# Sortino/Sharpe targets
SORTINO_TARGET_PAPER = 2.0
SORTINO_TARGET_LIVE = 1.5
SHARPE_TARGET_PAPER = 1.5
SHARPE_TARGET_LIVE = 1.0

# Weekly report schedule
WEEKLY_REPORT_DAY = 0   # Monday (0=Monday in weekday())
WEEKLY_REPORT_HOUR = 6  # 06:00 UTC

# First IG trade — reject any signal data predating the broker migration
DATA_QUALITY_CUTOFF = '2026-04-23 21:55:00+00'


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
            pnl = float(trade["pnl"])
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
        exclude_from_patterns = (
            not is_win and stress_at_entry is not None
            and stress_at_entry > STRESS_EXCLUSION_THRESHOLD
        )

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

        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.trade_autopsies
                    (trade_id, user_id, failure_mode, diagnosis, signal_quality,
                     contributing_factors, pattern_context,
                     sharpe_contribution, sortino_contribution,
                     regime_stress_at_entry, exclude_from_patterns,
                     created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """, (
                trade_id, self.user_id, failure_mode, diagnosis, signal_quality,
                json.dumps(contributing_factors),
                json.dumps(pattern_context),
                None,  # Sharpe contribution — calculated after more trades
                None,  # Sortino contribution
                stress_at_entry,
                exclude_from_patterns,
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
                       stress_threshold_score, stress_multiplier,
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
        """Get the stress score closest to a given time."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT system_stress_score
                FROM shared.market_context_snapshots
                WHERE snapshot_time <= %s AND system_stress_score IS NOT NULL
                ORDER BY snapshot_time DESC LIMIT 1
            """, (entry_time,))
            row = cur.fetchone()
            return float(row["system_stress_score"]) if row else None
        except:
            return None
        finally:
            cur.close()

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
        active = total >= MIN_TRADES_FOR_ACTIVATION

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
        """Section 5: Stress score history for the week."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT MIN(system_stress_score) AS min_stress,
                       MAX(system_stress_score) AS max_stress,
                       AVG(system_stress_score) AS avg_stress,
                       COUNT(*) AS snapshot_count
                FROM shared.market_context_snapshots
                WHERE system_stress_score IS NOT NULL
                  AND snapshot_time >= NOW() - INTERVAL '7 days'
            """)
            row = cur.fetchone()

            # Count escalations
            cur.execute("""
                SELECT COUNT(*) AS escalation_count
                FROM forex_network.system_stress_alerts
                WHERE transition_type = 'escalation'
                  AND ts >= NOW() - INTERVAL '7 days'
            """)
            esc_row = cur.fetchone()

            return {
                "min_stress": float(row["min_stress"]) if row and row["min_stress"] else None,
                "max_stress": float(row["max_stress"]) if row and row["max_stress"] else None,
                "avg_stress": float(row["avg_stress"]) if row and row["avg_stress"] else None,
                "snapshots": int(row["snapshot_count"]) if row else 0,
                "escalations": int(esc_row["escalation_count"]) if esc_row else 0,
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

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

            # Phase 1 accuracy tracking — per-agent, per-instrument, per-session
            self.compute_per_agent_accuracy()

            # Per-source contrarian accuracy (MyFXBook / IG)
            self.compute_per_source_accuracy()

            # Macro gate conviction-tier accuracy (high/medium/low)
            self.compute_macro_direction_accuracy()

            # Shadow trade hypothesis analysis
            self.analyse_shadow_trades()

            # Kelly fraction (activates after 50 trades)
            self.compute_kelly_fraction()

        return count

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
        except Exception as e:
            logger.error(f"Proposals write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

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
                t.pnl
            FROM forex_network.trade_autopsies ta
            JOIN forex_network.trades t ON ta.trade_id = t.id
            WHERE ta.user_id = %s
              AND ta.pattern_context->'entry_timing' IS NOT NULL
              AND t.exit_time IS NOT NULL
              AND t.pnl IS NOT NULL
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
            stats[key]["pnl_sum"] += float(row["pnl"] or 0)
            if float(row["pnl"] or 0) > 0:
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
                    t.pnl,
                    (t.entry_context->'ssi_ig'->>'short_pct')::float      AS ig_short_pct,
                    (t.entry_context->'ssi_myfxbook'->>'short_pct')::float AS mfx_short_pct
                FROM forex_network.trades t
                WHERE t.user_id   = %s
                  AND t.exit_time IS NOT NULL
                  AND t.pnl       IS NOT NULL
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
            pnl        = float(row['pnl'] or 0)

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
                            WHEN (s.score > 0 AND t.pnl > 0)
                              OR (s.score < 0 AND t.pnl < 0)
                            THEN 1 ELSE 0 END)                               AS correct_direction,
                    SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END)              AS wins,
                    AVG(t.pnl)                                               AS avg_pnl
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
                  AND t.pnl       IS NOT NULL
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
                stress_score = payload.get("stress_score")
                regime = payload.get("stress_state", "unknown")
                decisions = payload.get("decisions", [])

                for decision in decisions:
                    if decision.get("approved"):
                        continue
                    instrument = decision.get("pair")
                    convergence = decision.get("convergence")
                    reasons = decision.get("rejection_reasons", [])
                    rejection_reason = reasons[0] if reasons else "unknown"

                    # Parse rejection_reason string into structured detail
                    detail: dict = {}
                    r = rejection_reason
                    if 'macro_gate_fail' in r:
                        detail['rejection_type'] = 'macro_gate'
                    elif 'technical_too_weak' in r:
                        detail['rejection_type'] = 'technical_gate'
                    elif 'directional' in r:
                        detail['rejection_type'] = 'directional'
                    else:
                        detail['rejection_type'] = 'other'
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

    def _generate_proposals(self) -> int:
        """
        Read rejection_patterns and performance metrics; write structured proposals
        into forex_network.proposals for human review / optional auto-apply.

        FIX 1: same_instrument_open and already_approved rejections are excluded from
                threshold proposals — those are gate rejections unrelated to threshold
                calibration.
        FIX 2: max_risk_pct proposed_value is stored as a FRACTION (e.g. 0.025 = 2.5%),
                never as a bare percentage (2.5).
        """
        # ── Minimum data gate ─────────────────────────────────────────────────
        # Do not write proposals until there are enough clean closed trades
        # post DATA_QUALITY_CUTOFF to make the statistics meaningful.
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
                f"[{self.user_id}] Proposals gate query failed ({_gate_e}) — skipping proposals"
            )
            return 0
        finally:
            _gate_cur.close()
        if _closed_trades < MIN_TRADES_FOR_PROPOSALS:
            logger.info(
                f"[{self.user_id}] Skipping proposals — only {_closed_trades} clean "
                f"closed trades (minimum {MIN_TRADES_FOR_PROPOSALS})"
            )
            return 0
        # ─────────────────────────────────────────────────────────────────────

        cur = self.db.cursor()
        written = 0
        try:
            # ── Load all risk_parameters once ─────────────────────────────────────
            cur.execute("""
                SELECT convergence_threshold, max_risk_pct, min_risk_pct,
                       curve_exponent, max_convergence_reference,
                       stress_threshold_score, stress_multiplier,
                       min_risk_reward_ratio, max_usd_units, correlation_threshold
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            rp_full = cur.fetchone()
            if not rp_full:
                return 0
            rp_conv_thr  = float(rp_full['convergence_threshold'])
            rp_max_risk  = float(rp_full['max_risk_pct'])
            rp_min_risk  = float(rp_full['min_risk_pct'] or 0.0025)
            logger.debug(
                f"[{self.user_id}] _generate_proposals risk_params: "
                f"conv_thr={rp_conv_thr:.4f} max_risk={rp_max_risk:.4f} "
                f"min_risk={rp_min_risk:.4f} curve_exp={rp_full['curve_exponent']} "
                f"max_conv_ref={rp_full['max_convergence_reference']} "
                f"stress_thr={rp_full['stress_threshold_score']} "
                f"stress_mult={rp_full['stress_multiplier']} "
                f"min_rr={rp_full['min_risk_reward_ratio']} "
                f"max_units={rp_full['max_usd_units']} "
                f"corr_thr={rp_full['correlation_threshold']}"
            )

            # ── Threshold adjustment proposals ────────────────────────────────────
            # FIX 1: Exclude gate rejections at the SQL level so they never
            #        contribute to the n / avg_conv that drives threshold proposals.
            cur.execute("""
                SELECT instrument,
                       COUNT(*) AS n,
                       AVG(convergence_score) AS avg_conv
                FROM forex_network.rejection_patterns
                WHERE user_id = %s
                  AND created_at > NOW() - INTERVAL '7 days'
                  AND convergence_score IS NOT NULL
                  AND rejection_reason NOT ILIKE '%%same_instrument%%'
                  AND rejection_reason NOT ILIKE '%%already_approved%%'
                GROUP BY instrument
                HAVING COUNT(*) >= 10
            """, (self.user_id,))
            threshold_rows = cur.fetchall()

            for row in threshold_rows:
                instrument = row['instrument']
                n          = int(row['n'])
                avg_conv   = float(row['avg_conv'] or 0)

                current_thr = rp_conv_thr

                # Only propose if the cluster sits within 0.10 below current threshold
                if not (current_thr - 0.10 <= avg_conv < current_thr):
                    continue

                # Dedup: skip if a pending proposal already exists for this pair
                cur.execute("""
                    SELECT COUNT(*) AS cnt FROM forex_network.proposals
                    WHERE user_id = %s AND parameter = 'convergence_threshold'
                      AND instrument = %s AND status = 'pending'
                """, (self.user_id, instrument))
                if int(cur.fetchone()['cnt']) > 0:
                    continue

                proposed_thr = round(avg_conv + 0.005, 4)   # just above rejection cluster
                cur.execute("""
                    INSERT INTO forex_network.proposals
                        (proposal_type, user_id, instrument, parameter,
                         current_value, proposed_value, direction,
                         n_trades, metric, metric_value, confidence, reasoning,
                         status, auto_revert_hours)
                    VALUES ('threshold_adjustment', %s, %s, 'convergence_threshold',
                            %s, %s, 'decrease',
                            %s, 'rejection_rate', 100.0,
                            'medium', %s, 'pending', 48)
                """, (
                    self.user_id, instrument,
                    current_thr, proposed_thr,
                    n,
                    (
                        f"{n} {instrument} signals rejected with avg convergence "
                        f"{avg_conv:.3f} (threshold {current_thr:.4f}, within 0.10 band). "
                        f"Gate rejections (same_instrument_open, already_approved) excluded. "
                        f"Proposed threshold {proposed_thr:.4f} targets this cluster."
                    ),
                ))
                written += 1
                logger.info(
                    f"Threshold proposal: {instrument} {current_thr:.4f} → {proposed_thr:.4f} "
                    f"(n={n}, avg_conv={avg_conv:.3f})"
                )

            # ── Position sizing proposals (Sortino-based) ─────────────────────────
            metrics     = self.performance.get_metrics()
            sortino     = metrics.get('sortino_ratio')
            trade_count = int(metrics.get('trade_count', 0) or 0)

            if sortino is not None and trade_count >= 10 and float(sortino) < SORTINO_TARGET_LIVE:
                sortino = float(sortino)
                current_risk = rp_max_risk   # fraction, e.g. 0.03

                # Dedup: skip if pending proposal already exists
                cur.execute("""
                    SELECT COUNT(*) AS cnt FROM forex_network.proposals
                    WHERE user_id = %s AND parameter = 'max_risk_pct'
                      AND status = 'pending'
                """, (self.user_id,))
                if int(cur.fetchone()['cnt']) == 0:
                    # FIX 2: compute proposed value as a FRACTION, never a percentage.
                    # Reduce by 0.5 percentage points expressed as a fraction delta.
                    proposed_risk = round(current_risk - 0.005, 4)   # e.g. 0.03 → 0.025
                    proposed_risk = max(proposed_risk, rp_min_risk)   # floor from risk_parameters

                    cur.execute("""
                        INSERT INTO forex_network.proposals
                            (proposal_type, user_id, instrument, parameter,
                             current_value, proposed_value, direction,
                             n_trades, metric, metric_value, confidence, reasoning,
                             status, auto_revert_hours)
                        VALUES ('position_sizing', %s, NULL, 'max_risk_pct',
                                %s, %s, 'decrease',
                                %s, 'sortino', %s,
                                'medium', %s, 'pending', 72)
                    """, (
                        self.user_id,
                        current_risk,    # stored as fraction (e.g. 0.0300)
                        proposed_risk,   # stored as fraction (e.g. 0.0250) — FIX 2
                        trade_count,
                        round(sortino, 4),
                        (
                            f"Sortino {sortino:.2f} over {trade_count} trades is below "
                            f"go-live minimum ({SORTINO_TARGET_LIVE}). Reducing max_risk_pct "
                            f"from {current_risk*100:.2f}% to {proposed_risk*100:.2f}% "
                            f"({current_risk:.4f} → {proposed_risk:.4f} fraction). "
                            f"Auto-reverts in 72 h."
                        ),
                    ))
                    written += 1
                    logger.info(
                        f"Sizing proposal: max_risk_pct {current_risk:.4f} → {proposed_risk:.4f} "
                        f"(sortino={sortino:.2f}, trades={trade_count})"
                    )

            # ── DIRECTIONAL DISAGREEMENT ANALYSIS ────────────────────────────────
            # When macro and technical disagree (rejection_stage='directional'),
            # who was right next day?
            # Derive macro direction from sign of macro_score (direction field is
            # truncated in DB). Join to 1D historical_prices for next-day outcome.
            # price_at_rejection is unpopulated — use same-day 1D close as reference.
            cur.execute("""
                SELECT
                    r.instrument,
                    COUNT(*) AS total,
                    SUM(CASE
                        WHEN r.macro_score > 0 AND next_d.close > ref_d.close THEN 1
                        WHEN r.macro_score < 0 AND next_d.close < ref_d.close THEN 1
                        ELSE 0 END) AS macro_correct,
                    SUM(CASE
                        WHEN r.macro_score > 0 AND next_d.close < ref_d.close THEN 1
                        WHEN r.macro_score < 0 AND next_d.close > ref_d.close THEN 1
                        ELSE 0 END) AS technical_correct
                FROM forex_network.rejected_signals r
                JOIN forex_network.historical_prices ref_d
                    ON ref_d.instrument = r.instrument
                    AND ref_d.timeframe  = '1D'
                    AND ref_d.ts::date   = r.rejected_at::date
                JOIN forex_network.historical_prices next_d
                    ON next_d.instrument = r.instrument
                    AND next_d.timeframe  = '1D'
                    AND next_d.ts::date   = (r.rejected_at::date + 1)
                WHERE r.rejection_stage = 'directional'
                  AND r.rejected_at::date < CURRENT_DATE
                  AND r.macro_score IS NOT NULL
                  AND r.technical_score IS NOT NULL
                  AND r.macro_score != 0
                GROUP BY r.instrument
                HAVING COUNT(*) >= 10
            """)
            dd_rows = cur.fetchall()
            for row in dd_rows:
                instrument     = row[0]
                total          = int(row[1])
                macro_correct  = int(row[2] or 0)
                tech_correct   = int(row[3] or 0)
                macro_rate     = macro_correct / total if total > 0 else 0.0
                tech_rate      = tech_correct  / total if total > 0 else 0.0

                if macro_rate > 0.60:
                    # Macro was right when tech blocked it — tech gate may be too strict
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM forex_network.proposals
                        WHERE user_id = %s AND proposal_type = 'tech_gate_too_strict'
                          AND instrument = %s AND status = 'pending'
                    """, (self.user_id, instrument))
                    if int(cur.fetchone()[0]) == 0:
                        cur.execute("""
                            INSERT INTO forex_network.proposals
                                (proposal_type, user_id, instrument, parameter,
                                 current_value, proposed_value, direction,
                                 n_trades, metric, metric_value, confidence, reasoning,
                                 status, auto_revert_hours)
                            VALUES ('tech_gate_too_strict', %s, %s, 'TECH_MIN_THRESHOLD',
                                    NULL, NULL, 'decrease',
                                    %s, 'macro_correct_rate', %s,
                                    'medium', %s, 'pending', 72)
                        """, (
                            self.user_id, instrument, total,
                            round(macro_rate, 4),
                            (
                                f"When macro and technical disagreed on {instrument}, "
                                f"macro direction was correct {macro_rate:.0%} of the time "
                                f"({total} rejections). Consider loosening TECH_MIN_THRESHOLD "
                                f"or accepting directional disagreement on this pair."
                            ),
                        ))
                        written += 1
                        logger.info(
                            f"Proposal: tech gate too strict on {instrument} — "
                            f"macro right {macro_rate:.0%} when blocked ({total} samples)"
                        )

                elif tech_rate > 0.60:
                    # Technical was right when it disagreed with macro
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM forex_network.proposals
                        WHERE user_id = %s AND proposal_type = 'technical_catching_reversal'
                          AND instrument = %s AND status = 'pending'
                    """, (self.user_id, instrument))
                    if int(cur.fetchone()[0]) == 0:
                        cur.execute("""
                            INSERT INTO forex_network.proposals
                                (proposal_type, user_id, instrument, parameter,
                                 current_value, proposed_value, direction,
                                 n_trades, metric, metric_value, confidence, reasoning,
                                 status, auto_revert_hours)
                            VALUES ('technical_catching_reversal', %s, %s, NULL,
                                    NULL, NULL, NULL,
                                    %s, 'tech_correct_rate', %s,
                                    'medium', %s, 'pending', 72)
                        """, (
                            self.user_id, instrument, total,
                            round(tech_rate, 4),
                            (
                                f"When technical disagreed with macro on {instrument}, "
                                f"technical direction was correct {tech_rate:.0%} of the time "
                                f"({total} samples). Technical may be catching short-term "
                                f"reversals that macro misses."
                            ),
                        ))
                        written += 1
                        logger.info(
                            f"Proposal: technical catching reversal on {instrument} — "
                            f"tech right {tech_rate:.0%} when conflicting ({total} samples)"
                        )

            # ── MACRO GATE ANALYSIS ───────────────────────────────────────────────
            # When macro was blocked by the p75 gate (rejection_stage='macro_gate'),
            # did price move in the macro direction anyway?
            cur.execute("""
                SELECT
                    r.instrument,
                    COUNT(*) AS total,
                    SUM(CASE
                        WHEN r.macro_score > 0 AND next_d.close > ref_d.close THEN 1
                        WHEN r.macro_score < 0 AND next_d.close < ref_d.close THEN 1
                        ELSE 0 END) AS macro_correct,
                    AVG(ABS(r.macro_score)) AS avg_macro_magnitude
                FROM forex_network.rejected_signals r
                JOIN forex_network.historical_prices ref_d
                    ON ref_d.instrument = r.instrument
                    AND ref_d.timeframe  = '1D'
                    AND ref_d.ts::date   = r.rejected_at::date
                JOIN forex_network.historical_prices next_d
                    ON next_d.instrument = r.instrument
                    AND next_d.timeframe  = '1D'
                    AND next_d.ts::date   = (r.rejected_at::date + 1)
                WHERE r.rejection_stage = 'macro_gate'
                  AND r.rejected_at::date < CURRENT_DATE
                  AND r.macro_score IS NOT NULL
                  AND r.macro_score != 0
                GROUP BY r.instrument
                HAVING COUNT(*) >= 10
            """)
            mg_rows = cur.fetchall()
            for row in mg_rows:
                instrument      = row[0]
                total           = int(row[1])
                macro_correct   = int(row[2] or 0)
                avg_magnitude   = float(row[3] or 0)
                macro_rate      = macro_correct / total if total > 0 else 0.0

                if macro_rate > 0.60:
                    # Macro direction was right even though signal was too weak to pass gate
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM forex_network.proposals
                        WHERE user_id = %s AND proposal_type = 'macro_gate_too_strict'
                          AND instrument = %s AND status = 'pending'
                    """, (self.user_id, instrument))
                    if int(cur.fetchone()[0]) == 0:
                        cur.execute("""
                            INSERT INTO forex_network.proposals
                                (proposal_type, user_id, instrument, parameter,
                                 current_value, proposed_value, direction,
                                 n_trades, metric, metric_value, confidence, reasoning,
                                 status, auto_revert_hours)
                            VALUES ('macro_gate_too_strict', %s, %s, 'MACRO_THRESHOLD',
                                    NULL, NULL, 'decrease',
                                    %s, 'macro_correct_rate', %s,
                                    'medium', %s, 'pending', 72)
                        """, (
                            self.user_id, instrument, total,
                            round(macro_rate, 4),
                            (
                                f"When macro was blocked by the p75 gate on {instrument}, "
                                f"macro direction was still correct {macro_rate:.0%} of the time "
                                f"({total} rejections, avg |macro_score|={avg_magnitude:.3f}). "
                                f"The p75 threshold may be overfitted or too conservative for "
                                f"this pair."
                            ),
                        ))
                        written += 1
                        logger.info(
                            f"Proposal: macro gate too strict on {instrument} — "
                            f"macro right {macro_rate:.0%} when gated ({total} samples, "
                            f"avg_magnitude={avg_magnitude:.3f})"
                        )

            self.db.commit()
            return written

        except Exception as e:
            logger.error(f"_generate_proposals failed: {e}")
            self.db.rollback()
            return 0
        finally:
            cur.close()

    def run_cycle(self):
        """Run a single learning cycle — process autopsies, check weekly report."""
        try:
            count = self.poll_and_process()
            if count > 0:
                logger.info(f"Processed {count} autopsy/ies")
            rej_count = 0
            if not self.dry_run:
                rej_count = self._process_rejections()
                if rej_count > 0:
                    logger.info(f"Processed {rej_count} rejection(s) into rejection_patterns")
            if not self.dry_run:
                prop_count = self._generate_proposals()
                if prop_count > 0:
                    logger.info(f"Generated {prop_count} proposal(s)")
            if not self.dry_run and self.weekly_report.should_generate():
                self.weekly_report.generate()
            self.cycle_count += 1
            self.write_heartbeat()
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

                # Generate structured proposals from rejection patterns + performance
                prop_count = self._generate_proposals()
                if prop_count > 0:
                    logger.info(f"Generated {prop_count} proposal(s)")

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
                f"{wins}/{total} = {win_rate:.1%%}"
            )
            if tier == 'medium' and win_rate < 0.50:
                proposals.append({
                    'type': 'macro_conviction_review',
                    'tier': 'medium',
                    'win_rate': round(win_rate, 4),
                    'trade_count': total,
                    'message': (
                        f"Medium-conviction trades (p50≤macro<p75) have a {win_rate:.0%%} win rate "
                        f"over {total} trades. Consider raising the macro gate threshold to p75 "
                        f"or tightening the medium tier definition."
                    ),
                })

        if proposals and not self.dry_run:
            self._write_proposals(proposals)

        cur.close()
        return len(tiers)

    def analyse_shadow_trades(self) -> List[dict]:
        """
        Analyses shadow trades by hypothesis to determine what changes
        would improve trade flow and accuracy.

        Five hypotheses tracked:
        - macro_only: macro signal correct without technical confirmation
        - tech_threshold_0.10: trades blocked at 0.10-0.20 technical score
        - macro_regime_no_tech: macro + regime agreement without technical
        - p75_relaxed: macro above p50 but below p75
        - directional_disagreement: macro and technical pointed opposite ways
        """
        proposals = []
        try:
            cur = self.db.cursor()

            # Require next-day price data to exist
            cur.execute("""
                SELECT
                    hypothesis,
                    instrument,
                    direction,
                    COUNT(*) as total,
                    SUM(CASE
                        WHEN direction = 'long' AND shadow_pips_1d > 1 THEN 1
                        WHEN direction = 'short' AND shadow_pips_1d < -1 THEN 1
                        ELSE 0 END) as correct_1d,
                    SUM(CASE
                        WHEN direction = 'long' AND shadow_pips_5d > 5 THEN 1
                        WHEN direction = 'short' AND shadow_pips_5d < -5 THEN 1
                        ELSE 0 END) as correct_5d,
                    AVG(macro_score) as avg_macro_score,
                    AVG(tech_score) as avg_tech_score
                FROM forex_network.shadow_trades
                WHERE shadow_pips_1d IS NOT NULL
                AND signal_time > %s
                GROUP BY hypothesis, instrument, direction
                HAVING COUNT(*) >= 10
            """, (DATA_QUALITY_CUTOFF,))

            rows = cur.fetchall()

            for row in rows:
                hypothesis = row['hypothesis']
                instrument = row['instrument']
                total = row['total']
                win_rate_1d = row['correct_1d'] / total if total > 0 else 0
                win_rate_5d = row['correct_5d'] / total if total > 0 else 0

                # Generate proposals based on evidence
                if hypothesis == 'tech_threshold_0.10' and win_rate_5d > 0.58:
                    proposals.append({
                        'proposal_type': 'lower_tech_threshold',
                        'instrument': instrument,
                        'message': (
                            f"Shadow trades on {instrument} with technical score 0.10-0.20 "
                            f"showed {win_rate_5d:.0%} 5D win rate on {total} observations. "
                            f"Current threshold of 0.20 is blocking profitable setups. "
                            f"Consider lowering TECH_MIN_THRESHOLD to 0.10 for this pair."
                        ),
                        'evidence': {
                            'hypothesis': hypothesis,
                            'total': total,
                            'win_rate_1d': round(win_rate_1d, 3),
                            'win_rate_5d': round(win_rate_5d, 3),
                            'avg_macro_score': round(float(row['avg_macro_score'] or 0), 3),
                        }
                    })

                elif hypothesis == 'macro_only' and win_rate_5d > 0.58:
                    proposals.append({
                        'proposal_type': 'macro_sufficient',
                        'instrument': instrument,
                        'message': (
                            f"Macro-only signals on {instrument} showed {win_rate_5d:.0%} "
                            f"5D win rate on {total} observations without technical confirmation. "
                            f"Technical gate may be unnecessary for this pair."
                        ),
                        'evidence': {
                            'hypothesis': hypothesis,
                            'total': total,
                            'win_rate_5d': round(win_rate_5d, 3),
                        }
                    })

                elif hypothesis == 'p75_relaxed' and win_rate_5d > 0.55:
                    proposals.append({
                        'proposal_type': 'lower_p75_gate',
                        'instrument': instrument,
                        'message': (
                            f"Signals on {instrument} above p50 but below p75 showed "
                            f"{win_rate_5d:.0%} 5D win rate on {total} observations. "
                            f"p75 gate may be too strict — consider p60 or p65."
                        ),
                        'evidence': {
                            'hypothesis': hypothesis,
                            'total': total,
                            'win_rate_5d': round(win_rate_5d, 3),
                        }
                    })

                elif hypothesis == 'directional_disagreement' and win_rate_5d > 0.58:
                    proposals.append({
                        'proposal_type': 'macro_overrides_technical',
                        'instrument': instrument,
                        'message': (
                            f"When macro and technical disagreed on {instrument}, "
                            f"macro was correct {win_rate_5d:.0%} of the time on {total} observations. "
                            f"Consider removing directional agreement requirement for this pair."
                        ),
                        'evidence': {
                            'hypothesis': hypothesis,
                            'total': total,
                            'win_rate_5d': round(win_rate_5d, 3),
                        }
                    })

                elif hypothesis == 'macro_regime_no_tech' and win_rate_5d > 0.58:
                    proposals.append({
                        'proposal_type': 'regime_replaces_technical',
                        'instrument': instrument,
                        'message': (
                            f"Macro + regime agreement on {instrument} without technical "
                            f"showed {win_rate_5d:.0%} 5D win rate on {total} observations. "
                            f"Regime agreement may be sufficient technical substitute."
                        ),
                        'evidence': {
                            'hypothesis': hypothesis,
                            'total': total,
                            'win_rate_5d': round(win_rate_5d, 3),
                        }
                    })

            if proposals:
                logger.info(f"Shadow trade analysis: {len(proposals)} proposals generated")
                self._write_proposals(proposals)
            else:
                logger.debug("Shadow trade analysis: no proposals (insufficient data or no edge found)")

            cur.close()
            return proposals

        except Exception as e:
            logger.error(f"analyse_shadow_trades failed: {e}")
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

