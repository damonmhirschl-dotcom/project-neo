#!/usr/bin/env python3
"""
Project Neo — Execution Agent v1.0
====================================
Submits orders via broker abstraction, manages stops, monitors open positions.
The only agent that touches the broker. Paper mode enforced until go-live.

Cycle: Continuous (polls for risk_approved signals every 10 seconds)
IAM Role: platform-execution-role-dev
Kill Switch: Tighten and Hold (Option A) per Kill Switch Protocol v1.0

Run:
  source ~/algodesk/bin/activate
  python execution_agent.py --user neo_user_002
"""

import os
import sys
import json
import time
import uuid
import random
import logging
import argparse
import datetime
import traceback
import requests
from typing import Optional, Dict, List, Any, Tuple

import boto3
import sys as _alerting_sys
_alerting_sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.alerting import send_alert
import psycopg2
import psycopg2.extras
from shared.schema_validator import validate_schema
from shared.signal_validator import SignalValidator
from shared.market_hours import get_market_state
from shared.system_events import log_event
from shared.warn_log import warn
from shared.broker_interface import BrokerInterface

EXPECTED_TABLES = {
    "forex_network.trades":           ["user_id", "instrument", "direction", "entry_price",
                                       "stop_price", "target_price", "position_size",
                                       "entry_time", "exit_time", "exit_price",
                                       "exit_price_source", "pnl", "pnl_pips",
                                       "hold_days", "exit_reason", "ibkr_order_id",
                                       "ibkr_stop_order_id", "convergence_score",
                                       "approval_signal_id"],
    "forex_network.order_events":     ["trade_id", "event_type", "ibkr_order_id",
                                       "rejection_reason", "filled_price", "filled_size"],
    "forex_network.swap_rates":       ["instrument", "long_rate_pips",
                                       "short_rate_pips", "rate_date"],
    "forex_network.system_alerts":    ["alert_type", "severity", "title", "detail",
                                       "acknowledged", "created_at"],
    "forex_network.agent_heartbeats": ["agent_name", "user_id", "last_seen",
                                       "status", "cycle_count"],
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("neo.execution")


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


# =============================================================================
# CONSTANTS
# =============================================================================
AWS_REGION = "eu-west-2"
POLL_INTERVAL_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 60
AGENT_NAME = "execution"

def _ssm_get(name, default):
    try:
        import boto3 as _b
        return _b.client("ssm", region_name="eu-west-2").get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default

# Broker selection — set /platform/config/broker to: ig_demo | ig_live | ibkr_paper | ibkr_live
BROKER_NAME = _ssm_get("/platform/config/broker", "ig_demo")


def _build_broker():
    """Instantiate the configured broker. Called once at agent startup."""
    from shared.broker_interface import BrokerInterface
    if BROKER_NAME.startswith("ig"):
        from shared.brokers.ig_broker import IGBroker
        broker = IGBroker(demo=(BROKER_NAME == "ig_demo"))
    elif BROKER_NAME.startswith("ibkr"):
        from shared.brokers.ibkr_broker import IBKRBroker
        broker = IBKRBroker(paper=(BROKER_NAME == "ibkr_paper"))
    else:
        raise ValueError(f"Unknown broker: {BROKER_NAME!r}. "
                         f"Set /platform/config/broker to ig_demo|ig_live|ibkr_paper|ibkr_live")
    broker.authenticate()
    logger.info(f"Broker initialised: {BROKER_NAME} ({type(broker).__name__})")
    return broker

# Order retry config
def _to_float(val):
    """Safely coerce val to float, returning None if val is empty or unconvertible."""
    try:
        f = float(val)
        return f if f else None
    except (TypeError, ValueError):
        return None

SOFT_REJECT_MAX_RETRIES = 3
SOFT_REJECT_WAIT_SECONDS = 30
STOP_FAILURE_TIMEOUT_SECONDS = 60

# Adversarial defence — execution fingerprinting (Rules 14-17)
STOP_RANDOMISATION_RANGE = 0.15     # ±15% of ATR
TIMING_JITTER_MIN = 0               # seconds
TIMING_JITTER_MAX = 90              # seconds
POSITION_SIZE_VARIATION = 0.12      # ±12%
LIMIT_ORDER_SPREAD_PIPS = 0.5      # pips inside market for limit orders

# Partial fill thresholds
PARTIAL_FILL_ACCEPT_PCT = 80.0

# Slippage thresholds per pair (pips)
SLIPPAGE_THRESHOLDS = {
    "EURUSD": 3, "USDJPY": 3,
    "GBPUSD": 4, "USDCHF": 4,
    "AUDUSD": 5, "USDCAD": 5, "NZDUSD": 5,
}

# Kill switch stop tightening (from Kill Switch Protocol v1.0)
KILL_SWITCH_STOP_MULTIPLIERS = {
    "standard": 1.0,    # ATR × 1.0 for stress 85-92 or manual
    "severe":   0.75,   # ATR × 0.75 for stress 93-100
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
# ORDER EVENT WRITER
# =============================================================================
class OrderEventWriter:
    """Writes to order_events and system_alerts tables."""

    def __init__(self, db: DatabaseConnection, user_id: str):
        self.db = db
        self.user_id = user_id

    def write_order_event(self, trade_id: Optional[int], event_type: str,
                          details: Dict[str, Any], retry_attempt: int = 0):
        """Write to order_events using the actual DB schema.

        Schema (confirmed 2026-04-20):
            id, user_id, trade_id, ibkr_order_id, event_type, instrument,
            direction, order_type, requested_price, requested_size,
            filled_price, filled_size, rejection_reason, rejection_code,
            retry_attempt, retry_after_ms, session_at_event, created_at

        The `details` dict is decomposed into structured columns.  Fields not
        present in the schema are silently ignored — no 'details' JSONB column.
        """
        # Extract structured fields from the incoming details dict
        ibkr_order_id    = str(details.get("orderId", "") or details.get("ibkr_order_id", "") or "")
        instrument       = details.get("instrument")
        direction        = details.get("direction")
        order_type       = details.get("order_type") or details.get("orderType")
        requested_price  = _to_float(details.get("requested_price") or details.get("price"))
        requested_size   = _to_float(details.get("requested_size") or details.get("quantity"))
        filled_price     = _to_float(
            details.get("filled_price") or details.get("avgPrice")
            or details.get("exit_price")
        )
        filled_size      = _to_float(details.get("filled_size") or details.get("filled_quantity"))
        # Rejection info — prioritise explicit keys then check nested response
        _resp = details.get("response", {}) if isinstance(details.get("response"), dict) else {}
        rejection_reason = (
            details.get("rejection_reason")
            or details.get("error")
            or _resp.get("error")
        )
        rejection_code   = details.get("rejection_code") or details.get("code")
        retry_after_ms   = details.get("retry_after_ms")
        session_at_event = details.get("session_at_event") or details.get("session")

        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.order_events
                    (trade_id, user_id, ibkr_order_id, event_type, instrument,
                     direction, order_type, requested_price, requested_size,
                     filled_price, filled_size, rejection_reason, rejection_code,
                     retry_attempt, retry_after_ms, session_at_event, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                trade_id, self.user_id,
                ibkr_order_id or None, event_type, instrument,
                direction, order_type, requested_price, requested_size,
                filled_price, filled_size, rejection_reason, rejection_code,
                retry_attempt, retry_after_ms, session_at_event,
            ))
            self.db.commit()
        except Exception as e:
            logger.error(f"Order event write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def write_system_alert(self, alert_type: str, severity: str, title: str, detail: str):
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.system_alerts
                    (alert_type, severity, title, detail, user_id, acknowledged, created_at)
                VALUES (%s, %s, %s, %s, %s, FALSE, NOW())
            """, (alert_type, severity, title, detail, self.user_id))
            self.db.commit()
        except Exception as e:
            logger.error(f"System alert write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def write_audit_log(self, event_type: str, description: str,
                        metadata: Dict[str, Any], source: str = "execution_agent"):
        cur = self.db.cursor()
        try:
            cur.execute("""
                INSERT INTO forex_network.audit_log
                    (event_type, description, metadata, source)
                VALUES (%s, %s, %s, %s)
            """, (event_type, description, json.dumps(metadata), source))
            self.db.commit()
        except Exception as e:
            logger.error(f"Audit log write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()


# =============================================================================
# EXECUTION FINGERPRINTING DEFENCE (Rules 14-17)
# =============================================================================
class ExecutionDefence:
    """Applies randomisation to prevent execution pattern fingerprinting."""

    @staticmethod
    def randomise_stop(stop_price: float, atr: float, direction: str) -> float:
        """Rule 14: Add random offset to ATR-based stop level."""
        offset = random.uniform(-STOP_RANDOMISATION_RANGE, STOP_RANDOMISATION_RANGE) * atr
        randomised = stop_price + offset
        return round(randomised, 5)

    @staticmethod
    def get_timing_jitter() -> float:
        """Rule 15: Random delay before submitting order."""
        return random.uniform(TIMING_JITTER_MIN, TIMING_JITTER_MAX)

    @staticmethod
    def vary_position_size(size: float) -> float:
        """Rule 16: Apply ±12% variation to position size."""
        variation = random.uniform(-POSITION_SIZE_VARIATION, POSITION_SIZE_VARIATION)
        varied = size * (1.0 + variation)
        return round(varied, 2)

    @staticmethod
    def should_use_limit_order(spread_pips: float, normal_spread: float) -> bool:
        """Rule 17: Prefer limit orders when spread is within normal range."""
        return spread_pips <= normal_spread * 1.5


# =============================================================================
# RECONCILIATION ENGINE
# =============================================================================
class ReconciliationEngine:
    """Handles startup and per-cycle position reconciliation."""

    def __init__(self, db: DatabaseConnection, broker: BrokerInterface,
                 user_id: str, event_writer: OrderEventWriter):
        self.db = db
        self.broker = broker
        self.user_id = user_id
        self.events = event_writer
        self.agent = None  # C12: back-reference to ExecutionAgent, set after construction

    def reconcile(self) -> bool:
        """
        Cross-reference IG positions against RDS trades by dealId.
        Returns True if reconciliation is clean.
        Rule 11: Per-cycle IG position state verification.
        """
        logger.info("Running position reconciliation...")

        # Get IG positions
        ig_positions = self.broker.get_positions()
        ig_pos_by_deal = {p['dealId']: p for p in ig_positions if p.get('dealId')}
        ig_deal_ids = set(ig_pos_by_deal.keys())

        # Get RDS open trades
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, instrument, direction, entry_price, stop_price,
                       position_size_usd, position_size, ibkr_stop_order_id, entry_time, user_id,
                       ibkr_order_id
                FROM forex_network.trades
                WHERE exit_time IS NULL
            """)
            all_open_trades = cur.fetchall()
            # This user's open trades (for ghost detection), keyed by dealId
            db_trades_this_user = [dict(row) for row in all_open_trades if row["user_id"] == self.user_id]
            db_trade_by_deal = {row["ibkr_order_id"]: row for row in db_trades_this_user if row.get("ibkr_order_id")}
            this_user_deal_ids = set(db_trade_by_deal.keys())
            # All dealIds across all users (for orphan detection — IG is a single shared account)
            all_db_deal_ids = {row["ibkr_order_id"] for row in all_open_trades if row.get("ibkr_order_id")}
        except Exception as e:
            logger.error(f"Reconciliation RDS read failed: {e}")
            return False
        finally:
            cur.close()

        clean = True

        # Check for orphans: IG position with no DB trade across ALL users
        # IG is a single shared account — orphan only if NO user has this dealId tracked
        for deal_id, ig_pos in ig_pos_by_deal.items():
            if deal_id not in all_db_deal_ids:
                instrument = ig_pos.get("instrument", "UNKNOWN")
                logger.warning(f"ORPHAN: IG has {instrument} (dealId={deal_id}) but no DB trade for any user")
                self.events.write_system_alert(
                    "reconciliation_orphan", "critical",
                    f"Orphan position: {instrument} (dealId={deal_id}) in IG with no DB trade",
                    "Position exists in IG but not tracked in DB. Manual review required.",
                )
                clean = False

        # Check for ghosts: DB trade with no IG position
        for deal_id, rds_trade in db_trade_by_deal.items():
            if deal_id not in ig_deal_ids:
                instrument = rds_trade["instrument"]
                logger.warning(f"GHOST: DB has {instrument} (dealId={deal_id}) but no IG position")
                # Position was closed externally — attempt best-effort exit price via live quote
                # C6: never write entry_price as exit_price (that zeroes out real P&L)
                ghost_exit_price = None
                ghost_price_source = 'unavailable'
                try:
                    quote = self.broker.get_live_quote(instrument)
                    mid = quote.get('mid') or quote.get('ask') or quote.get('bid')
                    if mid:
                        candidate = float(mid)
                        if candidate > 0:
                            ghost_exit_price = candidate
                            ghost_price_source = 'live_quote'
                except Exception as e:
                    logger.warning(f"Ghost cleanup live_quote failed for {instrument}: {e}")
                if ghost_exit_price is None:
                    logger.warning(
                        f"Ghost cleanup {instrument}: no live quote available — "
                        "exit_price will be NULL (fail-safe: not writing entry_price as exit)"
                    )

                # Compute P&L if we have a price
                ghost_pnl_pips = None
                ghost_pnl_usd = None
                if ghost_exit_price is not None and rds_trade.get("entry_price") and rds_trade.get("direction"):
                    try:
                        _ep = float(rds_trade["entry_price"])
                        _sz = float(rds_trade.get("position_size_usd") or 0)
                        # _compute_pnl is a @staticmethod on ExecutionAgent — call via class
                        ghost_pnl_pips, ghost_pnl_usd = ExecutionAgent._compute_pnl(
                            instrument, rds_trade["direction"], _ep, ghost_exit_price, _sz
                        )
                    except Exception as e:
                        logger.warning(f"Ghost cleanup P&L calc failed for {instrument}: {e}")

                # Hold days
                ghost_hold_days = 0
                try:
                    import datetime as _dt
                    entry_time = rds_trade.get("entry_time")
                    if entry_time:
                        now = _dt.datetime.now(_dt.timezone.utc)
                        if entry_time.tzinfo is None:
                            entry_time = entry_time.replace(tzinfo=_dt.timezone.utc)
                        ghost_hold_days = (now - entry_time).total_seconds() / 86400
                except Exception:
                    pass

                cur = self.db.cursor()
                try:
                    cur.execute("""
                        UPDATE forex_network.trades
                        SET exit_time = NOW(), exit_reason = 'manual',
                            exit_price = %s,
                            exit_price_source = %s,
                            pnl_pips = %s,
                            pnl = %s,
                            hold_days = %s
                        WHERE id = %s AND exit_time IS NULL
                    """, (
                        ghost_exit_price,
                        ghost_price_source,
                        ghost_pnl_pips,
                        ghost_pnl_usd,
                        round(ghost_hold_days, 2),
                        rds_trade["id"],
                    ))
                    self.db.commit()
                except Exception as e:
                    logger.error(f"Ghost cleanup failed: {e}")
                    self.db.rollback()
                finally:
                    cur.close()
                self.events.write_order_event(
                    rds_trade["id"], "reconciled",
                    {"instrument": instrument, "action": "closed_externally",
                     "exit_price": ghost_exit_price, "exit_price_source": ghost_price_source},
                )
                clean = False

        # Matched positions — verify stops are still active
        for deal_id in ig_deal_ids & this_user_deal_ids:
            rds_trade = db_trade_by_deal[deal_id]
            instrument = rds_trade["instrument"]
            if rds_trade.get("ibkr_stop_order_id"):
                stop_status = self.broker.get_order_status(rds_trade["ibkr_stop_order_id"])
                status_val = stop_status.get("order_status") or stop_status.get("status") or ""
                if stop_status.get("error"):
                    logger.warning(f"Stop order status error for {instrument}: {stop_status.get('error')}")
                    self.events.write_order_event(
                        rds_trade["id"], "reconciled",
                        {"instrument": instrument, "stop_status": stop_status},
                    )
                elif status_val in ("Filled", "filled"):
                    # C12: Stop has filled — trade closed. Capture fill price and write close row.
                    logger.info(f"[reconcile] Stop filled for {instrument} (trade {rds_trade['id']})")
                    if self.agent is not None:
                        # Delegate to ExecutionAgent._handle_trade_closed with stop_fill path
                        self.agent._handle_trade_closed(rds_trade, prev_snapshot={},
                                                        close_cause='stop_hit')
                    else:
                        # Fallback: inline close if agent reference not available
                        _raw_fill = (stop_status.get('avgPrice') or stop_status.get('avg_price') or 0)
                        _fill_price = float(_raw_fill) if _raw_fill and float(_raw_fill) > 0 else None
                        _src = 'stop_fill' if _fill_price else 'unavailable'
                        _cur = self.db.cursor()
                        try:
                            _cur.execute("""
                                UPDATE forex_network.trades
                                SET exit_time = NOW(), exit_reason = 'stop_hit',
                                    exit_price = %s, exit_price_source = %s
                                WHERE id = %s AND exit_time IS NULL
                            """, (_fill_price, _src, rds_trade["id"]))
                            self.db.commit()
                        except Exception:
                            self.db.rollback()
                        finally:
                            _cur.close()
                    clean = False
                elif status_val in ("Cancelled", "cancelled"):
                    # C12: Stop was externally cancelled — position unprotected
                    logger.critical(
                        f"Stop CANCELLED externally for trade {rds_trade['id']} {instrument}. "
                        "Position unprotected."
                    )
                    self.events.write_system_alert(
                        'stop_cancelled_externally', 'critical',
                        f"Stop order cancelled for {instrument} trade {rds_trade['id']}",
                        "Stop order was externally cancelled. Position has no stop. "
                        "Manual review required."
                    )
                    clean = False

        if clean:
            logger.info("Reconciliation clean — all positions matched")
        else:
            logger.warning("Reconciliation found discrepancies — check alerts")

        return clean

    @staticmethod
    def _normalise_instrument(position: Any) -> str:
        """Extract normalised instrument name from a broker position dict."""
        if isinstance(position, dict):
            instrument = position.get("instrument")  # IG — already normalised pair name
            if not instrument:
                # IBKR positions use contractDesc / symbol with slash or dot separators
                instrument = position.get("contractDesc", position.get("symbol", ""))
            return instrument.upper().replace("/", "").replace(".", "").replace(" ", "")
        return str(position).upper().replace("/", "").replace(".", "").replace(" ", "")


# =============================================================================
# KILL SWITCH HANDLER
# =============================================================================
class KillSwitchHandler:
    """Implements Kill Switch Protocol v1.0 — Option A (Tighten and Hold)."""

    def __init__(self, db: DatabaseConnection, broker: BrokerInterface,
                 user_id: str, event_writer: OrderEventWriter):
        self.db = db
        self.broker = broker
        self.user_id = user_id
        self.events = event_writer
        self._activated = False

    def handle_activation(self, stress_score: float = 0):
        """Execute kill switch position handling."""
        if self._activated:
            return  # Already handled this activation
        self._activated = True

        logger.warning("KILL SWITCH ACTIVATED — executing Option A: Tighten and Hold")

        # Determine stop multiplier based on stress score
        if stress_score >= 93:
            stop_mult = KILL_SWITCH_STOP_MULTIPLIERS["severe"]
        else:
            stop_mult = KILL_SWITCH_STOP_MULTIPLIERS["standard"]

        # 1. Cancel all pending entry orders via IBKR
        logger.info("Kill switch: cancelling all pending entry orders")
        _cancelled = 0
        _cancel_errors = 0
        if self.account_id:
            try:
                open_orders = self.broker.get_open_orders()
                for ord_item in open_orders:
                    if not isinstance(ord_item, dict):
                        continue
                    order_id = str(ord_item.get("orderId") or ord_item.get("id") or "")
                    order_status = str(ord_item.get("status") or ord_item.get("orderStatus") or "")
                    # Only cancel orders that are still pending (not yet filled/cancelled)
                    if order_id and order_status.upper() not in ("FILLED", "CANCELLED", "INACTIVE"):
                        cancel_resp = self.broker.cancel_order(order_id)
                        if "error" not in cancel_resp:
                            _cancelled += 1
                            logger.info(f"Kill switch: cancelled order {order_id}")
                        else:
                            _cancel_errors += 1
                            logger.warning(
                                f"Kill switch: failed to cancel order {order_id}: "
                                f"{cancel_resp.get('error')}"
                            )
            except Exception as e:
                logger.error(f"Kill switch order cancellation failed: {e}")
        else:
            logger.warning("Kill switch: account_id not set — cannot cancel IBKR orders")

        logger.info(
            f"Kill switch: cancelled {_cancelled} pending orders "
            f"({_cancel_errors} errors)"
        )

        # 2. Tighten all open stops
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, instrument, direction, entry_price, stop_price,
                       ibkr_stop_order_id
                FROM forex_network.trades
                WHERE user_id = %s AND exit_time IS NULL
            """, (self.user_id,))
            open_trades = cur.fetchall()

            for trade in open_trades:
                # Get current ATR for the instrument
                cur2 = self.db.cursor()
                cur2.execute("""
                    SELECT atr_14 FROM forex_network.price_metrics
                    WHERE instrument = %s AND timeframe = '1H'
                    ORDER BY ts DESC LIMIT 1
                """, (trade["instrument"],))
                atr_row = cur2.fetchone()
                cur2.close()

                if atr_row and atr_row["atr_14"]:
                    atr = float(atr_row["atr_14"])
                    new_stop_distance = atr * stop_mult

                    if trade["direction"] == "long":
                        new_stop = float(trade["entry_price"]) - new_stop_distance
                        # Only tighten (move stop up for longs)
                        if trade["stop_price"] and new_stop > float(trade["stop_price"]):
                            self._modify_stop(trade, new_stop, stop_mult)
                    else:
                        new_stop = float(trade["entry_price"]) + new_stop_distance
                        # Only tighten (move stop down for shorts)
                        if trade["stop_price"] and new_stop < float(trade["stop_price"]):
                            self._modify_stop(trade, new_stop, stop_mult)

            logger.info(f"Tightened stops on {len(open_trades)} position(s) with ATR × {stop_mult}")

        except Exception as e:
            logger.error(f"Kill switch stop tightening failed: {e}")
        finally:
            cur.close()

        self.events.write_audit_log(
            "kill_switch_execution",
            f"Kill switch Option A executed: {len(open_trades)} positions tightened, ATR × {stop_mult}",
            {"stress_score": stress_score, "stop_multiplier": stop_mult,
             "positions_affected": len(open_trades)},
        )

    def _modify_stop(self, trade: Dict, new_stop: float, multiplier: float):
        """Modify a stop order on IBKR and update RDS (kill-switch path)."""
        order_id = trade.get("ibkr_stop_order_id")
        if order_id and self.account_id:
            try:
                resp = self.broker.modify_order(str(order_id),
                    {"auxPrice": new_stop},
                )
                if "error" in resp:
                    logger.error(
                        f"Kill switch: stop modification failed for trade "
                        f"{trade['id']} order {order_id}: {resp.get('error')}"
                    )
                else:
                    logger.info(
                        f"Kill switch: stop modified for trade {trade['id']} "
                        f"order {order_id} → {new_stop}"
                    )
            except Exception as e:
                logger.error(f"Kill switch: modify_order exception for trade {trade['id']}: {e}")
        elif order_id and not self.account_id:
            logger.warning(
                f"Kill switch: account_id not set — cannot modify stop for "
                f"trade {trade['id']} on IBKR"
            )

        # Update RDS
        cur = self.db.cursor()
        try:
            cur.execute("""
                UPDATE forex_network.trades
                SET stop_price = %s
                WHERE id = %s
            """, (new_stop, trade["id"]))
            self.db.commit()
        except Exception as e:
            logger.error(f"Stop modification DB update failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

        self.events.write_order_event(
            trade["id"], "kill_switch_tighten",
            {"instrument": trade["instrument"], "old_stop": float(trade["stop_price"]),
             "new_stop": new_stop, "multiplier": multiplier},
        )

    def handle_deactivation(self):
        """Reset state on kill switch deactivation. Stops remain tightened."""
        self._activated = False
        logger.info("Kill switch deactivated — stops remain at tightened levels")


# =============================================================================
# TRADE EXECUTOR
# =============================================================================
class TradeExecutor:
    """Handles the full order lifecycle: entry, stop, monitoring."""

    def __init__(self, db: DatabaseConnection, broker: BrokerInterface,
                 user_id: str, event_writer: OrderEventWriter):
        self.db = db
        self.broker = broker
        self.user_id = user_id
        self.events = event_writer
        self._validator = SignalValidator()
        self.defence = ExecutionDefence()
        self.account_id = None  # Set by ExecutionAgent after startup

    def _log_execution_failure(self, approval: Dict[str, Any], failure_type: str,
                               failure_reason: str, ibkr_error_code: int = None,
                               intended_entry: float = None):
        """Write a row to execution_failures for any failure in the trade entry path.

        Args:
            approval:        The original approval dict from the risk_guardian.
            failure_type:    Short code — 'ibkr_rejection', 'db_write_failure', etc.
            failure_reason:  Human-readable description of why the trade failed.
            ibkr_error_code: HTTP status or IBKR error code, if available.
            intended_entry:  The price we attempted to enter at (LMT price or live
                             quote at submission time).  Populated from order["price"]
                             or from a live quote captured just before submission.
        """
        cur = self.db.cursor()
        try:
            sizing = (
                approval.get('payload', {})
                        .get('risk_details', {})
                        .get('sizing', {})
            )
            intended_size_usd = (
                sizing.get('varied_size_usd')
                or sizing.get('position_size_usd')
                or approval.get('payload', {})
                           .get('risk_details', {})
                           .get('varied_size')
            )
            cur.execute("""
                INSERT INTO forex_network.execution_failures
                    (approval_signal_id, user_id, instrument, direction,
                     intended_entry, intended_size_usd,
                     failure_type, failure_reason, ibkr_error_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                approval.get('signal_id'),
                self.user_id,
                approval.get('instrument'),
                approval.get('direction'),
                intended_entry,
                intended_size_usd,
                failure_type,
                failure_reason,
                ibkr_error_code,
            ))
            self.db.commit()
        except Exception as e:
            logger.error(f'_log_execution_failure write failed: {e}')
            try:
                self.db.rollback()
            except Exception:
                pass
        finally:
            cur.close()

    def _wait_for_fill(self, order_id: str, instrument: str,
                       timeout_seconds: int = 30, poll_interval: float = 0.5) -> dict:
        """Poll broker until filled or timeout.

        For IG: broker.get_order_status returns from the fill cache set during
        place_order — no actual polling occurs. fill_price already captured.
        For IBKR: polls the gateway until status=Filled or timeout.
        """
        import time as _t
        deadline = _t.time() + timeout_seconds
        last_status = None

        while _t.time() < deadline:
            try:
                resp = self.broker.get_order_status(order_id)
            except Exception as e:
                logger.warning(f'[fill-poll] {instrument} {order_id}: status error {e}')
                _t.sleep(poll_interval)
                continue

            st = (resp.get('status') or resp.get('order_status') or
                  resp.get('orderStatus') or '').strip()
            last_status = st
            avg_p   = float(resp.get('avgPrice') or resp.get('avg_price') or
                            resp.get('fill_price') or resp.get('level') or 0)
            filled  = float(resp.get('filledQuantity') or resp.get('filled') or
                            resp.get('cum_fill') or resp.get('size') or 0)
            remaining = float(resp.get('remainingQuantity') or resp.get('remaining') or 0)

            logger.debug(f'[fill-poll] {instrument} {order_id}: {st} avg={avg_p}')

            if st.lower() in ('filled', 'open'):
                return {'status': 'filled', 'avg_price': avg_p,
                        'filled_qty': filled, 'remaining_qty': 0.0}

            if st.lower() in ('cancelled', 'rejected', 'inactive', 'cancelledonfill'):
                return {'status': st.lower(), 'avg_price': 0.0,
                        'filled_qty': 0.0, 'remaining_qty': remaining,
                        'error': resp.get('reject_reason') or resp.get('error')}

            if st.lower() == 'notfound':
                # IG: order not in cache and not in positions — treat as failed
                logger.warning(f'[fill-poll] {instrument} {order_id}: NotFound — treating as failed')
                return {'status': 'timeout', 'avg_price': 0.0,
                        'filled_qty': 0.0, 'remaining_qty': 0.0}

            _t.sleep(poll_interval)

        logger.warning(f'[fill-poll] {instrument} order {order_id} timed out after '
                       f'{timeout_seconds}s (last: {last_status}), cancelling')
        try:
            self.broker.cancel_order(order_id)
        except Exception as e:
            logger.error(f'[fill-poll] cancel failed for timed-out order {order_id}: {e}')

        return {'status': 'timeout', 'avg_price': 0.0, 'filled_qty': 0.0,
                'remaining_qty': 0.0, 'last_status': last_status}


    def execute_trade(self, approval: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a risk-approved trade."""
        instrument = approval["instrument"]
        direction = approval["direction"]
        payload = approval.get("payload", {})
        # Risk guardian signal contract check
        self._validator.validate_and_log("risk_guardian", payload, instrument, logger)
        risk_details = payload.get("risk_details", {})
        sizing = risk_details.get("sizing", {})

        logger.info(f"Executing: {instrument} {direction}")

        result = {
            "instrument": instrument,
            "direction": direction,
            "executed": False,
            "trade_id": None,
            "details": {},
        }

        # Rule 15: Apply timing jitter
        jitter = self.defence.get_timing_jitter()
        if jitter > 0:
            logger.info(f"Rule 15: Timing jitter {jitter:.0f}s")
            time.sleep(jitter)

        # Calculate position size from account value and risk percentage
        account_value = float(sizing.get("account_value", 10000))
        risk_pct = float(sizing.get("effective_risk_pct", 1.0))
        risk_amount = account_value * risk_pct / 100.0
        # Convert risk amount to position size using ATR stop distance
        # position_size = risk_amount / stop_distance_in_price
        stop_dist = sizing.get("stop_distance")
        if stop_dist and stop_dist > 0:
            base_size = risk_amount / stop_dist
        else:
            base_size = risk_amount * 100  # Fallback: approximate micro lots
        # Rule 16: Apply position size variation
        varied_size = self.defence.vary_position_size(base_size)
        result["details"]["account_value"] = account_value
        result["details"]["risk_amount"] = round(risk_amount, 2)
        result["details"]["requested_size"] = round(base_size, 2)
        result["details"]["varied_size"] = varied_size

        # Calculate stop with Rule 14 randomisation
        atr = sizing.get("atr_14")
        atr_mult = sizing.get("atr_stop_multiplier", 1.5)
        stop_price = None
        if atr:
            base_stop_distance = atr * atr_mult
            # Rule 14: Randomise
            randomised_stop_distance = base_stop_distance + random.uniform(
                -STOP_RANDOMISATION_RANGE * atr, STOP_RANDOMISATION_RANGE * atr
            )
            result["details"]["stop_distance_base"] = round(base_stop_distance, 5)
            result["details"]["stop_distance_randomised"] = round(randomised_stop_distance, 5)

        # Rule 17: Determine order type
        spread_pips = payload.get("risk_management", {}).get("current_spread", 1.0)
        use_limit = self.defence.should_use_limit_order(spread_pips, 1.5)
        result["details"]["order_type"] = "limit" if use_limit else "market"

        # Build broker order
        # Compute absolute stop price for broker payload
        _sizing = risk_details.get("sizing", {})
        _current_price: Optional[float] = None
        _stop_price_for_order: Optional[float] = None
        _stop_dist = _sizing.get("stop_distance")
        _risk_amount_gbp = float(_sizing.get("risk_amount_gbp") or
                                 (float(_sizing.get("account_value", 10000)) *
                                  float(_sizing.get("effective_risk_pct", 1.0)) / 100.0))
        try:
            _q = self.broker.get_live_quote(instrument)
            _current_price = _q.get("mid")
            if _current_price and _stop_dist:
                _stop_price_for_order = (
                    round(_current_price - _stop_dist, 5) if direction == "long"
                    else round(_current_price + _stop_dist, 5)
                )
        except Exception:
            pass
        order = self._build_order(instrument, direction, varied_size, use_limit,
                                  stop_price=_stop_price_for_order,
                                  current_price=_current_price,
                                  risk_amount_gbp=_risk_amount_gbp,
                                  stop_distance=_stop_dist)

        # Capture intended entry price for execution_failures logging.
        # For LMT orders, this is the limit price set in _build_order.
        # For MKT orders, attempt a quick live quote; fall back to None.
        intended_entry: Optional[float] = order.get("price") or None
        if intended_entry is None:
            try:
                _q = self.broker.get_live_quote(instrument)
                intended_entry = _q.get("mid")
            except Exception:
                pass
        result["details"]["intended_entry"] = intended_entry

        # Submit entry order
        if not self.account_id:
            raise RuntimeError(
                'account_id not resolved — startup incomplete, cannot submit order'
            )
        entry_start = time.time()
        response = self.broker.place_order(order)
        fill_time_ms = int((time.time() - entry_start) * 1000)
        result["details"]["fill_time_ms"] = fill_time_ms

        if "error" in response:
            # Classify rejection
            is_hard = self._is_hard_rejection(response)
            if is_hard:
                self._handle_hard_rejection(instrument, direction, response)
                self._log_execution_failure(
                    approval, 'ibkr_rejection',
                    response.get('error', 'hard rejection'),
                    ibkr_error_code=response.get('status'),
                    intended_entry=intended_entry,
                )
                result["details"]["rejection"] = "hard"
                return result
            else:
                # Soft rejection — retry
                success = self._retry_soft_rejection(instrument, direction, order, response)
                if not success:
                    self._log_execution_failure(
                        approval, 'ibkr_rejection',
                        f'soft rejection exhausted after {SOFT_REJECT_MAX_RETRIES} retries: '
                        + response.get('error', 'unknown'),
                        intended_entry=intended_entry,
                    )
                    result["details"]["rejection"] = "soft_exhausted"
                    return result

        # Extract order_id from place_order response
        order_id = response.get("orderId")
        if not order_id:
            logger.error(f'[fill-poll] {instrument}: no orderId in place_order response: {response}')
            self._log_execution_failure(
                approval, 'order_id_missing',
                f'place_order returned no orderId: {str(response)[:200]}',
                intended_entry=intended_entry,
            )
            return result

        # Poll for fill
        fill_result = self._wait_for_fill(order_id, instrument, timeout_seconds=30)

        if fill_result['status'] not in ('filled', 'partial_fill_timeout'):
            logger.error(
                f'[fill-poll] {instrument} order {order_id} not filled: '
                f'status={fill_result["status"]}'
            )
            self._log_execution_failure(
                approval, 'order_not_filled',
                f'status={fill_result["status"]}',
                intended_entry=intended_entry,
            )
            return result

        entry_price = fill_result['avg_price']

        # Guard: entry_price must be positive
        if entry_price <= 0:
            logger.critical(
                f'[fill-poll] {instrument} order {order_id} filled but avg_price={entry_price} '
                f'-- refusing to write trade without valid entry price'
            )
            self._log_execution_failure(
                approval, 'invalid_fill_price',
                f'fill status={fill_result["status"]} avg_price={entry_price}',
                intended_entry=intended_entry,
            )
            return result

        # Guard: stop_distance must be present (propagated by Stage 1)
        stop_distance = payload.get('stop_distance')
        if stop_distance is None:
            logger.critical(
                f'[fill-poll] {instrument}: stop_distance missing from approval '
                f'-- cannot write trade safely'
            )
            self._log_execution_failure(
                approval, 'stop_distance_missing',
                'stop_distance not present in approval payload after fill',
                intended_entry=intended_entry,
            )
            return result

        # Compute absolute stop price
        pip_precision = 3 if instrument.upper().endswith('JPY') else 5
        if direction == 'long':
            stop_price = round(entry_price - stop_distance, pip_precision)
        else:
            stop_price = round(entry_price + stop_distance, pip_precision)

        # Propagate fill price and stop price into details so _place_stop_order can use them
        result["details"]["entry_price"] = entry_price
        result["details"]["stop_price"] = stop_price

        # Broker-aware fill quality computation
        _is_ig = type(self.broker).__name__.lower().startswith("ig")
        if _is_ig:
            # IG market orders always fill 100% at market
            _fill_pct = 100.0
            _pip_size = 0.01 if instrument.upper().endswith("JPY") else 0.0001
            _requested = payload.get("current_price") or entry_price
            _slippage_pips = round(abs(entry_price - _requested) / _pip_size, 1)
        else:
            # IBKR: use filled_qty from fill_result, slippagePips from response
            _filled_qty = fill_result.get("filled_qty", fill_result.get("cumFill", varied_size))
            _fill_pct = (_filled_qty / varied_size * 100.0) if varied_size else 100.0
            _slippage_pips = response.get("slippagePips", 0)

        # Write trade to RDS
        # FIX: position_size stores actual IG lot size, not raw notional units
        _lot_size = float(response.get("size") or varied_size)
        _entry_context_dict = payload.get("entry_context") or {}
        _agents_agreed = _entry_context_dict.get("agents_agreed")
        # Derive session from UTC hour — consistent with learning module _get_session_at_time.
        # get_market_state().get('state') returns 'active'/'quiet', not a session name.
        _now_h = datetime.datetime.now(datetime.timezone.utc).hour
        if 0 <= _now_h < 7:
            _session_at_entry = 'asian'
        elif 7 <= _now_h < 12:
            _session_at_entry = 'london'
        elif 12 <= _now_h < 16:
            _session_at_entry = 'overlap'
        elif 16 <= _now_h < 20:
            _session_at_entry = 'newyork'
        elif 20 <= _now_h < 22:
            _session_at_entry = 'ny_close'
        else:
            _session_at_entry = 'off_hours'

        trade_id = self._write_trade(
            instrument=instrument,
            direction=direction,
            entry_price=entry_price,
            position_size=_lot_size,
            stop_distance=result["details"].get("stop_distance_randomised"),
            spread_at_entry=spread_pips,
            fill_pct=_fill_pct,
            slippage_pips=_slippage_pips,
            fill_time_ms=fill_time_ms,
            payload=payload,
            entry_context=payload.get("entry_context"),
            order_id=order_id,
            convergence_score=approval.get("payload", {}).get("convergence"),
            target_price=payload.get("target_price"),
            requested_size=varied_size,
            session_at_entry=_session_at_entry,
            agents_agreed=_agents_agreed,
            entry_rank_position=payload.get("entry_rank_position"),
            trade_parameters=self._build_trade_parameters_helper(payload),
        )

        if trade_id is None:
            # IBKR filled the order but DB write failed -- ghost trade risk
            logger.critical(
                f"TRADE FILLED BUT NOT RECORDED: {instrument} {direction} "
                f"entry={entry_price} size={varied_size}"
            )
            self._log_execution_failure(
                approval, 'db_write_failure',
                f'IBKR order filled but trades table INSERT failed -- '
                f'entry={entry_price} size={varied_size}',
                intended_entry=intended_entry,
            )

        if trade_id:
            result["executed"] = True
            result["trade_id"] = trade_id

            # Place stop order (entry_price and stop_price already in result["details"])
            self._place_stop_order(trade_id, instrument, direction, result["details"])

            # FIX 3: fill-quality events for dashboard
            _req_price = float(payload.get("current_price") or entry_price or 0)
            _pip_sz    = 0.01 if instrument.upper().endswith("JPY") else 0.0001
            _slippage  = round(abs(entry_price - _req_price) / _pip_sz, 1) if _req_price else 0
            if trade_id:
                self.events.write_order_event(trade_id, "order_placed", {
                    "instrument":      instrument,
                    "direction":       direction,
                    "requested_size":  varied_size,
                    "requested_price": entry_price,
                    "ibkr_order_id":   order_id,
                })
                self.events.write_order_event(trade_id, "order_filled", {
                    "filled_price":    entry_price,
                    "filled_size":     varied_size,
                    "requested_price": _req_price,
                })


            # Audit: trade executed
            try:
                size = int(varied_size)
                stop_d = result["details"].get("stop_distance_randomised")
                self.events.write_audit_log(
                    "trade_executed",
                    f"{instrument} {direction} executed — entry {entry_price:.5g}, "
                    f"size {size:,} units"
                    + (f", stop dist {stop_d:.5g}" if stop_d else ""),
                    {"instrument": instrument, "direction": direction,
                     "entry_price": entry_price, "position_size": size,
                     "stop_distance": stop_d,
                     "order_type": result["details"].get("order_type"),
                     "fill_time_ms": fill_time_ms, "trade_id": trade_id},
                )
            except Exception as _ae:
                logger.warning(f"trade_executed audit write failed: {_ae}")

            # C. Trade executed alert
            try:
                send_alert(
                    'INFO',
                    f'Trade executed: {instrument} {direction}',
                    {'pair': instrument, 'direction': direction,
                     'entry': entry_price,
                     'stop': result["details"].get("stop_price"),
                     'size': int(varied_size),
                     'fill_time_ms': fill_time_ms,
                     'trade_id': trade_id,
                     'convergence': approval.get("convergence", 0)},
                    source_agent='execution',
                )
            except Exception as _ale:
                logger.warning(f"trade_executed alert failed: {_ale}")

        return result

    def _build_order(self, instrument: str, direction: str, size: float,
                     use_limit: bool, stop_price: float = None,
                     current_price: float = None, risk_amount_gbp: float = None,
                     stop_distance: float = None) -> Dict[str, Any]:
        """Build generic broker order payload."""
        side = "BUY" if direction == "long" else "SELL"
        order = {
            "instrument":     instrument,
            "direction":      direction,
            "size":           size,
            "side":           side,           # legacy compat
            "orderType":      "MKT",
            "stop_price":     stop_price,
            "current_price":  current_price,
            "risk_amount_gbp": risk_amount_gbp,
            "stop_distance":  stop_distance,
        }
        if use_limit:
            try:
                quote = self.broker.get_live_quote(instrument)
                pip_precision = 3 if instrument.upper().endswith('JPY') else 5
                lmt_price = round(quote['ask'] if side == 'BUY' else quote['bid'], pip_precision)
                order['price'] = lmt_price
                order['orderType'] = 'LMT'
                if current_price is None:
                    order['current_price'] = quote.get('mid')
            except Exception as e:
                logger.warning(
                    f'Live quote fetch failed for {instrument}: {e} '
                    f'— falling back to MKT order'
                )
        return order

    def _get_conid(self, instrument: str) -> int:
        """Deprecated: conid is now broker-internal. Returns 0."""
        logger.debug(f'_get_conid called for {instrument} — no-op (broker-managed)')
        return 0

    def _prewarm_conid_cache(self):
        """Deprecated: broker manages instrument resolution internally."""
        logger.debug('_prewarm_conid_cache: no-op (broker-managed)')

    def _is_hard_rejection(self, response: Dict) -> bool:
        """Distinguish soft vs hard rejections. IBKR 200-299 = hard, 300-399 = soft."""
        status = response.get("status", 0)
        error = str(response.get("error", "")).lower()
        hard_keywords = ["margin", "position limit", "not tradeable", "suspended", "insufficient",
                         "currency not supported", "currency not supported for this instrument"]
        if any(kw in error for kw in hard_keywords):
            return True
        if 200 <= status < 300:
            return True
        return False

    def _handle_hard_rejection(self, instrument: str, direction: str, response: Dict):
        """Handle permanent rejection — no retry."""
        logger.error(f"HARD REJECTION: {instrument} {direction} — {response}")
        log_event('TRADE_FAILED', f'{instrument} {direction} rejected by IG: {response.get("error", "unknown")}',
            category='TRADE', agent='execution', severity='CRITICAL', instrument=instrument)
        self.events.write_order_event(None, "rejected_hard", {
            "instrument": instrument, "direction": direction, "rejection_reason": response.get("error", response.get("reason", str(response))),
            "response": str(response),
        })
        self.events.write_system_alert(
            "order_rejected_hard", "critical",
            f"{instrument} {direction} rejected by broker",
            f"Reason: {response.get('error', 'unknown')}. Manual review required.",
        )
        self.events.write_audit_log("order_rejected_hard",
            f"{instrument} {direction} hard rejected", {"response": str(response)})

    def _retry_soft_rejection(self, instrument: str, direction: str,
                               order: Dict, response: Dict) -> bool:
        """Retry soft rejection up to 3 times."""
        for attempt in range(1, SOFT_REJECT_MAX_RETRIES + 1):
            logger.warning(f"Soft rejection, retry {attempt}/{SOFT_REJECT_MAX_RETRIES}")
            self.events.write_order_event(None, "rejected_soft", {
                "instrument": instrument, "attempt": attempt, "rejection_reason": response.get("error", response.get("reason", str(response))),
                "response": str(response),
            }, retry_attempt=attempt)
            time.sleep(SOFT_REJECT_WAIT_SECONDS)

            if not self.account_id:
                raise RuntimeError(
                    'account_id not resolved — startup incomplete, cannot submit order'
                )
            response = self.broker.place_order(order)
            if "error" not in response:
                return True

        # All retries exhausted — escalate to hard
        self._handle_hard_rejection(instrument, direction, response)
        return False

    @staticmethod
    def _build_trade_parameters_helper(payload: Dict) -> Dict:
        """Extract risk parameter snapshot from RG approval payload."""
        _sizing = (payload.get("risk_details") or {}).get("sizing") or {}
        _swap   = (payload.get("risk_details") or {}).get("swap") or {}
        _ectx   = payload.get("entry_context") or {}
        return {
            "convergence_threshold": payload.get("effective_threshold"),
            "convergence_score":     payload.get("convergence"),
            "stress_score":          payload.get("stress_score") or _ectx.get("stress_score"),
            "stress_band":           payload.get("stress_band"),
            "stress_multiplier":     payload.get("stress_size_multiplier"),
            "conviction_exponent":   payload.get("conviction_exponent"),
            "atr_stop_multiplier":   payload.get("atr_stop_multiplier") or _sizing.get("atr_stop_multiplier"),
            "min_rr":               payload.get("min_risk_reward_ratio") or _swap.get("min_rr"),
            "risk_pct":             payload.get("effective_risk_pct") or _sizing.get("effective_risk_pct"),
            "combined_multiplier":  payload.get("combined_mult") or _sizing.get("combined_multiplier"),
        }

    def _write_trade(self, instrument: str, direction: str, entry_price: float,
                     position_size: float, stop_distance: Optional[float],
                     spread_at_entry: float, fill_pct: float,
                     slippage_pips: float, fill_time_ms: int,
                     payload: Dict,
                     entry_context: Optional[Dict] = None,
                     order_id: str = None,
                     convergence_score: Optional[float] = None,
                     target_price: Optional[float] = None,
                     requested_size: Optional[float] = None,
                     session_at_entry: Optional[str] = None,
                     agents_agreed: Optional[str] = None,
                     entry_rank_position: Optional[int] = None,
                     trade_parameters: Optional[Dict] = None) -> Optional[int]:
        """Write trade to RDS trades table."""
        cur = self.db.cursor()
        try:
            stop_price = None
            if stop_distance and entry_price:
                if direction == "long":
                    stop_price = entry_price - stop_distance
                else:
                    stop_price = entry_price + stop_distance

            is_partial = fill_pct < 100.0
            partial_action = None
            if is_partial:
                if fill_pct >= PARTIAL_FILL_ACCEPT_PCT:
                    partial_action = "accepted"
                else:
                    partial_action = "cancelled"

            # Slippage handling
            threshold = SLIPPAGE_THRESHOLDS.get(instrument, 5)
            slippage_action = None
            if slippage_pips > threshold:
                # Recalculate R:R at actual fill — simplified check
                slippage_action = "accepted"  # In production, check if R:R still valid

            _entry_ctx_json = json.dumps(entry_context) if entry_context else None
            cur.execute("""
                INSERT INTO forex_network.trades
                    (user_id, instrument, direction, entry_price, stop_price,
                     entry_time, position_size, position_size_usd, spread_at_entry,
                     requested_size, fill_pct, is_partial_fill,
                     slippage_pips, slippage_action, partial_fill_action,
                     fill_time_ms, entry_context, ibkr_order_id, convergence_score,
                     target_price, session_at_entry, agents_agreed, entry_rank_position,
                     trade_parameters)
                VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                self.user_id, instrument, direction, entry_price, stop_price,
                position_size, position_size, spread_at_entry,
                (requested_size if requested_size is not None else position_size), fill_pct, is_partial,
                slippage_pips, slippage_action, partial_action,
                fill_time_ms, _entry_ctx_json,
                order_id or None,
                convergence_score,
                target_price,
                session_at_entry,
                agents_agreed,
                entry_rank_position,
                json.dumps(trade_parameters) if trade_parameters else None,
            ))
            result = cur.fetchone()
            self.db.commit()
            trade_id = result["id"]
            logger.info(f"Trade written: ID {trade_id}, {instrument} {direction}")
            log_event('TRADE_OPENED', f'{instrument} {direction} {position_size:.2f}lots @ {entry_price}',
                category='TRADE', agent='execution', user_id=str(self.user_id), instrument=instrument,
                payload={'entry_price': entry_price, 'stop_distance': stop_distance,
                         'target': target_price, 'size': position_size, 'convergence': convergence_score})
            return trade_id
        except Exception as e:
            logger.error(f"Trade write failed: {e}")
            self.db.rollback()
            return None
        finally:
            cur.close()

    def _place_stop_order(self, trade_id: int, instrument: str, direction: str,
                          details: Dict[str, Any]):
        """Place stop order and handle failure protocol.

        Computes the absolute stop price from entry_price ± stop_distance so
        that IBKR receives a price-based STP order rather than a distance value.
        IBKR STP orders require an absolute price in the 'auxPrice' field.
        """
        stop_distance = details.get("stop_distance_randomised")
        if not stop_distance:
            logger.warning(f"No stop distance for trade {trade_id} — emergency close")
            self._emergency_close(trade_id, instrument, direction)
            return

        # ── Compute absolute stop price ────────────────────────────────────────
        entry_price = details.get("entry_price") or details.get("avgPrice", 0)
        if not entry_price:
            logger.warning(
                f"No entry price in details for trade {trade_id} — emergency close. "
                f"details keys: {list(details.keys())}"
            )
            self._emergency_close(trade_id, instrument, direction)
            return

        precision = 3 if instrument.upper().endswith("JPY") else 5
        if direction == "long":
            stop_price = round(entry_price - stop_distance, precision)
        else:
            stop_price = round(entry_price + stop_distance, precision)

        logger.info(
            f"Stop price for trade {trade_id}: entry={entry_price} dist={stop_distance} "
            f"→ stop={stop_price} ({direction})"
        )

        # Build stop order — auxPrice is the IBKR field for STP absolute price
        # Delegate stop placement to broker (inline for IG, STP order for IBKR)
        entry_order_id = details.get("order_id") or details.get("dealId", "")
        resp = self.broker.place_stop_order(entry_order_id, stop_price)
        if resp.get("status") in ("inline", "modified", "delegated_to_execute_path"):
            logger.info(f"Stop handled by broker for trade {trade_id}: {resp}")
            return
        stop_order_id = resp.get("stop_order_id", "")
        if stop_order_id:
            cur = self.db.cursor()
            try:
                cur.execute("""
                    UPDATE forex_network.trades
                    SET ibkr_stop_order_id = %s
                    WHERE id = %s
                """, (str(stop_order_id), trade_id))
                self.db.commit()
                logger.info(f"Stop placed for trade {trade_id}: {stop_order_id}")
            except Exception as e:
                logger.error(f"Stop order ID update failed: {e}")
                self.db.rollback()
            finally:
                cur.close()
        else:
            logger.error(f"STOP FAILED — emergency close for trade {trade_id}")
            self._emergency_close(trade_id, instrument, direction)

    def _get_broker_position_size(self, instrument: str) -> float:
        """Return open position size for instrument via broker.

        Returns 0.0 if not found or on error — caller must handle the zero case
        (alert + refuse to place an order, not silent write of zeros).
        """
        try:
            return self.broker.get_position_size(instrument)
        except Exception as e:
            logger.error(f"_get_broker_position_size({instrument}): broker call failed: {e}")
        return 0.0

    def _emergency_close(self, trade_id: int, instrument: str, direction: str):
        """Emergency close when stop cannot be placed.

        Fetches the actual IBKR position quantity so the close order is sized
        correctly.  If no IBKR position is found the trade is marked closed in
        the DB only (position may already be flat).  CRITICAL is logged if the
        close order itself fails — human intervention is required in that case.
        """
        logger.error(f"EMERGENCY CLOSE initiated: trade {trade_id} {instrument} {direction}")

        # ── Step 1: resolve actual quantity from live IBKR positions ──────────
        ibkr_positions = self.broker.get_positions()
        ibkr_qty = None
        ibkr_pos_sign = None  # +1 long, -1 short
        norm_instrument = instrument.replace("/", "").replace(".", "").upper()[:6]
        for pos in ibkr_positions:
            if not isinstance(pos, dict):
                continue
            pos_sym = pos.get("contractDesc", pos.get("symbol", ""))
            pos_norm = pos_sym.replace("/", "").replace(".", "").upper()[:6]
            if pos_norm == norm_instrument:
                raw_qty = pos.get("position", 0)
                try:
                    raw_qty = float(raw_qty)
                except (TypeError, ValueError):
                    raw_qty = 0.0
                if raw_qty != 0:
                    ibkr_qty = int(abs(raw_qty))
                    ibkr_pos_sign = 1 if raw_qty > 0 else -1
                break

        if ibkr_qty is None or ibkr_qty == 0:
            # Position not found on IBKR — may already be closed
            logger.warning(
                f"EMERGENCY CLOSE: no open IBKR position found for {instrument} "
                f"(trade {trade_id}) — marking DB closed without IBKR order"
            )
            self.events.write_system_alert(
                "stop_order_failed", "warning",
                f"Emergency close: no IBKR position for {instrument} (trade {trade_id})",
                "Position was not present on IBKR at emergency close time — "
                "may have already been closed. DB marked closed.",
            )
        else:
            # ── Step 2: derive close side from actual position sign ─────────────
            # Positive position = long → close with SELL; negative = short → BUY
            if ibkr_pos_sign is not None:
                close_side = "SELL" if ibkr_pos_sign > 0 else "BUY"
            else:
                # Fallback to direction param if sign unknown
                close_side = "SELL" if direction == "long" else "BUY"

            # ── Step 3: submit market close order (IOC — must fill immediately) ─
            close_order = {
                "conid": self._get_conid(instrument),
                "orderType": "MKT",
                "side": close_side,
                "quantity": ibkr_qty,
                "tif": "IOC",
            }
            if not self.account_id:
                raise RuntimeError(
                    'account_id not resolved — startup incomplete, cannot submit order'
                )
            response = self.broker.place_order(close_order)
            if "error" in response:
                logger.critical(
                    f"EMERGENCY CLOSE ORDER FAILED for trade {trade_id} {instrument}: "
                    f"{response.get('error')} — HUMAN INTERVENTION REQUIRED. "
                    f"Position may still be open with no stop."
                )
                self.events.write_system_alert(
                    "emergency_close_failed", "critical",
                    f"CRITICAL: Emergency close order FAILED for {instrument} (trade {trade_id})",
                    f"Market close order was rejected by broker: {response.get('error')}. "
                    "Position may still be open with no stop — manual intervention required.",
                )
                self.events.write_order_event(
                    trade_id, "emergency_close_failed",
                    {"instrument": instrument, "direction": direction,
                     "quantity": ibkr_qty, "error": response.get("error")},
                )
                # Do NOT mark DB closed — position is still open
                return
            else:
                logger.error(
                    f"EMERGENCY CLOSE order submitted for {instrument} trade {trade_id}: "
                    f"qty={ibkr_qty} side={close_side} response={response}"
                )

        # ── Step 4: mark trade closed in DB ────────────────────────────────────
        # C4: Capture fill price via poll + live_quote fallback; never write zeros
        em_exit_price = None
        em_exit_price_source = 'emergency_close'
        # Only attempt fill poll if an order was actually submitted (ibkr_qty > 0)
        if ibkr_qty and ibkr_qty > 0:
            try:
                em_exit_price, em_exit_price_source = self._fetch_order_fill_price(
                    response if not response.get('error') else {}, instrument, poll_secs=5
                )
            except Exception as e:
                logger.warning(f"Emergency close fill price lookup failed: {e}")

        # Compute P&L if exit price available
        em_pnl_pips = None
        em_pnl_usd = None
        if em_exit_price is not None:
            try:
                # Need trade record — fetch from DB for entry fields
                _cur2 = self.db.cursor()
                _cur2.execute(
                    "SELECT entry_price, direction, position_size_usd FROM forex_network.trades WHERE id = %s",
                    (trade_id,)
                )
                _trade_row = _cur2.fetchone()
                _cur2.close()
                if _trade_row and _trade_row.get("entry_price") and _trade_row.get("direction"):
                    em_pnl_pips, em_pnl_usd = self._compute_pnl(
                        instrument, _trade_row["direction"],
                        float(_trade_row["entry_price"]),
                        em_exit_price,
                        float(_trade_row.get("position_size_usd") or 0),
                    )
            except Exception as e:
                logger.warning(f"Emergency close P&L calc failed: {e}")

        cur = self.db.cursor()
        try:
            cur.execute("""
                UPDATE forex_network.trades
                SET exit_time         = NOW(),
                    exit_reason       = 'stop_failed',
                    exit_price        = %s,
                    exit_price_source = %s,
                    pnl_pips          = %s,
                    pnl               = %s
                WHERE id = %s AND exit_time IS NULL
            """, (em_exit_price, em_exit_price_source, em_pnl_pips, em_pnl_usd, trade_id,))
            self.db.commit()
        except Exception as e:
            logger.error(f"Emergency close DB update failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

        self.events.write_order_event(
            trade_id, "emergency_close_submitted",
            {"instrument": instrument, "direction": direction,
             "ibkr_quantity": ibkr_qty, "close_side": close_side if ibkr_qty else None},
        )
        self.events.write_system_alert(
            "stop_order_failed", "critical",
            f"Stop order failed for {instrument} — emergency close executed",
            "Open position had no stop. Closed at market per protocol.",
        )
        self.events.write_audit_log(
            "stop_order_failed",
            f"Emergency close: {instrument} trade {trade_id} qty={ibkr_qty}",
            {"instrument": instrument, "direction": direction, "ibkr_qty": ibkr_qty},
        )


# =============================================================================
# EXECUTION AGENT — MAIN
# =============================================================================
class ExecutionAgent:
    """Main execution agent — polls for approved trades, manages positions."""

    CYCLE_INTERVAL_MINUTES = 0.5  # 30s main-loop cycle

    def __init__(self, user_id: str, dry_run: bool = False):
        self.user_id = user_id
        self.dry_run = dry_run
        self.db = DatabaseConnection()
        self.db.connect()
        validate_schema(self.db.conn, EXPECTED_TABLES)
        self.user_id = _resolve_user_id(self.db.conn, user_id)
        logger.info(f"Execution Agent user_id: {self.user_id}")
        self.broker = _build_broker()
        self.events = OrderEventWriter(self.db, user_id)
        self.executor = TradeExecutor(self.db, self.broker, user_id, self.events)
        self.reconciler = ReconciliationEngine(self.db, self.broker, user_id, self.events)
        self.reconciler.agent = self  # C12: back-reference for _handle_trade_closed access
        self.kill_handler = KillSwitchHandler(self.db, self.broker, user_id, self.events)
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        self._kill_switch_was_active = False
        self.broker_account_id = None  # Resolved at startup from broker
        self._prev_ibkr_positions: Dict[str, Any] = {}  # Previous cycle snapshot for close-price capture

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

    def read_pending_approvals(self) -> List[Dict[str, Any]]:
        """Read risk_approved signals not yet processed."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT ON (instrument)
                    id, instrument, bias, confidence, payload, created_at
                FROM forex_network.agent_signals
                WHERE agent_name = 'risk_guardian'
                  AND signal_type = 'risk_approved'
                  AND user_id = %s
                  AND expires_at > NOW()
                  AND id NOT IN (
                      SELECT DISTINCT (payload->>'risk_signal_id')::bigint
                      FROM forex_network.agent_signals
                      WHERE agent_name = 'execution'
                        AND user_id = %s
                        AND payload->>'risk_signal_id' IS NOT NULL
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM forex_network.trades
                      WHERE trades.instrument = agent_signals.instrument
                        AND trades.user_id    = agent_signals.user_id
                        AND trades.exit_time IS NULL
                  )
                ORDER BY instrument, created_at DESC
            """, (self.user_id, self.user_id))
            rows = cur.fetchall()
            results = []
            for row in rows:
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                results.append({
                    "signal_id": row["id"],
                    "instrument": row["instrument"],
                    "bias": row["bias"],
                    "direction": "long" if row["bias"] == "bullish" else "short",
                    "confidence": float(row["confidence"]) if row["confidence"] else 0.0,
                    "payload": payload or {},
                    "created_at": row["created_at"],
                })
            return results
        except Exception as e:
            logger.error(f"Read approvals failed: {e}")
            return []
        finally:
            cur.close()

    def write_execution_signal(self, approval: Dict, result: Dict):
        """Write execution outcome signal."""
        if self.dry_run:
            return
        cur = self.db.cursor()
        try:
            signal_type = "executed" if result["executed"] else "execution_failed"
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES (%s, %s, %s, %s, 0.0, %s, 1.0, %s, NOW() + INTERVAL '20 minutes')
            """, (
                AGENT_NAME, self.user_id, approval["instrument"], signal_type,
                approval["bias"],
                json.dumps({
                    "risk_signal_id": approval["signal_id"],
                    "trade_id": result.get("trade_id"),
                    "details": result.get("details", {}),
                }),
            ))
            self.db.commit()
        except Exception as e:
            logger.error(f"Execution signal write failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def update_account_value(self):
        """Query broker for live account equity and update risk_parameters cache.
        Source of truth: broker (IG or IBKR). Database is a cache for dashboard/fallback.

        Honoured kill-switch: /platform/config/broker-account-value-autoupdate=false
        pins the DB value (used when operator wants a manual paper balance).
        """
        try:
            autoupdate = _ssm_get("/platform/config/broker-account-value-autoupdate", "true").strip().lower()
            if autoupdate in ("false", "0", "no", "off"):
                return
        except Exception:
            pass  # fail open — proceed with broker query

        _t0 = time.time()
        try:
            live_balance, live_currency = self.broker.get_account_value()
        except Exception as e:
            _ms = int((time.time() - _t0) * 1000)
            log_api_call(self.db.conn, BROKER_NAME, '/broker/account_value', 'execution',
                         False, _ms, error_type=str(e)[:80])
            logger.warning(f"Broker account value unavailable — using cached value: {e}")
            return
        _ms = int((time.time() - _t0) * 1000)
        log_api_call(self.db.conn, BROKER_NAME, '/broker/account_value', 'execution',
                     True, _ms)

        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT peak_account_value
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            current_peak = float(row["peak_account_value"] or 0) if row else 0.0

            # Reset peak if stale (> 2x current balance — likely from a prior test session)
            if current_peak > live_balance * 2.0:
                logger.warning(
                    f"peak_account_value {current_peak:,.2f} is >2x current balance "
                    f"{live_balance:,.2f} — resetting peak to current balance"
                )
                new_peak = live_balance
            else:
                new_peak = max(current_peak, live_balance)

            cur.execute("""
                UPDATE forex_network.risk_parameters
                SET account_value = %s,
                    account_value_currency = %s,
                    peak_account_value = %s,
                    updated_at = NOW()
                WHERE user_id = %s
            """, (live_balance, live_currency, new_peak, self.user_id))
            self.db.commit()

            # ── account_history snapshot ──────────────────────────────────────
            try:
                cur2 = self.db.cursor()
                # Compute cumulative realised P&L from closed trades
                cur2.execute("""
                    SELECT COALESCE(SUM(pnl), 0)
                    FROM forex_network.trades
                    WHERE user_id = %s
                      AND exit_time IS NOT NULL
                      AND pnl IS NOT NULL
                """, (self.user_id,))
                row2 = cur2.fetchone()
                realised_pnl_cum = float(row2[0] if not isinstance(row2, dict) else row2.get('coalesce', 0) or 0)
                # Drawdown: max(0, (peak - balance) / peak * 100) if peak > 0
                drawdown_pct = max(0.0, (new_peak - live_balance) / new_peak * 100.0) if new_peak > 0 else 0.0
                cur2.execute("""
                    INSERT INTO forex_network.account_history
                        (snapshot_time, user_id, account_value, peak_account_value,
                         unrealised_pnl, realised_pnl_cumulative, drawdown_pct, source)
                    VALUES (NOW(), %s, %s, %s, 0, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT uq_account_history_user_snapshot DO NOTHING
                """, (self.user_id, live_balance, new_peak,
                      realised_pnl_cum, round(drawdown_pct, 4), 'ig_demo'))
                self.db.commit()
                cur2.close()
            except Exception as _hist_err:
                logger.warning(f"account_history insert failed (non-fatal): {_hist_err}")
                try: self.db.rollback()
                except: pass
            # ── end account_history snapshot ──────────────────────────────────

            logger.info(f"Account value updated: {live_balance:,.2f} {live_currency} (peak: {new_peak:,.2f})")
        except Exception as e:
            logger.error(f"Account value update failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

    def manage_open_positions(self):
        """Per-cycle: manage trailing stops, detect closed trades, check Tier 2 circuit breaker."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT id, instrument, direction, entry_price, stop_price,
                       target_price, regime_at_entry, ibkr_order_id,
                       position_size_usd, position_size, entry_time, ibkr_stop_order_id
                FROM forex_network.trades
                WHERE user_id = %s AND exit_time IS NULL
            """, (self.user_id,))
            open_trades = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Open position scan failed: {e}")
            return
        finally:
            cur.close()

        if not open_trades:
            return

        # Read risk parameters for trailing stop config
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT trailing_stop_pct, daily_loss_limit_pct, account_value
                FROM forex_network.risk_parameters
                WHERE user_id = %s
            """, (self.user_id,))
            params = cur.fetchone()
        except:
            params = None
        finally:
            cur.close()

        if not params:
            return

        trailing_pct = float(params.get("trailing_stop_pct", 0) or 0)
        account_value = float(params.get("account_value", 0) or 0)

        # Get current IBKR positions to check P&L and detect closes
        ibkr_positions = self.broker.get_positions()
        ibkr_instruments = {}
        for pos in ibkr_positions:
            inst = self.reconciler._normalise_instrument(pos)
            ibkr_instruments[inst] = pos

        # Guard: empty response with known open trades means the broker call
        # is suspect (network blip, malformed body). Synthesise ibkr_instruments
        # from DB open trades so the closed-trade check below never fires
        # spuriously and ghost-closes all positions.
        if not ibkr_positions and open_trades:
            logger.warning(
                f'[{self.user_id}] get_positions() returned empty with '
                f'{len(open_trades)} open trade(s) — skipping close detection this cycle'
            )
            ibkr_instruments = {
                t['instrument']: {} for t in open_trades
            }

        # Snapshot from previous cycle used by _handle_trade_closed for close-price capture
        prev_snapshot = dict(self._prev_ibkr_positions)
        self._prev_ibkr_positions = dict(ibkr_instruments)
        current_stress = self._get_current_stress()

        for trade in open_trades:
            instrument = trade["instrument"]

            # ── Detect closed trades (stop hit, target hit, or manual) ────────
            if instrument not in ibkr_instruments:
                # Position no longer in IBKR — it was closed
                self._handle_trade_closed(trade, prev_snapshot)
                continue

            # ── Gap 2: Trailing stop management ───────────────────────────────
            if trailing_pct > 0 and trade.get("entry_price") and trade.get("stop_price"):
                ibkr_pos = ibkr_instruments[instrument]
                unrealised_pnl = float(
                    (ibkr_pos.get("unrealizedPnl") or ibkr_pos.get("unrealPnl") or
                     ibkr_pos.get("unrealized_pnl") or 0)
                    if isinstance(ibkr_pos, dict) else 0
                )
                entry_price = float(trade["entry_price"])
                current_stop = float(trade["stop_price"])
                risk_amount = abs(entry_price - current_stop)

                # Trailing activates once position is 1R in profit
                # C10: Compute 1R in USD using actual IBKR unit count, not position_size_usd
                if risk_amount > 0 and unrealised_pnl > 0:
                    ibkr_units = abs(float(ibkr_pos.get('position', 0) if isinstance(ibkr_pos, dict) else 0))
                    if ibkr_units == 0:
                        continue  # Can't compute 1R without unit count — skip trailing for this trade

                    # 1R = price_distance × units, in quote currency
                    one_r_quote = risk_amount * ibkr_units

                    # Convert to USD
                    instrument_upper = instrument.upper()
                    if instrument_upper.endswith('USD'):
                        one_r_usd = one_r_quote  # Quote is already USD
                    elif instrument_upper.startswith('USD'):
                        # Quote is foreign currency — convert using current rate
                        try:
                            _q = self.broker.get_live_quote(instrument)
                            _rate = float(_q.get('mid') or _q.get('ask') or 0)
                            one_r_usd = one_r_quote / _rate if _rate > 0 else one_r_quote
                        except Exception:
                            one_r_usd = one_r_quote  # Fallback: treat as USD (approximate)
                    else:
                        one_r_usd = one_r_quote  # Cross pair fallback

                    pnl_in_r = unrealised_pnl / one_r_usd if one_r_usd > 0 else 0
                    if pnl_in_r >= 1.0:
                        # Calculate new trailing stop
                        current_price = float(
                            (ibkr_pos.get("mktPrice") or ibkr_pos.get("mkt_price") or
                             ibkr_pos.get("marketPrice") or entry_price)
                            if isinstance(ibkr_pos, dict) else entry_price
                        )
                        trail_distance = current_price * (trailing_pct / 100.0)

                        if trade["direction"] == "long":
                            new_stop = current_price - trail_distance
                            if new_stop > current_stop:
                                self._update_trailing_stop(trade, new_stop)
                        else:
                            new_stop = current_price + trail_distance
                            if new_stop < current_stop:
                                self._update_trailing_stop(trade, new_stop)

            # ── Target price exit
            target_price = trade.get("target_price")
            _regime = (trade.get("regime_at_entry") or "").lower()
            _is_trending = "trend" in _regime
            if target_price and not _is_trending:
                # Trending regime: trailing stop handles exit — ignore fixed target
                # Ranging/transitional/unknown: close at structure target
                _tp_pos = ibkr_instruments.get(instrument, {})
                _current = (
                    _tp_pos.get("mktPrice")
                    or _tp_pos.get("mkt_price")
                    or _tp_pos.get("marketPrice")
                )
                if _current:
                    _current = float(_current)
                    _direction = trade.get("direction", "long")
                    _target_hit = (
                        (_direction == "long"  and _current >= float(target_price)) or
                        (_direction == "short" and _current <= float(target_price))
                    )
                    if _target_hit:
                        logger.info(
                            f"Target hit: {instrument} {_direction} "
                            f"current={_current} target={target_price} regime={_regime}"
                        )
                        self._close_position(
                            trade, ibkr_instruments, close_reason="take_profit"
                        )
                        continue
            elif target_price and _is_trending:
                logger.debug(
                    f"Trending regime — skipping fixed target for {instrument}, "
                    f"trailing stop active"
                )

            # ── Time-based exit (C13) ───────────────────────────────────────────────────
            _entry_t = trade.get("entry_time")
            if _entry_t:
                if _entry_t.tzinfo is None:
                    _entry_t = _entry_t.replace(tzinfo=datetime.timezone.utc)
                hold_days = (datetime.datetime.now(datetime.timezone.utc) - _entry_t).total_seconds() / 86400
                max_hold = ExecutionAgent._get_max_hold_days(
                    trade.get("regime_at_entry"), current_stress
                )
                if hold_days >= max_hold:
                    logger.info(
                        f"Time-based exit: {trade['instrument']} held {hold_days:.1f}d "
                        f"(max {max_hold}d, regime={trade.get('regime_at_entry')}, "
                        f"stress={current_stress:.0f})"
                    )
                    self._close_position(trade, ibkr_instruments, close_reason="max_hold_exceeded")
                    continue

        # ── Gap 3: Circuit Breaker Tier 2 ─────────────────────────────────────
        self._check_circuit_breaker_tier2(params, open_trades)

    def _update_trailing_stop_ibkr(self, trade: Dict, new_stop_price: float) -> bool:
        """Send a stop-price modification to the broker for an existing stop order.

        Returns True on success, False if the modification was skipped or failed.
        Designed to be safe to call speculatively — logs but does not raise.
        """
        order_id = trade.get("ibkr_stop_order_id")
        if not order_id:
            logger.warning(
                f"No stop order ID for trade {trade['id']} — cannot modify via broker"
            )
            return False
        if not self.broker_account_id:
            logger.warning(
                f"broker_account_id not set — cannot modify stop for trade {trade['id']}"
            )
            return False
        try:
            resp = self.broker.modify_order(
                account_id=self.broker_account_id,
                order_id=str(order_id),
                modifications={"auxPrice": new_stop_price},
            )
            if "error" in resp:
                logger.error(
                    f"Stop modification failed for trade {trade['id']}: "
                    f"{resp.get('error')}"
                )
                return False
            logger.info(
                f"Stop modified for trade {trade['id']}: "
                f"new stop = {new_stop_price}"
            )
            return True
        except Exception as e:
            logger.error(f"Stop modification failed for trade {trade['id']}: {e}")
            return False

    @staticmethod
    def _get_max_hold_days(regime, stress_score: float) -> int:
        """Research-backed maximum holding periods by regime and stress level."""
        if stress_score > 50:
            return 2
        if regime and "trend" in str(regime).lower():
            return 5
        return 3  # ranging, transitional, unknown, or NULL

    def _close_position(self, trade: Dict, ibkr_instruments: Dict,
                        close_reason: str = "time_exit") -> None:
        """Close a broker position and write exit record to DB."""
        instrument = trade["instrument"]
        trade_id   = trade["id"]
        deal_id    = trade.get("ibkr_order_id", "")
        direction  = trade["direction"]

        ig_pos   = ibkr_instruments.get(instrument) or {}
        pos_size = float(ig_pos.get("size", 0)) if isinstance(ig_pos, dict) else 0.0
        if pos_size <= 0:
            logger.warning(f"_close_position: no broker size for {instrument} trade {trade_id} — skipping")
            return

        # Pre-close quote — fallback only if IG confirm returns no fill price
        pre_close_price = None
        try:
            q = self.broker.get_live_quote(instrument)
            raw = q.get("bid") if direction == "long" else (q.get("ask") or q.get("bid"))
            if raw is not None:
                pre_close_price = float(raw)
        except Exception as _e:
            logger.warning(f"_close_position: quote failed for {instrument}: {_e}")

        try:
            _close_dir = "SELL" if direction == "long" else "BUY"
            result = self.broker.close_position(deal_id, pos_size, _close_dir)
        except Exception as _e:
            logger.error(f"_close_position broker call failed for trade {trade_id}: {_e}")
            return

        if result.get("error") or result.get("status") == "rejected":
            logger.error(
                f"_close_position rejected for trade {trade_id}: "
                f"{result.get('error') or result.get('status')}"
            )
            return

        # Prefer IG-confirmed fill price from /confirms; fall back to pre-close quote
        confirmed_fill = result.get("fill_price")
        if confirmed_fill and float(confirmed_fill) > 0:
            exit_price        = float(confirmed_fill)
            exit_price_source = "ig_confirm"
        else:
            exit_price        = pre_close_price
            exit_price_source = "pre_close_quote"
            logger.warning(
                f"_close_position: no confirmed fill price for trade {trade_id} — "
                f"falling back to pre-close quote"
            )

        # IG-confirmed P&L in account currency (from /confirms profitAndLoss field)
        ig_profit = result.get("profit")
        pnl_usd   = float(ig_profit) if ig_profit is not None else None

        # Pip P&L computed from confirmed fill price (IG does not return pips)
        pnl_pips = None
        if exit_price and trade.get("entry_price"):
            pnl_pips, _ = self._compute_pnl(
                instrument, direction,
                float(trade["entry_price"]), exit_price,
                float(trade.get("position_size_usd") or 0),
            )

        logger.info(
            f"Close confirmed: {instrument} trade {trade_id} reason={close_reason} "
            f"fill={exit_price} ({exit_price_source}) pnl={pnl_usd} pips={pnl_pips}"
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        cur = self.db.cursor()
        try:
            cur.execute("""
                UPDATE forex_network.trades
                SET exit_time = %s, exit_reason = %s,
                    exit_price = %s, exit_price_source = %s,
                    pnl = %s, pnl_pips = %s
                WHERE id = %s AND exit_time IS NULL
            """, (now, close_reason, exit_price, exit_price_source, pnl_usd, pnl_pips, trade_id))
            self.db.commit()
            logger.info(f"Trade {trade_id} exit written: {close_reason} @ {exit_price} pnl={pnl_usd}")
        except Exception as _e:
            logger.error(f"_close_position DB write failed for trade {trade_id}: {_e}")
            self.db.rollback()
        finally:
            cur.close()

        self.events.write_order_event(trade_id, close_reason, {
            "instrument": instrument, "exit_price": exit_price,
            "exit_price_source": exit_price_source,
            "pnl": pnl_usd, "pnl_pips": pnl_pips,
            "reason": close_reason,
        })

    def _update_trailing_stop(self, trade: Dict, new_stop: float):
        """Move trailing stop to new level on IBKR and in RDS."""
        trade_id = trade["id"]
        instrument = trade["instrument"]
        old_stop = float(trade["stop_price"])

        # Modify broker stop order — use dedicated helper for clean error handling
        ibkr_updated = self._update_trailing_stop_ibkr(trade, new_stop)
        if not ibkr_updated and trade.get("ibkr_stop_order_id"):
            # Log that the DB update will proceed even though IBKR wasn't updated.
            # The next reconciliation cycle will detect the mismatch.
            logger.warning(
                f"Broker stop not updated for trade {trade_id} — "
                f"DB will be updated anyway; reconciliation will re-sync."
            )

        # Update RDS
        cur = self.db.cursor()
        try:
            cur.execute("""
                UPDATE forex_network.trades
                    SET stop_price = %s, trailing_stop_active = TRUE
                    WHERE id = %s
            """, (new_stop, trade_id))
            self.db.commit()
        except Exception as e:
            logger.error(f"Trailing stop DB update failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

        self.events.write_order_event(trade_id, "trailing_stop_moved", {
            "instrument": instrument, "old_stop": old_stop,
            "new_stop": new_stop, "direction": trade["direction"],
        })
        logger.info(f"Trailing stop moved: {instrument} {old_stop:.5f} → {new_stop:.5f}")

    def _fetch_order_fill_price(self, order_response: Dict, instrument: str,
                               poll_secs: int = 5) -> tuple:
        """Poll get_order_status for a fill price after place_order.

        Returns (fill_price_or_None, source_str).
        source_str is 'ibkr_fill', 'live_quote', or 'unavailable'.
        Never writes a zero price — returns None if no valid price found.
        """
        fill_price = None
        source = 'unavailable'
        order_id = order_response.get('orderId') or order_response.get('order_id')

        # Poll for fill
        if order_id:
            deadline = time.time() + poll_secs
            while time.time() < deadline:
                try:
                    status = self.broker.get_order_status(order_id)
                    status_str = (status.get('order_status') or status.get('status') or '').lower()
                    if status_str in ('filled', 'fill'):
                        raw_fill = (status.get('avgPrice') or status.get('avg_price')
                                    or status.get('average_price') or 0)
                        try:
                            candidate = float(raw_fill)
                            if candidate > 0:
                                fill_price = candidate
                                source = 'ibkr_fill'
                                break
                        except (TypeError, ValueError):
                            pass
                except Exception as e:
                    logger.warning(f"_fetch_order_fill_price poll error: {e}")
                time.sleep(1)

        # Fallback: live mid quote
        if fill_price is None:
            try:
                quote = self.broker.get_live_quote(instrument)
                mid = quote.get('mid') or quote.get('ask') or quote.get('bid')
                if mid:
                    candidate = float(mid)
                    if candidate > 0:
                        fill_price = candidate
                        source = 'live_quote'
            except Exception as e:
                logger.warning(f"_fetch_order_fill_price live_quote fallback error: {e}")

        if fill_price is None:
            logger.warning(
                f"[fill_price] {instrument}: no fill price available from IBKR or live quote — "
                "exit_price will be NULL (fail-safe: refusing to write zero)"
            )

        return fill_price, source

    @staticmethod
    def _compute_pnl(instrument: str, direction: str, entry_price: float,
                     exit_price: float, position_size_usd: float):
        """
        Compute (pnl_pips, pnl_usd) for a closed FX trade.

        Pair families handled:
          XXX/USD (EURUSD, GBPUSD, AUDUSD, NZDUSD) — quote is USD
          USD/XXX (USDJPY, USDCHF, USDCAD)          — base is USD

        Returns (None, None) if any required input is missing/zero.
        """
        if not entry_price or not exit_price or not position_size_usd:
            return None, None

        pip_size = 0.01 if instrument.upper().endswith('JPY') else 0.0001
        direction_sign = 1 if direction in ('long', 'buy', 1) else -1

        price_move = float(exit_price) - float(entry_price)
        signed_move = price_move * direction_sign          # positive = profit
        pnl_pips = round(signed_move / pip_size, 1)

        size = float(position_size_usd)
        ep   = float(entry_price)
        xp   = float(exit_price)

        if instrument.upper().endswith('USD'):
            # Quote is USD: pip value = pip_size * (size / entry_price) USD
            pnl_usd = signed_move * (size / ep)
        elif instrument.upper().startswith('USD'):
            # Base is USD: P&L accrues in quote currency, convert back via exit_price
            pnl_usd = signed_move * size / xp
        else:
            # Cross pair — not currently traded; return None for pnl_usd
            pnl_usd = None

        return pnl_pips, (round(pnl_usd, 2) if pnl_usd is not None else None)

    def _handle_trade_closed(self, trade: Dict, prev_snapshot: Optional[Dict] = None,
                             close_cause: str = 'stop_hit'):
        """Gap 4: Detect trade closure, write exit data, P&L, and swap costs."""
        trade_id = trade["id"]
        instrument = trade["instrument"]

        # ── Exit price capture — priority: stop_fill > position_snapshot > warning ──
        exit_price = None
        exit_price_source = 'unknown'

        # C8: First try the actual stop order fill price from IBKR
        stop_order_id = trade.get('ibkr_stop_order_id')
        if stop_order_id and self.broker_account_id:
            try:
                stop_status = self.broker.get_order_status(stop_order_id)
                status_str = (stop_status.get('order_status') or stop_status.get('status') or '').lower()
                if status_str in ('filled', 'fill'):
                    raw_fill = (stop_status.get('avgPrice') or stop_status.get('avg_price')
                                or stop_status.get('average_price') or 0)
                    try:
                        candidate = float(raw_fill)
                        if candidate > 0:
                            exit_price = candidate
                            exit_price_source = 'stop_fill'
                    except (TypeError, ValueError):
                        pass
            except Exception as e:
                logger.warning(f'[close] {instrument}: stop fill price lookup failed: {e}')

        # Fallback: last-known market price from previous cycle snapshot.
        # IG returns 'current_price' (mapped from bid/offer); IBKR returns 'mktPrice'.
        if exit_price is None:
            prev_pos = (prev_snapshot or {}).get(instrument, {})
            if isinstance(prev_pos, dict):
                raw = (prev_pos.get("mktPrice") or prev_pos.get("mkt_price")
                       or prev_pos.get("marketPrice") or prev_pos.get("current_price"))
                if raw is not None:
                    try:
                        candidate = float(raw)
                        if candidate > 0:
                            exit_price = candidate
                            exit_price_source = 'position_snapshot'
                    except (TypeError, ValueError):
                        pass

        # Final fallback for stop_hit: use recorded stop level as best approximation.
        # IG inline stops fill at or near this level; more accurate than leaving NULL.
        if exit_price is None and close_cause == 'stop_hit':
            raw_stop = trade.get('stop_price')
            if raw_stop is not None:
                try:
                    candidate = float(raw_stop)
                    if candidate > 0:
                        exit_price = candidate
                        exit_price_source = 'stop_price_approx'
                        logger.warning(
                            f"[close] {instrument}: snapshot had no usable price — "
                            f"using stop_price={exit_price} as exit_price approx"
                        )
                except (TypeError, ValueError):
                    pass

        if exit_price is None:
            logger.warning(
                f"[close] {instrument}: no mktPrice in prev_snapshot — "
                f"exit_price will be NULL (first cycle after restart or snapshot miss)"
            )
            log_event('SNAPSHOT_MISS', f'{instrument} closed with no snapshot — exit_price will be NULL',
                category='TRADE', agent='execution', severity='WARN', instrument=instrument,
                payload={'trade_id': trade_id})
            warn("execution_agent", "SNAPSHOT_MISS", "No snapshot at close — exit_price NULL",
                 trade_id=trade_id, instrument=instrument)

        # ── P&L — primary: IG transaction history (confirmed account currency) ──
        # _compute_pnl cannot be used here: position_size_usd stores the IG deal
        # size (e.g. 8.3 lots), not a USD amount, making pnl_usd wrong by ~6000x.
        # _close_position() uses IG's profit field directly; we replicate that by
        # fetching /history/transactions and matching on size + open_level.
        pnl_pips     = None
        pnl_usd      = None   # stored in account currency (GBP); variable name is legacy
        ig_txn_match = None

        try:
            transactions  = self.broker.get_recent_transactions(hours=4)
            position_size = float(trade.get("position_size") or 0)
            entry_price_f = float(trade.get("entry_price")   or 0)
            for t in transactions:
                t_size = t.get("size")       or 0
                t_open = t.get("open_level") or 0
                if (t_size > 0 and abs(t_size - position_size) < 0.01
                        and t_open > 0 and abs(t_open - entry_price_f) < 0.00005):
                    ig_txn_match = t
                    break
        except Exception as _e:
            logger.warning(f"[close] {instrument}: IG transaction lookup failed: {_e}")

        if ig_txn_match:
            ig_close = ig_txn_match.get("close_level")
            if ig_close and ig_close > 0:
                exit_price        = ig_close
                exit_price_source = "ig_transaction"
            pnl_usd = ig_txn_match.get("profit_and_loss")
            logger.info(
                f"[close] {instrument}: IG transaction matched "
                f"(close={exit_price}, pnl=£{pnl_usd}, ref={ig_txn_match.get('reference')})"
            )
        else:
            logger.warning(
                f"[close] {instrument} trade {trade_id}: no matching IG transaction "
                f"(size={trade.get('position_size')}, entry={trade.get('entry_price')}) "
                f"— pnl will be NULL, manual backfill needed"
            )

        # pnl_pips: pip move from final exit_price (IG does not return pips)
        if exit_price is not None and trade.get("entry_price") and trade.get("direction"):
            _pip_size      = 0.01 if instrument.upper().endswith('JPY') else 0.0001
            _dir_sign      = 1 if trade["direction"] == "long" else -1
            pnl_pips       = round(
                (exit_price - float(trade["entry_price"])) * _dir_sign / _pip_size, 1
            )

        # ── Hold duration ────────────────────────────────────────────────────────
        entry_time = trade.get("entry_time")
        now = datetime.datetime.now(datetime.timezone.utc)
        hold_days = 0
        if entry_time:
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=datetime.timezone.utc)
            hold_days = (now - entry_time).total_seconds() / 86400

        # ── Swap cost ────────────────────────────────────────────────────────────
        swap_cost_pips = 0
        swap_cost_usd = 0
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT long_rate_pips, short_rate_pips FROM forex_network.swap_rates
                WHERE instrument = %s ORDER BY rate_date DESC, created_at DESC LIMIT 1
            """, (instrument,))
            swap_row = cur.fetchone()
            if swap_row and hold_days > 0:
                direction = trade["direction"]
                swap_rate = float(swap_row["long_rate_pips"] if direction == "long" else swap_row["short_rate_pips"])
                swap_cost_pips = abs(swap_rate) * hold_days
                # C11: Correct swap cost formula — use pip size and base units, then convert to USD
                _pip_size = 0.01 if instrument.upper().endswith('JPY') else 0.0001
                # Units in base currency: position_size_usd / entry_price (approx)
                _entry = float(trade.get("entry_price", 1) or 1)
                _base_units = float(trade.get("position_size_usd", 0)) / _entry if _entry > 0 else 0
                _swap_quote = swap_cost_pips * _pip_size * _base_units
                # Convert quote currency to USD
                _quote_ccy = instrument.upper()[-3:]
                if _quote_ccy == 'USD':
                    swap_cost_usd = _swap_quote
                elif _quote_ccy == 'JPY':
                    try:
                        _q = self.broker.get_live_quote('USDJPY')
                        _rate = float(_q.get('mid') or _q.get('ask') or 150)
                        swap_cost_usd = _swap_quote / _rate
                    except Exception:
                        swap_cost_usd = _swap_quote / 150.0
                else:
                    # CHF, CAD, AUD, NZD — approximate with quoted-vs-USD rate
                    try:
                        _q = self.broker.get_live_quote(f'{_quote_ccy}USD')
                        _rate = float(_q.get('mid') or _q.get('bid') or 1)
                        swap_cost_usd = _swap_quote * _rate
                    except Exception:
                        swap_cost_usd = _swap_quote

            # ── Write all close fields ───────────────────────────────────────────
            cur.execute("""
                UPDATE forex_network.trades
                SET exit_time        = NOW(),
                    exit_price       = %s,
                    exit_price_source = %s,
                    exit_reason      = %s,
                    pnl_pips         = %s,
                    pnl              = %s,
                    swap_cost_pips   = %s,
                    swap_cost_usd    = %s,
                    hold_days        = %s
                WHERE id = %s AND exit_time IS NULL
            """, (
                exit_price,
                exit_price_source,
                close_cause,
                pnl_pips,
                pnl_usd,
                round(swap_cost_pips, 2),
                round(swap_cost_usd, 2),
                round(hold_days, 2),
                trade_id,
            ))
            self.db.commit()
            logger.info(
                f"Trade closed: {instrument} exit={exit_price} src={exit_price_source} "
                f"pnl={pnl_pips}pips/${pnl_usd} "
                f"(hold: {hold_days:.1f}d, swap: {swap_cost_pips:.1f}pips/${swap_cost_usd:.2f})"
            )
            log_event('TRADE_CLOSED', f'{instrument} {close_cause} @ {exit_price} pnl={pnl_usd}',
                category='TRADE', agent='execution', user_id=str(self.user_id), instrument=instrument,
                payload={'exit_price': exit_price, 'pnl': pnl_usd, 'exit_reason': close_cause,
                         'pips': pnl_pips, 'trade_id': trade_id})
        except Exception as e:
            logger.error(f"Trade close handling failed: {e}")
            self.db.rollback()
        finally:
            cur.close()

        self.events.write_order_event(trade_id, "trade_closed", {
            "instrument": instrument,
            "exit_price": exit_price,
            "exit_price_source": exit_price_source,
            "pnl_pips": pnl_pips,
            "pnl_usd": pnl_usd,
            "hold_days": round(hold_days, 2),
            "swap_cost_pips": round(swap_cost_pips, 2),
            "swap_cost_usd": round(swap_cost_usd, 2),
        })
        try:
            hold_h = round(hold_days * 24, 1)
            self.events.write_audit_log(
                "trade_closed",
                f"{instrument} closed {exit_price_source} @ {exit_price} — "
                f"pnl {pnl_pips}pips (${pnl_usd}), "
                f"hold {hold_h}h, swap {swap_cost_pips:.1f}pips (${swap_cost_usd:.2f})",
                {"instrument": instrument, "exit_price": exit_price,
                 "exit_price_source": exit_price_source,
                 "pnl_pips": pnl_pips, "pnl_usd": pnl_usd,
                 "hold_hours": hold_h,
                 "swap_cost_pips": round(swap_cost_pips, 2),
                 "swap_cost_usd": round(swap_cost_usd, 2), "trade_id": trade_id},
            )
        except Exception as _ae:
            logger.warning(f"trade_closed audit write failed: {_ae}")

    def _check_circuit_breaker_tier2(self, params: Dict, open_trades: List[Dict]):
        """Circuit Breaker Tier 2 — refined protocol per professional FX consensus.
        Step 1: Close all LOSING positions at market
        Step 2: Tighten stops on WINNING positions to lock in breakeven or better
        Step 3: Recalculate — if still in breach, close winners starting with smallest profit
        Step 4: Set circuit_breaker_active = TRUE, block all new entries
        Rationale: Winning positions aren't causing the problem — preserve them where possible."""
        daily_limit = float(params.get("daily_loss_limit_pct", 3.0) or 3.0)
        account_value = float(params.get("account_value", 0) or 0)
        hard_limit_pct = daily_limit * 1.5  # 1.5× multiplier — fires at 6/9/12% for 4/6/8% daily limits

        if account_value <= 0:
            return

        # Calculate today's realised P&L
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT COALESCE(SUM(pnl), 0) AS realised
                FROM forex_network.trades
                WHERE user_id = %s AND exit_time IS NOT NULL
                  AND exit_time >= CURRENT_DATE
            """, (self.user_id,))
            realised = float(cur.fetchone()["realised"])
        except:
            realised = 0
        finally:
            cur.close()

        # Get unrealised P&L and actual position units per position from IBKR
        ibkr_positions = self.broker.get_positions()
        position_pnl = {}
        position_units = {}  # C2/C5c: actual IBKR unit counts keyed by normalised instrument
        total_unrealised = 0
        for pos in ibkr_positions:
            if isinstance(pos, dict):
                inst = self.reconciler._normalise_instrument(pos)
                pnl = float(pos.get("unrealizedPnl") or pos.get("unrealPnl") or pos.get("unrealized_pnl") or 0)
                position_pnl[inst] = pnl
                total_unrealised += pnl
                try:
                    raw_qty = pos.get("position", 0)
                    position_units[inst] = int(abs(float(raw_qty)))
                except (TypeError, ValueError):
                    position_units[inst] = 0

        total_daily_pnl = realised + total_unrealised
        daily_loss_pct = abs(total_daily_pnl) / account_value * 100 if total_daily_pnl < 0 else 0

        if daily_loss_pct < hard_limit_pct:
            return  # Not in breach

        logger.error(
            f"CIRCUIT BREAKER TIER 2: Daily loss {daily_loss_pct:.1f}% >= "
            f"hard limit {hard_limit_pct:.1f}%"
        )

        # Classify positions into losers and winners
        losers = []
        winners = []
        for trade in open_trades:
            inst = trade["instrument"]
            unrealised = position_pnl.get(inst, 0)
            trade["_unrealised"] = unrealised
            if unrealised < 0:
                losers.append(trade)
            else:
                winners.append(trade)

        # Sort winners by profit ascending (close smallest profit first if needed)
        winners.sort(key=lambda t: t["_unrealised"])

        closed_count = 0
        tightened_count = 0

        # Step 1: Close all LOSING positions at market
        if not self.broker_account_id:
            raise RuntimeError(
                'account_id not resolved — startup incomplete, cannot submit order'
            )
        for trade in losers:
            logger.info(f"CB Tier 2: Closing loser {trade['instrument']} (unrealised: £{trade['_unrealised']:.2f})")
            close_side = "SELL" if trade["direction"] == "long" else "BUY"
            # C2/C5c: use actual IBKR unit count, not nominal USD size
            _cb_norm = trade["instrument"].replace("/", "").replace(".", "").upper()[:6]
            _cb_qty = position_units.get(_cb_norm, 0)
            if _cb_qty == 0:
                logger.error(
                    f"CB Tier 2: IBKR position units = 0 for {trade['instrument']} "
                    "(fail-safe: skipping close order, may already be flat)"
                )
                self.events.write_system_alert(
                    "cb_close_quantity_zero", "critical",
                    f"CB Tier 2 close skipped for {trade['instrument']} (trade {trade['id']}): IBKR unit count = 0",
                    "Position may already be flat or IBKR data unavailable.",
                )
            else:
                _cb_resp = self.broker.place_order(self.broker_account_id, {
                    "conid": self.executor._get_conid(trade["instrument"]),
                    "orderType": "MKT",
                    "side": close_side,
                    "quantity": _cb_qty,
                    "tif": "GTC",
                })
                # C4/C5b: capture fill price — never write zeros
                _cb_exit_price = None
                _cb_exit_src = 'circuit_breaker'
                try:
                    _cb_exit_price, _cb_exit_src = self.executor._fetch_order_fill_price(
                        _cb_resp, trade["instrument"], poll_secs=5
                    )
                except Exception as _e:
                    logger.warning(f"CB loser fill price lookup failed for {trade['instrument']}: {_e}")

                # P&L
                _cb_pnl_pips = None
                _cb_pnl_usd = None
                if _cb_exit_price is not None and trade.get("entry_price") and trade.get("direction"):
                    try:
                        _cb_pnl_pips, _cb_pnl_usd = self._compute_pnl(
                            trade["instrument"], trade["direction"],
                            float(trade["entry_price"]), _cb_exit_price,
                            float(trade.get("position_size_usd") or 0),
                        )
                    except Exception:
                        pass

                # Hold days
                _cb_hold = 0
                try:
                    import datetime as _dt2
                    _ent = trade.get("entry_time")
                    if _ent:
                        _now2 = _dt2.datetime.now(_dt2.timezone.utc)
                        if _ent.tzinfo is None:
                            _ent = _ent.replace(tzinfo=_dt2.timezone.utc)
                        _cb_hold = (_now2 - _ent).total_seconds() / 86400
                except Exception:
                    pass

                cur = self.db.cursor()
                try:
                    cur.execute("""
                        UPDATE forex_network.trades
                        SET exit_time = NOW(), exit_reason = 'circuit_breaker',
                            exit_price = %s, exit_price_source = %s,
                            pnl_pips = %s, pnl = %s, hold_days = %s
                        WHERE id = %s AND exit_time IS NULL
                    """, (_cb_exit_price, _cb_exit_src, _cb_pnl_pips, _cb_pnl_usd,
                          round(_cb_hold, 2), trade["id"],))
                    self.db.commit()
                except Exception:
                    self.db.rollback()
                finally:
                    cur.close()
            closed_count += 1

        # Step 2: Tighten stops on WINNING positions to lock in at least breakeven
        for trade in winners:
            entry_price = float(trade.get("entry_price", 0))
            if entry_price > 0 and trade.get("stop_price"):
                current_stop = float(trade["stop_price"])
                # Move stop to entry price (breakeven) or better
                if trade["direction"] == "long" and current_stop < entry_price:
                    self._update_trailing_stop(trade, entry_price)
                    tightened_count += 1
                    logger.info(f"CB Tier 2: Tightened {trade['instrument']} stop to breakeven ({entry_price})")
                elif trade["direction"] == "short" and current_stop > entry_price:
                    self._update_trailing_stop(trade, entry_price)
                    tightened_count += 1
                    logger.info(f"CB Tier 2: Tightened {trade['instrument']} stop to breakeven ({entry_price})")

        # Step 3: Recalculate — if still in breach after closing losers, close winners
        # (smallest profit first to preserve the best performers)
        remaining_loss = realised  # Losers are now closed, their P&L becomes realised
        for trade in losers:
            remaining_loss += trade["_unrealised"]  # These are now realised losses

        remaining_unrealised = sum(w["_unrealised"] for w in winners)
        recalc_total = remaining_loss + remaining_unrealised
        recalc_loss_pct = abs(recalc_total) / account_value * 100 if recalc_total < 0 else 0

        if recalc_loss_pct >= hard_limit_pct:
            logger.error(
                f"CB Tier 2: Still in breach after closing losers ({recalc_loss_pct:.1f}%). "
                f"Closing winners starting with smallest profit."
            )
            for trade in winners:
                close_side = "SELL" if trade["direction"] == "long" else "BUY"
                if not self.broker_account_id:
                    raise RuntimeError(
                        'account_id not resolved — startup incomplete, cannot submit order'
                    )
                # C2/C5c: use actual IBKR unit count, not nominal USD size
                _cb_norm_w = trade["instrument"].replace("/", "").replace(".", "").upper()[:6]
                _cb_qty_w = position_units.get(_cb_norm_w, 0)
                if _cb_qty_w == 0:
                    logger.error(
                        f"CB Tier 2: IBKR position units = 0 for winner {trade['instrument']} "
                        "(fail-safe: skipping close order, may already be flat)"
                    )
                    self.events.write_system_alert(
                        "cb_winner_close_quantity_zero", "critical",
                        f"CB Tier 2 winner close skipped for {trade['instrument']} (trade {trade['id']}): IBKR unit count = 0",
                        "Position may already be flat or IBKR data unavailable.",
                    )
                else:
                    _cbw_resp = self.broker.place_order(self.broker_account_id, {
                        "conid": self.executor._get_conid(trade["instrument"]),
                        "orderType": "MKT",
                        "side": close_side,
                        "quantity": _cb_qty_w,
                        "tif": "GTC",
                    })
                    # C4/C5b: capture fill price — never write zeros
                    _cbw_exit_price = None
                    _cbw_exit_src = 'circuit_breaker'
                    try:
                        _cbw_exit_price, _cbw_exit_src = self.executor._fetch_order_fill_price(
                            _cbw_resp, trade["instrument"], poll_secs=5
                        )
                    except Exception as _ew:
                        logger.warning(f"CB winner fill price lookup failed for {trade['instrument']}: {_ew}")

                    # P&L
                    _cbw_pnl_pips = None
                    _cbw_pnl_usd = None
                    if _cbw_exit_price is not None and trade.get("entry_price") and trade.get("direction"):
                        try:
                            _cbw_pnl_pips, _cbw_pnl_usd = self._compute_pnl(
                                trade["instrument"], trade["direction"],
                                float(trade["entry_price"]), _cbw_exit_price,
                                float(trade.get("position_size_usd") or 0),
                            )
                        except Exception:
                            pass

                    # Hold days
                    _cbw_hold = 0
                    try:
                        import datetime as _dt3
                        _entw = trade.get("entry_time")
                        if _entw:
                            _now3 = _dt3.datetime.now(_dt3.timezone.utc)
                            if _entw.tzinfo is None:
                                _entw = _entw.replace(tzinfo=_dt3.timezone.utc)
                            _cbw_hold = (_now3 - _entw).total_seconds() / 86400
                    except Exception:
                        pass

                    cur = self.db.cursor()
                    try:
                        cur.execute("""
                            UPDATE forex_network.trades
                            SET exit_time = NOW(), exit_reason = 'circuit_breaker',
                                exit_price = %s, exit_price_source = %s,
                                pnl_pips = %s, pnl = %s, hold_days = %s
                            WHERE id = %s AND exit_time IS NULL
                        """, (_cbw_exit_price, _cbw_exit_src, _cbw_pnl_pips, _cbw_pnl_usd,
                              round(_cbw_hold, 2), trade["id"],))
                        self.db.commit()
                    except Exception:
                        self.db.rollback()
                    finally:
                        cur.close()
                closed_count += 1

                # Recalculate after each close — stop once below breach
                remaining_loss += trade["_unrealised"]
                remaining_unrealised -= trade["_unrealised"]
                recalc_total = remaining_loss + remaining_unrealised
                recalc_loss_pct = abs(recalc_total) / account_value * 100 if recalc_total < 0 else 0
                if recalc_loss_pct < hard_limit_pct:
                    logger.info(f"CB Tier 2: Breach resolved after closing {trade['instrument']}")
                    break

        # Step 4: Set circuit breaker flag — always, regardless of which positions were closed
        cur = self.db.cursor()
        try:
            cur.execute("""
                UPDATE forex_network.risk_parameters
                SET circuit_breaker_active = TRUE
                WHERE user_id = %s
            """, (self.user_id,))
            self.db.commit()
        except:
            self.db.rollback()
        finally:
            cur.close()

        # Alert and audit
        winners_preserved = len(winners) - max(0, closed_count - len(losers))
        self.events.write_system_alert(
            "circuit_breaker_user", "critical",
            f"Circuit breaker Tier 2: {self.user_id[:8]}... daily loss {daily_loss_pct:.1f}%",
            f"Losers closed: {len(losers)}. Winners tightened to breakeven: {tightened_count}. "
            f"Winners preserved: {winners_preserved}. Winners force-closed: {max(0, closed_count - len(losers))}. "
            f"Daily limit {daily_limit}%, hard limit {hard_limit_pct}%. "
            f"Trading halted until circuit_breaker_active manually reset.",
        )
        self.events.write_audit_log("circuit_breaker_tier2",
            f"Tier 2 CB: {self.user_id} daily loss {daily_loss_pct:.1f}%",
            {"daily_loss_pct": daily_loss_pct, "hard_limit_pct": hard_limit_pct,
             "realised": realised, "unrealised": total_unrealised,
             "losers_closed": len(losers), "winners_tightened": tightened_count,
             "winners_preserved": winners_preserved,
             "winners_force_closed": max(0, closed_count - len(losers))})

    def run_startup(self):
        """Startup sequence: check broker connectivity, reconcile, fetch swap rates."""
        logger.info("=== STARTUP SEQUENCE ===")

        # Check broker connectivity
        if not self.broker.is_connected():
            logger.error("Broker not reachable — entering holding state")
            retries = 0
            while not self.broker.is_connected():
                retries += 1
                if retries >= 5:
                    self.events.write_system_alert(
                        "broker_unreachable", "critical",
                        "Broker unreachable on startup",
                        f"Retried {retries} times. Execution agent cannot start.",
                    )
                time.sleep(60)
                self.write_heartbeat()

        logger.info("Broker connected")

        # Reconcile positions
        self.reconciler.reconcile()

        # Fetch swap rates
        for pair in FX_PAIRS:
            rates = self.broker.get_swap_rates(pair)
            if rates:
                cur = self.db.cursor()
                try:
                    cur.execute("""
                        INSERT INTO forex_network.swap_rates
                            (instrument, rate_date, long_rate_pips, short_rate_pips, source)
                        VALUES (%s, CURRENT_DATE, %s, %s, %s)
                        ON CONFLICT (instrument, rate_date)
                        DO UPDATE SET long_rate_pips = EXCLUDED.long_rate_pips,
                                      short_rate_pips = EXCLUDED.short_rate_pips,
                                      source = EXCLUDED.source
                    """, (pair, rates["swap_long"], rates["swap_short"],
                          rates.get("source", "static")))
                    self.db.commit()
                except Exception as e:
                    logger.warning(f"Swap rate write failed for {pair}: {e}")
                    self.db.rollback()
                finally:
                    cur.close()

        # Resolve broker account ID (Gap 5 fix — no hardcoded DU_PAPER)
        self.broker_account_id = self.broker.get_account_id()
        if self.broker_account_id:
            logger.info(f"Broker account ID resolved: {self.broker_account_id}")
        else:
            # Fallback: try Parameter Store
            try:
                self.broker_account_id = get_parameter("/platform/config/broker-account-id")
                logger.info(f"Broker account ID from Parameter Store: {self.broker_account_id}")
            except Exception:
                logger.error("Cannot resolve broker account ID — orders will fail")
                self.events.write_system_alert(
                    "broker_account_id_missing", "critical",
                    "Broker account ID could not be resolved",
                    "Check broker connection and /platform/config/broker-account-id parameter.",
                )

        # Query and cache account value from broker
        self.update_account_value()

        # Pre-warm conid cache so first order submission has no lookup latency
        self.executor._prewarm_conid_cache()

        logger.info("=== STARTUP COMPLETE ===")

        # Propagate resolved account ID to executor and kill handler.
        # NOTE: must live here — main() calls run_startup() + run_cycle() directly
        # and never calls run_continuous(), so propagation inside run_continuous()
        # alone was silently skipped in all production runs.
        self.executor.account_id = self.broker_account_id
        self.kill_handler.account_id = self.broker_account_id
        logger.info(f"Account ID propagated to executor/kill_handler: {self.broker_account_id}")

    def run_cycle(self):
        """Run a single execution cycle — manage positions, poll approvals."""
        try:
            # D1: Kill switch guard — check before any action this cycle
            if check_kill_switch():
                logger.info("Kill switch ACTIVE — execution cycle skipped")
                self.write_heartbeat()
                return {"skipped": "kill_switch_active"}
            self.manage_open_positions()
            self.update_account_value()
            approvals = self.read_pending_approvals()
            for approval in approvals:
                if self.dry_run:
                    logger.info(f"DRY RUN: Would execute {approval.get('instrument')} {approval.get('direction')}")
                else:
                    # D3: Re-check circuit breaker before execution
                    # Prevents stale approvals firing after circuit breaker was set
                    _cb_cur = self.db.cursor()
                    _cb_cur.execute(
                        "SELECT circuit_breaker_active FROM forex_network.risk_parameters WHERE user_id = %s",
                        (self.user_id,)
                    )
                    _cb_row = _cb_cur.fetchone()
                    _cb_cur.close()
                    if _cb_row and _cb_row["circuit_breaker_active"]:
                        logger.warning(f"Circuit breaker active for {self.user_id[:8]} — skipping {approval["instrument"]}")
                        continue
                    try:
                        result = self.executor.execute_trade(approval)
                        self.write_execution_signal(approval, result)
                    except Exception as _exec_e:
                        logger.error(
                            f"execute_trade failed for {approval.get('instrument')} "
                            f"— skipping (Trap-24 guard): {_exec_e}",
                            exc_info=True,
                        )
            self.cycle_count += 1
            self.write_heartbeat()
            return {"approvals_processed": len(approvals)}
        except Exception as e:
            logger.error(f"Cycle failed: {e}\n" + traceback.format_exc())
            return {"error": str(e)}

    def run_continuous(self):
        """Main loop: poll for approvals, execute trades, manage positions."""
        logger.info(f"Execution Agent starting — user: {self.user_id}")

        self.run_startup()

        # Propagate resolved account ID to sub-components
        self.executor.account_id = self.broker_account_id
        self.kill_handler.account_id = self.broker_account_id

        last_heartbeat = 0
        last_reconciliation = time.time()
        RECONCILIATION_INTERVAL = 900  # 15 minutes (Rule 11)

        while True:
            try:
                now = time.time()

                # Kill switch handling
                ks_active = check_kill_switch()
                if ks_active and not self._kill_switch_was_active:
                    # Kill switch just activated
                    stress = self._get_current_stress()
                    self.kill_handler.handle_activation(stress)
                    self._kill_switch_was_active = True
                elif not ks_active and self._kill_switch_was_active:
                    # Kill switch just deactivated
                    self.kill_handler.handle_deactivation()
                    self._kill_switch_was_active = False

                if ks_active:
                    # Kill switch active — no new trades, but MUST still manage positions
                    # Circuit breaker Tier 2 must fire even during kill switch
                    self.manage_open_positions()
                    self.write_heartbeat()
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Per-cycle reconciliation (Rule 11)
                if now - last_reconciliation >= RECONCILIATION_INTERVAL:
                    self.reconciler.reconcile()
                    last_reconciliation = now

                # Update account value from IBKR (every cycle)
                self.update_account_value()

                # Manage open positions — trailing stops + trade close detection
                self.manage_open_positions()

                # Poll for approved trades
                approvals = self.read_pending_approvals()
                for approval in approvals:
                    if self.dry_run:
                        logger.info(f"DRY RUN: Would execute {approval['instrument']} {approval['direction']}")
                        self.write_execution_signal(approval, {"executed": False, "details": {"dry_run": True}})
                    else:
                        try:
                            result = self.executor.execute_trade(approval)
                            self.write_execution_signal(approval, result)
                        except Exception as _exec_e:
                            logger.error(
                                f"execute_trade failed for {approval.get('instrument')} "
                                f"— skipping (Trap-24 guard): {_exec_e}",
                                exc_info=True,
                            )

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
        logger.info("Execution Agent shutdown complete.")

    def _get_current_stress(self) -> float:
        """Read current stress score for kill switch multiplier decision."""
        cur = self.db.cursor()
        try:
            cur.execute("""
                SELECT system_stress_score FROM shared.market_context_snapshots
                WHERE system_stress_score IS NOT NULL
                ORDER BY snapshot_time DESC LIMIT 1
            """)
            row = cur.fetchone()
            return float(row["system_stress_score"]) if row else 0
        except:
            return 0
        finally:
            cur.close()


# =============================================================================
# TEST SUITE
# =============================================================================
class ExecutionAgentTester:
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
        logger.info("EXECUTION AGENT TEST SUITE")
        logger.info("=" * 60)

        self.test_stop_randomisation()
        self.test_timing_jitter()
        self.test_position_size_variation()
        self.test_limit_order_decision()
        self.test_hard_rejection_classification()
        self.test_soft_rejection_classification()
        self.test_partial_fill_accept()
        self.test_partial_fill_cancel()
        self.test_slippage_thresholds()
        self.test_kill_switch_stop_multipliers()
        self.test_instrument_normalisation()

        logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASSED, {self.failed} FAILED (total: {self.passed + self.failed})")
        logger.info("=" * 60)
        return self.failed == 0

    def test_stop_randomisation(self):
        logger.info("\n--- Test: Rule 14 — Stop randomisation ---")
        base_stop = 1.10000
        atr = 0.00500
        stops = [ExecutionDefence.randomise_stop(base_stop, atr, "long") for _ in range(100)]
        min_s, max_s = min(stops), max(stops)
        expected_range = atr * STOP_RANDOMISATION_RANGE * 2
        self._assert(max_s - min_s > 0, "Stops are randomised (not identical)")
        self._assert(
            max_s - min_s <= expected_range + 0.00001,
            f"Randomisation within ±{STOP_RANDOMISATION_RANGE*100}% ATR",
            f"Range: {max_s - min_s:.5f}, expected max: {expected_range:.5f}"
        )

    def test_timing_jitter(self):
        logger.info("\n--- Test: Rule 15 — Timing jitter ---")
        jitters = [ExecutionDefence.get_timing_jitter() for _ in range(100)]
        self._assert(all(TIMING_JITTER_MIN <= j <= TIMING_JITTER_MAX for j in jitters),
                     f"All jitters in [{TIMING_JITTER_MIN}, {TIMING_JITTER_MAX}]")
        self._assert(max(jitters) - min(jitters) > 10, "Jitter has meaningful spread")

    def test_position_size_variation(self):
        logger.info("\n--- Test: Rule 16 — Position size variation ---")
        base = 50000
        sizes = [ExecutionDefence.vary_position_size(base) for _ in range(100)]
        min_expected = base * (1 - POSITION_SIZE_VARIATION)
        max_expected = base * (1 + POSITION_SIZE_VARIATION)
        self._assert(
            all(min_expected - 1 <= s <= max_expected + 1 for s in sizes),
            f"All sizes within ±{POSITION_SIZE_VARIATION*100}%"
        )
        self._assert(max(sizes) != min(sizes), "Sizes are varied")

    def test_limit_order_decision(self):
        logger.info("\n--- Test: Rule 17 — Limit order decision ---")
        self._assert(
            ExecutionDefence.should_use_limit_order(1.0, 1.5),
            "Use limit when spread (1.0) <= 1.5× normal (2.25)"
        )
        self._assert(
            not ExecutionDefence.should_use_limit_order(3.0, 1.5),
            "Use market when spread (3.0) > 1.5× normal (2.25)"
        )

    def test_hard_rejection_classification(self):
        logger.info("\n--- Test: Hard rejection classification ---")
        executor = TradeExecutor.__new__(TradeExecutor)
        self._assert(executor._is_hard_rejection({"error": "insufficient margin", "status": 200}),
                     "Margin error → hard")
        self._assert(executor._is_hard_rejection({"error": "position limit exceeded", "status": 201}),
                     "Position limit → hard")

    def test_soft_rejection_classification(self):
        logger.info("\n--- Test: Soft rejection classification ---")
        executor = TradeExecutor.__new__(TradeExecutor)
        self._assert(not executor._is_hard_rejection({"error": "timeout", "status": 300}),
                     "Timeout → soft")
        self._assert(not executor._is_hard_rejection({"error": "busy", "status": 350}),
                     "Busy → soft")

    def test_partial_fill_accept(self):
        logger.info("\n--- Test: Partial fill — accept ≥80% ---")
        self._assert(85.0 >= PARTIAL_FILL_ACCEPT_PCT, "85% fill → accepted")

    def test_partial_fill_cancel(self):
        logger.info("\n--- Test: Partial fill — cancel <80% ---")
        self._assert(70.0 < PARTIAL_FILL_ACCEPT_PCT, "70% fill → cancelled")

    def test_slippage_thresholds(self):
        logger.info("\n--- Test: Slippage thresholds ---")
        self._assert(SLIPPAGE_THRESHOLDS["EURUSD"] == 3, "EUR/USD: 3 pips")
        self._assert(SLIPPAGE_THRESHOLDS["GBPUSD"] == 4, "GBP/USD: 4 pips")
        self._assert(SLIPPAGE_THRESHOLDS["NZDUSD"] == 5, "NZD/USD: 5 pips")

    def test_kill_switch_stop_multipliers(self):
        logger.info("\n--- Test: Kill switch stop multipliers ---")
        self._assert(KILL_SWITCH_STOP_MULTIPLIERS["standard"] == 1.0, "Standard: ATR × 1.0")
        self._assert(KILL_SWITCH_STOP_MULTIPLIERS["severe"] == 0.75, "Severe: ATR × 0.75")

    def test_instrument_normalisation(self):
        logger.info("\n--- Test: Instrument normalisation ---")
        self._assert(
            ReconciliationEngine._normalise_instrument({"contractDesc": "EUR/USD"}) == "EURUSD",
            "EUR/USD → EURUSD"
        )
        self._assert(
            ReconciliationEngine._normalise_instrument({"symbol": "GBP.USD"}) == "GBPUSD",
            "GBP.USD → GBPUSD"
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
    """Main entry point — runs Execution Agent for ALL active users."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Execution Agent")
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
        logger.info("Execution Agent test mode — verifying configuration for all users")
        try:
            for uid in user_ids:
                agent = ExecutionAgent(user_id=uid, dry_run=True)
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
            agents[uid] = ExecutionAgent(user_id=uid, dry_run=getattr(args, "dry_run", False))
            logger.info(f"Initialized Execution Agent for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Execution Agent for {uid}: {e}")

    if not agents:
        logger.error("No agents initialized — exiting")
        sys.exit(1)

    # Run per-user startup (resolves broker account_id, warms account_value).
    # Without this the first /portfolio/<None>/summary returns 401.
    for uid, agent in agents.items():
        try:
            agent.run_startup()
        except Exception as e:
            logger.error(f"Startup failed for {uid}: {e}")

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
                # Use longer sleep during off-hours (22:00-07:00 UTC) to avoid
                # hammering IG demo API with unnecessary polling overnight
                import datetime as _dt
                _utc_hr = _dt.datetime.now(_dt.timezone.utc).hour
                _off_hours = _utc_hr >= 22 or _utc_hr < 7
                _interval = 300 if _off_hours else (agent.CYCLE_INTERVAL_MINUTES * 60 if hasattr(agent, "CYCLE_INTERVAL_MINUTES") else 30)
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

