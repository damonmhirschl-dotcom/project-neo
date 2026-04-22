"""
BrokerInterface — abstract base class for all execution brokers.

Switching brokers = change /platform/config/broker in Parameter Store + restart.
Supported values: ig_demo | ig_live | ibkr_paper | ibkr_live
"""
from abc import ABC, abstractmethod


class BrokerInterface(ABC):

    @abstractmethod
    def authenticate(self) -> None:
        """Establish and store session credentials."""

    @abstractmethod
    def get_live_quote(self, pair: str) -> dict:
        """Return {"bid": float, "ask": float, "spread": float, "mid": float}"""

    @abstractmethod
    def place_order(self, payload: dict) -> dict:
        """
        Place a market order with inline stop where supported.
        payload keys: instrument, direction, size, stop_price,
                      current_price, risk_amount_gbp, stop_distance
        Return {"order_id": str, "fill_price": float, "status": str}
        """

    @abstractmethod
    def get_positions(self) -> list:
        """
        Return list of open positions.
        Each item: {order_id, instrument, direction, size, entry_price,
                    current_price, contractDesc, symbol, position}
        """

    @abstractmethod
    def get_open_orders(self) -> list:
        """
        Return list of working orders.
        Each item: {order_id, instrument, direction, size}
        """

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel a working order. Return True on success."""

    @abstractmethod
    def close_position(self, order_id: str, size: float,
                       direction: str) -> dict:
        """
        Close an open position.
        Return {"fill_price": float, "status": str}
        """

    @abstractmethod
    def get_position_size(self, pair: str) -> float:
        """Return current open size for pair. 0.0 if no position."""

    @abstractmethod
    def place_stop_order(self, order_id: str, stop_price: float) -> dict:
        """
        Place or update a stop on an existing position.
        Brokers with inline stops (IG): no-op or modify.
        Brokers with separate stops (IBKR): submit STP order.
        Return {"stop_order_id": str, "status": str}
        """

    # ── Extended interface ──────────────────────────────────────────────────
    # Methods below are used by reconciliation, kill switch, and account value
    # queries. Concrete implementations must provide these even though they
    # are not part of the core trading contract above.

    def get_order_status(self, order_id: str) -> dict:
        """Return status dict for an order/deal. Used by reconciliation."""
        return {}

    def modify_order(self, order_id: str, modifications: dict) -> dict:
        """Modify an existing order (e.g. tighten stop). Used by kill switch."""
        return {}

    def get_netliquidation(self, account_id: str = None):
        """Return (amount, currency) tuple or None. Used for account value sync."""
        return None

    def is_connected(self) -> bool:
        """Liveness check. Return True if broker API is reachable."""
        return True

    def get_account_id(self) -> str:
        """Return the broker account identifier."""
        return ""

    @abstractmethod
    def get_account_value(self) -> tuple:
        """Return (equity, currency) including unrealised P&L."""
