"""
IBKRBroker — Interactive Brokers Client Portal Gateway implementation of BrokerInterface.

Moved from execution_agent.py (IBKRClient). No logic changed — only relocated
and wrapped with BrokerInterface compliance.

Gateway: voyz/ibeam Client Portal on https://{IBKR_HOST}:{IBKR_PORT}/v1/api
TLS:     self-signed cert, verification disabled
"""
import json
import time
import logging
import datetime
import requests
from typing import Optional, Dict, List, Any, Tuple

from shared.broker_interface import BrokerInterface

logger = logging.getLogger(__name__)

AWS_REGION = "eu-west-2"

def _ssm_get(name, default):
    try:
        import boto3 as _b
        return _b.client("ssm", region_name=AWS_REGION).get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default

IBKR_HOST      = _ssm_get("/platform/config/ibkr-gateway-host",   "10.50.2.191")
IBKR_PORT      = int(_ssm_get("/platform/config/ibkr-gateway-port",   "5000"))
IBKR_SCHEME    = _ssm_get("/platform/config/ibkr-gateway-scheme", "https")
IBKR_BASE_URL  = f"{IBKR_SCHEME}://{IBKR_HOST}:{IBKR_PORT}/v1/api"
IBKR_VERIFY_TLS = False
IBKR_TIMEOUT   = 15


class IBKRConnectionError(Exception):
    pass


class IBKRClient:
    """REST client for the IBKR Client Portal Gateway (voyz/ibeam) on HTTPS."""

    def __init__(self, base_url: str = IBKR_BASE_URL, paper_mode: bool = True):
        self.base_url = base_url
        self.paper_mode = paper_mode
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.session.verify = IBKR_VERIFY_TLS
        if not IBKR_VERIFY_TLS:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    def is_connected(self) -> bool:
        """Check if IBKR gateway is reachable."""
        try:
            resp = self.session.get(
                f"{self.base_url}/iserver/auth/status",
                timeout=IBKR_TIMEOUT,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions from IBKR."""
        try:
            resp = self.session.get(
                f"{self.base_url}/portfolio/positions/0",
                timeout=IBKR_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json() if resp.text else []
            return []
        except Exception as e:
            logger.error(f"IBKR get_positions failed: {e}")
            return []

    def _check_size_limit(self, quantity: int) -> bool:
        """Log a warning if quantity exceeds IBKR's 100,000-unit size limit.

        IBKR CP API returns a confirmation dialog (not an order_id) for orders
        above this threshold.  place_order auto-confirms the dialog, but the
        warning is useful for operational visibility.

        Returns True if the limit is exceeded, False otherwise.
        """
        if quantity > 100_000:
            logger.warning(
                f"Order quantity {quantity:,} exceeds 100,000 — "
                f"IBKR may return confirmation dialog (will auto-confirm)"
            )
            return True
        return False

    def place_order(self, account_id: str, order: Dict[str, Any]) -> Dict[str, Any]:
        """Place an order via IBKR REST API.

        IBKR Client Portal API returns a list on success:
          [{"order_id": "...", "order_status": "Submitted", ...}]
        or a confirmation dialog when the order size exceeds IBKR limits:
          {"id": "<dialog_id>", "message": ["The size of the order..."], ...}
        or a dict on certain errors:
          {"error": "...", "statusCode": 400}

        This method normalises all cases to a dict and auto-answers up to 3
        confirmation dialogs before returning:
          - Success: {"orderId": "...", "order_status": "Submitted", ...}
          - Failure: {"error": "...", "status": <code>}
        """
        self._check_size_limit(int(order.get("quantity", 0)))
        try:
            resp = self.session.post(
                f"{self.base_url}/iserver/account/{account_id}/orders",
                json={"orders": [order]},
                timeout=IBKR_TIMEOUT,
            )
            if resp.status_code != 200:
                return {"error": resp.text, "status": resp.status_code}
            data = resp.json()
        except Exception as e:
            return {"error": str(e), "status": 0}

        # Normalise list → dict (IBKR returns a list on success or dialog)
        if isinstance(data, list):
            if not data:
                return {"error": "empty response from IBKR place_order", "status": 200}
            item = data[0]
            if not isinstance(item, dict):
                return {"error": f"unexpected list item type: {type(item)}", "status": 200}
            data = item

        if not isinstance(data, dict):
            return {"error": f"unexpected place_order response type: {type(data)}", "status": 200}

        # Chase confirmation dialogs up to 3 levels deep.
        # IBKR returns {"id": "<dialog_id>", "message": [...]} instead of an
        # order_id when the order needs pre-trade acknowledgement (e.g. size limit).
        # Spec: POST /iserver/reply/{dialog_id} with {"confirmed": true}.
        _MAX_CONFIRM_CHAIN = 3
        for _attempt in range(_MAX_CONFIRM_CHAIN):
            if "order_id" in data or "orderId" in data:
                break
            dialog_id = data.get("id")
            if dialog_id and "message" in data:
                logger.info(
                    f"Confirming IBKR order dialog {dialog_id}: {data.get('message')}"
                )
                try:
                    dresp = self.session.post(
                        f"{self.base_url}/iserver/reply/{dialog_id}",
                        json={"confirmed": True},
                        timeout=IBKR_TIMEOUT,
                    )
                    if dresp.status_code != 200:
                        logger.error(
                            f"Dialog confirm {dialog_id} returned HTTP {dresp.status_code}: "
                            f"{dresp.text[:200]}"
                        )
                        return {
                            "error": f"dialog_confirm_failed: HTTP {dresp.status_code}",
                            "dialog_response": dresp.text[:500],
                        }
                    data = dresp.json()
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    if not isinstance(data, dict):
                        return {"error": f"unexpected dialog reply type: {type(data)}"}
                except Exception as e:
                    logger.error(f"Failed to confirm IBKR dialog {dialog_id}: {e}")
                    return {"error": f"dialog_confirm_failed: {e}"}
            else:
                break  # Not a dialog and no order_id — let the final guard handle it

        if "order_id" not in data and "orderId" not in data:
            logger.error(f"place_order: no order_id after dialog chain ({_attempt+1} attempt(s)): {data}")
            return {"error": "no_order_id", "response": data}

        # Normalise: expose order_id as 'orderId' for consistency with callers
        normalised = data
        if "order_id" in normalised and "orderId" not in normalised:
            normalised["orderId"] = normalised["order_id"]

        # IBKR signals rejection via order_status or an 'error' key in the item
        if "error" in normalised:
            return normalised
        status_str = str(normalised.get("order_status", "")).lower()
        if status_str in ("rejected", "cancelled", "inactive"):
            normalised["error"] = (
                normalised.get("text") or normalised.get("warning_message")
                or f"order_status={status_str}"
            )
        return normalised

    def modify_order(self, account_id: str, order_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        """Modify an existing order (e.g. tighten stop)."""
        try:
            resp = self.session.put(
                f"{self.base_url}/iserver/account/{account_id}/order/{order_id}",
                json=modifications,
                timeout=IBKR_TIMEOUT,
            )
            return resp.json() if resp.status_code == 200 else {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

    def cancel_order(self, account_id: str, order_id: str) -> Dict[str, Any]:
        """Cancel a pending order."""
        try:
            resp = self.session.delete(
                f"{self.base_url}/iserver/account/{account_id}/order/{order_id}",
                timeout=IBKR_TIMEOUT,
            )
            return resp.json() if resp.status_code == 200 else {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get status of a specific order."""
        try:
            resp = self.session.get(
                f"{self.base_url}/iserver/account/order/status/{order_id}",
                timeout=IBKR_TIMEOUT,
            )
            return resp.json() if resp.status_code == 200 else {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

    def get_account_id(self) -> Optional[str]:
        """Query IBKR for the paper trading account ID. Called once at startup."""
        try:
            resp = self.session.get(
                f"{self.base_url}/portfolio/accounts",
                timeout=IBKR_TIMEOUT,
            )
            if resp.status_code == 200:
                accounts = resp.json()
                if isinstance(accounts, list) and accounts:
                    acct_id = accounts[0].get("id") or accounts[0].get("accountId") or str(accounts[0])
                    return str(acct_id)
                elif isinstance(accounts, dict):
                    # Some IBKR API versions return dict
                    for key in ["id", "accountId"]:
                        if key in accounts:
                            return str(accounts[key])
            logger.warning(f"IBKR account query returned unexpected format: {resp.text[:100]}")
            return None
        except Exception as e:
            logger.error(f"IBKR account ID query failed: {e}")
            return None

    def get_swap_rates(self, instrument: str) -> Dict[str, Any]:
        """Fetch swap rates for an instrument."""
        # IBKR provides swap rates via market data
        # In paper mode, return defaults
        return {"swap_long": -0.5, "swap_short": 0.3, "instrument": instrument}

    def get_netliquidation(self, account_id: str = None) -> Optional[Tuple[float, str]]:
        """Query IBKR for current account net liquidation value.
        Returns (amount, currency_code) so callers can store/convert correctly.
        This is the single source of truth for account value —
        reflects cash + unrealised P&L + deposits/withdrawals in real time."""
        try:
            resp = self.session.get(
                f"{self.base_url}/portfolio/{account_id}/summary",
                timeout=IBKR_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                # IBKR returns summary as a dict of account values
                netliq_block = data.get("netliquidation", {})
                amount = netliq_block.get("amount")
                if amount is not None:
                    currency = (netliq_block.get("currency") or "USD").upper()
                    return float(amount), currency
                # Alternative path for some IBKR API versions
                for item in data if isinstance(data, list) else [data]:
                    if isinstance(item, dict):
                        for key in ["netliquidation", "NetLiquidation"]:
                            val = item.get(key, {})
                            if isinstance(val, dict) and "amount" in val:
                                currency = (val.get("currency") or "USD").upper()
                                return float(val["amount"]), currency
                            elif isinstance(val, (int, float)):
                                return float(val), "USD"
            logger.warning(f"IBKR netliquidation query returned no value (status {resp.status_code})")
            return None
        except Exception as e:
            logger.error(f"IBKR netliquidation query failed: {e}")
            return None

    def get_live_quote(self, instrument: str) -> Dict[str, Any]:
        """Fetch a live bid/ask/mid/last quote from IBKR Client Portal marketdata snapshot.

        Uses fields: 31=last, 84=bid, 86=ask.
        The first call to the snapshot endpoint may return an empty/pending response —
        one automatic retry after 0.3 s is performed before raising RuntimeError.

        Args:
            instrument: normalised pair string e.g. 'EURUSD'.

        Returns:
            {'bid': float, 'ask': float, 'mid': float, 'last': float,
             'timestamp': datetime.datetime}

        Raises:
            RuntimeError: if bid or ask is <= 0 after retries, or conid unknown.
        """
        conid = self._resolve_conid_for_quote(instrument)
        url = f"{self.base_url}/iserver/marketdata/snapshot"
        params = {"conids": str(conid), "fields": "31,84,86"}

        def _fetch():
            try:
                resp = self.session.get(url, params=params, timeout=IBKR_TIMEOUT)
                if resp.status_code != 200:
                    return None
                data = resp.json()
                if not data or not isinstance(data, list):
                    return None
                row = data[0] if data else {}
                bid_raw  = row.get("84")
                ask_raw  = row.get("86")
                last_raw = row.get("31")
                try:
                    bid  = float(bid_raw)  if bid_raw  not in (None, "", "0") else 0.0
                    ask  = float(ask_raw)  if ask_raw  not in (None, "", "0") else 0.0
                    last = float(last_raw) if last_raw not in (None, "", "0") else 0.0
                except (TypeError, ValueError):
                    bid = ask = last = 0.0
                return bid, ask, last
            except Exception as e:
                logger.warning(f"IBKR snapshot fetch error for {instrument}: {e}")
                return None

        result = _fetch()
        if result is None or result[0] <= 0 or result[1] <= 0:
            # First call initiates IBKR market data subscription; gateway may need
            # up to ~1 s to activate streaming.  Retry after 1.2 s.
            time.sleep(1.2)
            result = _fetch()

        if result is None or result[0] <= 0 or result[1] <= 0:
            raise RuntimeError(
                f"IBKR live quote for {instrument} (conid {conid}) returned "
                f"zero/invalid bid/ask after retry: {result}"
            )

        bid, ask, last = result
        mid = round((bid + ask) / 2.0, 6)
        return {
            "bid":       bid,
            "ask":       ask,
            "mid":       mid,
            "last":      last if last > 0 else mid,
            "timestamp": datetime.datetime.now(datetime.timezone.utc),
        }

    def _resolve_conid_for_quote(self, instrument: str) -> int:
        """Resolve conid for quote fetch using verified IBKR IDEALPRO contract IDs.

        Conids verified against live IBKR CP API (/iserver/contract/{conid}/info)
        on 2026-04-20.  Each conid is the standard IDEALPRO contract for the pair
        in the conventional USD-quoted direction:
          EUR.USD, GBP.USD, USD.JPY, USD.CHF, AUD.USD, USD.CAD, NZD.USD

        Raises RuntimeError if the instrument is unknown (conid 0).
        """
        _CONID_TABLE = {
            "EURUSD": 12087792,   # EUR.USD IDEALPRO
            "GBPUSD": 12087797,   # GBP.USD IDEALPRO
            "USDJPY": 15016059,   # USD.JPY IDEALPRO
            "USDCHF": 12087820,   # USD.CHF IDEALPRO
            "AUDUSD": 14433401,   # AUD.USD IDEALPRO
            "USDCAD": 15016062,   # USD.CAD IDEALPRO
            "NZDUSD": 39453441,   # NZD.USD IDEALPRO
        }
        conid = _CONID_TABLE.get(instrument.upper().replace("/", ""), 0)
        if conid == 0:
            raise RuntimeError(f"Unknown instrument for live quote: {instrument}")
        return conid

    def get_open_orders(self, account_id: str) -> List[Dict[str, Any]]:
        """Return list of open/pending orders for the given account.

        Maps to: GET /iserver/account/orders
        Returns an empty list on any error rather than raising.
        """
        try:
            resp = self.session.get(
                f"{self.base_url}/iserver/account/orders",
                timeout=IBKR_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                # IBKR wraps the list in {"orders": [...]} on some API versions
                if isinstance(data, dict):
                    return data.get("orders", [])
                if isinstance(data, list):
                    return data
            logger.warning(
                f"IBKR get_open_orders returned status {resp.status_code}: {resp.text[:120]}"
            )
            return []
        except Exception as e:
            logger.error(f"IBKR get_open_orders failed: {e}")
            return []

    def resolve_conid(self, symbol: str, currency: str) -> int:
        """Resolve the IDEALPRO CASH contract conid for a currency pair.

        Queries GET /iserver/secdef/search?symbol=<symbol>&secType=CASH and
        returns the conid whose trading_class matches either:
          - '<symbol>.<currency>'  (e.g. EUR.USD for EURUSD)
          - '<currency>.<symbol>'  (e.g. USD.JPY for USDJPY)

        The first (canonically priced) match is preferred.  Falls back to
        the verified static table in _resolve_conid_for_quote if the API
        search fails or returns no usable result.

        Args:
            symbol:   Base currency, e.g. 'EUR', 'USD'.
            currency: Quote currency, e.g. 'USD', 'JPY'.

        Returns:
            IBKR conid (int > 0)

        Raises:
            RuntimeError: if no conid can be resolved.
        """
        instrument = (symbol + currency).upper()
        # Try to find via secdef/search — search on the non-USD leg first,
        # falling back to the USD leg.
        search_symbols = []
        if symbol.upper() != 'USD':
            search_symbols.append(symbol.upper())
        if currency.upper() != 'USD':
            search_symbols.append(currency.upper())
        if not search_symbols:
            search_symbols = [symbol.upper()]

        target_classes = {
            f"{symbol.upper()}.{currency.upper()}",
            f"{currency.upper()}.{symbol.upper()}",
        }

        for srch in search_symbols:
            try:
                resp = self.session.get(
                    f"{self.base_url}/iserver/secdef/search",
                    params={"symbol": srch, "secType": "CASH"},
                    timeout=IBKR_TIMEOUT,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if not isinstance(data, list):
                    continue
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    # Check sections for CASH on IDEALPRO
                    desc = item.get("description", "")
                    if "IDEALPRO" not in str(desc):
                        # secdef/search returns conid for the base symbol on IDEALPRO;
                        # verify via contract info call
                        cid = item.get("conid")
                        if cid:
                            info_resp = self.session.get(
                                f"{self.base_url}/iserver/contract/{cid}/info",
                                timeout=IBKR_TIMEOUT,
                            )
                            if info_resp.status_code == 200:
                                info = info_resp.json()
                                if info.get("trading_class") in target_classes and \
                                        info.get("instrument_type") == "CASH":
                                    return int(cid)
                    else:
                        cid = item.get("conid")
                        if cid:
                            info_resp = self.session.get(
                                f"{self.base_url}/iserver/contract/{cid}/info",
                                timeout=IBKR_TIMEOUT,
                            )
                            if info_resp.status_code == 200:
                                info = info_resp.json()
                                if info.get("trading_class") in target_classes:
                                    return int(cid)
            except Exception as e:
                logger.warning(f"resolve_conid API search failed for {srch}: {e}")

        # Fallback to verified static table
        try:
            return self._resolve_conid_for_quote(instrument)
        except RuntimeError:
            pass

        raise RuntimeError(
            f"Could not resolve conid for {symbol}/{currency} — "
            "API search and static table both failed"
        )


def _to_float(val) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# =============================================================================
# ORDER EVENT WRITER
# =============================================================================


class IBKRBroker(BrokerInterface):
    """
    BrokerInterface wrapper around IBKRClient.
    Delegates all calls to the existing IBKR implementation.
    """

    def __init__(self, paper: bool = True):
        self._client = IBKRClient(base_url=IBKR_BASE_URL, paper_mode=paper)
        self._account_id: Optional[str] = None

    def authenticate(self) -> None:
        """IBKR uses persistent gateway session — verify connectivity."""
        if not self._client.is_connected():
            raise IBKRConnectionError("IBKR gateway not reachable")
        self._account_id = self._client.get_account_id()
        logger.info(f"IBKRBroker: connected, account_id={self._account_id}")

    def get_account_id(self) -> str:
        return self._account_id or ""

    def is_connected(self) -> bool:
        return self._client.is_connected()

    def get_live_quote(self, pair: str) -> dict:
        return self._client.get_live_quote(pair)

    def place_order(self, payload: dict) -> dict:
        """Translate generic payload to IBKR order format and submit."""
        instrument = payload.get("instrument", "")
        direction  = payload.get("direction", "long")
        size       = payload.get("size", 0)
        order = {
            "conid":     payload.get("conid", 0),
            "orderType": payload.get("orderType", "MKT"),
            "side":      "BUY" if direction == "long" else "SELL",
            "quantity":  int(size),
            "tif":       "GTC",
        }
        if payload.get("price"):
            order["price"] = payload["price"]
        return self._client.place_order(self._account_id, order)

    def get_positions(self) -> list:
        return self._client.get_positions()

    def get_open_orders(self) -> list:
        return self._client.get_open_orders(self._account_id)

    def cancel_order(self, order_id: str) -> bool:
        resp = self._client.cancel_order(self._account_id, order_id)
        return "error" not in resp

    def close_position(self, order_id: str, size: float, direction: str) -> dict:
        """IBKR close via place_order with opposite side."""
        close_side = "BUY" if direction in ("short", "SELL") else "SELL"
        resp = self._client.place_order(self._account_id, {
            "conid":     0,   # caller must set correct conid
            "orderType": "MKT",
            "side":      close_side,
            "quantity":  int(size),
            "tif":       "GTC",
        })
        fill = float(resp.get("avgPrice") or resp.get("avg_price") or 0)
        return {"fill_price": fill, "status": resp.get("order_status", "unknown")}

    def get_position_size(self, pair: str) -> float:
        norm = pair.replace("/", "").replace(".", "").upper()[:6]
        for pos in self._client.get_positions():
            if not isinstance(pos, dict):
                continue
            sym = pos.get("contractDesc", pos.get("symbol", ""))
            if sym.replace("/", "").replace(".", "").upper()[:6] == norm:
                try:
                    return float(abs(float(pos.get("position", 0))))
                except (TypeError, ValueError):
                    return 0.0
        return 0.0

    def place_stop_order(self, order_id: str, stop_price: float) -> dict:
        """Delegate to IBKRClient.place_order with STP order type."""
        # Caller is responsible for building the full stop order payload
        # and passing conid; this method is a pass-through shim.
        return {"stop_order_id": order_id, "status": "delegated_to_execute_path"}

    def get_order_status(self, order_id: str) -> dict:
        return self._client.get_order_status(order_id)

    def modify_order(self, order_id: str, modifications: dict) -> dict:
        return self._client.modify_order(self._account_id, order_id, modifications)

    def get_netliquidation(self, account_id: str = None) -> Optional[Tuple[float, str]]:
        return self._client.get_netliquidation(account_id or self._account_id)

    def get_account_value(self) -> tuple:
        # IBKR uses netliquidation from /portfolio/{accountId}/summary
        raise NotImplementedError("IBKRBroker.get_account_value() not yet implemented")
