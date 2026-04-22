"""
IGBroker — IG Markets REST API implementation of BrokerInterface.

Credentials: AWS Secrets Manager  platform/ig-markets/demo-credentials
Base URL:    https://demo-api.ig.com/gateway/deal   (demo)
             https://api.ig.com/gateway/deal         (live)
Account ID:  Z6AO3F (CFD, demo)

Position sizing (POINTS, £1/point mini CFD):
    stop_distance_points = abs(current_price - stop_price) * 10000 * 10
    size = risk_amount_gbp / stop_distance_points
    size = max(0.1, round(size, 1))
"""
import json
import logging
import datetime
import boto3
import requests
from requests.adapters import HTTPAdapter, Retry

from shared.broker_interface import BrokerInterface

logger = logging.getLogger(__name__)

AWS_REGION     = "eu-west-2"
IG_SECRET_ID   = "platform/ig-markets/demo-credentials"
IG_ACCOUNT_ID  = "Z6AO3F"
IG_DEMO_BASE   = "https://demo-api.ig.com/gateway/deal"
IG_LIVE_BASE   = "https://api.ig.com/gateway/deal"
IG_TIMEOUT     = 15
SESSION_TTL_H  = 6


class IGConnectionError(Exception):
    pass


class IGBroker(BrokerInterface):
    """IG Markets REST API broker."""

    EPIC_MAP = {
        # USD pairs
        "EURUSD": "CS.D.EURUSD.MINI.IP",
        "GBPUSD": "CS.D.GBPUSD.MINI.IP",
        "USDJPY": "CS.D.USDJPY.MINI.IP",
        "USDCHF": "CS.D.USDCHF.MINI.IP",
        "AUDUSD": "CS.D.AUDUSD.MINI.IP",
        "USDCAD": "CS.D.USDCAD.MINI.IP",
        "NZDUSD": "CS.D.NZDUSD.MINI.IP",
        # Cross pairs -- confirmed on IG demo 2026-04-22
        "EURGBP": "CS.D.EURGBP.MINI.IP",
        "EURJPY": "CS.D.EURJPY.MINI.IP",
        "GBPJPY": "CS.D.GBPJPY.MINI.IP",
        "EURCHF": "CS.D.EURCHF.MINI.IP",
        "GBPCHF": "CS.D.GBPCHF.MINI.IP",
        "EURAUD": "CS.D.EURAUD.MINI.IP",
        "GBPAUD": "CS.D.GBPAUD.MINI.IP",
        "EURCAD": "CS.D.EURCAD.MINI.IP",
        "GBPCAD": "CS.D.GBPCAD.MINI.IP",
        "AUDNZD": "CS.D.AUDNZD.MINI.IP",
        "AUDJPY": "CS.D.AUDJPY.MINI.IP",
        "CADJPY": "CS.D.CADJPY.MINI.IP",
        "NZDJPY": "CS.D.NZDJPY.MINI.IP",
    }

    # Quote currency (second leg) per instrument — required by IG currencyCode field.
    # GBP is NOT accepted for any of these instruments on a CFD account.
    CURRENCY_CODE_MAP = {
        # USD pairs
        "EURUSD": "USD",
        "GBPUSD": "USD",
        "AUDUSD": "USD",
        "NZDUSD": "USD",
        "USDJPY": "JPY",
        "USDCHF": "CHF",
        "USDCAD": "CAD",
        # Cross pairs
        "EURGBP": "GBP",
        "EURJPY": "JPY",
        "GBPJPY": "JPY",
        "EURCHF": "CHF",
        "GBPCHF": "CHF",
        "EURAUD": "AUD",
        "GBPAUD": "AUD",
        "EURCAD": "CAD",
        "GBPCAD": "CAD",
        "AUDNZD": "NZD",
        "AUDJPY": "JPY",
        "CADJPY": "JPY",
        "NZDJPY": "JPY",
    }

    PIP_SIZE_MAP = {
        # JPY pairs: 1 pip = 0.01
        "USDJPY": 0.01,
        "EURJPY": 0.01,
        "GBPJPY": 0.01,
        "AUDJPY": 0.01,
        "CADJPY": 0.01,
        "NZDJPY": 0.01,
        # All others: 1 pip = 0.0001 (default -- see get_pip_size())
    }

    def __init__(self, demo: bool = True):
        self.demo = demo
        self.base_url = IG_DEMO_BASE if demo else IG_LIVE_BASE
        self.account_id = IG_ACCOUNT_ID
        self._session = requests.Session()
        # Retry once on connection errors (stale keep-alive after IG drops idle connection)
        _retry = Retry(total=1, connect=1, read=1, backoff_factor=0.3,
                       status_forcelist=(), allowed_methods=frozenset(["GET","POST","DELETE","PUT"]))
        _adapter = HTTPAdapter(max_retries=_retry)
        self._session.mount("https://", _adapter)
        self._cst = None
        self._security_token = None
        self._session_expiry = None
        self._api_key = None
        self._username = None
        self._password = None
        self._fill_cache: dict = {}   # order_id → confirm dict
        self._market_cache: dict = {}  # epic → market details (min stop, etc.)
        self._load_credentials()
        self.authenticate()

    # ── Credentials ────────────────────────────────────────────────────────

    def _load_credentials(self) -> None:
        sm = boto3.client("secretsmanager", region_name=AWS_REGION)
        creds = json.loads(sm.get_secret_value(SecretId=IG_SECRET_ID)["SecretString"])
        self._api_key  = creds["api_key"]
        self._username = creds["username"]
        self._password = creds["password"]

    # ── Session ─────────────────────────────────────────────────────────────

    def authenticate(self, force: bool = False) -> None:
        """POST /session. Stores CST + X-SECURITY-TOKEN. Auto-refreshes if >6h old.

        Args:
            force: If True, bypass the TTL check and re-authenticate unconditionally.
                   Used by _force_reconnect() after a dead-socket ConnectionError.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if not force and self._cst and self._session_expiry and now < self._session_expiry:
            return
        resp = self._session.post(
            self.base_url + "/session",
            json={"identifier": self._username, "password": self._password,
                  "encryptedPassword": False},
            headers={"Content-Type": "application/json",
                     "X-IG-API-KEY": self._api_key, "Version": "2"},
            timeout=IG_TIMEOUT,
        )
        if resp.status_code != 200:
            raise IGConnectionError(
                f"IG auth failed: HTTP {resp.status_code} {resp.text[:200]}"
            )
        self._cst             = resp.headers.get("CST")
        self._security_token  = resp.headers.get("X-SECURITY-TOKEN")
        self._session_expiry  = now + datetime.timedelta(hours=SESSION_TTL_H)
        logger.info("IGBroker: session authenticated")

    def _force_reconnect(self) -> None:
        """Close the stale TCP session, create a fresh one, and force re-auth.

        Called when a requests.exceptions.ConnectionError wraps a
        RemoteDisconnected — IG demo drops idle keep-alive connections and
        the Retry adapter retries on the same dead socket.  This method
        replaces that socket entirely so the retry hits a live connection.
        """
        logger.warning("IGBroker: RemoteDisconnected detected — forcing session reconnect")
        try:
            self._session.close()
        except Exception:
            pass
        self._session = requests.Session()
        _retry = Retry(total=1, connect=1, read=1, backoff_factor=0.3,
                       status_forcelist=(),
                       allowed_methods=frozenset(["GET", "POST", "DELETE", "PUT"]))
        _adapter = HTTPAdapter(max_retries=_retry)
        self._session.mount("https://", _adapter)
        # Invalidate cached tokens so authenticate() will POST /session
        self._cst = None
        self._security_token = None
        self._session_expiry = None
        self.authenticate(force=True)
        logger.info("IGBroker: reconnect complete, new session established")

    def _get_headers(self, version: str = "2") -> dict:
        self.authenticate()
        return {
            "Content-Type":    "application/json",
            "X-IG-API-KEY":    self._api_key,
            "CST":             self._cst,
            "X-SECURITY-TOKEN": self._security_token,
            "Version":         version,
        }

    def _epic(self, pair: str) -> str:
        norm = pair.upper().replace("/", "")
        epic = self.EPIC_MAP.get(norm)
        if not epic:
            raise IGConnectionError(f"IGBroker: unknown instrument '{pair}'")
        return epic

    def is_connected(self) -> bool:
        try:
            r = self._session.get(
                self.base_url + "/accounts",
                headers=self._get_headers(version="1"), timeout=10,
            )
            return r.status_code == 200
        except Exception:
            return False

    def get_account_id(self) -> str:
        return self.account_id

    def get_swap_rates(self, pair: str):
        # IG REST does not expose swap rates - return None (rates from DB)
        return None

    # ── Quotes ──────────────────────────────────────────────────────────────

    def get_live_quote(self, pair: str) -> dict:
        """GET /markets/{epic} → {bid, ask, spread, mid}."""
        epic = self._epic(pair)
        try:
            r = self._session.get(
                self.base_url + f"/markets/{epic}",
                headers=self._get_headers(version="3"),
                timeout=IG_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IGBroker.get_live_quote {pair}: ConnectionError ({e}) — reconnecting")
            self._force_reconnect()
            r = self._session.get(
                self.base_url + f"/markets/{epic}",
                headers=self._get_headers(version="3"),
                timeout=IG_TIMEOUT,
            )
        if r.status_code != 200:
            raise IGConnectionError(
                f"IGBroker.get_live_quote {pair}: HTTP {r.status_code} {r.text[:200]}"
            )
        snap = r.json().get("snapshot", {})
        bid  = float(snap.get("bid")   or 0)
        ask  = float(snap.get("offer") or 0)
        if bid <= 0 or ask <= 0:
            raise IGConnectionError(
                f"IGBroker.get_live_quote {pair}: invalid bid/ask {bid}/{ask}"
            )
        mid = round((bid + ask) / 2.0, 6)
        return {"bid": bid, "ask": ask, "spread": round(ask - bid, 6), "mid": mid,
                "last": mid, "timestamp": datetime.datetime.now(datetime.timezone.utc)}

    # ── Orders ──────────────────────────────────────────────────────────────

    def _get_market_details(self, epic: str) -> dict:
        """GET /markets/{epic} — cached per epic for the lifetime of this broker instance."""
        if epic not in self._market_cache:
            resp = self._session.get(
                self.base_url + f"/markets/{epic}",
                headers=self._get_headers(version="3"),
                timeout=IG_TIMEOUT,
            )
            if resp.status_code == 200:
                self._market_cache[epic] = resp.json()
            else:
                raise IGConnectionError(
                    f"IGBroker._get_market_details {epic}: HTTP {resp.status_code}"
                )
        return self._market_cache[epic]

    def place_order(self, payload: dict) -> dict:
        """
        POST /positions/otc with inline stop, then GET /confirms/{dealRef}.
        payload keys: instrument, direction, current_price, stop_price,
                      risk_amount_gbp, stop_distance (price units)
        Returns: {order_id, fill_price, status, dealReference, size}
        """
        instrument    = payload.get("instrument", "")
        direction     = payload.get("direction", "long")
        current_price = float(payload.get("current_price") or 0)
        stop_price    = float(payload.get("stop_price") or 0)
        risk_amount   = float(payload.get("risk_amount_gbp") or 0)
        stop_distance = float(payload.get("stop_distance") or 0)

        epic      = self._epic(instrument)
        ig_dir    = "BUY" if direction == "long" else "SELL"
        is_jpy    = instrument.upper().endswith("JPY")
        pip_scale = 100 if is_jpy else 10000  # pips per price unit

        # Sizing
        if current_price > 0 and stop_price > 0:
            stop_pips = abs(current_price - stop_price) * pip_scale
        elif stop_distance > 0:
            stop_pips = stop_distance * pip_scale
        else:
            stop_pips = 10  # fallback — will produce minimum size
        # IG stopDistance is in PIPS (not sub-pips).
        # Do NOT multiply by 10 — send stop_pips directly.
        stop_distance_pips = stop_pips  # e.g. 20 for a 20-pip stop

        # Enforce IG minimum stop distance
        try:
            _details = self._get_market_details(epic)
            _rules = _details.get("dealingRules", {})
            _min_entry = _rules.get("minNormalStopLimitDistance", {})
            _min_stop_val  = float(_min_entry.get("value") or 0)
            _min_stop_unit = _min_entry.get("unit", "POINTS")
            # IG unit "POINTS" = sub-pips; 10 POINTS = 1 pip for most FX mini contracts
            _min_stop_pips = _min_stop_val / 10 if _min_stop_unit == "POINTS" else _min_stop_val
            if _min_stop_pips > 0 and stop_distance_pips < _min_stop_pips:
                logger.warning(
                    f"Stop distance {stop_distance_pips:.1f} pips below IG minimum "
                    f"{_min_stop_pips:.1f} pips for {epic} — adjusting to minimum"
                )
                stop_distance_pips = _min_stop_pips + 1  # 1 pip buffer above minimum
        except Exception as _min_e:
            logger.warning(f"Could not enforce min stop for {epic}: {_min_e}")

        _pip_to_points = 100 if is_jpy else 10
        _stop_points   = stop_distance_pips * _pip_to_points
        size = max(0.1, round(risk_amount / _stop_points, 1)) if _stop_points > 0 else 0.1

        # Compute limit distance from target_price if provided
        _target_price = payload.get("target_price")
        _limit_distance = None
        if _target_price is not None and current_price > 0:
            _pip_scale_lim = 100 if is_jpy else 10000
            _lim_pips = abs(float(_target_price) - current_price) * _pip_scale_lim
            if _lim_pips > 0:
                _limit_distance = round(_lim_pips, 1)

        body = {
            "epic":           epic,
            "expiry":         "-",
            "direction":      ig_dir,
            "size":           size,
            "orderType":      "MARKET",
            "stopDistance":   round(stop_distance_pips, 1),
            "guaranteedStop": False,
            "limitDistance":  _limit_distance,
            "currencyCode":   self.CURRENCY_CODE_MAP.get(instrument.upper().replace("/", ""), "USD"),
            "forceOpen":      True,
        }

        r = self._session.post(
            self.base_url + "/positions/otc",
            json=body,
            headers=self._get_headers(version="2"),
            timeout=IG_TIMEOUT,
        )
        if r.status_code not in (200, 201, 202):
            return {"error": r.text[:300], "status": r.status_code}

        deal_ref = r.json().get("dealReference")
        if not deal_ref:
            return {"error": "no dealReference in IG place_order response",
                    "response": r.json()}

        # Confirm
        import time as _t; _t.sleep(0.5)
        confirm = self._get_confirm(deal_ref)
        if "error" in confirm:
            return {"order_id": deal_ref, "dealReference": deal_ref,
                    "fill_price": 0.0, "status": "confirm_failed",
                    "error": confirm["error"]}

        deal_id    = confirm.get("dealId", deal_ref)
        fill_price = float(confirm.get("level") or 0)
        status     = confirm.get("dealStatus", "UNKNOWN")
        reason     = confirm.get("reason", "")

        if status == "REJECTED":
            logger.error(
                f"IGBroker.place_order REJECTED: {instrument} {ig_dir} "
                f"reason={reason} size={size} stopDistance={body['stopDistance']}"
            )
            return {
                "order_id":      deal_ref,
                "dealReference": deal_ref,
                "fill_price":    0.0,
                "avg_price":     0.0,
                "status":        "rejected",
                "error":         reason,
                "size":          size,
            }

        logger.info(
            f"IGBroker.place_order ACCEPTED: {instrument} {ig_dir} "
            f"dealId={deal_id} level={fill_price} size={confirm.get('size', size)}"
        )
        result = {
            "order_id":     deal_id,
            "orderId":      deal_id,       # legacy compat
            "dealId":       deal_id,
            "dealReference": deal_ref,
            "fill_price":   fill_price,
            "avg_price":    fill_price,    # legacy compat for _wait_for_fill
            "status":       status,
            "size":         confirm.get("size", size),
        }
        # Cache for get_order_status
        self._fill_cache[deal_id] = result
        self._fill_cache[deal_ref] = result
        return result

    def _get_confirm(self, deal_reference: str) -> dict:
        """GET /confirms/{dealReference}."""
        r = self._session.get(
            self.base_url + f"/confirms/{deal_reference}",
            headers=self._get_headers(version="1"),
            timeout=IG_TIMEOUT,
        )
        if r.status_code != 200:
            return {"error": f"confirm HTTP {r.status_code}: {r.text[:200]}"}
        return r.json()

    def get_order_status(self, order_id: str) -> dict:
        """Return cached confirm or fetch fresh. Used by reconciliation + _wait_for_fill."""
        if order_id in self._fill_cache:
            cached = self._fill_cache[order_id]
            # Translate IG deal status to internal status
            raw_status = cached.get("status", "UNKNOWN")
            if raw_status in ("ACCEPTED", "OPEN"):
                mapped = "Filled"
            elif raw_status == "rejected":
                mapped = "rejected"
            else:
                mapped = "Filled"  # treat any cached order as filled (stops are inline)
            return {
                "status":         mapped,
                "order_status":   mapped,
                "avgPrice":       cached.get("fill_price", 0),
                "avg_price":      cached.get("fill_price", 0),
                "filledQuantity": cached.get("size", 0),
            }
        # Not cached: IG stops are inline — check live positions for this deal
        try:
            positions = self.get_positions()
            for pos in positions:
                if pos.get("order_id") == order_id:
                    return {"status": "Open", "order_status": "Open",
                            "avgPrice": pos.get("entry_price", 0)}
        except Exception as e:
            logger.warning(f"IGBroker.get_order_status({order_id}): {e}")
        # Not found anywhere — return neutral (not an error for IG inline stops)
        return {"status": "NotFound", "order_status": "NotFound"}

    def place_stop_order(self, order_id: str, stop_price: float) -> dict:
        """Stop is placed inline with entry order. No-op for new positions."""
        logger.info(
            f"IGBroker.place_stop_order: stop is inline with IG entry order "
            f"(order_id={order_id}, stop_price={stop_price}) — no separate placement needed"
        )
        return {"stop_order_id": order_id, "status": "inline"}

    def modify_order(self, order_id: str, modifications: dict) -> dict:
        """
        Modify an existing IG position (e.g. tighten stop for kill switch).
        Uses PUT /positions/otc/{dealId}.
        """
        stop_level = modifications.get("auxPrice") or modifications.get("stop_price")
        if not stop_level:
            logger.warning(f"IGBroker.modify_order: no stop_price in modifications={modifications}")
            return {"warning": "no stop_price provided"}
        body = {"stopLevel": stop_level, "limitLevel": None, "trailingStop": False}
        r = self._session.put(
            self.base_url + f"/positions/otc/{order_id}",
            json=body,
            headers=self._get_headers(version="2"),
            timeout=IG_TIMEOUT,
        )
        if r.status_code not in (200, 201, 202):
            logger.error(f"IGBroker.modify_order {order_id}: HTTP {r.status_code} {r.text[:200]}")
            return {"error": r.text[:200], "status": r.status_code}
        return {"status": "modified", "dealReference": r.json().get("dealReference")}

    # ── Positions & Orders ──────────────────────────────────────────────────

    def get_positions(self) -> list:
        """GET /positions. Returns normalised position list.

        Catches requests.exceptions.ConnectionError (which wraps
        http.client.RemoteDisconnected when IG demo drops an idle TCP
        keep-alive) and performs a full session reconnect + one retry,
        so that callers never see a spurious dead-socket error.
        """
        try:
            r = self._session.get(
                self.base_url + "/positions",
                headers=self._get_headers(version="2"),
                timeout=IG_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IGBroker.get_positions: ConnectionError ({e}) — reconnecting")
            self._force_reconnect()
            r = self._session.get(
                self.base_url + "/positions",
                headers=self._get_headers(version="2"),
                timeout=IG_TIMEOUT,
            )
        if r.status_code != 200:
            raise IGConnectionError(
                f"IGBroker.get_positions: HTTP {r.status_code} {r.text[:200]}"
            )
        result = []
        for p in r.json().get("positions", []):
            pos = p.get("position", {})
            mkt = p.get("market", {})
            epic = mkt.get("epic", "")
            # Reverse EPIC_MAP to get pair name
            pair = next((k for k, v in self.EPIC_MAP.items() if v == epic), epic)
            size  = float(pos.get("size") or 0)
            ig_dir = pos.get("direction", "BUY")
            signed = size if ig_dir == "BUY" else -size
            result.append({
                "order_id":      pos.get("dealId"),
                "dealId":        pos.get("dealId"),
                "instrument":    pair,
                "epic":          epic,
                "direction":     "long" if ig_dir == "BUY" else "short",
                "size":          size,
                "entry_price":   float(pos.get("level") or 0),
                "current_price": float(mkt.get("bid") or mkt.get("offer") or 0),
                # Legacy IBKR-compat fields used by reconciliation
                "contractDesc":  mkt.get("instrumentName", epic),
                "symbol":        pair,
                "position":      signed,
            })
        return result

    def get_open_orders(self) -> list:
        """GET /workingorders. Returns normalised working order list."""
        try:
            r = self._session.get(
                self.base_url + "/workingorders",
                headers=self._get_headers(version="2"),
                timeout=IG_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IGBroker.get_open_orders: ConnectionError ({e}) — reconnecting")
            self._force_reconnect()
            r = self._session.get(
                self.base_url + "/workingorders",
                headers=self._get_headers(version="2"),
                timeout=IG_TIMEOUT,
            )
        if r.status_code != 200:
            raise IGConnectionError(
                f"IGBroker.get_open_orders: HTTP {r.status_code} {r.text[:200]}"
            )
        result = []
        for o in r.json().get("workingOrders", []):
            wd  = o.get("workingOrderData", {})
            mkt = o.get("marketData", {})
            epic = mkt.get("epic", "")
            pair = next((k for k, v in self.EPIC_MAP.items() if v == epic), epic)
            result.append({
                "order_id":  wd.get("dealId"),
                "orderId":   wd.get("dealId"),   # legacy compat
                "id":        wd.get("dealId"),
                "instrument": pair,
                "epic":      epic,
                "direction": wd.get("direction"),
                "size":      wd.get("size"),
                "level":     wd.get("level"),
                "status":    wd.get("status", "WORKING"),
                "orderStatus": wd.get("status", "WORKING"),
            })
        return result

    def cancel_order(self, order_id: str) -> bool:
        """DELETE /workingorders/otc/{dealId}."""
        r = self._session.delete(
            self.base_url + f"/workingorders/otc/{order_id}",
            headers=self._get_headers(version="2"),
            timeout=IG_TIMEOUT,
        )
        if r.status_code in (200, 201, 202):
            return True
        logger.error(f"IGBroker.cancel_order {order_id}: HTTP {r.status_code} {r.text[:200]}")
        return False

    def close_position(self, order_id: str, size: float, direction: str) -> dict:
        """
        Close a position via a counter MARKET order (POST /positions/otc, forceOpen=false).

        DELETE /positions/otc consistently returns validation errors on IG demo/CFD accounts.
        The canonical alternative: submit the opposite direction with forceOpen=false,
        which IG interprets as a close rather than opening a new position.

        order_id: the dealId of the position to close.
        direction: the closing direction — SELL to close a long, BUY to close a short.
        """
        # Normalise closing direction to IG format (caller passes closing direction)
        close_dir = "BUY" if direction in ("BUY", "long") else "SELL"

        # Look up the epic from open positions if we can
        epic = None
        currency = "USD"
        try:
            for pos in self.get_positions():
                if pos.get("order_id") == order_id or pos.get("dealId") == order_id:
                    epic     = pos.get("epic")
                    instr    = pos.get("instrument", pos.get("symbol", ""))
                    currency = self.CURRENCY_CODE_MAP.get(
                        instr.upper().replace("/", ""), "USD")
                    break
        except Exception as e:
            logger.warning(f"IGBroker.close_position: could not look up epic for {order_id}: {e}")

        if not epic:
            # Trap 24: ABORT if epic lookup fails. Falling back to a default epic
            # would close or open a position on the wrong instrument. Raise so
            # _close_position() catches it and skips the close safely.
            raise RuntimeError(
                f"IGBroker.close_position: epic not found for dealId={order_id} — "
                f"ABORTING close to prevent wrong-instrument trade (Trap 24)"
            )

        body = {
            "epic":          epic,
            "expiry":        "-",
            "direction":     close_dir,
            "size":          size,
            "orderType":     "MARKET",
            "currencyCode":  currency,
            "forceOpen":     False,       # False = close existing, not open new
            "guaranteedStop": False,
            "limitDistance": None,
            "stopDistance":  None,
        }
        r = self._session.post(
            self.base_url + "/positions/otc",
            json=body,
            headers=self._get_headers(version="2"),
            timeout=IG_TIMEOUT,
        )
        if r.status_code not in (200, 201, 202):
            logger.error(f"IGBroker.close_position {order_id}: HTTP {r.status_code} {r.text[:200]}")
            return {"error": r.text[:300], "status": r.status_code, "fill_price": 0.0}
        deal_ref = r.json().get("dealReference")
        if not deal_ref:
            return {"error": "no dealReference in close response", "fill_price": 0.0}
        import time as _t; _t.sleep(0.5)
        confirm = self._get_confirm(deal_ref)
        if "error" in confirm:
            return {"dealReference": deal_ref, "fill_price": 0.0,
                    "status": "confirm_failed", "error": confirm["error"]}
        fill_price = float(confirm.get("level") or 0)
        deal_status = confirm.get("dealStatus", "UNKNOWN")
        if deal_status == "REJECTED":
            reason = confirm.get("reason", "")
            logger.error(f"IGBroker.close_position REJECTED: {order_id} reason={reason}")
            return {"error": reason, "status": "rejected", "fill_price": 0.0}
        logger.info(
            f"IGBroker.close_position ACCEPTED: {order_id} level={fill_price} "
            f"profit={confirm.get('profit')} {confirm.get('profitCurrency', 'GBP')}"
        )
        return {
            "fill_price":    fill_price,
            "dealId":        confirm.get("dealId"),
            "dealReference": deal_ref,
            "status":        deal_status,
            "profit":        confirm.get("profit"),
            "profit_currency": confirm.get("profitCurrency"),
        }

    def get_position_size(self, pair: str) -> float:
        """Return open size for pair. 0.0 if no position."""
        epic = self._epic(pair)
        try:
            for pos in self.get_positions():
                if pos.get("epic") == epic:
                    return float(abs(pos.get("size") or 0))
        except Exception as e:
            logger.error(f"IGBroker.get_position_size({pair}): {e}")
        return 0.0

    # ── Account ─────────────────────────────────────────────────────────────

    def get_netliquidation(self, account_id: str = None) -> tuple:
        """GET /accounts → total equity (balance.balance, includes unrealised P&L)."""
        target = account_id or self.account_id
        try:
            r = self._session.get(
                self.base_url + "/accounts",
                headers=self._get_headers(version="1"),
                timeout=IG_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IGBroker.get_netliquidation: ConnectionError ({e}) — reconnecting")
            self._force_reconnect()
            r = self._session.get(
                self.base_url + "/accounts",
                headers=self._get_headers(version="1"),
                timeout=IG_TIMEOUT,
            )
        if r.status_code != 200:
            logger.warning(f"IGBroker.get_netliquidation: HTTP {r.status_code}")
            return None
        for acct in r.json().get("accounts", []):
            if acct.get("accountId") == target:
                bal = acct.get("balance", {})
                available = bal.get("balance")
                if available is not None:
                    return float(available), acct.get("currency", "GBP")
        return None

    def get_account_value(self) -> tuple:
        """GET /accounts → (equity, currency) including unrealised P&L."""
        try:
            r = self._session.get(
                self.base_url + "/accounts",
                headers=self._get_headers(version="1"),
                timeout=IG_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IGBroker.get_account_value: ConnectionError ({e}) — reconnecting")
            self._force_reconnect()
            r = self._session.get(
                self.base_url + "/accounts",
                headers=self._get_headers(version="1"),
                timeout=IG_TIMEOUT,
            )
        if r.status_code != 200:
            raise IGConnectionError(
                f"IGBroker.get_account_value: HTTP {r.status_code} {r.text[:200]}"
            )
        for acct in r.json().get("accounts", []):
            if acct.get("accountId") == self.account_id:
                bal = acct.get("balance", {})
                equity = bal.get("balance")
                if equity is not None:
                    return float(equity), acct.get("currency", "GBP")
        raise IGConnectionError(
            f"IGBroker.get_account_value: account {self.account_id} not found in /accounts response"
        )
