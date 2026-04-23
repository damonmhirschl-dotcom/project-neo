"""neo-dash-positions-dev — GET /dashboard/positions

Returns open positions for the JWT user. Joins local trades table (entry metadata
+ convergence/agents/regime context) with live IG Markets position data (current
price + unrealised P&L from IG REST API).
"""
import os, json, urllib.request, urllib.error, boto3, psycopg2, psycopg2.extras
from datetime import datetime, timezone
from decimal import Decimal

_REGION  = "eu-west-2"
_IG_BASE = "https://demo-api.ig.com/gateway/deal"
_conn    = None

PAIR_DISPLAY = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD", "USDCHF": "USD/CHF",
    "NZDUSD": "NZD/USD",
    "EURGBP": "EUR/GBP", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "EURCHF": "EUR/CHF", "GBPCHF": "GBP/CHF", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCAD": "EUR/CAD", "GBPCAD": "GBP/CAD",
    "AUDNZD": "AUD/NZD", "AUDJPY": "AUD/JPY", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY",
}

EPIC_MAP = {
    "CS.D.EURUSD.MINI.IP": "EURUSD",
    "CS.D.GBPUSD.MINI.IP": "GBPUSD",
    "CS.D.USDJPY.MINI.IP": "USDJPY",
    "CS.D.USDCHF.MINI.IP": "USDCHF",
    "CS.D.AUDUSD.MINI.IP": "AUDUSD",
    "CS.D.USDCAD.MINI.IP": "USDCAD",
    "CS.D.NZDUSD.MINI.IP": "NZDUSD",
    "CS.D.EURGBP.MINI.IP": "EURGBP",
    "CS.D.EURJPY.MINI.IP": "EURJPY",
    "CS.D.GBPJPY.MINI.IP": "GBPJPY",
    "CS.D.EURCHF.MINI.IP": "EURCHF",
    "CS.D.GBPCHF.MINI.IP": "GBPCHF",
    "CS.D.EURAUD.MINI.IP": "EURAUD",
    "CS.D.GBPAUD.MINI.IP": "GBPAUD",
    "CS.D.EURCAD.MINI.IP": "EURCAD",
    "CS.D.GBPCAD.MINI.IP": "GBPCAD",
    "CS.D.AUDNZD.MINI.IP": "AUDNZD",
    "CS.D.AUDJPY.MINI.IP": "AUDJPY",
    "CS.D.CADJPY.MINI.IP": "CADJPY",
    "CS.D.NZDJPY.MINI.IP": "NZDJPY",
}


def _pair(instrument):
    if not instrument:
        return instrument
    return PAIR_DISPLAY.get(instrument.upper().replace("/", ""), instrument)


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def _get_conn():
    global _conn
    if _conn is not None:
        try:
            cur = _conn.cursor(); cur.execute("SELECT 1"); cur.close()
            return _conn
        except Exception:
            try: _conn.close()
            except: pass
            _conn = None
    ssm = boto3.client("ssm", region_name=_REGION)
    sm  = boto3.client("secretsmanager", region_name=_REGION)
    endpoint = ssm.get_parameter(
        Name=os.environ.get("RDS_SSM_PARAM", "/platform/config/rds-endpoint")
    )["Parameter"]["Value"].split(":")[0]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    _conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, sslmode="require",
        options="-c search_path=forex_network,shared,public",
    )
    _conn.autocommit = True
    return _conn


def _resp(status, body):
    return {"statusCode": status,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, default=_json_default)}


def _ig_auth(creds):
    """POST /session -> (cst, token) or raises."""
    body = json.dumps({"identifier": creds["username"],
                       "password":   creds["password"]}).encode()
    req  = urllib.request.Request(
        f"{_IG_BASE}/session",
        data=body,
        headers={"Content-Type": "application/json",
                 "X-IG-API-KEY": creds["api_key"],
                 "Version": "2"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        cst   = r.headers.get("CST")
        token = r.headers.get("X-SECURITY-TOKEN")
    if not cst or not token:
        raise Exception("IG auth failed — missing CST or X-SECURITY-TOKEN")
    return cst, token


def _ig_live_positions():
    """GET /positions -> {instrument: {current_price, unrealised_pnl}} or {} on error."""
    try:
        sm    = boto3.client("secretsmanager", region_name=_REGION)
        creds = json.loads(sm.get_secret_value(
            SecretId="platform/ig-markets/demo-credentials")["SecretString"])
        cst, token = _ig_auth(creds)
        req = urllib.request.Request(
            f"{_IG_BASE}/positions",
            headers={"X-IG-API-KEY":     creds["api_key"],
                     "CST":              cst,
                     "X-SECURITY-TOKEN": token,
                     "Version":          "2"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))

        out = {}
        for pos in data.get("positions", []):
            market    = pos.get("market", {})
            position  = pos.get("position", {})
            epic      = market.get("epic", "")
            instrument = EPIC_MAP.get(epic, epic)
            direction  = position.get("direction", "BUY")
            bid        = market.get("bid")
            offer      = market.get("offer")
            level      = position.get("level")        # entry price
            size       = position.get("size", 0)      # lots
            csize      = position.get("contractSize", 10000)  # units per lot
            # Current close price: bid for long (BUY), offer for short (SELL)
            if direction == "BUY":
                current_price = float(bid)   if bid   is not None else None
                upl = (float(bid) - float(level)) * size * csize if (bid is not None and level is not None) else None
            else:
                current_price = float(offer) if offer is not None else None
                upl = (float(level) - float(offer)) * size * csize if (offer is not None and level is not None) else None
            # IG MINI CFDs: upl is in quote currency (USD for most pairs)
            out[instrument] = {
                "current_price":  current_price,
                "unrealised_pnl": round(upl, 2) if upl is not None else None,
            }
        return out
    except Exception as e:
        print(f"IG live positions failed: {e}")
        return {}


def handler(event, context):
    try:
        claims  = (event.get("requestContext", {}) or {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
        user_id = claims.get("sub")
        if not user_id:
            return _resp(401, {"error": "missing_sub_claim"})

        conn = _get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id, instrument, direction, entry_price, stop_price, target_price,
                   position_size, position_size_usd, entry_time,
                   convergence_score, agents_agreed, regime_at_entry,
                   session_at_entry, trailing_stop_pct
            FROM forex_network.trades
            WHERE user_id = %s::uuid AND exit_time IS NULL
            ORDER BY entry_time DESC
            """,
            (user_id,),
        )
        open_trades = cur.fetchall()

        live = _ig_live_positions()

        out = []
        for t in open_trades:
            instrument_db = t["instrument"]
            key           = (instrument_db or "").upper().replace("/", "")
            live_pos      = live.get(key) or {}
            entry         = float(t["entry_price"]) if t["entry_price"] is not None else None
            current       = live_pos.get("current_price")
            unreal        = live_pos.get("unrealised_pnl")

            # Pips delta — uses live current_price when available
            unreal_pips = None
            if current is not None and entry is not None:
                direction = t["direction"] or "long"
                delta     = (current - entry) if direction == "long" else (entry - current)
                pip_size  = 0.01 if "JPY" in (instrument_db or "") else 0.0001
                unreal_pips = round(delta / pip_size, 1)

            out.append({
                "id":                  int(t["id"]),
                "instrument":          _pair(instrument_db),
                "direction":           t["direction"],
                "entry_price":         entry,
                "current_price":       current,
                "stop_price":          float(t["stop_price"])    if t["stop_price"]    is not None else None,
                "target_price":        float(t["target_price"])  if t["target_price"]  is not None else None,
                "position_size":       float(t["position_size"]) if t["position_size"] is not None else None,
                "position_size_usd":   float(t["position_size_usd"]) if t["position_size_usd"] is not None else None,
                "unrealised_pnl":      unreal,
                "unrealised_pips":     unreal_pips,
                "entry_time":          t["entry_time"].strftime("%Y-%m-%d %H:%M") if t["entry_time"] else None,
                "session_at_entry":    t["session_at_entry"],
                "convergence_score":   float(t["convergence_score"]) if t["convergence_score"] is not None else None,
                "agents_agreed":       t["agents_agreed"],
                "trailing_stop_pct":   float(t["trailing_stop_pct"]) if t["trailing_stop_pct"] is not None else None,
                "trailing_stop_active": bool(t["trailing_stop_pct"] and t["trailing_stop_pct"] > 0),
                "regime_at_entry":     t["regime_at_entry"],
            })

        return _resp(200, out)
    except Exception as e:
        return _resp(500, {"error": "positions_failed", "message": str(e)})
