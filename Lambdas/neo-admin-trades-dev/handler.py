"""neo-admin-trades-dev — GET /admin/trades/admin (all users, full detail)"""
import os, json, time, boto3, psycopg2, psycopg2.extras
from datetime import datetime, timezone
from decimal import Decimal

_REGION = "eu-west-2"
_conn = None

# USER_PROFILES removed — V1 Swing uses flat 1% risk for all users (2026-04-25)

PAIR_DISPLAY = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD", "USDCHF": "USD/CHF",
    "NZDUSD": "NZD/USD",
}


def _pair(instrument):
    if not instrument:
        return instrument
    return PAIR_DISPLAY.get(instrument.upper().replace("/", ""), instrument)


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime,)):
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
    sm = boto3.client("secretsmanager", region_name=_REGION)
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
    return {"statusCode": status, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, default=_json_default)}


def handler(event, context):
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, user_id, instrument, direction, entry_time, exit_time,
                   entry_price, exit_price, stop_price, target_price,
                   pnl, pnl_pips, convergence_score, agents_agreed,
                   regime_at_entry, session_at_entry, spread_at_entry,
                   requested_size, position_size, position_size_usd,
                   fill_pct, is_partial_fill, slippage_pips, fill_time_ms,
                   slippage_action, partial_fill_action,
                   swap_cost_pips, swap_cost_usd, hold_days,
                   return_pct, is_downside_return, exit_reason,
                   adx_at_entry, rsi_at_entry, setup_type, strategy, ig_deal_reference
            FROM forex_network.trades
            WHERE exit_time IS NOT NULL
            ORDER BY exit_time DESC
            LIMIT 500
        """)
        out = []
        for r in cur.fetchall():
            uid = str(r["user_id"]) if r["user_id"] else None
            exit_t = r["exit_time"]
            out.append({
                "id": int(r["id"]),
                "user_id": uid,
                "strategy": r["strategy"] or "v1_swing",
                "instrument": _pair(r["instrument"]),
                "direction": r["direction"],
                "entry_time": r["entry_time"].isoformat() if r["entry_time"] else None,
                "exit_time": exit_t.strftime("%H:%M") if exit_t else None,
                "exit_date": exit_t.strftime("%Y-%m-%d") if exit_t else None,
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                "stop_price": float(r["stop_price"]) if r["stop_price"] is not None else None,
                "target_price": float(r["target_price"]) if r["target_price"] is not None else None,
                "pnl": float(r["pnl"]) if r["pnl"] is not None else None,
                "pnl_pips": float(r["pnl_pips"]) if r["pnl_pips"] is not None else None,
                "conv": float(r["convergence_score"]) if r["convergence_score"] is not None else None,
                "agents": r["agents_agreed"],
                "regime": r["regime_at_entry"],
                "session": r["session_at_entry"],
                "spread_at_entry": float(r["spread_at_entry"]) if r["spread_at_entry"] is not None else None,
                "requested_size": float(r["requested_size"]) if r["requested_size"] is not None else None,
                "position_size": float(r["position_size"]) if r["position_size"] is not None else None,
                "position_size_usd": float(r["position_size_usd"]) if r["position_size_usd"] is not None else None,
                "fill_pct": float(r["fill_pct"]) if r["fill_pct"] is not None else None,
                "is_partial_fill": bool(r["is_partial_fill"]),
                "slippage_pips": float(r["slippage_pips"]) if r["slippage_pips"] is not None else None,
                "fill_time_ms": int(r["fill_time_ms"]) if r["fill_time_ms"] is not None else None,
                "slippage_action": r["slippage_action"],
                "partial_fill_action": r["partial_fill_action"],
                "swap_cost_pips": float(r["swap_cost_pips"]) if r["swap_cost_pips"] is not None else None,
                "swap_cost_usd": float(r["swap_cost_usd"]) if r["swap_cost_usd"] is not None else None,
                "hold_days": int(r["hold_days"]) if r["hold_days"] is not None else None,
                "return_pct": float(r["return_pct"]) if r["return_pct"] is not None else None,
                "exit_reason": r["exit_reason"],
                "adx_at_entry": float(r["adx_at_entry"]) if r["adx_at_entry"] is not None else None,
                "rsi_at_entry": float(r["rsi_at_entry"]) if r["rsi_at_entry"] is not None else None,
                "setup_type": r["setup_type"],
                "ig_deal_reference": r["ig_deal_reference"],
            })
        return _resp(200, out)
    except Exception as e:
        return _resp(500, {"error": "trades_failed", "message": str(e)})
