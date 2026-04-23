"""neo-ingest-ig-sentiment-dev
EventBridge: cron(30 * ? * MON-FRI *) — hourly at :30 on weekdays.

Fetches from IG Markets REST API:
  1. FX client sentiment — 7 pairs
  2. Cross-asset sentiment — US500, DE30, FT100
  3. DXY daily price — CO.D.DX.Month2.IP

Writes:
  - Sentiment rows  → forex_network.ig_client_sentiment
  - DXY price       → forex_network.cross_asset_prices (instrument='DXY', timeframe='1D')
  - API calls       → forex_network.api_call_log (provider='ig_markets')
"""
import os, json, time, urllib.request, urllib.error, boto3, psycopg2, psycopg2.extras
from datetime import datetime, timezone

_REGION   = "eu-west-2"
_IG_BASE  = "https://api.ig.com/gateway/deal"
_DXY_EPIC = "CO.D.DX.Month2.IP"

# TraderMade /historical endpoint for UK100 and UKOIL daily closes
_TRADERMADE_SECRET = "platform/tradermade/api-key"
_TRADERMADE_HIST   = "https://marketdata.tradermade.com/api/v1/historical"
_TM_CFD_INSTRUMENTS = ["UK100", "UKOIL"]

# FX pairs: sentiment API market ID → instrument name stored in DB
FX_MARKET_IDS = {
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDJPY": "USDJPY",
    "USDCHF": "USDCHF",
    "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD",
    "NZDUSD": "NZDUSD",
    "EURGBP": "EURGBP",
    "EURJPY": "EURJPY",
    "GBPJPY": "GBPJPY",
    "EURCHF": "EURCHF",
    "GBPCHF": "GBPCHF",
    "EURAUD": "EURAUD",
    "GBPAUD": "GBPAUD",
    "EURCAD": "EURCAD",
    "GBPCAD": "GBPCAD",
    "AUDNZD": "AUDNZD",
    "AUDJPY": "AUDJPY",
    "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY",
}

# Cross-asset: sentiment API market ID → instrument name stored in DB
CROSS_MARKET_IDS = {
    "US500": "SPX500",
    "DE30":  "GER30",
    "FT100": "UK100",
}

# DXY prices from IG are already in standard index points (no scaling needed)
# FX prices from IG are scaled ×10,000 (e.g. 11760.7 = 1.17607 EURUSD)


def _get_db_conn():
    sm  = boto3.client("secretsmanager", region_name=_REGION)
    ssm = boto3.client("ssm", region_name=_REGION)
    ep  = ssm.get_parameter(
        Name=os.environ.get("RDS_SSM_PARAM", "/platform/config/rds-endpoint")
    )["Parameter"]["Value"].split(":")[0]
    cr  = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=ep, port=5432, dbname="postgres",
        user=cr["username"], password=cr["password"],
        connect_timeout=10, sslmode="require",
        options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    return conn


def _get_ig_creds():
    sm  = boto3.client("secretsmanager", region_name=_REGION)
    raw = json.loads(sm.get_secret_value(SecretId="platform/ig-markets/credentials")["SecretString"])
    return raw["api_key"], raw["username"], raw["password"]


def _ig_login(api_key, username, password):
    """POST /session → (cst, security_token). Raises on failure."""
    payload = json.dumps({"identifier": username, "password": password}).encode()
    req = urllib.request.Request(
        f"{_IG_BASE}/session",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-IG-API-KEY": api_key,
            "Version": "2",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            cst   = r.headers.get("CST")
            token = r.headers.get("X-SECURITY-TOKEN")
            if not cst or not token:
                raise RuntimeError("CST/X-SECURITY-TOKEN missing from session response")
            return cst, token
    except urllib.error.HTTPError as e:
        # IG returns tokens even on 4xx (e.g. pending agreements)
        cst   = e.headers.get("CST")
        token = e.headers.get("X-SECURITY-TOKEN")
        if cst and token:
            return cst, token
        raise RuntimeError(f"IG login failed HTTP {e.code}: {e.read(200)}")


def _ig_logout(api_key, cst, token):
    try:
        req = urllib.request.Request(
            f"{_IG_BASE}/session",
            headers={"X-IG-API-KEY": api_key, "CST": cst, "X-SECURITY-TOKEN": token},
            method="DELETE",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _ig_get(api_key, cst, token, path):
    """GET {_IG_BASE}{path} with auth headers. Returns parsed JSON dict."""
    req = urllib.request.Request(
        f"{_IG_BASE}{path}",
        headers={
            "Accept": "application/json",
            "X-IG-API-KEY": api_key,
            "CST": cst,
            "X-SECURITY-TOKEN": token,
            # NOTE: do NOT send Version header — Version:3 silently breaks /prices
        },
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def _log_call(cur, success, elapsed_ms, pairs_returned, error_type=None):
    try:
        cur.execute("""
            INSERT INTO forex_network.api_call_log
                (provider, success, response_time_ms, pairs_returned, error_type, called_at)
            VALUES ('ig_markets', %s, %s, %s, %s, NOW())
        """, (success, elapsed_ms, pairs_returned, error_type))
    except Exception:
        pass


def _fetch_tradermade_daily(tm_key: str, instrument: str, date_str: str) -> float | None:
    """Fetch single-day close from TraderMade /historical. Returns None on error."""
    import urllib.request as _ur
    url = (
        f"{_TRADERMADE_HIST}?currency={instrument}"
        f"&date={date_str}&api_key={tm_key}"
    )
    try:
        req = _ur.Request(url, headers={"User-Agent": "neo-ingest-ig-sentiment/1.0"})
        with _ur.urlopen(req, timeout=10) as r:
            data = __import__("json").loads(r.read())
        quotes = data.get("quotes", [])
        return float(quotes[0]["close"]) if quotes and "close" in quotes[0] else None
    except Exception as _e:
        print(f"[ig-sentiment] TraderMade {instrument} {date_str}: {_e}")
        return None


def handler(event, context):
    t0   = time.time()
    now  = datetime.now(timezone.utc)
    conn = _get_db_conn()
    cur  = conn.cursor()

    # ── Credentials ──────────────────────────────────────────────────────────
    try:
        api_key, username, password = _get_ig_creds()
    except Exception as e:
        _log_call(cur, False, int((time.time()-t0)*1000), 0, f"cred_error:{str(e)[:80]}")
        cur.close(); conn.close()
        return {"statusCode": 500, "body": json.dumps({"error": "credential_fetch_failed", "detail": str(e)})}

    # ── Login ─────────────────────────────────────────────────────────────────
    try:
        cst, token = _ig_login(api_key, username, password)
    except Exception as e:
        _log_call(cur, False, int((time.time()-t0)*1000), 0, f"login_error:{str(e)[:80]}")
        cur.close(); conn.close()
        return {"statusCode": 500, "body": json.dumps({"error": "ig_login_failed", "detail": str(e)})}

    results = {"fx_sentiment": [], "cross_sentiment": [], "dxy": None, "errors": []}

    # ── FX Sentiment ──────────────────────────────────────────────────────────
    try:
        fx_ids = ",".join(FX_MARKET_IDS.keys())
        data   = _ig_get(api_key, cst, token, f"/clientsentiment?marketIds={fx_ids}")
        for s in data.get("clientSentiments", []):
            mid      = s["marketId"]
            inst     = FX_MARKET_IDS.get(mid, mid)
            long_pct = s["longPositionPercentage"]
            short_pct= s["shortPositionPercentage"]
            cur.execute("""
                INSERT INTO forex_network.ig_client_sentiment
                    (instrument, long_percentage, short_percentage, market_id, ts)
                VALUES (%s, %s, %s, %s, %s)
            """, (inst, long_pct, short_pct, mid, now))
            results["fx_sentiment"].append({"instrument": inst, "long": long_pct, "short": short_pct})
    except Exception as e:
        results["errors"].append(f"fx_sentiment: {e}")

    # ── Cross-Asset Sentiment ─────────────────────────────────────────────────
    try:
        ca_ids = ",".join(CROSS_MARKET_IDS.keys())
        data   = _ig_get(api_key, cst, token, f"/clientsentiment?marketIds={ca_ids}")
        for s in data.get("clientSentiments", []):
            mid      = s["marketId"]
            inst     = CROSS_MARKET_IDS.get(mid, mid)
            long_pct = s["longPositionPercentage"]
            short_pct= s["shortPositionPercentage"]
            cur.execute("""
                INSERT INTO forex_network.ig_client_sentiment
                    (instrument, long_percentage, short_percentage, market_id, ts)
                VALUES (%s, %s, %s, %s, %s)
            """, (inst, long_pct, short_pct, mid, now))
            results["cross_sentiment"].append({"instrument": inst, "long": long_pct, "short": short_pct})
    except Exception as e:
        results["errors"].append(f"cross_sentiment: {e}")

    # ── DXY Daily Price ───────────────────────────────────────────────────────
    try:
        data   = _ig_get(api_key, cst, token, f"/prices/{_DXY_EPIC}/DAY/1")
        prices = data.get("prices", [])
        if prices:
            p      = prices[-1]
            ts_str = p.get("snapshotTime", "")          # e.g. "2026:04:19-00:00:00"
            # Parse IG time format
            try:
                bar_time = datetime.strptime(ts_str, "%Y:%m:%d-%H:%M:%S")
            except ValueError:
                bar_time = now.replace(tzinfo=None)
            op = p.get("openPrice",  {}).get("bid") or p.get("openPrice",  {}).get("ask")
            hi = p.get("highPrice",  {}).get("bid") or p.get("highPrice",  {}).get("ask")
            lo = p.get("lowPrice",   {}).get("bid") or p.get("lowPrice",   {}).get("ask")
            cl = p.get("closePrice", {}).get("bid") or p.get("closePrice", {}).get("ask")
            if cl:
                # DXY from IG is in hundredths — e.g. 9789.8 = 97.898
                dxy_close = round(cl / 100.0, 4)
                dxy_open  = round(op / 100.0, 4) if op else dxy_close
                dxy_high  = round(hi / 100.0, 4) if hi else dxy_close
                dxy_low   = round(lo / 100.0, 4) if lo else dxy_close
                cur.execute("""
                    INSERT INTO forex_network.cross_asset_prices
                        (instrument, timeframe, bar_time, open, high, low, close, created_at)
                    VALUES ('DXY', '1D', %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                """, (bar_time, dxy_open, dxy_high, dxy_low, dxy_close))
                results["dxy"] = {"close": dxy_close, "open": dxy_open, "high": dxy_high, "low": dxy_low, "bar_time": str(bar_time)}
    except Exception as e:
        results["errors"].append(f"dxy_price: {e}")

    # ── UK100 + UKOIL Daily Price (TraderMade /historical) ──────────────────
    try:
        import json as _json
        sm_cl = boto3.client("secretsmanager", region_name=_REGION)
        tm_key = _json.loads(
            sm_cl.get_secret_value(SecretId=_TRADERMADE_SECRET)["SecretString"]
        )["api_key"]
        today_str = now.strftime("%Y-%m-%d")
        for _tm_inst in _TM_CFD_INSTRUMENTS:
            _close = _fetch_tradermade_daily(tm_key, _tm_inst, today_str)
            if _close is not None:
                cur.execute("""
                    INSERT INTO forex_network.cross_asset_prices
                        (instrument, timeframe, bar_time, open, high, low, close)
                    VALUES (%s, '1D', %s, %s, %s, %s, %s)
                    ON CONFLICT (instrument, timeframe, bar_time)
                    DO UPDATE SET close = EXCLUDED.close
                    WHERE cross_asset_prices.close IS DISTINCT FROM EXCLUDED.close
                """, (_tm_inst, now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None),
                      _close, _close, _close, _close))
                conn.commit()
                results[_tm_inst.lower() + "_price"] = _close
                print(f"[ig-sentiment] {_tm_inst} daily close: {_close:.2f}")
    except Exception as e:
        results["errors"].append(f"tradermade_prices: {e}")

    # ── Logout ────────────────────────────────────────────────────────────────
    _ig_logout(api_key, cst, token)

    elapsed_ms  = int((time.time() - t0) * 1000)
    total_rows  = len(results["fx_sentiment"]) + len(results["cross_sentiment"])
    _log_call(cur, len(results["errors"]) == 0, elapsed_ms, total_rows,
              "; ".join(results["errors"])[:200] if results["errors"] else None)

    cur.close(); conn.close()
    return {
        "statusCode": 200,
        "body": json.dumps({
            "fx_pairs":      len(results["fx_sentiment"]),
            "cross_assets":  len(results["cross_sentiment"]),
            "dxy":           results["dxy"],
            "fx_sentiment":  results["fx_sentiment"],
            "cross_sentiment": results["cross_sentiment"],
            "errors":        results["errors"],
            "elapsed_ms":    elapsed_ms,
            "ts":            now.isoformat(),
        }),
    }
