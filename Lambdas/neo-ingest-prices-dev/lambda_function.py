"""
neo-ingest-prices-dev
Nightly Lambda (22:30 UTC Mon-Fri) that backfills missing bars from TraderMade
into forex_network.historical_prices and forex_network.price_metrics.

Supports all 7 FX pairs × 3 timeframes (1D, 1H, 15M).
15M bars are fetched as 1-minute data and resampled.
ATR_14 and realised vol are computed in pure Python — no pandas/numpy required.
"""

import os
import json
import math
import time
import boto3
import psycopg2
import psycopg2.extras
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
PAIRS     = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURNZD", "AUDCAD"]
TIMEFRAMES = ["1D", "1H", "15M"]
REGION    = "eu-west-2"
TM_BASE   = "https://marketdata.tradermade.com/api/v1/timeseries"

# Max lookback caps — prevent enormous catch-ups if the Lambda was down for weeks
MAX_LOOKBACK = {
    "1D":  timedelta(days=14),
    "1H":  timedelta(hours=14 * 24),
    "15M": timedelta(days=7),
}

# ── DB / credential helpers ────────────────────────────────────────────────────
def _get_db_conn():
    sm  = boto3.client("secretsmanager", region_name=REGION)
    ssm = boto3.client("ssm",            region_name=REGION)
    creds = json.loads(
        sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"]
    )
    ep = ssm.get_parameter(
        Name=os.environ["RDS_SSM_PARAM"], WithDecryption=True
    )["Parameter"]["Value"]
    conn = psycopg2.connect(
        host=ep, dbname="postgres",
        user=creds["username"], password=creds["password"],
        options="-c search_path=forex_network,shared,public",
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


def _get_tm_key():
    sm = boto3.client("secretsmanager", region_name=REGION)
    data = json.loads(
        sm.get_secret_value(SecretId="platform/tradermade/api-key")["SecretString"]
    )
    return data["api_key"]


# ── TraderMade fetcher ────────────────────────────────────────────────────────
def _tm_fetch(pair: str, interval: str, start_str: str, end_str: str, api_key: str):
    """
    Fetch OHLC bars from TraderMade timeseries endpoint.
    Returns list of dicts: {date, open, high, low, close}
    Raises on non-200 or empty response after retries.
    """
    params = urllib.parse.urlencode({
        "currency":   pair,
        "api_key":    api_key,
        "start_date": start_str,
        "end_date":   end_str,
        "format":     "records",
        "interval":   interval,
    })
    url = f"{TM_BASE}?{params}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                body = resp.read().decode()
            data = json.loads(body)
            return data.get("quotes", [])
        except Exception as exc:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return []


# ── 15M resampling (pure Python) ──────────────────────────────────────────────
def _resample_to_15m(minute_quotes: list) -> list:
    """
    Aggregate 1-minute OHLC bars into 15-minute bars.
    Each bar is aligned to the floor of the 15-minute boundary.
    """
    if not minute_quotes:
        return []

    buckets = {}   # ts_str → {open, high, low, close, seq}
    for q in minute_quotes:
        # Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM"
        raw = q["date"]
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M")

        # Floor to 15-minute boundary
        floored = dt.replace(minute=(dt.minute // 15) * 15, second=0)
        key = floored.strftime("%Y-%m-%d %H:%M:%S")

        o = float(q["open"])
        h = float(q["high"])
        l = float(q["low"])
        c = float(q["close"])

        if key not in buckets:
            buckets[key] = {"open": o, "high": h, "low": l, "close": c}
        else:
            b = buckets[key]
            b["high"]  = max(b["high"], h)
            b["low"]   = min(b["low"],  l)
            b["close"] = c   # last close wins

    result = []
    for key in sorted(buckets):
        b = buckets[key]
        result.append({
            "date":  key,
            "open":  b["open"],
            "high":  b["high"],
            "low":   b["low"],
            "close": b["close"],
        })
    return result


# ── Price metrics — pure Python ───────────────────────────────────────────────
def _compute_metrics(context_bars: list, new_bars: list) -> list:
    """
    Compute ATR_14, realised_vol_14, realised_vol_30 for each bar in new_bars.
    context_bars: list of {ts, open, high, low, close} already in DB (oldest→newest)
    new_bars:     list of {date, open, high, low, close} from TraderMade

    Returns list of {ts, atr_14, rv_14, rv_30} for new_bars only.
    """
    # Build unified bar list: [ts_utc, open, high, low, close]
    combined = []
    for b in context_bars:
        ts = b["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        combined.append((ts, float(b["open"]), float(b["high"]),
                         float(b["low"]), float(b["close"])))

    # Parse new bars
    new_parsed = []
    for q in new_bars:
        raw = q["date"]
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M")
            except ValueError:
                dt = datetime.strptime(raw, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        new_parsed.append((dt, float(q["open"]), float(q["high"]),
                           float(q["low"]), float(q["close"])))

    combined.extend(new_parsed)
    n = len(combined)

    # True Range
    tr_list = []
    for i, (ts, o, h, l, c) in enumerate(combined):
        if i == 0:
            tr = h - l
        else:
            prev_c = combined[i - 1][4]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_list.append(tr)

    # Wilder ATR (EMA alpha=1/14)
    alpha = 1.0 / 14
    atr_list = []
    ema = None
    for tr in tr_list:
        ema = tr if ema is None else alpha * tr + (1 - alpha) * ema
        atr_list.append(ema)

    # Log returns
    log_ret = [None]   # first bar has no prior close
    for i in range(1, n):
        prev_c = combined[i - 1][4]
        curr_c = combined[i][4]
        log_ret.append(math.log(curr_c / prev_c) if prev_c > 0 else None)

    # Rolling std (sample) for realised vol
    def _rolling_std(values, window, start_i):
        results = []
        for i in range(start_i, len(values)):
            window_vals = [v for v in values[max(0, i - window + 1): i + 1]
                           if v is not None]
            if len(window_vals) < 2:
                results.append(None)
            else:
                mean = sum(window_vals) / len(window_vals)
                var  = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
                results.append(math.sqrt(var))
        return results

    new_start = n - len(new_parsed)
    rv14_list = _rolling_std(log_ret, 14, new_start)
    rv30_list = _rolling_std(log_ret, 30, new_start)

    # Package results
    results = []
    for i, (ts, _, _, _, _) in enumerate(new_parsed):
        results.append({
            "ts":    ts,
            "atr_14": atr_list[new_start + i],
            "rv_14":  rv14_list[i],
            "rv_30":  rv30_list[i],
        })
    return results


# ── DB query helpers ──────────────────────────────────────────────────────────
def _get_last_bar_ts(conn, pair: str, timeframe: str):
    """Return timezone-aware datetime of the most recent bar, or None."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(ts) FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = %s
        """, (pair, timeframe))
        row = cur.fetchone()
        if row and row[0]:
            ts = row[0]
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return None


def _get_last_metric_ts(conn, pair: str, timeframe: str):
    """Return timezone-aware datetime of the most recent price_metrics row, or None."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(ts) FROM forex_network.price_metrics
            WHERE instrument = %s AND timeframe = %s
        """, (pair, timeframe))
        row = cur.fetchone()
        if row and row[0]:
            ts = row[0]
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return None


def _get_context_bars(conn, pair: str, timeframe: str, n: int = 60) -> list:
    """Return the last n bars as list of dicts (oldest first)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = %s
            ORDER BY ts DESC LIMIT %s
        """, (pair, timeframe, n))
        rows = cur.fetchall()
    return [dict(r) for r in reversed(rows)]


def _upsert_prices(conn, pair: str, timeframe: str, bars: list) -> int:
    """Insert bars into historical_prices. Returns count inserted."""
    if not bars:
        return 0
    count = 0
    with conn.cursor() as cur:
        for q in bars:
            raw = q["date"]
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(raw, "%Y-%m-%d %H:%M")
                except ValueError:
                    dt = datetime.strptime(raw, "%Y-%m-%d")
            ts = dt.replace(tzinfo=timezone.utc)
            cur.execute("""
                INSERT INTO forex_network.historical_prices
                    (instrument, timeframe, ts, open, high, low, close, session)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'utc')
                ON CONFLICT (instrument, timeframe, ts) DO UPDATE SET
                    atr_14=EXCLUDED.atr_14,
                    realised_vol_14=EXCLUDED.realised_vol_14,
                    realised_vol_30=EXCLUDED.realised_vol_30
            """, (pair, timeframe, ts,
                  float(q["open"]), float(q["high"]),
                  float(q["low"]),  float(q["close"])))
            count += cur.rowcount
    conn.commit()
    return count


def _upsert_metrics(conn, pair: str, timeframe: str, metrics: list) -> int:
    """Insert computed metrics into price_metrics. Returns count inserted."""
    if not metrics:
        return 0
    count = 0
    with conn.cursor() as cur:
        for m in metrics:
            if m["atr_14"] is None:
                continue
            ts = m["ts"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            cur.execute("""
                INSERT INTO forex_network.price_metrics
                    (instrument, timeframe, ts, atr_14, realised_vol_14, realised_vol_30)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (instrument, timeframe, ts) DO NOTHING
            """, (pair, timeframe, ts,
                  m["atr_14"],
                  m["rv_14"],
                  m["rv_30"]))
            count += cur.rowcount
    conn.commit()
    return count


def _log_audit(conn, description: str, metadata: dict) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forex_network.audit_log
                    (event_type, description, metadata, source, created_at)
                VALUES ('price_ingest', %s, %s, 'neo-ingest-prices-dev', NOW())
            """, (description, json.dumps(metadata)))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[WARN] audit_log write failed: {exc}")


# ── Per-pair/timeframe ingest logic ───────────────────────────────────────────
def _ingest_one(conn, pair: str, timeframe: str, api_key: str, now: datetime) -> dict:
    """
    Backfill one pair × timeframe combination.
    Returns {"price_rows": int, "metric_rows": int, "bars_fetched": int, "skipped": bool, "error": str|None}
    """
    result = {"price_rows": 0, "metric_rows": 0, "bars_fetched": 0, "skipped": False, "error": None}

    last_ts = _get_last_bar_ts(conn, pair, timeframe)
    if last_ts is None:
        result["skipped"] = True
        result["error"] = "no existing bars (bulk import required first)"
        return result

    # Compute start/end for fetch
    if timeframe == "1D":
        # Next calendar day after last bar
        start_dt = last_ts + timedelta(days=1)
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # End: today — TraderMade rejects future end_date with HTTP 400
        end_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "1H":
        start_dt = last_ts + timedelta(hours=1)
        start_dt = start_dt.replace(minute=0, second=0, microsecond=0)
        # Last complete hour bar before now
        end_dt = now.replace(minute=0, second=0, microsecond=0)
    else:  # 15M (fetched as minute, resampled)
        start_dt = last_ts + timedelta(minutes=15)
        start_dt = start_dt.replace(second=0, microsecond=0)
        # Last complete 15M bar
        floor_min = (now.minute // 15) * 15
        end_dt = now.replace(minute=floor_min, second=0, microsecond=0)

    # Apply max lookback cap
    cap_start = now - MAX_LOOKBACK[timeframe]
    if start_dt < cap_start:
        print(f"  [{pair}/{timeframe}] capping lookback from {start_dt.date()} to {cap_start.date()}")
        start_dt = cap_start

    if start_dt >= end_dt:
        # Bars are current — but check if price_metrics also needs updating
        metrics_ts = _get_last_metric_ts(conn, pair, timeframe)
        prices_ts  = _get_last_bar_ts(conn, pair, timeframe)
        if metrics_ts is not None and prices_ts is not None and metrics_ts >= prices_ts:
            result["skipped"] = True
            return result  # both current
        # else: bars current but metrics stale — fall through to metrics recalc
        all_bars = _get_context_bars(conn, pair, timeframe, 60)
        if not all_bars:
            result["skipped"] = True
            return result
        context  = all_bars
        p_count  = 0
        result["price_rows"] = 0

    # Format date strings for TraderMade
    if timeframe == "1D":
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str   = end_dt.strftime("%Y-%m-%d")
        tm_interval = "daily"
        all_bars = _tm_fetch(pair, tm_interval, start_str, end_str, api_key)

    elif timeframe == "1H":
        start_str = start_dt.strftime("%Y-%m-%d %H:%M")
        end_str   = end_dt.strftime("%Y-%m-%d %H:%M")
        tm_interval = "hourly"
        all_bars = _tm_fetch(pair, tm_interval, start_str, end_str, api_key)

    else:  # 15M — fetch minute data in 24h chunks, resample
        all_bars = []
        chunk_start = start_dt
        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(hours=24), end_dt)
            minute_bars = _tm_fetch(
                pair, "minute",
                chunk_start.strftime("%Y-%m-%d %H:%M"),
                chunk_end.strftime("%Y-%m-%d %H:%M"),
                api_key,
            )
            if minute_bars:
                all_bars.extend(_resample_to_15m(minute_bars))
            chunk_start = chunk_end
            if chunk_start < end_dt:
                time.sleep(0.4)   # gentle rate-limit spacing between minute chunks

    result["bars_fetched"] = len(all_bars)
    if not all_bars:
        result["skipped"] = True
        return result

    # Get context bars for metrics calculation
    context = _get_context_bars(conn, pair, timeframe, 60)

    # Upsert prices
    p_count = _upsert_prices(conn, pair, timeframe, all_bars)
    result["price_rows"] = p_count

    # Compute and upsert metrics
    if context:
        metrics = _compute_metrics(context, all_bars)
        m_count = _upsert_metrics(conn, pair, timeframe, metrics)
        result["metric_rows"] = m_count

    return result


# ── Lambda handler ─────────────────────────────────────────────────────────────
def handler(event, context):
    now = datetime.now(timezone.utc)
    print(f"[neo-ingest-prices-dev] Starting at {now.isoformat()}")

    conn     = _get_db_conn()
    api_key  = _get_tm_key()

    summary = {
        "run_at":          now.isoformat(),
        "total_price_rows":  0,
        "total_metric_rows": 0,
        "pairs_processed":   [],
        "pairs_skipped":     [],
        "errors":            [],
    }

    for pair in PAIRS:
        for tf in TIMEFRAMES:
            label = f"{pair}/{tf}"
            try:
                r = _ingest_one(conn, pair, tf, api_key, now)
                if r["skipped"] and not r["error"]:
                    summary["pairs_skipped"].append(label)
                    print(f"  [SKIP] {label} — already current")
                elif r["error"]:
                    summary["errors"].append(f"{label}: {r['error']}")
                    print(f"  [SKIP] {label} — {r['error']}")
                else:
                    summary["total_price_rows"]  += r["price_rows"]
                    summary["total_metric_rows"] += r["metric_rows"]
                    summary["pairs_processed"].append(
                        f"{label}: fetched={r['bars_fetched']} "
                        f"price_rows={r['price_rows']} metric_rows={r['metric_rows']}"
                    )
                    print(f"  [OK]   {label}: "
                          f"fetched {r['bars_fetched']} bars, "
                          f"+{r['price_rows']} price, "
                          f"+{r['metric_rows']} metrics")
            except Exception as exc:
                msg = f"{type(exc).__name__}: {str(exc)[:300]}"
                summary["errors"].append(f"{label}: {msg}")
                print(f"  [ERR]  {label}: {msg}")
                try:
                    conn.rollback()
                except Exception:
                    pass

    # Write to audit_log
    _now = datetime.now(timezone.utc)
    _suffix = ""
    if (summary['total_price_rows'] == 0 and summary['total_metric_rows'] == 0
            and _now.weekday() >= 5):
        _suffix = " (weekend — no new data expected)"
    desc = (f"Nightly price ingest complete — "
            f"{summary['total_price_rows']} price rows, "
            f"{summary['total_metric_rows']} metric rows, "
            f"{len(summary['errors'])} errors{_suffix}")
    _log_audit(conn, desc, summary)
    conn.close()

    print(f"[neo-ingest-prices-dev] Done: {desc}")
    return {"statusCode": 200, "body": json.dumps(summary, default=str)}
