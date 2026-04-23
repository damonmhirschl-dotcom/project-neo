"""
neo-ingest-bond-yields-dev
Refreshes forex_network.cross_asset_prices with daily EODHD bond yield data.
Instruments: US10Y, US2Y, US3M, UK10Y, DE10Y, JP10Y (GBOND exchange).
Runs at 22:00 UTC Mon-Fri (after market close).
Upserts on (instrument, timeframe, bar_time).
"""

import json
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
import psycopg2

REGION          = "eu-west-2"
EODHD_SECRET_ID = "platform/eodhd/api-key"
EODHD_BASE      = "https://eodhistoricaldata.com/api/eod"
INSTRUMENTS     = [
    # Bond yields
    ("US10Y",  "US10Y.GBOND"),
    ("US2Y",   "US2Y.GBOND"),
    ("US3M",   "US3M.GBOND"),
    ("UK10Y",  "UK10Y.GBOND"),
    ("DE10Y",  "DE10Y.GBOND"),
    ("JP10Y",  "JP10Y.GBOND"),
    # Equity indices
    ("SPX500", "GSPC.INDX"),
    ("GER30",  "GDAXI.INDX"),
    # Precious metals
    ("XAUUSD", "XAUUSD.FOREX"),
    ("XAGUSD", "XAGUSD.FOREX"),
]
LOOKBACK_DAYS   = 7   # fetch last 7 calendar days to catch any gaps / late postings


# ── AWS helpers ────────────────────────────────────────────────────────────────

def get_eodhd_key():
    sm = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(sm.get_secret_value(SecretId=EODHD_SECRET_ID)["SecretString"])
    return secret["api_key"]


def get_db_conn():
    sm  = boto3.client("secretsmanager", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    ep    = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    return psycopg2.connect(
        host=ep, dbname="postgres",
        user=creds["username"], password=creds["password"],
        options="-c search_path=forex_network,shared,public",
        connect_timeout=10,
    )


# ── EODHD fetch ────────────────────────────────────────────────────────────────

def fetch_eod_series(ticker: str, api_key: str, from_date: str, to_date: str) -> list:
    """
    Returns list of {date, open, high, low, close} dicts.
    EODHD endpoint: GET /api/eod/{ticker}?from=...&to=...&fmt=json&api_token=...
    """
    url = (
        f"{EODHD_BASE}/{ticker}"
        f"?from={from_date}&to={to_date}"
        f"&period=d&fmt=json&api_token={api_key}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "neo-ingest-bond-yields/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        print(f"[bond-yields] WARN: fetch failed for {ticker}: {exc}")
        return []

    if not isinstance(data, list):
        print(f"[bond-yields] WARN: unexpected response type for {ticker}: {type(data)}")
        return []
    return data


# ── DB upsert ─────────────────────────────────────────────────────────────────

UPSERT_SQL = """
    INSERT INTO forex_network.cross_asset_prices
        (instrument, timeframe, bar_time, open, high, low, close)
    VALUES
        (%(instrument)s, %(timeframe)s, %(bar_time)s,
         %(open)s, %(high)s, %(low)s, %(close)s)
    ON CONFLICT (instrument, timeframe, bar_time)
    DO UPDATE SET
        open  = EXCLUDED.open,
        high  = EXCLUDED.high,
        low   = EXCLUDED.low,
        close = EXCLUDED.close
    WHERE
        cross_asset_prices.close IS DISTINCT FROM EXCLUDED.close
    RETURNING (xmax = 0) AS is_insert
"""


def upsert_series(cur, instrument: str, rows: list) -> tuple[int, int]:
    """Return (inserted, updated) counts for one instrument."""
    inserted = updated = 0
    for row in rows:
        date_str = row.get("date") or row.get("Date")
        close_v  = row.get("close") or row.get("adjusted_close")
        if not date_str or close_v is None:
            continue
        try:
            bar_time = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            open_v   = float(row.get("open",  close_v))
            high_v   = float(row.get("high",  close_v))
            low_v    = float(row.get("low",   close_v))
            close_v  = float(close_v)
        except (ValueError, TypeError) as exc:
            print(f"[bond-yields] WARN: bad row for {instrument} {date_str}: {exc}")
            continue

        cur.execute(UPSERT_SQL, {
            "instrument": instrument,
            "timeframe":  "1D",
            "bar_time":   bar_time,
            "open":       open_v,
            "high":       high_v,
            "low":        low_v,
            "close":      close_v,
        })
        result = cur.fetchone()
        if result is None:
            pass           # no change — conflict row already up to date
        elif result[0]:
            inserted += 1
        else:
            updated += 1
    return inserted, updated


def write_audit(cur, description: str, metadata: dict):
    cur.execute("""
        INSERT INTO forex_network.audit_log
            (user_id, event_type, description, metadata, created_at, source)
        VALUES
            (NULL, 'bond_yield_refresh', %s, %s, NOW(), 'neo-ingest-bond-yields-dev')
    """, (description, json.dumps(metadata)))


# ── Handler ────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    now       = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date   = now.strftime("%Y-%m-%d")

    print(f"[bond-yields] Fetching {from_date} → {to_date} for {len(INSTRUMENTS)} instruments")

    api_key = get_eodhd_key()

    totals = {}
    all_rows = {}
    for instrument, ticker in INSTRUMENTS:
        rows = fetch_eod_series(ticker, api_key, from_date, to_date)
        all_rows[instrument] = rows
        print(f"[bond-yields] {ticker}: {len(rows)} rows fetched")

    conn = get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                total_inserted = total_updated = 0
                for instrument, _ in INSTRUMENTS:
                    ins, upd = upsert_series(cur, instrument, all_rows[instrument])
                    totals[instrument] = {"inserted": ins, "updated": upd}
                    total_inserted += ins
                    total_updated  += upd
                    print(f"[bond-yields] {instrument}: {ins} inserted, {upd} updated")

                metadata = {
                    "from_date":      from_date,
                    "to_date":        to_date,
                    "total_inserted": total_inserted,
                    "total_updated":  total_updated,
                    "by_instrument":  totals,
                }
                suffix = ""
                if total_inserted == 0 and total_updated == 0 and now.weekday() >= 5:
                    suffix = " (weekend — no new data expected)"
                description = (
                    f"EODHD bond yield refresh {from_date}→{to_date}: "
                    f"{total_inserted} inserted, {total_updated} updated{suffix}"
                )
                write_audit(cur, description, metadata)

        print(f"[bond-yields] Done — {total_inserted} inserted, {total_updated} updated")
        return {"statusCode": 200, "body": metadata}

    finally:
        conn.close()
