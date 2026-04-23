#!/usr/bin/env python3
"""
Backfills FRED macro data for all 8 currencies into economic_releases.
Yields, CPI (YoY computed from index), and unemployment going back ~25 years.

Fixes vs user draft:
  - Column is 'actual' not 'actual_value'
  - ON CONFLICT target: (source, country, indicator, release_time)
  - CPI YoY computed from index before insert (raw index would break scoring)
  - GBP CPI series corrected to GBRCPIALLMINMEI
  - Unemployment stores previous period rate in 'previous' column
  - Yield uses country=currency code (e.g. 'EUR') not 'EU'
  - EUR unemployment supplemented by Eurostat EA21 series (FRED ends ~2023)
"""
import requests, boto3, json, psycopg2, time, subprocess
from datetime import datetime

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_conn():
    ssm  = boto3.client('ssm', region_name='eu-west-2')
    sm   = boto3.client('secretsmanager', region_name='eu-west-2')
    ep   = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    cr   = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(host=ep, dbname='postgres',
        user=cr['username'], password=cr['password'],
        options='-c search_path=forex_network')

def get_fred_key():
    sm = boto3.client('secretsmanager', region_name='eu-west-2')
    return json.loads(sm.get_secret_value(SecretId='platform/fred/api-key')['SecretString'])['api_key']

# ---------------------------------------------------------------------------
# FRED series definitions
# country key = currency code stored in economic_releases
# ---------------------------------------------------------------------------

# 10Y government bond yields — already in % (e.g. 3.5 = 3.5% p.a.)
FRED_YIELDS = {
    'USD': 'IRLTLT01USM156N',
    'EUR': 'IRLTLT01EZM156N',
    'GBP': 'IRLTLT01GBM156N',
    'JPY': 'IRLTLT01JPM156N',
    'AUD': 'IRLTLT01AUM156N',
    'CAD': 'IRLTLT01CAM156N',
    'NZD': 'IRLTLT01NZM156N',
    'CHF': 'IRLTLT01CHM156N',
}

# CPI index series — YoY will be computed before inserting
# Tuple: (series_id, periods_for_yoy)  12=monthly, 4=quarterly
FRED_CPI = {
    'USD': ('CPIAUCSL',           12),
    'EUR': ('CP0000EZ19M086NEST', 12),
    'GBP': ('GBRCPIALLMINMEI',    12),   # corrected from GBKCPI...
    'JPY': ('JPNCPIALLMINMEI',    12),
    'AUD': ('AUSCPIALLQINMEI',     4),   # quarterly
    'CAD': ('CANCPIALLMINMEI',    12),
    'NZD': ('NZLCPIALLQINMEI',     4),   # quarterly
    'CHF': ('CHECPIALLMINMEI',    12),
}

# Unemployment rate (%)
FRED_UNEMPLOYMENT = {
    'USD': 'UNRATE',
    'EUR': 'LRHUTTTTEZM156S',
    'GBP': 'LRHUTTTTGBM156S',
    'JPY': 'LRHUTTTTJPM156S',
    'AUD': 'LRHUTTTTAUM156S',
    'CAD': 'LRHUTTTTCAM156S',
    'NZD': 'LRHUTTTNZQ156S',    # quarterly
    # CHF: no FRED unemployment rate series available
}

# ---------------------------------------------------------------------------
# FRED fetch
# ---------------------------------------------------------------------------

def fetch_fred(series_id, fred_key, start='2000-01-01'):
    r = requests.get(
        'https://api.stlouisfed.org/fred/series/observations',
        params={
            'series_id':         series_id,
            'api_key':           fred_key,
            'file_type':         'json',
            'observation_start': start,
            'sort_order':        'asc',
            'limit':             100000,
        },
        timeout=20,
    )
    if r.status_code != 200:
        print(f"    FRED API error {r.status_code} for {series_id}")
        return []
    obs = r.json().get('observations', [])
    return [o for o in obs if o['value'] != '.']

# ---------------------------------------------------------------------------
# YoY computation for CPI
# ---------------------------------------------------------------------------

def compute_yoy(obs, periods):
    """
    Given sorted raw index observations, return new list with
    value replaced by YoY % change.  periods=12 for monthly, 4 for quarterly.
    """
    result = []
    for i in range(periods, len(obs)):
        curr = obs[i]
        prev = obs[i - periods]
        try:
            yoy = (float(curr['value']) / float(prev['value']) - 1) * 100
            result.append({'date': curr['date'], 'value': str(round(yoy, 3))})
        except (ValueError, ZeroDivisionError):
            continue
    return result

# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO forex_network.economic_releases
        (source, country, indicator, release_time, actual, previous, unit, impact_level)
    VALUES ('FRED', %s, %s, %s, %s, %s, %s, 'HIGH')
    ON CONFLICT (source, country, indicator, release_time) DO UPDATE SET
        actual   = EXCLUDED.actual,
        previous = EXCLUDED.previous
"""

def insert_yield_series(conn, currency, obs):
    """Inserts monthly yield observations. previous = prior period yield."""
    cur = conn.cursor()
    rows = 0
    for i, o in enumerate(obs):
        prev_val = float(obs[i-1]['value']) if i > 0 else None
        cur.execute(UPSERT_SQL, (
            currency, f'{currency}10Y_YIELD',
            o['date'], float(o['value']), prev_val, '%'
        ))
        rows += 1
    conn.commit()
    return rows

def insert_cpi_series(conn, currency, obs, periods):
    """Computes YoY then inserts."""
    yoy_obs = compute_yoy(obs, periods)
    cur = conn.cursor()
    for o in yoy_obs:
        cur.execute(UPSERT_SQL, (
            currency, f'{currency}_CPI_YOY',
            o['date'], float(o['value']), None, '%YOY'
        ))
    conn.commit()
    return len(yoy_obs)

def insert_unemployment_series(conn, currency, obs):
    """Inserts unemployment rate. previous = prior period rate."""
    cur = conn.cursor()
    rows = 0
    for i, o in enumerate(obs):
        prev_val = float(obs[i-1]['value']) if i > 0 else None
        cur.execute(UPSERT_SQL, (
            currency, f'{currency}_UNEMPLOYMENT',
            o['date'], float(o['value']), prev_val, '%'
        ))
        rows += 1
    conn.commit()
    return rows

# ---------------------------------------------------------------------------
# Eurostat EUR unemployment (fills FRED gap post-2023)
# ---------------------------------------------------------------------------

def ingest_eurostat_unemployment(conn):
    """Ingest EUR unemployment from Eurostat JSON-stat API.

    Uses EA21 (Euro area - 21 countries), seasonally adjusted, total, monthly.
    Extends coverage beyond the FRED LRHUTTTTEZM156S series which ends ~2023.
    Time format from API: 'YYYY-MM' e.g. '1983-01'.
    """
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m"
    params = {
        'geo': 'EA21', 's_adj': 'SA', 'age': 'TOTAL',
        'sex': 'T', 'unit': 'PC_ACT', 'format': 'JSON', 'lang': 'EN'
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    values = data.get('value', {})
    times = list(data['dimension']['time']['category']['index'].keys())

    # Build sorted list of (period, value) pairs — period is 'YYYY-MM'
    obs_sorted = sorted(
        [(times[int(k)], v) for k, v in values.items() if v is not None],
        key=lambda x: x[0]
    )

    cur = conn.cursor()
    inserted = 0
    for i, (period, val) in enumerate(obs_sorted):
        # 'YYYY-MM' -> 'YYYY-MM-01'
        year, month = period.split('-')
        release_date = f"{year}-{month}-01"
        prev_val = obs_sorted[i - 1][1] if i > 0 else None
        cur.execute(
            """
            INSERT INTO forex_network.economic_releases
                (source, country, indicator, release_time, actual, previous, unit, impact_level)
            VALUES ('EUROSTAT', %s, %s, %s, %s, %s, %s, 'HIGH')
            ON CONFLICT (source, country, indicator, release_time) DO UPDATE SET
                actual   = EXCLUDED.actual,
                previous = EXCLUDED.previous
            """,
            ('EUR', 'EUR_UNEMPLOYMENT', release_date, float(val), prev_val, '%')
        )
        inserted += 1

    conn.commit()
    if obs_sorted:
        print(f"  EUR Eurostat (EA21, une_rt_m): {inserted} rows  {obs_sorted[0][0]} -> {obs_sorted[-1][0]}")
    return inserted

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    fred_key = get_fred_key()
    conn     = get_conn()
    total    = 0

    print("=== YIELDS (10Y government bond) ===")
    for currency, series_id in FRED_YIELDS.items():
        obs = fetch_fred(series_id, fred_key)
        if not obs:
            print(f"  {currency}: no data")
            continue
        n = insert_yield_series(conn, currency, obs)
        print(f"  {currency} ({series_id}): {n} rows  {obs[0]['date']} -> {obs[-1]['date']}")
        total += n
        time.sleep(0.12)

    print("\n=== CPI YoY (computed from FRED index) ===")
    for currency, (series_id, periods) in FRED_CPI.items():
        obs = fetch_fred(series_id, fred_key)
        if not obs:
            print(f"  {currency}: no data")
            continue
        n = insert_cpi_series(conn, currency, obs, periods)
        freq = 'Q' if periods == 4 else 'M'
        print(f"  {currency} ({series_id}, {freq}, lag={periods}): {len(obs)} raw -> {n} YoY rows")
        total += n
        time.sleep(0.12)

    print("\n=== UNEMPLOYMENT RATE ===")
    for currency, series_id in FRED_UNEMPLOYMENT.items():
        obs = fetch_fred(series_id, fred_key)
        if not obs:
            print(f"  {currency}: no data")
            continue
        n = insert_unemployment_series(conn, currency, obs)
        print(f"  {currency} ({series_id}): {n} rows  {obs[0]['date']} -> {obs[-1]['date']}")
        total += n
        time.sleep(0.12)

    print("\n=== EUR UNEMPLOYMENT (Eurostat) ===")
    try:
        n = ingest_eurostat_unemployment(conn)
        total += n
    except Exception as e:
        print(f"  Eurostat ingest failed: {e}")

    print(f"\nTotal rows upserted: {total}")
    conn.close()

    print("\n=== RE-RUNNING MACRO HISTORY BACKFILL ===")
    result = subprocess.run(
        ['/root/algodesk/algodesk/bin/python3',
         '/root/Project_Neo_Damon/scripts/backfill_macro_history.py'],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[:600])

    print("\n=== WFO BACKTEST ===")
    result = subprocess.run(
        ['/root/algodesk/algodesk/bin/python3',
         '/root/Project_Neo_Damon/scripts/wfo_backtest.py'],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[:600])


if __name__ == '__main__':
    run()
