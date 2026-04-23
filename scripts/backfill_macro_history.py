#!/usr/bin/env python3
"""
Backfills forex_network.macro_signals_history with deterministic currency scores.

Data sources:
  - Yield differentials: cross_asset_prices (bar_time column) — DE10Y/UK10Y/JP10Y/US10Y only
    AUD/CAD/CHF/NZD have no yield data in cross_asset_prices → yield_score=None for those
  - CPI, PMI, Employment: economic_releases (FRED for US back to 2006; FINNHUB for others
    from ~2024-04)

Approach: load all source data into memory once, then do fast as-of lookups via bisect.
One row per currency per weekday date. Skips dates with zero signals rather than inserting
empty rows.
"""
import bisect, boto3, json, psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import date, timedelta

CURRENCIES = {
    'USD': {'country': 'US',  'yield_inst': 'US10Y'},
    'EUR': {'country': 'EU',  'yield_inst': 'DE10Y'},
    'GBP': {'country': 'GB',  'yield_inst': 'UK10Y'},
    'JPY': {'country': 'JP',  'yield_inst': 'JP10Y'},
    'AUD': {'country': 'AU',  'yield_inst': None},  # no yield data in DB
    'CAD': {'country': 'CA',  'yield_inst': None},
    'CHF': {'country': 'CH',  'yield_inst': None},
    'NZD': {'country': 'NZ',  'yield_inst': None},
}

COUNTRIES = [v['country'] for v in CURRENCIES.values()]


def get_conn():
    ssm = boto3.client('ssm', region_name='eu-west-2')
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ep  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    cr  = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(
        host=ep, dbname='postgres',
        user=cr['username'], password=cr['password'],
        options='-c search_path=forex_network',
    )


def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forex_network.macro_signals_history (
            id                      SERIAL PRIMARY KEY,
            currency                CHAR(3)       NOT NULL,
            signal_date             DATE          NOT NULL,
            yield_score             FLOAT,
            cpi_surprise_score      FLOAT,
            pmi_surprise_score      FLOAT,
            employment_surprise_score FLOAT,
            composite_score         FLOAT,
            signal_count            INT,
            computed_at             TIMESTAMPTZ   DEFAULT NOW(),
            UNIQUE (currency, signal_date)
        );
        CREATE INDEX IF NOT EXISTS idx_macro_hist_date
            ON forex_network.macro_signals_history (signal_date, currency);
    """)
    conn.commit()
    print("Table created/verified.")


# ---------------------------------------------------------------------------
# In-memory data loaders
# ---------------------------------------------------------------------------

def load_yields(conn):
    """Returns {instrument: [(date, close), ...]} sorted by date."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    instruments = [v['yield_inst'] for v in CURRENCIES.values() if v['yield_inst']]
    cur.execute("""
        SELECT instrument, bar_time::date AS d, close
        FROM forex_network.cross_asset_prices
        WHERE instrument = ANY(%s)
        ORDER BY instrument, bar_time
    """, (instruments,))
    data = defaultdict(list)
    for r in cur:
        data[r['instrument']].append((r['d'], float(r['close'])))
    print(f"  Yields loaded: {', '.join(f'{k}:{len(v)}rows' for k,v in data.items())}")
    return data


def load_cpi(conn):
    """Returns {country: [(release_date, actual_yoy), ...]} sorted by date.
    US uses FRED CPI_YOY; others use FINNHUB %CPI% releases.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT country, release_time::date AS d, actual
        FROM forex_network.economic_releases
        WHERE actual IS NOT NULL
          AND country = ANY(%s)
          AND (
              (source = 'FRED'    AND indicator = 'CPI_YOY')
           OR (source = 'FINNHUB' AND indicator ILIKE '%%CPI%%')
          )
        ORDER BY country, release_time
    """, (COUNTRIES,))
    data = defaultdict(list)
    for r in cur:
        if r['actual'] is not None:
            data[r['country']].append((r['d'], float(r['actual'])))
    print(f"  CPI loaded: {', '.join(f'{k}:{len(v)}' for k,v in data.items())}")
    return data


def load_pmi(conn):
    """Returns {country: [(release_date, actual, forecast), ...]}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT country, release_time::date AS d, actual, forecast
        FROM forex_network.economic_releases
        WHERE actual IS NOT NULL
          AND forecast IS NOT NULL
          AND country = ANY(%s)
          AND indicator ILIKE '%%PMI%%'
        ORDER BY country, release_time
    """, (COUNTRIES,))
    data = defaultdict(list)
    for r in cur:
        data[r['country']].append((r['d'], float(r['actual']), float(r['forecast'])))
    print(f"  PMI loaded: {', '.join(f'{k}:{len(v)}' for k,v in data.items()) or 'none'}")
    return data


def load_employment(conn):
    """Returns {country: [(release_date, actual, forecast), ...]}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT country, release_time::date AS d, actual, forecast
        FROM forex_network.economic_releases
        WHERE actual IS NOT NULL
          AND forecast IS NOT NULL
          AND country = ANY(%s)
          AND (
              indicator ILIKE '%%employment%%'
           OR indicator ILIKE '%%payroll%%'
           OR indicator = 'NFP_MOM'
          )
        ORDER BY country, release_time
    """, (COUNTRIES,))
    data = defaultdict(list)
    for r in cur:
        data[r['country']].append((r['d'], float(r['actual']), float(r['forecast'])))
    print(f"  Employment loaded: {', '.join(f'{k}:{len(v)}' for k,v in data.items())}")
    return data


# ---------------------------------------------------------------------------
# As-of lookup helpers
# ---------------------------------------------------------------------------

def _as_of_latest(series, as_of_date):
    """Binary search: return value of most recent entry on or before as_of_date.
    series = [(date, value, ...), ...] sorted ascending.
    Returns the tuple, or None.
    """
    if not series:
        return None
    dates = [s[0] for s in series]
    idx = bisect.bisect_right(dates, as_of_date) - 1
    return series[idx] if idx >= 0 else None


def compute_yield_score(currency, yield_data, usd_series, as_of_date):
    """Spread vs USD 10Y baseline. USD itself returns 0.0."""
    if currency == 'USD':
        return 0.0
    inst = CURRENCIES[currency]['yield_inst']
    if not inst:
        return None
    local_row = _as_of_latest(yield_data.get(inst, []), as_of_date)
    usd_row   = _as_of_latest(usd_series, as_of_date)
    if local_row is None or usd_row is None:
        return None
    diff = local_row[1] - usd_row[1]
    # 2% yield advantage/disadvantage → ±1.0
    return max(-1.0, min(1.0, diff / 2.0))


def compute_cpi_score(country, cpi_data, as_of_date):
    """Distance from 2% target, normalised by ±3% band → ±1.0."""
    row = _as_of_latest(cpi_data.get(country, []), as_of_date)
    if row is None:
        return None
    actual = row[1]
    raw = (actual - 2.0) / 3.0
    return max(-1.0, min(1.0, raw))


def compute_pmi_score(country, pmi_data, as_of_date):
    """Surprise = actual - forecast, normalised by ±5pt band → ±1.0."""
    row = _as_of_latest(pmi_data.get(country, []), as_of_date)
    if row is None:
        return None
    surprise = row[1] - row[2]
    return max(-1.0, min(1.0, surprise / 5.0))


def compute_employment_score(country, emp_data, as_of_date):
    """Surprise = actual - forecast. NFP in thousands → /100k normaliser.
    For FINNHUB employment (also typically thousands of persons) same scale works.
    """
    row = _as_of_latest(emp_data.get(country, []), as_of_date)
    if row is None:
        return None
    surprise = row[1] - row[2]
    return max(-1.0, min(1.0, surprise / 100.0))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    conn = get_conn()
    create_table(conn)

    print("Loading source data into memory...")
    yield_data = load_yields(conn)
    usd_yields = yield_data.get('US10Y', [])
    cpi_data   = load_cpi(conn)
    pmi_data   = load_pmi(conn)
    emp_data   = load_employment(conn)

    start = date(2016, 1, 1)
    end   = date.today()
    total_days = (end - start).days + 1

    print(f"\nComputing {total_days} calendar days × {len(CURRENCIES)} currencies...")

    rows_to_insert = []
    skipped = 0

    current = start
    while current <= end:
        if current.weekday() < 5:   # weekdays only
            for currency, cfg in CURRENCIES.items():
                country = cfg['country']

                yield_score = compute_yield_score(currency, yield_data, usd_yields, current)
                cpi_score   = compute_cpi_score(country, cpi_data, current)
                pmi_score   = compute_pmi_score(country, pmi_data, current)
                emp_score   = compute_employment_score(country, emp_data, current)

                scores = [s for s in [yield_score, cpi_score, pmi_score, emp_score]
                          if s is not None]
                if not scores:
                    skipped += 1
                    continue

                composite = sum(scores) / len(scores)
                rows_to_insert.append((
                    currency, current,
                    yield_score, cpi_score, pmi_score, emp_score,
                    round(composite, 4), len(scores),
                ))
        current += timedelta(days=1)

    print(f"  {len(rows_to_insert)} rows to insert, {skipped} skipped (no data)")
    print("Inserting in batches...")

    cur = conn.cursor()
    batch_size = 500
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        cur.executemany("""
            INSERT INTO forex_network.macro_signals_history
                (currency, signal_date,
                 yield_score, cpi_surprise_score, pmi_surprise_score,
                 employment_surprise_score, composite_score, signal_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (currency, signal_date) DO UPDATE SET
                yield_score               = EXCLUDED.yield_score,
                cpi_surprise_score        = EXCLUDED.cpi_surprise_score,
                pmi_surprise_score        = EXCLUDED.pmi_surprise_score,
                employment_surprise_score = EXCLUDED.employment_surprise_score,
                composite_score           = EXCLUDED.composite_score,
                signal_count              = EXCLUDED.signal_count,
                computed_at               = NOW()
        """, batch)
        conn.commit()
        if (i // batch_size) % 10 == 0:
            print(f"  {i + len(batch)} / {len(rows_to_insert)} rows committed")

    conn.commit()
    print(f"\nComplete: {len(rows_to_insert)} rows inserted/updated.")

    # Coverage summary
    cur.execute("""
        SELECT currency,
               COUNT(*)          AS days,
               MIN(signal_date)  AS earliest,
               MAX(signal_date)  AS latest,
               ROUND(AVG(signal_count)::numeric, 1) AS avg_signals,
               SUM(CASE WHEN yield_score IS NOT NULL THEN 1 ELSE 0 END)              AS yield_rows,
               SUM(CASE WHEN cpi_surprise_score IS NOT NULL THEN 1 ELSE 0 END)       AS cpi_rows,
               SUM(CASE WHEN pmi_surprise_score IS NOT NULL THEN 1 ELSE 0 END)       AS pmi_rows,
               SUM(CASE WHEN employment_surprise_score IS NOT NULL THEN 1 ELSE 0 END) AS emp_rows
        FROM forex_network.macro_signals_history
        GROUP BY currency
        ORDER BY currency
    """)
    print(f"\n{'CCY':<5} {'Days':>6} {'Earliest':>12} {'Latest':>12} "
          f"{'AvgSig':>7} {'Yield':>6} {'CPI':>6} {'PMI':>6} {'Emp':>6}")
    print("-" * 75)
    for r in cur.fetchall():
        print(f"{r[0]:<5} {r[1]:>6} {str(r[2]):>12} {str(r[3]):>12} "
              f"{float(r[4]):>7.1f} {r[5]:>6} {r[6]:>6} {r[7]:>6} {r[8]:>6}")

    conn.close()


if __name__ == '__main__':
    run()
