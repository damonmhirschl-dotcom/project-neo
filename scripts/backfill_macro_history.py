#!/usr/bin/env python3
"""
Backfills forex_network.macro_signals_history with deterministic currency scores.

Data sources (priority order):
  - Yields:       economic_releases {CCY}10Y_YIELD (FRED, back to ~1993)
                  fallback: cross_asset_prices DE10Y/UK10Y/JP10Y/US10Y (back to ~2024)
  - CPI:          economic_releases {CCY}_CPI_YOY (FRED, computed YoY, back to ~1990s)
                  fallback: economic_releases CPI_YOY country=US (original FRED series)
  - PMI:          economic_releases %PMI% (FINNHUB, back to 2010 for GB/JP/AU)
  - Employment:   economic_releases {CCY}_UNEMPLOYMENT (FRED, change-based score)
                  fallback: economic_releases %employment% (FINNHUB)

Extend date range to 2000-01-01 — pre-2024 data available for yields+CPI+unemployment.
"""
import bisect, boto3, json, psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import date, timedelta

CURRENCIES = {
    'USD': {'country_old': 'US',  'yield_inst': 'US10Y'},
    'EUR': {'country_old': 'EU',  'yield_inst': 'DE10Y'},
    'GBP': {'country_old': 'GB',  'yield_inst': 'UK10Y'},
    'JPY': {'country_old': 'JP',  'yield_inst': 'JP10Y'},
    'AUD': {'country_old': 'AU',  'yield_inst': None},
    'CAD': {'country_old': 'CA',  'yield_inst': None},
    'CHF': {'country_old': 'CH',  'yield_inst': None},
    'NZD': {'country_old': 'NZ',  'yield_inst': None},
}
COUNTRIES_OLD = [v['country_old'] for v in CURRENCIES.values()]


def get_conn():
    ssm = boto3.client('ssm', region_name='eu-west-2')
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ep  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    cr  = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(host=ep, dbname='postgres',
        user=cr['username'], password=cr['password'],
        options='-c search_path=forex_network')


def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forex_network.macro_signals_history (
            id                        SERIAL PRIMARY KEY,
            currency                  CHAR(3)       NOT NULL,
            signal_date               DATE          NOT NULL,
            yield_score               FLOAT,
            cpi_surprise_score        FLOAT,
            pmi_surprise_score        FLOAT,
            employment_surprise_score FLOAT,
            composite_score           FLOAT,
            signal_count              INT,
            computed_at               TIMESTAMPTZ   DEFAULT NOW(),
            UNIQUE (currency, signal_date)
        );
        CREATE INDEX IF NOT EXISTS idx_macro_hist_date
            ON forex_network.macro_signals_history (signal_date, currency);
    """)
    conn.commit()
    print("Table created/verified.")


# ---------------------------------------------------------------------------
# In-memory loaders
# ---------------------------------------------------------------------------

def _as_of_latest(series, as_of_date):
    """Binary search: most recent entry on or before as_of_date. Returns tuple or None."""
    if not series:
        return None
    dates = [s[0] for s in series]
    idx   = bisect.bisect_right(dates, as_of_date) - 1
    return series[idx] if idx >= 0 else None


def load_yields(conn):
    """
    Primary: {CCY}10Y_YIELD from economic_releases (FRED, all 8 currencies).
    Fallback: cross_asset_prices for EUR/GBP/JPY/USD if FRED series absent.
    Returns {currency: [(date, yield_pct), ...]} sorted by date.
    """
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    data = defaultdict(list)

    # Primary — FRED monthly + BOE daily for GBP
    cur.execute("""
        SELECT country, release_time::date AS d, actual
        FROM forex_network.economic_releases
        WHERE source IN ('FRED', 'BOE')
          AND indicator LIKE '%10Y_YIELD'
          AND actual IS NOT NULL
        ORDER BY country, release_time
    """)
    for r in cur:
        ccy = r['country']   # 'USD','EUR','GBP',...
        data[ccy].append((r['d'], float(r['actual'])))

    # Fallback — cross_asset_prices for currencies not yet in economic_releases
    fallback_map = {ccy: cfg['yield_inst']
                    for ccy, cfg in CURRENCIES.items()
                    if cfg['yield_inst'] and not data.get(ccy)}
    if fallback_map:
        inst_list = list(fallback_map.values())
        cur.execute("""
            SELECT instrument, bar_time::date AS d, close
            FROM forex_network.cross_asset_prices
            WHERE instrument = ANY(%s)
            ORDER BY instrument, bar_time
        """, (inst_list,))
        inst_to_ccy = {v: k for k, v in fallback_map.items()}
        for r in cur:
            ccy = inst_to_ccy.get(r['instrument'])
            if ccy:
                data[ccy].append((r['d'], float(r['close'])))

    # USD baseline — also store for later differential use
    usd = data.get('USD', [])
    counts = ', '.join(f'{k}:{len(v)}' for k, v in sorted(data.items()))
    print(f"  Yields: {counts}")
    return data, usd


def load_cpi(conn):
    """
    Primary: {CCY}_CPI_YOY from economic_releases (FRED YoY, all 8 currencies).
    Fallback: CPI_YOY with country code (original US FRED / FINNHUB).
    Returns {currency_or_country: [(date, yoy_pct), ...]} sorted.
    """
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    data = defaultdict(list)

    # Primary — new FRED series
    cur.execute("""
        SELECT country, release_time::date AS d, actual
        FROM forex_network.economic_releases
        WHERE source = 'FRED'
          AND indicator LIKE '%_CPI_YOY'
          AND actual IS NOT NULL
        ORDER BY country, release_time
    """)
    for r in cur:
        data[r['country']].append((r['d'], float(r['actual'])))

    # Fallback — old CPI_YOY / FINNHUB %CPI% keyed by old country codes
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
    """, (COUNTRIES_OLD,))
    for r in cur:
        # Only fill gaps — map old country code to currency if not already covered
        old_to_ccy = {'US':'USD','EU':'EUR','GB':'GBP','JP':'JPY',
                      'AU':'AUD','CA':'CAD','CH':'CHF','NZ':'NZD'}
        ccy = old_to_ccy.get(r['country'], r['country'])
        if not data.get(ccy):
            data[ccy].append((r['d'], float(r['actual'])))

    counts = ', '.join(f'{k}:{len(v)}' for k, v in sorted(data.items()))
    print(f"  CPI:    {counts}")
    return data


def load_pmi(conn):
    """FINNHUB PMI by old country code, then re-keyed to currency."""
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    data = defaultdict(list)
    cur.execute("""
        SELECT country, release_time::date AS d, actual, forecast
        FROM forex_network.economic_releases
        WHERE actual IS NOT NULL AND forecast IS NOT NULL
          AND country = ANY(%s)
          AND indicator ILIKE '%%PMI%%'
        ORDER BY country, release_time
    """, (COUNTRIES_OLD,))
    old_to_ccy = {'US':'USD','EU':'EUR','GB':'GBP','JP':'JPY',
                  'AU':'AUD','CA':'CAD','CH':'CHF','NZ':'NZD'}
    for r in cur:
        ccy = old_to_ccy.get(r['country'], r['country'])
        data[ccy].append((r['d'], float(r['actual']), float(r['forecast'])))
    counts = ', '.join(f'{k}:{len(v)}' for k, v in sorted(data.items()))
    print(f"  PMI:    {counts or 'none'}")
    return data


def load_unemployment(conn):
    """
    Primary: {CCY}_UNEMPLOYMENT (FRED) — score from rate change (prev stored in 'previous').
    Fallback: %employment% (FINNHUB) — surprise = actual - forecast.
    Returns {currency: [(date, actual_rate, previous_rate), ...]}
    """
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    data = defaultdict(list)

    # Primary — FRED unemployment rate with previous period
    cur.execute("""
        SELECT country, release_time::date AS d, actual, previous
        FROM forex_network.economic_releases
        WHERE source = 'FRED'
          AND indicator LIKE '%_UNEMPLOYMENT'
          AND actual IS NOT NULL
          AND previous IS NOT NULL
        ORDER BY country, release_time
    """)
    for r in cur:
        data[r['country']].append((r['d'], float(r['actual']), float(r['previous'])))

    # Fallback — FINNHUB employment surprise for currencies with no FRED data
    cur.execute("""
        SELECT country, release_time::date AS d, actual, forecast
        FROM forex_network.economic_releases
        WHERE actual IS NOT NULL AND forecast IS NOT NULL
          AND country = ANY(%s)
          AND (indicator ILIKE '%%employment%%' OR indicator ILIKE '%%payroll%%'
               OR indicator = 'NFP_MOM')
        ORDER BY country, release_time
    """, (COUNTRIES_OLD,))
    old_to_ccy = {'US':'USD','EU':'EUR','GB':'GBP','JP':'JPY',
                  'AU':'AUD','CA':'CAD','CH':'CHF','NZ':'NZD'}
    for r in cur:
        ccy = old_to_ccy.get(r['country'], r['country'])
        if not data.get(ccy):
            data[ccy].append((r['d'], float(r['actual']), float(r['forecast'])))

    counts = ', '.join(f'{k}:{len(v)}' for k, v in sorted(data.items()))
    print(f"  Unemp:  {counts}")
    return data


# ---------------------------------------------------------------------------
# Score computers
# ---------------------------------------------------------------------------

def compute_yield_score(currency, yield_data, usd_series, as_of_date):
    if currency == 'USD':
        return 0.0
    row = _as_of_latest(yield_data.get(currency, []), as_of_date)
    usd = _as_of_latest(usd_series, as_of_date)
    if row is None or usd is None:
        return None
    diff = row[1] - usd[1]
    return max(-1.0, min(1.0, diff / 2.0))


def compute_cpi_score(currency, cpi_data, as_of_date):
    row = _as_of_latest(cpi_data.get(currency, []), as_of_date)
    if row is None:
        return None
    return max(-1.0, min(1.0, (row[1] - 2.0) / 3.0))


def compute_pmi_score(currency, pmi_data, as_of_date):
    row = _as_of_latest(pmi_data.get(currency, []), as_of_date)
    if row is None:
        return None
    return max(-1.0, min(1.0, (row[1] - row[2]) / 5.0))


def compute_unemployment_score(currency, unemp_data, as_of_date):
    """
    FRED path: actual=current_rate, previous=prior_period_rate.
      change = actual - previous; negative = falling (bullish) → positive score.
    FINNHUB fallback: actual=release, previous=forecast.
      Employment surprise: actual > forecast = more jobs = bullish.
      Store actual-forecast as score.
    """
    row = _as_of_latest(unemp_data.get(currency, []), as_of_date)
    if row is None:
        return None
    _, actual, prev = row
    change = actual - prev
    # For FRED unemployment: falling rate → negative change → bullish → positive score
    # For FINNHUB employment surprise: positive difference → bullish
    # Both normalised by 0.5pp / 100k units — divide by 0.5 (rate change) or 100 (NFP)
    # FRED rate change rarely exceeds 0.5pp in a month; cap at ±1
    return max(-1.0, min(1.0, -change / 0.5))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    conn = get_conn()
    create_table(conn)

    print("Loading source data into memory...")
    yield_data, usd_yields = load_yields(conn)
    cpi_data   = load_cpi(conn)
    pmi_data   = load_pmi(conn)
    unemp_data = load_unemployment(conn)

    start = date(2000, 1, 1)
    end   = date.today()

    print(f"\nComputing {(end-start).days} calendar days × {len(CURRENCIES)} currencies...")

    rows_to_insert = []
    skipped = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            for currency in CURRENCIES:
                ys = compute_yield_score(currency, yield_data, usd_yields, current)
                cs = compute_cpi_score(currency, cpi_data, current)
                ps = compute_pmi_score(currency, pmi_data, current)
                es = compute_unemployment_score(currency, unemp_data, current)

                scores = [s for s in [ys, cs, ps, es] if s is not None]
                if not scores:
                    skipped += 1
                    continue

                composite = sum(scores) / len(scores)
                rows_to_insert.append((
                    currency, current,
                    ys, cs, ps, es,
                    round(composite, 4), len(scores),
                ))
        current += timedelta(days=1)

    print(f"  {len(rows_to_insert)} rows to insert, {skipped} skipped")

    cur = conn.cursor()
    batch_size = 500
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        cur.executemany("""
            INSERT INTO forex_network.macro_signals_history
                (currency, signal_date, yield_score, cpi_surprise_score,
                 pmi_surprise_score, employment_surprise_score,
                 composite_score, signal_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
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
        if (i // batch_size) % 20 == 0:
            print(f"  {i + len(batch):>6} / {len(rows_to_insert)} committed")

    conn.commit()
    print(f"Complete: {len(rows_to_insert)} rows upserted.")

    cur.execute("""
        SELECT currency, COUNT(*) AS days,
               MIN(signal_date) AS earliest, MAX(signal_date) AS latest,
               ROUND(AVG(signal_count)::numeric,1) AS avg_sig,
               SUM(CASE WHEN yield_score IS NOT NULL THEN 1 ELSE 0 END)               AS yield_r,
               SUM(CASE WHEN cpi_surprise_score IS NOT NULL THEN 1 ELSE 0 END)        AS cpi_r,
               SUM(CASE WHEN pmi_surprise_score IS NOT NULL THEN 1 ELSE 0 END)        AS pmi_r,
               SUM(CASE WHEN employment_surprise_score IS NOT NULL THEN 1 ELSE 0 END) AS emp_r
        FROM forex_network.macro_signals_history
        GROUP BY currency ORDER BY currency
    """)
    print(f"\n{'CCY':<5}{'Days':>7}{'Earliest':>12}{'Latest':>12}"
          f"{'AvgSig':>8}{'Yield':>7}{'CPI':>7}{'PMI':>7}{'Emp':>7}")
    print("-" * 70)
    for r in cur.fetchall():
        print(f"{r[0]:<5}{r[1]:>7}{str(r[2]):>12}{str(r[3]):>12}"
              f"{float(r[4]):>8.1f}{r[5]:>7}{r[6]:>7}{r[7]:>7}{r[8]:>7}")

    conn.close()


if __name__ == '__main__':
    run()
