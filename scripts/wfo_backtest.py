#!/usr/bin/env python3
"""
Walk-Forward Validation Backtest — Deterministic Macro Signal
Tests whether currency composite scores predict subsequent FX direction.

Note: historical_prices uses column 'ts' (not 'timestamp').
macro_signals_history has data from 2024-04 for non-USD; 2016-01 for USD.
"""
import boto3, json, psycopg2, psycopg2.extras
from datetime import date, timedelta

PAIRS = {
    'EURUSD': ('EUR', 'USD'),
    'GBPUSD': ('GBP', 'USD'),
    'USDJPY': ('USD', 'JPY'),
    'AUDUSD': ('AUD', 'USD'),
    'USDCAD': ('USD', 'CAD'),
    'USDCHF': ('USD', 'CHF'),
    'NZDUSD': ('NZD', 'USD'),
    'EURGBP': ('EUR', 'GBP'),
    'EURJPY': ('EUR', 'JPY'),
    'GBPJPY': ('GBP', 'JPY'),
    'EURAUD': ('EUR', 'AUD'),
    'EURCAD': ('EUR', 'CAD'),
    'GBPAUD': ('GBP', 'AUD'),
    'GBPCAD': ('GBP', 'CAD'),
    'AUDJPY': ('AUD', 'JPY'),
    'CADJPY': ('CAD', 'JPY'),
    'EURCHF': ('EUR', 'CHF'),
}


def get_conn():
    ssm  = boto3.client('ssm', region_name='eu-west-2')
    sm   = boto3.client('secretsmanager', region_name='eu-west-2')
    ep   = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    cr   = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(
        host=ep, dbname='postgres',
        user=cr['username'], password=cr['password'],
        options='-c search_path=forex_network',
    )


def run():
    conn = get_conn()
    cur  = conn.cursor()

    results = {}

    for pair, (base, quote) in PAIRS.items():
        cur.execute("""
            WITH pair_signals AS (
                SELECT
                    b.signal_date,
                    b.composite_score - q.composite_score                           AS pair_score,
                    CASE
                        WHEN b.composite_score - q.composite_score >  0.1 THEN 'long'
                        WHEN b.composite_score - q.composite_score < -0.1 THEN 'short'
                        ELSE 'neutral'
                    END AS signal_direction
                FROM forex_network.macro_signals_history b
                JOIN forex_network.macro_signals_history q
                    ON b.signal_date = q.signal_date
                WHERE b.currency = %s
                  AND q.currency = %s
                  AND b.signal_count >= 1
                  AND q.signal_count >= 1
            ),
            with_prices AS (
                SELECT
                    s.signal_date,
                    s.pair_score,
                    s.signal_direction,
                    -- Entry price
                    (SELECT close FROM forex_network.historical_prices h
                     WHERE h.instrument = %s AND h.timeframe = '1D'
                       AND h.ts::date <= s.signal_date
                     ORDER BY h.ts DESC LIMIT 1)                                    AS price_entry,
                    -- Price 1 trading day later
                    (SELECT close FROM forex_network.historical_prices h
                     WHERE h.instrument = %s AND h.timeframe = '1D'
                       AND h.ts::date > s.signal_date
                     ORDER BY h.ts LIMIT 1)                                         AS price_1d,
                    -- Price ~5 calendar days later
                    (SELECT close FROM forex_network.historical_prices h
                     WHERE h.instrument = %s AND h.timeframe = '1D'
                       AND h.ts::date >= s.signal_date + INTERVAL '5 days'
                     ORDER BY h.ts LIMIT 1)                                         AS price_5d
                FROM pair_signals s
                WHERE s.signal_direction != 'neutral'
            )
            SELECT
                COUNT(*)                                                             AS total_signals,
                ROUND(100.0 * SUM(CASE
                    WHEN signal_direction = 'long'  AND price_1d > price_entry THEN 1
                    WHEN signal_direction = 'short' AND price_1d < price_entry THEN 1
                    ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN price_1d IS NOT NULL THEN 1 END), 0),
                1)                                                                   AS hit_1d,
                ROUND(100.0 * SUM(CASE
                    WHEN signal_direction = 'long'  AND price_5d > price_entry THEN 1
                    WHEN signal_direction = 'short' AND price_5d < price_entry THEN 1
                    ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN price_5d IS NOT NULL THEN 1 END), 0),
                1)                                                                   AS hit_5d,
                ROUND(AVG(ABS(pair_score))::numeric, 3)                             AS avg_conviction,
                SUM(CASE WHEN signal_direction = 'long'  THEN 1 ELSE 0 END)        AS long_count,
                SUM(CASE WHEN signal_direction = 'short' THEN 1 ELSE 0 END)        AS short_count
            FROM with_prices
            WHERE price_entry IS NOT NULL
        """, (base, quote, pair, pair, pair))

        row = cur.fetchone()
        if row:
            results[pair] = {
                'signals':    row[0],
                'hit_1d':     row[1],
                'hit_5d':     row[2],
                'conviction': row[3],
                'longs':      row[4],
                'shorts':     row[5],
            }

    print("\n=== WALK-FORWARD BACKTEST RESULTS ===")
    print(f"{'Pair':<10} {'Signals':>8} {'Hit 1D':>8} {'Hit 5D':>8} "
          f"{'Conviction':>11} {'Longs':>7} {'Shorts':>7}")
    print("-" * 67)
    for pair, r in sorted(results.items(),
                           key=lambda x: float(x[1].get('hit_5d') or 0),
                           reverse=True):
        hit1 = f"{r['hit_1d']}%" if r['hit_1d'] is not None else "  n/a"
        hit5 = f"{r['hit_5d']}%" if r['hit_5d'] is not None else "  n/a"
        print(f"{pair:<10} {r['signals']:>8} {hit1:>8} {hit5:>8} "
              f"{str(r['conviction']):>11} {r['longs']:>7} {r['shorts']:>7}")

    # High conviction (|pair_score| > 0.3)
    print("\n=== HIGH CONVICTION SIGNALS (pair_score > 0.3) — 5D hit rate ===")
    any_hc = False
    for pair, (base, quote) in PAIRS.items():
        cur.execute("""
            WITH pair_signals AS (
                SELECT b.signal_date,
                       b.composite_score - q.composite_score AS pair_score,
                       CASE
                           WHEN b.composite_score - q.composite_score >  0.3 THEN 'long'
                           WHEN b.composite_score - q.composite_score < -0.3 THEN 'short'
                           ELSE 'neutral'
                       END AS signal_direction
                FROM forex_network.macro_signals_history b
                JOIN forex_network.macro_signals_history q
                    ON b.signal_date = q.signal_date
                WHERE b.currency = %s AND q.currency = %s
            ),
            with_prices AS (
                SELECT s.*,
                    (SELECT close FROM forex_network.historical_prices h
                     WHERE h.instrument = %s AND h.timeframe = '1D'
                       AND h.ts::date <= s.signal_date
                     ORDER BY h.ts DESC LIMIT 1)  AS price_entry,
                    (SELECT close FROM forex_network.historical_prices h
                     WHERE h.instrument = %s AND h.timeframe = '1D'
                       AND h.ts::date >= s.signal_date + INTERVAL '5 days'
                     ORDER BY h.ts LIMIT 1)        AS price_5d
                FROM pair_signals s
                WHERE s.signal_direction != 'neutral'
            )
            SELECT COUNT(*),
                ROUND(100.0 * SUM(CASE
                    WHEN signal_direction = 'long'  AND price_5d > price_entry THEN 1
                    WHEN signal_direction = 'short' AND price_5d < price_entry THEN 1
                    ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN price_5d IS NOT NULL THEN 1 END), 0),
                1)
            FROM with_prices
            WHERE price_entry IS NOT NULL
        """, (base, quote, pair, pair))
        row = cur.fetchone()
        if row and row[0] and row[0] > 5:
            any_hc = True
            print(f"  {pair:<10} {row[0]:>5} signals   5D hit rate: {row[1]}%")
    if not any_hc:
        print("  (no pairs with >5 high-conviction signals)")

    # Summary stats
    print("\n=== SIGNAL COVERAGE SUMMARY ===")
    cur.execute("""
        SELECT currency, COUNT(*) AS days,
               MIN(signal_date) AS earliest, MAX(signal_date) AS latest,
               ROUND(AVG(composite_score)::numeric, 3) AS avg_score
        FROM forex_network.macro_signals_history
        GROUP BY currency ORDER BY currency
    """)
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]} days, {r[2]} → {r[3]}, avg composite {float(r[4]):+.3f}")

    conn.close()


if __name__ == '__main__':
    run()
