#!/usr/bin/env python3
"""
Compute p50/p75/p90 of abs(base.composite_score - quote.composite_score)
from 25yr macro_signals_history and cache in macro_percentile_thresholds.

Run weekly (Monday 06:00 UTC) to keep thresholds calibrated as new data arrives.
"""
import boto3, json, psycopg2

PAIRS = {
    'EURJPY': ('EUR', 'JPY'),
    'AUDJPY': ('AUD', 'JPY'),
    'GBPJPY': ('GBP', 'JPY'),
    'CADJPY': ('CAD', 'JPY'),
    'NZDJPY': ('NZD', 'JPY'),
    'USDJPY': ('USD', 'JPY'),
    'EURAUD': ('EUR', 'AUD'),
    'EURGBP': ('EUR', 'GBP'),
    'EURUSD': ('EUR', 'USD'),
    'EURCAD': ('EUR', 'CAD'),
    'EURCHF': ('EUR', 'CHF'),
    'GBPUSD': ('GBP', 'USD'),
    'GBPAUD': ('GBP', 'AUD'),
    'GBPCAD': ('GBP', 'CAD'),
    'AUDUSD': ('AUD', 'USD'),
    'AUDNZD': ('AUD', 'NZD'),
    'USDCAD': ('USD', 'CAD'),
    'USDCHF': ('USD', 'CHF'),
    'NZDUSD': ('NZD', 'USD'),
}


def get_conn():
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    host  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value'].split(':')[0]
    return psycopg2.connect(host=host, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network')


conn = get_conn()
cur  = conn.cursor()

print(f"{'Pair':<8}  {'p50':>7}  {'p75':>7}  {'p90':>7}  {'days':>6}")
print('-' * 42)

inserted = 0
for pair, (base, quote) in sorted(PAIRS.items()):
    cur.execute(
        """
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ABS(b.composite_score - q.composite_score)),
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ABS(b.composite_score - q.composite_score)),
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ABS(b.composite_score - q.composite_score)),
            COUNT(*)
        FROM forex_network.macro_signals_history b
        JOIN forex_network.macro_signals_history q ON b.signal_date = q.signal_date
        WHERE b.currency = %s AND q.currency = %s
        """,
        (base, quote),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        print(f"{pair:<8}  (no data)")
        continue

    p50, p75, p90, days = float(row[0]), float(row[1]), float(row[2]), int(row[3])
    cur.execute(
        """
        INSERT INTO forex_network.macro_percentile_thresholds (pair, p50, p75, p90)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pair) DO UPDATE SET
            p50 = EXCLUDED.p50,
            p75 = EXCLUDED.p75,
            p90 = EXCLUDED.p90,
            computed_at = NOW()
        """,
        (pair, p50, p75, p90),
    )
    print(f"{pair:<8}  {p50:>7.3f}  {p75:>7.3f}  {p90:>7.3f}  {days:>6}")
    inserted += 1

conn.commit()
conn.close()
print(f"\n{inserted} pairs upserted into macro_percentile_thresholds.")
