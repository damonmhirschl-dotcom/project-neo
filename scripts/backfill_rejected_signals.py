#!/usr/bin/env python3
"""
Backfill price_1h_later / price_4h_later / price_24h_later and outcome
for rejected_signals rows where prices are still NULL.
Reads close prices from historical_prices (1H bars).
Runs at 22:30 UTC Mon-Fri via crontab.
"""
import boto3, json, psycopg2
from datetime import datetime, timezone, timedelta

def get_conn():
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    host  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    return psycopg2.connect(host=host, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network')

def fetch_price_after(cur, instrument, t_after):
    """Return the first available 1H close in historical_prices at or after t_after."""
    cur.execute("""
        SELECT close FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = '1H'
          AND ts >= %s
        ORDER BY ts
        LIMIT 1
    """, (instrument, t_after))
    row = cur.fetchone()
    return float(row[0]) if row else None

def run():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT id, instrument, direction, price_at_rejection, rejected_at
        FROM forex_network.rejected_signals
        WHERE price_1h_later IS NULL
          AND price_at_rejection IS NOT NULL
          AND rejected_at < NOW() - INTERVAL '1 hour'
        ORDER BY rejected_at
        LIMIT 500
    """)
    rows = cur.fetchall()
    print(f"Processing {len(rows)} rejected signals")

    updated = correct = incorrect = neutral = 0
    for row_id, instrument, direction, price_at, rejected_at in rows:
        # Ensure rejected_at is timezone-aware
        if rejected_at.tzinfo is None:
            rejected_at = rejected_at.replace(tzinfo=timezone.utc)

        prices = {}
        for hours, col in [(1, 'price_1h_later'), (4, 'price_4h_later'), (24, 'price_24h_later')]:
            t_after = rejected_at + timedelta(hours=hours)
            prices[col] = fetch_price_after(cur, instrument, t_after)

        outcome = None
        p1h = prices['price_1h_later']
        if p1h is not None and price_at and direction:
            # direction is stored as 5-char truncation: 'bulli'=long, 'beari'=short
            if direction.startswith('bulli'):
                direction_mult = 1
            elif direction.startswith('beari'):
                direction_mult = -1
            else:
                direction_mult = 0  # neutral — skip outcome
            if direction_mult != 0:
                pip_size = 0.01 if 'JPY' in instrument else 0.0001
                move_pips = (p1h - price_at) / pip_size * direction_mult
                if move_pips > 5:
                    outcome = 'incorrect'   # price moved the rejected way — rejection was wrong
                elif move_pips < -5:
                    outcome = 'correct'     # price moved against rejected direction — rejection was right
                else:
                    outcome = 'neutral'

        cur.execute("""
            UPDATE forex_network.rejected_signals
            SET price_1h_later=%s, price_4h_later=%s, price_24h_later=%s, outcome=%s
            WHERE id=%s
        """, (p1h, prices['price_4h_later'], prices['price_24h_later'], outcome, row_id))
        updated += 1
        if outcome == 'correct':    correct   += 1
        elif outcome == 'incorrect': incorrect += 1
        elif outcome == 'neutral':  neutral   += 1

    conn.commit()
    print(f"Updated {updated} rows — correct={correct} incorrect={incorrect} neutral={neutral}")

    # Verification
    cur.execute("""
        SELECT outcome, COUNT(*) FROM forex_network.rejected_signals
        WHERE outcome IS NOT NULL GROUP BY outcome ORDER BY outcome
    """)
    print("\n=== Outcome breakdown ===")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]}")

    cur.close()
    conn.close()

if __name__ == '__main__':
    run()
