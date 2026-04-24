#!/usr/bin/env python3
"""
Backfill price_at_signal, price_4h, price_1d, price_5d and pip outcomes
for shadow_trades rows where prices are still NULL.
Reads close prices from historical_prices (1H bars).
Runs at 22:45 UTC Mon-Fri via crontab.
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

def pips(entry, exit_price, instrument, direction):
    """Signed pip move: positive = price moved in signal direction."""
    if entry is None or exit_price is None:
        return None
    pip_size = 0.01 if instrument.endswith('JPY') else 0.0001
    move = (exit_price - entry) / pip_size
    if direction.startswith('beari'):
        move = -move
    return round(move, 1)

def run():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT id, instrument, direction, signal_time
        FROM forex_network.shadow_trades
        WHERE price_at_signal IS NULL
          AND signal_time < NOW() - INTERVAL '1 hour'
        ORDER BY signal_time
        LIMIT 500
    """)
    rows = cur.fetchall()
    print(f"Processing {len(rows)} shadow trades")

    updated = 0
    for row_id, instrument, direction, signal_time in rows:
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)

        p0  = fetch_price_after(cur, instrument, signal_time)
        p4h = fetch_price_after(cur, instrument, signal_time + timedelta(hours=4))
        p1d = fetch_price_after(cur, instrument, signal_time + timedelta(hours=24))
        p5d = fetch_price_after(cur, instrument, signal_time + timedelta(days=5))

        sp_4h = pips(p0, p4h, instrument, direction)
        sp_1d = pips(p0, p1d, instrument, direction)
        sp_5d = pips(p0, p5d, instrument, direction)

        # shadow_outcome: correct/incorrect/neutral based on 1d pip move > 1 pip threshold
        if sp_1d is not None:
            if sp_1d > 1:
                shadow_outcome = 'correct'
            elif sp_1d < -1:
                shadow_outcome = 'incorrect'
            else:
                shadow_outcome = 'neutral'
        else:
            shadow_outcome = None

        cur.execute("""
            UPDATE forex_network.shadow_trades
            SET price_at_signal = %s,
                price_4h  = %s,
                price_1d  = %s,
                price_5d  = %s,
                pips_4h   = %s,
                pips_1d   = %s,
                pips_5d   = %s,
                shadow_pips_4h = %s,
                shadow_pips_1d = %s,
                shadow_pips_5d = %s,
                shadow_outcome = %s
            WHERE id = %s
        """, (
            p0, p4h, p1d, p5d,
            sp_4h, sp_1d, sp_5d,
            sp_4h, sp_1d, sp_5d,
            shadow_outcome,
            row_id,
        ))
        updated += 1

    conn.commit()
    conn.close()
    print(f"Updated {updated} shadow trade rows — {datetime.now(timezone.utc).isoformat()}")

if __name__ == '__main__':
    run()
