#!/usr/bin/env python3
"""
Ingests GBP 10Y gilt yield (IUDMNPY) from BoE IADB into economic_releases.
source='BOE', country='GBP', indicator='GBP10Y_YIELD'
Daily data 2000-01-04 → present.
"""
import requests, io, boto3, json, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/csv,text/plain,*/*',
    'Referer': 'https://www.bankofengland.co.uk/statistics/yield-curves',
}

def fetch_boe():
    url = "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
    params = {
        'csv.x': '1',
        'Datefrom': '01/Jan/2000',
        'Dateto': '23/Apr/2026',
        'SeriesCodes': 'IUDMNPY',
        'CSVF': 'TT',
        'UsingCodes': 'Y',
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # Format: 2 metadata rows, blank row, then "DATE,IUDMNPY" header, then data
    lines = r.text.strip().split('\n')
    # Find the data header row
    data_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('DATE,'):
            data_start = i
            break
    if data_start is None:
        raise ValueError(f"Could not find DATE header in {len(lines)}-line response")

    csv_text = '\n'.join(lines[data_start:])
    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = ['date_str', 'yield_pct']
    df = df.dropna(subset=['yield_pct'])
    df['yield_pct'] = pd.to_numeric(df['yield_pct'], errors='coerce')
    df = df.dropna(subset=['yield_pct'])
    df['date'] = pd.to_datetime(df['date_str'], format='%d %b %Y')
    print(f"Fetched {len(df)} rows, {df['date'].min().date()} → {df['date'].max().date()}")
    return df

def get_conn():
    ssm = boto3.client('ssm', region_name='eu-west-2')
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ep  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    cr  = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(host=ep, dbname='postgres',
        user=cr['username'], password=cr['password'],
        options='-c search_path=forex_network')

def ingest(df, conn):
    cur = conn.cursor()
    rows = [
        ('BOE', 'GBP', 'GBP10Y_YIELD', row['date'].to_pydatetime(), float(row['yield_pct']))
        for _, row in df.iterrows()
    ]
    inserted = 0
    for batch_start in range(0, len(rows), 500):
        batch = rows[batch_start:batch_start+500]
        cur.executemany("""
            INSERT INTO forex_network.economic_releases
                (source, country, indicator, release_time, actual)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, country, indicator, release_time)
            DO UPDATE SET actual = EXCLUDED.actual
        """, batch)
        conn.commit()
        inserted += len(batch)
        print(f"  Committed {inserted}/{len(rows)}")
    return inserted

def main():
    print("Fetching BoE IUDMNPY (10Y gilt yield)...")
    df = fetch_boe()

    print("Connecting to DB...")
    conn = get_conn()

    print(f"Inserting {len(df)} rows...")
    n = ingest(df, conn)

    # Verify
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*), MIN(release_time::date), MAX(release_time::date),
               ROUND(AVG(actual)::numeric, 3)
        FROM forex_network.economic_releases
        WHERE source='BOE' AND country='GBP' AND indicator='GBP10Y_YIELD'
    """)
    row = cur.fetchone()
    print(f"\nVerification — BOE GBP10Y_YIELD:")
    print(f"  {row[0]} rows, {row[1]} → {row[2]}, avg yield = {row[3]}%")
    conn.close()
    print("Done.")

if __name__ == '__main__':
    main()
