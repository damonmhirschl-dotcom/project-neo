#!/usr/bin/env python3
import boto3, json, psycopg2
import pandas as pd
import numpy as np

PAIRS = {
    'EURJPY': ('EUR','JPY'),
    'GBPJPY': ('GBP','JPY'),
    'USDJPY': ('USD','JPY'),
    'EURGBP': ('EUR','GBP'),
    'EURUSD': ('EUR','USD'),
    'GBPUSD': ('GBP','USD'),
    'AUDUSD': ('AUD','USD'),
}

def get_conn():
    sm = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    host = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value'].split(':')[0]
    return psycopg2.connect(host=host, port=5432, dbname='postgres',
        user=creds['username'], password=creds['password'],
        sslmode='require', options='-c search_path=forex_network')

def compute_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def compute_adx(high, low, close, period=14):
    tr = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
    atr = tr.ewm(com=period-1, min_periods=period).mean()
    up = high.diff()
    down = -low.diff()
    dm_plus = up.where((up > down) & (up > 0), 0)
    dm_minus = down.where((down > up) & (down > 0), 0)
    di_plus = 100 * dm_plus.ewm(com=period-1, min_periods=period).mean() / atr
    di_minus = 100 * dm_minus.ewm(com=period-1, min_periods=period).mean() / atr
    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
    return dx.ewm(com=period-1, min_periods=period).mean()

def run():
    conn = get_conn()
    cur = conn.cursor()

    print("=== RSI PULLBACK ENTRY BACKTEST ===")
    print("Macro direction gate + RSI pullback timing")
    print(f"{'Pair':<10} {'Signals':>8} {'Hit 4H':>8} {'Hit 1D':>8} {'Hit 5D':>8} {'vs EMA 4H':>10}")
    print("-" * 60)

    ema_baseline = {
        'EURJPY': 50.6, 'GBPJPY': 48.5, 'USDJPY': 50.3,
        'EURGBP': 51.5, 'EURUSD': 49.6, 'GBPUSD': 49.9, 'AUDUSD': None
    }

    for pair, (base, quote) in PAIRS.items():
        cur.execute("""
            SELECT b.signal_date,
                b.composite_score - q.composite_score AS pair_score,
                CASE WHEN b.composite_score - q.composite_score > 0.1 THEN 1
                     WHEN b.composite_score - q.composite_score < -0.1 THEN -1
                     ELSE 0 END as macro_direction
            FROM forex_network.macro_signals_history b
            JOIN forex_network.macro_signals_history q ON b.signal_date = q.signal_date
            WHERE b.currency = %s AND q.currency = %s
            AND b.composite_score - q.composite_score != 0
        """, (base, quote))
        macro_rows = {str(r[0]): r[2] for r in cur.fetchall()}

        if not macro_rows:
            continue

        cur2 = conn.cursor()
        cur2.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1H'
            AND ts >= '2005-01-01'
            ORDER BY ts ASC
        """, (pair,))
        rows = cur2.fetchall()
        if len(rows) < 500:
            continue

        df = pd.DataFrame(rows, columns=['timestamp','open','high','low','close'])
        df = df.set_index('timestamp')
        df = df.astype(float)

        rsi = compute_rsi(df['close'])
        adx = compute_adx(df['high'], df['low'], df['close'])
        closes = df['close']

        hits_4h, hits_1d, hits_5d = [], [], []
        signal_count = 0

        for i in range(len(df)):
            idx = df.index[i]
            signal_date = str(idx.date())
            macro_dir = macro_rows.get(signal_date, 0)

            if macro_dir == 0:
                continue

            rsi_val = rsi.iloc[i]
            adx_val = adx.iloc[i]

            if pd.isna(rsi_val) or pd.isna(adx_val):
                continue

            if adx_val < 20:
                continue

            if macro_dir == 1 and not (40 <= rsi_val <= 55):
                continue
            if macro_dir == -1 and not (45 <= rsi_val <= 60):
                continue

            try:
                p0  = closes.iloc[i]
                p4h = closes.iloc[i+4]   if i+4   < len(closes) else None
                p1d = closes.iloc[i+24]  if i+24  < len(closes) else None
                p5d = closes.iloc[i+120] if i+120 < len(closes) else None
            except IndexError:
                continue

            if p4h is not None:
                hits_4h.append(1 if (macro_dir==1 and p4h>p0) or (macro_dir==-1 and p4h<p0) else 0)
            if p1d is not None:
                hits_1d.append(1 if (macro_dir==1 and p1d>p0) or (macro_dir==-1 and p1d<p0) else 0)
            if p5d is not None:
                hits_5d.append(1 if (macro_dir==1 and p5d>p0) or (macro_dir==-1 and p5d<p0) else 0)
            signal_count += 1

        if signal_count > 20:
            h4  = round(100*sum(hits_4h)/len(hits_4h),  1) if hits_4h else None
            h1d = round(100*sum(hits_1d)/len(hits_1d),  1) if hits_1d else None
            h5d = round(100*sum(hits_5d)/len(hits_5d),  1) if hits_5d else None
            baseline = ema_baseline.get(pair)
            diff = f"+{h4-baseline:.1f}pp" if (baseline and h4) else "N/A"
            print(f"{pair:<10} {signal_count:>8} {str(h4)+'%':>8} {str(h1d)+'%':>8} {str(h5d)+'%':>8} {diff:>10}")

if __name__ == '__main__':
    run()
