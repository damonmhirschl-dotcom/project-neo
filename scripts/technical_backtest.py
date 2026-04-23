#!/usr/bin/env python3
"""
Technical Signal Backtest
Tests RSI, EMA crossover, and ADX against historical 1H price data.
Compares hit rates against the macro signal results.
"""
import boto3, json, psycopg2
import pandas as pd
import numpy as np
from datetime import date

PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD',
    'USDCHF', 'NZDUSD', 'EURJPY', 'GBPJPY', 'EURGBP'
]

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
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(com=period-1, min_periods=period).mean()
    up = high.diff()
    down = -low.diff()
    dm_plus = up.where((up > down) & (up > 0), 0)
    dm_minus = down.where((down > up) & (down > 0), 0)
    di_plus = 100 * dm_plus.ewm(com=period-1, min_periods=period).mean() / atr
    di_minus = 100 * dm_minus.ewm(com=period-1, min_periods=period).mean() / atr
    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
    return dx.ewm(com=period-1, min_periods=period).mean()

def get_signals(df):
    """
    Generate combined technical signal:
    - EMA 50/200 crossover for direction
    - RSI confirmation (not extreme against direction)
    - ADX > 20 for trend confirmation
    Returns series: 1=long, -1=short, 0=neutral
    """
    close = df['close']
    high = df['high']
    low = df['low']

    ema50 = close.ewm(span=50, min_periods=50).mean()
    ema200 = close.ewm(span=200, min_periods=200).mean()
    rsi = compute_rsi(close)
    adx = compute_adx(high, low, close)

    ema_bull = ema50 > ema200
    ema_bear = ema50 < ema200
    rsi_ok_long = rsi < 70
    rsi_ok_short = rsi > 30
    trending = adx > 20

    signal = pd.Series(0, index=df.index)
    signal[ema_bull & rsi_ok_long & trending] = 1
    signal[ema_bear & rsi_ok_short & trending] = -1

    return signal

def run():
    conn = get_conn()
    print("=== TECHNICAL SIGNAL BACKTEST (1H EMA50/200 + RSI + ADX) ===\n")
    print(f"{'Pair':<10} {'Signals':>8} {'Hit 1H':>8} {'Hit 4H':>8} {'Hit 1D':>8} {'Longs':>7} {'Shorts':>7}")
    print("-" * 65)

    all_results = {}

    for pair in PAIRS:
        cur = conn.cursor()
        cur.execute("""
            SELECT ts as timestamp, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1H'
            AND ts >= '2005-01-01'
            ORDER BY ts ASC
        """, (pair,))
        rows = cur.fetchall()

        if len(rows) < 500:
            print(f"{pair:<10} {'insufficient data':>50}")
            continue

        df = pd.DataFrame(rows, columns=['timestamp','open','high','low','close'])
        df = df.set_index('timestamp')
        df = df.astype(float)

        signals = get_signals(df)

        results = {'pair': pair, 'signals': 0, 'hit_1h': [], 'hit_4h': [], 'hit_1d': [], 'longs': 0, 'shorts': 0}

        signal_indices = signals[signals != 0].index
        closes = df['close']

        for idx in signal_indices:
            pos = df.index.get_loc(idx)
            direction = signals[idx]

            try:
                p0 = closes.iloc[pos]
                p1h = closes.iloc[pos + 1] if pos + 1 < len(closes) else None
                p4h = closes.iloc[pos + 4] if pos + 4 < len(closes) else None
                p1d = closes.iloc[pos + 24] if pos + 24 < len(closes) else None
            except IndexError:
                continue

            if p1h is not None:
                results['hit_1h'].append(1 if (direction == 1 and p1h > p0) or (direction == -1 and p1h < p0) else 0)
            if p4h is not None:
                results['hit_4h'].append(1 if (direction == 1 and p4h > p0) or (direction == -1 and p4h < p0) else 0)
            if p1d is not None:
                results['hit_1d'].append(1 if (direction == 1 and p1d > p0) or (direction == -1 and p1d < p0) else 0)

            results['signals'] += 1
            if direction == 1: results['longs'] += 1
            else: results['shorts'] += 1

        if results['signals'] > 0:
            h1  = round(100 * sum(results['hit_1h']) / len(results['hit_1h']), 1) if results['hit_1h'] else None
            h4  = round(100 * sum(results['hit_4h']) / len(results['hit_4h']), 1) if results['hit_4h'] else None
            h1d = round(100 * sum(results['hit_1d']) / len(results['hit_1d']), 1) if results['hit_1d'] else None
            print(f"{pair:<10} {results['signals']:>8} {str(h1)+'%':>8} {str(h4)+'%':>8} {str(h1d)+'%':>8} {results['longs']:>7} {results['shorts']:>7}")
            all_results[pair] = {'signals': results['signals'], 'hit_1h': h1, 'hit_4h': h4, 'hit_1d': h1d}

    # Combined: macro direction gate + technical timing trigger
    print("\n=== COMBINED: MACRO GATE + TECHNICAL TRIGGER ===")
    print("(Only technical signals that agree with macro direction)\n")
    print(f"{'Pair':<10} {'Combined':>10} {'Hit 4H':>8} {'Hit 1D':>8} {'Tech 4H alone':>15} {'Macro 5D alone':>16}")
    print("-" * 75)

    BASE_QUOTE = {
        'EURUSD': ('EUR','USD'), 'GBPUSD': ('GBP','USD'),
        'USDJPY': ('USD','JPY'), 'AUDUSD': ('AUD','USD'),
        'USDCAD': ('USD','CAD'), 'USDCHF': ('USD','CHF'),
        'NZDUSD': ('NZD','USD'), 'EURJPY': ('EUR','JPY'),
        'GBPJPY': ('GBP','JPY'), 'EURGBP': ('EUR','GBP'),
    }

    MACRO_5D = {
        'EURUSD': 50.5, 'GBPUSD': 50.7, 'USDJPY': 53.3,
        'AUDUSD': 51.2, 'USDCAD': 48.6, 'USDCHF': 50.1,
        'NZDUSD': 48.8, 'EURJPY': 58.6, 'GBPJPY': 51.9, 'EURGBP': 50.2,
    }

    cur = conn.cursor()
    for pair in PAIRS:
        if pair not in BASE_QUOTE:
            continue
        base, quote = BASE_QUOTE[pair]

        cur.execute("""
            WITH macro_dir AS (
                SELECT b.signal_date,
                    CASE WHEN b.composite_score - q.composite_score > 0.1 THEN 1
                         WHEN b.composite_score - q.composite_score < -0.1 THEN -1
                         ELSE 0 END as macro_direction
                FROM forex_network.macro_signals_history b
                JOIN forex_network.macro_signals_history q
                  ON b.signal_date = q.signal_date
                WHERE b.currency = %s AND q.currency = %s
            )
            SELECT signal_date, macro_direction FROM macro_dir
            WHERE macro_direction != 0
        """, (base, quote))
        macro_rows = {str(r[0]): r[1] for r in cur.fetchall()}

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
        signals = get_signals(df)
        closes = df['close']

        combined_hits_4h = []
        combined_hits_1d = []

        for idx in signals[signals != 0].index:
            tech_direction = int(signals[idx])
            signal_date = str(idx.date())
            macro_dir = macro_rows.get(signal_date)
            if macro_dir is None or macro_dir != tech_direction:
                continue

            pos = df.index.get_loc(idx)
            try:
                p0  = closes.iloc[pos]
                p4h = closes.iloc[pos + 4]  if pos + 4  < len(closes) else None
                p1d = closes.iloc[pos + 24] if pos + 24 < len(closes) else None
            except IndexError:
                continue

            if p4h is not None:
                combined_hits_4h.append(1 if (tech_direction == 1 and p4h > p0) or (tech_direction == -1 and p4h < p0) else 0)
            if p1d is not None:
                combined_hits_1d.append(1 if (tech_direction == 1 and p1d > p0) or (tech_direction == -1 and p1d < p0) else 0)

        if len(combined_hits_4h) > 20:
            c4h = round(100 * sum(combined_hits_4h) / len(combined_hits_4h), 1)
            c1d = round(100 * sum(combined_hits_1d) / len(combined_hits_1d), 1) if combined_hits_1d else None
            tech_4h  = all_results.get(pair, {}).get('hit_4h', '?')
            mac_5d   = MACRO_5D.get(pair, '?')
            delta_4h = f"+{round(c4h - tech_4h, 1)}%" if isinstance(tech_4h, float) else ''
            print(f"{pair:<10} {len(combined_hits_4h):>10} {str(c4h)+'%':>8} {str(c1d)+'%':>8} "
                  f"{str(tech_4h)+'%':>13} {str(mac_5d)+'%':>14}   delta={delta_4h}")

    print("\nDone.")

if __name__ == '__main__':
    run()
