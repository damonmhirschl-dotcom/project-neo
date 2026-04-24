#!/usr/bin/env python3
"""
RSI pullback backtest — p75 macro gate variant.
Replaces the original hardcoded ±0.1 composite-diff gate with the live system's
tanh(raw_diff * 1.5) score thresholded against per-pair p75 from
forex_network.macro_percentile_thresholds.
Full 20-pair universe.
"""
import boto3, json, math, psycopg2
import pandas as pd
import numpy as np

PAIRS = {
    'AUDJPY':  ('AUD','JPY'),
    'AUDNZD':  ('AUD','NZD'),
    'AUDUSD':  ('AUD','USD'),
    'CADJPY':  ('CAD','JPY'),
    'EURAUD':  ('EUR','AUD'),
    'EURCAD':  ('EUR','CAD'),
    'EURCHF':  ('EUR','CHF'),
    'EURGBP':  ('EUR','GBP'),
    'EURJPY':  ('EUR','JPY'),
    'EURUSD':  ('EUR','USD'),
    'GBPAUD':  ('GBP','AUD'),
    'GBPCAD':  ('GBP','CAD'),
    'GBPCHF':  ('GBP','CHF'),
    'GBPJPY':  ('GBP','JPY'),
    'GBPUSD':  ('GBP','USD'),
    'NZDJPY':  ('NZD','JPY'),
    'NZDUSD':  ('NZD','USD'),
    'USDCAD':  ('USD','CAD'),
    'USDCHF':  ('USD','CHF'),
    'USDJPY':  ('USD','JPY'),
}

# Original ±0.1 EMA baselines (4H hit rate) for comparison — None where not measured
EMA_BASELINE = {
    'EURJPY': 50.6, 'GBPJPY': 48.5, 'USDJPY': 50.3,
    'EURGBP': 51.5, 'EURUSD': 49.6, 'GBPUSD': 49.9,
}

def get_conn():
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    host  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value'].split(':')[0]
    return psycopg2.connect(host=host, port=5432, dbname='postgres',
        user=creds['username'], password=creds['password'],
        sslmode='require', options='-c search_path=forex_network')

def compute_rsi(prices, period=14):
    delta    = prices.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def compute_adx(high, low, close, period=14):
    tr        = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
    atr       = tr.ewm(com=period-1, min_periods=period).mean()
    up        = high.diff()
    down      = -low.diff()
    dm_plus   = up.where((up > down) & (up > 0), 0)
    dm_minus  = down.where((down > up) & (down > 0), 0)
    di_plus   = 100 * dm_plus.ewm(com=period-1, min_periods=period).mean() / atr
    di_minus  = 100 * dm_minus.ewm(com=period-1, min_periods=period).mean() / atr
    dx        = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
    return dx.ewm(com=period-1, min_periods=period).mean()

def run():
    conn = get_conn()
    cur  = conn.cursor()

    # Load p75 thresholds for all pairs
    cur.execute("SELECT pair, p75, p50 FROM forex_network.macro_percentile_thresholds")
    p75_rows = {r[0]: (float(r[1]), float(r[2])) for r in cur.fetchall()}
    print(f"Loaded p75 thresholds for {len(p75_rows)} pairs")

    # Check macro_signals_history coverage
    cur.execute("SELECT MIN(signal_date), MAX(signal_date), COUNT(DISTINCT signal_date) FROM forex_network.macro_signals_history")
    r = cur.fetchone()
    print(f"macro_signals_history: {r[0]} → {r[1]} ({r[2]} days)\n")

    print("=== RSI PULLBACK BACKTEST — p75 MACRO GATE ===")
    print("Gate: tanh(raw_diff * 1.5) vs per-pair p75 threshold")
    print("RSI zones: long 40–55, short 45–60  |  ADX >= 20")
    print()
    print(f"{'Pair':<10} {'p75':>6} {'Sigs':>6} {'Hit4H':>7} {'Hit1D':>7} {'Hit5D':>7} {'vs_base':>9}  orig_gate_sigs")
    print("-" * 72)

    summary_rows = []

    for pair, (base, quote) in sorted(PAIRS.items()):
        # Derive per-pair p75 threshold
        if pair in p75_rows:
            p75, p50 = p75_rows[pair]
        else:
            # Fallback: compute from currencies if pair not directly in table
            base_pair = next((k for k in p75_rows if k.startswith(base[:3])), None)
            p75 = 0.35   # reasonable fallback
            p50 = 0.20

        # Macro direction from tanh-normalised score vs p75
        cur.execute("""
            SELECT b.signal_date,
                   (b.composite_score - q.composite_score) AS raw_diff
            FROM forex_network.macro_signals_history b
            JOIN forex_network.macro_signals_history q
              ON b.signal_date = q.signal_date
            WHERE b.currency = %s AND q.currency = %s
            ORDER BY b.signal_date
        """, (base, quote))
        macro_rows_p75 = {}
        macro_rows_orig = {}
        for row in cur.fetchall():
            date_str  = str(row[0])
            raw_diff  = float(row[1]) if row[1] else 0.0
            tanh_score = math.tanh(raw_diff * 1.5)
            # p75 gate (live system)
            if   tanh_score >  p75: macro_rows_p75[date_str] =  1
            elif tanh_score < -p75: macro_rows_p75[date_str] = -1
            else:                   macro_rows_p75[date_str] =  0
            # Original ±0.1 gate (for orig_gate_sigs comparison)
            if   raw_diff >  0.1: macro_rows_orig[date_str] =  1
            elif raw_diff < -0.1: macro_rows_orig[date_str] = -1
            else:                  macro_rows_orig[date_str] =  0

        if not macro_rows_p75:
            continue

        # Load 1H price history
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1H'
            ORDER BY ts ASC
        """, (pair,))
        rows = cur2.fetchall()
        if len(rows) < 500:
            print(f"{pair:<10}  skipped (only {len(rows)} bars)")
            continue

        df = pd.DataFrame(rows, columns=['timestamp','open','high','low','close'])
        df = df.set_index('timestamp').astype(float)

        rsi    = compute_rsi(df['close'])
        adx    = compute_adx(df['high'], df['low'], df['close'])
        closes = df['close']

        hits_4h, hits_1d, hits_5d = [], [], []
        orig_count = 0
        signal_count = 0

        for i in range(len(df)):
            idx        = df.index[i]
            signal_date = str(idx.date())
            macro_dir  = macro_rows_p75.get(signal_date, 0)
            orig_dir   = macro_rows_orig.get(signal_date, 0)

            if orig_dir != 0:
                orig_count += 1

            if macro_dir == 0:
                continue

            rsi_val = rsi.iloc[i]
            adx_val = adx.iloc[i]
            if pd.isna(rsi_val) or pd.isna(adx_val):
                continue
            if adx_val < 20:
                continue
            if macro_dir ==  1 and not (40 <= rsi_val <= 55):
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
            baseline = EMA_BASELINE.get(pair)
            vs_base  = f"+{h4-baseline:.1f}pp" if (baseline and h4) else "N/A"
            print(f"{pair:<10} {p75:>6.3f} {signal_count:>6} "
                  f"{str(h4)+'%':>7} {str(h1d)+'%':>7} {str(h5d)+'%':>7} "
                  f"{vs_base:>9}  {orig_count//24} days active")
            summary_rows.append((pair, signal_count, h4, h1d, h5d))
        elif signal_count > 0:
            print(f"{pair:<10}  only {signal_count} signals (< 20 threshold)")
        else:
            print(f"{pair:<10}  0 signals passed all gates")

    # Summary stats
    if summary_rows:
        valid_h4  = [r[2] for r in summary_rows if r[2] is not None]
        valid_h1d = [r[3] for r in summary_rows if r[3] is not None]
        valid_h5d = [r[4] for r in summary_rows if r[4] is not None]
        print()
        print("=== SUMMARY ===")
        print(f"Pairs with edge (4H > 52%): {sum(1 for h in valid_h4 if h > 52)} / {len(valid_h4)}")
        print(f"Avg hit rate — 4H: {sum(valid_h4)/len(valid_h4):.1f}%  "
              f"1D: {sum(valid_h1d)/len(valid_h1d):.1f}%  "
              f"5D: {sum(valid_h5d)/len(valid_h5d):.1f}%")
        total_sigs = sum(r[1] for r in summary_rows)
        print(f"Total signals across all pairs: {total_sigs:,}")

    conn.close()

if __name__ == '__main__':
    run()
