#!/usr/bin/env python3
"""
compute_price_metrics.py — Compute ATR(14), realised vol, SMA-50, SMA-50 slope
for all 20 V1 Swing pairs across timeframes 1D and 4H.

1D: ATR(14), realised_vol_14/30, sma_50, sma_50_10d_ago
4H: ATR(14), realised_vol_14/30  (resampled from 1H historical_prices bars)

Safe to re-run: ON CONFLICT DO UPDATE.
Replaces the old compute_1d_price_metrics.py (which covered 10 pairs, 1D only).
"""
import sys, math, logging
import pandas as pd

sys.path.insert(0, '/root/Project_Neo_Damon/Orchestrator')
from orchestrator_agent import DatabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('compute_price_metrics')

V1_PAIRS = [
    'EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY',
    'EURGBP', 'EURAUD', 'EURCHF', 'EURCAD', 'EURJPY', 'EURNZD',
    'GBPAUD', 'GBPCAD', 'GBPCHF', 'GBPJPY',
    'AUDJPY', 'AUDCAD', 'AUDNZD',
]

ATR_PERIOD = 14

# ─────────────────────────────── helpers ────────────────────────────────────

def compute_atr_ema(highs, lows, closes, period=14):
    """Wilder ATR. Returns list of same length as inputs."""
    trs = []
    for i in range(len(closes)):
        h, l, c = highs[i], lows[i], closes[i]
        if i == 0:
            trs.append(h - l)
        else:
            trs.append(max(h - l, abs(h - closes[i-1]), abs(l - closes[i-1])))
    if len(trs) < period:
        return [None] * len(trs)
    atrs = [None] * (period - 1)
    atrs.append(sum(trs[:period]) / period)
    alpha = 1.0 / period
    for tr in trs[period:]:
        atrs.append(atrs[-1] * (1 - alpha) + tr * alpha)
    return atrs

def compute_rolling_vol(closes, window):
    """Annualised realised vol (daily log-return std × √252)."""
    ANNUALISE = math.sqrt(252)
    log_returns = [None] + [
        math.log(closes[i] / closes[i-1]) if closes[i-1] > 0 else None
        for i in range(1, len(closes))
    ]
    result = [None] * len(closes)
    for i in range(window, len(closes)):
        chunk = [r for r in log_returns[i-window+1:i+1] if r is not None]
        if len(chunk) < window:
            continue
        mean = sum(chunk) / len(chunk)
        variance = sum((x - mean)**2 for x in chunk) / (len(chunk) - 1)
        result[i] = math.sqrt(variance) * ANNUALISE
    return result

def compute_sma_series(closes, period=50):
    """Rolling SMA. Returns list; positions < period are None."""
    result = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        result[i] = sum(closes[i - period + 1:i + 1]) / period
    return result

# ─────────────────────────────── DB ─────────────────────────────────────────

db = DatabaseConnection()
db.connect()
cur = db.cursor()

total_1d = 0; total_4h = 0

# ═══════════════════════════ 1D PATH ════════════════════════════════════════

print("\n━━━━ 1D path ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
results_1d = []

for pair in V1_PAIRS:
    cur.execute("""
        SELECT ts, high, low, close
        FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = '1D'
        ORDER BY ts ASC
    """, (pair,))
    rows = cur.fetchall()

    if len(rows) < ATR_PERIOD + 1:
        logger.warning(f"{pair}: only {len(rows)} 1D bars — skipping")
        continue

    timestamps = [r['ts'] for r in rows]
    highs  = [float(r['high'])  for r in rows]
    lows   = [float(r['low'])   for r in rows]
    closes = [float(r['close']) for r in rows]

    atrs    = compute_atr_ema(highs, lows, closes, ATR_PERIOD)
    rv14    = compute_rolling_vol(closes, 14)
    rv30    = compute_rolling_vol(closes, 30)
    sma50   = compute_sma_series(closes, 50)

    # sma_50_10d_ago: the SMA-50 value from 10 bars ago (same window, shifted)
    # For position i: sma_50_10d_ago = avg(closes[i-59:i-9]) if i >= 59 else None
    sma50_10d = [None] * len(closes)
    for i in range(59, len(closes)):
        sma50_10d[i] = sum(closes[i - 59:i - 9]) / 50

    inserted = 0
    for i in range(len(rows)):
        if atrs[i] is None:
            continue
        try:
            cur.execute("""
                INSERT INTO forex_network.price_metrics
                    (instrument, timeframe, ts, atr_14,
                     realised_vol_14, realised_vol_30,
                     sma_50, sma_50_10d_ago)
                VALUES (%s, '1D', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (instrument, timeframe, ts) DO UPDATE SET
                    atr_14           = EXCLUDED.atr_14,
                    realised_vol_14  = EXCLUDED.realised_vol_14,
                    realised_vol_30  = EXCLUDED.realised_vol_30,
                    sma_50           = EXCLUDED.sma_50,
                    sma_50_10d_ago   = EXCLUDED.sma_50_10d_ago
            """, (
                pair, timestamps[i],
                round(atrs[i], 6),
                round(rv14[i], 6) if rv14[i] else None,
                round(rv30[i], 6) if rv30[i] else None,
                round(sma50[i], 6) if sma50[i] else None,
                round(sma50_10d[i], 6) if sma50_10d[i] else None,
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"{pair} 1D row {i}: {e}")
            db.rollback()
    db.commit()
    pip = 100 if 'JPY' in pair else 10000
    latest = atrs[-1] or 0
    latest_sma = sma50[-1]
    latest_close = closes[-1]
    slope_str = ""
    if sma50[-1] and sma50_10d[-1]:
        slope = sma50[-1] - sma50_10d[-1]
        slope_str = f"slope={slope:+.5f}"
    above_str = "price>SMA" if latest_sma and latest_close > latest_sma else "price<SMA" if latest_sma else "n/a"
    results_1d.append((pair, inserted, latest, latest_sma))
    sma_str = f"{latest_sma:.5f}" if latest_sma else "N/A"
    logger.info(f"{pair} 1D: {inserted} rows | ATR={latest:.5f} ({latest*pip*2:.1f}pip stop) | SMA50={sma_str} {above_str} {slope_str}")
    total_1d += inserted

# ═══════════════════════════ 4H PATH ════════════════════════════════════════

print("\n━━━━ 4H path (resampled from 1H bars) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
results_4h = []

for pair in V1_PAIRS:
    # Fetch last 210 1H bars (sufficient for 4H resample → ~52 4H bars for ATR-14)
    cur.execute("""
        SELECT ts, open, high, low, close
        FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = '1H'
        ORDER BY ts DESC LIMIT 210
    """, (pair,))
    rows_raw = cur.fetchall()

    if len(rows_raw) < 60:
        logger.warning(f"{pair}: only {len(rows_raw)} 1H bars — skipping 4H")
        continue

    # Sort ascending for resample
    df = pd.DataFrame([{
        'ts': r['ts'], 'open': float(r['open']), 'high': float(r['high']),
        'low': float(r['low']), 'close': float(r['close'])
    } for r in rows_raw]).sort_values('ts').reset_index(drop=True)

    df4h = df.set_index('ts').resample('4h', label='right', closed='right').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna().reset_index()

    if len(df4h) < ATR_PERIOD + 1:
        logger.warning(f"{pair}: only {len(df4h)} 4H bars after resample — skipping")
        continue

    highs  = df4h['high'].tolist()
    lows   = df4h['low'].tolist()
    closes = df4h['close'].tolist()
    timestamps = df4h['ts'].tolist()

    atrs = compute_atr_ema(highs, lows, closes, ATR_PERIOD)
    rv14 = compute_rolling_vol(closes, 14)
    rv30 = compute_rolling_vol(closes, 30)

    inserted = 0
    for i in range(len(df4h)):
        if atrs[i] is None:
            continue
        try:
            cur.execute("""
                INSERT INTO forex_network.price_metrics
                    (instrument, timeframe, ts, atr_14,
                     realised_vol_14, realised_vol_30)
                VALUES (%s, '4H', %s, %s, %s, %s)
                ON CONFLICT (instrument, timeframe, ts) DO UPDATE SET
                    atr_14          = EXCLUDED.atr_14,
                    realised_vol_14 = EXCLUDED.realised_vol_14,
                    realised_vol_30 = EXCLUDED.realised_vol_30
            """, (
                pair, timestamps[i],
                round(atrs[i], 6),
                round(rv14[i], 6) if rv14[i] else None,
                round(rv30[i], 6) if rv30[i] else None,
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"{pair} 4H row {i}: {e}")
            db.rollback()
    db.commit()
    latest = atrs[-1] or 0
    pip = 100 if 'JPY' in pair else 10000
    results_4h.append((pair, inserted, latest))
    logger.info(f"{pair} 4H: {inserted} rows | ATR-14={latest:.5f} ({latest*pip:.1f} pips)")
    total_4h += inserted

cur.close()
db.conn.close()

print(f"\n1D total: {total_1d} rows  |  4H total: {total_4h} rows")

print(f"\n{'Pair':<10} {'1D ATR':>10} {'SMA-50':>12}")
print("─" * 36)
for pair, ins, atr, sma in results_1d:
    sma_str = f"{sma:.5f}" if sma else "N/A"
    print(f"{pair:<10} {atr:>10.5f} {sma_str:>12}")
