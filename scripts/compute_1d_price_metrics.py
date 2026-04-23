"""
compute_1d_price_metrics.py — Compute 1D ATR(14) + realised vol from
historical_prices and upsert into price_metrics.

Runs for all 10 primary pairs. Safe to re-run; uses ON CONFLICT DO UPDATE.
"""
import sys
import math
sys.path.insert(0, '/root/Project_Neo_Damon/Orchestrator')
from orchestrator_agent import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('compute_1d_atr')

PAIRS = [
    'EURJPY','AUDJPY','USDJPY','GBPJPY','CADJPY',
    'EURAUD','EURUSD','GBPUSD','AUDUSD','EURGBP',
]

ATR_PERIOD = 14

def compute_atr_ema(trs, period=14):
    """Wilder EMA ATR: seed with SMA of first `period` values, then smooth."""
    if len(trs) < period:
        return [None] * len(trs)
    atrs = [None] * (period - 1)
    seed = sum(trs[:period]) / period
    atrs.append(seed)
    alpha = 1.0 / period  # Wilder smoothing = 1/N
    for tr in trs[period:]:
        atrs.append(atrs[-1] * (1 - alpha) + tr * alpha)
    return atrs

def compute_rolling_std(returns, window):
    """Rolling std over a list; returns None for positions with < window values."""
    result = [None] * len(returns)
    for i in range(window - 1, len(returns)):
        chunk = [r for r in returns[i - window + 1:i + 1] if r is not None]
        if len(chunk) < window:
            result[i] = None
        else:
            mean = sum(chunk) / len(chunk)
            variance = sum((x - mean) ** 2 for x in chunk) / (len(chunk) - 1)
            result[i] = math.sqrt(variance)
    return result

db = DatabaseConnection()
db.connect()
cur = db.cursor()
total_inserted = 0
results = []

for pair in PAIRS:
    cur.execute("""
        SELECT ts, high, low, close
        FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = '1D'
        ORDER BY ts ASC
    """, (pair,))
    rows = cur.fetchall()

    if len(rows) < ATR_PERIOD + 1:
        logger.warning(f"{pair}: only {len(rows)} 1D bars — skipping (need >= {ATR_PERIOD+1})")
        continue

    timestamps = [r['ts'] for r in rows]
    highs      = [float(r['high'])  for r in rows]
    lows       = [float(r['low'])   for r in rows]
    closes     = [float(r['close']) for r in rows]

    # True Range
    trs = []
    for i in range(len(rows)):
        h, l, c = highs[i], lows[i], closes[i]
        if i == 0:
            trs.append(h - l)
        else:
            prev_c = closes[i - 1]
            trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))

    atrs = compute_atr_ema(trs, ATR_PERIOD)

    # Log returns for realised vol (annualised)
    log_returns = [None]
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            log_returns.append(math.log(closes[i] / closes[i - 1]))
        else:
            log_returns.append(None)

    rv14_raw = compute_rolling_std(log_returns, 14)
    rv30_raw = compute_rolling_std(log_returns, 30)
    ANNUALISE = math.sqrt(252)
    rv14 = [v * ANNUALISE if v is not None else None for v in rv14_raw]
    rv30 = [v * ANNUALISE if v is not None else None for v in rv30_raw]

    inserted = 0
    for i in range(len(rows)):
        if atrs[i] is None:
            continue
        try:
            cur.execute("""
                INSERT INTO forex_network.price_metrics
                    (instrument, timeframe, ts, atr_14, realised_vol_14, realised_vol_30)
                VALUES (%s, '1D', %s, %s, %s, %s)
                ON CONFLICT (instrument, timeframe, ts) DO UPDATE SET
                    atr_14          = EXCLUDED.atr_14,
                    realised_vol_14 = EXCLUDED.realised_vol_14,
                    realised_vol_30 = EXCLUDED.realised_vol_30
            """, (
                pair, timestamps[i],
                round(atrs[i], 6),
                round(rv14[i], 6) if rv14[i] is not None else None,
                round(rv30[i], 6) if rv30[i] is not None else None,
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"{pair} row {i}: {e}")
            db.rollback()
            continue
    db.commit()

    latest_atr = atrs[-1]
    # Pip factor: JPY pairs * 100, others * 10000
    pip_factor = 100 if 'JPY' in pair else 10000
    results.append((pair, inserted, latest_atr, latest_atr * pip_factor, len(rows)))
    logger.info(f"{pair}: {inserted:,} rows upserted | latest 1D ATR = {latest_atr:.5f} ({latest_atr*pip_factor:.1f} pips)")
    total_inserted += inserted

cur.close()
db.conn.close()

print()
print(f"{'Pair':<8} {'Bars':>6}  {'Upserted':>9}  {'1D ATR':>10}  {'Pips x2.0':>12}")
print("-" * 55)
for pair, inserted, atr, pips, bars in results:
    stop_pips = pips * 2.0
    print(f"{pair:<8} {bars:>6}  {inserted:>9}  {atr:>10.5f}  {stop_pips:>12.1f}")
print(f"\nTotal upserted: {total_inserted:,}")
