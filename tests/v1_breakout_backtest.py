#!/usr/bin/env python3
"""
V1 Breakout Backtest Harness
Volatility compression → directional breakout strategy
Signal: Bollinger Band squeeze (BB inside Keltner Channel) + directional close outside BB
Filter: Macro EMA regime alignment (same as V1 Trend)
Exit: 1.5×ATR stop, T1 at 1.5×ATR (50% close + breakeven), trail 2×ATR remainder

Deploy to: /root/Project_Neo_Damon/tests/v1_breakout_backtest.py
Run: sudo /root/Project_Neo_Damon/venv/bin/python3 /root/Project_Neo_Damon/tests/v1_breakout_backtest.py
"""

import json
import boto3
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────

ACCOUNT_VALUE       = 147619.97
RISK_PER_TRADE      = 0.01        # 1% — to be optimised after initial results
MAX_CONCURRENT      = 4
CONCENTRATION_CAP   = 2           # max positions per currency per side

# Bollinger Band squeeze parameters
BB_PERIOD           = 20
BB_STD              = 2.0
KELTNER_PERIOD      = 20
KELTNER_ATR_MULT    = 1.5         # KC uses 1.5×ATR — tighter than BB std = squeeze confirmed

# Squeeze detection
SQUEEZE_LOOKBACK    = 5           # BB must have been inside KC for at least N bars

# Entry: close must break outside BB after squeeze
# Direction confirmed by macro EMA regime (same as V1 Trend)

# Exit parameters
STOP_ATR_MULT       = 1.5         # tighter stop — breakout should not retrace into band
T1_ATR_MULT         = 1.5         # T1 at 1.5×ATR from entry
T1_CLOSE_PCT        = 0.50        # close 50% at T1
TRAIL_ATR_MULT      = 2.0         # trail remainder
TIME_STOP_DAYS      = 7           # exit if T1 not hit in 7 days

# Macro regime (daily EMA, identical to V1 Trend)
EMA_FAST            = 50
EMA_SLOW            = 200
EMA_SLOPE_LOOKBACK  = 10

# Correlation block
CORRELATION_BLOCK   = 0.70

BACKTEST_MONTHS     = 12
WARMUP_BARS         = 250

PAIRS = [
    'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','NZDUSD',
    'EURGBP','EURAUD','EURCHF','EURCAD','EURJPY','EURNZD',
    'GBPAUD','GBPCAD','GBPCHF','GBPJPY',
    'AUDJPY','AUDCAD','AUDNZD',
    'NZDJPY','CADJPY',
]

# ── DB ────────────────────────────────────────────────────────────────────────

def get_db_conn():
    ssm  = boto3.client('ssm', region_name='eu-west-2')
    sm   = boto3.client('secretsmanager', region_name='eu-west-2')
    host = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(
        host=host, dbname='postgres',
        user=creds['username'], password=creds['password'], port=5432,
        options='-c search_path=forex_network,shared,public',
    )

# ── Indicators ────────────────────────────────────────────────────────────────

def compute_ema(series: np.ndarray, period: int) -> np.ndarray:
    k   = 2.0 / (period + 1)
    ema = np.full(len(series), np.nan)
    if len(series) < period:
        return ema
    ema[period - 1] = np.mean(series[:period])
    for i in range(period, len(series)):
        ema[i] = series[i] * k + ema[i - 1] * (1 - k)
    return ema

def compute_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                period: int = 14) -> np.ndarray:
    tr  = np.maximum(high[1:] - low[1:],
          np.maximum(abs(high[1:] - close[:-1]),
                     abs(low[1:]  - close[:-1])))
    atr = np.full(len(close), np.nan)
    if len(tr) < period:
        return atr
    atr[period] = np.mean(tr[:period])
    for i in range(period + 1, len(close)):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i - 1]) / period
    return atr

def compute_bollinger(close: np.ndarray, period: int = BB_PERIOD,
                      std_mult: float = BB_STD):
    mid  = np.full(len(close), np.nan)
    upper = np.full(len(close), np.nan)
    lower = np.full(len(close), np.nan)
    for i in range(period - 1, len(close)):
        window = close[i - period + 1:i + 1]
        m = np.mean(window)
        s = np.std(window, ddof=1)
        mid[i]   = m
        upper[i] = m + std_mult * s
        lower[i] = m - std_mult * s
    return mid, upper, lower

def compute_keltner(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                    period: int = KELTNER_PERIOD, mult: float = KELTNER_ATR_MULT):
    mid   = compute_ema(close, period)
    atr   = compute_atr(high, low, close, period)
    upper = mid + mult * atr
    lower = mid - mult * atr
    return mid, upper, lower

def compute_bb_width(upper: np.ndarray, lower: np.ndarray,
                     mid: np.ndarray) -> np.ndarray:
    """Normalised band width — used to detect compression."""
    return np.where(mid != 0, (upper - lower) / mid, np.nan)

# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(conn, pair: str, backtest_start: datetime, backtest_end: datetime):
    daily_start = backtest_start - timedelta(days=WARMUP_BARS * 1.5)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1D'
              AND ts >= %s AND ts <= %s
            ORDER BY ts
        """, (pair, daily_start, backtest_end))
        daily = pd.DataFrame(cur.fetchall(), columns=['ts','open','high','low','close'])

        cur.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '4H'
              AND ts >= %s AND ts <= %s
            ORDER BY ts
        """, (pair, backtest_start, backtest_end))
        h4 = pd.DataFrame(cur.fetchall(), columns=['ts','open','high','low','close'])

    for df in (daily, h4):
        df['ts'] = pd.to_datetime(df['ts'], utc=True)
        for col in ('open','high','low','close'):
            df[col] = df[col].astype(float)
        df.set_index('ts', inplace=True)

    return daily, h4

# ── Macro regime (identical to V1 Trend) ─────────────────────────────────────

def get_macro_signal(daily: pd.DataFrame, as_of: datetime):
    df = daily[daily.index <= as_of]
    if len(df) < EMA_SLOW + EMA_SLOPE_LOOKBACK + 5:
        return None, None
    c      = df['close'].values
    h      = df['high'].values
    lo     = df['low'].values
    ema50  = compute_ema(c, EMA_FAST)
    ema200 = compute_ema(c, EMA_SLOW)
    atr_arr = compute_atr(h, lo, c, 14)
    last50  = ema50[-1]
    last200 = ema200[-1]
    last_c  = c[-1]
    last_atr = atr_arr[-1]
    if np.isnan(last50) or np.isnan(last200) or np.isnan(last_atr):
        return None, None
    slope_ok = bool(last50 > ema50[-EMA_SLOPE_LOOKBACK - 1]) if len(ema50) > EMA_SLOPE_LOOKBACK else False
    if last50 > last200 and last_c > last50 and slope_ok:
        return 'long', last_atr
    elif last50 < last200 and last_c < last50:
        return 'short', last_atr
    return 'neutral', last_atr

# ── Squeeze detection ─────────────────────────────────────────────────────────

def detect_squeeze_and_signal(h4: pd.DataFrame, bar_idx: int):
    """
    Returns (direction, atr_daily_equiv) or (None, None).
    Squeeze confirmed: BB upper < KC upper AND BB lower > KC lower for SQUEEZE_LOOKBACK bars.
    Signal fires: BB was in squeeze, now close breaks outside BB.
    """
    min_bars = max(BB_PERIOD, KELTNER_PERIOD) + SQUEEZE_LOOKBACK + 5
    if bar_idx < min_bars:
        return None, None

    df  = h4.iloc[:bar_idx + 1]
    c   = df['close'].values
    h   = df['high'].values
    lo  = df['low'].values

    bb_mid, bb_upper, bb_lower = compute_bollinger(c, BB_PERIOD, BB_STD)
    kc_mid, kc_upper, kc_lower = compute_keltner(h, lo, c, KELTNER_PERIOD, KELTNER_ATR_MULT)
    atr_arr = compute_atr(h, lo, c, 14)

    last_atr = atr_arr[-1]
    if np.isnan(last_atr) or last_atr == 0:
        return None, None

    # Check squeeze was active for SQUEEZE_LOOKBACK bars ending at bar_idx - 1
    squeeze_active = True
    for k in range(1, SQUEEZE_LOOKBACK + 1):
        idx = -(k + 1)  # bars before current
        if abs(idx) > len(bb_upper):
            squeeze_active = False
            break
        if np.isnan(bb_upper[idx]) or np.isnan(kc_upper[idx]):
            squeeze_active = False
            break
        if not (bb_upper[idx] < kc_upper[idx] and bb_lower[idx] > kc_lower[idx]):
            squeeze_active = False
            break

    if not squeeze_active:
        return None, None

    # Current bar closes outside BB — breakout
    current_close = c[-1]
    if np.isnan(bb_upper[-1]) or np.isnan(bb_lower[-1]):
        return None, None

    if current_close > bb_upper[-1]:
        return 'long', last_atr
    elif current_close < bb_lower[-1]:
        return 'short', last_atr

    return None, None

# ── Session helper ────────────────────────────────────────────────────────────

def get_session(ts: datetime) -> str:
    h = ts.hour
    if  8 <= h < 17: return 'active'
    if 17 <= h < 21: return 'ny_late'
    return 'dead'

# ── Position ──────────────────────────────────────────────────────────────────

class Position:
    def __init__(self, pair, direction, entry, stop, t1, atr, opened_ts):
        self.pair       = pair
        self.direction  = direction
        self.entry      = entry
        self.stop       = stop
        self.t1_price   = t1
        self.atr        = atr
        self.opened_ts  = opened_ts
        self.t1_hit     = False
        self.remaining  = 1.0
        self.trail_stop = None
        self.best_close = entry

# ── Main backtest ─────────────────────────────────────────────────────────────

def run_backtest():
    conn = get_db_conn()
    now  = datetime.now(timezone.utc)
    backtest_end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
    backtest_start = backtest_end - timedelta(days=365)

    print(f"\nV1 Breakout Backtest — {backtest_start.date()} to {backtest_end.date()}")
    print(f"Signal: BB squeeze (BB inside KC {SQUEEZE_LOOKBACK}+ bars) + directional close outside BB")
    print(f"Filter: Macro EMA50/200 regime alignment")
    print(f"Stop: {STOP_ATR_MULT}×ATR | T1: {T1_ATR_MULT}×ATR ({int(T1_CLOSE_PCT*100)}% close) | Trail: {TRAIL_ATR_MULT}×ATR")
    print(f"Risk/trade: {RISK_PER_TRADE*100:.1f}% | Max concurrent: {MAX_CONCURRENT}\n")

    print("Loading data...")
    daily_data = {}
    h4_data    = {}
    for pair in PAIRS:
        d, h = load_data(conn, pair, backtest_start, backtest_end)
        if len(d) >= WARMUP_BARS and len(h) >= 100:
            daily_data[pair] = d
            h4_data[pair]    = h
        else:
            print(f"  Skipping {pair} — insufficient data")
    conn.close()

    active_pairs = list(daily_data.keys())
    print(f"Active pairs: {len(active_pairs)}\n")

    all_bars = sorted(set(
        ts for pair in active_pairs for ts in h4_data[pair].index
    ))

    open_positions = []
    reentry_blocks = {}
    trades         = []
    equity         = ACCOUNT_VALUE
    peak_equity    = ACCOUNT_VALUE
    max_dd         = 0.0
    signals_gen    = 0
    entries_att    = 0

    for bar_ts in all_bars:
        # Position management runs on ALL 4H bars — no session filter
        # Entry signals are filtered by session below

        # ── Update open positions ─────────────────────────────────────────────
        closed = []
        for pos in open_positions:
            if bar_ts not in h4_data[pos.pair].index:
                continue
            bar   = h4_data[pos.pair].loc[bar_ts]
            close = bar['close']
            high  = bar['high']
            low   = bar['low']
            hold_days = (bar_ts - pos.opened_ts).total_seconds() / 86400

            # Macro regime check — exit if regime reverses
            macro_dir, _ = get_macro_signal(daily_data[pos.pair], bar_ts)
            regime_rev = (
                (pos.direction == 'long'  and macro_dir in ('short', 'neutral')) or
                (pos.direction == 'short' and macro_dir in ('long',  'neutral'))
            )

            exit_reason = None
            exit_price  = close

            if regime_rev:
                exit_reason = 'regime_exit'
            elif pos.t1_hit and pos.trail_stop is not None:
                if pos.direction == 'long'  and low  <= pos.trail_stop:
                    exit_reason = 'trail_stop'
                    exit_price  = pos.trail_stop
                elif pos.direction == 'short' and high >= pos.trail_stop:
                    exit_reason = 'trail_stop'
                    exit_price  = pos.trail_stop
            elif pos.direction == 'long'  and low  <= pos.stop:
                exit_reason = 'stop_loss'
                exit_price  = pos.stop
            elif pos.direction == 'short' and high >= pos.stop:
                exit_reason = 'stop_loss'
                exit_price  = pos.stop
            elif not pos.t1_hit and hold_days > TIME_STOP_DAYS:
                exit_reason = 'time_stop'

            if exit_reason:
                risk_amt  = ACCOUNT_VALUE * RISK_PER_TRADE
                stop_dist = abs(pos.entry - pos.stop)
                if stop_dist > 0:
                    pnl_r = (exit_price - pos.entry) / stop_dist * pos.remaining
                    if pos.direction == 'short':
                        pnl_r = -pnl_r
                    if pos.t1_hit:
                        pnl_r += T1_CLOSE_PCT * T1_ATR_MULT  # banked R at T1
                else:
                    pnl_r = 0.0
                pnl_cash = pnl_r * risk_amt
                equity  += pnl_cash
                peak_equity = max(peak_equity, equity)
                dd = (peak_equity - equity) / peak_equity
                max_dd = max(max_dd, dd)

                trades.append({
                    'pair':        pos.pair,
                    'direction':   pos.direction,
                    'entry_ts':    pos.opened_ts,
                    'exit_ts':     bar_ts,
                    'hold_days':   round(hold_days, 1),
                    'pnl_r':       round(pnl_r, 3),
                    'exit_reason': exit_reason,
                    't1_hit':      pos.t1_hit,
                })
                if exit_reason == 'stop_loss':
                    reentry_blocks[(pos.pair, pos.direction)] = bar_ts + timedelta(days=1)
                closed.append(pos)
            else:
                # T1 check
                if not pos.t1_hit:
                    if pos.direction == 'long'  and high >= pos.t1_price:
                        pos.t1_hit     = True
                        pos.remaining  = 1.0 - T1_CLOSE_PCT
                        pos.stop       = pos.entry
                        pos.trail_stop = close - TRAIL_ATR_MULT * pos.atr
                    elif pos.direction == 'short' and low <= pos.t1_price:
                        pos.t1_hit     = True
                        pos.remaining  = 1.0 - T1_CLOSE_PCT
                        pos.stop       = pos.entry
                        pos.trail_stop = close + TRAIL_ATR_MULT * pos.atr
                # Trail update
                if pos.t1_hit and pos.trail_stop is not None:
                    if pos.direction == 'long':
                        pos.best_close = max(pos.best_close, close)
                        new_t = pos.best_close - TRAIL_ATR_MULT * pos.atr
                        if new_t > pos.trail_stop:
                            pos.trail_stop = new_t
                    else:
                        pos.best_close = min(pos.best_close, close)
                        new_t = pos.best_close + TRAIL_ATR_MULT * pos.atr
                        if new_t < pos.trail_stop:
                            pos.trail_stop = new_t

        for pos in closed:
            open_positions.remove(pos)

        if len(open_positions) >= MAX_CONCURRENT:
            continue

        # ── Entry signals ─────────────────────────────────────────────────────
        open_currencies = defaultdict(lambda: defaultdict(int))
        for p in open_positions:
            c1, c2 = p.pair[:3], p.pair[3:]
            open_currencies[c1][p.direction] += 1
            open_currencies[c2][p.direction] += 1

        for pair in active_pairs:
            if len(open_positions) >= MAX_CONCURRENT:
                break
            if any(p.pair == pair for p in open_positions):
                continue
            if bar_ts not in h4_data[pair].index:
                continue

            bar_idx = h4_data[pair].index.get_loc(bar_ts)

            # Squeeze + breakout signal
            breakout_dir, atr_4h = detect_squeeze_and_signal(h4_data[pair], bar_idx)
            if breakout_dir is None:
                continue
            signals_gen += 1

            # Macro regime must agree
            macro_dir, atr_daily = get_macro_signal(daily_data[pair], bar_ts)
            if macro_dir != breakout_dir:
                continue

            # Use daily ATR for stop sizing (wider, more robust)
            atr = atr_daily if atr_daily and not np.isnan(atr_daily) else atr_4h
            if atr is None or np.isnan(atr) or atr == 0:
                continue

            # Re-entry block
            block_key = (pair, breakout_dir)
            if block_key in reentry_blocks and reentry_blocks[block_key] >= bar_ts:
                continue

            # Concentration cap
            c1, c2 = pair[:3], pair[3:]
            if (open_currencies[c1][breakout_dir] >= CONCENTRATION_CAP or
                open_currencies[c2][breakout_dir] >= CONCENTRATION_CAP):
                continue

            entries_att += 1
            entry = h4_data[pair].loc[bar_ts]['close']

            if breakout_dir == 'long':
                stop = entry - STOP_ATR_MULT * atr
                t1   = entry + T1_ATR_MULT   * atr
            else:
                stop = entry + STOP_ATR_MULT * atr
                t1   = entry - T1_ATR_MULT   * atr

            pos = Position(pair, breakout_dir, entry, stop, t1, atr, bar_ts)
            open_positions.append(pos)
            open_currencies[c1][breakout_dir] += 1
            open_currencies[c2][breakout_dir] += 1

    # Close remaining positions at last bar
    for pos in open_positions:
        pair_h4 = h4_data.get(pos.pair)
        if pair_h4 is None or pair_h4.empty:
            continue
        exit_price = pair_h4.iloc[-1]['close']
        stop_dist  = abs(pos.entry - pos.stop)
        hold_days  = (all_bars[-1] - pos.opened_ts).total_seconds() / 86400
        if stop_dist > 0:
            pnl_r = (exit_price - pos.entry) / stop_dist * pos.remaining
            if pos.direction == 'short':
                pnl_r = -pnl_r
            if pos.t1_hit:
                pnl_r += T1_CLOSE_PCT * T1_ATR_MULT
        else:
            pnl_r = 0.0
        trades.append({
            'pair': pos.pair, 'direction': pos.direction,
            'entry_ts': pos.opened_ts, 'exit_ts': all_bars[-1],
            'hold_days': round(hold_days, 1), 'pnl_r': round(pnl_r, 3),
            'exit_reason': 'backtest_end', 't1_hit': pos.t1_hit,
        })

    return trades, equity, peak_equity, max_dd, signals_gen, entries_att, backtest_start, backtest_end

# ── Results ───────────────────────────────────────────────────────────────────

def print_results(trades, equity, peak_equity, max_dd,
                  signals_gen, entries_att, bs, be):
    if not trades:
        print("NO TRADES generated.\n")
        return

    df = pd.DataFrame(trades)
    total   = len(df)
    weeks   = (be - bs).days / 7
    tpw     = total / weeks
    winners = df[df['pnl_r'] > 0]
    losers  = df[df['pnl_r'] <= 0]
    wr      = len(winners) / total * 100
    gp      = winners['pnl_r'].sum()
    gl      = abs(losers['pnl_r'].sum())
    pf      = gp / gl if gl > 0 else 999.0
    avg_w   = winners['pnl_r'].mean() if len(winners) else 0
    avg_l   = losers['pnl_r'].mean()  if len(losers)  else 0
    avg_h   = df['hold_days'].mean()
    t1_rate = df['t1_hit'].mean() * 100
    total_r = df['pnl_r'].sum()
    exits   = df['exit_reason'].value_counts().to_dict()

    gate_tpw = tpw >= 2.0
    gate_pf  = pf  >= 1.2
    gate_dd  = max_dd <= 0.22
    go       = gate_tpw and gate_pf and gate_dd

    print("=" * 60)
    print("V1 BREAKOUT BACKTEST RESULTS")
    print(f"Period: {bs.date()} → {be.date()} ({weeks:.1f} weeks)")
    print("=" * 60)

    print(f"\n{'TRADE SUMMARY':─<40}")
    print(f"  Signals generated  : {signals_gen}")
    print(f"  Entries attempted  : {entries_att}")
    print(f"  Total trades       : {total}")
    print(f"  Trades / week      : {tpw:.2f}")
    print(f"  Win rate           : {wr:.1f}%")
    print(f"  Avg hold (days)    : {avg_h:.1f}")
    print(f"  T1 hit rate        : {t1_rate:.1f}%")

    print(f"\n{'R PERFORMANCE':─<40}")
    print(f"  Total R            : {total_r:+.2f}R")
    print(f"  Profit factor      : {pf:.2f}")
    print(f"  Avg R (winners)    : {avg_w:.2f}R")
    print(f"  Avg R (losers)     : {avg_l:.2f}R")
    print(f"  Max drawdown       : {max_dd*100:.1f}%")
    print(f"  Net P&L            : £{equity - ACCOUNT_VALUE:+,.2f} ({(equity/ACCOUNT_VALUE - 1)*100:+.2f}%)")
    print(f"  Final equity       : £{equity:,.2f}")

    print(f"\n{'EXIT BREAKDOWN':─<40}")
    for reason, count in sorted(exits.items(), key=lambda x: -x[1]):
        print(f"  {reason:<22} : {count}")

    print(f"\n{'GO / NO-GO GATES':─<40}")
    print(f"  Trades/week >= 2.0 : {'✓ PASS' if gate_tpw else '✗ FAIL'}  ({tpw:.2f})")
    print(f"  Profit factor >= 1.2: {'✓ PASS' if gate_pf  else '✗ FAIL'}  ({pf:.2f})")
    print(f"  Max DD <= 22%      : {'✓ PASS' if gate_dd  else '✗ FAIL'}  ({max_dd*100:.1f}%)")
    print(f"\n  VERDICT: {'🟢 GO' if go else '🔴 NO-GO'}")
    print("=" * 60)

    print(f"\n{'PER-PAIR BREAKDOWN':─<40}")
    pair_stats = df.groupby('pair').agg(
        trades=('pnl_r','count'),
        total_r=('pnl_r','sum'),
        win_rate=('pnl_r', lambda x: (x > 0).mean() * 100),
        avg_r=('pnl_r','mean')
    ).sort_values('total_r', ascending=False)
    print(pair_stats.to_string())
    print()

if __name__ == '__main__':
    trades, equity, peak_equity, max_dd, sig, ent, bs, be = run_backtest()
    print_results(trades, equity, peak_equity, max_dd, sig, ent, bs, be)
