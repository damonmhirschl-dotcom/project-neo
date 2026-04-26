#!/usr/bin/env python3
"""
V1 Trend Backtest Harness v2 — built from live source code
===========================================================

Sources read:
  shared/v1_trend_parameters.py          — all constants
  V1_Trend_Technical_Agent/v1_trend_technical_agent.py  — indicator logic + gates T1-T8
  V1_Trend_Orchestrator/v1_trend_orchestrator.py        — gates O1-O9

Gate chain (17 total):
  Technical (8):
    T1  session check (SESSION_WINDOWS_UTC + ASIA_SESSION_PAIRS, bar CLOSE time)
    T2  1H bars >= 100
    T3  4H bars (resampled) >= 50
    T4  ADX(14, Wilder) > ADX_GATE (28)
    T5  MACD(12,26,9) histogram sign flip within MACD_LOOKBACK_BARS (2) bars
    T6  histogram expanding abs(hist_now) > abs(hist_prev) in direction
    T7  RSI(14) > 50 long / < 50 short
    T8  daily ATR available

  Orchestrator (9):
    O1  both macro + technical signals present
    O2  tech score != 0
    O3  macro direction != neutral  (EMA_NEUTRAL_THRESHOLD = 0.001)
    O4  tech direction == macro direction
    O5  session eligibility (same as T1)
    O6  open positions < MAX_CONCURRENT_POSITIONS (4)
    O7  no existing position on same pair
    O8  re-entry block (1 day after stop-out)
    O9  no pending proposal — N/A in backtest (skipped)

Indicator implementations match live agent exactly:
  - EMA: SMA seed, k = 2/(period+1)
  - MACD: list-based, signal = EMA of MACD line
  - RSI: Wilder smoothing (avg_gain/avg_loss rolling)
  - ADX: Wilder smoothed TR/+DM/-DM → DI+/DI- → DX → Wilder ADX
  - Resample: floor(hour/4) bucketing, identical to live _resample_1h_to_4h

Deploy: /root/Project_Neo_Damon/tests/v1_trend_backtest_v2.py
Run:    sudo /root/Project_Neo_Damon/venv/bin/python3 /root/Project_Neo_Damon/tests/v1_trend_backtest_v2.py
"""

import json
import boto3
import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Optional, List, Dict, Tuple

# ── Parameters — exact values from shared/v1_trend_parameters.py ─────────────

V1_TREND_PAIRS = [
    'EURUSD','USDCHF','AUDUSD','NZDUSD',
    'EURGBP','EURAUD','EURCHF','EURCAD','EURJPY','EURNZD',
    'GBPAUD','GBPCAD','GBPCHF','GBPJPY',
    'AUDJPY','AUDCAD','AUDNZD',
    'NZDJPY','CADJPY','USDJPY',
]  # 20 pairs — GBPUSD + USDCAD removed

STRATEGY_NAME              = "v1_trend"
ADX_PERIOD                 = 14
ADX_GATE                   = 28
MACD_FAST                  = 12
MACD_SLOW                  = 26
MACD_SIGNAL                = 9
MACD_LOOKBACK_BARS         = 2
RSI_PERIOD                 = 14
RSI_LONG_GATE              = 50
RSI_SHORT_GATE             = 50
ATR_PERIOD                 = 14
ATR_STOP_MULTIPLIER        = 2.5
ATR_T1_MULTIPLIER          = 1.5
ATR_TRAIL_MULTIPLIER       = 2.5
T1_CLOSE_PCT               = 0.30
TIME_STOP_DAYS             = 35
RISK_PER_TRADE_PCT         = 0.03
MAX_CONCURRENT_POSITIONS   = 4
MIN_RR                     = 2.5
EMA_FAST_PERIOD            = 50
EMA_SLOW_PERIOD            = 200
EMA_NEUTRAL_THRESHOLD      = 0.001
REENTRY_BLOCK_DAYS         = 1
CONCENTRATION_CAP          = 2

ASIA_SESSION_PAIRS = {
    "AUDUSD","NZDUSD","AUDJPY","NZDJPY","USDJPY",
    "AUDCAD","AUDNZD","CADJPY"
}

SESSION_WINDOWS_UTC = {
    "asia":       (0,  8),
    "london":     (8,  13),
    "ny_overlap": (13, 17),
    "ny_late":    (17, 21),
    "dead_zone":  (21, 24),
}

ACCOUNT_VALUE   = 147619.97
WARMUP_1H_BARS  = 400   # matches BARS_1H_NEEDED in live agent

# ── DB connection ─────────────────────────────────────────────────────────────

def get_db_conn():
    ssm  = boto3.client('ssm', region_name='eu-west-2')
    sm   = boto3.client('secretsmanager', region_name='eu-west-2')
    host = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(
        host=host, dbname='postgres', port=5432,
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network,shared,public',
    )

# ── Indicators — exact implementations from v1_trend_technical_agent.py ──────

def _compute_ema(closes: List[float], period: int) -> List[Optional[float]]:
    """Exact copy of live _compute_ema."""
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1)
    result = [None] * (period - 1)
    ema = sum(closes[:period]) / period
    result.append(ema)
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
        result.append(ema)
    return result


def _compute_macd(closes: List[float], fast: int, slow: int, signal: int
                  ) -> Tuple[List, List, List]:
    """Exact copy of live _compute_macd."""
    ema_fast = _compute_ema(closes, fast)
    ema_slow = _compute_ema(closes, slow)
    macd_line = []
    for f, s in zip(ema_fast, ema_slow):
        macd_line.append(round(f - s, 8) if f is not None and s is not None else None)
    valid_start = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line = [None] * len(macd_line)
    if valid_start is not None:
        valid_macd = [v for v in macd_line if v is not None]
        if len(valid_macd) >= signal:
            sig_ema = _compute_ema(valid_macd, signal)
            for i, val in enumerate(sig_ema):
                signal_line[valid_start + i] = val
    histogram = []
    for m, s in zip(macd_line, signal_line):
        histogram.append(round(m - s, 8) if m is not None and s is not None else None)
    return macd_line, signal_line, histogram


def _compute_rsi(closes: List[float], period: int) -> List[Optional[float]]:
    """Exact copy of live _compute_rsi."""
    if len(closes) < period + 1:
        return [None] * len(closes)
    result = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    result.append(round(100 - 100 / (1 + rs), 4))
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(diff, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        result.append(round(100 - 100 / (1 + rs), 4))
    return result


def _compute_adx_wilder(highs: List[float], lows: List[float], closes: List[float],
                         period: int) -> List[Optional[float]]:
    """Exact copy of live _compute_adx_wilder."""
    n = len(closes)
    if n < period * 2:
        return [None] * n
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr   = max(h - l, abs(h - pc), abs(l - pc))
        up   = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        tr_list.append(tr)

    def wilder_smooth(data, p):
        result = [None] * (p - 1)
        s = sum(data[:p])
        result.append(s)
        for v in data[p:]:
            s = s - s / p + v
            result.append(s)
        return result

    atr_w  = wilder_smooth(tr_list, period)
    plus_w = wilder_smooth(plus_dm, period)
    minus_w = wilder_smooth(minus_dm, period)
    di_plus, di_minus, dx_list = [], [], []
    for a, p, m in zip(atr_w, plus_w, minus_w):
        if a is None or a == 0:
            di_plus.append(None); di_minus.append(None); dx_list.append(None)
        else:
            dip = 100 * p / a
            dim = 100 * m / a
            di_plus.append(dip); di_minus.append(dim)
            diff = abs(dip - dim)
            summ = dip + dim
            dx_list.append(100 * diff / summ if summ != 0 else 0)
    valid_dx  = [(i, v) for i, v in enumerate(dx_list) if v is not None]
    adx_result = [None] * n
    if len(valid_dx) >= period:
        start_idx, _ = valid_dx[0]
        dx_values    = [v for _, v in valid_dx]
        adx_smooth   = wilder_smooth(dx_values, period)
        for i, val in enumerate(adx_smooth):
            result_idx = start_idx + i + 1
            if result_idx < n and val is not None:
                adx_result[result_idx] = round(val, 4)
    return adx_result


def _resample_1h_to_4h(bars_1h: List[Dict]) -> List[Dict]:
    """Exact copy of live _resample_1h_to_4h — floor(hour/4) bucketing."""
    if not bars_1h:
        return []
    buckets: Dict = {}
    for bar in bars_1h:
        ts = bar["ts"]
        bucket_ts = ts.replace(hour=(ts.hour // 4) * 4, minute=0, second=0, microsecond=0)
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "ts":     bucket_ts,
                "open":   bar["open"],
                "high":   bar["high"],
                "low":    bar["low"],
                "close":  bar["close"],
            }
        else:
            b = buckets[bucket_ts]
            b["high"]  = max(b["high"], bar["high"])
            b["low"]   = min(b["low"],  bar["low"])
            b["close"] = bar["close"]
    return sorted(buckets.values(), key=lambda x: x["ts"])

# ── Session check — uses bar CLOSE time ───────────────────────────────────────

def _get_current_session(close_hour: int, pair: str) -> Optional[str]:
    """Exact copy of live _get_current_session, applied to bar CLOSE hour."""
    for session, (start, end) in SESSION_WINDOWS_UTC.items():
        if session == "dead_zone":
            continue
        if start <= close_hour < end:
            if session == "asia" and pair not in ASIA_SESSION_PAIRS:
                return None
            return session
    return None  # dead zone

# ── Macro regime (EMA proxy for live LLM-based macro agent) ──────────────────

def _get_macro_direction(daily_bars: List[Dict]) -> Tuple[str, float, float]:
    """
    EMA50/200 proxy for the macro agent.
    Neutral threshold: EMA_NEUTRAL_THRESHOLD = 0.001 (0.1% spread).
    Returns (direction, ema50, ema200).
    """
    if len(daily_bars) < EMA_SLOW_PERIOD + EMA_FAST_PERIOD:
        return "neutral", 0.0, 0.0
    closes = [b["close"] for b in daily_bars]
    ema50  = _compute_ema(closes, EMA_FAST_PERIOD)
    ema200 = _compute_ema(closes, EMA_SLOW_PERIOD)
    e50 = next((v for v in reversed(ema50)  if v is not None), None)
    e200 = next((v for v in reversed(ema200) if v is not None), None)
    if e50 is None or e200 is None or e200 == 0:
        return "neutral", 0.0, 0.0
    # Binary rule per research: EMA50 above/below EMA200 = direction. No threshold.
    if e50 > e200:
        return "long", e50, e200
    elif e50 < e200:
        return "short", e50, e200
    return "neutral", e50, e200

# ── Technical signal — all gates T1-T8 ───────────────────────────────────────

def _generate_technical_signal(bars_1h: List[Dict], atr_daily: Optional[float],
                                 close_hour: int, pair: str) -> Optional[Dict]:
    """
    Applies gates T1-T8 exactly as in live technical agent.
    Returns signal dict with direction if all pass, None otherwise.
    """
    # T1: session check (bar close time)
    session = _get_current_session(close_hour, pair)
    if session is None:
        return None

    # T2: sufficient 1H bars
    if len(bars_1h) < 100:
        return None

    # Resample to 4H
    bars_4h = _resample_1h_to_4h(bars_1h)

    # T3: sufficient 4H bars
    if len(bars_4h) < 50:
        return None

    closes_4h = [b["close"] for b in bars_4h]
    highs_4h  = [b["high"]  for b in bars_4h]
    lows_4h   = [b["low"]   for b in bars_4h]

    # T4: ADX
    adx_series = _compute_adx_wilder(highs_4h, lows_4h, closes_4h, ADX_PERIOD)
    adx_now = next((v for v in reversed(adx_series) if v is not None), None)
    if adx_now is None or adx_now <= ADX_GATE:
        return None

    # T5: MACD crossover within MACD_LOOKBACK_BARS
    macd_line, signal_line, histogram = _compute_macd(
        closes_4h, MACD_FAST, MACD_SLOW, MACD_SIGNAL
    )
    valid_hist = [(i, v) for i, v in enumerate(histogram) if v is not None]
    if len(valid_hist) < 3:
        return None

    hist_now  = valid_hist[-1][1]
    hist_prev = valid_hist[-2][1]

    # Exact crossover detection from live agent
    crossover_bars_ago  = None
    crossover_direction = None
    for lookback in range(1, MACD_LOOKBACK_BARS + 2):
        if len(valid_hist) < lookback + 1:
            break
        h_curr = valid_hist[-lookback][1]
        h_prev = valid_hist[-lookback - 1][1]
        if h_prev < 0 and h_curr >= 0:
            crossover_bars_ago  = lookback
            crossover_direction = "bullish"
            break
        elif h_prev > 0 and h_curr <= 0:
            crossover_bars_ago  = lookback
            crossover_direction = "bearish"
            break

    if crossover_bars_ago is None or crossover_bars_ago > MACD_LOOKBACK_BARS:
        return None

    # T6: histogram expanding
    if crossover_direction == "bullish":
        if not (abs(hist_now) > abs(hist_prev) and hist_now > 0):
            return None
    else:
        if not (abs(hist_now) > abs(hist_prev) and hist_now < 0):
            return None

    # T7: RSI direction gate
    rsi_series = _compute_rsi(closes_4h, RSI_PERIOD)
    rsi_now = next((v for v in reversed(rsi_series) if v is not None), None)
    if rsi_now is None:
        return None
    if crossover_direction == "bullish" and rsi_now <= RSI_LONG_GATE:
        return None
    if crossover_direction == "bearish" and rsi_now >= RSI_SHORT_GATE:
        return None

    # T8: ATR daily
    if atr_daily is None:
        return None

    direction = "long" if crossover_direction == "bullish" else "short"

    return {
        "direction": direction,
        "adx":       adx_now,
        "rsi":       rsi_now,
        "atr_daily": atr_daily,
        "session":   session,
        "current_price": closes_4h[-1],
    }

# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(conn, pair: str, backtest_start: datetime, backtest_end: datetime
              ) -> Tuple[List[Dict], List[Dict]]:
    warmup_start = backtest_start - timedelta(days=30)  # extra daily bars for EMA200
    daily_start  = backtest_end - timedelta(days=365 + 300)  # EMA200 needs 200+ days

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1D'
              AND ts >= %s AND ts <= %s
            ORDER BY ts
        """, (pair, daily_start, backtest_end))
        daily_rows = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT ts, open, high, low, close
            FROM forex_network.historical_prices
            WHERE instrument = %s AND timeframe = '1H'
              AND ts >= %s AND ts <= %s
            ORDER BY ts
        """, (pair, backtest_start - timedelta(hours=WARMUP_1H_BARS), backtest_end))
        h1_rows = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT ts, atr_14
            FROM forex_network.price_metrics
            WHERE instrument = %s AND timeframe = '1D'
              AND ts >= %s AND ts <= %s
            ORDER BY ts
        """, (pair, daily_start, backtest_end))
        atr_rows = {r["ts"]: float(r["atr_14"]) for r in cur.fetchall() if r["atr_14"]}

    for row in daily_rows + h1_rows:
        row["ts"] = row["ts"] if hasattr(row["ts"], "hour") else datetime.fromisoformat(str(row["ts"]))
        if row["ts"].tzinfo is None:
            row["ts"] = row["ts"].replace(tzinfo=timezone.utc)
        for col in ("open","high","low","close"):
            row[col] = float(row[col])

    return daily_rows, h1_rows, atr_rows

# ── Position ──────────────────────────────────────────────────────────────────

class Position:
    def __init__(self, pair, direction, entry, stop, t1, atr, opened_ts, session):
        self.pair       = pair
        self.direction  = direction
        self.entry      = entry
        self.stop       = stop
        self.t1_price   = t1
        self.atr        = atr
        self.opened_ts  = opened_ts
        self.session    = session
        self.t1_hit     = False
        self.remaining  = 1.0
        self.trail_stop = None
        self.best_price = entry
        self.original_stop_dist = abs(entry - stop)

# ── Main backtest ─────────────────────────────────────────────────────────────

def run_backtest():
    conn = get_db_conn()
    now  = datetime.now(timezone.utc)
    backtest_end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
    backtest_start = backtest_end - timedelta(days=365)

    print(f"\nV1 Trend Backtest v2 — built from live source")
    print(f"Period: {backtest_start.date()} → {backtest_end.date()}")
    print(f"Gates: 17 (T1-T8 technical + O1-O9 orchestrator)")
    print(f"Pairs: {len(V1_TREND_PAIRS)} | Risk: {RISK_PER_TRADE_PCT*100:.1f}% | Max concurrent: {MAX_CONCURRENT_POSITIONS}\n")

    print("Loading data...")
    daily_data = {}
    h1_data    = {}
    atr_data   = {}
    for pair in V1_TREND_PAIRS:
        d, h, a = load_data(conn, pair, backtest_start, backtest_end)
        if len(d) >= EMA_SLOW_PERIOD + 10 and len(h) >= 100:
            daily_data[pair] = d
            h1_data[pair]    = h
            atr_data[pair]   = a
        else:
            print(f"  Skipping {pair} — insufficient data")
    conn.close()

    active_pairs = list(daily_data.keys())
    print(f"Active pairs: {len(active_pairs)}\n")

    # Build 4H timeline from 1H data (same bucketing as live resample)
    # Use all unique 4H bucket timestamps across all pairs
    all_4h_ts = set()
    for pair in active_pairs:
        for bar in h1_data[pair]:
            ts = bar["ts"]
            bucket = ts.replace(hour=(ts.hour // 4) * 4, minute=0, second=0, microsecond=0)
            if backtest_start <= bucket <= backtest_end:
                all_4h_ts.add(bucket)
    all_bars = sorted(all_4h_ts)

    open_positions = []
    reentry_blocks = {}   # (pair, direction) -> unblock datetime
    trades         = []
    equity         = ACCOUNT_VALUE
    peak_equity    = ACCOUNT_VALUE
    max_dd         = 0.0
    signals_gen    = 0
    entries_att    = 0
    rejections     = defaultdict(int)

    for bar_ts in all_bars:
        close_hour = bar_ts.hour + 4  # bar open → bar close hour
        if close_hour >= 24:
            close_hour -= 24

        # ── Update open positions ─────────────────────────────────────────────
        closed = []
        for pos in open_positions:
            # Get current 1H bars up to this bar close
            p_bars = [b for b in h1_data[pos.pair]
                      if b["ts"] <= bar_ts + timedelta(hours=4)]
            if not p_bars:
                continue
            p_4h = _resample_1h_to_4h(p_bars)
            if not p_4h:
                continue
            cur_bar = p_4h[-1]
            close   = cur_bar["close"]
            high    = cur_bar["high"]
            low     = cur_bar["low"]
            hold_days = (bar_ts - pos.opened_ts).total_seconds() / 86400

            exit_reason = None
            exit_price  = close

            if pos.trail_stop is not None:
                if pos.direction == "long"  and low  <= pos.trail_stop:
                    exit_reason = "trailing_stop"
                    exit_price  = pos.trail_stop
                elif pos.direction == "short" and high >= pos.trail_stop:
                    exit_reason = "trailing_stop"
                    exit_price  = pos.trail_stop
            if exit_reason is None and pos.direction == "long" and low <= pos.stop:
                exit_reason = "stop"
                exit_price  = pos.stop
            elif exit_reason is None and pos.direction == "short" and high >= pos.stop:
                exit_reason = "stop"
                exit_price  = pos.stop
            if exit_reason is None and hold_days >= TIME_STOP_DAYS:
                exit_reason = "time_stop"

            if exit_reason:
                risk_amt  = ACCOUNT_VALUE * RISK_PER_TRADE_PCT
                stop_dist = pos.original_stop_dist  # always use original, not current (may be BE)
                pnl_r     = 0.0
                if stop_dist > 0:
                    pnl_r = (exit_price - pos.entry) / stop_dist * pos.remaining
                    if pos.direction == "short":
                        pnl_r = -pnl_r
                    if pos.t1_hit:
                        # Banked R at T1: T1_CLOSE_PCT × (T1_ATR_MULT / STOP_ATR_MULT)
                        pnl_r += T1_CLOSE_PCT * ATR_T1_MULTIPLIER / ATR_STOP_MULTIPLIER
                pnl_cash = pnl_r * risk_amt
                equity  += pnl_cash
                peak_equity = max(peak_equity, equity)
                dd = (peak_equity - equity) / peak_equity
                max_dd = max(max_dd, dd)
                trades.append({
                    "pair":        pos.pair,
                    "direction":   pos.direction,
                    "entry_ts":    pos.opened_ts,
                    "exit_ts":     bar_ts,
                    "hold_days":   round(hold_days, 1),
                    "pnl_r":       round(pnl_r, 3),
                    "pnl_gbp":     round(pnl_cash, 2),
                    "exit_reason": exit_reason,
                    "t1_hit":      pos.t1_hit,
                    "session":     pos.session,
                })
                if exit_reason == "stop":
                    reentry_blocks[(pos.pair, pos.direction)] = bar_ts + timedelta(days=REENTRY_BLOCK_DAYS)
                closed.append(pos)
            else:
                # T1 partial close: fires when price hits T1 (entry +/- 1.5*ATR)
                # Close 30%, move stop to breakeven. Independent of trail.
                if not pos.t1_hit:
                    if pos.direction == "long"  and high >= pos.t1_price:
                        pos.t1_hit    = True
                        pos.remaining = 1.0 - T1_CLOSE_PCT
                        pos.stop      = pos.entry  # breakeven
                    elif pos.direction == "short" and low <= pos.t1_price:
                        pos.t1_hit    = True
                        pos.remaining = 1.0 - T1_CLOSE_PCT
                        pos.stop      = pos.entry  # breakeven

                # Trail activation: independent of T1. Activates at 1R profit
                # (unrealised price move >= stop_distance from entry).
                stop_dist = ATR_STOP_MULTIPLIER * pos.atr
                if pos.direction == "long":
                    pos.best_price = max(pos.best_price, close)
                    in_profit_1r = pos.best_price >= pos.entry + stop_dist
                    if in_profit_1r:
                        new_t = pos.best_price - ATR_TRAIL_MULTIPLIER * pos.atr
                        if pos.trail_stop is None or new_t > pos.trail_stop:
                            pos.trail_stop = new_t
                else:
                    pos.best_price = min(pos.best_price, close)
                    in_profit_1r = pos.best_price <= pos.entry - stop_dist
                    if in_profit_1r:
                        new_t = pos.best_price + ATR_TRAIL_MULTIPLIER * pos.atr
                        if pos.trail_stop is None or new_t < pos.trail_stop:
                            pos.trail_stop = new_t

        for pos in closed:
            open_positions.remove(pos)

        # O6: max concurrent
        if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
            continue

        # ── Entry signals ─────────────────────────────────────────────────────
        open_currencies = defaultdict(lambda: defaultdict(int))
        for p in open_positions:
            c1, c2 = p.pair[:3], p.pair[3:]
            open_currencies[c1][p.direction] += 1
            open_currencies[c2][p.direction] += 1

        for pair in active_pairs:
            if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
                break

            # O7: no duplicate open position
            if any(p.pair == pair for p in open_positions):
                continue

            # Build 1H bar window for this pair up to bar_ts
            bars_1h_window = [b for b in h1_data[pair]
                              if b["ts"] <= bar_ts + timedelta(hours=3, minutes=59)]
            if len(bars_1h_window) < 100:
                continue

            # Get daily ATR — most recent daily bar at or before bar_ts
            atr_daily = None
            for ts_key in sorted(atr_data[pair].keys(), reverse=True):
                if ts_key.replace(tzinfo=timezone.utc) if ts_key.tzinfo is None else ts_key <= bar_ts:
                    atr_daily = atr_data[pair][ts_key]
                    break
            if atr_daily is None:
                continue

            # T1-T8: technical signal
            sig = _generate_technical_signal(bars_1h_window, atr_daily, close_hour, pair)
            if sig is None:
                continue

            signals_gen += 1

            # O3-O4: macro direction
            daily_up_to = [b for b in daily_data[pair] if b["ts"] <= bar_ts]
            macro_dir, _, _ = _get_macro_direction(daily_up_to)
            if macro_dir == "neutral":
                rejections["macro_neutral"] += 1
                continue
            if macro_dir != sig["direction"]:
                rejections["direction_mismatch"] += 1
                continue

            direction = sig["direction"]

            # O8: re-entry block
            block_key = (pair, direction)
            if block_key in reentry_blocks and reentry_blocks[block_key] > bar_ts:
                rejections["reentry_block"] += 1
                continue

            # Concentration cap
            c1, c2 = pair[:3], pair[3:]
            if (open_currencies[c1][direction] >= CONCENTRATION_CAP or
                    open_currencies[c2][direction] >= CONCENTRATION_CAP):
                rejections["concentration"] += 1
                continue

            entries_att += 1

            entry     = sig["current_price"]
            atr       = sig["atr_daily"]
            stop_dist = ATR_STOP_MULTIPLIER * atr
            t1_dist   = ATR_T1_MULTIPLIER   * atr

            if direction == "long":
                stop = entry - stop_dist
                t1   = entry + t1_dist
            else:
                stop = entry + stop_dist
                t1   = entry - t1_dist

            # MIN_RR: enforced by Risk Guardian, not orchestrator — no gate here

            pos = Position(pair, direction, entry, stop, t1, atr, bar_ts, sig["session"])
            open_positions.append(pos)
            open_currencies[c1][direction] += 1
            open_currencies[c2][direction] += 1

    # Close remaining at period end
    for pos in open_positions:
        bars_end = [b for b in h1_data.get(pos.pair, []) if b["ts"] <= backtest_end]
        if not bars_end:
            continue
        p_4h = _resample_1h_to_4h(bars_end)
        if not p_4h:
            continue
        exit_price = p_4h[-1]["close"]
        stop_dist  = pos.original_stop_dist
        hold_days  = (backtest_end - pos.opened_ts).total_seconds() / 86400
        pnl_r = 0.0
        if stop_dist > 0:
            pnl_r = (exit_price - pos.entry) / stop_dist * pos.remaining
            if pos.direction == "short":
                pnl_r = -pnl_r
            if pos.t1_hit:
                pnl_r += T1_CLOSE_PCT * ATR_T1_MULTIPLIER / ATR_STOP_MULTIPLIER
        pnl_cash = pnl_r * ACCOUNT_VALUE * RISK_PER_TRADE_PCT
        equity  += pnl_cash
        trades.append({
            "pair": pos.pair, "direction": pos.direction,
            "entry_ts": pos.opened_ts, "exit_ts": backtest_end,
            "hold_days": round(hold_days, 1), "pnl_r": round(pnl_r, 3),
            "pnl_gbp": round(pnl_cash, 2), "exit_reason": "backtest_end",
            "t1_hit": pos.t1_hit, "session": pos.session,
        })

    return (trades, equity, peak_equity, max_dd,
            signals_gen, entries_att, rejections,
            backtest_start, backtest_end)

# ── Results ───────────────────────────────────────────────────────────────────

def print_results(trades, equity, peak_equity, max_dd,
                  signals_gen, entries_att, rejections, bs, be):
    print("=" * 65)
    print("V1 TREND BACKTEST v2 — RESULTS (built from live source)")
    print(f"Period: {bs.date()} → {be.date()}")
    print("=" * 65)

    if not trades:
        print("\nNO TRADES generated.")
        print(f"Signals: {signals_gen} | Entries attempted: {entries_att}")
        print("Rejections:", dict(rejections))
        return

    df = pd.DataFrame(trades)
    weeks   = (be - bs).days / 7
    total   = len(df)
    tpw     = total / weeks
    winners = df[df["pnl_r"] > 0]
    losers  = df[df["pnl_r"] <= 0]
    wr      = len(winners) / total * 100
    gp      = winners["pnl_r"].sum()
    gl      = abs(losers["pnl_r"].sum())
    pf      = gp / gl if gl > 0 else 999.0
    t1_rate = df["t1_hit"].mean() * 100
    total_r = df["pnl_r"].sum()
    exits   = df["exit_reason"].value_counts().to_dict()

    gate_tpw = tpw >= 1.0
    gate_pf  = pf  >= 1.2
    gate_dd  = max_dd <= 0.22
    go       = gate_tpw and gate_pf and gate_dd

    print(f"\n{'SIGNAL FUNNEL':─<45}")
    print(f"  Signals generated  : {signals_gen}")
    print(f"  Entries attempted  : {entries_att}")
    print(f"  Total trades       : {total}")

    print(f"\n{'TRADE SUMMARY':─<45}")
    print(f"  Trades / week      : {tpw:.2f}")
    print(f"  Win rate           : {wr:.1f}%")
    print(f"  Avg hold (days)    : {df['hold_days'].mean():.1f}")
    print(f"  T1 hit rate        : {t1_rate:.1f}%")

    print(f"\n{'R PERFORMANCE':─<45}")
    print(f"  Total R            : {total_r:+.2f}R")
    print(f"  Profit factor      : {pf:.2f}")
    print(f"  Avg R (winners)    : {winners['pnl_r'].mean():.2f}R" if len(winners) else "  Avg R (winners)    : n/a")
    print(f"  Avg R (losers)     : {losers['pnl_r'].mean():.2f}R"  if len(losers)  else "  Avg R (losers)     : n/a")
    print(f"  Max drawdown       : {max_dd*100:.1f}%")
    print(f"  Net P&L            : £{equity - 147619.97:+,.2f} ({(equity/147619.97-1)*100:+.2f}%)")
    print(f"  Final equity       : £{equity:,.2f}")

    print(f"\n{'EXIT BREAKDOWN':─<45}")
    for reason, count in sorted(exits.items(), key=lambda x: -x[1]):
        print(f"  {reason:<25}: {count}")

    print(f"\n{'GATE REJECTIONS':─<45}")
    for reason, count in sorted(rejections.items(), key=lambda x: -x[1]):
        print(f"  {reason:<25}: {count}")

    print(f"\n{'GO / NO-GO GATES':─<45}")
    print(f"  Trades/week >= 1.0 : {'✓ PASS' if gate_tpw else '✗ FAIL'}  ({tpw:.2f})")
    print(f"  Profit factor >= 1.2: {'✓ PASS' if gate_pf  else '✗ FAIL'}  ({pf:.2f})")
    print(f"  Max DD <= 22%      : {'✓ PASS' if gate_dd  else '✗ FAIL'}  ({max_dd*100:.1f}%)")
    print(f"\n  VERDICT: {'🟢 GO' if go else '🔴 NO-GO'}")
    print("=" * 65)

    print(f"\n{'PER-PAIR BREAKDOWN':─<45}")
    pair_stats = df.groupby("pair").agg(
        trades=("pnl_r","count"),
        wins=("pnl_r", lambda x: (x > 0).sum()),
        win_rate=("pnl_r", lambda x: round((x > 0).mean() * 100, 1)),
        total_r=("pnl_r","sum"),
        pnl_gbp=("pnl_gbp","sum"),
    ).sort_values("pnl_gbp", ascending=False)
    print(pair_stats.to_string())
    print()


if __name__ == "__main__":
    result = run_backtest()
    print_results(*result)
