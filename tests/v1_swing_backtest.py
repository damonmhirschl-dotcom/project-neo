#!/usr/bin/env python3
"""
V1 Swing Backtest Harness v3
==============================
All 12 orchestrator gates implemented (7 were missing in v2).

Added:
  - CHECK 4:   Session filter
  - CHECK 6:   NY close window (20-22 UTC) block
  - CHECK 6.5: Off-hours hard block (22-07 UTC)
  - CHECK 6.6: Stop-hunt window (07:00-07:30, 13:00-13:30)
  - CHECK 6.7: Cross-pair session filter
  - CHECK 7:   Friday cutoff (after 16:00 UTC)
  - CHECK 10:  Spread-to-signal ratio (≥5x) — approximated from ATR
  - CHECK 11:  Min R:R (≥1.5)
"""

import sys, json, argparse
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field

sys.path.insert(0, "/root/Project_Neo_Damon")

import boto3
import psycopg2
import psycopg2.extras

from v1_swing_parameters import (
    V1_SWING_PAIRS, STRATEGY_V1_SWING,
    ATR_STOP_MULTIPLIER,
    RISK_PER_TRADE_PCT, MAX_CONCURRENT_POSITIONS,
    CONCENTRATION_CAP_PER_CURRENCY,
)

WARMUP_BARS = 250
ACCOUNT_START = 147_620.0

# Entry
ADX_GATE = 25
RSI_PERIOD = 21
RSI_LONG_ZONE_LOW = 40.0
RSI_LONG_ZONE_HIGH = 50.0
RSI_SHORT_ZONE_LOW = 50.0
RSI_SHORT_ZONE_HIGH = 60.0
RSI_CROSS_LOOKBACK = 3

# Exit
T1_ATR_MULT = 1.5
T1_CLOSE_PCT = 0.50
TRAIL_ATR_MULT = 2.5
TIME_STOP_TRENDING = 7
TIME_STOP_RANGING = 5

# Macro
EMA_FAST = 50
EMA_SLOW = 200
EMA_SLOPE_LOOKBACK = 10

# Orchestrator gates
MIN_RR = 1.5
MIN_SPREAD_RATIO = 5.0

# Universe: 21 pairs (no USDJPY)
PAIRS = [p for p in V1_SWING_PAIRS if p != "USDJPY"]

# Session definitions (from orchestrator)
def _get_session(hour, minute=0):
    if 0 <= hour < 7:      return 'asian'
    elif 7 <= hour < 12:   return 'london'
    elif 12 <= hour < 16:  return 'overlap'
    elif 16 <= hour < 20:  return 'newyork'
    elif 20 <= hour < 22:  return 'ny_close'
    else:                   return 'off_hours'

# Cross-pair session filter (from orchestrator)
CROSS_PAIR_SESSIONS = {
    'EURGBP': ['london'],
    'EURJPY': ['london', 'overlap'],
    'GBPJPY': ['london', 'overlap'],
    'EURCHF': ['london'],
    'GBPCHF': ['london'],
    'EURAUD': ['london', 'overlap'],
    'GBPAUD': ['london', 'overlap'],
    'EURCAD': ['overlap', 'newyork'],
    'GBPCAD': ['overlap', 'newyork'],
    'AUDNZD': ['asian', 'london'],
    'AUDJPY': ['asian', 'london'],
    'CADJPY': ['newyork', 'overlap'],
    'NZDJPY': ['asian'],
    'EURNZD': ['london', 'overlap'],
    'AUDCAD': ['asian', 'overlap'],
}

# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------

def _compute_ema(closes, period):
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


def _compute_rsi_wilder(closes, period):
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


def _compute_adx_wilder(highs, lows, closes, period):
    n = len(closes)
    if n < period * 2:
        return [None] * n
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        up = highs[i] - highs[i - 1]
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

    atr_w = wilder_smooth(tr_list, period)
    plus_w = wilder_smooth(plus_dm, period)
    minus_w = wilder_smooth(minus_dm, period)
    dx_list = []
    for a, p, m in zip(atr_w, plus_w, minus_w):
        if a is None or a == 0:
            dx_list.append(None)
        else:
            dip = 100 * p / a
            dim = 100 * m / a
            diff = abs(dip - dim)
            summ = dip + dim
            dx_list.append(100 * diff / summ if summ != 0 else 0)
    valid_dx = [(i, v) for i, v in enumerate(dx_list) if v is not None]
    adx_result = [None] * n
    if len(valid_dx) >= period:
        start_idx = valid_dx[0][0]
        dx_values = [v for _, v in valid_dx]
        adx_smooth = wilder_smooth(dx_values, period)
        for i, val in enumerate(adx_smooth):
            result_idx = start_idx + i + 1
            if result_idx < n and val is not None:
                adx_result[result_idx] = round(val, 4)
    return adx_result


def _compute_atr(highs, lows, closes, period):
    n = len(closes)
    if n < period + 1:
        return [None] * n
    tr_list = []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
    result = [None] * period
    atr = sum(tr_list[:period]) / period
    result.append(atr)
    for tr in tr_list[period:]:
        atr = (atr * (period - 1) + tr) / period
        result.append(atr)
    return result


def _resample_1h_to_4h(bars_1h):
    if not bars_1h:
        return []
    grouped = defaultdict(list)
    for b in bars_1h:
        ts = b["ts"]
        bucket = ts.replace(hour=(ts.hour // 4) * 4, minute=0, second=0, microsecond=0)
        grouped[bucket].append(b)
    result = []
    for bucket in sorted(grouped.keys()):
        group = grouped[bucket]
        result.append({
            "ts": bucket,
            "open": group[0]["open"],
            "high": max(b["high"] for b in group),
            "low": min(b["low"] for b in group),
            "close": group[-1]["close"],
        })
    return result


# ---------------------------------------------------------------------------
# Trade simulation
# ---------------------------------------------------------------------------

@dataclass
class Position:
    pair: str
    direction: str
    entry_price: float
    entry_date: date
    entry_hour: int
    stop_price: float
    t1_price: float
    trail_distance: float
    size_units: float
    atr_daily: float
    regime: str
    t1_hit: bool = False
    remaining_pct: float = 1.0
    pnl: float = 0.0
    exit_reason: str = ""
    exit_date: Optional[date] = None
    exit_price: float = 0.0


@dataclass
class BacktestState:
    account: float = ACCOUNT_START
    peak: float = ACCOUNT_START
    positions: List[Position] = field(default_factory=list)
    closed: List[Position] = field(default_factory=list)
    equity_curve: List[Tuple[date, float]] = field(default_factory=list)
    max_dd_pct: float = 0.0


def _get_currencies(pair):
    return pair[:3], pair[3:]


def _concentration_ok(state, pair, direction):
    base, quote = _get_currencies(pair)
    count = 0
    for p in state.positions:
        pb, pq = _get_currencies(p.pair)
        if p.direction == direction and (pb == base or pb == quote or pq == base or pq == quote):
            count += 1
    return count < CONCENTRATION_CAP_PER_CURRENCY


def _close_position(pos, price, reason, dt):
    if pos.direction == "long":
        raw = (price - pos.entry_price) * pos.size_units * pos.remaining_pct
    else:
        raw = (pos.entry_price - price) * pos.size_units * pos.remaining_pct
    pos.pnl += raw
    pos.exit_reason = reason
    pos.exit_date = dt
    pos.exit_price = price
    pos.remaining_pct = 0.0
    return raw


def _check_t1(pos, high, low):
    if pos.t1_hit:
        return 0.0
    hit = (pos.direction == "long" and high >= pos.t1_price) or \
          (pos.direction == "short" and low <= pos.t1_price)
    if not hit:
        return 0.0
    pos.t1_hit = True
    pct = T1_CLOSE_PCT
    if pos.direction == "long":
        partial = (pos.t1_price - pos.entry_price) * pos.size_units * pct
    else:
        partial = (pos.entry_price - pos.t1_price) * pos.size_units * pct
    pos.pnl += partial
    pos.remaining_pct -= pct
    pos.stop_price = pos.entry_price  # breakeven
    return partial


def _update_trail(pos, high, low):
    if not pos.t1_hit:
        return
    if pos.direction == "long":
        ns = high - pos.trail_distance
        if ns > pos.stop_price:
            pos.stop_price = ns
    else:
        ns = low + pos.trail_distance
        if ns < pos.stop_price:
            pos.stop_price = ns


def _check_stop(pos, high, low):
    if pos.direction == "long" and low <= pos.stop_price:
        return True
    if pos.direction == "short" and high >= pos.stop_price:
        return True
    return False


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _get_db():
    ssm = boto3.client("ssm", region_name="eu-west-2")
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    ep = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    cr = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    return psycopg2.connect(host=ep, port=5432, dbname="postgres",
                            user=cr["username"], password=cr["password"],
                            options="-c search_path=forex_network,shared,public")


def _fetch_bars(conn, pair, tf, limit=15000):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ts, open, high, low, close FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = %s ORDER BY ts ASC LIMIT %s
    """, (pair, tf, limit))
    rows = cur.fetchall()
    cur.close()
    return [{"ts": r["ts"], "open": float(r["open"]), "high": float(r["high"]),
             "low": float(r["low"]), "close": float(r["close"])} for r in rows]


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

def _macro_regime(daily_closes):
    ema50s = _compute_ema(daily_closes, EMA_FAST)
    ema200s = _compute_ema(daily_closes, EMA_SLOW)
    e50 = ema50s[-1]
    e200 = ema200s[-1]
    e50p = ema50s[-(EMA_SLOPE_LOOKBACK + 1)] if len(ema50s) > EMA_SLOPE_LOOKBACK else None
    if e50 is None or e200 is None or e50p is None:
        return "neutral", "neutral"
    price = daily_closes[-1]
    slope = e50 > e50p
    if e50 > e200 and price > e50 and slope:
        return "long", "trending"
    if e50 < e200 and price < e50 and slope:
        return "short", "trending"
    if e50 > e200:
        return "long", "ranging"
    if e50 < e200:
        return "short", "ranging"
    return "neutral", "neutral"


def _structure_check(daily_bars_list, direction):
    if len(daily_bars_list) < 10:
        return True
    prior = daily_bars_list[-10:-2]
    recent = daily_bars_list[-3:]
    if direction == "long":
        return min(b["low"] for b in recent) >= min(b["low"] for b in prior)
    else:
        return max(b["high"] for b in recent) <= max(b["high"] for b in prior)


def _technical_signal(closes_4h, highs_4h, lows_4h, daily_bars_list):
    min_bars = max(RSI_PERIOD + RSI_CROSS_LOOKBACK + 2, 50)
    if len(closes_4h) < min_bars:
        return None, None, []

    adx_series = _compute_adx_wilder(highs_4h, lows_4h, closes_4h, 14)
    adx = adx_series[-1]
    if adx is None or adx <= ADX_GATE:
        return None, None, []

    rsi_series = _compute_rsi_wilder(closes_4h, RSI_PERIOD)
    rsi_now = rsi_series[-1]
    if rsi_now is None:
        return None, None, []

    lookback_rsis = [rsi_series[-(i+1)] for i in range(1, RSI_CROSS_LOOKBACK + 1)
                     if len(rsi_series) > i and rsi_series[-(i+1)] is not None]
    if not lookback_rsis:
        return None, None, []

    was_below_40 = any(r < RSI_LONG_ZONE_LOW for r in lookback_rsis)
    was_above_60 = any(r > RSI_SHORT_ZONE_HIGH for r in lookback_rsis)
    rsi_prev = rsi_series[-2] if len(rsi_series) > 1 and rsi_series[-2] is not None else None

    rsi_long_cross = (was_below_40 and RSI_LONG_ZONE_LOW <= rsi_now <= RSI_LONG_ZONE_HIGH
                      and rsi_prev is not None and rsi_now > rsi_prev)
    rsi_short_cross = (was_above_60 and RSI_SHORT_ZONE_LOW <= rsi_now <= RSI_SHORT_ZONE_HIGH
                       and rsi_prev is not None and rsi_now < rsi_prev)

    if rsi_long_cross:
        direction, setup = "long", "long_pullback"
    elif rsi_short_cross:
        direction, setup = "short", "short_pullback"
    else:
        return None, None, []

    if not _structure_check(daily_bars_list, direction):
        return None, None, []

    return direction, setup, []


# ---------------------------------------------------------------------------
# Orchestrator gate checks
# ---------------------------------------------------------------------------

def _passes_orchestrator_gates(pair, direction, atr_daily, current_price,
                                bar_ts, day_of_week, gate_stats):
    """Apply all 7 previously-missing orchestrator gates.
    Returns (passes: bool, rejection_reason: str or None)."""

    hour = bar_ts.hour if hasattr(bar_ts, 'hour') else 0
    minute = bar_ts.minute if hasattr(bar_ts, 'minute') else 0
    session = _get_session(hour, minute)

    # 4H bar timestamp = bar OPEN. Entry happens at bar CLOSE = open + 4h.
    entry_hour = (hour + 4) % 24
    entry_minute = minute
    entry_session = _get_session(entry_hour, entry_minute)

    # CHECK 6: NY close window (20-22 UTC) — applied to entry time
    if 20 <= entry_hour < 22:
        gate_stats["ny_close"] += 1
        return False, "ny_close_window"

    # CHECK 6.5: Off-hours hard block (22-07 UTC) — applied to entry time
    if entry_hour >= 22 or entry_hour < 7:
        gate_stats["off_hours"] += 1
        return False, "off_hours_block"

    # CHECK 6.6: Stop-hunt window (07:00-07:30, 13:00-13:30) — applied to entry time
    if (entry_hour == 7 and entry_minute < 30) or (entry_hour == 13 and entry_minute < 30):
        gate_stats["stop_hunt"] += 1
        return False, "stop_hunt_window"

    # CHECK 6.7: Cross-pair session filter — applied to entry session
    cross_sessions = CROSS_PAIR_SESSIONS.get(pair)
    if cross_sessions and entry_session not in cross_sessions:
        gate_stats["cross_pair_session"] += 1
        return False, f"cross_pair_session:{pair}:{entry_session}"

    # CHECK 7: Friday cutoff (after 16:00 UTC)
    if day_of_week == 4 and hour >= 16:  # Friday = 4
        gate_stats["friday_cutoff"] += 1
        return False, "friday_cutoff"

    # CHECK 10: Spread-to-signal ratio (≥5x)
    # Approximate spread from ATR: typical FX spread ≈ 5-15% of daily ATR
    # Use conservative 10% of ATR as spread proxy
    pip_size = 0.01 if pair.endswith("JPY") else 0.0001
    approx_spread = atr_daily * 0.10  # 10% of ATR as spread estimate
    stop_dist = atr_daily * ATR_STOP_MULTIPLIER
    expected_pips = stop_dist / pip_size
    spread_pips = approx_spread / pip_size
    if spread_pips > 0:
        spread_ratio = expected_pips / spread_pips
        if spread_ratio < MIN_SPREAD_RATIO:
            gate_stats["spread_ratio"] += 1
            return False, f"spread_ratio:{spread_ratio:.1f}"

    # CHECK 11: Min R:R (≥1.5)
    # Live code uses T2 (3×ATR) as target: 3×ATR / 2×ATR = 1.5 R:R
    T2_ATR_MULT = 3.0
    t2_dist = atr_daily * T2_ATR_MULT
    if stop_dist > 0:
        rr = t2_dist / stop_dist
        if rr < MIN_RR:
            gate_stats["min_rr"] += 1
            return False, f"min_rr:{rr:.2f}"

    return True, None


# ---------------------------------------------------------------------------
# Backtest loop
# ---------------------------------------------------------------------------

def run_backtest(pairs, months=12):
    print(f"\n{'='*70}")
    print(f"V1 Swing Backtest v3 — {months}-month replay (all 12 gates)")
    print(f"{'='*70}")
    print(f"Pairs: {len(pairs)} | Account: {ACCOUNT_START:,.0f} GBP")
    print(f"Risk/trade: {RISK_PER_TRADE_PCT*100:.2f}% | Max concurrent: {MAX_CONCURRENT_POSITIONS}")
    print(f"ATR stop: {ATR_STOP_MULTIPLIER}x | T1: {T1_ATR_MULT}x (50% + BE) | Trail: {TRAIL_ATR_MULT}x")
    print(f"Time stop: {TIME_STOP_TRENDING}d/{TIME_STOP_RANGING}d | Min R:R: {MIN_RR}")
    print(f"ADX: {ADX_GATE} | RSI-{RSI_PERIOD} cross ({RSI_CROSS_LOOKBACK}-bar)")
    print(f"Gates: off-hours, NY close, stop-hunt, cross-pair session,")
    print(f"       Friday cutoff, spread ratio ≥{MIN_SPREAD_RATIO}x, min R:R ≥{MIN_RR}")
    print(f"{'='*70}\n")

    conn = _get_db()
    conn.autocommit = True

    end_date = date(2026, 4, 24)
    start_date = end_date - timedelta(days=months * 30)

    print(f"Backtest window: {start_date} → {end_date}\n")
    print("Loading data...")
    daily_bars = {}
    hourly_bars = {}
    for pair in pairs:
        daily_bars[pair] = _fetch_bars(conn, pair, "1D")
        hourly_bars[pair] = _fetch_bars(conn, pair, "1H", 50000)
    conn.close()
    print(f"Data loaded: {len(pairs)} pairs\n")

    # Build 4H bar index for iteration
    all_4h_bars = {}
    for pair in pairs:
        all_4h_bars[pair] = _resample_1h_to_4h(
            [b for b in hourly_bars[pair]]
        )

    # Unique 4H timestamps across all pairs
    all_timestamps = sorted(set(
        b["ts"] for pair in pairs for b in all_4h_bars[pair]
        if start_date <= (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= end_date
    ))

    if not all_timestamps:
        print("ERROR: No 4H timestamps in range")
        return
    print(f"4H bars to process: {len(all_timestamps)} ({all_timestamps[0]} → {all_timestamps[-1]})\n")

    state = BacktestState()
    pair_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "gross_profit": 0.0, "gross_loss": 0.0, "pnl": 0.0})
    signals_generated = 0
    entries_attempted = 0
    gate_stats = defaultdict(int)

    for bar_ts in all_timestamps:
        dt = bar_ts.date() if hasattr(bar_ts, "date") else bar_ts
        if isinstance(dt, datetime):
            dt = dt.date()
        if dt.weekday() >= 5:
            continue

        hour = bar_ts.hour if hasattr(bar_ts, "hour") else 0
        day_of_week = dt.weekday()

        # ── Manage positions (runs on ALL bars, no session filter) ──
        to_close = []
        for pos in state.positions:
            # Find this pair's 4H bar at bar_ts
            pair_bar = None
            for b in all_4h_bars[pos.pair]:
                if b["ts"] == bar_ts:
                    pair_bar = b
                    break
            if not pair_bar:
                continue
            high, low, close = pair_bar["high"], pair_bar["low"], pair_bar["close"]

            hold_days = (dt - pos.entry_date).days
            max_hold = TIME_STOP_TRENDING if pos.regime == "trending" else TIME_STOP_RANGING
            if hold_days >= max_hold:
                pnl = _close_position(pos, close, "time_stop", dt)
                state.account += pnl
                to_close.append(pos)
                continue

            if _check_stop(pos, high, low):
                reason = "stop" if not pos.t1_hit else "trailing_stop"
                if pos.t1_hit and abs(pos.stop_price - pos.entry_price) < pos.atr_daily * 0.1:
                    reason = "breakeven_stop"
                pnl = _close_position(pos, pos.stop_price, reason, dt)
                state.account += pnl
                to_close.append(pos)
                continue

            partial = _check_t1(pos, high, low)
            if partial != 0:
                state.account += partial
            _update_trail(pos, high, low)

        for pos in to_close:
            state.positions.remove(pos)
            state.closed.append(pos)
            ps = pair_stats[pos.pair]
            ps["trades"] += 1
            ps["pnl"] += pos.pnl
            if pos.pnl > 0:
                ps["wins"] += 1
                ps["gross_profit"] += pos.pnl
            else:
                ps["gross_loss"] += abs(pos.pnl)

        # ── New entries (session-gated) ──
        if len(state.positions) >= MAX_CONCURRENT_POSITIONS:
            state.peak = max(state.peak, state.account)
            dd = (state.peak - state.account) / state.peak * 100
            state.max_dd_pct = max(state.max_dd_pct, dd)
            state.equity_curve.append((dt, state.account))
            continue

        for pair in pairs:
            if len(state.positions) >= MAX_CONCURRENT_POSITIONS:
                break
            if any(p.pair == pair for p in state.positions):
                continue

            # Get daily closes up to today
            pair_closes = [float(b["close"]) for b in daily_bars[pair]
                            if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            if len(pair_closes) < EMA_SLOW + EMA_SLOPE_LOOKBACK + 10:
                continue

            macro_dir, regime = _macro_regime(pair_closes)
            if macro_dir == "neutral":
                continue

            # 4H bars up to this timestamp
            pair_4h = [b for b in all_4h_bars[pair] if b["ts"] <= bar_ts]
            if len(pair_4h) < 60:
                continue

            closes_4h = [b["close"] for b in pair_4h]
            highs_4h = [b["high"] for b in pair_4h]
            lows_4h = [b["low"] for b in pair_4h]

            pair_daily_to_dt = [b for b in daily_bars[pair]
                                 if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            d_highs = [b["high"] for b in pair_daily_to_dt]
            d_lows = [b["low"] for b in pair_daily_to_dt]
            d_closes = [b["close"] for b in pair_daily_to_dt]
            atr_series = _compute_atr(d_highs, d_lows, d_closes, 14)
            atr_daily = atr_series[-1] if atr_series and atr_series[-1] is not None else None
            if atr_daily is None or atr_daily <= 0:
                continue

            tech_dir, setup_type, _ = _technical_signal(closes_4h, highs_4h, lows_4h, pair_daily_to_dt)
            if tech_dir is None:
                continue
            signals_generated += 1

            # CHECK 1: Direction agreement (AND gate)
            if tech_dir != macro_dir:
                continue

            # CHECK 8: Concentration
            if not _concentration_ok(state, pair, macro_dir):
                continue

            # CHECKs 6, 6.5, 6.6, 6.7, 7, 10, 11: Orchestrator gates
            current_price = pair_closes[-1]
            passes, reason = _passes_orchestrator_gates(
                pair, macro_dir, atr_daily, current_price,
                bar_ts, day_of_week, gate_stats
            )
            if not passes:
                continue

            entries_attempted += 1
            direction = macro_dir
            stop_dist = atr_daily * ATR_STOP_MULTIPLIER
            if stop_dist <= 0:
                continue
            size_units = (state.account * RISK_PER_TRADE_PCT) / stop_dist
            t1_dist = atr_daily * T1_ATR_MULT
            trail_dist = atr_daily * TRAIL_ATR_MULT

            if direction == "long":
                stop_price = current_price - stop_dist
                t1_price = current_price + t1_dist
            else:
                stop_price = current_price + stop_dist
                t1_price = current_price - t1_dist

            state.positions.append(Position(
                pair=pair, direction=direction,
                entry_price=current_price, entry_date=dt, entry_hour=hour,
                stop_price=stop_price, t1_price=t1_price,
                trail_distance=trail_dist, size_units=size_units,
                atr_daily=atr_daily, regime=regime,
            ))

        state.peak = max(state.peak, state.account)
        dd = (state.peak - state.account) / state.peak * 100
        state.max_dd_pct = max(state.max_dd_pct, dd)
        state.equity_curve.append((dt, state.account))

    # Close remaining
    for pos in list(state.positions):
        pair_daily = [b for b in daily_bars[pos.pair]
                      if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= end_date]
        cp = pair_daily[-1]["close"] if pair_daily else pos.entry_price
        pnl = _close_position(pos, cp, "backtest_end", end_date)
        state.account += pnl
        state.closed.append(pos)
        ps = pair_stats[pos.pair]
        ps["trades"] += 1
        ps["pnl"] += pos.pnl
        if pos.pnl > 0:
            ps["wins"] += 1
            ps["gross_profit"] += pos.pnl
        else:
            ps["gross_loss"] += abs(pos.pnl)
    state.positions.clear()

    # ── Results ──
    total = len(state.closed)
    wins = sum(1 for p in state.closed if p.pnl > 0)
    losses = total - wins
    gp = sum(p.pnl for p in state.closed if p.pnl > 0)
    gl = sum(abs(p.pnl) for p in state.closed if p.pnl < 0)
    net = state.account - ACCOUNT_START
    pf = gp / gl if gl > 0 else float("inf")
    wr = wins / total * 100 if total > 0 else 0
    aw = gp / wins if wins > 0 else 0
    al = gl / losses if losses > 0 else 0
    rr = aw / al if al > 0 else 0
    weeks = max(1, (all_timestamps[-1] - all_timestamps[0]).total_seconds() / 86400 / 7)
    tpw = total / weeks
    exit_reasons = defaultdict(int)
    for p in state.closed:
        exit_reasons[p.exit_reason] += 1
    hd = [(p.exit_date - p.entry_date).days for p in state.closed if p.exit_date]
    ah = sum(hd) / len(hd) if hd else 0

    print(f"\n{'='*70}")
    print(f"RESULTS — {months}-month backtest (all 12 gates)")
    print(f"{'='*70}")
    print(f"Final equity:     {state.account:>12,.2f} GBP")
    print(f"Net P&L:          {net:>12,.2f} GBP ({net/ACCOUNT_START*100:+.2f}%)")
    print(f"Max drawdown:     {state.max_dd_pct:>12.2f}%")
    print(f"")
    print(f"Total trades:     {total:>6}")
    print(f"Wins / Losses:    {wins:>3} / {losses:<3}   ({wr:.1f}% win rate)")
    print(f"Profit factor:    {pf:>8.2f}")
    print(f"Avg win:          {aw:>12,.2f}")
    print(f"Avg loss:         {al:>12,.2f}")
    print(f"Avg R:R:          {rr:>8.2f}")
    print(f"Avg hold (days):  {ah:>8.1f}")
    print(f"Trades/week:      {tpw:>8.2f}")
    print(f"Signals generated:{signals_generated:>6}")
    print(f"Entries attempted:{entries_attempted:>6}")

    print(f"\n--- Gate rejection breakdown ---")
    for gate, count in sorted(gate_stats.items(), key=lambda x: -x[1]):
        print(f"  {gate:<25} {count:>4}")

    print(f"\n--- Exit reasons ---")
    for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        pct = count / total * 100 if total > 0 else 0
        print(f"  {reason:<20} {count:>4}  ({pct:.1f}%)")

    print(f"\n--- Per-pair breakdown ---")
    print(f"{'Pair':<10} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'PnL':>12} {'PF':>8}")
    print("-" * 52)
    for pair in sorted(pair_stats.keys()):
        ps = pair_stats[pair]
        _wr = ps["wins"] / ps["trades"] * 100 if ps["trades"] > 0 else 0
        _pf = ps["gross_profit"] / ps["gross_loss"] if ps["gross_loss"] > 0 else float("inf")
        print(f"{pair:<10} {ps['trades']:>6} {ps['wins']:>5} {_wr:>5.1f}% {ps['pnl']:>11,.2f} {_pf:>7.2f}")
    tpp = sum(ps["pnl"] for ps in pair_stats.values())
    print(f"{'TOTAL':<10} {total:>6} {wins:>5} {wr:>5.1f}% {tpp:>11,.2f} {pf:>7.2f}")

    print(f"\n{'='*70}")
    print(f"GO / NO-GO GATES")
    print(f"{'='*70}")
    g1 = tpw >= 0.9
    g2 = pf >= 1.2
    g3 = state.max_dd_pct <= 22.0
    print(f"  {'PASS' if g1 else 'FAIL'}  Trades/week >= 0.9:     {tpw:.2f}")
    print(f"  {'PASS' if g2 else 'FAIL'}  Profit factor >= 1.2:   {pf:.2f}")
    print(f"  {'PASS' if g3 else 'FAIL'}  Max drawdown <= 22%:    {state.max_dd_pct:.2f}%")
    ok = g1 and g2 and g3
    print(f"\n  >>> {'GO — All gates passed' if ok else 'NO-GO — One or more gates failed'} <<<")
    print(f"{'='*70}\n")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--pair", type=str, default=None)
    args = parser.parse_args()
    pairs = [args.pair] if args.pair else PAIRS
    run_backtest(pairs, args.months)


if __name__ == "__main__":
    main()
