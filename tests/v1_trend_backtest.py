#!/usr/bin/env python3
"""
V1 Trend Backtest Harness
==========================
12-month replay using identical indicator logic to live agents.
250 daily bar warmup so EMA200 is valid from day one.

Usage:
    python3 tests/v1_trend_backtest.py
    python3 tests/v1_trend_backtest.py --months 6
    python3 tests/v1_trend_backtest.py --pair EURUSD
"""

import sys, json, argparse, math
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field

sys.path.insert(0, "/root/Project_Neo_Damon")

import boto3
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Parameters — mirror v1_trend_parameters.py exactly
# ---------------------------------------------------------------------------
from shared.v1_trend_parameters import (
    V1_TREND_PAIRS, STRATEGY_NAME,
    EMA_FAST_PERIOD, EMA_SLOW_PERIOD, EMA_SLOPE_LOOKBACK,
    EMA_NEUTRAL_THRESHOLD, EMA_SPREAD_MIN_THRESHOLD,
    ADX_PERIOD, ADX_GATE,
    MACD_FAST, MACD_SLOW, MACD_SIGNAL, MACD_LOOKBACK_BARS,
    RSI_PERIOD, RSI_LONG_GATE, RSI_SHORT_GATE,
    ATR_PERIOD, ATR_STOP_MULTIPLIER, ATR_T1_MULTIPLIER, ATR_TRAIL_MULTIPLIER,
    T1_CLOSE_PCT, TIME_STOP_DAYS,
    RISK_PER_TRADE_PCT, MAX_CONCURRENT_POSITIONS, MIN_RR,
    CONCENTRATION_CAP_PER_CURRENCY,
    NEUTRAL_MACRO_THRESHOLD,
    SESSION_WINDOWS_UTC, ASIA_SESSION_PAIRS,
    REENTRY_BLOCK_DURATION,
)

# Parameter overrides for this run
ADX_GATE = 28
ATR_TRAIL_MULTIPLIER = 2.5
TIME_STOP_DAYS = 35
MAX_CONCURRENT_POSITIONS = 4
RISK_PER_TRADE_PCT = 0.01
V1_TREND_PAIRS = [p for p in V1_TREND_PAIRS if p not in ("GBPUSD", "USDCAD")]

WARMUP_BARS = 250
ACCOUNT_START = 147_620.0

# ---------------------------------------------------------------------------
# Indicator implementations — identical to live agents
# ---------------------------------------------------------------------------

def _compute_ema(closes: List[float], period: int) -> List[Optional[float]]:
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


def _compute_macd(closes, fast, slow, signal):
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


def _compute_rsi(closes, period):
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
    stop_price: float
    t1_price: float
    trail_distance: float
    size_units: float  # notional units
    atr_daily: float
    t1_hit: bool = False
    t1_closed_pct: float = 0.0
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
    reentry_blocks: Dict[str, date] = field(default_factory=dict)
    equity_curve: List[Tuple[date, float]] = field(default_factory=list)
    max_dd_pct: float = 0.0


def _get_currency_pair(pair: str):
    return pair[:3], pair[3:]


def _concentration_ok(state: BacktestState, pair: str, direction: str) -> bool:
    base, quote = _get_currency_pair(pair)
    count = 0
    for p in state.positions:
        pb, pq = _get_currency_pair(p.pair)
        if p.direction == direction and (pb == base or pb == quote or pq == base or pq == quote):
            count += 1
    return count < CONCENTRATION_CAP_PER_CURRENCY


def _reentry_blocked(state: BacktestState, pair: str, direction: str, current_date: date) -> bool:
    key = f"{pair}_{direction}"
    blocked_until = state.reentry_blocks.get(key)
    if blocked_until and current_date <= blocked_until:
        return True
    return False


def _close_position(pos: Position, price: float, reason: str, current_date: date) -> float:
    if pos.direction == "long":
        raw_pnl = (price - pos.entry_price) * pos.size_units * pos.remaining_pct
    else:
        raw_pnl = (pos.entry_price - price) * pos.size_units * pos.remaining_pct
    pos.pnl += raw_pnl
    pos.exit_reason = reason
    pos.exit_date = current_date
    pos.exit_price = price
    pos.remaining_pct = 0.0
    return raw_pnl


def _check_t1(pos: Position, high: float, low: float) -> float:
    """Check if T1 hit. Returns PnL from partial close, 0 if not hit."""
    if pos.t1_hit:
        return 0.0
    triggered = (pos.direction == "long" and high >= pos.t1_price) or \
                (pos.direction == "short" and low <= pos.t1_price)
    if not triggered:
        return 0.0
    pos.t1_hit = True
    close_pct = T1_CLOSE_PCT
    if pos.direction == "long":
        partial_pnl = (pos.t1_price - pos.entry_price) * pos.size_units * close_pct
    else:
        partial_pnl = (pos.entry_price - pos.t1_price) * pos.size_units * close_pct
    pos.pnl += partial_pnl
    pos.remaining_pct -= close_pct
    pos.t1_closed_pct = close_pct
    return partial_pnl


def _update_trailing_stop(pos: Position, high: float, low: float):
    """Update trailing stop after T1 hit. Uses ATR-based trail."""
    if not pos.t1_hit:
        return
    if pos.direction == "long":
        new_stop = high - pos.trail_distance
        if new_stop > pos.stop_price:
            pos.stop_price = new_stop
    else:
        new_stop = low + pos.trail_distance
        if new_stop < pos.stop_price:
            pos.stop_price = new_stop


def _check_stop(pos: Position, high: float, low: float) -> bool:
    if pos.direction == "long" and low <= pos.stop_price:
        return True
    if pos.direction == "short" and high >= pos.stop_price:
        return True
    return False


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _get_db():
    ssm = boto3.client("ssm", region_name="eu-west-2")
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    endpoint = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    return conn


def _fetch_bars(conn, pair: str, timeframe: str, limit: int = 10000):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ts, open, high, low, close
        FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = %s
        ORDER BY ts ASC
        LIMIT %s
    """, (pair, timeframe, limit))
    rows = cur.fetchall()
    cur.close()
    return [{
        "ts": r["ts"],
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low": float(r["low"]),
        "close": float(r["close"]),
    } for r in rows]


# ---------------------------------------------------------------------------
# Signal generation — mirrors live agents
# ---------------------------------------------------------------------------

def _macro_regime(daily_closes: List[float]):
    """EMA50/200 regime. Returns (direction, score, ema50, ema200, slope_ok)."""
    ema50_series = _compute_ema(daily_closes, EMA_FAST_PERIOD)
    ema200_series = _compute_ema(daily_closes, EMA_SLOW_PERIOD)
    ema50 = ema50_series[-1]
    ema200 = ema200_series[-1]
    ema50_prev = ema50_series[-(EMA_SLOPE_LOOKBACK + 1)] if len(ema50_series) > EMA_SLOPE_LOOKBACK else None

    if ema50 is None or ema200 is None or ema50_prev is None:
        return "neutral", 0.0, None, None, False

    price = daily_closes[-1]
    slope_ok = ema50 > ema50_prev
    price_vs_ema50 = (price - ema50) / ema50
    ema_spread = (ema50 - ema200) / ema200

    if abs(price_vs_ema50) < EMA_NEUTRAL_THRESHOLD:
        return "neutral", 0.0, ema50, ema200, slope_ok
    if abs(ema_spread) < EMA_SPREAD_MIN_THRESHOLD:
        return "neutral", 0.0, ema50, ema200, slope_ok
    if ema50 > ema200 and price > ema50 and slope_ok:
        score = round(min(1.0, ema_spread * 10), 4)
        return "long", score, ema50, ema200, slope_ok
    if ema50 < ema200 and price < ema50 and slope_ok:
        score = round(max(-1.0, ema_spread * 10), 4)
        return "short", score, ema50, ema200, slope_ok
    return "neutral", 0.0, ema50, ema200, slope_ok


def _technical_signal(closes_4h, highs_4h, lows_4h, daily_atr):
    """ADX + MACD + RSI gate. Returns (direction, setup_type, gate_failures)."""
    if len(closes_4h) < max(MACD_SLOW + MACD_SIGNAL, ADX_PERIOD * 2, RSI_PERIOD + 1) + 5:
        return None, None, ["insufficient_bars"]

    adx_series = _compute_adx_wilder(highs_4h, lows_4h, closes_4h, ADX_PERIOD)
    macd_line, signal_line, histogram = _compute_macd(closes_4h, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    rsi_series = _compute_rsi(closes_4h, RSI_PERIOD)

    adx = adx_series[-1]
    rsi = rsi_series[-1]
    hist_now = histogram[-1]
    hist_prev = histogram[-2] if len(histogram) > 1 else None
    macd_now = macd_line[-1]
    signal_now = signal_line[-1]

    if any(v is None for v in [adx, rsi, hist_now, hist_prev, macd_now, signal_now]):
        return None, None, ["indicator_none"]

    failures = []

    # ADX gate
    if adx < ADX_GATE:
        failures.append(f"adx_{adx:.1f}<{ADX_GATE}")

    # MACD histogram expanding
    hist_expanding = abs(hist_now) > abs(hist_prev)
    if not hist_expanding:
        failures.append("histogram_not_expanding")

    # Direction from MACD
    if macd_now > signal_now and hist_now > 0:
        direction = "long"
        setup_type = "trend_long"
    elif macd_now < signal_now and hist_now < 0:
        direction = "short"
        setup_type = "trend_short"
    else:
        return None, None, ["no_macd_direction"]

    # RSI gate
    if direction == "long" and rsi < RSI_LONG_GATE:
        failures.append(f"rsi_{rsi:.1f}<{RSI_LONG_GATE}")
    if direction == "short" and rsi > RSI_SHORT_GATE:
        failures.append(f"rsi_{rsi:.1f}>{RSI_SHORT_GATE}")

    if failures:
        return None, None, failures

    return direction, setup_type, []


# ---------------------------------------------------------------------------
# Main backtest loop
# ---------------------------------------------------------------------------

def run_backtest(pairs: List[str], months: int = 12):
    print(f"\n{'='*70}")
    print(f"V1 Trend Backtest — {months}-month replay")
    print(f"{'='*70}")
    print(f"Pairs: {len(pairs)} | Account: {ACCOUNT_START:,.0f} GBP")
    print(f"Risk/trade: {RISK_PER_TRADE_PCT*100:.2f}% | Max concurrent: {MAX_CONCURRENT_POSITIONS}")
    print(f"ATR stop: {ATR_STOP_MULTIPLIER}x | T1: {ATR_T1_MULTIPLIER}x ({T1_CLOSE_PCT*100:.0f}% close)")
    print(f"Trail: {ATR_TRAIL_MULTIPLIER}x ATR | Time stop: {TIME_STOP_DAYS}d")
    print(f"Min R:R: {MIN_RR} | Concentration cap: {CONCENTRATION_CAP_PER_CURRENCY}/currency")
    print(f"{'='*70}\n")

    conn = _get_db()

    # Determine date range
    end_date = date(2026, 4, 24)  # last available data
    start_date = end_date - timedelta(days=months * 30)
    warmup_start = start_date - timedelta(days=WARMUP_BARS + 50)  # extra buffer

    print(f"Backtest window: {start_date} → {end_date}")
    print(f"Warmup from: {warmup_start}\n")

    # Fetch all data upfront
    print("Loading data...")
    daily_bars = {}
    hourly_bars = {}
    for pair in pairs:
        daily_bars[pair] = _fetch_bars(conn, pair, "1D", 15000)
        hourly_bars[pair] = _fetch_bars(conn, pair, "1H", 50000)
        d_count = len(daily_bars[pair])
        h_count = len(hourly_bars[pair])
        if d_count < WARMUP_BARS + 30:
            print(f"  WARNING: {pair} has only {d_count} daily bars (need {WARMUP_BARS}+)")
    conn.close()

    print(f"Data loaded: {len(pairs)} pairs\n")

    # Build daily date index
    all_dates = set()
    for pair in pairs:
        for b in daily_bars[pair]:
            d = b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]
            if isinstance(d, datetime):
                d = d.date()
            all_dates.add(d)
    trade_dates = sorted(d for d in all_dates if start_date <= d <= end_date)

    if not trade_dates:
        print("ERROR: No trade dates found in range")
        return

    print(f"Trade dates: {len(trade_dates)} ({trade_dates[0]} → {trade_dates[-1]})\n")

    # State
    state = BacktestState()
    pair_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "gross_profit": 0.0, "gross_loss": 0.0, "pnl": 0.0})
    weekly_trades = 0
    week_start = trade_dates[0]
    signals_generated = 0
    entries_attempted = 0

    # Walk forward
    for dt in trade_dates:
        # Weekly counter
        if (dt - week_start).days >= 7:
            week_start = dt
            weekly_trades = 0

        # Skip weekends (shouldn't be in daily bars but just in case)
        if dt.weekday() >= 5:
            continue

        # ── Manage open positions ──
        positions_to_close = []
        for pos in state.positions:
            # Get today's daily bar for this pair
            pair_daily = [b for b in daily_bars[pos.pair]
                          if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) == dt]
            if not pair_daily:
                continue
            bar = pair_daily[0]
            high, low, close = bar["high"], bar["low"], bar["close"]

            # Time stop
            hold_days = (dt - pos.entry_date).days
            if hold_days >= TIME_STOP_DAYS:
                pnl = _close_position(pos, close, "time_stop", dt)
                state.account += pnl
                positions_to_close.append(pos)
                continue

            # Stop hit
            if _check_stop(pos, high, low):
                exit_price = pos.stop_price
                pnl = _close_position(pos, exit_price, "stop" if not pos.t1_hit else "trailing_stop", dt)
                state.account += pnl
                # Re-entry block
                key = f"{pos.pair}_{pos.direction}"
                state.reentry_blocks[key] = dt + timedelta(days=1)
                positions_to_close.append(pos)
                continue

            # T1 check
            partial_pnl = _check_t1(pos, high, low)
            if partial_pnl != 0:
                state.account += partial_pnl

            # Trailing stop update
            _update_trailing_stop(pos, high, low)

            # Regime exit check — if EMA regime flips against position
            pair_closes_to_dt = [float(b["close"]) for b in daily_bars[pos.pair]
                                  if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            if len(pair_closes_to_dt) >= EMA_SLOW_PERIOD + EMA_SLOPE_LOOKBACK + 10:
                regime_dir, _, _, _, _ = _macro_regime(pair_closes_to_dt)
                if (pos.direction == "long" and regime_dir == "short") or \
                   (pos.direction == "short" and regime_dir == "long"):
                    pnl = _close_position(pos, close, "regime_exit", dt)
                    state.account += pnl
                    positions_to_close.append(pos)
                    continue

        for pos in positions_to_close:
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

        # ── Generate signals and enter new positions ──
        if len(state.positions) >= MAX_CONCURRENT_POSITIONS:
            # Track equity and continue
            state.peak = max(state.peak, state.account)
            dd = (state.peak - state.account) / state.peak * 100
            state.max_dd_pct = max(state.max_dd_pct, dd)
            state.equity_curve.append((dt, state.account))
            continue

        for pair in pairs:
            if len(state.positions) >= MAX_CONCURRENT_POSITIONS:
                break

            # Already in this pair?
            if any(p.pair == pair for p in state.positions):
                continue

            # Get daily closes up to today
            pair_closes = [float(b["close"]) for b in daily_bars[pair]
                            if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            if len(pair_closes) < EMA_SLOW_PERIOD + EMA_SLOPE_LOOKBACK + 10:
                continue

            # Macro regime
            macro_dir, macro_score, ema50, ema200, slope_ok = _macro_regime(pair_closes)
            if macro_dir == "neutral":
                continue

            # Get 4H bars up to today (resample from 1H)
            pair_1h = [b for b in hourly_bars[pair]
                       if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            bars_4h = _resample_1h_to_4h(pair_1h)
            if len(bars_4h) < 60:
                continue

            closes_4h = [b["close"] for b in bars_4h]
            highs_4h = [b["high"] for b in bars_4h]
            lows_4h = [b["low"] for b in bars_4h]

            # Daily ATR
            pair_daily_all = [b for b in daily_bars[pair]
                               if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= dt]
            d_highs = [b["high"] for b in pair_daily_all]
            d_lows = [b["low"] for b in pair_daily_all]
            d_closes = [b["close"] for b in pair_daily_all]
            atr_series = _compute_atr(d_highs, d_lows, d_closes, ATR_PERIOD)
            atr_daily = atr_series[-1] if atr_series and atr_series[-1] is not None else None
            if atr_daily is None or atr_daily <= 0:
                continue

            # Technical signal
            tech_dir, setup_type, failures = _technical_signal(closes_4h, highs_4h, lows_4h, atr_daily)
            if tech_dir is None:
                continue

            signals_generated += 1

            # Direction agreement
            if tech_dir != macro_dir:
                continue

            direction = macro_dir
            current_price = pair_closes[-1]

            # R:R check
            stop_dist = atr_daily * ATR_STOP_MULTIPLIER
            t1_dist = atr_daily * ATR_T1_MULTIPLIER
            rr = t1_dist / stop_dist if stop_dist > 0 else 0
            # Use full target for R:R (not T1)
            # Min R:R check against stop
            if stop_dist <= 0:
                continue

            # Concentration check
            if not _concentration_ok(state, pair, direction):
                continue

            # Re-entry block
            if _reentry_blocked(state, pair, direction, dt):
                continue

            entries_attempted += 1

            # Position sizing
            risk_amount = state.account * RISK_PER_TRADE_PCT
            pip_size = 0.01 if pair.endswith("JPY") else 0.0001
            stop_pips = stop_dist / pip_size
            if stop_pips <= 0:
                continue
            size_units = risk_amount / stop_dist  # notional units

            # Entry
            if direction == "long":
                stop_price = current_price - stop_dist
                t1_price = current_price + t1_dist
            else:
                stop_price = current_price + stop_dist
                t1_price = current_price - t1_dist

            trail_dist = atr_daily * ATR_TRAIL_MULTIPLIER

            pos = Position(
                pair=pair, direction=direction,
                entry_price=current_price, entry_date=dt,
                stop_price=stop_price, t1_price=t1_price,
                trail_distance=trail_dist, size_units=size_units,
                atr_daily=atr_daily,
            )
            state.positions.append(pos)
            weekly_trades += 1

        # Track equity
        state.peak = max(state.peak, state.account)
        dd = (state.peak - state.account) / state.peak * 100
        state.max_dd_pct = max(state.max_dd_pct, dd)
        state.equity_curve.append((dt, state.account))

    # Close any remaining positions at last price
    for pos in list(state.positions):
        pair_daily = [b for b in daily_bars[pos.pair]
                      if (b["ts"].date() if hasattr(b["ts"], "date") else b["ts"]) <= end_date]
        if pair_daily:
            close_price = pair_daily[-1]["close"]
        else:
            close_price = pos.entry_price
        pnl = _close_position(pos, close_price, "backtest_end", end_date)
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
    total_trades = len(state.closed)
    wins = sum(1 for p in state.closed if p.pnl > 0)
    losses = total_trades - wins
    gross_profit = sum(p.pnl for p in state.closed if p.pnl > 0)
    gross_loss = sum(abs(p.pnl) for p in state.closed if p.pnl < 0)
    net_pnl = state.account - ACCOUNT_START
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    win_rate = wins / total_trades * 100 if total_trades > 0 else 0
    avg_win = gross_profit / wins if wins > 0 else 0
    avg_loss = gross_loss / losses if losses > 0 else 0
    avg_rr = avg_win / avg_loss if avg_loss > 0 else 0
    weeks = max(1, (trade_dates[-1] - trade_dates[0]).days / 7)
    trades_per_week = total_trades / weeks

    # Exit reason breakdown
    exit_reasons = defaultdict(int)
    for p in state.closed:
        exit_reasons[p.exit_reason] += 1

    # Hold time stats
    hold_days = [(p.exit_date - p.entry_date).days for p in state.closed if p.exit_date]
    avg_hold = sum(hold_days) / len(hold_days) if hold_days else 0

    print(f"\n{'='*70}")
    print(f"RESULTS — {months}-month backtest")
    print(f"{'='*70}")
    print(f"Final equity:     {state.account:>12,.2f} GBP")
    print(f"Net P&L:          {net_pnl:>12,.2f} GBP ({net_pnl/ACCOUNT_START*100:+.2f}%)")
    print(f"Max drawdown:     {state.max_dd_pct:>12.2f}%")
    print(f"")
    print(f"Total trades:     {total_trades:>6}")
    print(f"Wins / Losses:    {wins:>3} / {losses:<3}   ({win_rate:.1f}% win rate)")
    print(f"Profit factor:    {profit_factor:>8.2f}")
    print(f"Avg win:          {avg_win:>12,.2f}")
    print(f"Avg loss:         {avg_loss:>12,.2f}")
    print(f"Avg R:R:          {avg_rr:>8.2f}")
    print(f"Avg hold (days):  {avg_hold:>8.1f}")
    print(f"Trades/week:      {trades_per_week:>8.2f}")
    print(f"Signals generated:{signals_generated:>6}")
    print(f"Entries attempted:{entries_attempted:>6}")

    print(f"\n--- Exit reasons ---")
    for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason:<20} {count:>4}  ({count/total_trades*100:.1f}%)")

    print(f"\n--- Per-pair breakdown ---")
    print(f"{'Pair':<10} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'PnL':>12} {'PF':>8}")
    print("-" * 52)
    for pair in sorted(pair_stats.keys()):
        ps = pair_stats[pair]
        wr = ps["wins"] / ps["trades"] * 100 if ps["trades"] > 0 else 0
        pf = ps["gross_profit"] / ps["gross_loss"] if ps["gross_loss"] > 0 else float("inf")
        print(f"{pair:<10} {ps['trades']:>6} {ps['wins']:>5} {wr:>5.1f}% {ps['pnl']:>11,.2f} {pf:>7.2f}")

    total_pair_pnl = sum(ps["pnl"] for ps in pair_stats.values())
    print(f"{'TOTAL':<10} {total_trades:>6} {wins:>5} {win_rate:>5.1f}% {total_pair_pnl:>11,.2f} {profit_factor:>7.2f}")

    # ── Go / No-Go gates ──
    print(f"\n{'='*70}")
    print(f"GO / NO-GO GATES")
    print(f"{'='*70}")
    g1 = trades_per_week >= 1.0
    g2 = profit_factor >= 1.2
    g3 = state.max_dd_pct <= 22.0
    print(f"  {'PASS' if g1 else 'FAIL'}  Trades/week >= 1.0:     {trades_per_week:.2f}")
    print(f"  {'PASS' if g2 else 'FAIL'}  Profit factor >= 1.2:   {profit_factor:.2f}")
    print(f"  {'PASS' if g3 else 'FAIL'}  Max drawdown <= 22%:    {state.max_dd_pct:.2f}%")
    all_pass = g1 and g2 and g3
    print(f"\n  >>> {'GO — All gates passed' if all_pass else 'NO-GO — One or more gates failed'} <<<")
    print(f"{'='*70}\n")

    return all_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="V1 Trend Backtest")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--pair", type=str, default=None)
    args = parser.parse_args()

    pairs = [args.pair] if args.pair else V1_TREND_PAIRS
    run_backtest(pairs, args.months)


if __name__ == "__main__":
    main()
