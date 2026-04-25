# =============================================================================
# V1 Trend — Parameter File
# Single source of truth for all V1 Trend constants.
# All agents import from here. One edit propagates everywhere.
# Never hardcode these values in agent files.
# =============================================================================

from v1_swing_parameters import V1_SWING_PAIRS  # shared universe, single source

# ---------------------------------------------------------------------------
# Strategy identity
# ---------------------------------------------------------------------------
STRATEGY_NAME = "v1_trend"

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
V1_TREND_PAIRS = V1_SWING_PAIRS  # identical 22-pair G10 universe

# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------
SIGNAL_TIMEFRAME  = "4H"   # resampled from 1H — same pattern as V1 Swing
CONTEXT_TIMEFRAME = "1D"   # EMA regime computed on daily bars

# ---------------------------------------------------------------------------
# EMA regime (Macro Agent)
# ---------------------------------------------------------------------------
EMA_FAST_PERIOD          = 50    # EMA50 — intermediate trend
EMA_SLOW_PERIOD          = 200   # EMA200 — long-term trend
EMA_SLOPE_LOOKBACK       = 10    # days: EMA50 must be > EMA50(10d ago)
EMA_NEUTRAL_THRESHOLD    = 0.001 # 0.1% price-to-EMA50 proximity → neutral
EMA_SPREAD_MIN_THRESHOLD = 0.0005 # 0.05% EMA50/EMA200 spread → neutral if below

# ---------------------------------------------------------------------------
# ADX (Technical Agent)
# ---------------------------------------------------------------------------
ADX_PERIOD = 14   # Wilder smoothing, alpha = 1/14
ADX_GATE   = 25   # Wilder original; Trading Strategy Guides; Opofinance

# ---------------------------------------------------------------------------
# MACD (Technical Agent)
# ---------------------------------------------------------------------------
MACD_FAST          = 12  # Gerald Appel original
MACD_SLOW          = 26
MACD_SIGNAL        = 9
MACD_LOOKBACK_BARS = 2   # crossover must have occurred within last N 4H bars

# ---------------------------------------------------------------------------
# RSI (Technical Agent)
# ---------------------------------------------------------------------------
RSI_PERIOD       = 14
RSI_LONG_GATE    = 50   # RSI > 50 required for long — directional, not zone
RSI_SHORT_GATE   = 50   # RSI < 50 required for short

# ---------------------------------------------------------------------------
# ATR — stop and trail (Execution Agent)
# ---------------------------------------------------------------------------
ATR_PERIOD          = 14
ATR_STOP_MULTIPLIER = 2.5   # stop = entry ± 2.5× ATR(14, daily)
ATR_T1_MULTIPLIER   = 1.5   # T1   = entry ± 1.5× ATR(14, daily)
ATR_TRAIL_MULTIPLIER = 2.0  # trail = 2× ATR behind highest/lowest close since entry

# ---------------------------------------------------------------------------
# Position management
# ---------------------------------------------------------------------------
T1_CLOSE_PCT      = 0.30  # close 30% at T1; 70% runs to trail
TIME_STOP_DAYS    = 25    # close if T1 not hit after 25 days

# ---------------------------------------------------------------------------
# Risk and sizing
# ---------------------------------------------------------------------------
RISK_PER_TRADE_PCT          = 0.0075  # 0.75% — narrower than V1 Swing's 1.0%
MAX_CONCURRENT_POSITIONS    = 3
MIN_RR                      = 2.5     # minimum R:R post swap-cost — higher than V1 Swing's 2.0
CONCENTRATION_CAP_PER_CURRENCY = 2   # max 2 positions per currency per side
CORRELATION_BLOCK_THRESHOLD = 0.70   # 30-day correlation

# ---------------------------------------------------------------------------
# Hard rules — drawdown halts (shared thresholds with V1 Swing)
# ---------------------------------------------------------------------------
DAILY_DRAWDOWN_HALT_PCT  = 0.03   # -3% equity → close all, halt until next UTC day
WEEKLY_DRAWDOWN_HALT_PCT = 0.07   # -7% equity → halt new entries, no force-close

# ---------------------------------------------------------------------------
# News blackout
# ---------------------------------------------------------------------------
NEWS_BLACKOUT_HOURS = 4   # ±4H around FOMC, NFP, CPI, BoE, ECB, BoJ, SNB, RBA, BoC, RBNZ

# ---------------------------------------------------------------------------
# Combined heat cap (shared with V1 Swing — enforced by RG)
# ---------------------------------------------------------------------------
COMBINED_MAX_HEAT_PCT  = 0.05   # 5.0% combined open risk → RG rejects both strategies
COMBINED_HEAT_RESUME_PCT = 0.04 # resume accepting proposals when heat drops below 4.0%

# ---------------------------------------------------------------------------
# Re-entry blocks
# ---------------------------------------------------------------------------
REENTRY_BLOCK_DURATION = "day"  # block remainder of current UTC day after stop-out

# ---------------------------------------------------------------------------
# Session rules (pair eligibility per session)
# ---------------------------------------------------------------------------
ASIA_SESSION_PAIRS = [
    "AUDUSD", "NZDUSD", "AUDJPY", "NZDJPY", "USDJPY",
    "AUDCAD", "AUDNZD", "CADJPY"
]
# London, NY overlap, NY late: all 22 pairs eligible

SESSION_WINDOWS_UTC = {
    "asia":       (0,  8),
    "london":     (8,  13),
    "ny_overlap": (13, 17),
    "ny_late":    (17, 21),
    "dead_zone":  (21, 24),  # no new entries
}

# ---------------------------------------------------------------------------
# Enum values — must stay in sync with v1_swing_parameters.STRATEGY_NAMES
# ---------------------------------------------------------------------------
SETUP_TYPES = ("trend_long", "trend_short")

EXIT_REASONS = (
    "stop",
    "trailing_stop",
    "regime_exit",
    "time_stop",
    "daily_halt",
    "manual",
)

# ---------------------------------------------------------------------------
# Macro Agent neutral threshold
# ---------------------------------------------------------------------------
NEUTRAL_MACRO_THRESHOLD = 0.15   # abs(score) < 0.15 → emit neutral

# ---------------------------------------------------------------------------
# Learning Module
# ---------------------------------------------------------------------------
LM_MIN_TRADES_FOR_PROPOSAL = 20    # minimum closed trades before pair-removal proposal
LM_WIN_RATE_REMOVAL_THRESHOLD = 0.35  # propose removal if win rate < 35% over min sample
