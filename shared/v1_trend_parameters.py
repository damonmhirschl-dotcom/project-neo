# =============================================================================
# V1 Trend — Parameter File
# Single source of truth for all V1 Trend constants.
# All agents import from here. One edit propagates everywhere.
# Never hardcode these values in agent files.
# =============================================================================

import sys; sys.path.insert(0, "/root/Project_Neo_Damon"); from v1_swing_parameters import V1_SWING_PAIRS  # shared universe, single source

# ---------------------------------------------------------------------------
# Strategy identity
# ---------------------------------------------------------------------------
STRATEGY_NAME = "v1_trend"

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
V1_TREND_PAIRS = [  # 20-pair universe — GBPUSD/USDCAD removed (news-driven, backtest negative)
    p for p in V1_SWING_PAIRS if p not in ("GBPUSD", "USDCAD")
]

# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------
SIGNAL_TIMEFRAME  = "4H"   # resampled from 1H — same pattern as V1 Swing
CONTEXT_TIMEFRAME = "1D"   # EMA regime computed on daily bars

# ---------------------------------------------------------------------------
# EMA regime (Macro Agent)
# ---------------------------------------------------------------------------
EMA_FAST_PERIOD          = 50
EMA_SLOW_PERIOD          = 200
EMA_SLOPE_LOOKBACK       = 10
EMA_NEUTRAL_THRESHOLD    = 0.001
EMA_SPREAD_MIN_THRESHOLD = 0.0005

# ---------------------------------------------------------------------------
# ADX (Technical Agent)
# ---------------------------------------------------------------------------
ADX_PERIOD = 14
ADX_GATE   = 28

# ---------------------------------------------------------------------------
# MACD (Technical Agent)
# ---------------------------------------------------------------------------
MACD_FAST          = 12
MACD_SLOW          = 26
MACD_SIGNAL        = 9
MACD_LOOKBACK_BARS = 2

# ---------------------------------------------------------------------------
# RSI (Technical Agent)
# ---------------------------------------------------------------------------
RSI_PERIOD    = 14
RSI_LONG_GATE  = 50
RSI_SHORT_GATE = 50

# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------
ATR_PERIOD           = 14
ATR_STOP_MULTIPLIER  = 2.5
ATR_T1_MULTIPLIER    = 1.5
ATR_TRAIL_MULTIPLIER = 2.5

# ---------------------------------------------------------------------------
# Position management
# ---------------------------------------------------------------------------
T1_CLOSE_PCT   = 0.30
TIME_STOP_DAYS = 35

# ---------------------------------------------------------------------------
# Risk and sizing
# ---------------------------------------------------------------------------
RISK_PER_TRADE_PCT             = 0.01
MAX_CONCURRENT_POSITIONS       = 4
MIN_RR                         = 2.5
CONCENTRATION_CAP_PER_CURRENCY = 2
CORRELATION_BLOCK_THRESHOLD    = 0.70

# ---------------------------------------------------------------------------
# Drawdown halts
# ---------------------------------------------------------------------------
DAILY_DRAWDOWN_HALT_PCT  = 0.03
WEEKLY_DRAWDOWN_HALT_PCT = 0.07

# ---------------------------------------------------------------------------
# News blackout
# ---------------------------------------------------------------------------
NEWS_BLACKOUT_HOURS = 4

# ---------------------------------------------------------------------------
# Combined heat cap
# ---------------------------------------------------------------------------
COMBINED_MAX_HEAT_PCT    = 0.05
COMBINED_HEAT_RESUME_PCT = 0.04

# ---------------------------------------------------------------------------
# Re-entry blocks
# ---------------------------------------------------------------------------
REENTRY_BLOCK_DURATION = "day"

# ---------------------------------------------------------------------------
# Session rules
# ---------------------------------------------------------------------------
ASIA_SESSION_PAIRS = [
    "AUDUSD","NZDUSD","AUDJPY","NZDJPY","USDJPY",
    "AUDCAD","AUDNZD","CADJPY"
]

SESSION_WINDOWS_UTC = {
    "asia":       (0,  8),
    "london":     (8,  13),
    "ny_overlap": (13, 17),
    "ny_late":    (17, 21),
    "dead_zone":  (21, 24),
}

# ---------------------------------------------------------------------------
# Enums
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
# Macro Agent
# ---------------------------------------------------------------------------
NEUTRAL_MACRO_THRESHOLD = 0.15

# ---------------------------------------------------------------------------
# Learning Module
# ---------------------------------------------------------------------------
LM_MIN_TRADES_FOR_PROPOSAL    = 20
LM_WIN_RATE_REMOVAL_THRESHOLD = 0.35
