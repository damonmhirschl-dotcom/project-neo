"""
V1 Swing parameter set — single source of truth for all V1 Swing constants.
Source: V1 Swing specification (2026-04-24).
"""

# === Universe ===
V1_SWING_PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    'EURGBP', 'EURAUD', 'EURCHF', 'EURCAD', 'EURJPY', 'EURNZD',
    'GBPAUD', 'GBPCAD', 'GBPCHF', 'GBPJPY',
    'AUDJPY', 'AUDCAD', 'AUDNZD',
    'NZDJPY', 'CADJPY',
]  # 22 pairs total — canonical V1 Swing universe

# === Setup types ===
SETUP_LONG_PULLBACK  = 'long_pullback'
SETUP_SHORT_PULLBACK = 'short_pullback'
SETUP_NONE           = None
SETUP_TYPES          = (SETUP_LONG_PULLBACK, SETUP_SHORT_PULLBACK)

# === Sessions ===
SESSION_ASIA       = 'asia'
SESSION_LONDON     = 'london'
SESSION_NY_OVERLAP = 'ny_overlap'
SESSION_NY_LATE    = 'ny_late'
SESSION_DEAD_ZONE  = 'dead_zone'
SESSION_NAMES      = (SESSION_ASIA, SESSION_LONDON, SESSION_NY_OVERLAP, SESSION_NY_LATE, SESSION_DEAD_ZONE)

# === Macro directions ===
DIRECTION_LONG    = 'long'
DIRECTION_SHORT   = 'short'
DIRECTION_NEUTRAL = 'neutral'
MACRO_DIRECTIONS  = (DIRECTION_LONG, DIRECTION_SHORT, DIRECTION_NEUTRAL)

# === Strategy names ===
STRATEGY_V1_SWING = 'v1_swing'
STRATEGY_LEGACY   = 'legacy'
STRATEGY_NAMES    = (STRATEGY_V1_SWING, STRATEGY_LEGACY)

# === Rejection stages ===
# Orchestrator-level
RJ_TECHNICAL_GATE_FAIL   = 'technical_gate_fail'
RJ_MACRO_NO_DIRECTION    = 'macro_no_direction'
RJ_DIRECTION_DISAGREEMENT = 'direction_disagreement'
# Technical agent gate failures
RJ_TREND_FILTER_FAIL  = 'trend_filter_fail'
RJ_ADX_GATE_FAIL      = 'adx_gate_fail'
RJ_RSI_NOT_IN_ZONE    = 'rsi_not_in_zone'
RJ_RSI_NO_CROSS       = 'rsi_no_cross'
RJ_STRUCTURE_FAIL     = 'structure_fail'
RJ_SESSION_INVALID    = 'session_invalid'
# RG-level
RJ_CORRELATION_BLOCK    = 'correlation_block'
RJ_CONCENTRATION_BLOCK  = 'concentration_block'
RJ_NEWS_BLACKOUT        = 'news_blackout'
RJ_DAILY_DRAWDOWN_HALT  = 'daily_drawdown_halt'
RJ_WEEKLY_DRAWDOWN_HALT = 'weekly_drawdown_halt'
RJ_RR_GATE_FAIL         = 'rr_gate_fail'
RJ_SWAP_RR_FAIL         = 'swap_rr_fail'
RJ_MIN_STOP_DISTANCE    = 'min_stop_distance'
RJ_PIP_VALUE_UNAVAILABLE = 'pip_value_unavailable'

REJECTION_STAGES = (
    RJ_TECHNICAL_GATE_FAIL, RJ_MACRO_NO_DIRECTION, RJ_DIRECTION_DISAGREEMENT,
    RJ_TREND_FILTER_FAIL, RJ_ADX_GATE_FAIL, RJ_RSI_NOT_IN_ZONE, RJ_RSI_NO_CROSS,
    RJ_STRUCTURE_FAIL, RJ_SESSION_INVALID,
    RJ_CORRELATION_BLOCK, RJ_CONCENTRATION_BLOCK, RJ_NEWS_BLACKOUT,
    RJ_DAILY_DRAWDOWN_HALT, RJ_WEEKLY_DRAWDOWN_HALT,
    RJ_RR_GATE_FAIL, RJ_SWAP_RR_FAIL,
    RJ_MIN_STOP_DISTANCE, RJ_PIP_VALUE_UNAVAILABLE,
)

# === Exit reasons (for trade autopsies) ===
EXIT_TARGET_1_ONLY = 'target_1_only'
EXIT_TARGET_2      = 'target_2'
EXIT_STOP          = 'stop'
EXIT_BREAKEVEN     = 'breakeven'
EXIT_MANUAL        = 'manual'
EXIT_REASONS       = (EXIT_TARGET_1_ONLY, EXIT_TARGET_2, EXIT_STOP, EXIT_BREAKEVEN, EXIT_MANUAL)

# === Thresholds ===
NEUTRAL_MACRO_THRESHOLD  = 0.15
RISK_PER_TRADE_PCT       = 0.01
MAX_CONCURRENT_POSITIONS = 4
CONCENTRATION_CAP_PER_CURRENCY = 2
CORRELATION_BLOCK_THRESHOLD    = 0.70
DAILY_DRAWDOWN_HALT_PCT        = 0.03
WEEKLY_DRAWDOWN_HALT_PCT       = 0.07
NEWS_BLACKOUT_HOURS            = 4

# Minimum bars required for 4H RSI-21 computation (200 1H bars → ~50 4H bars)
MIN_4H_BARS_FOR_SIGNAL = 21

# === ATR multipliers ===
ATR_STOP_MULTIPLIER      = 2  # stop at 2× daily ATR
ATR_TARGET_1_MULTIPLIER  = 2  # T1 at 2× daily ATR
ATR_TARGET_2_MULTIPLIER  = 3  # T2 at 3× daily ATR (pro FX consensus; was 4× prior to 2026-04-25)
