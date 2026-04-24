"""
V1 Swing parameter set — single profile, all users.
Source: V1 Swing specification (2026-04-24).
"""

# Macro direction neutral threshold — magnitude below which macro is 'neutral'
NEUTRAL_MACRO_THRESHOLD = 0.15

# Position sizing — flat 1% of equity per trade
RISK_PER_TRADE_PCT = 0.01

# Maximum concurrent positions across all instruments
MAX_CONCURRENT_POSITIONS = 4

# Correlation block — reject new trade if existing position correlation exceeds this
CORRELATION_BLOCK_THRESHOLD = 0.70

# Minimum bars required for 4H RSI-21 computation (200 1H bars → ~50 4H bars)
MIN_4H_BARS_FOR_SIGNAL = 21
