# MIRROR OF v1_swing_parameters.py — keep in sync with /root/Project_Neo_Damon/v1_swing_parameters.py
# Lambdas are deployed independently so cannot import from the repo root directly.
# When v1_swing_parameters.py changes, update this file too.

V1_SWING_PAIRS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    'EURGBP', 'EURAUD', 'EURCHF', 'EURCAD', 'EURJPY', 'EURNZD',
    'GBPAUD', 'GBPCAD', 'GBPCHF', 'GBPJPY',
    'AUDJPY', 'AUDCAD', 'AUDNZD',
    'NZDJPY', 'CADJPY',
]  # 22 pairs total — canonical V1 Swing universe

STRATEGY_V1_SWING = 'v1_swing'
STRATEGY_LEGACY   = 'legacy'
