#!/usr/bin/env python3
"""
update_portfolio_correlation.py — Compute and upsert 30d pairwise Pearson
correlations for all 22 V1 Swing pairs into shared.portfolio_correlation.

Reads 1D closes from forex_network.historical_prices (last 35 trading days
to ensure 30 clean data points after gaps).

Safe to re-run: ON CONFLICT DO UPDATE.
Cron: 30 22 * * 1-5 (22:30 UTC Mon-Fri, after price ingest, before agents)
"""
import sys, logging
import numpy as np
import pandas as pd

sys.path.insert(0, '/root/Project_Neo_Damon/Orchestrator')
from orchestrator_agent import DatabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('portfolio_correlation')

V1_PAIRS = [
    'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','NZDUSD',
    'EURGBP','EURAUD','EURCHF','EURCAD','EURJPY','EURNZD',
    'GBPAUD','GBPCAD','GBPCHF','GBPJPY',
    'AUDJPY','AUDCAD','AUDNZD','NZDJPY','CADJPY',
]

LOOKBACK_DAYS = 35   # fetch 35 days to get ~30 clean trading days
SYSTEM_USER_ID = None  # NULL — no per-user partitioning needed


def fetch_closes(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT instrument, ts::date AS dt, close
        FROM forex_network.historical_prices
        WHERE timeframe = '1D'
          AND instrument = ANY(%s)
          AND ts >= NOW() - INTERVAL '%s days'
        ORDER BY instrument, ts
    """, (V1_PAIRS, LOOKBACK_DAYS))
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("No 1D closes returned from historical_prices")
    df = pd.DataFrame(rows, columns=['instrument', 'dt', 'close'])
    df['close'] = df['close'].astype(float)
    pivot = df.pivot(index='dt', columns='instrument', values='close')
    return pivot


def compute_correlations(pivot: pd.DataFrame) -> dict:
    """Pearson correlation of log returns, 30d window."""
    log_ret = np.log(pivot / pivot.shift(1)).dropna()
    if len(log_ret) < 10:
        raise RuntimeError(f"Only {len(log_ret)} log-return rows — insufficient for correlation")
    corr_matrix = log_ret.corr(method='pearson')
    pairs_present = corr_matrix.columns.tolist()
    logger.info(f"Computed correlations from {len(log_ret)} log-return rows, {len(pairs_present)} pairs")
    return corr_matrix, pairs_present


def upsert_correlations(cur, corr_matrix: pd.DataFrame, pairs: list) -> int:
    upserted = 0
    for i, pair_a in enumerate(pairs):
        for pair_b in pairs[i+1:]:
            val = corr_matrix.loc[pair_a, pair_b]
            if np.isnan(val):
                continue
            cur.execute("""
                INSERT INTO shared.portfolio_correlation
                    (user_id, instrument_a, instrument_b, correlation_30d, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (instrument_a, instrument_b)
                DO UPDATE SET
                    correlation_30d = EXCLUDED.correlation_30d,
                    updated_at      = EXCLUDED.updated_at
            """, (SYSTEM_USER_ID, pair_a, pair_b, round(float(val), 4)))
            upserted += 1
    return upserted


def main():
    db = DatabaseConnection()
    db.connect()
    conn = db.conn
    cur = conn.cursor()

    # Check if unique constraint exists on (instrument_a, instrument_b)
    cur.execute("""
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'shared.portfolio_correlation'::regclass
          AND contype IN ('u','p')
    """)
    constraints = [r[0] for r in cur.fetchall()]
    logger.info(f"portfolio_correlation constraints: {constraints}")

    try:
        pivot = fetch_closes(cur)
        missing = [p for p in V1_PAIRS if p not in pivot.columns]
        if missing:
            logger.warning(f"Pairs missing from historical_prices 1D: {missing}")

        corr_matrix, pairs_present = compute_correlations(pivot)
        n = upsert_correlations(cur, corr_matrix, pairs_present)
        conn.commit()
        logger.info(f"Upserted {n} correlation pairs for {len(pairs_present)} instruments")

        # Verify
        cur.execute("SELECT COUNT(*), MAX(updated_at) FROM shared.portfolio_correlation")
        total, latest = cur.fetchone()
        logger.info(f"portfolio_correlation: {total} rows, latest updated_at={latest}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
