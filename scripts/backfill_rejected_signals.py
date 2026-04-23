#!/usr/bin/env python3
"""
Backfill price_1h_later, price_4h_later, price_24h_later and compute outcome
for rows in forex_network.rejected_signals.

Price source: forex_network.historical_prices (1H candles, column `close`).
NOTE: price_metrics has no price column; historical_prices is the correct table.

Direction mapping (rejected_signals.direction is VARCHAR(5), truncated):
  'long'  or 'bulli' → expected_direction = +1 (bullish, market should go up)
  'short' or 'beari' → expected_direction = -1 (bearish, market should go down)
  all others          → skip outcome computation

Outcome logic:
  actual_move = (price_Xh_later - price_at_rejection) * expected_direction
  > +atr_threshold → 'incorrect'  (market moved right way, rejection was wrong)
  < -atr_threshold → 'correct'    (market moved wrong way, rejection was right)
  else             → 'neutral'

ATR threshold: 0.0005 for non-JPY; 0.05 for JPY pairs (5-pip equivalent).

Runs nightly at 22:30 UTC (Mon-Fri) via cron. Safe to run repeatedly.
"""

import sys
import json
import logging
import datetime
import boto3
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("backfill_rejected_signals")

AWS_REGION   = "eu-west-2"
ATR_THRESHOLD_DEFAULT = 0.0005  # 5 pips for non-JPY
ATR_THRESHOLD_JPY     = 0.050   # 5 pips for JPY pairs


def get_db_conn():
    sm  = boto3.client("secretsmanager", region_name=AWS_REGION)
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    creds = json.loads(
        sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"]
    )
    host = ssm.get_parameter(Name="/platform/config/rds-endpoint")["Parameter"]["Value"]
    conn = psycopg2.connect(
        host=host, dbname="postgres",
        user=creds["username"], password=creds["password"],
    )
    return conn


def direction_to_sign(direction: str):
    """Map truncated direction string to +1/-1, or None to skip."""
    d = (direction or "").strip().lower()
    if d in ("long", "bulli"):
        return +1
    if d in ("short", "beari"):
        return -1
    return None


def atr_threshold(instrument: str) -> float:
    return ATR_THRESHOLD_JPY if "JPY" in instrument.upper() else ATR_THRESHOLD_DEFAULT


def fetch_price(cur, instrument: str, after_ts) -> float | None:
    """Return the closing price of the first 1H candle at or after after_ts."""
    cur.execute(
        """
        SELECT close FROM forex_network.historical_prices
        WHERE instrument = %s AND timeframe = '1H' AND ts >= %s
        ORDER BY ts ASC
        LIMIT 1
        """,
        (instrument, after_ts),
    )
    row = cur.fetchone()
    return float(row["close"]) if row else None


def compute_outcome(price_ref: float, price_later: float,
                    exp_dir: int, threshold: float) -> str:
    move = (price_later - price_ref) * exp_dir
    if move > threshold:
        return "incorrect"   # market went the right way — rejection was wrong
    if move < -threshold:
        return "correct"     # market went the wrong way — rejection was right
    return "neutral"


def main():
    conn = get_db_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch rows eligible for backfill
    cur.execute("""
        SELECT id, instrument, direction, rejected_at, price_at_rejection
        FROM forex_network.rejected_signals
        WHERE price_1h_later IS NULL
          AND rejected_at < NOW() - INTERVAL '1 hour'
        ORDER BY rejected_at ASC
    """)
    rows = cur.fetchall()

    if not rows:
        logger.info("No rows eligible for backfill.")
        conn.close()
        return

    logger.info(f"Found {len(rows)} eligible row(s) to backfill.")

    n_updated = n_correct = n_incorrect = n_neutral = n_no_price = n_skip_dir = 0

    for row in rows:
        rid        = row["id"]
        instrument = row["instrument"]
        direction  = row["direction"]
        rejected_at = row["rejected_at"]
        price_ref  = float(row["price_at_rejection"]) if row["price_at_rejection"] else None

        if price_ref is None:
            n_no_price += 1
            continue  # can't compute movement without reference price

        exp_dir = direction_to_sign(direction)

        p1h  = fetch_price(cur, instrument, rejected_at + datetime.timedelta(hours=1))
        p4h  = fetch_price(cur, instrument, rejected_at + datetime.timedelta(hours=4))
        p24h = fetch_price(cur, instrument, rejected_at + datetime.timedelta(hours=24))

        outcome = None
        if exp_dir is not None and p1h is not None:
            threshold = atr_threshold(instrument)
            outcome = compute_outcome(price_ref, p1h, exp_dir, threshold)
        elif exp_dir is None:
            n_skip_dir += 1

        try:
            cur.execute("""
                UPDATE forex_network.rejected_signals
                SET price_1h_later  = %s,
                    price_4h_later  = %s,
                    price_24h_later = %s,
                    outcome         = %s
                WHERE id = %s
            """, (p1h, p4h, p24h, outcome, rid))
            conn.commit()
            n_updated += 1
            if outcome == "correct":   n_correct   += 1
            elif outcome == "incorrect": n_incorrect += 1
            elif outcome == "neutral":   n_neutral   += 1
        except Exception as e:
            logger.error(f"UPDATE failed for id={rid}: {e}")
            conn.rollback()

    conn.close()
    logger.info(
        f"Done. Updated={n_updated} | correct={n_correct} | "
        f"incorrect={n_incorrect} | neutral={n_neutral} | "
        f"no_price={n_no_price} | skipped_dir={n_skip_dir}"
    )


if __name__ == "__main__":
    main()
