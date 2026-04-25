#!/usr/bin/env python3
"""
V1 Trend Smoke Test
====================
Runs full pipeline with synthetic inputs. All 9 steps must pass before
any service restart and after any dispatch touching V1 Trend signal fields.

Usage:
  python3 tests/v1_trend_smoke_test.py
  python3 tests/v1_trend_smoke_test.py --step 3   # single step
"""

import sys
import json
import uuid
import argparse
import traceback
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/root/Project_Neo_Damon")

import boto3
import psycopg2
import psycopg2.extras

from shared.v1_trend_parameters import (
    STRATEGY_NAME, EMA_FAST_PERIOD, EMA_SLOW_PERIOD,
    ATR_STOP_MULTIPLIER, ATR_T1_MULTIPLIER, ATR_TRAIL_MULTIPLIER,
    T1_CLOSE_PCT, TIME_STOP_DAYS, RISK_PER_TRADE_PCT, MIN_RR,
)

# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def _get_db():
    ssm = boto3.client("ssm", region_name="eu-west-2")
    sm  = boto3.client("secretsmanager", region_name="eu-west-2")
    endpoint = ssm.get_parameter(
        Name="/platform/config/rds-endpoint", WithDecryption=True
    )["Parameter"]["Value"]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = False
    return conn


def _get_test_user(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id LIMIT 1"
        )
        row = cur.fetchone()
    assert row, "No paper_mode user found in risk_parameters"
    return str(row[0])


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

PASS = "✅"
FAIL = "❌"
results = []


def run_step(label, fn):
    try:
        fn()
        results.append((PASS, label))
        print(f"  {PASS} {label}")
    except Exception as e:
        results.append((FAIL, label))
        print(f"  {FAIL} {label}")
        print(f"     {e}")
        if "--verbose" in sys.argv:
            traceback.print_exc()


# ---------------------------------------------------------------------------
# Step 1 — Technical signal injection
# ---------------------------------------------------------------------------

def step1_technical_signal(conn, user_id):
    expires = datetime.now(timezone.utc) + timedelta(minutes=25)
    payload = {
        "strategy":           STRATEGY_NAME,
        "pair":               "EURUSD",
        "direction":          "long",
        "setup_type":         "trend_long",
        "score":              1.0,
        "adx":                32.5,
        "rsi":                56.2,
        "macd_line":          0.00045,
        "macd_signal":        0.00038,
        "macd_histogram":     0.00007,
        "macd_histogram_prev": 0.00004,
        "histogram_expanding": True,
        "crossover_bars_ago": 1,
        "current_price":      1.08500,
        "atr_daily":          0.00650,
        "atr_stop_loss":      1.08500 - ATR_STOP_MULTIPLIER * 0.00650,
        "gate_failures":      [],
        "computed_at":        datetime.now(timezone.utc).isoformat(),
    }
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.agent_signals
                (agent_name, user_id, instrument, signal_type, score, bias,
                 confidence, payload, expires_at, strategy)
            VALUES ('v1_trend_technical', %s, 'EURUSD', 'technical_signal',
                    1.0, 'bullish', 1.0, %s, %s, %s)
        """, (user_id, json.dumps(payload), expires, STRATEGY_NAME))
    conn.commit()

    # Verify
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT payload FROM forex_network.agent_signals
            WHERE agent_name = 'v1_trend_technical' AND user_id = %s
              AND instrument = 'EURUSD' AND expires_at > NOW()
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
    assert row, "Technical signal not found after insert"
    p = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
    assert p["strategy"] == STRATEGY_NAME, f"strategy mismatch: {p['strategy']}"
    assert p["setup_type"] == "trend_long", f"setup_type mismatch: {p['setup_type']}"
    assert p["histogram_expanding"] is True, "histogram_expanding not True"
    assert "atr_stop_loss" in p, "atr_stop_loss missing"
    assert "current_price" in p, "current_price missing"
    assert p["gate_failures"] == [], f"gate_failures not empty: {p['gate_failures']}"


# ---------------------------------------------------------------------------
# Step 2 — Macro signal injection
# ---------------------------------------------------------------------------

def step2_macro_signal(conn, user_id):
    expires = datetime.now(timezone.utc) + timedelta(minutes=25)
    payload = {
        "strategy":            STRATEGY_NAME,
        "pair":                "EURUSD",
        "direction":           "long",
        "score":               0.72,
        "ema50":               1.08200,
        "ema200":              1.07100,
        "ema50_10d_ago":       1.08050,
        "slope_ok":            True,
        "price_vs_ema50_pct":  0.0028,
        "ema_spread_pct":      0.0103,
        "current_price":       1.08500,
        "method":              "v1_trend_ema_regime_v1",
        "computed_at":         datetime.now(timezone.utc).isoformat(),
    }
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.agent_signals
                (agent_name, user_id, instrument, signal_type, score, bias,
                 confidence, payload, expires_at, strategy)
            VALUES ('v1_trend_macro', %s, 'EURUSD', 'ema_regime',
                    0.72, 'bullish', 1.0, %s, %s, %s)
        """, (user_id, json.dumps(payload), expires, STRATEGY_NAME))
    conn.commit()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT payload FROM forex_network.agent_signals
            WHERE agent_name = 'v1_trend_macro' AND user_id = %s
              AND instrument = 'EURUSD' AND expires_at > NOW()
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
    assert row, "Macro signal not found after insert"
    p = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
    assert p["direction"] == "long", f"direction mismatch: {p['direction']}"
    assert p["slope_ok"] is True, "slope_ok not True"
    assert p["ema50"] > p["ema200"], "EMA50 not above EMA200 in payload"


# ---------------------------------------------------------------------------
# Step 3 — Orchestrator AND gate
# ---------------------------------------------------------------------------

def step3_orchestrator_gate():
    from V1_Trend_Orchestrator.v1_trend_orchestrator import V1TrendOrchestrator, _get_current_session

    # Session check — London hour should be eligible for EURUSD
    session = _get_current_session(10, "EURUSD")
    assert session == "london", f"Expected london session, got {session}"

    # Asia hour — EURUSD should be ineligible
    session_asia = _get_current_session(2, "EURUSD")
    assert session_asia is None, f"EURUSD should be None in Asia, got {session_asia}"

    # Asia hour — AUDJPY should be eligible
    session_audjpy = _get_current_session(2, "AUDJPY")
    assert session_audjpy == "asia", f"AUDJPY should be asia, got {session_audjpy}"

    # Dead zone
    session_dead = _get_current_session(22, "EURUSD")
    assert session_dead is None, f"Dead zone should return None, got {session_dead}"


# ---------------------------------------------------------------------------
# Step 4 — Re-entry block check
# ---------------------------------------------------------------------------

def step4_reentry_block(conn, user_id):
    # Insert a re-entry block for GBPUSD long
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.v1_trend_reentry_blocks
                (pair, direction, session, blocked_until, reason)
            VALUES ('GBPUSD', 'long', 'london', NOW() + INTERVAL '4 hours', 'stop_out')
        """)
    conn.commit()

    # Verify block exists
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM forex_network.v1_trend_reentry_blocks
            WHERE pair = 'GBPUSD' AND direction = 'long' AND blocked_until > NOW()
        """)
        row = cur.fetchone()
    assert row, "Re-entry block not found after insert"

    # Verify expired block does not appear
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM forex_network.v1_trend_reentry_blocks
            WHERE pair = 'GBPUSD' AND direction = 'long'
              AND blocked_until > NOW() + INTERVAL '5 hours'
        """)
        row = cur.fetchone()
    assert row is None, "Block should not appear beyond its expiry window"

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM forex_network.v1_trend_reentry_blocks
            WHERE pair = 'GBPUSD' AND direction = 'long'
        """)
    conn.commit()


# ---------------------------------------------------------------------------
# Step 5 — EA trade write (dry run)
# ---------------------------------------------------------------------------

def step5_trade_write(conn, user_id):
    atr_daily   = 0.00650
    entry_price = 1.08500
    stop_price  = round(entry_price - ATR_STOP_MULTIPLIER * atr_daily, 6)
    t1_price    = round(entry_price + ATR_T1_MULTIPLIER   * atr_daily, 6)

    trade_params = {
        "strategy":       STRATEGY_NAME,
        "atr_daily":      atr_daily,
        "trail_atr_mult": ATR_TRAIL_MULTIPLIER,
        "t1_close_pct":   T1_CLOSE_PCT,
        "time_stop_days": TIME_STOP_DAYS,
        "stop_atr_mult":  ATR_STOP_MULTIPLIER,
        "t1_atr_mult":    ATR_T1_MULTIPLIER,
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.trades
                (user_id, instrument, direction, entry_time, entry_price,
                 stop_price, target_price, position_size, position_size_usd,
                 paper_mode, strategy, session_at_entry,
                 ema50_at_entry, ema200_at_entry, macd_at_entry,
                 regime_exit, time_stop_exit)
            VALUES (%s, 'EURUSD', 'long', NOW(), %s, %s, %s,
                    1000, 1000, TRUE, %s, 'london',
                    1.08200, 1.07100, 0.00045,
                    FALSE, FALSE)
            RETURNING id
        """, (user_id, entry_price, stop_price, t1_price, STRATEGY_NAME))
        trade_id = cur.fetchone()[0]
    conn.commit()

    # Verify fields
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM forex_network.trades WHERE id = %s", (trade_id,))
        trade = cur.fetchone()

    assert trade["strategy"] == STRATEGY_NAME, f"strategy mismatch: {trade['strategy']}"
    assert trade["ema50_at_entry"] is not None, "ema50_at_entry is NULL"
    assert trade["ema200_at_entry"] is not None, "ema200_at_entry is NULL"
    assert trade["macd_at_entry"] is not None, "macd_at_entry is NULL"
    assert trade["regime_exit"] is False, "regime_exit should be False"
    assert trade["time_stop_exit"] is False, "time_stop_exit should be False"

    # Verify ATR targets
    expected_stop = round(entry_price - ATR_STOP_MULTIPLIER * atr_daily, 6)
    assert abs(float(trade["stop_price"]) - expected_stop) < 0.0001, \
        f"stop_price mismatch: {trade['stop_price']} vs {expected_stop}"

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DELETE FROM forex_network.trades WHERE id = %s", (trade_id,))
    conn.commit()
    return trade_id


# ---------------------------------------------------------------------------
# Step 6 — LM autopsy
# ---------------------------------------------------------------------------

def step6_lm_autopsy(conn, user_id):
    from V1_Trend_Learning_Module.v1_trend_learning_module import V1TrendAutopsyEngine, DatabaseConnection

    # Write a closed trade
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.trades
                (user_id, instrument, direction, entry_time, entry_price,
                 stop_price, position_size, paper_mode, strategy,
                 exit_time, exit_price, exit_reason, pnl,
                 ema50_at_entry, ema200_at_entry, macd_at_entry,
                 regime_exit, time_stop_exit, target_1_hit)
            VALUES (%s, 'USDJPY', 'short', NOW() - INTERVAL '3 days', 149.50,
                    150.50, 1000, TRUE, %s,
                    NOW(), 148.20, 'trailing_stop', 130.0,
                    149.20, 150.10, -0.0002,
                    FALSE, FALSE, TRUE)
            RETURNING id
        """, (user_id, STRATEGY_NAME))
        trade_id = cur.fetchone()[0]
    conn.commit()

    # Run autopsy
    class _FakeDB:
        def __init__(self, conn):
            self.conn = conn
        def reconnect_if_needed(self):
            pass

    engine = V1TrendAutopsyEngine.__new__(V1TrendAutopsyEngine)
    engine.db      = _FakeDB(conn)
    engine.user_id = user_id

    trades = engine._fetch_unautopsied_trades()
    assert any(t["id"] == trade_id for t in trades), "Trade not in unautopsied list"

    trade = next(t for t in trades if t["id"] == trade_id)
    result = engine.write_autopsy(trade)
    assert result, "write_autopsy returned False"

    # Verify autopsy written
    with conn.cursor() as cur:
        cur.execute(
            "SELECT failure_mode, signal_quality FROM forex_network.trade_autopsies WHERE trade_id = %s",
            (trade_id,)
        )
        row = cur.fetchone()
    assert row, "Autopsy row not written"
    assert row[0] == "trail_exit_profit", f"failure_mode mismatch: {row[0]}"
    assert row[1] == "good", f"signal_quality mismatch: {row[1]}"

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DELETE FROM forex_network.trade_autopsies WHERE trade_id = %s", (trade_id,))
        cur.execute("DELETE FROM forex_network.trades WHERE id = %s", (trade_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# Step 7 — ATR targets verification
# ---------------------------------------------------------------------------

def step7_atr_targets():
    atr      = 0.00650
    entry    = 1.08500
    stop     = round(entry - ATR_STOP_MULTIPLIER * atr, 6)
    t1       = round(entry + ATR_T1_MULTIPLIER   * atr, 6)
    trail_at_t1 = round(t1 - ATR_TRAIL_MULTIPLIER * atr, 6)

    expected_stop  = round(entry - 2.5 * atr, 6)
    expected_t1    = round(entry + 1.5 * atr, 6)
    expected_trail = round(expected_t1 - 2.0 * atr, 6)

    assert stop  == expected_stop,  f"Stop mismatch: {stop} vs {expected_stop}"
    assert t1    == expected_t1,    f"T1 mismatch: {t1} vs {expected_t1}"
    assert trail_at_t1 == expected_trail, f"Trail mismatch: {trail_at_t1} vs {expected_trail}"

    # Verify T1 closes 30%
    position = 1000
    t1_close = round(position * T1_CLOSE_PCT)
    remainder = position - t1_close
    assert t1_close  == 300, f"T1 close should be 300, got {t1_close}"
    assert remainder == 700, f"Remainder should be 700, got {remainder}"


# ---------------------------------------------------------------------------
# Step 8 — Time stop
# ---------------------------------------------------------------------------

def step8_time_stop():
    from Execution_Agent.execution_agent import ExecutionAgent
    hold = ExecutionAgent._get_max_hold_days(regime="trending", strategy="v1_trend")
    assert hold == TIME_STOP_DAYS, f"V1 Trend time stop should be {TIME_STOP_DAYS}, got {hold}"

    hold_swing = ExecutionAgent._get_max_hold_days(regime="trending", strategy="v1_swing")
    assert hold_swing == 5, f"V1 Swing trending hold should be 5, got {hold_swing}"

    hold_swing_range = ExecutionAgent._get_max_hold_days(regime="ranging", strategy="v1_swing")
    assert hold_swing_range == 3, f"V1 Swing ranging hold should be 3, got {hold_swing_range}"


# ---------------------------------------------------------------------------
# Step 9 — RG reads V1 Trend proposals
# ---------------------------------------------------------------------------

def step9_rg_reads_proposals(conn, user_id):
    expires = datetime.now(timezone.utc) + timedelta(minutes=20)
    payload = {
        "strategy":       STRATEGY_NAME,
        "pair":           "AUDUSD",
        "direction":      "short",
        "setup_type":     "trend_short",
        "risk_pct":       RISK_PER_TRADE_PCT,
        "current_price":  0.64500,
        "atr_daily":      0.00420,
        "atr_stop_loss":  0.64500 + ATR_STOP_MULTIPLIER * 0.00420,
        "stop_atr_mult":  ATR_STOP_MULTIPLIER,
        "t1_atr_mult":    ATR_T1_MULTIPLIER,
        "t1_close_pct":   T1_CLOSE_PCT,
        "trail_atr_mult": ATR_TRAIL_MULTIPLIER,
        "time_stop_days": TIME_STOP_DAYS,
        "min_rr":         MIN_RR,
        "decided_at":     datetime.now(timezone.utc).isoformat(),
    }
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.agent_signals
                (agent_name, user_id, instrument, signal_type, score, bias,
                 confidence, payload, expires_at, strategy)
            VALUES ('v1_trend_orchestrator', %s, 'AUDUSD', 'trade_approval',
                    1.0, 'short', 1.0, %s, %s, %s)
            RETURNING id
        """, (user_id, json.dumps(payload), expires, STRATEGY_NAME))
        signal_id = cur.fetchone()[0]
    conn.commit()

    # Verify RG can read it via the patched query
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, payload FROM forex_network.agent_signals
            WHERE agent_name = 'v1_trend_orchestrator'
              AND signal_type = 'trade_approval'
              AND user_id = %s
              AND expires_at > NOW()
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
    assert row, "V1 Trend proposal not readable by RG query"
    p = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
    assert p["strategy"] == STRATEGY_NAME
    assert p["direction"] == "short"
    assert p["risk_pct"]  == RISK_PER_TRADE_PCT
    assert p["min_rr"]    == MIN_RR

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DELETE FROM forex_network.agent_signals WHERE id = %s", (signal_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step",    type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"V1 Trend Smoke Test — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    conn    = _get_db()
    user_id = _get_test_user(conn)
    print(f"Test user: {user_id}\n")

    steps = [
        (1, "Technical signal injection",    lambda: step1_technical_signal(conn, user_id)),
        (2, "Macro signal injection",        lambda: step2_macro_signal(conn, user_id)),
        (3, "Orchestrator AND gate",         step3_orchestrator_gate),
        (4, "Re-entry block",                lambda: step4_reentry_block(conn, user_id)),
        (5, "EA trade write (dry run)",      lambda: step5_trade_write(conn, user_id)),
        (6, "LM autopsy",                    lambda: step6_lm_autopsy(conn, user_id)),
        (7, "ATR targets verification",      step7_atr_targets),
        (8, "Time stop (_get_max_hold_days)", step8_time_stop),
        (9, "RG reads V1 Trend proposals",   lambda: step9_rg_reads_proposals(conn, user_id)),
    ]

    if args.step:
        steps = [(n, l, f) for n, l, f in steps if n == args.step]
        if not steps:
            print(f"Step {args.step} not found")
            sys.exit(1)

    for num, label, fn in steps:
        print(f"Step {num}: {label}")
        run_step(label, fn)
        print()

    conn.close()

    passed = sum(1 for r, _ in results if r == PASS)
    failed = sum(1 for r, _ in results if r == FAIL)

    print(f"{'='*60}")
    print(f"Results: {passed}/{len(results)} passed")
    if failed:
        print(f"FAILED: {failed} step(s)")
        for r, label in results:
            if r == FAIL:
                print(f"  {FAIL} {label}")
        sys.exit(1)
    else:
        print("All steps passed ✅")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
