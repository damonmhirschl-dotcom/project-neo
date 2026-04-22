#!/usr/bin/env python3
"""
ig_trade_test.py — End-to-end IGBroker open/close test.
Tests the broker layer directly (not via execution agent).
Run only during active session (london or london_new_york).
"""
import sys
import time
import json
sys.path.insert(0, '/root/Project_Neo_Damon')

from shared.brokers.ig_broker import IGBroker

print("=" * 60)
print("IGBroker E2E Test — EURUSD mini, £10 risk")
print("=" * 60)

broker = IGBroker(demo=True)
broker.authenticate()
print("[1] Authenticated OK")

# ── Quote ──────────────────────────────────────────────────────────────────
quote = broker.get_live_quote("EURUSD")
bid   = quote["bid"]
ask   = quote["ask"]
mid   = quote["mid"]
spread_pips = round((ask - bid) * 10000, 1)
print(f"[2] Quote: bid={bid} ask={ask} mid={mid} spread={spread_pips}p")

# ── Build minimal order ────────────────────────────────────────────────────
current_price = ask
stop_price    = round(current_price - 0.0020, 5)   # 20-pip stop
risk_amount   = 10.0                                 # £10

payload = {
    "instrument":      "EURUSD",
    "direction":       "long",
    "size":            None,        # IGBroker computes from risk_amount / stop_distance
    "current_price":   current_price,
    "stop_price":      stop_price,
    "risk_amount_gbp": risk_amount,
    "stop_distance":   round(current_price - stop_price, 5),
}
print(f"[3] Order payload: {json.dumps(payload, indent=2)}")

# ── Place ──────────────────────────────────────────────────────────────────
result = broker.place_order(payload)
print(f"[4] place_order response: {json.dumps(result, indent=2, default=str)}")

if result.get("status") not in ("OPEN", "AMENDED", "open") and "error" in result:
    print(f"[!] Order failed — aborting. Error: {result.get('error')}")
    sys.exit(1)

order_id   = result.get("order_id") or result.get("dealId", "")
fill_price = result.get("fill_price") or result.get("level", current_price)
size       = result.get("size", 0.1)
print(f"[4] Trade OPEN: order_id={order_id} fill_price={fill_price} size={size}")

# ── Confirm position appears ───────────────────────────────────────────────
time.sleep(5)
positions = broker.get_positions()
matched = [p for p in positions if order_id and (
    p.get("order_id") == order_id or
    p.get("instrument", "").upper().replace("/","").startswith("EURUSD")
)]
if matched:
    print(f"[5] Position confirmed: {json.dumps(matched[0], indent=2, default=str)}")
else:
    print(f"[5] WARNING: position not found in get_positions(). All positions: {positions}")

# ── Close ──────────────────────────────────────────────────────────────────
close_resp = broker.close_position(order_id, size, "short")
print(f"[6] close_position response: {json.dumps(close_resp, indent=2, default=str)}")
close_price = close_resp.get("fill_price") or close_resp.get("level", 0)

# ── Confirm position gone ──────────────────────────────────────────────────
time.sleep(3)
positions_after = broker.get_positions()
still_open = [p for p in positions_after if
    p.get("instrument", "").upper().replace("/","").startswith("EURUSD")]
if not still_open:
    print("[7] Position closed — no EURUSD in get_positions(). PASS.")
else:
    print(f"[7] WARNING: position may still be open: {still_open}")

# ── P&L summary ───────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY")
print(f"  Open  fill : {fill_price}")
print(f"  Close fill : {close_price}")
if fill_price and close_price:
    pnl_pips = round((close_price - fill_price) * 10000, 1)
    print(f"  P&L (pips) : {pnl_pips}  (spread cost expected ≈ -{spread_pips}p)")
print(f"  order_id   : {order_id}")
print("=" * 60)
print("NOTE: DB row is NOT written by this script (broker layer test only).")
print("The execution agent writes DB rows via _write_trade().")
