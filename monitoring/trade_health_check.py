#!/usr/bin/env python3
"""
Trade Health Check
Single run: open positions, today's closed trades, circuit breakers,
IBKR order events, correlation constraints, system alerts.
"""
import boto3, json, psycopg2, psycopg2.extras, subprocess
from datetime import datetime, timezone, timedelta
from decimal import Decimal

USERS = {
    'e61202e4-30d1-70f8-9927-30b8a439e042': 'Conservative (001)',
    '76829264-20e1-7023-1e31-37b7a37a1274': 'Balanced     (002)',
    'd6c272e4-a031-7053-af8e-ade000f0d0d5': 'Aggressive   (003)',
}

sm  = boto3.client('secretsmanager', region_name='eu-west-2')
ssm = boto3.client('ssm', region_name='eu-west-2')
creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
ep    = ssm.get_parameter(Name='/platform/config/rds-endpoint', WithDecryption=True)['Parameter']['Value']

conn = psycopg2.connect(host=ep, dbname='postgres', user=creds['username'],
                        password=creds['password'],
                        options='-c search_path=forex_network,shared,public')
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
now  = datetime.now(timezone.utc)

print(f"\n{'#'*80}")
print(f"  PROJECT NEO — TRADE HEALTH CHECK")
print(f"  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"{'#'*80}")

# --- Open positions ---
print("\n── OPEN POSITIONS ───────────────────────────────────────────────────────────────")
cur.execute("""
    SELECT t.id, t.user_id, t.instrument, t.direction,
           t.entry_price, t.stop_price, t.target_price,
           t.position_size_usd, t.convergence_score,
           t.entry_time, t.paper_mode, t.ibkr_order_id,
           t.agents_agreed, t.session_at_entry,
           EXTRACT(EPOCH FROM (NOW() - t.entry_time)) / 60 AS age_min
    FROM forex_network.trades t
    WHERE t.exit_time IS NULL
    ORDER BY t.entry_time DESC
""")
open_trades = cur.fetchall()
if open_trades:
    for tr in open_trades:
        uid   = tr['user_id']
        label = USERS.get(str(uid), str(uid)[:8])
        rr = (float(tr['target_price']) - float(tr['entry_price'])) / \
             abs(float(tr['entry_price']) - float(tr['stop_price'])) \
             if tr['stop_price'] and float(tr['entry_price']) != float(tr['stop_price']) else 0
        if tr['direction'] == 'short':
            rr = -rr
        print(f"\n  #{tr['id']}  {tr['instrument']}  {tr['direction'].upper()}  "
              f"{'PAPER' if tr['paper_mode'] else 'LIVE'}")
        print(f"    User:    {label}")
        print(f"    Entry:   {tr['entry_price']}  Stop: {tr['stop_price']}  Target: {tr['target_price']}  R:R={rr:.2f}")
        print(f"    Size:    ${float(tr['position_size_usd']):,.0f}  Conv={tr['convergence_score']}  Agents={tr['agents_agreed']}")
        print(f"    Age:     {float(tr['age_min']):.0f} min  Session={tr['session_at_entry']}  IBKR={tr['ibkr_order_id'] or 'N/A'}")
else:
    print("  No open positions")

# --- Today's closed trades ---
print("\n── TODAY'S CLOSED TRADES ────────────────────────────────────────────────────────")
cur.execute("""
    SELECT t.instrument, t.direction, t.entry_price, t.exit_price,
           t.pnl, t.pnl_pips, t.exit_reason, t.convergence_score,
           t.return_pct, t.rr_after_swap, t.paper_mode,
           t.entry_time, t.exit_time,
           EXTRACT(EPOCH FROM (t.exit_time - t.entry_time)) / 60 AS hold_min
    FROM forex_network.trades t
    WHERE t.exit_time IS NOT NULL
      AND DATE(t.entry_time AT TIME ZONE 'UTC') = CURRENT_DATE
    ORDER BY t.exit_time DESC
""")
closed = cur.fetchall()
if closed:
    total_pnl = sum(float(t['pnl'] or 0) for t in closed)
    wins = sum(1 for t in closed if float(t['pnl'] or 0) > 0)
    print(f"\n  {len(closed)} trades  PnL=${total_pnl:+,.2f}  Wins={wins}/{len(closed)}")
    for tr in closed:
        pnl_flag = '✓' if float(tr['pnl'] or 0) > 0 else '✗'
        print(f"  {pnl_flag} {tr['instrument']}  {tr['direction'].upper():5s}  "
              f"in={tr['entry_price']} out={tr['exit_price']}  "
              f"PnL=${float(tr['pnl'] or 0):+,.2f}  {tr['pnl_pips']:.1f}pips  "
              f"exit={tr['exit_reason']}  {float(tr['hold_min'] or 0):.0f}min  "
              f"{'PAPER' if tr['paper_mode'] else 'LIVE'}")
else:
    print("  No closed trades today")

# --- Circuit breakers & risk state ---
print("\n── CIRCUIT BREAKERS & RISK STATE ───────────────────────────────────────────────")
cur.execute("""
    SELECT user_id, convergence_threshold, circuit_breaker_active,
           circuit_breaker_reason, circuit_breaker_at,
           size_multiplier, account_value, account_value_currency,
           max_open_positions, daily_loss_limit_pct,
           drawdown_step_level, peak_account_value
    FROM forex_network.risk_parameters
    ORDER BY convergence_threshold DESC
""")
for rp in cur.fetchall():
    uid   = str(rp['user_id'])
    label = USERS.get(uid, uid[:8])
    cb    = rp['circuit_breaker_active']
    drawdown = 0.0
    if rp['peak_account_value'] and float(rp['peak_account_value']) > 0:
        drawdown = (1 - float(rp['account_value']) / float(rp['peak_account_value'])) * 100
    print(f"\n  {label}")
    print(f"    Account: ${float(rp['account_value']):,.2f} {rp['account_value_currency']}  "
          f"Peak: ${float(rp['peak_account_value'] or 0):,.2f}  Drawdown: {drawdown:.2f}%")
    print(f"    Positions: {rp['max_open_positions']} max  "
          f"DailyLossLim: {rp['daily_loss_limit_pct']}%  "
          f"SizeMult: {rp['size_multiplier']}  StepLevel: {rp['drawdown_step_level']}")
    if cb:
        print(f"    ⚡ CIRCUIT BREAKER ACTIVE: {rp['circuit_breaker_reason']} "
              f"(at {rp['circuit_breaker_at']})")
    else:
        print(f"    ✓ No circuit breaker")

# --- IBKR order events (last 24h) ---
print("\n── IBKR ORDER EVENTS (last 24h) ─────────────────────────────────────────────────")
cur.execute("""
    SELECT event_type, instrument, direction, filled_price,
           filled_size, rejection_reason, rejection_code,
           retry_attempt, session_at_event, created_at
    FROM forex_network.order_events
    WHERE created_at > NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
    LIMIT 20
""")
events = cur.fetchall()
if events:
    for ev in events:
        rej = f"  REJECT: {ev['rejection_reason']} [{ev['rejection_code']}]" \
              if ev['rejection_reason'] else ''
        retry = f"  retry#{ev['retry_attempt']}" if ev['retry_attempt'] else ''
        print(f"  {str(ev['created_at'])[:19]}  {ev['event_type']:20s}  "
              f"{(ev['instrument'] or ''):8s}  {(ev['direction'] or ''):5s}  "
              f"fill={ev['filled_price']} sz={ev['filled_size']}"
              f"{rej}{retry}")
else:
    print("  No IBKR order events in last 24h")

# --- IBKR gateway health (curl test) ---
print("\n── IBKR GATEWAY HEALTH ──────────────────────────────────────────────────────────")
try:
    result = subprocess.run(
        ['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}',
         '--max-time', '5', 'https://localhost:5000/v1/api/iserver/auth/status', '-k'],
        capture_output=True, text=True, timeout=8
    )
    code = result.stdout.strip()
    print(f"  ibeam /auth/status → HTTP {code}")
except Exception as e:
    print(f"  ibeam check failed: {e}")

# --- Correlation constraints in force ---
print("\n── CORRELATION CONSTRAINTS ──────────────────────────────────────────────────────")
cur.execute("SELECT * FROM forex_network.correlation_rules LIMIT 10")
rules = cur.fetchall()
if rules:
    for r in rules:
        print(f"  {dict(r)}")
else:
    print("  No correlation rules configured")

# --- System alerts ---
print("\n── SYSTEM ALERTS (unacknowledged) ───────────────────────────────────────────────")
cur.execute("""
    SELECT alert_type, severity, title, detail, created_at
    FROM forex_network.system_alerts
    WHERE acknowledged = false
    ORDER BY created_at DESC LIMIT 10
""")
alerts = cur.fetchall()
if alerts:
    for a in alerts:
        print(f"  [{a['severity'].upper():6s}]  {str(a['created_at'])[:19]}  "
              f"{a['alert_type']:25s}  {a['title'][:50]}")
else:
    print("  None")

# --- Trade autopsies (recent failures) ---
print("\n── RECENT TRADE AUTOPSIES ───────────────────────────────────────────────────────")
cur.execute("""
    SELECT ta.failure_mode, ta.diagnosis, ta.signal_quality,
           ta.sharpe_contribution, ta.sortino_contribution,
           ta.created_at
    FROM forex_network.trade_autopsies ta
    ORDER BY ta.created_at DESC LIMIT 5
""")
autopsies = cur.fetchall()
if autopsies:
    for a in autopsies:
        print(f"  {str(a['created_at'])[:19]}  [{a['failure_mode']}]  "
              f"quality={a['signal_quality']}  "
              f"sortino={float(a['sortino_contribution'] or 0):+.4f}")
        print(f"    {a['diagnosis'][:80]}")
else:
    print("  No trade autopsies on record")

cur.close(); conn.close()
print(f"\n{'#'*80}")
print(f"  Trade health check complete  —  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
print(f"{'#'*80}\n")
