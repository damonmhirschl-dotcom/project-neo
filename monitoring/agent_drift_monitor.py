#!/usr/bin/env python3
"""
Agent Drift Monitor
Single run: score distributions, stuck signals, missing signals,
convergence trends, regime health.
"""
import boto3, json, psycopg2, psycopg2.extras
from datetime import datetime, timezone, timedelta
from decimal import Decimal


def is_market_active():
    """London/NY session: 07:00-17:00 UTC Mon-Fri."""
    now = datetime.now(timezone.utc)
    return now.weekday() < 5 and 7 <= now.hour < 17


def get_stale_threshold(agent_name):
    """Per-agent staleness threshold in minutes, aware of quiet-hours cycle lengths.

    Macro/regime run 60-min cycles during quiet hours (Asian session) so they will
    always appear STALE under a universal 20-min threshold. Use 35 min during the
    London/NY session and 125 min outside it (60-min cycle + 5-min buffer × 2).
    """
    active = is_market_active()
    return {
        'technical':       15,
        'orchestrator':    15,
        'macro':           35 if active else 125,
        'regime':          35 if active else 125,
        'risk_guardian':    5,
        'execution':        5,
        'learning_module':  5,
    }.get(agent_name, 20)


PAIRS = ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD']
AGENTS = ['macro', 'technical', 'regime']
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
print(f"  PROJECT NEO — AGENT DRIFT MONITOR")
print(f"  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"{'#'*80}")

# --- Score distributions (last 4 hours) ---
print("\n── SCORE DISTRIBUTIONS — LAST 4 HOURS ──────────────────────────────────────────")
print(f"\n  {'Agent':12s}  {'Pair':8s}  {'Avg sc':>7s}  {'Std':>6s}  {'Min':>7s}  "
      f"{'Max':>7s}  {'N':>4s}  {'Biases'}")
for agent in AGENTS:
    cur.execute("""
        SELECT instrument,
               ROUND(AVG(score::float)::numeric, 3) AS avg_s,
               ROUND(STDDEV(score::float)::numeric, 3) AS std_s,
               ROUND(MIN(score::float)::numeric, 3) AS min_s,
               ROUND(MAX(score::float)::numeric, 3) AS max_s,
               COUNT(*) AS n,
               COUNT(CASE WHEN bias='bullish' THEN 1 END) AS bulls,
               COUNT(CASE WHEN bias='bearish' THEN 1 END) AS bears,
               COUNT(CASE WHEN bias='neutral' THEN 1 END) AS neutrals
        FROM forex_network.agent_signals
        WHERE agent_name = %s
          AND created_at > NOW() - INTERVAL '4 hours'
        GROUP BY instrument
        ORDER BY instrument NULLS LAST
    """, (agent,))
    rows = cur.fetchall()
    for r in rows:
        instr = r['instrument'] or 'GLOBAL'
        biases = f"↑{r['bulls']} ↓{r['bears']} ={r['neutrals']}"
        # Drift flag: std dev > 0.20 = high volatility in scoring
        drift = ' ⚠ HIGH_VAR' if r['std_s'] and float(r['std_s']) > 0.20 else ''
        print(f"  {agent:12s}  {instr:8s}  {float(r['avg_s'] or 0):>+7.3f}  "
              f"{float(r['std_s'] or 0):>6.3f}  {float(r['min_s'] or 0):>+7.3f}  "
              f"{float(r['max_s'] or 0):>+7.3f}  {r['n']:>4d}  {biases}{drift}")

# --- Missing signals check ---
print("\n── MISSING SIGNALS CHECK ────────────────────────────────────────────────────────")
print(f"\n  Checking for pairs missing valid (non-expired) signals from each agent...\n")
for agent in AGENTS:
    cur.execute("""
        SELECT instrument FROM forex_network.agent_signals
        WHERE agent_name = %s AND expires_at > NOW()
          AND instrument IS NOT NULL
    """, (agent,))
    present = {r['instrument'] for r in cur.fetchall()}
    missing = [p for p in PAIRS if p not in present]
    if missing:
        print(f"  ⚠ {agent:12s}  MISSING live signal for: {', '.join(missing)}")
    else:
        print(f"  ✓ {agent:12s}  All 7 pairs have valid signals")

# Check regime global fallback
cur.execute("""
    SELECT score::float AS s, confidence::float AS c, created_at
    FROM forex_network.agent_signals
    WHERE agent_name='regime' AND instrument IS NULL AND expires_at > NOW()
    ORDER BY created_at DESC LIMIT 1
""")
gr = cur.fetchone()
if gr:
    print(f"\n  ✓ Regime GLOBAL fallback: score={gr['s']:+.3f} conf={gr['c']:.2f}")
else:
    print(f"\n  ⚠ Regime GLOBAL fallback: no live signal")

# --- Stuck signals (signal_persistence) ---
print("\n── STUCK SIGNALS (signal_persistence) ──────────────────────────────────────────")
cur.execute("""
    SELECT agent_name, instrument, current_bias, consecutive_cycles,
           ROUND(EXTRACT(EPOCH FROM (NOW() - first_seen_at)) / 3600, 1) AS hours_stuck,
           last_updated
    FROM forex_network.signal_persistence
    ORDER BY agent_name, instrument
""")
stuck_all = cur.fetchall()
if stuck_all:
    print(f"\n  {'Agent':12s}  {'Pair':8s}  {'Bias':8s}  {'Cycles':>6s}  {'Hours':>6s}  Flag")
    for r in stuck_all:
        cycles = r['consecutive_cycles']
        hours  = float(r['hours_stuck'] or 0)
        flag   = ''
        if cycles >= 20:  flag = ' ⚠⚠ VERY_STUCK'
        elif cycles >= 10: flag = ' ⚠ STUCK'
        elif cycles >= 5:  flag = ' (persistent)'
        print(f"  {r['agent_name']:12s}  {(r['instrument'] or 'GLOBAL'):8s}  "
              f"{r['current_bias']:8s}  {cycles:>6d}  {hours:>6.1f}{flag}")
else:
    print("  No signal persistence records")

# --- Convergence trends (hourly buckets, last 8 hours) ---
print("\n── CONVERGENCE TRENDS — HOURLY BUCKETS ─────────────────────────────────────────")
cur.execute("""
    SELECT DATE_TRUNC('hour', cycle_timestamp) AS hour_bucket,
           instrument,
           ROUND(AVG(ABS(convergence_score::float))::numeric, 4) AS avg_abs_conv,
           ROUND(MAX(ABS(convergence_score::float))::numeric, 4) AS peak_abs,
           COUNT(*) AS cycles
    FROM forex_network.convergence_history
    WHERE cycle_timestamp > NOW() - INTERVAL '8 hours'
    GROUP BY hour_bucket, instrument
    ORDER BY hour_bucket DESC, avg_abs_conv DESC
""")
rows = cur.fetchall()
if rows:
    last_bucket = None
    for r in rows:
        bucket = str(r['hour_bucket'])[:16]
        if bucket != last_bucket:
            print(f"\n  {bucket} UTC")
            last_bucket = bucket
        print(f"    {r['instrument']:8s}  avg|conv|={float(r['avg_abs_conv']):.4f}  "
              f"peak={float(r['peak_abs']):.4f}  ({r['cycles']} cycles)")
else:
    print("  No convergence history in last 8 hours")

# --- Regime health ---
print("\n── REGIME HEALTH ────────────────────────────────────────────────────────────────")
cur.execute("""
    SELECT instrument, score::float AS s, confidence::float AS c, bias,
           ROUND(EXTRACT(EPOCH FROM (NOW() - created_at)) / 60) AS age_min,
           expires_at > NOW() AS live
    FROM forex_network.agent_signals
    WHERE agent_name = 'regime'
    ORDER BY instrument NULLS LAST, created_at DESC
""")
seen = set()
print(f"\n  {'Pair/Global':12s}  {'Score':>7s}  {'Conf':>6s}  {'Bias':8s}  {'Age':>5s}  Live")
for r in cur.fetchall():
    key = r['instrument'] or 'GLOBAL'
    if key in seen: continue
    seen.add(key)
    rm = 0.80 + (r['s'] * 0.40)
    live_flag = '✓' if r['live'] else '✗'
    print(f"  {key:12s}  {r['s']:>+7.3f}  {r['c']:>6.2f}  {r['bias']:8s}  "
          f"{float(r['age_min'] or 0):>5.0f}m  {live_flag}  (regime_mult={rm:.3f})")

# --- Orchestrator cycle timing ---
print("\n── ORCHESTRATOR CYCLE HEALTH ────────────────────────────────────────────────────")
cur.execute("""
    SELECT agent_name, user_id, cycle_count, last_seen, status,
           error_count, degradation_mode, absent_cycles
    FROM forex_network.agent_heartbeats
    WHERE agent_name = 'orchestrator'
    ORDER BY last_seen DESC
""")
for r in cur.fetchall():
    age = (now - r['last_seen']).total_seconds() / 60
    uid = str(r['user_id'])
    label = USERS.get(uid, uid[:8])
    flag = ' ⚠ STALE' if age > get_stale_threshold('orchestrator') else ''
    degrade = f"  degrade={r['degradation_mode']}" if r['degradation_mode'] else ''
    absent  = f"  absent_cycles={r['absent_cycles']}" if r['absent_cycles'] else ''
    print(f"  {label}  cycles={r['cycle_count']}  {age:.0f}min ago  "
          f"status={r['status']}  errs={r['error_count']}{degrade}{absent}{flag}")

# --- Agent heartbeat summary ---
print("\n── ALL AGENT HEARTBEATS ─────────────────────────────────────────────────────────")
cur.execute("""
    SELECT agent_name,
           MAX(last_seen) AS latest,
           ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(last_seen))) / 60) AS mins_ago,
           SUM(cycle_count) AS total_cycles,
           SUM(error_count) AS total_errors,
           COUNT(DISTINCT user_id) AS users,
           MAX(absent_cycles) AS max_absent
    FROM forex_network.agent_heartbeats
    GROUP BY agent_name
    ORDER BY mins_ago
""")
print(f"\n  {'Agent':20s}  {'MinsAgo':>7s}  {'Cycles':>7s}  {'Errs':>5s}  {'Users':>5s}  Status")
for r in cur.fetchall():
    mins = float(r['mins_ago'] or 0)
    thresh = get_stale_threshold(r['agent_name'])
    flag = '✓' if mins < thresh else ('⚠ STALE' if mins < thresh * 3 else '✗ DEAD')
    print(f"  {r['agent_name']:20s}  {mins:>7.0f}  {(r['total_cycles'] or 0):>7d}  "
          f"{(r['total_errors'] or 0):>5d}  {r['users']:>5d}  {flag}")

cur.close(); conn.close()
print(f"\n{'#'*80}")
print(f"  Drift monitor complete  —  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
print(f"{'#'*80}\n")
