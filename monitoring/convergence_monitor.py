#!/usr/bin/env python3
"""
Convergence Deep-Dive Monitor
Single run: full 7-stage pipeline trace for every pair and every user.
"""
import boto3, json, psycopg2, psycopg2.extras
from datetime import datetime, timezone, timedelta
from decimal import Decimal

PAIRS = ['EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD']
WEIGHTS = {'macro': 0.35, 'technical': 0.45}
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
print(f"  PROJECT NEO — CONVERGENCE DEEP-DIVE")
print(f"  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"{'#'*80}")

# --- Stage 1: Agent signal freshness ---
print("\n╔══ STAGE 1: SIGNAL FRESHNESS ══════════════════════════════════════════════════╗")
for agent in ['macro', 'technical', 'regime']:
    cur.execute("""
        SELECT instrument,
               score::float AS s, confidence::float AS c, bias,
               ROUND(EXTRACT(EPOCH FROM (NOW() - created_at)) / 60) AS age_min,
               expires_at > NOW() AS live
        FROM forex_network.agent_signals
        WHERE agent_name = %s
        ORDER BY instrument NULLS LAST, created_at DESC
    """, (agent,))
    rows = cur.fetchall()
    seen = set()
    fresh = [r for r in rows if r['instrument'] not in seen and not seen.add(r['instrument'])]
    print(f"\n  ── {agent.upper():12s} ({len(fresh)} signals) ──")
    for r in fresh:
        live_flag = '✓' if r['live'] else '✗ EXPIRED'
        instr = r['instrument'] or 'GLOBAL'
        print(f"    {instr:8s}  {r['s']:+.3f}  conf={r['c']:.2f}  {r['bias']:8s}  "
              f"{r['age_min']:.0f}min ago  [{live_flag}]")

# --- Stage 2: Convergence formula per pair ---
print("\n╔══ STAGE 2: CONVERGENCE FORMULA TRACE ════════════════════════════════════════╗")
print(f"\n  Formula: conv = (macro×0.35 + tech×0.45) × (0.80 + regime×0.40)")
print(f"  Confidence gate: < 0.30 → excluded from directional sum\n")

# Stress
cur.execute("SELECT stress_score FROM forex_network.rejection_log ORDER BY created_at DESC LIMIT 1")
sr = cur.fetchone()
stress     = float(sr['stress_score']) if sr and sr['stress_score'] is not None else 0.0
stress_adj = 0.05 if 30 <= stress < 50 else 0.0
stress_band = 'Elevated (+0.05 to thresholds)' if 30 <= stress < 50 else \
              ('High' if stress >= 50 else 'Normal')
print(f"  Stress: {stress:.1f} → {stress_band}")

# Regime global fallback
cur.execute("""
    SELECT score::float AS s FROM forex_network.agent_signals
    WHERE agent_name='regime' AND instrument IS NULL AND expires_at > NOW()
    ORDER BY created_at DESC LIMIT 1
""")
gr = cur.fetchone()
global_regime = gr['s'] if gr else 0.0

print(f"  Global regime fallback score: {global_regime:+.3f}")
print()
print(f"  {'Pair':8s}  {'M_sc':>6s} {'M_cf':>5s}  {'T_sc':>6s} {'T_cf':>5s}  "
      f"{'R_sc':>6s} {'RM':>5s}  {'DirSum':>7s}  {'Conv':>7s}  {'Dir?':5s}")
print(f"  {'-'*8}  {'-'*6} {'-'*5}  {'-'*6} {'-'*5}  {'-'*6} {'-'*5}  {'-'*7}  {'-'*7}  {'-'*5}")

pair_convs = {}
for pair in PAIRS:
    cur.execute("""
        SELECT agent_name, score::float AS s, confidence::float AS c, bias
        FROM forex_network.agent_signals
        WHERE instrument = %s AND expires_at > NOW()
        ORDER BY agent_name, created_at DESC
    """, (pair,))
    sigs = {}
    for row in cur.fetchall():
        if row['agent_name'] not in sigs:
            sigs[row['agent_name']] = row

    ms = float(sigs['macro']['s'])     if 'macro'     in sigs else 0.0
    mc = float(sigs['macro']['c'])     if 'macro'     in sigs else 0.0
    ts = float(sigs['technical']['s']) if 'technical' in sigs else 0.0
    tc = float(sigs['technical']['c']) if 'technical' in sigs else 0.0
    rs = float(sigs['regime']['s'])    if 'regime'    in sigs and sigs['regime']['s'] != 0 else global_regime

    macro_ok = mc >= 0.30
    tech_ok  = tc >= 0.30
    rm       = 0.80 + (rs * 0.40)

    dir_sum = (ms * 0.35 if macro_ok else 0) + (ts * 0.45 if tech_ok else 0)
    conv    = dir_sum * rm

    # Directional consensus check
    dir_ok = True
    dir_note = 'agree'
    if macro_ok and tech_ok and ms != 0 and ts != 0:
        if (ms > 0) != (ts > 0):
            dir_ok   = False
            dir_note = 'CONF!'

    pair_convs[pair] = {'conv': conv, 'dir_ok': dir_ok, 'macro_ok': macro_ok, 'tech_ok': tech_ok}

    m_flag = '' if macro_ok else '*'
    t_flag = '' if tech_ok  else '*'
    print(f"  {pair:8s}  {ms:>+6.3f} {mc:>5.2f}{m_flag}  {ts:>+6.3f} {tc:>5.2f}{t_flag}  "
          f"{rs:>+6.3f} {rm:>5.3f}  {dir_sum:>+7.3f}  {conv:>+7.3f}  {dir_note}")

print(f"\n  (* = below 0.30 confidence gate — excluded from directional sum)")

# --- Stage 3: Threshold comparison per user ---
print("\n╔══ STAGE 3: THRESHOLD COMPARISON (per user) ══════════════════════════════════╗\n")
cur.execute("SELECT user_id, convergence_threshold FROM forex_network.risk_parameters ORDER BY convergence_threshold DESC")
risk_rows = cur.fetchall()

header = f"  {'Pair':8s}"
for rp in risk_rows:
    label = USERS.get(rp['user_id'], rp['user_id'][:8])[:12]
    thresh = float(rp['convergence_threshold']) + stress_adj
    header += f"  {label}({thresh:.2f})"
print(header)
print(f"  {'-'*8}" + "  " + "  ".join(["-"*16] * len(risk_rows)))

for pair in PAIRS:
    cv = pair_convs[pair]
    line = f"  {pair:8s}"
    for rp in risk_rows:
        thresh = float(rp['convergence_threshold']) + stress_adj
        passes = abs(cv['conv']) >= thresh and cv['dir_ok'] and abs(cv['conv']) > 0
        result = f"PASS  {cv['conv']:+.3f}" if passes else f"skip  {cv['conv']:+.3f}"
        line += f"  {result:>16s}"
    print(line)

# --- Stage 4: Convergence history trends (last 2 hours) ---
print("\n╔══ STAGE 4: CONVERGENCE HISTORY — 2-HOUR TREND ═══════════════════════════════╗")
cur.execute("""
    SELECT instrument,
           ROUND(AVG(ABS(convergence_score::float))::numeric, 4) AS avg_abs_conv,
           ROUND(MAX(ABS(convergence_score::float))::numeric, 4) AS peak_conv,
           COUNT(*) AS cycles
    FROM forex_network.convergence_history
    WHERE cycle_timestamp > NOW() - INTERVAL '2 hours'
    GROUP BY instrument
    ORDER BY avg_abs_conv DESC
""")
print(f"\n  {'Pair':8s}  {'Avg|Conv|':>10s}  {'Peak|Conv|':>10s}  {'Cycles':>6s}")
for r in cur.fetchall():
    print(f"  {r['instrument']:8s}  {float(r['avg_abs_conv']):>10.4f}  "
          f"{float(r['peak_conv']):>10.4f}  {r['cycles']:>6d}")

# --- Stage 5: Rejection breakdown (last 4 hours) ---
print("\n╔══ STAGE 5: REJECTION BREAKDOWN — 4 HOURS ════════════════════════════════════╗")
cur.execute("""
    SELECT instrument, conflict_category, reason,
           COUNT(*) AS cnt,
           ROUND(AVG(macro_score::float)::numeric, 3) AS avg_m,
           ROUND(AVG(tech_score::float)::numeric, 3) AS avg_t
    FROM forex_network.rejection_log
    WHERE created_at > NOW() - INTERVAL '4 hours'
    GROUP BY instrument, conflict_category, reason
    ORDER BY cnt DESC
    LIMIT 20
""")
print(f"\n  {'Pair':8s}  {'Count':>5s}  {'Category':20s}  {'AvgM':>6s}  {'AvgT':>6s}  Reason")
for r in cur.fetchall():
    print(f"  {r['instrument']:8s}  {r['cnt']:>5d}  {(r['conflict_category'] or ''):20s}  "
          f"{float(r['avg_m'] or 0):>+6.3f}  {float(r['avg_t'] or 0):>+6.3f}  {r['reason'][:50]}")

# --- Stage 6: Signal persistence (stuck signals) ---
print("\n╔══ STAGE 6: SIGNAL PERSISTENCE (stuck biases) ════════════════════════════════╗")
cur.execute("""
    SELECT agent_name, instrument, current_bias, consecutive_cycles,
           ROUND(EXTRACT(EPOCH FROM (NOW() - first_seen_at)) / 3600, 1) AS hours_stuck
    FROM forex_network.signal_persistence
    WHERE consecutive_cycles >= 5
    ORDER BY consecutive_cycles DESC
    LIMIT 20
""")
stuck = cur.fetchall()
if stuck:
    print(f"\n  {'Agent':12s}  {'Pair':8s}  {'Bias':8s}  {'Cycles':>6s}  {'Hours':>6s}")
    for r in stuck:
        flag = ' ⚠' if r['consecutive_cycles'] >= 15 else ''
        print(f"  {r['agent_name']:12s}  {(r['instrument'] or 'GLOBAL'):8s}  "
              f"{r['current_bias']:8s}  {r['consecutive_cycles']:>6d}  "
              f"{float(r['hours_stuck']):>6.1f}{flag}")
else:
    print("\n  No stuck signals (all < 5 consecutive cycles)")

# --- Stage 7: Known issues snapshot ---
print("\n╔══ STAGE 7: KNOWN ISSUES SNAPSHOT ════════════════════════════════════════════╗")
cur.execute("""
    SELECT alert_type, severity, title, created_at
    FROM forex_network.system_alerts
    WHERE acknowledged = false
    ORDER BY created_at DESC LIMIT 10
""")
alerts = cur.fetchall()
if alerts:
    for a in alerts:
        print(f"  [{a['severity'].upper():6s}]  {a['alert_type']:25s}  {a['title'][:50]}")
else:
    print("\n  No unacknowledged system alerts")

# API health summary
cur.execute("""
    SELECT provider, avg_response_ms, error_rate_pct, uptime_pct, total_calls
    FROM forex_network.provider_reports
    WHERE report_date = CURRENT_DATE
    ORDER BY avg_response_ms DESC
""")
print(f"\n  API Performance Today:")
print(f"  {'Provider':15s}  {'Avg ms':>8s}  {'Error%':>7s}  {'Uptime%':>8s}  {'Calls':>5s}")
for r in cur.fetchall():
    flag = ' ⚠' if float(r['error_rate_pct'] or 0) > 5 else ''
    print(f"  {r['provider']:15s}  {r['avg_response_ms']:>8d}  "
          f"{float(r['error_rate_pct'] or 0):>7.1f}  {float(r['uptime_pct'] or 0):>8.1f}  "
          f"{r['total_calls']:>5d}{flag}")

cur.close(); conn.close()
print(f"\n{'#'*80}")
print(f"  Convergence deep-dive complete  —  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
print(f"{'#'*80}\n")
