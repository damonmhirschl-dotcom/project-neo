#!/usr/bin/env python3
"""
Project Neo — Diagnostic Agent
Runs 17 system-health checks and reports PASS / WARN / FAIL.
Usage: python3 diagnostic_agent.py [--skip-ibkr]
"""
import sys, json, subprocess, urllib.request
from datetime import datetime, timezone

import boto3, psycopg2, psycopg2.extras

REGION = 'eu-west-2'
IG_BASE = 'https://demo-api.ig.com/gateway/deal'

results = []

def check(name, status, detail=''):
    icon = {'PASS': '✓', 'WARN': '~', 'FAIL': '✗'}[status]
    results.append((status, name, detail))
    print(f"  [{icon}] {status:<4}  {name:<45} {detail}")

def section(title):
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")

# ── setup ──────────────────────────────────────────────────────────────────────
def get_conn():
    sm  = boto3.client('secretsmanager', region_name=REGION)
    ssm = boto3.client('ssm', region_name=REGION)
    ep  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value'].split(':')[0]
    cr  = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    return psycopg2.connect(
        host=ep, dbname='postgres', user=cr['username'], password=cr['password'],
        connect_timeout=10, options='-c search_path=forex_network,shared,public'
    )

def ig_auth():
    sm    = boto3.client('secretsmanager', region_name=REGION)
    creds = json.loads(sm.get_secret_value(SecretId='platform/ig-markets/demo-credentials')['SecretString'])
    body  = json.dumps({'identifier': creds['username'], 'password': creds['password']}).encode()
    req   = urllib.request.Request(
        f'{IG_BASE}/session', data=body,
        headers={'Content-Type': 'application/json', 'X-IG-API-KEY': creds['api_key'], 'Version': '2'},
        method='POST'
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.headers.get('CST'), r.headers.get('X-SECURITY-TOKEN'), creds['api_key']

now = datetime.now(timezone.utc)
print(f"\n{'='*70}")
print(f"  PROJECT NEO — DIAGNOSTIC AGENT   {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"{'='*70}")

# ── CHECK 1: DB connectivity ──────────────────────────────────────────────────
section("INFRASTRUCTURE")
conn = None
try:
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT 1")
    check("DB connectivity", "PASS", "RDS reachable")
except Exception as e:
    check("DB connectivity", "FAIL", str(e)[:80])
    print("\nCannot proceed without DB — aborting.\n")
    sys.exit(1)

# ── CHECK 2: Schema tables present ──────────────────────────────────────────
EXPECTED_TABLES = [
    'forex_network.trades', 'forex_network.proposals', 'forex_network.agent_signals',
    'forex_network.agent_heartbeats', 'forex_network.convergence_history',
    'forex_network.risk_parameters', 'forex_network.macro_signals_history',
    'forex_network.economic_releases', 'shared.market_context_snapshots',
]
try:
    missing = []
    for fqt in EXPECTED_TABLES:
        schema, table = fqt.split('.')
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table)
        )
        if not cur.fetchone():
            missing.append(fqt)
    if missing:
        check("Schema tables", "FAIL", f"Missing: {missing}")
    else:
        check("Schema tables", "PASS", f"{len(EXPECTED_TABLES)} tables present")
except Exception as e:
    check("Schema tables", "FAIL", str(e)[:80])

# ── CHECK 3: Agent heartbeats ─────────────────────────────────────────────────
section("AGENT HEARTBEATS")
RUNNING_AGENTS  = ['macro', 'technical', 'regime']
STOPPED_AGENTS  = ['orchestrator', 'execution', 'risk_guardian', 'learning']
THRESHOLDS      = {'macro': 130, 'technical': 900, 'regime': 700}  # seconds; tech has 300s stagger

try:
    cur.execute("""
        SELECT agent_name,
               MAX(last_seen) AS last_seen,
               EXTRACT(EPOCH FROM (NOW() - MAX(last_seen))) AS age_s
        FROM forex_network.agent_heartbeats
        GROUP BY agent_name
    """)
    hb = {r['agent_name']: r for r in cur.fetchall()}

    for agent in RUNNING_AGENTS:
        row = hb.get(agent)
        if not row:
            check(f"Heartbeat: {agent}", "WARN", "No heartbeat row yet (normal if just started)")
        else:
            age = float(row['age_s'])
            thr = THRESHOLDS.get(agent, 300)
            ts  = str(row['last_seen'])[:19]
            if age < thr:
                check(f"Heartbeat: {agent}", "PASS", f"age={age:.0f}s  last={ts}")
            else:
                check(f"Heartbeat: {agent}", "FAIL", f"STALE age={age:.0f}s > {thr}s  last={ts}")

    for agent in STOPPED_AGENTS:
        row = hb.get(agent)
        if row:
            age = float(row['age_s'])
            if age < 300:
                check(f"Heartbeat: {agent} (expect stopped)", "WARN",
                      f"Heartbeat is fresh (age={age:.0f}s) — still running?")
            else:
                check(f"Heartbeat: {agent} (expect stopped)", "PASS",
                      f"Stale/absent heartbeat age={age:.0f}s — correctly stopped")
        else:
            check(f"Heartbeat: {agent} (expect stopped)", "PASS", "No heartbeat — correctly stopped")
except Exception as e:
    check("Agent heartbeats", "FAIL", str(e)[:80])

# ── CHECK 4: Systemd service states ──────────────────────────────────────────
section("SYSTEMD SERVICE STATES")
SERVICES = {
    'neo-macro-agent':       'active',
    'neo-technical-agent':   'active',
    'neo-regime-agent':      'active',
    'neo-orchestrator-agent':'inactive',
    'neo-execution-agent':   'inactive',
    'neo-risk-guardian-agent':'inactive',
    'neo-learning-module':   'inactive',
}
for svc, expected in SERVICES.items():
    try:
        r = subprocess.run(['systemctl', 'is-active', svc],
                           capture_output=True, text=True, timeout=5)
        actual = r.stdout.strip()
        if actual == expected:
            check(f"Service: {svc}", "PASS", actual)
        else:
            status = "FAIL" if expected == 'inactive' and actual == 'active' else "WARN"
            check(f"Service: {svc}", status, f"expected={expected} actual={actual}")
    except Exception as e:
        check(f"Service: {svc}", "WARN", str(e)[:60])

# ── CHECK 5: Signal freshness ─────────────────────────────────────────────────
section("SIGNAL FRESHNESS")
try:
    cur.execute("""
        SELECT agent_name,
               MAX(created_at) AS latest,
               EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) AS age_s,
               COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '2 hours') AS recent_count
        FROM forex_network.agent_signals
        GROUP BY agent_name
    """)
    sigs = {r['agent_name']: r for r in cur.fetchall()}

    for agent in RUNNING_AGENTS:
        row = sigs.get(agent)
        if not row:
            check(f"Signals: {agent}", "WARN", "No signals yet (normal if just started)")
        else:
            age   = float(row['age_s'])
            cnt   = row['recent_count']
            ts    = str(row['latest'])[:19]
            limit = 7200  # 2h — quiet hours cycle is 60-min
            if age < limit:
                check(f"Signals: {agent}", "PASS", f"age={age:.0f}s  recent={cnt}  last={ts}")
            else:
                check(f"Signals: {agent}", "FAIL", f"STALE age={age:.0f}s  last={ts}")

    for agent in STOPPED_AGENTS:
        row = sigs.get(agent)
        if row and float(row['age_s']) < 300:
            check(f"Signals: {agent} (expect stopped)", "WARN",
                  f"Fresh signal age={float(row['age_s']):.0f}s — agent still active?")
        # else: fine, no check needed
except Exception as e:
    check("Signal freshness", "FAIL", str(e)[:80])

# ── CHECK 6: Convergence data ─────────────────────────────────────────────────
section("CONVERGENCE & MARKET CONTEXT")
try:
    cur.execute("""
        SELECT MAX(cycle_timestamp) AS latest,
               EXTRACT(EPOCH FROM (NOW() - MAX(cycle_timestamp))) AS age_s,
               COUNT(*) FILTER (WHERE cycle_timestamp > NOW() - INTERVAL '2 hours') AS recent
        FROM forex_network.convergence_history
    """)
    row = cur.fetchone()
    if not row['latest']:
        check("Convergence history", "WARN", "Empty — orchestrator not yet run (expected if stopped)")
    else:
        age = float(row['age_s'])
        if age < 7200:
            check("Convergence history", "PASS", f"age={age:.0f}s  recent={row['recent']}")
        else:
            check("Convergence history", "WARN", f"Stale age={age:.0f}s — orchestrator stopped")
except Exception as e:
    check("Convergence history", "FAIL", str(e)[:80])

try:
    cur.execute("""
        SELECT MAX(snapshot_time) AS latest,
               EXTRACT(EPOCH FROM (NOW() - MAX(snapshot_time))) AS age_s
        FROM shared.market_context_snapshots
    """)
    row = cur.fetchone()
    if not row['latest']:
        check("Market context snapshot", "WARN", "No snapshot yet")
    else:
        age = float(row['age_s'])
        status = "PASS" if age < 7200 else "WARN"
        check("Market context snapshot", status, f"age={age:.0f}s  latest={str(row['latest'])[:19]}")
except Exception as e:
    check("Market context snapshot", "WARN", str(e)[:80])

# ── CHECK 7: Clean wipe verification ─────────────────────────────────────────
section("CLEAN STATE (POST-WIPE)")
ZERO_TABLES = [
    ('forex_network.trades',           0, 'FAIL'),
    ('forex_network.proposals',        0, 'FAIL'),
    ('forex_network.rejection_patterns', 0, 'WARN'),
    ('forex_network.agent_accuracy_tracking', 0, 'WARN'),
]
for table, expected_max, fail_level in ZERO_TABLES:
    try:
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        n = cur.fetchone()['n']
        if n <= expected_max:
            check(f"Table zeroed: {table.split('.')[-1]}", "PASS", f"{n} rows")
        else:
            check(f"Table zeroed: {table.split('.')[-1]}", fail_level, f"{n} rows (expected 0)")
    except Exception as e:
        check(f"Table zeroed: {table.split('.')[-1]}", "WARN", str(e)[:60])

# ── CHECK 8: Risk parameters ──────────────────────────────────────────────────
section("RISK PARAMETERS & ACCOUNT STATE")
try:
    cur.execute("""
        SELECT user_id, account_value, peak_account_value, circuit_breaker_active,
               drawdown_step_level, updated_at
        FROM forex_network.risk_parameters
        ORDER BY user_id
    """)
    rows = cur.fetchall()
    profiles = {
        'e61202e4-30d1-70f8-9927-30b8a439e042': 'Conservative',
        '76829264-20e1-7023-1e31-37b7a37a1274': 'Balanced',
        'd6c272e4-a031-7053-af8e-ade000f0d0d5': 'Aggressive',
    }
    if not rows:
        check("Risk parameters", "FAIL", "No rows found")
    else:
        for r in rows:
            uid   = str(r['user_id'])
            label = profiles.get(uid, uid[:8])
            av    = float(r['account_value'] or 0)
            cb    = r['circuit_breaker_active']
            dd    = r['drawdown_step_level']
            if cb:
                check(f"Risk params: {label}", "FAIL", f"CIRCUIT BREAKER ACTIVE  av={av:.2f}")
            elif av <= 0:
                check(f"Risk params: {label}", "FAIL", f"account_value={av}")
            elif av < 100000:
                check(f"Risk params: {label}", "WARN", f"av={av:.2f} (low?)  dd_step={dd}")
            else:
                check(f"Risk params: {label}", "PASS", f"av={av:.2f}  dd_step={dd}  cb=False")
except Exception as e:
    check("Risk parameters", "FAIL", str(e)[:80])

# ── CHECK 9: DATA_QUALITY_CUTOFF ─────────────────────────────────────────────
try:
    r = subprocess.run(
        ['grep', 'DATA_QUALITY_CUTOFF', '/home/ubuntu/learning_module.py'],
        capture_output=True, text=True
    )
    line = r.stdout.strip()
    if not line:
        check("DATA_QUALITY_CUTOFF", "WARN", "Not found in learning_module.py")
    else:
        # Extract timestamp
        import re
        m = re.search(r"'(\d{4}-\d{2}-\d{2})", line)
        if m:
            cutoff_str = m.group(1)
            cutoff_dt  = datetime.strptime(cutoff_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            age_days   = (now - cutoff_dt).days
            if age_days <= 1:
                check("DATA_QUALITY_CUTOFF", "PASS", f"{cutoff_str} (set {age_days}d ago)")
            elif age_days <= 7:
                check("DATA_QUALITY_CUTOFF", "WARN", f"{cutoff_str} (set {age_days}d ago — consider updating)")
            else:
                check("DATA_QUALITY_CUTOFF", "FAIL", f"{cutoff_str} (set {age_days}d ago — STALE)")
        else:
            check("DATA_QUALITY_CUTOFF", "WARN", f"Could not parse: {line[:80]}")
except Exception as e:
    check("DATA_QUALITY_CUTOFF", "WARN", str(e)[:60])

# ── CHECK 10: Macro history coverage ─────────────────────────────────────────
section("DATA COVERAGE")
try:
    cur.execute("""
        SELECT currency,
               MIN(signal_date) AS earliest, MAX(signal_date) AS latest,
               COUNT(*) AS rows
        FROM forex_network.macro_signals_history
        GROUP BY currency
        ORDER BY currency
    """)
    rows = cur.fetchall()
    if not rows:
        check("Macro signals history", "FAIL", "Empty table")
    else:
        currencies = [r['currency'] for r in rows]
        min_rows   = min(r['rows'] for r in rows)
        latest_all = max(str(r['latest']) for r in rows)
        days_stale = (now.date() - max(r['latest'] for r in rows)).days
        if len(currencies) < 8:
            check("Macro signals history", "WARN",
                  f"Only {len(currencies)}/8 currencies: {currencies}")
        elif days_stale > 3:
            check("Macro signals history", "WARN",
                  f"Latest={latest_all} ({days_stale}d ago) — needs refresh")
        else:
            check("Macro signals history", "PASS",
                  f"8 currencies, min={min_rows} rows, latest={latest_all}")
except Exception as e:
    check("Macro signals history", "FAIL", str(e)[:80])

# ── CHECK 11: Economic releases coverage ──────────────────────────────────────
try:
    cur.execute("""
        SELECT country, source,
               COUNT(*) AS rows,
               MAX(release_time::date) AS latest
        FROM forex_network.economic_releases
        GROUP BY country, source
        ORDER BY country, source
    """)
    rows = cur.fetchall()
    if not rows:
        check("Economic releases", "FAIL", "Empty table")
    else:
        total = sum(r['rows'] for r in rows)
        # Check EUR has Eurostat
        eur_eurostat = next((r for r in rows if r['country'] == 'EUR' and r['source'] == 'EUROSTAT'), None)
        eur_fred     = next((r for r in rows if r['country'] == 'EUR' and r['source'] == 'FRED'), None)
        currencies   = list({r['country'] for r in rows})
        if not eur_eurostat:
            check("Economic releases — EUR Eurostat", "WARN",
                  "No EUROSTAT rows for EUR (Eurostat ingest may not have run)")
        else:
            check("Economic releases — EUR Eurostat", "PASS",
                  f"{eur_eurostat['rows']} rows, latest={eur_eurostat['latest']}")
        coverage = len(currencies)
        check("Economic releases — total coverage", "PASS" if coverage >= 8 else "WARN",
              f"{total} rows, {coverage} currencies, sources={list({r['source'] for r in rows})}")
except Exception as e:
    check("Economic releases", "FAIL", str(e)[:80])

# ── CHECK 12: Historical prices coverage ─────────────────────────────────────
try:
    cur.execute("""
        SELECT timeframe, COUNT(DISTINCT instrument) AS pairs,
               MAX(ts) AS latest,
               EXTRACT(EPOCH FROM (NOW() - MAX(ts)))/3600 AS hours_stale
        FROM forex_network.historical_prices
        GROUP BY timeframe
        ORDER BY timeframe
    """)
    rows = cur.fetchall()
    if not rows:
        check("Historical prices", "FAIL", "Empty table")
    else:
        for r in rows:
            tf     = r['timeframe']
            pairs  = r['pairs']
            stale  = float(r['hours_stale'])
            latest = str(r['latest'])[:19]
            # Prices for a closed market (weekends) can be 48h+ stale
            status = "PASS" if stale < 72 else "WARN"
            check(f"Historical prices: {tf}", status,
                  f"{pairs} pairs  latest={latest}  stale={stale:.1f}h")
except Exception as e:
    check("Historical prices", "FAIL", str(e)[:80])

# ── CHECK 13: IG connectivity ─────────────────────────────────────────────────
section("IG MARKETS CONNECTIVITY")
try:
    cst, token, api_key = ig_auth()
    req = urllib.request.Request(
        f'{IG_BASE}/positions',
        headers={'X-IG-API-KEY': api_key, 'CST': cst, 'X-SECURITY-TOKEN': token, 'Version': '2'}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read().decode())
    n = len(data.get('positions', []))
    check("IG auth + positions fetch", "PASS", f"{n} open positions on demo account")
except Exception as e:
    check("IG auth + positions fetch", "FAIL", str(e)[:80])

try:
    req2 = urllib.request.Request(
        f'{IG_BASE}/accounts',
        headers={'X-IG-API-KEY': api_key, 'CST': cst, 'X-SECURITY-TOKEN': token, 'Version': '1'}
    )
    with urllib.request.urlopen(req2, timeout=10) as r2:
        acct = json.loads(r2.read().decode())
    for a in acct.get('accounts', []):
        bal = a.get('balance', {})
        av  = bal.get('available', 0)
        check(f"IG account: {a['accountId']} ({a['accountName']})",
              "PASS" if float(av) > 0 else "WARN",
              f"balance={bal.get('balance')}  available={av}")
except Exception as e:
    check("IG account balance", "WARN", str(e)[:80])

# ── CHECK 14: No stale LLM calls in recent logs ───────────────────────────────
section("LOG HEALTH")
LOG_FILES = {
    'macro':     '/var/log/neo/macro_agent.error.log',
    'technical': '/var/log/neo/technical_agent.error.log',
    'regime':    '/var/log/neo/regime_agent.error.log',
}
for agent, logpath in LOG_FILES.items():
    try:
        r = subprocess.run(
            ['tail', '-200', logpath],
            capture_output=True, text=True, timeout=5
        )
        lines = r.stdout
        # Only flag LLM calls that occurred AFTER the service last restarted.
        # Pull the restart timestamp from systemd so pre-restart log entries
        # (e.g. an earlier LLM-based version) don't create false positives.
        restart_ts = None
        try:
            rs = subprocess.run(
                ['systemctl', 'show', f'neo-{agent}-agent',
                 '--property=ActiveEnterTimestamp'],
                capture_output=True, text=True, timeout=5
            )
            ts_str = rs.stdout.strip().replace('ActiveEnterTimestamp=', '')
            if ts_str and ts_str != 'n/a':
                from dateutil import parser as dtparser
                restart_ts = dtparser.parse(ts_str)
        except Exception:
            pass

        # Fallback: anything in last 30 minutes if we can't get restart time
        cutoff_str = restart_ts.strftime('%Y-%m-%d %H:%M') if restart_ts else \
                     (now.replace(tzinfo=None) - __import__('datetime').timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M')

        recent_llm = [l for l in lines.splitlines()
                      if any(k in l for k in ['anthropic.com', 'invoke_model', 'bedrock', 'openai'])
                      and l[:19] > cutoff_str]
        if recent_llm:
            check(f"LLM calls: {agent}", "WARN",
                  f"{len(recent_llm)} LLM call(s) since last restart")
        else:
            check(f"LLM calls: {agent}", "PASS", "No LLM calls in recent logs")
    except Exception as e:
        check(f"LLM calls: {agent}", "WARN", f"Could not read log: {e}")

# Check for errors in recent log lines
for agent, logpath in LOG_FILES.items():
    try:
        r = subprocess.run(
            ['tail', '-50', logpath],
            capture_output=True, text=True, timeout=5
        )
        errors = [l for l in r.stdout.splitlines() if ' ERROR ' in l or 'Traceback' in l]
        if errors:
            check(f"Recent errors: {agent}", "WARN",
                  f"{len(errors)} ERROR lines in last 50 log rows")
        else:
            check(f"Recent errors: {agent}", "PASS", "No recent ERRORs")
    except Exception as e:
        check(f"Recent errors: {agent}", "WARN", f"Could not read log: {e}")

# ── Summary ───────────────────────────────────────────────────────────────────
section("SUMMARY")
n_pass = sum(1 for s, _, _ in results if s == 'PASS')
n_warn = sum(1 for s, _, _ in results if s == 'WARN')
n_fail = sum(1 for s, _, _ in results if s == 'FAIL')

print(f"\n  Total checks: {len(results)}   PASS: {n_pass}   WARN: {n_warn}   FAIL: {n_fail}\n")

if n_fail:
    print("  FAILURES:")
    for s, name, detail in results:
        if s == 'FAIL':
            print(f"    [✗] {name}: {detail}")

if n_warn:
    print("\n  WARNINGS:")
    for s, name, detail in results:
        if s == 'WARN':
            print(f"    [~] {name}: {detail}")

verdict = "NOT PRODUCTION READY" if n_fail else ("REVIEW WARNINGS" if n_warn else "PRODUCTION READY")
print(f"\n  Verdict: {verdict}\n")

conn.close()
