#!/usr/bin/env python3
"""
Monday Market Open Monitor
Runs every 5 minutes from 07:00-10:00 UTC (London open window).
Stops early if first trade fires. Run via start_monday_monitor.sh.
"""
import boto3, json, psycopg2, psycopg2.extras, time, sys
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

def get_conn():
    return psycopg2.connect(host=ep, dbname='postgres', user=creds['username'],
                            password=creds['password'],
                            options='-c search_path=forex_network,shared,public')

def fmt(v):
    if v is None: return '  N/A '
    return f'{float(v):+.3f}'

def run_check(check_num):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now  = datetime.now(timezone.utc)

    print(f"\n{'='*80}")
    print(f"  MONDAY OPEN CHECK #{check_num}  —  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*80}", flush=True)

    # --- Agent health ---
    print("\n── AGENT HEALTH ──────────────────────────────────────────────────────────────")
    cur.execute("""
        SELECT agent_name,
               COUNT(DISTINCT user_id) AS users,
               MAX(last_seen) AS latest,
               ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(last_seen))) / 60) AS mins_ago,
               SUM(error_count) AS total_errors,
               MAX(status) AS status
        FROM forex_network.agent_heartbeats
        GROUP BY agent_name
        ORDER BY agent_name
    """)
    for row in cur.fetchall():
        mins = float(row['mins_ago'] or 0)
        thresh = get_stale_threshold(row['agent_name'])
        flag = '✓' if mins < thresh else ('⚠ STALE' if mins < thresh * 3 else '✗ DEAD')
        print(f"  {flag}  {row['agent_name']:20s}  {row['users']} users  "
              f"{mins:.0f} min ago  errs={row['total_errors']}  status={row['status']}")

    # --- System stress ---
    print("\n── STRESS & CIRCUIT BREAKERS ─────────────────────────────────────────────────")
    cur.execute("""
        SELECT user_id, convergence_threshold, circuit_breaker_active,
               circuit_breaker_reason, size_multiplier, account_value,
               account_value_currency, daily_loss_limit_pct
        FROM forex_network.risk_parameters
        ORDER BY convergence_threshold DESC
    """)
    risk_rows = cur.fetchall()
    stress_adj = 0.0
    for rp in risk_rows:
        uid   = rp['user_id']
        label = USERS.get(uid, uid[:8])
        cb    = ' ⚡ CB_ACTIVE: ' + str(rp['circuit_breaker_reason']) if rp['circuit_breaker_active'] else ''
        thresh_eff = float(rp['convergence_threshold'])
        print(f"  {label}  threshold={thresh_eff:.2f}  size_mult={float(rp['size_multiplier']):.2f}"
              f"  acct=${float(rp['account_value']):,.0f}{rp['account_value_currency']}"
              f"  daily_lim={float(rp['daily_loss_limit_pct']):.1f}%{cb}")

    # Stress score from latest rejection log entry
    cur.execute("SELECT stress_score FROM forex_network.rejection_log ORDER BY created_at DESC LIMIT 1")
    sr = cur.fetchone()
    if sr and sr['stress_score'] is not None:
        stress = float(sr['stress_score'])
        band = 'Normal' if stress < 30 else ('Elevated' if stress < 50 else 'High')
        if 30 <= stress < 50: stress_adj = 0.05
        print(f"\n  Stress: {stress:.1f} ({band})"
              + (f"  → thresholds +0.05 today" if stress_adj else ""))

    # --- Current signals by pair ---
    print("\n── CURRENT SIGNALS ───────────────────────────────────────────────────────────")
    print(f"  {'Pair':8s}  {'Macro sc':>8s}  {'Mac cf':>6s}  {'Tech sc':>8s}  {'Tch cf':>6s}  "
          f"{'Regime sc':>9s}  {'Conv':>6s}  {'Gate':>6s}")
    print(f"  {'-'*8}  {'-'*8}  {'-'*6}  {'-'*8}  {'-'*6}  {'-'*9}  {'-'*6}  {'-'*6}")

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

        ms = sigs.get('macro',     {}).get('s')
        mc = sigs.get('macro',     {}).get('c')
        ts = sigs.get('technical', {}).get('s')
        tc = sigs.get('technical', {}).get('c')

        # Regime: per-pair or GLOBAL fallback
        rs = sigs.get('regime', {}).get('s')
        if rs is None or rs == 0.0:
            cur.execute("""
                SELECT score::float AS s FROM forex_network.agent_signals
                WHERE agent_name='regime' AND instrument IS NULL AND expires_at > NOW()
                ORDER BY created_at DESC LIMIT 1
            """)
            gr = cur.fetchone()
            rs = gr['s'] if gr else 0.0

        rm = 0.80 + (float(rs) * 0.40) if rs is not None else 0.80

        ms_v = float(ms) if ms is not None else 0.0
        ts_v = float(ts) if ts is not None else 0.0
        mc_v = float(mc) if mc is not None else 0.0
        tc_v = float(tc) if tc is not None else 0.0

        macro_ok = mc_v >= 0.30 if mc is not None else False
        tech_ok  = tc_v >= 0.30 if tc is not None else False

        dir_sum = (ms_v * 0.35 if macro_ok else 0) + (ts_v * 0.45 if tech_ok else 0)
        conv    = dir_sum * rm

        # Direction check: macro & technical must agree
        if macro_ok and tech_ok and ms_v != 0 and ts_v != 0:
            agrees = (ms_v > 0) == (ts_v > 0)
        else:
            agrees = True

        gated = not agrees or abs(conv) == 0

        # Threshold check across all users
        passes = []
        for rp in risk_rows:
            t = float(rp['convergence_threshold']) + stress_adj
            if abs(conv) >= t and not gated:
                passes.append(USERS.get(rp['user_id'], '?')[:3])

        gate_str = ('PASS:' + ','.join(passes)) if passes else ('DIR?' if not agrees else 'BELOW')

        print(f"  {pair:8s}  {fmt(ms):>8s}  {mc_v:>6.2f}  {fmt(ts):>8s}  {tc_v:>6.2f}  "
              f"{fmt(rs):>9s}  {conv:>+6.3f}  {gate_str:<10s}")

    # --- Rejections this session ---
    print("\n── RECENT REJECTIONS (last 60 min) ──────────────────────────────────────────")
    cur.execute("""
        SELECT instrument, reason, conflict_category,
               COUNT(*) AS cnt, MAX(created_at) AS latest
        FROM forex_network.rejection_log
        WHERE created_at > NOW() - INTERVAL '60 minutes'
        GROUP BY instrument, reason, conflict_category
        ORDER BY cnt DESC, latest DESC
        LIMIT 12
    """)
    rej = cur.fetchall()
    if rej:
        for r in rej:
            print(f"  {r['instrument']:8s}  x{r['cnt']}  [{r['conflict_category']}]  {r['reason'][:65]}")
    else:
        print("  None")

    # --- Open trades ---
    print("\n── OPEN TRADES ───────────────────────────────────────────────────────────────")
    cur.execute("""
        SELECT t.id, t.instrument, t.direction, t.entry_price, t.stop_price,
               t.target_price, t.position_size_usd, t.convergence_score,
               t.entry_time, t.paper_mode, rp.account_value_currency
        FROM forex_network.trades t
        JOIN forex_network.risk_parameters rp ON rp.user_id = t.user_id
        WHERE t.exit_time IS NULL
        ORDER BY t.entry_time DESC
    """)
    trades = cur.fetchall()
    if trades:
        for tr in trades:
            age_m = (now - tr['entry_time']).total_seconds() / 60
            print(f"  #{tr['id']}  {tr['instrument']}  {tr['direction'].upper():5s}  "
                  f"entry={tr['entry_price']}  stop={tr['stop_price']}  tgt={tr['target_price']}  "
                  f"size=${float(tr['position_size_usd']):,.0f}  conv={tr['convergence_score']}  "
                  f"{age_m:.0f}min ago  {'PAPER' if tr['paper_mode'] else 'LIVE'}")
    else:
        print("  No open positions")

    # --- Today's closed trades ---
    cur.execute("""
        SELECT COUNT(*) AS n, SUM(pnl) AS total_pnl,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM forex_network.trades
        WHERE exit_time IS NOT NULL AND DATE(entry_time AT TIME ZONE 'UTC') = CURRENT_DATE
    """)
    ct = cur.fetchone()
    if ct and ct['n']:
        wr = float(ct['wins']) / float(ct['n']) * 100 if ct['n'] else 0
        print(f"\n  Today closed: {ct['n']} trades  PnL=${float(ct['total_pnl'] or 0):+,.2f}  Win rate={wr:.0f}%")

    cur.close(); conn.close()

    return len(trades) > 0  # True = first trade detected

def main():
    print(f"\n{'#'*80}")
    print(f"  PROJECT NEO — MONDAY OPEN MONITOR")
    print(f"  Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Checks every 5 min. Stops at 10:00 UTC or after first trade.")
    print(f"{'#'*80}")
    sys.stdout.flush()

    check_num = 0
    end_time  = datetime.now(timezone.utc).replace(hour=10, minute=0, second=0, microsecond=0)
    if datetime.now(timezone.utc) > end_time:
        # If started after 10:00, run for 3 hours from now
        end_time = datetime.now(timezone.utc) + timedelta(hours=3)

    while datetime.now(timezone.utc) < end_time:
        check_num += 1
        try:
            trade_fired = run_check(check_num)
            sys.stdout.flush()
            if trade_fired:
                print(f"\n  *** TRADE DETECTED — stopping monitor ***")
                sys.stdout.flush()
                break
        except Exception as e:
            print(f"\n  ERROR in check #{check_num}: {e}", flush=True)

        remaining = (end_time - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 300:
            break
        print(f"\n  Next check in 5 min  (ends {end_time.strftime('%H:%M')} UTC)", flush=True)
        time.sleep(300)

    print(f"\n{'#'*80}")
    print(f"  Monitor complete — {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
    print(f"{'#'*80}\n")

if __name__ == '__main__':
    main()
