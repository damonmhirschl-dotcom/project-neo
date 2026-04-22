#!/usr/bin/env python3
"""
End-of-Day Summary
Single run: full daily recap — trades, rejections, peak convergence,
API provider performance, account status, stress metrics.
"""
import boto3, json, psycopg2, psycopg2.extras
from datetime import datetime, timezone, date
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
today = date.today()

print(f"\n{'#'*80}")
print(f"  PROJECT NEO — END-OF-DAY SUMMARY")
print(f"  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC  (for {today})")
print(f"{'#'*80}")

# --- Daily summary record ---
print("\n── DAILY SUMMARY RECORD ─────────────────────────────────────────────────────────")
cur.execute("SELECT * FROM forex_network.daily_summaries WHERE summary_date = %s", (today,))
ds = cur.fetchone()
if ds:
    wr = f"{float(ds['win_rate'])*100:.1f}%" if ds['win_rate'] else 'N/A'
    pf = f"{float(ds['profit_factor']):.2f}" if ds.get('profit_factor') else 'N/A'
    print(f"\n  Trades executed:     {ds['trades_executed']}")
    print(f"  Net PnL:             ${float(ds['net_pnl']):+,.2f}")
    print(f"  Win rate:            {wr}")
    print(f"  Profit factor:       {pf}")
    print(f"  Signals generated:   {ds['signals_generated']}")
    print(f"  Opportunities missed:{ds['opportunities_missed']}")
    print(f"  Circuit breakers:    {ds['circuit_breakers_fired']}")
    print(f"  Agent degradations:  {ds['agent_degradations']}")
    print(f"  Data issues:         {ds['data_issues']}")
    print(f"  Stress avg:          {float(ds['stress_score_avg'] or 0):.1f}  "
          f"min={float(ds['stress_score_min'] or 0):.1f}  "
          f"max={float(ds['stress_score_max'] or 0):.1f}")
    if ds.get('sortino_rolling'):
        print(f"  Sortino (rolling):   {float(ds['sortino_rolling']):.4f}")
else:
    print("  No daily summary record for today yet")

# --- All trades today ---
print("\n── ALL TRADES TODAY ──────────────────────────────────────────────────────────────")
cur.execute("""
    SELECT t.instrument, t.direction, t.entry_price, t.exit_price,
           t.pnl, t.pnl_pips, t.exit_reason, t.convergence_score,
           t.return_pct, t.rr_after_swap, t.paper_mode,
           t.slippage_pips, t.session_at_entry, t.regime_at_entry,
           t.entry_time, t.exit_time,
           EXTRACT(EPOCH FROM (COALESCE(t.exit_time, NOW()) - t.entry_time)) / 60 AS hold_min
    FROM forex_network.trades t
    WHERE DATE(t.entry_time AT TIME ZONE 'UTC') = CURRENT_DATE
    ORDER BY t.entry_time
""")
trades = cur.fetchall()
if trades:
    total_pnl = sum(float(t['pnl'] or 0) for t in trades)
    open_cnt  = sum(1 for t in trades if t['exit_time'] is None)
    closed_cnt = sum(1 for t in trades if t['exit_time'] is not None)
    wins = sum(1 for t in trades if t['exit_time'] and float(t['pnl'] or 0) > 0)
    print(f"\n  Total: {len(trades)}  Open: {open_cnt}  Closed: {closed_cnt}  "
          f"Wins: {wins}/{closed_cnt}  Net PnL: ${total_pnl:+,.2f}")
    print()
    print(f"  {'Pair':8s}  {'Dir':5s}  {'PnL':>10s}  {'Pips':>6s}  "
          f"{'Slip':>5s}  {'Exit reason':20s}  {'Session':8s}  {'Min':>5s}")
    for tr in trades:
        status = 'OPEN' if tr['exit_time'] is None else (
            '✓' if float(tr['pnl'] or 0) > 0 else '✗')
        print(f"  {tr['instrument']:8s}  {tr['direction']:5s}  "
              f"${float(tr['pnl'] or 0):>+9,.2f}  "
              f"{float(tr['pnl_pips'] or 0):>6.1f}  "
              f"{float(tr['slippage_pips'] or 0):>5.1f}  "
              f"{(tr['exit_reason'] or status):20s}  "
              f"{(tr['session_at_entry'] or ''):8s}  "
              f"{float(tr['hold_min'] or 0):>5.0f}")
else:
    print("  No trades today")

# --- Peak convergence today ---
print("\n── PEAK CONVERGENCE TODAY ───────────────────────────────────────────────────────")
cur.execute("""
    SELECT instrument,
           ROUND(MAX(ABS(convergence_score::float))::numeric, 4) AS peak_abs,
           ROUND(MAX(convergence_score::float)::numeric, 4) AS peak_pos,
           ROUND(MIN(convergence_score::float)::numeric, 4) AS peak_neg,
           COUNT(*) AS cycles,
           MAX(cycle_timestamp) AS latest
    FROM forex_network.convergence_history
    WHERE DATE(cycle_timestamp AT TIME ZONE 'UTC') = CURRENT_DATE
    GROUP BY instrument
    ORDER BY peak_abs DESC
""")
print(f"\n  {'Pair':8s}  {'Peak |Conv|':>12s}  {'Peak +':>8s}  {'Peak -':>8s}  {'Cycles':>6s}")
for r in cur.fetchall():
    print(f"  {r['instrument']:8s}  {float(r['peak_abs']):>12.4f}  "
          f"{float(r['peak_pos']):>+8.4f}  {float(r['peak_neg']):>+8.4f}  "
          f"{r['cycles']:>6d}")

# --- Rejection analysis ---
print("\n── REJECTION ANALYSIS TODAY ─────────────────────────────────────────────────────")
cur.execute("""
    SELECT conflict_category, COUNT(*) AS cnt,
           COUNT(DISTINCT instrument) AS pairs,
           ROUND(AVG(stress_score::float)::numeric, 1) AS avg_stress
    FROM forex_network.rejection_log
    WHERE DATE(created_at AT TIME ZONE 'UTC') = CURRENT_DATE
    GROUP BY conflict_category
    ORDER BY cnt DESC
""")
print(f"\n  {'Category':30s}  {'Count':>6s}  {'Pairs':>5s}  {'AvgStress':>9s}")
for r in cur.fetchall():
    print(f"  {(r['conflict_category'] or 'unknown'):30s}  {r['cnt']:>6d}  "
          f"{r['pairs']:>5d}  {float(r['avg_stress'] or 0):>9.1f}")

# Top rejected pairs
cur.execute("""
    SELECT instrument, COUNT(*) AS cnt
    FROM forex_network.rejection_log
    WHERE DATE(created_at AT TIME ZONE 'UTC') = CURRENT_DATE
    GROUP BY instrument
    ORDER BY cnt DESC LIMIT 7
""")
print(f"\n  Top rejected pairs:")
for r in cur.fetchall():
    print(f"    {r['instrument']:8s}  {r['cnt']} rejections")

# --- API provider performance today ---
print("\n── API PROVIDER PERFORMANCE TODAY ───────────────────────────────────────────────")
cur.execute("""
    SELECT pr.provider, pr.uptime_pct, pr.avg_response_ms, pr.p95_response_ms,
           pr.error_rate_pct, pr.total_calls, pr.recommendation
    FROM forex_network.provider_reports pr
    WHERE pr.report_date = CURRENT_DATE
    ORDER BY pr.avg_response_ms DESC NULLS LAST
""")
print(f"\n  {'Provider':15s}  {'Uptime%':>8s}  {'Avg ms':>8s}  {'P95 ms':>8s}  "
      f"{'Err%':>6s}  {'Calls':>6s}  Recommendation")
for r in cur.fetchall():
    flag = ' ⚠' if float(r['error_rate_pct'] or 0) > 5 or float(r['uptime_pct'] or 100) < 95 else ''
    print(f"  {r['provider']:15s}  {float(r['uptime_pct'] or 0):>8.1f}  "
          f"{(r['avg_response_ms'] or 0):>8d}  {(r['p95_response_ms'] or 0):>8d}  "
          f"{float(r['error_rate_pct'] or 0):>6.1f}  {(r['total_calls'] or 0):>6d}  "
          f"{(r['recommendation'] or ''):20s}{flag}")

# Also real-time last 4h from api_call_log
print(f"\n  Real-time (last 4h from api_call_log):")
cur.execute("""
    SELECT provider, COUNT(*) AS calls,
           ROUND(AVG(response_time_ms)) AS avg_ms,
           SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failures
    FROM forex_network.api_call_log
    WHERE called_at > NOW() - INTERVAL '4 hours'
    GROUP BY provider
    ORDER BY avg_ms DESC NULLS LAST
""")
for r in cur.fetchall():
    fail_flag = f"  ✗{r['failures']} fails" if r['failures'] else ''
    print(f"    {r['provider']:15s}  {r['avg_ms']}ms avg  {r['calls']} calls{fail_flag}")

# --- Account status per user ---
print("\n── ACCOUNT STATUS ───────────────────────────────────────────────────────────────")
cur.execute("""
    SELECT user_id, account_value, account_value_currency,
           peak_account_value, size_multiplier, circuit_breaker_active,
           drawdown_step_level
    FROM forex_network.risk_parameters
    ORDER BY account_value DESC
""")
for rp in cur.fetchall():
    uid   = str(rp['user_id'])
    label = USERS.get(uid, uid[:8])
    dd = 0.0
    if rp['peak_account_value'] and float(rp['peak_account_value']) > 0:
        dd = (1 - float(rp['account_value']) / float(rp['peak_account_value'])) * 100
    cb = '  ⚡ CB ACTIVE' if rp['circuit_breaker_active'] else ''
    print(f"  {label}  ${float(rp['account_value']):>12,.2f} {rp['account_value_currency']}  "
          f"DD={dd:.2f}%  step={rp['drawdown_step_level']}  "
          f"size_mult={float(rp['size_multiplier']):.2f}{cb}")

# --- Last 7 days performance ---
print("\n── LAST 7 DAYS ──────────────────────────────────────────────────────────────────")
cur.execute("""
    SELECT summary_date, trades_executed, net_pnl, win_rate,
           circuit_breakers_fired, stress_score_avg
    FROM forex_network.daily_summaries
    ORDER BY summary_date DESC LIMIT 7
""")
print(f"\n  {'Date':12s}  {'Trades':>6s}  {'PnL':>10s}  {'Win%':>6s}  {'CBs':>3s}  {'Stress':>6s}")
for r in cur.fetchall():
    wr = f"{float(r['win_rate'])*100:.0f}%" if r['win_rate'] else "  N/A"
    print(f"  {str(r['summary_date']):12s}  {r['trades_executed']:>6d}  "
          f"${float(r['net_pnl'] or 0):>+9,.2f}  {wr:>6s}  "
          f"{r['circuit_breakers_fired']:>3d}  {float(r['stress_score_avg'] or 0):>6.1f}")

cur.close(); conn.close()
print(f"\n{'#'*80}")
print(f"  EOD summary complete  —  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
print(f"{'#'*80}\n")
