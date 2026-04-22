#!/usr/bin/env python3
"""
Trading Day Monitor — automated pre-market, in-session, and post-session monitoring.
Runs via cron on EC2. Reports via SNS email alerts.

Cron schedule:
  06:00 UTC Mon-Fri — pre-market health check
  07:00-17:00 UTC Mon-Fri — every 15 min session monitoring
  18:00 UTC Mon-Fri — end-of-day summary
"""

import boto3, json, psycopg2, psycopg2.extras, sys, os
from datetime import datetime, timezone, timedelta


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


MODE = sys.argv[1] if len(sys.argv) > 1 else 'session'  # pre_market | session | eod_summary

def get_db_connection():
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm',            region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    ep    = ssm.get_parameter(Name='/platform/config/rds-endpoint', WithDecryption=True)['Parameter']['Value']
    return psycopg2.connect(
        host=ep, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network,shared,public'
    )

def send_alert(subject, message, severity='INFO'):
    ssm = boto3.client('ssm', region_name='eu-west-2')
    topic_arn = ssm.get_parameter(Name='/platform/config/alert-topic-arn')['Parameter']['Value']
    sns = boto3.client('sns', region_name='eu-west-2')
    sns.publish(TopicArn=topic_arn, Subject=f"[{severity}] {subject}", Message=message)

def pre_market_check():
    """Run at 06:00 UTC before London open. Full system health verification."""
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    issues = []
    report = [
        "=== PRE-MARKET HEALTH CHECK ===",
        f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    # 1. Agent heartbeats
    report.append("--- AGENT HEARTBEATS ---")
    cur.execute("""
        SELECT agent_name, COUNT(*) AS users, MAX(last_seen) AS latest,
               EXTRACT(EPOCH FROM (NOW() - MAX(last_seen))) / 60 AS mins_ago
        FROM forex_network.agent_heartbeats
        GROUP BY agent_name ORDER BY agent_name
    """)
    for row in cur.fetchall():
        status = 'OK' if row['mins_ago'] < get_stale_threshold(row['agent_name']) else 'STALE'
        report.append(f"  [{status}] {row['agent_name']:20s} {row['users']} users  {row['mins_ago']:.0f}min ago")
        if row['mins_ago'] >= get_stale_threshold(row['agent_name']):
            issues.append(f"{row['agent_name']} heartbeat stale ({row['mins_ago']:.0f}min)")

    # 2. Latest convergence scores (DISTINCT ON to deduplicate across users)
    report.append("\n--- CONVERGENCE SCORES ---")
    cur.execute("""
        SELECT DISTINCT ON (instrument)
               instrument, convergence_score::float, bias, cycle_timestamp
        FROM forex_network.convergence_history
        ORDER BY instrument, cycle_timestamp DESC
    """)
    for row in cur.fetchall():
        report.append(f"  {row['instrument']:8s} {row['convergence_score']:+.3f}  {row['bias'] or ''}")

    # 3. Stress score (column is system_stress_score)
    report.append("\n--- STRESS SCORE ---")
    cur.execute("""
        SELECT system_stress_score::float AS stress_score, stress_state,
               EXTRACT(EPOCH FROM (NOW() - created_at)) / 60 AS mins_ago
        FROM shared.market_context_snapshots
        ORDER BY created_at DESC LIMIT 1
    """)
    stress = cur.fetchone()
    if stress:
        report.append(
            f"  Score: {stress['stress_score']:.1f} ({stress['stress_state']}) "
            f"— {stress['mins_ago']:.0f}min ago"
        )
        if stress['mins_ago'] > 30:
            issues.append(f"Stress snapshot stale ({stress['mins_ago']:.0f}min)")

    # 4. Data freshness (use SQL age to avoid Python tz issues)
    report.append("\n--- DATA FRESHNESS ---")
    cur.execute("""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ts))) / 3600 AS age_hrs
        FROM forex_network.sentiment_ssi
    """)
    row = cur.fetchone()
    if row and row['age_hrs'] is not None:
        report.append(f"  MyFXBook SSI: {row['age_hrs']:.1f}hrs ago")
        if row['age_hrs'] > 30:
            issues.append(f"MyFXBook SSI stale ({row['age_hrs']:.1f}hrs)")

    cur.execute("""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ts))) / 3600 AS age_hrs
        FROM forex_network.ig_client_sentiment
    """)
    row = cur.fetchone()
    if row and row['age_hrs'] is not None:
        report.append(f"  IG Sentiment:  {row['age_hrs']:.1f}hrs ago")
        if row['age_hrs'] > 30:
            issues.append(f"IG sentiment stale ({row['age_hrs']:.1f}hrs)")

    cur.execute("""
        SELECT MAX(bar_time) AS latest
        FROM forex_network.cross_asset_prices
        WHERE instrument = 'DXY'
    """)
    row = cur.fetchone()
    if row and row['latest']:
        report.append(f"  DXY latest bar: {row['latest']}")

    # 5. Kill switch / maintenance mode
    report.append("\n--- CONTROLS ---")
    ssm = boto3.client('ssm', region_name='eu-west-2')
    for param, label in [
        ('/platform/config/kill-switch',    'Kill switch'),
        ('/platform/config/maintenance-mode', 'Maintenance mode'),
    ]:
        try:
            val = ssm.get_parameter(Name=param)['Parameter']['Value']
            report.append(f"  {label}: {val}")
            if val.lower() == 'true':
                issues.append(f"{label.upper()} IS ACTIVE")
        except ssm.exceptions.ParameterNotFound:
            report.append(f"  {label}: (param not set)")
        except Exception:
            pass

    # 6. Upcoming high-impact events today
    # economic_releases uses: indicator, release_time, country, impact_level
    report.append("\n--- HIGH-IMPACT EVENTS TODAY ---")
    cur.execute("""
        SELECT indicator, country, release_time, impact_level
        FROM forex_network.economic_releases
        WHERE release_time::date = CURRENT_DATE
          AND impact_level = 'high'
        ORDER BY release_time
    """)
    events = cur.fetchall()
    for e in events:
        report.append(f"  {e['release_time'].strftime('%H:%M UTC')} — {e['country']} {e['indicator']}")
    if not events:
        report.append("  No high-impact events today")

    # Summary
    report.append(f"\n--- SUMMARY ---")
    if issues:
        report.append(f"  ISSUES FOUND: {len(issues)}")
        for i in issues:
            report.append(f"  ! {i}")
        severity = 'WARNING'
    else:
        report.append("  All systems healthy")
        severity = 'INFO'

    full_report = "\n".join(report)
    print(full_report)
    send_alert(f"Pre-Market Check — {'ISSUES' if issues else 'HEALTHY'}", full_report, severity)
    cur.close()
    conn.close()


def session_monitor():
    """Run every 15 min during trading hours (07:00-17:00 UTC). Alert on issues only."""
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    issues = []
    now_str = datetime.now(timezone.utc).strftime('%H:%M UTC')

    # Agent heartbeats
    cur.execute("""
        SELECT agent_name,
               EXTRACT(EPOCH FROM (NOW() - MAX(last_seen))) / 60 AS mins_ago
        FROM forex_network.agent_heartbeats
        GROUP BY agent_name
    """)
    for row in cur.fetchall():
        if row['mins_ago'] >= get_stale_threshold(row['agent_name']):
            issues.append(f"{row['agent_name']} heartbeat stale ({row['mins_ago']:.0f}min)")

    # New trades in last 15 min (informational)
    cur.execute("""
        SELECT COUNT(*) AS cnt FROM forex_network.trades
        WHERE created_at >= NOW() - INTERVAL '15 minutes'
    """)
    new_trades = cur.fetchone()['cnt']

    # Stress snapshot staleness
    cur.execute("""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 60 AS mins_ago
        FROM shared.market_context_snapshots
    """)
    row = cur.fetchone()
    if row and row['mins_ago'] and row['mins_ago'] > 30:
        issues.append(f"Stress snapshot stale ({row['mins_ago']:.0f}min)")

    if issues:
        msg = f"Session Monitor Alert — {now_str}\n\n"
        msg += "\n".join(f"! {i}" for i in issues)
        print(msg)
        send_alert("Session Monitor — ISSUES", msg, 'WARNING')
    else:
        print(f"[{now_str}] Session monitor: all healthy, {new_trades} new trade(s) in last 15min")

    cur.close()
    conn.close()


def eod_summary():
    """Run at 18:00 UTC. Daily trading summary."""
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    report = ["=== END OF DAY SUMMARY ===", f"Date: {today}", ""]

    # Trades today — trades uses direction (long/short), no status column
    cur.execute("""
        SELECT instrument, direction, entry_price::float,
               exit_time, pnl_pips::float,
               entry_context->>'consensus_grade' AS consensus,
               entry_context->>'extremity_bucket' AS extremity
        FROM forex_network.trades
        WHERE created_at::date = CURRENT_DATE
        ORDER BY created_at
    """)
    trades = cur.fetchall()
    report.append(f"--- TRADES TODAY: {len(trades)} ---")
    total_pnl = 0
    for t in trades:
        pnl    = float(t['pnl_pips'] or 0)
        status = 'closed' if t['exit_time'] else 'open'
        total_pnl += pnl
        report.append(
            f"  {t['instrument']} {t['direction']:5s} [{status}]  "
            f"{pnl:+.1f} pips  "
            f"consensus={t['consensus'] or 'N/A'}  "
            f"extremity={t['extremity'] or 'N/A'}"
        )
    report.append(f"  Total realised P&L: {total_pnl:+.1f} pips")

    # Convergence range today
    report.append("\n--- CONVERGENCE RANGE (today) ---")
    cur.execute("""
        SELECT instrument,
               MIN(convergence_score::float) AS min_conv,
               MAX(convergence_score::float) AS max_conv,
               AVG(convergence_score::float) AS avg_conv
        FROM forex_network.convergence_history
        WHERE cycle_timestamp::date = CURRENT_DATE
        GROUP BY instrument ORDER BY instrument
    """)
    for row in cur.fetchall():
        report.append(
            f"  {row['instrument']:8s}  "
            f"min={row['min_conv']:+.3f}  "
            f"max={row['max_conv']:+.3f}  "
            f"avg={row['avg_conv']:+.3f}"
        )

    # Stress range today
    report.append("\n--- STRESS RANGE (today) ---")
    cur.execute("""
        SELECT MIN(system_stress_score::float) AS min_s,
               MAX(system_stress_score::float) AS max_s,
               AVG(system_stress_score::float) AS avg_s
        FROM shared.market_context_snapshots
        WHERE created_at::date = CURRENT_DATE
    """)
    stress = cur.fetchone()
    if stress and stress['min_s'] is not None:
        report.append(
            f"  Min={stress['min_s']:.1f}  "
            f"Max={stress['max_s']:.1f}  "
            f"Avg={stress['avg_s']:.1f}"
        )

    # Rejections today — column is 'reason', not 'rejection_reason'
    cur.execute("""
        SELECT reason, COUNT(*) AS cnt
        FROM forex_network.rejection_log
        WHERE created_at::date = CURRENT_DATE
        GROUP BY reason ORDER BY cnt DESC
    """)
    rejections = cur.fetchall()
    total_rej = sum(r['cnt'] for r in rejections)
    report.append(f"\n--- REJECTIONS TODAY: {total_rej} ---")
    for r in rejections[:10]:  # cap at 10 lines
        report.append(f"  {r['reason']}: {r['cnt']}")

    # Learning module status (no status column — use exit_time IS NOT NULL)
    report.append("\n--- LEARNING MODULE ---")
    cur.execute("""
        SELECT COUNT(*) AS cnt FROM forex_network.trades
        WHERE exit_time IS NOT NULL AND entry_context IS NOT NULL
    """)
    closed = cur.fetchone()['cnt']
    needed = 50
    report.append(
        f"  Closed trades with context: {closed}/{needed} needed for contrarian analysis"
        + (" — ACTIVE" if closed >= needed else " — accumulating")
    )

    cur.execute("""
        SELECT COUNT(*) AS cnt FROM forex_network.learning_adjustments
        WHERE reverted_at IS NULL
    """)
    adj = cur.fetchone()['cnt']
    report.append(f"  Active contrarian adjustments: {adj}")

    full_report = "\n".join(report)
    print(full_report)
    send_alert("End of Day Summary", full_report, 'INFO')
    cur.close()
    conn.close()


if __name__ == '__main__':
    if MODE == 'pre_market':
        pre_market_check()
    elif MODE == 'session':
        session_monitor()
    elif MODE == 'eod_summary':
        eod_summary()
    else:
        print(f"Unknown mode: {MODE}. Use: pre_market | session | eod_summary")
        sys.exit(1)
