#!/usr/bin/env python3
"""
Neo System Monitor — continuous background daemon
Runs every 60s, alerts via shared.alerting.send_alert() on WARN/FAIL
Writes CRITICAL events to system_alerts for Lambda SMS pickup
Logs to /var/log/neo/monitor.log
"""
import time, logging, psycopg2, boto3, json, subprocess, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.alerting import send_alert
from v1_swing_parameters import V1_SWING_PAIRS

LOG_FILE = '/var/log/neo/monitor.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger('neo_monitor')

CHECK_INTERVAL = 60  # seconds

SERVICES = [
    'neo-technical-agent',
    'neo-macro-agent',
    'neo-orchestrator',
    'neo-risk-guardian',
    'neo-execution-agent',
    'neo-learning-module',
]


def get_db_conn():
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    ep    = ssm.get_parameter(Name='/platform/config/rds-endpoint', WithDecryption=True)['Parameter']['Value']
    return psycopg2.connect(
        host=ep, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network',
    )


def write_system_alert(conn, severity, title, detail):
    """Write critical alerts to system_alerts so Lambda picks them up for SMS."""
    if severity != 'critical':
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forex_network.system_alerts
                (alert_type, severity, title, detail, acknowledged)
            VALUES (%s, %s, %s, %s, false)
        """, ('neo_monitor', severity, title[:255], detail[:1000]))
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f'Failed to write system_alert: {e}')
        try: conn.rollback()
        except Exception: pass


def is_market_hours():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    hour = now.hour
    return 0 <= hour < 21  # dead zone 21:00-00:00 UTC


# --- SERVICE LIVENESS ---
def check_services(conn):
    for svc in SERVICES:
        result = subprocess.run(
            ['systemctl', 'is-active', svc],
            capture_output=True, text=True
        )
        state = result.stdout.strip()
        if state != 'active':
            msg = f'{svc} is {state}'
            send_alert('CRITICAL', f'Service down: {svc}', {'state': state}, 'neo_monitor')
            write_system_alert(conn, 'critical', f'Service down: {svc}', msg)
            log.error(f'FAIL {msg}')
        else:
            log.info(f'PASS service {svc}')


# --- SIGNAL FLOW ---
def check_signal_flow(conn):
    if not is_market_hours():
        log.info('SKIP signal flow — outside market hours')
        return
    cur = conn.cursor()

    # TA signals — all 22 pairs should have emitted in last 6H
    cur.execute("""
        SELECT COUNT(DISTINCT instrument)
        FROM forex_network.agent_signals
        WHERE agent_name = 'technical'
        AND created_at > NOW() - INTERVAL '6 hours'
    """)
    ta_pairs = cur.fetchone()[0]
    if ta_pairs == 0:
        msg = 'Technical agent not emitting — may be stuck or crashing'
        send_alert('CRITICAL', 'TA: zero signals in 6H', {'pairs': 0}, 'neo_monitor')
        write_system_alert(conn, 'critical', 'TA: zero signals in 6H', msg)
    elif ta_pairs < 22:
        send_alert('WARNING', f'TA: only {ta_pairs}/22 pairs signalling',
            {'pairs': ta_pairs, 'expected': 22}, 'neo_monitor')
    log.info(f'TA signals last 6H: {ta_pairs}/22 pairs')

    # Macro agent — should have emitted within last 30 min
    cur.execute("""
        SELECT MAX(created_at)
        FROM forex_network.agent_signals
        WHERE agent_name = 'macro'
    """)
    last_macro = cur.fetchone()[0]
    if last_macro:
        age_min = (datetime.now(timezone.utc) - last_macro).total_seconds() / 60
        if age_min > 30:
            send_alert('WARNING', f'Macro agent silent for {age_min:.0f} min',
                {'age_min': round(age_min)}, 'neo_monitor')
        log.info(f'Last macro signal: {age_min:.0f} min ago')

    # Orchestrator decisions
    cur.execute("""
        SELECT COUNT(*)
        FROM forex_network.agent_signals
        WHERE agent_name = 'orchestrator'
        AND created_at > NOW() - INTERVAL '6 hours'
    """)
    orch = cur.fetchone()[0]
    log.info(f'Orchestrator decisions last 6H: {orch}')

    # RG — flag if 0 approvals but >10 proposals (nothing getting through)
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE decision = 'approved') as approved,
            COUNT(*) as total
        FROM forex_network.rejected_signals
        WHERE created_at > NOW() - INTERVAL '6 hours'
        AND strategy = 'v1_swing'
    """)
    row = cur.fetchone()
    if row and row[1] > 10 and row[0] == 0:
        send_alert('WARNING', 'RG: 0 approvals in 6H',
            {'approved': 0, 'total': row[1]}, 'neo_monitor')
    log.info(f'RG last 6H: {row[0]} approved / {row[1]} total')

    cur.close()


# --- TRADE EXECUTION HEALTH ---
def check_trade_execution(conn):
    cur = conn.cursor()

    # Duplicate open positions — same instrument open twice
    cur.execute("""
        SELECT instrument, COUNT(*)
        FROM forex_network.trades
        WHERE exit_time IS NULL AND strategy = 'v1_swing'
        GROUP BY instrument
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    for instrument, count in dupes:
        msg = f'{count} open rows for same instrument — possible double-entry'
        send_alert('CRITICAL', f'Duplicate position: {instrument}',
            {'instrument': instrument, 'count': count}, 'neo_monitor')
        write_system_alert(conn, 'critical', f'Duplicate position: {instrument}', msg)

    # Open count for logging
    cur.execute("""
        SELECT COUNT(*) FROM forex_network.trades
        WHERE exit_time IS NULL AND strategy = 'v1_swing'
    """)
    open_count = cur.fetchone()[0]
    log.info(f'Open V1 Swing positions: {open_count}')

    # Stale open positions — open > 7 days
    cur.execute("""
        SELECT instrument, entry_time
        FROM forex_network.trades
        WHERE exit_time IS NULL
        AND strategy = 'v1_swing'
        AND entry_time < NOW() - INTERVAL '7 days'
    """)
    for instrument, entry_time in cur.fetchall():
        send_alert('WARNING', f'Stale position: {instrument}',
            {'instrument': instrument, 'entry_time': str(entry_time)}, 'neo_monitor')

    # Last trade activity
    cur.execute("SELECT MAX(created_at) FROM forex_network.trades WHERE strategy = 'v1_swing'")
    last_trade = cur.fetchone()[0]
    if last_trade:
        age_h = (datetime.now(timezone.utc) - last_trade).total_seconds() / 3600
        log.info(f'Last V1 Swing trade: {age_h:.1f}h ago')

    cur.close()


# --- DATA FRESHNESS ---
def check_data_freshness(conn):
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    # price_metrics freshness
    cur.execute("""
        SELECT timeframe, MAX(updated_at)
        FROM forex_network.price_metrics
        GROUP BY timeframe
    """)
    for tf, last in cur.fetchall():
        if last:
            age_h = (now - last).total_seconds() / 3600
            if tf == '4H' and age_h > 2 and is_market_hours():
                send_alert('WARNING', f'price_metrics 4H stale: {age_h:.1f}h',
                    {'timeframe': '4H', 'age_h': round(age_h, 1)}, 'neo_monitor')
            elif tf == '1D' and age_h > 26:
                send_alert('WARNING', f'price_metrics 1D stale: {age_h:.1f}h',
                    {'timeframe': '1D', 'age_h': round(age_h, 1)}, 'neo_monitor')
            log.info(f'price_metrics {tf}: {age_h:.1f}h old')

    # Correlation matrix
    cur.execute("SELECT MAX(updated_at) FROM shared.portfolio_correlation")
    last = cur.fetchone()[0]
    if last:
        age_h = (now - last).total_seconds() / 3600
        if age_h > 26:
            send_alert('WARNING', f'Correlation matrix stale: {age_h:.1f}h',
                {'age_h': round(age_h, 1)}, 'neo_monitor')
        log.info(f'portfolio_correlation: {age_h:.1f}h old')

    # Economic calendar — next 7 days
    cur.execute("""
        SELECT COUNT(*) FROM forex_network.economic_releases
        WHERE event_time > NOW()
        AND event_time < NOW() + INTERVAL '7 days'
        AND impact = 'high'
    """)
    upcoming = cur.fetchone()[0]
    if upcoming == 0:
        msg = 'RG news blackout checks will pass everything — calendar ingest may have failed'
        send_alert('CRITICAL', 'Economic calendar empty for next 7 days',
            {'high_impact_events': 0}, 'neo_monitor')
        write_system_alert(conn, 'critical', 'Economic calendar empty for next 7 days', msg)
    log.info(f'High-impact events next 7 days: {upcoming}')

    # Swap rates completeness
    cur.execute("SELECT COUNT(DISTINCT instrument) FROM forex_network.swap_rates")
    swap_count = cur.fetchone()[0]
    if swap_count < 22:
        send_alert('WARNING', f'swap_rates: {swap_count}/22 pairs',
            {'count': swap_count, 'expected': 22}, 'neo_monitor')
    log.info(f'swap_rates: {swap_count}/22')

    cur.close()


# --- INFRASTRUCTURE ---
def check_infrastructure(conn):
    # Disk space
    result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True)
    lines = result.stdout.strip().split('\n')
    if len(lines) > 1:
        usage = int(lines[1].split()[4].replace('%', ''))
        if usage > 90:
            msg = 'EC2 root volume nearly full — agents will crash on log write'
            send_alert('CRITICAL', f'Disk critical: {usage}% used',
                {'usage_pct': usage}, 'neo_monitor')
            write_system_alert(conn, 'critical', f'Disk critical: {usage}% used', msg)
        elif usage > 80:
            send_alert('WARNING', f'Disk high: {usage}% used',
                {'usage_pct': usage}, 'neo_monitor')
        log.info(f'Disk usage: {usage}%')

    # RDS connection count
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pg_stat_activity
        WHERE datname = current_database()
    """)
    active = cur.fetchone()[0]
    if active > 80:
        msg = 'Connection pool near exhaustion — check for leaks'
        send_alert('CRITICAL', f'RDS connections critical: {active}',
            {'connections': active}, 'neo_monitor')
        write_system_alert(conn, 'critical', f'RDS connections critical: {active}', msg)
    elif active > 60:
        send_alert('WARNING', f'RDS connections high: {active}',
            {'connections': active}, 'neo_monitor')
    log.info(f'RDS connections: {active}')
    cur.close()

    # Stale file locks
    lock_dir = Path('/root/Project_Neo_Damon/.neo_locks')
    if lock_dir.exists():
        for lock_file in lock_dir.glob('*.lock'):
            age_min = (time.time() - lock_file.stat().st_mtime) / 60
            if age_min > 60:
                try:
                    lock_data = json.loads(lock_file.read_text())
                    send_alert('WARNING', f'Stale lock: {lock_file.stem}',
                        {'cli_id': lock_data.get('cli_id'), 'age_min': round(age_min)}, 'neo_monitor')
                except Exception:
                    send_alert('WARNING', f'Stale lock: {lock_file.stem}',
                        {'age_min': round(age_min)}, 'neo_monitor')
                log.warning(f'Stale lock: {lock_file.name} ({age_min:.0f} min)')


# --- DRAWDOWN HALTS ---
def check_halt_status(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT halt_reason, halt_until, event_time
            FROM forex_network.system_events
            WHERE halt_until > NOW()
            ORDER BY event_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            msg = f'Trading halted until {row[1]}'
            send_alert('CRITICAL', f'Active halt: {row[0]}',
                {'reason': row[0], 'until': str(row[1])}, 'neo_monitor')
            write_system_alert(conn, 'critical', f'Active halt: {row[0]}', msg)
            log.warning(f'Active halt: {row[0]} until {row[1]}')
        else:
            log.info('No active halts')
    except Exception as e:
        log.warning(f'halt_status check skipped: {e}')
    cur.close()


def run_all_checks():
    log.info('=== Health check cycle start ===')
    conn = None
    try:
        conn = get_db_conn()
        check_services(conn)
        check_signal_flow(conn)
        check_trade_execution(conn)
        check_data_freshness(conn)
        check_infrastructure(conn)
        check_halt_status(conn)
    except Exception as e:
        send_alert('CRITICAL', 'Monitor cycle failed', {'error': str(e)}, 'neo_monitor')
        log.error(f'Monitor cycle error: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
    log.info('=== Health check cycle complete ===')


if __name__ == '__main__':
    log.info('Neo monitor daemon starting')
    while True:
        run_all_checks()
        time.sleep(CHECK_INTERVAL)
