"""
Project Neo — Daily Summary & Weekly Report Lambda v1.0
========================================================
Generates end-of-day performance summaries and weekly learning reviews.

Triggers (EventBridge):
  Daily summary:   22:00 UTC every day       → forex_network.daily_summaries
  Weekly report:   06:00 UTC every Monday    → forex_network.weekly_reports

Both summaries are:
  - Stored in RDS (readable by admin dashboard Reports tab)
  - Emailed via SES (same template as critical alerter)

Secrets required:
  platform/rds/credentials      → {"username": "X", "password": "Y"}
  platform/alerts/email         → {"recipient": "...", "sender": "..."}

Parameters required:
  /platform/config/rds-endpoint
"""

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
import psycopg2
import psycopg2.extras

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

AWS_REGION   = os.environ.get("AWS_REGION", "eu-west-2")
DASHBOARD_URL = "https://d2x7fli919yf4c.cloudfront.net"


# ── AWS helpers ───────────────────────────────────────────────────────────────

def get_secret(name: str) -> dict:
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=name)
    try:
        return json.loads(response.get("SecretString", "{}"))
    except json.JSONDecodeError:
        return {}


def get_parameter(name: str) -> str:
    client = boto3.client("ssm", region_name=AWS_REGION)
    return client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]


def get_db_connection(endpoint: str, creds: dict):
    return psycopg2.connect(
        host=endpoint, port=5432, database="postgres",
        user=creds["username"], password=creds["password"],
        sslmode="require", connect_timeout=15,
    )


# ── Daily summary computation ─────────────────────────────────────────────────

def compute_daily_summary(conn, summary_date: date) -> dict:
    """
    Compute the full daily summary for summary_date.
    Queries trades, signals, stress scores, agent events.
    """
    day_start = datetime.combine(summary_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    day_end   = day_start + timedelta(days=1)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # ── Closed trades for the day ──────────────────────────────────────────
        cur.execute("""
            SELECT
                user_id::text,
                instrument,
                direction,
                pnl,
                position_size_usd,
                convergence_score,
                agents_agreed,
                regime_at_entry,
                session_at_entry,
                entry_time,
                exit_time,
                exit_reason
            FROM forex_network.trades
            WHERE exit_time >= %s AND exit_time < %s
            ORDER BY exit_time
        """, (day_start, day_end))
        trades = cur.fetchall()

        # ── Signals generated ─────────────────────────────────────────────────
        cur.execute("""
            SELECT COUNT(*) FROM forex_network.agent_signals
            WHERE created_at >= CURRENT_DATE
        """)
        signals_generated = cur.fetchone()["count"]

        # ── V1 Swing health: open positions ──────────────────────────────────
        # market_context_snapshots no longer populated (regime agent decommissioned)
        cur.execute("""
            SELECT COUNT(*) AS open_count
            FROM forex_network.trades
            WHERE exit_time IS NULL
        """)
        _open_pos = cur.fetchone()
        stress = None  # schema fields preserved as 0 below

        # ── Circuit breakers ──────────────────────────────────────────────────
        cur.execute("""
            SELECT COUNT(*) FROM forex_network.system_alerts
            WHERE alert_type = 'circuit_breaker_user'
              AND created_at >= %s AND created_at < %s
        """, (day_start, day_end))
        circuit_breakers = cur.fetchone()["count"]

        # ── Agent degradations ────────────────────────────────────────────────
        cur.execute("""
            SELECT COUNT(*) FROM forex_network.system_alerts
            WHERE alert_type = 'agent_degraded'
              AND created_at >= %s AND created_at < %s
        """, (day_start, day_end))
        degradations = cur.fetchone()["count"]

        # ── Pattern memory changes ─────────────────────────────────────────────
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE active = TRUE  AND last_updated >= %s) AS activated,
                COUNT(*) FILTER (WHERE active = FALSE AND last_updated >= %s) AS deactivated
            FROM forex_network.pattern_memory
            WHERE created_at >= '2026-04-21 18:15:00'
        """, (day_start, day_start))
        patterns = cur.fetchone()

        # ── Performance metrics (rolling 30 trades at day end) ─────────────────
        cur.execute("""
            SELECT sortino_ratio, sharpe_ratio
            FROM forex_network.performance_risk_adjusted
            LIMIT 1
        """)
        perf = cur.fetchone() or {}

    # ── Compute aggregates ────────────────────────────────────────────────────
    net_pnl       = sum(t["pnl"] or 0 for t in trades)
    gross_wins    = sum(t["pnl"] for t in trades if (t["pnl"] or 0) > 0)
    gross_losses  = abs(sum(t["pnl"] for t in trades if (t["pnl"] or 0) < 0))
    win_count     = sum(1 for t in trades if (t["pnl"] or 0) > 0)
    win_rate      = win_count / len(trades) if trades else None
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else None

    # Per-user breakdown
    users = {}
    for t in trades:
        uid = t["user_id"]
        if uid not in users:
            users[uid] = {"user": uid, "trades": 0, "pnl": 0.0, "wins": 0}
        users[uid]["trades"] += 1
        users[uid]["pnl"]    += float(t["pnl"] or 0)
        if (t["pnl"] or 0) > 0:
            users[uid]["wins"] += 1
    for u in users.values():
        u["win_rate"] = u["wins"] / u["trades"] if u["trades"] > 0 else 0

    # Best / worst trade
    _non_zero = [t for t in trades if t["pnl"] is not None and t["pnl"] != 0]
    best  = max(_non_zero, key=lambda t: t["pnl"]) if _non_zero else None
    worst = min(_non_zero, key=lambda t: t["pnl"]) if _non_zero else None

    # Top pairs by trade count
    pair_counts = {}
    for t in trades:
        pair_counts[t["instrument"]] = pair_counts.get(t["instrument"], 0) + 1
    top_pairs = sorted(pair_counts, key=pair_counts.get, reverse=True)[:3]

    # Best session
    session_counts = {}
    for t in trades:
        s = t["session_at_entry"]
        if s:
            session_counts[s] = session_counts.get(s, 0) + 1
    top_session = max(session_counts, key=session_counts.get) if session_counts else None

    # Capital deployed estimate — total position size as % of daily risk budget
    # Approximate: sum of position sizes / (3 users × £25k × 3% daily limit)
    total_deployed = sum(float(t["position_size_usd"] or 0) for t in trades)
    estimated_budget = 3 * 25000 * 0.03  # rough estimate
    capital_deployed_pct = min((total_deployed / estimated_budget) * 100, 100.0) if estimated_budget > 0 else 0

    # V1 Swing: stress monitoring decommissioned; preserve DB schema fields as 0
    stress_min = 0
    stress_max = 0
    open_positions_now = int(_open_pos["open_count"]) if _open_pos else 0
    stress_state_label = "V1 Swing"

    return {
        "summary_date":          summary_date.isoformat(),
        "trades_executed":       len(trades),
        "signals_generated":     int(signals_generated),
        "opportunities_missed":  0,  # TODO: compute from blocked signals
        "net_pnl":               round(net_pnl, 2),
        "gross_wins":            round(gross_wins, 2),
        "gross_losses":          round(gross_losses, 2),
        "win_rate":              round(win_rate, 4) if win_rate is not None else None,
        "profit_factor":         round(profit_factor, 4) if profit_factor else None,
        "sortino_rolling":       float(perf.get("sortino_ratio") or 0),
        "sharpe_rolling":        float(perf.get("sharpe_ratio") or 0),
        "capital_deployed_pct":  round(capital_deployed_pct, 1),
        "active_users":          len(users),
        "circuit_breakers_fired": int(circuit_breakers),
        "agent_degradations":    int(degradations),
        "data_issues":           0,
        "stress_score_min":      0,   # regime agent decommissioned
        "stress_score_max":      0,
        "stress_score_avg":      0,
        "open_positions_count":  open_positions_now,
        "patterns_activated":    int(patterns["activated"]) if patterns else 0,
        "patterns_deactivated":  int(patterns["deactivated"]) if patterns else 0,
        "detail": {
            "by_user":    list(users.values()),
            "best_trade": {
                "user": best["user_id"], "instrument": best["instrument"],
                "direction": best["direction"], "pnl": float(best["pnl"] or 0)
            } if best else None,
            "worst_trade": {
                "user": worst["user_id"], "instrument": worst["instrument"],
                "direction": worst["direction"], "pnl": float(worst["pnl"] or 0)
            } if worst else None,
            "top_pairs":        top_pairs,
            "top_session":      top_session,
            "stress_state":     stress_state_label,
            "open_positions":   open_positions_now,
        },
    }


# ── Email body builders ────────────────────────────────────────────────────────

def build_daily_email(summary: dict) -> tuple[str, str]:
    d = summary
    fmt = lambda n: f"{'£' if n>=0 else '-£'}{abs(n):,.2f}" if n is not None else "—"
    pct = lambda n: f"{n*100:.1f}%" if n is not None else "—"

    subject = (
        f"📊 Project Neo — Daily Summary {d['summary_date']} | "
        f"P&L: {fmt(d.get('net_pnl',0))} | "
        f"{d.get('trades_executed',0)} trades"
    )

    detail = d.get("detail", {})
    by_user = detail.get("by_user", [])

    user_lines = "\n".join(
        f"  {u['user']:<18} {fmt(u['pnl']):<12} {u['trades']} trade(s)  {pct(u.get('win_rate'))} win"
        for u in by_user
    ) or "  No trades today"

    best  = detail.get("best_trade")
    worst = detail.get("worst_trade")

    body = f"""Project Neo — Daily Summary
============================
Date:     {d['summary_date']}
Generated: {datetime.now(timezone.utc).strftime('%H:%M UTC')}

────────────────────────────
PERFORMANCE
────────────────────────────
Net P&L:           {fmt(d.get('net_pnl'))}
Gross wins:        {fmt(d.get('gross_wins'))}
Gross losses:      {fmt(d.get('gross_losses'))}
Win rate:          {pct(d.get('win_rate'))}
Profit factor:     {f"{d['profit_factor']:.2f}" if d.get('profit_factor') is not None else ('N/A (no losses)' if d.get('trades_executed', 0) > 0 else '—')}
Trades executed:   {d.get('trades_executed', 0)}
Signals generated: {d.get('signals_generated', 0)}

Sortino (rolling 30d): {f"{d['sortino_rolling']:.2f}" if d.get('sortino_rolling') else '—'}
Sharpe (rolling 30d):  {f"{d['sharpe_rolling']:.2f}" if d.get('sharpe_rolling') else '—'}

Capital deployed:  {f"{d.get('capital_deployed_pct', 0):.1f}%" }

────────────────────────────
PER USER
────────────────────────────
{user_lines}

────────────────────────────
HIGHLIGHTS
────────────────────────────
Best trade:   {f"{best['instrument']} {best['direction']} {fmt(best['pnl'])} ({best['user']})" if best else '—'}
Worst trade:  {f"{worst['instrument']} {worst['direction']} {fmt(worst['pnl'])} ({worst['user']})" if worst else '—'}
Top pairs:    {', '.join(detail.get('top_pairs', [])) or '—'}
Best session: {(detail.get('top_session') or 'N/A').title()}

────────────────────────────
SYSTEM
────────────────────────────
Open positions: {detail.get('open_positions', 0)}  ({detail.get('stress_state', 'V1 Swing')})
Circuit bkrs:  {d.get('circuit_breakers_fired', 0)}
Degradations:  {d.get('agent_degradations', 0)}
Patterns +/-:  +{d.get('patterns_activated', 0)} / -{d.get('patterns_deactivated', 0)}

────────────────────────────
Dashboard: {DASHBOARD_URL}

— Project Neo Monitoring
"""
    return subject, body


def build_weekly_email(report: dict) -> tuple[str, str]:
    d = report
    fmt = lambda n: f"{'£' if n>=0 else '-£'}{abs(n):,.2f}" if n is not None else "—"
    pct = lambda n: f"{n*100:.1f}%" if n is not None else "—"

    perf = d.get("performance", {})
    subject = (
        f"📋 Project Neo — Weekly Review w/e {d.get('week_ending')} | "
        f"P&L: {fmt(perf.get('net_pnl'))} | "
        f"Sortino: {perf.get('sortino', 0):.2f}"
    )

    by_user = perf.get("by_user", [])
    user_lines = "\n".join(
        f"  {u['user']:<18} {fmt(u['pnl']):<12} {u['trades']} trades  Sortino {u.get('sortino',0):.2f}"
        for u in by_user
    )

    uncertainty = d.get("uncertainty_events", [])
    uncertainty_lines = "\n".join(
        f"  [{e['rule']}] {e['pair']} — {e['outcome'].upper()}: {e['detail']}"
        for e in uncertainty
    ) or "  None this week"

    recommendations = d.get("rule_recommendations", [])
    rec_lines = "\n".join(f"  → {r}" for r in recommendations) or "  No recommendations"

    stress = d.get("stress_summary", {})
    proposals = d.get("proposals_summary", {})

    body = f"""Project Neo — Weekly Learning Review
=====================================
Week ending: {d.get('week_ending')}
Generated:   {d.get('generated_at', 'Monday 06:00 UTC')}

────────────────────────────
WEEKLY PERFORMANCE
────────────────────────────
Net P&L:         {fmt(perf.get('net_pnl'))}
Trades:          {perf.get('total_trades', 0)}
Win rate:        {pct(perf.get('win_rate'))}
Profit factor:   {f"{perf.get('profit_factor',0):.2f}"}
Sortino ratio:   {f"{perf.get('sortino',0):.2f}"} (target >1.5)
Sharpe ratio:    {f"{perf.get('sharpe',0):.2f}"} (target >1.0)

Per user:
{user_lines}

────────────────────────────
RULE UNCERTAINTY EVENTS
────────────────────────────
{uncertainty_lines}

────────────────────────────
LEARNING MODULE RECOMMENDATIONS
────────────────────────────
{rec_lines}

────────────────────────────
SYSTEM HEALTH
────────────────────────────
Stress avg/peak: {stress.get('avg',0):.1f} / {stress.get('peak',0):.1f} (peak: {stress.get('peak_day','—')})
Proposals:       {sum(v for v in proposals.values() if isinstance(v, int))} total

────────────────────────────
Dashboard: {DASHBOARD_URL}

— Project Neo Monitoring
"""
    return subject, body


# ── Write to RDS ──────────────────────────────────────────────────────────────

def save_daily_summary(conn, summary: dict):
    detail = summary.pop("detail", {})
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forex_network.daily_summaries (
                summary_date, trades_executed, signals_generated,
                opportunities_missed, net_pnl, gross_wins, gross_losses,
                win_rate, profit_factor, sortino_rolling, sharpe_rolling,
                capital_deployed_pct, active_users,
                circuit_breakers_fired, agent_degradations, data_issues,
                stress_score_min, stress_score_max, stress_score_avg,
                patterns_activated, patterns_deactivated, detail
            ) VALUES (
                %(summary_date)s, %(trades_executed)s, %(signals_generated)s,
                %(opportunities_missed)s, %(net_pnl)s, %(gross_wins)s, %(gross_losses)s,
                %(win_rate)s, %(profit_factor)s, %(sortino_rolling)s, %(sharpe_rolling)s,
                %(capital_deployed_pct)s, %(active_users)s,
                %(circuit_breakers_fired)s, %(agent_degradations)s, %(data_issues)s,
                %(stress_score_min)s, %(stress_score_max)s, %(stress_score_avg)s,
                %(patterns_activated)s, %(patterns_deactivated)s, %(detail_json)s
            )
            ON CONFLICT (summary_date) DO UPDATE SET
                trades_executed       = EXCLUDED.trades_executed,
                net_pnl               = EXCLUDED.net_pnl,
                sortino_rolling       = EXCLUDED.sortino_rolling,
                detail                = EXCLUDED.detail,
                generated_at          = NOW()
        """, {**summary, "detail_json": json.dumps(detail)})
    conn.commit()
    summary["detail"] = detail  # restore


def save_weekly_report(conn, report: dict):
    with conn.cursor() as cur:
        perf = report.get("performance", {})
        stress = report.get("stress_summary", {})
        pattern = report.get("pattern_updates", {})
        cur.execute("""
            INSERT INTO forex_network.weekly_reports (
                week_ending, week_starting,
                total_trades, net_pnl, win_rate, profit_factor,
                sortino_ratio, sharpe_ratio,
                stress_score_avg, stress_score_peak, stress_peak_date,
                patterns_activated, patterns_deactivated, patterns_monitoring,
                detail
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (week_ending) DO UPDATE SET
                net_pnl        = EXCLUDED.net_pnl,
                sortino_ratio  = EXCLUDED.sortino_ratio,
                detail         = EXCLUDED.detail,
                generated_at   = NOW()
        """, (
            report.get("week_ending"),
            report.get("week_starting"),
            perf.get("total_trades", 0),
            perf.get("net_pnl"),
            perf.get("win_rate"),
            perf.get("profit_factor"),
            perf.get("sortino"),
            perf.get("sharpe"),
            stress.get("avg"),
            stress.get("peak"),
            stress.get("peak_day"),
            pattern.get("activated", 0),
            pattern.get("deactivated", 0),
            pattern.get("monitoring", 0),
            json.dumps(report),
        ))
    conn.commit()


# ── Send email ────────────────────────────────────────────────────────────────

def send_email(ses_client, sender, recipient, subject, body):
    ses_client.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body":    {"Text": {"Data": body,    "Charset": "UTF-8"}},
        },
    )
    logger.info(f"Email sent: {subject[:60]}")


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Triggered by EventBridge with event source indicating daily or weekly:
      {"source": "daily"}   — fires at 22:00 UTC every day
      {"source": "weekly"}  — fires at 06:00 UTC every Monday

    If no source key, defaults to daily.
    """
    start    = time.time()
    ts_now   = datetime.now(timezone.utc)
    mode     = event.get("source", "daily")
    logger.info(f"Daily summary Lambda starting — mode={mode} — {ts_now.isoformat()}")

    # ── Load credentials ──────────────────────────────────────────────────────
    try:
        rds_creds    = get_secret("platform/rds/credentials")
        email_config = get_secret("platform/alerts/email")
        rds_endpoint = get_parameter("/platform/config/rds-endpoint")
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        return {"statusCode": 500, "body": str(e)}

    # ── Connect to RDS ────────────────────────────────────────────────────────
    try:
        conn = get_db_connection(rds_endpoint, rds_creds)
    except Exception as e:
        logger.error(f"RDS connection failed: {e}")
        return {"statusCode": 500, "body": str(e)}

    ses_client = boto3.client("ses", region_name=AWS_REGION)

    try:
        if mode == "daily":
            # Compute summary for yesterday (Lambda fires at 22:00 for that day)
            summary_date = ts_now.date()
            logger.info(f"Computing daily summary for {summary_date}")

            summary = compute_daily_summary(conn, summary_date)
            save_daily_summary(conn, summary)

            subject, body = build_daily_email(summary)
            send_email(ses_client, email_config["sender"],
                       email_config["recipient"], subject, body)

            # Mark email sent
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE forex_network.daily_summaries
                    SET email_sent_at = NOW()
                    WHERE summary_date = %s
                """, (summary_date,))
            conn.commit()

            logger.info(f"Daily summary complete: {summary['trades_executed']} trades, P&L {summary['net_pnl']}")

        elif mode == "weekly":
            # Compute weekly report for the past week (Mon-Sun)
            today      = ts_now.date()
            week_end   = today - timedelta(days=1)  # Sunday
            week_start = week_end - timedelta(days=6)  # Monday
            logger.info(f"Computing weekly report for {week_start} to {week_end}")

            # Build weekly report from the 7 daily summaries
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM forex_network.daily_summaries
                    WHERE summary_date >= %s AND summary_date <= %s
                    ORDER BY summary_date
                """, (week_start, week_end))
                daily_rows = cur.fetchall()

            # Aggregate
            total_trades = sum(r["trades_executed"] for r in daily_rows)
            total_pnl    = sum(float(r["net_pnl"] or 0) for r in daily_rows)
            all_wins     = sum(float(r["gross_wins"] or 0) for r in daily_rows)
            all_losses   = sum(float(r["gross_losses"] or 0) for r in daily_rows)

            # Fetch current performance ratios
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT sortino_ratio, sharpe_ratio FROM forex_network.performance_risk_adjusted LIMIT 1")
                perf_row = cur.fetchone() or {}

            stress_scores = [float(r["stress_score_avg"]) for r in daily_rows if r["stress_score_avg"]]
            stress_peaks  = [(float(r["stress_score_max"]), r["summary_date"]) for r in daily_rows if r["stress_score_max"]]
            peak_score, peak_date = max(stress_peaks) if stress_peaks else (0, None)

            report = {
                "week_ending":   week_end.isoformat(),
                "week_starting": week_start.isoformat(),
                "generated_at":  ts_now.strftime("%Y-%m-%d %H:%M UTC"),
                "performance": {
                    "total_trades":  total_trades,
                    "net_pnl":       round(total_pnl, 2),
                    "win_rate":      None,
                    "profit_factor": round(all_wins / all_losses, 4) if all_losses > 0 else None,
                    "sortino":       float(perf_row.get("sortino_ratio") or 0),
                    "sharpe":        float(perf_row.get("sharpe_ratio") or 0),
                },
                "stress_summary": {
                    "avg":      round(sum(stress_scores) / len(stress_scores), 2) if stress_scores else 0,
                    "peak":     round(peak_score, 2),
                    "peak_day": str(peak_date) if peak_date else None,
                },
                "pattern_updates": {"activated": 0, "deactivated": 0, "monitoring": 0},
                "uncertainty_events": [],
                "rule_recommendations": [],
                "proposals_summary": {},
            }

            save_weekly_report(conn, report)

            subject, body = build_weekly_email(report)
            send_email(ses_client, email_config["sender"],
                       email_config["recipient"], subject, body)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE forex_network.weekly_reports
                    SET email_sent_at = NOW()
                    WHERE week_ending = %s
                """, (week_end,))
            conn.commit()

            logger.info(f"Weekly report complete: {total_trades} trades, P&L {total_pnl:.2f}")

    except Exception as e:
        logger.error(f"Summary computation failed: {e}", exc_info=True)
        conn.rollback()
        return {"statusCode": 500, "body": str(e)}
    finally:
        conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mode":      mode,
            "elapsed_s": round(time.time() - start, 2),
            "ts":        ts_now.isoformat(),
        })
    }

