#!/usr/bin/env python3
"""
send_daily_lm_summary.py — Daily learning-module intelligence summary.

Runs at 22:00 UTC Mon–Fri via crontab.
Complements the neo-daily-summary-dev Lambda (which handles trade P&L / email).
This script publishes LM-specific findings (shadow trades, proposals,
RG rejections, architecture issues) to neo-trading-alerts-dev SNS topic.
"""

import json
import logging
import sys
import os
from datetime import datetime, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

AWS_REGION = "eu-west-2"
SNS_TOPIC_ARN = "arn:aws:sns:eu-west-2:956177812472:neo-trading-alerts-dev"

# Add project root to path so learning_module is importable
sys.path.insert(0, "/root/Project_Neo_Damon/Learning_Module")


def format_summary(summary: dict, ts: str) -> str:
    lines = [
        "Project Neo — Learning Module Daily Summary",
        "=" * 44,
        f"Generated: {ts}",
        "",
    ]

    # Trades today
    t = summary.get("trades_today", {})
    total = int(t.get("total") or 0)
    winners = int(t.get("winners") or 0)
    pips = float(t.get("total_pips") or 0)
    win_rate = (winners / total * 100) if total > 0 else 0
    lines += [
        "── TRADES TODAY ──────────────────────────",
        f"  Closed:   {total}",
        f"  Winners:  {winners}  ({win_rate:.0f}% win rate)",
        f"  Net pips: {pips:+.1f}",
        "",
    ]

    # Shadow trades by hypothesis
    shadows = summary.get("shadow_trades_today", [])
    if shadows:
        lines.append("── SHADOW TRADES (last 24h) ──────────────")
        for s in shadows[:8]:
            avg = s.get("avg_pips_1d")
            avg_str = f"  avg_pips_1d={avg:+.1f}" if avg is not None else ""
            lines.append(
                f"  {str(s.get('hypothesis') or 'unknown'):<35} "
                f"n={s.get('total', 0):<4} "
                f"with_outcomes={s.get('with_outcomes', 0)}"
                f"{avg_str}"
            )
        lines.append("")

    # RG rejections
    rg = summary.get("rg_rejections_today", [])
    if rg:
        lines.append("── RG REJECTIONS (last 24h) ───────────────")
        for r in rg[:6]:
            reason = str(r.get("rejection_reason") or "")[:60]
            lines.append(f"  {r.get('total', 0):>4}×  {reason}")
        lines.append("")

    # LM proposals
    props = summary.get("proposals_today", [])
    if props:
        lines.append("── LM PROPOSALS ──────────────────────────")
        for p in props[:5]:
            ptype = str(p.get("proposal_type") or "")
            inst = str(p.get("instrument") or "—")
            msg = str(p.get("message") or "")[:80]
            lines.append(f"  [{ptype}] {inst}: {msg}")
        lines.append("")

    # Architecture issues
    arch = summary.get("architecture_issues", [])
    if arch:
        lines.append("── ARCHITECTURE ISSUES ───────────────────")
        for a in arch[:5]:
            ptype = str(a.get("proposal_type") or "")
            inst = str(a.get("instrument") or "—")
            msg = str(a.get("message") or "")[:80]
            lines.append(f"  [{ptype}] {inst}: {msg}")
        lines.append("")

    if not shadows and not rg and not props and not arch and total == 0:
        lines.append("  No activity in the last 24 hours.")
        lines.append("")

    lines.append("─" * 44)
    return "\n".join(lines)


def main():
    ts_now = datetime.now(timezone.utc)
    ts_str = ts_now.strftime("%Y-%m-%d %H:%M UTC")
    logger.info(f"Daily LM summary starting — {ts_str}")

    # ── Load credentials and connect ──────────────────────────────────────────
    try:
        sm  = boto3.client("secretsmanager", region_name=AWS_REGION)
        ssm = boto3.client("ssm", region_name=AWS_REGION)
        rds_endpoint = ssm.get_parameter(
            Name="/platform/config/rds-endpoint"
        )["Parameter"]["Value"]
        creds = json.loads(
            sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"]
        )
    except Exception as e:
        logger.error(f"Credential fetch failed: {e}")
        sys.exit(1)

    # ── Connect to DB and run queries directly ────────────────────────────────
    # Avoids instantiating the full LearningModule (which requires user_id iteration).
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(
            host=rds_endpoint, dbname="postgres",
            user=creds["username"], password=creds["password"],
            port=5432, cursor_factory=psycopg2.extras.RealDictCursor,
        )
        cur = conn.cursor()

        summary: dict = {}

        # Shadow trades last 24h
        cur.execute("""
            SELECT hypothesis,
                   COUNT(*) as total,
                   SUM(CASE WHEN shadow_pips_1d IS NOT NULL THEN 1 ELSE 0 END) as with_outcomes,
                   ROUND(AVG(shadow_pips_1d)::numeric, 2) as avg_pips_1d
            FROM forex_network.shadow_trades
            WHERE signal_time > NOW() - INTERVAL '24 hours'
            GROUP BY hypothesis ORDER BY total DESC
        """)
        summary["shadow_trades_today"] = [dict(r) for r in cur.fetchall()]

        # LM proposals from agent_signals
        cur.execute("""
            SELECT payload, created_at
            FROM forex_network.agent_signals
            WHERE agent_name = 'learning_module'
              AND signal_type = 'learning_review'
              AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC LIMIT 20
        """)
        proposals_today = []
        arch_issues = []
        for row in cur.fetchall():
            payload = row["payload"] or {}
            for p in payload.get("proposals", []):
                ptype = p.get("proposal_type", p.get("type", "unknown"))
                msg = p.get("message", p.get("suggestion", p.get("reasoning", "")))
                entry = {
                    "proposal_type": ptype,
                    "instrument":    p.get("instrument"),
                    "message":       msg,
                    "created_at":    str(row["created_at"]),
                }
                if str(ptype).startswith("architecture_"):
                    arch_issues.append(entry)
                else:
                    proposals_today.append(entry)
        summary["proposals_today"] = proposals_today[:10]
        summary["architecture_issues"] = arch_issues[:5]

        # RG rejections (rejected_signals)
        cur.execute("""
            SELECT rejection_reason, COUNT(*) as total
            FROM forex_network.rejected_signals
            WHERE rejected_at > NOW() - INTERVAL '24 hours'
            GROUP BY rejection_reason ORDER BY total DESC
        """)
        summary["rg_rejections_today"] = [dict(r) for r in cur.fetchall()]

        # Closed trades today
        cur.execute("""
            SELECT COUNT(*) as total,
                   COALESCE(SUM(CASE WHEN pnl_pips > 0 THEN 1 ELSE 0 END), 0) as winners,
                   COALESCE(SUM(pnl_pips), 0) as total_pips
            FROM forex_network.trades
            WHERE entry_time > NOW() - INTERVAL '24 hours'
              AND exit_time IS NOT NULL
        """)
        summary["trades_today"] = dict(cur.fetchone())

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"DB query failed: {e}")
        sys.exit(1)

    # ── Format and publish ────────────────────────────────────────────────────
    body = format_summary(summary, ts_str)
    logger.info(f"Summary:\n{body}")

    trades_today = int((summary.get("trades_today") or {}).get("total") or 0)
    n_proposals  = len(summary.get("proposals_today", []))
    n_arch       = len(summary.get("architecture_issues", []))
    subject = (
        f"Neo LM Daily | {ts_now.strftime('%Y-%m-%d')} | "
        f"{trades_today} trade(s) | "
        f"{n_proposals} proposal(s) | "
        f"{n_arch} arch issue(s)"
    )

    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=body,
        )
        logger.info(f"SNS published: MessageId={response['MessageId']}")
    except Exception as e:
        logger.error(f"SNS publish failed: {e}")
        sys.exit(1)

    # ── Also invoke the performance Lambda to ensure it fires ─────────────────
    try:
        lam = boto3.client("lambda", region_name=AWS_REGION)
        lam.invoke(
            FunctionName="neo-daily-summary-dev",
            InvocationType="Event",          # async — don't wait
            Payload=json.dumps({"source": "daily"}),
        )
        logger.info("Invoked neo-daily-summary-dev Lambda (async)")
    except Exception as e:
        logger.warning(f"Lambda invoke failed (non-fatal): {e}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
