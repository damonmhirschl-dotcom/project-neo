"""
Project Neo — Critical Alert Lambda v1.0
=========================================
Checks system_stress_alerts and system_alerts every 5 minutes.
Sends email (SES) and/or SMS (SNS) for qualifying critical events.

Alert triggers:
  Pre-crisis escalation    → email only
  Crisis escalation        → email + SMS
  Crisis sustained 3+ cy.  → SMS reminder only
  Circuit breaker (user)   → email only
  Technical agent dead     → email only
  IBKR connection lost     → email only
  Kill switch proposal     → email + SMS

Everything else goes to admin dashboard only. No out-of-band notification.

AWS services used:
  SES  — email via platform/alerts/email secret
  SNS  — SMS via platform/alerts/sms secret

Prerequisites:
  - SES sender email verified in eu-west-2
  - SNS topic neo-critical-alerts-dev created with SMS subscription
  - platform/alerts/email and platform/alerts/sms in Secrets Manager
  - alerted_at columns added by migrate_alerting_v1.sql
  - platform-monitoring-role-dev has ses:SendEmail and sns:Publish permissions

EventBridge trigger: rate(5 minutes)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import psycopg2

# ── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")

# Admin dashboard URL — included in every alert
DASHBOARD_URL = "https://d2x7fli919yf4c.cloudfront.net"

# Kill switch commands — included in every Crisis email for immediate action
KILL_SWITCH_CMD = """To activate kill switch (halts all new positions immediately):
  aws ssm put-parameter \\
    --name /platform/config/kill-switch \\
    --value active --overwrite --region eu-west-2

To deactivate (resumes normal operation):
  aws ssm put-parameter \\
    --name /platform/config/kill-switch \\
    --value inactive --overwrite --region eu-west-2"""


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
    response = client.get_parameter(Name=name, WithDecryption=True)
    return response["Parameter"]["Value"]


def get_db_connection(endpoint: str, credentials: dict):
    return psycopg2.connect(
        host=endpoint,
        port=5432,
        database="postgres",
        user=credentials["username"],
        password=credentials["password"],
        sslmode="require",
        connect_timeout=10,
    )


# ── SES email ─────────────────────────────────────────────────────────────────

def send_email(ses_client, sender: str, recipient: str,
               subject: str, body_text: str, body_html: str = None):
    """Send email via SES. Falls back to text-only if HTML fails."""
    message = {
        "Subject": {"Data": subject, "Charset": "UTF-8"},
        "Body": {"Text": {"Data": body_text, "Charset": "UTF-8"}},
    }
    if body_html:
        message["Body"]["Html"] = {"Data": body_html, "Charset": "UTF-8"}

    ses_client.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message=message,
    )
    logger.info(f"Email sent to {recipient}: {subject}")


# ── SNS SMS ───────────────────────────────────────────────────────────────────

def send_sms(sns_client, topic_arn: str, message: str):
    """Publish SMS via SNS topic. Transactional type for high priority."""
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        MessageAttributes={
            "AWS.SNS.SMS.SMSType": {
                "DataType": "String",
                "StringValue": "Transactional",
            },
            "AWS.SNS.SMS.SenderID": {
                "DataType": "String",
                "StringValue": "ALGODESK",
            },
        },
    )
    logger.info(f"SMS published to topic {topic_arn}")


# ── Component bar renderer ─────────────────────────────────────────────────────

def render_component_bar(value: float, width: int = 9) -> str:
    """Render a simple ASCII progress bar for a 0.0-1.0 value."""
    filled = round(value * width)
    return "█" * filled + "░" * (width - filled)


def format_components(components: dict) -> str:
    """Format stress components as aligned text block."""
    if not components:
        return "  (component data unavailable)"
    labels = {
        "vix":           "VIX:             ",
        "yield_curve":   "Yield curve:     ",
        "vol_divergence":"Vol divergence:  ",
        "correlation":   "Correlation:     ",
        "cot":           "COT extremes:    ",
        "geopolitical":  "Geopolitical:    ",
        "order_flow":    "Order flow:      ",
    }
    lines = []
    for key, label in labels.items():
        val = components.get(key, 0.0)
        try:
            val = float(val)
        except (TypeError, ValueError):
            val = 0.0
        lines.append(f"  {label}{val:.2f}  {render_component_bar(val)}")
    return "\n".join(lines)


# ── Open positions query ──────────────────────────────────────────────────────

def get_open_positions(conn) -> str:
    """Return a brief summary of open positions for inclusion in alerts."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT instrument, direction, entry_price, position_size_usd
                FROM forex_network.trades
                WHERE exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
        if not rows:
            return "None"
        parts = [
            f"{r[0]} {r[1]} (${r[3]:,.0f})" if r[3] else f"{r[0]} {r[1]}"
            for r in rows
        ]
        return ", ".join(parts)
    except Exception as e:
        logger.warning(f"Could not fetch open positions: {e}")
        return "Unknown"


# ── Email body builders ────────────────────────────────────────────────────────

def build_precrisis_email(alert: dict, open_positions: str) -> tuple[str, str]:
    """Build Pre-crisis email subject and body."""
    score      = alert.get("stress_score", 0)
    prev_state = alert.get("previous_state", "Elevated")
    dominant   = alert.get("dominant_component", "unknown")
    ts         = alert.get("ts", datetime.now(timezone.utc))
    components = alert.get("components") or {}

    subject = f"⚠️ Project Neo — Pre-Crisis Alert [Score: {score:.0f}]"

    body = f"""Project Neo — Pre-Crisis Stress Alert
======================================

System stress score has escalated to Pre-Crisis level.

Score:           {score:.1f} / 100
State:           Pre-crisis (was: {prev_state})
Dominant driver: {dominant.replace('_', ' ').title()}
Time:            {ts.strftime('%d %B %Y, %H:%M UTC') if hasattr(ts, 'strftime') else ts}

Automatic response applied:
  - Convergence threshold raised +0.15
  - Position sizing reduced to 0.25×
  - No new entries permitted
  - Existing positions managed with tighter trailing stops (×0.70)

Open positions: {open_positions}

Stress components:
{format_components(components)}

Review dashboard: {DASHBOARD_URL}

No immediate action required. The system is operating within its
designed Pre-crisis parameters. Monitor stress score trend.

If score continues rising above 85, you will receive a Crisis alert
with kill switch recommendation.

— Project Neo Monitoring
"""
    return subject, body


def build_crisis_email(alert: dict, open_positions: str) -> tuple[str, str]:
    """Build Crisis email subject and body — includes kill switch command."""
    score      = alert.get("stress_score", 0)
    prev_state = alert.get("previous_state", "Pre-crisis")
    dominant   = alert.get("dominant_component", "unknown")
    ts         = alert.get("ts", datetime.now(timezone.utc))
    components = alert.get("components") or {}

    subject = f"🔴 Project Neo — CRISIS ALERT [Score: {score:.0f}] — Kill Switch Review Required"

    body = f"""Project Neo — CRISIS Stress Alert
===================================

System stress score has entered Crisis level.
Kill switch review is recommended.

Score:           {score:.1f} / 100
State:           CRISIS (was: {prev_state})
Dominant driver: {dominant.replace('_', ' ').title()}
Time:            {ts.strftime('%d %B %Y, %H:%M UTC') if hasattr(ts, 'strftime') else ts}

Automatic response applied:
  - ALL new entries suspended across all users
  - Kill switch proposal written to admin dashboard
  - Existing positions managed with tighter trailing stops (×0.70)

Open positions: {open_positions}

Stress components:
{format_components(components)}

REVIEW DASHBOARD:
{DASHBOARD_URL}

KILL SWITCH COMMANDS:
{KILL_SWITCH_CMD}

— Project Neo Monitoring
"""
    return subject, body


def build_circuit_breaker_email(alert: dict) -> tuple[str, str]:
    """Build circuit breaker email."""
    detail     = alert.get("detail", "")
    ts         = alert.get("created_at", datetime.now(timezone.utc))
    alert_type = alert.get("alert_type", "circuit_breaker_user")

    subject = f"⚠️ Project Neo — Circuit Breaker Activated"
    body = f"""Project Neo — Circuit Breaker Alert
=====================================

A circuit breaker has been activated.

Type:   {alert_type.replace('_', ' ').title()}
Detail: {detail}
Time:   {ts.strftime('%d %B %Y, %H:%M UTC') if hasattr(ts, 'strftime') else ts}

The affected user's daily loss limit has been reached.
No new positions will be opened for that user today.
Existing positions continue to be managed normally.

Review dashboard: {DASHBOARD_URL}

— Project Neo Monitoring
"""
    return subject, body


def build_agent_dead_email(alert: dict) -> tuple[str, str]:
    """Build technical agent dead email."""
    detail = alert.get("detail", "")
    ts     = alert.get("created_at", datetime.now(timezone.utc))

    agent_label = (detail or alert.get("title", "unknown")).split()[0].title()
    subject = f"🔴 Project Neo — Agent Degraded: {agent_label} — Check Required"
    body = f"""Project Neo — Technical Agent Dead
=====================================

The technical agent has stopped responding.
Trading has been automatically halted (no new entries).

Detail: {detail}
Time:   {ts.strftime('%d %B %Y, %H:%M UTC') if hasattr(ts, 'strftime') else ts}

Trading will remain halted until the technical agent reconnects
and resumes heartbeating. Check EC2 instance and agent logs.

Review dashboard: {DASHBOARD_URL}

— Project Neo Monitoring
"""
    return subject, body


def build_ibkr_lost_email(alert: dict) -> tuple[str, str]:
    """Build IBKR connection lost email."""
    detail = alert.get("detail", "")
    ts     = alert.get("created_at", datetime.now(timezone.utc))

    subject = "🔴 Project Neo — IBKR Connection Lost"
    body = f"""Project Neo — IBKR Connection Lost
=====================================

IB Gateway has been unreachable for 2 or more consecutive cycles.
No new orders can be submitted. Open positions cannot be managed.

Detail: {detail}
Time:   {ts.strftime('%d %B %Y, %H:%M UTC') if hasattr(ts, 'strftime') else ts}

Check ECS cluster dev-ibgateway-cluster.
Restart IB Gateway task if needed.

Review dashboard: {DASHBOARD_URL}

— Project Neo Monitoring
"""
    return subject, body


# ── SMS body builders ──────────────────────────────────────────────────────────

def build_crisis_sms(score: float) -> str:
    return (
        f"NEO CRISIS ALERT: Stress {score:.0f}/100. "
        f"New entries suspended. Kill switch review needed. "
        f"Check email + dashboard immediately."
    )


def build_crisis_sustained_sms(score: float) -> str:
    return (
        f"NEO: Crisis stress sustained at {score:.0f}/100 for 45+ min. "
        f"Kill switch still not activated. Check dashboard."
    )


def build_kill_switch_sms(score: float) -> str:
    return (
        f"NEO KILL SWITCH PROPOSAL: Stress {score:.0f}/100. "
        f"System requests kill switch review. Check email immediately."
    )


# ── Alert processing ───────────────────────────────────────────────────────────

def process_stress_alerts(conn, ses_client, sns_client,
                           email_config: dict, sms_config: dict):
    """
    Check system_stress_alerts for unalerted Pre-crisis / Crisis events.
    Fire appropriate notifications and mark as alerted.
    """
    sent = 0

    with conn.cursor() as cur:
        # Fetch unalerted stress escalation events
        cur.execute("""
            SELECT id, stress_score, stress_state, previous_state,
                   transition_type, dominant_component, components,
                   proposal_text, ts
            FROM forex_network.system_stress_alerts
            WHERE alerted_at IS NULL
              AND stress_state IN ('Pre-crisis', 'Crisis')
              AND transition_type = 'escalation'
            ORDER BY ts ASC
            LIMIT 10
        """)
        alerts = cur.fetchall()

    for row in alerts:
        (alert_id, score, state, prev_state, transition,
         dominant, components_raw, proposal, ts) = row

        # Parse components JSONB
        if isinstance(components_raw, str):
            try:
                components = json.loads(components_raw)
            except Exception:
                components = {}
        else:
            components = components_raw or {}

        alert = {
            "stress_score": float(score) if score else 0,
            "stress_state": state,
            "previous_state": prev_state,
            "dominant_component": dominant or "unknown",
            "components": components,
            "ts": ts,
        }

        open_pos = get_open_positions(conn)
        channels_used = []

        try:
            if state == "Pre-crisis":
                subject, body = build_precrisis_email(alert, open_pos)
                send_email(ses_client, email_config["sender"],
                           email_config["recipient"], subject, body)
                channels_used.append("email")

            elif state == "Crisis":
                # Email
                subject, body = build_crisis_email(alert, open_pos)
                send_email(ses_client, email_config["sender"],
                           email_config["recipient"], subject, body)
                channels_used.append("email")

                # SMS
                sms_text = build_crisis_sms(alert["stress_score"])
                send_sms(sns_client, sms_config["sns_topic_arn"], sms_text)
                channels_used.append("sms")

            # Mark as alerted
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE forex_network.system_stress_alerts
                    SET alerted_at     = NOW(),
                        alert_channels = %s
                    WHERE id = %s
                """, ("+".join(channels_used), alert_id))
            conn.commit()
            sent += 1
            logger.info(f"Stress alert {alert_id} ({state}) sent via {channels_used}")

        except Exception as e:
            logger.error(f"Failed to send stress alert {alert_id}: {e}")
            conn.rollback()

    # Check for sustained crisis — score > 85 for 3+ consecutive cycles (45 min)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), MAX(stress_score)
                FROM forex_network.system_stress_alerts
                WHERE stress_state = 'Crisis'
                  AND ts >= NOW() - INTERVAL '50 minutes'
            """)
            row = cur.fetchone()
            if row and row[0] >= 3:
                # Check if we already sent a sustained reminder in the last hour
                cur.execute("""
                    SELECT COUNT(*) FROM forex_network.system_stress_alerts
                    WHERE stress_state = 'Crisis'
                      AND alert_channels LIKE '%sms%'
                      AND alerted_at >= NOW() - INTERVAL '60 minutes'
                      AND transition_type = 'sustained'
                """)
                already_reminded = cur.fetchone()[0] > 0

                if not already_reminded:
                    max_score = float(row[1]) if row[1] else 0
                    sms_text = build_crisis_sustained_sms(max_score)
                    send_sms(sns_client, sms_config["sns_topic_arn"], sms_text)
                    logger.info(f"Crisis sustained SMS sent (score {max_score:.0f})")
                    sent += 1
    except Exception as e:
        logger.error(f"Error checking sustained crisis: {e}")

    return sent


def process_system_alerts(conn, ses_client, sns_client,
                           email_config: dict, sms_config: dict):
    """
    Check system_alerts for unalerted qualifying events.
    Qualifying types: circuit_breaker_user, agent_dead (technical only),
    ibkr_connection_lost.
    """
    sent = 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, alert_type, severity, title, detail, created_at
            FROM forex_network.system_alerts
            WHERE alerted_at IS NULL
              AND alert_type IN (
                  'circuit_breaker_sys',
                  'agent_degraded',
                  'connection_drop'
              )
            ORDER BY created_at ASC
            LIMIT 10
        """)
        alerts = cur.fetchall()

    for row in alerts:
        alert_id, alert_type, severity, title, detail, created_at = row
        alert = {
            "alert_type":  alert_type,
            "severity":    severity,
            "title":       title,
            "detail":      detail or title,
            "created_at":  created_at,
        }

        # all agent_degraded events fire — any agent offline is critical

        channels_used = []
        try:
            if alert_type == "circuit_breaker_user":
                subject, body = build_circuit_breaker_email(alert)
                send_email(ses_client, email_config["sender"],
                           email_config["recipient"], subject, body)
                channels_used.append("email")

            elif alert_type == "agent_degraded":
                subject, body = build_agent_dead_email(alert)
                send_email(ses_client, email_config["sender"],
                           email_config["recipient"], subject, body)
                channels_used.append("email")

            elif alert_type == "connection_drop":
                subject, body = build_ibkr_lost_email(alert)
                send_email(ses_client, email_config["sender"],
                           email_config["recipient"], subject, body)
                channels_used.append("email")

            if channels_used:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE forex_network.system_alerts
                        SET alerted_at     = NOW(),
                            alert_channels = %s
                        WHERE id = %s
                    """, ("+".join(channels_used), alert_id))
                conn.commit()
                sent += 1
                logger.info(f"System alert {alert_id} ({alert_type}) sent via {channels_used}")

        except Exception as e:
            logger.error(f"Failed to send system alert {alert_id}: {e}")
            conn.rollback()

    return sent




# ── Kill switch check ─────────────────────────────────────────────────────────

def process_kill_switch(conn, ses_client, sns_client,
                         email_config: dict, sms_config: dict) -> int:
    """Alert when kill switch is activated. Deduplicates within 4 hours."""
    try:
        kill_switch = get_parameter("/platform/config/kill-switch")
    except Exception as e:
        logger.warning(f"Could not read kill switch state: {e}")
        return 0

    if kill_switch != "active":
        return 0

    # Dedup: have we already fired a kill_switch_activated alert in the last 4h?
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM forex_network.system_alerts
            WHERE alert_type = 'kill_switch_activated'
              AND alerted_at IS NOT NULL
              AND alerted_at > NOW() - INTERVAL '4 hours'
        """)
        if cur.fetchone():
            return 0

    subject = "\U0001f534 Project Neo \u2014 Kill Switch ACTIVE \u2014 All New Entries Halted"
    body = f"""Project Neo \u2014 Kill Switch Activated
======================================

The system kill switch has been set to ACTIVE.
All new position entries are halted immediately.
Existing positions continue to be managed normally.

Time: {datetime.now(timezone.utc).strftime('%d %B %Y, %H:%M UTC')}

Kill switch commands:
{KILL_SWITCH_CMD}

Review dashboard: {DASHBOARD_URL}

\u2014 Project Neo Monitoring
"""
    try:
        send_email(ses_client, email_config["sender"],
                   email_config["recipient"], subject, body)
        sms_text = ("NEO KILL SWITCH ACTIVE: All new entries halted. "
                    "Check email + dashboard immediately.")
        send_sms(sns_client, sms_config["sns_topic_arn"], sms_text)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forex_network.system_alerts
                    (alert_type, severity, title, detail, alerted_at, alert_channels)
                VALUES ('kill_switch_activated', 'CRITICAL',
                        'Kill switch activated', 'Kill switch set to active via SSM',
                        NOW(), 'email+sms')
            """)
        conn.commit()
        logger.info("Kill switch alert sent")
        return 1
    except Exception as e:
        logger.error(f"Failed to send kill switch alert: {e}")
        conn.rollback()
        return 0


# ── Drawdown alerts ───────────────────────────────────────────────────────────

def process_drawdown_alerts(conn, ses_client, sns_client,
                              email_config: dict, sms_config: dict) -> int:
    """Alert when risk guardian logs a DRAWDOWN_LIMIT event. Deduplicates per user per hour."""
    sent = 0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, user_id, message, event_time
            FROM forex_network.system_events
            WHERE event_type = 'DRAWDOWN_LIMIT'
              AND event_time > NOW() - INTERVAL '6 minutes'
            ORDER BY event_time ASC
        """)
        events = cur.fetchall()

    for event_id, user_id, message, event_time in events:
        # Dedup: one drawdown alert per user_id per hour
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM forex_network.system_alerts
                WHERE alert_type = 'drawdown_warning'
                  AND user_id = %s
                  AND alerted_at IS NOT NULL
                  AND alerted_at > NOW() - INTERVAL '1 hour'
            """, (user_id,))
            if cur.fetchone():
                continue

        subject = "\u26a0\ufe0f Project Neo \u2014 Drawdown Limit Approaching"
        body = f"""Project Neo \u2014 Drawdown Warning
================================

The risk guardian has flagged a drawdown limit event.

Detail:  {message}
User ID: {user_id}
Time:    {event_time.strftime('%d %B %Y, %H:%M UTC') if hasattr(event_time, 'strftime') else event_time}

No new positions will be opened for this profile until
drawdown recovers within limits.

Review dashboard: {DASHBOARD_URL}

\u2014 Project Neo Monitoring
"""
        try:
            send_email(ses_client, email_config["sender"],
                       email_config["recipient"], subject, body)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.system_alerts
                        (alert_type, severity, title, detail, user_id, alerted_at, alert_channels)
                    VALUES ('drawdown_warning', 'WARNING',
                            'Drawdown limit reached', %s, %s, NOW(), 'email')
                """, (message, user_id))
            conn.commit()
            logger.info(f"Drawdown alert sent for user {user_id}")
            sent += 1
        except Exception as e:
            logger.error(f"Failed to send drawdown alert: {e}")
            conn.rollback()

    return sent


# ── Stale agent check ─────────────────────────────────────────────────────────

def process_stale_agents(conn, ses_client, sns_client,
                          email_config: dict, sms_config: dict) -> int:
    """Alert when any agent heartbeat goes stale (last_seen > 30 min). Deduplicates per agent per 4 hours."""
    sent = 0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT agent_name, MAX(last_seen) as last_seen
            FROM forex_network.agent_heartbeats
            GROUP BY agent_name
            HAVING MAX(last_seen) < NOW() - INTERVAL '30 minutes'
        """)
        stale = cur.fetchall()

    for agent_name, last_seen in stale:
        # Dedup: one stale alert per agent per 4 hours
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM forex_network.system_alerts
                WHERE alert_type = 'agent_stale'
                  AND detail LIKE %s
                  AND alerted_at IS NOT NULL
                  AND alerted_at > NOW() - INTERVAL '4 hours'
            """, (f"{agent_name}%",))
            if cur.fetchone():
                continue

        minutes_stale = int((datetime.now(timezone.utc) - last_seen).total_seconds() / 60)
        subject = f"\U0001f534 Project Neo \u2014 Agent Stale: {agent_name.title()} ({minutes_stale} min)"
        body = f"""Project Neo \u2014 Agent Stale
==========================

Agent '{agent_name}' has not heartbeated in {minutes_stale} minutes.
Trading signals from this agent are no longer being generated.

Last seen: {last_seen.strftime('%d %B %Y, %H:%M UTC')}
Threshold: 30 minutes

Check EC2 and restart if needed:
  sudo systemctl status neo-{agent_name.replace('_', '-')}-agent
  sudo systemctl restart neo-{agent_name.replace('_', '-')}-agent

Review dashboard: {DASHBOARD_URL}

\u2014 Project Neo Monitoring
"""
        try:
            send_email(ses_client, email_config["sender"],
                       email_config["recipient"], subject, body)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO forex_network.system_alerts
                        (alert_type, severity, title, detail, alerted_at, alert_channels)
                    VALUES ('agent_stale', 'CRITICAL', %s, %s, NOW(), 'email')
                """, (f"Agent stale: {agent_name}",
                      f"{agent_name} — {minutes_stale} min since last heartbeat"))
            conn.commit()
            logger.info(f"Stale agent alert sent: {agent_name}")
            sent += 1
        except Exception as e:
            logger.error(f"Failed to send stale agent alert for {agent_name}: {e}")
            conn.rollback()

    return sent

# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    start  = time.time()
    ts_now = datetime.now(timezone.utc)
    logger.info(f"Critical alerter starting — {ts_now.isoformat()}")

    # ── Load credentials ──────────────────────────────────────────────────────
    try:
        email_config    = get_secret("platform/alerts/email")
        sms_config      = get_secret("platform/alerts/sms")
        rds_credentials = get_secret("platform/rds/credentials")
        rds_endpoint    = get_parameter("/platform/config/rds-endpoint")
        kill_switch     = get_parameter("/platform/config/kill-switch")
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        return {"statusCode": 500, "body": str(e)}

    # ── Kill switch check ─────────────────────────────────────────────────────
    # Even if kill switch is active, we still want to alert — don't skip
    if kill_switch == "active":
        logger.info("Kill switch is active — alerter continues to monitor")

    # ── Connect to RDS ────────────────────────────────────────────────────────
    try:
        conn = get_db_connection(rds_endpoint, rds_credentials)
    except Exception as e:
        logger.error(f"RDS connection failed: {e}")
        return {"statusCode": 500, "body": f"RDS connection failed: {e}"}

    # ── Initialise AWS clients ────────────────────────────────────────────────
    ses_client = boto3.client("ses", region_name=AWS_REGION)
    sns_client = boto3.client("sns", region_name=AWS_REGION)

    # ── Process alerts ────────────────────────────────────────────────────────
    total_sent = 0
    errors     = []

    try:
        stress_sent = process_stress_alerts(
            conn, ses_client, sns_client, email_config, sms_config
        )
        total_sent += stress_sent
    except Exception as e:
        msg = f"Stress alert processing failed: {e}"
        logger.error(msg)
        errors.append(msg)

    try:
        system_sent = process_system_alerts(
            conn, ses_client, sns_client, email_config, sms_config
        )
        total_sent += system_sent
    except Exception as e:
        msg = f"System alert processing failed: {e}"
        logger.error(msg)
        errors.append(msg)


    try:
        ks_sent = process_kill_switch(conn, ses_client, sns_client, email_config, sms_config)
        total_sent += ks_sent
    except Exception as e:
        msg = f"Kill switch check failed: {e}"
        logger.error(msg)
        errors.append(msg)

    try:
        dd_sent = process_drawdown_alerts(conn, ses_client, sns_client, email_config, sms_config)
        total_sent += dd_sent
    except Exception as e:
        msg = f"Drawdown alert processing failed: {e}"
        logger.error(msg)
        errors.append(msg)

    try:
        sa_sent = process_stale_agents(conn, ses_client, sns_client, email_config, sms_config)
        total_sent += sa_sent
    except Exception as e:
        msg = f"Stale agent check failed: {e}"
        logger.error(msg)
        errors.append(msg)

    conn.close()
    elapsed = round(time.time() - start, 2)

    logger.info(
        f"Critical alerter complete in {elapsed}s — "
        f"{total_sent} notification(s) sent"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ts":         ts_now.isoformat(),
            "sent":       total_sent,
            "elapsed_s":  elapsed,
            "errors":     errors,
        })
    }

