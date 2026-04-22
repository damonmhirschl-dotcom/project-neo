"""Project Neo — shared email alerting via SNS."""
import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_last_alert_times: dict = {}


def send_alert(severity: str, title: str, details: dict, source_agent: str = 'system') -> None:
    """Send an email alert via SNS with 15-minute per-key deduplication.

    severity : 'CRITICAL' | 'WARNING' | 'INFO'
    title    : short subject line (kept under 100 chars)
    details  : dict of diagnostic information
    source_agent: which agent triggered the alert
    """
    alert_key = f"{source_agent}:{title[:50]}"
    now = datetime.now(timezone.utc)

    last = _last_alert_times.get(alert_key)
    if last is not None and (now - last).total_seconds() < 900:
        return  # suppress duplicate within 15 minutes

    _last_alert_times[alert_key] = now

    try:
        ssm = boto3.client('ssm', region_name='eu-west-2')
        topic_arn = ssm.get_parameter(
            Name='/platform/config/alert-topic-arn'
        )['Parameter']['Value']

        sns = boto3.client('sns', region_name='eu-west-2')

        timestamp = now.strftime('%Y-%m-%d %H:%M:%S UTC')
        subject = f"[Neo {severity}] {title}"

        body = (
            f"Project Neo Alert\n"
            f"{'=' * 60}\n"
            f"Severity: {severity}\n"
            f"Source:   {source_agent}\n"
            f"Time:     {timestamp}\n"
            f"{'=' * 60}\n\n"
            f"{title}\n\n"
            f"Details:\n"
            f"{json.dumps(details, indent=2, default=str)}\n"
        )

        sns.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],
            Message=body,
        )
        logger.info(f"Alert sent [{severity}]: {title}")

    except Exception as exc:
        logger.warning(f"send_alert failed (non-fatal): {exc}")
