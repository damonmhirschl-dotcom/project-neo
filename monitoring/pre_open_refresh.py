#!/usr/bin/env python3
"""
Pre-Open Data Refresh for Project Neo.

Two invocation modes:
  pre_open  — Sunday 21:00 UTC. Ingests all data that accumulated during the
              weekend closure, so agents have fresh data when the market opens
              at 22:00 UTC. Also runs COT (CFTC releases Fridays, data lands
              over the weekend).
  morning   — Weekdays 06:30 UTC. Catches overnight / Asian-session data
              before the London open at 07:00 UTC.

Usage:
  python3 pre_open_refresh.py pre_open
  python3 pre_open_refresh.py morning
"""
import json
import sys
from datetime import datetime, timezone

import boto3


REGION = 'eu-west-2'


def invoke_lambda(client, name: str) -> bool:
    """Invoke a Lambda function synchronously and print the result."""
    try:
        resp = client.invoke(FunctionName=name, InvocationType='RequestResponse')
        payload = json.loads(resp['Payload'].read())
        status = payload.get('statusCode', 'unknown')
        body = payload.get('body', '')
        body_str = body[:120] if isinstance(body, str) else str(body)[:120]
        print(f"  ✓ {name}: HTTP {status} — {body_str}")
        return True
    except Exception as e:
        print(f"  ✗ {name}: ERROR — {e}")
        return False


def send_summary_alert(subject: str, message: str) -> None:
    """Send SNS alert with the refresh summary."""
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        topic_arn = ssm.get_parameter(
            Name='/platform/config/alert-topic-arn',
            WithDecryption=True,
        )['Parameter']['Value']
        sns = boto3.client('sns', region_name=REGION)
        sns.publish(TopicArn=topic_arn, Subject=f"[INFO] {subject}", Message=message)
    except Exception as e:
        print(f"  Alert send failed: {e}")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else 'pre_open'
    now = datetime.now(timezone.utc)
    is_pre_open = (mode == 'pre_open')

    print(f"=== {'PRE-OPEN' if is_pre_open else 'MORNING'} DATA REFRESH ===")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}  mode={mode}\n")

    lam = boto3.client('lambda', region_name=REGION)
    results = []

    # 1. FX prices — latest bars from provider
    print("--- Price Data ---")
    results.append(('prices', invoke_lambda(lam, 'neo-ingest-prices-dev')))

    # 2. Economic calendar — refresh upcoming week
    print("\n--- Economic Calendar ---")
    results.append(('econ_calendar', invoke_lambda(lam, 'neo-ingest-econ-calendar-dev')))

    # 3. MyFXBook SSI — latest retail positioning
    print("\n--- MyFXBook SSI ---")
    results.append(('ssi', invoke_lambda(lam, 'neo-ingest-ssi-dev')))

    # 4. IG Client Sentiment
    print("\n--- IG Sentiment ---")
    results.append(('ig_sentiment', invoke_lambda(lam, 'neo-ingest-ig-sentiment-dev')))

    # 5. Bond yields
    print("\n--- Bond Yields ---")
    results.append(('bond_yields', invoke_lambda(lam, 'neo-ingest-bond-yields-dev')))

    # 6. COT data — pre_open only (CFTC releases Friday; data available Sunday)
    if is_pre_open:
        print("\n--- COT Positioning ---")
        results.append(('cot', invoke_lambda(lam, 'neo-ingest-cot-dev')))

    ok = sum(1 for _, success in results if success)
    total = len(results)
    label = 'PRE-OPEN' if is_pre_open else 'MORNING'
    print(f"\n=== {label} REFRESH COMPLETE: {ok}/{total} successful ===")

    if ok < total:
        failed = [name for name, success in results if not success]
        msg = (
            f"{label} data refresh at {now.strftime('%Y-%m-%d %H:%M UTC')}: "
            f"{ok}/{total} succeeded. Failed: {', '.join(failed)}"
        )
        send_summary_alert(f"{label} Refresh — PARTIAL FAILURE", msg)
    else:
        msg = (
            f"{label} data refresh at {now.strftime('%Y-%m-%d %H:%M UTC')}: "
            f"all {total} sources refreshed successfully"
        )
        send_summary_alert(f"{label} Refresh — Complete", msg)


if __name__ == '__main__':
    main()
