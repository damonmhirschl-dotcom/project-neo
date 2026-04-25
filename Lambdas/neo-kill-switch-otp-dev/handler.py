"""
neo-kill-switch-otp-dev — Kill Switch OTP activation handler

Flow:
  1. POST {"action": "activate"}              → generate OTP, SMS to registered phone,
                                                 store in DDB with 90s TTL, intent=activate
  2. POST {"action": "confirm", "otp": "NNN"} → validate OTP, flip kill-switch to 'active'

  1. POST {"action": "deactivate"}             → generate OTP, SMS, store intent=deactivate
  2. POST {"action": "confirm", "otp": "NNN"}  → validate OTP, flip kill-switch to 'inactive'

OTP storage: DynamoDB neo-otp-store, singleton row otp_id='kill_switch_otp', TTL 90s.
SMS delivery: SNS direct to phone_number from Secrets Manager platform/alerts/sms.
"""

import json, os, time, random, string, hmac, logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION            = "eu-west-2"
OTP_TABLE         = "neo-otp-store"
OTP_KEY           = "kill_switch_otp"
OTP_TTL_SECS      = 90
KILL_SWITCH_PARAM = "/platform/config/kill-switch"
SMS_SECRET        = "platform/alerts/sms"

_ddb = boto3.resource("dynamodb", region_name=REGION)
_ssm = boto3.client("ssm",          region_name=REGION)
_sm  = boto3.client("secretsmanager", region_name=REGION)
_sns = boto3.client("sns",           region_name=REGION)


def _generate_otp() -> str:
    return "".join(random.choices(string.digits, k=6))


def _get_phone() -> str:
    secret = json.loads(_sm.get_secret_value(SecretId=SMS_SECRET)["SecretString"])
    return secret["phone_number"]


def _store_otp(otp: str, intent: str):
    now = int(time.time())
    _ddb.Table(OTP_TABLE).put_item(Item={
        "otp_id":     OTP_KEY,
        "otp":        otp,
        "intent":     intent,
        "created_at": now,
        "expires_at": now + OTP_TTL_SECS,
    })


def _get_otp_record():
    resp = _ddb.Table(OTP_TABLE).get_item(Key={"otp_id": OTP_KEY})
    item = resp.get("Item")
    if not item:
        return None
    # Manual TTL check — DDB TTL deletion can lag by minutes
    if int(item["expires_at"]) < int(time.time()):
        return None
    return item


def _delete_otp():
    _ddb.Table(OTP_TABLE).delete_item(Key={"otp_id": OTP_KEY})


def _get_kill_switch_state() -> str:
    return _ssm.get_parameter(Name=KILL_SWITCH_PARAM)["Parameter"]["Value"]


def _set_kill_switch(state: str):
    _ssm.put_parameter(
        Name=KILL_SWITCH_PARAM, Value=state, Type="String", Overwrite=True
    )


def _respond(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body),
    }


def handle_initiate(intent: str) -> dict:
    """activate or deactivate — generate OTP, SMS it, store in DDB."""
    current_state = _get_kill_switch_state()

    if intent == "activate" and current_state == "active":
        return _respond(409, {"error": "kill_switch_already_active", "state": current_state})
    if intent == "deactivate" and current_state == "inactive":
        return _respond(409, {"error": "kill_switch_already_inactive", "state": current_state})

    otp   = _generate_otp()
    phone = _get_phone()

    _store_otp(otp, intent)

    _sns.publish(
        PhoneNumber=phone,
        Message=(
            f"Project Neo kill-switch OTP: {otp} "
            f"(expires {OTP_TTL_SECS}s). "
            f"Action: {intent.upper()}."
        ),
    )

    logger.info(f"OTP issued: intent={intent}, current_state={current_state}")
    return _respond(200, {
        "status":             "otp_sent",
        "intent":             intent,
        "expires_in_seconds": OTP_TTL_SECS,
    })


def handle_confirm(otp_input: str) -> dict:
    """Validate OTP and flip kill switch per stored intent."""
    if not otp_input:
        return _respond(400, {"error": "otp_required"})

    record = _get_otp_record()
    if not record:
        return _respond(410, {"error": "otp_expired_or_not_found"})

    if not hmac.compare_digest(str(record["otp"]), str(otp_input).strip()):
        logger.warning("Kill-switch OTP mismatch attempt")
        return _respond(403, {"error": "otp_invalid"})

    intent    = record["intent"]
    new_state = "active" if intent == "activate" else "inactive"

    _set_kill_switch(new_state)
    _delete_otp()

    logger.warning(
        f"KILL SWITCH → '{new_state}' confirmed via OTP (intent={intent})"
    )
    return _respond(200, {"status": "confirmed", "kill_switch": new_state})


def lambda_handler(event, context):
    try:
        body = event.get("body") or event
        if isinstance(body, str):
            body = json.loads(body)

        action = body.get("action", "")

        if action == "activate":
            return handle_initiate("activate")
        elif action == "deactivate":
            return handle_initiate("deactivate")
        elif action == "confirm":
            return handle_confirm(body.get("otp", ""))
        else:
            return _respond(400, {
                "error":        "invalid_action",
                "valid_actions": ["activate", "confirm", "deactivate"],
            })

    except Exception as e:
        logger.error(f"OTP handler error: {e}", exc_info=True)
        return _respond(500, {"error": "internal_error", "detail": str(e)})
