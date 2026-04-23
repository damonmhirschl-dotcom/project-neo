import psycopg2
import boto3
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)
_conn_cache = None

def _get_conn():
    global _conn_cache
    if _conn_cache and not _conn_cache.closed:
        return _conn_cache
    sm  = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
    host  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value']
    _conn_cache = psycopg2.connect(
        host=host, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network',
    )
    _conn_cache.autocommit = True
    return _conn_cache

def log_event(
    event_type: str,
    message: str,
    category: str = 'SYSTEM',
    agent: str = None,
    user_id: str = None,
    instrument: str = None,
    severity: str = 'INFO',
    payload: dict = None,
    cycle_id: int = None,
):
    """Write a structured event to forex_network.system_events. Never raises."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO forex_network.system_events
                (category, agent, user_id, instrument, severity,
                 event_type, message, payload, cycle_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            category, agent,
            user_id if user_id else None,
            instrument, severity, event_type, message,
            json.dumps(payload) if payload else None,
            cycle_id,
        ))
        cur.close()
    except Exception as e:
        logger.warning(f'system_events write failed: {e}')
