"""
shared/agent_state.py — agent state persistence helpers.

Provides save_state / load_state / delete_state / log_loaded_state_summary
backed by forex_network.agent_state (JSONB, UPSERT, per-user keyed).

Design rules:
  - save_state  never raises; returns True/False
  - load_state  never raises; returns default on any error or stale data
  - All operations are idempotent
  - user_id is required at every call site
  - Use AGENT_SCOPE_USER_ID for agent-level (non-user-scoped) state
  - max_age_minutes in load_state: if state is older than threshold, treat as absent
"""

import json
import logging
import uuid
from typing import Any, Optional

import boto3
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Sentinel UUID for agent-level (non-user-scoped) state.
# Use this for orchestrator cycle_count, macro cycle_count, etc.
# A fixed nil UUID participates cleanly in PK constraints.
# ─────────────────────────────────────────────────────────────────────────────
AGENT_SCOPE_USER_ID = uuid.UUID('00000000-0000-0000-0000-000000000000')

_CURRENT_SCHEMA_VERSION = 1

# ─────────────────────────────────────────────────────────────────────────────
# Module-level connection cache
# ─────────────────────────────────────────────────────────────────────────────
_conn: Optional[psycopg2.extensions.connection] = None


def _get_connection() -> psycopg2.extensions.connection:
    """Return a cached module-level psycopg2 connection, reconnecting if needed."""
    global _conn
    try:
        if _conn is not None and not _conn.closed:
            return _conn
    except Exception:
        pass

    sm = boto3.client('secretsmanager', region_name='eu-west-2')
    ssm = boto3.client('ssm', region_name='eu-west-2')
    creds = json.loads(
        sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString']
    )
    ep = ssm.get_parameter(
        Name='/platform/config/rds-endpoint', WithDecryption=True
    )['Parameter']['Value']
    _conn = psycopg2.connect(
        host=ep, dbname='postgres',
        user=creds['username'], password=creds['password'],
        options='-c search_path=forex_network',
    )
    return _conn


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def save_state(agent_name: str, user_id, state_key: str, value: Any,
               schema_version: int = _CURRENT_SCHEMA_VERSION) -> bool:
    """
    Persist a JSON-serialisable value under (agent_name, user_id, state_key).
    user_id required — use AGENT_SCOPE_USER_ID for agent-level state.
    Returns True on success, False on failure (logged). Never raises.
    """
    if user_id is None:
        logger.error(f'[agent_state] save_state called without user_id for {agent_name}/{state_key}')
        return False

    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forex_network.agent_state
                    (agent_name, user_id, state_key, state_value, schema_version, updated_at)
                VALUES (%s, %s::uuid, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (agent_name, user_id, state_key)
                DO UPDATE SET
                    state_value    = EXCLUDED.state_value,
                    schema_version = EXCLUDED.schema_version,
                    updated_at     = NOW()
            """, (agent_name, str(user_id), state_key, json.dumps(value), schema_version))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f'[agent_state] save_state({agent_name}, {user_id}, {state_key}) failed: {e}')
        try:
            _get_connection().rollback()
        except Exception:
            pass
        return False


def load_state(agent_name: str, user_id, state_key: str,
               default: Any = None,
               max_age_minutes: Optional[int] = None) -> Any:
    """
    Load persisted value for (agent_name, user_id, state_key).
    Returns `default` on any error, missing key, or stale data. Never raises.
    """
    if user_id is None:
        logger.error(f'[agent_state] load_state called without user_id for {agent_name}/{state_key}')
        return default

    try:
        conn = _get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT state_value, schema_version, updated_at,
                       EXTRACT(EPOCH FROM (NOW() - updated_at))/60 AS age_min
                FROM forex_network.agent_state
                WHERE agent_name = %s AND user_id = %s::uuid AND state_key = %s
            """, (agent_name, str(user_id), state_key))
            row = cur.fetchone()

        if row is None:
            logger.info(f'[agent_state] {agent_name}/{state_key} (user={str(user_id)[:8]}) -> no prior state, using default')
            return default

        age_min = float(row['age_min'])
        if max_age_minutes is not None and age_min > max_age_minutes:
            logger.warning(
                f'[agent_state] {agent_name}/{state_key} is {age_min:.0f}m old '
                f'(max {max_age_minutes}m) — using default'
            )
            return default

        value = row['state_value']
        if isinstance(value, str):
            value = json.loads(value)
        logger.info(
            f'[agent_state] {agent_name}/{state_key} (user={str(user_id)[:8]}) -> '
            f'loaded, age={age_min:.0f}min, schema_v{row["schema_version"]}'
        )
        return value

    except Exception as e:
        logger.error(f'[agent_state] load_state({agent_name}, {user_id}, {state_key}) failed: {e}')
        try:
            _get_connection().rollback()
        except Exception:
            pass
        return default


def delete_state(agent_name: str, user_id, state_key: str) -> bool:
    """Delete a single state entry. Returns True on success, False on error. Never raises."""
    if user_id is None:
        logger.error(f'[agent_state] delete_state called without user_id for {agent_name}/{state_key}')
        return False

    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM forex_network.agent_state
                WHERE agent_name = %s AND user_id = %s::uuid AND state_key = %s
            """, (agent_name, str(user_id), state_key))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f'[agent_state] delete_state({agent_name}, {user_id}, {state_key}) failed: {e}')
        try:
            _get_connection().rollback()
        except Exception:
            pass
        return False


def log_loaded_state_summary(agent_name: str, user_id=None) -> None:
    """
    Log a one-line summary of persisted keys for this agent.
    If user_id is None, shows summary across all users (useful at startup).
    Called at startup so logs confirm what was (or wasn't) restored.
    Never raises.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            if user_id is None:
                cur.execute("""
                    SELECT state_key, user_id,
                           EXTRACT(EPOCH FROM (NOW() - updated_at))/60 AS age_min
                    FROM forex_network.agent_state
                    WHERE agent_name = %s
                    ORDER BY user_id, state_key
                """, (agent_name,))
            else:
                cur.execute("""
                    SELECT state_key, user_id,
                           EXTRACT(EPOCH FROM (NOW() - updated_at))/60 AS age_min
                    FROM forex_network.agent_state
                    WHERE agent_name = %s AND user_id = %s::uuid
                    ORDER BY state_key
                """, (agent_name, str(user_id)))
            rows = cur.fetchall()

        if not rows:
            logger.info(f'[agent_state] {agent_name}: no persisted state found (cold start)')
            return

        summary = ', '.join(
            f'{r[0]}(u={str(r[1])[:8]},{float(r[2]):.0f}m)' for r in rows
        )
        logger.info(
            f'[agent_state] {agent_name}: {len(rows)} state '
            f"entr{'y' if len(rows) == 1 else 'ies'}: {summary}"
        )
    except Exception as e:
        logger.error(f'[agent_state] log_loaded_state_summary({agent_name}) failed: {e}')
