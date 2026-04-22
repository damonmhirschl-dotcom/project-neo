"""
schema_validator.py — startup schema validation for all Project Neo agents.
Call validate_schema(conn, EXPECTED_TABLES) at agent startup before first cycle.
Catches column-missing and table-missing defects before they cause silent runtime failures.
"""

import logging
logger = logging.getLogger(__name__)

def validate_schema(conn, expected: dict) -> bool:
    """
    Validate that all expected tables and columns exist in the live DB.

    Args:
        conn: active psycopg2 connection
        expected: dict of {
            "schema.table": ["col1", "col2", ...]
        }

    Returns:
        True if all pass.
        Logs ERROR for each missing table or column.
        Raises RuntimeError if any CRITICAL columns are missing
        (caller decides which are critical by passing them in).

    Usage:
        from shared.schema_validator import validate_schema
        validate_schema(conn, EXPECTED_TABLES)
    """
    all_ok = True
    cur = conn.cursor()

    for table_ref, columns in expected.items():
        # Parse schema.table
        parts = table_ref.split(".")
        schema = parts[0] if len(parts) == 2 else "public"
        table  = parts[1] if len(parts) == 2 else parts[0]

        # Check table exists
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        if cur.fetchone()[0] == 0:
            logger.error(f"SCHEMA VALIDATION FAILED: table '{table_ref}' does not exist")
            all_ok = False
            continue

        # Check each expected column
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        live_cols = {row[0] for row in cur.fetchall()}

        for col in columns:
            if col not in live_cols:
                logger.error(
                    f"SCHEMA VALIDATION FAILED: column '{col}' missing from '{table_ref}'. "
                    f"Live columns: {sorted(live_cols)}"
                )
                all_ok = False

    cur.close()

    if not all_ok:
        raise RuntimeError(
            "Schema validation failed at startup — one or more expected columns are missing. "
            "Check logs for details. Halting agent to prevent silent data corruption."
        )

    logger.info("Schema validation passed — all expected tables and columns present.")
    return True
