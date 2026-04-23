"""
shared/warn_log.py — Structured warning logger for Project Neo agents.

Format: 2026-04-23 07:15:32 UTC | AGENT | CATEGORY | message | key=value key2=value2
Location: /var/log/neo/{agent}.warnings.log
"""
import os
import logging
from datetime import datetime, timezone

LOG_DIR = '/var/log/neo'

# Valid categories (OPERATIONAL_NOTES spec)
VALID_CATEGORIES = {
    'GAP_FILL', 'API_FAIL', 'STALE_DATA', 'FALLBACK', 'NULL_DATA',
    'MISSING_SIGNAL', 'THRESHOLD', 'DB_FAIL', 'PARSE_FAIL', 'SKIP',
    'TIMEOUT', 'RETRY', 'RATE_LIMIT', 'INVALID_DATA', 'LOOP_BREAK',
    'EXCEPTION', 'SIZE_WARN', 'MISSING_CONFIG', 'POSITION_MISMATCH',
    'SIGNAL_EXPIRED', 'HEARTBEAT_FAIL', 'CYCLE_SLOW', 'CONSTRAINT_FAIL',
    'PARTIAL_FILL', 'PRICE_STALE', 'CORRELATION_SKIP', 'CAP_BLOCK',
    'RECONNECT', 'DIVERGENCE', 'SNAPSHOT_MISS',
}

_loggers: dict = {}
_std_logger = logging.getLogger(__name__)


def _get_file_logger(agent: str) -> logging.Logger:
    """Return (cached) file logger for the given agent name."""
    if agent in _loggers:
        return _loggers[agent]

    log_path = os.path.join(LOG_DIR, f'{agent}.warnings.log')
    logger = logging.getLogger(f'neo.warnings.{agent}')
    logger.setLevel(logging.WARNING)
    logger.propagate = False  # don't double-emit to root logger

    if not logger.handlers:
        try:
            fh = logging.FileHandler(log_path, encoding='utf-8')
            fh.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(fh)
        except OSError as e:
            _std_logger.error(f"warn_log: cannot open {log_path}: {e}")

    _loggers[agent] = logger
    return logger


def warn(agent: str, category: str, message: str, **kwargs) -> None:
    """
    Write a structured warning entry.

    Args:
        agent:    Agent identifier, e.g. 'macro_agent', 'risk_guardian'
        category: One of VALID_CATEGORIES
        message:  Human-readable description
        **kwargs: Optional key=value pairs appended to the line
    """
    if category not in VALID_CATEGORIES:
        _std_logger.warning(f"warn_log: unknown category '{category}' — writing anyway")

    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    kv = ' '.join(f'{k}={v}' for k, v in kwargs.items()) if kwargs else ''
    parts = [ts, agent.upper(), category, message]
    if kv:
        parts.append(kv)
    line = ' | '.join(parts)

    file_logger = _get_file_logger(agent)
    file_logger.warning(line)
