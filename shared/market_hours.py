"""
Shared market hours awareness module for Project Neo agents.

FX market: Sunday 22:00 UTC to Friday 22:00 UTC.

States
------
closed   — Weekend (Friday 22:00 to Sunday 21:00 UTC).
             No trading, no LLM calls. Agents write heartbeats only.
pre_open — Sunday 21:00-22:00 UTC. Data refresh window.
             Agents stand by; pre_open_refresh.py script runs.
active   — Major sessions open (07:00-21:00 UTC weekdays).
             Full cycle frequency.
quiet    — Low-liquidity hours (22:00-07:00 weekdays).
             Reduced cycle frequency.
"""
from datetime import datetime, timezone


def get_market_state() -> dict:
    """
    Return the current FX market operating state.

    Returns a dict with:
      state:    'closed' | 'pre_open' | 'active' | 'quiet'
      sessions: list of currently open sessions (may be empty)
      reason:   human-readable description for log messages
    """
    now = datetime.now(timezone.utc)
    weekday = now.isoweekday()  # 1=Mon … 7=Sun
    hour = now.hour

    # ---- Weekend closure: Friday 22:00 → Sunday 21:00 ----
    if weekday == 6:  # Saturday (all day)
        return {'state': 'closed', 'sessions': [], 'reason': 'Weekend — Saturday'}
    if weekday == 5 and hour >= 22:  # Friday after close
        return {'state': 'closed', 'sessions': [], 'reason': 'Weekend — Friday after close'}
    if weekday == 7 and hour < 21:  # Sunday before pre-open
        return {'state': 'closed', 'sessions': [], 'reason': 'Weekend — Sunday before pre-open'}

    # ---- Pre-open: Sunday 21:00-22:00 ----
    if weekday == 7 and 21 <= hour < 22:
        return {'state': 'pre_open', 'sessions': [], 'reason': 'Pre-open data refresh window'}

    # ---- Weekdays (Mon-Fri before 22:00) ----
    sessions = []
    if hour >= 22 or hour < 8:
        sessions.append('asian')
    if 7 <= hour < 16:
        sessions.append('london')
    if 12 <= hour < 21:
        sessions.append('new_york')

    if 7 <= hour < 21:
        return {
            'state': 'active',
            'sessions': sessions,
            'reason': f"Active — {', '.join(sessions) if sessions else 'session overlap'}",
        }

    # Quiet: 21:00-22:00 gap or 22:00-07:00 Asian-only
    return {
        'state': 'quiet',
        'sessions': sessions,
        'reason': f"Quiet hours — {'Asian session' if sessions else 'inter-session gap'}",
    }
