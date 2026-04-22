"""
Score trajectory retrieval and analysis.

Every agent cycle calls get_recent_trajectory() to see its own recent scores
for a pair, and analyse_trajectory() to derive velocity/volatility/persistence
features.
"""
import logging
from datetime import timedelta
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_recent_trajectory(conn, agent_name: str, instrument: str,
                           lookback_minutes: int = 120,
                           max_cycles: int = 10) -> list:
    """
    Return the agent's own recent scores for this instrument, oldest first.

    Returns: list of dicts with keys: created_at, score, confidence, bias
    Empty list if no history found.
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT created_at, score::float AS score,
                   confidence::float AS confidence, bias
            FROM forex_network.agent_signals
            WHERE agent_name = %s
              AND instrument = %s
              AND created_at > NOW() - (%s || ' minutes')::interval
            ORDER BY created_at DESC
            LIMIT %s
        """, (agent_name, instrument, lookback_minutes, max_cycles))
        rows = list(reversed(cur.fetchall()))  # oldest first
        return rows
    except Exception as e:
        logger.error(f'get_recent_trajectory({agent_name}, {instrument}) failed: {e}')
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def analyse_trajectory(trajectory: list) -> dict:
    """
    Derive trajectory features from a list of score history.

    Returns dict with:
      n_cycles: int — how many cycles in the window
      velocity: float — average per-cycle change (+ve = strengthening toward current bias)
      volatility: float — standard deviation of scores
      persistence: int — consecutive cycles with same-sign score as latest
      direction: 'strengthening' | 'weakening' | 'stable' | 'reversing' | 'unstable' | 'insufficient_data'
      current_score: float — most recent score (for reference)
    """
    if not trajectory or len(trajectory) < 2:
        return {
            'n_cycles': len(trajectory),
            'velocity': 0.0,
            'volatility': 0.0,
            'persistence': 1 if trajectory else 0,
            'direction': 'insufficient_data',
            'current_score': trajectory[-1]['score'] if trajectory else 0.0,
        }

    scores = [r['score'] for r in trajectory]
    current = scores[-1]
    n = len(scores)

    # Velocity: signed average per-cycle change, aligned with current bias direction
    changes = [scores[i] - scores[i - 1] for i in range(1, n)]
    raw_velocity = sum(changes) / len(changes)
    velocity = raw_velocity if current >= 0 else -raw_velocity

    # Volatility: std dev of scores
    mean_score = sum(scores) / n
    variance = sum((s - mean_score) ** 2 for s in scores) / n
    volatility = variance ** 0.5

    # Persistence: how many consecutive cycles ending at latest have same sign
    persistence = 1
    current_sign = 1 if current > 0 else (-1 if current < 0 else 0)
    for i in range(n - 2, -1, -1):
        s = scores[i]
        s_sign = 1 if s > 0 else (-1 if s < 0 else 0)
        if s_sign == current_sign and s_sign != 0:
            persistence += 1
        else:
            break

    # Direction classification
    if volatility > 0.15:
        if velocity > 0.02:
            direction = 'strengthening'
        elif velocity < -0.02:
            direction = 'reversing'
        else:
            direction = 'unstable'
    else:
        if velocity > 0.02:
            direction = 'strengthening'
        elif velocity < -0.02:
            direction = 'weakening'
        else:
            direction = 'stable'

    return {
        'n_cycles': n,
        'velocity': round(velocity, 4),
        'volatility': round(volatility, 4),
        'persistence': persistence,
        'direction': direction,
        'current_score': round(current, 4),
    }


def format_trajectory_for_prompt(trajectory: list, pair: str) -> str:
    """
    Format trajectory as a human-readable string suitable for LLM prompt injection.
    """
    if not trajectory:
        return f"No recent scoring history for {pair}."

    lines = [f"Your recent scores for {pair}:"]
    for row in trajectory:
        ts = row['created_at'].strftime('%H:%M')
        score = row['score']
        bias = row['bias'] or 'neutral'
        conf = row['confidence']
        lines.append(f"  {ts} UTC: score={score:+.3f} bias={bias} confidence={conf:.2f}")

    features = analyse_trajectory(trajectory)
    lines.append(
        f"\nTrajectory: direction={features['direction']}, "
        f"velocity={features['velocity']:+.3f} per cycle, "
        f"volatility={features['volatility']:.3f}, "
        f"persistence={features['persistence']} cycles"
    )

    return '\n'.join(lines)


def get_recent_trajectory_batch(conn, agent_name: str, instruments: list,
                                lookback_minutes: int = 120,
                                max_cycles_per_pair: int = 10) -> dict:
    """
    Batch version of get_recent_trajectory. Returns trajectories for multiple
    instruments in a single query.

    Returns: dict mapping instrument -> list of row dicts (oldest first).
             Missing instruments get an empty list.
             On query failure, returns a dict of empty lists for all requested instruments.
    """
    result = {inst: [] for inst in instruments}
    if not instruments:
        return result
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            WITH ranked AS (
                SELECT instrument, created_at,
                       score::float AS score,
                       confidence::float AS confidence,
                       bias,
                       ROW_NUMBER() OVER (
                           PARTITION BY instrument
                           ORDER BY created_at DESC
                       ) AS rn
                FROM forex_network.agent_signals
                WHERE agent_name = %s
                  AND instrument = ANY(%s)
                  AND created_at > NOW() - (%s || ' minutes')::interval
            )
            SELECT instrument, created_at, score, confidence, bias
            FROM ranked
            WHERE rn <= %s
            ORDER BY instrument, created_at ASC
        """, (agent_name, instruments, lookback_minutes, max_cycles_per_pair))

        for row in cur.fetchall():
            result[row['instrument']].append({
                'created_at': row['created_at'],
                'score': row['score'],
                'confidence': row['confidence'],
                'bias': row['bias'],
            })
        return result
    except Exception as e:
        logger.error(f'get_recent_trajectory_batch({agent_name}, {instruments}) failed: {e}')
        try:
            conn.rollback()
        except Exception:
            pass
        return {inst: [] for inst in instruments}
