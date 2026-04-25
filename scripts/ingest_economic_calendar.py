#!/usr/bin/env python3
"""
ingest_economic_calendar.py — Fetch forward economic calendar from Finnhub
and upsert into forex_network.economic_releases.

Fetches next 30 days, filtered to V1 Swing currency countries.
Safe to re-run: ON CONFLICT (source, country, indicator, release_time) DO UPDATE.
Cron: 0 6 * * 1-5  (06:00 UTC Mon-Fri, before London open)
"""
import sys, json, logging, datetime
import requests, boto3

sys.path.insert(0, '/root/Project_Neo_Damon/Orchestrator')
from orchestrator_agent import DatabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('economic_calendar')

# V1 Swing currency countries (both ISO-2 and full names Finnhub uses)
V1_COUNTRIES = {
    'US', 'GB', 'EU', 'JP', 'CH', 'AU', 'CA', 'NZ',
    'United States', 'United Kingdom', 'European Union',
    'Japan', 'Switzerland', 'Australia', 'Canada', 'New Zealand',
    'Euro Zone', 'Euro Area',
}

IMPACT_MAP = {'high': 'high', 'medium': 'medium', 'low': 'low', '3': 'high', '2': 'medium', '1': 'low'}


def get_finnhub_key() -> str:
    sm = boto3.client('secretsmanager', region_name='eu-west-2')
    return json.loads(sm.get_secret_value(
        SecretId='platform/finnhub/api-key')['SecretString'])['api_key']


def fetch_calendar(api_key: str, date_from: str, date_to: str) -> list:
    r = requests.get(
        'https://finnhub.io/api/v1/calendar/economic',
        params={'from': date_from, 'to': date_to, 'token': api_key},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    events = data.get('economicCalendar', [])
    logger.info(f"Finnhub returned {len(events)} events for {date_from} → {date_to}")
    return events


def upsert_events(cur, events: list) -> tuple:
    inserted = skipped = 0
    for e in events:
        country = e.get('country', '')
        if country not in V1_COUNTRIES:
            skipped += 1
            continue

        release_time_str = e.get('time') or e.get('date')
        if not release_time_str:
            skipped += 1
            continue

        indicator   = e.get('event') or e.get('indicator', '')
        actual      = e.get('actual')
        forecast    = e.get('estimate') or e.get('forecast')
        previous    = e.get('prev') or e.get('previous')
        unit        = e.get('unit', '')
        impact_raw  = str(e.get('impact', '') or '').lower()
        impact_level = IMPACT_MAP.get(impact_raw, 'low')

        # Compute surprise_pct if actual and forecast are available
        surprise_pct = None
        try:
            if actual is not None and forecast is not None and float(forecast) != 0:
                surprise_pct = round((float(actual) - float(forecast)) / abs(float(forecast)) * 100, 2)
        except (TypeError, ValueError):
            pass

        cur.execute("""
            INSERT INTO forex_network.economic_releases
                (source, country, indicator, release_time, actual, forecast,
                 previous, surprise_pct, unit, impact_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, country, indicator, release_time)
            DO UPDATE SET
                actual       = EXCLUDED.actual,
                forecast     = EXCLUDED.forecast,
                previous     = EXCLUDED.previous,
                surprise_pct = EXCLUDED.surprise_pct,
                unit         = EXCLUDED.unit,
                impact_level = EXCLUDED.impact_level
        """, (
            'finnhub', country, indicator, release_time_str,
            actual, forecast, previous, surprise_pct, unit, impact_level,
        ))
        inserted += 1

    return inserted, skipped


def main():
    today = datetime.date.today()
    date_from = today.strftime('%Y-%m-%d')
    date_to   = (today + datetime.timedelta(days=30)).strftime('%Y-%m-%d')

    api_key = get_finnhub_key()
    events  = fetch_calendar(api_key, date_from, date_to)

    db = DatabaseConnection()
    db.connect()
    conn = db.conn
    cur  = conn.cursor()

    try:
        inserted, skipped = upsert_events(cur, events)
        conn.commit()
        logger.info(f"Upserted {inserted} events, skipped {skipped} non-V1 countries")

        # Confirm upcoming high-impact count
        cur.execute("""
            SELECT COUNT(*) FROM forex_network.economic_releases
            WHERE release_time > NOW()
              AND release_time < NOW() + INTERVAL '30 days'
              AND impact_level = 'high'
              AND source = 'finnhub'
        """)
        upcoming_high = cur.fetchone()[0]
        logger.info(f"Upcoming high-impact events (30d, finnhub): {upcoming_high}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
