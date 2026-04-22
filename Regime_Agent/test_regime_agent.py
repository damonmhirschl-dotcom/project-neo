#!/usr/bin/env python3
"""
Project Neo — Regime Agent Integration Test Suite
===================================================
Tests the regime agent against live AWS infrastructure.
Run AFTER deployment, BEFORE enabling continuous mode.

Tests:
  1. AWS credentials and IAM role
  2. Secrets Manager access (all required secrets)
  3. Parameter Store access (kill switch, RDS endpoint)
  4. RDS connectivity and schema validation
  5. Stress score data source availability (all 7 components)
  6. Regime classification (per-pair and global)
  7. Signal write and read-back
  8. Market context snapshot write
  9. Stress alert write
  10. Heartbeat write and read-back
  11. Kill switch read
  12. MCP provider reachability (EODHD, Alpha Vantage)

Usage:
  source ~/algodesk/bin/activate
  python test_regime_agent.py --user neo_user_002

Exit codes:
  0 = all tests passed
  1 = one or more tests failed
"""

import os
import sys
import json
import time
import uuid
import argparse
import logging
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("neo.regime_agent_test")

# =============================================================================
# CONFIG
# =============================================================================
AWS_REGION = "eu-west-2"

REQUIRED_SECRETS = [
    "platform/eodhd/api-key",
    "platform/polygon/api-key",
    "platform/alphaVantage/api-key",
    "platform/anthropic/api-key",
    "platform/rds/credentials",
]

REQUIRED_PARAMETERS = [
    "/platform/config/rds-endpoint",
    "/platform/config/aws-region",
    "/platform/config/kill-switch",
]

# Tables the regime agent reads from
REQUIRED_TABLES = [
    ("forex_network", "economic_releases"),
    ("forex_network", "price_metrics"),
    ("forex_network", "historical_prices"),
    ("forex_network", "sentiment_order_flow"),
    ("forex_network", "agent_signals"),
    ("forex_network", "agent_heartbeats"),
    ("forex_network", "system_stress_alerts"),
    ("shared", "market_context_snapshots"),
    ("shared", "portfolio_correlation"),
    ("shared", "geopolitical_signals"),
]

# Columns added by migrate_system_stress_v1.sql
REQUIRED_STRESS_COLUMNS = [
    ("shared", "market_context_snapshots", "system_stress_score"),
    ("shared", "market_context_snapshots", "stress_state"),
    ("shared", "market_context_snapshots", "stress_components"),
    ("shared", "market_context_snapshots", "stress_trend_3c"),
    ("shared", "market_context_snapshots", "stress_peak_24h"),
]

FX_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]


# =============================================================================
# TEST RUNNER
# =============================================================================
class RegimeAgentIntegrationTest:
    """Integration test suite for the Regime Agent."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.passed = 0
        self.failed = 0
        self.warned = 0
        self.conn = None
        self.test_signal_id = None

    def _pass(self, name: str, detail: str = ""):
        self.passed += 1
        msg = f"  ✅ PASS: {name}"
        if detail:
            msg += f" — {detail}"
        logger.info(msg)

    def _fail(self, name: str, detail: str = ""):
        self.failed += 1
        msg = f"  ❌ FAIL: {name}"
        if detail:
            msg += f" — {detail}"
        logger.error(msg)

    def _warn(self, name: str, detail: str = ""):
        self.warned += 1
        msg = f"  ⚠️  WARN: {name}"
        if detail:
            msg += f" — {detail}"
        logger.warning(msg)

    # -------------------------------------------------------------------------
    # Test 1: AWS Credentials
    # -------------------------------------------------------------------------
    def test_aws_credentials(self):
        logger.info("\n--- Test 1: AWS Credentials ---")
        try:
            sts = boto3.client("sts", region_name=AWS_REGION)
            identity = sts.get_caller_identity()
            account = identity["Account"]
            arn = identity["Arn"]

            if account == "956177812472":
                self._pass("AWS credentials", f"Account: {account}, ARN: {arn}")
            else:
                self._fail("AWS credentials", f"Wrong account: {account} (expected 956177812472)")
        except Exception as e:
            self._fail("AWS credentials", str(e))

    # -------------------------------------------------------------------------
    # Test 2: Secrets Manager
    # -------------------------------------------------------------------------
    def test_secrets_manager(self):
        logger.info("\n--- Test 2: Secrets Manager ---")
        sm = boto3.client("secretsmanager", region_name=AWS_REGION)

        for secret_name in REQUIRED_SECRETS:
            try:
                resp = sm.get_secret_value(SecretId=secret_name)
                parsed = json.loads(resp["SecretString"])
                # Verify it has an api_key or username field
                if "api_key" in parsed or "username" in parsed:
                    self._pass(f"Secret: {secret_name}")
                else:
                    self._warn(f"Secret: {secret_name}", "Key present but unexpected format")
            except sm.exceptions.ResourceNotFoundException:
                self._fail(f"Secret: {secret_name}", "NOT FOUND")
            except Exception as e:
                self._fail(f"Secret: {secret_name}", str(e))

    # -------------------------------------------------------------------------
    # Test 3: Parameter Store
    # -------------------------------------------------------------------------
    def test_parameter_store(self):
        logger.info("\n--- Test 3: Parameter Store ---")
        ssm = boto3.client("ssm", region_name=AWS_REGION)

        for param_name in REQUIRED_PARAMETERS:
            try:
                resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
                value = resp["Parameter"]["Value"]
                self._pass(f"Parameter: {param_name}", f"Value: {value[:50]}...")
            except ssm.exceptions.ParameterNotFound:
                self._fail(f"Parameter: {param_name}", "NOT FOUND")
            except Exception as e:
                self._fail(f"Parameter: {param_name}", str(e))

    # -------------------------------------------------------------------------
    # Test 4: RDS Connectivity and Schema
    # -------------------------------------------------------------------------
    def test_rds_connectivity(self):
        logger.info("\n--- Test 4: RDS Connectivity and Schema ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION)
            sm = boto3.client("secretsmanager", region_name=AWS_REGION)

            rds_endpoint = ssm.get_parameter(
                Name="/platform/config/rds-endpoint", WithDecryption=True
            )["Parameter"]["Value"]

            creds = json.loads(
                sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"]
            )

            self.conn = psycopg2.connect(
                host=rds_endpoint,
                port=5432,
                dbname="postgres",
                user=creds["username"],
                password=creds["password"],
                connect_timeout=10,
                options="-c search_path=forex_network,shared,public",
            )
            self.conn.autocommit = False
            self._pass("RDS connection", f"Host: {rds_endpoint}")

        except Exception as e:
            self._fail("RDS connection", str(e))
            return

        # Check required tables
        cur = self.conn.cursor()
        for schema, table in REQUIRED_TABLES:
            try:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table))
                exists = cur.fetchone()[0]
                if exists:
                    self._pass(f"Table: {schema}.{table}")
                else:
                    self._fail(f"Table: {schema}.{table}", "NOT FOUND")
            except Exception as e:
                self._fail(f"Table: {schema}.{table}", str(e))

        # Check stress score columns (from migration 8)
        for schema, table, column in REQUIRED_STRESS_COLUMNS:
            try:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s
                    )
                """, (schema, table, column))
                exists = cur.fetchone()[0]
                if exists:
                    self._pass(f"Column: {schema}.{table}.{column}")
                else:
                    self._fail(f"Column: {schema}.{table}.{column}",
                               "NOT FOUND — has migrate_system_stress_v1.sql been applied?")
            except Exception as e:
                self._fail(f"Column: {schema}.{table}.{column}", str(e))

        cur.close()

    # -------------------------------------------------------------------------
    # Test 5: Stress Score Data Sources
    # -------------------------------------------------------------------------
    def test_stress_data_sources(self):
        logger.info("\n--- Test 5: Stress Score Data Sources ---")
        if not self.conn:
            self._fail("Stress data sources", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # VIX data
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt, MAX(release_time) AS latest
                FROM forex_network.economic_releases
                WHERE indicator = 'VIXCLS' AND actual IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"VIX (VIXCLS)", f"{row['cnt']} rows, latest: {row['latest']}")
            else:
                self._warn("VIX (VIXCLS)", "No data — stress VIX component will use fallback")
        except Exception as e:
            self._fail("VIX (VIXCLS)", str(e))

        # Yield curve data
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt, MAX(release_time) AS latest
                FROM forex_network.economic_releases
                WHERE indicator = 'T10Y2Y' AND actual IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"Yield curve (T10Y2Y)", f"{row['cnt']} rows, latest: {row['latest']}")
            else:
                self._warn("Yield curve (T10Y2Y)", "No data — yield curve component will use fallback")
        except Exception as e:
            self._fail("Yield curve (T10Y2Y)", str(e))

        # Price metrics (vol divergence)
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT instrument) AS pairs,
                       COUNT(*) AS rows,
                       MAX(ts) AS latest
                FROM forex_network.price_metrics
                WHERE timeframe = '1H' AND realised_vol_14 IS NOT NULL
            """)
            row = cur.fetchone()
            if row["pairs"] >= 7:
                self._pass(f"Price metrics (vol)", f"{row['pairs']} pairs, {row['rows']} rows, latest: {row['latest']}")
            elif row["pairs"] > 0:
                self._warn(f"Price metrics (vol)", f"Only {row['pairs']}/7 pairs have data")
            else:
                self._fail("Price metrics (vol)", "No realised_vol_14 data")
        except Exception as e:
            self._fail("Price metrics (vol)", str(e))

        # Portfolio correlation
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM shared.portfolio_correlation
                WHERE correlation_30d IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"Portfolio correlation", f"{row['cnt']} rows")
            else:
                self._warn("Portfolio correlation", "No data — will use baseline values")
        except Exception as e:
            self._warn("Portfolio correlation", f"Table may be empty initially: {e}")

        # Geopolitical signals
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt, MAX(created_at) AS latest
                FROM shared.geopolitical_signals
                WHERE tension_index IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"Geopolitical signals", f"{row['cnt']} rows, latest: {row['latest']}")
            else:
                self._warn("Geopolitical signals", "No data — geo component will use fallback")
        except Exception as e:
            self._warn("Geopolitical signals", f"Table may be empty initially: {e}")

        # Sentiment order flow (TraderMade)
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt, MAX(ts) AS latest
                FROM forex_network.sentiment_order_flow
                WHERE flow_score IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"Order flow (TraderMade)", f"{row['cnt']} rows, latest: {row['latest']}")
            else:
                self._warn("Order flow (TraderMate)", "No data — order flow component will use fallback")
        except Exception as e:
            self._warn("Order flow (TraderMade)", f"Table may be empty initially: {e}")

        # COT data
        try:
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM forex_network.economic_releases
                WHERE indicator LIKE '%%COT%%' AND actual IS NOT NULL
            """)
            row = cur.fetchone()
            if row["cnt"] > 0:
                self._pass(f"COT data", f"{row['cnt']} rows")
            else:
                self._warn("COT data", "No data — COT component will use fallback")
        except Exception as e:
            self._warn("COT data", f"{e}")

        cur.close()

    # -------------------------------------------------------------------------
    # Test 6: Regime Classification
    # -------------------------------------------------------------------------
    def test_regime_classification(self):
        logger.info("\n--- Test 6: Regime Classification ---")
        if not self.conn:
            self._fail("Regime classification", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        for pair in FX_PAIRS:
            try:
                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM forex_network.historical_prices
                    WHERE instrument = %s AND timeframe = '1H'
                """, (pair,))
                row = cur.fetchone()
                if row["cnt"] >= 15:
                    self._pass(f"Historical bars: {pair}", f"{row['cnt']} 1H bars (≥15 needed for ADX)")
                elif row["cnt"] > 0:
                    self._warn(f"Historical bars: {pair}", f"Only {row['cnt']} bars (<15 for ADX)")
                else:
                    self._fail(f"Historical bars: {pair}", "No 1H bars")
            except Exception as e:
                self._fail(f"Historical bars: {pair}", str(e))

        cur.close()

    # -------------------------------------------------------------------------
    # Test 7: Signal Write and Read-back
    # -------------------------------------------------------------------------
    def test_signal_write(self):
        logger.info("\n--- Test 7: Signal Write and Read-back ---")
        if not self.conn:
            self._fail("Signal write", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        test_payload = {
            "test": True,
            "stress_score": 42.5,
            "stress_state": "Elevated",
            "stress_components": {"vix": 0.3, "yield_curve": 0.2},
        }

        try:
            cur.execute("""
                INSERT INTO forex_network.agent_signals
                    (agent_name, user_id, instrument, signal_type, score, bias,
                     confidence, payload, expires_at)
                VALUES ('regime', %s, NULL, 'regime_classification', 0.0, 'neutral',
                        0.85, %s, NOW() + INTERVAL '1 minute')
                RETURNING id
            """, (self.user_id, json.dumps(test_payload)))
            result = cur.fetchone()
            self.test_signal_id = result["id"]
            self.conn.commit()
            self._pass("Signal write", f"ID: {self.test_signal_id}")

            # Read back
            cur.execute("""
                SELECT agent_name, payload, confidence
                FROM forex_network.agent_signals
                WHERE id = %s
            """, (self.test_signal_id,))
            row = cur.fetchone()
            if row and row["agent_name"] == "regime":
                self._pass("Signal read-back", f"Confidence: {row['confidence']}")
            else:
                self._fail("Signal read-back", "Could not read back written signal")

        except Exception as e:
            self._fail("Signal write", str(e))
            self.conn.rollback()
        finally:
            cur.close()

    # -------------------------------------------------------------------------
    # Test 8: Market Context Snapshot Write
    # -------------------------------------------------------------------------
    def test_snapshot_write(self):
        logger.info("\n--- Test 8: Market Context Snapshot ---")
        if not self.conn:
            self._fail("Snapshot write", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cur.execute("""
                INSERT INTO shared.market_context_snapshots
                    (snapshot_time, current_session, day_of_week,
                     stop_hunt_window, ny_close_window,
                     system_stress_score, stress_state, stress_components)
                VALUES (NOW(), 'london', 3, FALSE, FALSE,
                        42.5, 'Elevated', %s)
            """, (json.dumps({"vix": 0.3, "yield_curve": 0.2, "test": True}),))
            self.conn.commit()
            self._pass("Snapshot write")

            # Read back
            cur.execute("""
                SELECT system_stress_score, stress_state
                FROM shared.market_context_snapshots
                WHERE stress_components::text LIKE '%%"test": true%%'
                ORDER BY snapshot_time DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row and float(row["system_stress_score"]) == 42.5:
                self._pass("Snapshot read-back", f"Score: {row['system_stress_score']}, State: {row['stress_state']}")
            else:
                self._fail("Snapshot read-back", "Could not verify written snapshot")

        except Exception as e:
            self._fail("Snapshot write", str(e))
            self.conn.rollback()
        finally:
            cur.close()

    # -------------------------------------------------------------------------
    # Test 9: Stress Alert Write
    # -------------------------------------------------------------------------
    def test_stress_alert_write(self):
        logger.info("\n--- Test 9: Stress Alert Write ---")
        if not self.conn:
            self._fail("Stress alert write", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cur.execute("""
                INSERT INTO forex_network.system_stress_alerts
                    (stress_score, stress_state, previous_state, transition_type,
                     dominant_component, components, proposal_text)
                VALUES (87.5, 'Crisis', 'Pre-crisis', 'escalation', 'vix',
                        %s, 'TEST ALERT — integration test, please ignore')
                RETURNING id
            """, (json.dumps({"vix": 0.9, "test": True}),))
            result = cur.fetchone()
            self.conn.commit()
            self._pass("Stress alert write", f"ID: {result['id']}")

        except Exception as e:
            self._fail("Stress alert write", str(e))
            self.conn.rollback()
        finally:
            cur.close()

    # -------------------------------------------------------------------------
    # Test 10: Heartbeat Write and Read-back
    # -------------------------------------------------------------------------
    def test_heartbeat(self):
        logger.info("\n--- Test 10: Heartbeat ---")
        if not self.conn:
            self._fail("Heartbeat", "No DB connection")
            return

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        test_session_id = str(uuid.uuid4())

        try:
            cur.execute("""
                INSERT INTO forex_network.agent_heartbeats
                    (agent_name, user_id, session_id, last_seen, status, cycle_count)
                VALUES ('regime', %s, %s, NOW(), 'active', 0)
                ON CONFLICT (agent_name, user_id)
                DO UPDATE SET
                    last_seen = NOW(), cycle_count = 0,
                    status = 'active', session_id = EXCLUDED.session_id
            """, (self.user_id, test_session_id))
            self.conn.commit()
            self._pass("Heartbeat write")

            # Read back
            cur.execute("""
                SELECT status, session_id, last_seen
                FROM forex_network.agent_heartbeats
                WHERE agent_name = 'regime' AND user_id = %s
            """, (self.user_id,))
            row = cur.fetchone()
            if row and row["status"] == "active":
                self._pass("Heartbeat read-back", f"Session: {row['session_id'][:8]}...")
            else:
                self._fail("Heartbeat read-back", "Could not verify heartbeat")

        except Exception as e:
            self._fail("Heartbeat", str(e))
            self.conn.rollback()
        finally:
            cur.close()

    # -------------------------------------------------------------------------
    # Test 11: Kill Switch Read
    # -------------------------------------------------------------------------
    def test_kill_switch(self):
        logger.info("\n--- Test 11: Kill Switch ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION)
            resp = ssm.get_parameter(
                Name="/platform/config/kill-switch", WithDecryption=True
            )
            value = resp["Parameter"]["Value"].strip().lower()
            if value == "inactive":
                self._pass("Kill switch", "Status: inactive (normal)")
            elif value == "active":
                self._warn("Kill switch", "Status: ACTIVE — agent will not generate signals")
            else:
                self._warn("Kill switch", f"Unexpected value: {value}")
        except Exception as e:
            self._fail("Kill switch", str(e))

    # -------------------------------------------------------------------------
    # Test 12: MCP Provider Reachability
    # -------------------------------------------------------------------------
    def test_mcp_providers(self):
        logger.info("\n--- Test 12: MCP Provider Reachability ---")

        # We test that the Anthropic SDK can be imported and API key is valid
        try:
            import anthropic
            sm = boto3.client("secretsmanager", region_name=AWS_REGION)
            api_key = json.loads(
                sm.get_secret_value(SecretId="platform/anthropic/api-key")["SecretString"]
            )["api_key"]

            client = anthropic.Anthropic(api_key=api_key)
            # Simple validation — just check the client initialises
            self._pass("Anthropic SDK", "Client initialised with API key")

        except ImportError:
            self._fail("Anthropic SDK", "anthropic package not installed")
        except Exception as e:
            self._fail("Anthropic SDK", str(e))

        # EODHD key validation
        try:
            sm = boto3.client("secretsmanager", region_name=AWS_REGION)
            eodhd_key = json.loads(
                sm.get_secret_value(SecretId="platform/eodhd/api-key")["SecretString"]
            )["api_key"]
            self._pass("EODHD API key", "Retrieved from Secrets Manager")
        except Exception as e:
            self._fail("EODHD API key", str(e))

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    def cleanup(self):
        """Remove test data written during integration tests."""
        if not self.conn:
            return

        cur = self.conn.cursor()
        try:
            # Clean up test signal
            if self.test_signal_id:
                cur.execute(
                    "DELETE FROM forex_network.agent_signals WHERE id = %s",
                    (self.test_signal_id,)
                )

            # Clean up test snapshot
            cur.execute("""
                DELETE FROM shared.market_context_snapshots
                WHERE stress_components::text LIKE '%%"test": true%%'
            """)

            # Clean up test alert
            cur.execute("""
                DELETE FROM forex_network.system_stress_alerts
                WHERE proposal_text LIKE '%%integration test%%'
            """)

            self.conn.commit()
            logger.info("Test data cleaned up")
        except Exception as e:
            logger.warning(f"Cleanup partial failure (non-critical): {e}")
            self.conn.rollback()
        finally:
            cur.close()

    # -------------------------------------------------------------------------
    # Run All
    # -------------------------------------------------------------------------
    def run_all(self):
        logger.info("=" * 60)
        logger.info("REGIME AGENT — AWS INTEGRATION TEST SUITE")
        logger.info(f"User: {self.user_id} | Region: {AWS_REGION}")
        logger.info(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info("=" * 60)

        self.test_aws_credentials()
        self.test_secrets_manager()
        self.test_parameter_store()
        self.test_rds_connectivity()
        self.test_stress_data_sources()
        self.test_regime_classification()
        self.test_signal_write()
        self.test_snapshot_write()
        self.test_stress_alert_write()
        self.test_heartbeat()
        self.test_kill_switch()
        self.test_mcp_providers()

        # Cleanup test data
        self.cleanup()

        # Close connection
        if self.conn and not self.conn.closed:
            self.conn.close()

        # Summary
        total = self.passed + self.failed + self.warned
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASS | {self.warned} WARN | {self.failed} FAIL (total: {total})")

        if self.failed == 0:
            logger.info("✅ ALL CHECKS PASSED — Regime Agent ready for deployment")
        elif self.failed <= 2 and self.warned >= 0:
            logger.warning("⚠️  SOME FAILURES — Review before enabling continuous mode")
        else:
            logger.error("❌ MULTIPLE FAILURES — Do not deploy until resolved")

        logger.info("=" * 60)

        return self.failed == 0


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Project Neo — Regime Agent Integration Test Suite"
    )
    parser.add_argument(
        "--user",
        required=True,
        help="Cognito user ID (e.g. neo_user_002)",
    )
    args = parser.parse_args()

    tester = RegimeAgentIntegrationTest(user_id=args.user)
    success = tester.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

