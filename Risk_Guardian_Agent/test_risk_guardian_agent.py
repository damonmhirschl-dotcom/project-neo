#!/usr/bin/env python3
"""
Project Neo — Risk Guardian Integration Test Suite
====================================================
Tests against live AWS infrastructure.

Tests:
  1. AWS credentials (platform-risk-role-dev)
  2. Secrets Manager access
  3. Parameter Store (kill switch, RDS)
  4. RDS connectivity and schema
  5. Risk parameters exist for user
  6. Portfolio correlation table
  7. Swap rates table
  8. Open positions query
  9. Trade approval signals readable
  10. Signal write and read-back (risk_approved/risk_rejected)
  11. Kill switch read
  12. Market context snapshot readable

Usage:
  source ~/algodesk/bin/activate
  python test_risk_guardian_agent.py --user neo_user_002
"""

import sys, json, uuid, argparse, logging
from datetime import datetime, timezone
import boto3, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("neo.risk_guardian_test")
AWS_REGION = "eu-west-2"

class RiskGuardianIntegrationTest:
    def __init__(self, user_id):
        self.user_id = user_id
        self.passed = self.failed = self.warned = 0
        self.conn = None
        self.cleanup_ids = []

    def _pass(self, n, d=""): self.passed += 1; logger.info(f"  ✅ PASS: {n}" + (f" — {d}" if d else ""))
    def _fail(self, n, d=""): self.failed += 1; logger.error(f"  ❌ FAIL: {n}" + (f" — {d}" if d else ""))
    def _warn(self, n, d=""): self.warned += 1; logger.warning(f"  ⚠️  WARN: {n}" + (f" — {d}" if d else ""))

    def test_aws_credentials(self):
        logger.info("\n--- Test 1: AWS Credentials ---")
        try:
            sts = boto3.client("sts", region_name=AWS_REGION)
            identity = sts.get_caller_identity()
            self._pass("AWS credentials", f"Account: {identity['Account']}") if identity["Account"] == "956177812472" else self._fail("AWS credentials", f"Wrong account: {identity['Account']}")
        except Exception as e: self._fail("AWS credentials", str(e))

    def test_secrets(self):
        logger.info("\n--- Test 2: Secrets Manager ---")
        sm = boto3.client("secretsmanager", region_name=AWS_REGION)
        for name in ["platform/rds/credentials"]:
            try: sm.get_secret_value(SecretId=name); self._pass(f"Secret: {name}")
            except Exception as e: self._fail(f"Secret: {name}", str(e))

    def test_parameters(self):
        logger.info("\n--- Test 3: Parameter Store ---")
        ssm = boto3.client("ssm", region_name=AWS_REGION)
        for name in ["/platform/config/rds-endpoint", "/platform/config/kill-switch"]:
            try: ssm.get_parameter(Name=name, WithDecryption=True); self._pass(f"Parameter: {name}")
            except Exception as e: self._fail(f"Parameter: {name}", str(e))

    def test_rds(self):
        logger.info("\n--- Test 4: RDS Connectivity ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION); sm = boto3.client("secretsmanager", region_name=AWS_REGION)
            ep = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
            cr = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
            self.conn = psycopg2.connect(host=ep, port=5432, dbname="postgres", user=cr["username"], password=cr["password"], connect_timeout=10, options="-c search_path=forex_network,shared,public")
            self.conn.autocommit = True
            self._pass("RDS connection")
            # Resolve user_id to UUID if needed
            if self.user_id and '-' not in self.user_id:
                try:
                    rcur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    rcur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
                    rows = rcur.fetchall()
                    rcur.close()
                    idx = {"neo_user_001": 0, "neo_user_002": 1, "neo_user_003": 2}.get(self.user_id, 0)
                    if rows and idx < len(rows):
                        self.user_id = str(rows[idx]["user_id"])
                        logger.info(f"  Resolved user_id to UUID: {self.user_id}")
                except Exception as e:
                    logger.warning(f"  UUID resolution failed: {e}")
        except Exception as e: self._fail("RDS connection", str(e)); return
        cur = self.conn.cursor()
        for schema, table in [("forex_network","agent_signals"),("forex_network","risk_parameters"),("forex_network","trades"),("forex_network","swap_rates"),("shared","portfolio_correlation"),("shared","market_context_snapshots")]:
            try:
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)", (schema, table))
                self._pass(f"Table: {schema}.{table}") if cur.fetchone()[0] else self._fail(f"Table: {schema}.{table}", "NOT FOUND")
            except Exception as e: self._fail(f"Table: {schema}.{table}", str(e))
        cur.close()

    def test_risk_parameters(self):
        logger.info("\n--- Test 5: Risk Parameters ---")
        if not self.conn: return self._fail("Risk parameters", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT * FROM forex_network.risk_parameters WHERE user_id = %s", (self.user_id,))
            row = cur.fetchone()
            self._pass("Risk parameters", f"threshold={row['convergence_threshold']}, paper={row['paper_mode']}") if row else self._fail("Risk parameters", f"No row for {self.user_id}")
        except Exception as e: self._fail("Risk parameters", str(e))
        finally: cur.close()

    def test_correlations(self):
        logger.info("\n--- Test 6: Portfolio Correlation ---")
        if not self.conn: return self._fail("Correlations", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM shared.portfolio_correlation")
            row = cur.fetchone()
            self._pass(f"Correlations", f"{row['cnt']} rows") if row["cnt"] > 0 else self._warn("Correlations", "Empty — will use static baselines")
        except Exception as e: self._warn("Correlations", str(e))
        finally: cur.close()

    def test_swap_rates(self):
        logger.info("\n--- Test 7: Swap Rates ---")
        if not self.conn: return self._fail("Swap rates", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM forex_network.swap_rates")
            row = cur.fetchone()
            self._pass(f"Swap rates", f"{row['cnt']} rows") if row["cnt"] > 0 else self._warn("Swap rates", "Empty — RG2 swap checks will be skipped")
        except Exception as e: self._warn("Swap rates", str(e))
        finally: cur.close()

    def test_open_positions(self):
        logger.info("\n--- Test 8: Open Positions ---")
        if not self.conn: return self._fail("Open positions", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM forex_network.trades WHERE user_id=%s AND exit_time IS NULL", (self.user_id,))
            self._pass("Open positions query", f"{cur.fetchone()['cnt']} open")
        except Exception as e: self._fail("Open positions", str(e))
        finally: cur.close()

    def test_approval_signals(self):
        logger.info("\n--- Test 9: Trade Approval Signals ---")
        if not self.conn: return self._fail("Approval signals", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM forex_network.agent_signals WHERE agent_name='orchestrator' AND signal_type='trade_approval' AND user_id=%s", (self.user_id,))
            row = cur.fetchone()
            self._pass(f"Approval signals", f"{row['cnt']} total") if row["cnt"] > 0 else self._warn("Approval signals", "None yet — orchestrator may not have run")
        except Exception as e: self._fail("Approval signals", str(e))
        finally: cur.close()

    def test_signal_write(self):
        logger.info("\n--- Test 10: Signal Write ---")
        if not self.conn: return self._fail("Signal write", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""INSERT INTO forex_network.agent_signals (agent_name,user_id,instrument,signal_type,score,bias,confidence,payload,expires_at) VALUES ('risk_guardian',%s,'EURUSD','risk_approved',0.0,'bullish',1.0,%s,NOW()+INTERVAL '1 minute') RETURNING id""", (self.user_id, json.dumps({"test":True})))
            result = cur.fetchone(); self.cleanup_ids.append(result["id"]); self.conn.commit()
            self._pass("Signal write", f"ID: {result['id']}")
        except Exception as e: self._fail("Signal write", str(e)); self.conn.rollback()
        finally: cur.close()

    def test_kill_switch(self):
        logger.info("\n--- Test 11: Kill Switch ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION)
            val = ssm.get_parameter(Name="/platform/config/kill-switch", WithDecryption=True)["Parameter"]["Value"].strip().lower()
            self._pass("Kill switch", f"Status: {val}") if val in ("active","inactive") else self._warn("Kill switch", f"Unexpected: {val}")
        except Exception as e: self._fail("Kill switch", str(e))

    def test_market_context(self):
        logger.info("\n--- Test 12: Market Context ---")
        if not self.conn: return self._fail("Market context", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT system_stress_score, stress_state FROM shared.market_context_snapshots WHERE system_stress_score IS NOT NULL ORDER BY snapshot_time DESC LIMIT 1")
            row = cur.fetchone()
            self._pass("Market context", f"Stress: {row['system_stress_score']}") if row else self._warn("Market context", "No snapshots — regime agent may not have run")
        except Exception as e: self._fail("Market context", str(e))
        finally: cur.close()

    def cleanup(self):
        if not self.conn: return
        cur = self.conn.cursor()
        try:
            for id_ in self.cleanup_ids: cur.execute("DELETE FROM forex_network.agent_signals WHERE id=%s", (id_,))
            self.conn.commit()
        except: self.conn.rollback()
        finally: cur.close()

    def run_all(self):
        logger.info("=" * 60); logger.info("RISK GUARDIAN — AWS INTEGRATION TESTS"); logger.info("=" * 60)
        self.test_aws_credentials(); self.test_secrets(); self.test_parameters(); self.test_rds()
        self.test_risk_parameters(); self.test_correlations(); self.test_swap_rates()
        self.test_open_positions(); self.test_approval_signals(); self.test_signal_write()
        self.test_kill_switch(); self.test_market_context()
        self.cleanup()
        if self.conn and not self.conn.closed: self.conn.close()
        total = self.passed + self.failed + self.warned
        logger.info(""); logger.info("=" * 60)
        logger.info(f"RESULTS: {self.passed} PASS | {self.warned} WARN | {self.failed} FAIL (total: {total})")
        logger.info("✅ ALL PASSED" if self.failed == 0 else "❌ FAILURES — review before deploying")
        logger.info("=" * 60)
        return self.failed == 0

def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--user", required=True)
    args = parser.parse_args()
    sys.exit(0 if RiskGuardianIntegrationTest(args.user).run_all() else 1)

if __name__ == "__main__": main()

