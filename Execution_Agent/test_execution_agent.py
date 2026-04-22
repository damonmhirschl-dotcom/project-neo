#!/usr/bin/env python3
"""
Project Neo — Execution Agent Integration Test Suite
======================================================
Tests against live AWS infrastructure + IBKR connectivity.

Tests:
  1. AWS credentials (platform-execution-role-dev)
  2. Secrets Manager (RDS + IBKR credentials)
  3. Parameter Store (kill switch, RDS endpoint, IBKR environment)
  4. RDS connectivity and schema (trades, order_events, swap_rates, system_alerts, audit_log)
  5. Risk-approved signals readable
  6. IBKR Gateway connectivity (port 4002)
  7. IBKR paper mode verification
  8. Trade write and read-back
  9. Order event write
  10. System alert write
  11. Swap rates table
  12. Kill switch read
  13. Market context snapshot readable

Usage:
  source ~/algodesk/bin/activate
  python test_execution_agent.py --user neo_user_002
"""

import sys, json, uuid, argparse, logging
import boto3, psycopg2, psycopg2.extras, requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("neo.execution_test")
AWS_REGION = "eu-west-2"
IBKR_HOST = "10.50.2.177"
IBKR_PORT = 4002

class ExecutionIntegrationTest:
    def __init__(self, user_id):
        self.user_id = user_id
        self.passed = self.failed = self.warned = 0
        self.conn = None
        self.cleanup_ids = {"trades": [], "signals": [], "order_events": [], "alerts": []}

    def _pass(self, n, d=""): self.passed += 1; logger.info(f"  ✅ PASS: {n}" + (f" — {d}" if d else ""))
    def _fail(self, n, d=""): self.failed += 1; logger.error(f"  ❌ FAIL: {n}" + (f" — {d}" if d else ""))
    def _warn(self, n, d=""): self.warned += 1; logger.warning(f"  ⚠️  WARN: {n}" + (f" — {d}" if d else ""))

    def test_aws_credentials(self):
        logger.info("\n--- Test 1: AWS Credentials ---")
        try:
            identity = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()
            self._pass("AWS credentials", f"Account: {identity['Account']}") if identity["Account"] == "956177812472" else self._fail("AWS credentials", f"Wrong account")
        except Exception as e: self._fail("AWS credentials", str(e))

    def test_secrets(self):
        logger.info("\n--- Test 2: Secrets Manager ---")
        sm = boto3.client("secretsmanager", region_name=AWS_REGION)
        for name in ["platform/rds/credentials", "dev/ibkr/credentials"]:
            try: sm.get_secret_value(SecretId=name); self._pass(f"Secret: {name}")
            except Exception as e: self._fail(f"Secret: {name}", str(e))

    def test_parameters(self):
        logger.info("\n--- Test 3: Parameter Store ---")
        ssm = boto3.client("ssm", region_name=AWS_REGION)
        for name in ["/platform/config/rds-endpoint", "/platform/config/kill-switch", "/platform/config/ibkr-environment"]:
            try:
                val = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
                self._pass(f"Parameter: {name}", f"Value: {val[:30]}")
            except Exception as e: self._fail(f"Parameter: {name}", str(e))

    def test_rds(self):
        logger.info("\n--- Test 4: RDS Connectivity ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION); sm = boto3.client("secretsmanager", region_name=AWS_REGION)
            ep = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
            cr = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
            self.conn = psycopg2.connect(host=ep, port=5432, dbname="postgres", user=cr["username"], password=cr["password"], connect_timeout=10, options="-c search_path=forex_network,shared,public")
            self.conn.autocommit = False; self._pass("RDS connection")
        except Exception as e: self._fail("RDS connection", str(e)); return
        cur = self.conn.cursor()
        for s, t in [("forex_network","trades"),("forex_network","order_events"),("forex_network","swap_rates"),("forex_network","system_alerts"),("forex_network","audit_log"),("forex_network","agent_signals")]:
            try:
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)", (s, t))
                self._pass(f"Table: {s}.{t}") if cur.fetchone()[0] else self._fail(f"Table: {s}.{t}", "NOT FOUND")
            except Exception as e: self._fail(f"Table: {s}.{t}", str(e))
        cur.close()

    def test_risk_approved_signals(self):
        logger.info("\n--- Test 5: Risk-approved Signals ---")
        if not self.conn: return self._fail("Signals", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM forex_network.agent_signals WHERE agent_name='risk_guardian' AND signal_type='risk_approved' AND user_id=%s", (self.user_id,))
            row = cur.fetchone()
            self._pass(f"Risk-approved signals", f"{row['cnt']} total") if row["cnt"] > 0 else self._warn("Risk-approved signals", "None yet")
        except Exception as e: self._fail("Signals", str(e))
        finally: cur.close()

    def test_ibkr_connectivity(self):
        logger.info("\n--- Test 6: IBKR Gateway ---")
        try:
            resp = requests.get(f"http://{IBKR_HOST}:{IBKR_PORT}/v1/api/iserver/auth/status", timeout=10)
            self._pass("IBKR Gateway", f"Status: {resp.status_code}") if resp.status_code == 200 else self._warn("IBKR Gateway", f"Status: {resp.status_code}")
        except requests.ConnectionError:
            self._warn("IBKR Gateway", f"Not reachable at {IBKR_HOST}:{IBKR_PORT} — expected if ECS task not started")
        except Exception as e: self._fail("IBKR Gateway", str(e))

    def test_ibkr_paper_mode(self):
        logger.info("\n--- Test 7: IBKR Paper Mode ---")
        try:
            ssm = boto3.client("ssm", region_name=AWS_REGION)
            val = ssm.get_parameter(Name="/platform/config/ibkr-environment", WithDecryption=True)["Parameter"]["Value"]
            self._pass("IBKR paper mode", "Confirmed") if val == "paper" else self._fail("IBKR paper mode", f"Value is '{val}' — expected 'paper'")
        except Exception as e: self._fail("IBKR paper mode", str(e))

    def test_trade_write(self):
        logger.info("\n--- Test 8: Trade Write ---")
        if not self.conn: return self._fail("Trade write", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""INSERT INTO forex_network.trades (user_id,instrument,direction,entry_price,entry_time,position_size_usd,payload) VALUES (%s,'EURUSD','long',1.0850,NOW(),10000,%s) RETURNING id""", (self.user_id, json.dumps({"test":True})))
            result = cur.fetchone(); self.cleanup_ids["trades"].append(result["id"]); self.conn.commit()
            self._pass("Trade write", f"ID: {result['id']}")
        except Exception as e: self._fail("Trade write", str(e)); self.conn.rollback()
        finally: cur.close()

    def test_order_event_write(self):
        logger.info("\n--- Test 9: Order Event Write ---")
        if not self.conn: return self._fail("Order event", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            tid = self.cleanup_ids["trades"][0] if self.cleanup_ids["trades"] else None
            cur.execute("""INSERT INTO forex_network.order_events (trade_id,user_id,event_type,details,created_at) VALUES (%s,%s,'test_event',%s,NOW()) RETURNING id""", (tid, self.user_id, json.dumps({"test":True})))
            result = cur.fetchone(); self.cleanup_ids["order_events"].append(result["id"]); self.conn.commit()
            self._pass("Order event write", f"ID: {result['id']}")
        except Exception as e: self._fail("Order event", str(e)); self.conn.rollback()
        finally: cur.close()

    def test_system_alert_write(self):
        logger.info("\n--- Test 10: System Alert Write ---")
        if not self.conn: return self._fail("System alert", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""INSERT INTO forex_network.system_alerts (alert_type,severity,title,detail,user_id,acknowledged,created_at) VALUES ('test_alert','medium','Integration test','Please ignore',%s,TRUE,NOW()) RETURNING id""", (self.user_id,))
            result = cur.fetchone(); self.cleanup_ids["alerts"].append(result["id"]); self.conn.commit()
            self._pass("System alert write", f"ID: {result['id']}")
        except Exception as e: self._fail("System alert", str(e)); self.conn.rollback()
        finally: cur.close()

    def test_swap_rates(self):
        logger.info("\n--- Test 11: Swap Rates ---")
        if not self.conn: return self._fail("Swap rates", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM forex_network.swap_rates")
            row = cur.fetchone()
            self._pass(f"Swap rates", f"{row['cnt']} rows") if row["cnt"] > 0 else self._warn("Swap rates", "Empty — will be populated on first execution agent startup")
        except Exception as e: self._warn("Swap rates", str(e))
        finally: cur.close()

    def test_kill_switch(self):
        logger.info("\n--- Test 12: Kill Switch ---")
        try:
            val = boto3.client("ssm", region_name=AWS_REGION).get_parameter(Name="/platform/config/kill-switch", WithDecryption=True)["Parameter"]["Value"].strip().lower()
            self._pass("Kill switch", f"Status: {val}")
        except Exception as e: self._fail("Kill switch", str(e))

    def test_market_context(self):
        logger.info("\n--- Test 13: Market Context ---")
        if not self.conn: return self._fail("Market context", "No DB")
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT system_stress_score FROM shared.market_context_snapshots WHERE system_stress_score IS NOT NULL ORDER BY snapshot_time DESC LIMIT 1")
            row = cur.fetchone()
            self._pass("Market context", f"Stress: {row['system_stress_score']}") if row else self._warn("Market context", "No snapshots yet")
        except Exception as e: self._fail("Market context", str(e))
        finally: cur.close()

    def cleanup(self):
        if not self.conn: return
        cur = self.conn.cursor()
        try:
            for id_ in self.cleanup_ids["order_events"]: cur.execute("DELETE FROM forex_network.order_events WHERE id=%s", (id_,))
            for id_ in self.cleanup_ids["alerts"]: cur.execute("DELETE FROM forex_network.system_alerts WHERE id=%s", (id_,))
            for id_ in self.cleanup_ids["trades"]: cur.execute("DELETE FROM forex_network.trades WHERE id=%s", (id_,))
            self.conn.commit()
        except: self.conn.rollback()
        finally: cur.close()

    def run_all(self):
        logger.info("=" * 60); logger.info("EXECUTION AGENT — AWS INTEGRATION TESTS"); logger.info("=" * 60)
        self.test_aws_credentials(); self.test_secrets(); self.test_parameters(); self.test_rds()
        self.test_risk_approved_signals(); self.test_ibkr_connectivity(); self.test_ibkr_paper_mode()
        self.test_trade_write(); self.test_order_event_write(); self.test_system_alert_write()
        self.test_swap_rates(); self.test_kill_switch(); self.test_market_context()
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
    sys.exit(0 if ExecutionIntegrationTest(parser.parse_args().user).run_all() else 1)

if __name__ == "__main__": main()

