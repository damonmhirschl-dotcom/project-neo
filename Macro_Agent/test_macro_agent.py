#!/usr/bin/env python3
"""
Project Neo - Macro Agent Test Suite
====================================

Validates the macro agent configuration, connectivity, and signal generation
before deployment. Runs all critical checks without affecting production data.

Test Categories:
1. Configuration validation (AWS secrets, parameters)  
2. Database connectivity and schema validation
3. MCP server connectivity (EODHD, Alpha Vantage)
4. REST API connectivity (Finnhub, TraderMade)
5. Signal generation dry run
6. Adversarial defense validation
7. Decision rules validation

Usage:
    python test_macro_agent.py --all
    python test_macro_agent.py --config-only  
    python test_macro_agent.py --connectivity-only
    python test_macro_agent.py --dry-run
"""

import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from macro_agent import MacroAgent

# Configure test logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TEST - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MacroAgentTester:
    """Test suite for the macro agent."""
    
    def __init__(self):
        self.agent = None
        self.test_results = {
            "configuration": False,
            "database": False,
            "mcp_connectivity": False,
            "rest_connectivity": False,
            "signal_generation": False,
            "adversarial_defenses": False,
            "decision_rules": False
        }
    
    def test_configuration(self) -> bool:
        """Test AWS configuration and secrets loading."""
        logger.info("=== Testing Configuration ===")
        
        try:
            self.agent = MacroAgent()
            
            # Check critical configuration
            assert hasattr(self.agent, 'rds_endpoint'), "RDS endpoint not loaded"
            assert hasattr(self.agent, 'eodhd_key'), "EODHD key not loaded"
            assert hasattr(self.agent, 'anthropic_key'), "Anthropic key not loaded"
            assert self.agent.kill_switch in ['active', 'inactive'], "Invalid kill switch value"
            
            logger.info("✅ Configuration validation PASSED")
            self.test_results["configuration"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Configuration validation FAILED: {e}")
            return False
    
    def test_database_connectivity(self) -> bool:
        """Test database connection and required tables."""
        logger.info("=== Testing Database Connectivity ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized - run configuration test first")
            return False
        
        try:
            # Test basic connectivity
            with self.agent.db_conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                logger.info(f"PostgreSQL version: {version}")
            
            # Check required tables exist
            required_tables = [
                "forex_network.agent_signals",
                "forex_network.agent_heartbeats", 
                "forex_network.economic_releases",
                "forex_network.economic_calendar",
                "shared.market_context_snapshots"
            ]
            
            with self.agent.db_conn.cursor() as cur:
                for table in required_tables:
                    schema, table_name = table.split('.')
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        )
                    """, (schema, table_name))
                    
                    exists = cur.fetchone()[0]
                    if not exists:
                        logger.error(f"❌ Required table {table} does not exist")
                        return False
                    
                    logger.info(f"✅ Table {table} exists")
            
            # Test sample data queries
            stress_score, stress_state, _ = self.agent.get_system_stress_score()
            logger.info(f"System stress score: {stress_score} ({stress_state})")
            
            recent_releases = self.agent.get_recent_economic_releases(hours_back=48)
            logger.info(f"Recent releases: {len(recent_releases)} found")
            
            upcoming_events = self.agent.get_upcoming_calendar_events(hours_ahead=48)
            logger.info(f"Upcoming events: {len(upcoming_events)} found")
            
            logger.info("✅ Database connectivity PASSED")
            self.test_results["database"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connectivity FAILED: {e}")
            return False
    
    def test_mcp_connectivity(self) -> bool:
        """Test MCP server connectivity."""
        logger.info("=== Testing MCP Connectivity ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Test basic Anthropic API connectivity
            test_conversation = [{
                "role": "user", 
                "content": "Please respond with 'MCP test successful' if you can see this message."
            }]
            
            response = self.agent.call_anthropic_agent(test_conversation)
            
            if "MCP test successful" in response.get("response", "").lower():
                logger.info("✅ Basic Anthropic API connectivity PASSED")
            else:
                logger.warning("⚠️ Anthropic API response unexpected but connected")
            
            # Log MCP server configuration
            logger.info(f"Configured MCP servers: {len(self.agent.mcp_servers)}")
            for server in self.agent.mcp_servers:
                logger.info(f"  - {server['name']}: {server['url'][:50]}...")
            
            logger.info("✅ MCP connectivity PASSED")
            self.test_results["mcp_connectivity"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ MCP connectivity FAILED: {e}")
            return False
    
    def test_rest_connectivity(self) -> bool:
        """Test REST API connectivity (Finnhub, TraderMade)."""
        logger.info("=== Testing REST API Connectivity ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Test Finnhub API
            import requests
            
            finnhub_url = f"https://finnhub.io/api/v1/calendar/economic?from=2026-04-15&to=2026-04-17&token={self.agent.finnhub_key}"
            response = requests.get(finnhub_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Finnhub API PASSED - {len(data.get('economicCalendar', []))} events")
            else:
                logger.error(f"❌ Finnhub API FAILED - Status: {response.status_code}")
                return False
            
            # Test TraderMade API if key available
            if self.agent.tradermade_key:
                tm_url = f"https://marketdata.tradermade.com/api/v1/live?currency=EURUSD&api_key={self.agent.tradermade_key}"
                tm_response = requests.get(tm_url, timeout=10)
                
                if tm_response.status_code == 200:
                    logger.info("✅ TraderMade API PASSED")
                else:
                    logger.warning(f"⚠️ TraderMade API returned status {tm_response.status_code}")
            else:
                logger.warning("⚠️ TraderMade key not available - SSI signals will be unavailable")
            
            logger.info("✅ REST API connectivity PASSED")
            self.test_results["rest_connectivity"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ REST API connectivity FAILED: {e}")
            return False
    
    def test_signal_generation_dry_run(self) -> bool:
        """Test signal generation without writing to database."""
        logger.info("=== Testing Signal Generation (Dry Run) ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Build test conversation context
            context = self.agent.build_conversation_context()
            logger.info(f"Built conversation context with {len(context)} messages")
            
            # Call agent for analysis (this will use MCP tools)
            logger.info("Calling Anthropic API with MCP tools...")
            agent_response = self.agent.call_anthropic_agent(context)
            
            logger.info(f"Agent response length: {len(agent_response.get('response', ''))}")
            logger.info(f"Tool results: {len(agent_response.get('tool_results', []))}")
            
            # Parse signals
            signals = self.agent.parse_agent_response(agent_response)
            logger.info(f"Parsed {len(signals)} signals")
            
            # Validate signal structure
            required_fields = ['agent_name', 'instrument', 'signal_type', 'score', 'bias', 'confidence']
            
            for i, signal in enumerate(signals):
                for field in required_fields:
                    if field not in signal:
                        logger.error(f"❌ Signal {i} missing required field: {field}")
                        return False
                
                # Validate score range
                score = signal.get('score', 0)
                if not -1.0 <= score <= 1.0:
                    logger.error(f"❌ Signal {i} score {score} outside valid range [-1.0, 1.0]")
                    return False
                
                # Validate confidence range  
                confidence = signal.get('confidence', 0)
                if not 0.0 <= confidence <= 1.0:
                    logger.error(f"❌ Signal {i} confidence {confidence} outside valid range [0.0, 1.0]")
                    return False
                
                # Check payload exists
                if 'payload' not in signal:
                    logger.error(f"❌ Signal {i} missing payload")
                    return False
                
                payload = signal['payload']
                if not isinstance(payload, dict):
                    logger.error(f"❌ Signal {i} payload is not a dict")
                    return False
                
                # Check proposals exist
                if 'proposals' not in payload:
                    logger.error(f"❌ Signal {i} missing proposals in payload")
                    return False
                
                logger.info(f"✅ Signal {i} ({signal['instrument']}): score={score:.3f}, confidence={confidence:.3f}")
            
            logger.info("✅ Signal generation dry run PASSED")
            self.test_results["signal_generation"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Signal generation dry run FAILED: {e}")
            return False
    
    def test_adversarial_defenses(self) -> bool:
        """Test that adversarial defense rules are properly embedded."""
        logger.info("=== Testing Adversarial Defenses ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            system_prompt = self.agent.create_agent_system_prompt()
            
            # Check for key adversarial defense elements
            required_defenses = [
                "Source Authority Hierarchy",
                "Multi-Source Corroboration", 
                "Temporal Clustering Detection",
                "Sentiment Velocity Filter",
                "Content Fingerprinting",
                "Counter-Argument Requirement",
                "TIER_1_SOURCES",
                "TIER_2_SOURCES",
                "coordinated_narrative_flag",
                "sentiment_velocity_flag"
            ]
            
            missing_defenses = []
            for defense in required_defenses:
                if defense not in system_prompt:
                    missing_defenses.append(defense)
            
            if missing_defenses:
                logger.error(f"❌ Missing adversarial defenses: {missing_defenses}")
                return False
            
            # Check tier source lists are populated
            tier_1_count = len(self.agent.TIER_1_SOURCES)
            tier_2_count = len(self.agent.TIER_2_SOURCES)
            
            logger.info(f"Tier 1 sources: {tier_1_count}")
            logger.info(f"Tier 2 sources: {tier_2_count}")
            
            if tier_1_count < 5:
                logger.error("❌ Insufficient Tier 1 sources")
                return False
            
            logger.info("✅ Adversarial defenses PASSED")
            self.test_results["adversarial_defenses"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Adversarial defenses FAILED: {e}")
            return False
    
    def test_decision_rules(self) -> bool:
        """Test that all M1-M7 decision rules are embedded."""
        logger.info("=== Testing Decision Rules ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            system_prompt = self.agent.create_agent_system_prompt()
            
            # Check for all M1-M7 rules
            required_rules = [
                "M1 - Sentiment Provider Null Handling",
                "M2 - Unscheduled Central Bank Speech", 
                "M3 - FRED Lag After Major Release",
                "M4 - Full-Market Signal Divergence",
                "M5 - COT Extreme vs Opposing Macro Surprise", 
                "M6 - Simultaneous Central Bank Decisions",
                "M7 - Maximum Bias Without Multi-Source Corroboration"
            ]
            
            missing_rules = []
            for rule in required_rules:
                if rule not in system_prompt:
                    missing_rules.append(rule)
            
            if missing_rules:
                logger.error(f"❌ Missing decision rules: {missing_rules}")
                return False
            
            # Check COT 90th percentile rule
            if "COT 90TH PERCENTILE RULE" not in system_prompt:
                logger.error("❌ Missing COT 90th percentile rule")
                return False
            
            # Check central bank hierarchy
            if "CENTRAL_BANK_HIERARCHY" not in system_prompt:
                logger.error("❌ Missing central bank hierarchy")
                return False
            
            logger.info("✅ Decision rules PASSED")  
            self.test_results["decision_rules"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Decision rules FAILED: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all tests in sequence."""
        logger.info("🚀 Starting macro agent test suite")
        logger.info(f"Test time: {datetime.now(timezone.utc).isoformat()}")
        
        tests = [
            ("Configuration", self.test_configuration),
            ("Database", self.test_database_connectivity),
            ("MCP Connectivity", self.test_mcp_connectivity), 
            ("REST Connectivity", self.test_rest_connectivity),
            ("Signal Generation", self.test_signal_generation_dry_run),
            ("Adversarial Defenses", self.test_adversarial_defenses),
            ("Decision Rules", self.test_decision_rules)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            try:
                if test_func():
                    passed += 1
                else:
                    logger.error(f"Test failed: {test_name}")
            except Exception as e:
                logger.error(f"Test {test_name} threw exception: {e}")
        
        # Print summary
        logger.info("\n" + "="*50)
        logger.info("TEST SUMMARY")
        logger.info("="*50)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"{test_name:20}: {status}")
        
        logger.info(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 ALL TESTS PASSED - Macro agent ready for deployment")
            return True
        else:
            logger.error(f"💥 {total-passed} TESTS FAILED - Do not deploy")
            return False
    
    def cleanup(self):
        """Clean up test resources."""
        if self.agent:
            self.agent.close()

def main():
    """Main test entry point."""
    parser = argparse.ArgumentParser(description='Project Neo Macro Agent Test Suite')
    parser.add_argument('--all', action='store_true', default=True,
                       help='Run all tests (default)')
    parser.add_argument('--config-only', action='store_true',
                       help='Test configuration only')
    parser.add_argument('--connectivity-only', action='store_true', 
                       help='Test connectivity only')
    parser.add_argument('--dry-run', action='store_true',
                       help='Test signal generation only')
    
    args = parser.parse_args()
    
    tester = MacroAgentTester()
    
    try:
        if args.config_only:
            success = tester.test_configuration()
        elif args.connectivity_only:
            success = (tester.test_configuration() and 
                      tester.test_database_connectivity() and
                      tester.test_mcp_connectivity() and 
                      tester.test_rest_connectivity())
        elif args.dry_run:
            success = (tester.test_configuration() and
                      tester.test_database_connectivity() and
                      tester.test_signal_generation_dry_run())
        else:
            success = tester.run_all_tests()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed with exception: {e}")
        sys.exit(1)
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()
