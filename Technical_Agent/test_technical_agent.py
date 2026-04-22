#!/usr/bin/env python3
"""
Project Neo - Technical Agent Test Suite
========================================

Validates the technical agent configuration, connectivity, indicator calculations,
and signal generation before deployment.

Test Categories:
1. Configuration validation (AWS secrets, parameters)
2. Database connectivity and historical data access  
3. Polygon API connectivity and live price retrieval
4. Technical indicator calculations
5. Multi-timeframe analysis and T1-T2 rules
6. Risk management calculations (ATR stops, R:R ratios)
7. Spread validation and adversarial defenses
8. Signal generation dry run

Usage:
    python test_technical_agent.py --all
    python test_technical_agent.py --indicators-only
    python test_technical_agent.py --price-data-only
    python test_technical_agent.py --dry-run
"""

import sys
import json
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from technical_agent import TechnicalAgent

# Configure test logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TEST - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechnicalAgentTester:
    """Test suite for the technical agent."""
    
    def __init__(self):
        self.agent = None
        self.test_results = {
            "configuration": False,
            "database": False,
            "polygon_api": False,
            "indicators": False,
            "timeframe_analysis": False,
            "risk_management": False,
            "spread_validation": False,
            "signal_generation": False
        }
    
    def test_configuration(self) -> bool:
        """Test AWS configuration and secrets loading."""
        logger.info("=== Testing Configuration ===")
        
        try:
            self.agent = TechnicalAgent()
            
            # Check critical configuration
            assert hasattr(self.agent, 'rds_endpoint'), "RDS endpoint not loaded"
            assert hasattr(self.agent, 'polygon_key'), "Polygon key not loaded"
            assert hasattr(self.agent, 'anthropic_key'), "Anthropic key not loaded"
            assert self.agent.kill_switch in ['active', 'inactive'], "Invalid kill switch value"
            
            # Check constants are properly loaded
            assert len(self.agent.PAIRS) == 7, f"Expected 7 pairs, got {len(self.agent.PAIRS)}"
            assert "EURUSD" in self.agent.PAIRS, "EURUSD not in pairs list"
            
            logger.info(f"Loaded pairs: {self.agent.PAIRS}")
            logger.info(f"Session alignment rules: {len(self.agent.SESSION_PAIR_ALIGNMENT)} sessions")
            
            logger.info("✅ Configuration validation PASSED")
            self.test_results["configuration"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Configuration validation FAILED: {e}")
            return False
    
    def test_database_connectivity(self) -> bool:
        """Test database connection and historical data access."""
        logger.info("=== Testing Database Connectivity ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Test basic connectivity
            with self.agent.db_conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                logger.info(f"PostgreSQL version: {version}")
            
            # Test historical bars access
            test_pair = "EURUSD"
            df_1h = self.agent.get_historical_bars(test_pair, "1H", 50)
            df_15m = self.agent.get_historical_bars(test_pair, "15M", 50)
            
            logger.info(f"Historical bars - 1H: {len(df_1h)} bars, 15M: {len(df_15m)} bars")
            
            if df_1h.empty:
                logger.error(f"❌ No 1H historical data for {test_pair}")
                return False
            
            # Test price metrics access
            price_metrics = self.agent.get_price_metrics(test_pair, "1H", 20)
            logger.info(f"Price metrics: {len(price_metrics)} rows")
            
            if not price_metrics.empty:
                latest_atr = price_metrics['atr_14'].iloc[-1]
                logger.info(f"Latest ATR(14) for {test_pair}: {latest_atr}")
            
            # Test historical spreads loading
            logger.info(f"Historical spreads loaded for {len(self.agent.historical_spreads)} pairs")
            if test_pair in self.agent.historical_spreads:
                spreads = self.agent.historical_spreads[test_pair]
                logger.info(f"EUR/USD spreads: {spreads}")
            
            logger.info("✅ Database connectivity PASSED")
            self.test_results["database"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connectivity FAILED: {e}")
            return False
    
    def test_polygon_api(self) -> bool:
        """Test Polygon API connectivity and live price retrieval."""
        logger.info("=== Testing Polygon API ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Test live price retrieval
            live_prices = self.agent.get_polygon_live_prices()
            
            if not live_prices:
                logger.error("❌ No live prices retrieved from Polygon")
                return False
            
            logger.info(f"Retrieved live prices for {len(live_prices)} pairs")
            
            # Validate price data structure
            for pair, price_data in live_prices.items():
                required_fields = ["bid", "ask", "spread"]
                missing_fields = [field for field in required_fields if field not in price_data]
                
                if missing_fields:
                    logger.error(f"❌ {pair} missing fields: {missing_fields}")
                    return False
                
                # Validate reasonable values
                bid = price_data.get("bid")
                ask = price_data.get("ask")
                spread = price_data.get("spread")
                
                if bid and ask and spread:
                    if spread <= 0 or spread > 0.01:  # Unreasonable spread
                        logger.warning(f"⚠️ {pair} unusual spread: {spread}")
                    
                    if bid <= 0 or ask <= 0:
                        logger.error(f"❌ {pair} invalid prices: bid={bid}, ask={ask}")
                        return False
                    
                    logger.info(f"✅ {pair}: bid={bid}, ask={ask}, spread={spread:.5f}")
            
            logger.info("✅ Polygon API PASSED")
            self.test_results["polygon_api"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Polygon API FAILED: {e}")
            return False
    
    def test_technical_indicators(self) -> bool:
        """Test technical indicator calculations."""
        logger.info("=== Testing Technical Indicators ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            test_pair = "EURUSD"
            df = self.agent.get_historical_bars(test_pair, "1H", 200)
            
            if df.empty:
                logger.error(f"❌ No historical data for indicator testing")
                return False
            
            # Test indicator calculation
            indicators = self.agent.calculate_technical_indicators(df)
            
            if not indicators:
                logger.error("❌ No indicators calculated")
                return False
            
            # Check key indicators are present
            key_indicators = ["adx", "rsi", "macd", "current_price"]
            missing_indicators = [ind for ind in key_indicators if ind not in indicators]
            
            if missing_indicators:
                logger.error(f"❌ Missing indicators: {missing_indicators}")
                return False
            
            # Validate indicator ranges
            rsi = indicators.get("rsi", 0)
            adx = indicators.get("adx", 0)
            
            if not (0 <= rsi <= 100):
                logger.error(f"❌ RSI out of range: {rsi}")
                return False
            
            if not (0 <= adx <= 100):
                logger.error(f"❌ ADX out of range: {adx}")
                return False
            
            logger.info(f"✅ Indicators calculated - RSI: {rsi:.1f}, ADX: {adx:.1f}")
            logger.info(f"Available indicators: {list(indicators.keys())}")
            
            logger.info("✅ Technical indicators PASSED")
            self.test_results["indicators"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Technical indicators FAILED: {e}")
            return False
    
    def test_timeframe_analysis(self) -> bool:
        """Test multi-timeframe analysis and T2 rule implementation."""
        logger.info("=== Testing Timeframe Analysis ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            test_pair = "EURUSD"
            
            # Get data for all timeframes
            df_1d = self.agent.get_historical_bars(test_pair, "1D", 50)
            df_1h = self.agent.get_historical_bars(test_pair, "1H", 100)
            df_15m = self.agent.get_historical_bars(test_pair, "15M", 100)
            
            if df_1h.empty:
                logger.error("❌ Insufficient data for timeframe analysis")
                return False
            
            # Test timeframe alignment analysis
            alignment = self.agent.analyze_timeframe_alignment(df_1d, df_1h, df_15m)
            
            required_fields = ["timeframe_alignment", "final_direction", "confidence_multiplier"]
            missing_fields = [field for field in required_fields if field not in alignment]
            
            if missing_fields:
                logger.error(f"❌ Missing alignment fields: {missing_fields}")
                return False
            
            # Validate alignment values
            valid_alignments = ["full", "partial_1D_1H", "partial_1H_15M", "conflict", "insufficient", "unknown"]
            if alignment["timeframe_alignment"] not in valid_alignments:
                logger.error(f"❌ Invalid alignment: {alignment['timeframe_alignment']}")
                return False
            
            valid_directions = ["bullish", "bearish", "neutral"]
            if alignment["final_direction"] not in valid_directions:
                logger.error(f"❌ Invalid direction: {alignment['final_direction']}")
                return False
            
            confidence = alignment["confidence_multiplier"]
            if not (0 <= confidence <= 1.0):
                logger.error(f"❌ Invalid confidence multiplier: {confidence}")
                return False
            
            logger.info(f"✅ Timeframe alignment: {alignment['timeframe_alignment']}")
            logger.info(f"✅ Final direction: {alignment['final_direction']}")
            logger.info(f"✅ Confidence multiplier: {confidence}")
            
            logger.info("✅ Timeframe analysis PASSED")
            self.test_results["timeframe_analysis"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Timeframe analysis FAILED: {e}")
            return False
    
    def test_risk_management(self) -> bool:
        """Test ATR-based stop calculation and R:R ratios."""
        logger.info("=== Testing Risk Management ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            test_pair = "EURUSD"
            test_price = 1.0850
            
            # Get price metrics for ATR calculation
            price_metrics = self.agent.get_price_metrics(test_pair, "1H", 50)
            
            if price_metrics.empty:
                logger.warning("⚠️ No price metrics available, using fallback")
                # Create fallback metrics for testing
                price_metrics = pd.DataFrame({
                    'atr_14': [0.0012] * 20,
                    'ts': pd.date_range('2026-04-15', periods=20, freq='H')
                })
            
            # Test ATR stop calculation for different profiles
            profiles = ["conservative", "balanced", "aggressive"]
            
            for profile in profiles:
                stop_loss, target, rr_ratio = self.agent.calculate_atr_stop_and_target(
                    test_pair, "long", test_price, price_metrics, profile
                )
                
                if stop_loss is None or target is None or rr_ratio is None:
                    logger.error(f"❌ ATR calculation failed for {profile} profile")
                    return False
                
                # Validate reasonable values
                if stop_loss >= test_price:  # Long position
                    logger.error(f"❌ Invalid stop loss for long: {stop_loss} >= {test_price}")
                    return False
                
                if target <= test_price:  # Long position
                    logger.error(f"❌ Invalid target for long: {target} <= {test_price}")
                    return False
                
                if rr_ratio <= 0:
                    logger.error(f"❌ Invalid R:R ratio: {rr_ratio}")
                    return False
                
                logger.info(f"✅ {profile}: stop={stop_loss:.4f}, target={target:.4f}, R:R={rr_ratio:.2f}")
            
            # Test spread-to-signal ratio check
            test_spread = 0.0001
            expected_pips = 50
            
            for profile in profiles:
                ratio_ok = self.agent.check_spread_to_signal_ratio(expected_pips, test_spread, profile)
                min_ratio = self.agent.MIN_SIGNAL_SPREAD_RATIOS[profile]
                expected_ok = expected_pips > (test_spread * min_ratio)
                
                if ratio_ok != expected_ok:
                    logger.error(f"❌ Spread ratio check failed for {profile}")
                    return False
                
                logger.info(f"✅ {profile} spread ratio check: {ratio_ok} (expected: {expected_ok})")
            
            logger.info("✅ Risk management PASSED")
            self.test_results["risk_management"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Risk management FAILED: {e}")
            return False
    
    def test_spread_validation(self) -> bool:
        """Test spread validation and adversarial defenses."""
        logger.info("=== Testing Spread Validation ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Create mock live prices with different spread scenarios
            mock_prices = {
                "EURUSD": {"bid": 1.0850, "ask": 1.0851, "spread": 0.0001},  # Normal
                "GBPUSD": {"bid": 1.2500, "ask": 1.2520, "spread": 0.0020},  # Wide spread
                "USDJPY": {"bid": 150.00, "ask": 150.05, "spread": 0.05}     # Normal for JPY
            }
            
            session = "london"
            
            # Test spread validation
            spread_validation = self.agent.validate_spreads(mock_prices, session)
            
            logger.info(f"Spread validation results: {spread_validation}")
            
            # Test session detection
            current_session = self.agent.get_current_session()
            valid_sessions = ["london", "overlap", "newyork", "asian", "off_hours"]
            
            if current_session not in valid_sessions:
                logger.error(f"❌ Invalid session detected: {current_session}")
                return False
            
            logger.info(f"✅ Current session: {current_session}")
            
            # Test session-pair alignment weighting
            test_pairs = ["EURUSD", "USDJPY", "AUDUSD"]
            for pair in test_pairs:
                weight = self.agent.get_session_pair_weight(pair, current_session)
                if not (0.5 <= weight <= 1.0):
                    logger.error(f"❌ Invalid weight for {pair} in {current_session}: {weight}")
                    return False
                
                logger.info(f"✅ {pair} weight in {current_session}: {weight}")
            
            logger.info("✅ Spread validation PASSED")
            self.test_results["spread_validation"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Spread validation FAILED: {e}")
            return False
    
    def test_signal_generation_dry_run(self) -> bool:
        """Test complete signal generation without writing to database."""
        logger.info("=== Testing Signal Generation (Dry Run) ===")
        
        if not self.agent:
            logger.error("❌ Agent not initialized")
            return False
        
        try:
            # Build conversation context
            context = self.agent.build_conversation_context()
            logger.info(f"Built conversation context with {len(context)} messages")
            
            # Call agent for analysis
            logger.info("Calling Anthropic API for technical analysis...")
            agent_response = self.agent.call_anthropic_agent(context)
            
            logger.info(f"Agent response length: {len(agent_response.get('response', ''))}")
            
            # Parse signals
            signals = self.agent.parse_agent_response(agent_response)
            logger.info(f"Parsed {len(signals)} signals")
            
            # Validate signal structure
            required_fields = ['agent_name', 'instrument', 'signal_type', 'score', 'bias', 'confidence']
            
            for i, signal in enumerate(signals):
                # Check required fields
                for field in required_fields:
                    if field not in signal:
                        logger.error(f"❌ Signal {i} missing required field: {field}")
                        return False
                
                # Validate ranges
                score = signal.get('score', 0)
                if not -1.0 <= score <= 1.0:
                    logger.error(f"❌ Signal {i} score {score} outside valid range")
                    return False
                
                confidence = signal.get('confidence', 0)
                if not 0.0 <= confidence <= 1.0:
                    logger.error(f"❌ Signal {i} confidence {confidence} outside valid range")
                    return False
                
                # Check payload structure
                payload = signal.get('payload', {})
                if not isinstance(payload, dict):
                    logger.error(f"❌ Signal {i} payload is not a dict")
                    return False
                
                # Check for proposals
                proposals = payload.get('proposals', [])
                if not isinstance(proposals, list):
                    logger.error(f"❌ Signal {i} proposals is not a list")
                    return False
                
                logger.info(f"✅ Signal {i} ({signal['instrument']}): score={score:.3f}, confidence={confidence:.3f}")
            
            logger.info("✅ Signal generation dry run PASSED")
            self.test_results["signal_generation"] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Signal generation dry run FAILED: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all tests in sequence."""
        logger.info("🚀 Starting technical agent test suite")
        logger.info(f"Test time: {datetime.now(timezone.utc).isoformat()}")
        
        tests = [
            ("Configuration", self.test_configuration),
            ("Database", self.test_database_connectivity),
            ("Polygon API", self.test_polygon_api),
            ("Technical Indicators", self.test_technical_indicators),
            ("Timeframe Analysis", self.test_timeframe_analysis),
            ("Risk Management", self.test_risk_management),
            ("Spread Validation", self.test_spread_validation),
            ("Signal Generation", self.test_signal_generation_dry_run)
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
        logger.info("TECHNICAL AGENT TEST SUMMARY")
        logger.info("="*50)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"{test_name:20}: {status}")
        
        logger.info(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 ALL TESTS PASSED - Technical agent ready for deployment")
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
    parser = argparse.ArgumentParser(description='Project Neo Technical Agent Test Suite')
    parser.add_argument('--all', action='store_true', default=True,
                       help='Run all tests (default)')
    parser.add_argument('--indicators-only', action='store_true',
                       help='Test indicator calculations only')
    parser.add_argument('--price-data-only', action='store_true',
                       help='Test price data and Polygon API only')
    parser.add_argument('--dry-run', action='store_true',
                       help='Test signal generation only')
    
    args = parser.parse_args()
    
    tester = TechnicalAgentTester()
    
    try:
        if args.indicators_only:
            success = (tester.test_configuration() and
                      tester.test_database_connectivity() and
                      tester.test_technical_indicators() and
                      tester.test_timeframe_analysis() and
                      tester.test_risk_management())
        elif args.price_data_only:
            success = (tester.test_configuration() and
                      tester.test_database_connectivity() and
                      tester.test_polygon_api() and
                      tester.test_spread_validation())
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

