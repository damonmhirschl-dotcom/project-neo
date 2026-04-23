#!/usr/bin/env python3
"""
Project Neo - Macro Agent v1.0
=====================================

The macro agent analyzes economic releases, central bank policy, news sentiment, 
and produces macro bias scores (-1.0 to +1.0) for each of the 7 currency pairs.

Architecture:
- EODHD MCP v2 OAuth: Primary sentiment and news source
- Alpha Vantage MCP: Fallback sentiment and macro indicators  
- Finnhub REST API: Economic calendar
- TraderMade REST API: SSI contrarian signals (fallback - MCP not ready)
- RDS: FRED economic data, recent releases, system stress score

Signal Output:
- Score: -1.0 (strongly bearish) to +1.0 (strongly bullish)
- Confidence: 0.0 to 1.0
- Expiry: NOW() + 20 minutes
- Proposals: Mandatory structured suggestions for human review

Adversarial Defenses:
- 17 rules embedded in system prompt (Attack Surfaces 1-4)
- Source authority hierarchy (Tier 1/2/3 weighting)
- Multi-source corroboration for extreme signals
- Temporal clustering and sentiment velocity filters

Decision Rules:
- M1-M7: All macro agent decision rules embedded
- COT 90th percentile rule: 0.5x weight on bullish signals when positioning extreme
- Central bank hierarchy: Fed+ECB for EUR/USD, Fed+BOE for GBP/USD, etc.

Build Date: April 16, 2026
"""

import os
import sys
import json
import uuid
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError
import anthropic

sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.market_hours import get_market_state
from shared.agent_state import save_state, load_state, log_loaded_state_summary, AGENT_SCOPE_USER_ID
from shared.score_trajectory import get_recent_trajectory, analyse_trajectory, format_trajectory_for_prompt
from shared.schema_validator import validate_schema
from shared.system_events import log_event

EXPECTED_TABLES = {
    "forex_network.agent_signals":     ["agent_name", "instrument", "signal_type", "score",
                                        "bias", "confidence", "payload", "expires_at", "user_id"],
    "forex_network.agent_heartbeats":  ["agent_name", "user_id", "last_seen", "status", "cycle_count"],
    "forex_network.economic_releases": ["indicator", "release_time", "actual", "forecast",
                                        "previous", "surprise_pct", "country"],
    "forex_network.economic_calendar": ["country", "indicator", "scheduled_time", "forecast",
                                        "previous", "importance", "resolved"],
    "forex_network.sentiment_ssi":     ["instrument", "long_pct", "short_pct",
                                        "contrarian_score", "ts"],
    "shared.market_context_snapshots": ["system_stress_score", "stress_state", "snapshot_time"],
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def log_api_call(db_conn, provider, endpoint, agent_name, success,
                 response_time_ms, error_type=None, pairs_returned=0,
                 data_age_seconds=0):
    try:
        cur = db_conn.cursor()
        cur.execute("""
            INSERT INTO forex_network.api_call_log
                (provider, endpoint, agent_name, success, response_time_ms,
                 error_type, pairs_returned, data_age_seconds, called_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (provider, endpoint, agent_name, success, response_time_ms,
              error_type, pairs_returned, data_age_seconds))
        db_conn.commit()
    except Exception:
        try:
            db_conn.rollback()
        except Exception:
            pass


class MacroAgent:
    """
    Project Neo Macro Agent
    
    Analyzes macro economic conditions, news sentiment, and central bank policy
    to generate directional bias signals for 7 major FX pairs.
    """
    
    # Configuration constants
    AGENT_NAME = "macro"
    CYCLE_INTERVAL_MINUTES = 15
    SIGNAL_EXPIRY_MINUTES = 25
    AWS_REGION = "eu-west-2"
    
    # FX pairs and their central bank hierarchies
    PAIRS = [
    # USD pairs
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    # Cross pairs confirmed on IG demo 2026-04-22
    "EURGBP", "EURJPY", "GBPJPY", "EURCHF", "GBPCHF",
    "EURAUD", "GBPAUD", "EURCAD", "GBPCAD",
    "AUDNZD", "AUDJPY", "CADJPY", "NZDJPY",
]

    # Per-currency scoring — 8 currencies scored independently, 20 pairs derived
    CURRENCIES = ['EUR', 'GBP', 'USD', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD']

    PAIR_DERIVATION = {
        'EURUSD': ('EUR', 'USD'), 'GBPUSD': ('GBP', 'USD'),
        'USDJPY': ('USD', 'JPY'), 'USDCHF': ('USD', 'CHF'),
        'AUDUSD': ('AUD', 'USD'), 'USDCAD': ('USD', 'CAD'),
        'NZDUSD': ('NZD', 'USD'),
        'EURGBP': ('EUR', 'GBP'), 'EURJPY': ('EUR', 'JPY'),
        'GBPJPY': ('GBP', 'JPY'), 'EURCHF': ('EUR', 'CHF'),
        'GBPCHF': ('GBP', 'CHF'), 'EURAUD': ('EUR', 'AUD'),
        'GBPAUD': ('GBP', 'AUD'), 'EURCAD': ('EUR', 'CAD'),
        'GBPCAD': ('GBP', 'CAD'), 'AUDNZD': ('AUD', 'NZD'),
        'AUDJPY': ('AUD', 'JPY'), 'CADJPY': ('CAD', 'JPY'),
        'NZDJPY': ('NZD', 'JPY'),
    }

    USD_PAIRS = {
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
    }
    
    CENTRAL_BANK_HIERARCHY = {
        "EURUSD": {"primary_cbs": ["Fed", "ECB"], "key_releases": ["US NFP", "US CPI", "US GDP", "ECB rate decision", "HICP", "PMI"]},
        "GBPUSD": {"primary_cbs": ["Fed", "BOE"], "key_releases": ["US NFP", "US CPI", "UK CPI", "UK employment", "BOE rate decision"]},
        "USDJPY": {"primary_cbs": ["Fed", "BOJ"], "key_releases": ["US data dominant", "BOJ intervention risk", "yield differential"]},
        "USDCHF": {"primary_cbs": ["Fed", "SNB"], "key_releases": ["US data dominant", "SNB rarely acts but moves are sharp"]},
        "AUDUSD": {"primary_cbs": ["Fed", "RBA"], "key_releases": ["Australian employment", "CPI", "RBA", "Chinese PMI", "trade data"]},
        "USDCAD": {"primary_cbs": ["Fed", "BOC"], "key_releases": ["Canadian employment", "CPI", "crude oil price direction"]},
        "NZDUSD": {"primary_cbs": ["Fed", "RBNZ"], "key_releases": ["NZ CPI", "employment", "RBNZ rate decisions"]},
        # Cross pairs
        "EURGBP": {"primary_cbs": ["ECB", "BOE"], "key_releases": ["ECB rate", "BOE rate", "UK/EU macro"]},
        "EURJPY": {"primary_cbs": ["ECB", "BOJ"], "key_releases": ["ECB rate", "BOJ policy", "yield differential"]},
        "GBPJPY": {"primary_cbs": ["BOE", "BOJ"], "key_releases": ["BOE rate", "BOJ policy", "risk sentiment"]},
        "EURCHF": {"primary_cbs": ["ECB", "SNB"], "key_releases": ["ECB rate", "SNB policy", "safe-haven flow"]},
        "GBPCHF": {"primary_cbs": ["BOE", "SNB"], "key_releases": ["BOE rate", "SNB policy", "safe-haven flow"]},
        "EURAUD": {"primary_cbs": ["ECB", "RBA"], "key_releases": ["ECB rate", "RBA rate", "Chinese PMI", "iron ore"]},
        "GBPAUD": {"primary_cbs": ["BOE", "RBA"], "key_releases": ["BOE rate", "RBA rate", "commodity prices"]},
        "EURCAD": {"primary_cbs": ["ECB", "BOC"], "key_releases": ["ECB rate", "BOC rate", "crude oil"]},
        "GBPCAD": {"primary_cbs": ["BOE", "BOC"], "key_releases": ["BOE rate", "BOC rate", "crude oil"]},
        "AUDNZD": {"primary_cbs": ["RBA", "RBNZ"], "key_releases": ["RBA vs RBNZ divergence", "AU/NZ macro"]},
        "AUDJPY": {"primary_cbs": ["RBA", "BOJ"], "key_releases": ["risk sentiment", "carry trade", "commodity prices"]},
        "CADJPY": {"primary_cbs": ["BOC", "BOJ"], "key_releases": ["crude oil", "carry trade", "risk sentiment"]},
        "NZDJPY": {"primary_cbs": ["RBNZ", "BOJ"], "key_releases": ["risk sentiment", "NZ macro", "carry trade"]}
    }
    
    # Source authority hierarchy for adversarial defense
    TIER_1_SOURCES = [
        "reuters", "bloomberg", "afp", "socgen", "rabobank", "ing", "jpmorgan", 
        "goldman", "fed", "ecb", "boe", "boj", "snb", "rba", "boc", "rbnz"
    ]
    
    TIER_2_SOURCES = [
        "ft", "wsj", "financial times", "fxstreet", "marketwatch", "cnbc"
    ]
    
    # Everything else defaults to Tier 3 (0.4x weight)
    
    def __init__(self, user_id: str = "neo_user_002", dry_run: bool = False):
        """Initialize the macro agent with AWS secrets and database connection."""
        self.session_id = str(uuid.uuid4())
        self.cycle_count = 0
        self.user_id = user_id
        self.dry_run = dry_run
        
        # Initialize AWS clients
        self.ssm_client = boto3.client('ssm', region_name=self.AWS_REGION)
        self.secrets_client = boto3.client('secretsmanager', region_name=self.AWS_REGION)
        
        # Load configuration and secrets
        self._load_configuration()
        
        # Initialize database connection
        self._init_database()

        # Restore cycle_count from agent_state (persists across restarts)
        saved_count = load_state('macro', AGENT_SCOPE_USER_ID, 'cycle_count', default=0)
        if isinstance(saved_count, int) and saved_count > 0:
            self.cycle_count = saved_count
            logger.info(f"[state] Restored cycle_count={self.cycle_count} from agent_state")

        # Initialize Anthropic client with MCP servers
        self._init_anthropic_client()

        logger.info(f"Macro agent initialized - Session ID: {self.session_id}")
    
    def _load_configuration(self):
        """Load configuration from Parameter Store and secrets from Secrets Manager."""
        try:
            # Parameter Store (WITH leading slash)
            self.rds_endpoint = self._get_parameter('/platform/config/rds-endpoint')
            self.aws_region = self._get_parameter('/platform/config/aws-region')
            self.kill_switch = self._get_parameter('/platform/config/kill-switch')
            
            # Secrets Manager (NO leading slash)
            self.rds_credentials = self._get_secret('platform/rds/credentials')
            eodhd_secret = self._get_secret('platform/eodhd/api-key')
            self.eodhd_key = eodhd_secret['api_key']
            self.eodhd_mcp_url = eodhd_secret.get('mcp_url', 'https://mcpv2.eodhd.dev/v2/mcp')
            self.finnhub_key = self._get_secret('platform/finnhub/api-key')['api_key']
            self.anthropic_key = self._get_secret('platform/anthropic/api-key')['api_key']
            
            # TraderMade for SSI (fallback since MCP not ready)
            try:
                self.tradermade_key = self._get_secret('platform/tradermade/api-key')['api_key']
            except:
                logger.warning("TraderMade key not found - SSI signals will be unavailable")
                self.tradermade_key = None
            
            logger.info("Configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _get_parameter(self, name: str) -> str:
        """Get parameter from Parameter Store."""
        try:
            response = self.ssm_client.get_parameter(Name=name, WithDecryption=True)
            return response['Parameter']['Value']
        except ClientError as e:
            logger.error(f"Failed to get parameter {name}: {e}")
            raise
    
    def _get_secret(self, name: str) -> Dict[str, Any]:
        """Get secret from Secrets Manager."""
        try:
            response = self.secrets_client.get_secret_value(SecretId=name)
            return json.loads(response['SecretString'])
        except ClientError as e:
            logger.error(f"Failed to get secret {name}: {e}")
            raise
    
    def _init_database(self):
        """Initialize PostgreSQL database connection."""
        try:
            self.db_conn = psycopg2.connect(
                host=self.rds_endpoint,
                database='postgres',
                user=self.rds_credentials['username'],
                password=self.rds_credentials['password'],
                port=5432,
                options='-c search_path=forex_network,shared,public'
            )
            self.db_conn.autocommit = False
            logger.info("Database connection established")
            validate_schema(self.db_conn, EXPECTED_TABLES)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _init_anthropic_client(self):
        """Initialize Anthropic client with MCP servers."""
        self.anthropic_client = anthropic.Anthropic(api_key=self.anthropic_key)
        
        # MCP server configurations
        # All MCPs disabled — the Anthropic MCP connector either 400s (EODHD) or
        # times out (Alpha Vantage) against our providers. The agent runs MCP-less;
        # all needed data is injected into the conversation context via REST/DB:
        #   • EODHD sentiment → get_eodhd_sentiment_rest()
        #   • Economic releases / calendar → Postgres (pre-ingested)
        #   • TraderMade SSI → get_tradermade_ssi_data()
        #   • System stress → market_context_snapshots
        # Re-enable individual MCPs here once provider issues are resolved.
        self.mcp_servers = []
        logger.info(f"Anthropic client initialized with {len(self.mcp_servers)} MCP servers (REST-only mode)")
    
    def check_kill_switch(self) -> bool:
        """Check if the system kill switch is active."""
        try:
            kill_switch = self._get_parameter('/platform/config/kill-switch')
            if kill_switch == 'active':
                logger.warning("Kill switch is ACTIVE - halting all signal generation")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to check kill switch: {e}")
            # Conservative default - assume active if we can't read it
            return True
    
    def get_system_stress_score(self) -> Tuple[Optional[int], Optional[str], Optional[Dict]]:
        """Get current system stress score from market context snapshots."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT system_stress_score, stress_state, stress_components
                    FROM shared.market_context_snapshots
                    ORDER BY snapshot_time DESC LIMIT 1
                """)
                result = cur.fetchone()
                
                if result:
                    return result['system_stress_score'], result['stress_state'], result['stress_components']
                else:
                    logger.warning("No system stress score found - assuming normal (30)")
                    return 30, "normal", {}
                    
        except Exception as e:
            logger.error(f"Failed to get system stress score: {e}")
            return None, None, None
    
    def get_yield_differentials(self) -> Optional[Dict]:
        """Read cross-country yield differentials stored in metadata by the regime agent."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT metadata FROM shared.market_context_snapshots
                    WHERE metadata IS NOT NULL AND metadata != '{}'::jsonb
                    ORDER BY snapshot_time DESC LIMIT 1
                """)
                result = cur.fetchone()
                if result and result['metadata']:
                    meta = result['metadata']
                    if isinstance(meta, str):
                        import json as _j
                        meta = _j.loads(meta)
                    return meta.get('yield_differentials')
                return None
        except Exception as e:
            logger.warning(f"Failed to read yield differentials from snapshot: {e}")
            return None

    def get_recent_economic_releases(self, hours_back: int = 24) -> List[Dict]:
        """Get recent economic releases from RDS."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT country, indicator, release_time, actual, forecast, previous, 
                           surprise_pct
                    FROM forex_network.economic_releases
                    WHERE release_time >= NOW() - INTERVAL '%s hours'
                    ORDER BY release_time DESC
                    LIMIT 50
                """, (hours_back,))
                
                return [dict(row) for row in cur.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get economic releases: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass
            return []
    
    def get_upcoming_calendar_events(self, hours_ahead: int = 24) -> List[Dict]:
        """Get upcoming tier-1 calendar events from RDS."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT country, indicator, scheduled_time, forecast, previous, importance
                    FROM forex_network.economic_calendar
                    WHERE importance = 3 
                      AND resolved = FALSE
                      AND scheduled_time BETWEEN NOW() AND NOW() + INTERVAL '%s hours'
                    ORDER BY scheduled_time ASC
                """, (hours_ahead,))
                
                return [dict(row) for row in cur.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get calendar events: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass
            return []
    
    def get_tradermade_ssi_data(self) -> Dict[str, Dict]:
        """Get SSI contrarian data from RDS. Written hourly by neo-ingest-ssi-dev Lambda (source: MyFXBook)."""
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT instrument, long_pct, short_pct, contrarian_score,
                           avg_long_price, avg_short_price, ts
                    FROM forex_network.sentiment_ssi
                    WHERE ts >= NOW() - INTERVAL '90 minutes'
                    ORDER BY instrument, ts DESC
                """)
                
                results = cur.fetchall()
                
                # Group by instrument, take most recent
                ssi_data = {}
                for row in results:
                    instrument = row['instrument']
                    if instrument not in ssi_data:
                        ssi_data[instrument] = dict(row)
                
                return ssi_data
                
        except Exception as e:
            logger.error(f"Failed to get TraderMade SSI data: {e}")
            try:
                self.db_conn.rollback()
            except:
                pass
            return {}

    def get_multi_broker_sentiment(self) -> dict:
        """
        Read retail sentiment from both MyFXBook SSI and IG Client Sentiment.
        Returns:
          {
            'fx': {instrument: {mfx_long, mfx_short, mfx_contrarian, ig_long, ig_short,
                                consensus, contrarian_signal}},
            'cross_asset': {instrument: {ig_long, ig_short}},
            'dxy_close': float or None,
          }
        """
        FX_PAIRS         = {'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','NZDUSD','EURGBP','EURJPY','GBPJPY','EURCHF','GBPCHF','EURAUD','GBPAUD','EURCAD','GBPCAD','AUDNZD','AUDJPY','CADJPY','NZDJPY'}
        CROSS_ASSET_PAIRS = {'SPX500','GER30','UK100'}
        result = {'fx': {}, 'cross_asset': {}, 'dxy_close': None}
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT ON (instrument)
                           instrument, long_pct, short_pct, contrarian_score, ts
                    FROM forex_network.sentiment_ssi
                    WHERE ts >= NOW() - INTERVAL '26 hours'
                    ORDER BY instrument, ts DESC
                """)
                ssi_rows = {r['instrument']: dict(r) for r in cur.fetchall()}
                cur.execute("""
                    SELECT DISTINCT ON (instrument)
                           instrument, long_percentage, short_percentage, ts
                    FROM forex_network.ig_client_sentiment
                    WHERE ts >= NOW() - INTERVAL '26 hours'
                    ORDER BY instrument, ts DESC
                """)
                ig_rows = {r['instrument']: dict(r) for r in cur.fetchall()}
                cur.execute("""
                    SELECT close FROM forex_network.cross_asset_prices
                    WHERE instrument = 'DXY' AND timeframe = '1D'
                    ORDER BY bar_time DESC LIMIT 1
                """)
                dxy_row = cur.fetchone()
                if dxy_row:
                    result['dxy_close'] = float(dxy_row['close'])
        except Exception as e:
            logger.error(f"Failed to get multi-broker sentiment: {e}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            return result

        for inst in FX_PAIRS:
            mfx = ssi_rows.get(inst)
            ig  = ig_rows.get(inst)
            entry = {}
            if mfx:
                entry['mfx_long']       = float(mfx.get('long_pct') or 0)
                entry['mfx_short']      = float(mfx.get('short_pct') or 0)
                entry['mfx_contrarian'] = float(mfx.get('contrarian_score') or 0)
            if ig:
                entry['ig_long']  = float(ig.get('long_percentage') or 0)
                entry['ig_short'] = float(ig.get('short_percentage') or 0)
            if mfx and ig:
                mfx_crowd_short = entry['mfx_short'] > 50
                ig_crowd_short  = entry['ig_short']  > 50
                agree           = mfx_crowd_short == ig_crowd_short
                crowd_side      = 'short' if (entry['mfx_short'] + entry['ig_short']) / 2 > 50 else 'long'
                avg_dominant    = max(
                    (entry['mfx_short'] + entry['ig_short']) / 2,
                    (entry['mfx_long']  + entry['ig_long'])  / 2,
                )
                if agree:
                    if avg_dominant >= 75:
                        entry['consensus'] = 'HIGH'
                        entry['contrarian_signal'] = f"{'BUY' if crowd_side == 'short' else 'SELL'} (strong)"
                    elif avg_dominant >= 60:
                        entry['consensus'] = 'MODERATE'
                        entry['contrarian_signal'] = f"{'BUY' if crowd_side == 'short' else 'SELL'} (moderate)"
                    else:
                        entry['consensus'] = 'MODERATE'
                        entry['contrarian_signal'] = f"{'BUY' if crowd_side == 'short' else 'SELL'} (weak)"
                else:
                    entry['consensus'] = 'MIXED'
                    entry['contrarian_signal'] = 'Conflicting -- reduce weight'
            elif mfx:
                crowd_side = 'short' if entry['mfx_short'] > 50 else 'long'
                entry['consensus'] = 'STANDARD'
                entry['contrarian_signal'] = f"{'BUY' if crowd_side == 'short' else 'SELL'} (MyFXBook only)"
            elif ig:
                crowd_side = 'short' if entry['ig_short'] > 50 else 'long'
                entry['consensus'] = 'STANDARD'
                entry['contrarian_signal'] = f"{'BUY' if crowd_side == 'short' else 'SELL'} (IG only)"
            else:
                entry['consensus'] = 'UNAVAILABLE'
                entry['contrarian_signal'] = 'No data'
            if entry:
                result['fx'][inst] = entry

        for inst in CROSS_ASSET_PAIRS:
            ig = ig_rows.get(inst)
            if ig:
                result['cross_asset'][inst] = {
                    'ig_long':  float(ig.get('long_percentage') or 0),
                    'ig_short': float(ig.get('short_percentage') or 0),
                }

        return result

    _USD_BASE_PAIRS = {"USDJPY", "USDCHF", "USDCAD"}

    def get_cot_positioning_data(self) -> Dict[str, Dict]:
        """
        Return CFTC speculator positioning for the 6 tracked FX pairs from the
        most recent weekly report stored in shared.cot_positioning.

        pct_signed_adj is normalised to the pair's perspective (0-100):
          100 = speculators at their most LONG (bullish) extreme in 52 weeks
            0 = speculators at their most SHORT (bearish) extreme in 52 weeks
        For USD-base pairs (USDJPY/USDCHF/USDCAD) the raw DB value is flipped
        because the underlying futures contract measures the foreign leg.

        Returns {} on any error so callers degrade gracefully.
        """
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT pair_code, noncomm_net, open_interest_all,
                           pct_signed_52w, pct_abs_52w, report_date
                    FROM shared.cot_positioning
                    WHERE report_date = (
                        SELECT MAX(report_date) FROM shared.cot_positioning
                    )
                      AND pair_code IS NOT NULL
                    ORDER BY pair_code
                """)
                rows = cur.fetchall()

            result = {}
            for row in rows:
                pair = row["pair_code"]
                raw_signed = float(row["pct_signed_52w"] or 0)
                # Adjust direction for USD-base pairs
                adj_signed = (100.0 - raw_signed) if pair in self._USD_BASE_PAIRS else raw_signed
                result[pair] = {
                    "pct_signed_52w_adj": round(adj_signed, 1),
                    "pct_abs_52w": round(float(row["pct_abs_52w"] or 0), 1),
                    "noncomm_net": int(row["noncomm_net"] or 0),
                    "report_date": str(row["report_date"]),
                }
            return result

        except Exception as e:
            logger.error(f"Failed to get COT positioning data: {e}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            return {}

    def get_cross_asset_context(self) -> Optional[str]:
        """Read latest cross-asset prices from DB for macro context.

        Fetches the most recent and prior 1D close for all instruments in
        forex_network.cross_asset_prices, computes day-on-day % change, and
        returns a formatted string ready for injection into the LLM prompt.
        Returns None on any error so the caller degrades gracefully.
        """
        _t0 = time.time()
        try:
            with self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Latest 1D bar
                cur.execute("""
                    SELECT instrument, close, bar_time
                    FROM forex_network.cross_asset_prices
                    WHERE timeframe = '1D'
                      AND bar_time = (
                          SELECT MAX(bar_time)
                          FROM forex_network.cross_asset_prices
                          WHERE timeframe = '1D'
                      )
                    ORDER BY instrument
                """)
                latest_rows = {row['instrument']: dict(row) for row in cur.fetchall()}

                # Previous 1D bar (for % change)
                cur.execute("""
                    SELECT instrument, close
                    FROM forex_network.cross_asset_prices
                    WHERE timeframe = '1D'
                      AND bar_time = (
                          SELECT MAX(bar_time)
                          FROM forex_network.cross_asset_prices
                          WHERE timeframe = '1D'
                            AND bar_time < (
                                SELECT MAX(bar_time)
                                FROM forex_network.cross_asset_prices
                                WHERE timeframe = '1D'
                            )
                      )
                    ORDER BY instrument
                """)
                prev_rows = {row['instrument']: dict(row) for row in cur.fetchall()}

            _ms = int((time.time() - _t0) * 1000)
            log_api_call(self.db_conn, 'internal_db', 'cross_asset_prices', 'macro',
                         True, _ms, pairs_returned=len(latest_rows))

            if not latest_rows:
                logger.warning("cross_asset_prices: no 1D rows found")
                return None

            bar_date = next(iter(latest_rows.values()))['bar_time'].strftime('%Y-%m-%d')

            EQUITIES = ['SPX500', 'GER30', 'UK100']
            COMMODITIES = ['XAUUSD', 'XAGUSD', 'UKOIL']
            BONDS = ['US10Y', 'US2Y', 'US3M', 'DE10Y', 'UK10Y', 'JP10Y']

            def fmt_row(inst):
                if inst not in latest_rows:
                    return None
                close = latest_rows[inst]['close']
                prev_close = prev_rows.get(inst, {}).get('close')
                if prev_close and prev_close != 0:
                    pct = (close - prev_close) / prev_close * 100
                    pct_str = f"{pct:+.2f}%"
                else:
                    pct_str = "N/A"
                return f"  {inst:10s}: {close:>12,.4f}  ({pct_str} vs prev day)"

            lines = [f"## CROSS-ASSET CONTEXT (latest daily close: {bar_date})"]

            lines.append("\n### Equity Indices")
            for inst in EQUITIES:
                row = fmt_row(inst)
                if row:
                    lines.append(row)

            lines.append("\n### Commodities & Precious Metals")
            for inst in COMMODITIES:
                row = fmt_row(inst)
                if row:
                    lines.append(row)

            lines.append("\n### Bond Yields (%)")
            for inst in BONDS:
                row = fmt_row(inst)
                if row:
                    lines.append(row)

            lines.append("""
### Interpretation guidelines
- Equity indices falling + Gold (XAUUSD) rising = risk-off -> supports JPY, CHF; weighs on AUD, NZD
- Oil (UKOIL) rising = supports CAD (USDCAD bearish), adds inflation pressure to CPI outlook
- Regional divergence (GER30 up, UK100 down) = EUR/GBP relative strength signal
- US10Y rising = supports USD via rate differential; watch USDJPY especially
- JP10Y rising = potential BOJ policy shift signal; bearish USDJPY
- US10Y-US2Y spread: narrowing/inverted = recession risk, risk-off signal
- XAUUSD rising = safe-haven demand / geopolitical risk proxy
- XAGUSD often leads industrial/commodity risk-off before XAUUSD""")

            return "\n".join(lines)

        except Exception as e:
            _ms = int((time.time() - _t0) * 1000)
            log_api_call(self.db_conn, 'internal_db', 'cross_asset_prices', 'macro',
                         False, _ms, error_type=type(e).__name__)
            logger.error(f"Failed to get cross-asset context: {e}")
            try:
                self.db_conn.rollback()
            except Exception:
                pass
            return None

    def create_agent_system_prompt(self) -> str:
        """System prompt v2 — per-currency scoring. Cycle data goes in the user message.
        The engine derives all 20 pair signals mathematically from 8 currency scores.
        """
        return """You are the MACRO AGENT for Project Neo, an autonomous FX trading system.
Score 8 major currencies independently against the basket of all others.
Pair signals are derived mathematically from your scores — focus on currencies, not pairs.

CURRENCIES TO SCORE: EUR  GBP  USD  JPY  CHF  AUD  CAD  NZD

OBJECTIVE: Maximize risk-adjusted returns (Sortino). ACTIVE profit-seeking agent — use the full score range when evidence is strong.

DATA TIERS:
Tier1 (every cycle): EODHD sentiment aggregates, economic calendar (RDS), system stress score
Tier2 (agent decision): EODHD full articles/word-weights, Alpha Vantage cross-check, Finnhub calendar
Tier3 (conditional): EODHD macro indicators, GDELT (geopolitical tension >60 min from Tier1 event)

CURRENCY FUNDAMENTALS:
USD: Fed policy, DXY direction, US yield curve (10Y-2Y slope), NFP/CPI/GDP releases, COT speculator positioning
EUR: ECB policy + forward guidance, EZ composite PMI (sub-50 = contractionary), HICP inflation, EODHD EUR sentiment
GBP: BoE policy, UK composite PMI, UK CPI/employment, UK100 equity direction
JPY: BoJ policy (YCC, rate expectations), JP 10Y yield level, intervention risk (USDJPY >155), safe-haven demand (VIX, stress score)
CHF: SNB policy, safe-haven demand (VIX, stress score), EUR/CHF floor history; SNB rarely acts but moves are sharp
AUD: RBA policy, AU employment/CPI, Chinese PMI sensitivity, iron ore direction — AUD is a China/commodity proxy
CAD: BoC policy, CA employment/CPI, crude oil direction (UKOIL) — oil-CAD correlation ~0.70
NZD: RBNZ policy, NZ CPI/employment, dairy prices, NZ-AU spread; ~0.88 correlation with AUD — rarely diverge strongly

SESSION RULES (metadata only — do not adjust score magnitude for session):
London 07:00-09:00 UTC: highest priority session
London-NY overlap 12:00-16:00 UTC: peak volume, most reliable signals
Asian 00:00-07:00 UTC: thin liquidity, note in reasoning
NY close 20:00-22:00 UTC: note pre-close conditions but score on fundamentals

SOURCE AUTHORITY:
Tier1 (1.0x): reuters, bloomberg, afp, socgen, rabobank, ing, jpmorgan, goldman, fed, ecb, boe, boj, snb, rba, boc, rbnz
Tier2 (0.7x): ft, wsj, financial times, fxstreet, marketwatch, cnbc
Tier3 (0.4x): retail brokerage commentary, unrecognized domains
New domain (0.3x): first 30 days in EODHD feed

ADVERSARIAL RULES (mandatory):
- |score| > 0.6 requires: 2 independent tiers agreeing, OR calendar event, OR same-direction economic release
- Temporal clustering: 3+ articles same currency within 30 min, no Tier1 event -> confidence -0.20
- Sentiment velocity: >0.4 point move in one cycle, no Tier1 event -> confidence -0.20
- Content fingerprinting: 2+ articles >70% lexically similar -> count as single data point
- Tier3-only signal -> confidence -0.25
- Always reason the counter-argument before committing to a direction

DECISION RULES:
M1: Sentiment null 1-2 cycles -> 0.80x confidence, note degraded. 3+ cycles -> 0.60x confidence. Never score 0.0 solely due to thin data.
M2: Velocity flag fires with no calendar event -> apply -0.20 and proceed.
M3: Tier1 event in calendar but FRED not yet updated -> use EODHD as primary, 0.85x confidence.
M4: Macro vs technical diverge on 5+ pairs -> cap confidence at 0.50x, note regime transition risk.
M5: COT pct_signed_52w_adj >= 90 for a currency -> reduce bullish weight 0.5x. <= 10 -> reduce bearish weight 0.5x.
M6: Fed+ECB decisions within 48h -> note pre-event uncertainty; overlapping windows use larger effect, not sum.
M7: |score| > 0.65 requires 2 independent tiers OR calendar event alignment.

SAFE-HAVEN CURRENCIES — JPY and CHF only:
Score on TWO independent axes:
1. score: fundamental strength based on BoJ/SNB policy, yield, and macro data (same scale as all currencies)
2. safe_haven_demand: 0.0 to 1.0 — risk-off demand INDEPENDENT of fundamentals, driven by VIX and stress score
   VIX < 15  -> safe_haven_demand <= 0.2
   VIX 15-20 -> safe_haven_demand 0.2-0.4
   VIX 20-25 -> safe_haven_demand 0.4-0.6
   VIX 25-30 -> safe_haven_demand 0.6-0.8
   VIX > 30  -> safe_haven_demand >= 0.8
The derivation engine applies safe_haven_demand to strengthen JPY/CHF pair scores independently.
All other currencies: set safe_haven_demand to null.

RETAIL SENTIMENT — STRUCTURAL CONTEXT & INTERPRETATION

BACKGROUND:
1. MARKET STRUCTURE
   - The FX market trades $9.6 trillion/day (BIS 2025). Retail = ~2.5% of volume.
   - 70-80% of retail accounts lose money over time (regulated broker disclosures).

2. WHY RETAIL LOSES
   - Excessive leverage. Behavioural bias (cut winners, hold losers).
   - Predictable stop placement at obvious technical levels. Late entry.

3. STOP-HUNT DYNAMICS
   - Retail stops cluster at key levels -> larger participants sweep them -> price reverses
   - Sweeps most common: London open (07:00-08:00 UTC), NY open (12:00-13:00 UTC)

INTERPRETATION FRAMEWORK:
A. POSITIONING EXTREMITY
   50-60%: No signal | 60-70%: Weak | 70-80%: Moderate | 80-90%: Strong | 90%+: Extreme
B. MULTI-BROKER CONSENSUS
   Both >75% same side: HIGH confidence | Both >60%: MODERATE | Disagree: LOW confidence
C. COT CROSS-REFERENCE
   COT institutions OPPOSITE to retail: STRONGEST signal | Same as retail: WEAKENS signal
D. CONTEXT OVERRIDES
   - Tier1 event justifies extreme positioning -> reduce contrarian weight
   - Post-sweep direction is often true institutional intent
   - When fundamental data is degraded, contrarian sentiment is supporting factor only

SCORE CALIBRATION (use the full range — do not anchor conservatively):
±0.80 to ±1.00: Extreme conviction — multiple independent drivers strongly aligned
±0.60 to ±0.79: Strong conviction — clear case with 2+ supporting data points
±0.40 to ±0.59: Moderate conviction — clear lean with some evidence
±0.20 to ±0.39: Weak conviction — marginal signal, mixed data
±0.00 to ±0.19: Neutral — no clear direction

OUTPUT FORMAT (strict — no preamble):
Begin your response IMMEDIATELY with the first ```json block. No analysis or commentary before, between, or after the blocks.
ALL 8 currencies must be scored every cycle — no exceptions. Missing currency = gap-filled as neutral 0.0 by the engine.
Order: EUR  GBP  USD  JPY  CHF  AUD  CAD  NZD

CURRENCY SIGNAL SCHEMA (all fields required):
{
  "currency": "<EUR|GBP|USD|JPY|CHF|AUD|CAD|NZD>",
  "score": <-1.0 to +1.0, currency strength vs basket>,
  "confidence": <0.0 to 1.0>,
  "reasoning": "<2-3 sentences: primary driver + key evidence + counter-argument considered>",
  "key_drivers": ["<driver1>", "<driver2>", "<driver3 max>"],
  "safe_haven_demand": <0.0 to 1.0 for JPY and CHF only, null for all others>
}
"""

    def call_anthropic_agent(self, conversation_context: List[Dict]) -> Dict:
        """Call the Anthropic API (with MCP servers if any are configured)."""
        try:
            system_prompt = self.create_agent_system_prompt()

            _t0 = time.time()
            try:
                if self.mcp_servers:
                    response = self.anthropic_client.beta.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=8000,
                        mcp_servers=self.mcp_servers,
                        system=system_prompt,
                        messages=conversation_context,
                        extra_headers={"anthropic-beta": "mcp-client-2025-04-04"},
                    )
                else:
                    # No MCP servers — use the stable messages endpoint.
                    response = self.anthropic_client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=8000,
                        system=system_prompt,
                        messages=conversation_context,
                    )
                log_api_call(self.db_conn, 'anthropic', '/v1/messages', 'macro',
                             True, int((time.time() - _t0) * 1000))
            except Exception as _api_err:
                log_api_call(self.db_conn, 'anthropic', '/v1/messages', 'macro',
                             False, int((time.time() - _t0) * 1000),
                             error_type=type(_api_err).__name__)
                raise
            
            # Extract text response and any tool results
            full_response = ""
            tool_results = []
            
            block_types = []
            for content_block in response.content:
                if hasattr(content_block, 'type'):
                    block_types.append(content_block.type)
                    if content_block.type == "text":
                        full_response += content_block.text + "\n"
                    elif content_block.type == "mcp_tool_result":
                        tool_results.append(content_block.content)
                elif hasattr(content_block, 'text'):
                    block_types.append("text(fallback)")
                    full_response += content_block.text + "\n"
            logger.info(f"Response content block types: {block_types}")
            
            return {
                "response": full_response.strip(),
                "tool_results": tool_results,
                "usage": getattr(response, 'usage', {})
            }
            
        except Exception as e:
            logger.error(f"Anthropic API call failed: {e}")
            raise
    
    def get_eodhd_sentiment_rest(self, days_back: int = 7) -> Dict[str, List[Dict]]:
        """Fetch daily sentiment aggregates per pair via EODHD REST (replaces broken MCP).

        Returns dict keyed by pair (e.g. 'EURUSD') of lists of {date, count, normalized}.
        Returns {} on failure — macro agent handles missing sentiment gracefully.
        """
        try:
            import requests  # local import; the module is already a dependency
            from datetime import date, timedelta
            symbols = ",".join(f"{p}.FOREX" for p in self.PAIRS)
            since = (date.today() - timedelta(days=days_back)).isoformat()
            url = "https://eodhd.com/api/sentiments"
            _t0 = time.time()
            try:
                resp = requests.get(url, params={
                    "s": symbols,
                    "from": since,
                    "api_token": self.eodhd_key,
                    "fmt": "json",
                }, timeout=10)
                _ms = int((time.time() - _t0) * 1000)
                if resp.status_code != 200:
                    log_api_call(self.db_conn, 'eodhd', '/api/sentiments', 'macro',
                                 False, _ms, error_type=f'http_{resp.status_code}')
                    logger.warning(f"EODHD REST sentiment HTTP {resp.status_code} — {resp.text[:120]}")
                    return {}
                log_api_call(self.db_conn, 'eodhd', '/api/sentiments', 'macro',
                             True, _ms, pairs_returned=len(self.PAIRS))
            except requests.Timeout:
                log_api_call(self.db_conn, 'eodhd', '/api/sentiments', 'macro',
                             False, int((time.time() - _t0) * 1000), error_type='timeout')
                logger.warning("EODHD REST sentiment fetch timed out")
                return {}
            raw = resp.json() or {}
            out = {}
            for key, rows in raw.items():
                pair = key.split(".")[0]  # strip '.FOREX'
                out[pair] = rows if isinstance(rows, list) else []
            return out
        except Exception as e:
            logger.warning(f"EODHD REST sentiment fetch failed: {e}")
            return {}

    def get_finnhub_forex_news(self, max_articles: int = 5) -> List[Dict]:
        """Fetch latest forex news headlines and summaries from Finnhub.

        Returns a list of dicts with headline, summary, source, datetime.
        Returns [] on any failure so the prompt degrades gracefully.
        """
        try:
            import requests
            _t0 = time.time()
            resp = requests.get(
                "https://finnhub.io/api/v1/news",
                params={"category": "forex", "token": self.finnhub_key},
                timeout=10,
            )
            _ms = int((time.time() - _t0) * 1000)
            if resp.status_code != 200:
                log_api_call(self.db_conn, 'finnhub', '/news?category=forex', 'macro',
                             False, _ms, error_type=f'http_{resp.status_code}')
                logger.warning(f"Finnhub news returned HTTP {resp.status_code}")
                return []
            articles = resp.json()[:max_articles]
            log_api_call(self.db_conn, 'finnhub', '/news?category=forex', 'macro',
                         True, _ms, pairs_returned=len(articles))
            logger.info(f"Finnhub forex news: {len(articles)} articles fetched in {_ms}ms")
            return [
                {
                    'headline': a.get('headline', ''),
                    'summary':  a.get('summary', '')[:200],
                    'source':   a.get('source', ''),
                    'datetime': a.get('datetime', 0),
                }
                for a in articles
            ]
        except Exception as e:
            logger.warning(f"Finnhub news fetch failed: {e}")
            return []

    def get_vix_from_stress_components(self, stress_components: dict) -> float:
        """Extract VIX value cleanly from stress_components dict. Returns 18.0 as neutral default."""
        try:
            return float(stress_components.get('vix_level_trend', {}).get('raw_value', 18.0))
        except Exception:
            return 18.0

    def get_ukoil_30d_momentum(self) -> float:
        """Returns (current_close - 30d_avg_close) / 30d_avg_close for UKOIL. Returns 0.0 on failure."""
        try:
            cur = self.db_conn.cursor()
            cur.execute("""
                SELECT close FROM forex_network.cross_asset_prices
                WHERE instrument = 'UKOIL' AND timeframe = '1D'
                ORDER BY bar_time DESC LIMIT 30
            """)
            rows = cur.fetchall()
            cur.close()
            if len(rows) < 2:
                return 0.0
            current = float(rows[0][0])
            avg_30d = sum(float(r[0]) for r in rows) / len(rows)
            return (current - avg_30d) / avg_30d if avg_30d > 0 else 0.0
        except Exception:
            return 0.0

    def build_conversation_context(self) -> List[Dict]:
        """Build the conversation context with all necessary data for the agent."""

        # Get current data from various sources
        stress_score, stress_state, stress_components = self.get_system_stress_score()
        yield_differentials = self.get_yield_differentials()
        recent_releases = self.get_recent_economic_releases()
        upcoming_events = self.get_upcoming_calendar_events()
        cross_asset_context = self.get_cross_asset_context()
        broker_sentiment = self.get_multi_broker_sentiment()
        eodhd_sentiment = self.get_eodhd_sentiment_rest()
        cot_data = self.get_cot_positioning_data()
        forex_news = self.get_finnhub_forex_news()

        # Format COT data for context: show pct_signed_52w_adj so Claude knows direction
        if cot_data:
            cot_report_date = next(iter(cot_data.values()))["report_date"]
            cot_lines = [f"Report date: {cot_report_date}"]
            cot_lines.append("pair     | pct_signed_52w_adj | pct_abs_52w | interpretation")
            cot_lines.append("---------|-------------------|-------------|--------------------------------------------------")
            for pair, d in sorted(cot_data.items()):
                ps = d["pct_signed_52w_adj"]
                pa = d["pct_abs_52w"]
                if ps >= 90:
                    interp = "⚠ near 52w LONG extreme — M5: reduce bullish weight to 0.5x"
                elif ps <= 10:
                    interp = "⚠ near 52w SHORT extreme — M5: reduce bearish weight to 0.5x"
                elif ps >= 75:
                    interp = "elevated long positioning"
                elif ps <= 25:
                    interp = "elevated short positioning"
                else:
                    interp = "neutral range"
                cot_lines.append(f"{pair:8s} | {ps:>17.1f} | {pa:>11.1f} | {interp}")
            cot_context = "\n".join(cot_lines)
        else:
            cot_context = "COT data unavailable"

        # Format Finnhub news for prompt — headlines + truncated summary, one line each
        if forex_news:
            news_lines = "\n".join(
                f"- [{a['source']}] {a['headline']}"
                + (f"\n  {a['summary']}" if a.get('summary') else "")
                for a in forex_news
            )
        else:
            news_lines = "No recent forex news available."

        # Format yield differentials for prompt
        if yield_differentials:
            _yd = yield_differentials
            _yd_lines = []
            if _yd.get('us_uk_spread') is not None:
                _s = _yd['us_uk_spread']
                _dir = "USD yields higher -> GBPUSD bearish pressure" if _s > 0 else "GBP yields higher -> GBPUSD bullish pressure"
                _yd_lines.append(f"US-UK 10Y spread: {_s:+.3f}%  ({_dir})")
            if _yd.get('us_de_spread') is not None:
                _s = _yd['us_de_spread']
                _dir = "USD yields higher -> EURUSD bearish pressure" if _s > 0 else "EUR yields higher -> EURUSD bullish pressure"
                _yd_lines.append(f"US-DE 10Y spread: {_s:+.3f}%  ({_dir})")
            if _yd.get('us_jp_spread') is not None:
                _s = _yd['us_jp_spread']
                _dir = "USD yields higher -> USDJPY bullish (yen weakens)" if _s > 0 else "JPY yields higher -> USDJPY bearish (yen strengthens)"
                _yd_lines.append(f"US-JP 10Y spread: {_s:+.3f}%  ({_dir})")
            if _yd.get('us10y') and _yd.get('us2y'):
                _curve = _yd['us10y'] - _yd['us2y']
                _shape = "normal" if _curve > 0 else "inverted (recession risk flag)"
                _yd_lines.append(f"US 10Y: {_yd['us10y']:.3f}%  |  US 2Y: {_yd['us2y']:.3f}%  |  10Y-2Y curve: {_curve:+.3f}% ({_shape})")
            yield_diff_context = "\n".join(_yd_lines)
        else:
            yield_diff_context = "Unavailable — regime agent metadata not yet populated"

        # Build multi-broker sentiment consensus table
        _bs = broker_sentiment
        _fx = _bs.get('fx', {})
        _ca = _bs.get('cross_asset', {})
        _dxy_close = _bs.get('dxy_close')

        if _fx:
            _hdr = "| Pair   | MyFXBook Short% | IG Short% | Consensus | Contrarian Signal |"
            _sep = "|--------|-----------------|-----------|-----------|-------------------|"
            _rows = []
            for inst in sorted(_fx.keys()):
                e = _fx[inst]
                mfx_s = f"{e['mfx_short']:.0f}%" if 'mfx_short' in e else 'n/a'
                ig_s  = f"{e['ig_short']:.0f}%"  if 'ig_short'  in e else 'n/a'
                _rows.append(f"| {inst:6s} | {mfx_s:>15s} | {ig_s:>9s} | {e.get('consensus','?'):9s} | {e.get('contrarian_signal','?'):17s} |")
            consensus_table = "\n".join([_hdr, _sep] + _rows)
        else:
            consensus_table = "Sentiment consensus unavailable"

        # Cross-asset sentiment summary
        if _ca:
            _ca_lines = []
            _ca_names = {'SPX500': 'US500 (S&P 500)', 'GER30': 'DE30 (DAX)', 'UK100': 'FT100 (FTSE)'}
            _all_bearish = all(_ca.get(i, {}).get('ig_short', 0) > 60 for i in ['SPX500','GER30','UK100'] if i in _ca)
            for inst in ['SPX500', 'GER30', 'UK100']:
                d = _ca.get(inst)
                if d:
                    label = _ca_names.get(inst, inst)
                    s = d['ig_short']
                    direction = 'retail bearish' if s > 50 else 'retail bullish'
                    signal = f"contrarian: {'risk-on' if s > 50 else 'risk-off'}"
                    _ca_lines.append(f"{label}: {d['ig_long']:.0f}% long / {s:.0f}% short -> {direction} ({signal})")
            if _all_bearish:
                _ca_lines.append("-> ALL THREE equity indices >60% retail short -> strong contrarian risk-on signal")
                _ca_lines.append("   (supports AUD, NZD, GBP; weighs against JPY, CHF safe havens)")
            cross_asset_sentiment_context = "\n".join(_ca_lines)
        else:
            cross_asset_sentiment_context = "Cross-asset IG sentiment unavailable"

        # DXY context
        if _dxy_close:
            dxy_context = (
                f"Current: {_dxy_close:.3f} (daily close from IG)\n"
                "The DXY measures USD strength against a basket of 6 currencies "
                "(EUR 57.6%, JPY 13.6%, GBP 11.9%, CAD 9.1%, SEK 4.2%, CHF 3.6%).\n"
                "A falling DXY confirms broad USD weakness across all pairs."
            )
        else:
            dxy_context = "DXY unavailable"

        # Build comprehensive context message
        learned_multipliers_section = self._fetch_learned_multipliers()

        # Retrieve scoring history for all 20 pairs in a single batch query
        from shared.score_trajectory import get_recent_trajectory_batch
        _trajectories = get_recent_trajectory_batch(
            self.db_conn, 'macro', list(self.PAIRS),
            lookback_minutes=120, max_cycles_per_pair=8,
        )
        trajectory_section_lines = ["## YOUR RECENT SCORING HISTORY"]
        for _pair in self.PAIRS:
            _traj = _trajectories.get(_pair, [])
            _features = analyse_trajectory(_traj)
            logger.info(f"Macro trajectory for {_pair}: {_features}")
            trajectory_section_lines.append(format_trajectory_for_prompt(_traj, _pair))
        trajectory_section = "\n\n".join(trajectory_section_lines)

        # Named market context values for currency scoring
        vix_val = self.get_vix_from_stress_components(stress_components or {})
        _ukoil_close = _xauusd_close = None
        try:
            _mc = self.db_conn.cursor()
            _mc.execute("""
                SELECT instrument, close
                FROM forex_network.cross_asset_prices
                WHERE instrument IN ('UKOIL', 'XAUUSD') AND timeframe = '1D'
                  AND (instrument, bar_time) IN (
                      SELECT instrument, MAX(bar_time)
                      FROM forex_network.cross_asset_prices
                      WHERE instrument IN ('UKOIL', 'XAUUSD') AND timeframe = '1D'
                      GROUP BY instrument
                  )
            """)
            for _row in _mc.fetchall():
                if _row[0] == 'UKOIL':
                    _ukoil_close = float(_row[1])
                elif _row[0] == 'XAUUSD':
                    _xauusd_close = float(_row[1])
            _mc.close()
        except Exception as _e:
            logger.warning(f"build_conversation_context: UKOIL/XAUUSD fetch failed: {_e}")
        _ukoil_str  = f"{_ukoil_close:.2f}"  if _ukoil_close  is not None else "unavailable"
        _xauusd_str = f"{_xauusd_close:.2f}" if _xauusd_close is not None else "unavailable"

        context_message = f"""
MACRO AGENT CYCLE #{self.cycle_count + 1}
Current Time: {datetime.now(timezone.utc).isoformat()}
Session ID: {self.session_id}

## CURRENT MARKET CONTEXT
VIX: {vix_val:.1f} | Crude Oil (UKOIL): {_ukoil_str} | Gold (XAUUSD): {_xauusd_str}
System Stress Score: {stress_score} ({stress_state})
Stress Components: {json.dumps(stress_components, default=str) if stress_components else 'N/A'}

## RECENT ECONOMIC RELEASES (Last 24 Hours)
{json.dumps(recent_releases, indent=2, default=str) if recent_releases else 'No recent releases'}

## UPCOMING TIER-1 EVENTS (Next 24 Hours)
{json.dumps(upcoming_events, indent=2, default=str) if upcoming_events else 'No upcoming tier-1 events'}

{cross_asset_context if cross_asset_context else '## CROSS-ASSET CONTEXT\nUnavailable — cross_asset_prices query failed.'}

## YIELD DIFFERENTIALS (cross-country bond spreads, computed by regime agent)
{yield_diff_context}

## MULTI-BROKER RETAIL SENTIMENT CONSENSUS
Source 1: MyFXBook Community Outlook (verified retail accounts)
Source 2: IG Client Sentiment (world's largest CFD broker -- 68% of retail clients lose money)

CONSENSUS RULES:
- BOTH sources >75% on same side -> HIGH CONFIDENCE contrarian signal
- BOTH sources >60% on same side -> MODERATE CONFIDENCE contrarian signal
- Sources disagree on direction  -> MIXED -- reduce contrarian weight
- Only one source available      -> STANDARD -- use available source normally

{consensus_table}

## IG CROSS-ASSET RETAIL SENTIMENT
{cross_asset_sentiment_context}

## DXY (US Dollar Index)
{dxy_context}

## COT POSITIONING (CFTC speculator positioning, weekly)
pct_signed_52w_adj: 0=near 52w short extreme, 100=near 52w long extreme (pair-perspective adjusted).
pct_abs_52w: absolute extremity regardless of direction.
{cot_context}

## EODHD DAILY SENTIMENT (last 7 days, per pair)
Source: EODHD /api/sentiments REST (MCP temporarily unavailable).
Each row: date, article_count, normalized_score [-1..+1 where +1 is fully bullish].
{json.dumps(eodhd_sentiment, indent=2, default=str) if eodhd_sentiment else 'EODHD sentiment unavailable — apply sentiment_degraded flag per decision rule'}

## RECENT FOREX NEWS (last 24h)
Source: Finnhub /news?category=forex — Forexlive, Reuters, and other wires. Apply source authority tiers from system prompt.
{news_lines}

{learned_multipliers_section}

=== YOUR RECENT SCORING HISTORY ===
{trajectory_section}

This history is provided so you can reason about continuity and drift.
Consider: does current data confirm the trajectory (strengthening or
weakening as expected)? Or does current data warrant breaking from it?
A score that oscillates wildly across cycles may indicate unstable data or
genuine market indecision. A score drifting steadily in one direction may
indicate a strengthening conviction worth maintaining.

DO NOT anchor on the prior trajectory if current data contradicts it.
Treat the history as context, not as a thesis to defend.
=== END HISTORY ===

## TASK
EODHD sentiment is pre-fetched above — use it directly. Pairs with no rows are null: apply M1. Cross-check with Alpha Vantage MCP on divergence or macro context when useful. Apply all M1-M7 rules, adversarial defenses, COT rule, and CB hierarchy. Output 20 signals per schema.
"""

        return [{"role": "user", "content": context_message}]
    
    def _fetch_learned_multipliers(self) -> str:
        """
        Query the latest active contrarian multipliers from learning_adjustments.
        Returns a formatted string for inclusion in the LLM context message.
        """
        try:
            cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("""
                    SELECT DISTINCT ON (adjustment_type, factor_name)
                        adjustment_type, factor_name, new_value,
                        win_rate, supporting_trades
                    FROM forex_network.learning_adjustments
                    WHERE reverted_at IS NULL
                    ORDER BY adjustment_type, factor_name, applied_at DESC
                """)
                rows = cur.fetchall()
            finally:
                cur.close()

            if not rows:
                return ("## LEARNED CONTRARIAN ADJUSTMENTS\n"
                        "No adjustments yet — learning module accumulating trades (need 50+).")

            label_map = {
                'extremity': 'Extremity',
                'consensus': 'Consensus',
                'cot':       'COT',
                'session':   'Session',
            }
            lines = ["## LEARNED CONTRARIAN ADJUSTMENTS (auto-updated by learning module)"]
            for row in rows:
                adj_type    = row['adjustment_type']
                factor_name = row['factor_name']
                multiplier  = float(row['new_value'] or 1.0)
                win_rate    = float(row['win_rate'] or 0) * 100
                trades      = int(row['supporting_trades'] or 0)
                label       = label_map.get(adj_type, adj_type.title())
                direction   = "up" if multiplier > 1.0 else ("down" if multiplier < 1.0 else "neutral")
                lines.append(
                    f"  {label} {factor_name}: multiplier {multiplier:.2f} "
                    f"({direction}, win_rate {win_rate:.0f}% over {trades} trades)"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"_fetch_learned_multipliers failed: {e}")
            return "## LEARNED CONTRARIAN ADJUSTMENTS\nUnavailable (query error)."

    def parse_agent_response(self, agent_response: Dict) -> List[Dict]:
        """Parse 8 currency scores from the LLM response and derive 20 pair signals.

        Phase 1: Extract currency JSON blocks (keyed on 'currency').
                 Gap-fill any missing currencies as neutral.
        Phase 2: Derive pair signals mathematically with safe-haven, crude-oil,
                 and cross-pair amplifier adjustments.
        """
        import re

        def _neutral_currency(ccy: str) -> dict:
            return {
                'currency': ccy,
                'score': 0.0,
                'confidence': 0.1,
                'reasoning': 'Signal not returned by LLM this cycle — neutral gap-fill',
                'key_drivers': [],
                'safe_haven_demand': None,
            }

        def _neutral_pair_signal(pair: str, reason: str) -> dict:
            return {
                'agent_name': self.AGENT_NAME,
                'instrument': pair,
                'signal_type': 'macro_bias',
                'score': 0.0,
                'bias': 'neutral',
                'confidence': 0.1,
                'payload': {
                    'reasoning': reason,
                    'sentiment_velocity_flag': False,
                    'cot_extreme': False,
                    'sentiment_degraded': True,
                    'proposals': [],
                    'currency_scores': {},
                },
            }

        try:
            response_text = agent_response.get('response', '')
            logger.info(f"parse_agent_response: response length={len(response_text)} chars")
            try:
                with open('/tmp/neo_macro_response_debug.txt', 'w') as _f:
                    _f.write(response_text)
            except Exception:
                pass

            # ── Phase 1: extract currency blocks ──────────────────────────────
            # Robust bracket-balance extraction anchored on ```json fence.
            # Simple regex (.*?) breaks when reasoning fields contain embedded
            # triple-backtick sequences — it prematurely closes the match,
            # producing a truncated block that fails json.loads silently.
            json_blocks: list = []
            for _fence in re.finditer(r'```json\s*', response_text):
                _jstart = _fence.end()
                _ws = re.match(r'\s*', response_text[_jstart:])
                if _ws:
                    _jstart += _ws.end()
                if _jstart >= len(response_text) or response_text[_jstart] not in ('{', '['):
                    continue
                _depth, _in_str, _esc = 0, False, False
                for _i, _ch in enumerate(response_text[_jstart:], _jstart):
                    if _esc:
                        _esc = False
                        continue
                    if _ch == '\\' and _in_str:
                        _esc = True
                        continue
                    if _ch == '"':
                        _in_str = not _in_str
                        continue
                    if not _in_str:
                        if _ch in ('{', '['):
                            _depth += 1
                        elif _ch in ('}', ']'):
                            _depth -= 1
                            if _depth == 0:
                                json_blocks.append(response_text[_jstart:_i + 1])
                                break
            logger.info(f"parse_agent_response: {len(json_blocks)} JSON blocks found")

            if not json_blocks:
                # Bracket/brace-balance fallback — no ```json fence found.
                # Handles both array [...] and object {...} top-level responses.
                for _opener, _closer in ('[', ']'), ('{', '}'):
                    _depth, _start_idx, _in_str, _esc = 0, -1, False, False
                    for _i, _ch in enumerate(response_text):
                        if _esc:
                            _esc = False
                            continue
                        if _ch == '\\' and _in_str:
                            _esc = True
                            continue
                        if _ch == '"':
                            _in_str = not _in_str
                            continue
                        if not _in_str:
                            if _ch == _opener:
                                if _depth == 0:
                                    _start_idx = _i
                                _depth += 1
                            elif _ch == _closer:
                                _depth -= 1
                                if _depth == 0 and _start_idx >= 0:
                                    json_blocks.append(response_text[_start_idx:_i + 1])
                                    _start_idx = -1
                    if json_blocks:
                        break

            currency_scores: dict = {}
            for block in json_blocks:
                try:
                    data = json.loads(block)
                except json.JSONDecodeError:
                    continue
                # Handle both a single object {"currency":...} and an array [{...},{...}]
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict) and 'currency' in item:
                        ccy = str(item['currency']).upper().strip()
                        if ccy in self.CURRENCIES:
                            currency_scores[ccy] = item

            logger.info(f"parse_agent_response: parsed {len(currency_scores)}/8 currency scores: "
                        f"{sorted(currency_scores.keys())}")

            # Total-failure fallback — no currency blocks at all
            if not currency_scores:
                logger.warning(
                    "parse_agent_response: no currency blocks found — returning 20 degraded signals"
                )
                logger.warning(f"Response text (first 1000 chars): {response_text[:1000]!r}")
                log_event('PARSE_FAILURE',
                          'Macro LLM returned no parseable currency blocks — degraded signals emitted',
                          category='DATA', agent='macro')
                return [_neutral_pair_signal(p, 'Agent response parse failure — no currency blocks')
                        for p in self.PAIRS]

            # Gap-fill missing currencies
            _missing_ccys = [c for c in self.CURRENCIES if c not in currency_scores]
            if _missing_ccys:
                logger.warning(f"parse_agent_response: gap-filling missing currencies: {_missing_ccys}")
                log_event('GAP_FILL',
                          f'LLM scored {len(currency_scores)}/8 currencies — gap-filled: {_missing_ccys}',
                          category='DATA', agent='macro',
                          payload={'scored': sorted(currency_scores.keys()), 'missing': _missing_ccys})
                for _ccy in _missing_ccys:
                    currency_scores[_ccy] = _neutral_currency(_ccy)

            # ── Phase 2: derive pair signals ──────────────────────────────────
            ukoil_momentum = self.get_ukoil_30d_momentum()
            signals = []

            for pair in self.PAIRS:
                base_ccy, quote_ccy = self.PAIR_DERIVATION[pair]
                base_data  = currency_scores[base_ccy]
                quote_data = currency_scores[quote_ccy]

                base_score  = float(base_data.get('score',  0.0) or 0.0)
                quote_score = float(quote_data.get('score', 0.0) or 0.0)
                base_conf   = float(base_data.get('confidence',  0.1) or 0.1)
                quote_conf  = float(quote_data.get('confidence', 0.1) or 0.1)

                raw_score = base_score - quote_score

                # Safe-haven adjustment: demand strengthens JPY/CHF on quote side
                if quote_ccy in ('JPY', 'CHF'):
                    safe_haven = float(quote_data.get('safe_haven_demand') or 0.0)
                    raw_score -= safe_haven * 0.3

                # Crude oil adjustment for CAD pairs
                if base_ccy == 'CAD' or quote_ccy == 'CAD':
                    cad_oil_adj = ukoil_momentum * 0.25
                    if base_ccy == 'CAD':
                        raw_score += cad_oil_adj
                    else:
                        raw_score -= cad_oil_adj

                # Cross-pair amplifier (non-USD pairs have amplified relative moves)
                if pair not in self.USD_PAIRS:
                    raw_score *= 1.5

                # Clamp to [-1.0, 1.0]
                pair_score = max(-1.0, min(1.0, raw_score))

                # Derive bias
                if pair_score > 0.05:
                    bias = 'bullish'
                elif pair_score < -0.05:
                    bias = 'bearish'
                else:
                    bias = 'neutral'

                # Confidence: min of both currency confidences
                pair_conf = min(base_conf, quote_conf)

                # Build reasoning string from currency reasonings
                base_reasoning  = base_data.get('reasoning',  '') or ''
                quote_reasoning = quote_data.get('reasoning', '') or ''
                pair_reasoning  = (
                    f"{base_ccy}: {base_reasoning[:120].rstrip('.')}. "
                    f"{quote_ccy}: {quote_reasoning[:120].rstrip('.')}."
                )

                signals.append({
                    'agent_name':  self.AGENT_NAME,
                    'instrument':  pair,
                    'signal_type': 'macro_bias',
                    'score':       round(pair_score, 4),
                    'bias':        bias,
                    'confidence':  round(pair_conf, 4),
                    'payload': {
                        'reasoning':              pair_reasoning,
                        'sentiment_velocity_flag': bool(
                            base_data.get('sentiment_velocity_flag') or
                            quote_data.get('sentiment_velocity_flag')
                        ),
                        'cot_extreme': bool(
                            base_data.get('cot_extreme') or
                            quote_data.get('cot_extreme')
                        ),
                        'sentiment_degraded': bool(
                            base_data.get('sentiment_degraded') or
                            quote_data.get('sentiment_degraded')
                        ),
                        'proposals': (
                            list(base_data.get('proposals', []) or []) +
                            list(quote_data.get('proposals', []) or [])
                        ),
                        'currency_scores': {
                            base_ccy:  {'score': base_score,  'confidence': base_conf},
                            quote_ccy: {'score': quote_score, 'confidence': quote_conf},
                        },
                        'ukoil_momentum': round(ukoil_momentum, 4),
                    },
                })

            logger.info(f"parse_agent_response: derived {len(signals)} pair signals from "
                        f"{len(currency_scores)} currency scores")
            return signals

        except Exception as e:
            logger.error(f"parse_agent_response failed: {e}", exc_info=True)
            return [_neutral_pair_signal(p, f'parse_agent_response exception: {e}')
                    for p in self.PAIRS]
    
    def write_signals_to_database(self, signals: List[Dict]) -> bool:
        """Write generated signals to the database."""
        try:
            with self.db_conn.cursor() as cur:
                for signal in signals:
                    # Calculate expiry time
                    expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.SIGNAL_EXPIRY_MINUTES)
                    
                    # Insert signal
                    cur.execute("""
                        INSERT INTO forex_network.agent_signals 
                        (agent_name, user_id, instrument, signal_type, score, bias, confidence, payload, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        signal.get('agent_name', self.AGENT_NAME),
                        self.user_id,
                        signal.get('instrument'),
                        signal.get('signal_type', 'macro_bias'),
                        signal.get('score', 0.0),
                        signal.get('bias', 'neutral'),
                        signal.get('confidence', 0.0),
                        json.dumps(signal.get('payload', {})),
                        expires_at
                    ))
                
                # Feature 1: Signal persistence tracking
                for signal in signals:
                    instrument = signal.get('instrument')
                    if not instrument:
                        continue
                    bias = signal.get('bias', 'neutral')
                    cur.execute("""
                        INSERT INTO forex_network.signal_persistence
                            (user_id, agent_name, instrument, current_bias,
                             consecutive_cycles, first_seen_at, last_updated)
                        VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                        ON CONFLICT (user_id, agent_name, instrument)
                        DO UPDATE SET
                            consecutive_cycles = CASE
                                WHEN signal_persistence.current_bias = EXCLUDED.current_bias
                                THEN LEAST(signal_persistence.consecutive_cycles + 1, 20)
                                ELSE 1
                            END,
                            current_bias = EXCLUDED.current_bias,
                            first_seen_at = CASE
                                WHEN signal_persistence.current_bias = EXCLUDED.current_bias
                                THEN signal_persistence.first_seen_at
                                ELSE NOW()
                            END,
                            last_updated = NOW()
                    """, (self.user_id, self.AGENT_NAME, instrument, bias))

                self.db_conn.commit()
                logger.info(f"Successfully wrote {len(signals)} signals to database")
                return True

        except Exception as e:
            logger.error(f"Failed to write signals to database: {e}")
            self.db_conn.rollback()
            return False

    def update_heartbeat(self) -> bool:
        """Update agent heartbeat in the database."""
        try:
            with self.db_conn.cursor() as cur:
                # Upsert heartbeat record
                cur.execute("""
                    INSERT INTO forex_network.agent_heartbeats 
                    (agent_name, user_id, session_id, last_seen, status, cycle_count)
                    VALUES (%s, %s, %s, NOW(), 'active', %s)
                    ON CONFLICT (agent_name, user_id)
                    DO UPDATE SET 
                        last_seen = NOW(),
                        status = 'active', 
                        cycle_count = EXCLUDED.cycle_count,
                        session_id = EXCLUDED.session_id
                """, (self.AGENT_NAME, self.user_id, self.session_id, self.cycle_count))
                
                self.db_conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {e}")
            self.db_conn.rollback()
            return False
    
    def run_cycle(self) -> bool:
        """Execute one complete macro analysis cycle."""
        cycle_start = time.time()
        self.cycle_count += 1
        
        logger.info(f"Starting macro agent cycle #{self.cycle_count}")
        
        try:
            # Check kill switch first
            if self.check_kill_switch():
                logger.warning("Kill switch active - skipping cycle")
                return False
            
            # Update heartbeat at start of cycle
            self.update_heartbeat()
            
            # Build conversation context with current market data
            conversation_context = self.build_conversation_context()
            
            # Call Anthropic API with MCP tools for analysis
            agent_response = self.call_anthropic_agent(conversation_context)
            
            # Parse structured signals from agent response
            signals = self.parse_agent_response(agent_response)
            
            # Write signals to database
            success = self.write_signals_to_database(signals)
            
            # Final heartbeat update
            self.update_heartbeat()
            
            cycle_duration = time.time() - cycle_start
            logger.info(f"Cycle #{self.cycle_count} completed in {cycle_duration:.2f}s - Success: {success}")
            
            return success
            
        except Exception as e:
            logger.error(f"Cycle #{self.cycle_count} failed: {e}")
            self.update_heartbeat()  # Update heartbeat even on failure
            return False
    
    def run_continuous(self):
        """Run the macro agent continuously with 15-minute cycles."""
        logger.info("Starting macro agent continuous operation")
        
        while True:
            try:
                # Run one cycle
                success = self.run_cycle()
                
                if not success:
                    logger.warning("Cycle failed - continuing to next cycle")
                
                # Sleep for cycle interval
                sleep_seconds = self.CYCLE_INTERVAL_MINUTES * 60
                logger.info(f"Sleeping for {sleep_seconds} seconds until next cycle")
                time.sleep(sleep_seconds)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal - shutting down")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous loop: {e}")
                # Sleep and continue
                time.sleep(60)
    
    def run_single_cycle(self):
        """Run a single cycle for testing purposes."""
        logger.info("Running single macro agent cycle")
        return self.run_cycle()
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()
        logger.info("Macro agent resources cleaned up")

# Module-level state tracker for market-transition detection
_LAST_KNOWN_MARKET_STATE = None  # set/checked inside _interruptible_sleep


def _interruptible_sleep(total_seconds: int, check_interval: int = 30) -> bool:
    """Sleep for total_seconds, but wake early if the market state changes.

    Checks market state every check_interval seconds.  Returns True if a
    state transition was detected (sleep cut short), False if sleep completed
    normally.  Swallows get_market_state() exceptions so a transient API
    failure never interrupts the sleep silently.
    """
    global _LAST_KNOWN_MARKET_STATE
    start = time.time()
    while time.time() - start < total_seconds:
        remaining = total_seconds - (time.time() - start)
        time.sleep(min(check_interval, max(0, remaining)))
        try:
            current_state = get_market_state().get("state")
            if _LAST_KNOWN_MARKET_STATE is None:
                _LAST_KNOWN_MARKET_STATE = current_state
            elif current_state != _LAST_KNOWN_MARKET_STATE:
                logger.info(
                    f"Market state transition detected: "
                    f"{_LAST_KNOWN_MARKET_STATE} -> {current_state}. "
                    f"Breaking sleep early to fire immediate cycle."
                )
                _LAST_KNOWN_MARKET_STATE = current_state
                return True
        except Exception as e:
            logger.warning(f"Market state check during sleep failed: {e}")
    return False


def _get_active_user_ids(region="eu-west-2"):
    """Query all active user IDs from risk_parameters."""
    import psycopg2, psycopg2.extras, json, boto3
    ssm = boto3.client("ssm", region_name=region)
    sm = boto3.client("secretsmanager", region_name=region)
    endpoint = ssm.get_parameter(Name="/platform/config/rds-endpoint", WithDecryption=True)["Parameter"]["Value"]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, options="-c search_path=forex_network,shared,public",
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
    user_ids = [str(row["user_id"]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return user_ids


def main():
    """Main entry point — runs Macro Agent for ALL active users."""
    import argparse

    parser = argparse.ArgumentParser(description="Project Neo Macro Agent")
    parser.add_argument("--user", default=None,
                        help="Optional: run for a single user only (for debugging)")
    parser.add_argument("--single", action="store_true",
                        help="Run a single cycle then exit")
    parser.add_argument("--test", action="store_true",
                        help="Run configuration test only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not write to DB or execute trades")

    args = parser.parse_args()

    # Resolve user list
    if args.user:
        # Single user mode (debugging)
        user_ids = [args.user]
    else:
        # Multi-user mode (production)
        try:
            user_ids = _get_active_user_ids()
        except Exception as e:
            logger.error(f"Failed to query active users: {e}")
            sys.exit(1)

    if not user_ids:
        logger.error("No active users found in risk_parameters")
        sys.exit(1)

    logger.info(f"{len(user_ids)} active user(s): {user_ids}")

    if args.test:
        logger.info("Macro Agent test mode — verifying configuration for all users")
        try:
            for uid in user_ids:
                agent = MacroAgent(user_id=uid, dry_run=True)
                logger.info(f"  ✅ PASS: {uid}")
                agent.close() if hasattr(agent, "close") else None
            logger.info("✅ All users configured successfully")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ FAIL: {e}")
            sys.exit(1)

    # Create agent instances per user
    agents = {}
    for uid in user_ids:
        try:
            agents[uid] = MacroAgent(user_id=uid, dry_run=getattr(args, "dry_run", False))
            logger.info(f"Initialized Macro Agent for {uid}")
        except Exception as e:
            logger.error(f"Failed to initialize Macro Agent for {uid}: {e}")

    if not agents:
        logger.error("No agents initialized — exiting")
        sys.exit(1)

    try:
        if args.single:
            for uid, agent in agents.items():
                logger.info(f"--- {uid} ---")
                try:
                    agent.run_cycle()
                except Exception as e:
                    logger.error(f"Cycle failed for {uid}: {e}")
            logger.info("Single cycle complete for all users")
            sys.exit(0)
        else:
            # Continuous mode — one API call per cycle, shared across all users
            logger.info("Starting continuous operation for all users")
            agent_list = list(agents.values())
            primary = agent_list[0]
            while True:
                market = get_market_state()

                # Standby during weekend and pre-open window
                if market['state'] in ('closed', 'pre_open'):
                    logger.info(f"STANDBY — {market['reason']}, skipping cycle")
                    for agent in agent_list:
                        try:
                            agent.update_heartbeat()
                        except Exception:
                            pass
                    time.sleep(300)  # re-check every 5 min
                    continue

                if market['state'] == 'quiet':
                    logger.info(f"QUIET HOURS — {market['reason']}, 60-min reduced cycle")

                try:
                    # Kill switch check once for all users
                    if primary.check_kill_switch():
                        logger.warning("Kill switch active - skipping cycle")
                        time.sleep(60)
                        continue

                    # Increment cycle count and heartbeat for all users
                    for agent in agent_list:
                        agent.cycle_count += 1
                        agent.update_heartbeat()
                    save_state('macro', AGENT_SCOPE_USER_ID, 'cycle_count', primary.cycle_count)

                    # ONE API call — analysis is identical for all users
                    logger.info("Running macro analysis (shared across all users)")
                    conversation_context = primary.build_conversation_context()
                    agent_response = primary.call_anthropic_agent(conversation_context)
                    signals = primary.parse_agent_response(agent_response)

                    # Write signals and update heartbeat for each user
                    for uid, agent in agents.items():
                        logger.info(f"Writing signals for {uid}")
                        agent.write_signals_to_database(signals)
                        agent.update_heartbeat()

                except Exception as e:
                    logger.error(f"Cycle failed: {e}")
                    try:
                        for agent in agent_list:
                            agent.update_heartbeat()
                    except Exception as hb_err:
                        logger.error(f"Heartbeat update failed: {hb_err}")

                # Quiet hours: interruptible 60-min sleep
                # Wakes early on: state transition OR 06:40/12:40 UTC pre-open window
                if market['state'] == 'quiet':
                    _sleep_start = time.time()
                    _sleep_total = 3600
                    while time.time() - _sleep_start < _sleep_total:
                        time.sleep(30)
                        for _agent in agent_list:
                            try:
                                _agent.update_heartbeat()
                            except Exception:
                                pass
                        import datetime as _dt
                        _now = _dt.datetime.now(_dt.timezone.utc)
                        _hr, _min = _now.hour, _now.minute
                        _pre_open = (
                            (_hr == 6  and _min >= 40) or  # London pre-open 06:40-07:00
                            (_hr == 12 and _min >= 40)     # NY pre-open 12:40-13:00
                        )
                        if _pre_open:
                            logger.info(
                                f"Pre-open wake at {_hr:02d}:{_min:02d} UTC -- "
                                f"firing early cycle before market open"
                            )
                            break
                        try:
                            _new_state = get_market_state().get('state')
                            if _new_state != 'quiet':
                                logger.info(
                                    f"Market state transition: quiet -> {_new_state} -- "
                                    f"breaking sleep early"
                                )
                                break
                        except Exception:
                            pass
                else:
                    _interruptible_sleep(primary.CYCLE_INTERVAL_MINUTES * 60)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        for uid, agent in agents.items():
            try:
                if hasattr(agent, "close"):
                    agent.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

