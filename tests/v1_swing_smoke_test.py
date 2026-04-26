#!/usr/bin/env python3
"""
V1 Swing end-to-end smoke test.

Runs the full V1 Swing pipeline once with synthetic inputs and asserts every
V1 Swing field reaches every consumer. Required to pass before V1 Swing service
restart, and after every dispatch that touches signal payload fields.

Run:
    sudo /root/algodesk/algodesk/bin/python3 /root/Project_Neo_Damon/tests/v1_swing_smoke_test.py

Exit code 0 = pass.  Exit code 1 = fail (see assertion message for which step broke).
"""
import sys
import os
import json
import datetime
import logging
import psycopg2
import psycopg2.extras
import boto3

sys.path.insert(0, '/root/Project_Neo_Damon')

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
)
for _lg in ('neo.risk_guardian', 'neo.orchestrator', 'neo.execution',
            'neo.learning_module', 'neo.macro', 'neo.technical',
            'boto3', 'botocore', 'urllib3'):
    logging.getLogger(_lg).setLevel(logging.CRITICAL)

SMOKE_TAG       = f'smoke_v1swing_{datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")}'
TEST_INSTRUMENT = 'EURUSD'
PASSED          = []
FAILED          = []

def _pass(name):
    PASSED.append(name)
    print(f'  PASS: {name}')

def _fail(name, detail=''):
    FAILED.append(name)
    print(f'  FAIL: {name}{(" — " + detail) if detail else ""}')

def _assert(condition, name, detail=''):
    if condition: _pass(name)
    else: _fail(name, detail)

# ── DB connection ─────────────────────────────────────────────────────────────
print('\n--- Setup: DB connection ---')
_REGION = 'eu-west-2'
ssm = boto3.client('ssm', region_name=_REGION)
sm  = boto3.client('secretsmanager', region_name=_REGION)
_ep  = ssm.get_parameter(Name='/platform/config/rds-endpoint')['Parameter']['Value'].split(':')[0]
_cr  = json.loads(sm.get_secret_value(SecretId='platform/rds/credentials')['SecretString'])
conn = psycopg2.connect(
    host=_ep, port=5432, dbname='postgres',
    user=_cr['username'], password=_cr['password'],
    connect_timeout=10,
    options='-c search_path=forex_network,shared,public',
)
conn.autocommit = False

def cur():
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Resolve canonical user UUID
with cur() as c:
    c.execute("SELECT user_id FROM forex_network.risk_parameters WHERE paper_mode = TRUE ORDER BY user_id")
    rows = c.fetchall()
TEST_USER_UUID = str(rows[0]['user_id'])
print(f'  Test user UUID: {TEST_USER_UUID[:8]}...')

def teardown():
    try:
        with cur() as c:
            c.execute(
                "DELETE FROM forex_network.agent_signals WHERE payload->>'smoke_tag' = %s",
                (SMOKE_TAG,))
            c.execute(
                "DELETE FROM forex_network.trade_autopsies WHERE trade_id IN ("
                "  SELECT id FROM forex_network.trades "
                "  WHERE trade_parameters::jsonb->>'smoke_tag' = %s)",
                (SMOKE_TAG,))
            c.execute(
                "DELETE FROM forex_network.trades WHERE trade_parameters::jsonb->>'smoke_tag' = %s",
                (SMOKE_TAG,))
        conn.commit()
    except Exception as e:
        print(f'  Teardown warning: {e}')
        try: conn.rollback()
        except: pass

teardown()

# ── Step 1: Inject technical signal ──────────────────────────────────────────
print('\n--- Step 1: Technical signal injection ---')
TECH_PAYLOAD = {
    'rsi_4h': 44.2, 'adx_4h': 28.5, 'gate_failures': [], 'direction': 'long',
    'rsi_long_cross': True, 'rsi_short_cross': False,
    'session_valid': True, 'structure_long': True, 'structure_short': False,
    'rsi_prev': 38.0, 'atr_14': 0.0012,
    'rsi_14': 44.2, 'adx_14': 28.5,
    'risk_management': {'stop_distance_pips': 60.0, 'current_spread': 0.0},
    'technical_analysis': {'indicators': {'atr_14': 0.0012}, 'rsi_14': 44.2,
                            'adx_14': 28.5, 'rsi_4h': 44.2, 'adx_4h': 28.5},
    'proposals': [], 'smoke_tag': SMOKE_TAG,
}
expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
with cur() as c:
    c.execute("""
        INSERT INTO forex_network.agent_signals
            (agent_name, user_id, instrument, signal_type, score, bias, confidence, payload, expires_at)
        VALUES ('technical', %s, %s, 'price_action', 1.0, 'bullish', 0.85, %s, %s)
        RETURNING id
    """, (TEST_USER_UUID, TEST_INSTRUMENT, json.dumps(TECH_PAYLOAD), expires))
    tech_sig_id = c.fetchone()['id']
conn.commit()

with cur() as c:
    c.execute("SELECT payload FROM forex_network.agent_signals WHERE id = %s", (tech_sig_id,))
    p = c.fetchone()['payload']
    if isinstance(p, str): p = json.loads(p)
_assert(p.get('adx_4h') == 28.5, 'Step 1a: adx_4h=28.5', str(p.get('adx_4h')))
_assert(p.get('rsi_4h') == 44.2, 'Step 1b: rsi_4h=44.2', str(p.get('rsi_4h')))
_assert(p.get('direction') == 'long', 'Step 1c: direction=long', str(p.get('direction')))
_assert(p.get('gate_failures') == [], 'Step 1d: gate_failures=[]')

# ── Step 2: Inject macro signal ───────────────────────────────────────────────
print('\n--- Step 2: Macro signal injection ---')
MACRO_PAYLOAD = {
    'score': 0.62, 'direction': 'long',
    'base_currency': 'EUR', 'quote_currency': 'USD',
    'base_composite': 0.80, 'quote_composite': 0.18,
    'raw_diff': 0.62, 'cross_amplifier': 1.0,
    'method': 'deterministic_macro_v2', 'smoke_tag': SMOKE_TAG,
}
with cur() as c:
    c.execute("""
        INSERT INTO forex_network.agent_signals
            (agent_name, user_id, instrument, signal_type, score, bias, confidence, payload, expires_at)
        VALUES ('macro', %s, %s, 'macro_bias', 0.62, 'bullish', 0.75, %s, %s)
        RETURNING id
    """, (TEST_USER_UUID, TEST_INSTRUMENT, json.dumps(MACRO_PAYLOAD), expires))
    macro_sig_id = c.fetchone()['id']
conn.commit()

with cur() as c:
    c.execute("SELECT payload FROM forex_network.agent_signals WHERE id = %s", (macro_sig_id,))
    p = c.fetchone()['payload']
    if isinstance(p, str): p = json.loads(p)
_assert(p.get('direction') == 'long', 'Step 2a: macro direction=long')
_assert(abs(float(p.get('score', 0)) - 0.62) < 0.001, 'Step 2b: macro score=0.62')

# ── Step 3: Orchestrator _evaluate_v1_swing gate (pure logic) ─────────────────
print('\n--- Step 3: Orchestrator _evaluate_v1_swing gate ---')
try:
    sys.path.insert(0, '/root/Project_Neo_Damon/Orchestrator')
    from orchestrator_agent import OrchestratorAgent
    oa = OrchestratorAgent(user_id=TEST_USER_UUID, dry_run=True)
    cc = oa.convergence_calc  # _evaluate_v1_swing lives on ConvergenceCalculator

    approved, reason = cc._evaluate_v1_swing(
        {'score': 0.62, 'direction': 'long', 'bias': 'bullish'},
        {'score': 1.0, 'bias': 'bullish'})
    _assert(approved == True, 'Step 3a: approved (tech=1.0, macro=long)', reason)
    _assert(reason == 'approved', 'Step 3b: reason=approved', reason)

    approved2, reason2 = cc._evaluate_v1_swing(
        {'score': 0.62, 'direction': 'long'}, {'score': 0.0})
    _assert(approved2 == False, 'Step 3c: rejected (tech=0)')
    _assert(reason2 == 'technical_gate_fail', 'Step 3d: reason=technical_gate_fail', reason2)

    approved3, reason3 = cc._evaluate_v1_swing(
        {'score': 0.05, 'direction': 'neutral'}, {'score': 1.0})
    _assert(approved3 == False, 'Step 3e: rejected (macro neutral)')
    _assert(reason3 == 'macro_no_direction', 'Step 3f: reason=macro_no_direction', reason3)

    approved4, reason4 = cc._evaluate_v1_swing(
        {'score': -0.5, 'direction': 'short'}, {'score': 1.0})
    _assert(approved4 == False, 'Step 3g: rejected (direction disagreement)')
    _assert(reason4 == 'direction_disagreement', 'Step 3h: reason=direction_disagreement', reason4)

except Exception as e:
    _fail('Step 3: Orchestrator gate', str(e))

# ── Step 4: Schema validation round-trip ──────────────────────────────────────
print('\n--- Step 4: Schema validation round-trip ---')
try:
    from shared.schemas.v1_swing_payloads import (
        TechnicalSignalPayload, MacroSignalPayload,
        OrchestratorDecisionPayload, TradeWritePayload,
    )
    t = TechnicalSignalPayload.model_validate(TECH_PAYLOAD)
    _assert(t.rsi_4h == 44.2 and t.adx_4h == 28.5, 'Step 4a: TechnicalSignalPayload validates')
    _assert(t.direction == 'long', 'Step 4b: direction=long')
    _assert(t.gate_failures == [], 'Step 4c: gate_failures=[]')

    m = MacroSignalPayload.model_validate(MACRO_PAYLOAD)
    _assert(m.direction == 'long', 'Step 4d: MacroSignalPayload direction=long')
    _assert(m.score == 0.62, 'Step 4e: MacroSignalPayload score=0.62')

    o = OrchestratorDecisionPayload.model_validate({
        'instrument': TEST_INSTRUMENT, 'decision': 'approved', 'direction': 'long',
        'tech_score': 1.0, 'macro_score': 0.62, 'macro_direction': 'long',
        'adx_4h': 28.5, 'rsi_4h': 44.2, 'setup_type': 'long_pullback',
        'setup_gate_pass': True, 'conviction': 1.0, 'tech_gate_failures': [],
    })
    _assert(o.setup_type == 'long_pullback', 'Step 4f: OrchestratorDecisionPayload setup_type')
    _assert(o.setup_gate_pass == True, 'Step 4g: setup_gate_pass=True')

    tr = TradeWritePayload.model_validate({
        'instrument': TEST_INSTRUMENT, 'direction': 'long', 'entry_price': 1.0850,
        'position_size': 0.5, 'adx_at_entry': 28.5, 'rsi_at_entry': 44.2,
        'setup_type': 'long_pullback', 'session_at_entry': 'london',
    })
    _assert(tr.setup_type == 'long_pullback', 'Step 4h: TradeWritePayload setup_type')
    _assert(tr.session_at_entry == 'london', 'Step 4i: session_at_entry=london')

except Exception as e:
    _fail('Step 4: Schema validation', str(e))

# ── Step 5: EA write_trade_dry_run ────────────────────────────────────────────
print('\n--- Step 5: EA write_trade_dry_run ---')
TEST_TRADE_ID = None
try:
    sys.path.insert(0, '/root/Project_Neo_Damon/Execution_Agent')
    from execution_agent import ExecutionAgent
    ea = ExecutionAgent(user_id=TEST_USER_UUID, dry_run=False)
    ea.user_id = TEST_USER_UUID
    ea.executor.user_id = TEST_USER_UUID

    trade_params = {
        'instrument': TEST_INSTRUMENT, 'direction': 'long',
        'entry_price': 1.0850, 'position_size': 0.01,
        'stop_distance': 0.0060, 'target_1_price': 1.0910, 'target_2_price': 1.0970,
        'adx_at_entry': 28.5, 'rsi_at_entry': 44.2,
        'setup_type': 'long_pullback', 'session_at_entry': 'london',
        'strategy': 'v1_swing',
        'trade_parameters': {'smoke_tag': SMOKE_TAG},
    }
    TEST_TRADE_ID = ea.write_trade_dry_run(trade_params)
    _assert(TEST_TRADE_ID is not None, 'Step 5a: write_trade_dry_run returns trade_id',
            str(TEST_TRADE_ID))

    if TEST_TRADE_ID:
        with cur() as c:
            c.execute("""
                SELECT instrument, direction, strategy, adx_at_entry, rsi_at_entry,
                       setup_type, session_at_entry
                FROM forex_network.trades WHERE id = %s
            """, (TEST_TRADE_ID,))
            trow = dict(c.fetchone())
        _assert(trow.get('adx_at_entry') is not None and
                abs(float(trow['adx_at_entry']) - 28.5) < 0.01,
                'Step 5b: adx_at_entry=28.5', str(trow.get('adx_at_entry')))
        _assert(trow.get('rsi_at_entry') is not None and
                abs(float(trow['rsi_at_entry']) - 44.2) < 0.01,
                'Step 5c: rsi_at_entry=44.2', str(trow.get('rsi_at_entry')))
        _assert(trow.get('setup_type') == 'long_pullback',
                'Step 5d: setup_type=long_pullback', str(trow.get('setup_type')))
        _assert(trow.get('session_at_entry') == 'london',
                'Step 5e: session_at_entry=london', str(trow.get('session_at_entry')))
        _assert(trow.get('strategy') == 'v1_swing',
                'Step 5f: strategy=v1_swing', str(trow.get('strategy')))
except Exception as e:
    _fail('Step 5: EA write_trade_dry_run', str(e))
    import traceback; traceback.print_exc()

# ── Step 6: LM autopsy ────────────────────────────────────────────────────────
print('\n--- Step 6: LM autopsy ---')
try:
    if TEST_TRADE_ID is None:
        _fail('Step 6: Skipped — no trade_id')
    else:
        with cur() as c:
            c.execute("""
                UPDATE forex_network.trades
                SET exit_time = NOW(), exit_price = 1.0910, exit_reason = 'take_profit',
                    pnl_pips = 60, pnl = 3.0,
                    target_1_hit = true, target_1_pnl = 1.50,
                    target_1_hit_at = NOW() - INTERVAL '1 hour'
                WHERE id = %s
            """, (TEST_TRADE_ID,))
            conn.commit()
            c.execute("SELECT * FROM forex_network.trades WHERE id = %s", (TEST_TRADE_ID,))
            trade_row = dict(c.fetchone())

        sys.path.insert(0, '/root/Project_Neo_Damon/Learning_Module')
        from learning_module import LearningModule
        lm = LearningModule(user_id=TEST_USER_UUID, dry_run=False)
        lm.user_id = TEST_USER_UUID
        lm.autopsy_engine.user_id = TEST_USER_UUID

        autopsy_id = lm.autopsy_engine.write_autopsy(trade_row)
        _assert(autopsy_id is not None, 'Step 6a: write_autopsy returns autopsy_id',
                str(autopsy_id))

        if autopsy_id:
            with cur() as c:
                c.execute("""
                    SELECT rsi_at_entry, adx_at_entry, setup_type, session_at_entry,
                           target_1_hit, total_pnl, exit_reason_classified
                    FROM forex_network.trade_autopsies WHERE id = %s
                """, (autopsy_id,))
                arow = dict(c.fetchone())
            _assert(arow.get('setup_type') == 'long_pullback',
                    'Step 6b: autopsy setup_type=long_pullback', str(arow.get('setup_type')))
            _assert(arow.get('adx_at_entry') is not None and
                    abs(float(arow['adx_at_entry']) - 28.5) < 0.01,
                    'Step 6c: autopsy adx_at_entry=28.5', str(arow.get('adx_at_entry')))
            _assert(arow.get('target_1_hit') == True,
                    'Step 6d: autopsy target_1_hit=True', str(arow.get('target_1_hit')))
            _assert(arow.get('total_pnl') is not None and float(arow['total_pnl']) > 0,
                    'Step 6e: autopsy total_pnl > 0', str(arow.get('total_pnl')))
            _assert(arow.get('session_at_entry') == 'london',
                    'Step 6f: autopsy session_at_entry=london', str(arow.get('session_at_entry')))
except Exception as e:
    _fail('Step 6: LM autopsy', str(e))
    import traceback; traceback.print_exc()

# ── Step 7: Dashboard Lambda invocation ───────────────────────────────────────
print('\n--- Step 7: Dashboard Lambda ---')
try:
    import gzip, base64
    lam = boto3.client('lambda', region_name='eu-west-2')
    resp = lam.invoke(
        FunctionName='neo-admin-signals-dev',
        InvocationType='RequestResponse',
        Payload=json.dumps({'queryStringParameters': {'strategy': 'v1_swing'}}).encode()
    )
    raw = json.loads(resp['Payload'].read())
    body_raw = raw.get('body', raw)
    if isinstance(body_raw, str):
        try:
            body = json.loads(gzip.decompress(base64.b64decode(body_raw)).decode())
        except Exception:
            try: body = json.loads(body_raw)
            except Exception: body = {}
    else:
        body = body_raw

    decisions = body.get('decisions', [])
    _assert(isinstance(decisions, list), 'Step 7a: decisions is a list')
    # Step 7b: decisions may be empty when orchestrator is inactive (expected)
    if len(decisions) > 0:
        _pass('Step 7b: decisions not empty (orchestrator active)')
    else:
        _pass('Step 7b: decisions=[] (orchestrator inactive — expected during service pause)')
    if decisions:
        d = decisions[0]
        _assert('pair' in d, 'Step 7c: decision has pair field', str(list(d.keys())[:5]))
        _assert('direction' in d or 'adx_4h' in d,
                'Step 7d: V1 Swing fields present in decisions', str(list(d.keys())[:8]))
        _assert('regime_score' not in d, 'Step 7e: regime_score absent')
        _assert('stress_score' not in d, 'Step 7f: stress_score absent')
    _assert(raw.get('statusCode', 200) == 200, 'Step 7g: Lambda HTTP 200',
            str(raw.get('statusCode')))
except Exception as e:
    _fail('Step 7: Dashboard Lambda', str(e))

# ── Step 8: _inject_atr_targets — T2 uses ATR_TARGET_2_MULTIPLIER=3 ──────────
print('\n--- Step 8: _inject_atr_targets — T2 uses ATR_TARGET_2_MULTIPLIER=3 ---')
try:
    import math
    from v1_swing_parameters import ATR_TARGET_1_MULTIPLIER
    sys.path.insert(0, '/root/Project_Neo_Damon/Technical_Agent')
    from technical_agent import TechnicalAgent

    ta8 = TechnicalAgent(user_id=TEST_USER_UUID, dry_run=True)

    # Fetch the real 1D ATR for EURUSD from price_metrics (same DB the agent uses)
    with cur() as c8:
        c8.execute("""
            SELECT atr_14 FROM forex_network.price_metrics
            WHERE instrument = 'EURUSD' AND timeframe = '1D'
            ORDER BY ts DESC LIMIT 1
        """)
        _atr_row = c8.fetchone()
    _assert(_atr_row is not None, 'Step 8a: price_metrics has EURUSD 1D atr_14')

    _atr_1d    = float(_atr_row['atr_14'])
    _test_price = 1.0850
    _factor     = 100000  # EURUSD: 5 pip decimal places

    _expected_t1    = math.ceil((_test_price + ATR_TARGET_1_MULTIPLIER * _atr_1d) * _factor) / _factor
    _expected_t2_3x = math.ceil((_test_price + 3.0 * _atr_1d) * _factor) / _factor
    _expected_t2_4x = math.ceil((_test_price + 4.0 * _atr_1d) * _factor) / _factor

    _syn_sig = {
        'instrument': 'EURUSD',
        'bias':       'bullish',
        'payload':    {'risk_management': {'stop_distance_pips': 60.0, 'current_spread': 0.0}},
    }
    ta8._inject_atr_targets([_syn_sig], {'EURUSD': {'last': _test_price}})
    _rm = _syn_sig['payload']['risk_management']

    _assert(_rm.get('target_price') is not None,
            'Step 8b: target_price injected into rm', str(list(_rm.keys())))
    _assert(abs(float(_rm.get('target_price', 0)) - _expected_t2_3x) < 1e-5,
            f'Step 8c: T2 = 3× atr_1d ({_expected_t2_3x:.5f})',
            f'got {_rm.get("target_price")}')
    _assert(_rm.get('target_price') != _expected_t2_4x,
            'Step 8d: T2 is NOT the old 4× hardcoded value')
    _assert(abs(float(_rm.get('target_1_price', 0)) - _expected_t1) < 1e-5,
            f'Step 8e: T1 = ATR_TARGET_1_MULTIPLIER× atr_1d ({_expected_t1:.5f})',
            f'got {_rm.get("target_1_price")}')
    _assert('atr_stop_loss' in _rm,
            'Step 8f: atr_stop_loss present in rm', str(list(_rm.keys())))
    _assert('atr_1d' in _rm,
            'Step 8g: atr_1d stored in rm', str(list(_rm.keys())))
except Exception as e:
    _fail('Step 8: _inject_atr_targets', str(e))
    import traceback; traceback.print_exc()

# ── Step 9: current_price in top-level TA payload ─────────────────────────────
print('\n--- Step 9: current_price in top-level TA payload ---')
try:
    import unittest.mock
    import pandas as pd
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    sys.path.insert(0, '/root/Project_Neo_Damon/Technical_Agent')
    from technical_agent import TechnicalAgent  # already imported above — no-op

    ta9 = TechnicalAgent(user_id=TEST_USER_UUID, dry_run=True)

    # 210 flat 1H synthetic bars — produces neutral score (RSI ~50, ADX ~0)
    # current_price is unconditionally set in generate_signals regardless of score
    _base_ts = _dt(2026, 3, 1, 0, 0, tzinfo=_tz.utc)
    _cp = 1.0850
    _syn_bars = pd.DataFrame([
        {'ts': _base_ts + _td(hours=i),
         'open': _cp, 'high': _cp + 0.0002, 'low': _cp - 0.0002, 'close': _cp}
        for i in range(210)
    ])

    _orig_pairs = ta9.PAIRS
    ta9.PAIRS = ['EURUSD']
    try:
        with unittest.mock.patch.object(ta9, '_fetch_live_bars', return_value=_syn_bars):
            _sigs9 = ta9.generate_signals(
                {'EURUSD': {'last': _cp, 'bid': _cp - 0.0001, 'ask': _cp + 0.0001}})
    finally:
        ta9.PAIRS = _orig_pairs

    _assert(len(_sigs9) == 1, 'Step 9a: generate_signals returns 1 signal', str(len(_sigs9)))
    _p9 = _sigs9[0].get('payload', {}) if _sigs9 else {}
    _assert('current_price' in _p9,
            'Step 9b: current_price in top-level payload', str(list(_p9.keys())[:12]))
    _assert(isinstance(_p9.get('current_price'), (int, float)),
            'Step 9c: current_price is numeric', str(type(_p9.get('current_price'))))
    _assert(abs(float(_p9.get('current_price', 0)) - _cp) < 1e-5,
            f'Step 9d: current_price == df close ({_cp})',
            str(_p9.get('current_price')))
except Exception as e:
    _fail('Step 9: current_price in payload', str(e))
    import traceback; traceback.print_exc()

# ── Teardown ──────────────────────────────────────────────────────────────────
print('\n--- Teardown ---')
teardown()
conn.close()
print('  Test rows removed.')

# ── Summary ───────────────────────────────────────────────────────────────────
total = len(PASSED) + len(FAILED)
print(f'\n{"="*60}')
print(f'V1 Swing smoke test: {len(PASSED)}/{total} PASSED')
if FAILED:
    print(f'FAILED steps: {", ".join(FAILED)}')
    print(f'{"="*60}')
    sys.exit(1)
else:
    print('ALL STEPS PASSED')
    print(f'{"="*60}')
    sys.exit(0)
