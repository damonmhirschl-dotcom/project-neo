"""neo-admin-signals-dev — GET /admin/signals | GET /admin/portfolio-exposure

Routes:
  GET /admin/signals           → convergence, signals, signals_full, decisions, currency_strength
  GET /admin/portfolio-exposure → profiles (per-user USD units / positions / risk %) + portfolio currency exposure
"""
import logging
import os, json, gzip, base64, boto3, psycopg2, psycopg2.extras
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_REGION = "eu-west-2"
_conn = None

PAIR_DISPLAY = {
    # USD pairs
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD", "USDCHF": "USD/CHF",
    "NZDUSD": "NZD/USD",
    # Cross pairs
    "EURGBP": "EUR/GBP", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "EURCHF": "EUR/CHF", "GBPCHF": "GBP/CHF", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCAD": "EUR/CAD", "GBPCAD": "GBP/CAD",
    "AUDNZD": "AUD/NZD", "AUDJPY": "AUD/JPY", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY",
}


def _pair(instrument):
    if not instrument:
        return instrument
    return PAIR_DISPLAY.get(instrument.upper().replace("/", ""), instrument)


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def _get_conn():
    global _conn
    if _conn is not None:
        try:
            cur = _conn.cursor(); cur.execute("SELECT 1"); cur.close()
            return _conn
        except Exception:
            try: _conn.close()
            except: pass
            _conn = None
    ssm = boto3.client("ssm", region_name=_REGION)
    sm  = boto3.client("secretsmanager", region_name=_REGION)
    endpoint = ssm.get_parameter(
        Name=os.environ.get("RDS_SSM_PARAM", "/platform/config/rds-endpoint")
    )["Parameter"]["Value"].split(":")[0]
    creds = json.loads(sm.get_secret_value(SecretId="platform/rds/credentials")["SecretString"])
    _conn = psycopg2.connect(
        host=endpoint, port=5432, dbname="postgres",
        user=creds["username"], password=creds["password"],
        connect_timeout=10, sslmode="require",
        options="-c search_path=forex_network,shared,public",
    )
    _conn.autocommit = True
    return _conn


def _resp(status, body):
    raw   = json.dumps(body, default=_json_default)
    raw_b = raw.encode("utf-8")
    comp  = gzip.compress(raw_b)
    logger.info(
        f"Response: {len(raw_b):,} bytes raw → {len(comp):,} bytes compressed "
        f"({100*(1-len(comp)/len(raw_b)):.0f}% reduction)"
    )
    if len(comp) < len(raw_b):
        return {
            "statusCode": status,
            "headers": {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
            "body": base64.b64encode(comp).decode("utf-8"),
            "isBase64Encoded": True,
        }
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": raw,
    }


_PAYLOAD_KEEP = frozenset({
    # Generic agent fields
    "reasoning", "key_factors", "key_levels", "signal_scores",
    "upcoming_events", "pre_event_active", "pre_event_reason", "decision",
    # Regime global signal fields (read by regime panel)
    "system_stress_score", "stress_state", "stress_score_confidence",
    "regime_per_pair", "stress_components", "session_context",
    "kill_switch_active", "convergence_boost",
    # Regime per-pair signal fields (read by signals panel cards)
    "regime", "adx", "regime_confidence",
    # Macro cross-pair derivation (base/quote currency scores)
    "currency_scores",
})


def _truncate_signal(sig):
    """Keep only essential payload fields and cap reasoning at 300 chars.

    Applied to macro/technical/regime signals whose raw DB payload contains
    full LLM output (technical_analysis, session_context, proposals, etc.).
    Orchestrator card_payload is explicitly constructed -- not passed here.

    For macro deterministic_macro_v2 payloads, synthesises currency_scores
    from base_currency/base_composite and quote_currency/quote_composite so
    the dashboard renderCard cross-pair derivation JS needs no changes.
    """
    p = sig.get("payload")
    if not isinstance(p, dict):
        return sig
    trimmed = {k: v for k, v in p.items() if k in _PAYLOAD_KEEP}
    r = trimmed.get("reasoning", "")
    if r and len(r) > 300:
        trimmed["reasoning"] = r[:300] + "\u2026"
    # Synthesise currency_scores for v2 flat macro payload so renderCard
    # cross-pair derivation continues to work without JS changes.
    if "currency_scores" not in trimmed:
        cs = {}
        for side in ("base", "quote"):
            ccy   = p.get(f"{side}_currency")
            score = p.get(f"{side}_composite")
            if ccy and score is not None:
                cs[ccy] = {"score": float(score)}
        if cs:
            trimmed["currency_scores"] = cs
    sig["payload"] = trimmed
    return sig


_USD_PAIRS = {
    'EURUSD':  1, 'GBPUSD':  1, 'AUDUSD':  1, 'NZDUSD':  1,
    'USDCHF': -1, 'USDCAD': -1, 'USDJPY': -1,
}
_PROFILE_MAP = {
    'e61202e4': {'name': 'Conservative', 'order': 1},
    '76829264': {'name': 'Balanced',     'order': 2},
    'd6c272e4': {'name': 'Aggressive',   'order': 3},
}


def _handle_portfolio_exposure(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT t.user_id, t.instrument, t.direction,
               t.entry_price, t.stop_price, t.position_size_usd,
               r.max_usd_units, r.max_open_positions, r.max_portfolio_risk_pct
        FROM forex_network.trades t
        JOIN forex_network.risk_parameters r ON r.user_id = t.user_id
        WHERE t.exit_time IS NULL AND t.paper_mode = true
        ORDER BY t.user_id, t.instrument
    """)
    trades = list(cur.fetchall())

    cur.execute("""
        SELECT DISTINCT ON (user_id) user_id, account_value
        FROM forex_network.account_history
        ORDER BY user_id, snapshot_time DESC
    """)
    equity_map = {str(r['user_id']): float(r['account_value'] or 0)
                  for r in cur.fetchall()}

    # ── Per-user aggregation ─────────────────────────────────────────────────
    per_user = {}
    for t in trades:
        uid  = str(t['user_id'])
        uid8 = uid[:8]
        if uid8 not in _PROFILE_MAP:
            continue
        if uid8 not in per_user:
            per_user[uid8] = {
                'user_id':              uid,
                'name':                 _PROFILE_MAP[uid8]['name'],
                'order':                _PROFILE_MAP[uid8]['order'],
                'trades':               [],
                'max_usd_units':        int(t['max_usd_units'] or 0),
                'max_open_positions':   int(t['max_open_positions'] or 0),
                'max_portfolio_risk_pct': float(t['max_portfolio_risk_pct'] or 0),
            }
        per_user[uid8]['trades'].append(t)

    profiles = []
    for uid8, ud in sorted(per_user.items(), key=lambda x: x[1]['order']):
        tlist  = ud['trades']
        equity = equity_map.get(ud['user_id'], 148548.77)

        # USD units — signed net, then abs
        usd_sum = 0
        for t in tlist:
            inst = (t['instrument'] or '').upper()
            if inst in _USD_PAIRS:
                usd_sum += _USD_PAIRS[inst] * (1 if t['direction'] == 'long' else -1)
        usd_units_open = abs(usd_sum)

        # Portfolio risk % = Σ(stop_distance_pct × position_size_usd) / equity
        port_risk_usd = 0.0
        for t in tlist:
            ep = float(t['entry_price'] or 0)
            sp = float(t['stop_price']  or 0)
            ps = float(t['position_size_usd'] or 0)
            if ep > 0 and sp > 0:
                port_risk_usd += abs(ep - sp) / ep * ps
        port_risk_pct = round(port_risk_usd / equity * 100, 2) if equity > 0 else 0.0

        profiles.append({
            'name':                ud['name'],
            'user_id':             ud['user_id'],
            'usd_units_open':      usd_units_open,
            'usd_units_cap':       ud['max_usd_units'],
            'positions_open':      len(tlist),
            'positions_cap':       ud['max_open_positions'],
            'portfolio_risk_pct':  port_risk_pct,
            'portfolio_risk_cap':  ud['max_portfolio_risk_pct'],
        })

    # ── Cross-portfolio currency exposure ────────────────────────────────────
    ccy_exp = {}
    for t in trades:
        inst  = (t['instrument'] or '').upper()
        base  = inst[:3]
        quote = inst[3:] if len(inst) >= 6 else None
        is_long = t['direction'] == 'long'
        for ccy, bullish in ((base, is_long), (quote, not is_long)):
            if not ccy or len(ccy) != 3:
                continue
            ccy_exp.setdefault(ccy, {'long': 0, 'short': 0})
            if bullish:
                ccy_exp[ccy]['long']  += 1
            else:
                ccy_exp[ccy]['short'] += 1

    top_currencies = sorted(
        [
            {
                'currency':      ccy,
                'net_direction': 'long' if v['long'] > v['short']
                                 else 'short' if v['short'] > v['long'] else 'neutral',
                'units':         v['long'] + v['short'],
                'net':           v['long'] - v['short'],
            }
            for ccy, v in ccy_exp.items()
        ],
        key=lambda x: -x['units'],
    )

    return _resp(200, {
        'profiles': profiles,
        'portfolio': {
            'top_currencies': top_currencies[:6],
            'total_open':     len(trades),
        },
    })


def handler(event, context):
    path = (event.get('rawPath') or event.get('path') or '')
    if 'portfolio-exposure' in path:
        try:
            return _handle_portfolio_exposure(_get_conn())
        except Exception as e:
            import traceback
            return _resp(500, {"error": "portfolio_exposure_failed", "message": str(e),
                               "trace": traceback.format_exc()})

    try:
        conn = _get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        strategy = (event.get('queryStringParameters') or {}).get('strategy', 'v1_swing')

        # ── 1a. Most-recent signals per (agent, instrument, user) — used for overview aggregation.
        #        No TTL filter: most recent signal IS the current analysis until replaced.
        #        DISTINCT ON ensures one row per (agent, instrument) — always the latest.
        #        Agent liveness shown separately via agent_heartbeats (neo-admin-agents-dev).
        cur.execute("""
            SELECT DISTINCT ON (agent_name, instrument)
                   id, agent_name, instrument, user_id, signal_type,
                   score, bias, confidence, payload, created_at, expires_at
            FROM forex_network.agent_signals
            WHERE agent_name IN ('macro','technical','orchestrator','risk_guardian','execution')
            ORDER BY agent_name, instrument, created_at DESC
        """)
        raw = cur.fetchall()

        # ── Build tech_map for V1 Swing field lookup in decisions[] ─────────
        # Keys: raw instrument string (e.g. "GBPUSD") -> technical agent payload dict.
        tech_map = {}
        for _r in raw:
            if _r["agent_name"] == "technical" and _r["instrument"]:
                _p = _r["payload"]
                if isinstance(_p, str):
                    try: _p = __import__("json").loads(_p)
                    except: _p = {}
                tech_map[_r["instrument"]] = _p or {}

        # ── 1b. Latest signals within 4 h — for observation panel (shows agents
        #        even when quiet-market price-skip cycles let the TTL expire)
        cur.execute("""
            SELECT DISTINCT ON (agent_name, instrument)
                   id, agent_name, instrument, user_id, signal_type,
                   score, bias, confidence, payload, created_at, expires_at
            FROM forex_network.agent_signals
            WHERE created_at > NOW() - INTERVAL '4 hours'
              AND agent_name IN ('macro','technical','orchestrator','risk_guardian','execution')
            ORDER BY agent_name, instrument, created_at DESC
        """)
        raw_obs = cur.fetchall()

        # ── 2. Aggregate per pair for overview panel (macro, technical, regime only)
        #        Excludes orchestrator, execution, risk_guardian — their scores are not
        #        convergence signals and dilute the average shown in the dashboard.
        per_pair = {}
        for r in raw:
            if r["agent_name"] not in ("macro", "technical"):
                continue
            if r["instrument"] is None:
                continue
            pair = _pair(r["instrument"])
            per_pair.setdefault(pair, {
                "pair": pair, "agents": [], "agent_scores": {},
                "bias_counter": {"bullish": 0, "bearish": 0, "neutral": 0},
            })
            e = per_pair[pair]
            if r["agent_name"] not in e["agents"]:
                e["agents"].append(r["agent_name"])
            e["agent_scores"][r["agent_name"]] = float(r["score"] or 0)
            b = (r["bias"] or "neutral").lower()
            if b in e["bias_counter"]:
                e["bias_counter"][b] += 1

        _WEIGHTS = {"macro": 0.40, "technical": 0.40}  # V1 Swing: equal weights, regime decommissioned (2026-04-25)
        aggregated = []
        for pair, e in per_pair.items():
            if not e["agent_scores"]:
                continue
            weighted_score = sum(
                e["agent_scores"][a] * w
                for a, w in _WEIGHTS.items()
                if a in e["agent_scores"]
            )
            total_weight = sum(
                w for a, w in _WEIGHTS.items()
                if a in e["agent_scores"]
            )
            avg_score = weighted_score / total_weight if total_weight > 0 else 0.0
            bias = max(e["bias_counter"], key=e["bias_counter"].get)
            if e["bias_counter"][bias] == 0:
                bias = "neutral"
            aggregated.append({
                "pair": pair,
                "agents": " · ".join(sorted(e["agents"])),
                "bias": bias,
                "score": round(avg_score, 3),
            })
        aggregated.sort(key=lambda s: abs(s["score"]), reverse=True)

        convergence = [
            {"pair": e["pair"], "score": e["score"]}
            for e in aggregated if e["pair"] is not None
        ]

        # ── 2b. Currency strength — aggregate from deterministic_macro_v2 flat payload ──
        # v2 macro payload has flat fields: base_currency, base_composite,
        # quote_currency, quote_composite (no longer a nested currency_scores dict).
        ccy_accum = {}  # {ccy: [composite_score, ...]}
        for r in raw:
            if r["agent_name"] != "macro" or r["instrument"] is None:
                continue
            p = r["payload"]
            if isinstance(p, str):
                try: p = json.loads(p)
                except: p = {}
            if not isinstance(p, dict):
                continue
            for side in ("base", "quote"):
                ccy   = p.get(f"{side}_currency")
                score = p.get(f"{side}_composite")
                if ccy and score is not None:
                    ccy_accum.setdefault(ccy, []).append(float(score))

        currency_strength = []
        for ccy, scores in ccy_accum.items():
            avg = sum(scores) / len(scores)
            currency_strength.append({
                "currency":    ccy,
                "score":       round(avg, 3),
                "sample_size": len(scores),
            })
        currency_strength.sort(key=lambda x: x["score"], reverse=True)

        # ── 3. Full per-(agent,instrument,user) list for Agent Observation panel
        # Orchestrator writes one row per user with all pair decisions in decisions[].
        # We explode it into per-pair rows so the observation panel can group
        # orchestrator cards alongside the other agents' per-pair cards.
        signals_full = []
        for r in raw_obs:
            payload = r["payload"]
            if isinstance(payload, str):
                try: payload = json.loads(payload)
                except: payload = {}
            payload = payload or {}

            if r["agent_name"] == "orchestrator":
                # Explode: one card per pair decision so the UI cycle-groups correctly
                decisions = payload.get("decisions") or []
                base = {
                    "id":          int(r["id"]),
                    "agent_name":  "orchestrator",
                    "user_id":     str(r["user_id"]) if r["user_id"] else None,
                    "signal_type": r["signal_type"],
                    "score":       float(r["score"] or 0),
                    "bias":        r["bias"],
                    "confidence":  float(r["confidence"] or 0),
                    "created_at":  r["created_at"].isoformat() if r["created_at"] else None,
                    "expires_at":  r["expires_at"].isoformat() if r["expires_at"] else None,
                }
                # Shared context fields for every card.
                # Omit fields that are None so null values don't spread into card_payload
                # (stress_score/stress_state are None post-regime-decommission).
                ctx = {k: payload[k] for k in
                       ("cycle","stress_score","stress_state","current_session",
                        "day_of_week","effective_threshold","threshold_breakdown",
                        "approved_count","rejected_count","open_positions","risk_budget")
                       if k in payload and payload[k] is not None}

                if decisions:
                    _WEIGHTS = {"macro": 0.40, "technical": 0.40}  # V1 Swing: equal weights, regime decommissioned (2026-04-25)
                    for dec in decisions:
                        pair_raw = dec.get("pair", "")
                        pair_disp = _pair(pair_raw) or pair_raw

                        # Human-readable decision string for renderCard badge
                        dec_str = "APPROVED" if dec.get("approved") else "REJECTED"

                        # Build reasoning from rejection reasons or approval context
                        det = dec.get("convergence_detail") or {}
                        if dec.get("approved"):
                            aligned = [a for a in ("macro", "technical")
                                       if det.get(f"{a}_bias") == dec.get("bias")]
                            reasoning = (
                                f"{pair_raw} {(dec.get('bias') or '').upper()} approved. "
                                f"Convergence {dec.get('convergence', 0):.3f} > threshold "
                                f"{dec.get('effective_threshold', 0):.3f}."
                            )
                            if aligned:
                                reasoning += f" Aligned agents: {', '.join(aligned)}."
                        else:
                            reasons = dec.get("rejection_reasons") or []
                            reasoning = ("; ".join(reasons)
                                         if reasons else "Rejected — insufficient convergence.")

                        # Build signal_scores dict from convergence_detail flat fields
                        signal_scores = {}
                        for agent, w in _WEIGHTS.items():
                            sv = det.get(f"{agent}_score")
                            if sv is not None:
                                signal_scores[agent] = {
                                    "score":      round(float(sv), 4),
                                    "weight":     w,
                                    "bias":       det.get(f"{agent}_bias", "neutral"),
                                    "confidence": round(float(det.get(f"{agent}_confidence") or 0), 4),
                                }

                        # Flat card_payload — renderCard reads these at top level.
                        # Strip convergence_detail (already flattened into signal_scores above)
                        # and proposals (raw pair proposals, not displayed) to keep payload small.
                        _dec_slim = {k: v for k, v in dec.items() if k != "convergence_detail"}
                        # V1 Swing fields — sourced from tech_map (latest technical agent payload)
                        _tech_tp = tech_map.get(pair_raw, {})
                        _tech_score = dec.get('tech_score') or float(_tech_tp.get('score') or 0) or None
                        _raw_dir = (
                            dec.get('macro_direction')
                            or dec.get('direction')
                            or (dec.get('macro_signal') or {}).get('direction')
                            or dec.get('bias', 'neutral')
                        )
                        _direction = str(_raw_dir).lower() if _raw_dir else 'neutral'
                        # Normalise legacy bullish/bearish labels during transition
                        _direction = {'bullish': 'long', 'bearish': 'short'}.get(_direction, _direction)
                        _adx_4h_raw = _tech_tp.get('adx_4h') or _tech_tp.get('adx_14')
                        _rsi_21_raw = _tech_tp.get('rsi_4h') or _tech_tp.get('rsi_14')
                        _adx_4h = round(float(_adx_4h_raw), 1) if _adx_4h_raw is not None else None
                        _rsi_21 = round(float(_rsi_21_raw), 1) if _rsi_21_raw is not None else None
                        # GAP: technical agent emits trend_structure ('bullish'/'bearish'),
                        # not setup_type ('long_pullback'/'short_pullback').
                        # setup_type is None until technical agent adds the field.
                        _setup_type = _tech_tp.get('setup_type') or None
                        _gate_failures = _tech_tp.get('gate_failures') or []
                        _rej_list_card = dec.get('rejection_reasons') or []
                        _setup_gate_pass = not any('missing_signal' in str(r).lower() for r in _rej_list_card)
                        _rejection_stage = next((k for k, v in (dec.get('checks') or {}).items() if v == 'FAIL'), None)
                        card_payload = {
                            **ctx,
                            "decision":           dec_str,
                            "reasoning":          reasoning,
                            "signal_scores":      signal_scores or None,
                            "approved":           dec.get("approved"),
                            "convergence":        dec.get("convergence"),  # kept for one cycle
                            "rejection_reasons":  dec.get("rejection_reasons"),
                            "rejection_stage":    dec.get("rejection_stage"),
                            "rejection_reason":   dec.get("rejection_reason"),
                            "checks":             dec.get("checks"),
                            "size_multiplier":    dec.get("size_multiplier"),
                            # V1 Swing display fields
                            "direction":          _direction,
                            "tech_score":         _tech_score,
                            "tech_gate_failures": _gate_failures,
                            "adx_4h":             _adx_4h,
                            "rsi_21":             _rsi_21,
                            "setup_type":         _setup_type,
                            "setup_gate_pass":    _setup_gate_pass,
                            "rejection_stage":    _rejection_stage,
                            # Slim decision for raw payload view (convergence_detail stripped)
                            "decisions":          [_dec_slim],
                        }
                        signals_full.append({
                            **base,
                            # Use per-pair values, not row-level aggregates
                            "score":      float(dec.get("convergence") or base["score"]),
                            "bias":       dec.get("bias") or base["bias"],
                            "confidence": float(dec.get("confidence") or base["confidence"]),
                            "pair":       pair_disp,
                            "instrument": pair_raw,
                            "payload":    card_payload,
                        })
                else:
                    # No decisions yet — show the system card as-is
                    signals_full.append({
                        **base,
                        "pair":       None,
                        "instrument": None,
                        "payload":    payload,
                    })
            else:
                signals_full.append(_truncate_signal({
                    "id":          int(r["id"]),
                    "agent_name":  r["agent_name"],
                    "pair":        _pair(r["instrument"]),
                    "instrument":  r["instrument"],
                    "user_id":     str(r["user_id"]) if r["user_id"] else None,
                    "signal_type": r["signal_type"],
                    "score":       float(r["score"] or 0),
                    "bias":        r["bias"],
                    "confidence":  float(r["confidence"] or 0),
                    "payload":     payload,
                    "created_at":  r["created_at"].isoformat() if r["created_at"] else None,
                    "expires_at":  r["expires_at"].isoformat() if r["expires_at"] else None,
                }))

        # ── 4. Orchestrator decisions (last 24 h, all users) for CSV export
        cur.execute("""
            SELECT user_id, payload, created_at
            FROM forex_network.agent_signals
            WHERE agent_name = 'orchestrator'
              AND signal_type = 'orchestrator_decision'
              AND created_at > NOW() - INTERVAL '6 hours'
            ORDER BY created_at DESC
            LIMIT 50
        """)
        orch_rows = cur.fetchall()

        cur.execute("""
            SELECT user_id, agent_name, instrument, current_bias, consecutive_cycles
            FROM forex_network.signal_persistence
        """)
        pers_map = {}
        for r in cur.fetchall():
            pers_map[(str(r["user_id"]), r["instrument"], r["agent_name"])] = {
                "cycles": r["consecutive_cycles"],
                "bias":   r["current_bias"],
            }

        cur.execute("""
            SELECT user_id, instrument, convergence_score, cycle_timestamp
            FROM forex_network.convergence_history
            WHERE cycle_timestamp > NOW() - INTERVAL '3 hours'
            ORDER BY user_id, instrument, cycle_timestamp ASC
        """)
        conv_hist_map = {}
        for r in cur.fetchall():
            key = (str(r["user_id"]), r["instrument"])
            conv_hist_map.setdefault(key, [])
            conv_hist_map[key].append(float(r["convergence_score"] or 0))
        for k in conv_hist_map:
            conv_hist_map[k] = conv_hist_map[k][-6:]

        decisions = []
        for sig in orch_rows:
            payload = sig["payload"]
            if isinstance(payload, str):
                try: payload = json.loads(payload)
                except: payload = {}

            uid          = str(sig["user_id"]) if sig["user_id"] else None
            created_at   = sig["created_at"].isoformat() if sig["created_at"] else None
            cycle        = payload.get("cycle", 0)
            eff_thresh   = float(payload.get("effective_threshold") or 0)
            stress_score = payload.get("stress_score")
            stress_state = payload.get("stress_state", "")
            session      = payload.get("current_session", "")
            dow          = payload.get("day_of_week")
            open_pos     = payload.get("open_positions", 0)

            for dec in (payload.get("decisions") or []):
                pair = dec.get("pair", "")
                cd   = dec.get("convergence_detail") or {}
                # Normalise direction: prefer explicit field, fall back to bias,
                # then map legacy bullish/bearish to long/short
                _dec_raw_dir = (
                    dec.get('macro_direction')
                    or dec.get('direction')
                    or (dec.get('macro_signal') or {}).get('direction')
                    or dec.get('bias', 'neutral')
                )
                _dec_direction = str(_dec_raw_dir).lower() if _dec_raw_dir else 'neutral'
                _dec_direction = {'bullish': 'long', 'bearish': 'short'}.get(_dec_direction, _dec_direction)
                hist = cd.get("historical_bonus") or {}
                dc   = cd.get("directional_consensus") or {}
                trend = hist.get("trend") or {}

                _tech = tech_map.get(pair, {})
                _checks = dec.get("checks") or {}
                _rej_list = dec.get("rejection_reasons") or []
                _rejection_stage = next((k for k, v in _checks.items() if v == "FAIL"), None)

                mp = pers_map.get((uid, pair, "macro"), {})
                tp = pers_map.get((uid, pair, "technical"), {})

                ch_scores = conv_hist_map.get((uid, pair), [])
                conv_hist_str = " → ".join(f"{x:.3f}" for x in ch_scores) if ch_scores else ""

                decisions.append({
                    "cycle_time_utc":               created_at,
                    "cycle_number":                 cycle,
                    "user_id":                      uid,
                    "pair":                         _pair(pair),
                    "approved":                     dec.get("approved", False),
                    "rejection_reasons":            "; ".join(dec.get("rejection_reasons") or []),
                    "directional_gate":             dc.get("reason", ""),
                    "directional_direction":        dc.get("direction") or "",
                    "bias":                         dec.get("bias", ""),
                    "confidence":                   round(float(dec.get("confidence") or 0), 4),
                    "final_convergence":            round(float(dec.get("convergence") or 0), 4),
                    "base_convergence":             round(float(cd.get("convergence_score") or 0), 4),
                    "effective_threshold":          round(float(dec.get("effective_threshold") or eff_thresh), 4),
                    "persistence_bonus":            round(float(hist.get("persistence_bonus") or 0), 4),
                    "trend_modifier":               round(float(trend.get("modifier") or 0), 4),
                    "trend_state":                  trend.get("trend", ""),
                    "pattern_bonus":                round(float(hist.get("pattern_bonus") or 0), 4),
                    "total_historical_bonus":       round(float(hist.get("total_historical_bonus") or 0), 4),
                    "macro_score":                  round(float(cd.get("macro_score") or 0), 4),
                    "macro_confidence":             round(float(cd.get("macro_confidence") or 0), 4),
                    "macro_bias":                   cd.get("macro_bias") or "",
                    "macro_persistence_cycles":     mp.get("cycles", 0),
                    "technical_score":              round(float(cd.get("technical_score") or 0), 4),
                    "technical_confidence":         round(float(cd.get("technical_confidence") or 0), 4),
                    "technical_bias":               cd.get("technical_bias") or "",
                    "technical_persistence_cycles": tp.get("cycles", 0),
                    # stress_score omitted when None (regime agent decommissioned)
                    **({"stress_score": float(stress_score)} if stress_score is not None else {}),
                    "current_session":              session,
                    "day_of_week":                  dow,
                    "open_positions":               open_pos,
                    "conv_history_6cycles":         conv_hist_str,
                    # V1 Swing fields — sourced from latest technical agent signal per pair
                    "direction":                    _dec_direction,
                    "adx_4h":                       round(float(_tech["adx_14"]), 1) if _tech.get("adx_14") is not None else None,
                    "rsi_21":                       round(float(_tech["rsi_14"]), 1) if _tech.get("rsi_14") is not None else None,
                    # GAP: technical agent emits trend_structure, not setup_type;
                    # setup_type is empty until technical agent adds the field.
                    "setup_type":                   _tech.get("setup_type") or "",
                    "setup_gate_pass":              not any("missing_signal" in r.lower() for r in _rej_list),
                    "rejection_stage":              _rejection_stage,
                    "rejection_reason":             _rej_list[0] if _rej_list else "",
                })

        # ── 5. Proposals from LM (agent_signals table, filtered by strategy in payload) ──
        cur.execute("""
            SELECT payload, created_at
            FROM forex_network.agent_signals
            WHERE agent_name = 'learning'
              AND signal_type = 'learning_review'
              AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC
            LIMIT 5
        """)
        lm_raw = cur.fetchall()
        lm_proposals = []
        for row in lm_raw:
            p = row["payload"]
            if isinstance(p, str):
                try: p = json.loads(p)
                except: p = {}
            for prop in (p.get("proposals") or []):
                # Filter by strategy if prop carries it; else include all (legacy)
                prop_strat = prop.get("strategy") or prop.get("proposal_strategy")
                if prop_strat and prop_strat != strategy:
                    continue
                lm_proposals.append({
                    **prop,
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "agent": prop.get("agent_name") or "learning",
                })

        _payload = {
            "convergence":       convergence,
            "signals":           aggregated,
            "signals_full":      signals_full,
            "decisions":         decisions,
            "currency_strength": currency_strength,
            "lm_proposals":      lm_proposals,
        }
        _body_bytes = json.dumps(_payload, default=_json_default).encode("utf-8")
        logger.info(
            f"Response size: {len(_body_bytes):,} bytes "
            f"({len(_body_bytes)/1024/1024:.2f} MB) — "
            f"convergence={len(convergence)} signals={len(aggregated)} "
            f"signals_full={len(signals_full)} decisions={len(decisions)} "
            f"currency_strength={len(currency_strength)}"
        )
        return _resp(200, _payload)

    except Exception as e:
        import traceback
        return _resp(500, {"error": "signals_failed", "message": str(e), "trace": traceback.format_exc()})
