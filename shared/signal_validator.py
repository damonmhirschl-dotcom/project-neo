"""shared/signal_validator.py — Read-only signal contract validation.

Validates signal payloads at agent-to-agent boundaries.
Logs violations as warnings but NEVER blocks signal flow.
All public methods are exception-safe.

Boundary map
------------
Orchestrator (inbound)  : macro signal row, technical signal row, regime payload dict
Risk guardian (inbound) : orchestrator trade_approval payload dict
Execution agent (inbound): risk_guardian risk_approved payload dict

Signal shapes (from live agent_signals rows, 2026-04-21)
---------------------------------------------------------
macro row     : score(float), bias(str), confidence(float), payload.reasoning(str)
technical row : score(float), bias(str), confidence(float),
                payload.risk_management.{stop_distance_pips,current_spread}(float),
                payload.technical_analysis.indicators.atr_14(float),
                payload.technical_analysis(dict)
regime payload: stress_state(str), system_stress_score(float 0-100),
                regime_per_pair(dict), kill_switch_active(bool)
orchestrator trade_approval payload:
                approved(bool), convergence(float -1..1), pair(str),
                current_price(float>0), atr_14(float>0),
                effective_threshold(float 0..1), stop_price(float>0 when approved)
risk_guardian risk_approved payload:
                approved(bool), instrument(str), direction(long|short),
                stop_distance(float>0), risk_details(dict)
"""

import logging
from typing import Any, Dict, List, Optional

_LOG = logging.getLogger(__name__)

_VALID_BIASES   = {"bullish", "bearish", "neutral"}
_VALID_DIRS     = {"long", "short"}
_VALID_STRESSES = {"normal", "elevated", "high", "critical", "low"}
_VALID_REGIMES  = {"trending", "ranging", "transitional",
                   "low_volatility", "high_volatility"}


class SignalValidator:
    """Validates signal payloads at agent-to-agent boundaries.

    Usage
    -----
    validator = SignalValidator()

    # In orchestrator (per pair, after freshness check):
    validator.validate_and_log("macro", macro_sig, pair, logger)
    validator.validate_and_log("technical", tech_sig, pair, logger)

    # Once per cycle, after regime_payload is set:
    validator.validate_and_log("regime", regime_payload, None, logger)

    # In risk guardian validate_trade():
    validator.validate_and_log("orchestrator", payload, instrument, logger)

    # In execution agent execute_trade():
    validator.validate_and_log("risk_guardian", payload, instrument, logger)
    """

    # ── internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _float(d: dict, key: str,
                lo: Optional[float] = None,
                hi: Optional[float] = None) -> Optional[str]:
        val = d.get(key)
        if val is None:
            return f"missing/null: {key}"
        try:
            f = float(val)
        except (TypeError, ValueError):
            return f"not a number: {key}={val!r}"
        if lo is not None and f < lo:
            return f"range: {key}={f} < min {lo}"
        if hi is not None and f > hi:
            return f"range: {key}={f} > max {hi}"
        return None

    @staticmethod
    def _str(d: dict, key: str,
             allowed: Optional[set] = None) -> Optional[str]:
        val = d.get(key)
        if val is None:
            return f"missing/null: {key}"
        if not isinstance(val, str):
            return f"not a string: {key}={val!r}"
        if allowed and val.lower() not in allowed:
            return f"invalid value: {key}={val!r} (expected one of {sorted(allowed)})"
        return None

    @staticmethod
    def _bool(d: dict, key: str) -> Optional[str]:
        val = d.get(key)
        if val is None:
            return f"missing/null: {key}"
        if not isinstance(val, bool):
            return f"not a bool: {key}={val!r} ({type(val).__name__})"
        return None

    @staticmethod
    def _dict(d: dict, key: str) -> Optional[str]:
        val = d.get(key)
        if val is None:
            return f"missing/null: {key}"
        if not isinstance(val, dict):
            return f"not a dict: {key}={type(val).__name__}"
        return None

    @staticmethod
    def _add(violations: list, err: Optional[str]) -> None:
        if err:
            violations.append(err)

    # ── per-agent validators ────────────────────────────────────────────────

    def _validate_macro(self, sig: dict) -> List[str]:
        """sig is a full agent_signals row: score/bias/confidence at top level,
        nested payload dict with reasoning, data_sources, etc."""
        v: List[str] = []
        a = self._add
        a(v, self._float(sig, "score", -1.0, 1.0))
        a(v, self._str(sig, "bias", _VALID_BIASES))
        a(v, self._float(sig, "confidence", 0.0, 1.0))
        p = sig.get("payload")
        if not isinstance(p, dict):
            v.append("payload is missing or not a dict")
        else:
            if not p.get("reasoning"):
                v.append("payload.reasoning missing or empty")
        return v

    def _validate_technical(self, sig: dict) -> List[str]:
        """sig is a full agent_signals row: score/bias/confidence at top level,
        payload contains risk_management and technical_analysis sub-dicts."""
        v: List[str] = []
        a = self._add
        a(v, self._float(sig, "score", -1.0, 1.0))
        a(v, self._str(sig, "bias", _VALID_BIASES))
        a(v, self._float(sig, "confidence", 0.0, 1.0))
        p = sig.get("payload")
        if not isinstance(p, dict):
            v.append("payload is missing or not a dict")
            return v
        rm = p.get("risk_management")
        if not isinstance(rm, dict):
            v.append("payload.risk_management missing or not a dict")
        else:
            a(v, self._float(rm, "stop_distance_pips", 0.0))
            a(v, self._float(rm, "current_spread", 0.0))
        # atr_14 lives under technical_analysis.indicators
        ta = p.get("technical_analysis")
        if isinstance(ta, dict):
            inds = ta.get("indicators") or {}
            if isinstance(inds, dict):
                a(v, self._float(inds, "atr_14", 0.0))
        if not isinstance(p.get("technical_analysis"), dict):
            v.append("payload.technical_analysis missing or not a dict")
        # timeframe_alignment is optional; when present must be one of the three values
        _tf_align = p.get("timeframe_alignment")
        if _tf_align is not None:
            a(v, self._str(p, "timeframe_alignment",
                           {"aligned", "mixed", "conflicted"}))
        return v

    def _validate_regime(self, payload: dict) -> List[str]:
        """payload is the regime signal's payload dict (instrument=None global row).
        Keys: stress_state, system_stress_score, regime_per_pair, kill_switch_active."""
        v: List[str] = []
        a = self._add
        a(v, self._str(payload, "stress_state", _VALID_STRESSES))
        a(v, self._float(payload, "system_stress_score", 0.0, 100.0))
        a(v, self._bool(payload, "kill_switch_active"))
        if not isinstance(payload.get("regime_per_pair"), dict):
            v.append("regime_per_pair missing or not a dict")
        else:
            # Spot-check each pair entry has required subkeys
            for pair, pr in payload["regime_per_pair"].items():
                if not isinstance(pr, dict):
                    v.append(f"regime_per_pair.{pair} is not a dict")
                    continue
                if pr.get("regime") and pr["regime"] not in _VALID_REGIMES:
                    v.append(
                        f"regime_per_pair.{pair}.regime={pr['regime']!r} "
                        f"not in {sorted(_VALID_REGIMES)}"
                    )
                if pr.get("adx") is not None:
                    try:
                        adx = float(pr["adx"])
                        if adx < 0 or adx > 100:
                            v.append(f"regime_per_pair.{pair}.adx={adx} out of range 0-100")
                    except (TypeError, ValueError):
                        v.append(f"regime_per_pair.{pair}.adx not a number")
        return v

    def _validate_orchestrator(self, payload: dict) -> List[str]:
        """payload is a trade_approval signal payload.
        Keys: approved, convergence, pair, current_price, atr_14,
              effective_threshold, stop_price (when approved), rr_ratio."""
        v: List[str] = []
        a = self._add
        a(v, self._bool(payload, "approved"))
        a(v, self._float(payload, "convergence", -1.0, 1.0))
        a(v, self._float(payload, "current_price", 0.0))
        a(v, self._float(payload, "atr_14", 0.0))
        a(v, self._float(payload, "effective_threshold", 0.0, 1.0))
        if not payload.get("pair"):
            v.append("missing/null: pair")
        # stop_price required only when approved
        if payload.get("approved"):
            a(v, self._float(payload, "stop_price", 0.0))
        return v

    def _validate_risk_guardian(self, payload: dict) -> List[str]:
        """payload is a risk_approved signal payload.
        Keys: approved, instrument, direction, stop_distance, risk_details."""
        v: List[str] = []
        a = self._add
        a(v, self._bool(payload, "approved"))
        a(v, self._str(payload, "instrument"))
        a(v, self._str(payload, "direction", _VALID_DIRS))
        a(v, self._float(payload, "stop_distance", 0.0))
        a(v, self._dict(payload, "risk_details"))
        return v

    # ── dispatch table ──────────────────────────────────────────────────────

    _VALIDATORS = {
        "macro":         _validate_macro,
        "technical":     _validate_technical,
        "regime":        _validate_regime,
        "orchestrator":  _validate_orchestrator,
        "risk_guardian": _validate_risk_guardian,
    }

    # ── public API ──────────────────────────────────────────────────────────

    def validate(self, agent_name: str, payload: dict,
                 instrument: Optional[str] = None) -> List[str]:
        """Validate a signal payload.

        Returns a (possibly empty) list of violation strings.
        Empty list means valid. Never raises.
        """
        try:
            if not isinstance(payload, dict):
                return [f"payload is not a dict: {type(payload).__name__}"]
            fn = self._VALIDATORS.get(agent_name)
            if fn is None:
                return [f"unknown agent_name: {agent_name!r}"]
            return fn(self, payload)
        except Exception as exc:
            return [f"validator internal error ({agent_name}): {exc}"]

    def validate_and_log(self, agent_name: str, payload: dict,
                         instrument: Optional[str] = None,
                         logger: Optional[logging.Logger] = None) -> bool:
        """Validate and log any violations.

        Returns True if valid (no violations), False if violations found.
        Logs each violation at WARNING level. Never raises, never blocks.
        """
        try:
            violations = self.validate(agent_name, payload, instrument)
            if not violations:
                return True
            log = logger or _LOG
            tag = f"[signal_contract] {agent_name}"
            if instrument:
                tag += f"/{instrument}"
            for violation in violations:
                log.warning(f"{tag} violation: {violation}")
            return False
        except Exception:
            return True  # validator failure must never block signal flow
