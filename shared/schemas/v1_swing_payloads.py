"""
V1 Swing Pydantic schema contracts — single definition for all cross-agent payloads.

Producers: validate payload before writing to agent_signals or trades table.
Consumers: validate payload after reading from agent_signals.

Enums mirror v1_swing_parameters.py string constants.
Validation is fail-soft: ValidationError is logged as warning but does NOT block.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

# ── Literal types (mirrored from v1_swing_parameters.py) ────────────────────
DirectionType  = Optional[str]   # 'long' | 'short' | 'neutral' | None
SetupType      = Optional[str]   # 'long_pullback' | 'short_pullback' | None
SessionType    = Optional[str]   # 'asia' | 'london' | 'ny_overlap' | 'ny_late' | 'dead_zone'
StrategyType   = str             # 'v1_swing' | 'legacy'


# ── Technical agent — inner payload dict ────────────────────────────────────
class TechnicalSignalPayload(BaseModel):
    """Payload dict written by TechnicalAgent.write_signals_to_database.
    Represents the JSONB payload column for agent_name='technical' rows.
    """
    rsi_4h:           Optional[float] = None
    adx_4h:           Optional[float] = None
    gate_failures:    List[str]        = Field(default_factory=list)
    direction:        DirectionType    = None
    setup_type:       SetupType        = None
    rsi_long_cross:   Optional[bool]   = None
    rsi_short_cross:  Optional[bool]   = None
    session_valid:    Optional[bool]   = None
    structure_long:   Optional[bool]   = None
    structure_short:  Optional[bool]   = None
    rsi_prev:         Optional[float]  = None
    # Backward-compat aliases (transition period)
    rsi_14:             Optional[float] = None
    adx_14:             Optional[float] = None
    atr_14:             Optional[float] = None
    stop_distance_pips: Optional[float] = None
    trending:           Optional[bool]  = None
    session:            Optional[str]   = None
    session_weight:     Optional[float] = None
    trend_structure:    Optional[str]   = None
    swing_high:         Optional[float] = None
    swing_low:          Optional[float] = None
    error:              Optional[str]   = None

    model_config = {"extra": "allow"}  # permit unknown keys without error


# ── Macro agent — inner payload dict ────────────────────────────────────────
class MacroSignalPayload(BaseModel):
    """Payload dict written by MacroAgent.write_signals_to_database.
    Represents the JSONB payload column for agent_name='macro' rows.
    """
    score:            Optional[float] = None
    direction:        DirectionType   = None
    base_currency:    Optional[str]   = None
    quote_currency:   Optional[str]   = None
    base_composite:   Optional[float] = None
    quote_composite:  Optional[float] = None
    raw_diff:         Optional[float] = None
    cross_amplifier:  Optional[float] = None
    method:           Optional[str]   = None
    reasoning:        Optional[str]   = None
    # Error / neutral path
    reason:           Optional[str]   = None

    model_config = {"extra": "allow"}


# ── Orchestrator decision — inner payload dict ───────────────────────────────
class OrchestratorDecisionPayload(BaseModel):
    """Payload dict written by Orchestrator for agent_name='orchestrator' rows.
    Also used by the signals Lambda to build the decisions[] array.
    """
    instrument:           Optional[str]   = None
    decision:             Optional[str]   = None  # 'approved' | 'rejected'
    direction:            DirectionType   = None
    tech_score:           Optional[float] = None
    macro_score:          Optional[float] = None
    macro_direction:      DirectionType   = None
    rejection_stage:      Optional[str]   = None
    rejection_reason:     Optional[str]   = None
    conviction:           Optional[float] = None
    risk_per_trade_pct:   Optional[float] = None
    # V1 Swing entry context fields
    adx_4h:               Optional[float] = None
    rsi_4h:               Optional[float] = None
    rsi_21:               Optional[float] = None
    setup_type:           SetupType       = None
    setup_gate_pass:      Optional[bool]  = None
    tech_gate_failures:   List[str]       = Field(default_factory=list)
    macro_gate_passed:    Optional[bool]  = None
    tech_gate_passed:     Optional[bool]  = None

    model_config = {"extra": "allow"}


# ── RG decision — used in _write_decision ───────────────────────────────────
class RGDecisionPayload(BaseModel):
    """Payload serialized by RiskGuardian._write_decision into agent_signals.
    The full `decision` dict is serialized as-is; this validates its key fields.
    """
    instrument:                Optional[str]  = None
    approved:                  Optional[bool] = None
    rejection_stage:           Optional[str]  = None
    rejection_reason:          Optional[str]  = None
    rr_ratio:                  Optional[float] = None
    pip_value_gbp:             Optional[float] = None
    correlation_check_passed:  Optional[bool]  = None
    concentration_check_passed: Optional[bool] = None
    news_blackout_active:      Optional[bool]  = None

    model_config = {"extra": "allow"}


# ── Trade write payload — used in EA _write_trade ────────────────────────────
class TradeWritePayload(BaseModel):
    """Validates the V1 Swing fields that must be present when _write_trade is called.
    Ensures adx_at_entry, rsi_at_entry, setup_type, and session_at_entry are populated.
    """
    instrument:         str
    direction:          str                   # 'long' | 'short'
    strategy:           StrategyType          = 'v1_swing'
    entry_price:        float
    position_size:      float
    # V1 Swing required fields
    adx_at_entry:       Optional[float]       = None
    rsi_at_entry:       Optional[float]       = None
    setup_type:         SetupType             = None
    session_at_entry:   SessionType           = None

    model_config = {"extra": "allow"}


# ── Validation helpers ───────────────────────────────────────────────────────
import logging
_log = logging.getLogger("neo.schemas")

def validate_technical_payload(payload: dict) -> Optional[TechnicalSignalPayload]:
    """Validate technical signal payload. Logs warning on failure, returns None."""
    try:
        return TechnicalSignalPayload.model_validate(payload)
    except Exception as e:
        _log.warning(f"TechnicalSignalPayload validation failed: {e}")
        return None

def validate_macro_payload(payload: dict) -> Optional[MacroSignalPayload]:
    try:
        return MacroSignalPayload.model_validate(payload)
    except Exception as e:
        _log.warning(f"MacroSignalPayload validation failed: {e}")
        return None

def validate_orchestrator_payload(payload: dict) -> Optional[OrchestratorDecisionPayload]:
    try:
        return OrchestratorDecisionPayload.model_validate(payload)
    except Exception as e:
        _log.warning(f"OrchestratorDecisionPayload validation failed: {e}")
        return None

def validate_rg_decision(payload: dict) -> Optional[RGDecisionPayload]:
    try:
        return RGDecisionPayload.model_validate(payload)
    except Exception as e:
        _log.warning(f"RGDecisionPayload validation failed: {e}")
        return None

def validate_trade_write(instrument: str, direction: str, entry_price: float,
                          position_size: float, adx_at_entry=None, rsi_at_entry=None,
                          setup_type=None, session_at_entry=None, strategy='v1_swing',
                          **kwargs) -> Optional[TradeWritePayload]:
    try:
        return TradeWritePayload.model_validate({
            "instrument": instrument, "direction": direction,
            "entry_price": entry_price, "position_size": position_size,
            "adx_at_entry": adx_at_entry, "rsi_at_entry": rsi_at_entry,
            "setup_type": setup_type, "session_at_entry": session_at_entry,
            "strategy": strategy,
        })
    except Exception as e:
        _log.warning(f"TradeWritePayload validation failed for {instrument}: {e}")
        return None
