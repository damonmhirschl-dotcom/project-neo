# =============================================================================
# V1 Trend — Pydantic Payload Schemas
# Producers call model_dump(). Consumers call model_validate().
# Mismatches raise ValidationError, not silent NULLs.
# =============================================================================

from pydantic import BaseModel, field_validator, model_validator
from typing import Optional, List
from datetime import datetime


class V1TrendMacroSignalPayload(BaseModel):
    """Emitted by v1_trend_macro_agent per pair per 5min cycle."""
    strategy: str = "v1_trend"
    pair: str
    direction: str
    score: float
    ema50: float
    ema200: float
    ema50_10d_ago: float
    slope_ok: bool
    price_vs_ema50_pct: float
    ema_spread_pct: float
    computed_at: datetime

    @field_validator("direction")
    @classmethod
    def direction_valid(cls, v):
        assert v in ("long", "short", "neutral"), f"Invalid direction: {v}"
        return v

    @field_validator("score")
    @classmethod
    def score_range(cls, v):
        assert -1.0 <= v <= 1.0, f"Score out of range: {v}"
        return v

    @field_validator("strategy")
    @classmethod
    def strategy_valid(cls, v):
        assert v == "v1_trend", f"Wrong strategy on macro payload: {v}"
        return v


class V1TrendTechnicalSignalPayload(BaseModel):
    """Emitted by v1_trend_technical_agent per pair per 5min cycle."""
    strategy: str = "v1_trend"
    pair: str
    direction: Optional[str] = None
    setup_type: Optional[str] = None
    score: float

    adx: float
    rsi: float
    macd_line: float
    macd_signal: float
    macd_histogram: float
    macd_histogram_prev: float
    histogram_expanding: bool
    crossover_bars_ago: Optional[int] = None

    current_price: float
    atr_daily: float
    atr_stop_loss: float

    gate_failures: List[str] = []
    computed_at: datetime

    @field_validator("direction")
    @classmethod
    def direction_valid(cls, v):
        if v is not None:
            assert v in ("long", "short"), f"Invalid direction: {v}"
        return v

    @field_validator("setup_type")
    @classmethod
    def setup_type_valid(cls, v):
        if v is not None:
            assert v in ("trend_long", "trend_short"), f"Invalid setup_type: {v}"
        return v

    @field_validator("strategy")
    @classmethod
    def strategy_valid(cls, v):
        assert v == "v1_trend", f"Wrong strategy on technical payload: {v}"
        return v

    @model_validator(mode="after")
    def score_direction_consistency(self):
        if self.score != 0:
            assert self.direction is not None, "Non-zero score must have direction"
            assert self.setup_type is not None, "Non-zero score must have setup_type"
            assert len(self.gate_failures) == 0, "Non-zero score must have empty gate_failures"
        else:
            assert self.direction is None, "Zero score must have direction=None"
        return self


class V1TrendOrchestratorDecisionPayload(BaseModel):
    """Emitted by v1_trend_orchestrator to Risk Guardian."""
    strategy: str = "v1_trend"
    pair: str
    direction: str
    setup_type: str

    risk_pct: float
    current_price: float
    atr_daily: float
    atr_stop_loss: float

    stop_atr_mult: float
    t1_atr_mult: float
    t1_close_pct: float
    trail_atr_mult: float
    time_stop_days: int

    ema50_at_decision: float
    ema200_at_decision: float
    macd_at_decision: float
    adx_at_decision: float
    rsi_at_decision: float

    decided_at: datetime

    @field_validator("direction")
    @classmethod
    def direction_valid(cls, v):
        assert v in ("long", "short"), f"Invalid direction: {v}"
        return v

    @field_validator("strategy")
    @classmethod
    def strategy_valid(cls, v):
        assert v == "v1_trend", f"Wrong strategy on orchestrator payload: {v}"
        return v

    @field_validator("risk_pct")
    @classmethod
    def risk_pct_valid(cls, v):
        assert 0 < v <= 0.02, f"risk_pct out of safe range: {v}"
        return v


class V1TrendRGDecisionPayload(BaseModel):
    """Emitted by Risk Guardian to Execution Agent."""
    strategy: str = "v1_trend"
    pair: str
    direction: str
    approved: bool
    rejection_reason: Optional[str] = None
    rejection_stage: Optional[str] = None

    position_size: Optional[float] = None
    position_size_usd: Optional[float] = None
    stop_price: Optional[float] = None
    t1_price: Optional[float] = None
    trail_atr_mult: Optional[float] = None
    t1_close_pct: Optional[float] = None
    time_stop_days: Optional[int] = None
    ema50_at_entry: Optional[float] = None
    ema200_at_entry: Optional[float] = None
    macd_at_entry: Optional[float] = None

    decided_at: datetime

    @model_validator(mode="after")
    def approval_fields_present(self):
        if self.approved:
            assert self.position_size is not None, "Approved trade must have position_size"
            assert self.stop_price is not None, "Approved trade must have stop_price"
            assert self.t1_price is not None, "Approved trade must have t1_price"
        else:
            assert self.rejection_reason is not None, "Rejected trade must have rejection_reason"
            assert self.rejection_stage is not None, "Rejected trade must have rejection_stage"
        return self
