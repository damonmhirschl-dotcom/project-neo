-- Migration: 2026-04-24
-- Update max_convergence_reference to match macro score range (0.0-1.0)
-- Values = p90 of abs(base.composite_score - quote.composite_score) for each
-- profile's characteristic pair, computed from macro_percentile_thresholds.
--
-- Aggressive (d6c272e4)  -- p90 AUDJPY = 1.032
-- Balanced   (76829264)  -- p90 EURJPY = 0.724
-- Conservative (e61202e4) -- p90 USDJPY = 0.800
--
-- Rationale: old value 0.55 caused the power curve to saturate immediately
-- for any conviction > 0.55, giving all high-signal trades identical max sizing.
-- New values spread the curve across the realistic score range so conviction
-- above p75 (gate threshold) still grows position size up to the p90 ceiling.

UPDATE forex_network.risk_parameters
SET max_convergence_reference = 1.032
WHERE user_id = 'd6c272e4-a031-7053-af8e-ade000f0d0d5';  -- Aggressive

UPDATE forex_network.risk_parameters
SET max_convergence_reference = 0.724
WHERE user_id = '76829264-20e1-7023-1e31-37b7a37a1274';  -- Balanced

UPDATE forex_network.risk_parameters
SET max_convergence_reference = 0.800
WHERE user_id = 'e61202e4-30d1-70f8-9927-30b8a439e042';  -- Conservative
