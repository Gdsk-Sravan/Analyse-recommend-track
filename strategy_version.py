"""Strategy version marker for the canonical tracking dataset.

This constant is stamped onto every new tracking occurrence at the moment it
first enters `tracking_store.json`. It never changes for an existing record.

Rules (see redesign spec §3.7):
  * When trading logic, factor weights, gates, or T1/T2/STOP formulas change,
    bump this constant.
  * Do NOT rewrite historical records' strategy_version — that is the whole
    point of versioning.
  * The tracking layer is agnostic to the version string; it just records it.

The value is decoupled from `main.py` deliberately so this module can be
imported by tracking/reporting code without pulling in strategy logic.
"""
from __future__ import annotations

# Update on the day trading logic changes. Format: V<major>_FREEZE_<yyyymmdd>
# or V<major>_<label>_<yyyymmdd>. Free-form after that — just keep it short.
STRATEGY_VERSION: str = "V1_FREEZE_20260711"

# Observation-only reference target/stop percentages used by the tracking
# layer when a record has no pre-computed t1/t2/stop_price. These MIRROR the
# defaults documented in the strategy but are NOT read by the strategy —
# they exist purely so the observation layer has fallback numbers when
# migrating rows that lost their level fields.
OBSERVATION_T1_PCT: float = 0.05    # +5%
OBSERVATION_T2_PCT: float = 0.10    # +10%
OBSERVATION_STOP_PCT: float = -0.03  # -3%
