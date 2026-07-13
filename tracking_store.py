"""Canonical tracking store — one record per (symbol, first_seen_date).

This module is the SINGLE SOURCE OF TRUTH for the observation dataset.
It is READ-ONLY with respect to strategy state — it never modifies
`trade_tracker.json`, `tracker.json`, `portfolio.json`, `shadow_master.xlsx`,
or any other pre-existing file. All persisted state lives in:

    results/tracking_store.json     — canonical records (atomic writes)
    results/tracking_events.jsonl   — append-only stage/hit events
    results/weekly_review_history.jsonl — append-only weekly summaries

Invariants enforced (see redesign spec §3.4):
    1. No two records share tracking_id.
    2. `entry.*` fields are set exactly once and never overwritten.
    3. `strategy_version` never changes for an existing record.
    4. `t1_hit_date`, `t2_hit_date`, `stop_hit_date` are write-once.
    5. `mfe_pct` can only increase; `mae_pct` can only decrease.
    6. `journey_path` is append-only (prefix of old must equal old).
    7. Atomic save: tmp + os.replace.
    8. Rerunning `upsert()` for the same (symbol, day) is idempotent.

The store is deliberately small (~300 LoC of Python) so it can be audited
line-by-line. Nothing here decides trading behaviour.
"""
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Local imports (kept small so this module is easy to test in isolation)
try:
    from strategy_version import (  # type: ignore
        STRATEGY_VERSION,
        OBSERVATION_T1_PCT,
        OBSERVATION_T2_PCT,
        OBSERVATION_STOP_PCT,
    )
except Exception:  # pragma: no cover — allows running from repo-root
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from strategy_version import (  # type: ignore
        STRATEGY_VERSION,
        OBSERVATION_T1_PCT,
        OBSERVATION_T2_PCT,
        OBSERVATION_STOP_PCT,
    )

# ---------------------------------------------------------------------------
# Constants & paths
# ---------------------------------------------------------------------------

# Candidate stages — the pipeline emits these labels (see agent-2 field report).
# Values are the union: main.py's watchlist tier classifier emits
# NEAR_MISS, NEAR_MISS_RISING, NEAR_MISS_FADING, DEVELOPING, MONITOR.
CANDIDATE_STAGES: Tuple[str, ...] = (
    "BUY",
    "WATCHLIST",
    "DEVELOPING",
    "NEAR_MISS",
    "NEAR_MISS_RISING",
    "NEAR_MISS_FADING",
    "MONITOR",
)

# Tracking status lifecycle.
STATUS_ACTIVE = "ACTIVE"
STATUS_T1_HIT = "T1_HIT"        # T1 reached, still tracking toward T2
STATUS_T2_HIT = "T2_HIT"        # terminal
STATUS_STOPPED = "STOPPED"      # terminal
STATUS_STOPPED_AFTER_T1 = "STOPPED_AFTER_T1"  # terminal, informative
TERMINAL_STATUSES = frozenset({STATUS_T2_HIT, STATUS_STOPPED, STATUS_STOPPED_AFTER_T1})

# Origin whitelist — where a tracked occurrence entered the pipeline from.
# Purely informational; no invariant depends on the exact value.
KNOWN_ORIGINS = (
    "buys_list",
    "watchlist_tier",
    "rejected_list",
    "shadow_bucket_A",
    "shadow_bucket_B",
    "shadow_bucket_C",
    "shadow_bucket_D",
    "tracker_v2_buys",
    "tracker_v2_watchlist",
    "watchlist_persist",
    "migration",
)

# Migration status.
MIGRATION_NATIVE = "NATIVE"      # created by tracking_store itself
MIGRATION_MIGRATED = "MIGRATED"  # created by migrate_to_canonical.py, confident
MIGRATION_NEEDS_REVIEW = "NEEDS_REVIEW"

# Root dirs (resolved relative to this file so import order doesn't matter).
_HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = _HERE / "results"
DEFAULT_STORE_PATH = DEFAULT_RESULTS_DIR / "tracking_store.json"
DEFAULT_EVENTS_PATH = DEFAULT_RESULTS_DIR / "tracking_events.jsonl"
DEFAULT_WEEKLY_HISTORY_PATH = DEFAULT_RESULTS_DIR / "weekly_review_history.jsonl"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class TrackingInvariantError(Exception):
    """Raised when a caller tries to violate an invariant."""


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _today_str(today: Optional[date] = None) -> str:
    return (today or date.today()).isoformat()


def _as_date(s: Any) -> Optional[date]:
    if isinstance(s, date):
        return s
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s)[:10]).date()
    except Exception:
        return None


def _atomic_write_json(path: Path, data: Any) -> None:
    """Write JSON atomically (tmp file + os.replace).

    This is one of the invariants — every pre-existing writer uses raw
    `open(f, "w")` which can corrupt state on crash. We do it right here.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def make_tracking_id(symbol: str, first_seen: Optional[str] = None) -> str:
    """Canonical tracking-ID format: SYMBOL_YYYYMMDD (no .NS suffix stripped)."""
    d = _as_date(first_seen) or date.today()
    return f"{symbol}_{d.strftime('%Y%m%d')}"


# ---------------------------------------------------------------------------
# TrackingStore
# ---------------------------------------------------------------------------

class TrackingStore:
    """In-memory canonical dataset with atomic-write persistence.

    Typical use:
        store = TrackingStore.load()
        store.upsert(symbol="CDSL", stage="WATCHLIST", stock=stock_dict, ...)
        store.update_prices({"CDSL": {"high": ..., "low": ..., "close": ...}})
        store.save()
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        records: Optional[Dict[str, Dict[str, Any]]] = None,
        strategy_version: str = STRATEGY_VERSION,
        store_path: Path = DEFAULT_STORE_PATH,
        events_path: Path = DEFAULT_EVENTS_PATH,
    ) -> None:
        self.records: Dict[str, Dict[str, Any]] = records or {}
        self.strategy_version = strategy_version
        self.store_path = Path(store_path)
        self.events_path = Path(events_path)

    # ------------------------------------------------------------------
    # Load / save
    # ------------------------------------------------------------------

    @classmethod
    def load(
        cls,
        store_path: Path = DEFAULT_STORE_PATH,
        events_path: Path = DEFAULT_EVENTS_PATH,
    ) -> "TrackingStore":
        store_path = Path(store_path)
        if not store_path.exists():
            return cls(store_path=store_path, events_path=events_path)
        try:
            with store_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            # Corrupt / partial file — start fresh but preserve corrupt copy.
            bad = store_path.with_suffix(".corrupt.json")
            try:
                store_path.rename(bad)
            except OSError:
                pass
            return cls(store_path=store_path, events_path=events_path)
        return cls(
            records=data.get("records", {}),
            strategy_version=data.get("strategy_version", STRATEGY_VERSION),
            store_path=store_path,
            events_path=events_path,
        )

    def save(self) -> None:
        payload = {
            "schema_version": self.SCHEMA_VERSION,
            "strategy_version": self.strategy_version,
            "last_saved": datetime.now().isoformat(timespec="seconds"),
            "records": self.records,
        }
        _atomic_write_json(self.store_path, payload)

    # ------------------------------------------------------------------
    # Event log (append-only)
    # ------------------------------------------------------------------

    def _log_event(self, tracking_id: str, kind: str, **fields: Any) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        line = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "tracking_id": tracking_id,
            "kind": kind,
            **fields,
        }
        with self.events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line, default=str) + "\n")

    # ------------------------------------------------------------------
    # Core: upsert
    # ------------------------------------------------------------------

    def find_active(self, symbol: str) -> Optional[str]:
        """Return the tracking_id of the currently ACTIVE record for `symbol`, if any.

        A record is "active" when its status is one of {ACTIVE, T1_HIT}. Terminal
        records are ignored so a stock that finished a prior tracking journey can
        legitimately re-enter with a new first_seen_date.
        """
        for tid, rec in self.records.items():
            if rec.get("symbol") != symbol:
                continue
            if rec.get("tracking_status") in TERMINAL_STATUSES:
                continue
            return tid
        return None

    def upsert(
        self,
        *,
        symbol: str,
        stage: str,
        entry_snapshot: Optional[Dict[str, Any]] = None,
        origin: str = "unknown",
        as_of: Optional[str] = None,
        current_snapshot: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create or update the canonical record for `symbol`.

        Returns the tracking_id.

        Rules:
          * If no ACTIVE record exists for `symbol`, create one with `entry_snapshot`
            frozen — `strategy_version`, `first_seen_date`, and all `entry.*`
            fields are set exactly once here.
          * If an ACTIVE record already exists, we only update `current.*` and,
            if `stage` differs, append to `journey_path`.
          * `entry_snapshot` is IGNORED on updates. It cannot mutate.
        """
        as_of = as_of or _today_str()
        tid = self.find_active(symbol)

        if tid is None:
            # Fresh occurrence.
            tid = make_tracking_id(symbol, as_of)
            if tid in self.records:
                # Same tracking-id already exists but is terminal — must not clash.
                # Advance the date one day to guarantee uniqueness.
                d = _as_date(as_of) or date.today()
                for i in range(1, 400):
                    cand = f"{symbol}_{(d + timedelta(days=i)).strftime('%Y%m%d')}"
                    if cand not in self.records:
                        tid = cand
                        as_of = (d + timedelta(days=i)).isoformat()
                        break

            entry = _build_entry_snapshot(entry_snapshot or {}, stage=stage, origin=origin)
            rec = {
                "tracking_id": tid,
                "symbol": symbol,
                "first_seen_date": as_of,
                "strategy_version": self.strategy_version,
                "entry": entry,
                "current": _build_current_snapshot(current_snapshot or {}, stage=stage, as_of=as_of),
                "journey_path": [stage],
                "stage_change_count": 0,
                "previous_stage": None,
                "last_stage_change_date": as_of,
                "tracking_status": STATUS_ACTIVE,
                "t1_hit": False, "t1_hit_date": None, "days_to_t1": None,
                "t2_hit": False, "t2_hit_date": None, "days_to_t2": None,
                "stop_hit": False, "stop_hit_date": None, "days_to_stop": None,
                "mfe_pct": 0.0,
                "mae_pct": 0.0,
                "max_price_seen": entry.get("reference_entry_price"),
                "min_price_seen": entry.get("reference_entry_price"),
                "days_active": 0,
                "migration_status": MIGRATION_NATIVE,
                "created_at": as_of,
                "updated_at": as_of,
            }
            self.records[tid] = rec
            self._log_event(tid, "created", stage=stage, origin=origin)
            return tid

        # Existing active record — updates only.
        rec = self.records[tid]
        # Invariant 3: strategy_version never changes.
        if rec.get("strategy_version") != self.strategy_version:
            # A version bump happened while this record was active. Keep the
            # OLD version — this is exactly what freeze-time capture is for.
            pass

        # Stage transition.
        cur_stage = (rec.get("current") or {}).get("stage")
        if stage and stage != cur_stage:
            path: List[str] = list(rec.get("journey_path") or [])
            # Invariant 6: journey_path is append-only. Simply append.
            path.append(stage)
            rec["journey_path"] = path
            rec["previous_stage"] = cur_stage
            rec["stage_change_count"] = int(rec.get("stage_change_count") or 0) + 1
            rec["last_stage_change_date"] = as_of
            self._log_event(
                tid,
                "stage_change",
                from_stage=cur_stage,
                to_stage=stage,
            )

        # Current snapshot: full replace of `current.*` is fine.
        rec["current"] = _build_current_snapshot(
            current_snapshot or {}, stage=stage, as_of=as_of
        )
        rec["updated_at"] = as_of
        # days_active is derived from first_seen_date at price-update time.
        return tid

    # ------------------------------------------------------------------
    # Price / event updates
    # ------------------------------------------------------------------

    def update_prices(
        self,
        prices: Dict[str, Dict[str, float]],
        as_of: Optional[str] = None,
    ) -> None:
        """Feed price bars in and let the store update MFE/MAE/T1/T2/STOP.

        `prices` maps symbol -> {"high": float, "low": float, "close": float}.
        Only records whose tracking_status is ACTIVE or T1_HIT are updated.
        """
        as_of = as_of or _today_str()
        as_of_d = _as_date(as_of) or date.today()

        for tid, rec in self.records.items():
            if rec.get("tracking_status") in TERMINAL_STATUSES:
                continue
            sym = rec.get("symbol")
            bar = prices.get(sym)
            if not bar:
                continue

            entry_price = float((rec.get("entry") or {}).get("reference_entry_price") or 0.0)
            if entry_price <= 0:
                continue

            hi = float(bar.get("high") or 0)
            lo = float(bar.get("low") or 0)
            close = float(bar.get("close") or 0)
            if hi <= 0 or lo <= 0:
                continue

            # Invariant 5: MFE monotonically increases, MAE monotonically
            # decreases (more negative).
            bar_fav = (hi - entry_price) / entry_price * 100.0
            bar_adv = (lo - entry_price) / entry_price * 100.0
            prev_mfe = float(rec.get("mfe_pct") or 0.0)
            prev_mae = float(rec.get("mae_pct") or 0.0)
            new_mfe = round(max(prev_mfe, bar_fav), 4)
            new_mae = round(min(prev_mae, bar_adv), 4)
            if new_mfe < prev_mfe:  # defensive; math above guarantees >=
                raise TrackingInvariantError(
                    f"[{tid}] MFE regression: {prev_mfe} -> {new_mfe}"
                )
            if new_mae > prev_mae:
                raise TrackingInvariantError(
                    f"[{tid}] MAE regression: {prev_mae} -> {new_mae}"
                )
            rec["mfe_pct"] = new_mfe
            rec["mae_pct"] = new_mae
            rec["max_price_seen"] = round(max(float(rec.get("max_price_seen") or hi), hi), 4)
            rec["min_price_seen"] = round(min(float(rec.get("min_price_seen") or lo), lo), 4)

            # Days active.
            first = _as_date(rec.get("first_seen_date"))
            if first:
                rec["days_active"] = max((as_of_d - first).days, 0)

            # T1/T2/STOP detection.
            entry = rec.get("entry") or {}
            t1 = float(entry.get("t1_price") or 0)
            t2 = float(entry.get("t2_price") or 0)
            stop = float(entry.get("stop_price") or 0)

            t1_hit_today = t1 > 0 and hi >= t1
            t2_hit_today = t2 > 0 and hi >= t2
            stop_hit_today = stop > 0 and lo <= stop

            # T1 — write-once.
            if t1_hit_today and not rec.get("t1_hit"):
                rec["t1_hit"] = True
                rec["t1_hit_date"] = as_of
                rec["days_to_t1"] = rec.get("days_active")
                if rec.get("tracking_status") == STATUS_ACTIVE:
                    rec["tracking_status"] = STATUS_T1_HIT
                self._log_event(tid, "t1_hit", price=hi, days=rec["days_to_t1"])

            # T2 — write-once. T2 wins over STOP on same-bar ambiguity.
            if t2_hit_today and not rec.get("t2_hit"):
                rec["t2_hit"] = True
                rec["t2_hit_date"] = as_of
                rec["days_to_t2"] = rec.get("days_active")
                rec["tracking_status"] = STATUS_T2_HIT
                self._log_event(tid, "t2_hit", price=hi, days=rec["days_to_t2"])
                # Terminal — stop processing further hit fields.
                rec["updated_at"] = as_of
                continue

            # STOP — write-once. If T1 already hit, this is STOPPED_AFTER_T1.
            if stop_hit_today and not rec.get("stop_hit"):
                rec["stop_hit"] = True
                rec["stop_hit_date"] = as_of
                rec["days_to_stop"] = rec.get("days_active")
                if rec.get("t1_hit"):
                    rec["tracking_status"] = STATUS_STOPPED_AFTER_T1
                else:
                    rec["tracking_status"] = STATUS_STOPPED
                self._log_event(tid, "stop_hit", price=lo, days=rec["days_to_stop"])

            # Refresh current price info.
            cur = rec.get("current") or {}
            cur["current_price"] = round(close or hi, 4)
            cur["as_of_date"] = as_of
            rec["current"] = cur
            rec["updated_at"] = as_of

    # ------------------------------------------------------------------
    # Read views
    # ------------------------------------------------------------------

    def active_records(self) -> List[Dict[str, Any]]:
        return [r for r in self.records.values()
                if r.get("tracking_status") not in TERMINAL_STATUSES]

    def resolved_records(self) -> List[Dict[str, Any]]:
        return [r for r in self.records.values()
                if r.get("tracking_status") in TERMINAL_STATUSES]

    def by_stage(self, stage: str) -> List[Dict[str, Any]]:
        s = str(stage).upper()
        if s == "NEAR_MISS":  # collapse near-miss trajectory tiers
            wanted = {"NEAR_MISS", "NEAR_MISS_RISING", "NEAR_MISS_FADING"}
            return [r for r in self.active_records()
                    if ((r.get("current") or {}).get("stage") or "").upper() in wanted]
        return [r for r in self.active_records()
                if ((r.get("current") or {}).get("stage") or "").upper() == s]

    def by_setup(self, setup_type: str) -> List[Dict[str, Any]]:
        s = str(setup_type).upper()
        return [r for r in self.active_records()
                if ((r.get("entry") or {}).get("setup_type") or "").upper() == s]

    def stats(self) -> Dict[str, int]:
        recs = list(self.records.values())
        return {
            "total": len(recs),
            "active": sum(1 for r in recs if r.get("tracking_status") == STATUS_ACTIVE),
            "t1_hit_active": sum(1 for r in recs if r.get("tracking_status") == STATUS_T1_HIT),
            "t2_hit": sum(1 for r in recs if r.get("tracking_status") == STATUS_T2_HIT),
            "stopped": sum(1 for r in recs if r.get("tracking_status") == STATUS_STOPPED),
            "stopped_after_t1": sum(1 for r in recs if r.get("tracking_status") == STATUS_STOPPED_AFTER_T1),
            "needs_review": sum(1 for r in recs if r.get("migration_status") == MIGRATION_NEEDS_REVIEW),
        }


# ---------------------------------------------------------------------------
# Snapshot builders (pure helpers)
# ---------------------------------------------------------------------------

# The complete factor / research field lists (verified by agent-2 report).
# Fields NOT produced by the pipeline (revenue_growth, profit_growth, eps,
# research_grade, standalone fii/dii) are still allocated slots but left None
# — this is documented in the RESEARCH sheet's legend.
FACTOR_FIELDS = (
    "trend_quality",
    "momentum_quality",
    "sector_strength",
    "sector_composite_score",
    "rs_vs_nifty",
    "mtf_quality",
    "volume_delivery",
    "ownership_quality",
    "news_risk",
    "macro_alignment",
    "options_sentiment",
)

RESEARCH_FIELDS = (
    "roe",
    "roce",
    "de_ratio",
    "promoter_pledge_pct",
    "market_cap_cr",
    "fundamentals_source",
    # Not produced today — documented blanks:
    "revenue_growth",
    "profit_growth",
    "eps",
    "eps_growth",
    "research_grade",
    "fii_change",
    "dii_change",
    "sector_relative_strength",
    "distance_from_52w_high",
)

DELIVERY_FIELDS = (
    "delivery_pct_today",
    "delivery_pct_20d_avg",
    "delivery_ratio",
    "delivery_signal",
    "delivery_source",
)


def _pick(src: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return the first non-None value in `src` among `keys`."""
    for k in keys:
        if k in src and src[k] is not None:
            return src[k]
    return default


def _build_entry_snapshot(
    src: Dict[str, Any],
    *,
    stage: str,
    origin: str,
) -> Dict[str, Any]:
    """Build the FROZEN entry-time snapshot. Field names match agent-2's grep.

    `src` is a `stock` dict as produced by main.py OR a row-dict read from a
    persisted store. We accept both by aliasing common names.
    """
    # Prices — reuse whatever the strategy already computed.
    ref_entry = _pick(src, "reference_entry_price", "entry_price", "entry", "Entry", "current")
    t1 = _pick(src, "t1_price", "target1", "T1", "target_1")
    t2 = _pick(src, "t2_price", "target2", "T2", "target_2")
    stop = _pick(src, "stop_price", "stop", "Stop", "stop_loss")

    # If any level is missing (typical for REJECTED rows) fall back to
    # observation-only defaults based on the reference entry.
    try:
        ref_f = float(ref_entry) if ref_entry is not None else 0.0
    except (TypeError, ValueError):
        ref_f = 0.0
    if ref_f > 0:
        if t1 in (None, "", 0):
            t1 = round(ref_f * (1 + OBSERVATION_T1_PCT), 4)
        if t2 in (None, "", 0):
            t2 = round(ref_f * (1 + OBSERVATION_T2_PCT), 4)
        if stop in (None, "", 0):
            stop = round(ref_f * (1 + OBSERVATION_STOP_PCT), 4)

    entry: Dict[str, Any] = {
        "stage": stage,
        "decision": _pick(src, "decision", "Category", default=stage),
        "setup_type": _pick(src, "setup_type", "Setup"),
        "regime": _pick(src, "regime_at_classification", "regime", "Regime"),
        "sector": _pick(src, "sector", "Sector"),
        "fail_reasons": _pick(src, "fail_reasons", "Fail Reasons", default=[]) or [],
        "warnings": _pick(src, "warnings", default=[]) or [],
        "origin": origin,

        # Core scores — using exact grep-verified field names.
        "confidence": _pick(src, "final_confidence", "confidence", "conf", "Confidence"),
        "base_confidence": _pick(src, "base_confidence"),
        "tq": _pick(src, "trade_quality_score", "tq", "TQ"),
        "rr": _pick(src, "rr_ratio", "rr", "R/R"),
        "opportunity_score": _pick(src, "opportunity_score", "Opp Score"),

        # Price levels.
        "reference_entry_price": ref_entry,
        "t1_price": t1,
        "t2_price": t2,
        "stop_price": stop,

        # Setup / regime context.
        "setup_conf_bonus": _pick(src, "setup_conf_bonus"),
        "chop_momentum_penalty": _pick(src, "chop_momentum_penalty"),
        "phase_i_skip": _pick(src, "phase_i_skip"),

        # Watchlist-only fields (harmless None on BUY records).
        "tier": _pick(src, "tier"),
        "conf_gap": _pick(src, "conf_gap"),
        "trajectory": _pick(src, "trajectory"),

        # Factors — allocate every slot; missing ones remain None.
        "factors": {f: _pick(src, f) for f in FACTOR_FIELDS},

        # Delivery detail.
        "delivery": {f: _pick(src, f) for f in DELIVERY_FIELDS},

        # Research / fundamentals.
        "research": {f: _pick(src, f) for f in RESEARCH_FIELDS},
    }
    return entry


def _build_current_snapshot(
    src: Dict[str, Any],
    *,
    stage: str,
    as_of: str,
) -> Dict[str, Any]:
    return {
        "stage": stage,
        "confidence": _pick(src, "final_confidence", "confidence", "conf", "Confidence"),
        "tq": _pick(src, "trade_quality_score", "tq", "TQ"),
        "rr": _pick(src, "rr_ratio", "rr", "R/R"),
        "opportunity_score": _pick(src, "opportunity_score", "Opp Score"),
        "current_price": _pick(src, "current_price", "current", "close", "Close"),
        "as_of_date": as_of,
    }


# ---------------------------------------------------------------------------
# Score bands (locked based on real distribution — see redesign spec §3.5)
# ---------------------------------------------------------------------------

CONFIDENCE_BANDS: Tuple[Tuple[str, float, float], ...] = (
    ("<60",     -1e9, 60.0),
    ("60-69",   60.0, 70.0),
    ("70-74",   70.0, 75.0),
    ("75-79",   75.0, 80.0),
    ("80-84",   80.0, 85.0),
    ("85+",     85.0, 1e9),
)

# TQ distribution observed: min 43, Q1 59, med 67, Q3 71, max 78.
TQ_BANDS: Tuple[Tuple[str, float, float], ...] = (
    ("<50",     -1e9, 50.0),
    ("50-59",   50.0, 60.0),
    ("60-69",   60.0, 70.0),
    ("70-74",   70.0, 75.0),
    ("75+",     75.0, 1e9),
)

RR_BANDS: Tuple[Tuple[str, float, float], ...] = (
    ("<1.5",    -1e9, 1.5),
    ("1.5-1.99", 1.5, 2.0),
    ("2.0-2.49", 2.0, 2.5),
    ("2.5-2.99", 2.5, 3.0),
    ("3.0+",     3.0, 1e9),
)

# Opportunity distribution observed: min 44, Q1 56, med 64, Q3 67, max 77.
OPPORTUNITY_BANDS: Tuple[Tuple[str, float, float], ...] = (
    ("<50",     -1e9, 50.0),
    ("50-59",   50.0, 60.0),
    ("60-69",   60.0, 70.0),
    ("70+",     70.0, 1e9),
)


def band_of(value: Any, bands: Iterable[Tuple[str, float, float]]) -> str:
    if value is None:
        return "unknown"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "unknown"
    for label, lo, hi in bands:
        if lo <= v < hi:
            return label
    return "unknown"
