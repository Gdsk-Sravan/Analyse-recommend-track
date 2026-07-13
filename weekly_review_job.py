"""Weekly review builder — append-only historical summaries.

Computes per-category weekly metrics from the canonical tracking store and
appends ONE JSON line per (iso_week, category) to
`results/weekly_reviewstory.jsonl`.

Historical weeks are NEVER overwritten (this is the invariant the current
`shadow_weekly_job.py` violates by rebuilding a fresh Workbook each run).

Categories (see redesign spec §3.5):
    - BUY, WATCHLIST, DEVELOPING, NEAR_MISS       (candidate-stage views)
    - BREAKOUT, MOMENTUM                          (setup-type views)

Metrics per category:
    new_this_week, total_active, resolved, t1_count, t2_count, stop_count,
    t1_rate, t2_rate, stop_rate, avg_mfe, avg_mae,
    median_days_to_t1/t2/stop,
    avg_entry_confidence / tq / rr / opp

Plus score-band cross-tabs (confidence / tq / rr / opportunity).

Usage:
    python weekly_review_job.py --write
    python weekly_review_job.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from tracking_store import (  # noqa: E402
    CONFIDENCE_BANDS,
    DEFAULT_STORE_PATH,
    DEFAULT_WEEKLY_HISTORY_PATH,
    OPPORTUNITY_BANDS,
    RR_BANDS,
    STATUS_STOPPED,
    STATUS_STOPPED_AFTER_T1,
    STATUS_T1_HIT,
    STATUS_T2_HIT,
    TERMINAL_STATUSES,
    TQ_BANDS,
    TrackingStore,
    band_of,
)

CATEGORIES: Tuple[str, ...] = (
    "BUY",
    "WATCHLIST",
    "DEVELOPING",
    "NEAR_MISS",
    "BREAKOUT",
    "MOMENTUM",
)


def _iso_week(d: date) -> Tuple[str, str]:
    """Return (iso_week_key, monday_iso). iso_week_key like '2026-W28'."""
    iso = d.isocalendar()
    monday = date.fromisocalendar(iso.year, iso.week, 1)
    return f"{iso.year}-W{iso.week:02d}", monday.isoformat()


def _in_week(d_iso: Optional[str], monday: date) -> bool:
    if not d_iso:
        return False
    try:
        d = datetime.fromisoformat(str(d_iso)[:10]).date()
    except ValueError:
        return False
    return monday <= d <= monday + timedelta(days=6)


def _category_filter(rec: Dict[str, Any], category: str) -> bool:
    cur_stage = str((rec.get("current") or {}).get("stage") or "").upper()
    setup = str((rec.get("entry") or {}).get("setup_type") or "").upper()
    if category == "BUY":
        return cur_stage == "BUY" or "BUY" in (rec.get("journey_path") or [])
    if category == "WATCHLIST":
        return "WATCHLIST" in (rec.get("journey_path") or [])
    if category == "DEVELOPING":
        return "DEVELOPING" in (rec.get("journey_path") or [])
    if category == "NEAR_MISS":
        return any(s.startswith("NEAR_MISS") for s in (rec.get("journey_path") or []))
    if category == "BREAKOUT":
        return setup == "BREAKOUT"
    if category == "MOMENTUM":
        return setup == "MOMENTUM"
    return False


def _median(xs: Iterable[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return round(statistics.median(xs), 2)


def _mean(xs: Iterable[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return round(statistics.mean(xs), 2)


def _band_table_str(band_counts: Dict[str, Dict[str, Any]]) -> str:
    """Return a compact human-readable cross-tab: 'band=cnt(t1%/t2%/stop%)'."""
    parts = []
    for band, d in band_counts.items():
        n = d["n"]
        t1 = d["t1"] / n * 100 if n else 0
        t2 = d["t2"] / n * 100 if n else 0
        st = d["stop"] / n * 100 if n else 0
        parts.append(f"{band}=n{n}(t1={t1:.0f}%,t2={t2:.0f}%,st={st:.0f}%)")
    return " | ".join(parts)


def _score_band_table(
    recs: List[Dict[str, Any]],
    band_getter,
    bands: Iterable[Tuple[str, float, float]],
) -> Dict[str, Dict[str, Any]]:
    table: Dict[str, Dict[str, Any]] = {
        band[0]: {"n": 0, "t1": 0, "t2": 0, "stop": 0,
                  "mfe": [], "mae": []}
        for band in bands
    }
    table["unknown"] = {"n": 0, "t1": 0, "t2": 0, "stop": 0, "mfe": [], "mae": []}
    for r in recs:
        value = band_getter(r)
        band = band_of(value, bands)
        row = table.setdefault(band, {"n": 0, "t1": 0, "t2": 0, "stop": 0,
                                      "mfe": [], "mae": []})
        row["n"] += 1
        if r.get("t1_hit"):
            row["t1"] += 1
        if r.get("t2_hit"):
            row["t2"] += 1
        if r.get("stop_hit"):
            row["stop"] += 1
        if r.get("mfe_pct") is not None:
            row["mfe"].append(r["mfe_pct"])
        if r.get("mae_pct") is not None:
            row["mae"].append(r["mae_pct"])
    # Cleanup zero-n bands from output for readability.
    return {k: v for k, v in table.items() if v["n"] > 0}


def compute_week_summary(
    store: TrackingStore,
    monday: date,
    category: str,
) -> Dict[str, Any]:
    all_recs = list(store.records.values())
    cat_recs = [r for r in all_recs if _category_filter(r, category)]

    new_this_week = sum(1 for r in cat_recs if _in_week(r.get("first_seen_date"), monday))
    resolved_this_week = sum(
        1 for r in cat_recs
        if _in_week(r.get("t2_hit_date"), monday)
        or _in_week(r.get("stop_hit_date"), monday)
    )
    total_active = sum(
        1 for r in cat_recs if r.get("tracking_status") not in TERMINAL_STATUSES
    )

    t1_count = sum(1 for r in cat_recs if r.get("t1_hit"))
    t2_count = sum(1 for r in cat_recs if r.get("t2_hit"))
    stop_count = sum(1 for r in cat_recs if r.get("stop_hit"))
    n = len(cat_recs)

    def pct(a: int) -> Optional[float]:
        return round(a / n * 100, 2) if n else None

    conf_band = _score_band_table(
        cat_recs, lambda r: (r.get("entry") or {}).get("confidence"), CONFIDENCE_BANDS
    )
    tq_band = _score_band_table(
        cat_recs, lambda r: (r.get("entry") or {}).get("tq"), TQ_BANDS
    )
    rr_band = _score_band_table(
        cat_recs, lambda r: (r.get("entry") or {}).get("rr"), RR_BANDS
    )
    opp_band = _score_band_table(
        cat_recs, lambda r: (r.get("entry") or {}).get("opportunity_score"), OPPORTUNITY_BANDS
    )

    return {
        "iso_week": _iso_week(monday)[0],
        "week_start": monday.isoformat(),
        "category": category,
        "new_this_week": new_this_week,
        "total_active": total_active,
        "resolved": resolved_this_week,
        "t1_count": t1_count,
        "t2_count": t2_count,
        "stop_count": stop_count,
        "t1_rate": pct(t1_count),
        "t2_rate": pct(t2_count),
        "stop_rate": pct(stop_count),
        "avg_mfe": _mean(r.get("mfe_pct") for r in cat_recs),
        "avg_mae": _mean(r.get("mae_pct") for r in cat_recs),
        "median_days_to_t1": _median(r.get("days_to_t1") for r in cat_recs),
        "median_days_to_t2": _median(r.get("days_to_t2") for r in cat_recs),
        "median_days_to_stop": _median(r.get("days_to_stop") for r in cat_recs),
        "avg_entry_confidence": _mean((r.get("entry") or {}).get("confidence") for r in cat_recs),
        "avg_entry_tq": _mean((r.get("entry") or {}).get("tq") for r in cat_recs),
        "avg_entry_rr": _mean((r.get("entry") or {}).get("rr") for r in cat_recs),
        "avg_entry_opp": _mean((r.get("entry") or {}).get("opportunity_score") for r in cat_recs),
        # Bands compressed to string for workbook display; full JSON always kept.
        "conf_band_table": conf_band,
        "tq_band_table": tq_band,
        "rr_band_table": rr_band,
        "opp_band_table": opp_band,
        "conf_band_table_str": _band_table_str(conf_band),
        "tq_band_table_str": _band_table_str(tq_band),
        "rr_band_table_str": _band_table_str(rr_band),
        "opp_band_table_str": _band_table_str(opp_band),
    }


def append_weekly_history(
    store: TrackingStore,
    history_path: Path = DEFAULT_WEEKLY_HISTORY_PATH,
    today: Optional[date] = None,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    today = today or date.today()
    _, monday_iso = _iso_week(today)
    monday = datetime.fromisoformat(monday_iso).date()

    # Idempotency: skip categories already written for this week.
    existing_keys: set = set()
    if history_path.exists():
        with history_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    existing_keys.add((d.get("iso_week"), d.get("category")))
                except json.JSONDecodeError:
                    continue

    written: List[Dict[str, Any]] = []
    history_path.parent.mkdir(parents=True, exist_ok=True)
    for cat in CATEGORIES:
        summary = compute_week_summary(store, monday, cat)
        key = (summary["iso_week"], cat)
        if key in existing_keys:
            print(f"[weekly] skip {key} (already appended)")
            continue
        written.append(summary)
        if not dry_run:
            with history_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(summary, default=str) + "\n")
    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Append weekly review history")
    p.add_argument("--store", default=str(DEFAULT_STORE_PATH))
    p.add_argument("--history", default=str(DEFAULT_WEEKLY_HISTORY_PATH))
    p.add_argument("--as-of", default=None, help="YYYY-MM-DD (defaults to today)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    today = date.today()
    if args.as_of:
        today = datetime.fromisoformat(args.as_of).date()

    store = TrackingStore.load(store_path=Path(args.store))
    print(f"[weekly] store stats: {store.stats()}")
    rows = append_weekly_history(store, Path(args.history), today=today, dry_run=args.dry_run)
    print(f"[weekly] wrote {len(rows)} category rows for week {_iso_week(today)[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
