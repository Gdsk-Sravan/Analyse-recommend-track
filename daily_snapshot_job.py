"""Daily snapshot log — append-only per (symbol, bucket, run_date).

This is the second layer of the tracking system. The canonical
tracking_store.json holds ONE record per (symbol, first_seen_date) with a
frozen entry snapshot. This file holds one line per (symbol, bucket, day) so
each stage/setup sheet in the workbook is a proper time-series journal.

Buckets recorded per symbol per day:
  Stage buckets  : BUY | WATCHLIST | NEAR_MISS | DEVELOPING | MONITOR | REJECTED
  Setup buckets  : BREAKOUT | MOMENTUM | PULLBACK | REVERSAL   (OTHER not tracked)

A stock classified BUY with setup=BREAKOUT on 2026-07-13 emits TWO lines:
  {run_date:2026-07-13, symbol:X, bucket:BUY,      source:stage, ...}
  {run_date:2026-07-13, symbol:X, bucket:BREAKOUT, source:setup, ...}

Storage: results/daily_snapshots.jsonl  (append-only, never rewritten)
Idempotency: same (run_date, symbol, bucket) is de-duplicated on append.

CLI:
    python daily_snapshot_job.py --append-from-store          # from tracking_store.json
    python daily_snapshot_job.py --append-from-store --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from tracking_store import (  # noqa: E402
    DEFAULT_RESULTS_DIR,
    DEFAULT_STORE_PATH,
    TERMINAL_STATUSES,
    TrackingStore,
)

# Honour the same FRESH_START env var as main.py — if true, this run must
# start with an EMPTY snapshot log (main.py should have already deleted the
# file, but we also honour the flag defensively).
FRESH_START = os.getenv("FRESH_START", "false").lower() == "true"

DAILY_SNAPSHOTS_PATH = DEFAULT_RESULTS_DIR / "daily_snapshots.jsonl"

STAGE_BUCKETS = ("BUY", "NEAR_MISS", "DEVELOPING", "MONITOR", "REJECTED")
# Setup sheets are 5: 4 active setup types + REJECTED_SETUP for stocks whose
# stage was rejected. REJECTED_SETUP is distinct from the stage REJECTED bucket
# so the two REJECTED views don't collide.
SETUP_BUCKETS = ("BREAKOUT", "MOMENTUM", "PULLBACK", "REVERSAL", "REJECTED_SETUP")
ACTIVE_SETUP_BUCKETS = ("BREAKOUT", "MOMENTUM", "PULLBACK", "REVERSAL")


def _load_existing_keys(path: Path) -> set:
    """Return set of (run_date, symbol, bucket) already appended.

    On FRESH_START, treat the log as empty regardless of what's on disk —
    defensive fallback in case main.py's wipe missed the file.
    """
    keys = set()
    if FRESH_START:
        print("[daily_snapshot] FRESH_START active — treating existing snapshots as empty")
        return keys
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                keys.add((d.get("run_date"), d.get("symbol"), d.get("bucket")))
            except json.JSONDecodeError:
                continue
    return keys


def _snap_from_record(rec: Dict[str, Any], run_date: str, bucket: str,
                      source: str) -> Dict[str, Any]:
    """Build one daily-snapshot line."""
    entry = rec.get("entry") or {}
    current = rec.get("current") or {}
    return {
        "run_date": run_date,
        "symbol": rec.get("symbol"),
        "bucket": bucket,                    # e.g. BUY, BREAKOUT
        "source": source,                    # "stage" or "setup"
        "tracking_id": rec.get("tracking_id"),
        "first_seen_date": rec.get("first_seen_date"),
        "days_active": rec.get("days_active"),
        "journey_path": rec.get("journey_path") or [],
        "current_stage": current.get("stage"),
        "entry_stage": entry.get("stage"),
        "setup_type": entry.get("setup_type"),
        "regime": entry.get("regime"),
        "sector": entry.get("sector"),
        # Entry snapshot (frozen — never changes across daily rows).
        "entry_confidence": entry.get("confidence"),
        "entry_tq": entry.get("tq"),
        "entry_rr": entry.get("rr_ratio") or entry.get("rr"),
        "entry_opp": entry.get("opportunity_score"),
        "reference_entry_price": entry.get("reference_entry_price"),
        "t1_price": entry.get("t1_price"),
        "t2_price": entry.get("t2_price"),
        "stop_price": entry.get("stop_price"),
        # Current snapshot (moves every run).
        "current_confidence": current.get("confidence"),
        "current_tq": current.get("tq"),
        "current_rr": current.get("rr_ratio") or current.get("rr"),
        "current_opp": current.get("opportunity_score"),
        "current_price": current.get("close") or current.get("price")
                         or current.get("current_price"),
        # Running metrics from the canonical record.
        "mfe_pct": rec.get("mfe_pct"),
        "mae_pct": rec.get("mae_pct"),
        "t1_hit": rec.get("t1_hit"),
        "t2_hit": rec.get("t2_hit"),
        "stop_hit": rec.get("stop_hit"),
        # 2026-07-15: propagate durations + hit dates so all daily sheets
        # (BUY / WATCHLIST / REJECTED / REJECTED_SETUPS) can show how long
        # each stock took to reach T1 / T2 / Stop. These fields are already
        # tracked in tracking_store.py (write-once when the outcome fires).
        # None until the outcome fires — rendered blank in the workbook.
        "t1_hit_date":   rec.get("t1_hit_date"),
        "t2_hit_date":   rec.get("t2_hit_date"),
        "stop_hit_date": rec.get("stop_hit_date"),
        "days_to_t1":    rec.get("days_to_t1"),
        "days_to_t2":    rec.get("days_to_t2"),
        "days_to_stop":  rec.get("days_to_stop"),
        "tracking_status": rec.get("tracking_status"),
        "fail_reasons": entry.get("fail_reasons") or [],
    }


def _bucket_stage(current_stage: Optional[str], entry_stage: Optional[str]) -> Optional[str]:
    """Map a record's current stage to a stage bucket (or None if not applicable).

    Bucket set (WATCHLIST intentionally excluded — its sub-tiers cover it):
        BUY, NEAR_MISS, DEVELOPING, MONITOR, REJECTED
    Bare WATCHLIST (no sub-tier) folds into MONITOR — "keep an eye on it".
    """
    for s in (current_stage, entry_stage):
        if not s:
            continue
        s = str(s).upper()
        if s in ("NEAR_MISS_RISING", "NEAR_MISS_FADING"):
            return "NEAR_MISS"
        if s == "WATCHLIST":
            return "MONITOR"
        if s in STAGE_BUCKETS:
            return s
    return None


def _bucket_setup(setup_type: Optional[str], stage_bucket: Optional[str]) -> Optional[str]:
    """Route a (setup_type, stage_bucket) pair to a setup sheet.

    Rules:
      * No setup_type      -> no setup row.
      * Stage is REJECTED  -> REJECTED_SETUP (keeps active setup sheets clean).
      * Otherwise          -> the setup sheet named after setup_type
                              (BREAKOUT / MOMENTUM / PULLBACK / REVERSAL).
      * Unknown setup_type -> no setup row.
    """
    if not setup_type:
        return None
    s = str(setup_type).upper()
    if s not in ACTIVE_SETUP_BUCKETS:
        return None
    if stage_bucket == "REJECTED":
        return "REJECTED_SETUP"
    return s


def append_from_store(
    store: TrackingStore,
    run_date: Optional[str] = None,
    snapshots_path: Path = DAILY_SNAPSHOTS_PATH,
    dry_run: bool = False,
    include_resolved: bool = False,
) -> List[Dict[str, Any]]:
    """Append one row per (record, bucket) for today.

    * A record contributes AT MOST one stage-bucket row and AT MOST one
      setup-bucket row per day.
    * Terminal records (T2/STOPPED/STOPPED_AFTER_T1) are skipped by default
      — they live in the RESOLVED sheet.
    * Same-day rerun is idempotent (existing (run_date, symbol, bucket) skipped).
    """
    run_date = run_date or date.today().isoformat()
    existing = _load_existing_keys(snapshots_path)
    to_write: List[Dict[str, Any]] = []

    for rec in store.records.values():
        if not include_resolved and rec.get("tracking_status") in TERMINAL_STATUSES:
            continue
        current = rec.get("current") or {}
        entry = rec.get("entry") or {}
        stage_bucket = _bucket_stage(current.get("stage"), entry.get("stage"))
        setup_bucket = _bucket_setup(entry.get("setup_type"), stage_bucket)

        # Stage row
        if stage_bucket:
            key = (run_date, rec.get("symbol"), stage_bucket)
            if key not in existing:
                to_write.append(_snap_from_record(rec, run_date, stage_bucket, "stage"))
                existing.add(key)

        # Setup row — active setup sheets get promising candidates only.
        # Rejected-stage stocks with a setup go to REJECTED_SETUP.
        if setup_bucket:
            key = (run_date, rec.get("symbol"), setup_bucket)
            if key not in existing:
                to_write.append(_snap_from_record(rec, run_date, setup_bucket, "setup"))
                existing.add(key)

    if not to_write:
        print(f"[daily_snapshot] {run_date}: no new rows to append")
        return []

    if dry_run:
        print(f"[daily_snapshot] DRY RUN — would append {len(to_write)} rows for {run_date}")
    else:
        snapshots_path.parent.mkdir(parents=True, exist_ok=True)
        with snapshots_path.open("a", encoding="utf-8") as f:
            for row in to_write:
                f.write(json.dumps(row, default=str) + "\n")
        print(f"[daily_snapshot] appended {len(to_write)} rows to {snapshots_path}")

    # Per-bucket summary
    from collections import Counter
    counts = Counter(r["bucket"] for r in to_write)
    for b in list(STAGE_BUCKETS) + list(SETUP_BUCKETS):
        if counts.get(b):
            print(f"    {b:12s} : {counts[b]}")
    return to_write


def read_snapshots(path: Path = DAILY_SNAPSHOTS_PATH) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Append daily bucket snapshots")
    p.add_argument("--store", default=str(DEFAULT_STORE_PATH))
    p.add_argument("--out", default=str(DAILY_SNAPSHOTS_PATH))
    p.add_argument("--run-date", default=None, help="YYYY-MM-DD (defaults to today)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--include-resolved", action="store_true",
                   help="Also emit rows for T2/STOPPED records (default: skip)")
    args = p.parse_args(argv)

    store = TrackingStore.load(store_path=Path(args.store))
    print(f"[daily_snapshot] store stats: {store.stats()}")
    append_from_store(
        store=store,
        run_date=args.run_date,
        snapshots_path=Path(args.out),
        dry_run=args.dry_run,
        include_resolved=args.include_resolved,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
