#!/usr/bin/env python3
"""
reject_followup.py — Phase N-2 (2026-07-03)
─────────────────────────────────────────────────────────────────────────────
Reject-outcome tracker. Reads reject_watch.json (populated by main.py's audit
loop for every REJECTED stock), and for entries aged ≥5 / ≥10 / ≥20 trading
days fetches the subsequent close via yfinance and computes realized return.

Writes reject_outcome_log.csv — one row per (symbol, reject_date) that has
at least one forward return measured. Rows are updated in-place as later
horizons become available (5d filled first, 10d + 20d fill in subsequent
runs).

WHY this matters
────────────────
Every gate rejection is a hypothesis: "this stock will underperform if we
buy it." reject_followup.py measures whether the hypothesis holds. If the
data shows a rejected pattern actually rallied +8% on average over 10 days,
that gate is producing false negatives on this pattern and should be
loosened.

WHAT this script does NOT do
────────────────────────────
- Does NOT change any gate logic (analysis-only).
- Does NOT hit the network for stocks whose N-day horizon hasn't elapsed.
- Does NOT delete reject_watch.json entries newer than 30 days (they may
  still need their T+20 measurement).

Run frequency: once per trading day, after tracker_job.py, on the runner.

Usage:
    python scripts/reject_followup.py [--dry-run]
    python scripts/reject_followup.py --watch reject_watch.json --out reject_outcome_log.csv

Exit codes:
    0 = success (even if no rows updated — always non-fatal)
    2 = catastrophic error (missing yfinance, watch file corrupt, etc.)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

try:
    import yfinance as yf
except ImportError:
    print("[ERR] yfinance not installed. Install: pip install yfinance", file=sys.stderr)
    sys.exit(2)


DEFAULT_WATCH_FILE   = os.getenv("REJECT_WATCH_FILE",   "reject_watch.json")
DEFAULT_OUT_CSV      = os.getenv("REJECT_OUTCOME_CSV",  "reject_outcome_log.csv")
DEFAULT_HORIZONS     = (5, 10, 20)      # calendar-day horizons
PRUNE_OLDER_THAN_DAYS = int(os.getenv("REJECT_WATCH_PRUNE_DAYS", "35"))  # after all horizons closed
FETCH_PAUSE_SEC      = float(os.getenv("REJECT_FOLLOWUP_PAUSE_SEC", "0.15"))
MAX_STOCKS_PER_RUN   = int(os.getenv("REJECT_FOLLOWUP_MAX_PER_RUN", "300"))


# ── I/O helpers ──────────────────────────────────────────────────────────────
def _log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def _load_watch(path: str) -> list:
    if not os.path.exists(path):
        _log(f"[INFO] reject watch file not found: {path} — nothing to do")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            _log(f"[ERR] reject watch file is not a list: {path}")
            return []
        return data
    except (json.JSONDecodeError, OSError) as e:
        _log(f"[ERR] failed to load {path}: {e}")
        return []


def _save_watch(path: str, rows: list) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)
    except OSError as e:
        _log(f"[ERR] failed to save {path}: {e}")


def _load_existing_outcomes(path: str) -> dict:
    """
    Read existing CSV → dict keyed by (symbol, reject_date) → row_dict.
    Used to skip fetches for already-fully-measured entries.
    """
    out: dict = {}
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("symbol", ""), row.get("reject_date", ""))
                out[key] = row
    except OSError as e:
        _log(f"[WARN] failed to load existing CSV {path}: {e}")
    return out


def _write_outcomes(path: str, rows: list) -> None:
    if not rows:
        return
    # Stable column order — new fields append at the end if we extend later
    fieldnames = [
        "reject_date", "symbol", "sector", "close_at_reject",
        "market_cap_cr", "trade_quality", "confidence", "top_reasons",
        "close_5d", "return_5d_pct", "close_10d", "return_10d_pct",
        "close_20d", "return_20d_pct",
        "measured_at", "notes",
    ]
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    except OSError as e:
        _log(f"[ERR] failed to write outcomes CSV {path}: {e}")


# ── Date math ────────────────────────────────────────────────────────────────
def _parse_date(s: str) -> date | None:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _days_since(d: date) -> int:
    return (date.today() - d).days


# ── Price fetch ──────────────────────────────────────────────────────────────
def _fetch_close_on_or_after(symbol: str, target_date: date) -> tuple[float | None, date | None]:
    """
    Return (close_price, actual_date) — the first trading-day close on or
    after target_date. If target_date is in the future or the stock hasn't
    traded since, returns (None, None).

    We fetch a 10-day window starting at target_date (buffer for weekends /
    holidays). If empty, fall back to a wider window before giving up.
    """
    try:
        end_dt = target_date + timedelta(days=10)
        # yfinance is inclusive on start, exclusive on end
        df = yf.download(
            symbol,
            start=target_date.isoformat(),
            end=(end_dt + timedelta(days=1)).isoformat(),
            progress=False,
            auto_adjust=False,
            threads=False,
        )
        if df is None or df.empty:
            # Wider retry — maybe symbol delisted or gap week
            df = yf.download(
                symbol,
                start=target_date.isoformat(),
                end=(target_date + timedelta(days=20) + timedelta(days=1)).isoformat(),
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty:
                return None, None
        # Get the first row on or after target_date
        # (yfinance sometimes gives a MultiIndex; guard for both shapes)
        try:
            close_col = df["Close"]
            # If MultiIndex (multiple tickers request), take the symbol column
            if hasattr(close_col, "columns"):
                close_col = close_col.iloc[:, 0]
            first_val = float(close_col.iloc[0])
            first_date = close_col.index[0].date()
            return first_val, first_date
        except (KeyError, IndexError, ValueError):
            return None, None
    except Exception as e:  # noqa: BLE001 — yfinance can raise many things
        _log(f"[WARN] fetch failed for {symbol} @ {target_date}: {e}")
        return None, None


# ── Core scoring loop ────────────────────────────────────────────────────────
def _row_key(entry: dict) -> tuple:
    return (entry.get("symbol", ""), entry.get("date", ""))


def _existing_has_all_horizons(row: dict, horizons: tuple = DEFAULT_HORIZONS) -> bool:
    for h in horizons:
        v = row.get(f"return_{h}d_pct", "")
        if v in ("", None):
            return False
    return True


def _measure_one(entry: dict, existing: dict, horizons: tuple = DEFAULT_HORIZONS,
                 dry_run: bool = False) -> dict | None:
    """
    Compute forward returns for one reject_watch entry. Returns an outcome row
    (dict) if at least one horizon was newly measured; None if entry is too
    fresh (< 5 days) or unfetchable.
    """
    symbol = entry.get("symbol", "")
    reject_date = _parse_date(entry.get("date", ""))
    if not symbol or reject_date is None:
        return None

    days_old = _days_since(reject_date)
    if days_old < min(horizons):
        return None  # too fresh

    key = (symbol, entry.get("date", ""))
    row = dict(existing.get(key, {})) if key in existing else {}

    # Seed base fields (idempotent — same on every run)
    close_at_reject = entry.get("close", 0) or 0
    row.update({
        "reject_date":     entry.get("date", ""),
        "symbol":          symbol,
        "sector":          entry.get("sector", ""),
        "close_at_reject": round(float(close_at_reject), 2),
        "market_cap_cr":   entry.get("market_cap_cr", 0),
        "trade_quality":   entry.get("trade_quality", 0),
        "confidence":      entry.get("confidence", 0),
        "top_reasons":     " | ".join(entry.get("reasons", [])[:3]),
    })

    changed = False
    for h in horizons:
        col_close  = f"close_{h}d"
        col_return = f"return_{h}d_pct"
        # Skip if already measured
        if row.get(col_return) not in ("", None):
            continue
        # Skip if horizon hasn't elapsed
        if days_old < h:
            continue
        target = reject_date + timedelta(days=h)
        if dry_run:
            row[col_close]  = ""
            row[col_return] = "(dry-run)"
            changed = True
            continue
        close_h, actual_date = _fetch_close_on_or_after(symbol, target)
        if close_h is None or close_at_reject <= 0:
            continue
        ret_pct = round((close_h - close_at_reject) / close_at_reject * 100.0, 2)
        row[col_close]  = round(close_h, 2)
        row[col_return] = ret_pct
        changed = True
        # Gentle pace to avoid yfinance rate limiting
        time.sleep(FETCH_PAUSE_SEC)

    if not changed and key not in existing:
        # Nothing new + no pre-existing row → don't emit
        return None
    if changed:
        row["measured_at"] = datetime.now().isoformat(timespec="seconds")
    return row


# ── Main entry point ─────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="Follow up on REJECTED stocks — measure T+5/10/20 returns")
    ap.add_argument("--watch", default=DEFAULT_WATCH_FILE, help="reject_watch.json path")
    ap.add_argument("--out",   default=DEFAULT_OUT_CSV,    help="output CSV path")
    ap.add_argument("--dry-run", action="store_true", help="skip network fetches; write placeholder rows")
    ap.add_argument("--horizons", default="5,10,20", help="comma-separated horizons in days")
    ap.add_argument("--max-per-run", type=int, default=MAX_STOCKS_PER_RUN,
                    help="cap fetches per run to avoid long CI cycles")
    args = ap.parse_args()

    try:
        horizons = tuple(sorted(int(x) for x in args.horizons.split(",") if x.strip()))
    except ValueError:
        _log(f"[ERR] bad --horizons: {args.horizons}")
        return 2

    watch = _load_watch(args.watch)
    if not watch:
        _log(f"[INFO] no reject watch entries in {args.watch}")
        # Still emit an empty CSV so downstream tooling can distinguish
        # "ran but nothing to score" from "never ran".
        if not os.path.exists(args.out):
            _write_outcomes(args.out, [])
        return 0

    existing = _load_existing_outcomes(args.out)
    _log(f"[INFO] loaded {len(watch)} reject entries + {len(existing)} existing outcome rows")

    fetched_count = 0
    updated_rows = dict(existing)  # start from existing, then merge

    for entry in watch:
        key = _row_key(entry)
        # Skip if all horizons already measured
        if key in existing and _existing_has_all_horizons(existing[key], horizons):
            continue
        # Cap fetches per run
        if fetched_count >= args.max_per_run:
            _log(f"[INFO] hit max_per_run={args.max_per_run} — stopping fetches for this cycle")
            break
        row = _measure_one(entry, existing, horizons, dry_run=args.dry_run)
        if row is None:
            continue
        updated_rows[key] = row
        fetched_count += 1

    _log(f"[INFO] measured {fetched_count} entries this run "
         f"({len(updated_rows)} total outcome rows)")

    # ── Prune reject_watch.json ─────────────────────────────────────────────
    # An entry is safe to drop once it's older than the longest horizon +
    # PRUNE_OLDER_THAN_DAYS buffer AND has been recorded in the outcome CSV.
    max_h = max(horizons)
    cutoff = date.today() - timedelta(days=max_h + PRUNE_OLDER_THAN_DAYS)
    keep = []
    dropped = 0
    for entry in watch:
        d = _parse_date(entry.get("date", ""))
        if d is None:
            keep.append(entry)
            continue
        key = _row_key(entry)
        if d < cutoff and key in updated_rows and _existing_has_all_horizons(updated_rows[key], horizons):
            dropped += 1
            continue
        keep.append(entry)
    if dropped:
        _save_watch(args.watch, keep)
        _log(f"[INFO] pruned {dropped} fully-measured entries older than "
             f"{max_h + PRUNE_OLDER_THAN_DAYS}d from {args.watch}")

    # ── Write outcome CSV ────────────────────────────────────────────────────
    # Sort by reject_date DESC then symbol for stable diffs
    all_rows = sorted(
        updated_rows.values(),
        key=lambda r: (r.get("reject_date", ""), r.get("symbol", "")),
        reverse=True,
    )
    _write_outcomes(args.out, all_rows)
    _log(f"[OK] wrote {len(all_rows)} rows → {args.out}")

    # ── Summary block for CI log ────────────────────────────────────────────
    _print_summary(all_rows, horizons)
    return 0


def _print_summary(rows: list, horizons: tuple) -> None:
    """
    Print a compact aggregate: mean forward return by horizon,
    plus split by top-fail-reason to spot false-negative patterns.
    """
    if not rows:
        return
    print("─" * 60)
    print("REJECT-OUTCOME SUMMARY")
    print("─" * 60)
    for h in horizons:
        col = f"return_{h}d_pct"
        vals = []
        for r in rows:
            v = r.get(col, "")
            if v in ("", None):
                continue
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
        if not vals:
            print(f"  T+{h:>2}d: n=0  (no measurements yet)")
            continue
        vals.sort()
        mean = sum(vals) / len(vals)
        med  = vals[len(vals) // 2]
        wins = sum(1 for v in vals if v >  2.0)   # >+2% = clearly missed opportunity
        blowups = sum(1 for v in vals if v < -5.0)  # <-5% = reject was correct
        print(f"  T+{h:>2}d: n={len(vals):>3}  mean={mean:+6.2f}%  "
              f"median={med:+6.2f}%  wins(>+2%)={wins}  blowups(<-5%)={blowups}")
    print("─" * 60)
    # ── Split by top gate reason (only the leading reason per row) ──────────
    from collections import defaultdict
    by_reason: dict = defaultdict(list)
    for r in rows:
        reasons = r.get("top_reasons", "")
        first = reasons.split("|")[0].strip() if reasons else "UNKNOWN"
        v = r.get("return_10d_pct", "")   # 10d = middle horizon
        if v in ("", None):
            continue
        try:
            by_reason[first].append(float(v))
        except (TypeError, ValueError):
            continue
    if by_reason:
        print("BY LEADING REJECT REASON (T+10d):")
        for reason, vals in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
            if len(vals) < 3:
                continue  # need at least 3 samples to say anything
            mean = sum(vals) / len(vals)
            marker = "⚠️  possible false-neg" if mean > 2.0 else "✓ reject held" if mean < -2.0 else "  neutral"
            print(f"  {reason[:60]:<60}  n={len(vals):>3}  mean={mean:+6.2f}%   {marker}")
    print("─" * 60)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        _log("[INTERRUPT] aborted by user")
        sys.exit(130)
