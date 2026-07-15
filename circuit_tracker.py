"""circuit_tracker.py — Track stocks the pipeline SKIPS due to circuit-limit moves.

The Question
------------
The main pipeline drops any stock with a same-day |return| > 15% via:
    [SKIP] SYMBOL — circuit move +X.X% today   (score_stock hard filter)

Your intuition: those big movers might KEEP moving next day. If so, we're
throwing away edge. This module tests that hypothesis by tracking every
skipped circuit stock for 30 calendar days forward and recording daily
cumulative returns.

What It Does (per daily run)
----------------------------
1.  Parses today's `run_log_YYYYMMDD.txt` for `[SKIP] ... circuit move X.X%`
    lines. Splits POSITIVE (+15% or more) and NEGATIVE (-15% or worse).
2.  Ranks each direction by |move| size, keeps top 10.
3.  For each of the top-10-per-direction, generates a track_id
    `SYMBOL#YYYYMMDD` and adds it to `circuit_tracker.json` if not already
    present. Same stock hitting circuit again on a different day starts a
    NEW parallel track (different date suffix).
4.  For EVERY active track in the JSON, fetches today's close via yfinance,
    computes cumulative return since the circuit date, and stamps the
    matching day_offset column (day+1, day+2, ... day+30).
5.  Auto-retires tracks when:
        days_tracked >= 30              → RETIRED_30DAY
        no price for 3 consecutive days → RETIRED_NODATA
        cumulative >= +50%              → RETIRED_BIG_WIN
        cumulative <= -50%              → RETIRED_BIG_LOSS

Design notes
------------
* Persistent state:  `results/circuit_tracker.json` (schema versioned).
* Idempotent: same-day rerun is a no-op (each track_id append is guarded).
* Non-fatal: any yfinance failure logs + continues, never breaks the pipeline.
* Only runs when SCHEDULED_RUN=true (manual runs don't touch state).
* Kept COMPLETELY isolated from the main scoring pipeline — no cross-imports
  into main.py logic. Only imports `fetch_price_data` lazily.

Public API
----------
    run_circuit_tracker(log_path, as_of_date, is_scheduled)
        → returns dict of counts for the pipeline log line

Usage from main.py (one line near the end of run_pipeline):
    import circuit_tracker
    circuit_tracker.run_circuit_tracker(
        log_path=f"run_log_{ist_today().strftime('%Y%m%d')}.txt",
        as_of_date=ist_today().isoformat(),
        is_scheduled=IS_SCHEDULED,
    )
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Persistent state file — sits next to tracking_store.json under results/.
RESULTS_DIR             = _HERE / "results"
CIRCUIT_STORE_PATH      = RESULTS_DIR / "circuit_tracker.json"
BACKUP_DIR              = RESULTS_DIR / "backups"

# Log file naming pattern (matches main.py init_run_log).
LOG_FILENAME_TEMPLATE   = "run_log_{yyyymmdd}.txt"

# Filter parameters (matches score_stock circuit-breaker at |ret1d| > 15%).
CIRCUIT_MOVE_THRESHOLD  = 15.0    # |%| — anything >15% was skipped by the pipeline
TOP_N_PER_DIRECTION     = 10      # Track top-10 POS and top-10 NEG per day

# Tracking window (calendar days from circuit_date, inclusive of day+1).
TRACK_DAYS              = 30

# Early-exit thresholds (cumulative return absolute values).
BIG_WIN_THRESHOLD_PCT   = 50.0    # cumulative >= +50% → retire (winner)
BIG_LOSS_THRESHOLD_PCT  = -50.0   # cumulative <= -50% → retire (loser)

# Consecutive missing-data days before RETIRED_NODATA.
NODATA_RETIRE_DAYS      = 3

# Regex to parse the log line. Matches:
#   [22:26:32] [SKIP] BFUTILITIE.NS — circuit move 16.2% today
#   [19:07:15] [SKIP] HARDWYN.NS — circuit move -19.3% today
_CIRCUIT_LINE_RE = re.compile(
    r"\[SKIP\]\s+(?P<symbol>[A-Z0-9\-\.]+)\s+[—-]+\s+circuit move\s+"
    r"(?P<move>-?\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)


# Status enum values.
STATUS_ACTIVE           = "ACTIVE"
STATUS_RETIRED_30DAY    = "RETIRED_30DAY"
STATUS_RETIRED_NODATA   = "RETIRED_NODATA"
STATUS_RETIRED_BIG_WIN  = "RETIRED_BIG_WIN"
STATUS_RETIRED_BIG_LOSS = "RETIRED_BIG_LOSS"

TERMINAL_STATUSES = {
    STATUS_RETIRED_30DAY,
    STATUS_RETIRED_NODATA,
    STATUS_RETIRED_BIG_WIN,
    STATUS_RETIRED_BIG_LOSS,
}

DIRECTION_POS           = "POS"
DIRECTION_NEG           = "NEG"


# ---------------------------------------------------------------------------
# LOGGING (soft — falls back to print if main._log unavailable)
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    """Prefer main._log if importable, else print. Non-fatal."""
    try:
        from main import _log as _main_log  # type: ignore
        _main_log(msg)
    except Exception:
        print(msg)


# ---------------------------------------------------------------------------
# PERSISTENCE
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1


def _empty_store() -> Dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "last_updated":   None,
        "tracks":         {},   # keyed by track_id
    }


def load_store(path: Path = CIRCUIT_STORE_PATH) -> Dict[str, Any]:
    """Load the persistent circuit tracker store. Returns empty on missing/corrupt."""
    if not path.exists():
        return _empty_store()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "tracks" not in data:
            _log(f"[circuit_tracker] WARN — {path.name} malformed, starting fresh")
            return _empty_store()
        # Backward-compat: earlier schemas can migrate here.
        data.setdefault("schema_version", _SCHEMA_VERSION)
        data.setdefault("last_updated", None)
        data.setdefault("tracks", {})
        return data
    except Exception as e:
        _log(f"[circuit_tracker] WARN — load failed ({e}), starting fresh")
        return _empty_store()


def save_store(store: Dict[str, Any], path: Path = CIRCUIT_STORE_PATH) -> None:
    """Atomic write with same-day backup copy (like tracking_store.py)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # One backup per day (before first write of the day).
    today_stamp = date.today().isoformat()
    backup_path = BACKUP_DIR / f"circuit_tracker.{today_stamp}.bak.json"
    if path.exists() and not backup_path.exists():
        try:
            shutil.copy2(path, backup_path)
        except Exception:
            pass  # never let backup failure block the save

    store["last_updated"] = datetime.now().isoformat(timespec="seconds")

    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# LOG PARSING
# ---------------------------------------------------------------------------

def parse_circuits_from_log(log_path: Path) -> List[Dict[str, Any]]:
    """Extract circuit-skip events from a run log.

    Returns list of dicts:
        {"symbol": "JUSTDIAL.NS", "move_pct": 20.0, "direction": "POS"}
    De-duplicates within the log (same symbol appearing twice keeps the
    largest |move|).
    """
    if not log_path.exists():
        _log(f"[circuit_tracker] log not found: {log_path}")
        return []

    seen: Dict[str, float] = {}   # symbol → largest |move| seen
    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = _CIRCUIT_LINE_RE.search(line)
                if not m:
                    continue
                sym = m.group("symbol").upper()
                try:
                    mv = float(m.group("move"))
                except ValueError:
                    continue
                # Match the pipeline's |ret1d| > 15% skip rule.
                if abs(mv) <= CIRCUIT_MOVE_THRESHOLD:
                    continue
                # Keep the largest-magnitude move if the symbol appears twice.
                if abs(mv) > abs(seen.get(sym, 0.0)):
                    seen[sym] = mv
    except Exception as e:
        _log(f"[circuit_tracker] log parse error: {e}")
        return []

    events: List[Dict[str, Any]] = []
    for sym, mv in seen.items():
        events.append({
            "symbol":    sym,
            "move_pct":  round(mv, 2),
            "direction": DIRECTION_POS if mv > 0 else DIRECTION_NEG,
        })
    return events


def top_n_by_direction(events: List[Dict[str, Any]], n: int = TOP_N_PER_DIRECTION
                       ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split events by direction and return (top_n_pos, top_n_neg) ranked by |move|."""
    pos = sorted([e for e in events if e["direction"] == DIRECTION_POS],
                 key=lambda e: e["move_pct"], reverse=True)[:n]
    neg = sorted([e for e in events if e["direction"] == DIRECTION_NEG],
                 key=lambda e: e["move_pct"])[:n]    # ascending = most-negative first
    return pos, neg


# ---------------------------------------------------------------------------
# TRACK LIFECYCLE
# ---------------------------------------------------------------------------

def make_track_id(symbol: str, circuit_date: str) -> str:
    """`SYMBOL#YYYYMMDD` — stable, human-readable, unique per (stock, day)."""
    return f"{symbol}#{circuit_date.replace('-', '')}"


def _new_track_record(event: Dict[str, Any], circuit_date: str,
                      circuit_close: Optional[float]) -> Dict[str, Any]:
    """Fresh track record — all forward-return slots start empty."""
    return {
        "track_id":        make_track_id(event["symbol"], circuit_date),
        "symbol":          event["symbol"],
        "circuit_date":    circuit_date,
        "direction":       event["direction"],
        "circuit_move":    event["move_pct"],
        "circuit_close":   circuit_close,      # anchor price for return math
        "status":          STATUS_ACTIVE,
        "days_tracked":    0,
        "last_update":     None,
        "consecutive_nodata": 0,
        # 30 slots — filled in progressively. Keyed by day offset (1..30).
        "returns":         {str(d): None for d in range(1, TRACK_DAYS + 1)},
        # Running extrema (updated every day).
        "max_up_pct":      0.0,
        "max_down_pct":    0.0,
        "latest_return":   None,
        # Retirement metadata (set when track transitions to terminal).
        "retired_date":    None,
        "retired_reason":  None,
    }


def _fetch_close(symbol: str) -> Optional[float]:
    """Fetch today's close via main.fetch_price_data. Returns None on failure.

    Lazy-imports main to avoid a circular import at module load time.
    """
    try:
        from main import fetch_price_data  # type: ignore
    except Exception as e:
        _log(f"[circuit_tracker] cannot import fetch_price_data: {e}")
        return None
    try:
        df = fetch_price_data(symbol, period="5d")
        if df is None or df.empty:
            return None
        last = df.iloc[-1]
        # Handle both title-case and lower-case column names.
        for key in ("Close", "close"):
            if key in df.columns:
                val = last.get(key)
                if val is None:
                    continue
                try:
                    c = float(val)
                    if c > 0:
                        return c
                except (TypeError, ValueError):
                    continue
        return None
    except Exception as e:
        _log(f"[circuit_tracker] price fetch failed for {symbol}: {e}")
        return None


def _compute_day_offset(circuit_date_str: str, as_of_str: str) -> int:
    """Days elapsed since circuit_date (calendar days, not trading days).

    Trading days would be cleaner but calendar days keep the sheet layout
    simple and constant. Weekends just carry Friday's price forward.
    """
    try:
        cd = datetime.strptime(circuit_date_str, "%Y-%m-%d").date()
        ad = datetime.strptime(as_of_str,        "%Y-%m-%d").date()
        return (ad - cd).days
    except Exception:
        return 0


def _update_active_track(track: Dict[str, Any], as_of: str) -> None:
    """Fetch today's close and stamp the day-offset return column.

    Mutates track in-place. Also checks retirement conditions.
    """
    if track["status"] in TERMINAL_STATUSES:
        return

    day_off = _compute_day_offset(track["circuit_date"], as_of)
    if day_off <= 0:
        # Same-day rerun after seeding: nothing to update yet.
        track["last_update"] = as_of
        return
    if day_off > TRACK_DAYS:
        # Beyond the tracking window — retire.
        track["status"]         = STATUS_RETIRED_30DAY
        track["retired_date"]   = as_of
        track["retired_reason"] = f"day+{day_off} > {TRACK_DAYS}"
        return

    anchor = track.get("circuit_close") or 0.0
    if anchor <= 0:
        # No anchor price — cannot compute returns. Try to fetch it (rare).
        anchor_now = _fetch_close(track["symbol"])
        if anchor_now and anchor_now > 0:
            track["circuit_close"] = anchor_now
            anchor = anchor_now
        else:
            track["consecutive_nodata"] += 1
            if track["consecutive_nodata"] >= NODATA_RETIRE_DAYS:
                track["status"]         = STATUS_RETIRED_NODATA
                track["retired_date"]   = as_of
                track["retired_reason"] = f"no anchor after {NODATA_RETIRE_DAYS} tries"
            return

    close = _fetch_close(track["symbol"])
    if close is None or close <= 0:
        track["consecutive_nodata"] += 1
        track["last_update"] = as_of
        if track["consecutive_nodata"] >= NODATA_RETIRE_DAYS:
            track["status"]         = STATUS_RETIRED_NODATA
            track["retired_date"]   = as_of
            track["retired_reason"] = f"no data {NODATA_RETIRE_DAYS} days in a row"
        return

    # Success — compute cumulative return vs anchor.
    ret_pct = round((close / anchor - 1.0) * 100.0, 2)

    track["consecutive_nodata"] = 0
    track["returns"][str(day_off)] = ret_pct
    track["days_tracked"]  = day_off
    track["latest_return"] = ret_pct
    track["last_update"]   = as_of
    if ret_pct > track["max_up_pct"]:
        track["max_up_pct"]   = ret_pct
    if ret_pct < track["max_down_pct"]:
        track["max_down_pct"] = ret_pct

    # Early-exit retirement checks.
    if ret_pct >= BIG_WIN_THRESHOLD_PCT:
        track["status"]         = STATUS_RETIRED_BIG_WIN
        track["retired_date"]   = as_of
        track["retired_reason"] = f"cumulative {ret_pct:+.1f}% ≥ +{BIG_WIN_THRESHOLD_PCT:.0f}%"
    elif ret_pct <= BIG_LOSS_THRESHOLD_PCT:
        track["status"]         = STATUS_RETIRED_BIG_LOSS
        track["retired_date"]   = as_of
        track["retired_reason"] = f"cumulative {ret_pct:+.1f}% ≤ {BIG_LOSS_THRESHOLD_PCT:.0f}%"
    elif day_off >= TRACK_DAYS:
        track["status"]         = STATUS_RETIRED_30DAY
        track["retired_date"]   = as_of
        track["retired_reason"] = f"reached day+{TRACK_DAYS}"


# ---------------------------------------------------------------------------
# VERDICT (matches the doc — auto-labels)
# ---------------------------------------------------------------------------

def compute_verdict(track: Dict[str, Any]) -> str:
    """Human-readable verdict driven by direction + latest cumulative return."""
    days   = track.get("days_tracked", 0) or 0
    latest = track.get("latest_return")
    direction = track.get("direction", DIRECTION_POS)

    if days < 3 or latest is None:
        return "TOO_EARLY"

    if direction == DIRECTION_POS:
        if latest > 10:   return "STILL_UP"
        if latest > 5:    return "HOLDING"
        if latest > -5:   return "FLAT"
        if latest > -10:  return "FADING"
        return "CRASHED"
    else:  # NEG circuits — we want to see if they bounce
        if latest > 10:   return "BOUNCED_HARD"
        if latest > 5:    return "BOUNCED"
        if latest > -5:   return "STABILISED"
        if latest > -10:  return "STILL_DOWN"
        return "KEPT_FALLING"


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATION
# ---------------------------------------------------------------------------

def run_circuit_tracker(
    log_path: Optional[Path | str] = None,
    as_of_date: Optional[str] = None,
    is_scheduled: bool = True,
    store_path: Path = CIRCUIT_STORE_PATH,
) -> Dict[str, int]:
    """Main entry point — call once per pipeline run.

    Args:
        log_path      Path to today's run_log_YYYYMMDD.txt. If None,
                      derived from as_of_date.
        as_of_date    Run date in YYYY-MM-DD form. Defaults to today.
        is_scheduled  If False (manual run), does NOT persist state.
        store_path    Override for the persistent JSON store.

    Returns a counts dict for the pipeline log line:
        {"new_pos": int, "new_neg": int, "active": int, "retired_today": int}
    """
    as_of_date = as_of_date or date.today().isoformat()

    if log_path is None:
        yyyymmdd = as_of_date.replace("-", "")
        log_path = _HERE / LOG_FILENAME_TEMPLATE.format(yyyymmdd=yyyymmdd)
    else:
        log_path = Path(log_path)

    _log(f"[circuit_tracker] start · as_of={as_of_date} · log={log_path.name} "
         f"· scheduled={is_scheduled}")

    # 1. Load existing state.
    store  = load_store(store_path)
    tracks = store["tracks"]

    # 2. Parse today's log for new circuit events.
    events = parse_circuits_from_log(log_path)
    _log(f"[circuit_tracker] parsed {len(events)} circuit events from log")

    pos_top, neg_top = top_n_by_direction(events, TOP_N_PER_DIRECTION)
    new_events = pos_top + neg_top

    # 3. Seed new tracks (skip duplicates for this same date).
    new_pos = 0
    new_neg = 0
    for ev in new_events:
        tid = make_track_id(ev["symbol"], as_of_date)
        if tid in tracks:
            continue  # idempotent same-day rerun
        anchor_close = _fetch_close(ev["symbol"]) if is_scheduled else None
        tracks[tid] = _new_track_record(ev, as_of_date, anchor_close)
        if ev["direction"] == DIRECTION_POS:
            new_pos += 1
        else:
            new_neg += 1

    _log(f"[circuit_tracker] added {new_pos} POS + {new_neg} NEG new tracks")

    # 4. Update every ACTIVE track (including the ones we just added — their
    #    day+0 is today, so update loop is a no-op for them via _compute_day_offset).
    updated = 0
    retired_today = 0
    for tid, track in list(tracks.items()):
        if track["status"] in TERMINAL_STATUSES:
            continue
        prev_status = track["status"]
        _update_active_track(track, as_of_date)
        if track["status"] != prev_status and track["status"] in TERMINAL_STATUSES:
            retired_today += 1
        else:
            updated += 1

    active_after = sum(1 for t in tracks.values() if t["status"] == STATUS_ACTIVE)
    retired_all  = sum(1 for t in tracks.values() if t["status"] in TERMINAL_STATUSES)

    _log(f"[circuit_tracker] updated {updated} active tracks · "
         f"retired today: {retired_today} · "
         f"active total: {active_after} · retired total: {retired_all}")

    # 5. Persist (scheduled runs only).
    if is_scheduled:
        save_store(store, store_path)
        _log(f"[circuit_tracker] saved {store_path.name}")
    else:
        _log("[circuit_tracker] MANUAL run — state NOT persisted")

    return {
        "new_pos":       new_pos,
        "new_neg":       new_neg,
        "active":        active_after,
        "retired_today": retired_today,
        "retired_total": retired_all,
        "total":         len(tracks),
    }


# ---------------------------------------------------------------------------
# VIEW HELPERS — used by tracking_workbook_job.py
# ---------------------------------------------------------------------------

def load_all_tracks(store_path: Path = CIRCUIT_STORE_PATH) -> List[Dict[str, Any]]:
    """Return every track (active + retired) enriched with computed verdict.

    Sort order: ACTIVE first, then retired. Within each group, newest
    circuit_date first (so today's additions surface at the top).
    """
    store = load_store(store_path)
    out: List[Dict[str, Any]] = []
    for t in store["tracks"].values():
        t = dict(t)
        t["verdict"] = compute_verdict(t)
        out.append(t)
    # Stable descending sort by circuit_date (secondary sort by symbol asc).
    out.sort(key=lambda t: t.get("symbol", ""))
    out.sort(key=lambda t: t.get("circuit_date", ""), reverse=True)
    # Primary partition: ACTIVE first.
    out.sort(key=lambda t: 0 if t.get("status") == STATUS_ACTIVE else 1)
    return out


def summary_stats(store_path: Path = CIRCUIT_STORE_PATH) -> Dict[str, Any]:
    """Compute aggregate stats split by direction for the summary strip."""
    tracks = load_all_tracks(store_path)

    def _group(direction: str) -> Dict[str, Any]:
        group = [t for t in tracks if t.get("direction") == direction]
        retired = [t for t in group if t.get("status") in TERMINAL_STATUSES]
        active  = [t for t in group if t.get("status") == STATUS_ACTIVE]
        # Focus stats on retired (completed 30-day observation) — that's the
        # signal. Active are still in progress and would bias the average.
        d30_returns = [t["returns"].get("30") for t in retired
                       if t.get("returns", {}).get("30") is not None]
        avg_d30 = round(sum(d30_returns) / len(d30_returns), 2) if d30_returns else None
        # Fallback: use latest_return for retired tracks that didn't reach day 30
        # (RETIRED_BIG_WIN / RETIRED_BIG_LOSS / RETIRED_NODATA).
        latest_returns = [t.get("latest_return") for t in retired
                          if t.get("latest_return") is not None]
        avg_latest = round(sum(latest_returns) / len(latest_returns), 2) if latest_returns else None
        pos_pct = None
        if latest_returns:
            wins = sum(1 for r in latest_returns if r > 0)
            pos_pct = round(wins / len(latest_returns) * 100, 1)
        return {
            "active":   len(active),
            "retired":  len(retired),
            "avg_d30":  avg_d30,
            "avg_latest": avg_latest,
            "pct_positive": pos_pct,
        }

    return {
        "as_of":  date.today().isoformat(),
        "total":  len(tracks),
        "pos":    _group(DIRECTION_POS),
        "neg":    _group(DIRECTION_NEG),
    }


# ---------------------------------------------------------------------------
# CLI (for manual testing / backfill)
# ---------------------------------------------------------------------------

def _cli():
    import argparse
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--log",       help="Path to run_log_YYYYMMDD.txt")
    p.add_argument("--as-of",     help="Run date YYYY-MM-DD (default: today)")
    p.add_argument("--store",     default=str(CIRCUIT_STORE_PATH),
                   help="Path to circuit_tracker.json")
    p.add_argument("--dry-run",   action="store_true",
                   help="Parse + update but do not persist state")
    p.add_argument("--summary",   action="store_true",
                   help="Only print current summary_stats() and exit")
    args = p.parse_args()

    if args.summary:
        stats = summary_stats(Path(args.store))
        print(json.dumps(stats, indent=2))
        return 0

    counts = run_circuit_tracker(
        log_path     = args.log,
        as_of_date   = args.as_of,
        is_scheduled = not args.dry_run,
        store_path   = Path(args.store),
    )
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
