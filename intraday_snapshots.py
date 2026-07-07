"""
intraday_snapshots.py
======================
Level-1 intraday OHLC snapshot store for smarter next-day entries.

Rationale:
    The evening pipeline picks candidates on daily bars but often the entry
    price it recommends is stale by morning (gaps, overnight news, ADRs).
    A 5-min snapshot store lets `morning_check.py` (a) refine the entry
    limit price, (b) detect gap-up above target, (c) skip if the setup
    has already broken down intraday.

Storage:
    JSON at $INTRADAY_SNAPSHOT_FILE (default `intraday_snapshots.json`).
    Structure:
        {
            "date": "2026-07-07",
            "snapshots": {
                "RELIANCE.NS": [
                    {"t": "09:15", "o": 2450, "h": 2455, "l": 2448, "c": 2452, "v": 12345},
                    ...
                ]
            },
            "last_updated": "..."
        }

    File resets each trading day (evening pipeline clears it).

Snapshot cadence: caller supplies interval (typical: 5 min, aligns with the
intraday_monitor tick).

Public API:
    record_snapshot(symbol, price, high=None, low=None, volume=None) -> None
    load_todays_snapshots() -> dict
    entry_hint(symbol, planned_entry, planned_stop) -> dict
        returns {suggested_entry, gap_type, breakout_confirmed,
                 volume_confirms, action_hint}
    reset_for_new_day() -> None
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_SNAP_FILE = os.environ.get("INTRADAY_SNAPSHOT_FILE", "intraday_snapshots.json")
_BUCKET_MIN = int(os.environ.get("INTRADAY_SNAPSHOT_INTERVAL_MIN", "5"))


# ---------------------------------------------------------------------------
# Store I/O
# ---------------------------------------------------------------------------

def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _load() -> Dict[str, Any]:
    if not os.path.exists(_SNAP_FILE):
        return {"date": _today_str(), "snapshots": {}, "last_updated": None}
    try:
        with open(_SNAP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # auto-reset if date changed
        if data.get("date") != _today_str():
            return {"date": _today_str(), "snapshots": {}, "last_updated": None}
        return data
    except Exception as e:
        log.warning("intraday_snapshots: load failed: %s", e)
        return {"date": _today_str(), "snapshots": {}, "last_updated": None}


def _save(data: Dict[str, Any]) -> None:
    try:
        data["last_updated"] = datetime.now().isoformat(timespec="seconds")
        tmp = _SNAP_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(tmp, _SNAP_FILE)
    except Exception as e:
        log.warning("intraday_snapshots: save failed: %s", e)


def reset_for_new_day() -> None:
    _save({"date": _today_str(), "snapshots": {}, "last_updated": None})


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------

def _bucket_time(now: Optional[datetime] = None) -> str:
    """Round `now` down to _BUCKET_MIN and format HH:MM."""
    dt = now or datetime.now()
    m = (dt.minute // _BUCKET_MIN) * _BUCKET_MIN
    return f"{dt.hour:02d}:{m:02d}"


def record_snapshot(
    symbol: str,
    price: float,
    high: Optional[float] = None,
    low: Optional[float] = None,
    volume: Optional[float] = None,
) -> None:
    """
    Append (or update-in-place if same bucket) a single L1 snapshot for `symbol`.

    Safe to call from every tick of intraday_monitor — it de-dups on the
    5-min bucket and only mutates the current bucket's H/L/close.
    """
    if not symbol or price is None or price <= 0:
        return
    data = _load()
    bucket = _bucket_time()
    snaps = data.setdefault("snapshots", {}).setdefault(symbol, [])
    if snaps and snaps[-1].get("t") == bucket:
        # update in-place
        b = snaps[-1]
        b["h"] = max(b.get("h", price), high if high is not None else price)
        b["l"] = min(b.get("l", price), low  if low  is not None else price)
        b["c"] = price
        if volume is not None:
            b["v"] = max(b.get("v", 0), volume)
    else:
        snaps.append({
            "t": bucket,
            "o": price,
            "h": high if high is not None else price,
            "l": low  if low  is not None else price,
            "c": price,
            "v": volume if volume is not None else 0,
        })
    _save(data)


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------

def load_todays_snapshots(symbol: Optional[str] = None) -> Any:
    data = _load()
    snaps = data.get("snapshots", {})
    if symbol is None:
        return snaps
    return snaps.get(symbol, [])


def entry_hint(symbol: str, planned_entry: float, planned_stop: float) -> Dict[str, Any]:
    """
    Given the evening pipeline's plan, inspect today's intraday tape and
    return an entry hint for morning_check.py.

    Returns:
        {
            "have_data": bool,
            "current_price": float | None,
            "gap_type": "GAP_UP_ABOVE_TARGET" | "GAP_UP_SMALL" | "FLAT"
                        | "GAP_DOWN_ABOVE_STOP" | "GAP_DOWN_BELOW_STOP",
            "suggested_entry": float | None,   # refined limit
            "breakout_confirmed": bool,        # crossed entry with volume?
            "action_hint": "TAKE" | "WAIT_PULLBACK" | "SKIP_GAP" | "SKIP_BROKEN"
        }
    """
    snaps = load_todays_snapshots(symbol)
    if not snaps or planned_entry <= 0:
        return {"have_data": False, "action_hint": "TAKE",
                "suggested_entry": planned_entry}

    # First-bar open + latest snapshot
    first = snaps[0]
    latest = snaps[-1]
    open_px = float(first.get("o", 0))
    latest_px = float(latest.get("c", 0))
    session_high = max(float(s.get("h", 0)) for s in snaps)
    session_low  = min(float(s.get("l", latest_px)) for s in snaps if s.get("l"))

    gap_pct = (open_px - planned_entry) / planned_entry * 100.0 if planned_entry else 0.0

    # Classify gap
    if gap_pct >= 2.0:
        gap_type = "GAP_UP_LARGE"
    elif gap_pct >= 0.5:
        gap_type = "GAP_UP_SMALL"
    elif gap_pct <= -1.5 and latest_px <= planned_stop:
        gap_type = "GAP_DOWN_BELOW_STOP"
    elif gap_pct <= -0.5:
        gap_type = "GAP_DOWN"
    else:
        gap_type = "FLAT"

    # Volume confirms? — compare latest bucket vol vs median of first 3
    breakout_confirmed = False
    if len(snaps) >= 2:
        vols = [float(s.get("v", 0)) for s in snaps]
        median_vol = sorted(vols)[len(vols) // 2]
        if latest_px >= planned_entry and vols[-1] > median_vol * 1.3:
            breakout_confirmed = True

    # Action logic
    if latest_px <= planned_stop:
        action = "SKIP_BROKEN"           # stop already hit
        suggested = None
    elif gap_type == "GAP_UP_LARGE":
        # Skip chase — the edge is gone once price is 2%+ above entry
        action = "SKIP_GAP"
        suggested = None
    elif gap_type == "GAP_UP_SMALL":
        # Enter at planned entry (pullback) if latest below entry, else market
        action = "WAIT_PULLBACK" if latest_px > planned_entry * 1.01 else "TAKE"
        suggested = planned_entry
    elif breakout_confirmed:
        action = "TAKE"
        # Suggest slightly above planned entry to guarantee fill (0.1%)
        suggested = round(planned_entry * 1.001, 2)
    else:
        action = "TAKE"
        suggested = planned_entry

    return {
        "have_data": True,
        "current_price": latest_px,
        "open_price": open_px,
        "session_high": session_high,
        "session_low":  session_low,
        "gap_pct": round(gap_pct, 2),
        "gap_type": gap_type,
        "breakout_confirmed": breakout_confirmed,
        "suggested_entry": suggested,
        "action_hint": action,
        "n_snapshots": len(snaps),
    }


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    # smoke test
    reset_for_new_day()
    record_snapshot("RELIANCE.NS", 2455, high=2458, low=2450, volume=10000)
    record_snapshot("RELIANCE.NS", 2460, high=2462, low=2455, volume=15000)
    h = entry_hint("RELIANCE.NS", planned_entry=2450, planned_stop=2420)
    print(json.dumps(h, indent=2))
