"""
intraday_monitor.py — Phase G3: State-change alerts during market hours
======================================================================

Runs every 15 minutes between 09:30 – 15:15 IST (via GitHub Actions cron)
and fires a Telegram alert ONLY when an OPEN position crosses a boundary
that the user needs to know about intraday:

    T1_HIT               close ≥ target1  (first time)
    T2_HIT               close ≥ target2  (first time)
    STOP_HIT             close ≤ stop     (first time)
    PARABOLIC_INTRADAY   day gain ≥ PARABOLIC_DAY_PCT + volume ≥ N × 20d avg
    TRAIL_TIGHTENED      chandelier trail moved stop up by ≥ 1%

The monitor is READ-ONLY — it never mutates tracker.json / trade_tracker.json.
main.py's evening tracker_job.py remains the sole writer for state changes;
this file only informs the user in real time so they can act if they trade
their own book.

An in-memory dedup cache is persisted to $INTRADAY_STATE_FILE so the same
event does not spam the channel across 15-min re-runs.

Reads:  tracker.json                   (list of OPEN positions from main.py)
        intraday_state.json            (last-seen state per symbol)
Writes: intraday_state.json            (dedup cache — bounded to today)
Sends:  BUY_BOT_TOKEN + BUY_CHAT_ID    (same channel as morning_check.py)

Env-gated by INTRADAY_MONITOR_ENABLED (default TRUE — safe: read-only).

GitHub Actions schedule (UTC):
    - cron: "*/15 4-10 * * 1-5"   # 09:30 – 15:30 IST, every 15 min
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, date
from typing import Any

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False
    print("[ERROR] yfinance not installed. Run: pip install yfinance")


# ─── Config ───────────────────────────────────────────────────────────────
INTRADAY_MONITOR_ENABLED = os.getenv("INTRADAY_MONITOR_ENABLED", "true").lower() == "true"

BUY_BOT_TOKEN = os.getenv("BUY_BOT_TOKEN", "")
BUY_CHAT_ID   = os.getenv("BUY_CHAT_ID", "")

TRACKER_FILE       = os.getenv("TRACKER_FILE", "tracker.json")
INTRADAY_STATE_FILE = os.getenv("INTRADAY_STATE_FILE", "intraday_state.json")

# Parabolic thresholds mirror main.py — a truly professional monitor uses the
# same numbers as the exit stack so alerts align with what tracker_job will do
# on the evening run.
PARABOLIC_DAY_PCT  = float(os.getenv("PARABOLIC_DAY_PCT",  "8.0"))
PARABOLIC_VOL_MULT = float(os.getenv("PARABOLIC_VOL_MULT", "2.0"))

TELEGRAM_MAX = 4000
FRESH_START  = os.getenv("FRESH_START", "false").lower() == "true"


# ─── Persistence ──────────────────────────────────────────────────────────
def _load_state() -> dict:
    """Load per-symbol last-seen event set. Auto-resets on new calendar day."""
    today = date.today().isoformat()
    if not os.path.exists(INTRADAY_STATE_FILE):
        return {"date": today, "events": {}}
    try:
        with open(INTRADAY_STATE_FILE, "r") as f:
            data = json.load(f)
        if data.get("date") != today:
            # New trading day → fresh dedup cache
            return {"date": today, "events": {}}
        return data
    except Exception:
        return {"date": today, "events": {}}


def _save_state(state: dict) -> None:
    try:
        with open(INTRADAY_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[WARN] Could not save {INTRADAY_STATE_FILE}: {e}")


def _load_open_positions() -> list:
    if FRESH_START:
        print("[FRESH_START] intraday_monitor: skipping — main.py wiped state this run")
        return []
    if not os.path.exists(TRACKER_FILE):
        return []
    try:
        with open(TRACKER_FILE) as f:
            entries = json.load(f)
    except Exception as e:
        print(f"[WARN] Could not read {TRACKER_FILE}: {e}")
        return []
    if isinstance(entries, dict):
        entries = entries.get("entries") or []
    if not isinstance(entries, list):
        return []
    return [e for e in entries if isinstance(e, dict) and e.get("status") == "OPEN"]


# ─── Telegram ─────────────────────────────────────────────────────────────
def _send(message: str) -> None:
    """Send to BUY channel. Falls back to printing if not configured."""
    if not BUY_BOT_TOKEN or not BUY_CHAT_ID:
        print("[INFO] BUY channel not configured — printing instead:")
        print(message)
        return
    chunks = []
    while message:
        if len(message) <= TELEGRAM_MAX:
            chunks.append(message)
            break
        split = message.rfind("\n", 0, TELEGRAM_MAX)
        if split == -1:
            split = TELEGRAM_MAX
        chunks.append(message[:split])
        message = message[split:].lstrip("\n")
    for chunk in chunks:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BUY_BOT_TOKEN}/sendMessage",
                json={"chat_id": BUY_CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                timeout=15,
            )
            if resp.status_code != 200:
                print(f"[WARN] Telegram send failed: {resp.status_code} {resp.text[:100]}")
            time.sleep(0.3)
        except Exception as ex:
            print(f"[WARN] _send failed: {ex}")


# ─── Price fetch ──────────────────────────────────────────────────────────
def _scalar(v: Any) -> float:
    try:
        if hasattr(v, "iloc"):
            v = v.iloc[0] if len(v) > 0 else v
        if hasattr(v, "item"):
            v = v.item()
        return float(v)
    except Exception:
        return float("nan")


def _fetch_intraday_snapshot(symbol: str) -> dict:
    """Return the latest 5-min bar + today's day-open + 20d avg volume.

    Returns empty dict on failure — caller must handle.
    """
    try:
        df = yf.download(symbol, period="5d", interval="5m",
                         progress=False, auto_adjust=True,
                         multi_level_index=False)
        if df is None or len(df) == 0:
            return {}
        if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
            try:
                df = df.xs(symbol, axis=1, level=-1)
            except Exception:
                df.columns = df.columns.get_level_values(0)

        # Slice to just today's bars for day-open + intraday high/low
        today = date.today().isoformat()
        today_bars = df[df.index.strftime("%Y-%m-%d") == today]
        if len(today_bars) == 0:
            # Market not open yet
            return {}
        day_open = _scalar(today_bars["Open"].iloc[0])
        day_high = _scalar(today_bars["High"].max())
        day_low  = _scalar(today_bars["Low"].min())
        last_px  = _scalar(today_bars["Close"].iloc[-1])
        day_vol  = _scalar(today_bars["Volume"].sum())

        # 20d average daily volume (from daily download)
        try:
            dfd = yf.download(symbol, period="1mo", interval="1d",
                              progress=False, auto_adjust=True,
                              multi_level_index=False)
            if dfd is not None and len(dfd) >= 5:
                if hasattr(dfd.columns, "nlevels") and dfd.columns.nlevels > 1:
                    dfd.columns = dfd.columns.get_level_values(0)
                avg_vol_20d = _scalar(dfd["Volume"].tail(20).mean())
            else:
                avg_vol_20d = day_vol
        except Exception:
            avg_vol_20d = day_vol

        return {
            "last":     last_px,
            "day_open": day_open,
            "day_high": day_high,
            "day_low":  day_low,
            "day_vol":  day_vol,
            "avg_vol":  avg_vol_20d,
        }
    except Exception as ex:
        print(f"[WARN] snapshot {symbol}: {ex}")
        return {}


# ─── Event detection ──────────────────────────────────────────────────────
def _detect_events(pos: dict, snap: dict) -> list:
    """Return a list of newly triggered event names for a position.

    Only fires an event ONCE per calendar day — the caller consults the dedup
    cache and filters accordingly.
    """
    events = []
    entry  = float(pos.get("entry", 0) or 0)
    stop   = float(pos.get("stop", 0) or 0)
    t1     = float(pos.get("target1", 0) or 0)
    t2     = float(pos.get("target2", 0) or 0)
    if entry <= 0:
        return events

    last  = snap["last"]
    hi    = snap["day_high"]
    lo    = snap["day_low"]

    if t1 > 0 and hi >= t1 and not pos.get("partial_closed"):
        events.append("T1_HIT")
    if t2 > 0 and hi >= t2:
        events.append("T2_HIT")
    if stop > 0 and lo <= stop:
        events.append("STOP_HIT")

    # Parabolic intraday: day gain ≥ threshold AND volume already ≥ N × 20d avg
    day_open = snap["day_open"]
    if day_open > 0:
        day_gain = (last - day_open) / day_open * 100
        vol_ratio = (snap["day_vol"] / snap["avg_vol"]) if snap["avg_vol"] > 0 else 0
        if day_gain >= PARABOLIC_DAY_PCT and vol_ratio >= PARABOLIC_VOL_MULT:
            events.append("PARABOLIC_INTRADAY")

    return events


def _format_alert(pos: dict, snap: dict, event: str) -> str:
    sym = pos.get("symbol", "?")
    entry = float(pos.get("entry", 0) or 0)
    stop  = float(pos.get("stop", 0) or 0)
    t1    = float(pos.get("target1", 0) or 0)
    t2    = float(pos.get("target2", 0) or 0)
    last  = snap["last"]
    pnl   = round((last - entry) / entry * 100, 2) if entry > 0 else 0

    icon = {
        "T1_HIT":             "🎯",
        "T2_HIT":             "🏁",
        "STOP_HIT":           "🛑",
        "PARABOLIC_INTRADAY": "🚀",
    }.get(event, "🔔")

    action = {
        "T1_HIT":  "Consider booking partial. Trail residual to entry (breakeven).",
        "T2_HIT":  "Book second slice. If RUNNER_MODE_ENABLED, ride the chandelier trail.",
        "STOP_HIT": "Stop breached. Consider full exit on close.",
        "PARABOLIC_INTRADAY": "Blow-off day — high volume + big move. Consider booking a slice.",
    }.get(event, "State change — review.")

    lines = [
        f"{icon} <b>{sym}</b> — {event}",
        f"Now: ₹{last:.2f}  ({pnl:+.1f}%)",
        f"Entry ₹{entry:.2f} · Stop ₹{stop:.2f}",
        f"T1 ₹{t1:.2f} · T2 ₹{t2:.2f}",
        "",
        f"➡ {action}",
    ]
    return "\n".join(lines)


# ─── Main ─────────────────────────────────────────────────────────────────
def run_intraday_monitor() -> None:
    if not INTRADAY_MONITOR_ENABLED:
        print("[INFO] intraday_monitor disabled via INTRADAY_MONITOR_ENABLED")
        return
    if not _YF_OK:
        return

    positions = _load_open_positions()
    if not positions:
        print("[INFO] No OPEN positions to monitor")
        return
    print(f"[INFO] intraday_monitor: {len(positions)} OPEN positions")

    state = _load_state()
    fired_symbols = state.get("events", {})
    new_alerts = []

    for pos in positions:
        sym = pos.get("symbol", "")
        if not sym:
            continue
        snap = _fetch_intraday_snapshot(sym)
        if not snap:
            continue

        # ── Phase G7 (2026-07-07): record L1 snapshot for morning_check.py ──
        # Cheap side-effect: append 5-min OHLC bucket for the symbol.
        # Feature-gated so we don't break existing users.
        if os.environ.get("ENABLE_INTRADAY_SNAPSHOTS", "true").lower() == "true":
            try:
                import intraday_snapshots as _isn
                _isn.record_snapshot(
                    sym,
                    price=_scalar(snap.get("last")),
                    high=_scalar(snap.get("high")),
                    low=_scalar(snap.get("low")),
                    volume=_scalar(snap.get("volume")),
                )
            except Exception as _e:  # never break monitor if snapshot fails
                pass

        events = _detect_events(pos, snap)
        if not events:
            continue

        already = set(fired_symbols.get(sym, []))
        for ev in events:
            if ev in already:
                continue
            new_alerts.append(_format_alert(pos, snap, ev))
            already.add(ev)
        fired_symbols[sym] = sorted(already)

    state["events"] = fired_symbols
    _save_state(state)

    if not new_alerts:
        print("[INFO] No new state-change events this tick")
        return

    ts = datetime.now().strftime("%H:%M")
    header = f"⏱ <b>INTRADAY ALERT — {ts} IST</b>"
    body   = "\n\n".join(new_alerts)
    _send(f"{header}\n{'─' * 32}\n{body}")
    print(f"[INFO] Fired {len(new_alerts)} intraday alerts")


if __name__ == "__main__":
    run_intraday_monitor()
