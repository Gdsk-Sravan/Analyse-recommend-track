"""
shadow_log.py — Phase I observation-mode logger.

Records "what would have happened" for stocks that Phase I rejects via
SETUP_EDGE_SKIP. Zero real money at risk — pure paper-trade tracking so
we can validate the backtest predictions in live NSE conditions.

Design (2026-07-07):
  • record_shadow_trade(): called from apply_setup_edge() when a stock is
    tagged phase_i_skip. Writes one PENDING row to shadow_trades.csv.
  • update_shadow_outcomes(): called at start of every evening run.
    Fetches recent OHLC for every PENDING row and resolves:
        WIN       → high >= target_1 first
        LOSS      → low  <= stop_loss first
        TIME_EXIT → still open after MAX_SHADOW_DAYS calendar days
  • format_shadow_summary(): compact text block for the Telegram brief.

Break-even math (mirrors backtest_walkforward.py):
    entry     = today's close
    target_1  = entry * 1.05     (+5%)
    stop_loss = entry * 0.97     (-3%)
    max hold  = 10 trading days   (env: MAX_SHADOW_DAYS)

Enable via env: PHASE_I_SHADOW_LOG=true  (default: true — always on)

CSV schema (shadow_trades.csv):
    date_added, symbol, setup, regime, conf, entry, target_1, stop_loss,
    status, exit_date, exit_price, r_multiple, days_held, note

No external dependencies beyond pandas + yfinance (already used by main.py).
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd

# yfinance is optional — if unavailable, outcome updates gracefully skip.
try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False


# ─── config ─────────────────────────────────────────────────────────────────
SHADOW_CSV_PATH = os.getenv("SHADOW_CSV_PATH", "shadow_trades.csv")
SHADOW_ENABLED  = os.getenv("PHASE_I_SHADOW_LOG", "true").lower() in ("1", "true", "yes")
MAX_SHADOW_DAYS = int(os.getenv("MAX_SHADOW_DAYS", "10"))
TARGET_PCT      = float(os.getenv("SHADOW_TARGET_PCT", "5.0"))   # +5%
STOP_PCT        = float(os.getenv("SHADOW_STOP_PCT",   "3.0"))   # -3%

_CSV_COLS = [
    "date_added", "symbol", "setup", "regime", "conf",
    "entry", "target_1", "stop_loss",
    "status", "exit_date", "exit_price", "r_multiple", "days_held", "note",
]

_STATUS_PENDING   = "PENDING"
_STATUS_WIN       = "WIN"
_STATUS_LOSS      = "LOSS"
_STATUS_TIME_EXIT = "TIME_EXIT"
_STATUS_ERROR     = "ERROR"


# ─── module-level guard: file exists with correct header ────────────────────
def _ensure_csv() -> None:
    """Create shadow_trades.csv with header if missing."""
    if not os.path.exists(SHADOW_CSV_PATH):
        with open(SHADOW_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(_CSV_COLS)


def _read_all() -> List[dict]:
    """Read all rows. Returns empty list if file is missing/malformed."""
    if not os.path.exists(SHADOW_CSV_PATH):
        return []
    try:
        with open(SHADOW_CSV_PATH, "r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _write_all(rows: List[dict]) -> None:
    """Rewrite the whole CSV. Small file — safe to rewrite atomically."""
    tmp = SHADOW_CSV_PATH + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_COLS)
        w.writeheader()
        for r in rows:
            # Only keep known columns; skip stray keys
            w.writerow({c: r.get(c, "") for c in _CSV_COLS})
    os.replace(tmp, SHADOW_CSV_PATH)


# ─── PUBLIC API ─────────────────────────────────────────────────────────────
def record_shadow_trade(stock: dict, regime: str) -> None:
    """Log one PENDING shadow trade for a Phase-I-skipped stock.

    Called from main.py apply_setup_edge() when phase_i_skip is set.
    Silent no-op if PHASE_I_SHADOW_LOG=false or entry price is missing.
    Also silent no-op if the same symbol was already recorded today
    (prevents duplicates across intraday re-runs).
    """
    if not SHADOW_ENABLED:
        return
    try:
        symbol = str(stock.get("symbol") or "").strip()
        entry  = float(stock.get("close", 0) or 0)
        setup  = str(stock.get("setup_type") or "OTHER")
        conf   = float(stock.get("final_confidence", 0) or 0)
        if not symbol or entry <= 0:
            return

        today = datetime.now().strftime("%Y-%m-%d")

        _ensure_csv()
        rows = _read_all()

        # Dedup: same symbol + today = skip
        for r in rows:
            if r.get("symbol") == symbol and r.get("date_added") == today:
                return

        target_1  = round(entry * (1 + TARGET_PCT / 100.0), 2)
        stop_loss = round(entry * (1 - STOP_PCT   / 100.0), 2)

        rows.append({
            "date_added": today,
            "symbol":     symbol,
            "setup":      setup,
            "regime":     regime,
            "conf":       f"{conf:.1f}",
            "entry":      f"{entry:.2f}",
            "target_1":   f"{target_1:.2f}",
            "stop_loss":  f"{stop_loss:.2f}",
            "status":     _STATUS_PENDING,
            "exit_date":  "",
            "exit_price": "",
            "r_multiple": "",
            "days_held":  "",
            "note":       "phase_i_shadow",
        })
        _write_all(rows)
    except Exception as e:
        # Never let shadow logging crash the pipeline
        print(f"[shadow_log] record failed for {stock.get('symbol')}: {e}")


def update_shadow_outcomes(quiet: bool = False) -> dict:
    """Resolve every PENDING row: WIN / LOSS / TIME_EXIT.

    Called once at start of the evening pipeline. Returns a small stats dict:
        {"pending": int, "resolved_today": int, "wins": int, "losses": int,
         "time_exits": int, "errors": int}
    """
    stats = {"pending": 0, "resolved_today": 0,
             "wins": 0, "losses": 0, "time_exits": 0, "errors": 0}

    if not SHADOW_ENABLED:
        return stats

    if not _YF_OK:
        if not quiet:
            print("[shadow_log] yfinance unavailable — skipping outcome update")
        return stats

    _ensure_csv()
    rows = _read_all()
    if not rows:
        return stats

    today = datetime.now().date()
    changed = False

    for r in rows:
        if r.get("status") != _STATUS_PENDING:
            continue
        stats["pending"] += 1

        symbol = r.get("symbol", "")
        try:
            entry     = float(r.get("entry")     or 0)
            target_1  = float(r.get("target_1")  or 0)
            stop_loss = float(r.get("stop_loss") or 0)
            added_str = r.get("date_added", "")
            added_dt  = datetime.strptime(added_str, "%Y-%m-%d").date()
        except Exception:
            r["status"] = _STATUS_ERROR
            r["note"]   = "bad_row_fields"
            stats["errors"] += 1
            changed = True
            continue

        # Fetch OHLC from day-after-add up to today
        start = added_dt + timedelta(days=1)
        if start > today:
            continue  # not enough data yet

        try:
            yfsym = symbol if symbol.endswith(".NS") else symbol + ".NS"
            df = yf.download(
                yfsym,
                start=start.strftime("%Y-%m-%d"),
                end=(today + timedelta(days=1)).strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty:
                # Try again without .NS suffix (some symbols have odd tickers)
                continue
        except Exception as e:
            if not quiet:
                print(f"[shadow_log] fetch failed for {symbol}: {e}")
            continue

        # Walk bars chronologically. Which comes first: target or stop?
        resolved = None
        exit_dt = None
        exit_px = None

        for idx, row in df.iterrows():
            try:
                hi = float(row.get("High", 0))
                lo = float(row.get("Low", 0))
            except Exception:
                continue
            # Tie-break: if both hit same day, assume STOP (conservative)
            if lo <= stop_loss:
                resolved = _STATUS_LOSS
                exit_dt  = idx.date() if hasattr(idx, "date") else today
                exit_px  = stop_loss
                break
            if hi >= target_1:
                resolved = _STATUS_WIN
                exit_dt  = idx.date() if hasattr(idx, "date") else today
                exit_px  = target_1
                break

        # No hit yet? Check if we've exceeded the max hold period
        if resolved is None:
            days_open = (today - added_dt).days
            if days_open >= MAX_SHADOW_DAYS:
                resolved = _STATUS_TIME_EXIT
                exit_dt  = today
                try:
                    exit_px = float(df["Close"].iloc[-1])
                except Exception:
                    exit_px = entry  # fallback: assume flat

        if resolved:
            # r_multiple: reward-risk units. LOSS = -1R, WIN = +target/stop ratio
            risk_per_share = entry - stop_loss
            if risk_per_share <= 0:
                r_mult = 0.0
            else:
                r_mult = round((exit_px - entry) / risk_per_share, 2)

            r["status"]     = resolved
            r["exit_date"]  = exit_dt.strftime("%Y-%m-%d") if hasattr(exit_dt, "strftime") else str(exit_dt)
            r["exit_price"] = f"{exit_px:.2f}"
            r["r_multiple"] = f"{r_mult:.2f}"
            r["days_held"]  = str((exit_dt - added_dt).days) if hasattr(exit_dt, "strftime") else ""
            stats["resolved_today"] += 1
            if resolved == _STATUS_WIN:       stats["wins"] += 1
            elif resolved == _STATUS_LOSS:    stats["losses"] += 1
            elif resolved == _STATUS_TIME_EXIT: stats["time_exits"] += 1
            changed = True

    if changed:
        _write_all(rows)

    return stats


def format_shadow_summary(max_lines: int = 6) -> str:
    """Return a compact multi-line summary suitable for Telegram.

    Uses only ROWS ALREADY IN THE CSV — call update_shadow_outcomes() first.
    Returns "" if shadow logging disabled or file empty.
    """
    if not SHADOW_ENABLED:
        return ""
    rows = _read_all()
    if not rows:
        return ""

    total   = len(rows)
    pending = sum(1 for r in rows if r.get("status") == _STATUS_PENDING)
    wins    = sum(1 for r in rows if r.get("status") == _STATUS_WIN)
    losses  = sum(1 for r in rows if r.get("status") == _STATUS_LOSS)
    time_ex = sum(1 for r in rows if r.get("status") == _STATUS_TIME_EXIT)

    resolved = wins + losses + time_ex
    win_pct  = (wins / resolved * 100.0) if resolved else 0.0

    # Sum r_multiple across resolved trades
    total_r = 0.0
    for r in rows:
        if r.get("status") in (_STATUS_WIN, _STATUS_LOSS, _STATUS_TIME_EXIT):
            try:
                total_r += float(r.get("r_multiple") or 0)
            except Exception:
                pass
    exp_r = (total_r / resolved) if resolved else 0.0

    lines = [
        "🔬 SHADOW LOG (Phase I observation)",
        f"  Total logged: {total} · Pending: {pending} · Resolved: {resolved}",
    ]
    if resolved > 0:
        verdict = (
            "✅ matches backtest" if 42 <= win_pct <= 52
            else ("⚠️ better than backtest" if win_pct > 52
                  else "⚠️ worse than backtest")
        )
        lines.append(
            f"  Wins {wins}/{resolved} ({win_pct:.1f}%) · "
            f"Exp {exp_r:+.2f}R · {verdict}"
        )

    # Top 3 recent PENDING for transparency
    pending_rows = [r for r in rows if r.get("status") == _STATUS_PENDING]
    pending_rows.sort(key=lambda r: r.get("date_added", ""), reverse=True)
    for r in pending_rows[:max_lines - len(lines)]:
        lines.append(
            f"    · {r.get('date_added','')} {r.get('symbol',''):<12} "
            f"{r.get('setup',''):<9} conf {r.get('conf','')} "
            f"[{r.get('regime','')}]"
        )

    return "\n".join(lines)


# ─── CLI mode (for debugging / cron) ────────────────────────────────────────
if __name__ == "__main__":
    print("[shadow_log] CLI: updating outcomes...")
    s = update_shadow_outcomes(quiet=False)
    print(f"[shadow_log] stats: {s}")
    print()
    print(format_shadow_summary())
