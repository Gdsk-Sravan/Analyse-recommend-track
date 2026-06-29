"""
weekly_summary_job.py — Saturday Morning Weekly Recap (FEATURE 6)
GitHub Actions: runs every Saturday at 9:30 AM IST (4:00 UTC)
Sends a concise weekly summary to Telegram.
"""
import os
import json
import requests
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TRACKER_FILE       = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")
CONF_HISTORY_FILE  = os.getenv("CONF_HISTORY_FILE", "confidence_history.json")

SECTOR_PROXIES = {
    "PHARMA":        "SUNPHARMA.NS",
    "BANKING":       "HDFCBANK.NS",
    "IT":            "INFY.NS",
    "AUTO":          "MARUTI.NS",
    "CAPITAL_GOODS": "BHEL.NS",
    "ENERGY":        "RELIANCE.NS",
    "METALS":        "TATASTEEL.NS",
    "DEFENCE":       "HAL.NS",
}


def _weekly_pct(ticker: str) -> float:
    try:
        df = yf.download(ticker, period="5d", interval="1d",
                         progress=False, auto_adjust=True, multi_level_index=False)
        if df is None or len(df) < 2:
            return 0.0
        return round(
            (float(df["Close"].iloc[-1]) - float(df["Close"].iloc[0])) /
            float(df["Close"].iloc[0]) * 100, 2
        )
    except Exception:
        return 0.0


def run_weekly_summary():
    today      = datetime.now()
    week_start = (today - timedelta(days=6)).strftime("%b %d")
    week_end   = today.strftime("%b %d, %Y")
    print(f"=== WEEKLY SUMMARY: {week_start} \u2014 {week_end} ===")

    # Load tracker
    tracker = {}
    try:
        if os.path.exists(TRACKER_FILE):
            with open(TRACKER_FILE, "r") as f:
                tracker = json.load(f)
    except Exception as e:
        print(f"[WARN] Tracker load failed: {e}")

    active    = tracker.get("buys", [])
    watching  = tracker.get("watchlist", [])
    completed = tracker.get("completed", [])
    perf      = tracker.get("performance", {})

    # NIFTY weekly
    nifty_pct = _weekly_pct("^NSEI")

    # Sector performance
    sector_perf = {}
    for sector, proxy in SECTOR_PROXIES.items():
        pct = _weekly_pct(proxy)
        if pct != 0.0:
            sector_perf[sector] = pct

    best_sector  = max(sector_perf, key=sector_perf.get) if sector_perf else "\u2014"
    worst_sector = min(sector_perf, key=sector_perf.get) if sector_perf else "\u2014"

    # Confidence history near miss rising stocks
    rising_stocks = []
    try:
        if os.path.exists(CONF_HISTORY_FILE):
            with open(CONF_HISTORY_FILE, "r") as f:
                conf_hist = json.load(f)
            for sym, data in conf_hist.items():
                confs = data.get("confs", [])
                if len(confs) >= 3 and confs[-1] > confs[-3] + 3:
                    rising_stocks.append((sym, confs[-3], confs[-1]))
            rising_stocks.sort(key=lambda x: x[2] - x[1], reverse=True)
    except Exception:
        pass

    # Build message
    lines = []
    lines.append("=" * 40)
    lines.append("\U0001f4c5 WEEKLY SUMMARY")
    lines.append(f"   {week_start} \u2014 {week_end}")
    lines.append("=" * 40)
    lines.append("")

    # Market
    nifty_icon = "\U0001f7e2" if nifty_pct > 0 else "\U0001f534"
    lines.append("\U0001f4ca MARKET THIS WEEK")
    lines.append(f"  NIFTY: {nifty_icon} {nifty_pct:+.2f}%")
    if sector_perf:
        lines.append(
            f"  Best Sector:  {best_sector} {sector_perf.get(best_sector, 0):+.2f}%"
        )
        lines.append(
            f"  Worst Sector: {worst_sector} {sector_perf.get(worst_sector, 0):+.2f}%"
        )
    lines.append("")

    # Signals summary
    lines.append("\U0001f4cb SIGNALS THIS WEEK")
    lines.append(f"  Active Positions:  {len(active)}")
    lines.append(f"  Watching:          {len(watching)} stocks")
    lines.append(f"  Completed (total): {len(completed)}")
    if completed:
        lines.append(
            f"  Win Rate: {perf.get('win_rate', 0):.0f}% | "
            f"Avg W {perf.get('avg_win', 0):+.1f}% | "
            f"Avg L {perf.get('avg_loss', 0):+.1f}%"
        )
    lines.append("")

    # Active position summary
    if active:
        lines.append("\U0001f4bc ACTIVE POSITIONS")
        for pos in active:
            try:
                hist     = pos.get("pnl_history", [])
                pnl      = hist[-1]["pnl"] if hist else 0
                day_n    = pos.get("days_tracked", 0)
                pnl_icon = "\U0001f7e2" if pnl > 0 else ("\U0001f534" if pnl < 0 else "\u26aa")
                lines.append(
                    f"  {pnl_icon} {pos['symbol']} | Day {day_n}/15 | PnL {pnl:+.1f}%"
                )
            except Exception:
                lines.append(f"  {pos.get('symbol', '?')}")
        lines.append("")

    # Near miss with rising confidence
    if rising_stocks:
        lines.append("\U0001f4c8 RISING CONFIDENCE THIS WEEK")
        for sym, c_old, c_new in rising_stocks[:5]:
            lines.append(f"  \u2191\u2191 {sym}: {c_old:.0f} \u2192 {c_new:.0f} (+{c_new-c_old:.0f})")
        lines.append("")

    # Near miss status
    near_miss = [w for w in watching if w.get("tier") == "NEAR_MISS"]
    if near_miss:
        lines.append("\U0001f441 NEAR MISS STATUS")
        for w in near_miss[:5]:
            try:
                entry     = float(w.get("entry", 0) or 0)
                conf_gap  = float(w.get("conf_gap", 0) or 0)
                direction = w.get("direction", "\u2014")
                lines.append(
                    f"  {w['symbol']} | Gap {conf_gap:.1f} | {direction}"
                )
            except Exception:
                lines.append(f"  {w.get('symbol', '?')}")
        lines.append("")

    # Next week outlook
    lines.append("\U0001f52d NEXT WEEK OUTLOOK")
    if nifty_pct < -1:
        lines.append("  Market pulled back \u2014 watch for recovery or further weakness")
    elif nifty_pct > 1:
        lines.append("  Market up this week \u2014 momentum may continue, watch for overextension")
    else:
        lines.append("  Market flat \u2014 waiting for directional catalyst")
    lines.append("  Check evening reports for daily signals")
    lines.append("")
    lines.append("\u2500" * 40)
    lines.append("\u26a0\ufe0f  Weekly recap only. Not a trading signal.")
    lines.append("=" * 40)

    message = "\n".join(lines)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram not configured \u2014 printing to stdout")
        print(message)
        return

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=15
        )
        if r.status_code == 200:
            print("[INFO] Weekly summary sent")
        else:
            print(f"[WARN] Send failed: {r.text[:100]}")
    except Exception as e:
        print(f"[WARN] Send error: {e}")


if __name__ == "__main__":
    run_weekly_summary()
