"""
premarket_job.py — NSE Pre-Market Brief (FEATURE 3)
Runs every trading day at 9:00 AM IST via GitHub Actions.
Sends a concise morning brief before market opens.
Takes < 30 seconds. Completely independent of main pipeline.
"""
import os
import json
import requests
import yfinance as yf
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TRACKER_FILE       = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")

PREMARKET_TICKERS = {
    "US_CLOSE":  "^GSPC",
    "NASDAQ":    "^IXIC",
    "DOW":       "^DJI",
    "CRUDE":     "CL=F",
    "GOLD":      "GC=F",
    "USDINR":    "USDINR=X",
    "VIX_US":    "^VIX",
}


def _fetch_signal(ticker: str) -> dict:
    try:
        df = yf.download(ticker, period="2d", interval="1d",
                         progress=False, auto_adjust=True, multi_level_index=False)
        if df is None or len(df) < 1:
            return {"price": 0, "pct": 0}
        last = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2]) if len(df) >= 2 else last
        pct  = round((last - prev) / prev * 100, 2) if prev > 0 else 0.0
        return {"price": last, "pct": pct}
    except Exception as e:
        print(f"[WARN] {ticker} fetch failed: {e}")
        return {"price": 0, "pct": 0}


def run_premarket():
    today = datetime.now().strftime("%b %d, %Y")
    print(f"=== PRE-MARKET BRIEF: {today} ===")

    # Fetch global signals
    signals = {}
    for name, ticker in PREMARKET_TICKERS.items():
        signals[name] = _fetch_signal(ticker)

    # Load tracker for active positions
    active_positions = []
    watching_stocks  = []
    try:
        if os.path.exists(TRACKER_FILE):
            with open(TRACKER_FILE, "r") as f:
                tracker = json.load(f)
            active_positions = tracker.get("buys", [])
            watching_stocks  = [
                w for w in tracker.get("watchlist", [])
                if w.get("tier") == "NEAR_MISS"
            ]
    except Exception as e:
        print(f"[WARN] Tracker load failed: {e}")

    # Build message
    lines = []
    lines.append("=" * 40)
    lines.append(f"\U0001f305 PRE-MARKET BRIEF \u2014 {today}")
    lines.append("=" * 40)
    lines.append("")

    # Global overnight
    us    = signals.get("US_CLOSE", {})
    nas   = signals.get("NASDAQ", {})
    dow   = signals.get("DOW", {})
    crude = signals.get("CRUDE", {})
    gold  = signals.get("GOLD", {})
    usdinr = signals.get("USDINR", {})
    vix    = signals.get("VIX_US", {})

    us_icon  = "\U0001f7e2" if us.get("pct", 0) > 0 else "\U0001f534"
    nas_icon = "\U0001f7e2" if nas.get("pct", 0) > 0 else "\U0001f534"

    lines.append("\U0001f30d OVERNIGHT GLOBAL")
    lines.append(
        f"  {us_icon} S&P500  {us.get('pct', 0):+.2f}% | "
        f"{nas_icon} Nasdaq {nas.get('pct', 0):+.2f}%"
    )
    lines.append(
        f"  DOW   {dow.get('pct', 0):+.2f}% | "
        f"VIX-US {vix.get('price', 0):.1f}"
    )
    lines.append(
        f"  Crude ${crude.get('price', 0):.1f} ({crude.get('pct', 0):+.2f}%) | "
        f"Gold ${gold.get('price', 0):.0f}"
    )
    lines.append(f"  USD/INR {usdinr.get('price', 0):.2f}")
    lines.append("")

    # Gap estimate
    us_pct = us.get("pct", 0)
    if us_pct > 0.5:
        gap_signal = f"\U0001f7e2 GAP UP likely (~+{us_pct * 0.4:.1f}% NIFTY)"
    elif us_pct < -0.5:
        gap_signal = f"\U0001f534 GAP DOWN likely (~{us_pct * 0.4:.1f}% NIFTY)"
    else:
        gap_signal = "\u26aa Flat open expected"
    lines.append(f"  Open Estimate: {gap_signal}")
    lines.append("")

    # Active positions to watch
    if active_positions:
        lines.append("\U0001f4c1 WATCH YOUR POSITIONS TODAY")
        for pos in active_positions:
            try:
                hist     = pos.get("pnl_history", [])
                cur_pnl  = hist[-1]["pnl"] if hist else 0
                cur_px   = hist[-1]["price"] if hist else pos.get("entry", 0)
                stop     = float(pos.get("stop", 0) or 0)
                dist_pct = round((cur_px - stop) / cur_px * 100, 1) if cur_px > 0 else 0
                pnl_icon = "\U0001f7e2" if cur_pnl > 0 else "\U0001f534"
                lines.append(
                    f"  {pnl_icon} {pos['symbol']} | Day {pos.get('days_tracked', 0)}/15 | "
                    f"PnL {cur_pnl:+.1f}%"
                )
                stop_note = "\u26a0\ufe0f  CLOSE TO STOP" if dist_pct < 3 else "\u2705 Safe"
                lines.append(f"    Stop Rs{stop:.1f} \u2014 {dist_pct:.1f}% away. {stop_note}")
            except Exception:
                lines.append(f"  {pos.get('symbol', '?')} (data error)")
        lines.append("")

    # Top near miss to watch
    if watching_stocks:
        lines.append("\U0001f441 NEAR MISS TO WATCH TODAY")
        for w in watching_stocks[:3]:
            try:
                entry    = float(w.get("entry", 0) or 0)
                conf_gap = float(w.get("conf_gap", 0) or 0)
                lines.append(
                    f"  {w['symbol']} | Entry Rs{entry:.1f} | "
                    f"Needs +{conf_gap:.1f} conf"
                )
                lines.append("    Watch for: volume surge above 20d avg at open")
            except Exception:
                lines.append(f"  {w.get('symbol', '?')}")
        lines.append("")

    lines.append("\u2500" * 40)
    lines.append("\u26a0\ufe0f  Pre-market only. Evening report at 7 PM.")
    lines.append("=" * 40)

    message = "\n".join(lines)

    # Send
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
            print("[INFO] Pre-market brief sent")
        else:
            print(f"[WARN] Telegram failed: {r.text[:100]}")
    except Exception as e:
        print(f"[WARN] Send failed: {e}")


if __name__ == "__main__":
    run_premarket()
