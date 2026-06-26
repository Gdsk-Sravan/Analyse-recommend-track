"""
morning_check.py — 9:15 AM Gap-Open Decision Checker

Run this every trading morning at 9:20 AM IST (5 mins after open).
It fetches real opening prices for today's BUY signals and sends
ENTER / SKIP / WAIT / DEAD verdicts directly to your BUY Telegram channel.

Schedule in GitHub Actions (runs at 3:50 UTC = 9:20 AM IST Mon-Fri):
    - cron: "50 3 * * 1-5"

Or run manually:
    python morning_check.py

Reads:  tracker.json  (today's BUY signals added by last night's main.py)
Sends:  BUY_BOT_TOKEN + BUY_CHAT_ID (same channel as evening BUY signals)
"""

import os
import json
import datetime
import time

import requests
import yfinance as yf

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Config ───────────────────────────────────────────────────────────────────
BUY_BOT_TOKEN  = os.getenv("BUY_BOT_TOKEN", "")
BUY_CHAT_ID    = os.getenv("BUY_CHAT_ID", "")
TRACKER_FILE   = os.getenv("TRACKER_FILE", "tracker.json")
TELEGRAM_MAX   = 4000


# ─── Helpers ──────────────────────────────────────────────────────────────────

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
                print(f"[WARN] Telegram send failed: {resp.status_code} {resp.text[:80]}")
            time.sleep(0.3)
        except Exception as e:
            print(f"[WARN] _send failed: {e}")


def _load_tracker() -> list:
    try:
        if os.path.exists(TRACKER_FILE):
            with open(TRACKER_FILE) as f:
                return json.load(f)
    except Exception as e:
        print(f"[WARN] load_tracker: {e}")
    return []


def _fetch_open_price(symbol: str) -> float:
    """
    Fetches today's opening price using a 1-minute yfinance download.
    Returns 0.0 if market not yet open or data unavailable.
    """
    try:
        df = yf.download(symbol, period="1d", interval="1m",
                         progress=False, auto_adjust=True)
        if df is None or len(df) == 0:
            return 0.0
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        # First candle of the day = 9:15 AM open
        open_px = float(df["Open"].squeeze().iloc[0])
        return round(open_px, 2)
    except Exception as e:
        print(f"[WARN] fetch_open_price {symbol}: {e}")
        return 0.0


def check_gap_validity(signal_entry: float, signal_stop: float,
                       signal_target1: float, min_rr: float,
                       open_price: float) -> dict:
    """
    Standalone copy of main.py's check_gap_validity.
    Determines whether to ENTER, WAIT_PULLBACK, or VOID the signal.
    """
    try:
        if signal_entry <= 0 or signal_stop <= 0 or signal_target1 <= 0:
            return {"action": "VOID", "reason": "Invalid signal data",
                    "gap_pct": 0.0, "adjusted_rr": 0.0, "max_valid_entry": 0.0}

        # Max valid entry: solve (t1 - max_e) / (max_e - stop) = min_rr
        max_valid_entry = round(
            (signal_target1 + min_rr * signal_stop) / (1 + min_rr), 2
        )

        if open_price <= 0:
            return {"action": "PENDING", "reason": "No open price yet",
                    "gap_pct": 0.0, "adjusted_rr": 0.0,
                    "max_valid_entry": max_valid_entry}

        gap_pct    = round((open_price - signal_entry) / signal_entry * 100, 2)
        rr_at_open = round(
            (signal_target1 - open_price) / (open_price - signal_stop), 2
        ) if open_price > signal_stop else 0.0

        # Opened at or below stop — signal dead
        if open_price <= signal_stop:
            return {"action": "VOID",
                    "reason": f"Opened at/below stop (Rs{signal_stop:.2f}). Signal dead.",
                    "gap_pct": gap_pct, "adjusted_rr": 0.0,
                    "max_valid_entry": max_valid_entry}

        # Gap up > 5%
        if gap_pct > 5.0:
            return {"action": "VOID",
                    "reason": f"Gap +{gap_pct:.1f}% — too extended. R/R={rr_at_open:.2f}x. Do NOT chase.",
                    "gap_pct": gap_pct, "adjusted_rr": rr_at_open,
                    "max_valid_entry": max_valid_entry}

        # Gap up 3–5% — wait for pullback
        if gap_pct > 3.0:
            return {"action": "WAIT_PULLBACK",
                    "reason": f"Gap +{gap_pct:.1f}%. Wait for pullback to Rs{max_valid_entry:.2f}.",
                    "gap_pct": gap_pct, "adjusted_rr": rr_at_open,
                    "max_valid_entry": max_valid_entry}

        # Gap up 1.5–3% — recalculate R/R
        if gap_pct > 1.5:
            if rr_at_open >= min_rr:
                return {"action": "ENTER",
                        "reason": f"Gap +{gap_pct:.1f}%. R/R {rr_at_open:.2f}x ≥ min {min_rr:.1f}x. Enter at open.",
                        "gap_pct": gap_pct, "adjusted_rr": rr_at_open,
                        "max_valid_entry": max_valid_entry}
            else:
                return {"action": "WAIT_PULLBACK",
                        "reason": f"Gap +{gap_pct:.1f}%. R/R dropped to {rr_at_open:.2f}x (min {min_rr:.1f}x). "
                                  f"Wait for Rs{max_valid_entry:.2f}.",
                        "gap_pct": gap_pct, "adjusted_rr": rr_at_open,
                        "max_valid_entry": max_valid_entry}

        # Gap ≤ 1.5% (including small gaps down that didn't breach stop)
        return {"action": "ENTER",
                "reason": f"Gap {gap_pct:+.1f}%. R/R {rr_at_open:.2f}x. Enter at market open.",
                "gap_pct": gap_pct, "adjusted_rr": rr_at_open,
                "max_valid_entry": max_valid_entry}

    except Exception as e:
        return {"action": "VOID", "reason": f"Error: {e}",
                "gap_pct": 0.0, "adjusted_rr": 0.0, "max_valid_entry": 0.0}


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_morning_check():
    today_str = datetime.date.today().isoformat()
    now_str   = datetime.datetime.now().strftime("%d %b %Y %H:%M IST")

    print(f"Morning check — {now_str}")

    # Load tracker and find today's open BUY signals
    entries = _load_tracker()
    todays_buys = [
        e for e in entries
        if e.get("type") == "BUY"
        and e.get("status") == "OPEN"
        and e.get("suggested_date") == today_str
    ]

    if not todays_buys:
        msg = (
            f"📋 <b>MORNING CHECK — {now_str}</b>\n"
            f"No new BUY signals from last night to act on today.\n"
            f"Check tracker for open positions."
        )
        _send(msg)
        print("No new BUY signals today.")
        return

    print(f"Found {len(todays_buys)} BUY signal(s). Fetching opening prices...")

    lines = []
    lines.append(f"⏰ <b>MORNING CHECK — {now_str}</b>")
    lines.append(f"Checking {len(todays_buys)} signal(s) from last evening")
    lines.append("─" * 38)

    enter_count = wait_count = void_count = 0

    for e in todays_buys:
        sym         = e.get("symbol", "?")
        entry_price = float(e.get("entry", 0) or e.get("suggested_price", 0))
        stop_price  = float(e.get("stop", 0))
        target1     = float(e.get("target1", 0))
        conf        = float(e.get("conf", 0))
        tq          = float(e.get("tq", 0))
        # Use R/R to get min_rr — derive from stored entry/stop/t1
        if entry_price > stop_price > 0 and target1 > entry_price:
            signal_rr = round((target1 - entry_price) / (entry_price - stop_price), 2)
        else:
            signal_rr = 1.8
        # Signal min_rr — use 80% of signal's own R/R as threshold
        min_rr_thresh = max(1.5, round(signal_rr * 0.80, 1))

        print(f"  Fetching {sym}...")
        open_px = _fetch_open_price(sym)
        time.sleep(0.3)

        result   = check_gap_validity(entry_price, stop_price, target1,
                                      min_rr_thresh, open_px)
        action   = result["action"]
        reason   = result["reason"]
        gap_pct  = result["gap_pct"]
        adj_rr   = result["adjusted_rr"]
        max_ent  = result["max_valid_entry"]

        # Action emoji
        if action == "ENTER":
            emoji = "✅ ENTER"
            enter_count += 1
        elif action == "WAIT_PULLBACK":
            emoji = "⏳ WAIT / PULLBACK"
            wait_count += 1
        elif action == "VOID":
            emoji = "❌ SKIP"
            void_count += 1
        else:
            emoji = "⏳ PENDING"

        lines.append(f"\n<b>{sym}</b>")
        lines.append(f"{emoji}")
        lines.append(f"Last night entry: Rs{entry_price:.2f} | Open: Rs{open_px:.2f} ({gap_pct:+.1f}%)")
        lines.append(f"Stop: Rs{stop_price:.2f} | T1: Rs{target1:.2f} | Signal R/R: {signal_rr:.2f}x")

        if action == "ENTER":
            lines.append(f"Current R/R: {adj_rr:.2f}x ✅")
            lines.append(f"→ {reason}")

        elif action == "WAIT_PULLBACK":
            lines.append(f"Current R/R at open: {adj_rr:.2f}x")
            lines.append(f"→ {reason}")
            lines.append(f"→ Max entry: Rs{max_ent:.2f}")
            lines.append(f"→ Set price alert at Rs{max_ent:.2f}")
            lines.append(f"→ If no pullback by 10:30 AM → move on")

        elif action == "VOID":
            lines.append(f"→ {reason}")
            if gap_pct < 0 and open_px <= stop_price:
                lines.append(f"→ Gapped below stop — no valid entry exists")
            else:
                lines.append(f"→ R/R at open: {adj_rr:.2f}x — not worth the risk")

        lines.append(f"Conf: {conf:.1f} | TQ: {tq:.1f}")
        lines.append("─" * 38)

    # Summary line
    lines.append(f"\n<b>Summary:</b> {enter_count} ENTER | {wait_count} WAIT | {void_count} SKIP")

    if enter_count > 0:
        lines.append("\n<b>Action for ENTER signals:</b>")
        lines.append("Place limit/market order in first 5 mins.")
        lines.append("If fill > max valid entry → cancel and don't chase.")

    if wait_count > 0:
        lines.append("\n<b>Action for WAIT signals:</b>")
        lines.append("Set price alert at max entry shown above.")
        lines.append("If price pulls back → re-check volume before entering.")
        lines.append("If no pullback by 10:30 AM → skip for today.")

    if void_count > 0 and enter_count == 0 and wait_count == 0:
        lines.append("\nAll signals voided. No trades today. Cash is a position.")

    message = "\n".join(lines)
    _send(message)
    print(f"Sent. ENTER={enter_count} WAIT={wait_count} SKIP={void_count}")


if __name__ == "__main__":
    run_morning_check()
