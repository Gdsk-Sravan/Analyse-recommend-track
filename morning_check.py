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
# Phase C7 kill-switch inputs (must mirror main.py exactly)
TRADE_TRACKER_V2_FILE = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")
# Capital — read CAPITAL first (matches main.py), fall back to PORTFOLIO_CAPITAL
# for backward compatibility, then TOTAL_CAPITAL as last resort.
PORTFOLIO_CAPITAL     = float(
    os.getenv("CAPITAL",
              os.getenv("PORTFOLIO_CAPITAL",
                        os.getenv("TOTAL_CAPITAL", "500000")))
)
KS_MAX_CONSEC_LOSSES  = int(os.getenv("KS_MAX_CONSEC_LOSSES", "3"))
KS_DAY_STOP_PCT       = float(os.getenv("KS_DAY_STOP_PCT", "2.0"))
KS_WEEK_STOP_PCT      = float(os.getenv("KS_WEEK_STOP_PCT", "3.0"))
KS_DD_HALVE_PCT       = float(os.getenv("KS_DD_HALVE_PCT", "5.0"))
KS_DD_HALT_PCT        = float(os.getenv("KS_DD_HALT_PCT", "10.0"))
# Phase 2 #31 — mirrors main.py: consecutive-loss counter only considers
# losses closed within the last KS_LOSS_WINDOW_DAYS calendar days. Old
# losses no longer haunt the kill switch forever. MUST match main.py's
# compute_kill_switch_state (see main.py ~L5820).
KS_LOSS_WINDOW_DAYS   = int(os.getenv("KS_LOSS_WINDOW_DAYS", "7"))
TELEGRAM_MAX   = 4000


# ─── Phase C7d: Kill-Switch Awareness ────────────────────────────────────────
# Standalone kill-switch computation. Mirrors main.py's compute_kill_switch_state
# logic exactly so morning_check produces the same buys_paused verdict.
# CRITICAL: if kill-switch is HALTED, morning_check MUST NOT tell the user to
# enter any BUY signal — regardless of gap/RR quality — because main.py's Gate
# 5b would have already rejected them if the state was known at evening scan.
def _compute_morning_kill_switch() -> dict:
    """Return {'buys_paused': bool, 'reason': str, 'sizing_multiplier': float}.

    Uses the same rules as main.py's compute_kill_switch_state:
      - 3+ consecutive losses          → HALT
      - Day P&L ≤ -2% of capital       → HALT (rest of day)
      - Week P&L ≤ -3% of capital      → HALT (rest of week)
      - Drawdown from peak ≥ 10%       → HALT
      - Drawdown from peak 5% – 10%    → HALVE position size (still allow)
    """
    if os.getenv("FRESH_START", "false").lower() == "true":
        return {"buys_paused": False, "reason": "fresh_start", "sizing_multiplier": 1.0}

    if not os.path.exists(TRADE_TRACKER_V2_FILE):
        return {"buys_paused": False, "reason": "no_tracker", "sizing_multiplier": 1.0}

    try:
        with open(TRADE_TRACKER_V2_FILE, "r") as f:
            tracker = json.load(f)
    except Exception:
        return {"buys_paused": False, "reason": "tracker_unreadable", "sizing_multiplier": 1.0}

    completed = tracker.get("completed", []) or []
    if not completed:
        return {"buys_paused": False, "reason": "no_completed_trades", "sizing_multiplier": 1.0}

    # Sort by close date. V2 tracker records the date under one of
    # stop_hit_date / t2_hit_date / t1_hit_date / expired_date. V1 (legacy)
    # used close_date. We walk this priority so both shapes work.
    def _close_date(p):
        for k in ("stop_hit_date", "t2_hit_date", "t1_hit_date",
                  "expired_date", "close_date"):
            v = p.get(k)
            if v:
                try:
                    return datetime.datetime.fromisoformat(str(v)[:10])
                except Exception:
                    continue
        # Fallback: rec_date + days_tracked
        try:
            rd = datetime.datetime.fromisoformat(str(p.get("rec_date", ""))[:10])
            return rd + datetime.timedelta(days=int(p.get("days_tracked", 0) or 0))
        except Exception:
            return datetime.datetime(1970, 1, 1)

    # V2 writes 'final_pnl' (raw), V1 wrote 'final_pnl_pct' / 'blended_pnl_pct'.
    # Walk v2-first, then v1 legacy names so both shapes work.
    def _pnl(pos):
        return float(
            pos.get("final_pnl",
                    pos.get("final_pnl_pct",
                            pos.get("blended_pnl_pct", 0))) or 0
        )

    completed_sorted = sorted(completed, key=_close_date)

    # 1. Consecutive losses (walk back from most recent)
    # Phase 2 #31: only count losses closed within the last
    # KS_LOSS_WINDOW_DAYS calendar days. Losses older than the cutoff
    # break the streak. MUST mirror main.py compute_kill_switch_state
    # so evening scan and morning check produce identical verdicts.
    _ks_cutoff = datetime.datetime.combine(
        datetime.date.today() - datetime.timedelta(days=KS_LOSS_WINDOW_DAYS),
        datetime.time.min,
    )
    consec_losses = 0
    for pos in reversed(completed_sorted):
        _dt = _close_date(pos)
        if _dt < _ks_cutoff:
            break  # too old — streak ends
        if _pnl(pos) < 0:
            consec_losses += 1
        else:
            break
    if consec_losses >= KS_MAX_CONSEC_LOSSES:
        return {"buys_paused": True,
                "reason": f"{consec_losses} consecutive losses",
                "sizing_multiplier": 0.0}

    # 2. Day P&L (positions closed today)
    today = datetime.date.today()
    day_pnl_rs = 0.0
    for pos in completed_sorted:
        cd = _close_date(pos).date()
        if cd == today:
            pnl_pct = _pnl(pos)
            entry = float(pos.get("entry", 0) or 0)
            # Approximate ₹ P&L using position size (assume 25% of capital max, or entry * qty)
            # Since we don't have qty here, use % of capital as proxy
            day_pnl_rs += pnl_pct / 100.0 * PORTFOLIO_CAPITAL * 0.10  # 10% notional per position
    day_pnl_pct = day_pnl_rs / PORTFOLIO_CAPITAL * 100.0 if PORTFOLIO_CAPITAL > 0 else 0.0
    if day_pnl_pct <= -KS_DAY_STOP_PCT:
        return {"buys_paused": True,
                "reason": f"Day P&L {day_pnl_pct:.2f}% ≤ -{KS_DAY_STOP_PCT}%",
                "sizing_multiplier": 0.0}

    # 3. Week P&L (last 7 days)
    week_ago = today - datetime.timedelta(days=7)
    week_pnl_rs = 0.0
    for pos in completed_sorted:
        cd = _close_date(pos).date()
        if cd >= week_ago:
            pnl_pct = _pnl(pos)
            week_pnl_rs += pnl_pct / 100.0 * PORTFOLIO_CAPITAL * 0.10
    week_pnl_pct = week_pnl_rs / PORTFOLIO_CAPITAL * 100.0 if PORTFOLIO_CAPITAL > 0 else 0.0
    if week_pnl_pct <= -KS_WEEK_STOP_PCT:
        return {"buys_paused": True,
                "reason": f"Week P&L {week_pnl_pct:.2f}% ≤ -{KS_WEEK_STOP_PCT}%",
                "sizing_multiplier": 0.0}

    # 4. Drawdown from peak (cumulative P&L)
    cum_pnl = 0.0
    peak = 0.0
    for pos in completed_sorted:
        pnl_pct = _pnl(pos)
        cum_pnl += pnl_pct / 100.0 * PORTFOLIO_CAPITAL * 0.10
        if cum_pnl > peak:
            peak = cum_pnl
    dd_rs = peak - cum_pnl  # current drawdown in ₹
    dd_pct = dd_rs / PORTFOLIO_CAPITAL * 100.0 if PORTFOLIO_CAPITAL > 0 else 0.0
    if dd_pct >= KS_DD_HALT_PCT:
        return {"buys_paused": True,
                "reason": f"Drawdown {dd_pct:.2f}% ≥ {KS_DD_HALT_PCT}%",
                "sizing_multiplier": 0.0}
    if dd_pct >= KS_DD_HALVE_PCT:
        return {"buys_paused": False,
                "reason": f"Drawdown {dd_pct:.2f}% — sizing halved",
                "sizing_multiplier": 0.5}

    return {"buys_paused": False, "reason": "ok", "sizing_multiplier": 1.0}


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
    # Phase C7c: FRESH_START wipes tracker state for one run
    if os.getenv("FRESH_START", "false").lower() == "true":
        print("[FRESH_START] morning_check: ignoring old tracker.json — nothing to check yet")
        return []
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

    # Phase C7d: Kill-switch pre-flight check — if the equity curve is bleeding
    # since evening scan, HALT all new entries regardless of gap/RR quality.
    ks = _compute_morning_kill_switch()
    if ks.get("buys_paused"):
        halt_msg = (
            f"⛔ <b>KILL SWITCH ACTIVE — {now_str}</b>\n"
            f"<b>Reason:</b> {ks.get('reason', 'unknown')}\n\n"
            f"🚨 <b>DO NOT ENTER ANY BUY SIGNAL TODAY</b>\n"
            f"The portfolio kill switch has tripped. Review the recent losses\n"
            f"before resuming new entries. Existing positions unaffected —\n"
            f"stop-losses and targets still apply.\n\n"
            f"<i>Kill switch will auto-reset once conditions improve.</i>"
        )
        _send(halt_msg)
        print(f"[KILL_SWITCH] {ks.get('reason')} — skipping all entries")
        return

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
    # Phase C7d: warn if sizing halved (drawdown 5-10%)
    if ks.get("sizing_multiplier", 1.0) < 1.0:
        lines.append(
            f"⚠ <b>REDUCED SIZING</b> — {ks.get('reason')}. "
            f"Cut planned position size by {int((1.0 - ks['sizing_multiplier']) * 100)}%."
        )
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
