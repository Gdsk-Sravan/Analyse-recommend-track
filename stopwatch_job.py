"""
stopwatch_job.py — Intraday Stop Loss Monitor (FEATURE 4)
Run every 30 min during market hours via GitHub Actions.
Checks ONLY active portfolio positions — very fast (<10 seconds).
Sends Telegram alert if any position approaches or hits stop.
"""
import os
import json
import requests
import yfinance as yf
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TRACKER_FILE       = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")
ALERT_FILE         = "stop_alerts_sent.json"  # prevent duplicate alerts

STOP_WARNING_PCT  = 3.0  # alert when within 3% of stop
STOP_CRITICAL_PCT = 1.0  # critical alert when within 1% of stop

# Market-open run: 9:15 AM IST = 3:45 UTC → cron '45 3 * * 1-5'
# Market-close run: 3:30 PM IST = 10:00 UTC → cron '00 10 * * 1-5'
_OPEN_CRON_UTC  = (3, 45)   # (hour, minute) UTC
_CLOSE_CRON_UTC = (10, 0)
_HEARTBEAT_WINDOW_MIN = 12  # minutes either side to match the cron slot


def _get_live_price(sym: str) -> float:
    """Fetch current price. Returns 0.0 on failure."""
    try:
        ticker = yf.Ticker(sym)
        info   = ticker.fast_info
        cur    = float(info.last_price) if hasattr(info, "last_price") and info.last_price else 0.0
        if cur > 0:
            return cur
        df = yf.download(sym, period="1d", interval="5m",
                         progress=False, auto_adjust=True, multi_level_index=False)
        if df is not None and len(df) > 0:
            return float(df["Close"].iloc[-1])
    except Exception as e:
        print(f"[WARN] Price fetch failed for {sym}: {e}")
    return 0.0


def _send_alert(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[ALERT] {text}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=12
        )
    except Exception as e:
        print(f"[WARN] Alert send failed: {e}")


def run_stopwatch():
    now = datetime.now()
    print(f"=== STOPWATCH: {now.strftime('%H:%M')} ===")

    # Load tracker
    if not os.path.exists(TRACKER_FILE):
        print("[INFO] No tracker file \u2014 nothing to watch")
        return

    try:
        with open(TRACKER_FILE, "r") as f:
            tracker = json.load(f)
    except Exception as e:
        print(f"[WARN] Tracker load failed: {e}")
        return

    active = tracker.get("buys", [])
    if not active:
        print("[INFO] No active positions to watch")
        return

    # Load sent alerts to prevent duplicates
    alerts_sent = {}
    try:
        if os.path.exists(ALERT_FILE):
            with open(ALERT_FILE, "r") as f:
                alerts_sent = json.load(f)
    except Exception:
        pass

    today_str = now.strftime("%Y-%m-%d")
    alerts    = []

    for pos in active:
        try:
            sym   = pos["symbol"]
            stop  = float(pos.get("stop", 0) or 0)
            entry = float(pos.get("entry", 0) or 0)
            t1    = float(pos.get("target1", 0) or 0)
            t2    = float(pos.get("target2", 0) or 0)

            cur = _get_live_price(sym)
            if cur <= 0 or stop <= 0:
                continue

            pnl_pct   = round((cur - entry) / entry * 100, 2) if entry > 0 else 0.0
            dist_stop = round((cur - stop) / cur * 100, 1)

            alert_key = f"{sym}_{today_str}"

            # STOP HIT
            if cur <= stop:
                if alerts_sent.get(alert_key) != "STOP_HIT":
                    alerts.append({
                        "key": alert_key, "level": "STOP_HIT",
                        "message": (
                            f"\U0001f534 STOP HIT \u2014 {sym}\n"
                            f"   Current Rs{cur:.2f} \u2264 Stop Rs{stop:.2f}\n"
                            f"   PnL: {pnl_pct:+.1f}%\n"
                            f"   ACTION: Exit position immediately"
                        )
                    })

            # CRITICAL \u2014 within 1% of stop
            elif dist_stop <= STOP_CRITICAL_PCT:
                if alerts_sent.get(alert_key) not in ("STOP_HIT", "CRITICAL"):
                    alerts.append({
                        "key": alert_key, "level": "CRITICAL",
                        "message": (
                            f"\u26a0\ufe0f  CRITICAL \u2014 {sym}\n"
                            f"   Rs{cur:.2f} \u2014 only {dist_stop:.1f}% above stop Rs{stop:.2f}\n"
                            f"   PnL: {pnl_pct:+.1f}% | Watch closely"
                        )
                    })

            # WARNING \u2014 within 3% of stop
            elif dist_stop <= STOP_WARNING_PCT:
                if alerts_sent.get(alert_key) not in ("STOP_HIT", "CRITICAL", "WARNING"):
                    alerts.append({
                        "key": alert_key, "level": "WARNING",
                        "message": (
                            f"\U0001f7e0 STOP WARNING \u2014 {sym}\n"
                            f"   Rs{cur:.2f} \u2014 {dist_stop:.1f}% above stop Rs{stop:.2f}\n"
                            f"   PnL: {pnl_pct:+.1f}%"
                        )
                    })

            # TARGET 1 HIT
            if t1 > 0 and cur >= t1:
                t1_key = f"{sym}_{today_str}_T1"
                if alerts_sent.get(t1_key) != "T1_HIT":
                    alerts.append({
                        "key": t1_key, "level": "T1_HIT",
                        "message": (
                            f"\U0001f3af TARGET 1 HIT \u2014 {sym}\n"
                            f"   Rs{cur:.2f} \u2265 T1 Rs{t1:.2f}\n"
                            f"   PnL: {pnl_pct:+.1f}%\n"
                            f"   Consider: book 50%, trail stop to entry"
                        )
                    })

        except Exception as e:
            print(f"[WARN] Check failed for {pos.get('symbol', '?')}: {e}")

    # Send alerts and update state
    for alert in alerts:
        _send_alert(alert["message"])
        alerts_sent[alert["key"]] = alert["level"]
        print(f"[INFO] Alert sent: {alert['level']} for {alert['key']}")

    # Save alert state
    try:
        with open(ALERT_FILE, "w") as f:
            json.dump(alerts_sent, f, indent=2)
    except Exception as e:
        print(f"[WARN] Alert state save failed: {e}")

    if not alerts:
        print(f"[INFO] All {len(active)} positions safe at {now.strftime('%H:%M')}")

    # ── Heartbeat messages ──
    # Market open (9:15 AM IST ≈ 3:45 UTC): confirm monitoring started
    # Market close (3:30 PM IST ≈ 10:00 UTC): day summary
    try:
        now_utc_h = now.utctimetuple().tm_hour
        now_utc_m = now.utctimetuple().tm_min

        def _near_cron(h, m):
            total_now  = now_utc_h * 60 + now_utc_m
            total_cron = h * 60 + m
            return abs(total_now - total_cron) <= _HEARTBEAT_WINDOW_MIN

        if _near_cron(*_OPEN_CRON_UTC):
            # Market open heartbeat — always send
            pos_lines = []
            for pos in active:
                sym  = pos.get("symbol", "?")
                stop = float(pos.get("stop", 0) or 0)
                t1   = float(pos.get("target1", 0) or 0)
                t2   = float(pos.get("target2", 0) or 0)
                pos_lines.append(f"  {sym} | Stop Rs{stop:.0f} | T1 Rs{t1:.0f} | T2 Rs{t2:.0f}")
            body = "\n".join(pos_lines) if pos_lines else "  (no positions)"
            _send_alert(
                f"\U0001f7e2 STOPWATCH STARTED — {now.strftime('%d %b %Y')}\n"
                f"Monitoring {len(active)} position(s) every 30 min until 3:30 PM IST\n"
                f"{body}"
            )
            print("[INFO] Market-open heartbeat sent")

        elif _near_cron(*_CLOSE_CRON_UTC):
            # Market close summary
            day_alerts = [k for k, v in alerts_sent.items()
                          if today_str in k and v in ("STOP_HIT", "CRITICAL", "WARNING", "T1_HIT")]
            if day_alerts:
                alert_summary = "\n".join(f"  {k}: {alerts_sent[k]}" for k in day_alerts)
                _send_alert(
                    f"\U0001f534 STOPWATCH CLOSED — {now.strftime('%d %b %Y')}\n"
                    f"Alerts today:\n{alert_summary}"
                )
            else:
                _send_alert(
                    f"\U0001f7e2 STOPWATCH CLOSED — {now.strftime('%d %b %Y')}\n"
                    f"All {len(active)} position(s) safe today. No stop alerts fired.\n"
                    f"Market closes in a few minutes — review open positions."
                )
            print("[INFO] Market-close summary sent")
    except Exception as e:
        print(f"[WARN] Heartbeat send failed: {e}")


if __name__ == "__main__":
    run_stopwatch()
