"""
backtest_diff_notify.py — Phase G4 safeguard.

Compares a staged backtest recommendation against the live one and sends a
concise Telegram alert with the proposed change. NEVER overwrites the live
file — user must promote manually.

Usage:
    python backtest_diff_notify.py \\
        --live threshold_recommendation.txt \\
        --staged backtest_staging/threshold_recommendation.txt

Reads BUY_BOT_TOKEN / BUY_CHAT_ID (same channel as morning_check /
intraday_monitor) so the alert lands where you already look.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BUY_BOT_TOKEN = os.getenv("BUY_BOT_TOKEN", "")
BUY_CHAT_ID   = os.getenv("BUY_CHAT_ID", "")
TELEGRAM_MAX  = 4000


def _extract_threshold(path: str) -> tuple[int | None, str]:
    """Return (recommended_min_conf, raw_text) from a recommendation file.

    Falls back to (None, '') on any parse failure — the caller decides
    whether that's a hard fail.
    """
    if not path or not os.path.exists(path):
        return (None, "")
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        return (None, "")
    # "Recommended min_confidence (60% win rate threshold): 78"
    m = re.search(r"Recommended\s+min_confidence[^:]*:\s*(\d+)", raw)
    if m:
        try:
            return (int(m.group(1)), raw)
        except ValueError:
            pass
    return (None, raw)


def _extract_buckets(text: str) -> dict:
    """Return {bucket_int: (winrate_pct, total)} from the recommendation body."""
    buckets = {}
    # Rows look like:  "      78 |    142 |   61.2% |    18.3%"
    for line in text.splitlines():
        m = re.match(r"\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d.]+)%\s*\|\s*[\d.]+%", line)
        if m:
            b  = int(m.group(1))
            tot = int(m.group(2))
            wr = float(m.group(3))
            buckets[b] = (wr, tot)
    return buckets


def _send(message: str) -> None:
    if not BUY_BOT_TOKEN or not BUY_CHAT_ID:
        print("[INFO] BUY channel not configured — printing instead:")
        print(message)
        return
    # Simple length cap; backtest diffs are small.
    if len(message) > TELEGRAM_MAX:
        message = message[:TELEGRAM_MAX - 20] + "\n… (truncated)"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BUY_BOT_TOKEN}/sendMessage",
            json={"chat_id": BUY_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"[WARN] Telegram send failed: {resp.status_code} {resp.text[:120]}")
    except Exception as e:
        print(f"[WARN] _send failed: {e}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live",   default="threshold_recommendation.txt")
    parser.add_argument("--staged", default="backtest_staging/threshold_recommendation.txt")
    args = parser.parse_args()

    live_th, live_txt     = _extract_threshold(args.live)
    staged_th, staged_txt = _extract_threshold(args.staged)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M IST")

    if staged_th is None:
        _send(
            f"⚠ <b>Backtest recalibration failed</b>\n{ts}\n\n"
            f"No threshold parsed from staging file:\n<code>{args.staged}</code>\n\n"
            f"No changes recommended. Live config untouched."
        )
        return 1

    live_bkts   = _extract_buckets(live_txt)   if live_txt   else {}
    staged_bkts = _extract_buckets(staged_txt) if staged_txt else {}

    lines = [
        "🧪 <b>Quarterly Backtest Recalibration</b>",
        f"Run: {ts}",
        "─" * 32,
        f"Current live threshold: <b>{live_th if live_th is not None else 'not found'}</b>",
        f"Proposed threshold:     <b>{staged_th}</b>",
    ]

    if live_th is None:
        lines.append("\n➡ No live file yet — safe to promote.")
    elif staged_th == live_th:
        lines.append("\n✓ No change recommended — threshold is stable.")
    else:
        delta = staged_th - live_th
        arrow = "🔻 LOOSER" if delta < 0 else "🔺 STRICTER"
        lines.append(f"\n{arrow} by {abs(delta)} points")

        # Show the win-rate at the two threshold buckets side-by-side
        def _fmt(bkts, k):
            if k in bkts:
                wr, tot = bkts[k]
                return f"WR {wr:.1f}% · n={tot}"
            return "n/a"
        lines.append("")
        lines.append(f"@{live_th}  live   → {_fmt(staged_bkts, live_th)}")
        lines.append(f"@{staged_th}  new    → {_fmt(staged_bkts, staged_th)}")

    lines.append("")
    lines.append("─" * 32)
    lines.append("<b>Action required (manual — no auto-apply)</b>")
    lines.append("1. Review <code>backtest_staging/threshold_recommendation.txt</code>")
    lines.append("2. Sanity check bucket win rates vs prior run")
    lines.append("3. If confident, copy staged → live and commit")
    lines.append("")
    lines.append("💡 Never accept the proposal blindly — check that ")
    lines.append("   sample sizes (n) at the new bucket are ≥ 40.")

    _send("\n".join(lines))
    print(f"[INFO] backtest_diff_notify: live={live_th} staged={staged_th}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
