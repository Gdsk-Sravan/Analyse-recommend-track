#!/usr/bin/env python3
"""
notify_telegram.py — Phase W (watchdog, 2026-07-03)
─────────────────────────────────────────────────────────────────────────────
Standalone Telegram notifier. Used by workflow YAMLs to send:
  • Failure alerts (if: failure() steps)
  • Staleness warnings (tracker detects evening pipeline hasn't run)
  • Post-run one-liner summaries

Reads TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID from env. If either is missing,
prints a warning and exits 0 (never fails the workflow — notification is
best-effort). Message text comes from --text or stdin.

Usage:
    python scripts/notify_telegram.py --text "✅ Evening pipeline OK — 3 BUYs"
    echo "PIPELINE FAILED" | python scripts/notify_telegram.py --stdin --prefix "🚨"

Env:
    TELEGRAM_BOT_TOKEN   — bot token (falls back to TRACKER_BOT_TOKEN)
    TELEGRAM_CHAT_ID     — chat id  (falls back to TRACKER_CHAT_ID)
    NOTIFY_DRY_RUN=1     — print message to stdout, no HTTP call (for CI tests)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from urllib.parse import quote

try:
    import requests  # noqa: F401 — used inside main
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


TELEGRAM_MAX_CHARS = 4000  # actual limit is 4096; leave safety margin
MAX_RETRIES        = int(os.getenv("NOTIFY_RETRIES", "2"))
RETRY_BACKOFF_SEC  = float(os.getenv("NOTIFY_RETRY_BACKOFF", "1.5"))


def _resolve_creds() -> tuple[str, str]:
    """Return (token, chat_id) — prefer TELEGRAM_*, fall back to TRACKER_*."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat  = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token:
        token = os.getenv("TRACKER_BOT_TOKEN", "").strip()
    if not chat:
        chat = os.getenv("TRACKER_CHAT_ID", "").strip()
    return token, chat


def _chunk(text: str, limit: int = TELEGRAM_MAX_CHARS) -> list[str]:
    if len(text) <= limit:
        return [text]
    out = []
    while text:
        head, text = text[:limit], text[limit:]
        out.append(head)
    return out


def send(text: str, dry_run: bool = False) -> int:
    if not text.strip():
        print("[notify_telegram] empty message — skipping")
        return 0

    if dry_run or os.getenv("NOTIFY_DRY_RUN") == "1":
        print("─" * 60)
        print("[notify_telegram] DRY RUN — message NOT sent:")
        print(text)
        print("─" * 60)
        return 0

    token, chat = _resolve_creds()
    if not token or not chat:
        print("[notify_telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping (non-fatal)")
        return 0

    if not _HAS_REQUESTS:
        print("[notify_telegram] requests library missing — skipping (non-fatal)")
        return 0

    import requests  # local import safe now
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok_count = 0
    chunks = _chunk(text)
    for i, chunk in enumerate(chunks):
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = requests.post(
                    url,
                    json={
                        "chat_id": chat,
                        "text": chunk,
                        # Plain text — safer than HTML for CI-generated content
                        # which may contain unescaped < > & characters.
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    ok_count += 1
                    break
                # 429 = rate-limited; honor retry_after if present
                if resp.status_code == 429:
                    try:
                        wait = float(resp.json().get("parameters", {}).get("retry_after", RETRY_BACKOFF_SEC))
                    except Exception:
                        wait = RETRY_BACKOFF_SEC * (attempt + 1)
                    print(f"[notify_telegram] 429 rate-limited — sleeping {wait:.1f}s")
                    time.sleep(wait)
                    continue
                print(f"[notify_telegram] HTTP {resp.status_code}: {resp.text[:200]}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
                    continue
                break
            except Exception as e:  # noqa: BLE001 — best-effort notifier
                print(f"[notify_telegram] send failed (attempt {attempt+1}/{MAX_RETRIES+1}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
                    continue
                break

    if ok_count == len(chunks):
        print(f"[notify_telegram] sent {ok_count}/{len(chunks)} chunk(s) OK")
    else:
        print(f"[notify_telegram] partial send: {ok_count}/{len(chunks)} chunk(s) — remaining lost")
    return 0  # always non-fatal


def main() -> int:
    ap = argparse.ArgumentParser(description="Best-effort Telegram notifier for CI workflows")
    ap.add_argument("--text",   default="", help="message text (mutually exclusive with --stdin)")
    ap.add_argument("--stdin",  action="store_true", help="read message from stdin")
    ap.add_argument("--prefix", default="", help="prepend prefix + space to the message")
    ap.add_argument("--dry-run", action="store_true", help="print message; do not send")
    args = ap.parse_args()

    if args.stdin:
        msg = sys.stdin.read()
    else:
        msg = args.text

    if args.prefix:
        msg = f"{args.prefix} {msg}"

    return send(msg, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
