#!/usr/bin/env python3
"""
notify_telegram_document.py — Phase I Excel report deliverer (2026-07-09)
─────────────────────────────────────────────────────────────────────────────
Sends a file to Telegram as a document. Sibling to `notify_telegram.py`
(which handles text messages). Used by main.py to deliver the daily
shadow_report.xlsx and the weekly shadow_report_weekly.xlsx.

Reuses the same TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID + TRACKER_* fallback
resolver as notify_telegram.py. If credentials or requests are missing,
prints a warning and returns 0 — never fails the caller (delivery is
best-effort).

Usage as module:
    import scripts.notify_telegram_document as tg_doc
    tg_doc.send_document("shadow_report.xlsx", caption="📊 Daily report")

Usage as CLI:
    python scripts/notify_telegram_document.py shadow_report.xlsx --caption "test"

Env:
    TELEGRAM_BOT_TOKEN   — bot token (falls back to TRACKER_BOT_TOKEN)
    TELEGRAM_CHAT_ID     — chat id  (falls back to TRACKER_CHAT_ID)
    NOTIFY_DRY_RUN=1     — log the request but don't POST (for CI tests)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    import requests  # noqa: F401 — verified inside send_document()
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


# ─── Config ──────────────────────────────────────────────────────────────
MAX_RETRIES         = int(os.getenv("NOTIFY_DOC_RETRIES", "2"))
RETRY_BACKOFF_SEC   = float(os.getenv("NOTIFY_DOC_RETRY_BACKOFF", "2.0"))
TELEGRAM_DOC_MAX_MB = 50   # Telegram Bot API upload limit
TELEGRAM_CAPTION_MAX = 1024  # HTML caption max chars


def _resolve_creds() -> tuple[str, str]:
    """Return (token, chat_id) — prefer TELEGRAM_*, fall back to TRACKER_*."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat  = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token:
        token = os.getenv("TRACKER_BOT_TOKEN", "").strip()
    if not chat:
        chat = os.getenv("TRACKER_CHAT_ID", "").strip()
    return token, chat


def send_document(file_path: str, caption: str = "",
                   dry_run: bool = False) -> int:
    """POST /sendDocument with the given file. Never raises.

    Returns 0 on success, non-zero on failure — but the caller (evening
    pipeline) should treat any return value as non-fatal.
    """
    p = Path(file_path)
    if not p.exists():
        print(f"[notify_telegram_document] file not found: {file_path}")
        return 1

    size_mb = p.stat().st_size / (1024 * 1024)
    if size_mb > TELEGRAM_DOC_MAX_MB:
        print(
            f"[notify_telegram_document] file too large: {size_mb:.1f} MB "
            f"(limit {TELEGRAM_DOC_MAX_MB} MB) — skipping"
        )
        return 1

    if caption and len(caption) > TELEGRAM_CAPTION_MAX:
        caption = caption[:TELEGRAM_CAPTION_MAX - 3] + "..."

    if dry_run or os.getenv("NOTIFY_DRY_RUN") == "1":
        print("─" * 60)
        print(f"[notify_telegram_document] DRY RUN — would send:")
        print(f"  file:    {p.name} ({size_mb:.2f} MB)")
        print(f"  caption: {caption[:200]}")
        print("─" * 60)
        return 0

    token, chat = _resolve_creds()
    if not token or not chat:
        print("[notify_telegram_document] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping (non-fatal)")
        return 0

    if not _HAS_REQUESTS:
        print("[notify_telegram_document] requests library missing — skipping (non-fatal)")
        return 0

    import requests  # local import — verified above
    url = f"https://api.telegram.org/bot{token}/sendDocument"

    for attempt in range(MAX_RETRIES + 1):
        try:
            with open(p, "rb") as fh:
                files = {"document": (p.name, fh)}
                data = {"chat_id": chat}
                if caption:
                    data["caption"] = caption
                resp = requests.post(url, data=data, files=files, timeout=90)

            if resp.status_code == 200:
                print(f"[notify_telegram_document] sent {p.name} ({size_mb:.2f} MB) OK")
                return 0

            # 429 rate-limit — honor retry_after
            if resp.status_code == 429:
                try:
                    wait = float(resp.json().get("parameters", {})
                                  .get("retry_after", RETRY_BACKOFF_SEC))
                except Exception:
                    wait = RETRY_BACKOFF_SEC * (attempt + 1)
                print(f"[notify_telegram_document] 429 rate-limited — sleeping {wait:.1f}s")
                time.sleep(wait)
                continue

            print(f"[notify_telegram_document] HTTP {resp.status_code}: {resp.text[:200]}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
                continue
            return 1

        except Exception as e:  # noqa: BLE001 — best-effort notifier
            print(f"[notify_telegram_document] attempt {attempt + 1}/{MAX_RETRIES + 1} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
                continue
            return 1

    return 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Best-effort Telegram document sender for CI workflows"
    )
    ap.add_argument("file", help="path to file to send (xlsx, pdf, etc.)")
    ap.add_argument("--caption", default="", help="optional caption text")
    ap.add_argument("--dry-run", action="store_true", help="log the request; do not POST")
    args = ap.parse_args()
    return send_document(args.file, caption=args.caption, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
