#!/usr/bin/env python3
"""
pipeline_health.py — Phase W (watchdog, 2026-07-03)
─────────────────────────────────────────────────────────────────────────────
Single source of truth for pipeline health signals across all workflows.

Reads/writes run_health.json — a small state file committed back to the repo
so every workflow can see when every other workflow last succeeded.

File layout:
    {
      "jobs": {
        "evening":  {"last_success_utc": "2026-07-03T13:45:12Z", "mode": "scheduled",
                     "exit_status": "ok", "buys": 2, "watchlist": 6,
                     "tradable": 1892, "regime": "SIDEWAYS",
                     "warnings": []},
        "tracker":  {"last_success_utc": "2026-07-03T14:12:03Z", "mode": "scheduled",
                     "active_positions": 3, "closed_today": 1, "yfinance_ok": true},
        "weekly":   {"last_success_utc": "2026-06-27T09:00:00Z", "mode": "scheduled"},
        "research": {"last_success_utc": "2026-06-30T04:00:00Z", "mode": "scheduled"}
      },
      "fresh_start_history": [
        {"date": "2026-07-01", "workflow": "evening"}
      ]
    }

CLI subcommands:
    record      — write success/warn entry for a job (called at end of workflow)
    check-stale — read the file, print stale jobs, exit 1 if any job > threshold
    guard-fresh-start — refuse if FRESH_START was already used in the last N days
    dump        — pretty-print current state

Usage in a workflow:
    python scripts/pipeline_health.py record --job evening --status ok \
        --extras buys=2 watchlist=6 tradable=1892 regime=SIDEWAYS
    python scripts/pipeline_health.py check-stale --job evening --max-hours 36
    python scripts/pipeline_health.py guard-fresh-start --workflow evening --window-days 2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# BUG-E3 fix: default HEALTH_FILE anchored to the project root (parent of
# scripts/) instead of cwd. Callers who set PIPELINE_HEALTH_FILE explicitly
# still win; otherwise the state file lands next to the product code, not
# in whatever transient cwd the CI runner is in.
_DEFAULT_HEALTH_FILE = str(Path(__file__).resolve().parent.parent / "run_health.json")
HEALTH_FILE = os.getenv("PIPELINE_HEALTH_FILE", _DEFAULT_HEALTH_FILE)

# Staleness thresholds — a job that hasn't succeeded in this many hours triggers
# a warning. Tuned per-job: evening pipeline runs daily on weekdays, so 36h
# covers a Friday-evening → Monday-morning gap without false alarms.
DEFAULT_STALE_HOURS = {
    "evening":  36,      # daily pipeline — 36h covers a weekend by design
    "tracker":  36,      # runs after evening — same window
    "morning":  36,      # gap-open check — daily on weekdays
    "research": 8 * 24,  # weekly (typically Sunday) — 8 days is our alert cliff
    "weekly":   8 * 24,  # weekly summary — same
}


# ── I/O ──────────────────────────────────────────────────────────────────────
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _load() -> dict:
    if not os.path.exists(HEALTH_FILE):
        return {"jobs": {}, "fresh_start_history": []}
    try:
        with open(HEALTH_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"jobs": {}, "fresh_start_history": []}
        data.setdefault("jobs", {})
        data.setdefault("fresh_start_history", [])
        return data
    except (OSError, json.JSONDecodeError) as e:
        print(f"[pipeline_health] failed to load {HEALTH_FILE}: {e}")
        return {"jobs": {}, "fresh_start_history": []}


def _save(state: dict) -> None:
    try:
        # Atomic-ish: write to tmp, rename
        tmp = HEALTH_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True, default=str)
        Path(tmp).replace(HEALTH_FILE)
    except OSError as e:
        print(f"[pipeline_health] failed to save {HEALTH_FILE}: {e}")


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        # tolerate "Z" suffix and offset-naive fallbacks
        if s.endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except ValueError:
        return None


# ── record: called at end of a workflow ──────────────────────────────────────
def cmd_record(args) -> int:
    state = _load()
    jobs = state["jobs"]
    entry = {
        "last_success_utc": _now_utc_iso() if args.status == "ok" else jobs.get(args.job, {}).get("last_success_utc", ""),
        "last_run_utc":     _now_utc_iso(),
        "mode":             args.mode or "unknown",
        "exit_status":      args.status,
    }
    # Preserve prior fields if we're only recording a failure (don't nuke history)
    prior = jobs.get(args.job, {})
    for k, v in prior.items():
        if k not in entry:
            entry[k] = v

    # --extras k=v k=v k=v (v is coerced to int/float/str)
    for kv in args.extras or []:
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        entry[k.strip()] = _coerce(v.strip())

    if args.warning:
        entry.setdefault("warnings", [])
        # Cap warnings history at 5 to keep the file small
        entry["warnings"] = ([args.warning] + list(entry.get("warnings", [])))[:5]

    jobs[args.job] = entry
    _save(state)
    print(f"[pipeline_health] recorded {args.job} status={args.status} mode={args.mode}")
    return 0


def _coerce(v: str):
    """Coerce string → int/float/bool/None if it obviously matches, else str."""
    if v == "":
        return ""
    lv = v.lower()
    if lv in ("true", "false"):
        return lv == "true"
    if lv in ("null", "none"):
        return None
    try:
        if "." in v:
            return float(v)
        return int(v)
    except ValueError:
        return v


# ── check-stale: alert if a job hasn't run recently ──────────────────────────
def cmd_check_stale(args) -> int:
    state = _load()
    jobs = state["jobs"]
    if args.job:
        job_names = [args.job]
    else:
        job_names = list(jobs.keys())

    now = _now_utc()
    stale = []
    for name in job_names:
        entry = jobs.get(name)
        if not entry:
            # No entry at all yet — first-run scenario. Not stale, just unknown.
            print(f"[pipeline_health] {name}: NO ENTRY yet — first run")
            continue
        threshold_h = args.max_hours if args.max_hours else DEFAULT_STALE_HOURS.get(name, 36)
        last = _parse_iso(entry.get("last_success_utc", ""))
        if last is None:
            print(f"[pipeline_health] {name}: no last_success_utc — treating as stale")
            stale.append((name, "never succeeded", threshold_h))
            continue
        age_h = (now - last).total_seconds() / 3600.0
        marker = "STALE" if age_h > threshold_h else "OK"
        print(f"[pipeline_health] {name}: {marker}  age={age_h:.1f}h  threshold={threshold_h}h  last={entry.get('last_success_utc')}")
        if age_h > threshold_h:
            stale.append((name, f"{age_h:.1f}h", threshold_h))

    if not stale:
        return 0

    # Emit human-readable summary to stdout — the workflow can pipe it to Telegram
    lines = ["🚨 PIPELINE STALE — one or more jobs haven't succeeded recently:"]
    for name, age, thr in stale:
        lines.append(f"  • {name}: last success {age} ago (threshold {thr}h)")
    lines.append("")
    lines.append("Check GitHub Actions — external trigger may have failed.")
    print("─" * 60)
    print("\n".join(lines))
    print("─" * 60)

    # If --telegram flag set, write the message to a well-known path so the
    # workflow can pick it up in a follow-up step (workflows can't pipe stdout
    # between steps easily — file is more robust).
    if args.write_alert_file:
        try:
            with open(args.write_alert_file, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
        except OSError as e:
            print(f"[pipeline_health] failed to write alert file {args.write_alert_file}: {e}")

    return 1  # non-zero so the workflow step can trigger the notification


# ── guard-fresh-start: prevent accidental double-wipe ────────────────────────
def cmd_guard_fresh_start(args) -> int:
    state = _load()
    history = state.get("fresh_start_history", [])
    now = _now_utc()

    # If not actually running with FRESH_START=true, this is a no-op success
    fs_env = os.getenv("FRESH_START", "false").lower() == "true"
    if not fs_env:
        return 0  # nothing to guard

    # Check if we ran FRESH_START recently
    window = timedelta(days=args.window_days)
    recent = []
    for h in history:
        d = h.get("date", "")
        dt = _parse_iso(d + "T00:00:00Z") if len(d) == 10 else _parse_iso(d)
        if dt and (now - dt) < window:
            recent.append(h)

    if recent:
        msg = (
            f"🛑 FRESH_START refused — already used {len(recent)} time(s) in "
            f"the last {args.window_days} day(s). Recent uses: "
            + ", ".join(f"{h.get('date')}/{h.get('workflow','?')}" for h in recent[:3])
            + ". If this is intentional (real re-baseline), delete run_health.json "
            "manually and re-dispatch."
        )
        print(msg)
        if args.write_alert_file:
            try:
                with open(args.write_alert_file, "w", encoding="utf-8") as f:
                    f.write(msg)
            except OSError:
                pass
        return 2  # distinct exit code — workflow can branch on it

    # First FRESH_START in the window — record and allow
    history.append({
        "date":     now.strftime("%Y-%m-%d"),
        "workflow": args.workflow or "unknown",
        "run_ts":   _now_utc_iso(),
    })
    # Keep history bounded — last 20 entries
    state["fresh_start_history"] = history[-20:]
    _save(state)
    print(f"[pipeline_health] FRESH_START allowed for {args.workflow} — recorded")
    return 0


# ── dump: pretty-print current state ─────────────────────────────────────────
def cmd_dump(args) -> int:
    state = _load()
    print(json.dumps(state, indent=2, default=str))
    return 0


# ── main dispatch ────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="pipeline_health — read/write run_health.json")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("record")
    r.add_argument("--job", required=True, choices=["evening", "tracker", "morning", "weekly", "research"])
    r.add_argument("--status", choices=["ok", "warn", "fail"], default="ok")
    r.add_argument("--mode", default="")
    r.add_argument("--warning", default="")
    r.add_argument("--extras", nargs="*", help="key=value pairs to attach to the entry")
    r.set_defaults(func=cmd_record)

    s = sub.add_parser("check-stale")
    s.add_argument("--job", default="", help="specific job (empty = all)")
    s.add_argument("--max-hours", type=float, default=0, help="override default threshold")
    s.add_argument("--write-alert-file", default="", help="write human-readable alert to this file")
    s.set_defaults(func=cmd_check_stale)

    g = sub.add_parser("guard-fresh-start")
    g.add_argument("--workflow", default="unknown")
    g.add_argument("--window-days", type=int, default=2)
    g.add_argument("--write-alert-file", default="", help="write refusal message to this file")
    g.set_defaults(func=cmd_guard_fresh_start)

    d = sub.add_parser("dump")
    d.set_defaults(func=cmd_dump)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
