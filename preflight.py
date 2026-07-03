"""
preflight_checks.py — Static verification for the Analyse-recommend-track repo.

Purpose: catch bugs at push time (in the preflight.yml CI job) that would
otherwise only surface hours later inside a scheduled workflow run.

Each check is independently invocable via a CLI flag so the preflight.yml
workflow can report exactly which check failed:

    python scripts/preflight_checks.py --json
    python scripts/preflight_checks.py --gitignore-workflow
    python scripts/preflight_checks.py --compliance
    python scripts/preflight_checks.py --env-drift
    python scripts/preflight_checks.py --import-smoke
    python scripts/preflight_checks.py --all            # run everything

Exit codes:
    0 = all requested checks passed
    1 = at least one check failed (CI-red)
    2 = internal error (misconfigured check itself)

Design principles:
  - Zero runtime deps beyond stdlib (no requests / yaml / etc.)
  - Deterministic — no network, no wall-clock, no API calls
  - Fast — total runtime should be well under 5 seconds
  - Clear failure messages that name the exact offending file + line
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path

# Repo root = parent of this scripts/ folder
REPO_ROOT = Path(__file__).resolve().parent.parent


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 1 — JSON state file validity
# ═══════════════════════════════════════════════════════════════════════════
# Rationale: main.py + tracker_job.py + weekly_job.py write JSON state files
# atomically, but if a runner is killed mid-write (out-of-memory, timeout,
# force-cancel) the file can end up half-written. The NEXT run then crashes
# with json.JSONDecodeError early — wasting an entire scheduled slot.
#
# By validating at push time we ensure everything currently committed to the
# repo is parseable. This does NOT check schema validity — a file with the
# wrong keys will still pass. That's a deeper concern for a schema layer.

# Files we know are JSON state files. Anything else with .json extension is
# also validated but a non-existent file is not an error (they're generated
# at runtime).
KNOWN_JSON_STATE_FILES = [
    "tracker.json",
    "trade_tracker.json",
    "confidence_history.json",
    "gate_memory.json",
    "watchlist_persist.json",
    "regime_calibration.json",
    "sector_rank_history.json",
    "weekly_metrics.json",
    "delivery_cache.json",
    "sector_cache.json",
    "backtest_summary.json",
    "readiness_report.json",
]


def check_json_files() -> int:
    """Validate every committed *.json file parses. Return 0=OK, 1=fail."""
    failed: list[tuple[str, str]] = []
    seen: set[str] = set()

    # First: all *.json in repo root (state files)
    for pat in ["*.json"]:
        for p in REPO_ROOT.glob(pat):
            if p.name in seen:
                continue
            seen.add(p.name)
            try:
                with p.open("r", encoding="utf-8") as f:
                    json.load(f)
            except json.JSONDecodeError as e:
                failed.append((str(p.relative_to(REPO_ROOT)),
                               f"parse error line {e.lineno} col {e.colno}: {e.msg}"))
            except OSError as e:
                failed.append((str(p.relative_to(REPO_ROOT)), f"read error: {e}"))

    if failed:
        print("❌ JSON validation FAILED:")
        for f, msg in failed:
            print(f"   {f}: {msg}")
        return 1

    print(f"✅ JSON validation OK ({len(seen)} files checked)")
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 2 — .gitignore vs workflow `git add` consistency
# ═══════════════════════════════════════════════════════════════════════════
# Rationale: on 2026-07-03 the tracker.yml workflow failed for weeks because
# it tried to `git add delivery_cache.json` while .gitignore was blocking that
# exact file. `git add` returns exit code 1 with the message:
#   "The following paths are ignored by one of your .gitignore files"
# This kills the whole commit-back step → the freshly-generated tracker xlsx
# never gets pushed back → the next run reads the OLD xlsx → looks like
# "FRESH_START didn't work" from the user's perspective.
#
# This check parses every workflow YAML for `git add <file>` occurrences and
# verifies none of those files match any pattern in .gitignore.

_GIT_ADD_RE = re.compile(
    r"""(?xm)
    ^\s*                            # optional leading whitespace
    (?:                             # OPTIONAL prefixes:
        (?:git\s+add\s+             #   direct `git add`
            (?:-[a-zA-Z]+\s+)*      #     any flags (-f, -A etc.)
        )
        |
        (?:files=\(                 #   or a `files=( ... )` bash array
            (?P<array>[^)]+)
        \))
    )
    """
)


def _load_gitignore_patterns() -> set[str]:
    """
    Load .gitignore patterns as a set of *normalised literal names*.

    Normalisation:
      - Strip leading '/' (repo-root anchor is implicit here)
      - Strip trailing '/' (directory marker → we compare against workflow
        targets that may or may not have a trailing slash)
      - We store BOTH the slash-stripped form AND the with-slash form so
        `foo/` in .gitignore matches `foo/` in a workflow `git add foo/`.

    We deliberately do NOT try to implement full gitignore glob semantics
    (**, negation with `!`, `[abc]` classes). The workflow bug we're
    catching always involves a plain filename or plain dirname.
    """
    gi = REPO_ROOT / ".gitignore"
    if not gi.exists():
        return set()
    patterns: set[str] = set()
    for line in gi.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("!"):
            # Un-ignore rules — treat as complexity we don't handle. Skip.
            continue
        # Strip leading /, keep the rest verbatim
        pat = line.lstrip("/")
        patterns.add(pat)
        # Also add the version without a trailing slash (foo/ → foo)
        patterns.add(pat.rstrip("/"))
    return patterns


def _extract_git_add_targets(workflow_text: str) -> list[str]:
    """
    Very forgiving extractor: pulls out filenames from:
      files=(
        foo.json
        bar.csv
      )
      git add "${existing[@]}"
    AND direct `git add foo.json bar.csv` forms.

    IMPORTANT: `git add -f <target>` (force-add) is treated as an
    intentional override — the workflow author has explicitly opted to
    ignore .gitignore for that path. We skip those.
    """
    targets: list[str] = []

    # Case A: files=( ... ) blocks
    # These are always paired with a subsequent `git add "${...[@]}"` that
    # is NOT force-add. So the files listed here must NOT be gitignored.
    for m in re.finditer(r"files=\(([^)]+)\)", workflow_text):
        for tok in m.group(1).split():
            tok = tok.strip().strip('"').strip("'")
            if not tok or tok.startswith("$") or tok.startswith("#"):
                continue
            if tok in ("existing", "existing[@]", "${existing[@]}"):
                continue
            targets.append(tok)

    # Case B: direct `git add <literal>` — but exclude `git add -f` and any
    # form with flags (which typically means the author knows what they're
    # doing).
    #   Matches:      git add foo.json      → foo.json (checked)
    #   Skips:        git add -f foo.json   → force-add is intentional
    #   Skips:        git add -A            → adds everything, no target
    for m in re.finditer(r"git\s+add\s+([^\n]+)", workflow_text):
        rest = m.group(1).strip()
        # If -f / --force is present anywhere in the flags, treat as
        # intentional override and skip.
        # Tokenize just enough to check the flag list before any positional.
        toks = rest.split()
        # If any token before the first non-flag is -f or --force, skip.
        i = 0
        forced = False
        while i < len(toks) and toks[i].startswith("-"):
            flag = toks[i]
            if flag in ("-f", "--force") or flag.startswith("-f") \
               or "force" in flag:
                forced = True
                break
            i += 1
        if forced:
            continue
        # Take remaining positional tokens as targets. Skip variable
        # expansions ($..., ${...}) — those are handled by Case A.
        for tok in toks[i:]:
            tok = tok.strip().strip('"').strip("'")
            if not tok or tok.startswith("$") or tok.startswith("#"):
                continue
            if tok.startswith("-"):
                continue
            targets.append(tok)

    return targets


def check_gitignore_vs_workflows() -> int:
    """Fail if any workflow tries to git-add a gitignore'd file."""
    workflows = list((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    if not workflows:
        print("⚠️  No workflow files found under .github/workflows/ — nothing to check")
        return 0

    ignored = _load_gitignore_patterns()
    if not ignored:
        print("⚠️  .gitignore is empty — nothing to check")
        return 0

    failures: list[tuple[str, str]] = []
    total_checked = 0

    for wf in workflows:
        text = wf.read_text(encoding="utf-8")
        targets = _extract_git_add_targets(text)
        for t in targets:
            total_checked += 1
            # Match both raw and slash-stripped form (foo vs foo/)
            candidates = {t, t.rstrip("/")}
            if candidates & ignored:
                failures.append((wf.name, t))

    if failures:
        print("❌ .gitignore ↔ workflow MISMATCH — this class of bug broke prod on 2026-07-03:")
        print()
        for wf, target in failures:
            print(f"   {wf}: `git add {target}` will fail because {target}")
            print(f"      is listed in .gitignore")
        print()
        print("   FIX (pick one):")
        print("     A. Remove that filename from .gitignore (recommended if the")
        print("        workflow is supposed to persist it across runs)")
        print("     B. Change the workflow to `git add -f <file>` (force-add)")
        print("     C. Remove the file from the workflow's `files=(...)` array")
        return 1

    print(f"✅ .gitignore vs workflow OK ({total_checked} git-add targets checked, "
          f"{len(workflows)} workflows scanned)")
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 3 — Compliance / hype-phrase scan
# ═══════════════════════════════════════════════════════════════════════════
# Rationale: even for friends & family (no SEBI registration), guaranteed-
# return language destroys trust the moment a trade goes wrong. First
# drawdown + "sure shot" wording = the friend feels lied to. Static-scan any
# file that formats a Telegram message.

# Case-insensitive substrings. Whole-word matching is done in code.
FORBIDDEN_PHRASES = [
    r"\bguaranteed\b",
    r"\bsure[\s\-]?shot\b",
    r"\bjackpot\b",
    r"\boperator\s+stock\b",
    r"\binsider\s+info\b",
    r"\bfixed\s+return\b",
    r"\bno\s+loss\b",
    r"\b100%\s+profit\b",
    r"\brisk[\s\-]?free\b",
    r"\bmultibagger\s+confirmed\b",
    r"\bbuy\s+immediately\b",
    r"\bassured\s+returns?\b",
]

# Files most likely to contain user-facing text. Extending to all *.py is
# too noisy (docstrings mention "guaranteed" in a technical sense etc.)
_MSG_FILE_PATTERNS = [
    "main.py",
    "tracker_job.py",
    "weekly_job.py",
    "research_job.py",
    "*_telegram*.py",
    "app/messaging/*.py",
    "app/messaging/templates.py",
]


def check_compliance_phrases() -> int:
    """Scan messaging-adjacent files for forbidden hype language."""
    matched: list[tuple[str, int, str, str]] = []

    files: set[Path] = set()
    for pat in _MSG_FILE_PATTERNS:
        for p in REPO_ROOT.glob(pat):
            if p.is_file() and p.suffix == ".py":
                files.add(p)

    if not files:
        print("⚠️  No messaging files found — nothing to scan")
        return 0

    patterns = [(re.compile(p, re.IGNORECASE), p) for p in FORBIDDEN_PHRASES]

    for f in sorted(files):
        try:
            for lineno, line in enumerate(f.read_text(encoding="utf-8",
                                                       errors="ignore").splitlines(),
                                           start=1):
                # Skip comments — a comment mentioning "sure shot" as
                # something to AVOID is fine.
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                for pat, human in patterns:
                    if pat.search(line):
                        matched.append((str(f.relative_to(REPO_ROOT)),
                                        lineno, human, line.strip()[:100]))
        except OSError:
            continue

    if matched:
        print("❌ FORBIDDEN hype phrases in messaging files:")
        for f, ln, pat, snippet in matched:
            print(f"   {f}:{ln}  [{pat}]  {snippet}")
        print()
        print("   Even for friends & family: this language destroys trust the")
        print("   moment a recommendation goes wrong. Rephrase as neutral")
        print("   research language.")
        return 1

    print(f"✅ Compliance scan OK ({len(files)} files, {len(FORBIDDEN_PHRASES)} phrases)")
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 4 — env var drift (.env.example vs actual os.getenv usage)
# ═══════════════════════════════════════════════════════════════════════════
# Rationale: .env.example is where the operator learns what to configure. If
# it lists env vars no code ever reads, the operator wastes time setting them
# — and worse, gets false confidence that a feature is "on" when it's
# actually dead config.
#
# This is a soft warning (workflow uses `continue-on-error: true`) because
# some vars may be legitimately read via runtime string-composition
# (e.g. os.environ.get(f"THRESHOLD_{name}")) which we can't detect
# statically.

def check_env_drift() -> int:
    env_example = REPO_ROOT / ".env.example"
    if not env_example.exists():
        print("ℹ️  No .env.example found — skipping env drift check")
        return 0

    declared: set[str] = set()
    for line in env_example.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Match KEY=... form
        m = re.match(r"^([A-Z][A-Z0-9_]*)\s*=", line)
        if m:
            declared.add(m.group(1))

    if not declared:
        print("ℹ️  .env.example declares no vars")
        return 0

    # Scan all .py for os.getenv("KEY") / os.environ["KEY"] / os.environ.get("KEY")
    getenv_re = re.compile(
        r"""os\.(?:getenv|environ(?:\.get)?)\s*[\(\[]\s*['"]([A-Z][A-Z0-9_]*)['"]"""
    )
    used: set[str] = set()
    for p in REPO_ROOT.rglob("*.py"):
        if any(part in {".git", "__pycache__", ".forge_venv",
                        "backtest_staging"} for part in p.parts):
            continue
        try:
            for m in getenv_re.finditer(p.read_text(encoding="utf-8", errors="ignore")):
                used.add(m.group(1))
        except OSError:
            continue

    orphaned = sorted(declared - used)
    missing = sorted(used - declared - _CORE_RUNTIME_VARS)

    ok = True
    if orphaned:
        ok = False
        print(f"⚠️  {len(orphaned)} env var(s) declared in .env.example but no *.py reads them:")
        for v in orphaned:
            print(f"     {v}")
    if missing:
        # Just informational — missing from .env.example doesn't fail
        print(f"ℹ️  {len(missing)} env var(s) used in code but not documented in .env.example:")
        for v in missing[:20]:
            print(f"     {v}")
        if len(missing) > 20:
            print(f"     ... and {len(missing) - 20} more")

    if ok:
        print(f"✅ env drift OK ({len(declared)} declared, all used)")
        return 0
    # Soft-fail: return 0 so CI still passes; the workflow uses
    # continue-on-error to make this a warning rather than a red badge.
    # If you want it strict, change to `return 1`.
    return 0


# Env vars that are set by the CI runner / GitHub Actions / OS and don't
# need to appear in .env.example.
_CORE_RUNTIME_VARS = {
    "PATH", "HOME", "USER", "TZ", "PYTHONPATH", "PYTHONUNBUFFERED",
    "GITHUB_ACTIONS", "GITHUB_WORKFLOW", "GITHUB_RUN_ID", "GITHUB_TOKEN",
    "GITHUB_REPOSITORY", "GITHUB_REF", "GITHUB_SHA", "GITHUB_EVENT_NAME",
    "GITHUB_EVENT_PATH", "GITHUB_OUTPUT", "GITHUB_ENV", "GITHUB_STEP_SUMMARY",
    "RUNNER_OS", "RUNNER_TEMP", "RUNNER_TOOL_CACHE",
    "IMPORT_SMOKE_TEST",
}


# ═══════════════════════════════════════════════════════════════════════════
# CHECK 5 — import smoke test
# ═══════════════════════════════════════════════════════════════════════════
# Rationale: py_compile catches syntax errors, but a missing `from foo
# import bar` where foo exists but bar doesn't will only surface at import
# time. Running `python -c "import main"` is the cheapest way to catch that
# class of error, but main.py currently does work at import (fetching yfin
# etc.) that we don't want on the CI runner.
#
# The IMPORT_SMOKE_TEST=1 env var is a sentinel main.py can respect to skip
# side effects during import. If main.py doesn't currently honor it, we can
# still try the import and catch the exception cleanly — the goal is just
# to surface ImportError, ModuleNotFoundError, and AttributeError at CI.

_MODULES_TO_SMOKE = [
    "main",
    "tracker_job",
]


def check_import_smoke() -> int:
    """Attempt to import main modules under IMPORT_SMOKE_TEST=1."""
    # Ensure REPO_ROOT is on sys.path (mimics what CI does)
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    # Signal to code that this is a smoke import — anything that does work
    # at import time should honor this and skip network / API calls.
    os.environ.setdefault("IMPORT_SMOKE_TEST", "1")
    os.environ.setdefault("DRY_RUN", "true")

    failed: list[tuple[str, str]] = []
    ok: list[str] = []
    for mod in _MODULES_TO_SMOKE:
        # Only try if the file actually exists at repo root (some may not
        # be present in every branch)
        candidate = REPO_ROOT / f"{mod}.py"
        if not candidate.exists():
            print(f"ℹ️  {mod}.py not found — skipping")
            continue
        try:
            # Use fresh module import (delete from sys.modules first so a
            # partial prior import doesn't mask errors)
            sys.modules.pop(mod, None)
            __import__(mod)
            ok.append(mod)
        except SystemExit as e:
            # main.py may call sys.exit() during its main block; the top-
            # level `if __name__ == "__main__":` guard should prevent that
            # during a bare import, but be defensive.
            if e.code not in (None, 0):
                failed.append((mod, f"sys.exit({e.code}) at import"))
        except Exception as e:  # noqa: BLE001 — smoke test wants all errors
            failed.append((mod, f"{type(e).__name__}: {e}"))

    if failed:
        print("❌ Import smoke test FAILED:")
        for mod, err in failed:
            print(f"   import {mod}: {err}")
        print()
        print("   This means a scheduled workflow would crash before doing")
        print("   any useful work. Fix imports before pushing.")
        return 1

    print(f"✅ Import smoke test OK ({len(ok)} modules imported: {', '.join(ok)})")
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# Main dispatcher
# ═══════════════════════════════════════════════════════════════════════════

CHECKS = [
    ("json",                check_json_files),
    ("gitignore-workflow",  check_gitignore_vs_workflows),
    ("compliance",          check_compliance_phrases),
    ("env-drift",           check_env_drift),
    ("import-smoke",        check_import_smoke),
]


def main() -> int:
    ap = argparse.ArgumentParser(description="Static preflight checks")
    ap.add_argument("--json",               action="store_true")
    ap.add_argument("--gitignore-workflow", action="store_true")
    ap.add_argument("--compliance",         action="store_true")
    ap.add_argument("--env-drift",          action="store_true")
    ap.add_argument("--import-smoke",       action="store_true")
    ap.add_argument("--all",                action="store_true",
                    help="Run every check")
    args = ap.parse_args()

    # If no flag given, treat as --all
    any_flag = any([args.json, args.gitignore_workflow, args.compliance,
                    args.env_drift, args.import_smoke, args.all])
    if not any_flag:
        args.all = True

    selected = []
    flag_map = {
        "json":               args.json               or args.all,
        "gitignore-workflow": args.gitignore_workflow or args.all,
        "compliance":         args.compliance         or args.all,
        "env-drift":          args.env_drift          or args.all,
        "import-smoke":       args.import_smoke       or args.all,
    }
    for name, fn in CHECKS:
        if flag_map.get(name):
            selected.append((name, fn))

    rc_total = 0
    for name, fn in selected:
        print(f"── running check: {name} ──")
        try:
            rc = fn()
        except Exception as e:  # noqa: BLE001
            print(f"❌ INTERNAL ERROR in check {name!r}: {type(e).__name__}: {e}")
            return 2
        rc_total |= rc
        print()

    if rc_total == 0:
        print("═══════════════════════════════════════════════════════════")
        print("✅ All preflight checks passed")
        print("═══════════════════════════════════════════════════════════")
    else:
        print("═══════════════════════════════════════════════════════════")
        print("❌ Preflight FAILED — do not merge / do not dispatch scheduled runs")
        print("═══════════════════════════════════════════════════════════")
    return rc_total


if __name__ == "__main__":
    sys.exit(main())
