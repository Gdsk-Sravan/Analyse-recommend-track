"""
shadow_log.py — Phase I observation-mode logger (4-bucket edition).

Records "what would have happened" for stocks that would-be, near-miss,
or definitely-not BUY candidates. Zero real money at risk — pure paper-trade
tracking so we can validate the backtest predictions in live NSE conditions
AND identify calibration issues (too strict / too loose filters).

═══ THE 4 BUCKETS ═══════════════════════════════════════════════════════════

  A · TAKEN         Real BUY signal — the bot bought this. Ground truth.
                    (populated from run_pipeline() when decision == "BUY")

  B · WATCH_ME      BREAKOUT / MOMENTUM setup, BUT chop regime → phase_i_skip.
                    "My setup is fine, the market regime blocked me."
                    Hypothesis: if this bucket beats ~40% wr, we're being
                    too strict about SIDEWAYS/WEAK regimes.

  C · NOT_MY_STYLE  PULLBACK / REVERSAL / OTHER setup → phase_i_skip.
                    "Backtest already said these setups don't work."
                    Hypothesis: if this bucket wins <25% wr, filter is
                    correctly rejecting bad setups. If ≥ 40%, backtest was
                    wrong (or market has changed).

  D · SO_CLOSE      BREAKOUT / MOMENTUM setup + bullish-ish regime, but
                    confidence 60-69 (just below min_confidence gate).
                    "Right setup, right regime, just not confident enough."
                    Hypothesis: if this bucket beats 42%, threshold is too
                    high; if <38%, threshold is right.

═══════════════════════════════════════════════════════════════════════════

Design (2026-07-07):
  • record_shadow_trade(bucket, stock, regime): called from main.py when
    a stock lands in one of buckets A/B/C/D. Writes one PENDING row to
    shadow_trades.csv, tagged with the bucket letter.
  • update_shadow_outcomes(): called at start of every evening run.
    Fetches recent OHLC for every PENDING row and resolves outcomes.
  • format_shadow_summary(): compact text block for the Telegram brief,
    with per-bucket win rate, expectancy, and calibration verdict.

Break-even math (mirrors backtest_walkforward.py):
    entry     = today's close
    target_1  = entry * (1 + SHADOW_TARGET_PCT/100)     default +5%
    stop_loss = entry * (1 - SHADOW_STOP_PCT/100)       default -3%
    max hold  = MAX_SHADOW_DAYS calendar days           default 10

Enable via env: PHASE_I_SHADOW_LOG=true  (default: true — always on)

CSV schema (shadow_trades.csv):
    date_added, symbol, bucket, setup, regime, conf,
    entry, target_1, stop_loss,
    status, exit_date, exit_price, r_multiple, days_held, note

No external dependencies beyond pandas + yfinance (already used by main.py).
"""

from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from typing import List, Optional

# yfinance is optional — if unavailable, outcome updates gracefully skip.
try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False


# ─── config ─────────────────────────────────────────────────────────────────
SHADOW_CSV_PATH = os.getenv("SHADOW_CSV_PATH", "shadow_trades.csv")
SHADOW_ENABLED  = os.getenv("PHASE_I_SHADOW_LOG", "true").lower() in ("1", "true", "yes")
MAX_SHADOW_DAYS = int(os.getenv("MAX_SHADOW_DAYS", "10"))
TARGET_PCT      = float(os.getenv("SHADOW_TARGET_PCT", "5.0"))
STOP_PCT        = float(os.getenv("SHADOW_STOP_PCT",   "3.0"))

# Bucket letters — kept short so they read cleanly in the CSV
BUCKET_A = "A"   # TAKEN
BUCKET_B = "B"   # WATCH_ME
BUCKET_C = "C"   # NOT_MY_STYLE
BUCKET_D = "D"   # SO_CLOSE

_BUCKET_NAME = {
    BUCKET_A: "A · TAKEN",
    BUCKET_B: "B · WATCH_ME",
    BUCKET_C: "C · NOT_MY_STYLE",
    BUCKET_D: "D · SO_CLOSE",
}

# What the backtest predicts for each bucket's win rate. Used to build a
# calibration verdict ("matches backtest" vs "better/worse than expected").
_BUCKET_EXPECTED_WR = {
    BUCKET_A: 48.0,   # our proven edge zone
    BUCKET_B: 35.0,   # right setup, wrong regime — backtest 32-38%
    BUCKET_C: 25.0,   # wrong setup — backtest 21-28%
    BUCKET_D: 40.0,   # setup ok, conf just below — backtest 38-43%
}

_CSV_COLS = [
    "date_added", "symbol", "bucket", "setup", "regime", "conf",
    "entry", "target_1", "stop_loss",
    "status", "exit_date", "exit_price", "r_multiple", "days_held", "note",
]

_STATUS_PENDING   = "PENDING"
_STATUS_WIN       = "WIN"
_STATUS_LOSS      = "LOSS"
_STATUS_TIME_EXIT = "TIME_EXIT"
_STATUS_ERROR     = "ERROR"


# ─── internal file helpers ──────────────────────────────────────────────────
def _ensure_csv() -> None:
    """Create shadow_trades.csv with header if missing.

    Also upgrades old CSVs (pre-4-bucket schema) that lack the 'bucket'
    column by rewriting them with the new header + a 'B_LEGACY' bucket tag.
    """
    if not os.path.exists(SHADOW_CSV_PATH):
        with open(SHADOW_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(_CSV_COLS)
        return
    # Detect old schema (no 'bucket' col) and upgrade in-place
    try:
        with open(SHADOW_CSV_PATH, "r", newline="", encoding="utf-8") as f:
            header = next(csv.reader(f), [])
        if "bucket" not in header:
            with open(SHADOW_CSV_PATH, "r", newline="", encoding="utf-8") as f:
                old_rows = list(csv.DictReader(f))
            for r in old_rows:
                r["bucket"] = "B_LEGACY"
            _write_all(old_rows)
    except Exception as e:
        print(f"[shadow_log] schema upgrade check failed: {e}")


def _read_all() -> List[dict]:
    """Read all rows. Returns [] if file missing/malformed."""
    if not os.path.exists(SHADOW_CSV_PATH):
        return []
    try:
        with open(SHADOW_CSV_PATH, "r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _write_all(rows: List[dict]) -> None:
    """Rewrite whole CSV atomically."""
    tmp = SHADOW_CSV_PATH + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in _CSV_COLS})
    os.replace(tmp, SHADOW_CSV_PATH)


# ─── PUBLIC: bucket classification helpers ──────────────────────────────────
def classify_skip_bucket(setup: str) -> str:
    """B or C, based on the setup type of a phase_i_skip stock.

    BREAKOUT / MOMENTUM  → B (right setup, blocked by regime)
    everything else      → C (wrong setup — backtest already vetoes)
    """
    if setup in ("BREAKOUT", "MOMENTUM"):
        return BUCKET_B
    return BUCKET_C


def is_near_miss_conf(setup: str, regime: str, conf: float,
                      min_conf: float, near_miss_band: float = 10.0) -> bool:
    """True if this stock qualifies for Bucket D:
    - Right setup (BREAKOUT/MOMENTUM)
    - Right regime (BULLISH-ish, not chop)
    - conf below min_conf but within `near_miss_band` points of it.

    Called from main.py right after the CONF_FAIL gate to decide whether
    to shadow-log this near-miss.
    """
    if setup not in ("BREAKOUT", "MOMENTUM"):
        return False
    # Regime name may be e.g. "BULLISH", "CAUTIOUS_BULLISH", "STRONG_BULL",
    # "BULL" — treat any non-chop regime as "OK" for this bucket.
    chop = {"SIDEWAYS", "TRANSITION", "HIGH_VOLATILITY", "WEAK",
            "BEAR", "STRONG_BEAR"}
    if regime in chop:
        return False
    if conf >= min_conf:
        return False  # actually passed — not a near-miss
    return (min_conf - conf) <= near_miss_band


# ─── PUBLIC: record + update APIs ───────────────────────────────────────────
def record_shadow_trade(bucket: str, stock: dict, regime: str,
                        note: str = "") -> None:
    """Log one PENDING shadow trade in the given bucket.

    Called from main.py at the appropriate bucket-decision points:
      • Bucket A: after decision == "BUY" is finalised
      • Bucket B: apply_setup_edge() when phase_i_skip fires + setup ∈ B/M
      • Bucket C: apply_setup_edge() when phase_i_skip fires + setup ∉ B/M
      • Bucket D: CONF_FAIL gate when is_near_miss_conf() returns True

    Silent no-op if:
      • PHASE_I_SHADOW_LOG=false
      • entry price missing
      • same (symbol, bucket, today) tuple already in CSV (dedup)
    """
    if not SHADOW_ENABLED:
        return
    if bucket not in _BUCKET_NAME:
        print(f"[shadow_log] refusing unknown bucket {bucket!r}")
        return
    try:
        symbol = str(stock.get("symbol") or "").strip()
        # Prefer explicit `entry` (set by main.py at scoring time), else close
        entry  = float(stock.get("entry") or stock.get("close") or 0)
        setup  = str(stock.get("setup_type") or "OTHER")
        conf   = float(stock.get("final_confidence", 0) or 0)
        if not symbol or entry <= 0:
            return

        today = datetime.now().strftime("%Y-%m-%d")

        _ensure_csv()
        rows = _read_all()

        # Dedup: same (symbol, bucket, date) = skip
        for r in rows:
            if (r.get("symbol") == symbol
                    and r.get("bucket") == bucket
                    and r.get("date_added") == today):
                return

        target_1  = round(entry * (1 + TARGET_PCT / 100.0), 2)
        stop_loss = round(entry * (1 - STOP_PCT   / 100.0), 2)

        rows.append({
            "date_added": today,
            "symbol":     symbol,
            "bucket":     bucket,
            "setup":      setup,
            "regime":     regime,
            "conf":       f"{conf:.1f}",
            "entry":      f"{entry:.2f}",
            "target_1":   f"{target_1:.2f}",
            "stop_loss":  f"{stop_loss:.2f}",
            "status":     _STATUS_PENDING,
            "exit_date":  "",
            "exit_price": "",
            "r_multiple": "",
            "days_held":  "",
            "note":       note or f"shadow_{bucket}",
        })
        _write_all(rows)
    except Exception as e:
        print(f"[shadow_log] record failed for {stock.get('symbol')}/{bucket}: {e}")


def update_shadow_outcomes(quiet: bool = False) -> dict:
    """Resolve every PENDING row: WIN / LOSS / TIME_EXIT.

    Called at start of the evening pipeline. Returns:
        {"pending": int, "resolved_today": int, "wins": int, "losses": int,
         "time_exits": int, "errors": int}
    """
    stats = {"pending": 0, "resolved_today": 0,
             "wins": 0, "losses": 0, "time_exits": 0, "errors": 0}
    if not SHADOW_ENABLED:
        return stats
    if not _YF_OK:
        if not quiet:
            print("[shadow_log] yfinance unavailable — skipping outcome update")
        return stats

    _ensure_csv()
    rows = _read_all()
    if not rows:
        return stats

    today = datetime.now().date()
    changed = False

    for r in rows:
        if r.get("status") != _STATUS_PENDING:
            continue
        stats["pending"] += 1

        symbol = r.get("symbol", "")
        try:
            entry     = float(r.get("entry")     or 0)
            target_1  = float(r.get("target_1")  or 0)
            stop_loss = float(r.get("stop_loss") or 0)
            added_str = r.get("date_added", "")
            added_dt  = datetime.strptime(added_str, "%Y-%m-%d").date()
        except Exception:
            r["status"] = _STATUS_ERROR
            r["note"]   = "bad_row_fields"
            stats["errors"] += 1
            changed = True
            continue

        # Fetch OHLC from day-after-add up to today
        start = added_dt + timedelta(days=1)
        if start > today:
            continue  # not enough data yet

        try:
            yfsym = symbol if symbol.endswith(".NS") else symbol + ".NS"
            df = yf.download(
                yfsym,
                start=start.strftime("%Y-%m-%d"),
                end=(today + timedelta(days=1)).strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty:
                continue
        except Exception as e:
            if not quiet:
                print(f"[shadow_log] fetch failed for {symbol}: {e}")
            continue

        resolved = None
        exit_dt = None
        exit_px = None

        for idx, row in df.iterrows():
            try:
                hi = float(row.get("High", 0))
                lo = float(row.get("Low",  0))
            except Exception:
                continue
            # Tie-break: if both hit same day, assume STOP (conservative)
            if lo <= stop_loss:
                resolved = _STATUS_LOSS
                exit_dt  = idx.date() if hasattr(idx, "date") else today
                exit_px  = stop_loss
                break
            if hi >= target_1:
                resolved = _STATUS_WIN
                exit_dt  = idx.date() if hasattr(idx, "date") else today
                exit_px  = target_1
                break

        if resolved is None:
            days_open = (today - added_dt).days
            if days_open >= MAX_SHADOW_DAYS:
                resolved = _STATUS_TIME_EXIT
                exit_dt  = today
                try:
                    exit_px = float(df["Close"].iloc[-1])
                except Exception:
                    exit_px = entry

        if resolved:
            risk_per_share = entry - stop_loss
            r_mult = 0.0 if risk_per_share <= 0 \
                else round((exit_px - entry) / risk_per_share, 2)

            r["status"]     = resolved
            r["exit_date"]  = exit_dt.strftime("%Y-%m-%d") if hasattr(exit_dt, "strftime") else str(exit_dt)
            r["exit_price"] = f"{exit_px:.2f}"
            r["r_multiple"] = f"{r_mult:.2f}"
            r["days_held"]  = str((exit_dt - added_dt).days) if hasattr(exit_dt, "strftime") else ""
            stats["resolved_today"] += 1
            if resolved == _STATUS_WIN:         stats["wins"] += 1
            elif resolved == _STATUS_LOSS:      stats["losses"] += 1
            elif resolved == _STATUS_TIME_EXIT: stats["time_exits"] += 1
            changed = True

    if changed:
        _write_all(rows)
    return stats


# ─── PUBLIC: telegram summary ───────────────────────────────────────────────
def _bucket_verdict(actual_wr: float, expected_wr: float,
                    resolved: int) -> str:
    """Compact verdict string comparing actual to backtest prediction."""
    if resolved < 10:
        return f"⏳ n={resolved} (need ~20 for verdict)"
    diff = actual_wr - expected_wr
    if abs(diff) <= 5.0:
        return f"✅ matches backtest ({expected_wr:.0f}% pred)"
    if diff > 5.0:
        return f"⚠️ BETTER than backtest ({expected_wr:.0f}% pred, +{diff:.1f}pp)"
    return f"⚠️ WORSE than backtest ({expected_wr:.0f}% pred, {diff:.1f}pp)"


def _bucket_stats(rows: List[dict], bucket: str) -> dict:
    b_rows   = [r for r in rows if r.get("bucket") == bucket]
    total    = len(b_rows)
    pending  = sum(1 for r in b_rows if r.get("status") == _STATUS_PENDING)
    wins     = sum(1 for r in b_rows if r.get("status") == _STATUS_WIN)
    losses   = sum(1 for r in b_rows if r.get("status") == _STATUS_LOSS)
    time_ex  = sum(1 for r in b_rows if r.get("status") == _STATUS_TIME_EXIT)
    resolved = wins + losses + time_ex
    wr       = (wins / resolved * 100.0) if resolved else 0.0
    total_r  = 0.0
    for r in b_rows:
        if r.get("status") in (_STATUS_WIN, _STATUS_LOSS, _STATUS_TIME_EXIT):
            try:
                total_r += float(r.get("r_multiple") or 0)
            except Exception:
                pass
    exp_r = (total_r / resolved) if resolved else 0.0
    return {
        "total": total, "pending": pending, "resolved": resolved,
        "wins": wins, "losses": losses, "time_exits": time_ex,
        "wr": wr, "exp_r": exp_r,
    }


def format_shadow_summary(max_pending_shown: int = 3) -> str:
    """Multi-line Telegram-friendly summary, one line per non-empty bucket.

    Uses only ROWS ALREADY IN THE CSV — call update_shadow_outcomes() first.
    Returns "" if shadow logging disabled or CSV empty.
    """
    if not SHADOW_ENABLED:
        return ""
    rows = _read_all()
    if not rows:
        return ""

    total_all   = len(rows)
    pending_all = sum(1 for r in rows if r.get("status") == _STATUS_PENDING)
    resolved_all = total_all - pending_all

    lines = [
        "🔬 SHADOW LOG (Phase I observation — 4-bucket)",
        f"  Total: {total_all} · Pending: {pending_all} · Resolved: {resolved_all}",
    ]

    # Per-bucket rollups
    for bucket in (BUCKET_A, BUCKET_B, BUCKET_C, BUCKET_D):
        s = _bucket_stats(rows, bucket)
        if s["total"] == 0:
            continue
        expected = _BUCKET_EXPECTED_WR[bucket]
        verdict  = _bucket_verdict(s["wr"], expected, s["resolved"])
        # Phase Polish (2026-07-11): suppress "wr=0.0%" when n<10 resolved.
        wr_display = f"{s['wr']:5.1f}%" if s["resolved"] >= 10 else "  —  "
        lines.append(
            f"  · {_BUCKET_NAME[bucket]:<20} "
            f"n={s['total']:>4} · pend={s['pending']:>3} · "
            f"wr={wr_display} · exp={s['exp_r']:+.2f}R · {verdict}"
        )

    # Show top 3 most recent PENDING for transparency (across all buckets)
    pending_rows = [r for r in rows if r.get("status") == _STATUS_PENDING]
    pending_rows.sort(key=lambda r: r.get("date_added", ""), reverse=True)
    if pending_rows:
        lines.append("  Recent pending:")
        for r in pending_rows[:max_pending_shown]:
            lines.append(
                f"    [{r.get('bucket','?')}] {r.get('date_added','')} "
                f"{r.get('symbol',''):<12} {r.get('setup',''):<9} "
                f"conf {r.get('conf','')} [{r.get('regime','')}]"
            )

    return "\n".join(lines)


# ─── Telegram-formatted per-bucket brief (2026-07-07) ───────────────────────
# Rich HTML-friendly block that shows WHAT was added tonight in each bucket,
# plus resolved outcomes from the last run. This is the block that lets you
# actually *see* the observation happening from your phone.
def format_shadow_telegram(top_n_per_bucket: int = 5,
                            show_resolved_today: bool = True) -> str:
    """Full Telegram-oriented per-bucket brief.

    - Per-bucket header with n / wr / verdict
    - Up to `top_n_per_bucket` most-recent PENDING rows per bucket
    - Optionally lists RESOLVED wins/losses from the current day
    - Uses simple text (no markup) so it renders identically in Telegram
      HTML mode with parse_mode disabled or enabled
    """
    if not SHADOW_ENABLED:
        return ""
    rows = _read_all()
    if not rows:
        return ""

    total_all   = len(rows)
    pending_all = sum(1 for r in rows if r.get("status") == _STATUS_PENDING)
    resolved_all = total_all - pending_all

    lines = [
        "🔬 <b>SHADOW LOG</b> (Phase I observation — 4-bucket)",
        f"Total: {total_all} · Pending: {pending_all} · Resolved: {resolved_all}",
        "",
    ]

    _icons = {"A": "🎯", "B": "👀", "C": "🚫", "D": "🎚️"}

    # Per-bucket detail block
    for bucket in (BUCKET_A, BUCKET_B, BUCKET_C, BUCKET_D):
        b_rows = [r for r in rows if r.get("bucket") == bucket]
        if not b_rows:
            continue
        s = _bucket_stats(rows, bucket)
        expected = _BUCKET_EXPECTED_WR[bucket]
        verdict  = _bucket_verdict(s["wr"], expected, s["resolved"])
        icon     = _icons.get(bucket, "•")

        # Phase Polish (2026-07-11): suppress the alarming "wr=0.0%" display
        # when we haven't resolved enough trades yet. 0.0% reads as a total
        # failure but really means "no data yet". Threshold 10 matches
        # _bucket_verdict()'s "need ~20" cliff.
        wr_display = f"{s['wr']:.1f}%" if s["resolved"] >= 10 else "—"
        lines.append(
            f"{icon} <b>{_BUCKET_NAME[bucket]}</b> "
            f"· n={s['total']} · pend={s['pending']} · "
            f"wr={wr_display} (exp {expected:.0f}%) · {verdict}"
        )

        # Show most-recent pending rows for this bucket
        b_pending = [r for r in b_rows if r.get("status") == _STATUS_PENDING]
        b_pending.sort(key=lambda r: r.get("date_added", ""), reverse=True)
        for r in b_pending[:top_n_per_bucket]:
            lines.append(
                f"   • {r.get('date_added','')} "
                f"<code>{r.get('symbol','')}</code> · "
                f"{r.get('setup','')} · conf {r.get('conf','')} · "
                f"[{r.get('regime','')}] @ ₹{r.get('entry','')}"
            )
        if len(b_pending) > top_n_per_bucket:
            lines.append(f"   … +{len(b_pending) - top_n_per_bucket} more")
        lines.append("")

    # Resolved-today section (WINs and LOSSes from most recent resolution)
    if show_resolved_today and resolved_all > 0:
        # "Today" = rows resolved on the most recent exit_date
        resolved_rows = [r for r in rows
                         if r.get("status") in (_STATUS_WIN, _STATUS_LOSS,
                                                 _STATUS_TIME_EXIT)
                         and r.get("exit_date")]
        if resolved_rows:
            resolved_rows.sort(key=lambda r: r.get("exit_date", ""),
                               reverse=True)
            latest_date = resolved_rows[0].get("exit_date", "")
            today_resolved = [r for r in resolved_rows
                              if r.get("exit_date") == latest_date]
            if today_resolved:
                wins   = [r for r in today_resolved
                          if r.get("status") == _STATUS_WIN]
                losses = [r for r in today_resolved
                          if r.get("status") == _STATUS_LOSS]
                lines.append(
                    f"📌 <b>Resolved {latest_date}</b> "
                    f"({len(wins)}W / {len(losses)}L / "
                    f"{len(today_resolved) - len(wins) - len(losses)}TE)"
                )
                for r in today_resolved[:6]:
                    _st_icon = "✅" if r.get("status") == _STATUS_WIN else (
                        "❌" if r.get("status") == _STATUS_LOSS else "⏱️")
                    lines.append(
                        f"   {_st_icon} [{r.get('bucket','?')}] "
                        f"<code>{r.get('symbol','')}</code> · "
                        f"R={r.get('r_multiple','')} · "
                        f"{r.get('days_held','')}d"
                    )
                if len(today_resolved) > 6:
                    lines.append(f"   … +{len(today_resolved) - 6} more")

    return "\n".join(lines).rstrip()


# ─── CLI entrypoint (debug / cron) ──────────────────────────────────────────
if __name__ == "__main__":
    print("[shadow_log] CLI: updating outcomes...")
    s = update_shadow_outcomes(quiet=False)
    print(f"[shadow_log] stats: {s}")
    print()
    print(format_shadow_summary())
