"""shadow_weekly_job.py — Weekly 7-sheet Excel rollup for Phase I shadow log.

Generates `shadow_report_weekly.xlsx` every Saturday morning (~9 AM IST).
Purpose: detect which bucket is quietly outperforming so you can decide
whether to relax filters — with a "suggestions only" disclaimer.

Sheets:
  1. Week_at_a_glance    — this week vs last week per bucket
  2. Bucket_Trends       — 4-week rolling win rate per bucket
  3. Setup_x_Regime      — cross-tab heatmap of live win rates
  4. Outperformers       — buckets beating backtest by >5pp (n>=20)
  5. Underperformers     — buckets missing backtest by >5pp (n>=20)
  6. Recommendations     — auto-generated tuning suggestions (text only)
  7. All_Trades_History  — full shadow_trades.csv dump

Also archives to `reports/archive/weekly/shadow_report_weekly_YYYY-MM-DD.xlsx`.

Reads shadow_trades.csv read-only. Never modifies source data.
Never mutates any filter — Recommendations are suggestions only.
"""
from __future__ import annotations

import csv
import os
import shutil
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    _OPENPYXL_OK = True
except ImportError:
    _OPENPYXL_OK = False

try:
    import shadow_log
    _SHADOW_LOG_OK = True
except ImportError:
    _SHADOW_LOG_OK = False

# ─── Config ──────────────────────────────────────────────────────────────
SHADOW_CSV_PATH   = os.getenv("SHADOW_CSV_PATH", "shadow_trades.csv")
WEEKLY_XLSX_PATH  = os.getenv("SHADOW_WEEKLY_PATH", "shadow_report_weekly.xlsx")
ARCHIVE_DIR       = os.getenv("SHADOW_WEEKLY_ARCHIVE_DIR", "reports/archive/weekly")

# Verdict / recommendation thresholds
MIN_N_FOR_VERDICT = int(os.getenv("SHADOW_MIN_N_FOR_VERDICT", "20"))
DELTA_PP_THRESHOLD = float(os.getenv("SHADOW_DELTA_PP_THRESHOLD", "5.0"))

# ─── Styling ─────────────────────────────────────────────────────────────
_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
_HEADER_FILL = PatternFill(start_color="2F5F8F", end_color="2F5F8F", fill_type="solid")
_TITLE_FONT  = Font(bold=True, size=14, color="1F3864")
_SUBTITLE_FONT = Font(italic=True, size=10, color="7F8C8D")

_BUCKET_COLORS = {"A": "1E7C3A", "B": "F1C40F", "C": "95A5A6", "D": "3498DB"}

_FILL_GREEN  = PatternFill(start_color="D5F5E3", end_color="D5F5E3", fill_type="solid")
_FILL_RED    = PatternFill(start_color="FADBD8", end_color="FADBD8", fill_type="solid")
_FILL_YELLOW = PatternFill(start_color="FCF3CF", end_color="FCF3CF", fill_type="solid")
_FILL_GRAY   = PatternFill(start_color="F2F3F4", end_color="F2F3F4", fill_type="solid")

_BORDER_THIN = Border(
    left=Side(style="thin", color="BDC3C7"),
    right=Side(style="thin", color="BDC3C7"),
    top=Side(style="thin", color="BDC3C7"),
    bottom=Side(style="thin", color="BDC3C7"),
)
_CENTER = Alignment(horizontal="center", vertical="center")
_LEFT   = Alignment(horizontal="left", vertical="center")


# ─── Data helpers ────────────────────────────────────────────────────────
def _load_rows(csv_path: str) -> List[Dict]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _iso_week(d: date) -> str:
    """Return ISO year-week label like '2026-W28'."""
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def _stats_for(rows: List[Dict]) -> Dict:
    """Compute n / resolved / wins / losses / time_exits / wr / avg_r."""
    n = len(rows)
    pending = sum(1 for r in rows if r.get("status") == "PENDING")
    resolved = n - pending
    wins   = sum(1 for r in rows if r.get("status") == "WIN")
    losses = sum(1 for r in rows if r.get("status") == "LOSS")
    exits  = sum(1 for r in rows if r.get("status") == "TIME_EXIT")
    wr = (wins / resolved * 100) if resolved > 0 else 0.0
    r_vals = []
    for r in rows:
        try:
            r_vals.append(float(r.get("r_multiple") or 0))
        except (TypeError, ValueError):
            pass
    avg_r = (sum(r_vals) / len(r_vals)) if r_vals else 0.0
    return {"n": n, "pending": pending, "resolved": resolved,
            "wins": wins, "losses": losses, "time_exits": exits,
            "wr": wr, "avg_r": avg_r}


def _split_by_week(rows: List[Dict]) -> Dict[str, List[Dict]]:
    """Bucket rows by ISO week of date_added."""
    out = defaultdict(list)
    for r in rows:
        d = _parse_date(r.get("date_added", ""))
        if d:
            out[_iso_week(d)].append(r)
    return dict(out)


def _apply_header(ws, row_idx: int, headers: List[str],
                  fill: PatternFill | None = None):
    header_fill = fill if fill is not None else _HEADER_FILL
    for c_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=row_idx, column=c_idx, value=h)
        cell.font = _HEADER_FONT
        cell.fill = header_fill
        cell.alignment = _CENTER
        cell.border = _BORDER_THIN


def _autosize(ws, min_width: int = 10, max_width: int = 32):
    for col_idx in range(1, ws.max_column + 1):
        letter = get_column_letter(col_idx)
        longest = 0
        for row in ws.iter_rows(min_col=col_idx, max_col=col_idx):
            for cell in row:
                if cell.value is None:
                    continue
                longest = max(longest, len(str(cell.value)))
        ws.column_dimensions[letter].width = max(min_width, min(max_width, longest + 2))


# ─── Sheet 1: Week at a glance ───────────────────────────────────────────
def _build_week_glance(ws, rows: List[Dict], today: date):
    ws["A1"] = "WEEK AT A GLANCE — this week vs last week per bucket"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:K1")

    this_week = _iso_week(today)
    last_week = _iso_week(today - timedelta(days=7))

    ws["A2"] = f"This week: {this_week}  |  Last week: {last_week}  |  Generated: {datetime.now():%Y-%m-%d %H:%M IST}"
    ws["A2"].font = _SUBTITLE_FONT
    ws.merge_cells("A2:K2")

    by_week = _split_by_week(rows)
    this_rows = by_week.get(this_week, [])
    last_rows = by_week.get(last_week, [])

    headers = ["Bucket", "Name",
               "This n", "This WR", "This Avg R",
               "Last n", "Last WR", "Last Avg R",
               "Δ n", "Δ WR pp", "Trend"]
    _apply_header(ws, 4, headers)

    r_idx = 5
    for letter in ("A", "B", "C", "D"):
        this_b = [r for r in this_rows if (r.get("bucket") or "").upper().rstrip("_LEGACY") == letter or (r.get("bucket") == "B_LEGACY" and letter == "B")]
        last_b = [r for r in last_rows if (r.get("bucket") or "").upper().rstrip("_LEGACY") == letter or (r.get("bucket") == "B_LEGACY" and letter == "B")]
        s_this = _stats_for(this_b)
        s_last = _stats_for(last_b)

        d_n = s_this["n"] - s_last["n"]
        d_wr = s_this["wr"] - s_last["wr"]
        trend = "↑" if d_wr > 2 else ("↓" if d_wr < -2 else "→")
        fill = _FILL_GREEN if d_wr > 2 else (_FILL_RED if d_wr < -2 else _FILL_GRAY)

        name = _SHADOW_LOG_OK and shadow_log._BUCKET_NAME.get(letter, letter) or letter
        vals = [letter, name,
                s_this["n"], f"{s_this['wr']:.1f}%", f"{s_this['avg_r']:+.2f}R",
                s_last["n"], f"{s_last['wr']:.1f}%", f"{s_last['avg_r']:+.2f}R",
                f"{d_n:+d}", f"{d_wr:+.1f}pp", trend]
        for c_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = _CENTER if c_idx > 2 else _LEFT
            if c_idx == 1:
                cell.fill = PatternFill(start_color=_BUCKET_COLORS[letter],
                                          end_color=_BUCKET_COLORS[letter],
                                          fill_type="solid")
                cell.font = Font(bold=True, color="FFFFFF")
            elif c_idx in (9, 10, 11):
                cell.fill = fill
        r_idx += 1

    _autosize(ws)


# ─── Sheet 2: 4-week rolling trends ──────────────────────────────────────
def _build_bucket_trends(ws, rows: List[Dict], today: date):
    ws["A1"] = "BUCKET TRENDS — last 4 weeks rolling win rate per bucket"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:F1")

    weeks = [_iso_week(today - timedelta(days=7 * i)) for i in range(3, -1, -1)]
    by_week = _split_by_week(rows)

    headers = ["Bucket"] + [f"{w} n / WR" for w in weeks]
    _apply_header(ws, 3, headers)

    r_idx = 4
    for letter in ("A", "B", "C", "D"):
        cells = [letter]
        for w in weeks:
            w_rows = [r for r in by_week.get(w, [])
                       if (r.get("bucket") or "").upper() == letter
                       or (letter == "B" and r.get("bucket") == "B_LEGACY")]
            s = _stats_for(w_rows)
            cells.append(f"n={s['n']}  WR={s['wr']:.0f}%")

        for c_idx, v in enumerate(cells, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = _CENTER if c_idx > 1 else _LEFT
            if c_idx == 1:
                cell.fill = PatternFill(start_color=_BUCKET_COLORS[letter],
                                          end_color=_BUCKET_COLORS[letter],
                                          fill_type="solid")
                cell.font = Font(bold=True, color="FFFFFF")
        r_idx += 1

    r_idx += 2
    ws.cell(row=r_idx, column=1,
            value="Read the trend left→right: is the bucket's win rate improving or eroding week over week?").font = _SUBTITLE_FONT
    ws.merge_cells(start_row=r_idx, start_column=1, end_row=r_idx, end_column=6)

    _autosize(ws, min_width=14, max_width=28)


# ─── Sheet 3: Setup × Regime cross-tab ───────────────────────────────────
def _build_setup_regime(ws, rows: List[Dict]):
    ws["A1"] = "SETUP × REGIME — live win rate cross-tab (all buckets combined)"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:H1")

    # Aggregate (setup, regime) → wins / resolved
    grid = defaultdict(lambda: {"resolved": 0, "wins": 0, "n": 0})
    setups = set()
    regimes = set()
    for r in rows:
        s = (r.get("setup") or "OTHER").upper()
        rg = (r.get("regime") or "?").upper()
        setups.add(s)
        regimes.add(rg)
        g = grid[(s, rg)]
        g["n"] += 1
        if r.get("status") in ("WIN", "LOSS", "TIME_EXIT"):
            g["resolved"] += 1
            if r.get("status") == "WIN":
                g["wins"] += 1

    setups = sorted(setups)
    regimes = sorted(regimes)

    headers = ["Setup \\ Regime"] + regimes + ["Total n", "Overall WR"]
    _apply_header(ws, 3, headers)

    r_idx = 4
    for s in setups:
        row_total_n = 0
        row_total_wins = 0
        row_total_resolved = 0
        cells = [s]
        for rg in regimes:
            g = grid.get((s, rg), {"resolved": 0, "wins": 0, "n": 0})
            row_total_n += g["n"]
            row_total_resolved += g["resolved"]
            row_total_wins += g["wins"]
            if g["resolved"] >= 5:
                wr = g["wins"] / g["resolved"] * 100
                cells.append(f"{wr:.0f}% (n={g['resolved']})")
            elif g["n"] > 0:
                cells.append(f"pending (n={g['n']})")
            else:
                cells.append("—")
        overall_wr = (row_total_wins / row_total_resolved * 100) if row_total_resolved else 0.0
        cells.append(row_total_n)
        cells.append(f"{overall_wr:.0f}% (n={row_total_resolved})" if row_total_resolved else "—")

        for c_idx, v in enumerate(cells, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = _CENTER if c_idx > 1 else _LEFT
            # Color code by win rate strength
            txt = str(v)
            if "%" in txt and "n=" in txt:
                try:
                    pct = float(txt.split("%")[0])
                    if pct >= 50:
                        cell.fill = _FILL_GREEN
                    elif pct >= 30:
                        cell.fill = _FILL_YELLOW
                    else:
                        cell.fill = _FILL_RED
                except (ValueError, IndexError):
                    pass
        r_idx += 1

    _autosize(ws, min_width=12, max_width=26)


# ─── Sheets 4 & 5: Outperformers / Underperformers ───────────────────────
def _compute_bucket_deltas(rows: List[Dict]) -> List[Dict]:
    """For each bucket, compute (live_wr - expected_wr) with n gate."""
    out = []
    for letter in ("A", "B", "C", "D"):
        b_rows = [r for r in rows
                   if (r.get("bucket") or "").upper() == letter
                   or (letter == "B" and r.get("bucket") == "B_LEGACY")]
        s = _stats_for(b_rows)
        expected = _SHADOW_LOG_OK and shadow_log._BUCKET_EXPECTED_WR.get(letter, 0.0) or 0.0
        delta = s["wr"] - expected if s["resolved"] >= MIN_N_FOR_VERDICT else None
        out.append({
            "bucket": letter,
            "name": _SHADOW_LOG_OK and shadow_log._BUCKET_NAME.get(letter, letter) or letter,
            "n": s["n"], "resolved": s["resolved"],
            "wr": s["wr"], "expected": expected, "delta": delta,
            "avg_r": s["avg_r"],
        })
    return out


def _build_outperformers(ws, deltas: List[Dict]):
    ws["A1"] = "OUTPERFORMERS — buckets beating backtest by more than 5pp"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:H1")
    ws["A2"] = f"Threshold: delta > +{DELTA_PP_THRESHOLD}pp AND n ≥ {MIN_N_FOR_VERDICT} resolved"
    ws["A2"].font = _SUBTITLE_FONT
    ws.merge_cells("A2:H2")

    headers = ["Bucket", "Name", "n Resolved", "Live WR", "Expected WR", "Δ pp", "Avg R", "Interpretation"]
    _apply_header(ws, 4, headers)

    r_idx = 5
    outs = [d for d in deltas if d["delta"] is not None and d["delta"] > DELTA_PP_THRESHOLD]
    if not outs:
        ws.cell(row=r_idx, column=1, value=f"(no buckets outperforming by >+{DELTA_PP_THRESHOLD}pp with n≥{MIN_N_FOR_VERDICT} yet)").font = _SUBTITLE_FONT
        _autosize(ws)
        return

    for d in sorted(outs, key=lambda x: -x["delta"]):
        interp = _interpret_outperformer(d)
        vals = [d["bucket"], d["name"], d["resolved"],
                f"{d['wr']:.1f}%", f"{d['expected']:.0f}%",
                f"+{d['delta']:.1f}pp", f"{d['avg_r']:+.2f}R", interp]
        for c_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = _CENTER if c_idx not in (2, 8) else _LEFT
            cell.fill = _FILL_GREEN
        r_idx += 1

    _autosize(ws, max_width=50)


def _build_underperformers(ws, deltas: List[Dict]):
    ws["A1"] = "UNDERPERFORMERS — buckets missing backtest by more than 5pp"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:H1")
    ws["A2"] = f"Threshold: delta < -{DELTA_PP_THRESHOLD}pp AND n ≥ {MIN_N_FOR_VERDICT} resolved"
    ws["A2"].font = _SUBTITLE_FONT
    ws.merge_cells("A2:H2")

    headers = ["Bucket", "Name", "n Resolved", "Live WR", "Expected WR", "Δ pp", "Avg R", "Interpretation"]
    _apply_header(ws, 4, headers)

    r_idx = 5
    unders = [d for d in deltas if d["delta"] is not None and d["delta"] < -DELTA_PP_THRESHOLD]
    if not unders:
        ws.cell(row=r_idx, column=1, value=f"(no buckets underperforming by >-{DELTA_PP_THRESHOLD}pp with n≥{MIN_N_FOR_VERDICT} yet)").font = _SUBTITLE_FONT
        _autosize(ws)
        return

    for d in sorted(unders, key=lambda x: x["delta"]):
        interp = _interpret_underperformer(d)
        vals = [d["bucket"], d["name"], d["resolved"],
                f"{d['wr']:.1f}%", f"{d['expected']:.0f}%",
                f"{d['delta']:+.1f}pp", f"{d['avg_r']:+.2f}R", interp]
        for c_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = _CENTER if c_idx not in (2, 8) else _LEFT
            cell.fill = _FILL_RED
        r_idx += 1

    _autosize(ws, max_width=50)


def _interpret_outperformer(d: Dict) -> str:
    b = d["bucket"]
    if b == "B":
        return "Chop-regime skips are winning too often — regime filter may be too strict"
    if b == "C":
        return "Wrong-setup stocks winning — backtest may be pessimistic on these setups"
    if b == "D":
        return "Near-miss confidence rejects winning — consider lowering min_confidence"
    if b == "A":
        return "Real BUYs exceeding expectation — edge is intact"
    return "Bucket exceeding expectation — inspect setup+regime mix"


def _interpret_underperformer(d: Dict) -> str:
    b = d["bucket"]
    if b == "A":
        return "Real BUYs underperforming — regime or setup filters may be too loose"
    if b == "B":
        return "Chop-regime skips also losing — regime filter working as intended"
    if b == "C":
        return "Wrong-setup stocks losing — filter correctly rejecting these"
    if b == "D":
        return "Near-miss rejects losing — min_confidence threshold is well-calibrated"
    return "Bucket missing expectation — no action"


# ─── Sheet 6: Recommendations (suggestions only) ─────────────────────────
def _build_recommendations(ws, deltas: List[Dict], rows: List[Dict]):
    ws["A1"] = "RECOMMENDATIONS — SUGGESTIONS ONLY (no filter is auto-modified)"
    ws["A1"].font = Font(bold=True, size=14, color="C0392B")
    ws.merge_cells("A1:D1")

    ws["A2"] = ("These are data-driven suggestions based on the current shadow log. "
                "You decide whether to act. Buckets need n ≥ "
                f"{MIN_N_FOR_VERDICT} resolved trades before a suggestion is generated.")
    ws["A2"].font = _SUBTITLE_FONT
    ws["A2"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A2:D2")
    ws.row_dimensions[2].height = 40

    headers = ["Priority", "Bucket", "Suggestion", "Basis"]
    _apply_header(ws, 4, headers)

    recs = _generate_recommendations(deltas, rows)
    if not recs:
        ws.cell(row=5, column=1,
                value=f"(no actionable suggestions yet — need n ≥ {MIN_N_FOR_VERDICT} resolved per bucket)").font = _SUBTITLE_FONT
        _autosize(ws, max_width=60)
        return

    for r_idx, rec in enumerate(recs, start=5):
        vals = [rec["priority"], rec["bucket"], rec["suggestion"], rec["basis"]]
        for c_idx, v in enumerate(vals, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.border = _BORDER_THIN
            cell.alignment = Alignment(wrap_text=True, vertical="top",
                                        horizontal="left" if c_idx >= 3 else "center")
            if c_idx == 1:
                if rec["priority"] == "HIGH":
                    cell.fill = _FILL_RED
                    cell.font = Font(bold=True, color="C0392B")
                elif rec["priority"] == "MEDIUM":
                    cell.fill = _FILL_YELLOW
                else:
                    cell.fill = _FILL_GREEN
        ws.row_dimensions[r_idx].height = 60

    # Footer disclaimer
    r_idx = 5 + len(recs) + 2
    ws.cell(row=r_idx, column=1,
            value="⚠️ DISCLAIMER: These recommendations are informational only. The trading pipeline does NOT auto-apply any of them. All filter changes require your manual review and edit of the code.").font = Font(italic=True, size=9, color="C0392B")
    ws.merge_cells(start_row=r_idx, start_column=1, end_row=r_idx, end_column=4)

    ws.column_dimensions["A"].width = 12
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 60
    ws.column_dimensions["D"].width = 40


def _generate_recommendations(deltas: List[Dict], rows: List[Dict]) -> List[Dict]:
    """Deterministic rule-based recommendations. No ML, no auto-apply."""
    recs = []
    for d in deltas:
        if d["delta"] is None:
            continue
        b = d["bucket"]
        abs_delta = abs(d["delta"])
        if abs_delta < DELTA_PP_THRESHOLD:
            continue

        priority = "HIGH" if abs_delta >= 15 else ("MEDIUM" if abs_delta >= 10 else "LOW")

        if d["delta"] > 0:  # Outperformer
            if b == "B":
                sugg = ("Bucket B (WATCH_ME) is winning more than backtest predicted. "
                        "Consider relaxing the regime filter — the chop-regime skip may be "
                        "throwing out real signals. Try allowing MOMENTUM setups when "
                        "regime=SIDEWAYS but breadth > 60%.")
            elif b == "D":
                sugg = ("Bucket D (SO_CLOSE) is winning more than backtest predicted. "
                        "Consider lowering min_confidence by 2-3 points. Near-miss rejects "
                        "at conf 65-69 are performing like real BUYs.")
            elif b == "C":
                sugg = ("Bucket C (NOT_MY_STYLE) is winning more than backtest predicted. "
                        "One of the excluded setups (PULLBACK / REVERSAL / OTHER) may deserve "
                        "its own bonus. Cross-check Setup × Regime tab to see which setup is driving this.")
            else:  # A
                sugg = ("Bucket A (real BUYs) is exceeding backtest expectations. Edge is intact. "
                        "No action needed — keep the current filters.")
        else:  # Underperformer
            if b == "A":
                sugg = ("Bucket A (real BUYs) is underperforming backtest. Filters may be too "
                        "loose or regime detection is off. Review recent losses in A_TAKEN tab "
                        "to spot pattern.")
            elif b == "D":
                sugg = ("Bucket D (SO_CLOSE) is losing more than backtest predicted. min_confidence "
                        "threshold is well-calibrated. Do NOT lower it.")
            else:
                sugg = (f"Bucket {b} is underperforming backtest — filter is correctly rejecting "
                        f"these. No action needed.")

        recs.append({
            "priority": priority,
            "bucket": b,
            "suggestion": sugg,
            "basis": f"n={d['resolved']} resolved, live WR {d['wr']:.1f}% vs expected {d['expected']:.0f}% (Δ {d['delta']:+.1f}pp)",
        })

    # Sort HIGH > MEDIUM > LOW
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    recs.sort(key=lambda x: order.get(x["priority"], 3))
    return recs


# ─── Sheet 7: Full history ───────────────────────────────────────────────
def _build_all_history(ws, rows: List[Dict]):
    ws["A1"] = "ALL TRADES — full shadow_trades.csv history"
    ws["A1"].font = _TITLE_FONT
    ws.merge_cells("A1:O1")

    if not rows:
        ws["A3"] = "(no shadow trades yet)"
        ws["A3"].font = _SUBTITLE_FONT
        return

    cols = list(rows[0].keys())
    _apply_header(ws, 3, cols)

    for r_idx, r in enumerate(rows, start=4):
        for c_idx, k in enumerate(cols, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=r.get(k, ""))
            cell.border = _BORDER_THIN
            cell.alignment = _LEFT
        # Row fill by status
        s = (r.get("status") or "").upper()
        fill = None
        if s == "WIN":       fill = _FILL_GREEN
        elif s == "LOSS":    fill = _FILL_RED
        elif s == "TIME_EXIT": fill = _FILL_YELLOW
        elif s == "PENDING": fill = _FILL_GRAY
        if fill:
            for c_idx in range(1, len(cols) + 1):
                ws.cell(row=r_idx, column=c_idx).fill = fill

    _autosize(ws)


# ─── Public API ──────────────────────────────────────────────────────────
def generate_weekly_report(csv_path: str = None,
                             xlsx_path: str = None,
                             archive_dir: str = None,
                             quiet: bool = False) -> Dict:
    """Build shadow_report_weekly.xlsx + archived dated copy."""
    csv_path    = csv_path    or SHADOW_CSV_PATH
    xlsx_path   = xlsx_path   or WEEKLY_XLSX_PATH
    archive_dir = archive_dir or ARCHIVE_DIR

    if not _OPENPYXL_OK:
        msg = "openpyxl not available — install to enable Excel reports"
        if not quiet:
            print(f"[shadow_weekly] {msg}")
        return {"ok": False, "error": msg}

    rows = _load_rows(csv_path)
    if not quiet:
        print(f"[shadow_weekly] Loaded {len(rows)} rows from {csv_path}")

    today = date.today()
    deltas = _compute_bucket_deltas(rows)

    wb = Workbook()

    ws1 = wb.active
    ws1.title = "Week_at_a_glance"
    _build_week_glance(ws1, rows, today)

    ws2 = wb.create_sheet("Bucket_Trends")
    _build_bucket_trends(ws2, rows, today)

    ws3 = wb.create_sheet("Setup_x_Regime")
    _build_setup_regime(ws3, rows)

    ws4 = wb.create_sheet("Outperformers")
    _build_outperformers(ws4, deltas)

    ws5 = wb.create_sheet("Underperformers")
    _build_underperformers(ws5, deltas)

    ws6 = wb.create_sheet("Recommendations")
    _build_recommendations(ws6, deltas, rows)

    ws7 = wb.create_sheet("All_Trades_History")
    _build_all_history(ws7, rows)

    wb.save(xlsx_path)
    if not quiet:
        print(f"[shadow_weekly] Wrote {xlsx_path}")

    archived_path = None
    try:
        Path(archive_dir).mkdir(parents=True, exist_ok=True)
        archived_path = os.path.join(archive_dir,
                                       f"shadow_report_weekly_{today.isoformat()}.xlsx")
        shutil.copy2(xlsx_path, archived_path)
        if not quiet:
            print(f"[shadow_weekly] Archived {archived_path}")
    except Exception as ex:
        if not quiet:
            print(f"[shadow_weekly] Archive failed: {ex}")

    n_recs = sum(1 for d in deltas if d["delta"] is not None
                 and abs(d["delta"]) >= DELTA_PP_THRESHOLD)

    return {
        "ok": True,
        "xlsx_path": xlsx_path,
        "archived_path": archived_path,
        "n_rows": len(rows),
        "n_recommendations": n_recs,
        "deltas": [{"bucket": d["bucket"], "delta": d["delta"],
                    "wr": d["wr"], "n": d["resolved"]}
                   for d in deltas],
    }


# ─── CLI ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = generate_weekly_report()
    if result["ok"]:
        print(f"\nWeekly report generated. Recommendations: {result['n_recommendations']}")
        for d in result["deltas"]:
            delta_str = f"{d['delta']:+.1f}pp" if d["delta"] is not None else "n<20"
            print(f"  Bucket {d['bucket']}: n={d['n']} · WR={d['wr']:.1f}% · Δ={delta_str}")
    else:
        print(f"FAILED: {result.get('error', 'unknown')}")
