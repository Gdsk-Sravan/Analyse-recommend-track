"""
research_job.py — Research & Analytics (JOB 3)
===============================================
Run weekly (Saturdays) and monthly.
GitHub Actions: schedule 0 4 * * 6 (Saturdays 9:30 AM IST / 4:00 UTC)

Reads:  recommendation_tracker.xlsx
Writes: recommendation_tracker.xlsx — Analysis sheets updated

Usage:
    python research_job.py
"""

import os
import numpy as np
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import pandas as pd
    _PD_OK = True
except ImportError:
    _PD_OK = False
    print("[ERROR] pandas not installed. Run: pip install pandas")

try:
    import openpyxl
    _OPENPYXL_OK = True
except ImportError:
    _OPENPYXL_OK = False
    print("[ERROR] openpyxl not installed. Run: pip install openpyxl")

TRACKER_XLSX = os.getenv("TRACKER_XLSX", "recommendation_tracker.xlsx")


def run_research():
    print(f"=== RESEARCH JOB: {datetime.now().strftime('%Y-%m-%d')} ===")

    if not _PD_OK or not _OPENPYXL_OK:
        print("[ERROR] Missing dependencies — aborting")
        return

    if not os.path.exists(TRACKER_XLSX):
        print(f"[WARN] No tracker file at {TRACKER_XLSX} — nothing to analyze yet")
        return

    try:
        wb = openpyxl.load_workbook(TRACKER_XLSX)
    except Exception as e:
        print(f"[ERROR] Could not open {TRACKER_XLSX}: {e}")
        return

    df_rec   = _sheet_to_df(wb, "Recommendations")
    df_track = _sheet_to_df(wb, "Daily Tracking")

    if df_rec.empty:
        print("[INFO] No recommendation data yet")
        wb.save(TRACKER_XLSX)
        return

    print(f"[INFO] Analyzing {len(df_rec)} recommendations, {len(df_track)} tracking records")

    # Auto-create v2 research sheets if missing (for older tracker files)
    _ensure_sheets(wb, [
        "Weekday Analysis", "Holding Period Analysis", "Category Comparison",
        "Conf x TQ Matrix", "Catalyst Analysis", "Fail Reason Analysis",
        "Regime x Sector", "Confidence Trajectory",
    ])

    # Confidence analysis
    _analyze_by_column(wb, df_rec, df_track, "Confidence", "Confidence Analysis",
                        [70, 75, 80, 83, 85, 88, 90, 95])

    # TQ analysis
    _analyze_by_column(wb, df_rec, df_track, "TQ", "TQ Analysis",
                        [70, 75, 80, 85, 90, 95])

    # Opportunity Score analysis
    _analyze_by_column(wb, df_rec, df_track, "Opp Score", "Opp Score Analysis",
                        [50, 60, 70, 75, 80, 85, 90])

    # Sector analysis
    _analyze_by_sector(wb, df_rec, df_track)

    # Regime analysis
    _analyze_by_regime(wb, df_rec, df_track)

    # Threshold simulation note
    _add_threshold_note(wb)

    # Monthly report
    _generate_monthly_report(wb, df_rec, df_track)

    # ── v2 research sheets ──
    _analyze_by_weekday(wb, df_rec, df_track)
    _analyze_holding_period(wb, df_rec, df_track)
    _analyze_category_comparison(wb, df_rec, df_track)
    _analyze_conf_tq_matrix(wb, df_rec, df_track)
    _analyze_catalysts(wb, df_rec, df_track)
    _analyze_fail_reasons(wb, df_rec, df_track)
    _analyze_regime_x_sector(wb, df_rec, df_track)
    _analyze_confidence_trajectory(wb, df_rec, df_track)

    wb.save(TRACKER_XLSX)
    print(f"[INFO] Research job complete — {TRACKER_XLSX} updated")


def _ensure_sheets(wb, names):
    """Create any sheets in the list that don't already exist."""
    existing = set(wb.sheetnames)
    for name in names:
        if name not in existing:
            wb.create_sheet(name)


def _sheet_to_df(wb, sheet_name):
    try:
        ws = wb[sheet_name]
        data = list(ws.values)
        if len(data) < 2:
            return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception:
        return pd.DataFrame()


def _analyze_by_column(wb, df_rec, df_track, col, sheet_name, thresholds):
    try:
        ws = wb[sheet_name]
        # Clear existing data (keep header)
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.value = None

        ws.append(["Range", "Count", "Avg Return%", "Median Return%",
                    "Win Rate%", "Avg Max Gain%", "T1 Hit%", "T2 Hit%"])

        merged = df_rec.copy()
        if not df_track.empty and "Ticker" in df_track.columns:
            best = (df_track
                    .assign(_day=pd.to_numeric(df_track.get("Day#", pd.Series(0)), errors="coerce"))
                    .sort_values("_day", ascending=False)
                    .drop_duplicates("Ticker"))
            for c in ("Return%", "Max Gain%", "T1 Hit", "T2 Hit"):
                if c in best.columns:
                    merged = merged.merge(best[["Ticker", c]], on="Ticker", how="left",
                                          suffixes=("", "_track"))

        for i in range(len(thresholds)):
            lo = thresholds[i]
            hi = thresholds[i + 1] if i + 1 < len(thresholds) else 9999
            col_series = pd.to_numeric(merged.get(col, pd.Series()), errors="coerce")
            subset = merged[(col_series >= lo) & (col_series < hi)]
            if subset.empty:
                continue

            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins    = (returns > 0).sum()
            mg      = pd.to_numeric(subset.get("Max Gain%", pd.Series()), errors="coerce").dropna()
            t1_hits = subset.get("T1 Hit", pd.Series(False)).astype(bool).sum()
            t2_hits = subset.get("T2 Hit", pd.Series(False)).astype(bool).sum()

            ws.append([
                f"{lo}-{hi}",
                len(subset),
                round(float(returns.mean()), 2) if len(returns) > 0 else 0,
                round(float(returns.median()), 2) if len(returns) > 0 else 0,
                round(float(wins / len(returns) * 100), 1) if len(returns) > 0 else 0,
                round(float(mg.mean()), 2) if len(mg) > 0 else 0,
                round(float(t1_hits / len(subset) * 100), 1),
                round(float(t2_hits / len(subset) * 100), 1),
            ])

    except Exception as e:
        print(f"[WARN] Analysis failed for {sheet_name}: {e}")


def _analyze_by_sector(wb, df_rec, df_track):
    try:
        ws = wb["Sector Analysis"]
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.value = None
        ws.append(["Sector", "Count", "Avg Return%", "Win Rate%", "Avg Max Gain%"])

        if df_rec.empty or "Sector" not in df_rec.columns:
            return

        merged = df_rec.copy()
        if not df_track.empty and "Ticker" in df_track.columns:
            best = (df_track
                    .assign(_day=pd.to_numeric(df_track.get("Day#", pd.Series(0)), errors="coerce"))
                    .sort_values("_day", ascending=False)
                    .drop_duplicates("Ticker"))
            merge_cols = ["Ticker"]
            for c in ("Return%", "Max Gain%"):
                if c in best.columns:
                    merge_cols.append(c)
            if len(merge_cols) > 1:
                merged = merged.merge(best[merge_cols], on="Ticker", how="left")

        for sector in df_rec["Sector"].dropna().unique():
            subset  = merged[merged["Sector"] == sector]
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            max_gain = pd.to_numeric(subset.get("Max Gain%", pd.Series()), errors="coerce").dropna()
            wins    = (returns > 0).sum()
            ws.append([
                sector,
                len(subset),
                round(float(returns.mean()), 2) if len(returns) > 0 else 0,
                round(float(wins / len(returns) * 100), 1) if len(returns) > 0 else 0,
                round(float(max_gain.mean()), 2) if len(max_gain) > 0 else 0,
            ])
    except Exception as e:
        print(f"[WARN] Sector analysis failed: {e}")


def _analyze_by_regime(wb, df_rec, df_track):
    try:
        ws = wb["Regime Analysis"]
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.value = None
        ws.append(["Regime", "Count", "Avg Return%", "Win Rate%", "Best Sector"])

        if df_rec.empty or "Regime" not in df_rec.columns:
            return

        merged = df_rec.copy()
        if not df_track.empty and "Ticker" in df_track.columns:
            best = (df_track
                    .assign(_day=pd.to_numeric(df_track.get("Day#", pd.Series(0)), errors="coerce"))
                    .sort_values("_day", ascending=False)
                    .drop_duplicates("Ticker"))
            if "Return%" in best.columns:
                merged = merged.merge(best[["Ticker", "Return%"]], on="Ticker", how="left")

        for regime in df_rec["Regime"].dropna().unique():
            subset  = merged[merged["Regime"] == regime]
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins    = (returns > 0).sum()

            # Best sector inside this regime = highest average Return%
            best_sector = "—"
            if "Sector" in subset.columns and not subset.empty:
                try:
                    tmp = subset.copy()
                    tmp["_ret"] = pd.to_numeric(tmp.get("Return%", pd.Series()), errors="coerce")
                    tmp = tmp.dropna(subset=["_ret", "Sector"])
                    if not tmp.empty:
                        sec_means = tmp.groupby("Sector")["_ret"].mean()
                        # only consider sectors with 2+ picks so we don't crown a single outlier
                        sec_counts = tmp.groupby("Sector")["_ret"].count()
                        eligible = sec_means[sec_counts >= 2]
                        if not eligible.empty:
                            best_sector = f"{eligible.idxmax()} ({eligible.max():+.1f}%)"
                        elif not sec_means.empty:
                            best_sector = f"{sec_means.idxmax()} ({sec_means.max():+.1f}%)"
                except Exception:
                    best_sector = "—"

            ws.append([
                regime,
                len(subset),
                round(float(returns.mean()), 2) if len(returns) > 0 else 0,
                round(float(wins / len(returns) * 100), 1) if len(returns) > 0 else 0,
                best_sector,
            ])
    except Exception as e:
        print(f"[WARN] Regime analysis failed: {e}")


def _add_threshold_note(wb):
    """Simulation note in Confidence Analysis sheet."""
    try:
        ws    = wb["Confidence Analysis"]
        note_row = ws.max_row + 2
        ws.cell(row=note_row,     column=1, value="THRESHOLD SIMULATION")
        ws.cell(row=note_row + 1, column=1,
                value="Note: Adjust thresholds in config based on above win rates.")
        ws.cell(row=note_row + 2, column=1,
                value="Never auto-modify — use statistical evidence only (3+ months data).")
    except Exception:
        pass


def _generate_monthly_report(wb, df_rec, df_track):
    try:
        ws = wb["Monthly Report"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        now = datetime.now()
        ws["A1"] = f"Monthly Research Report — {now.strftime('%B %Y')}"
        ws["A2"] = f"Generated: {now.strftime('%Y-%m-%d %H:%M')}"

        # ── Build month-by-month rollup table ──
        # Merge Recommendations with latest Daily Tracking row per ticker to get Return% + Status
        merged = df_rec.copy()
        if not df_track.empty and "Ticker" in df_track.columns:
            best = (df_track
                    .assign(_day=pd.to_numeric(df_track.get("Day#", pd.Series(0)), errors="coerce"))
                    .sort_values("_day", ascending=False)
                    .drop_duplicates("Ticker"))
            keep = ["Ticker"]
            for c in ("Return%", "Max Gain%", "Status"):
                if c in best.columns:
                    keep.append(c)
            if len(keep) > 1:
                merged = merged.merge(best[keep], on="Ticker", how="left", suffixes=("", "_trk"))

        if merged.empty or "Date" not in merged.columns:
            ws["A4"] = "No data yet — first Recommendations sheet needs data."
            return

        merged["_dt"] = pd.to_datetime(merged["Date"], errors="coerce")
        merged = merged.dropna(subset=["_dt"])
        merged["_month"] = merged["_dt"].dt.strftime("%Y-%m")

        # Table headers at row 4
        headers = ["Month", "Picks", "Closed", "Win Rate%", "Avg Return%",
                   "Best Trade%", "Worst Trade%", "Best Sector"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=4, column=i, value=h)

        # Group by month (chronological — oldest first)
        row_idx = 5
        month_stats = []  # keep list so we can compute deltas after
        for month, subset in merged.groupby("_month"):
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()

            status_col = subset.get("Status_trk", subset.get("Status", pd.Series()))
            closed_mask = status_col.astype(str).str.upper().isin(
                ["T1_HIT", "T2_HIT", "STOPPED", "EXPIRED"]
            )
            closed_count = int(closed_mask.sum())

            wins = int((returns > 0).sum())
            settled_returns = returns  # settled = has a Return% number
            hit_rate = round(wins / len(settled_returns) * 100, 1) if len(settled_returns) else 0
            avg_ret  = round(float(returns.mean()), 2) if len(returns) else 0
            best_tr  = round(float(returns.max()), 2) if len(returns) else 0
            worst_tr = round(float(returns.min()), 2) if len(returns) else 0

            # Best sector inside this month
            best_sector = "—"
            if "Sector" in subset.columns:
                tmp = subset.copy()
                tmp["_ret"] = pd.to_numeric(tmp.get("Return%", pd.Series()), errors="coerce")
                tmp = tmp.dropna(subset=["_ret", "Sector"])
                if not tmp.empty:
                    sec_means = tmp.groupby("Sector")["_ret"].mean()
                    sec_counts = tmp.groupby("Sector")["_ret"].count()
                    eligible = sec_means[sec_counts >= 2]
                    if not eligible.empty:
                        best_sector = f"{eligible.idxmax()} ({eligible.max():+.1f}%)"
                    elif not sec_means.empty:
                        best_sector = f"{sec_means.idxmax()} ({sec_means.max():+.1f}%)"

            row_data = [month, len(subset), closed_count, hit_rate, avg_ret,
                        best_tr, worst_tr, best_sector]
            for i, v in enumerate(row_data, start=1):
                ws.cell(row=row_idx, column=i, value=v)
            month_stats.append({"month": month, "hit_rate": hit_rate, "avg_ret": avg_ret})
            row_idx += 1

        # ── Trend section ──
        row_idx += 1
        ws.cell(row=row_idx, column=1, value="TREND (last month vs previous)")
        row_idx += 1
        if len(month_stats) >= 2:
            last = month_stats[-1]
            prev = month_stats[-2]
            hr_delta  = round(last["hit_rate"] - prev["hit_rate"], 1)
            ret_delta = round(last["avg_ret"]  - prev["avg_ret"],  2)
            trend_dir = "improving" if (hr_delta > 0 and ret_delta > 0) \
                        else ("degrading" if (hr_delta < 0 and ret_delta < 0) \
                              else "mixed")
            ws.cell(row=row_idx,     column=1,
                    value=f"Win Rate: {prev['hit_rate']}% → {last['hit_rate']}% (Δ {hr_delta:+.1f} pts)")
            ws.cell(row=row_idx + 1, column=1,
                    value=f"Avg Return: {prev['avg_ret']}% → {last['avg_ret']}% (Δ {ret_delta:+.2f} pts)")
            ws.cell(row=row_idx + 2, column=1, value=f"Direction: {trend_dir.upper()}")
            row_idx += 4
        else:
            ws.cell(row=row_idx, column=1, value="Need at least 2 months of data for trend.")
            row_idx += 2

        # ── Static guidance (kept from original) ──
        ws.cell(row=row_idx, column=1, value="LESSONS LEARNED")
        row_idx += 1
        for note in [
            "1. Monitor win rates by confidence band (aim for 60%+ at min threshold)",
            "2. Compare NEAR MISS vs BUY outcomes over time",
            "3. Track sector performance vs broad market",
            "4. Validate regime-specific thresholds after 3 months",
            "5. Never lower thresholds based on fewer than 20 closed trades",
        ]:
            ws.cell(row=row_idx, column=1, value=note)
            row_idx += 1

        # ── Formatting ──
        try:
            from openpyxl.styles import Font, PatternFill
            ws["A1"].font = Font(bold=True, size=13)
            ws["A2"].font = Font(italic=True, size=10)
            header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            for i in range(1, len(headers) + 1):
                c = ws.cell(row=4, column=i)
                c.font = Font(bold=True)
                c.fill = header_fill
            for i, w in enumerate([10, 8, 8, 10, 12, 12, 12, 22], start=1):
                ws.column_dimensions[chr(64 + i)].width = w
        except Exception:
            pass

    except Exception as e:
        print(f"[WARN] Monthly report failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# v2 RESEARCH SHEETS
# ═══════════════════════════════════════════════════════════════════════════

def _merge_outcomes(df_rec, df_track):
    """Merge Recommendations with latest Daily Tracking row per ticker.
    Returns merged DataFrame with Return%, Max Gain%, Max DD%, T1 Hit, T2 Hit, Status_trk.
    """
    if df_rec.empty:
        return df_rec.copy()
    merged = df_rec.copy()
    if not df_track.empty and "Ticker" in df_track.columns:
        try:
            best = (df_track
                    .assign(_day=pd.to_numeric(df_track.get("Day#", pd.Series(0)), errors="coerce"))
                    .sort_values("_day", ascending=False)
                    .drop_duplicates("Ticker"))
            keep = ["Ticker"]
            for c in ("Return%", "Max Gain%", "Max DD%", "T1 Hit", "T2 Hit",
                      "Holding Days", "Status"):
                if c in best.columns:
                    keep.append(c)
            if len(keep) > 1:
                merged = merged.merge(best[keep], on="Ticker", how="left",
                                      suffixes=("", "_trk"))
        except Exception:
            pass
    return merged


def _format_header(ws, headers, row=1):
    """Bold + light-blue fill for a header row."""
    try:
        from openpyxl.styles import Font, PatternFill
        fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        for i in range(1, len(headers) + 1):
            c = ws.cell(row=row, column=i)
            c.font = Font(bold=True)
            c.fill = fill
    except Exception:
        pass


# ── 1. Weekday Analysis ──────────────────────────────────────────────────
def _analyze_by_weekday(wb, df_rec, df_track):
    """Do Monday picks outperform Friday picks?"""
    try:
        ws = wb["Weekday Analysis"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Weekday Analysis — Does day-of-recommendation matter?"
        headers = ["Weekday", "Count", "Win Rate%", "Avg Return%",
                   "Best Trade%", "Worst Trade%"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Date" not in merged.columns:
            ws["A5"] = "No data yet."
            return

        merged["_dt"] = pd.to_datetime(merged["Date"], errors="coerce")
        merged = merged.dropna(subset=["_dt"])
        merged["_weekday"] = merged["_dt"].dt.day_name()

        row_idx = 4
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        for wd in weekday_order:
            subset = merged[merged["_weekday"] == wd]
            if subset.empty:
                continue
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins = int((returns > 0).sum())
            ws.cell(row=row_idx, column=1, value=wd)
            ws.cell(row=row_idx, column=2, value=len(subset))
            ws.cell(row=row_idx, column=3,
                    value=round(wins / len(returns) * 100, 1) if len(returns) else 0)
            ws.cell(row=row_idx, column=4,
                    value=round(float(returns.mean()), 2) if len(returns) else 0)
            ws.cell(row=row_idx, column=5,
                    value=round(float(returns.max()), 2) if len(returns) else 0)
            ws.cell(row=row_idx, column=6,
                    value=round(float(returns.min()), 2) if len(returns) else 0)
            row_idx += 1

        row_idx += 1
        ws.cell(row=row_idx, column=1,
                value="INSIGHT: If Friday's Win Rate is >5pts below the average, "
                      "consider skipping Friday scans (weekend gap risk).")
    except Exception as e:
        print(f"[WARN] Weekday analysis failed: {e}")


# ── 2. Holding Period Analysis ───────────────────────────────────────────
def _analyze_holding_period(wb, df_rec, df_track):
    """When do winners hit target? Buckets by Holding Days."""
    try:
        ws = wb["Holding Period Analysis"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Holding Period Analysis — When do trades close out?"
        headers = ["Holding Days", "Count", "% of All Closed",
                   "Win Rate%", "Avg Return%"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Holding Days" not in merged.columns:
            ws["A5"] = "No Daily Tracking data yet (need Holding Days column)."
            return

        merged["_hd"] = pd.to_numeric(merged["Holding Days"], errors="coerce")
        settled = merged.dropna(subset=["_hd"])
        if settled.empty:
            ws["A5"] = "No settled trades yet."
            return

        buckets = [
            ("1-2 days",    lambda x: x <= 2),
            ("3-5 days",    lambda x: (x >= 3) & (x <= 5)),
            ("6-10 days",   lambda x: (x >= 6) & (x <= 10)),
            ("11-15 days",  lambda x: (x >= 11) & (x <= 15)),
            ("16+ days",    lambda x: x >= 16),
        ]

        total = len(settled)
        row_idx = 4
        for label, mask_fn in buckets:
            subset = settled[mask_fn(settled["_hd"])]
            if subset.empty:
                continue
            returns = pd.to_numeric(subset.get("Return%", pd.Series()),
                                    errors="coerce").dropna()
            wins = int((returns > 0).sum())
            ws.cell(row=row_idx, column=1, value=label)
            ws.cell(row=row_idx, column=2, value=len(subset))
            ws.cell(row=row_idx, column=3,
                    value=round(len(subset) / total * 100, 1))
            ws.cell(row=row_idx, column=4,
                    value=round(wins / len(returns) * 100, 1) if len(returns) else 0)
            ws.cell(row=row_idx, column=5,
                    value=round(float(returns.mean()), 2) if len(returns) else 0)
            row_idx += 1

        row_idx += 1
        avg_hd = float(settled["_hd"].mean())
        ws.cell(row=row_idx, column=1,
                value=f"AVG HOLDING PERIOD: {avg_hd:.1f} days")
        ws.cell(row=row_idx + 1, column=1,
                value="INSIGHT: If '11-15 days' bucket has poor win rate, "
                      "TIME_EXIT_DAYS is likely too long — trim to 10.")
    except Exception as e:
        print(f"[WARN] Holding period analysis failed: {e}")


# ── 3. Category Comparison ───────────────────────────────────────────────
def _analyze_category_comparison(wb, df_rec, df_track):
    """BUY vs NEAR_MISS vs WATCHLIST — is the tier system meaningful?"""
    try:
        ws = wb["Category Comparison"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Category Comparison — Do BUY, NEAR_MISS, WATCHLIST tiers differ?"
        headers = ["Category", "Count", "Win Rate%", "Avg Return%",
                   "Avg Confidence", "Avg TQ", "Avg Opp Score"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Category" not in merged.columns:
            ws["A5"] = "No Category column in Recommendations."
            return

        row_idx = 4
        # Preserve a sensible order if categories present
        preferred = ["BUY", "NEAR_MISS", "WATCHLIST"]
        cats_present = [c for c in preferred if c in merged["Category"].dropna().unique()]
        others = [c for c in merged["Category"].dropna().unique() if c not in preferred]
        for cat in cats_present + others:
            subset = merged[merged["Category"] == cat]
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins    = int((returns > 0).sum())
            conf    = pd.to_numeric(subset.get("Confidence", pd.Series()), errors="coerce").dropna()
            tq      = pd.to_numeric(subset.get("TQ", pd.Series()), errors="coerce").dropna()
            opp     = pd.to_numeric(subset.get("Opp Score", pd.Series()), errors="coerce").dropna()
            ws.cell(row=row_idx, column=1, value=cat)
            ws.cell(row=row_idx, column=2, value=len(subset))
            ws.cell(row=row_idx, column=3,
                    value=round(wins / len(returns) * 100, 1) if len(returns) else 0)
            ws.cell(row=row_idx, column=4,
                    value=round(float(returns.mean()), 2) if len(returns) else 0)
            ws.cell(row=row_idx, column=5,
                    value=round(float(conf.mean()), 1) if len(conf) else 0)
            ws.cell(row=row_idx, column=6,
                    value=round(float(tq.mean()), 1) if len(tq) else 0)
            ws.cell(row=row_idx, column=7,
                    value=round(float(opp.mean()), 1) if len(opp) else 0)
            row_idx += 1

        row_idx += 1
        ws.cell(row=row_idx, column=1,
                value="INSIGHT: BUY win rate should be >= NEAR_MISS >= WATCHLIST.")
        ws.cell(row=row_idx + 1, column=1,
                value="If NEAR_MISS matches BUY, the tier boundary is too strict.")
    except Exception as e:
        print(f"[WARN] Category comparison failed: {e}")


# ── 4. Confidence × TQ Matrix ────────────────────────────────────────────
def _analyze_conf_tq_matrix(wb, df_rec, df_track):
    """Cross-tab of Confidence buckets × TQ buckets — sweet-spot detection."""
    try:
        ws = wb["Conf x TQ Matrix"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Confidence × TQ Matrix — where do the two factors compound?"
        ws["A2"] = "Each cell shows: Win Rate% (n=count)"

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty:
            ws["A4"] = "No data yet."
            return

        conf_bins = [(0, 75, "Low"), (75, 83, "Mid"), (83, 100, "High")]
        tq_bins   = [(0, 75, "Low"), (75, 85, "Mid"), (85, 100, "High")]

        merged["_conf"] = pd.to_numeric(merged.get("Confidence", pd.Series()), errors="coerce")
        merged["_tq"]   = pd.to_numeric(merged.get("TQ",         pd.Series()), errors="coerce")
        merged["_ret"]  = pd.to_numeric(merged.get("Return%",    pd.Series()), errors="coerce")

        # Column headers (TQ buckets)
        ws.cell(row=4, column=1, value="Confidence \\ TQ")
        for j, (_, _, tq_lbl) in enumerate(tq_bins, start=2):
            ws.cell(row=4, column=j, value=f"{tq_lbl} TQ")
        _format_header(ws, ["Confidence \\ TQ"] + [b[2] for b in tq_bins], row=4)

        best_cell = ("", 0.0, 0)  # (label, win_rate, count)
        for i, (cf_lo, cf_hi, cf_lbl) in enumerate(conf_bins, start=5):
            ws.cell(row=i, column=1, value=f"{cf_lbl} Conf")
            for j, (tq_lo, tq_hi, tq_lbl) in enumerate(tq_bins, start=2):
                cell_data = merged[
                    (merged["_conf"] >= cf_lo) & (merged["_conf"] < cf_hi) &
                    (merged["_tq"]   >= tq_lo) & (merged["_tq"]   < tq_hi)
                ]
                rets = cell_data["_ret"].dropna()
                if len(rets) == 0:
                    ws.cell(row=i, column=j, value="—")
                    continue
                wr = round(int((rets > 0).sum()) / len(rets) * 100, 1)
                ws.cell(row=i, column=j, value=f"{wr}% (n={len(rets)})")
                # Track best cell (need >= 5 samples to matter)
                if len(rets) >= 5 and wr > best_cell[1]:
                    best_cell = (f"{cf_lbl} Conf × {tq_lbl} TQ", wr, len(rets))

        # Also compute avg Return% matrix below
        ws.cell(row=9,  column=1, value="")
        ws.cell(row=10, column=1, value="Avg Return% matrix")
        _format_header(ws, ["Confidence \\ TQ"] + [b[2] for b in tq_bins], row=11)
        ws.cell(row=11, column=1, value="Confidence \\ TQ")
        for j, (_, _, tq_lbl) in enumerate(tq_bins, start=2):
            ws.cell(row=11, column=j, value=f"{tq_lbl} TQ")

        for i, (cf_lo, cf_hi, cf_lbl) in enumerate(conf_bins, start=12):
            ws.cell(row=i, column=1, value=f"{cf_lbl} Conf")
            for j, (tq_lo, tq_hi, tq_lbl) in enumerate(tq_bins, start=2):
                cell_data = merged[
                    (merged["_conf"] >= cf_lo) & (merged["_conf"] < cf_hi) &
                    (merged["_tq"]   >= tq_lo) & (merged["_tq"]   < tq_hi)
                ]
                rets = cell_data["_ret"].dropna()
                ws.cell(row=i, column=j,
                        value=f"{round(float(rets.mean()), 2)}%" if len(rets) else "—")

        if best_cell[0]:
            ws.cell(row=16, column=1,
                    value=f"SWEET SPOT (n>=5): {best_cell[0]} — "
                          f"{best_cell[1]}% win rate on {best_cell[2]} picks.")
        ws.cell(row=17, column=1,
                value="INSIGHT: If High×High dominates, "
                      "tighten filter to require BOTH factors high.")
    except Exception as e:
        print(f"[WARN] Conf×TQ matrix failed: {e}")


# ── 5. Catalyst Analysis ─────────────────────────────────────────────────
def _analyze_catalysts(wb, df_rec, df_track):
    """Which catalyst tokens (EARNINGS_BEAT, BREAKOUT, ...) drive returns?"""
    try:
        ws = wb["Catalyst Analysis"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Catalyst Analysis — which catalysts actually pay off?"
        headers = ["Catalyst", "Count", "Win Rate%", "Avg Return%", "Best Trade%"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Catalysts" not in merged.columns:
            ws["A5"] = "No Catalysts column."
            return

        # Explode comma-separated tokens
        from collections import defaultdict
        cat_stats = defaultdict(list)  # token -> [Return%, ...]
        for _, r in merged.iterrows():
            tokens_raw = str(r.get("Catalysts", "") or "")
            if not tokens_raw or tokens_raw.lower() == "nan":
                continue
            ret = pd.to_numeric(r.get("Return%", None), errors="coerce")
            if pd.isna(ret):
                continue
            for tok in [t.strip().upper() for t in tokens_raw.split(",") if t.strip()]:
                cat_stats[tok].append(float(ret))

        if not cat_stats:
            ws["A5"] = "No parseable catalyst data with outcomes yet."
            return

        # Sort by win-rate desc
        rows = []
        for tok, rets in cat_stats.items():
            n = len(rets)
            wins = sum(1 for r in rets if r > 0)
            wr = round(wins / n * 100, 1) if n else 0
            rows.append((tok, n, wr, round(sum(rets) / n, 2) if n else 0, round(max(rets), 2)))
        rows.sort(key=lambda x: (-x[2], -x[1]))

        for row_idx, r in enumerate(rows, start=4):
            for j, v in enumerate(r, start=1):
                ws.cell(row=row_idx, column=j, value=v)

        row_idx = 4 + len(rows) + 1
        ws.cell(row=row_idx, column=1,
                value="INSIGHT: Catalysts with Win Rate < 50% on 20+ picks "
                      "should be dropped or down-weighted in scoring.")
    except Exception as e:
        print(f"[WARN] Catalyst analysis failed: {e}")


# ── 6. Fail Reason Analysis ──────────────────────────────────────────────
def _analyze_fail_reasons(wb, df_rec, df_track):
    """Fail Reasons frequency — which 'amber warnings' become real losses?"""
    try:
        ws = wb["Fail Reason Analysis"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Fail Reason Analysis — which amber flags actually predict losses?"
        headers = ["Fail Reason", "Count in Buys+NearMiss", "Win Rate%",
                   "Avg Return%"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Fail Reasons" not in merged.columns:
            ws["A5"] = "No Fail Reasons column."
            return

        # Only look at picks that made it through (BUY / NEAR_MISS) — WATCHLIST
        # is expected to fail. We want to see which fail flags leak into the
        # promoted picks and correlate with loss.
        if "Category" in merged.columns:
            promoted = merged[merged["Category"].isin(["BUY", "NEAR_MISS"])]
        else:
            promoted = merged

        from collections import defaultdict
        fr_stats = defaultdict(list)
        for _, r in promoted.iterrows():
            tokens_raw = str(r.get("Fail Reasons", "") or "")
            if not tokens_raw or tokens_raw.lower() == "nan":
                continue
            ret = pd.to_numeric(r.get("Return%", None), errors="coerce")
            if pd.isna(ret):
                continue
            for tok in [t.strip().upper() for t in tokens_raw.split(",") if t.strip()]:
                fr_stats[tok].append(float(ret))

        if not fr_stats:
            ws["A5"] = ("No fail reasons observed in BUY/NEAR_MISS picks with outcomes. "
                        "Either all promoted picks are clean, or data is thin.")
            return

        rows = []
        for tok, rets in fr_stats.items():
            n = len(rets)
            wins = sum(1 for r in rets if r > 0)
            wr = round(wins / n * 100, 1) if n else 0
            rows.append((tok, n, wr, round(sum(rets) / n, 2) if n else 0))
        rows.sort(key=lambda x: (x[2], -x[1]))  # worst win rate first

        for row_idx, r in enumerate(rows, start=4):
            for j, v in enumerate(r, start=1):
                ws.cell(row=row_idx, column=j, value=v)

        row_idx = 4 + len(rows) + 1
        ws.cell(row=row_idx, column=1,
                value="INSIGHT: Any fail-flag with Win Rate < 45% on 10+ picks "
                      "should be promoted from amber warning to HARD BLOCK.")
    except Exception as e:
        print(f"[WARN] Fail reason analysis failed: {e}")


# ── 7. Regime × Sector Cross-tab ─────────────────────────────────────────
def _analyze_regime_x_sector(wb, df_rec, df_track):
    """Which sectors work in which regimes? (defensive vs cyclical detection)"""
    try:
        ws = wb["Regime x Sector"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Regime × Sector — which sectors win in each regime?"
        ws["A2"] = "Cell = Avg Return% (n=count). Blank if <2 picks."

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty or "Regime" not in merged.columns or "Sector" not in merged.columns:
            ws["A4"] = "Missing Regime or Sector column."
            return

        merged["_ret"] = pd.to_numeric(merged.get("Return%", pd.Series()), errors="coerce")
        settled = merged.dropna(subset=["_ret", "Regime", "Sector"])
        if settled.empty:
            ws["A4"] = "No settled trades with Regime + Sector yet."
            return

        regimes = sorted(settled["Regime"].dropna().unique().tolist())
        sectors = sorted(settled["Sector"].dropna().unique().tolist())

        # Header row (regimes across the top)
        ws.cell(row=4, column=1, value="Sector \\ Regime")
        for j, rg in enumerate(regimes, start=2):
            ws.cell(row=4, column=j, value=str(rg))
        _format_header(ws, ["Sector \\ Regime"] + regimes, row=4)

        for i, sec in enumerate(sectors, start=5):
            ws.cell(row=i, column=1, value=sec)
            for j, rg in enumerate(regimes, start=2):
                cell_data = settled[
                    (settled["Regime"] == rg) & (settled["Sector"] == sec)
                ]
                if len(cell_data) < 2:
                    ws.cell(row=i, column=j, value="")
                    continue
                avg = round(float(cell_data["_ret"].mean()), 2)
                ws.cell(row=i, column=j, value=f"{avg:+.1f}% (n={len(cell_data)})")

        note_row = 5 + len(sectors) + 1
        ws.cell(row=note_row, column=1,
                value="INSIGHT: Green cells = sector works in that regime. "
                      "Use this to overweight/skip sectors when regime flips.")
    except Exception as e:
        print(f"[WARN] Regime×Sector failed: {e}")


# ── 8. Confidence Trajectory ─────────────────────────────────────────────
def _analyze_confidence_trajectory(wb, df_rec, df_track):
    """Do rising-confidence stocks outperform stable/falling ones?
    Reads confidence_history.json — {symbol: {dates:[3], confs:[3]}}
    """
    try:
        ws = wb["Confidence Trajectory"]
        for row in ws.iter_rows(min_row=1):
            for cell in row:
                cell.value = None

        ws["A1"] = "Confidence Trajectory — does rising conf beat stable/falling?"
        headers = ["Trajectory", "Count", "Win Rate%", "Avg Return%", "Definition"]
        for i, h in enumerate(headers, start=1):
            ws.cell(row=3, column=i, value=h)
        _format_header(ws, headers, row=3)

        conf_file = os.getenv("CONF_HISTORY_FILE", "confidence_history.json")
        if not os.path.exists(conf_file):
            ws["A5"] = f"No {conf_file} yet."
            return

        try:
            import json
            with open(conf_file, "r") as f:
                history = json.load(f)
        except Exception as e:
            ws["A5"] = f"Could not read {conf_file}: {e}"
            return

        # Build trajectory per symbol: delta of latest vs first in 3-day window
        traj = {}  # symbol -> "RISING" / "STABLE" / "FALLING"
        for sym, rec in history.items():
            confs = rec.get("confs", []) if isinstance(rec, dict) else []
            if len(confs) < 2:
                continue
            delta = confs[-1] - confs[0]
            if delta >= 5:
                traj[sym] = "RISING"
            elif delta <= -5:
                traj[sym] = "FALLING"
            else:
                traj[sym] = "STABLE"

        if not traj:
            ws["A5"] = ("Confidence history too thin (need 2+ days per symbol). "
                        "Wait ~1 week for meaningful data.")
            return

        merged = _merge_outcomes(df_rec, df_track)
        if merged.empty:
            ws["A5"] = "No recommendations to correlate."
            return

        merged["_traj"] = merged["Ticker"].map(traj).fillna("UNKNOWN")

        definitions = {
            "RISING":   "Confidence up 5+ points in 3-day window",
            "STABLE":   "Confidence changed <5 points",
            "FALLING":  "Confidence down 5+ points",
            "UNKNOWN":  "No 3-day history",
        }

        row_idx = 4
        for label in ["RISING", "STABLE", "FALLING", "UNKNOWN"]:
            subset = merged[merged["_traj"] == label]
            if subset.empty:
                continue
            rets = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins = int((rets > 0).sum())
            ws.cell(row=row_idx, column=1, value=label)
            ws.cell(row=row_idx, column=2, value=len(subset))
            ws.cell(row=row_idx, column=3,
                    value=round(wins / len(rets) * 100, 1) if len(rets) else 0)
            ws.cell(row=row_idx, column=4,
                    value=round(float(rets.mean()), 2) if len(rets) else 0)
            ws.cell(row=row_idx, column=5, value=definitions[label])
            row_idx += 1

        row_idx += 1
        ws.cell(row=row_idx, column=1,
                value="INSIGHT: If RISING clearly beats STABLE, add a "
                      "'momentum-in-confidence' bonus (+2 pts) to the scorer.")
    except Exception as e:
        print(f"[WARN] Confidence trajectory failed: {e}")


if __name__ == "__main__":
    run_research()
