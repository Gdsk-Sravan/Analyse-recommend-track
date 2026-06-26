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

    wb.save(TRACKER_XLSX)
    print(f"[INFO] Research job complete — {TRACKER_XLSX} updated")


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
            if "Return%" in best.columns:
                merged = merged.merge(best[["Ticker", "Return%"]], on="Ticker", how="left")

        for sector in df_rec["Sector"].dropna().unique():
            subset  = merged[merged["Sector"] == sector]
            returns = pd.to_numeric(subset.get("Return%", pd.Series()), errors="coerce").dropna()
            wins    = (returns > 0).sum()
            ws.append([
                sector,
                len(subset),
                round(float(returns.mean()), 2) if len(returns) > 0 else 0,
                round(float(wins / len(returns) * 100), 1) if len(returns) > 0 else 0,
                0,
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
            ws.append([
                regime,
                len(subset),
                round(float(returns.mean()), 2) if len(returns) > 0 else 0,
                round(float(wins / len(returns) * 100), 1) if len(returns) > 0 else 0,
                "—",
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
        ws["A4"] = "Total Recommendations This Month"

        # Count this month
        this_month = 0
        if not df_rec.empty and "Date" in df_rec.columns:
            try:
                month_mask = pd.to_datetime(df_rec["Date"], errors="coerce").dt.month == now.month
                this_month = int(month_mask.sum())
            except Exception:
                this_month = len(df_rec)
        ws["B4"] = this_month

        ws["A5"] = "Data accumulates over time — revisit after 20+ closed trades"
        ws["A7"] = "LESSONS LEARNED"
        ws["A8"] = "1. Monitor win rates by confidence band (aim for 60%+ at min threshold)"
        ws["A9"] = "2. Compare NEAR MISS vs BUY outcomes over time"
        ws["A10"] = "3. Track sector performance vs broad market"
        ws["A11"] = "4. Validate regime-specific thresholds after 3 months"
        ws["A12"] = "5. Never lower thresholds based on fewer than 20 closed trades"

    except Exception as e:
        print(f"[WARN] Monthly report failed: {e}")


if __name__ == "__main__":
    run_research()
