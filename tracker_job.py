"""
tracker_job.py — Recommendation Tracker (JOB 2)
================================================
Run separately every trading day after market close.
GitHub Actions: schedule weekdays at 4:30 PM IST (11:00 UTC).

Reads:  recommendation_tracker.xlsx (created by main.py daily scanner)
Writes: recommendation_tracker.xlsx — Daily Tracking sheet + Performance Summary

Usage:
    python tracker_job.py
"""

import os
import numpy as np
from datetime import datetime, date

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import yfinance as yf
    _YF_OK = True
except ImportError:
    _YF_OK = False
    print("[ERROR] yfinance not installed. Run: pip install yfinance")

try:
    import openpyxl
    _OPENPYXL_OK = True
except ImportError:
    _OPENPYXL_OK = False
    print("[ERROR] openpyxl not installed. Run: pip install openpyxl")

TRACKER_XLSX  = os.getenv("TRACKER_XLSX", "recommendation_tracker.xlsx")
TRACKING_DAYS = 60  # track for 60 trading days after recommendation
# Only write new tracking rows when triggered by GitHub Actions cron schedule
IS_SCHEDULED  = os.getenv("SCHEDULED_RUN", "false").lower() == "true"


def run_tracker():
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"=== TRACKER JOB: {today_str} ===")
    print(f"[INFO] Run mode: {'SCHEDULED — will write tracking rows' if IS_SCHEDULED else 'MANUAL — read-only, no rows written'}")

    if not IS_SCHEDULED:
        print("[INFO] Manual run detected. Tracker rows are NOT written to preserve accurate day counts.")
        print("[INFO] Use the scheduled run (4:30 PM IST weekdays) for proper daily tracking.")
        return

    if not _YF_OK or not _OPENPYXL_OK:
        print("[ERROR] Missing dependencies — aborting")
        return

    if not os.path.exists(TRACKER_XLSX):
        print(f"[WARN] No tracker file found at {TRACKER_XLSX} — run daily scanner first")
        return

    try:
        wb       = openpyxl.load_workbook(TRACKER_XLSX)
        ws_rec   = wb["Recommendations"]
        ws_track = wb["Daily Tracking"]
    except Exception as e:
        print(f"[ERROR] Could not open {TRACKER_XLSX}: {e}")
        return

    # ── Read active recommendations ──
    headers = [cell.value for cell in ws_rec[1]]
    active_recs = []
    for row in ws_rec.iter_rows(min_row=2, values_only=True):
        d = dict(zip(headers, row))
        if d.get("Status") == "ACTIVE":
            active_recs.append(d)

    print(f"[INFO] Tracking {len(active_recs)} active recommendations")
    if not active_recs:
        print("[INFO] No active recommendations to track")
        _update_performance_sheet(wb)
        wb.save(TRACKER_XLSX)
        return

    # ── Batch download prices ──
    symbols = list(set(str(r["Ticker"]) for r in active_recs if r.get("Ticker")))
    prices  = {}
    for sym in symbols:
        try:
            df = yf.download(sym, period="5d", interval="1d",
                             progress=False, auto_adjust=True)
            if df is not None and len(df) > 0:
                prices[sym] = {
                    "close": float(df["Close"].iloc[-1]),
                    "high":  float(df["High"].iloc[-1]),
                    "low":   float(df["Low"].iloc[-1]),
                    "vol":   float(df["Volume"].iloc[-1]),
                    "max_close": float(df["Close"].max()),
                    "min_close": float(df["Close"].min()),
                }
        except Exception as e:
            print(f"[WARN] Price fetch failed for {sym}: {e}")

    # ── Write tracking rows ──
    rows_added = 0
    for rec in active_recs:
        sym = str(rec.get("Ticker", ""))
        px  = prices.get(sym)
        if not px:
            continue

        rec_date = rec.get("Date", "")
        try:
            days_held = (date.today() -
                         datetime.strptime(str(rec_date), "%Y-%m-%d").date()).days
        except Exception:
            days_held = 0

        entry = float(rec.get("Entry") or 0)
        stop  = float(rec.get("Stop") or 0)
        t1    = float(rec.get("T1") or 0)
        t2    = float(rec.get("T2") or 0)

        cur_return  = round((px["close"] - entry) / entry * 100, 2) if entry > 0 else 0
        max_gain    = round((px["max_close"] - entry) / entry * 100, 2) if entry > 0 else 0
        max_dd      = round((px["min_close"] - entry) / entry * 100, 2) if entry > 0 else 0
        remain_up   = round((t2 - px["close"]) / px["close"] * 100, 1) if t2 > px["close"] > 0 else 0

        t1_hit   = px["high"] >= t1   if t1 > 0 else False
        t2_hit   = px["high"] >= t2   if t2 > 0 else False
        stop_hit = px["low"]  <= stop if stop > 0 else False

        # Determine status
        if t2_hit:              status = "T2_HIT"
        elif stop_hit:          status = "STOPPED"
        elif days_held >= TRACKING_DAYS: status = "EXPIRED"
        elif t1_hit:            status = "T1_HIT_ACTIVE"
        else:                   status = "ACTIVE"

        ws_track.append([
            today_str, sym, str(rec_date), days_held,
            px["close"], px["high"], px["low"], px["vol"],
            cur_return, max_gain, max_dd,
            t1_hit, t2_hit, stop_hit, remain_up, days_held, status
        ])
        rows_added += 1

        # Update recommendation status if terminal
        if status in ("T2_HIT", "STOPPED", "EXPIRED"):
            for row in ws_rec.iter_rows(min_row=2):
                if (str(row[1].value) == sym and
                        str(row[0].value) == str(rec_date)):
                    row[-1].value = status
                    break

    print(f"[INFO] Added {rows_added} tracking rows")
    wb.save(TRACKER_XLSX)
    _update_performance_sheet(wb)
    wb.save(TRACKER_XLSX)
    print(f"[INFO] Tracker job complete — {TRACKER_XLSX} updated")


def _update_performance_sheet(wb):
    """Recalculates Performance Summary sheet from tracking data."""
    try:
        ws_track = wb["Daily Tracking"]
        ws_perf  = wb["Performance Summary"]

        # Clear existing (keep header)
        for row in ws_perf.iter_rows(min_row=2):
            for cell in row:
                cell.value = None

        # Read all tracking records
        headers = [cell.value for cell in ws_track[1]]
        records = [dict(zip(headers, [cell.value for cell in row]))
                   for row in ws_track.iter_rows(min_row=2)]

        # Group by ticker + rec_date for final outcomes
        outcomes = {}
        for r in records:
            key = f"{r.get('Ticker')}_{r.get('Rec Date')}"
            day = int(r.get("Day#") or 0)
            if key not in outcomes or day > int(outcomes[key].get("Day#") or 0):
                outcomes[key] = r

        closed = [o for o in outcomes.values()
                  if o.get("Status") not in ("ACTIVE", "T1_HIT_ACTIVE")]
        wins   = [o for o in closed if float(o.get("Return%") or 0) > 0]
        losses = [o for o in closed if float(o.get("Return%") or 0) <= 0]

        stats = [
            ("Total Tracked",   len(outcomes)),
            ("Closed",          len(closed)),
            ("Active",          len(outcomes) - len(closed)),
            ("Win Rate %",      round(len(wins)/len(closed)*100, 1) if closed else 0),
            ("Avg Return %",    round(np.mean([float(o.get("Return%") or 0) for o in closed]), 2) if closed else 0),
            ("Avg Win %",       round(np.mean([float(o.get("Return%") or 0) for o in wins]), 2) if wins else 0),
            ("Avg Loss %",      round(np.mean([float(o.get("Return%") or 0) for o in losses]), 2) if losses else 0),
            ("Avg Max Gain %",  round(np.mean([float(o.get("Max Gain%") or 0) for o in closed]), 2) if closed else 0),
            ("Avg Max DD %",    round(np.mean([float(o.get("Max DD%") or 0) for o in closed]), 2) if closed else 0),
            ("T1 Hit Rate %",   round(sum(1 for o in closed if o.get("T1 Hit"))/len(closed)*100, 1) if closed else 0),
            ("T2 Hit Rate %",   round(sum(1 for o in closed if o.get("T2 Hit"))/len(closed)*100, 1) if closed else 0),
            ("Stop Hit Rate %", round(sum(1 for o in closed if o.get("Stop Hit"))/len(closed)*100, 1) if closed else 0),
            ("Last Updated",    datetime.now().strftime("%Y-%m-%d %H:%M")),
        ]

        ws_perf["A1"] = "Metric"
        ws_perf["B1"] = "Value"
        for i, (metric, value) in enumerate(stats, start=2):
            ws_perf[f"A{i}"] = metric
            ws_perf[f"B{i}"] = value

    except Exception as e:
        print(f"[WARN] Performance sheet update failed: {e}")


if __name__ == "__main__":
    run_tracker()
