"""
backtest_walkforward.py — Walk-forward backtest for main.py scoring engine.

Run after 30+ days of paper trading OR to calibrate thresholds from historical data.

Usage:
    python backtest_walkforward.py

What it does:
    1. Loads stocks.txt
    2. Downloads 2 years of daily OHLCV for each stock
    3. For each stock-day in the history, computes the main.py confidence score
    4. Simulates: entry at close, target1 = entry + 2*ATR, stop = EMA20 - 0.5*ATR
    5. Records outcome: T1_HIT | STOP_HIT | TIME_EXIT (20 bars)
    6. Produces:
       - win_rate_by_confidence.csv — actual win rate at each confidence bucket
       - threshold_recommendation.txt — recommended min_confidence per regime
       - backtest_summary.txt — overall stats

Output files are written to the current directory.
This script is STANDALONE — does not import from main.py to avoid side effects.
"""

import os
import csv
import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Config ───────────────────────────────────────────────────────────────────
LOOKBACK_YEARS    = 2
MIN_HISTORY_BARS  = 252
MAX_HOLD_BARS     = 20
TOP_N_SYMBOLS     = 200   # limit for speed; increase for full universe
MIN_AVG_VALUE_CR  = 2.0   # Rs2Cr daily value minimum
WORKERS           = 8
STOCKS_FILE       = os.getenv("STOCKS_FILE", "stocks.txt")
OUTPUT_DIR        = "."

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ema(series: np.ndarray, span: int) -> np.ndarray:
    return pd.Series(series).ewm(span=span, adjust=False).mean().values


def _atr14(highs, lows, closes) -> np.ndarray:
    tr = np.maximum(highs - lows,
         np.maximum(np.abs(highs - np.roll(closes, 1)),
                    np.abs(lows  - np.roll(closes, 1))))
    return pd.Series(tr[1:]).rolling(14).mean().values


def _rsi14(closes: np.ndarray) -> np.ndarray:
    diff   = np.diff(closes)
    gains  = pd.Series(np.where(diff > 0, diff, 0)).rolling(14).mean().values
    losses = pd.Series(np.where(diff < 0, -diff, 0)).rolling(14).mean().values
    with np.errstate(divide="ignore", invalid="ignore"):
        rs  = np.where(losses > 0, gains / losses, 100)
        rsi = np.where(losses > 0, 100 - 100 / (1 + rs), 100)
    return np.concatenate([[50]*14, rsi[13:]])


def _confidence_at(i: int, closes, highs, lows, volumes) -> float:
    """
    Computes a simplified 5-factor confidence score at bar i.
    Mirrors main.py's scoring but self-contained.
    """
    if i < 60:
        return 0.0
    try:
        c  = closes[:i+1]
        h  = highs[:i+1]
        l  = lows[:i+1]
        v  = volumes[:i+1]
        last = c[-1]

        ema9   = _ema(c, 9)[-1]
        ema20  = _ema(c, 20)[-1]
        ema50  = _ema(c, 50)[-1]
        ema200 = _ema(c, 200)[-1] if len(c) >= 200 else ema50
        atr_arr = _atr14(h, l, c)
        atr14v  = atr_arr[-1] if not np.isnan(atr_arr[-1]) else (last * 0.015)
        avg_vol = float(pd.Series(v).rolling(20).mean().iloc[-1])
        vol_ratio = v[-1] / avg_vol if avg_vol > 0 else 1.0

        # Trend score
        if last > ema9 > ema20 > ema50 > ema200:  tq = 92
        elif last > ema20 > ema50 > ema200:         tq = 78
        elif last > ema50 > ema200:                  tq = 62
        elif last > ema200:                          tq = 48
        else:                                         tq = 30

        # Momentum
        ret5d  = (last / c[-6]  - 1) * 100 if len(c) > 6  else 0
        ret21d = (last / c[-22] - 1) * 100 if len(c) > 22 else 0
        if ret5d > 3 and ret21d > 6:       mom = 90
        elif ret5d > 1.5 and ret21d > 3:   mom = 75
        elif ret5d > 0 and ret21d > 0:     mom = 60
        elif ret5d < 0:                     mom = 35
        else:                               mom = 50

        # Volume
        vol_score = min(100, vol_ratio * 45)

        # RSI momentum bonus
        rsi = _rsi14(c)[-1]
        if rsi > 65:   mom = min(100, mom + 5)
        elif rsi < 35: mom = max(0, mom - 10)

        # Risk/reward (simplified)
        stop = ema20 - 0.5 * atr14v
        if stop >= last:
            stop = last * 0.94
        t1   = last + 2.0 * atr14v
        rr   = (t1 - last) / (last - stop) if last > stop else 0.0
        rr_score = min(100, rr * 30)

        # Weighted confidence
        conf = (tq * 0.35 + mom * 0.30 + vol_score * 0.15 + rr_score * 0.20)
        return round(conf, 1)
    except Exception:
        return 0.0


def _simulate_trade(i: int, closes, highs, lows, atr14_arr, ema20_arr,
                    max_hold: int = 20) -> str:
    """
    Simulates holding from bar i.
    Returns "T1_HIT" | "STOP_HIT" | "TIME_EXIT"
    """
    try:
        entry = closes[i]
        atr   = atr14_arr[i]
        ema20 = ema20_arr[i]
        stop  = max(ema20 - 0.5 * atr, entry * 0.92)
        if stop >= entry:
            stop = entry * 0.94
        t1    = entry + 2.0 * atr

        for j in range(i + 1, min(i + max_hold + 1, len(closes))):
            low_j  = lows[j]
            high_j = highs[j]
            if low_j <= stop:
                return "STOP_HIT"
            if high_j >= t1:
                return "T1_HIT"
        return "TIME_EXIT"
    except Exception:
        return "TIME_EXIT"


def _process_symbol(symbol: str, period: str = "2y") -> list:
    """
    Downloads data and runs backtest for one symbol.
    Returns list of {symbol, date, confidence, outcome} dicts.
    """
    rows = []
    try:
        time.sleep(random.uniform(0.1, 0.4))
        df = yf.download(symbol, period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < MIN_HISTORY_BARS:
            return rows
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        closes  = df["Close"].squeeze().values.astype(float)
        highs   = df["High"].squeeze().values.astype(float)
        lows    = df["Low"].squeeze().values.astype(float)
        volumes = df["Volume"].squeeze().values.astype(float)
        dates   = df.index.tolist()

        # Liquidity filter
        avg_vol = float(pd.Series(volumes).rolling(20).mean().iloc[-1])
        avg_px  = float(pd.Series(closes).rolling(20).mean().iloc[-1])
        if (avg_vol * avg_px) / 1e7 < MIN_AVG_VALUE_CR:
            return rows

        atr14_arr = _atr14(highs, lows, closes)
        # Pad atr14_arr to match closes length
        atr14_arr = np.concatenate([[np.nan]*15, atr14_arr])[:len(closes)]
        ema20_arr = _ema(closes, 20)

        # Walk forward: test every bar from bar 60 to len-MAX_HOLD-1
        for i in range(60, len(closes) - MAX_HOLD_BARS - 1):
            if np.isnan(atr14_arr[i]):
                continue
            conf    = _confidence_at(i, closes, highs, lows, volumes)
            if conf < 40:   # skip very low confidence — not interesting
                continue
            outcome = _simulate_trade(i, closes, highs, lows, atr14_arr, ema20_arr)
            rows.append({
                "symbol":     symbol,
                "date":       str(dates[i])[:10],
                "confidence": conf,
                "outcome":    outcome,
            })
    except Exception as e:
        print(f"[WARN] {symbol}: {e}")
    return rows


def load_symbols(path: str, limit: int = TOP_N_SYMBOLS) -> list:
    syms = []
    try:
        with open(path) as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    if not s.endswith(".NS"):
                        s += ".NS"
                    syms.append(s)
    except Exception:
        pass
    return syms[:limit]


def run_backtest():
    print("=" * 60)
    print("WALK-FORWARD BACKTEST")
    print(f"Symbols:    up to {TOP_N_SYMBOLS} from {STOCKS_FILE}")
    print(f"History:    {LOOKBACK_YEARS} years | Max hold: {MAX_HOLD_BARS} bars")
    print("=" * 60)

    symbols = load_symbols(STOCKS_FILE)
    if not symbols:
        print("[ERROR] No symbols loaded. Check stocks.txt.")
        return

    print(f"Loaded {len(symbols)} symbols. Running in parallel ({WORKERS} workers)...")
    all_rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_process_symbol, s): s for s in symbols}
        for fut in as_completed(futs):
            rows = fut.result()
            all_rows.extend(rows)
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{len(symbols)} symbols done, {len(all_rows)} bars collected")

    if not all_rows:
        print("[ERROR] No data collected.")
        return

    print(f"\nTotal bars: {len(all_rows)}")

    # ── Analysis ──────────────────────────────────────────────────────────────
    # Group by confidence bucket (every 5 points)
    buckets: dict = {}
    for row in all_rows:
        b = int(row["confidence"] // 5) * 5
        if b not in buckets:
            buckets[b] = {"t1": 0, "stop": 0, "time": 0, "total": 0}
        buckets[b]["total"] += 1
        if row["outcome"] == "T1_HIT":
            buckets[b]["t1"] += 1
        elif row["outcome"] == "STOP_HIT":
            buckets[b]["stop"] += 1
        else:
            buckets[b]["time"] += 1

    # Write win_rate_by_confidence.csv
    wrc_path = os.path.join(OUTPUT_DIR, "win_rate_by_confidence.csv")
    with open(wrc_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["conf_bucket", "total", "t1_hit", "stop_hit", "time_exit",
                    "win_rate_pct", "stop_rate_pct"])
        for b in sorted(buckets.keys()):
            d   = buckets[b]
            wr  = round(d["t1"] / d["total"] * 100, 1) if d["total"] > 0 else 0
            sr  = round(d["stop"] / d["total"] * 100, 1) if d["total"] > 0 else 0
            w.writerow([b, d["total"], d["t1"], d["stop"], d["time"], wr, sr])
    print(f"Saved: {wrc_path}")

    # Write all raw rows
    raw_path = os.path.join(OUTPUT_DIR, "backtest_raw.csv")
    with open(raw_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "date", "confidence", "outcome"])
        w.writeheader()
        w.writerows(all_rows)
    print(f"Saved: {raw_path}")

    # ── Threshold recommendations ──────────────────────────────────────────────
    # Find the confidence bucket where win_rate first exceeds 60%
    target_wr = 60.0
    rec_threshold = 85  # default if not found
    for b in sorted(buckets.keys()):
        d  = buckets[b]
        if d["total"] < 20:
            continue
        wr = d["t1"] / d["total"] * 100
        if wr >= target_wr:
            rec_threshold = b
            break

    rec_path = os.path.join(OUTPUT_DIR, "threshold_recommendation.txt")
    with open(rec_path, "w") as f:
        f.write("WALK-FORWARD BACKTEST — THRESHOLD RECOMMENDATIONS\n")
        f.write("=" * 55 + "\n")
        f.write(f"Run date: {datetime.date.today()}\n")
        f.write(f"Symbols tested: {len(symbols)}\n")
        f.write(f"Total bars: {len(all_rows)}\n\n")
        f.write(f"Recommended min_confidence (60% win rate threshold): {rec_threshold}\n\n")
        f.write("Win rate by confidence bucket:\n")
        f.write(f"{'Bucket':>8} | {'Total':>6} | {'WinRate':>8} | {'StopRate':>9}\n")
        f.write("-" * 40 + "\n")
        for b in sorted(buckets.keys()):
            d  = buckets[b]
            if d["total"] < 5:
                continue
            wr = d["t1"] / d["total"] * 100
            sr = d["stop"] / d["total"] * 100
            f.write(f"{b:>8} | {d['total']:>6} | {wr:>7.1f}% | {sr:>8.1f}%\n")
        f.write("\n")
        f.write("HOW TO USE:\n")
        f.write("1. Update REGIME_THRESHOLDS in main.py min_confidence values\n")
        f.write("   based on the bucket where win_rate >= 60%\n")
        f.write("2. Tighten thresholds by 3-5 pts during earnings season\n")
        f.write("3. Re-run this backtest quarterly to recalibrate\n")
    print(f"Saved: {rec_path}")

    # ── Summary ────────────────────────────────────────────────────────────────
    total     = len(all_rows)
    t1_all    = sum(1 for r in all_rows if r["outcome"] == "T1_HIT")
    stop_all  = sum(1 for r in all_rows if r["outcome"] == "STOP_HIT")
    time_all  = sum(1 for r in all_rows if r["outcome"] == "TIME_EXIT")

    summ_path = os.path.join(OUTPUT_DIR, "backtest_summary.txt")
    with open(summ_path, "w") as f:
        f.write("BACKTEST SUMMARY\n")
        f.write("=" * 40 + "\n")
        f.write(f"Total bars tested : {total}\n")
        f.write(f"T1 hit (wins)     : {t1_all} ({t1_all/total*100:.1f}%)\n")
        f.write(f"Stop hit (losses) : {stop_all} ({stop_all/total*100:.1f}%)\n")
        f.write(f"Time exit         : {time_all} ({time_all/total*100:.1f}%)\n")
        f.write(f"\nRecommended min_confidence: {rec_threshold}\n")
    print(f"Saved: {summ_path}")

    print("\n" + "=" * 60)
    print(f"RESULTS: {total} bars | T1 hit: {t1_all/total*100:.1f}% | Stop: {stop_all/total*100:.1f}%")
    print(f"Recommended min_confidence: {rec_threshold}")
    print("=" * 60)


if __name__ == "__main__":
    run_backtest()
