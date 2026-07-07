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
# Phase G4 (2026-07-02): output directory is env-controllable so scheduled
# backtests can write to a staging folder without overwriting the live
# calibration files. Default preserves legacy behaviour.
OUTPUT_DIR        = os.getenv("BACKTEST_OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Phase C7 (2026-07-02): Realistic slippage model. Real fills differ from the
# close by ~10-30bps on entry (chasing on open) and ~15-30bps on exit
# (stop-hits fill BELOW the stop level, targets fill AT the level, not above).
# These defaults were chosen conservatively — a paper backtest with zero
# slippage systematically overstates win rate by 3-8% vs live trading.
# Override via env vars if you want stress-test scenarios.
SLIPPAGE_ENTRY_PCT = float(os.getenv("BT_SLIPPAGE_ENTRY_PCT", "0.15")) / 100.0
SLIPPAGE_STOP_PCT  = float(os.getenv("BT_SLIPPAGE_STOP_PCT",  "0.20")) / 100.0
SLIPPAGE_TARGET_PCT = float(os.getenv("BT_SLIPPAGE_TARGET_PCT", "0.00")) / 100.0

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
                    max_hold: int = 20) -> tuple:
    """
    Simulates holding from bar i.

    Returns (outcome, r_multiple, exit_bar_offset)
      outcome        : "T1_HIT" | "STOP_HIT" | "TIME_EXIT"
      r_multiple     : realised return as multiples of initial risk (R).
                       +2R for a T1_HIT, -1R for a STOP_HIT, and
                       (exit_price − entry)/risk for a TIME_EXIT. Enables
                       expectancy + profit-factor math.
      exit_bar_offset: bars held from entry (1..max_hold). Enables average-
                       holding-period metric per bucket.

    Phase G5 (2026-07-03): Return R-multiple + exit-bar offset so the caller
    can compute expectancy, profit factor, and average holding period per
    setup / regime / sector bucket. This is what turns raw hit-rate into
    an actionable edge signal.

    Phase C7 (2026-07-02): Realistic slippage:
      - Entry filled at close × (1 + SLIPPAGE_ENTRY_PCT) — chasing cost
      - Stop fills at stop × (1 - SLIPPAGE_STOP_PCT) — slip through stop
      - Target fills at t1 × (1 - SLIPPAGE_TARGET_PCT) — limit orders
        don't slip UP so this is usually 0.
    Net effect: entry raised, stop hurdle unchanged (widest interpretation).
    """
    try:
        raw_entry = closes[i]
        entry     = raw_entry * (1.0 + SLIPPAGE_ENTRY_PCT)
        atr   = atr14_arr[i]
        ema20 = ema20_arr[i]
        stop  = max(ema20 - 0.5 * atr, entry * 0.92)
        if stop >= entry:
            stop = entry * 0.94
        t1    = entry + 2.0 * atr

        risk_per_share = entry - stop
        if risk_per_share <= 0:
            # Defensive: degenerate risk — treat as TIME_EXIT flat.
            return ("TIME_EXIT", 0.0, max_hold)

        for j in range(i + 1, min(i + max_hold + 1, len(closes))):
            low_j  = lows[j]
            high_j = highs[j]
            # STOP check first — worst case wins on same bar (conservative).
            if low_j <= stop:
                slipped_stop = stop * (1.0 - SLIPPAGE_STOP_PCT)
                r = (slipped_stop - entry) / risk_per_share
                return ("STOP_HIT", r, j - i)
            if high_j >= t1:
                slipped_t1 = t1 * (1.0 - SLIPPAGE_TARGET_PCT)
                r = (slipped_t1 - entry) / risk_per_share
                return ("T1_HIT", r, j - i)

        exit_bar = min(i + max_hold, len(closes) - 1)
        exit_px  = closes[exit_bar]
        r        = (exit_px - entry) / risk_per_share
        return ("TIME_EXIT", r, exit_bar - i)
    except Exception:
        return ("TIME_EXIT", 0.0, max_hold)


# ─── Phase G5 (2026-07-03): setup classifier ────────────────────────────────
# Cheap heuristics that mirror how main.py labels a setup. Precision is not
# critical here — we just need enough separation to see if one setup type
# systematically outperforms others. A trade at bar i is classified by the
# price context ENDING at bar i, using NO forward data.

def _classify_setup_at(i: int, closes, highs, lows) -> str:
    """Return BREAKOUT | PULLBACK | MOMENTUM | REVERSAL | OTHER."""
    try:
        if i < 25:
            return "OTHER"
        last  = closes[i]
        h20   = float(np.max(highs[i-20:i]))   # prior 20-day high excludes i
        l20   = float(np.min(lows [i-20:i]))
        ema20 = _ema(closes[:i+1], 20)[-1]
        ema50 = _ema(closes[:i+1], 50)[-1]

        ret5  = (last / closes[i-5] - 1) * 100 if i >= 5 else 0.0

        # 1) BREAKOUT — closes above prior 20-day high AND above ema20/50
        if last > h20 * 0.998 and last > ema20 > ema50:
            return "BREAKOUT"

        # 2) PULLBACK — uptrend intact (ema20 > ema50 & price > ema50) but
        #    price has dipped toward the 20-EMA (within 1.5%) with a
        #    negative 5-day return.
        if ema20 > ema50 and last > ema50 and ret5 < 0 \
           and abs(last - ema20) / ema20 < 0.015:
            return "PULLBACK"

        # 3) REVERSAL — price recently made a 20-day low then bounced 5-20%.
        #    Historically the LEAST reliable setup type; want visibility.
        if last > ema20 and last < ema50 and 0.05 < (last - l20) / l20 < 0.20:
            return "REVERSAL"

        # 4) MOMENTUM — sustained uptrend, no immediate breakout event,
        #    strong 5-day return.
        if last > ema20 > ema50 and ret5 > 2.0:
            return "MOMENTUM"

        return "OTHER"
    except Exception:
        return "OTHER"


# ─── Phase G5 (2026-07-03): per-stock regime proxy ──────────────────────────
# main.py computes market regime from NIFTY. To avoid a second yfinance
# download per backtest (2× runtime cost), we approximate regime from each
# stock's own EMA structure. This is a proxy, not the real thing — but it
# separates trend-up from trend-down bars cleanly enough to see if one
# regime bucket systematically loses money.

def _classify_regime_at(i: int, closes) -> str:
    """Return BULLISH | CAUTIOUS_BULLISH | SIDEWAYS | WEAK."""
    try:
        if i < 60:
            return "SIDEWAYS"
        c = closes[:i+1]
        last   = c[-1]
        ema20  = _ema(c, 20)[-1]
        ema50  = _ema(c, 50)[-1]
        ema200 = _ema(c, 200)[-1] if len(c) >= 200 else ema50
        ret20  = (last / c[-21] - 1) * 100 if len(c) >= 22 else 0.0

        if last > ema20 > ema50 > ema200 and ret20 > 3.0:
            return "BULLISH"
        if last > ema20 > ema50 and ret20 > 0:
            return "CAUTIOUS_BULLISH"
        if abs(last - ema20) / ema20 < 0.03 and abs(ret20) < 3.0:
            return "SIDEWAYS"
        return "WEAK"
    except Exception:
        return "SIDEWAYS"


# ─── Phase G5 (2026-07-03): sector map loader (cached, no network) ──────────
_SECTOR_MAP_CACHE: dict = {}


def _load_sector_map() -> dict:
    """Load sector_cache.json (produced by main.py) as {SYMBOL: SECTOR}."""
    global _SECTOR_MAP_CACHE
    if _SECTOR_MAP_CACHE:
        return _SECTOR_MAP_CACHE
    try:
        import json as _json
        p = os.getenv("SECTOR_CACHE_FILE", "sector_cache.json")
        with open(p, "r", encoding="utf-8") as f:
            _SECTOR_MAP_CACHE = _json.load(f) or {}
    except Exception:
        _SECTOR_MAP_CACHE = {}
    return _SECTOR_MAP_CACHE


def _process_symbol(symbol: str, period: str = "2y") -> list:
    """
    Downloads data and runs backtest for one symbol.

    Returns list of {symbol, date, confidence, outcome, r_multiple,
    exit_bars, setup, regime, sector} dicts.

    Phase G5 (2026-07-03): rows now include r_multiple + exit_bars + setup
    + regime + sector so the analysis phase can produce actionable
    breakdowns.
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
        atr14_arr = np.concatenate([[np.nan]*15, atr14_arr])[:len(closes)]
        ema20_arr = _ema(closes, 20)

        sector = _load_sector_map().get(symbol, "UNKNOWN")

        for i in range(60, len(closes) - MAX_HOLD_BARS - 1):
            if np.isnan(atr14_arr[i]):
                continue
            conf = _confidence_at(i, closes, highs, lows, volumes)
            if conf < 40:
                continue
            outcome, r_mult, exit_bars = _simulate_trade(
                i, closes, highs, lows, atr14_arr, ema20_arr
            )
            setup  = _classify_setup_at(i, closes, highs, lows)
            regime = _classify_regime_at(i, closes)
            rows.append({
                "symbol":     symbol,
                "date":       str(dates[i])[:10],
                "confidence": conf,
                "outcome":    outcome,
                "r_multiple": round(r_mult, 3),
                "exit_bars":  int(exit_bars),
                "setup":      setup,
                "regime":     regime,
                "sector":     sector,
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
    # 2026-07-07 fix: rows in `all_rows` may carry extra fields (exit_bars,
    # regime, sector, r_multiple, setup) added by later trade-simulator
    # enhancements. Set extrasaction="ignore" so DictWriter drops unknown
    # keys instead of raising ValueError. Full-fat rows are also written to
    # backtest_raw_full.csv below for anyone who wants everything.
    raw_path = os.path.join(OUTPUT_DIR, "backtest_raw.csv")
    with open(raw_path, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["symbol", "date", "confidence", "outcome"],
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(all_rows)
    print(f"Saved: {raw_path}")

    # Full-fat dump — includes exit_bars/regime/sector/r_multiple/setup when
    # present. Auto-derives field list from the union of all row keys.
    if all_rows:
        full_fields = sorted({k for r in all_rows for k in r.keys()})
        full_path = os.path.join(OUTPUT_DIR, "backtest_raw_full.csv")
        with open(full_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=full_fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)
        print(f"Saved: {full_path}")

    # ── Threshold recommendations ──────────────────────────────────────────────
    # Break-even math: with T1=+5% and Stop=-3%, R/R ≈ 1.67
    #                  → break-even win rate ≈ 1 / (1 + 1.67) = 37.5%
    # We add a comfortable safety cushion and target 45% win rate.
    # Empirically the peak we see across NSE (2y) is ~48% in the 70-80 band,
    # so 45% picks the SWEET SPOT rather than falling back to the default.
    target_wr = 45.0            # was 60.0 — unreachable, always fell back to default
    min_bucket_size = 100       # need at least 100 trades for the number to be stable
    rec_threshold = 70          # sensible default aligned with backtest evidence (was 85)
    for b in sorted(buckets.keys()):
        d  = buckets[b]
        if d["total"] < min_bucket_size:
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
        f.write(f"Recommended min_confidence ({target_wr:.0f}% win rate threshold, {min_bucket_size}+ trades): {rec_threshold}\n\n")
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

    # ── Phase C7e (2026-07-02): JSON sidecar for research_job consumption ─────
    # research_job._analyze_backtest_vs_live looks for backtest_summary.json /
    # backtest_results.json / walkforward_results.json. Emit the same summary
    # as JSON so that sheet can populate.
    try:
        import json as _json
        # Approximate a Sharpe proxy from win_rate + payoff. We don't track
        # per-bar returns here so this is a scalar rough estimate. Downstream
        # code treats missing/zero Sharpe gracefully.
        win_rate = (t1_all / total * 100.0) if total > 0 else 0.0
        # Payoff = %T1 - %STOP as a very rough per-bar avg return proxy
        avg_return_pct = ((t1_all - stop_all) / total * 1.0) if total > 0 else 0.0

        # ── Phase G6 (2026-07-07): Monte Carlo permutation + bootstrap CI ──
        # Guards against luck. Reconstruct per-trade P&L% from stored rows:
        # T1_HIT ≈ +target, STOP ≈ -stop. If the row carries an explicit
        # `pnl_pct` field we prefer that.
        adv_stats = {"available": False, "reason": "backtest_stats disabled"}
        try:
            import backtest_stats as _bs  # local import to keep top-level clean
            pnls: list = []
            for _row in all_rows:
                if "pnl_pct" in _row and _row["pnl_pct"] is not None:
                    pnls.append(float(_row["pnl_pct"]))
                    continue
                # fall back to outcome mapping (approximation)
                _oc = _row.get("outcome")
                _tgt = float(_row.get("target_pct") or _row.get("t1_pct") or 3.0)
                _stp = float(_row.get("stop_pct")   or 1.5)
                if _oc == "T1_HIT":
                    pnls.append(+_tgt)
                elif _oc == "STOP_HIT":
                    pnls.append(-_stp)
                else:  # TIME_EXIT — assume near-flat
                    pnls.append(0.0)
            # `n_trials_tested` reflects the parameter sweep breadth. We test a
            # single canonical configuration per run, but a conservative deflation
            # of 10 acknowledges implicit threshold hunting during development.
            adv_stats = _bs.compute_all(
                pnls,
                n_trials_tested=int(os.environ.get("BT_N_TRIALS", "10")),
                trades_per_year=max(1, int(total / 2)),  # ~2y of data typically
            )
        except Exception as _e:
            adv_stats = {"available": False, "reason": f"error: {_e}"}
        # Extract a canonical Sharpe for backward compat
        _sr = 0.0
        if adv_stats.get("available"):
            _sr = float(adv_stats.get("sr_annualised", 0.0) or 0.0)

        summary_json = {
            "trades":               total,
            "n_trades":             total,
            "win_rate":             round(win_rate, 2),
            "winrate":              round(win_rate, 2),
            "stop_rate":            round((stop_all / total * 100.0) if total > 0 else 0.0, 2),
            "time_exit_rate":       round((time_all / total * 100.0) if total > 0 else 0.0, 2),
            "avg_return":           round(avg_return_pct, 2),
            "avg_return_pct":       round(avg_return_pct, 2),
            "sharpe":               round(_sr, 4),
            "sharpe_ratio":         round(_sr, 4),
            "advanced_stats":       adv_stats,     # Phase G6 — MC / bootstrap (Stage B: DSR removed)
            "recommended_min_conf": rec_threshold,
            "run_date":             str(datetime.date.today()),
            "source":               "backtest_walkforward.py",
        }
        summ_json_path = os.path.join(OUTPUT_DIR, "backtest_summary.json")
        with open(summ_json_path, "w") as f:
            _json.dump(summary_json, f, indent=2)
        print(f"Saved: {summ_json_path}")
    except Exception as _e:
        print(f"[WARN] Could not write backtest_summary.json: {_e}")

    
    # ─── Phase G5 (2026-07-03): actionable breakdowns ─────────────────────
    # Raw hit-rate alone isn't actionable — you need to know WHICH bucket
    # the losing trades cluster in. These four reports let you answer:
    #   * "which setup type loses money?"        → backtest_by_setup.csv
    #   * "which regime kills my edge?"          → backtest_by_regime.csv
    #   * "which sector is a systematic loser?"  → backtest_by_sector.csv
    #   * "is expectancy actually positive?"     → backtest_expectancy.txt
    #
    # Once these files exist, the next iteration of main.py can add:
    #   * setup weightings                (favour BREAKOUT, deprioritise REVERSAL)
    #   * regime-conditional confidence   (raise bar in SIDEWAYS)
    #   * sector exclusions               (blacklist METALS if it's -EV)
    # This is the single biggest lever for improving live edge.

    def _breakdown(rows_all, key, min_n=20):
        """
        Group rows_all by rows_all[key] and compute:
          n, wins, losses, time_exits, win_rate_pct, stop_rate_pct,
          avg_r, expectancy_R, profit_factor, avg_hold_bars.
        Buckets with n < min_n are still emitted (marked LOW_N) so we do
        not silently drop rare-but-important buckets like REVERSAL that
        may be small but consistently losing.
        """
        by: dict = {}
        for r in rows_all:
            k = r.get(key) or "UNKNOWN"
            b = by.setdefault(k, {"rs": [], "outs": [], "holds": []})
            b["rs"].append(float(r.get("r_multiple", 0.0)))
            b["outs"].append(r.get("outcome"))
            b["holds"].append(int(r.get("exit_bars", 0)))
        result = []
        for k, b in by.items():
            n = len(b["rs"])
            wins = sum(1 for o in b["outs"] if o == "T1_HIT")
            losses = sum(1 for o in b["outs"] if o == "STOP_HIT")
            times = sum(1 for o in b["outs"] if o == "TIME_EXIT")
            avg_r = sum(b["rs"]) / n if n else 0.0
            # Expectancy per trade in R units — this is THE key stat.
            expectancy = avg_r
            # Profit factor = gross R gained / gross R lost. Anything above
            # 1.0 is profitable; above 1.5 is a decent system.
            gains = sum(x for x in b["rs"] if x > 0)
            drags = -sum(x for x in b["rs"] if x < 0)
            pf = (gains / drags) if drags > 0 else float("inf") if gains > 0 else 0.0
            avg_hold = sum(b["holds"]) / n if n else 0.0
            wr = (wins / n * 100) if n else 0.0
            sr = (losses / n * 100) if n else 0.0
            result.append({
                "bucket":         k,
                "n":              n,
                "wins":           wins,
                "losses":         losses,
                "time_exits":     times,
                "win_rate_pct":   round(wr, 1),
                "stop_rate_pct":  round(sr, 1),
                "avg_r":          round(avg_r, 3),
                "expectancy_R":   round(expectancy, 3),
                "profit_factor":  round(pf, 2) if pf != float("inf") else "inf",
                "avg_hold_bars":  round(avg_hold, 1),
                "low_n":          "yes" if n < min_n else "no",
            })
        # Rank: worst-expectancy bucket first (that's what you'll act on)
        result.sort(key=lambda x: x["expectancy_R"])
        return result

    def _write_breakdown_csv(name, rows_bd):
        path = os.path.join(OUTPUT_DIR, name)
        cols = ["bucket", "n", "wins", "losses", "time_exits",
                "win_rate_pct", "stop_rate_pct", "avg_r", "expectancy_R",
                "profit_factor", "avg_hold_bars", "low_n"]
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows_bd)
        print(f"Saved: {path}")

    _write_breakdown_csv("backtest_by_setup.csv",
                         _breakdown(all_rows, "setup"))
    _write_breakdown_csv("backtest_by_regime.csv",
                         _breakdown(all_rows, "regime"))
    _write_breakdown_csv("backtest_by_sector.csv",
                         _breakdown(all_rows, "sector"))

    # ─── Expectancy report (plain text, human-readable) ──────────────────
    exp_path = os.path.join(OUTPUT_DIR, "backtest_expectancy.txt")
    total_r = sum(float(r.get("r_multiple", 0.0)) for r in all_rows)
    gains_r = sum(float(r.get("r_multiple", 0.0)) for r in all_rows
                  if float(r.get("r_multiple", 0.0)) > 0)
    drags_r = -sum(float(r.get("r_multiple", 0.0)) for r in all_rows
                   if float(r.get("r_multiple", 0.0)) < 0)
    with open(exp_path, "w") as f:
        f.write("EXPECTANCY REPORT — WALK-FORWARD BACKTEST\n")
        f.write("=" * 55 + "\n")
        f.write(f"Run date        : {datetime.date.today()}\n")
        f.write(f"Trades          : {len(all_rows)}\n")
        f.write(f"Total R         : {total_r:+.2f}\n")
        f.write(f"Expectancy / trade: {(total_r / len(all_rows) if all_rows else 0):+.3f} R\n")
        f.write(f"Profit factor   : {(gains_r / drags_r if drags_r > 0 else float('inf')):.2f}\n")
        f.write("\n")
        f.write("INTERPRETATION:\n")
        f.write("  Expectancy > 0.10 R    : reasonable edge\n")
        f.write("  Expectancy > 0.20 R    : strong edge\n")
        f.write("  Expectancy < 0         : LOSING SYSTEM — do not follow live\n")
        f.write("  Profit factor > 1.5    : good\n")
        f.write("  Profit factor > 2.0    : excellent\n")
        f.write("\n")
        f.write("ACTION ITEMS: look at backtest_by_setup.csv,\n")
        f.write("backtest_by_regime.csv, and backtest_by_sector.csv for the\n")
        f.write("worst-expectancy buckets (sorted first). These are the buckets\n")
        f.write("to blacklist, tighten thresholds on, or reduce weighting for.\n")
    print(f"Saved: {exp_path}")

    print("\n" + "=" * 60)
    print(f"RESULTS: {total} bars | T1 hit: {t1_all/total*100:.1f}% | Stop: {stop_all/total*100:.1f}%")
    print(f"Recommended min_confidence: {rec_threshold}")
    print("=" * 60)


if __name__ == "__main__":
    run_backtest()
