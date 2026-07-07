"""
options_data.py
================
Options analytics for NSE F&O stocks — PCR (Put-Call Ratio), Max-Pain, and
OI (Open Interest) build-up. These are among the most predictive short-term
signals for individual F&O names on NSE.

Data source: `nselib.derivatives.nse_live_option_chain` (already in requirements).

Concepts:
    - PCR (OI): sum(put_OI) / sum(call_OI)
        > 1.3  = very bearish sentiment (contrarian bullish?)
        < 0.7  = very bullish sentiment (contrarian bearish?)
        0.8 - 1.2 = neutral

    - Max Pain: strike at which total option-holder loss is maximum
      (equivalently, option-writers' profit maximum). Price often gravitates
      toward this strike near expiry.

    - OI Build-up:
        Price ↑ + OI ↑ = LONG_BUILDUP  (bullish)
        Price ↓ + OI ↑ = SHORT_BUILDUP (bearish)
        Price ↑ + OI ↓ = SHORT_COVERING (bullish, unwinding shorts)
        Price ↓ + OI ↓ = LONG_UNWINDING (bearish)

Public API:
    fetch_option_chain(symbol: str) -> dict | None
    compute_pcr(chain: dict) -> dict
    compute_max_pain(chain: dict) -> dict
    options_signal(symbol: str, spot: float, prev_oi: dict = None) -> dict

Feature flag: `ENABLE_OPTIONS_GATE=true|false` (default true — soft penalty).

Rate-limit conscious: caches responses for 5 minutes to avoid NSE throttling.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 min
_CACHE: Dict[str, Tuple[float, Any]] = {}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_option_chain(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Return a normalised option chain:
        {
            "symbol": "RELIANCE",
            "underlying": 2456.75,
            "expiry": "27-Feb-2026",
            "strikes": [
                {"strike": 2400, "call_oi": 12345, "call_chg_oi": 500,
                 "call_iv": 22.3, "put_oi": 8000, "put_chg_oi": -100,
                 "put_iv": 24.1, "call_ltp": 65.0, "put_ltp": 22.0},
                ...
            ]
        }
    Returns None on failure. Cached for 5 minutes.
    """
    now = time.time()
    if symbol in _CACHE:
        ts, data = _CACHE[symbol]
        if now - ts < _CACHE_TTL:
            return data

    try:
        from nselib import derivatives as _der
    except Exception as e:
        log.warning("options_data: nselib not available: %s", e)
        return None

    raw = None
    for attempt in range(3):
        try:
            raw = _der.nse_live_option_chain(symbol)
            if raw is not None:
                break
        except Exception as e:
            log.debug("options_data: nse_live_option_chain(%s) attempt %d failed: %s",
                      symbol, attempt + 1, e)
            time.sleep(1.5 * (attempt + 1))
    if raw is None:
        return None

    normalised = _normalise_chain(symbol, raw)
    if normalised is not None:
        _CACHE[symbol] = (now, normalised)
    return normalised


def _normalise_chain(symbol: str, raw: Any) -> Optional[Dict[str, Any]]:
    """Convert nselib output to a stable shape.

    nselib returns a pandas DataFrame with the option chain — one row per
    strike with call/put columns. Column names in nselib (as of 2.5.x) are:
        Strike Price, CALLS_OI, CALLS_CHNG_IN_OI, CALLS_IV, CALLS_LTP,
        PUTS_OI, PUTS_CHNG_IN_OI, PUTS_IV, PUTS_LTP  (case varies by version).
    We do case-insensitive matching.
    """
    try:
        import pandas as pd
        if not isinstance(raw, pd.DataFrame) or raw.empty:
            return None
        cols = {c.lower().replace(" ", "_"): c for c in raw.columns}

        def col(*candidates):
            for cand in candidates:
                if cand in cols:
                    return cols[cand]
            return None

        c_strike   = col("strike_price", "strike")
        c_call_oi  = col("calls_oi", "call_oi", "ce_oi")
        c_call_chg = col("calls_chng_in_oi", "call_chng_in_oi", "ce_chng_oi", "calls_chg_oi")
        c_call_iv  = col("calls_iv", "call_iv", "ce_iv")
        c_call_ltp = col("calls_ltp", "call_ltp", "ce_ltp")
        c_put_oi   = col("puts_oi", "put_oi", "pe_oi")
        c_put_chg  = col("puts_chng_in_oi", "put_chng_in_oi", "pe_chng_oi", "puts_chg_oi")
        c_put_iv   = col("puts_iv", "put_iv", "pe_iv")
        c_put_ltp  = col("puts_ltp", "put_ltp", "pe_ltp")

        if not c_strike:
            log.warning("options_data: no strike column for %s", symbol)
            return None

        # Underlying price
        underlying = None
        for cand in ("underlying_value", "underlying", "spot_price"):
            if cand in cols:
                try:
                    underlying = float(raw[cols[cand]].dropna().iloc[0])
                    break
                except Exception:
                    pass

        strikes: List[Dict[str, Any]] = []
        for _, row in raw.iterrows():
            try:
                strike = float(row[c_strike])
            except (TypeError, ValueError):
                continue
            entry = {"strike": strike}
            for label, col_ref in (
                ("call_oi", c_call_oi), ("call_chg_oi", c_call_chg),
                ("call_iv", c_call_iv), ("call_ltp", c_call_ltp),
                ("put_oi",  c_put_oi),  ("put_chg_oi", c_put_chg),
                ("put_iv",  c_put_iv),  ("put_ltp",  c_put_ltp),
            ):
                v = 0.0
                if col_ref is not None:
                    try:
                        raw_v = row[col_ref]
                        if raw_v is not None and str(raw_v).strip() not in ("", "-", "nan"):
                            v = float(raw_v)
                    except (TypeError, ValueError):
                        v = 0.0
                entry[label] = v
            strikes.append(entry)
        if not strikes:
            return None
        strikes.sort(key=lambda x: x["strike"])
        return {
            "symbol": symbol,
            "underlying": underlying,
            "strikes": strikes,
            "n_strikes": len(strikes),
        }
    except Exception as e:
        log.warning("options_data: normalise failed for %s: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def compute_pcr(chain: Dict[str, Any]) -> Dict[str, Any]:
    """Put-Call Ratio on OI (across all strikes) + on ATM ±3 strikes."""
    if not chain or not chain.get("strikes"):
        return {"pcr_all": None, "pcr_atm": None}

    strikes = chain["strikes"]
    total_call_oi = sum(s.get("call_oi", 0) or 0 for s in strikes)
    total_put_oi  = sum(s.get("put_oi", 0) or 0 for s in strikes)
    pcr_all = (total_put_oi / total_call_oi) if total_call_oi > 0 else None

    # ATM band
    spot = chain.get("underlying")
    pcr_atm = None
    if spot is not None and strikes:
        # find nearest strike
        atm_idx = min(range(len(strikes)),
                      key=lambda i: abs(strikes[i]["strike"] - spot))
        lo = max(0, atm_idx - 3); hi = min(len(strikes), atm_idx + 4)
        band = strikes[lo:hi]
        c_oi = sum(s.get("call_oi", 0) or 0 for s in band)
        p_oi = sum(s.get("put_oi", 0) or 0 for s in band)
        pcr_atm = (p_oi / c_oi) if c_oi > 0 else None

    sentiment = _pcr_sentiment(pcr_atm if pcr_atm is not None else pcr_all)
    return {
        "pcr_all": round(pcr_all, 3) if pcr_all is not None else None,
        "pcr_atm": round(pcr_atm, 3) if pcr_atm is not None else None,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "sentiment": sentiment,
    }


def _pcr_sentiment(pcr: Optional[float]) -> str:
    if pcr is None:
        return "UNKNOWN"
    if pcr >= 1.5:  return "EXTREME_BEARISH"
    if pcr >= 1.2:  return "BEARISH"
    if pcr >= 0.85: return "NEUTRAL"
    if pcr >= 0.6:  return "BULLISH"
    return "EXTREME_BULLISH"


def compute_max_pain(chain: Dict[str, Any]) -> Dict[str, Any]:
    """
    Max Pain: strike K* that minimises total option-writer loss at expiry.
    Total loss at K = Σ_strikes call_oi(k) * max(0, K - k) + put_oi(k) * max(0, k - K)
    """
    if not chain or not chain.get("strikes"):
        return {"max_pain": None}
    strikes = chain["strikes"]
    K_list = [s["strike"] for s in strikes]

    best_K = None; best_pain = float("inf")
    for K in K_list:
        pain = 0.0
        for s in strikes:
            k = s["strike"]
            pain += (s.get("call_oi", 0) or 0) * max(0.0, K - k)
            pain += (s.get("put_oi", 0)  or 0) * max(0.0, k - K)
        if pain < best_pain:
            best_pain = pain
            best_K = K

    spot = chain.get("underlying")
    dist_pct = None
    if best_K is not None and spot and spot > 0:
        dist_pct = (best_K - spot) / spot * 100.0
    return {
        "max_pain": best_K,
        "underlying": spot,
        "distance_pct": round(dist_pct, 2) if dist_pct is not None else None,
        # bias: positive dist = market "wants" to rise to max-pain (bullish gravity)
        "bias": ("BULLISH" if (dist_pct or 0) > 0.5 else
                 "BEARISH" if (dist_pct or 0) < -0.5 else
                 "NEUTRAL"),
    }


def oi_buildup(spot_pct_change: float, chain_prev: Dict[str, Any],
               chain_now: Dict[str, Any]) -> Dict[str, Any]:
    """Classify OI build-up given yesterday's chain and today's.

    We use *aggregate* OI change (not per-strike) as a proxy — cheap but
    directionally reliable for stock futures / F&O names.
    """
    if not (chain_prev and chain_now):
        return {"buildup": "UNKNOWN"}
    prev_oi = sum((s.get("call_oi", 0) or 0) + (s.get("put_oi", 0) or 0)
                  for s in chain_prev.get("strikes", []))
    now_oi = sum((s.get("call_oi", 0) or 0) + (s.get("put_oi", 0) or 0)
                 for s in chain_now.get("strikes", []))
    if prev_oi <= 0:
        return {"buildup": "UNKNOWN"}
    oi_chg_pct = (now_oi - prev_oi) / prev_oi * 100.0

    if spot_pct_change > 0.1 and oi_chg_pct > 1.0:
        b = "LONG_BUILDUP"
    elif spot_pct_change < -0.1 and oi_chg_pct > 1.0:
        b = "SHORT_BUILDUP"
    elif spot_pct_change > 0.1 and oi_chg_pct < -1.0:
        b = "SHORT_COVERING"
    elif spot_pct_change < -0.1 and oi_chg_pct < -1.0:
        b = "LONG_UNWINDING"
    else:
        b = "NEUTRAL"
    return {
        "buildup": b,
        "spot_pct_change": round(spot_pct_change, 3),
        "oi_pct_change": round(oi_chg_pct, 3),
    }


# ---------------------------------------------------------------------------
# Unified signal for the pipeline
# ---------------------------------------------------------------------------

def options_signal(symbol: str, spot: Optional[float] = None) -> Dict[str, Any]:
    """
    One-shot: fetch chain, compute all metrics, return a compact signal.

    Return shape (safe for main.py integration):
        {
            "ok": True/False,
            "symbol": ...,
            "pcr_all": 1.05, "pcr_atm": 0.98, "sentiment": "NEUTRAL",
            "max_pain": 2450, "max_pain_dist_pct": +1.8, "max_pain_bias": "BULLISH",
            "factor_bonus": -0.15,      # +/- into 10-factor score (soft signal)
            "hard_reject": False,
            "reject_reason": "",
            "warnings": [...]
        }
    """
    out: Dict[str, Any] = {"ok": False, "symbol": symbol,
                           "factor_bonus": 0.0, "hard_reject": False,
                           "reject_reason": "", "warnings": []}
    chain = fetch_option_chain(symbol)
    if not chain:
        out["warnings"].append(f"no option chain for {symbol}")
        return out
    if spot is not None and chain.get("underlying") is None:
        chain["underlying"] = spot

    pcr = compute_pcr(chain)
    mp  = compute_max_pain(chain)

    out.update({
        "ok": True,
        "pcr_all": pcr["pcr_all"],
        "pcr_atm": pcr["pcr_atm"],
        "sentiment": pcr["sentiment"],
        "max_pain": mp["max_pain"],
        "max_pain_dist_pct": mp["distance_pct"],
        "max_pain_bias": mp["bias"],
    })

    # ── Soft-signal scoring (hybrid — no hard reject by default) ────────
    bonus = 0.0
    if pcr["sentiment"] in ("EXTREME_BEARISH",):
        bonus -= 0.30
    elif pcr["sentiment"] == "BEARISH":
        bonus -= 0.15
    elif pcr["sentiment"] == "BULLISH":
        bonus += 0.15
    elif pcr["sentiment"] == "EXTREME_BULLISH":
        # extreme bullishness = crowded trade, small penalty
        bonus += 0.05

    if mp["bias"] == "BULLISH":
        bonus += 0.10
    elif mp["bias"] == "BEARISH":
        bonus -= 0.10

    # Hard gate only if HARD flag is on AND signal is unambiguous
    if os.environ.get("OPTIONS_HARD_GATE", "false").lower() == "true":
        if pcr["sentiment"] == "EXTREME_BEARISH" and mp["bias"] == "BEARISH":
            out["hard_reject"] = True
            out["reject_reason"] = f"Options: PCR={pcr['pcr_atm']:.2f} bearish + max-pain below spot"

    out["factor_bonus"] = round(bonus, 3)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    args = ap.parse_args()
    sig = options_signal(args.symbol)
    print(json.dumps(sig, indent=2, default=str))
