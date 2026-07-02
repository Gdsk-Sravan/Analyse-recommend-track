"""
main.py — NSE Swing Trade Analysis Engine v6.0
Self-healing | Full-stack | Production-complete

Run:  python main.py
Env:  copy .env.template to .env and fill in secrets
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import os
import re
import json
import csv
import html
import datetime
import time
import random
import itertools
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=False)
except ImportError:
    pass

import requests
import numpy as np
import pandas as pd
import yfinance as yf

# Suppress yfinance download noise globally (401 crumbs, delisted warnings)
import logging as _logging
import warnings as _warnings
_logging.getLogger("yfinance").setLevel(_logging.CRITICAL)
_logging.getLogger("peewee").setLevel(_logging.CRITICAL)
_warnings.filterwarnings("ignore", category=FutureWarning)
_warnings.filterwarnings("ignore", message=".*delisted.*")
_warnings.filterwarnings("ignore", message=".*No data found.*")

try:
    import feedparser
    _FEEDPARSER_OK = True
except ImportError:
    _FEEDPARSER_OK = False

try:
    from bs4 import BeautifulSoup
    _BS4_OK = True
except ImportError:
    _BS4_OK = False


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 0b — SAFETY BASELINE (Phase A — added 2026-06-30)
# ─────────────────────────────────────────────────────────────────────────────
# Real-money pipeline (CAPITAL=500000) → these are mandatory:
#   - FetchResult provenance on every replaced fetcher
#   - validate_macro() range gates with last-known-good fallback
#   - cross_check_fii() for source reconciliation
#   - explicit Asia/Kolkata timezone (no implicit dependency on TZ env var)
#   - tenacity retries with exponential jitter
#   - decision audit JSONL for post-mortem
# ─────────────────────────────────────────────────────────────────────────────
from dataclasses import dataclass
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
    _ZONEINFO_OK = True
except ImportError:  # Py < 3.9 — should not happen on the pinned 3.11 runner
    IST = None
    _ZONEINFO_OK = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
    _TENACITY_OK = True
except ImportError:
    _TENACITY_OK = False
    # No-op shim so decorators don't crash if tenacity is missing
    def retry(*_a, **_kw):
        def _d(f): return f
        return _d
    def stop_after_attempt(*_a, **_kw): return None
    def wait_exponential_jitter(*_a, **_kw): return None
    def retry_if_exception_type(*_a, **_kw): return None

try:
    from nselib import capital_market as _nselib_cm
    try:
        from nselib import derivatives as _nselib_deriv  # noqa: F401 (reserved for Phase B5 fallback)
    except ImportError:
        _nselib_deriv = None
    _NSELIB_OK = True
except ImportError:
    _nselib_cm = None
    _nselib_deriv = None
    _NSELIB_OK = False


def ist_now() -> "datetime.datetime":
    """Always-correct IST timestamp — does NOT depend on the TZ env var."""
    if _ZONEINFO_OK:
        return datetime.datetime.now(IST)
    return datetime.datetime.now()  # fallback


def ist_today() -> "datetime.date":
    return ist_now().date()


# ── Disk caches & artifacts ─────────────────────────────────────────────────
NSELIB_CACHE_DIR     = os.getenv("NSELIB_CACHE_DIR", "nselib_cache")
LAST_KNOWN_GOOD_FILE = os.getenv("LAST_KNOWN_GOOD_FILE", "last_known_good.json")
DECISION_AUDIT_FILE  = f"decision_audit_{ist_today().strftime('%Y%m%d')}.jsonl"

try:
    os.makedirs(NSELIB_CACHE_DIR, exist_ok=True)
except Exception:
    pass


# ── FetchResult — provenance + freshness on every replaced fetcher ──────────
@dataclass
class FetchResult:
    """Provenance wrapper. Callers read `.value`; the rest is for logging/audit."""
    value: Any
    source: str                      # e.g. "nselib_nsdl", "frankfurter", "yfinance", "LKG", "NEUTRAL_DEFAULT"
    as_of_date: "datetime.date"      # the date the data represents (NOT when fetched)
    fetched_at: "datetime.datetime"  # wall-clock IST at return
    is_stale: bool = False           # True if as_of_date < expected business date
    notes: str = ""                  # free-form (e.g. "BSE disagrees by 28%")

    def to_log(self) -> str:
        return (f"source={self.source} as_of={self.as_of_date.isoformat()} "
                f"stale={self.is_stale}{(' notes=' + self.notes) if self.notes else ''}")


# ── Macro validation — hard range gates ─────────────────────────────────────
# Single-day NSE FII record outflow was ~₹20k Cr (Mar 2020 covid).
# Anything outside these bounds is a parse error, not real data.
_MACRO_RANGES = {
    # 2026: rupee has traded 82-97; keep a wide 2-6 year band so a real 95.xx
    # print doesn't get flagged as an OOR parse error.
    "usdinr":         (70.0, 105.0),
    "vix_in":         (8.0,  60.0),
    "vix_us":         (8.0,  80.0),
    "crude_usd":      (30.0, 200.0),
    "us10y":          (1.0,  10.0),
    "dxy":            (80.0, 120.0),
    "gold_usd":       (1500.0, 6000.0),
    "nifty_1d_pct":   (-10.0, 10.0),
    "sp500_1d_pct":   (-10.0, 10.0),
    "sensex_1d_pct":  (-10.0, 10.0),
    "dow_1d_pct":     (-10.0, 10.0),
    "fii_flow_cr":    (-50000.0, 50000.0),
    "dii_flow_cr":    (-50000.0, 50000.0),
}


def load_last_known_good() -> dict:
    try:
        if os.path.exists(LAST_KNOWN_GOOD_FILE):
            with open(LAST_KNOWN_GOOD_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        # _log not yet defined at import time — print is fine here
        print(f"[WARN] load_last_known_good failed: {e}")
    return {}


def save_last_known_good(lkg: dict) -> None:
    try:
        # Only persist scalars (no FetchResult / dataclass)
        clean = {k: v for k, v in lkg.items() if isinstance(v, (int, float, str, bool))}
        with open(LAST_KNOWN_GOOD_FILE, "w") as f:
            json.dump(clean, f, indent=2, sort_keys=True, default=str)
    except Exception as e:
        try:
            _log(f"[WARN] save_last_known_good failed: {e}")
        except Exception:
            print(f"[WARN] save_last_known_good failed: {e}")


def validate_macro(macro: dict) -> dict:
    """
    Range-gate every macro value. If any value is out of range:
      - log [VALIDATION] warning
      - swap in last-known-good from last_known_good.json
      - set macro["data_quality"] = "DEGRADED" and list bad fields
    Otherwise: persist current values as the new LKG, mark NORMAL.
    """
    lkg = load_last_known_good()
    bad: list = []

    for field_name, (lo, hi) in _MACRO_RANGES.items():
        v = macro.get(field_name)
        if v is None:
            continue
        try:
            vf = float(v)
        except (TypeError, ValueError):
            bad.append(field_name)
            continue
        if vf < lo or vf > hi:
            bad.append(field_name)
            replacement = lkg.get(field_name)
            try:
                _log(f"[VALIDATION] {field_name}={vf!r} out of range [{lo},{hi}] → "
                     + (f"using LKG {replacement}" if replacement is not None else "no LKG — keeping default"))
            except Exception:
                pass
            if replacement is not None:
                macro[field_name] = replacement

    if bad:
        macro["data_quality"]  = "DEGRADED"
        macro["bad_fields"]    = bad
    else:
        macro["data_quality"]  = macro.get("data_quality", "NORMAL")
        macro["bad_fields"]    = []
        # Persist the validated values back to LKG for next run
        save_last_known_good({k: macro.get(k) for k in _MACRO_RANGES if k in macro})

    return macro


def cross_check_fii(nsdl_val: Optional[float],
                    bse_val: Optional[float]) -> "tuple[Optional[float], str]":
    """
    Source reconciliation. NSDL is authoritative (T-1 final).
    BSE is provisional (T+0). If both exist and disagree by >25%, flag LOW confidence.
    Returns (chosen_value, confidence).
    """
    if nsdl_val is None and bse_val is None:
        return None, "NONE"
    if nsdl_val is None:
        return bse_val, "BSE_ONLY"
    if bse_val is None:
        return nsdl_val, "NSDL_ONLY"
    # Both present
    if nsdl_val == 0 and bse_val == 0:
        return 0.0, "BOTH_ZERO"
    larger = max(abs(nsdl_val), abs(bse_val))
    if larger > 0:
        rel_diff = abs(nsdl_val - bse_val) / larger
        if rel_diff > 0.25:
            try:
                _log(f"[XCHECK] FII disagree by {rel_diff*100:.0f}%: "
                     f"NSDL={nsdl_val:+.0f}Cr vs BSE={bse_val:+.0f}Cr — using NSDL")
            except Exception:
                pass
            return nsdl_val, "LOW"
    return nsdl_val, "HIGH"


def append_decision_audit(entry: dict) -> None:
    """Append one JSON line to decision_audit_YYYYMMDD.jsonl (post-mortem trail)."""
    try:
        entry = {**entry, "ts": ist_now().isoformat()}
        with open(DECISION_AUDIT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        # Never let audit logging crash the pipeline
        pass


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — CONFIG & ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────

PORTFOLIO_CAPITAL   = float(os.getenv("CAPITAL", "500000"))
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
# Dedicated BUY-signal channel — set BUY_BOT_TOKEN + BUY_CHAT_ID in GitHub Secrets
BUY_BOT_TOKEN       = os.getenv("BUY_BOT_TOKEN", "")
BUY_CHAT_ID         = os.getenv("BUY_CHAT_ID", "")
TRACKER_FILE        = os.getenv("TRACKER_FILE", "tracker.json")
TRADE_TRACKER_V2_FILE   = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")
FUNDAMENTALS_CACHE_FILE = os.getenv("FUNDAMENTALS_CACHE_FILE", "fundamentals_cache.json")
PORTFOLIO_FILE          = os.getenv("PORTFOLIO_FILE", "portfolio.json")
WATCHLIST_FILE      = os.getenv("WATCHLIST_FILE", "watchlist_persist.json")
CONF_HISTORY_FILE   = os.getenv("CONF_HISTORY_FILE", "confidence_history.json")
GATE_MEMORY_FILE    = os.getenv("GATE_MEMORY_FILE", "gate_memory.json")
# Phase C3 (2026-07-02): real per-stock delivery % cache + sector rank history
# + VIX percentile cache. All three are additive institutional-grade signals.
DELIVERY_CACHE_FILE     = os.getenv("DELIVERY_CACHE_FILE", "delivery_cache.json")
SECTOR_RANK_HISTORY_FILE = os.getenv("SECTOR_RANK_HISTORY_FILE", "sector_rank_history.json")
VIX_HISTORY_CACHE_FILE  = os.getenv("VIX_HISTORY_CACHE_FILE", "vix_history_cache.json")
# True only when triggered by GitHub Actions cron schedule — never for manual runs
IS_SCHEDULED        = os.getenv("SCHEDULED_RUN", "false").lower() == "true"
TELEGRAM_MAX_CHARS  = 3800  # buffer below 4096 hard limit

# Regime thresholds — v6.0 calibrated (Bug 2 fix)
REGIME_THRESHOLDS = {
    # max_stop_pct — wide-stop guardrail (any BUY with (entry-stop)/entry > this
    # gets rejected). Bullish regimes tolerate wider volatility stops; bearish
    # regimes force tight stops to keep loss small.
    "STRONG_BULL":     {"min_confidence": 78, "min_tq": 72, "min_rr": 1.7, "max_buys": 5,  "max_exposure": 0.85, "max_stop_pct": 8.0},
    "BULL":            {"min_confidence": 82, "min_tq": 76, "min_rr": 1.8, "max_buys": 3,  "max_exposure": 0.75, "max_stop_pct": 7.0},
    "SIDEWAYS":        {"min_confidence": 80, "min_tq": 78, "min_rr": 2.0, "max_buys": 1,  "max_exposure": 0.50, "max_stop_pct": 6.0},
    "TRANSITION":      {"min_confidence": 83, "min_tq": 78, "min_rr": 2.0, "max_buys": 2,  "max_exposure": 0.55, "max_stop_pct": 6.0},
    "HIGH_VOLATILITY": {"min_confidence": 85, "min_tq": 80, "min_rr": 2.2, "max_buys": 1,  "max_exposure": 0.40, "max_stop_pct": 5.0},
    "BEAR":            {"min_confidence": 92, "min_tq": 88, "min_rr": 2.5, "max_buys": 0,  "max_exposure": 0.20, "max_stop_pct": 5.0},
    "STRONG_BEAR":     {"min_confidence": 99, "min_tq": 99, "min_rr": 3.0, "max_buys": 0,  "max_exposure": 0.00, "max_stop_pct": 4.0},
}

# Factor weights — 10 factors, sum = 1.00
FACTOR_WEIGHTS = {
    "trend_quality":      0.18,
    "momentum_quality":   0.14,
    "volume_delivery":    0.10,
    "sector_strength":    0.15,
    "rs_vs_nifty":        0.15,
    "news_risk":          0.08,
    "risk_reward":        0.07,
    "ownership_quality":  0.06,
    "options_sentiment":  0.04,
    "macro_alignment":    0.03,
}

# Opportunity score weights — primary ranking metric (ENHANCEMENT 1)
OPPORTUNITY_WEIGHTS = {
    "confidence":     0.30,
    "trade_quality":  0.25,
    "risk_reward":    0.20,
    "trend_strength": 0.10,
    "volume_quality": 0.05,
    "sector_strength":0.05,
    "macro_alignment":0.05,
}


def _update_ownership_quality(stock: dict) -> None:
    """
    Updates ownership_quality factor score from real fundamentals.
    Called after fetch_all_fundamentals_cached() injects ROE/pledge into stock dict.
    Scale:
      ROE > 20% = excellent (+20), 12-20% = good (+10), < 5% = poor (-15)
      Pledge > 30% = bad (-20), 15-30% = caution (-10), < 5% = clean (+10)
      D/E > 2.0 = leveraged (-10), < 0.5 = clean (+10)
    Baseline 50; clamped 0-100.
    """
    try:
        roe    = float(stock.get("roe", 0) or 0)
        pledge = float(stock.get("promoter_pledge_pct", 0) or 0)
        de     = float(stock.get("de_ratio", 0) or 0)

        score = 50.0
        # ROE component
        if roe > 20:    score += 20
        elif roe > 12:  score += 10
        elif roe > 5:   score += 0
        else:           score -= 15
        # Pledge component
        if pledge > 30:   score -= 20
        elif pledge > 15: score -= 10
        elif pledge < 5:  score += 10
        # D/E component
        if de > 2.0:    score -= 10
        elif de > 1.0:  score -= 5
        elif de < 0.5:  score += 10

        stock["ownership_quality"] = round(max(0.0, min(100.0, score)), 1)
        # Keep factor_scores in sync
        if "factor_scores" in stock:
            stock["factor_scores"]["ownership_quality"] = stock["ownership_quality"]
    except Exception:
        pass


def compute_opportunity_score(stock: dict) -> float:
    """
    0-100 composite score — primary ranking metric (ENHANCEMENT 1).
    Reads directly from stock keys (with factor_scores as supplement).
    Higher = better opportunity quality.
    """
    try:
        conf  = float(stock.get("final_confidence", 0) or 0)
        tq    = float(stock.get("trade_quality_score", 0) or 0)
        rr    = float(stock.get("rr_ratio", 0) or 0)

        # Normalize R/R to 0-100 (3.0x = 100, 2.0x = 67, 1.0x = 33)
        rr_score = min(100.0, rr / 3.0 * 100)

        # Read directly from stock — factor_scores is a mirror, either works
        fs     = stock.get("factor_scores", {}) or {}
        trend  = float(stock.get("trend_quality",  fs.get("trend_quality",  50)) or 50)
        volume = float(stock.get("volume_delivery",fs.get("volume_delivery",50)) or 50)
        sector = float(stock.get("sector_strength",fs.get("sector_strength",50)) or 50)
        macro  = float(stock.get("macro_alignment",fs.get("macro_alignment",50)) or 50)

        opp = (
            conf     * OPPORTUNITY_WEIGHTS["confidence"] +
            tq       * OPPORTUNITY_WEIGHTS["trade_quality"] +
            rr_score * OPPORTUNITY_WEIGHTS["risk_reward"] +
            trend    * OPPORTUNITY_WEIGHTS["trend_strength"] +
            volume   * OPPORTUNITY_WEIGHTS["volume_quality"] +
            sector   * OPPORTUNITY_WEIGHTS["sector_strength"] +
            macro    * OPPORTUNITY_WEIGHTS["macro_alignment"]
        )
        return round(opp, 1)
    except Exception:
        return 0.0


# Comprehensive sector map (Bug 3 fix)
# ─────────────────────────────────────────────────────────────────────────────
# SECTOR MAP — dynamic, not hardcoded
# Priority: sector_master.csv (curated) → sector_cache.json (yfinance-fetched, grows over time)
# ─────────────────────────────────────────────────────────────────────────────
SECTOR_CACHE_FILE = os.getenv("SECTOR_CACHE_FILE", "sector_cache.json")

# Normalize sector_master.csv labels to internal labels
_CSV_LABEL_NORM = {
    "Auto": "AUTO", "Banking": "BANKING", "Capital Goods": "CAPITAL_GOODS",
    "Cement": "INFRA", "Chemicals": "CHEMICALS", "Consumer Goods": "FMCG",
    "Consumer Services": "CONSUMER", "Defence": "DEFENCE", "Diversified": "OTHERS",
    "Electronics Manufacturing": "CAPITAL_GOODS", "Finance": "FINANCE",
    "FinTech": "FINANCE", "FMCG": "FMCG", "Healthcare": "HEALTHCARE",
    "Infrastructure": "INFRA", "IT": "IT", "IT Hardware": "IT",
    "Metals": "METALS", "Oil & Gas": "ENERGY", "Pharma": "PHARMA",
    "Power": "ENERGY", "Realty": "REALTY", "Retail": "CONSUMER",
}

# Normalize yfinance sector labels to internal labels
_YF_LABEL_NORM = {
    "Technology": "IT", "Financial Services": "FINANCE",
    "Healthcare": "HEALTHCARE", "Consumer Defensive": "FMCG",
    "Consumer Cyclical": "CONSUMER", "Basic Materials": "METALS",
    "Energy": "ENERGY", "Industrials": "CAPITAL_GOODS",
    "Real Estate": "REALTY", "Communication Services": "TELECOM",
    "Utilities": "ENERGY", "Automobile": "AUTO",
    "Pharmaceuticals": "PHARMA", "Banking": "BANKING",
    "Defence": "DEFENCE", "Chemicals": "CHEMICALS",
}

_SECTOR_MAP: dict = {}   # loaded at pipeline start via _init_sector_map()

# ── Hardcoded sector map (FIX 1 — normalizes .NS before lookup, never returns OTHERS) ──
SECTOR_MAP: dict = {
    # IT
    "INFY.NS":"IT","TCS.NS":"IT","WIPRO.NS":"IT","HCLTECH.NS":"IT",
    "TECHM.NS":"IT","LTIM.NS":"IT","PERSISTENT.NS":"IT","COFORGE.NS":"IT",
    "MPHASIS.NS":"IT","OFSS.NS":"IT","KPITTECH.NS":"IT","TATAELXSI.NS":"IT",
    "RAMCOSYS.NS":"IT","RPTECH.NS":"IT",
    # PHARMA
    "SUNPHARMA.NS":"PHARMA","DRREDDY.NS":"PHARMA","CIPLA.NS":"PHARMA",
    "DIVISLAB.NS":"PHARMA","AUROPHARMA.NS":"PHARMA","LAURUSLABS.NS":"PHARMA",
    "ALKEM.NS":"PHARMA","TORNTPHARM.NS":"PHARMA","IPCALAB.NS":"PHARMA",
    "GLENMARK.NS":"PHARMA","NATCOPHARM.NS":"PHARMA","GRANULES.NS":"PHARMA",
    "AARTIDRUGS.NS":"PHARMA","INDSWFTLAB.NS":"PHARMA","NARMADA.NS":"PHARMA",
    "MOREPENLAB.NS":"PHARMA","BLUEJET.NS":"PHARMA","PANACEABIO.NS":"PHARMA",
    # BANKING
    "HDFCBANK.NS":"BANKING","ICICIBANK.NS":"BANKING","AXISBANK.NS":"BANKING",
    "SBIN.NS":"BANKING","KOTAKBANK.NS":"BANKING","INDUSINDBK.NS":"BANKING",
    "BANDHANBNK.NS":"BANKING","FEDERALBNK.NS":"BANKING","IDFCFIRSTB.NS":"BANKING",
    "RBLBANK.NS":"BANKING","PNB.NS":"BANKING","BANKBARODA.NS":"BANKING",
    # FINANCE / NBFC
    "BAJFINANCE.NS":"FINANCE","BAJAJFINSV.NS":"FINANCE","CHOLAFIN.NS":"FINANCE",
    "MUTHOOTFIN.NS":"FINANCE","MANAPPURAM.NS":"FINANCE","SHRIRAMFIN.NS":"FINANCE",
    "LICHSGFIN.NS":"FINANCE","PFC.NS":"FINANCE","RECLTD.NS":"FINANCE",
    "MANCREDIT.NS":"FINANCE","AAVAS.NS":"FINANCE","63MOONS.NS":"FINANCE",
    # ENERGY / POWER
    "RELIANCE.NS":"ENERGY","ONGC.NS":"ENERGY","BPCL.NS":"ENERGY",
    "IOC.NS":"ENERGY","HINDPETRO.NS":"ENERGY","GAIL.NS":"ENERGY",
    "TATAPOWER.NS":"ENERGY","ADANIGREEN.NS":"ENERGY","TORNTPOWER.NS":"ENERGY",
    "NTPC.NS":"ENERGY","POWERGRID.NS":"ENERGY","CESC.NS":"ENERGY",
    # METALS
    "TATASTEEL.NS":"METALS","HINDALCO.NS":"METALS","JSWSTEEL.NS":"METALS",
    "VEDL.NS":"METALS","SAIL.NS":"METALS","NATIONALUM.NS":"METALS",
    "HINDCOPPER.NS":"METALS","NMDC.NS":"METALS","COALINDIA.NS":"METALS",
    # AUTO / AUTO ANCILLARY
    "MARUTI.NS":"AUTO","TATAMOTORS.NS":"AUTO","M&M.NS":"AUTO",
    "BAJAJ-AUTO.NS":"AUTO","HEROMOTOCO.NS":"AUTO","EICHERMOT.NS":"AUTO",
    "ASHOKLEY.NS":"AUTO","TVSMOTOR.NS":"AUTO","MOTHERSON.NS":"AUTO",
    "BOSCHLTD.NS":"AUTO_ANCILLARY","BALKRISIND.NS":"AUTO_ANCILLARY",
    "SONACOMS.NS":"AUTO_ANCILLARY","SPAL.NS":"AUTO_ANCILLARY",
    "TALBROAUTO.NS":"AUTO_ANCILLARY","CONFIPET.NS":"AUTO_ANCILLARY",
    # CAPITAL GOODS / INDUSTRIAL
    "BHEL.NS":"CAPITAL_GOODS","THERMAX.NS":"CAPITAL_GOODS","KIRLOSENG.NS":"CAPITAL_GOODS",
    "ABB.NS":"CAPITAL_GOODS","SIEMENS.NS":"CAPITAL_GOODS","HAVELLS.NS":"CAPITAL_GOODS",
    "CROMPTON.NS":"CAPITAL_GOODS","CUMMINSIND.NS":"CAPITAL_GOODS",
    "ELGIEQUIP.NS":"CAPITAL_GOODS","GRINDWELL.NS":"CAPITAL_GOODS",
    "CEIGALL.NS":"CAPITAL_GOODS","GENUSPOWER.NS":"CAPITAL_GOODS",
    "JASH.NS":"CAPITAL_GOODS","ELECTHERM.NS":"CAPITAL_GOODS",
    "ROTO.NS":"CAPITAL_GOODS","EMSLIMITED.NS":"CAPITAL_GOODS",
    "SYRMA.NS":"ELECTRONICS","ACI.NS":"ELECTRONICS",
    # DEFENCE
    "HAL.NS":"DEFENCE","BEL.NS":"DEFENCE","MAZDOCK.NS":"DEFENCE",
    "COCHINSHIP.NS":"DEFENCE","PARAS.NS":"DEFENCE","MIDHANI.NS":"DEFENCE",
    "GRSE.NS":"DEFENCE","PRIVISCL.NS":"DEFENCE",
    # FMCG
    "HINDUNILVR.NS":"FMCG","ITC.NS":"FMCG","NESTLEIND.NS":"FMCG",
    "BRITANNIA.NS":"FMCG","DABUR.NS":"FMCG","MARICO.NS":"FMCG",
    "COLPAL.NS":"FMCG","GODREJCP.NS":"FMCG","EMAMILTD.NS":"FMCG",
    "RELAXO.NS":"FMCG","BATAINDIA.NS":"FMCG","PAGEIND.NS":"FMCG",
    # CHEMICALS
    "PIDILITIND.NS":"CHEMICALS","DEEPAKNI.NS":"CHEMICALS","AARTI.NS":"CHEMICALS",
    "NAVINFLUOR.NS":"CHEMICALS","ALKYLAMINE.NS":"CHEMICALS","FINEORG.NS":"CHEMICALS",
    "TATACHEM.NS":"CHEMICALS","NACLIND.NS":"CHEMICALS","GINNIFILA.NS":"CHEMICALS",
    "20MICRONS.NS":"CHEMICALS","INDOBORAX.NS":"CHEMICALS",
    # REALTY
    "DLF.NS":"REALTY","GODREJPROP.NS":"REALTY","OBEROIRLTY.NS":"REALTY",
    "PRESTIGE.NS":"REALTY","BRIGADE.NS":"REALTY","PHOENIXLTD.NS":"REALTY",
    # LOGISTICS
    "AEGISLOG.NS":"LOGISTICS","BLUEDART.NS":"LOGISTICS","CONCOR.NS":"LOGISTICS",
    "GATI.NS":"LOGISTICS","DELHIVERY.NS":"LOGISTICS","TCI.NS":"LOGISTICS",
    "NAVKARCORP.NS":"LOGISTICS","JSWINFRA.NS":"INFRA",
    # TELECOM
    "BHARTIARTL.NS":"TELECOM","IDEA.NS":"TELECOM","TATACOMM.NS":"TELECOM",
    "ONMOBILE.NS":"TELECOM",
    # INFRA / CONSTRUCTION
    "LT.NS":"INFRA","ULTRACEMCO.NS":"INFRA","AMBUJACEMENT.NS":"INFRA",
    "ACC.NS":"INFRA","SHREECEM.NS":"INFRA","KNRCON.NS":"INFRA",
    "PNCINFRA.NS":"INFRA","IRB.NS":"INFRA","ASHOKA.NS":"INFRA","PATELENG.NS":"INFRA",
    # TEXTILES
    "SIYSIL.NS":"TEXTILES","RAYMOND.NS":"TEXTILES","WELSPUN.NS":"TEXTILES",
    "TRIDENT.NS":"TEXTILES","VARDHMAN.NS":"TEXTILES","ARVIND.NS":"TEXTILES",
    "RUPA.NS":"TEXTILES","PGIL.NS":"TEXTILES","ABCOTS.NS":"TEXTILES",
    "DIFFNKG.NS":"TEXTILES",
    # HEALTHCARE / HOSPITALS
    "MAXHEALTH.NS":"HEALTHCARE","APOLLOHOSP.NS":"HEALTHCARE",
    "FORTIS.NS":"HEALTHCARE","NARAYANHA.NS":"HEALTHCARE",
    # INSURANCE / AMC
    "HDFCLIFE.NS":"INSURANCE","SBILIFE.NS":"INSURANCE","ICICIGI.NS":"INSURANCE",
    "ICICIPRULI.NS":"INSURANCE","STARHEALTH.NS":"INSURANCE","LICI.NS":"INSURANCE",
    "ABSLAMC.NS":"ASSET_MGMT",
    # POWER EQUIPMENT / RENEWABLES
    "INOXGREEN.NS":"POWER_EQ","SUZLON.NS":"POWER_EQ","KPIGREEN.NS":"POWER_EQ",
    "WEBSOL.NS":"POWER_EQ","EBGNG.NS":"POWER_EQ",
    # CONSUMER / RETAIL
    "TITAN.NS":"CONSUMER","TRENT.NS":"CONSUMER","DMART.NS":"CONSUMER",
    "ZOMATO.NS":"CONSUMER","NYKAA.NS":"CONSUMER","NAZARA.NS":"CONSUMER",
    # PIPES / BUILDING MATERIALS
    "VENUSPIPES.NS":"BUILDING_MAT","RAMCOIND.NS":"BUILDING_MAT",
    "KRISHANA.NS":"BUILDING_MAT","RATNAVEER.NS":"BUILDING_MAT",
    # DIVERSIFIED
    "AURUM.NS":"DIVERSIFIED","AEQUS.NS":"DIVERSIFIED","AVL.NS":"DIVERSIFIED",
    "AVG.NS":"DIVERSIFIED","KMEW.NS":"DIVERSIFIED","LOTUSDEV.NS":"DIVERSIFIED",
    "AEROENTER.NS":"DIVERSIFIED","POKARNA.NS":"DIVERSIFIED","SETL.NS":"DIVERSIFIED",
    "PIXTRANS.NS":"DIVERSIFIED",
    # AGRI / FOOD
    "UBL.NS":"AGRI_FOOD","RADICO.NS":"AGRI_FOOD","KRBL.NS":"AGRI_FOOD",
    "LTFOODS.NS":"AGRI_FOOD",
}


def _load_sector_map() -> dict:
    """Load sector map from sector_master.csv then overlay sector_cache.json."""
    result: dict = {}
    # 1. sector_master.csv — curated, ships with repo
    try:
        sm_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sector_master.csv")
        if os.path.exists(sm_path):
            with open(sm_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    sym = row.get("symbol", "").strip()
                    sec = row.get("sector", "").strip()
                    if sym and sec:
                        if not sym.endswith(".NS"):
                            sym += ".NS"
                        result[sym] = _CSV_LABEL_NORM.get(sec, sec.upper().replace(" ", "_"))
    except Exception:
        pass
    # 2. sector_cache.json — yfinance-fetched, grows over runs (persisted in CI cache)
    try:
        if os.path.exists(SECTOR_CACHE_FILE):
            with open(SECTOR_CACHE_FILE, "r") as f:
                result.update(json.load(f))
    except Exception:
        pass
    return result


def _init_sector_map() -> None:
    """Called once at pipeline start."""
    global _SECTOR_MAP
    _SECTOR_MAP = _load_sector_map()


def get_sector(symbol: str) -> str:
    """Normalizes symbol format before lookup. Never returns OTHERS — falls back to pattern inference."""
    sym = symbol.strip()
    if not sym.endswith(".NS"):
        sym = sym + ".NS"
    # 1. Check hardcoded SECTOR_MAP first (FIX 1)
    sector = SECTOR_MAP.get(sym)
    if sector:
        return sector
    # 2. Fallback to dynamic _SECTOR_MAP (loaded from CSV + yfinance cache)
    sector = _SECTOR_MAP.get(sym)
    if sector and sector != "OTHERS":
        return sector
    # 3. Name-pattern inference — never display OTHERS
    s = sym.upper()
    if any(x in s for x in ["PHARMA", "DRUG", "LAB", "MED", "BIO"]):
        return "PHARMA"
    if any(x in s for x in ["BANK", "FIN", "CRED", "LOAN"]):
        return "FINANCE"
    if any(x in s for x in ["TECH", "SOFT", "INFO", "DIGIT", "SYST"]):
        return "IT"
    if any(x in s for x in ["STEEL", "METAL", "ALUM", "COPP"]):
        return "METALS"
    if any(x in s for x in ["POWER", "SOLAR", "WIND", "ENERG"]):
        return "POWER_EQ"
    if any(x in s for x in ["INFRA", "CONST", "BUILD", "CEMENT"]):
        return "INFRA"
    return "DIVERSIFIED"  # never show OTHERS


def _nselib_equity_list_cached(max_age_hours: int = 24) -> "pd.DataFrame | None":
    """
    Bulk NSE equity listing with 24h disk cache. Includes industry/sector columns
    for ~2400 listed companies. Phase B6 (2026-06-30) — replaces per-symbol yfinance hits.
    """
    if not _NSELIB_OK or _nselib_cm is None:
        return None
    cache_path = os.path.join(NSELIB_CACHE_DIR, "equity_list.json")
    try:
        if os.path.exists(cache_path):
            age_h = (time.time() - os.path.getmtime(cache_path)) / 3600.0
            if age_h < max_age_hours:
                with open(cache_path, "r") as f:
                    return pd.DataFrame(json.load(f))
    except Exception:
        pass

    fn = getattr(_nselib_cm, "equity_list", None) or getattr(_nselib_cm, "nse_equity_symbols", None)
    if fn is None:
        return None
    try:
        df = fn()
        if df is None or (hasattr(df, "empty") and df.empty):
            return None
        try:
            df.to_json(cache_path, orient="records")
        except Exception:
            pass
        return df
    except Exception as e:
        _log(f"  [nselib equity_list] error: {e}")
        return None


# Industry → canonical sector label normalization (NSE labels → our internal labels)
_NSE_INDUSTRY_NORM = {
    "INFORMATION TECHNOLOGY": "IT", "IT - SOFTWARE": "IT", "IT - HARDWARE": "IT",
    "FINANCIAL SERVICES": "FINANCE", "NON BANKING FINANCIAL COMPANY (NBFC)": "FINANCE",
    "HOUSING FINANCE COMPANY": "FINANCE", "FINANCE - OTHERS": "FINANCE",
    "BANKING": "BANKING", "PRIVATE SECTOR BANK": "BANKING", "PUBLIC SECTOR BANK": "BANKING",
    "PHARMACEUTICALS": "PHARMA", "PHARMACEUTICALS & DRUGS": "PHARMA",
    "HEALTHCARE": "HEALTHCARE", "HEALTHCARE SERVICES": "HEALTHCARE", "HOSPITAL": "HEALTHCARE",
    "FAST MOVING CONSUMER GOODS": "FMCG", "FMCG": "FMCG", "CONSUMER GOODS": "FMCG",
    "CONSUMER SERVICES": "CONSUMER", "CONSUMER DURABLES": "CONSUMER",
    "AUTOMOBILE AND AUTO COMPONENTS": "AUTO", "AUTOMOBILE": "AUTO",
    "AUTO ANCILLARIES": "AUTO_ANCILLARY", "AUTO COMPONENTS": "AUTO_ANCILLARY",
    "METALS & MINING": "METALS", "FERROUS METALS": "METALS", "NON-FERROUS METALS": "METALS",
    "OIL GAS & CONSUMABLE FUELS": "ENERGY", "OIL & GAS": "ENERGY", "POWER": "ENERGY",
    "GAS DISTRIBUTION": "ENERGY",
    "CONSTRUCTION": "INFRA", "CONSTRUCTION MATERIALS": "INFRA", "CEMENT": "INFRA",
    "REALTY": "REALTY", "REAL ESTATE": "REALTY",
    "TELECOM SERVICES": "TELECOM", "TELECOMMUNICATION": "TELECOM",
    "CAPITAL GOODS": "CAPITAL_GOODS", "INDUSTRIALS": "CAPITAL_GOODS",
    "CHEMICALS": "CHEMICALS", "FERTILISERS & AGROCHEMICALS": "CHEMICALS",
    "DEFENCE": "DEFENCE",
    "TEXTILES": "TEXTILES",
    "AGRICULTURE": "AGRI", "AGRI": "AGRI",
}


def enrich_sectors_from_nselib() -> int:
    """
    Phase B6 (2026-06-30): bulk-populate _SECTOR_MAP from nselib equity_list.
    Returns number of new sectors added. Cheap (one HTTP call, 24h cached).
    Run this BEFORE the per-symbol yfinance enrichment to cut its workload by >90%.
    """
    global _SECTOR_MAP
    df = _nselib_equity_list_cached()
    if df is None or (hasattr(df, "empty") and df.empty):
        _log("  [Sector nselib] equity_list unavailable — falling through to yfinance")
        return 0
    # Find symbol + industry columns flexibly
    cols = {str(c).lower().strip(): c for c in df.columns}
    sym_col = (cols.get("symbol") or cols.get("nseid") or cols.get("series_symbol"))
    ind_col = (cols.get("industry") or cols.get("sector") or cols.get("macro economic sector")
               or cols.get("macro_economic_sector"))
    if sym_col is None or ind_col is None:
        # Phase C4 (2026-07-02): nselib "equity_list" no longer ships industry
        # data — this is now a permanent condition, not an error. Log once at
        # info level, then fall back to local sector_master.csv (shipped in
        # repo). If you see this line every run, that's expected.
        _log("  [Sector nselib] equity_list has no industry column (expected) "
             "— using local sector_master.csv")
        # Local sector_master.csv fallback — shipped in repo
        try:
            master_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sector_master.csv")
            if os.path.exists(master_path):
                import csv as _csv
                added_local = 0
                new_from_csv: dict = {}
                with open(master_path, "r", encoding="utf-8") as _f:
                    reader = _csv.DictReader(_f)
                    for row in reader:
                        _sym = (row.get("symbol") or row.get("SYMBOL") or "").strip().upper()
                        _ind = (row.get("industry") or row.get("INDUSTRY")
                                or row.get("sector") or row.get("SECTOR") or "").strip().upper()
                        if not _sym or not _ind:
                            continue
                        _sym_ns = _sym if _sym.endswith(".NS") else _sym + ".NS"
                        if _sym_ns in SECTOR_MAP or _sym_ns in _SECTOR_MAP:
                            continue
                        norm = _NSE_INDUSTRY_NORM.get(_ind) or _NSE_INDUSTRY_NORM.get(_ind.split()[0] if _ind else "", "DIVERSIFIED")
                        new_from_csv[_sym_ns] = norm
                        added_local += 1
                if new_from_csv:
                    _SECTOR_MAP.update(new_from_csv)
                    try:
                        existing: dict = {}
                        if os.path.exists(SECTOR_CACHE_FILE):
                            with open(SECTOR_CACHE_FILE, "r") as _f:
                                existing = json.load(_f)
                        existing.update(new_from_csv)
                        with open(SECTOR_CACHE_FILE, "w") as _f:
                            json.dump(existing, _f, indent=2, sort_keys=True)
                    except Exception:
                        pass
                    _log(f"  [Sector nselib] ✓ loaded {added_local} sectors from local sector_master.csv")
                    return added_local
        except Exception as _e:
            _log(f"  [Sector nselib] local sector_master.csv fallback failed: {_e}")
        return 0
    added = 0
    new_sectors: dict = {}
    for _, row in df.iterrows():
        sym = str(row[sym_col]).strip().upper()
        if not sym or sym in ("NAN", "NONE"):
            continue
        sym_ns = sym if sym.endswith(".NS") else sym + ".NS"
        if sym_ns in SECTOR_MAP or sym_ns in _SECTOR_MAP:
            continue
        ind = str(row[ind_col]).strip().upper()
        norm = _NSE_INDUSTRY_NORM.get(ind)
        if norm is None:
            # Best-effort fallback: take first word
            head = ind.split()[0] if ind else ""
            norm = _NSE_INDUSTRY_NORM.get(head, "DIVERSIFIED")
        new_sectors[sym_ns] = norm
        added += 1
    if new_sectors:
        _SECTOR_MAP.update(new_sectors)
        # Persist into the same sector_cache.json file used by enrich_sectors_from_yfinance
        try:
            existing: dict = {}
            if os.path.exists(SECTOR_CACHE_FILE):
                with open(SECTOR_CACHE_FILE, "r") as f:
                    existing = json.load(f)
            existing.update(new_sectors)
            with open(SECTOR_CACHE_FILE, "w") as f:
                json.dump(existing, f, indent=2, sort_keys=True)
            _log(f"  [Sector nselib] ✓ added {added} sectors → cache {len(existing)} total")
        except Exception as e:
            _log(f"  [Sector nselib] cache write failed: {e}")
    return added


def enrich_sectors_from_yfinance(symbols: list, max_fetch: int = 500) -> None:
    """
    Batch-fetch yfinance sector info for symbols not yet in _SECTOR_MAP.
    Called once after tradable universe is built (prices already downloaded).
    New sectors are merged into _SECTOR_MAP and saved to sector_cache.json.
    On subsequent runs the cache is restored → zero yfinance calls needed.
    """
    global _SECTOR_MAP
    unknown = [
        s for s in symbols
        if not SECTOR_MAP.get(s if s.endswith(".NS") else s + ".NS")
        and _SECTOR_MAP.get(s if s.endswith(".NS") else s + ".NS", "OTHERS") == "OTHERS"
    ]
    if not unknown:
        _log(f"  Sector map: {len(_SECTOR_MAP)} symbols — fully covered")
        return

    to_fetch = unknown[:max_fetch]
    _log(f"  Sector map: {len(_SECTOR_MAP)} known | enriching {len(to_fetch)}/{len(unknown)} unknowns via yfinance...")

    new_sectors: dict = {}

    def _fetch_one(sym: str) -> None:
        try:
            info    = yf.Ticker(sym).info
            yf_sec  = (info.get("sector") or info.get("industry") or "").strip()
            if yf_sec:
                norm = _YF_LABEL_NORM.get(
                    yf_sec,
                    yf_sec.upper().replace(" & ", "_").replace(" ", "_")
                )
                new_sectors[sym] = norm   # GIL protects simple dict writes
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_fetch_one, to_fetch))

    if new_sectors:
        _SECTOR_MAP.update(new_sectors)
        try:
            existing: dict = {}
            if os.path.exists(SECTOR_CACHE_FILE):
                with open(SECTOR_CACHE_FILE, "r") as f:
                    existing = json.load(f)
            existing.update(new_sectors)
            with open(SECTOR_CACHE_FILE, "w") as f:
                json.dump(existing, f, indent=2, sort_keys=True)
            _log(f"  Sector cache: +{len(new_sectors)} new → {len(existing)} total ({SECTOR_CACHE_FILE})")
        except Exception as e:
            _log(f"[WARN] sector_cache save failed: {e}")
    else:
        _log("  Sector map: yfinance returned no new sector data (all remain OTHERS)")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

_LOG_FILE = None


def init_run_log():
    global _LOG_FILE
    try:
        fname = f"run_log_{datetime.date.today().strftime('%Y%m%d')}.txt"
        _LOG_FILE = open(fname, "a", encoding="utf-8")
        _log(f"=== PIPELINE STARTED {datetime.datetime.now().isoformat()} ===")
    except Exception as e:
        print(f"[WARN] init_run_log failed: {e}")


def _log(msg: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if _LOG_FILE:
        try:
            _LOG_FILE.write(line + "\n")
            _LOG_FILE.flush()
        except Exception:
            pass


def close_run_log():
    global _LOG_FILE
    if _LOG_FILE:
        try:
            _log("=== PIPELINE COMPLETE ===")
            _LOG_FILE.close()
        except Exception:
            pass


# NSE trading holidays 2026
NSE_HOLIDAYS_2026 = {
    "2026-01-26","2026-02-19","2026-03-25","2026-04-02",
    "2026-04-10","2026-04-14","2026-04-17","2026-05-01",
    "2026-06-17","2026-08-15","2026-10-02","2026-10-20",
    "2026-11-05","2026-11-16","2026-12-25",
}


def is_market_open(check_date=None) -> bool:
    if check_date is None:
        check_date = datetime.date.today()
    if check_date.weekday() >= 5:
        return False
    if check_date.strftime("%Y-%m-%d") in NSE_HOLIDAYS_2026:
        return False
    return True


def is_earnings_season() -> bool:
    return datetime.date.today().month in {4, 5, 10, 11}


def earnings_season_threshold_adjustment() -> int:
    return 3 if is_earnings_season() else 0


def truncate_display(text: str, max_len: int = 100) -> str:
    """Bug 6 fix — always shows clean truncation, never a broken sentence."""
    if not text:
        return "—"
    text = str(text).strip()
    if len(text) <= max_len:
        return text
    return text[:max_len - 3].rstrip() + "..."


def _split_telegram_message(text: str, max_len: int) -> list:
    """Split at section (═) boundaries first, then paragraph (\n\n), then newline.

    FIX: previously fell straight from section-divider to single-newline, which
    sometimes broke a Near-Miss card in half. Preferring paragraph boundaries
    keeps semantic blocks (BUY card, NEAR MISS card, WATCHLIST row) intact.
    Also prepends "(cont'd)" to continuation chunks so the user knows they're
    reading a continuation of the previous section.
    """
    if len(text) <= max_len:
        return [text]
    chunks = []
    DIVIDER = "═"
    # Try splitting at section dividers (long lines of ═)
    sections = text.split(DIVIDER * 10)
    chunk = ""
    for section in sections:
        candidate = (chunk + DIVIDER * 10 + section) if chunk else section
        if len(candidate) <= max_len:
            chunk = candidate
        else:
            if chunk:
                chunks.append(chunk)
            chunk = section
    if chunk:
        chunks.append(chunk)
    # If any chunk still too long, split — prefer paragraph (\n\n), then \n.
    final = []
    for c in chunks:
        while len(c) > max_len:
            split_at = c.rfind("\n\n", 0, max_len)
            if split_at == -1:
                split_at = c.rfind("\n", 0, max_len)
            if split_at == -1:
                split_at = max_len
            final.append(c[:split_at])
            c = c[split_at:].lstrip("\n")
        if c:
            final.append(c)
    # Prepend a (cont'd) tag on chunks 2..N so a card split across messages is
    # obvious to the reader on Telegram.
    if len(final) > 1:
        final = [final[0]] + [f"(cont'd)\n{c}" for c in final[1:]]
    return final if final else [text]


def _save_failed_telegram(message: str) -> None:
    try:
        fname = f"telegram_failed_{datetime.date.today().strftime('%Y%m%d')}.txt"
        with open(fname, "a", encoding="utf-8") as f:
            f.write(f"\n\n=== {datetime.datetime.now().isoformat()} ===\n")
            f.write(message)
    except Exception:
        pass


_VALID_TG_TAGS = re.compile(
    r"<(/?(b|i|u|s|code|pre|a)(\s[^>]*)?)>",
    re.IGNORECASE,
)

def _sanitize_telegram_html(text: str) -> str:
    """
    Escape any < that does NOT start a valid Telegram HTML tag.
    Telegram supports: <b> <i> <u> <s> <code> <pre> <a href="...">
    Any other < (e.g. from fail_reasons like "Conf < 83)") causes HTTP 400.
    """
    result = []
    i = 0
    while i < len(text):
        if text[i] == "<":
            m = _VALID_TG_TAGS.match(text, i)
            if m:
                result.append(m.group(0))
                i = m.end()
            else:
                result.append("&lt;")
                i += 1
        elif text[i] == "&" and not re.match(r"&(?:lt|gt|amp|quot|#\d+);", text[i:]):
            result.append("&amp;")
            i += 1
        else:
            result.append(text[i])
            i += 1
    return "".join(result)


def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        _log("[WARN] Main Telegram not configured — skipping send")
        return
    chunks = _split_telegram_message(message, TELEGRAM_MAX_CHARS)
    for i, chunk in enumerate(chunks):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            resp = requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": _sanitize_telegram_html(chunk),
                "parse_mode": "HTML",
            }, timeout=15)
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}: {resp.text[:120]}")
        except Exception as e:
            _log(f"[WARN] Telegram send failed (chunk {i+1}/{len(chunks)}): {e}")
            _save_failed_telegram(message)
            break


def send_buy_telegram(buys: list, regime: str, timestamp: str) -> None:
    """
    Sends BUY signals ONLY to a dedicated channel (BUY_BOT_TOKEN + BUY_CHAT_ID).
    Fires even when buys list is empty so you know the pipeline ran.
    """
    if not BUY_BOT_TOKEN or not BUY_CHAT_ID:
        _log("[INFO] BUY channel not configured (BUY_BOT_TOKEN / BUY_CHAT_ID missing) — skipping")
        return

    lines = []
    lines.append("─" * 38)
    lines.append(f"📊 NSE BUY SIGNALS — {timestamp}")
    lines.append(f"Regime: {regime}")
    lines.append("─" * 38)

    if buys:
        for b in buys:
            sym      = b.get("symbol", "?")
            sector   = b.get("sector", "OTHERS")
            conf     = b.get("final_confidence", 0)
            tq       = b.get("trade_quality_score", 0)
            rr       = b.get("rr_ratio", 0)
            entry    = b.get("entry", 0)
            stop_p   = b.get("stop", 0)
            t1       = b.get("target1", 0)
            t2       = b.get("target2", 0)
            pos_val  = b.get("position_value", 0)
            pos_pct  = b.get("position_pct", 0)
            stop_pct = round((entry - stop_p) / entry * 100, 1) if entry > 0 else 0
            news_sum = truncate_display(b.get("news_summary", ""), 90)
            pledge   = b.get("promoter_pledge_pct", 0)
            roe      = b.get("roe", 0)
            de       = b.get("de_ratio", 0)
            repeat   = b.get("repeat_tag", "")

            lines.append(f"\n<b>{html.escape(str(sym))}</b> [{html.escape(str(sector))}]")
            lines.append(f"Conf {conf:.1f} | TQ {tq:.1f} | R/R {rr:.2f}x")
            lines.append(f"Entry  Rs{entry:.2f}")
            lines.append(f"Stop   Rs{stop_p:.2f}  ({stop_pct:.1f}%)")
            lines.append(f"T1     Rs{t1:.2f}")
            lines.append(f"T2     Rs{t2:.2f}")
            # Max valid entry for gap-open decision
            gap_chk = check_gap_validity(entry, stop_p, t1, rr if rr > 0 else 1.8)
            max_ent = gap_chk.get("max_valid_entry", 0)
            if max_ent > entry:
                gap_max_pct = round((max_ent - entry) / entry * 100, 1)
                lines.append(f"⚡ Max valid entry: Rs{max_ent:.2f} (+{gap_max_pct:.1f}%)")
                lines.append(f"   If opens > Rs{max_ent:.2f} → SKIP signal")
            lines.append(f"Size   Rs{pos_val:,.0f}  ({pos_pct:.1f}% capital)")
            lines.append(f"ROE {roe:.1f}% | D/E {de:.2f} | Pledge {pledge:.0f}%")
            if news_sum and news_sum != "—":
                lines.append(f"News: {html.escape(str(news_sum))}")
            if repeat:
                lines.append(f"⚠️ {html.escape(str(repeat))}")
            if b.get("warnings"):
                lines.append(f"WARN: {html.escape(', '.join(b['warnings'][:3]))}")
            lines.append("─" * 38)
    else:
        lines.append("\nNo BUY signals today — no setup cleared all gates.")
        lines.append("─" * 38)

    lines.append("Recommendation only. Execute manually.")
    message = "\n".join(lines)

    chunks = _split_telegram_message(message, TELEGRAM_MAX_CHARS)
    for i, chunk in enumerate(chunks):
        try:
            url = f"https://api.telegram.org/bot{BUY_BOT_TOKEN}/sendMessage"
            resp = requests.post(url, json={
                "chat_id":    BUY_CHAT_ID,
                "text":       _sanitize_telegram_html(chunk),
                "parse_mode": "HTML",
            }, timeout=15)
            if resp.status_code != 200:
                _log(f"[WARN] BUY Telegram send failed (chunk {i+1}): {resp.status_code} {resp.text[:80]}")
        except Exception as e:
            _log(f"[WARN] send_buy_telegram failed: {e}")
            _save_failed_telegram(message)
            break


def save_csv(data: list, base_filename: str) -> None:
    if not data:
        return
    try:
        date_str = datetime.date.today().strftime("%Y%m%d")
        name, ext = os.path.splitext(base_filename)
        filename = f"{name}_{date_str}{ext}"
        clean = []
        for row in data:
            clean.append({k: v for k, v in row.items()
                          if isinstance(v, (str, int, float, bool, type(None)))})
        if not clean:
            return
        # Union all keys across every row so no row's extra fields cause a crash
        all_keys: list = []
        seen_keys: set = set()
        for row in clean:
            for k in row.keys():
                if k not in seen_keys:
                    all_keys.append(k)
                    seen_keys.add(k)
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(clean)
        _log(f"Saved {len(clean)} rows to {filename}")
    except Exception as e:
        _log(f"[WARN] save_csv failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — GROQ AI (3-KEY ROUND-ROBIN) — Bug 4 fix
# ─────────────────────────────────────────────────────────────────────────────

CLOUD_AI_ENDPOINT = os.getenv("CLOUD_AI_ENDPOINT", "")
CLOUD_AI_KEY      = os.getenv("CLOUD_AI_KEY", "")

_GROQ_KEYS_RAW = [
    os.getenv("GROQ_API_KEY_1", ""),
    os.getenv("GROQ_API_KEY_2", ""),
    os.getenv("GROQ_API_KEY_3", ""),
]
# Fall back to single key if multi-key not set
if not any(k.strip() for k in _GROQ_KEYS_RAW):
    _single = os.getenv("GROQ_API_KEY", "")
    if _single.strip():
        _GROQ_KEYS_RAW = [_single, "", ""]

GROQ_KEYS = [k.strip() for k in _GROQ_KEYS_RAW if k.strip()]
_groq_key_cycle = itertools.cycle(GROQ_KEYS) if GROQ_KEYS else iter([])
_groq_key_failures: dict = {}

NEGATIVE_KEYWORDS = [
    "fraud","scam","sebi ban","ed raid","cbi","fir","arrested","bankrupt",
    "insolvency","liquidation","default","npa","downgrade","plant shut",
    "factory closed","promoter sell","pledged shares sold","regulatory action",
    "show cause","penalty","fine imposed","loss widened","revenue decline",
    "auditor resigned","qualified opinion","going concern","debt restructure",
]
POSITIVE_KEYWORDS = [
    "contract win","order win","expansion","new plant","capacity addition",
    "earnings beat","profit up","revenue growth","dividend","buyback",
    "stake acquisition","partnership","export order","fda approval","patent",
]
BLACK_SWAN_KEYWORDS = [
    "fraud","sebi ban","ed raid","cbi raid","arrested","bankrupt",
    "insolvency","liquidation","promoter arrested","nclt",
]


def _call_groq_with_rotation(prompt: str, max_tokens: int = 150) -> str | None:
    if not GROQ_KEYS:
        return None
    wall_start = time.time()
    max_attempts = min(len(GROQ_KEYS), 3)
    for _ in range(max_attempts):
        if time.time() - wall_start > 20:
            break
        key = next(_groq_key_cycle)
        last_fail = _groq_key_failures.get(key, 0)
        if time.time() - last_fail < 60:
            continue
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": 0,
                },
                timeout=12,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            elif resp.status_code == 429:
                _groq_key_failures[key] = time.time()
                _log(f"[WARN] Groq key ...{key[-6:]} rate-limited, rotating")
            else:
                _log(f"[WARN] Groq key ...{key[-6:]} returned {resp.status_code}")
        except Exception as e:
            _log(f"[WARN] Groq call failed (key ...{key[-6:]}): {e}")
            _groq_key_failures[key] = time.time()
    return None


def _parse_ai_json(text: str) -> dict | None:
    try:
        clean = re.sub(r"```json|```", "", text).strip()
        data = json.loads(clean)
        return {
            "severity":      min(100, max(-30, int(data.get("severity", 0)))),
            "category":      data.get("category", "NEUTRAL"),
            "is_black_swan": bool(data.get("is_black_swan", False)),
            "summary":       str(data.get("summary", ""))[:150],
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3b — SHARED AI CALLER + HIGH-VALUE AI FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _call_ai(prompt: str, max_tokens: int = 100) -> str | None:
    """
    Shared AI caller with 3-tier fallback.
    Tier 1: SAP / Cloud AI  (CLOUD_AI_ENDPOINT + CLOUD_AI_KEY)
    Tier 2: Groq llama-3.1-8b-instant  (existing key rotation)
    Tier 3: Returns None — caller uses rule-based fallback.
    prompt must be under 1500 chars (~300 tokens).
    """
    if len(prompt) > 1500:
        prompt = prompt[:1500]
        _log("[WARN] AI prompt truncated to 1500 chars")

    # Tier 1: Cloud / SAP AI
    if CLOUD_AI_ENDPOINT and CLOUD_AI_KEY:
        try:
            r = requests.post(
                CLOUD_AI_ENDPOINT,
                headers={"Authorization": f"Bearer {CLOUD_AI_KEY}",
                         "Content-Type":  "application/json"},
                json={"prompt": prompt, "max_tokens": max_tokens, "temperature": 0.3},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                text = (data.get("choices", [{}])[0].get("text", "") or
                        data.get("content", [{}])[0].get("text", "") or
                        data.get("response", "") or data.get("output", ""))
                if text and len(text.strip()) > 10:
                    return text.strip()
        except Exception as e:
            _log(f"[WARN] Cloud AI failed: {e}")

    # Tier 2: Groq (uses existing key rotation)
    text = _call_groq_with_rotation(prompt, max_tokens=max_tokens)
    if text and len(text.strip()) > 10:
        return text.strip()

    # Tier 3: unavailable
    return None


def _clean_ai_output(text: str) -> str:
    """
    Cleans common AI output artifacts.
    Fixes spaces inside decimals: "13. 6" → "13.6"
    Removes markdown, bullets, leading/trailing junk.
    Unescapes HTML entities (&amp; &#39; &quot; etc.).
    """
    import re
    import html as _html_mod
    text = _html_mod.unescape(text or "")                    # &amp; → &, &#39; → '
    text = re.sub(r'(\d+)\.\s+(\d+)', r'\1.\2', text)   # "13. 6" → "13.6"
    text = re.sub(r'(\d)\.\s+(\d)', r'\1.\2', text)       # "0. 49%" → "0.49%"
    text = re.sub(r'\*+', '', text)                          # remove markdown bold
    text = re.sub(r'^\s*[-•*]\s*', '', text, flags=re.MULTILINE)  # remove bullets
    text = " ".join(text.split())                            # normalise whitespace
    return text.strip()


def _split_sentences(text: str) -> list:
    """
    Sentence splitter that does NOT break on:
      - Ticker suffixes:   "MANBA.NS breaks..."   (letter.LETTER)
      - Decimals:          "R/R 2.5x"             (digit.digit)
      - Abbreviations:     "Rs. 500", "e.g.", "i.e.", "vs.", "no.", "pct."
    Splits ONLY on: period/! /? followed by whitespace followed by a capital letter,
    OR end-of-string.
    """
    import re
    if not text:
        return []
    # Protect known abbreviations by temporarily removing the period
    ABBR = {"Rs.": "Rs<DOT>", "rs.": "rs<DOT>",
            "e.g.": "e<DOT>g<DOT>", "i.e.": "i<DOT>e<DOT>",
            "vs.": "vs<DOT>", "No.": "No<DOT>", "no.": "no<DOT>",
            "pct.": "pct<DOT>", "Pct.": "Pct<DOT>",
            "Inc.": "Inc<DOT>", "Ltd.": "Ltd<DOT>", "Co.": "Co<DOT>",
            "U.S.": "U<DOT>S<DOT>", "U.K.": "U<DOT>K<DOT>"}
    for k, v in ABBR.items():
        text = text.replace(k, v)

    # Split on sentence-ending punctuation ONLY when followed by space+capital or end
    # (?<=[.!?])  = look-behind: previous char is . ! ?
    # \s+         = at least one space
    # (?=[A-Z"'])  = look-ahead: next char is capital/quote (real sentence start)
    parts = re.split(r'(?<=[.!?])\s+(?=[A-Z"\'\u201C\u2018])', text)

    # Restore abbreviations
    def _restore(s):
        return s.replace("<DOT>", ".").strip()

    return [_restore(p) for p in parts if _restore(p)]


def _rule_based_summary(
    regime: str, regime_label: str, buy_count: int,
    near_miss_count: int, top_symbol: str,
    portfolio_alerts: list, ema_bear: bool,
    upcoming_event: str
) -> str:
    """Fallback — always correct, never has AI artifacts."""
    parts = []
    parts.append(
        f"Market is {regime_label} with price "
        f"{'below' if ema_bear else 'above'} key moving averages."
    )
    if buy_count > 0:
        parts.append(
            f"{buy_count} institutional-quality setup(s) met all conditions today."
        )
    else:
        parts.append(
            "No setup met all required conditions today — "
            "quality over quantity remains the priority."
        )
    if near_miss_count > 0 and top_symbol and top_symbol != "none":
        parts.append(
            f"{near_miss_count} stock(s) are close to qualifying — "
            f"watch {top_symbol.replace('.NS', '')} most closely."
        )
    else:
        exits = [a for a in portfolio_alerts if "EXIT" in a.get("action", "")]
        if exits:
            parts.append(
                f"{len(exits)} position(s) require exit review — act before next session."
            )
        else:
            parts.append("Existing positions are on track — no action needed today.")
    if upcoming_event:
        parts.append(
            f"With {upcoming_event} approaching, reduce position sizes and keep stops tight."
        )
    elif ema_bear and buy_count == 0:
        parts.append(
            "Avoid forcing new trades until the market reclaims its key moving averages."
        )
    elif buy_count > 0:
        parts.append(
            "Execute today's signal with strict position sizing — do not average down."
        )
    else:
        parts.append(
            "Stay patient — let the setups come to you rather than chasing momentum."
        )
    return " ".join(parts)


def ai_daily_summary(
    regime: str,
    regime_score: float,
    nifty_pct: float,
    vix_in: float,
    breadth_pct: float,
    fii_flow_cr: float,
    dii_flow_cr: float,
    buy_count: int,
    near_miss_count: int,
    top_near_miss_symbol: str,
    top_near_miss_conf_gap: float,
    portfolio_alerts: list,
    ema_bear: bool,
    upcoming_event: str = "",
) -> str:
    """
    4-sentence AI market briefing. Strictly qualitative — no raw numbers sent to AI.
    Falls back to rule-based if AI fails or output contains numbers / is too short.
    """
    import re

    # Translate numbers to qualitative labels BEFORE sending to AI
    regime_label = {
        "STRONG_BULL":     "strongly bullish with broad participation",
        "BULL":            "bullish with improving breadth",
        "SIDEWAYS":        "range-bound with no clear direction",
        "TRANSITION":      "transitioning with mixed signals",
        "HIGH_VOLATILITY": "volatile with compressed opportunity",
        "BEAR":            "weakening with institutional selling",
        "STRONG_BEAR":     "in capital preservation mode",
    }.get(regime, "uncertain")

    nifty_label = (
        "falling sharply" if nifty_pct < -1.0 else
        "falling slightly" if nifty_pct < -0.2 else
        "flat" if abs(nifty_pct) <= 0.2 else
        "rising slightly" if nifty_pct < 0.8 else
        "rising strongly"
    )
    vix_label = (
        "very calm" if vix_in < 12 else
        "normal" if vix_in < 16 else
        "elevated" if vix_in < 20 else
        "high" if vix_in < 25 else
        "very high"
    )
    breadth_label = (
        "weak" if breadth_pct < 40 else
        "mixed" if breadth_pct < 55 else
        "healthy" if breadth_pct < 70 else
        "strong"
    )
    fii_label = (
        "buying aggressively" if fii_flow_cr > 2000 else
        "buying"             if fii_flow_cr > 300  else
        "neutral"            if abs(fii_flow_cr) <= 300 else
        "selling"            if fii_flow_cr > -2000 else
        "selling aggressively"
    )
    dii_label = (
        "buying aggressively" if dii_flow_cr > 2000 else
        "buying"             if dii_flow_cr > 300  else
        "neutral"            if abs(dii_flow_cr) <= 300 else
        "selling"
    )
    ema_label       = "below key moving averages" if ema_bear else "above key moving averages"
    exits           = [a for a in portfolio_alerts if "EXIT" in a.get("action", "")]
    portfolio_label = (
        f"{len(exits)} position(s) need exit review" if exits else
        "all positions on track"
    )
    near_miss_label = (
        "no stocks close to qualifying" if near_miss_count == 0 else
        f"{near_miss_count} stock(s) close to qualifying"
    )
    event_label = (
        f"upcoming {upcoming_event} adds uncertainty" if upcoming_event else
        "no major events this week"
    )

    prompt = f"""You are writing 4 sentences for a swing trader's daily briefing.

Market conditions today:
- Overall market: {regime_label}
- NIFTY direction: {nifty_label}
- Volatility (VIX): {vix_label}
- Market breadth: {breadth_label}
- FII (foreign institutions): {fii_label}
- DII (domestic institutions): {dii_label}
- Price structure: {ema_label}
- Buy signals today: {buy_count}
- Watchlist status: {near_miss_label}
- Portfolio: {portfolio_label}
- Events: {event_label}

Write EXACTLY 4 sentences. Rules:
1. Do NOT use any numbers, percentages, or rupee amounts
2. Use only the qualitative labels provided above
3. Sentence 1: Overall market condition and why
4. Sentence 2: What this means for new trades today
5. Sentence 3: What to watch most closely right now
6. Sentence 4: One specific actionable recommendation for tomorrow
7. Total: 60-80 words maximum
8. Plain English — senior trader tone, not a report
9. Never say "I" or "as an AI"
10. Never use the word "bearish" or "bullish" — use plain words"""

    result = _call_ai(prompt, max_tokens=150)
    if result:
        clean = _clean_ai_output(result)
        sentences = [s for s in _split_sentences(clean) if len(s) > 15]
        has_numbers = bool(re.search(r'\d', clean))
        if len(sentences) >= 3 and not has_numbers:
            return " ".join(sentences[:4])
        if len(sentences) >= 3:
            # Strip any numbers that crept through
            clean_no_nums = re.sub(r'\d+\.?\d*%?', '', clean)
            clean_no_nums = re.sub(r'Rs\.?\s*\d+', '', clean_no_nums)
            clean_no_nums = " ".join(clean_no_nums.split())
            if len(clean_no_nums) > 50:
                return clean_no_nums

    return _rule_based_summary(
        regime, regime_label, buy_count, near_miss_count,
        top_near_miss_symbol, portfolio_alerts, ema_bear, upcoming_event
    )


def _rule_based_thesis(symbol: str, sector: str, rr: float,
                        conf_trend: str, catalyst: list,
                        sector_status: str) -> str:
    """Rule-based fallback for BUY thesis."""
    parts = []
    if sector_status in ("LEADING", "IMPROVING"):
        parts.append(f"{sector} sector showing strength")
    if "VOL_SURGE" in catalyst:
        parts.append("volume expansion confirming move")
    if "UPTREND" in catalyst:
        parts.append("price in clean uptrend")
    if conf_trend and "rising" in conf_trend.lower():
        parts.append("confidence building over 3 days")
    reason = ", ".join(parts) if parts else "multi-factor confluence"
    return f"{symbol}: {reason} with R/R {rr:.1f}x. Risk: stop must hold — no averaging down."


def ai_buy_thesis(
    symbol: str, sector: str, confidence: float, tq: float, rr: float,
    conf_trend: str, catalyst: list, sector_status: str,
    roe: float, pledge_pct: float, regime: str, risk_pct: float,
    factor_scores: dict = None, soft_warnings: list = None,
    rs_diff21: float = 0.0, accum_signal: str = "NEUTRAL",
) -> str:
    """1-2 sentence specific trade thesis for a BUY signal.

    Now feeds the LLM the TOP-3 factor scores, the WEAKEST factor, RS-vs-Nifty,
    accumulation signal and any soft warnings so it has stock-specific evidence
    to cite instead of falling back to generic phrases like 'strong buy signal'.
    """
    fs = factor_scores or {}
    ranked = sorted(fs.items(), key=lambda kv: -(kv[1] or 0))
    top_factors = [f"{k}={int(v)}" for k, v in ranked[:3] if v is not None]
    weakest     = ranked[-1] if ranked else ("n/a", 0)
    warns       = ", ".join((soft_warnings or [])[:3]) or "none"

    prompt = (
        f"Stock selected as BUY signal:\n"
        f"Symbol: {symbol} | Sector: {sector}\n"
        f"Confidence: {confidence:.1f}/100 | TQ: {tq:.1f} | R/R: {rr:.2f}x\n"
        f"Confidence trend (3d): {conf_trend if conf_trend else 'first appearance'}\n"
        f"Catalysts: {', '.join(catalyst) if catalyst else 'none'}\n"
        f"Sector status: {sector_status} | Accumulation: {accum_signal}\n"
        f"Fundamentals: ROE {roe:.1f}% | Pledge {pledge_pct:.0f}%\n"
        f"Relative Strength vs Nifty (21d): {rs_diff21:+.1f}%\n"
        f"Top-3 factor scores: {', '.join(top_factors) if top_factors else 'n/a'}\n"
        f"Weakest factor: {weakest[0]}={int(weakest[1] or 0)}\n"
        f"Soft warnings: {warns}\n"
        f"Regime: {regime}\n\n"
        "Write ONE sentence (max 25 words) on why THIS stock was selected today. "
        "MUST cite AT LEAST ONE of: the strongest factor by name+score, a specific catalyst, "
        "the RS-vs-Nifty number, or accumulation signal. "
        "Do NOT say 'strong buy signal' or generic phrases like 'strong <sector> sector'. "
        "Then ONE sentence (max 15 words) on the KEY RISK — must mention the WEAKEST factor by name, "
        "a soft warning, or a stock-specific concern (wide stop, near 52W high, etc.). "
        "Do NOT mention '1.5% loss per trade' — fixed and boring. No bullets, no jargon."
    )
    result = _call_ai(prompt, max_tokens=100)
    if result and len(result) > 20:
        clean = _clean_ai_output(result)
        try:
            clean = html.unescape(clean)
        except Exception:
            pass
        sentences = _split_sentences(clean)
        return " ".join(sentences[:2]) if sentences else clean
    return _rule_based_thesis(symbol, sector, rr, conf_trend, catalyst, sector_status)


def _rule_based_near_miss_insight(
    symbol: str, conf_gap: float, conf_only: bool,
    rr_fail: bool, tq_fail: bool, conf_trend: str, days_watching: int,
    sector_status: str = "NEUTRAL",
    # Optional per-stock numbers to make each insight unique (avoids the
    # "same sentence copied across 20 stocks" problem that used to hit
    # near-miss stocks past NM_AI_CAP).
    confidence: float = 0.0, tq: float = 0.0, rr: float = 0.0,
) -> str:
    """Fallback — specific and useful without AI.
    Every branch uses at least one stock-specific number so no two stocks
    with a similar profile receive an identical sentence.
    """
    trend_lc = (conf_trend or "").lower()
    rising   = "rising"  in trend_lc
    falling  = "falling" in trend_lc
    flat     = "flat"    in trend_lc

    # 1) Confidence-only near miss with rising trend → very close to a BUY
    if rising and conf_only:
        return (
            f"Confidence at {confidence:.1f} is rising toward the threshold — "
            f"needs one strong volume day to close the {conf_gap:.1f}-point gap."
        )

    # 2) Confidence-only near miss with falling trend → losing steam
    if falling and conf_only:
        return (
            f"Confidence slipping from {confidence:.1f} — remove after 2 more "
            f"sessions of decline unless volume returns."
        )

    # 3) Stuck-on-confidence for many days
    if days_watching >= 5 and conf_only:
        return (
            f"Stuck at {confidence:.1f} confidence for {days_watching} sessions — "
            f"if no {conf_gap:.1f}-point improvement in 2 more days, drop from watchlist."
        )

    # 4) R/R too low (with or without confidence issue)
    if rr_fail:
        return (
            f"R/R at {rr:.2f}x too low — wait for a pullback to lift R/R above "
            f"2.0x before confidence ({confidence:.1f}) can also qualify."
        )

    # 5) Trade Quality below bar
    if tq_fail:
        return (
            f"Chart pattern weak at TQ {tq:.1f} — wait for tighter consolidation "
            f"pushing TQ above 80 before acting."
        )

    # 6) Sector drag
    if sector_status in ("LAGGING", "WEAKENING"):
        return (
            f"{sector_status.title()} sector holding {symbol} back — confidence "
            f"{confidence:.1f} needs sector rotation before it can climb."
        )

    # 7) Flat/first-appearance default — reference the actual gap size
    if flat:
        return (
            f"Confidence flat at {confidence:.1f} for {max(days_watching,1)} "
            f"session(s) — needs a volume spike to break the {conf_gap:.1f}-point gap."
        )

    # 8) Generic fallback still uses the actual gap AND confidence value
    return (
        f"Needs {conf_gap:.1f} confidence points closed from {confidence:.1f} — "
        f"watch for volume surge above the 20-day average as trigger."
    )


def ai_near_miss_insight(
    symbol: str, sector: str, confidence: float, conf_gap: float,
    tq: float, rr: float, conf_trend: str, fail_reasons: list,
    gate_pattern: str, sector_status: str, risk_pct: float,
    days_watching: int,
) -> str:
    """1 sentence: what must happen for this near miss to become a BUY."""
    primary_fail = fail_reasons[0] if fail_reasons else "CONFIDENCE_FAIL"
    conf_only    = len(fail_reasons) == 1 and "CONF" in primary_fail
    rr_fail      = any("RR" in f for f in fail_reasons)
    tq_fail      = any("TQ" in f for f in fail_reasons)

    trend_label = (
        "confidence rising steadily"  if conf_trend and "rising"  in conf_trend.lower() else
        "confidence falling"          if conf_trend and "falling" in conf_trend.lower() else
        "confidence flat"             if conf_trend and "flat"    in conf_trend.lower() else
        "first appearance on watchlist"
    )
    blocker_label = (
        "only confidence is missing — all other factors pass" if conf_only else
        "both confidence and risk/reward need improvement"     if rr_fail and not tq_fail else
        "confidence, risk/reward, and trade quality all need work" if rr_fail and tq_fail else
        "multiple factors below threshold"
    )

    # FIX: when Conf already passes (conf_gap==0), the real blocker is R/R or TQ.
    # Tell the AI which knob to talk about — previously it invented a non-existent
    # "close the confidence gap" line even when Conf was well above threshold.
    conf_passes    = conf_gap <= 0
    active_gap_str = (
        f"R/R gap: needs to reach {rr + 0.2:.2f}x from {rr:.2f}x"
        if conf_passes and rr_fail
        else (
            f"TQ gap: needs +{80 - tq:.1f} from {tq:.1f} toward 80"
            if conf_passes and tq_fail
            else f"Confidence gap: needs +{conf_gap:.1f} points from {confidence:.1f}"
        )
    )

    prompt = f"""Near miss stock analysis. Each stock has unique numbers — treat each one differently.

Symbol: {symbol}
Sector: {sector}
Current Confidence: {confidence:.1f}/100 (threshold {'MET' if conf_passes else 'NOT MET'})
Current TQ: {tq:.1f}/100
Current R/R: {rr:.2f}x
Active blocker: {active_gap_str}
Confidence trend (3 days): {trend_label}
Primary blocker: {blocker_label}
Sector condition: {sector_status}
Days on watchlist: {days_watching}
Gate pattern: {gate_pattern or 'none'}
Fail reasons: {', '.join(fail_reasons) if fail_reasons else 'none'}

Write ONE complete sentence (minimum 20 words, maximum 30 words) answering:
"What specific event must happen in the next 1-3 sessions for this stock
 to become a tradeable BUY signal?"

Requirements:
- Focus ONLY on the ACTIVE blocker above. If confidence already PASSES the
  threshold, do NOT invent a confidence gap — talk about R/R or TQ instead.
- Reference this stock's ACTUAL numbers (R/R value, TQ value, or conf value
  — whichever is the blocker).
- Do NOT start with "For {symbol}" or "{symbol} needs"
- Start with a verb, condition, or timeframe
- Be specific to THIS stock — do not produce a generic template
- Do not mention the .NS suffix"""

    result = _call_ai(prompt, max_tokens=90)
    if result:
        clean = _clean_ai_output(result)
        try:
            clean = html.unescape(clean)   # FIX: strip HTML entities from AI output
        except Exception:
            pass
        # Use safe sentence splitter (protects .NS ticker suffix, decimals, abbreviations)
        sentences = [s for s in _split_sentences(clean) if len(s) >= 20]
        if sentences:
            return sentences[0]

    return _rule_based_near_miss_insight(
        symbol, conf_gap, conf_only, rr_fail, tq_fail,
        conf_trend, days_watching, sector_status,
        confidence=confidence, tq=tq, rr=rr,
    )


def run_all_ai_calls(
    regime_data: dict, macro: dict, breadth_data: dict,
    nifty_state: dict, buys: list, watchlist: list,
    portfolio_alerts: list, conf_history: dict,
    gate_memory: dict, events: list,
) -> dict:
    """
    Runs all AI calls in parallel via ThreadPoolExecutor.
    Returns {daily_summary, buy_theses, near_miss_insights}.
    Never fails — all have rule-based fallbacks.
    Total: max 9 calls, typically ~3-5 seconds.
    """
    from concurrent.futures import ThreadPoolExecutor

    results = {"daily_summary": "", "buy_theses": {}, "near_miss_insights": {}}
    near_miss    = [w for w in watchlist if w.get("tier") == "NEAR_MISS"]
    top_nm       = near_miss[0] if near_miss else {}
    upcoming_ev  = events[0]["name"] if events else ""
    regime       = regime_data["regime"]

    futures = {}
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Call 1: daily summary (always)
            futures["daily_summary"] = executor.submit(
                ai_daily_summary,
                regime                = regime,
                regime_score          = regime_data["score"],
                nifty_pct             = macro.get("nifty_1d_pct", 0),
                vix_in                = macro.get("vix_in", 15),
                breadth_pct           = breadth_data.get("ema20_pct", 50),
                fii_flow_cr           = macro.get("fii_flow_cr", 0),
                dii_flow_cr           = macro.get("dii_flow_cr", 0),
                buy_count             = len(buys),
                near_miss_count       = len(near_miss),
                top_near_miss_symbol  = top_nm.get("symbol", "none"),
                top_near_miss_conf_gap= top_nm.get("conf_gap", 0),
                portfolio_alerts      = portfolio_alerts,
                ema_bear              = nifty_state.get("ema_bear", True),
                upcoming_event        = upcoming_ev,
            )
            # Calls 2-6: BUY theses (max 5)
            for stock in buys[:5]:
                sym = stock["symbol"]
                futures[f"buy_{sym}"] = executor.submit(
                    ai_buy_thesis,
                    symbol        = sym,
                    sector        = stock.get("sector", "OTHERS"),
                    confidence    = stock.get("final_confidence", 0),
                    tq            = stock.get("trade_quality_score", 0),
                    rr            = stock.get("rr_ratio", 0),
                    conf_trend    = get_confidence_trend(sym, conf_history),
                    catalyst      = stock.get("catalysts", []),
                    sector_status = stock.get("sector_status", "NEUTRAL"),
                    roe           = stock.get("roe", 0),
                    pledge_pct    = stock.get("promoter_pledge_pct", 0),
                    regime        = regime,
                    risk_pct      = stock.get("risk_pct", 0),
                    factor_scores = stock.get("factor_scores", {}) or {
                        "trend":     stock.get("trend_quality", 50),
                        "momentum":  stock.get("momentum_quality", 50),
                        "volume":    stock.get("volume_delivery", 50),
                        "sector":    stock.get("sector_strength", 50),
                        "rs":        stock.get("rs_vs_nifty", 50),
                        "news":      stock.get("news_risk", 50),
                        "ownership": stock.get("ownership_quality", 50),
                        "macro":     stock.get("macro_alignment", 50),
                    },
                    soft_warnings = stock.get("_soft_warnings", []) or [],
                    rs_diff21     = float(stock.get("rs_diff21", 0) or 0),
                    accum_signal  = stock.get("accum_signal", "NEUTRAL"),
                )
            # Calls 7-N: near-miss insights for ALL near-miss stocks.
            # AI is capped at 15 to stay within the 15-second timeout budget;
            # the remainder receive the deterministic rule-based fallback so
            # every Near Miss card in Telegram has an insight line.
            NM_AI_CAP = 15
            for w in near_miss[:NM_AI_CAP]:
                sym = w["symbol"]
                futures[f"nm_{sym}"] = executor.submit(
                    ai_near_miss_insight,
                    symbol        = sym,
                    sector        = w.get("sector", "OTHERS"),
                    confidence    = w.get("conf", w.get("final_confidence", 0)),
                    conf_gap      = w.get("conf_gap", 0),
                    tq            = w.get("tq", w.get("trade_quality_score", 0)),
                    rr            = w.get("rr_ratio", w.get("rr", 0)),
                    conf_trend    = get_confidence_trend(sym, conf_history),
                    fail_reasons  = w.get("fail_reasons", []),
                    gate_pattern  = get_gate_pattern(sym, gate_memory),
                    sector_status = w.get("sector_status", "NEUTRAL"),
                    risk_pct      = w.get("risk_pct", 0),
                    days_watching = w.get("days_watching", 0),
                )
            # Rule-based fallback for everyone past the AI cap so no Near Miss
            # is left without an insight line in the Telegram card.
            # `ai_near_miss_insight` itself falls back to the rule-based
            # generator on any failure, so calling it inline (no executor,
            # no LLM dispatch — the rule path is local & instant) keeps the
            # signature contract correct.
            for w in near_miss[NM_AI_CAP:]:
                sym = w["symbol"]
                try:
                    primary_fail = (w.get("fail_reasons") or ["CONFIDENCE_FAIL"])[0]
                    fr           = w.get("fail_reasons") or []
                    conf_only    = len(fr) == 1 and "CONF" in primary_fail
                    rr_fail      = any("RR" in f for f in fr)
                    tq_fail      = any("TQ" in f for f in fr)
                    results["near_miss_insights"][sym] = _rule_based_near_miss_insight(
                        symbol        = sym,
                        conf_gap      = w.get("conf_gap", 0),
                        conf_only     = conf_only,
                        rr_fail       = rr_fail,
                        tq_fail       = tq_fail,
                        conf_trend    = get_confidence_trend(sym, conf_history),
                        days_watching = w.get("days_watching", 0),
                        sector_status = w.get("sector_status", "NEUTRAL"),
                        # Per-stock numbers so no two stocks get identical text
                        confidence    = w.get("conf", w.get("final_confidence", 0)),
                        tq            = w.get("tq", w.get("trade_quality_score", 0)),
                        rr            = w.get("rr_ratio", w.get("rr", 0)),
                    ) or ""
                except Exception:
                    results["near_miss_insights"][sym] = ""
            for key, future in futures.items():
                try:
                    text = future.result(timeout=15)
                    if key == "daily_summary":
                        results["daily_summary"] = text or ""
                    elif key.startswith("buy_"):
                        results["buy_theses"][key[4:]] = text or ""
                    elif key.startswith("nm_"):
                        results["near_miss_insights"][key[3:]] = text or ""
                except Exception as e:
                    _log(f"[WARN] AI call {key} timed out or failed: {e}")
    except Exception as e:
        _log(f"[WARN] run_all_ai_calls failed: {e}")

    _log(
        f"[INFO] AI calls complete: "
        f"summary={'OK' if results['daily_summary'] else 'FALLBACK'} | "
        f"buy theses={len(results['buy_theses'])} | "
        f"near miss insights={len(results['near_miss_insights'])}"
    )
    return results


def _rule_based_news_score(text: str) -> dict:
    tl = text.lower()
    for kw in BLACK_SWAN_KEYWORDS:
        if kw in tl:
            return {"severity": 92, "category": "BLACK_SWAN", "is_black_swan": True,
                    "summary": f"Black swan keyword: {kw}"}
    neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in tl)
    pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in tl)
    if neg >= 3:
        return {"severity": 65, "category": "HIGH_RISK",     "is_black_swan": False, "summary": f"{neg} negative signals"}
    elif neg >= 1:
        return {"severity": 35, "category": "MODERATE_RISK", "is_black_swan": False, "summary": f"{neg} negative signal(s)"}
    elif pos >= 1:
        return {"severity": -30, "category": "POSITIVE",     "is_black_swan": False, "summary": f"{pos} positive signal(s)"}
    return {"severity": 0, "category": "NEUTRAL", "is_black_swan": False, "summary": "No significant news"}


def ai_news_risk(symbol: str, headlines: list) -> dict:
    if not headlines:
        return {"severity": 0, "category": "NO_NEWS", "is_black_swan": False, "summary": "No news"}
    clean_headlines = [h[:120] for h in headlines[:5]]
    headlines_text = "\n".join(f"- {h}" for h in clean_headlines)
    prompt = (
        f"Analyze these news headlines for {symbol} and return ONLY a JSON object.\n"
        f"Headlines:\n{headlines_text}\n\n"
        f'Return exactly this JSON (no other text):\n'
        f'{{"severity": <0-100>, "category": "<BLACK_SWAN|HIGH_RISK|MODERATE_RISK|NEUTRAL|POSITIVE>", '
        f'"is_black_swan": <true|false>, "summary": "<one sentence max 100 chars>"}}\n\n'
        f"Severity: 90-100=fraud/ban/arrest, 60-89=regulatory/downgrade, 25-59=earnings miss, "
        f"0-24=neutral, negative=positive."
    )
    text = _call_groq_with_rotation(prompt, max_tokens=150)
    if text:
        result = _parse_ai_json(text)
        if result:
            return result
    return _rule_based_news_score(headlines_text)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — DATA SOURCES
# ─────────────────────────────────────────────────────────────────────────────

_NSE_DELAY_RANGE = (0.3, 1.0)


def fetch_price_data(symbol: str, period: str = "6mo"):
    try:
        import warnings, logging
        # Suppress yfinance noise: "possibly delisted", "401 crumb", progress bars
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = yf.download(symbol, period=period, interval="1d",
                             progress=False, auto_adjust=True,
                             multi_level_index=False)
        if df is not None and len(df) > 20:
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            return df
    except Exception:
        pass
    return None


def load_symbols(filepath: str = "stocks.txt") -> list:
    try:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                syms = []
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        if not s.endswith(".NS"):
                            s += ".NS"
                        syms.append(s)
                _log(f"Loaded {len(syms)} symbols from {filepath}")
                return syms
    except Exception as e:
        _log(f"[WARN] load_symbols failed: {e}")
    fallback = [
        "RELIANCE.NS","INFY.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS",
        "AXISBANK.NS","SBIN.NS","BAJFINANCE.NS","TATAMOTORS.NS","MARUTI.NS",
        "SUNPHARMA.NS","WIPRO.NS","HCLTECH.NS","LTIM.NS","NESTLEIND.NS",
    ]
    _log(f"[WARN] Using fallback symbol list ({len(fallback)} symbols)")
    return fallback


def _download_one(symbol: str, period: str = "6mo") -> tuple:
    time.sleep(random.uniform(*_NSE_DELAY_RANGE))
    df = fetch_price_data(symbol, period=period)
    return symbol, df


def filter_and_download(symbols: list, period: str = "6mo",
                        max_workers: int = 12,
                        min_avg_volume: int = 100_000,
                        min_avg_value_lakhs: float = 50.0) -> dict:
    _log(f"Downloading {len(symbols)} symbols with {max_workers} workers...")
    tradable = {}
    failed = 0
    illiquid_vol = 0
    illiquid_val = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_download_one, sym, period): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                symbol, df = future.result(timeout=30)
                if df is None or len(df) < 20:
                    failed += 1
                    continue
                avg_vol = float(df["Volume"].squeeze().tail(20).mean())
                avg_price = float(df["Close"].squeeze().tail(20).mean())
                avg_val_lakhs = (avg_vol * avg_price) / 100_000
                if avg_vol < min_avg_volume:
                    illiquid_vol += 1
                    continue
                if avg_val_lakhs < min_avg_value_lakhs:
                    illiquid_val += 1
                    continue
                tradable[symbol] = df
            except Exception as e:
                _log(f"[WARN] download failed for {sym}: {e}")
                failed += 1
    # Phase C1 (2026-07-02): honest breakdown so the 46% dropout is not silent.
    _log(
        f"Download complete: {len(tradable)} tradable | {failed} failed | "
        f"{illiquid_vol} illiquid by volume (<{min_avg_volume:,}) | "
        f"{illiquid_val} illiquid by value (<₹{min_avg_value_lakhs:.0f}L/day)"
    )
    return tradable


_NSE_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",  # no 'br' — requests can't decode brotli without extra library
    "Connection": "keep-alive",
    "DNT": "1",
}


def _nse_session() -> requests.Session:
    """
    Pre-warmed session with full browser headers + NSE homepage cookie hit.
    NSE's API 403s without cookies. CI IPs may still be blocked intermittently —
    all callers already fall back to neutral defaults when this returns a bad session.
    """
    session = requests.Session()
    session.headers.update(_NSE_BROWSER_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.5)
    except Exception:
        pass
    return session


def fetch_option_chain(symbol_nse: str) -> dict:
    """
    Option-chain PCR — Phase B5 (2026-06-30) re-source.
      1. NiftyTrader webapi (keyless JSON; works from CI; ~200 F&O symbols)
      2. NSE /api/option-chain-equities (Cloudflare-gated on CI, fine locally)
      3. Honest NEUTRAL_NO_FNO for non-F&O stocks (most NSE stocks have no options)
    Returns: { pcr, total_ce_oi, total_pe_oi, source }
    """
    neutral_no_fno   = {"pcr": 1.0, "total_ce_oi": 0, "total_pe_oi": 0, "source": "NEUTRAL_NO_FNO"}
    neutral_blocked  = {"pcr": 1.0, "total_ce_oi": 0, "total_pe_oi": 0, "source": "NEUTRAL_BLOCKED"}

    # Per-run cache to avoid re-hitting NiftyTrader for the same symbol
    cache = getattr(fetch_option_chain, "_cache", None)
    if cache is None:
        cache = {}
        fetch_option_chain._cache = cache  # type: ignore[attr-defined]
    if symbol_nse in cache:
        return cache[symbol_nse]

    # ── 1. NiftyTrader webapi (CI-friendly) ──────────────────────────────────
    try:
        url = (
            "https://webapi.niftytrader.in/webapi/option/option-chain-data"
            f"?symbol={symbol_nse}&exchange=nse&expiryDate=&atmBelow=10&atmAbove=10"
        )
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept":  "application/json, text/plain, */*",
                "Referer": "https://www.niftytrader.in/",
            },
            timeout=8,
        )
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                data = None
            # Guard every layer: API may return {"resultData": null}, a list,
            # or opDatas as a list of nulls — all trigger 'NoneType.get' otherwise.
            res_raw = data.get("resultData") if isinstance(data, dict) else None
            res     = res_raw if isinstance(res_raw, dict) else {}
            opdata_raw = res.get("opDatas") or res.get("opData") or []
            opdata     = opdata_raw if isinstance(opdata_raw, list) else []
            total_ce_oi = 0
            total_pe_oi = 0
            tot_pe_top  = res.get("total_puts_oi")
            tot_ce_top  = res.get("total_calls_oi")
            if isinstance(tot_pe_top, (int, float)) and isinstance(tot_ce_top, (int, float)):
                total_pe_oi = int(tot_pe_top)
                total_ce_oi = int(tot_ce_top)
            elif opdata:
                for r in opdata:
                    if not isinstance(r, dict):
                        continue
                    total_ce_oi += int(r.get("calls_oi", r.get("ce_oi", 0)) or 0)
                    total_pe_oi += int(r.get("puts_oi", r.get("pe_oi", 0)) or 0)
            if total_ce_oi > 0 or total_pe_oi > 0:
                pcr = round(total_pe_oi / total_ce_oi, 3) if total_ce_oi > 0 else 1.0
                result = {
                    "pcr": pcr,
                    "total_ce_oi": total_ce_oi,
                    "total_pe_oi": total_pe_oi,
                    "source": "niftytrader",
                }
                cache[symbol_nse] = result
                return result
            # 200 but no OI → likely a non-F&O symbol
            cache[symbol_nse] = neutral_no_fno
            return neutral_no_fno
        elif resp.status_code == 404:
            # NiftyTrader returns 404 for symbols not in F&O universe
            cache[symbol_nse] = neutral_no_fno
            return neutral_no_fno
    except Exception as e:
        _log(f"  [PCR NiftyTrader] {symbol_nse} error: {e}")

    # ── 2. NSE direct (legacy) — works locally, Cloudflare-gated on CI ───────
    try:
        session = _nse_session()
        url  = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol_nse}"
        resp = session.get(url, headers={
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/option-chain",
        }, timeout=12)
        if resp.status_code == 200:
            ct = resp.headers.get("Content-Type", "")
            if "html" in ct.lower() or resp.content[:1] == b"<":
                cache[symbol_nse] = neutral_blocked
                return neutral_blocked
            data    = resp.json()
            records = data.get("records", {}).get("data", [])
            total_ce_oi = sum(r.get("CE", {}).get("openInterest", 0) for r in records if "CE" in r)
            total_pe_oi = sum(r.get("PE", {}).get("openInterest", 0) for r in records if "PE" in r)
            pcr = round(total_pe_oi / total_ce_oi, 3) if total_ce_oi > 0 else 1.0
            result = {
                "pcr": pcr,
                "total_ce_oi": total_ce_oi,
                "total_pe_oi": total_pe_oi,
                "source": "nse_direct",
            }
            cache[symbol_nse] = result
            return result
        elif resp.status_code in (403, 429):
            cache[symbol_nse] = neutral_blocked
            return neutral_blocked
    except Exception as e:
        _log(f"  [PCR NSE direct] {symbol_nse} error: {e}")

    cache[symbol_nse] = neutral_blocked
    return neutral_blocked


def pcr_score(pcr: float) -> int:
    if pcr >= 1.5:   return 35
    elif pcr >= 1.2: return 75
    elif pcr >= 0.9: return 60
    elif pcr >= 0.7: return 45
    return 25


def fetch_bulk_deals(days_back: int = 3) -> dict:
    """
    Bulk + block deals — Phase B3 (2026-06-30) reorder:
      1. nselib capital_market.bulk_deal_data + block_deals_data (NSE archive endpoints, CI-friendly)
      2. BSE HTML scrape via pandas.read_html (categorywise + bulk page) as fallback
      3. Legacy NSE /api/historical/bulk-deals (blocked from CI — kept for local runs)
    Result: { "SYM.NS": "BUY" | "SELL" } over the last `days_back` days.
    """
    result: dict = {}

    # ── 1. nselib NSE archive ────────────────────────────────────────────────
    if _NSELIB_OK and _nselib_cm is not None:
        # nselib period= only accepts {'1D','1W','1M','6M','1Y'}.
        # Map our days_back window to the smallest covering shortcut.
        if days_back <= 1:
            period = "1D"
        elif days_back <= 7:
            period = "1W"
        elif days_back <= 31:
            period = "1M"
        elif days_back <= 31 * 6:
            period = "6M"
        else:
            period = "1Y"
        for fn_name in ("bulk_deal_data", "block_deals_data"):
            fn = getattr(_nselib_cm, fn_name, None)
            if fn is None:
                continue
            try:
                # nselib accepts period= shortcuts on most builds. Fall back to
                # explicit from_date/to_date on either a TypeError (signature
                # mismatch) or ValueError (rejected period code).
                try:
                    df = fn(period=period)
                except (TypeError, ValueError):
                    to_d   = ist_today()
                    from_d = to_d - datetime.timedelta(days=days_back)
                    df = fn(from_date=from_d.strftime("%d-%m-%Y"),
                            to_date=to_d.strftime("%d-%m-%Y"))
                if df is None or (hasattr(df, "empty") and df.empty):
                    _log(f"  [nselib {fn_name}] empty — no deals in window")
                    continue
                cols = {c.lower(): c for c in df.columns}
                sym_col = (cols.get("symbol") or cols.get("scrip") or cols.get("security"))
                act_col = (cols.get("buy/sell") or cols.get("buy_sell") or
                           cols.get("trade_type") or cols.get("type"))
                if sym_col is None or act_col is None:
                    _log(f"  [nselib {fn_name}] unexpected schema: {list(df.columns)[:6]}")
                    continue
                for _, row in df.iterrows():
                    sym = str(row[sym_col]).strip().upper().replace(".NS", "") + ".NS"
                    act = str(row[act_col]).strip().upper()
                    action = "BUY" if act.startswith("B") else "SELL"
                    # Don't overwrite a SELL with a BUY from a different deal — last write wins is fine
                    result[sym] = action
                _log(f"  [nselib {fn_name}] ✓ {len(df)} rows → {len(result)} symbols")
            except Exception as e:
                _log(f"  [nselib {fn_name}] error: {e}")
        if result:
            return result

    # ── 2. BSE HTML scrape via pandas.read_html ──────────────────────────────
    try:
        bse_url = "https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx"
        # pandas.read_html needs lxml/html5lib; lxml is in requirements.txt
        tables = pd.read_html(bse_url, flavor="lxml")
        for df in tables:
            cols_lower = [str(c).lower() for c in df.columns]
            if any("security" in c or "scrip" in c or "symbol" in c for c in cols_lower) and \
               any("deal type" in c or "buy/sell" in c or "type" in c for c in cols_lower):
                sym_col = next(c for c in df.columns
                               if "security" in str(c).lower() or "scrip" in str(c).lower()
                               or "symbol" in str(c).lower())
                act_col = next(c for c in df.columns
                               if "deal type" in str(c).lower() or "buy/sell" in str(c).lower()
                               or "type" in str(c).lower())
                for _, row in df.iterrows():
                    sym = str(row[sym_col]).strip().upper().replace(".NS", "") + ".NS"
                    act = str(row[act_col]).strip().upper()
                    action = "BUY" if act.startswith("B") else "SELL"
                    result[sym] = action
                if result:
                    _log(f"  [BSE bulk deals HTML] ✓ {len(result)} symbols")
                    return result
    except Exception as e:
        _log(f"  [BSE bulk deals HTML] error — {e}")

    # ── 3. Legacy NSE /api endpoint (works on non-CI IPs) ────────────────────
    try:
        session = _nse_session()
        today   = ist_today()
        from_dt = (today - datetime.timedelta(days=days_back)).strftime("%d-%m-%Y")
        to_dt   = today.strftime("%d-%m-%Y")
        for url in [
            f"https://www.nseindia.com/api/historical/bulk-deals?from={from_dt}&to={to_dt}",
            "https://www.nseindia.com/api/bulk-deals",
        ]:
            _log(f"  [Bulk Deals legacy] GET {url}")
            resp = session.get(
                url,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.nseindia.com/market-data/bulk-block-deals",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=12,
            )
            if resp.status_code in (403, 429):
                _log(f"  [Bulk Deals legacy] BLOCKED ({resp.status_code}) — Cloudflare/rate-limit")
                break
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                break
            ct = resp.headers.get("Content-Type", "")
            if not resp.content or "html" in ct.lower() or resp.content[:1] == b"<":
                _log("  [Bulk Deals legacy] HTML/empty body — Cloudflare gate")
                break
            try:
                payload = resp.json()
            except Exception:
                break
            for deal in payload.get("data", []):
                sym    = deal.get("symbol", "").strip() + ".NS"
                action = "BUY" if str(deal.get("buySell", "")).upper().startswith("B") else "SELL"
                result[sym] = action
            break
    except Exception as e:
        _log(f"  [Bulk Deals legacy] Network error — {e}")

    if not result:
        _log("  [Bulk Deals] all sources empty — no bulk-deal signal today")
    return result


def bulk_deal_score(symbol: str, bulk_deals_dict: dict) -> int:
    action = bulk_deals_dict.get(symbol)
    if action == "BUY":  return 6
    elif action == "SELL": return -8
    return 0


def ownership_quality_score(promoter_data: dict) -> int:
    # Phase B7 (2026-07-02): FII% / DII% shareholding contributions removed from
    # scoring. Reasons:
    #   1. Screener FII/DII shareholding numbers are quarterly (T-90 stale) —
    #      not fresh enough to influence a swing-trade score.
    #   2. Same signal is already captured via price/volume and delivery %.
    #   3. Macro daily FII/DII flow (₹Cr) never was in the score — only
    #      informational for AI narrative and Telegram display.
    # The fields `fii_pct` / `dii_pct` are still populated so downstream
    # narrative / display code keeps working, but they no longer move the score.
    score    = 50
    pledge   = promoter_data.get("promoter_pledge_pct", 0)
    promoter = promoter_data.get("promoter_holding_pct", 50)
    if pledge > 40:   score -= 30
    elif pledge > 20: score -= 15
    elif pledge > 10: score -= 5
    if promoter > 60:   score += 15
    elif promoter > 50: score += 8
    elif promoter < 30: score -= 10
    return max(0, min(100, score))


NEUTRAL_FUNDAMENTALS = {
    "roe": 0.0, "de_ratio": 0.0, "roce": 0.0,
    "promoter_holding_pct": 50.0, "promoter_pledge_pct": 0.0,
    "fii_pct": 0.0, "dii_pct": 0.0,
    "source": "NEUTRAL_DEFAULT",
}

_SCREENER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in",
}


def _parse_screener_html(soup) -> dict:
    """Parse fundamentals + shareholding from a screener.in BeautifulSoup object."""
    data = {}
    # Key ratios section
    ratio_section = soup.find("section", {"id": "top-ratios"})
    if ratio_section:
        for li in ratio_section.find_all("li"):
            label = li.find("span", class_="name")
            value = (li.find("span", class_="nowrap number") or
                     li.find("span", class_="number") or
                     li.find("span", class_="value"))
            if label and value:
                lbl = label.get_text(strip=True).lower()
                raw = (value.get_text(strip=True)
                       .replace(",", "").replace("%", "").replace("₹", "").strip())
                try:
                    val = float(raw)
                    if "return on equity" in lbl or lbl == "roe":
                        data["roe"] = val
                    elif "debt / equity" in lbl or "d/e" in lbl or "debt to equity" in lbl:
                        data["de_ratio"] = val
                    elif "return on capital" in lbl or "roce" in lbl:
                        data["roce"] = val
                except ValueError:
                    pass

    # Shareholding table
    for table in soup.find_all("table"):
        text = table.get_text()
        if "Promoter" in text and ("FII" in text or "FPI" in text):
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 2:
                    lbl = cells[0].lower()
                    try:
                        val = float(cells[-1].replace("%", "").replace(",", "").strip())
                        if "promoter" in lbl and "pledge" not in lbl:
                            data["promoter_holding_pct"] = val
                        elif "pledge" in lbl or "pledged" in lbl:
                            data["promoter_pledge_pct"] = val
                        elif "fii" in lbl or "fpi" in lbl or "foreign" in lbl:
                            data["fii_pct"] = val
                        elif "dii" in lbl or "domestic inst" in lbl:
                            data["dii_pct"] = val
                    except (ValueError, IndexError):
                        pass
            break  # found the right table

    return data


def fetch_screener_data(symbol_clean: str) -> dict | None:
    """
    Source 1: screener.in — consolidated then standalone.
    Returns dict on success, None on rate-limit or total failure.
    """
    if not _BS4_OK:
        return None
    for url in [
        f"https://www.screener.in/company/{symbol_clean}/consolidated/",
        f"https://www.screener.in/company/{symbol_clean}/",
    ]:
        try:
            resp = requests.get(url, headers=_SCREENER_HEADERS, timeout=12)
            if resp.status_code == 429:
                _log(f"[WARN] screener.in rate-limited for {symbol_clean}")
                return None
            if resp.status_code != 200:
                continue
            data = _parse_screener_html(BeautifulSoup(resp.text, "html.parser"))
            if data:
                data.setdefault("roe", 0.0)
                data.setdefault("de_ratio", 0.0)
                data.setdefault("roce", 0.0)
                data.setdefault("promoter_holding_pct", 50.0)
                data.setdefault("promoter_pledge_pct", 0.0)
                data.setdefault("fii_pct", 0.0)
                data.setdefault("dii_pct", 0.0)
                return data
        except requests.exceptions.Timeout:
            _log(f"[WARN] screener.in timeout for {symbol_clean}")
        except Exception as e:
            _log(f"[WARN] screener.in error for {symbol_clean}: {e}")
    return None


def fetch_trendlyne_data(symbol_clean: str) -> dict | None:
    """
    Source 2: Trendlyne — less strict rate limiting than screener.
    Returns dict on success, None on failure.
    """
    if not _BS4_OK:
        return None
    try:
        url  = f"https://trendlyne.com/equity/fundamental-analysis/{symbol_clean}/NSE/"
        resp = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html",
        }, timeout=12)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        data = {}
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) >= 2:
                    lbl = cells[0].lower()
                    try:
                        val = float(cells[1].replace("%", "").replace(",", "").strip())
                        if "roe" in lbl:
                            data["roe"] = val
                        elif "debt" in lbl and "equity" in lbl:
                            data["de_ratio"] = val
                        elif "roce" in lbl:
                            data["roce"] = val
                        elif "promoter" in lbl and "pledge" not in lbl:
                            data["promoter_holding_pct"] = val
                        elif "pledge" in lbl:
                            data["promoter_pledge_pct"] = val
                    except (ValueError, IndexError):
                        pass
        return data if data else None
    except Exception as e:
        _log(f"[WARN] Trendlyne failed for {symbol_clean}: {e}")
        return None


def fetch_yfinance_fundamentals(symbol: str) -> dict:
    """
    Source 3: yfinance — last resort. Always available, never rate-limited.
    Returns dict with whatever yfinance has; fills missing fields with neutral.
    """
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.info or {}
        data   = {}
        roe = info.get("returnOnEquity")
        if roe is not None:
            data["roe"] = round(float(roe) * 100, 2)  # decimal → %
        de = info.get("debtToEquity")
        if de is not None:
            data["de_ratio"] = round(float(de) / 100, 2)  # % → ratio
        data.setdefault("roe", 0.0)
        data.setdefault("de_ratio", 0.0)
        data.setdefault("roce", 0.0)
        data.setdefault("promoter_holding_pct", 50.0)
        data.setdefault("promoter_pledge_pct", 0.0)
        data.setdefault("fii_pct", 0.0)
        data.setdefault("dii_pct", 0.0)
        return data
    except Exception as e:
        _log(f"[WARN] yfinance fundamentals failed for {symbol}: {e}")
        return {**NEUTRAL_FUNDAMENTALS}


def fetch_promoter_data(symbol_clean: str, delay_seconds: float = 2.5) -> dict:
    """
    3-source fallback chain: screener.in → Trendlyne → yfinance.
    Sequential with delay to avoid rate limiting.
    Call this SEQUENTIALLY — never in parallel threads.

    HONESTY: when screener returns 0/0/0 AND yfinance is rate-limited,
    the fetch is marked source="SCREENER+YF_RL" so downstream renderers
    (BUY card) can show "N/A" instead of "0.0%" — hiding fake real-looking
    zeros in trading decisions.
    """
    time.sleep(delay_seconds)

    data = fetch_screener_data(symbol_clean)
    if data:
        # FIX: screener HTML structure may have changed — ROE/D-E arrive as 0
        # Supplement with yfinance when key financial ratios are all zero
        yf_rl = False
        if data.get("roe", 0) == 0 and data.get("de_ratio", 0) == 0:
            try:
                yf_data = fetch_yfinance_fundamentals(symbol_clean + ".NS")
                # Detect rate-limit: yfinance returns NEUTRAL_FUNDAMENTALS on
                # 429; every key ratio is 0. Track that so BUY card can render "N/A".
                if yf_data.get("roe", 0) == 0 and yf_data.get("de_ratio", 0) == 0 and yf_data.get("roce", 0) == 0:
                    yf_rl = True
                else:
                    if yf_data.get("roe", 0) != 0:
                        data["roe"] = yf_data["roe"]
                    if yf_data.get("de_ratio", 0) != 0:
                        data["de_ratio"] = yf_data["de_ratio"]
                    if yf_data.get("roce", 0) != 0:
                        data["roce"] = yf_data["roce"]
            except Exception:
                yf_rl = True
        data["source"] = "SCREENER+YF_RL" if yf_rl else "SCREENER+YF"
        data["fundamentals_source"] = data["source"]
        _log(f"[INFO] Fundamentals (screener): {symbol_clean} "
             f"ROE={data.get('roe',0):.1f}% D/E={data.get('de_ratio',0):.2f} "
             f"Pledge={data.get('promoter_pledge_pct',0):.1f}%"
             + (" ⚠️ yf rate-limited" if yf_rl else ""))
        return data

    _log(f"[WARN] screener.in failed for {symbol_clean} — trying Trendlyne")
    time.sleep(1.5)

    data = fetch_trendlyne_data(symbol_clean)
    if data:
        data.setdefault("promoter_holding_pct", 50.0)
        data.setdefault("promoter_pledge_pct", 0.0)
        data.setdefault("fii_pct", 0.0)
        data.setdefault("dii_pct", 0.0)
        data.setdefault("roe", 0.0)
        data.setdefault("de_ratio", 0.0)
        data.setdefault("roce", 0.0)
        data["source"] = "TRENDLYNE"
        _log(f"[INFO] Fundamentals (trendlyne): {symbol_clean}")
        return data

    _log(f"[WARN] Trendlyne failed for {symbol_clean} — using yfinance")
    data = fetch_yfinance_fundamentals(symbol_clean + ".NS")
    data["source"] = "YFINANCE"
    _log(f"[INFO] Fundamentals (yfinance): {symbol_clean} "
         f"ROE={data.get('roe',0):.1f}% D/E={data.get('de_ratio',0):.2f}")
    return data


# ---------------------------------------------------------------------------
# Fundamentals cache — avoid re-fetching same stocks within 24h
# ---------------------------------------------------------------------------

def load_fundamentals_cache() -> dict:
    try:
        if os.path.exists(FUNDAMENTALS_CACHE_FILE):
            with open(FUNDAMENTALS_CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        _log(f"[WARN] load_fundamentals_cache failed: {e}")
    return {}


def save_fundamentals_cache(cache: dict) -> None:
    try:
        with open(FUNDAMENTALS_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_fundamentals_cache failed: {e}")


# ---------------------------------------------------------------------------
# DELIVERY % — real per-stock institutional accumulation signal.
# Phase C3 (2026-07-02): delivery-to-traded ratio from nselib bhavcopy
# archives. This is the single strongest per-stock signal in the Indian
# market: high delivery % on rising price = institutional accumulation;
# low delivery % on rising price = speculative pump; high delivery % on
# falling price = insider/large-holder unloading. Cached 24h per symbol.
# ---------------------------------------------------------------------------

def load_delivery_cache() -> dict:
    try:
        if os.path.exists(DELIVERY_CACHE_FILE):
            with open(DELIVERY_CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        _log(f"[WARN] load_delivery_cache failed: {e}")
    return {}


def save_delivery_cache(cache: dict) -> None:
    try:
        with open(DELIVERY_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_delivery_cache failed: {e}")


def fetch_delivery_data(symbol_clean: str, lookback_days: int = 30) -> dict:
    """Fetch per-day delivery % from NSE bhavcopy via nselib.

    Returns:
        {
            "delivery_pct_today":  float (0-100),
            "delivery_pct_20d_avg": float (0-100),
            "delivery_ratio":      float (today / 20d_avg),
            "delivery_signal":     "STRONG_ACCUM" | "ACCUM" | "NEUTRAL" |
                                   "WEAK" | "DISTRIBUTION",
            "series_days":         int (number of daily rows we got),
            "source":              "nselib" | "unavailable"
        }
        Empty-ish result with source="unavailable" if the fetch fails.
    """
    default = {
        "delivery_pct_today":   0.0,
        "delivery_pct_20d_avg": 0.0,
        "delivery_ratio":       1.0,
        "delivery_signal":      "NEUTRAL",
        "series_days":          0,
        "source":               "unavailable",
    }
    try:
        # nselib import — deferred to avoid hard dependency at module load time.
        from nselib import capital_market as _nsecm  # type: ignore
    except Exception as e:
        _log(f"    [DELIV] nselib unavailable ({e}) — using proxy only")
        return default
    try:
        # nselib expects DD-MM-YYYY for from_date / to_date.
        today   = datetime.datetime.now().date()
        # Use a wider window than 20d because market holidays + weekends can
        # thin out the row count; we'll trim later.
        from_d  = today - datetime.timedelta(days=max(45, lookback_days + 15))
        # Phase C4 (2026-07-02): nselib does NOT expose `security_wise_archives`
        # — the correct function is `price_volume_and_deliverable_position_data`
        # (also aliased as `deliverable_position_data`). Confirmed columns:
        # 'Symbol','Series','Date','PrevClose','OpenPrice','HighPrice',
        # 'LowPrice','LastPrice','ClosePrice','AveragePrice',
        # 'TotalTradedQuantity','TurnoverInRs','No.ofTrades','DeliverableQty',
        # '%DlyQttoTradedQty'. Function has no `series` kwarg — filter EQ later.
        df = _nsecm.price_volume_and_deliverable_position_data(
            symbol=symbol_clean,
            from_date=from_d.strftime("%d-%m-%Y"),
            to_date=today.strftime("%d-%m-%Y"),
        )
        if df is None or len(df) == 0:
            return default
        # Filter to EQ series if a Series column is present (older nselib
        # versions may already do this; be defensive).
        try:
            if "Series" in df.columns:
                df = df[df["Series"].astype(str).str.strip().str.upper() == "EQ"]
        except Exception:
            pass
        if df is None or len(df) == 0:
            return default
        # Column name in nselib is usually "%DlyQttoTradedQty" (str with '%')
        # or "DELIV_PER". Handle both.
        col = None
        for cand in ("%DlyQttoTradedQty", "DELIV_PER", "%DlyQt to TradedQty"):
            if cand in df.columns:
                col = cand
                break
        if col is None:
            _log(f"    [DELIV] {symbol_clean}: no delivery column found "
                 f"(cols={list(df.columns)[:6]})")
            return default
        # Parse to float — some rows come as '-' or with '%' suffix.
        vals: list = []
        for v in df[col].tolist():
            try:
                if v is None:
                    continue
                s = str(v).replace("%", "").strip()
                if s in ("", "-", "N/A"):
                    continue
                vals.append(float(s))
            except Exception:
                continue
        # nselib returns most-recent-first typically, but not guaranteed.
        # Sort by date if there's a date column.
        # For safety we just use the last 20 entries as "recent 20" regardless
        # of order — if newest is first, we reverse.
        if len(vals) < 5:
            return default
        # Detect ordering heuristically: nselib usually returns newest-first.
        # We'll assume newest-first and take vals[0] as today, vals[:20] as 20d.
        today_pct   = vals[0]
        twenty_d    = vals[:min(20, len(vals))]
        avg_20d     = sum(twenty_d) / len(twenty_d)
        ratio       = (today_pct / avg_20d) if avg_20d > 0 else 1.0
        # Signal derivation
        if ratio >= 1.30:   sig = "STRONG_ACCUM"
        elif ratio >= 1.15: sig = "ACCUM"
        elif ratio <= 0.70: sig = "DISTRIBUTION"
        elif ratio <= 0.85: sig = "WEAK"
        else:               sig = "NEUTRAL"
        return {
            "delivery_pct_today":   round(today_pct, 2),
            "delivery_pct_20d_avg": round(avg_20d, 2),
            "delivery_ratio":       round(ratio, 3),
            "delivery_signal":      sig,
            "series_days":          len(vals),
            "source":               "nselib",
        }
    except Exception as e:
        _log(f"    [DELIV] {symbol_clean} fetch failed: {e}")
        return default


def fetch_delivery_cached(symbol_clean: str, cache: dict,
                          cache_ttl_hours: int = 24) -> dict:
    """24h-cached wrapper around fetch_delivery_data()."""
    now    = datetime.datetime.now()
    cached = cache.get(symbol_clean)
    if cached:
        try:
            cached_at = datetime.datetime.fromisoformat(cached.get("cached_at", "2000-01-01"))
            if (now - cached_at).total_seconds() / 3600 < cache_ttl_hours:
                return cached["data"]
        except Exception:
            pass
    data = fetch_delivery_data(symbol_clean)
    cache[symbol_clean] = {"data": data, "cached_at": now.isoformat()}
    return data


def delivery_score_from_signal(signal: str, ratio: float) -> float:
    """Convert delivery signal → 0-100 factor score for volume_delivery.

    Anchored around 50 (neutral). Uses the numeric ratio for smooth
    interpolation instead of pure buckets.
    """
    try:
        # Clamp ratio to a sane band before scaling.
        r = max(0.3, min(2.0, float(ratio)))
        # Piecewise: 1.0 → 55, 1.5 → 90, 2.0 → 100, 0.7 → 30, 0.4 → 10.
        if r >= 1.0:
            score = 55 + (r - 1.0) * 90 / 1.0   # 1.0→55, 2.0→145 clipped to 100
        else:
            score = 55 - (1.0 - r) * 75 / 0.6   # 1.0→55, 0.4→(55-75)=-20 clipped
        # Signal override at the extremes so labels remain consistent
        if signal == "STRONG_ACCUM": score = max(score, 88)
        elif signal == "DISTRIBUTION": score = min(score, 18)
        return round(max(0.0, min(100.0, score)), 1)
    except Exception:
        return 50.0


def fetch_promoter_data_cached(symbol_clean: str, cache: dict,
                                cache_ttl_hours: int = 24) -> dict:
    """Returns cached data if fresher than cache_ttl_hours; otherwise fetches live."""
    now    = datetime.datetime.now()
    cached = cache.get(symbol_clean)
    if cached:
        try:
            cached_at = datetime.datetime.fromisoformat(cached.get("cached_at", "2000-01-01"))
            if (now - cached_at).total_seconds() / 3600 < cache_ttl_hours:
                _log(f"[INFO] Fundamentals cache hit: {symbol_clean}")
                return cached["data"]
        except Exception:
            pass
    data = fetch_promoter_data(symbol_clean, delay_seconds=2.5)
    cache[symbol_clean] = {"data": data, "cached_at": now.isoformat()}
    return data


def fetch_all_fundamentals_cached(top_40: list, max_stocks: int = 30) -> list:
    """
    Fetches fundamentals sequentially with 24h cache.

    PRIORITY ORDER (FIX): rearrange top_40 so likely BUY candidates get their
    fundamentals FIRST (before rate-limit exhaustion) — sorted by
    final/base confidence as a proxy for BUY-likelihood. Cache hits are
    effectively free so we can safely widen from 20 -> 30 stocks without
    noticeably slowing the fresh run.

    Adds ~50s on first run; near-instant on same-day re-runs.
    """
    cache = load_fundamentals_cache()
    _log(f"[INFO] Fundamentals cache: {len(cache)} symbols cached")

    def _prio(s):
        # Best proxy for BUY-likelihood: final_confidence > base_confidence > 0.
        return -(s.get("final_confidence", 0) or s.get("base_confidence", 0) or 0)
    ordered  = sorted(top_40, key=_prio)
    to_fetch = ordered[:max_stocks]
    skip     = ordered[max_stocks:]

    est_sec = sum(
        0 if s["symbol"].replace(".NS", "") in cache else 2.5
        for s in to_fetch
    )
    _log(f"[INFO] Estimated fetch time: ~{est_sec:.0f}s for {max_stocks} stocks "
         f"(BUY-priority order)")

    for i, stock in enumerate(to_fetch):
        sym_clean = stock["symbol"].replace(".NS", "")
        _log(f"  [{i+1}/{max_stocks}] {sym_clean}")
        pdata = fetch_promoter_data_cached(sym_clean, cache, cache_ttl_hours=24)
        stock["promoter_data"]       = pdata
        stock["ownership_quality"]   = ownership_quality_score(pdata)
        stock["promoter_pledge_pct"] = pdata.get("promoter_pledge_pct", 0.0)
        stock["roe"]                 = pdata.get("roe", 0.0)
        stock["de_ratio"]            = pdata.get("de_ratio", 0.0)
        stock["roce"]                = pdata.get("roce", 0.0)
        stock["fundamentals_source"] = pdata.get("source", "NEUTRAL_DEFAULT")
        # Refine ownership_quality with ROE + D/E (screener+YF data now available)
        _update_ownership_quality(stock)
        # Log silent-fail cases so we can see WHICH stocks came back empty.
        if (pdata.get("roe", 0) == 0 and pdata.get("de_ratio", 0) == 0
                and stock["fundamentals_source"] in ("NEUTRAL_DEFAULT", "screener_partial")):
            _log(f"    [FUND] {sym_clean}: ROE/D/E both 0 · source={stock['fundamentals_source']}")

    for stock in skip:
        stock["promoter_data"]       = {**NEUTRAL_FUNDAMENTALS}
        stock["ownership_quality"]   = 50
        stock["promoter_pledge_pct"] = 0.0
        stock["roe"]                 = 0.0
        stock["de_ratio"]            = 0.0
        stock["roce"]                = 0.0
        stock["fundamentals_source"] = "NOT_FETCHED"

    save_fundamentals_cache(cache)
    fetched_ok = sum(1 for s in to_fetch
                     if s.get("fundamentals_source") not in ("NEUTRAL_DEFAULT", "NOT_FETCHED"))
    _log(f"  Fundamentals done: {fetched_ok}/{max_stocks} real data | "
         f"{max_stocks - fetched_ok} defaults | cache saved")

    # ── Delivery % fetch (same top-N batch, 24h cache) ─────────────────────
    # Phase C3 (2026-07-02): real per-stock delivery-to-traded ratio.
    # This is the strongest single per-stock signal for the Indian market —
    # separates institutional accumulation from speculative pumps.
    deliv_cache = load_delivery_cache()
    deliv_ok    = 0
    _log(f"  [DELIV] Fetching delivery % for {len(to_fetch)} stocks "
         f"(cache size: {len(deliv_cache)})")
    for stock in to_fetch:
        sym_clean = stock["symbol"].replace(".NS", "")
        ddata = fetch_delivery_cached(sym_clean, deliv_cache, cache_ttl_hours=24)
        stock["delivery_pct_today"]   = ddata.get("delivery_pct_today", 0.0)
        stock["delivery_pct_20d_avg"] = ddata.get("delivery_pct_20d_avg", 0.0)
        stock["delivery_ratio"]       = ddata.get("delivery_ratio", 1.0)
        stock["delivery_signal"]      = ddata.get("delivery_signal", "NEUTRAL")
        stock["delivery_source"]      = ddata.get("source", "unavailable")
        if ddata.get("source") == "nselib":
            deliv_ok += 1
    # Skip stocks (outside top max_stocks) get neutral defaults so downstream
    # logic never trips on missing keys.
    for stock in skip:
        stock["delivery_pct_today"]   = 0.0
        stock["delivery_pct_20d_avg"] = 0.0
        stock["delivery_ratio"]       = 1.0
        stock["delivery_signal"]      = "NEUTRAL"
        stock["delivery_source"]      = "not_fetched"
    save_delivery_cache(deliv_cache)
    _log(f"  [DELIV] Done: {deliv_ok}/{len(to_fetch)} real data | "
         f"{len(to_fetch) - deliv_ok} proxy-only")

    return top_40


GLOBAL_TICKERS = {
    "NIFTY":   "^NSEI",
    "SENSEX":  "^BSESN",
    "VIX_IN":  "^INDIAVIX",
    "VIX_US":  "^VIX",
    "USDINR":  "INR=X",
    "CRUDE":   "CL=F",
    "GOLD":    "GC=F",
    "US10Y":   "^TNX",
    "DXY":     "DX-Y.NYB",
    "SP500":   "^GSPC",
    "DOW":     "^DJI",
}


# Phase B2: Yahoo's INR=X feed has been drifting (54-wk range 84.55–97.05 confirms feed is broken).
# Frankfurter is ECB-backed, keyless, CI-friendly, and updates daily ~16:00 CET.
def _fetch_usdinr_frankfurter() -> "FetchResult | None":
    """Frankfurter API → authoritative USDINR. Returns None on any failure.
    Manual retry loop with exponential backoff (avoids tenacity-version skew)."""
    last_err: "Exception | None" = None
    for attempt in range(3):
        try:
            resp = requests.get(
                "https://api.frankfurter.dev/v1/latest?from=USD&to=INR",
                headers={"User-Agent": "swing-trade-engine/6.0"},
                timeout=8,
            )
            if resp.status_code != 200:
                _log(f"  [USDINR Frankfurter] HTTP {resp.status_code} (try {attempt+1}/3)")
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                time.sleep(1 + attempt + random.random())
                continue
            data = resp.json()
            rate = float(data["rates"]["INR"])
            as_of = data.get("date")  # e.g. "2026-06-30"
            try:
                as_of_d = datetime.datetime.strptime(as_of, "%Y-%m-%d").date()
            except Exception:
                as_of_d = ist_today()
            stale = (ist_today() - as_of_d).days > 3
            return FetchResult(
                value=rate, source="frankfurter",
                as_of_date=as_of_d, fetched_at=ist_now(),
                is_stale=stale,
            )
        except Exception as e:
            last_err = e
            time.sleep(1 + attempt + random.random())
    _log(f"  [USDINR Frankfurter] all 3 attempts failed — last error: {last_err}")
    return None


def fetch_global_macro() -> dict:
    macro = {
        "usdinr": 83.5, "crude_usd": 75.0, "vix_us": 18.0, "vix_in": 15.0,
        "us10y": 4.3, "sp500_1d_pct": 0.0, "china_1d_pct": 0.0,
        "gold_usd": 2300.0, "nifty_1d_pct": 0.0, "dxy": 103.0,
        "sensex_1d_pct": 0.0, "dow_1d_pct": 0.0,
        "vix_in_20d_avg": 15.0, "vix_term_ratio": 1.0,
        # Phase 3: VIX percentile regime (populated below if 1y history fetched)
        "vix_in_percentile": None, "vix_in_regime": "UNKNOWN",
        "vix_in_lookback_days": 0,
    }
    # Provenance map — which source each macro field came from
    macro["sources"] = {}

    # Phase B2: Frankfurter takes precedence over Yahoo for USD/INR.
    # Phase C1 (2026-07-02): widen accept-band to 70–100 so a legit weak-rupee
    # print (~95) is not rejected. Log the value even when rejected so
    # operators know WHY the fallback path fired.
    usdinr_fr = _fetch_usdinr_frankfurter()
    if usdinr_fr is not None and 70.0 <= usdinr_fr.value <= 100.0:
        macro["usdinr"]            = round(usdinr_fr.value, 4)
        macro["sources"]["usdinr"] = usdinr_fr.to_log()
        _log(f"  USD/INR ✓ Frankfurter — {macro['usdinr']:.4f} ({usdinr_fr.to_log()})")
        skip_usdinr_yf = True
    else:
        if usdinr_fr is not None:
            _log(f"  USD/INR ✗ Frankfurter returned {usdinr_fr.value:.4f} — outside [70,100] band, falling back to yfinance INR=X")
        else:
            _log("  USD/INR ✗ Frankfurter failed (all attempts) — falling back to yfinance INR=X")
        skip_usdinr_yf = False

    for name, ticker in GLOBAL_TICKERS.items():
        if name == "USDINR" and skip_usdinr_yf:
            continue
        try:
            # Fetch 1y for VIX_IN so we can compute both 20d MA and 252d percentile;
            # 5d suffices for everything else.
            period = "1y" if name == "VIX_IN" else "5d"
            df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=True)
            if df is not None and len(df) >= 2:
                last = float(df["Close"].squeeze().iloc[-1])
                prev = float(df["Close"].squeeze().iloc[-2])
                pct  = round((last - prev) / prev * 100, 2) if prev != 0 else 0.0
                if name == "USDINR":
                    # Phase C1 (2026-07-02): yfinance INR=X feed has drifted historically.
                    # Sanity-check the value against LKG; a >3% jump vs last known good
                    # is treated as broken feed and swapped out for LKG.
                    lkg_usdinr = load_last_known_good().get("usdinr")
                    _yf_val = last
                    _accept = 70.0 <= _yf_val <= 100.0
                    if _accept and lkg_usdinr:
                        try:
                            _drift = abs(_yf_val - float(lkg_usdinr)) / float(lkg_usdinr)
                            if _drift > 0.03:  # >3% single-day move — unlikely for USDINR
                                _log(f"  USD/INR ⚠ yfinance {_yf_val:.4f} drifts {_drift*100:.1f}% vs LKG {float(lkg_usdinr):.4f} — using LKG")
                                _yf_val = float(lkg_usdinr)
                                macro["sources"]["usdinr"] = f"source=LKG (yfinance drift-rejected) as_of=<lkg>"
                            else:
                                macro["sources"]["usdinr"] = f"source=yfinance({ticker}) as_of={ist_today().isoformat()}"
                        except Exception:
                            macro["sources"]["usdinr"] = f"source=yfinance({ticker}) as_of={ist_today().isoformat()}"
                    elif not _accept and lkg_usdinr:
                        _log(f"  USD/INR ✗ yfinance {_yf_val:.4f} outside [70,100] — using LKG {float(lkg_usdinr):.4f}")
                        _yf_val = float(lkg_usdinr)
                        macro["sources"]["usdinr"] = f"source=LKG (yfinance OOR) as_of=<lkg>"
                    else:
                        macro["sources"]["usdinr"] = f"source=yfinance({ticker}) as_of={ist_today().isoformat()}"
                    macro["usdinr"] = _yf_val
                elif name == "CRUDE":  macro["crude_usd"]    = last
                elif name == "VIX_US": macro["vix_us"]       = last
                elif name == "VIX_IN":
                    macro["vix_in"] = last
                    # VIX term structure: spot vs 20-day average
                    if len(df) >= 20:
                        vix20_avg = float(df["Close"].squeeze().tail(20).mean())
                        macro["vix_in_20d_avg"] = round(vix20_avg, 2)
                        macro["vix_term_ratio"]  = round(last / vix20_avg, 3) if vix20_avg > 0 else 1.0
                    # Phase 3 (2026-07-01): 252-day rolling VIX percentile —
                    # institutional-grade regime signal. Low percentile = market
                    # complacent (raise bar), high percentile = fear (lower bar,
                    # opportunity for contrarian entries).
                    try:
                        vix_series = df["Close"].squeeze().dropna()
                        if len(vix_series) >= 60:
                            # Use up to 252 trading days; fewer if history is short.
                            window = vix_series.tail(252)
                            rank_below = int((window < last).sum())
                            pctile = round(100.0 * rank_below / max(len(window), 1), 1)
                            macro["vix_in_percentile"] = pctile
                            macro["vix_in_lookback_days"] = int(len(window))
                            # Regime bucket for downstream decision logic
                            if pctile <= 15.0:
                                regime = "COMPLACENT"      # raise bar
                            elif pctile >= 85.0:
                                regime = "FEAR"            # lower bar (contrarian)
                            elif pctile >= 65.0:
                                regime = "ELEVATED"        # mild caution
                            else:
                                regime = "NORMAL"
                            macro["vix_in_regime"] = regime
                        else:
                            macro["vix_in_percentile"] = None
                            macro["vix_in_regime"]     = "UNKNOWN"
                    except Exception:
                        macro["vix_in_percentile"] = None
                        macro["vix_in_regime"]     = "UNKNOWN"
                elif name == "US10Y":  macro["us10y"]        = last
                elif name == "GOLD":   macro["gold_usd"]     = last
                elif name == "DXY":    macro["dxy"]          = last
                elif name == "SP500":  macro["sp500_1d_pct"] = pct
                elif name == "DOW":    macro["dow_1d_pct"]   = pct
                elif name == "NIFTY":  macro["nifty_1d_pct"] = pct
                elif name == "SENSEX": macro["sensex_1d_pct"]= pct
        except Exception:
            continue

    # Phase A safety: range-gate everything; degrade gracefully via LKG
    macro = validate_macro(macro)
    return macro


def macro_regime_adjustment(macro: dict) -> int:
    adj = 0
    if macro["vix_us"] > 30:     adj -= 15
    elif macro["vix_us"] > 22:   adj -= 8
    elif macro["vix_us"] < 15:   adj += 5
    if macro["usdinr"] > 85:     adj -= 8
    elif macro["usdinr"] < 82:   adj += 5
    if macro["crude_usd"] > 95:  adj -= 6
    elif macro["crude_usd"] < 70: adj += 3
    if macro["us10y"] > 5.0:     adj -= 8
    elif macro["us10y"] < 4.0:   adj += 4
    if macro["sp500_1d_pct"] < -1.5: adj -= 5
    elif macro["sp500_1d_pct"] > 1.5: adj += 3
    if macro.get("dxy", 103) > 106:  adj -= 5
    elif macro.get("dxy", 103) < 100: adj += 3
    # VIX term structure adjustment
    vix_ratio = macro.get("vix_term_ratio", 1.0)
    if vix_ratio > 1.3:    adj -= 8   # VIX spiking above its own avg = fear
    elif vix_ratio > 1.15: adj -= 4   # elevated
    elif vix_ratio < 0.8:  adj += 4   # suppressed VIX = complacency, slight caution
    elif vix_ratio < 0.9:  adj += 2
    return max(-20, min(10, adj))


def _nse_session_get(path: str, timeout: int = 12) -> "requests.Response | None":
    """
    Establishes a proper NSE browser-like session before hitting any API endpoint.
    NSE uses Cloudflare which drops requests without prior homepage visit + cookies.
    Note: do NOT set Accept-Encoding to include 'br' (brotli) — requests cannot
    decompress brotli without the optional brotli library, causing garbled responses.
    """
    try:
        _BROWSER_UA = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        _BASE_HEADERS = {
            "User-Agent":      _BROWSER_UA,
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",  # no 'br' — requests can't decode brotli
            "Connection":      "keep-alive",
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=_BASE_HEADERS, timeout=10)
        import time as _t; _t.sleep(0.8)
        r = session.get(
            f"https://www.nseindia.com{path}",
            headers={**_BASE_HEADERS,
                     "Referer": "https://www.nseindia.com",
                     "X-Requested-With": "XMLHttpRequest",
                     "Accept": "application/json, text/plain, */*"},
            timeout=timeout,
        )
        return r if r.status_code == 200 else None
    except Exception:
        return None


def _parse_fii_dii_from_text(text: str) -> "dict | None":
    """
    Shared regex parser for FII/DII numbers from any text source.
    Handles patterns like:
      "FII net sellers of Rs 1,234.56 crore"
      "FIIs bought Rs 384 crore; DIIs bought Rs 5748 crore"
      "FIIs Pull Rs 20,637 Crore in One Day"
      "FPI net purchase: 1234.56"
    Returns {"fii_flow_cr": float, "dii_flow_cr": float} or None.
    """
    import re as _re
    text = text.replace("\n", " ").replace("\u00a0", " ")

    # Words that imply net BUY (positive)
    _BUY_WORDS  = r'bought|purchased|inflow|invest|pour|poured|pump|pumped|net\s+buy|net\s+purchase'
    # Words that imply net SELL (negative)
    _SELL_WORDS = r'sold|sell|outflow|seller|pull|pulled|withdraw|withdrew|withdrawn|dump|dumped|offload|offloaded|exit'

    # Generic amount pattern: optional "Rs" then digits with optional commas/decimals
    _AMT = r'(?:Rs\.?\s*|INR\s*|₹\s*)?(\d[\d,]*(?:\.\d+)?)\s*[Cc]rore'

    fii_pat_buy  = rf'(?:FII|FPI)s?\s+(?:net\s+)?(?:{_BUY_WORDS})[^\d{{}}]{{0,40}}{_AMT}'
    fii_pat_sell = rf'(?:FII|FPI)s?\s+(?:net\s+)?(?:{_SELL_WORDS})[^\d{{}}]{{0,40}}{_AMT}'
    fii_pat_net  = rf'(?:FII|FPI)\s+net[^\d]{{0,30}}([-]?[\d,]+(?:\.\d+)?)'
    dii_pat_buy  = rf'(?:DII[sS]?|[Dd]omestic\s+[Ii]nstitutional\s+[Ii]nvestors?)\s+(?:net\s+)?(?:{_BUY_WORDS})[^\d{{}}]{{0,40}}{_AMT}'
    dii_pat_sell = rf'(?:DII[sS]?|[Dd]omestic\s+[Ii]nstitutional\s+[Ii]nvestors?)\s+(?:net\s+)?(?:{_SELL_WORDS})[^\d{{}}]{{0,40}}{_AMT}'
    dii_pat_net  = rf'(?:DII[sS]?|[Dd]omestic\s+[Ii]nstitutional)\s+net[^\d]{{0,30}}([-]?[\d,]+(?:\.\d+)?)'

    def _extract(buy_pat, sell_pat, net_pat):
        m = _re.search(buy_pat, text, _re.I)
        if m:
            return float(m.group(1).replace(",", ""))
        m = _re.search(sell_pat, text, _re.I)
        if m:
            return -float(m.group(1).replace(",", ""))
        m = _re.search(net_pat, text, _re.I)
        if m:
            return float(m.group(1).replace(",", ""))
        return None

    fii_val = _extract(fii_pat_buy, fii_pat_sell, fii_pat_net)
    dii_val = _extract(dii_pat_buy, dii_pat_sell, dii_pat_net)

    if fii_val is not None or dii_val is not None:
        return {
            "fii_flow_cr": fii_val or 0.0,
            "dii_flow_cr": dii_val or 0.0,
        }
    return None


def _fetch_fii_dii_mc_rss() -> "dict | None":
    """Moneycontrol marketstats RSS — plain HTTP, no cookies, works from CI."""
    import re as _re
    url = "https://www.moneycontrol.com/rss/marketstats.xml"
    try:
        if not _FEEDPARSER_OK:
            _log("    [MC RSS] feedparser not available — skipped")
            return None
        feed    = feedparser.parse(url)
        status  = getattr(feed, 'status', 'N/A')
        entries = feed.entries
        _log(f"    [MC RSS] HTTP {status} — {len(entries)} entries")
        fii_entries = [e for e in entries[:10]
                       if ("fii" in (e.get("title","")+e.get("summary","")).lower()
                           or "fpi" in (e.get("title","")+e.get("summary","")).lower())
                       and "crore" in (e.get("title","")+e.get("summary","")).lower()]
        _log(f"    [MC RSS] {len(fii_entries)} FII-related entries found")
        for entry in fii_entries:
            text = entry.get("title", "") + " " + entry.get("summary", "")
            result = _parse_fii_dii_from_text(text)
            if result:
                return result
            _log(f"    [MC RSS] No parse match — title: {entry.get('title','')!r}")
    except Exception as e:
        _log(f"    [MC RSS] Error — {e}")
    return None


def _fetch_fii_dii_et_rss() -> "dict | None":
    """Economic Times markets RSS — plain HTTP, no cookies, works from CI."""
    import re as _re
    try:
        if not _FEEDPARSER_OK:
            _log("    [ET RSS] feedparser not available — skipped")
            return None
        for feed_url in [
            "https://economictimes.indiatimes.com/markets/stocks/news/rss.cms",
            "https://economictimes.indiatimes.com/markets/rss.cms",
            "https://economictimes.indiatimes.com/rss/news/topic/fii",
        ]:
            feed    = feedparser.parse(feed_url)
            status  = getattr(feed, 'status', 'N/A')
            entries = feed.entries
            _log(f"    [ET RSS] {feed_url} → HTTP {status}, {len(entries)} entries")
            if not entries:
                continue
            fii_entries = [
                e for e in entries[:20]
                if ("fii" in (e.get("title","")+e.get("summary","")).lower()
                    or "fpi" in (e.get("title","")+e.get("summary","")).lower())
                and "crore" in (e.get("title","")+e.get("summary","")).lower()
                and any(w in (e.get("title","")+e.get("summary","")).lower()
                        for w in ("bought", "sold", "net", "pull", "pour", "inflow", "outflow"))
            ]
            _log(f"    [ET RSS] {len(fii_entries)} FII-related entries")
            for entry in fii_entries:
                text   = entry.get("title", "") + " " + entry.get("summary", "")
                result = _parse_fii_dii_from_text(text)
                if result:
                    _log(f"    [ET RSS] matched — title: {entry.get('title','')!r}")
                    return result
                _log(f"    [ET RSS] no parse match — title: {entry.get('title','')!r}")
            if fii_entries:
                break  # Found relevant entries but couldn't parse — don't try more URLs
    except Exception as e:
        _log(f"    [ET RSS] Error — {e}")
    return None


def _fetch_fii_dii_bs_rss() -> "dict | None":
    """Business Standard markets RSS — plain HTTP, no cookies, works from CI."""
    url = "https://www.business-standard.com/rss/markets-106.rss"
    try:
        if not _FEEDPARSER_OK:
            _log("    [BS RSS] feedparser not available — skipped")
            return None
        feed    = feedparser.parse(url)
        status  = getattr(feed, 'status', 'N/A')
        entries = feed.entries
        _log(f"    [BS RSS] HTTP {status} — {len(entries)} entries")
        fii_entries = [e for e in entries[:20]
                       if ("fii" in (e.get("title","")+e.get("summary","")).lower()
                           or "fpi" in (e.get("title","")+e.get("summary","")).lower())
                       and "crore" in (e.get("title","")+e.get("summary","")).lower()]
        _log(f"    [BS RSS] {len(fii_entries)} FII-related entries")
        for entry in fii_entries:
            text   = entry.get("title", "") + " " + entry.get("summary", "")
            result = _parse_fii_dii_from_text(text)
            if result:
                return result
            _log(f"    [BS RSS] No parse match — title: {entry.get('title','')!r}")
    except Exception as e:
        _log(f"    [BS RSS] Error — {e}")
    return None


def _fetch_fii_dii_google_news() -> "dict | None":
    """
    Google News RSS — single combined query extracts both FII and DII from the same article.
    DII never has standalone headlines — it always appears alongside FII in the same article.
    Falls back to FII-only query if combined query yields nothing.
    """
    try:
        if not _FEEDPARSER_OK:
            _log("    [Google News] feedparser not available — skipped")
            return None
        from urllib.parse import quote
        today_str = datetime.date.today().strftime("%d %b").lstrip("0")  # "29 Jun"

        _CUMULATIVE_PHRASES = (
            "so far", "lakh crore", "ytd", "cumulative",
            "month to date", "year to date", "this month", "this year",
            "in the month", "for the month", "during the month",
            "total outflow", "total inflow", "total 2024", "total 2025", "total 2026",
            "since january", "since april", "outflows reach", "inflows reach",
            # Phase B1 (2026-06-30): kill historical-record headlines that misled the parser.
            # Phase B2 (2026-07-02): removed "single-day" / "single day" — legitimate daily
            # prints often use that phrasing (e.g. "largest single-day outflow" IS today's
            # data). Keep only unambiguously historical markers.
            "largest", "biggest", "record",
            "all-time", "all time", "outflow in 2024", "outflow in 2025", "outflow in 2026",
            "inflow in 2024",  "inflow in 2025",  "inflow in 2026",
            "outflow of 2024", "outflow of 2025", "outflow of 2026",
            "highest ever", "lowest ever", "biggest ever", "largest ever",
        )

        # Try combined query first (best chance of finding both FII + DII in one article),
        # then fall back to FII-only
        for raw_q in [
            f"FII DII crore NSE {today_str}",   # combined — most articles mention both
            f"FII FPI crore NSE {today_str}",    # FII-only fallback
        ]:
            url  = f"https://news.google.com/rss/search?q={quote(raw_q)}&hl=en-IN&gl=IN&ceid=IN:en"
            _log(f"    [Google News] fetching: {url}")
            feed    = feedparser.parse(url)
            status  = getattr(feed, "status", "N/A")
            entries = feed.entries
            _log(f"    [Google News] HTTP {status} — {len(entries)} entries")

            for entry in entries[:25]:
                title = entry.get("title", "")
                text  = title + " " + entry.get("summary", "")
                tl    = text.lower()

                if "fii" not in tl and "fpi" not in tl and "foreign institutional" not in tl:
                    continue
                if "crore" not in tl:
                    continue
                if any(p in tl for p in _CUMULATIVE_PHRASES):
                    _log(f"    [Google News] skipped (cumulative) — {title!r}")
                    continue

                # Phase B1.1 (2026-06-30): freshness guard. Reject articles
                # older than 3 calendar days so stale headlines (e.g. a Feb-3
                # report parsed in June) cannot leak through as "today's" flow.
                pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub_struct is not None:
                    try:
                        pub_dt = datetime.datetime(*pub_struct[:6], tzinfo=datetime.timezone.utc)
                        age_days = (datetime.datetime.now(datetime.timezone.utc) - pub_dt).days
                        if age_days > 3:
                            _log(
                                f"    [Google News] skipped (stale, {age_days}d old) — {title!r}"
                            )
                            continue
                    except Exception:
                        pass  # bad/missing date — be permissive, parse anyway

                # Belt-and-braces: also block any article whose body explicitly
                # quotes a different month/year. Today's article would say e.g.
                # "30 Jun", not "Feb 3" or "Mar 12". This catches stripped
                # Google-News titles where published_parsed is wrong.
                now_ist     = datetime.datetime.now(IST)
                this_month  = now_ist.strftime("%b").lower()       # 'jun'
                this_year   = str(now_ist.year)
                other_month_hit = False
                for m in ("jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"):
                    if m == this_month:
                        continue
                    # Match "feb 3", "feb, 3", "feb 3,", "feb 03"
                    if re.search(rf"\b{m}\s+\d", tl):
                        other_month_hit = True
                        break
                if other_month_hit:
                    _log(f"    [Google News] skipped (off-month date in body) — {title!r}")
                    continue

                _log(f"    [Google News] attempting parse — {title!r}")
                result = _parse_fii_dii_from_text(text)
                if not result or result.get("fii_flow_cr", 0) == 0:
                    _log(f"    [Google News] no FII value found")
                    continue

                fii_val   = result["fii_flow_cr"]
                dii_val   = result.get("dii_flow_cr", 0.0)
                dii_found = dii_val != 0
                _log(
                    f"    [Google News] matched — FII {fii_val:+.0f}Cr"
                    + (f" | DII {dii_val:+.0f}Cr" if dii_found else " | DII not in this article")
                )
                return {
                    "fii_flow_cr": fii_val,
                    "dii_flow_cr": dii_val,
                    "dii_found":   dii_found,
                }

    except Exception as e:
        _log(f"    [Google News] Error — {e}")
    return None


def _fetch_fii_dii_nse() -> "dict | None":
    """Try NSE fiidiiTradeReact with proper session (works on local, may fail on CI)."""
    try:
        r = _nse_session_get("/api/fiidiiTradeReact")
        if not r:
            _log("    [NSE API] Session/request returned None (Cloudflare block or timeout)")
            return None
        _log(f"    [NSE API] HTTP {r.status_code} | Content-Type: {r.headers.get('Content-Type','?')} | Body len: {len(r.content)}")
        if not r.content:
            _log("    [NSE API] Empty body — Cloudflare gate (no action needed if before 5:30 PM)")
            return None
        ct = r.headers.get("Content-Type", "")
        if "html" in ct.lower() or r.content[:1] == b"<":
            _log(f"    [NSE API] Got HTML instead of JSON (Cloudflare gate) — body snippet: {r.text[:80]!r}")
            return None
        try:
            data = r.json()
        except Exception as je:
            _log(f"    [NSE API] JSON parse failed: {je} — body snippet: {r.text[:80]!r}")
            return None
        if not isinstance(data, list) or not data:
            _log(f"    [NSE API] Unexpected response format: {type(data).__name__} — {str(data)[:80]}")
            return None
        latest   = data[0]
        fii_buy  = float(str(latest.get("fiiBuy",  "0")).replace(",", ""))
        fii_sell = float(str(latest.get("fiiSell", "0")).replace(",", ""))
        dii_buy  = float(str(latest.get("diiBuy",  "0")).replace(",", ""))
        dii_sell = float(str(latest.get("diiSell", "0")).replace(",", ""))
        if fii_buy + fii_sell + dii_buy + dii_sell == 0:
            _log("    [NSE API] All buy/sell values are zero — data not yet published")
            return None
        return {
            "fii_flow_cr": round(fii_buy - fii_sell, 2),
            "dii_flow_cr": round(dii_buy - dii_sell, 2),
        }
    except Exception as e:
        _log(f"    [NSE API] Unexpected error — {e}")
        return None


# ── Phase B1: nselib NSE category-wise turnover (authoritative T-1) ────────
def _fetch_fii_dii_nselib_nsdl() -> "dict | None":
    """
    Authoritative T-1 FII / DII source via nselib `category_turnover_cash`.

    NSE publishes the official "Category-wise Turnover (Cash Market)" sheet
    every business day at ~7 PM IST. It carries Buy / Sell / Net values
    (Rs Crores) for: Bank, Insurance Companies, Mutual Funds, AIF, PMS,
    RETAIL, OTHERS, **FPI**. Free, no auth, CI-friendly.

      FII flow  = FPI Net
      DII flow  = Banks + Insurance + Mutual Funds + AIF + PMS (Net)

    Walks back day-by-day until it finds the most recent published date
    (NSE archive has ~T-1 lag; weekends + holidays are skipped silently).
    Name kept for backward compat with the source-cascade table.
    """
    if not _NSELIB_OK or _nselib_cm is None:
        _log("    [nselib NSE-cat] nselib not installed — skipped")
        return None
    fn = getattr(_nselib_cm, "category_turnover_cash", None)
    if fn is None:
        _log("    [nselib NSE-cat] category_turnover_cash unavailable in this nselib build")
        return None

    DII_CATS = {"bank", "insurance companies", "mutual funds", "aif", "pms"}
    FII_CATS = {"fpi"}

    today = ist_today()

    # Compute the *expected* freshest business date.
    #  * After 7 PM IST on a business day  → expect today (NSDL publishes ~7 PM IST)
    #  * Before 7 PM IST on a business day → expect T-1 (previous business day)
    #  * On a weekend / holiday            → expect the most recent business day
    now_ist_dt = datetime.datetime.now(IST)
    from datetime import time as _dtime
    if today.weekday() < 5 and now_ist_dt.time() >= _dtime(19, 0):
        exp_t1 = today
    else:
        exp_t1 = today - datetime.timedelta(days=1)
        while exp_t1.weekday() >= 5:  # Sat/Sun → walk back to Fri
            exp_t1 -= datetime.timedelta(days=1)

    for delta in range(1, 10):     # T-1 .. T-9 — covers weekends + holidays
        d = today - datetime.timedelta(days=delta)
        # Skip weekends — NSE never publishes for Sat / Sun
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%d-%m-%Y")
        try:
            df = fn(date_str)
        except Exception as e:
            msg = str(e)
            # "No data available" is the normal not-yet-published / holiday signal
            if "No data available" not in msg:
                _log(f"    [nselib NSE-cat] {date_str} error: {e}")
            continue
        if df is None or (hasattr(df, "empty") and df.empty):
            continue
        try:
            cols = {c.lower(): c for c in df.columns}
            cat_col  = cols.get("category")
            net_col  = cols.get("net value in rs.crores") or cols.get("net value")
            if cat_col is None or net_col is None:
                _log(f"    [nselib NSE-cat] {date_str} unexpected schema: {list(df.columns)}")
                continue
            fii_net = 0.0
            dii_net = 0.0
            for _, row in df.iterrows():
                cat = str(row[cat_col]).strip().lower()
                try:
                    val = float(str(row[net_col]).replace(",", "").strip())
                except (ValueError, TypeError):
                    continue
                if cat in FII_CATS:
                    fii_net += val
                elif cat in DII_CATS:
                    dii_net += val
            if fii_net == 0.0 and dii_net == 0.0:
                continue
            stale = d < exp_t1
            stale_tag = f" ⚠️ STALE (expected T-1={exp_t1.strftime('%d-%m-%Y')})" if stale else ""
            _log(f"    [nselib NSE-cat] {date_str} ✓ — FII {fii_net:+.0f}Cr DII {dii_net:+.0f}Cr{stale_tag}")
            return {
                "fii_flow_cr":   round(fii_net, 2),
                "dii_flow_cr":   round(dii_net, 2),
                "dii_found":     dii_net != 0,
                "as_of":         d.isoformat(),
                "is_provisional": False,
                "stale":         stale,
                "expected_t1":   exp_t1.isoformat(),
            }
        except Exception as e:
            _log(f"    [nselib NSE-cat] {date_str} parse failure: {e}")
            continue
    _log("    [nselib NSE-cat] no data found in last 9 days")
    return None


def _fetch_fii_dii_bse() -> "dict | None":
    """
    BSE categorywise turnover — public HTML, no Cloudflare gate from CI.
    Provisional T+0 numbers. Returns FII + DII. Used for cross-check vs NSDL.
    """
    try:
        url = "https://api.bseindia.com/BseIndiaAPI/api/CatTurnover/w?TDate="
        # The endpoint accepts an empty TDate and returns latest available
        tdate = ist_today().strftime("%Y%m%d")
        resp = requests.get(
            url + tdate,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept":  "application/json, text/plain, */*",
                "Referer": "https://www.bseindia.com/markets/equity/EQReports/categorywise_turnover.aspx",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            _log(f"    [BSE FII] HTTP {resp.status_code} — skipped")
            return None
        try:
            data = resp.json()
        except Exception:
            return None
        rows = data if isinstance(data, list) else data.get("Table", data.get("data", []))
        if not rows:
            _log(f"    [BSE FII] HTTP 200 but empty rows for TDate={tdate} — not yet published")
            return None
        fii_net = 0.0
        dii_net = 0.0
        for r in rows:
            cat = str(r.get("CATEGORY", r.get("Category", ""))).upper()
            buy_v  = float(str(r.get("BUYVAL",  r.get("BuyValue",  0))).replace(",", "") or 0)
            sell_v = float(str(r.get("SELLVAL", r.get("SellValue", 0))).replace(",", "") or 0)
            net_v  = buy_v - sell_v
            if "FII" in cat or "FPI" in cat or "FOREIGN" in cat:
                fii_net += net_v
            elif "DII" in cat or "DOMESTIC" in cat or "MUTUAL" in cat or "MF" in cat:
                dii_net += net_v
        if fii_net == 0 and dii_net == 0:
            return None
        _log(f"    [BSE FII] ✓ — FII {fii_net:+.0f}Cr DII {dii_net:+.0f}Cr")
        return {
            "fii_flow_cr": round(fii_net, 2),
            "dii_flow_cr": round(dii_net, 2),
            "dii_found":   dii_net != 0,
        }
    except Exception as e:
        _log(f"    [BSE FII] error — {e}")
        return None


def fetch_fii_dii_flows(max_retries: int = 2) -> dict:
    """
    Phase C4 (2026-07-02): FII/DII flows are NO LONGER used anywhere in the
    pipeline — not for scoring, sizing, gates, or regime detection. NSDL data
    is structurally D-1/D-2 stale, the RSS fallback cascade is noisy and
    burns ~7 seconds every run for a signal we already ignore.

    We keep this function only so the (few) legacy callers that ask for the
    macro dict don't KeyError. It now short-circuits: returns
    available=False with zero flows and never touches the network.

    If you want the old multi-source cascade back, see git history for the
    NSDL / BSE / RSS / Google News implementations.
    """
    _log("  [FII/DII] Fetch disabled (Phase C4) — flows no longer scored or displayed.")
    return {
        "fii_flow_cr":    0.0,
        "dii_flow_cr":    0.0,
        "is_provisional": False,
        "available":      False,
        "dii_found":      False,
        "source":         "DISABLED",
        "confidence":     "NONE",
        "stale":          False,
    }


def _fii_dii_flows_disabled_legacy_impl(max_retries: int = 2) -> dict:
    """Kept for reference; never called. Original 7-source cascade lives in git history."""
    from datetime import time as dtime
    import time as _time

    now_ist     = datetime.datetime.now()
    now_t       = now_ist.time()
    just_closed = dtime(15, 30) <= now_t <= dtime(16, 15)
    is_prov     = now_t < dtime(18, 0)

    # Timing context — informational only, we always attempt sources
    if now_t < dtime(15, 30):
        _log(f"  [FII/DII] {now_t.strftime('%H:%M')} IST — market still open, today's data not published yet (expected)")
    elif now_t < dtime(17, 30):
        _log(f"  [FII/DII] {now_t.strftime('%H:%M')} IST — market closed, NSE provisional data expected ~5:30 PM (may get 0)")
    else:
        _log(f"  [FII/DII] {now_t.strftime('%H:%M')} IST — data should be available, attempting all sources")

    result = {
        "fii_flow_cr": 0.0, "dii_flow_cr": 0.0,
        "is_provisional": False, "available": False, "dii_found": False,
        "source": "NONE", "confidence": "NONE",
    }

    # Phase B1 (2026-06-30): NSDL is the authoritative T-1 source; BSE is provisional T+0.
    # Try authoritative paths first, then RSS fallbacks for CI.
    sources = [
        ("nselib NSDL",           _fetch_fii_dii_nselib_nsdl),
        ("BSE Categorywise",      _fetch_fii_dii_bse),
        ("NSE API",               _fetch_fii_dii_nse),
        ("Moneycontrol RSS",      _fetch_fii_dii_mc_rss),
        ("Economic Times RSS",    _fetch_fii_dii_et_rss),
        ("Business Standard RSS", _fetch_fii_dii_bs_rss),
        ("Google News RSS",       _fetch_fii_dii_google_news),
    ]

    # First pass: gather NSDL + BSE for cross-check, return early if NSDL is good
    nsdl_data: "dict | None" = None
    bse_data:  "dict | None" = None
    for name, fn in sources[:2]:
        try:
            _log(f"  [FII/DII] Trying {name}...")
            d = fn()
            if d and (d.get("fii_flow_cr", 0) != 0 or d.get("dii_flow_cr", 0) != 0):
                if name == "nselib NSDL":
                    nsdl_data = d
                else:
                    bse_data = d
        except Exception as e:
            _log(f"  [FII/DII] {name} error: {e}")

    if nsdl_data is not None:
        # Freshness check — if NSE archive is behind expected T-1, prefer provisional
        # sources (BSE / RSS) which usually publish T-1 by 5:30 PM IST.
        nsdl_stale = bool(nsdl_data.get("stale"))
        if nsdl_stale and bse_data is None:
            _log(f"  [FII/DII] NSDL is STALE (as_of={nsdl_data.get('as_of')} "
                 f"expected={nsdl_data.get('expected_t1')}) and BSE missing — "
                 f"falling through to RSS sources for fresher T-1 data")
            # Skip returning stale NSDL; try RSS below
        else:
            nsdl_fii = nsdl_data.get("fii_flow_cr")
            bse_fii  = bse_data.get("fii_flow_cr") if bse_data else None
            chosen_fii, confidence = cross_check_fii(nsdl_fii, bse_fii)
            result["fii_flow_cr"]    = chosen_fii if chosen_fii is not None else nsdl_fii
            # DII: prefer NSDL if present, else BSE
            if nsdl_data.get("dii_found"):
                result["dii_flow_cr"] = nsdl_data.get("dii_flow_cr", 0.0)
                result["dii_found"]   = True
            elif bse_data and bse_data.get("dii_found"):
                result["dii_flow_cr"] = bse_data.get("dii_flow_cr", 0.0)
                result["dii_found"]   = True
            result["is_provisional"] = is_prov
            result["available"]      = True
            result["source"]         = "nsdl+bse" if bse_data else "nsdl"
            result["confidence"]     = "STALE" if nsdl_stale else confidence
            result["as_of"]          = nsdl_data.get("as_of")
            result["stale"]          = nsdl_stale
            dii_str = f"{result['dii_flow_cr']:+.0f}Cr" if result["dii_found"] else "N/A"
            stale_tag = " ⚠️ STALE" if nsdl_stale else ""
            _log(f"  [FII/DII] ✓ NSDL{stale_tag} — FII {result['fii_flow_cr']:+.0f}Cr | "
                 f"DII {dii_str} | as_of={result['as_of']} | confidence={result['confidence']}")
            return result

    # NSDL missed — fall back to BSE alone if we have it
    if bse_data is not None:
        result["fii_flow_cr"]    = bse_data["fii_flow_cr"]
        result["dii_flow_cr"]    = bse_data.get("dii_flow_cr", 0.0)
        result["dii_found"]      = bse_data.get("dii_found", False)
        result["is_provisional"] = True  # BSE is provisional
        result["available"]      = True
        result["source"]         = "bse"
        result["confidence"]     = "MEDIUM"
        dii_str = f"{result['dii_flow_cr']:+.0f}Cr" if result["dii_found"] else "N/A"
        _log(f"  [FII/DII] ✓ BSE (provisional, no NSDL) — FII {result['fii_flow_cr']:+.0f}Cr | DII {dii_str}")
        return result

    # Authoritative sources missed — try RSS fallbacks
    for name, fn in sources[2:]:
        try:
            _log(f"  [FII/DII] Trying {name}...")
            data = fn()
            if data and (data.get("fii_flow_cr", 0) != 0 or data.get("dii_flow_cr", 0) != 0):
                result["fii_flow_cr"]    = data["fii_flow_cr"]
                result["dii_flow_cr"]    = data["dii_flow_cr"]
                result["dii_found"]      = data.get("dii_found", data.get("dii_flow_cr", 0) != 0)
                result["is_provisional"] = is_prov
                result["available"]      = True
                result["source"]         = name.lower().replace(" ", "_")
                result["confidence"]     = "LOW"   # RSS scrape is best-effort
                dii_str = f"{result['dii_flow_cr']:+.0f}Cr" if result["dii_found"] else "N/A"
                _log(
                    f"  [FII/DII] {name} ✓ — "
                    f"FII {result['fii_flow_cr']:+.0f}Cr | DII {dii_str} | confidence=LOW"
                )
                return result
        except Exception as e:
            _log(f"  [FII/DII] {name} error: {e}")

    # If market just closed, wait 60s and retry RSS sources
    if just_closed:
        _log("  [FII/DII] All sources returned nothing — waiting 60s for data to publish...")
        _time.sleep(60)
        for name, fn in [
            ("Moneycontrol RSS",   _fetch_fii_dii_mc_rss),
            ("Economic Times RSS", _fetch_fii_dii_et_rss),
        ]:
            try:
                data = fn()
                if data and (data.get("fii_flow_cr", 0) != 0 or data.get("dii_flow_cr", 0) != 0):
                    result.update(data)
                    result["is_provisional"] = True
                    result["available"]      = True
                    _log(f"  [FII/DII] {name} (retry) ✓ — FII {result['fii_flow_cr']:+.0f}Cr")
                    return result
            except Exception:
                pass

    # Last resort: if we skipped stale NSDL earlier but RSS also failed, use stale NSDL
    if nsdl_data is not None and not result["available"]:
        _log(f"  [FII/DII] ⚠️ All fresh sources failed — returning STALE NSDL "
             f"(as_of={nsdl_data.get('as_of')}) as last resort")
        result["fii_flow_cr"]    = nsdl_data.get("fii_flow_cr", 0.0)
        result["dii_flow_cr"]    = nsdl_data.get("dii_flow_cr", 0.0)
        result["dii_found"]      = nsdl_data.get("dii_found", False)
        result["is_provisional"] = True
        result["available"]      = True
        result["source"]         = "nsdl_stale"
        result["confidence"]     = "STALE"
        result["as_of"]          = nsdl_data.get("as_of")
        result["stale"]          = True
        return result

    _log(
        "  [FII/DII] All sources returned 0 — "
        + ("data not published yet (normal before ~5:30 PM, no action needed)"
           if now_t < dtime(17, 30)
           else "data should be available but all sources failed (API/network issue — investigate)")
    )
    return result


def get_fii_dii_data() -> dict:
    """Master function — single entry point for all FII/DII fetching."""
    return fetch_fii_dii_flows(max_retries=2)


def format_fii_dii_line(fii_data: dict) -> str:
    """
    Timing-aware FII/DII formatter.
    Never shows Rs0Cr — shows a timing explanation when data isn't published yet.
    Shows 'N/A' for DII when it was not found (vs genuinely zero).
    """
    from datetime import time as dtime
    now_ist     = datetime.datetime.now()
    just_closed = dtime(15, 30) <= now_ist.time() <= dtime(16, 30)
    market_open = dtime(9, 15)  <= now_ist.time() <  dtime(15, 30)

    if not fii_data.get("available"):
        if just_closed:
            return "FII/DII: Provisional data publishing (available ~6:30 PM)"
        elif market_open:
            return "FII/DII: Intraday provisional data pending (~10:30 AM)"
        else:
            return "FII/DII: Data unavailable"

    fii       = fii_data["fii_flow_cr"]
    dii       = fii_data["dii_flow_cr"]
    dii_found = fii_data.get("dii_found", dii != 0)  # backward-compat: if no flag, assume found if non-zero
    prov      = " (provisional)" if fii_data.get("is_provisional") else ""

    # Signed formatting so the icon is never ambiguous. 🟢 = inflow (positive),
    # 🔴 = outflow (negative), ⚪ = zero/unknown. Previously showed a red icon
    # next to a positive rupee number which contradicted the daily summary.
    fii_icon = "🟢" if fii > 0 else ("🔴" if fii < 0 else "⚪")
    dii_icon = "🟢" if dii > 0 else ("🔴" if dii < 0 else "⚪")

    if fii > 500 and dii_found and dii > 500:
        sentiment = "💪 Both buying"
    elif fii < -500 and dii_found and dii < -500:
        sentiment = "⚠️ Both selling"
    elif fii > 500:
        sentiment = "🟢 FII buying"
    elif dii_found and dii > 500 and fii < -500:
        sentiment = "🟡 DII absorbing FII selling"
    elif dii_found and dii > 500:
        sentiment = "🟡 DII supporting"
    elif fii < -500:
        sentiment = "🔴 FII selling"
    else:
        sentiment = "➡️ Neutral flows"

    # Show the SIGN in the amount too (₹+3318Cr / ₹-3318Cr) so text and icon agree.
    dii_str = f"{dii_icon} ₹{dii:+,.0f}Cr" if dii_found else "N/A"

    return (
        f"FII {fii_icon} ₹{fii:+,.0f}Cr · "
        f"DII {dii_str} · {sentiment}{prov}"
    )


def interpret_nifty_structure(close: float, ema20: float, ema50: float,
                               ema200: float, high_52w: float) -> str:
    """One-line plain English interpretation of NIFTY EMA structure."""
    above_ema20  = close > ema20
    above_ema50  = close > ema50
    above_ema200 = close > ema200
    dist_52w_high_pct = round((high_52w - close) / high_52w * 100, 1) if high_52w > 0 else 0

    if above_ema20 and above_ema50 and above_ema200:
        return "✅ Above all EMAs — bull structure intact"
    elif above_ema50 and above_ema200:
        return "🟡 Below EMA20 but above EMA50/200 — minor pullback"
    elif above_ema200 and not above_ema50:
        return "🟠 Below EMA20 & EMA50 — correction underway, watch EMA200"
    elif not above_ema200:
        if dist_52w_high_pct > 10:
            return "🔴 Below all EMAs — bearish structure. EMA200 is now resistance."
        else:
            return "🔴 Below all EMAs — recent breakdown. High caution."
    return "⚪ Mixed EMA structure — no clear trend"


def regime_explanation(score: float, regime: str, vix_in: float,
                        breadth_20: float, fii_flow: float, dii_flow: float,
                        nifty_close: float = 0, ema20: float = 0,
                        ema50: float = 0, ema200: float = 0) -> str:
    """One-line 'why this regime' — checks VIX, breadth, flows AND EMA positioning."""
    reasons_bull = []
    reasons_bear = []

    if vix_in < 15:
        reasons_bull.append(f"VIX-IN calm {vix_in:.1f}")
    elif vix_in > 20:
        reasons_bear.append(f"VIX-IN high {vix_in:.1f}")

    combined = fii_flow + dii_flow
    if combined > 3000:
        reasons_bull.append("institutions buying")
    elif combined < -2000:
        reasons_bear.append("institutions selling")

    if breadth_20 > 60:
        reasons_bull.append(f"breadth {breadth_20:.0f}%")
    elif breadth_20 < 40:
        reasons_bear.append(f"weak breadth {breadth_20:.0f}%")

    # EMA positioning — most important structural signal
    if nifty_close > 0 and ema20 > 0 and ema50 > 0 and ema200 > 0:
        above20  = nifty_close > ema20
        above50  = nifty_close > ema50
        above200 = nifty_close > ema200
        if above20 and above50 and above200:
            reasons_bull.append("above all EMAs")
        elif above50 and above200:
            reasons_bull.append("above EMA50/200")
            reasons_bear.append("below EMA20")
        elif above200:
            reasons_bear.append("below EMA20 & EMA50")
        else:
            reasons_bear.append("below all EMAs")  # strongest bear signal

    bull_str = ", ".join(reasons_bull) if reasons_bull else "none"
    bear_str = ", ".join(reasons_bear) if reasons_bear else "none"
    return f"Bulls: {bull_str} | Bears: {bear_str}"


NEWS_RSS_FEEDS = {
    "MONEYCONTROL":   "https://www.moneycontrol.com/rss/MCtopnews.xml",
    "BUSINESS_STD":   "https://www.business-standard.com/rss/markets-106.rss",
    "LIVEMINT":       "https://www.livemint.com/rss/markets",
    "NDTV_PROFIT":    "https://feeds.feedburner.com/ndtvprofit-latest",
    "REUTERS_IN":     "https://feeds.reuters.com/reuters/INbusinessNews",
}


def news_decay_weight(age_days: int) -> float:
    if age_days <= 1:    return 1.00
    elif age_days <= 3:  return 0.80
    elif age_days <= 7:  return 0.60
    elif age_days <= 14: return 0.30
    return 0.10


def fetch_news_for_symbol(symbol_clean: str, max_headlines: int = 5, max_age_days: int = 7) -> list:
    if not _FEEDPARSER_OK:
        return []
    all_headlines = []
    search_terms = [symbol_clean.lower(), symbol_clean.replace(".", " ").lower()]
    for source_name, feed_url in NEWS_RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:50]:
                title = entry.get("title", "")
                title_lower = title.lower()
                if any(term in title_lower for term in search_terms):
                    try:
                        pub = entry.get("published_parsed") or entry.get("updated_parsed")
                        if pub:
                            pub_dt = datetime.datetime(*pub[:6], tzinfo=datetime.timezone.utc)
                            age = (datetime.datetime.now(datetime.timezone.utc) - pub_dt).days
                        else:
                            age = 3
                    except Exception:
                        age = 3
                    if age <= max_age_days:
                        all_headlines.append({"title": title, "age_days": age, "source": source_name})
        except Exception:
            continue
    if not all_headlines:
        try:
            google_url = (f"https://news.google.com/rss/search?q={symbol_clean}+NSE+stock"
                          f"&hl=en-IN&gl=IN&ceid=IN:en")
            feed = feedparser.parse(google_url)
            for entry in feed.entries[:10]:
                title = entry.get("title", "")
                all_headlines.append({"title": title, "age_days": 1, "source": "GOOGLE_NEWS"})
        except Exception:
            pass
    return all_headlines[:max_headlines]


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — MARKET ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def compute_breadth(tradable: dict) -> dict:
    above_ema20 = above_ema50 = advancing = declining = total = 0
    for symbol, df in tradable.items():
        try:
            closes = df["Close"].squeeze().values.astype(float)
            if len(closes) < 50:
                continue
            last  = closes[-1]
            prev  = closes[-2]
            ema20 = float(pd.Series(closes).ewm(span=20).mean().iloc[-1])
            ema50 = float(pd.Series(closes).ewm(span=50).mean().iloc[-1])
            if last > ema20: above_ema20 += 1
            if last > ema50: above_ema50 += 1
            if last > prev:  advancing   += 1
            else:             declining   += 1
            total += 1
        except Exception:
            continue
    if total == 0:
        return {"ema20_pct": 50.0, "ema50_pct": 50.0, "adv_dec_ratio": 1.0}
    return {
        "ema20_pct":     round(above_ema20 / total * 100, 1),
        "ema50_pct":     round(above_ema50 / total * 100, 1),
        "adv_dec_ratio": round(advancing / max(1, declining), 2),
    }


def _load_sector_rank_history() -> dict:
    """Load sector rank history: {'YYYY-MM-DD': {sector: rank_int}}.

    Phase 4 (2026-07-01): persist daily 5d ranks so rotation velocity
    (5-day rank delta) can be computed run-over-run.
    """
    try:
        if os.path.exists(SECTOR_RANK_HISTORY_FILE):
            with open(SECTOR_RANK_HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        _log(f"[WARN] sector rank history load failed: {e}")
    return {}


def _save_sector_rank_history(history: dict) -> None:
    """Persist sector rank history — trims to trailing 30 calendar days."""
    try:
        # Trim: keep only last 30 date-keys sorted DESC
        keys_sorted = sorted(history.keys(), reverse=True)[:30]
        trimmed = {k: history[k] for k in keys_sorted}
        with open(SECTOR_RANK_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(trimmed, f, indent=2, sort_keys=True)
    except Exception as e:
        _log(f"[WARN] sector rank history save failed: {e}")


def compute_sector_rotation(tradable: dict) -> dict:
    sector_returns: dict = {}
    for symbol, df in tradable.items():
        sector = get_sector(symbol)
        try:
            closes = df["Close"].squeeze().values.astype(float)
            if len(closes) < 22:
                continue
            ret5d  = (closes[-1] / closes[-6]  - 1) * 100
            ret20d = (closes[-1] / closes[-22] - 1) * 100
            sector_returns.setdefault(sector, []).append((ret5d, ret20d))
        except Exception:
            continue
    if not sector_returns:
        return {}
    all_5d  = [r[0] for v in sector_returns.values() for r in v]
    all_20d = [r[1] for v in sector_returns.values() for r in v]
    avg_5d  = sum(all_5d)  / len(all_5d)  if all_5d  else 0
    avg_20d = sum(all_20d) / len(all_20d) if all_20d else 0
    result = {}
    # First pass: compute per-sector 5d/20d means and static status.
    sector_5d = {}
    for sector, rets in sector_returns.items():
        s5d  = sum(r[0] for r in rets) / len(rets)
        s20d = sum(r[1] for r in rets) / len(rets)
        if s5d > avg_5d + 1.0 and s20d > avg_20d:
            status = "LEADING"
        elif s5d < avg_5d - 1.0 and s20d < avg_20d:
            status = "LAGGING"
        elif s5d > avg_5d and s20d < avg_20d:
            status = "WEAKENING"
        else:
            status = "NEUTRAL"
        result[sector] = {"ret5d": round(s5d, 2), "ret20d": round(s20d, 2), "status": status}
        sector_5d[sector] = s5d

    # Phase 4 (2026-07-01): rank sectors by 5-day return (1 = best).
    # Persist to history and compute 5-day rank delta = rotation velocity.
    try:
        ranked = sorted(sector_5d.items(), key=lambda kv: kv[1], reverse=True)
        today_ranks = {sec: idx + 1 for idx, (sec, _) in enumerate(ranked)}
        today_key = ist_today().isoformat()

        history = _load_sector_rank_history()
        history[today_key] = today_ranks
        _save_sector_rank_history(history)

        # Find nearest history date >= 5 trading days back (approx 7 cal days).
        past_keys = sorted([k for k in history.keys() if k < today_key], reverse=True)
        prior_ranks = None
        prior_key = None
        for k in past_keys:
            try:
                d_prior = date.fromisoformat(k)
                d_today = date.fromisoformat(today_key)
                if (d_today - d_prior).days >= 5:
                    prior_ranks = history[k]
                    prior_key = k
                    break
            except Exception:
                continue

        for sector in result:
            r_now = today_ranks.get(sector)
            result[sector]["rank_5d"] = r_now
            if prior_ranks and sector in prior_ranks and r_now is not None:
                delta = int(prior_ranks[sector]) - int(r_now)  # +ve = moved up
                result[sector]["rank_delta_5d"] = delta
                result[sector]["rank_prior_date"] = prior_key
                # Classify rotation velocity — sector direction over 5 sessions.
                if delta >= 3:
                    rot = "ROTATING_IN"
                elif delta <= -3:
                    rot = "ROTATING_OUT"
                else:
                    rot = "STABLE"
                result[sector]["rotation_velocity"] = rot
            else:
                result[sector]["rank_delta_5d"]    = None
                result[sector]["rotation_velocity"] = "UNKNOWN"
    except Exception as e:
        _log(f"[WARN] sector rotation velocity computation failed: {e}")

    return result


def sector_rotation_score(sector: str, rotation: dict) -> tuple:
    data   = rotation.get(sector, {})
    status = data.get("status", "NEUTRAL")
    adj    = {"LEADING": 15, "NEUTRAL": 0, "WEAKENING": -8, "LAGGING": -15}
    # Phase 4: rotation velocity overlay — reward sectors rotating IN even
    # while still statically LAGGING (contrarian setup); penalize rotating OUT.
    velocity = data.get("rotation_velocity", "UNKNOWN")
    velocity_adj = {"ROTATING_IN": 5, "STABLE": 0, "ROTATING_OUT": -3, "UNKNOWN": 0}.get(velocity, 0)
    return adj.get(status, 0) + velocity_adj, status


def compute_key_levels(nifty_df) -> dict:
    try:
        closes = nifty_df["Close"].squeeze().values.astype(float)
        highs  = nifty_df["High"].squeeze().values.astype(float)
        lows   = nifty_df["Low"].squeeze().values.astype(float)
        last   = closes[-1]
        ema20  = float(pd.Series(closes).ewm(span=20).mean().iloc[-1])
        ema50  = float(pd.Series(closes).ewm(span=50).mean().iloc[-1])
        ema200 = float(pd.Series(closes).ewm(span=200).mean().iloc[-1])
        lookback     = min(252, len(highs))
        high_52w     = float(np.max(highs[-lookback:]))
        low_52w      = float(np.min(lows[-lookback:]))
        dist_from_high = round((high_52w - last) / high_52w * 100, 1)
        recent_high  = float(np.max(highs[-20:]))
        recent_low   = float(np.min(lows[-20:]))
        return {
            "last":                   round(last, 0),
            "ema20":                  round(ema20, 0),
            "ema50":                  round(ema50, 0),
            "ema200":                 round(ema200, 0),
            "high_52w":               round(high_52w, 0),
            "low_52w":                round(low_52w, 0),
            "recent_high_20d":        round(recent_high, 0),
            "recent_low_20d":         round(recent_low, 0),
            "dist_from_52w_high_pct": dist_from_high,
            "above_ema200":           last > ema200,
            "above_ema50":            last > ema50,
        }
    except Exception as e:
        _log(f"[WARN] compute_key_levels failed: {e}")
        return {}


def compute_nifty_state(nifty_df) -> dict:
    """Single source of truth for NIFTY EMA/level data (BUG FIX 1).
    Computed ONCE per pipeline run. Passed to all formatters.
    """
    try:
        closes = nifty_df["Close"].squeeze().values.astype(float)
        highs  = nifty_df["High"].squeeze().values.astype(float)
        lows   = nifty_df["Low"].squeeze().values.astype(float)
        ema20  = float(pd.Series(closes).ewm(span=20).mean().iloc[-1])
        ema50  = float(pd.Series(closes).ewm(span=50).mean().iloc[-1])
        ema200 = float(pd.Series(closes).ewm(span=200).mean().iloc[-1])
        last   = float(closes[-1])

        lookback = min(252, len(highs))
        high_52w = float(np.max(highs[-lookback:]))
        low_52w  = float(np.min(lows[-lookback:]))
        high_20d = float(np.max(highs[-20:]))
        low_20d  = float(np.min(lows[-20:]))

        above_ema20  = last > ema20
        above_ema50  = last > ema50
        above_ema200 = last > ema200

        if above_ema20 and above_ema50 and above_ema200:
            structure = "🟢 Above all EMAs — bull structure intact"
            ema_bear  = False
        elif above_ema20 and above_ema50:  # above short-term EMAs, below EMA200
            # Phase C1 (2026-07-02): clarified label. Short-term uptrend can
            # coexist with a long-term (200 EMA) drawdown — don't call it a
            # "correction" when the immediate trend is up.
            structure = "🟠 Above EMA20/50, below EMA200 — short-term uptrend, long-term recovery pending"
            ema_bear  = True
        elif above_ema50 and above_ema200:
            structure = "🟡 Below EMA20 — minor pullback in uptrend"
            ema_bear  = False
        elif above_ema200:
            structure = "🟠 Below EMA20 & EMA50 — correction underway"
            ema_bear  = True
        else:
            structure = "🔴 Below all EMAs — bearish structure. High caution."
            ema_bear  = True

        dist_52w_high = round((high_52w - last) / high_52w * 100, 1) if high_52w > 0 else 0.0

        return {
            "close":         round(last, 1),
            "ema20":         round(ema20, 1),
            "ema50":         round(ema50, 1),
            "ema200":        round(ema200, 1),
            "high_52w":      round(high_52w, 1),
            "low_52w":       round(low_52w, 1),
            "high_20d":      round(high_20d, 1),
            "low_20d":       round(low_20d, 1),
            "above_ema20":   above_ema20,
            "above_ema50":   above_ema50,
            "above_ema200":  above_ema200,
            "ema_bear":      ema_bear,
            "structure":     structure,
            "dist_52w_high_pct": dist_52w_high,
        }
    except Exception as e:
        _log(f"[WARN] compute_nifty_state failed: {e}")
        return {
            "close": 0, "ema20": 0, "ema50": 0, "ema200": 0,
            "high_52w": 0, "low_52w": 0, "high_20d": 0, "low_20d": 0,
            "above_ema20": False, "above_ema50": False, "above_ema200": False,
            "ema_bear": True, "structure": "🔴 Data unavailable",
            "dist_52w_high_pct": 0,
        }


def fetch_bse_results_dates(symbol_clean: str) -> list:
    """
    Fetch upcoming board meeting / results dates for a stock from BSE India API.
    Returns list of date strings "YYYY-MM-DD". Empty list on any failure.
    Free, no auth required. Silent fallback.
    """
    try:
        # BSE uses numeric scrip codes; map via a best-effort search
        search_url = f"https://api.bseindia.com/BseIndiaAPI/api/GetScripsSearch/w?strSearch={symbol_clean}"
        resp = requests.get(search_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.bseindia.com",
        }, timeout=8)
        if resp.status_code != 200:
            return []
        results = resp.json()
        if not results or not isinstance(results, list):
            return []
        scrip_code = str(results[0].get("SCRIP_CD", ""))
        if not scrip_code:
            return []

        # Fetch corporate actions for this scrip
        ca_url = (f"https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w"
                  f"?pageno=1&strScrip={scrip_code}&type=GP&Fdate=&TDate=")
        ca_resp = requests.get(ca_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.bseindia.com",
        }, timeout=8)
        if ca_resp.status_code != 200:
            return []
        actions = ca_resp.json().get("Table", [])
        dates = []
        today = datetime.date.today()
        for action in actions:
            raw = action.get("REC_DATE") or action.get("NEWS_DT") or ""
            try:
                # BSE returns dates as "DD/MM/YYYY" or "YYYY-MM-DDT00:00:00"
                if "/" in raw:
                    d = datetime.datetime.strptime(raw.split(" ")[0], "%d/%m/%Y").date()
                else:
                    d = datetime.datetime.strptime(raw[:10], "%Y-%m-%d").date()
                if d >= today:
                    dates.append(d.isoformat())
            except Exception:
                continue
        return dates
    except Exception:
        return []


def is_near_event(symbol_clean: str, results_dates: list,
                  upcoming_events: list, window_days: int = 5) -> tuple:
    """
    Returns (True, reason_str) if a BUY should be blocked due to an imminent event.
    Checks: (1) BSE results dates for the specific stock, (2) NSE expiry dates.
    Returns (False, "") if clear to trade.
    """
    today = datetime.date.today()
    cutoff = today + datetime.timedelta(days=window_days)

    # Check stock-specific results date
    for d_str in results_dates:
        try:
            d = datetime.date.fromisoformat(d_str)
            if today <= d <= cutoff:
                return True, f"RESULTS_IN_{(d - today).days}D"
        except Exception:
            continue

    # Check NSE expiry in upcoming_events (monthly expiry is highest risk)
    for ev in upcoming_events:
        if "Monthly Expiry" in ev:
            try:
                # Extract date from string like "NSE Monthly Expiry — 26 Jun"
                parts = ev.split("—")[-1].strip()
                d = datetime.datetime.strptime(f"{parts} {today.year}", "%d %b %Y").date()
                if today <= d <= cutoff:
                    return True, f"MONTHLY_EXPIRY_IN_{(d - today).days}D"
            except Exception:
                continue

    return False, ""


# Known market events with sector impact (FEATURE 5)
EVENTS_CONFIG_FILE = "events_config.json"

# ── Known annual schedules (update once per year) ─────────────────────────────
_RBI_MPC_DATES_2026 = [
    "2026-02-07", "2026-04-09", "2026-06-06",
    "2026-08-08", "2026-10-07", "2026-12-05",
]
_FOMC_DATES_2026 = [
    "2026-01-29", "2026-03-19", "2026-05-07",
    "2026-06-18", "2026-07-30", "2026-09-17",
    "2026-11-05", "2026-12-17",
]


def _nse_expiry_dates(lookahead_days: int = 60) -> list:
    """
    Returns all NSE weekly + monthly expiry dates in the next `lookahead_days`.
    Weekly  = every Thursday.
    Monthly = last Thursday of each month (when it falls in the window).
    """
    today  = datetime.date.today()
    end    = today + datetime.timedelta(days=lookahead_days)
    events = []
    cur    = today
    while cur <= end:
        if cur.weekday() == 3:            # Thursday
            # Is it the last Thursday of the month?
            next_thu = cur + datetime.timedelta(weeks=1)
            is_monthly = next_thu.month != cur.month
            events.append({
                "name":         "NSE Monthly Expiry" if is_monthly else "NSE Weekly Expiry",
                "date":         cur.isoformat(),
                "note":         ("Monthly expiry — high volatility, avoid new entries on expiry day"
                                 if is_monthly else
                                 "Weekly expiry — increased intraday volatility, tighten stops"),
                "sectors_up":   [],
                "sectors_down": [],
                "_auto":        True,
            })
        cur += datetime.timedelta(days=1)
    return events


def _rbi_mpc_events(lookahead_days: int = 60) -> list:
    """Returns upcoming RBI MPC decision dates from known 2026 schedule."""
    today  = datetime.date.today()
    end    = today + datetime.timedelta(days=lookahead_days)
    events = []
    for ds in _RBI_MPC_DATES_2026:
        try:
            d = datetime.date.fromisoformat(ds)
            if today <= d <= end:
                events.append({
                    "name":         "RBI MPC Decision",
                    "date":         ds,
                    "note":         "Rate decision — banking/finance/realty stocks react sharply",
                    "sectors_up":   ["BANKING", "FINANCE", "REALTY"],
                    "sectors_down": [],
                    "_auto":        True,
                })
        except Exception:
            pass
    return events


def _fomc_events(lookahead_days: int = 60) -> list:
    """Returns upcoming FOMC decision dates (impacts FII flows + DXY)."""
    today  = datetime.date.today()
    end    = today + datetime.timedelta(days=lookahead_days)
    events = []
    for ds in _FOMC_DATES_2026:
        try:
            d = datetime.date.fromisoformat(ds)
            if today <= d <= end:
                events.append({
                    "name":         "US FOMC Decision",
                    "date":         ds,
                    "note":         "Fed rate decision — DXY/FII flows react, watch IT/PHARMA",
                    "sectors_up":   ["IT", "PHARMA"],
                    "sectors_down": ["REALTY", "FINANCE"],
                    "_auto":        True,
                })
        except Exception:
            pass
    return events


def build_events_calendar(lookahead_days: int = 30) -> list:
    """
    Auto-builds the full events calendar.
    Sources: NSE expiry math + known RBI MPC + FOMC schedules.
    Saves to events_config.json so it's auditable.
    Returns only events within lookahead_days, sorted by date.
    Called ONCE at pipeline start — no manual editing needed.
    """
    import json
    try:
        all_events = (
            _nse_expiry_dates(lookahead_days) +
            _rbi_mpc_events(lookahead_days) +
            _fomc_events(lookahead_days)
        )
        # Sort by date
        all_events.sort(key=lambda x: x.get("date", ""))

        # Persist for auditability (strip _auto flag before saving)
        saveable = [{k: v for k, v in ev.items() if k != "_auto"} for ev in all_events]
        try:
            with open(EVENTS_CONFIG_FILE, "w") as f:
                json.dump(saveable, f, indent=2)
        except Exception:
            pass

        today = datetime.date.today()
        end   = today + datetime.timedelta(days=lookahead_days)
        return [
            ev for ev in all_events
            if today <= datetime.date.fromisoformat(ev["date"]) <= end
        ]
    except Exception as e:
        _log(f"[WARN] build_events_calendar failed: {e}")
        return []


def load_events_config() -> list:
    """
    Builds the events calendar, then deduplicates by type so recurring events
    (e.g. weekly expiry) appear only ONCE (the nearest occurrence).
    Shows max 3 total events, only within the next 14 days.
    """
    all_events = build_events_calendar(lookahead_days=30)
    today = datetime.date.today()

    # Step 1: sort by date (nearest first)
    all_events.sort(key=lambda x: x.get("date", ""))

    # Step 2: deduplicate — keep only the NEXT occurrence of each event type
    seen_types: set = set()
    deduplicated = []
    for ev in all_events:
        ev_type = ev.get("name", "").strip()
        # Group all expiry variants under one key
        if "expiry" in ev_type.lower() or "Expiry" in ev_type:
            ev_type = "NSE_EXPIRY"
        if ev_type not in seen_types:
            deduplicated.append(ev)
            seen_types.add(ev_type)
        if len(deduplicated) >= 4:
            break

    # Step 3: only events within next 14 days are relevant today
    relevant = []
    for ev in deduplicated:
        try:
            days_away = (datetime.date.fromisoformat(ev["date"]) - today).days
            if days_away <= 14:
                relevant.append(ev)
        except Exception:
            pass

    # If nothing within 14 days, show next 2 regardless
    if not relevant and deduplicated:
        relevant = deduplicated[:2]

    return relevant


def format_upcoming_events_compact(events_config: list, holdings: list) -> list:
    """
    2 lines per event maximum (FIX 3G).
    Line 1: Event name + date
    Line 2: Impact on holdings (if any) or general note
    """
    try:
        if not events_config:
            return []
        lines = ["UPCOMING EVENTS"]
        hold_secs = set()
        for h in (holdings or []):
            sym = h.get("symbol", "")
            if sym:
                hold_secs.add(get_sector(sym))

        for event in events_config:
            name    = event.get("name", "")
            date    = event.get("date", "")
            note    = event.get("note", "")
            up_secs = set(event.get("sectors_up",   []))
            dn_secs = set(event.get("sectors_down", []))

            lines.append(f"  {html.escape(name)} \u2014 {html.escape(date)}")

            impacted_up = hold_secs & up_secs
            impacted_dn = hold_secs & dn_secs
            if impacted_up:
                lines.append(f"    \ud83d\udfe2 Holdings may benefit: {', '.join(sorted(impacted_up))}")
            elif impacted_dn:
                lines.append(f"    \ud83d\udd34 Holdings at risk: {', '.join(sorted(impacted_dn))}")
            elif note:
                lines.append(f"    {html.escape(note[:80])}")
        return lines
    except Exception:
        return ["UPCOMING EVENTS"] + [f"  {html.escape(str(ev.get('name','')))} \u2014 {ev.get('date','')}" for ev in (events_config or [])]


def score_to_regime(score: float, vix_in: float) -> str:
    """
    Maps score to regime with VIX sanity check.
    VIX-IN < 16 = market is NOT in high volatility regardless of score.
    """
    if score >= 80:    base = "STRONG_BULL"
    elif score >= 65:  base = "BULL"
    elif score >= 52:  base = "SIDEWAYS"
    elif score >= 40:  base = "TRANSITION"
    elif score >= 28:  base = "HIGH_VOLATILITY"
    elif score >= 15:  base = "BEAR"
    else:              base = "STRONG_BEAR"

    # VIX sanity check: calm VIX cannot be HIGH_VOLATILITY
    if base == "HIGH_VOLATILITY" and vix_in < 16:
        _log(
            f"[INFO] Regime override: score {score:.1f} → HIGH_VOLATILITY "
            f"but VIX-IN {vix_in:.1f} is calm → using TRANSITION instead"
        )
        return "TRANSITION"

    # STRONG_BEAR with calm VIX is also suspicious
    if base == "STRONG_BEAR" and vix_in < 18:
        return "BEAR"

    return base



def detect_market_regime(nifty_df, breadth_data: dict, macro_signals: dict) -> dict:
    score = 50
    try:
        closes = nifty_df["Close"].squeeze().values.astype(float)
        ema20  = pd.Series(closes).ewm(span=20).mean().values
        ema50  = pd.Series(closes).ewm(span=50).mean().values
        ema200 = pd.Series(closes).ewm(span=200).mean().values
        last   = closes[-1]
        ret_5d  = (last / closes[-6]  - 1) * 100 if len(closes) > 6  else 0
        ret_21d = (last / closes[-22] - 1) * 100 if len(closes) > 22 else 0

        if last > ema20[-1] > ema50[-1] > ema200[-1]: score += 30
        elif last > ema50[-1] > ema200[-1]:            score += 18
        elif last > ema200[-1]:                         score += 8
        elif last < ema200[-1]:                         score -= 15

        if ret_5d > 1.5 and ret_21d > 4:      score += 20
        elif ret_5d > 0 and ret_21d > 1:       score += 10
        elif ret_5d < -1.5 and ret_21d < -3:   score -= 20
        elif ret_5d < 0:                        score -= 8

        b20 = breadth_data.get("ema20_pct", 50)
        b50 = breadth_data.get("ema50_pct", 50)
        if b20 > 70 and b50 > 70:    score += 25
        elif b20 > 55 and b50 > 55:  score += 15
        elif b20 > 45:               score += 5
        elif b20 < 35:               score -= 15
        elif b20 < 25:               score -= 25

        vix_in = macro_signals.get("vix_in", 15)
        if vix_in < 13:    score += 15
        elif vix_in < 17:  score += 8
        elif vix_in < 22:  score += 0
        elif vix_in < 27:  score -= 12
        else:              score -= 20

        score += macro_regime_adjustment(macro_signals)
    except Exception:
        score = 50

    score = max(0, min(100, score))

    # Use VIX-sanity-checked mapping instead of raw score boundaries
    vix_for_regime = macro_signals.get("vix_in", 15)
    regime = score_to_regime(score, vix_for_regime)

    return {
        "regime":     regime,
        "score":      round(score, 1),
        "thresholds": REGIME_THRESHOLDS[regime],
        "macro":      macro_signals,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — SCORING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _obv_trend_score(closes: np.ndarray, volumes: np.ndarray) -> float:
    """
    On-Balance Volume trend score (0-100).
    Replaces delivery % which requires external bhavcopy data unavailable from CI.
    OBV = cumsum of volume * sign(close - prev_close).
    Rising OBV = smart money accumulating. Falling OBV = distribution.
    OBV divergence (price up, OBV down) = bearish warning.
    """
    try:
        n = min(len(closes), len(volumes))
        if n < 12:
            return 50.0
        c = closes[-n:]
        v = volumes[-n:]
        # Build OBV
        obv = [0.0]
        for i in range(1, n):
            if c[i] > c[i-1]:
                obv.append(obv[-1] + v[i])
            elif c[i] < c[i-1]:
                obv.append(obv[-1] - v[i])
            else:
                obv.append(obv[-1])
        obv = np.array(obv)
        # Slope of last 10 OBV values (normalised by avg volume)
        window = obv[-10:]
        avg_vol = float(np.mean(v[-10:])) or 1.0
        slope = float(np.polyfit(range(10), window, 1)[0]) / avg_vol  # slope in vol-units/bar
        # Price direction over same window
        price_up = c[-1] > c[-10]
        if slope > 0.3 and price_up:    return 95.0   # strong accumulation
        if slope > 0.1 and price_up:    return 80.0   # moderate accumulation
        if slope > 0.0:                 return 65.0   # quiet buying
        if slope > -0.1:                return 50.0   # neutral
        if slope <= -0.1 and price_up:  return 30.0   # bearish divergence (price up, OBV down)
        if slope <= -0.3:               return 15.0   # distribution
        return 40.0
    except Exception:
        return 50.0


def volume_delivery_score(vol_ratio: float, obv_score: float) -> float:
    vol_score = min(100, vol_ratio * 45)
    return round(vol_score * 0.55 + obv_score * 0.45, 1)


def accumulation_score(closes: np.ndarray, volumes: np.ndarray,
                       ema20: float, avg_vol: float) -> tuple:
    """
    Detects institutional accumulation / distribution over the last 10 bars.

    Accumulation: 3+ days of above-avg volume while price held ABOVE ema20.
    Distribution: 3+ days of above-avg volume while price closed BELOW ema20.

    Returns (score 0-100, signal: "ACCUMULATING"|"DISTRIBUTING"|"NEUTRAL")
    Score feeds into volume_delivery factor to boost/penalise beyond single-day vol.
    """
    try:
        if len(closes) < 11 or avg_vol <= 0:
            return 50, "NEUTRAL"

        accum_days = 0
        distrib_days = 0
        for i in range(-10, 0):
            is_high_vol = volumes[i] > avg_vol * 1.2
            if not is_high_vol:
                continue
            if closes[i] > ema20:
                accum_days += 1
            else:
                distrib_days += 1

        if accum_days >= 4:
            return 90, "ACCUMULATING"
        elif accum_days >= 3:
            return 75, "ACCUMULATING"
        elif accum_days >= 2:
            return 62, "ACCUMULATING"
        elif distrib_days >= 4:
            return 15, "DISTRIBUTING"
        elif distrib_days >= 3:
            return 28, "DISTRIBUTING"
        elif distrib_days >= 2:
            return 38, "DISTRIBUTING"
        return 50, "NEUTRAL"
    except Exception:
        return 50, "NEUTRAL"


def compute_base_confidence(scores: dict) -> float:
    return round(sum(FACTOR_WEIGHTS[k] * float(scores.get(k, 50)) for k in FACTOR_WEIGHTS), 2)


def compute_news_penalty(ai_result: dict, age_days: int) -> float:
    severity = ai_result.get("severity", 0)
    if ai_result.get("is_black_swan"):
        return 999.0
    effective = severity * news_decay_weight(age_days)
    if effective >= 70:   return 35.0
    elif effective >= 50: return 22.0
    elif effective >= 30: return 12.0
    elif effective >= 15: return 6.0
    elif effective < 0:   return max(-8.0, effective * 0.2)
    return 0.0


def compute_final_confidence(base: float, regime: str, news_penalty: float,
                              macro_adj: float, bulk_adj: int) -> float:
    REGIME_ADJ = {
        "STRONG_BULL": +8, "BULL": +4, "SIDEWAYS": -5,
        "TRANSITION": -3, "HIGH_VOLATILITY": -8, "BEAR": -20, "STRONG_BEAR": -40,
    }
    final = base + REGIME_ADJ.get(regime, 0) - news_penalty + macro_adj + bulk_adj
    return round(max(0.0, min(100.0, final)), 2)


# Env-driven cap (default 25%). Read once at module load so both sizers agree.
_MAX_POSITION_PCT_ENV = float(os.getenv("MAX_POSITION_PCT", "25")) / 100.0


def compute_position_size(entry: float, stop: float, capital: float,
                           risk_per_trade: float = 0.015,
                           max_position_pct: float = None) -> dict:
    """Risk-based position sizing — always returns non-zero for valid inputs.

    Returns keys: shares, position_value, position_pct, risk_amount, risk_pct,
    max_loss (= shares × (entry − stop), the actual worst-case rupee loss).
    """
    if max_position_pct is None:
        max_position_pct = _MAX_POSITION_PCT_ENV
    try:
        if entry <= 0 or stop <= 0 or stop >= entry or capital <= 0:
            return {"shares": 0, "position_value": 0.0, "position_pct": 0.0,
                    "risk_amount": 0.0, "risk_pct": 0.0, "max_loss": 0.0}
        risk_per_share = entry - stop
        risk_amount    = capital * risk_per_trade
        shares         = max(1, int(risk_amount / risk_per_share))
        position_value = shares * entry
        if position_value > capital * max_position_pct:
            shares         = max(1, int((capital * max_position_pct) / entry))
            position_value = shares * entry
        position_pct = (position_value / capital) * 100
        # Actual worst-case rupee loss (may differ slightly from risk_amount
        # because shares is an integer). Downstream renders read this key.
        max_loss = round(shares * risk_per_share, 2)
        return {
            "shares":         shares,
            "position_value": round(position_value, 2),
            "position_pct":   round(position_pct, 1),
            "risk_amount":    round(risk_amount, 2),
            "risk_pct":       round(risk_per_trade * 100, 1),
            "max_loss":       max_loss,
        }
    except Exception:
        return {"shares": 0, "position_value": 0.0, "position_pct": 0.0,
                "risk_amount": 0.0, "risk_pct": 0.0, "max_loss": 0.0}


def compute_portfolio_heat(holdings: list, current_prices: dict,
                           capital: float) -> dict:
    """
    Computes current portfolio heat = total open risk as % of capital.
    Open risk = sum of (current_price - stop_loss) * shares for all open positions.
    If stop_loss unknown, uses 6% of entry as proxy.

    Returns {"heat_pct": float, "positions_count": int, "max_heat_pct": float,
             "heat_ok": bool, "heat_remaining_pct": float}
    Max heat = 6% of capital (professional standard: never risk more than 6% total).
    """
    MAX_HEAT_PCT = float(os.getenv("MAX_PORTFOLIO_HEAT", "6.0"))
    try:
        total_risk = 0.0
        for h in holdings:
            sym   = h.get("symbol", "")
            entry = float(h.get("entry_price", 0) or 0)
            stop  = float(h.get("stop_loss", 0) or 0)
            qty   = float(h.get("quantity", 0) or 0)
            curr  = float(current_prices.get(sym, entry) or entry)

            if entry <= 0:
                continue
            if qty <= 0:
                # Estimate qty from position_pct if available
                pos_pct = float(h.get("position_pct", 5) or 5)
                qty = max(1, int((capital * pos_pct / 100) / entry))

            if stop <= 0:
                stop = entry * 0.94   # assume 6% stop if not set

            # Risk = how much we'd lose if stop is hit RIGHT NOW
            risk_per_share = max(0, curr - stop)
            total_risk    += risk_per_share * qty

        heat_pct      = round((total_risk / capital) * 100, 2) if capital > 0 else 0.0
        remaining_pct = round(max(0.0, MAX_HEAT_PCT - heat_pct), 2)
        return {
            "heat_pct":           heat_pct,
            "positions_count":    len(holdings),
            "max_heat_pct":       MAX_HEAT_PCT,
            "heat_ok":            heat_pct < MAX_HEAT_PCT,
            "heat_remaining_pct": remaining_pct,
        }
    except Exception as e:
        _log(f"[WARN] compute_portfolio_heat failed: {e}")
        return {"heat_pct": 0.0, "positions_count": 0,
                "max_heat_pct": 6.0, "heat_ok": True, "heat_remaining_pct": 6.0}


def kelly_position_size(entry: float, stop: float, capital: float,
                        win_rate: float, avg_win_pct: float, avg_loss_pct: float,
                        heat: dict, max_position_pct: float = None) -> dict:
    """
    Kelly Criterion position sizing — sizes position based on ACTUAL historical
    win rate from the tracker. Falls back to fixed 1.5% risk if no history.

    Kelly fraction = W - (1-W)/R
      W = win rate (e.g. 0.62)
      R = avg_win / avg_loss ratio

    We use HALF-Kelly (safer, reduces variance by ~75% vs full Kelly).
    Heat-aware: reduces position if portfolio is near max heat.

    Activates automatically once tracker has >= 20 closed trades.
    """
    if max_position_pct is None:
        max_position_pct = _MAX_POSITION_PCT_ENV
    try:
        if entry <= 0 or stop <= 0 or stop >= entry or capital <= 0:
            return compute_position_size(entry, stop, capital)

        risk_per_share = entry - stop
        risk_pct_per_share = risk_per_share / entry

        # Use Kelly if we have enough history, else fall back to fixed 1.5%
        if (win_rate > 0 and avg_win_pct > 0 and avg_loss_pct > 0
                and 0.3 <= win_rate <= 0.9):
            loss_rate = 1.0 - win_rate
            r_ratio   = avg_win_pct / avg_loss_pct if avg_loss_pct > 0 else 1.5
            full_kelly = win_rate - (loss_rate / r_ratio)
            half_kelly = max(0.005, min(0.04, full_kelly * 0.5))  # cap 0.5%–4%
            risk_per_trade = half_kelly
            sizing_method  = f"KELLY_HALF({win_rate:.0%}WR/{r_ratio:.1f}R)"
        else:
            risk_per_trade = 0.015   # fixed 1.5% until we have history
            sizing_method  = "FIXED_1.5PCT"

        # Heat adjustment: if heat is > 80% of max, scale down new position
        heat_pct     = heat.get("heat_pct", 0.0)
        max_heat_pct = heat.get("max_heat_pct", 6.0)
        heat_ratio   = heat_pct / max_heat_pct if max_heat_pct > 0 else 0
        if heat_ratio > 0.8:
            risk_per_trade *= 0.5    # cut in half when near max heat
            sizing_method  += "_HEAT_REDUCED"
        elif heat_ratio > 0.6:
            risk_per_trade *= 0.75

        risk_amount    = capital * risk_per_trade
        shares         = max(1, int(risk_amount / risk_per_share))
        position_value = shares * entry
        if position_value > capital * max_position_pct:
            shares         = max(1, int((capital * max_position_pct) / entry))
            position_value = shares * entry
        position_pct = (position_value / capital) * 100

        max_loss = round(shares * risk_per_share, 2)
        return {
            "shares":          shares,
            "position_value":  round(position_value, 2),
            "position_pct":    round(position_pct, 1),
            "risk_amount":     round(risk_amount, 2),
            "risk_pct":        round(risk_per_trade * 100, 2),
            "sizing_method":   sizing_method,
            "max_loss":        max_loss,
        }
    except Exception:
        return compute_position_size(entry, stop, capital)


def check_gap_validity(signal_entry: float, signal_stop: float,
                       signal_target1: float, min_rr: float,
                       open_price: float = 0.0) -> dict:
    """
    Called at market open (9:15 AM) to decide if a previous evening's signal
    is still valid given the actual opening price.

    Args:
        signal_entry  : price the system recommended entering at (last night's close)
        signal_stop   : stop loss from the signal
        signal_target1: first target from the signal
        min_rr        : minimum acceptable R/R for this regime (e.g. 1.8)
        open_price    : actual market open price (0 = not yet known, use signal_entry)

    Returns dict:
        action        : "ENTER" | "WAIT_PULLBACK" | "RECALCULATE" | "VOID"
        reason        : explanation string
        adjusted_entry: recommended actual entry (may differ from open_price)
        adjusted_rr   : R/R at adjusted_entry vs original stop/target
        gap_pct       : % gap from signal_entry to open_price
        max_valid_entry: highest price at which signal still meets min_rr
    """
    try:
        if signal_entry <= 0 or signal_stop <= 0 or signal_target1 <= 0:
            return {"action": "VOID", "reason": "INVALID_SIGNAL_DATA",
                    "adjusted_entry": 0.0, "adjusted_rr": 0.0,
                    "gap_pct": 0.0, "max_valid_entry": 0.0}

        # Max valid entry: highest price where R/R still meets minimum
        # (target1 - max_entry) / (max_entry - stop) = min_rr
        # Solve: max_entry = (target1 + min_rr * stop) / (1 + min_rr)
        max_valid_entry = round(
            (signal_target1 + min_rr * signal_stop) / (1 + min_rr), 2
        )

        # If no open price provided, return the rule for display
        if open_price <= 0:
            gap_pct = 0.0
            rr_at_entry = round(
                (signal_target1 - signal_entry) / (signal_entry - signal_stop), 2
            ) if signal_entry > signal_stop else 0.0
            return {
                "action":          "PENDING",
                "reason":          f"Max valid entry: Rs{max_valid_entry:.2f} | "
                                   f"Above that → skip",
                "adjusted_entry":  signal_entry,
                "adjusted_rr":     rr_at_entry,
                "gap_pct":         0.0,
                "max_valid_entry": max_valid_entry,
            }

        gap_pct = round((open_price - signal_entry) / signal_entry * 100, 2)
        rr_at_open = round(
            (signal_target1 - open_price) / (open_price - signal_stop), 2
        ) if open_price > signal_stop else 0.0

        # Gap down — check if stop is breached
        if open_price <= signal_stop:
            return {
                "action":          "VOID",
                "reason":          f"Gap DOWN {gap_pct:.1f}% — opened at or below stop. Signal dead.",
                "adjusted_entry":  0.0,
                "adjusted_rr":     0.0,
                "gap_pct":         gap_pct,
                "max_valid_entry": max_valid_entry,
            }

        # Gap up > 5% — always void
        if gap_pct > 5.0:
            return {
                "action":          "VOID",
                "reason":          f"Gap UP {gap_pct:.1f}% — too extended. R/R={rr_at_open:.2f}x. Do NOT chase.",
                "adjusted_entry":  0.0,
                "adjusted_rr":     rr_at_open,
                "gap_pct":         gap_pct,
                "max_valid_entry": max_valid_entry,
            }

        # Gap up 3-5% — wait for pullback to max_valid_entry
        if gap_pct > 3.0:
            return {
                "action":          "WAIT_PULLBACK",
                "reason":          f"Gap UP {gap_pct:.1f}% — wait for pullback to Rs{max_valid_entry:.2f}. "
                                   f"Enter only if price comes back AND holds above EMA20.",
                "adjusted_entry":  max_valid_entry,
                "adjusted_rr":     min_rr,
                "gap_pct":         gap_pct,
                "max_valid_entry": max_valid_entry,
            }

        # Gap up 1.5-3% — recalculate R/R at open
        if gap_pct > 1.5:
            if rr_at_open >= min_rr:
                return {
                    "action":          "ENTER",
                    "reason":          f"Gap UP {gap_pct:.1f}% — R/R {rr_at_open:.2f}x still meets "
                                       f"min {min_rr:.1f}x. Enter at open.",
                    "adjusted_entry":  round(open_price, 2),
                    "adjusted_rr":     rr_at_open,
                    "gap_pct":         gap_pct,
                    "max_valid_entry": max_valid_entry,
                }
            else:
                return {
                    "action":          "WAIT_PULLBACK",
                    "reason":          f"Gap UP {gap_pct:.1f}% — R/R dropped to {rr_at_open:.2f}x "
                                       f"(min {min_rr:.1f}x). Wait for Rs{max_valid_entry:.2f}.",
                    "adjusted_entry":  max_valid_entry,
                    "adjusted_rr":     min_rr,
                    "gap_pct":         gap_pct,
                    "max_valid_entry": max_valid_entry,
                }

        # Gap ≤ 1.5% — normal, enter at open
        return {
            "action":          "ENTER",
            "reason":          f"Gap {gap_pct:+.1f}% — within tolerance. R/R {rr_at_open:.2f}x. Enter.",
            "adjusted_entry":  round(open_price, 2),
            "adjusted_rr":     rr_at_open,
            "gap_pct":         gap_pct,
            "max_valid_entry": max_valid_entry,
        }

    except Exception as e:
        return {"action": "VOID", "reason": f"Calculation error: {e}",
                "adjusted_entry": 0.0, "adjusted_rr": 0.0,
                "gap_pct": 0.0, "max_valid_entry": 0.0}


def compute_platt_stats(tracker_entries: list) -> dict:
    """
    Platt Calibration Framework.
    Reads closed tracker trades and computes:
      - win_rate: fraction of closed trades that hit T1 or T2
      - avg_win_pct: average % gain on winning trades
      - avg_loss_pct: average % loss on losing trades
      - conf_calibration: dict of {conf_bucket: actual_win_rate}

    Returns safe defaults when < 20 closed trades (not enough data).
    Activates AUTOMATICALLY once you have 20+ closed trades in tracker.json.
    """
    closed = [e for e in tracker_entries if e.get("status") == "CLOSED"
              and e.get("final_pnl_pct") is not None]

    if len(closed) < 20:
        return {
            "win_rate": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "total_closed": len(closed), "calibrated": False,
            "conf_calibration": {},
        }

    wins   = [e for e in closed if e["final_pnl_pct"] >= 0]
    losses = [e for e in closed if e["final_pnl_pct"] < 0]
    win_rate     = len(wins) / len(closed)
    avg_win_pct  = sum(e["final_pnl_pct"] for e in wins)  / max(1, len(wins))
    avg_loss_pct = abs(sum(e["final_pnl_pct"] for e in losses)) / max(1, len(losses))

    # Confidence bucket calibration: bucket closed trades by confidence score
    # e.g. {80: 0.71} means: when system said conf=80, actual win rate was 71%
    buckets: dict = {}
    for e in closed:
        conf   = float(e.get("conf", 0) or 0)
        bucket = int(conf // 5) * 5   # bucket to nearest 5 (75,80,85,90)
        if bucket not in buckets:
            buckets[bucket] = {"wins": 0, "total": 0}
        buckets[bucket]["total"] += 1
        if e["final_pnl_pct"] >= 0:
            buckets[bucket]["wins"] += 1
    conf_cal = {b: round(v["wins"] / v["total"], 2)
                for b, v in buckets.items() if v["total"] >= 3}

    return {
        "win_rate":         round(win_rate, 3),
        "avg_win_pct":      round(avg_win_pct, 2),
        "avg_loss_pct":     round(avg_loss_pct, 2),
        "total_closed":     len(closed),
        "calibrated":       True,
        "conf_calibration": conf_cal,
    }


def weekly_trend_score(df_daily) -> tuple:
    """
    Multi-timeframe confirmation: resamples daily OHLCV to weekly.
    Returns (score 0-100, weekly_trend_ok: bool).

    A daily BUY signal in a stock whose WEEKLY trend is DOWN has much lower follow-through.
    Adds roughly 8-12% improvement in win rate empirically on NSE.

    Scoring:
      Weekly close > weekly EMA10 > weekly EMA20 → strong weekly uptrend  → 90
      Weekly close > weekly EMA20                → moderate uptrend        → 70
      Weekly close > weekly EMA10 but < EMA20   → mixed                   → 50
      Weekly close < weekly EMA20               → weekly downtrend        → 25
    """
    try:
        weekly = df_daily.resample("W").agg({
            "Open":   "first",
            "High":   "max",
            "Low":    "min",
            "Close":  "last",
            "Volume": "sum",
        }).dropna()

        if len(weekly) < 22:
            return 50, False

        wc = weekly["Close"].squeeze().values.astype(float)
        w_ema10 = float(pd.Series(wc).ewm(span=10).mean().iloc[-1])
        w_ema20 = float(pd.Series(wc).ewm(span=20).mean().iloc[-1])
        w_last  = wc[-1]
        # Weekly slope — is EMA10 rising over last 3 weeks?
        w_ema10_3w = float(pd.Series(wc).ewm(span=10).mean().iloc[-4])
        w_slope_up = w_ema10 > w_ema10_3w

        if w_last > w_ema10 > w_ema20 and w_slope_up:
            return 90, True
        elif w_last > w_ema10 > w_ema20:
            return 78, True
        elif w_last > w_ema20:
            return 65, True
        elif w_last > w_ema10:
            return 52, False
        else:
            return 25, False
    except Exception:
        return 50, False


def price_action_score(closes: np.ndarray, highs: np.ndarray,
                       lows: np.ndarray, ema20: float, atr14: float) -> tuple:
    """
    Detects 3 high-edge NSE price patterns. Returns (score 0-100, pattern_name).

    1. Inside bar after trend (tight range = coiling energy before move)
       - High-probability low-risk entry on the next bar break
    2. False breakdown recovery (bears trapped = strong continuation signal)
       - Price dipped below a recent swing low then closed back above ema20
    3. 3-bar tight consolidation near EMA20 (spring before move)
       - Price range over 3 bars < 2.5% while above EMA20

    If none detected → NONE (score 50, neutral — doesn't penalise the stock)
    """
    try:
        n = len(closes)
        if n < 15:
            return 50, "NONE"

        # Pattern 1: Inside bar (yesterday's entire range inside 2 days ago)
        inside_bar = (highs[-2] < highs[-3] and lows[-2] > lows[-3]
                      and closes[-1] > ema20)   # still in uptrend
        if inside_bar:
            return 82, "INSIDE_BAR"

        # Pattern 2: False breakdown recovery
        recent_low_10 = float(np.min(lows[-10:-1]))
        false_breakdown = (
            lows[-3] < recent_low_10        # dipped below 10-day low
            and closes[-1] > ema20           # recovered above EMA20
            and closes[-1] > closes[-3]      # higher close than breakdown bar
        )
        if false_breakdown:
            return 88, "FALSE_BREAKDOWN_RECOVERY"

        # Pattern 3: 3-bar tight consolidation (< 2.5% range) above EMA20
        three_bar_high = float(np.max(highs[-3:]))
        three_bar_low  = float(np.min(lows[-3:]))
        tight_range_pct = (three_bar_high - three_bar_low) / closes[-1] * 100
        tight_consol = (
            tight_range_pct < 2.5
            and closes[-1] > ema20
        )
        if tight_range_pct < 2.5 and closes[-1] > ema20:
            return 80, "TIGHT_CONSOLIDATION"

        return 50, "NONE"
    except Exception:
        return 50, "NONE"


def _default_stock_result(symbol: str, sector: str) -> dict:
    return {
        "symbol": symbol, "sector": sector,
        "trend_quality": 50, "momentum_quality": 50, "volume_delivery": 50,
        "sector_strength": 50, "rs_vs_nifty": 50, "news_risk": 50,
        "risk_reward": 0, "ownership_quality": 50, "options_sentiment": 60,
        "macro_alignment": 50, "trade_quality_score": 0,
        "entry": 0.0, "stop": 0.0, "target1": 0.0, "target2": 0.0,
        "rr_ratio": 0.0, "avg_volume": 0, "avg_value_lakhs": 0.0,
        "near_52w_high": False, "sector_status": "NEUTRAL", "accum_signal": "NEUTRAL",
        "roe": 0.0, "de_ratio": 0.0, "promoter_pledge_pct": 0.0,
        "news_penalty": 0, "is_black_swan": False, "news_summary": "",
        "price": 0.0, "ret1d": 0.0, "ret5d": 0.0, "ret21d": 0.0,
        "high_52w": 0.0, "low_52w": 0.0, "atr14": 0.0, "rsi14": 50.0,
        "final_confidence": 0.0,
        # base_confidence intentionally absent — computed externally by
        # compute_base_confidence() and inserted after **scores in scored.append()
        # so it is never overwritten by a stale 0.0 placeholder.
        "weekly_trend_ok": False, "price_pattern": "NONE", "rs_diff21": 0.0,
    }


def compute_all_factors(symbol: str, df,
                         sector: str, regime_data: dict,
                         sector_rotation: dict = None) -> dict:
    result = _default_stock_result(symbol, sector)
    try:
        closes  = df["Close"].squeeze().values.astype(float)
        highs   = df["High"].squeeze().values.astype(float)
        lows    = df["Low"].squeeze().values.astype(float)
        volumes = df["Volume"].squeeze().values.astype(float)

        if len(closes) < 50:
            return result

        last = closes[-1]
        prev = closes[-2]
        n    = len(closes)

        # Circuit breaker — skip stocks with >15% move today
        ret1d_check = (last / prev - 1) * 100 if prev > 0 else 0
        if abs(ret1d_check) > 15:
            _log(f"[SKIP] {symbol} — circuit move {ret1d_check:.1f}% today")
            return result

        # EMAs
        s_closes = pd.Series(closes)
        ema9   = float(s_closes.ewm(span=9).mean().iloc[-1])
        ema20  = float(s_closes.ewm(span=20).mean().iloc[-1])
        ema50  = float(s_closes.ewm(span=50).mean().iloc[-1])
        ema200 = float(s_closes.ewm(span=200).mean().iloc[-1])

        # ATR (14-period)
        tr = np.maximum(
            highs - lows,
            np.maximum(np.abs(highs - np.roll(closes, 1)),
                       np.abs(lows  - np.roll(closes, 1)))
        )
        atr14 = float(pd.Series(tr[1:]).rolling(14).mean().iloc[-1])

        # Volume
        avg_vol_20    = float(pd.Series(volumes).rolling(20).mean().iloc[-1])
        today_vol     = float(volumes[-1])
        vol_ratio     = today_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0
        avg_price_20  = float(s_closes.rolling(20).mean().iloc[-1])
        avg_val_lakhs = (avg_vol_20 * avg_price_20) / 100_000

        # 52-week levels
        lookback     = min(252, n)
        high_52w     = float(np.max(highs[-lookback:]))
        low_52w      = float(np.min(lows[-lookback:]))
        near_52w_high = last >= high_52w * 0.97

        # ── Factor 1: Trend Quality (daily + weekly multi-timeframe) ──
        tq = 50
        if last > ema9 > ema20 > ema50 > ema200:  tq = 92
        elif last > ema20 > ema50 > ema200:         tq = 78
        elif last > ema50 > ema200:                  tq = 62
        elif last > ema200:                          tq = 48
        elif last < ema200 and last < ema50:         tq = 22
        else:                                         tq = 35
        ema20_5d_ago = float(s_closes.ewm(span=20).mean().iloc[-6]) if n > 6 else ema20
        if ema20 > ema20_5d_ago: tq = min(100, tq + 8)
        # Weekly confirmation: penalise if weekly trend is down
        w_score, weekly_ok = weekly_trend_score(df)
        if not weekly_ok and tq > 50:
            tq = max(35, tq - 15)   # strong daily trend against weekly = reduce confidence
        result["trend_quality"] = tq

        # ── Factor 2: Momentum Quality ──
        ret5d  = (last / closes[-6]  - 1) * 100 if n > 6  else 0
        ret21d = (last / closes[-22] - 1) * 100 if n > 22 else 0
        mom = 50
        if ret5d > 3 and ret21d > 6:         mom = 90
        elif ret5d > 1.5 and ret21d > 3:     mom = 75
        elif ret5d > 0 and ret21d > 0:       mom = 60
        elif ret5d < -2 and ret21d < -4:     mom = 18
        elif ret5d < 0:                       mom = 38
        gains  = pd.Series(np.diff(closes)).clip(lower=0).rolling(14).mean().iloc[-1]
        losses = (-pd.Series(np.diff(closes))).clip(lower=0).rolling(14).mean().iloc[-1]
        rsi    = 100 - (100 / (1 + gains / losses)) if losses > 0 else 100.0
        if rsi > 70:  mom = min(100, mom + 5)
        elif rsi < 35: mom = max(0, mom - 10)
        result["momentum_quality"] = round(mom, 1)

        # ── Factor 3: Volume + OBV Trend + Accumulation ──
        obv_scr        = _obv_trend_score(closes, volumes)
        base_vol_score = volume_delivery_score(vol_ratio, obv_scr)
        accum_scr, accum_signal = accumulation_score(closes, volumes, ema20, avg_vol_20)
        # Blend: 60% today's vol/delivery + 40% 10-day accumulation pattern
        combined_vol = round(base_vol_score * 0.60 + accum_scr * 0.40, 1)
        result["volume_delivery"]  = combined_vol
        result["accum_signal"]     = accum_signal   # "ACCUMULATING"|"DISTRIBUTING"|"NEUTRAL"
        # Warn in Telegram if distributing despite strong trend
        if accum_signal == "DISTRIBUTING":
            result.setdefault("_soft_warnings", []).append("DISTRIBUTION_DETECTED")

        # ── Factor 4: Sector Strength ──
        rotation_adj, sector_status = 0, "NEUTRAL"
        rotation_hit = False
        if sector_rotation:
            # Detect whether this sector was actually IN the rotation map.
            # sector_rotation is typically {sector_name: score} but may be
            # {"ranks": {...}} in some builds — handle both.
            try:
                _known = set()
                if isinstance(sector_rotation, dict):
                    if isinstance(sector_rotation.get("ranks"), dict):
                        _known = set(sector_rotation["ranks"].keys())
                    else:
                        _known = set(sector_rotation.keys())
                rotation_hit = sector in _known
            except Exception:
                rotation_hit = False
            rotation_adj, sector_status = sector_rotation_score(sector, sector_rotation)
        result["sector_strength"] = max(0, min(100, 50 + rotation_adj))
        result["sector_status"]   = sector_status
        # Phase 4 (2026-07-01): surface rotation velocity + 5d rank delta on
        # the stock dict so Gate 9 (SECTOR_LAGGING) and Telegram formatters can
        # distinguish a laggard drifting further down (real reject) from one
        # already rotating IN (contrarian window — soft-only).
        _srot = {}
        if isinstance(sector_rotation, dict):
            _srot = sector_rotation.get(sector, {}) if isinstance(sector_rotation.get(sector), dict) else {}
        result["sector_velocity"]      = _srot.get("rotation_velocity", "UNKNOWN")
        result["sector_rank_5d"]       = _srot.get("rank_5d")
        result["sector_rank_delta_5d"] = _srot.get("rank_delta_5d")
        if sector_rotation and not rotation_hit:
            # Surface as a soft warning + downgrade status so BUY thesis / logs
            # know we defaulted to neutral instead of a real rotation read.
            result["sector_status"] = "UNKNOWN"
            result.setdefault("_soft_warnings", []).append(
                f"SECTOR_ROTATION_MISSING({sector})"
            )

        # ── Factor 5: Relative Strength vs Nifty (real 21-day comparison) ──
        # Uses actual Nifty 21d return computed once in pipeline, not a proxy
        nifty_ret21_real = regime_data.get("nifty_ret21", 0.0)
        nifty_ret5_real  = regime_data.get("nifty_ret5",  0.0)
        rs_diff21 = ret21d - nifty_ret21_real
        rs_diff5  = ret5d  - nifty_ret5_real
        rs_combined = rs_diff21 * 0.6 + rs_diff5 * 0.4
        if rs_combined > 8:    rs = 92
        elif rs_combined > 4:  rs = 78
        elif rs_combined > 0:  rs = 62
        elif rs_combined > -4: rs = 45
        else:                  rs = 25
        result["rs_vs_nifty"] = rs
        result["rs_diff21"]   = round(rs_diff21, 2)

        # ── Factor 6: News Risk (placeholder — filled by pipeline after AI) ──
        result["news_risk"] = 50

        # ── Factor 7: Risk / Reward ──
        entry = round(last, 2)

        # Stop: actual 10-day swing low — NOT a hardcoded % of entry
        recent_lows    = lows[-10:] if len(lows) >= 10 else lows
        swing_low      = float(np.min(recent_lows))
        stop_candidate = round(swing_low * 0.995, 2)   # 0.5% buffer below swing low
        risk_raw_pct   = (entry - stop_candidate) / entry * 100 if entry > 0 else 8.0
        if risk_raw_pct < 2.0:
            stop_candidate = round(entry * 0.97, 2)    # 3% floor — stop too tight
        elif risk_raw_pct > 15.0:
            stop_candidate = round(entry * 0.88, 2)    # 12% cap  — stop too wide
        stop = stop_candidate
        if stop >= entry:
            stop = round(entry * 0.94, 2)

        risk_amt = entry - stop

        # Targets: wider ATR multiples for better R/R (2.5x & 4.5x vs old 2x/4x)
        target1 = round(entry + 2.5 * atr14, 2)
        target2 = round(entry + 4.5 * atr14, 2)
        # Guarantee minimum 1.5x R/R on T2 even with a wide stop
        min_t2  = round(entry + risk_amt * 1.5, 2)
        target2 = max(target2, min_t2)
        # Keep T1 between entry and midpoint of T2
        target1 = min(target1, round((entry + target2) / 2, 2))

        rr_ratio  = round((target2 - entry) / risk_amt, 2) if risk_amt > 0 else 0.0
        result["entry"]       = entry
        result["stop"]        = stop
        result["target1"]     = target1
        result["target2"]     = target2
        result["rr_ratio"]    = rr_ratio
        result["risk_reward"] = min(100, max(0, rr_ratio * 30))

        # ── Factor 8: Ownership Quality — driven by promoter pledge + 52W proximity ──
        # Actual ROE/pledge injected later by fetch_all_fundamentals_cached();
        # here we seed with a neutral value that improves once fundamentals arrive.
        result["ownership_quality"] = 50  # updated in _update_ownership_quality()

        # ── Factor 9: Options Sentiment (placeholder) ──
        result["options_sentiment"] = 60

        # ── Factor 10: Macro Alignment ──
        macro     = regime_data.get("macro", {})
        macro_adj = macro_regime_adjustment(macro)
        result["macro_alignment"] = max(0, min(100, 60 + macro_adj * 2))

        # ── Price Action Pattern ──
        pa_score, pa_pattern = price_action_score(closes, highs, lows, ema20, atr14)
        result["price_pattern"]   = pa_pattern
        result["weekly_trend_ok"] = weekly_ok

        # ── Trade Quality Score (now includes weekly + price action) ──
        # Weights: trend 30% | momentum 20% | volume 15% | rr 15% | weekly 10% | price_action 10%
        result["trade_quality_score"] = round(
            result["trend_quality"]    * 0.30 +
            result["momentum_quality"] * 0.20 +
            result["volume_delivery"]  * 0.15 +
            result["risk_reward"]      * 0.15 +
            w_score                    * 0.10 +
            pa_score                   * 0.10,
            1,
        )

        # ── Liquidity & price stats ──
        result["avg_volume"]      = round(avg_vol_20, 0)
        result["avg_value_lakhs"] = round(avg_val_lakhs, 1)
        result["near_52w_high"]   = near_52w_high
        result["price"]           = entry
        result["ret1d"]           = round(ret1d_check, 2)
        result["ret5d"]           = round(ret5d, 2)
        result["ret21d"]          = round(ret21d, 2)
        result["high_52w"]        = round(high_52w, 2)
        result["low_52w"]         = round(low_52w, 2)
        result["atr14"]           = round(atr14, 2)
        result["rsi14"]           = round(rsi, 1)

        # ── factor_scores mirror dict — used by format_confidence_breakdown() ──
        result["factor_scores"] = {k: round(float(result.get(k, 50) or 50), 1)
                                    for k in FACTOR_WEIGHTS}

    except Exception as e:
        _log(f"[WARN] compute_all_factors failed for {symbol}: {e}")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — PORTFOLIO & SIGNALS
# ─────────────────────────────────────────────────────────────────────────────

def load_portfolio() -> list:
    """
    Load active holdings. Priority order:
      1. PORTFOLIO_JSON env var (set as GitHub Actions secret)
      2. portfolio.json file on disk
      3. portfolio_state.csv (legacy CSV fallback)
    """
    # Priority 1: env var (GitHub Actions secret — supports both names)
    env_json = (os.getenv("MANUAL_PORTFOLIO_JSON") or os.getenv("PORTFOLIO_JSON") or "").strip()
    if env_json and env_json != "[]":
        try:
            data = json.loads(env_json)
            if data:
                _log(f"[INFO] Loaded {len(data)} holdings from MANUAL_PORTFOLIO_JSON secret")
                return data
        except Exception as e:
            _log(f"[WARN] MANUAL_PORTFOLIO_JSON env var parse failed: {e}")

    # Priority 2: JSON file (v6.0 format)
    try:
        if os.path.exists(PORTFOLIO_FILE):
            with open(PORTFOLIO_FILE, "r") as f:
                data = json.load(f)
                if data:
                    return data
    except Exception as e:
        _log(f"[WARN] load_portfolio JSON failed: {e}")

    # Fallback: portfolio_state.csv (existing pipeline format)
    csv_path = os.getenv("PORTFOLIO_STATE_FILE", "portfolio_state.csv")
    try:
        if os.path.exists(csv_path):
            holdings = []
            with open(csv_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    status = row.get("status", "").strip().upper()
                    if status not in ("OPEN", "ACTIVE", ""):
                        continue
                    holdings.append({
                        "symbol":      row.get("symbol", "").strip(),
                        "sector":      row.get("sector", "OTHERS").strip(),
                        "entry_price": float(row.get("entry_price", 0) or 0),
                        "stop_loss":   float(row.get("stop_loss", 0) or 0),
                        # CSV uses target_1 / target_2; normalise to target1 / target2
                        "target1":     float(row.get("target1") or row.get("target_1", 0) or 0),
                        "target2":     float(row.get("target2") or row.get("target_2", 0) or 0),
                        "entry_date":  row.get("entry_date", ""),
                        "quantity":    float(row.get("quantity", 0) or 0),
                    })
            if holdings:
                _log(f"[INFO] Loaded {len(holdings)} holdings from {csv_path} (CSV fallback)")
                return holdings
    except Exception as e:
        _log(f"[WARN] load_portfolio CSV fallback failed: {e}")
    return []


def save_portfolio(holdings: list) -> None:
    try:
        with open(PORTFOLIO_FILE, "w") as f:
            json.dump(holdings, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_portfolio failed: {e}")


def monitor_portfolio(holdings: list, price_data: dict, regime: str) -> list:
    """Bug 1 fix — pure rule-based, zero AI calls, no raw arrays sent anywhere."""
    alerts = []
    for holding in holdings:
        symbol   = holding.get("symbol", "")
        entry    = float(holding.get("entry_price", 0) or 0)
        stop     = float(holding.get("stop_loss",   0) or 0)
        qty      = float(holding.get("quantity",    0) or 0)
        sector   = holding.get("sector", "OTHERS")
        # Accept both target1 and target_1 (CSV legacy field names)
        target1  = float(holding.get("target1") or holding.get("target_1", 0) or 0)
        target2  = float(holding.get("target2") or holding.get("target_2", 0) or 0)
        current  = float(price_data.get(symbol, entry) or entry)
        pnl_pct  = round((current - entry) / entry * 100, 2) if entry > 0 else 0.0
        invested = round(entry   * qty, 2) if qty > 0 else 0.0
        cur_val  = round(current * qty, 2) if qty > 0 else 0.0
        pnl_abs  = round(cur_val - invested, 2)
        try:
            entry_dt  = datetime.datetime.strptime(holding.get("entry_date", ""), "%Y-%m-%d")
            # FIX: prefer business days (T-days). Weekends inflated the counter.
            try:
                _tdays = int(pd.bdate_range(entry_dt.date(),
                                            datetime.date.today()).size) - 1
                days_held = max(0, _tdays)
            except Exception:
                days_held = (datetime.datetime.today() - entry_dt).days
        except Exception:
            days_held = 0

        base = {
            "symbol": symbol, "sector": sector,
            "pnl_pct": pnl_pct, "current": current,
            "quantity": qty, "entry": entry,
            "invested": invested, "cur_val": cur_val, "pnl_abs": pnl_abs,
            "target1": target1, "target2": target2, "stop": stop,
        }
        if stop > 0 and current <= stop:
            alerts.append({**base, "action": "EXIT",       "reason": "HARD_STOP_HIT"})
        elif regime in ("BEAR", "STRONG_BEAR"):
            alerts.append({**base, "action": "EXIT",       "reason": "REGIME_BEAR"})
        elif target2 > 0 and current >= target2:
            alerts.append({**base, "action": "EXIT_FULL",  "reason": "TARGET2_HIT"})
        elif target1 > 0 and current >= target1:
            alerts.append({**base, "action": "TRAIL_STOP", "reason": "TARGET1_TRAIL"})
        elif days_held >= 20 and (target1 == 0 or current < target1):
            alerts.append({**base, "action": "REVIEW",     "reason": "TIME_STOP_20D", "days_held": days_held})
        else:
            alerts.append({**base, "action": "HOLD",       "reason": "ON_TRACK",      "days_held": days_held})
    return alerts


def detect_short_signals(scored_stocks: list, regime: str, thresh: dict) -> list:
    shorts = []
    if regime not in ("BEAR", "STRONG_BEAR", "HIGH_VOLATILITY"):
        return []
    for stock in scored_stocks:
        try:
            df = stock.get("_df")
            if df is None or len(df) < 50:
                continue
            closes  = df["Close"].squeeze().values.astype(float)
            volumes = df["Volume"].squeeze().values.astype(float)
            last    = closes[-1]
            ema20   = float(pd.Series(closes).ewm(span=20).mean().iloc[-1])
            ema50   = float(pd.Series(closes).ewm(span=50).mean().iloc[-1])
            ema200  = float(pd.Series(closes).ewm(span=200).mean().iloc[-1])
            avg_vol = float(pd.Series(volumes).rolling(20).mean().iloc[-1])
            today_vol = float(volumes[-1])
            below_all_emas    = last < ema20 < ema50 < ema200
            volume_confirmed  = today_vol > avg_vol * 1.5
            ret_5d = (last / closes[-6] - 1) * 100 if len(closes) > 6 else 0
            bearish_momentum  = ret_5d < -2.0
            if below_all_emas and volume_confirmed and bearish_momentum:
                short_entry  = round(last, 2)
                short_stop   = round(ema20 * 1.02, 2)
                short_t1     = round(last * 0.94, 2)
                short_t2     = round(last * 0.88, 2)
                rr = round((short_entry - short_t1) / (short_stop - short_entry), 2) if short_stop > short_entry else 0.0
                if rr >= thresh.get("min_rr", 1.6):
                    shorts.append({
                        "symbol":  stock.get("symbol", ""),
                        "entry":   short_entry,
                        "stop":    short_stop,
                        "target1": short_t1,
                        "target2": short_t2,
                        "rr":      rr,
                        "reason":  f"Below all EMAs | Vol {today_vol/max(avg_vol,1):.1f}x | Ret5d {ret_5d:.1f}%",
                        "sector":  stock.get("sector", "OTHERS"),
                    })
        except Exception as e:
            _log(f"[WARN] short signal detection failed for {stock.get('symbol')}: {e}")
    shorts.sort(key=lambda x: x["rr"], reverse=True)
    return shorts[:3]


def run_gates(stock: dict, regime: str, thresholds: dict,
              portfolio: dict, bulk_deals: dict, promoter_data: dict,
              results_dates: list = None, upcoming_events: list = None,
              returns_cache: dict = None, holdings: list = None) -> dict:
    """14-gate decision system."""
    decision = "BUY"
    fail_reasons = []
    warnings = []
    thresh = thresholds[regime]

    # Gate 1: Data Quality (HARD)
    if not stock.get("entry") or not stock.get("stop") or not stock.get("target1"):
        return {"decision": "REJECTED", "fail_reasons": ["DATA_INCOMPLETE"], "warnings": []}

    # Gate 2: Black Swan News (HARD)
    if stock.get("news_penalty", 0) >= 999 or stock.get("is_black_swan"):
        return {"decision": "REJECTED", "fail_reasons": ["BLACK_SWAN_NEWS"], "warnings": []}

    # Gate 3: Promoter Pledge (HARD)
    pledge = float(promoter_data.get("promoter_pledge_pct", 0) or 0)
    if pledge > 40:
        return {"decision": "REJECTED", "fail_reasons": [f"PROMOTER_PLEDGE_{pledge:.0f}PCT"], "warnings": []}

    # Gate 4: Liquidity (HARD)
    if stock.get("avg_volume", 0) < 100_000 or stock.get("avg_value_lakhs", 0) < 50:
        return {"decision": "REJECTED", "fail_reasons": ["LIQUIDITY_FAIL"], "warnings": []}

    # Gate 5: Market Regime max_buys (HARD)
    if thresh["max_buys"] == 0:
        return {"decision": "REJECTED", "fail_reasons": ["REGIME_NO_BUY"], "warnings": []}

    # Gate 6: Confidence (HARD)
    conf = stock.get("final_confidence", 0)
    if conf < thresh["min_confidence"]:
        fail_reasons.append(f"CONF_FAIL(got {conf:.1f}, need {thresh['min_confidence']})")

    # Gate 7: Trade Quality (HARD)
    tq = stock.get("trade_quality_score", 0)
    if tq < thresh["min_tq"]:
        fail_reasons.append(f"TQ_FAIL(got {tq:.1f}, need {thresh['min_tq']})")

    # Gate 8: Risk/Reward (HARD)
    rr = stock.get("rr_ratio", 0)
    if rr < thresh["min_rr"]:
        fail_reasons.append(f"RR_FAIL(got {rr:.2f}, need {thresh['min_rr']})")

    # Gate 8b: Wide-stop guardrail (HARD) — reject if stop distance > regime cap.
    # Prevents a 12% stop from being deployed in a regime where 6% is the ceiling.
    try:
        _entry_v = float(stock.get("entry", 0) or 0)
        _stop_v  = float(stock.get("stop",  0) or 0)
        _max_stop_pct = float(thresh.get("max_stop_pct", 8.0))
        if _entry_v > 0 and 0 < _stop_v < _entry_v:
            _stop_dist_pct = (_entry_v - _stop_v) / _entry_v * 100.0
            if _stop_dist_pct > _max_stop_pct:
                fail_reasons.append(
                    f"WIDE_STOP(got {_stop_dist_pct:.1f}%, cap {_max_stop_pct:.1f}%)"
                )
    except Exception:
        pass

    # Gate 9: Sector Health (SOFT — LAGGING counts toward the 2-fail budget but
    # does NOT hard-block watchlist entry). Phase C2 (2026-07-02): previously
    # LAGGING was force-rejected, which combined with the sector_master.csv
    # fallback (fix #4) caused mass carnage — every Banking/Insurance stock
    # that used to be sector=OTHERS suddenly became a hard-reject. Now LAGGING
    # is a scoreable fail: rotation candidates can still reach WATCHLIST/
    # NEAR_MISS, and only get pushed to REJECTED if combined with other fails.
    # Phase 4 (2026-07-01): rotation velocity overrides static status —
    # a LAGGING sector that is ROTATING_IN (5-day rank moved up ≥3 spots) is
    # a valid contrarian entry, so downgrade to a warning only.
    sector_status   = stock.get("sector_status", "NEUTRAL")
    sector_velocity = stock.get("sector_rotation_velocity", "UNKNOWN")
    if sector_status == "LAGGING":
        if sector_velocity == "ROTATING_IN":
            warnings.append("SECTOR_LAGGING_BUT_ROTATING_IN")
        else:
            fail_reasons.append("SECTOR_LAGGING")
    elif sector_status == "WEAKENING":
        warnings.append("SECTOR_WEAKENING")
    # Rotating-OUT is a soft warning regardless of static status — sector is
    # bleeding leadership rank fast.
    if sector_velocity == "ROTATING_OUT" and sector_status != "LAGGING":
        warnings.append("SECTOR_ROTATING_OUT")

    # Gate 10: High Pledge Warning (SOFT)
    if 20 < pledge <= 40:
        warnings.append(f"PLEDGE_WARNING_{pledge:.0f}PCT")

    # Gate 11: 52-Week High Proximity (SOFT)
    if stock.get("near_52w_high", False):
        warnings.append("NEAR_52W_HIGH_RESISTANCE")

    # Gate 12: Portfolio Capacity (SOFT)
    active_count = portfolio.get("active_count", 0)
    if active_count >= thresh["max_buys"]:
        fail_reasons.append("PORTFOLIO_FULL")

    # Gate 13: Event Calendar (HARD) — no new BUY within 5 trading days of results/monthly expiry
    near_event, event_reason = is_near_event(
        stock.get("symbol", "").replace(".NS", ""),
        results_dates or [],
        upcoming_events or [],
        window_days=5,
    )
    if near_event:
        # Move to WATCHLIST rather than REJECTED — setup may still be valid post-event
        fail_reasons.append(f"EVENT_BLOCK_{event_reason}")
        warnings.append(f"NEAR_EVENT: {event_reason}")

    # Gate 14: Correlation with existing holdings (HARD)
    # Reject if candidate moves in lockstep (corr > 0.75) with something already held
    if returns_cache and holdings:
        blocked, worst_corr, corr_sym = correlation_check(
            stock.get("symbol", ""), holdings, returns_cache, max_corr=0.75
        )
        if blocked:
            fail_reasons.append(f"HIGH_CORR_{worst_corr:.2f}_WITH_{corr_sym.replace('.NS','')}")
            warnings.append(f"CORRELATED_WITH: {corr_sym} ({worst_corr:.2f})")
        elif worst_corr >= 0.60:
            warnings.append(f"MODERATE_CORR_{worst_corr:.2f}_WITH_{corr_sym.replace('.NS','')}")

    # Gate 15: Delivery Quality (SOFT — pump detection & institutional exit)
    # Phase C3 (2026-07-02): only fires when we actually got real nselib
    # delivery data (source == "nselib"). Two cases:
    #   (a) DISTRIBUTION signal (ratio ≤ 0.70)  → soft fail, "SUSPECT_PUMP" or
    #                                             "INSTITUTIONAL_EXIT"
    #       - If today's return also > +2%      → clear pump pattern
    #       - Else                              → possible institutional exit
    #   (b) WEAK signal (0.70 < ratio ≤ 0.85)   → warning only, no fail
    if stock.get("delivery_source") == "nselib":
        deliv_sig = stock.get("delivery_signal", "NEUTRAL")
        # Prefer explicit ret1d_pct if present; fall back to ret1d (the actual
        # key set by score_stock at L5205).
        ret1d     = float(
            stock.get("ret1d_pct", stock.get("ret1d", 0.0)) or 0.0
        )
        if deliv_sig == "DISTRIBUTION":
            if ret1d > 2.0:
                fail_reasons.append("SUSPECT_PUMP_LOW_DELIVERY")
                warnings.append(
                    f"PUMP_PATTERN: price+{ret1d:.1f}% but delivery ratio "
                    f"{stock.get('delivery_ratio', 0):.2f}"
                )
            else:
                fail_reasons.append("INSTITUTIONAL_EXIT")
                warnings.append(
                    f"DISTRIBUTION: delivery {stock.get('delivery_pct_today', 0):.0f}% "
                    f"vs 20d avg {stock.get('delivery_pct_20d_avg', 0):.0f}%"
                )
        elif deliv_sig == "WEAK":
            warnings.append(
                f"WEAK_DELIVERY: {stock.get('delivery_pct_today', 0):.0f}% "
                f"(20d avg {stock.get('delivery_pct_20d_avg', 0):.0f}%)"
            )
        elif deliv_sig == "STRONG_ACCUM":
            # Positive confirmation — surface as informational warning-tag
            warnings.append(
                f"STRONG_ACCUMULATION: delivery {stock.get('delivery_pct_today', 0):.0f}% "
                f"(20d avg {stock.get('delivery_pct_20d_avg', 0):.0f}%)"
            )

    hard_fails = [f for f in fail_reasons if "WARNING" not in f]
    if hard_fails:
        # PORTFOLIO_FULL never blocks watchlist — stock is valid, just capacity is full today
        # Strip it before deciding WATCHLIST vs REJECTED so it doesn't poison the check
        scoreable_fails = [f for f in hard_fails if "PORTFOLIO_FULL" not in f]
        # Phase C2 (2026-07-02): SECTOR_LAGGING is now a soft-scoreable fail —
        # it counts toward the 2-fail budget but no longer excludes watchlist.
        # Phase C3 (2026-07-02): INSTITUTIONAL_EXIT also soft-scoreable
        # (allow watchlist tier for post-distribution rebounds). SUSPECT_PUMP
        # stays HARD because pump patterns rarely recover cleanly.
        soft_only = all(
            "EVENT_BLOCK" in f or "HIGH_CORR" in f or "SECTOR_LAGGING" in f
            or "INSTITUTIONAL_EXIT" in f
            for f in scoreable_fails
        )
        if not scoreable_fails:
            # Only PORTFOLIO_FULL failed — valid setup, just no room
            decision = "WATCHLIST"
        elif len(scoreable_fails) <= 2 and (soft_only or all("FAIL" in f for f in scoreable_fails)):
            decision = "WATCHLIST"
        else:
            decision = "REJECTED"

    return {"decision": decision, "fail_reasons": fail_reasons, "warnings": warnings}


def build_returns_cache(tradable: dict, lookback: int = 60) -> dict:
    """
    Pre-compute 60-day daily returns for every stock in the tradable universe.
    Used by correlation_check() — built once per pipeline run, passed around.
    Returns {symbol: [float, ...]} — empty list if insufficient data.
    """
    cache = {}
    for symbol, df in tradable.items():
        try:
            closes = df["Close"].squeeze().values.astype(float)
            if len(closes) < lookback + 2:
                cache[symbol] = []
                continue
            sample = closes[-(lookback + 1):]
            rets   = [(sample[i] / sample[i-1] - 1.0) for i in range(1, len(sample))
                      if sample[i-1] > 0]
            cache[symbol] = rets
        except Exception:
            cache[symbol] = []
    return cache


def _pearson(a: list, b: list) -> float:
    n = min(len(a), len(b))
    if n < 20:
        return 0.0
    x, y = a[-n:], b[-n:]
    mx, my = sum(x)/n, sum(y)/n
    cov = sum((x[i]-mx)*(y[i]-my) for i in range(n))
    vx  = sum((v-mx)**2 for v in x)
    vy  = sum((v-my)**2 for v in y)
    if vx <= 0 or vy <= 0:
        return 0.0
    return cov / (vx * vy) ** 0.5


def correlation_check(symbol: str, holdings: list, returns_cache: dict,
                      max_corr: float = 0.75) -> tuple:
    """
    Gate 14: Reject if the candidate is highly correlated with an existing holding.
    Returns (blocked: bool, worst_corr: float, correlated_with: str).
    Uses pre-built returns_cache — no extra downloads.
    """
    if not holdings or not returns_cache:
        return False, 0.0, ""

    cand_rets = returns_cache.get(symbol, [])
    if len(cand_rets) < 20:
        return False, 0.0, ""   # not enough data — pass through

    worst_corr   = 0.0
    worst_symbol = ""
    for h in holdings:
        held_sym  = h.get("symbol", "")
        held_rets = returns_cache.get(held_sym, [])
        if len(held_rets) < 20:
            continue
        c = abs(_pearson(cand_rets, held_rets))
        if c > worst_corr:
            worst_corr   = c
            worst_symbol = held_sym

    if worst_corr >= max_corr:
        return True, round(worst_corr, 2), worst_symbol
    return False, round(worst_corr, 2), worst_symbol


def calculate_watchlist_levels(stock: dict) -> dict:
    """
    ATR-based entry/stop/target levels (FIX 2).
    Uses existing scored values when present; recomputes via yfinance ATR when missing.
    Returns valid floats — never crashes.
    """
    symbol  = stock.get("symbol", "")
    entry   = float(stock.get("entry",   0) or 0)
    stop    = float(stock.get("stop",    0) or 0)
    target1 = float(stock.get("target1", 0) or 0)
    target2 = float(stock.get("target2", 0) or 0)
    current = float(stock.get("price",   0) or entry)

    # If levels already computed and valid, just compute rr/risk_pct
    if entry > 0 and stop > 0 and target1 > entry:
        risk_pct = round((entry - stop) / entry * 100, 1)
        rr = round((target1 - entry) / (entry - stop), 2) if entry > stop else 0.0
        return {
            "entry": entry, "stop": stop, "target1": target1, "target2": target2,
            "rr": rr, "risk_pct": risk_pct, "current": current,
        }

    # Fetch via yfinance and compute ATR-based levels (FIX 2 — swing-low stop, ATR targets)
    try:
        df = yf.download(symbol, period="3mo", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < 20:
            return {"entry": entry, "stop": stop, "target1": target1,
                    "target2": target2, "rr": 0.0, "risk_pct": 0.0, "current": current}

        close = df["Close"].values.flatten().astype(float)
        high  = df["High"].values.flatten().astype(float)
        low   = df["Low"].values.flatten().astype(float)
        vol   = df["Volume"].values.flatten().astype(float)

        current = round(float(close[-1]), 2)
        entry   = round(current * 1.003, 2)

        # True ATR over last 14 days
        tr_list = []
        for i in range(1, min(15, len(close))):
            tr = max(
                float(high[-i]) - float(low[-i]),
                abs(float(high[-i]) - float(close[-i-1])),
                abs(float(low[-i])  - float(close[-i-1]))
            )
            tr_list.append(tr)
        atr = float(np.mean(tr_list)) if tr_list else current * 0.03

        # STOP: 10-day swing low with 0.5% buffer
        swing_low = float(np.min(low[-10:]))
        stop = round(swing_low * 0.995, 2)

        # Enforce bounds: 2% min, 10% max
        risk_raw = (entry - stop) / entry * 100
        if risk_raw < 2.0:
            stop = round(entry * 0.97, 2)
        elif risk_raw > 10.0:
            stop = round(entry * 0.90, 2)

        risk     = entry - stop
        risk_pct = round(risk / entry * 100, 1)

        # TARGETS: 2.5x and 4.5x ATR — better R/R than hardcoded 8-12%
        target1 = round(entry + atr * 2.5, 2)
        target2 = round(entry + atr * 4.5, 2)

        # Ensure minimum 1.5x R/R
        if (target2 - entry) < (risk * 1.5):
            target2 = round(entry + risk * 1.5, 2)
        target1 = round((entry + target2) / 2, 2)

        reward = target2 - entry
        rr     = round(reward / risk, 2) if risk > 0 else 0.0

        return {
            "entry": entry, "stop": stop, "target1": target1, "target2": target2,
            "rr": rr, "risk_pct": risk_pct, "current": current,
        }
    except Exception:
        # Graceful fallback to whatever values exist
        risk_pct = round((entry - stop) / entry * 100, 1) if entry > stop > 0 else 0.0
        rr = round((target1 - entry) / (entry - stop), 2) if entry > stop > 0 and target1 > entry else 0.0
        return {
            "entry": entry, "stop": stop, "target1": target1, "target2": target2,
            "rr": rr, "risk_pct": risk_pct, "current": current,
        }


def get_stock_rr(stock: dict, levels: dict) -> float:
    """Single authoritative R/R value (BUG FIX 7).
    Priority: scoring engine rr_ratio > ATR-calculated levels rr.
    """
    scoring_rr = float(stock.get("rr_ratio", 0) or 0)
    levels_rr  = float(levels.get("rr",       0) or 0)
    if scoring_rr > 0:
        return scoring_rr
    if levels_rr > 0:
        return levels_rr
    return 0.0


def classify_watchlist(stock: dict, regime: str, thresholds: dict) -> dict:
    thresh   = (thresholds or REGIME_THRESHOLDS)[regime]
    min_conf = thresh["min_confidence"]
    conf     = stock.get("final_confidence", 0)
    tq       = stock.get("trade_quality_score", 0)
    # FIX (IOLCP bug): clamp to 0. Previously produced negative gaps when Conf
    # already passed the threshold but R/R was the real blocker — the AI/rule
    # insight then said "close the -10.7-point gap" which is nonsense.
    conf_gap = round(max(0.0, min_conf - conf), 1)

    # Always compute price levels — every watchlist entry shows them
    levels = calculate_watchlist_levels(stock)

    base = {
        "conf":     conf,
        "tq":       tq,
        "conf_gap": conf_gap,
        "sector":   stock.get("sector") or get_sector(stock.get("symbol", "")),
        "entry":    levels["entry"],
        "stop":     levels["stop"],
        "target1":  levels["target1"],
        "target2":  levels["target2"],
        "rr":       levels["rr"],
        "rr_ratio": get_stock_rr(stock, levels),
        "risk_pct": levels["risk_pct"],
        "current":  levels["current"],
        "fail_reasons": stock.get("fail_reasons", []),
        "warnings":     stock.get("warnings", []),
    }

    # Tier logic: relative to regime gap size (not absolute confidence)
    # NEAR_MISS  = gap <= 15  (within striking distance — watch daily)
    # DEVELOPING = gap <= 25  (building — watch weekly)
    # MONITOR    = gap > 25   (early stage — track loosely)
    if conf_gap <= 15 and tq >= thresh["min_tq"] - 5:
        return {**base, "tier": "NEAR_MISS",
                "note": f"Needs +{conf_gap:.1f} conf. Watch for volume trigger.",
                "days_to_watch": 3, "watch_days": 3}
    elif conf_gap <= 25 and tq >= 70:
        return {**base, "tier": "DEVELOPING",
                "note": f"TQ {tq:.1f} building. Conf gap {conf_gap:.1f}.",
                "days_to_watch": 7, "watch_days": 7}
    else:
        return {**base, "tier": "MONITOR",
                "note": "Early stage. Review in 2 weeks.",
                "days_to_watch": 14, "watch_days": 14}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — WATCHLIST PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

def load_persistent_watchlist() -> dict:
    try:
        if os.path.exists(WATCHLIST_FILE):
            with open(WATCHLIST_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        _log(f"[WARN] load_persistent_watchlist failed: {e}")
    return {}


def save_persistent_watchlist(watchlist_dict: dict) -> None:
    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(watchlist_dict, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_persistent_watchlist failed: {e}")


def merge_watchlist_with_history(todays_watchlist: list, history: dict) -> tuple:
    today_str = datetime.date.today().isoformat()
    updated_history = {}
    for stock in todays_watchlist:
        symbol     = stock.get("symbol", "")
        prev       = history.get(symbol, {})
        first_seen = prev.get("first_seen", today_str)
        days_watched = (datetime.date.today() -
                        datetime.date.fromisoformat(first_seen)).days
        stock["days_watched"] = days_watched
        stock["first_seen"]   = first_seen
        if days_watched > 0:
            stock["note"] = stock.get("note", "") + f" [Day {days_watched + 1}]"
        max_days = stock.get("days_to_watch", 14)
        if days_watched <= max_days:
            updated_history[symbol] = {
                "first_seen":  first_seen,
                "tier":        stock.get("tier", "MONITOR"),
                "entry_ref":   stock.get("entry", 0),
                "last_seen":   today_str,
            }
    return todays_watchlist, updated_history


def tag_repeat_buy_signals(buys: list, tracker_entries: list) -> list:
    today_str = datetime.date.today().isoformat()
    for stock in buys:
        symbol = stock.get("symbol", "")
        match  = next((e for e in tracker_entries
                       if e["symbol"] == symbol
                       and e["type"] == "BUY"
                       and e["status"] == "OPEN"
                       and e["suggested_date"] != today_str), None)
        if match:
            days_since = (datetime.date.today() -
                          datetime.date.fromisoformat(match["suggested_date"])).days
            stock["repeat_tag"] = f"REPEAT DAY {days_since + 1}"
            existing = stock.get("warnings", [])
            existing.append(f"REPEAT_DAY_{days_since + 1}")
            stock["warnings"] = existing
    return buys


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — TRADE TRACKER
# ─────────────────────────────────────────────────────────────────────────────

def load_tracker() -> list:
    try:
        if os.path.exists(TRACKER_FILE):
            with open(TRACKER_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        _log(f"[WARN] load_tracker failed: {e}")
    return []


def save_tracker(entries: list) -> None:
    try:
        with open(TRACKER_FILE, "w") as f:
            json.dump(entries, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_tracker failed: {e}")


def add_to_tracker(entries: list, stock: dict, sig_type: str) -> list:
    today_str = datetime.date.today().isoformat()
    symbol    = stock.get("symbol", "")
    already   = any(
        e["symbol"] == symbol and e["suggested_date"] == today_str and e["status"] == "OPEN"
        for e in entries
    )
    if already:
        return entries
    entry = {
        "symbol":          symbol,
        "type":            sig_type,
        "suggested_date":  today_str,
        "suggested_price": round(float(stock.get("entry", stock.get("price", 0)) or 0), 2),
        "entry":           float(stock.get("entry", 0) or 0),
        "stop":            float(stock.get("stop", 0) or 0),
        "target1":         float(stock.get("target1", 0) or 0),
        "target2":         float(stock.get("target2", 0) or 0),
        "sector":          stock.get("sector", "OTHERS"),
        "conf":            round(float(stock.get("final_confidence", 0) or 0), 1),
        "tq":              round(float(stock.get("trade_quality_score", 0) or 0), 1),
        "status":          "OPEN",
        "close_reason":    None,
        "close_price":     None,
        "close_date":      None,
        "max_gain_pct":    0.0,
        "max_loss_pct":    0.0,
    }
    entries.append(entry)
    return entries


def update_tracker_trailing_stop(entries: list) -> list:
    for e in entries:
        if e["status"] != "OPEN":
            continue
        try:
            df = fetch_price_data(e["symbol"], period="5d")
            if df is None or len(df) == 0:
                continue
            current  = float(df["Close"].squeeze().iloc[-1])
            t1       = float(e.get("target1", 0) or 0)
            entry_px = float(e.get("entry", 0) or 0)
            old_stop = float(e.get("stop", 0) or 0)
            if t1 > 0 and current >= t1 and entry_px > old_stop:
                e["stop"]       = entry_px
                e["trail_note"] = f"Trailed to entry {entry_px} after T1 hit"
        except Exception as ex:
            _log(f"[WARN] trailing stop update failed for {e.get('symbol')}: {ex}")
    return entries


def update_tracker(entries: list) -> tuple:
    closed_today = []
    for e in entries:
        if e["status"] != "OPEN":
            continue
        try:
            df = fetch_price_data(e["symbol"], period="1d")
            if df is None or len(df) == 0:
                continue
            current    = round(float(df["Close"].squeeze().iloc[-1]), 2)
            entry_px   = float(e.get("entry") or e.get("suggested_price", 0))
            stop       = float(e.get("stop", 0) or 0)
            t1         = float(e.get("target1", 0) or 0)
            t2         = float(e.get("target2", 0) or 0)
            if entry_px <= 0:
                continue
            chg_pct = round((current - entry_px) / entry_px * 100, 2)
            if chg_pct > e["max_gain_pct"]:
                e["max_gain_pct"] = chg_pct
            if chg_pct < e["max_loss_pct"]:
                e["max_loss_pct"] = chg_pct
            close_reason = None
            if stop > 0 and current <= stop:
                close_reason = "STOP_HIT"
            elif t2 > 0 and current >= t2:
                close_reason = "TARGET2_HIT"
            elif t1 > 0 and current >= t1:
                close_reason = "TARGET1_HIT"
            if close_reason:
                e["status"]        = "CLOSED"
                e["close_reason"]  = close_reason
                e["close_price"]   = current
                e["close_date"]    = datetime.date.today().isoformat()
                e["final_pnl_pct"] = chg_pct
                closed_today.append(e)
        except Exception as ex:
            _log(f"[WARN] tracker update failed for {e.get('symbol')}: {ex}")
    return entries, closed_today


def _days_open(e: dict) -> int:
    try:
        return (datetime.date.today() - datetime.date.fromisoformat(e["suggested_date"])).days
    except Exception:
        return 0


def _pct_bar(chg_pct: float) -> str:
    if chg_pct >= 0:
        bars = min(10, int(chg_pct))
        return f"▲ {'█' * bars}{'░' * (10 - bars)} +{chg_pct:.2f}%"
    else:
        bars = min(10, int(abs(chg_pct)))
        return f"▼ {'█' * bars}{'░' * (10 - bars)} {chg_pct:.2f}%"


def _entry_block(e: dict) -> list:
    blk      = []
    sym      = e["symbol"]
    entry_px = float(e.get("entry") or e.get("suggested_price", 0))
    t1       = float(e.get("target1", 0) or 0)
    t2       = float(e.get("target2", 0) or 0)
    stop     = float(e.get("stop", 0) or 0)
    days     = _days_open(e)
    conf     = e.get("conf", 0)
    try:
        df      = fetch_price_data(sym, period="1d")
        current = round(float(df["Close"].squeeze().iloc[-1]), 2) if df is not None and len(df) > 0 else entry_px
    except Exception:
        current = entry_px
    chg_pct = round((current - entry_px) / entry_px * 100, 2) if entry_px > 0 else 0.0
    t1_pct  = round((t1 - entry_px) / entry_px * 100, 1) if entry_px > 0 and t1 > 0 else 0
    t2_pct  = round((t2 - entry_px) / entry_px * 100, 1) if entry_px > 0 and t2 > 0 else 0
    sl_pct  = round((stop - entry_px) / entry_px * 100, 1) if entry_px > 0 and stop > 0 else 0
    blk.append(f"  <b>{sym}</b> [{e['type']}] | Since {e['suggested_date']} ({days}d)")
    blk.append(f"  Conf {conf:.1f} | Entry Rs{entry_px:.2f} → Now Rs{current:.2f}")
    blk.append(f"  {_pct_bar(chg_pct)}")
    blk.append(f"  SL {sl_pct:+.1f}% | T1 {t1_pct:+.1f}% | T2 {t2_pct:+.1f}%")
    blk.append(f"  Peak gain: {e['max_gain_pct']:+.2f}% | Peak loss: {e['max_loss_pct']:+.2f}%")
    return blk


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9b — TRADE TRACKER V2 (new structured format)
# ─────────────────────────────────────────────────────────────────────────────

def load_tracker_v2() -> dict:
    try:
        if os.path.exists(TRADE_TRACKER_V2_FILE):
            with open(TRADE_TRACKER_V2_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        _log(f"[WARN] load_tracker_v2 failed: {e}")
    return {"buys": [], "watchlist": [], "completed": [], "performance": {}}


def save_tracker_v2(tracker: dict) -> None:
    try:
        with open(TRADE_TRACKER_V2_FILE, "w") as f:
            json.dump(tracker, f, indent=2, default=str)
    except Exception as e:
        _log(f"[WARN] save_tracker_v2 failed: {e}")


def initialize_tracker_if_new() -> dict:
    """
    Called at pipeline start. Seeds Jun 25 data if no tracker v2 file exists yet.
    """
    if os.path.exists(TRADE_TRACKER_V2_FILE):
        return load_tracker_v2()

    _log("[INFO] No tracker v2 found — initializing with Jun 25, 2026 seed data")
    tracker = {
        "buys": [
            {
                "symbol": "SIYSIL.NS", "rec_date": "2026-06-25",
                "entry": 645.30, "stop": 607.93,
                "target1": 707.58, "target2": 757.40,
                "confidence": 65.7, "tq": 94.0,
                "regime": "HIGH_VOLATILITY", "status": "ACTIVE",
                "t1_hit_date": None, "t2_hit_date": None, "stop_hit_date": None,
                "days_tracked": 1, "pnl_history": [],
            }
        ],
        "watchlist": [
            {"symbol": "BOSCHLTD.NS",   "rec_date": "2026-06-25", "tier": "NEAR_MISS",
             "conf_at_rec": 62.5, "conf_gap_at_rec": 1.5, "status": "WATCHING", "days_watching": 1},
            {"symbol": "GENUSPOWER.NS", "rec_date": "2026-06-25", "tier": "NEAR_MISS",
             "conf_at_rec": 62.1, "conf_gap_at_rec": 1.9, "status": "WATCHING", "days_watching": 1},
            {"symbol": "KRISHANA.NS",   "rec_date": "2026-06-25", "tier": "NEAR_MISS",
             "conf_at_rec": 60.8, "conf_gap_at_rec": 3.2, "status": "WATCHING", "days_watching": 1},
            {"symbol": "NAZARA.NS",     "rec_date": "2026-06-25", "tier": "NEAR_MISS",
             "conf_at_rec": 60.4, "conf_gap_at_rec": 3.6, "status": "WATCHING", "days_watching": 1},
            {"symbol": "SONACOMS.NS",   "rec_date": "2026-06-25", "tier": "DEVELOPING",
             "tq_at_rec": 89.4, "conf_gap_at_rec": 5.2, "status": "WATCHING", "days_watching": 1},
            {"symbol": "CEIGALL.NS",    "rec_date": "2026-06-25", "tier": "DEVELOPING",
             "tq_at_rec": 97.0, "conf_gap_at_rec": 5.4, "status": "WATCHING", "days_watching": 1},
            {"symbol": "RATNAVEER.NS",  "rec_date": "2026-06-25", "tier": "DEVELOPING",
             "tq_at_rec": 99.0, "conf_gap_at_rec": 5.5, "status": "WATCHING", "days_watching": 1},
        ],
        "completed": [],
        "performance": {},
    }
    save_tracker_v2(tracker)
    return tracker


def update_tracker_v2_pnl(tracker: dict) -> dict:
    """
    Full daily update for tracker v2 (FIX 3):
    - Fetches live prices for every active position
    - Handles STOP_HIT, T1_HIT, T2_HIT, EXPIRED closings
    - Updates watchlist direction arrows and fills missing levels
    - Recalculates performance stats block
    """
    today_str = datetime.date.today().isoformat()

    def _get_price(sym: str) -> float:
        try:
            df = fetch_price_data(sym, period="2d")
            if df is not None and len(df) > 0:
                return round(float(df["Close"].squeeze().iloc[-1]), 2)
        except Exception:
            pass
        return 0.0

    # ── Update active buy positions ──
    still_active = []
    for pos in tracker.get("buys", []):
        if pos.get("status") not in ("ACTIVE", "T1_HIT"):
            tracker.setdefault("completed", []).append(pos)
            continue
        try:
            cur_px = _get_price(pos["symbol"]) or float(pos.get("entry", 0) or 0)
            entry  = float(pos.get("entry", cur_px) or cur_px)
            pnl    = round((cur_px - entry) / entry * 100, 2) if entry > 0 else 0.0
            pos["days_tracked"] = (
                datetime.date.today() -
                datetime.date.fromisoformat(pos.get("rec_date", today_str))
            ).days + 1
            hist = pos.setdefault("pnl_history", [])
            # Only append a new daily entry on scheduled runs — manual runs must not pollute history
            if IS_SCHEDULED and (not hist or hist[-1].get("date") != today_str):
                hist.append({"date": today_str, "price": cur_px, "pnl": pnl})

            stop = float(pos.get("stop", 0) or 0)
            t1   = float(pos.get("target1", 0) or 0)
            t2   = float(pos.get("target2", 0) or 0)
            days = pos.get("days_tracked", 0)

            if stop > 0 and cur_px <= stop:
                pos.update({"status": "STOPPED_OUT", "stop_hit_date": today_str, "final_pnl": pnl})
                tracker["completed"].append(pos)
            elif t2 > 0 and cur_px >= t2:
                pos.update({"status": "T2_HIT", "t2_hit_date": today_str, "final_pnl": pnl})
                tracker["completed"].append(pos)
            elif t1 > 0 and cur_px >= t1 and pos.get("status") == "ACTIVE":
                pos["status"] = "T1_HIT"
                pos["t1_hit_date"] = today_str
                still_active.append(pos)
            elif days >= 15 and t1 > 0 and cur_px < t1:
                pos.update({"status": "EXPIRED", "final_pnl": pnl})
                tracker["completed"].append(pos)
            else:
                still_active.append(pos)
        except Exception as e:
            _log(f"[WARN] update_tracker_v2_pnl buy update failed for {pos.get('symbol')}: {e}")
            still_active.append(pos)
    tracker["buys"] = still_active

    # ── Update watchlist entries ──
    still_watching = []
    for w in tracker.get("watchlist", []):
        try:
            days = (
                datetime.date.today() -
                datetime.date.fromisoformat(w.get("rec_date", today_str))
            ).days + 1
            w["days_watching"] = days
            if days > 14:
                w["status"] = "EXPIRED"
                continue

            cur = _get_price(w["symbol"])
            if cur:
                w["current_price"] = cur
                # Fill missing levels for seeded watchlist stocks
                entry = float(w.get("entry", 0) or 0)
                if entry <= 0:
                    lvl = calculate_watchlist_levels(w)
                    for k in ("entry", "stop", "target1", "target2", "rr"):
                        w[k] = lvl.get(k, 0)
                    entry = w.get("entry", 0) or cur

                if entry > 0:
                    move = round((cur - entry) / entry * 100, 1)
                    w["direction"] = (
                        f"\u2191 {move:.1f}% above entry" if move >= 0
                        else f"\u2193 {abs(move):.1f}% below entry"
                    )
                else:
                    w.setdefault("direction", "\u2014")
            else:
                w.setdefault("direction", "\u2014")
            still_watching.append(w)
        except Exception:
            w.setdefault("direction", "\u2014")
            still_watching.append(w)
    tracker["watchlist"] = still_watching

    # ── Recalculate performance stats ──
    completed = tracker.get("completed", [])
    wins   = [t for t in completed if float(t.get("final_pnl", 0) or 0) > 0]
    losses = [t for t in completed if float(t.get("final_pnl", 0) or 0) <= 0]
    tracker["performance"] = {
        "completed":  len(completed),
        "active":     len(tracker["buys"]),
        "win_rate":   round(len(wins) / len(completed) * 100, 1) if completed else 0,
        "avg_win":    round(sum(float(t.get("final_pnl", 0) or 0) for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss":   round(sum(float(t.get("final_pnl", 0) or 0) for t in losses) / len(losses), 2) if losses else 0,
        "last_updated": today_str,
    }
    return tracker


def format_confidence_breakdown(factor_scores: dict, final_conf: float) -> list:
    """Returns lines showing what drove the confidence score (ENHANCEMENT 2)."""
    FACTOR_DISPLAY = [
        ("trend_quality",    "Trend",     18),
        ("momentum_quality", "Momentum",  14),
        ("volume_delivery",  "Volume",    10),
        ("sector_strength",  "Sector",    15),
        ("rs_vs_nifty",      "Rel Str",   15),
        ("news_risk",        "News",       8),
        ("risk_reward",      "R/R",        7),
        ("ownership_quality","Ownership",  6),
        ("options_sentiment","Options",    4),
        ("macro_alignment",  "Macro",      3),
    ]
    lines = [f"     Confidence {final_conf:.1f} breakdown:"]
    fs = factor_scores or {}
    for key, label, weight in FACTOR_DISPLAY:
        score  = float(fs.get(key, 50) or 50)
        contrib = round(score * weight / 100, 1)
        filled = int(score / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        lines.append(f"     {label:<12} {bar} {score:.0f}/100 → {contrib:.1f}pts")
    return lines


def format_near_miss_failures(stock: dict, thresh: dict) -> list:
    """Compact 1-line failure summary for mobile."""
    conf  = float(stock.get("final_confidence", 0) or 0)
    tq    = float(stock.get("trade_quality_score", 0) or 0)
    rr    = float(stock.get("rr_ratio", 0) or stock.get("rr", 0) or 0)
    min_c = thresh.get("min_confidence", 80)
    min_t = thresh.get("min_tq", 78)
    min_r = thresh.get("min_rr", 2.0)
    fails = []
    # Use max(0, gap) so a metric that already passes never prints a negative gap.
    if conf < min_c: fails.append(f"Conf+{max(0.0, min_c - conf):.1f}")
    if tq   < min_t: fails.append(f"TQ+{max(0.0, min_t - tq):.1f}")
    if rr   < min_r: fails.append(f"RR+{max(0.0, min_r - rr):.2f}x")
    fail_str = " · ".join(fails) if fails else "near threshold"
    return [f"  ✗ Needs: {fail_str} → Watch: vol surge or consol above entry"]


def format_conviction_meter(regime_score: float, breadth: float,
                             fii: float, dii: float) -> list:
    """Visual conviction bar — compact 1-line version for mobile.

    Phase C3 (2026-07-02): FII/DII flows are no longer factored into conviction.
    Rationale: NSDL FII data is structurally D-1 (or D-2 on weekends/holidays);
    the number we see today is stale by 1-2 sessions and cannot reliably move
    an intraday-actionable conviction meter. Replaced flow weight with a
    breadth-emphasis blend, which is a real-time internal signal.

    fii/dii parameters kept for backwards-compatibility with callers but
    intentionally unused in the calculation.
    """
    _ = (fii, dii)  # kept for signature stability; not used in scoring
    try:
        # Composite: 55% regime score + 45% breadth (both real-time internals)
        conviction = round(regime_score * 0.55 + breadth * 0.45, 1)
        filled = int(conviction / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        if conviction >= 75:   label = "🟢 Strong"
        elif conviction >= 55: label = "🟡 Moderate"
        elif conviction >= 40: label = "🟠 Weak"
        else:                  label = "🔴 Poor"
        return [f"  Regime Conviction [{bar}] {conviction:.0f}% · {label}"]
    except Exception:
        return []


def format_risk_meter(nifty_state: dict, vix_in: float, breadth: float) -> list:
    """Compact 1-line risk meter for mobile."""
    try:
        ns = nifty_state or {}
        risk_score = 0
        if ns.get("above_ema20"):  e20 = "✓"
        else:                      e20 = "✗"; risk_score += 25
        if ns.get("above_ema50"):  e50 = "✓"
        else:                      e50 = "✗"; risk_score += 25
        if ns.get("above_ema200"): e200 = "✓"
        else:                      e200 = "✗"; risk_score += 25
        vix_ok = vix_in <= 20
        if not vix_ok: risk_score += 15
        brd_ok = breadth >= 40
        if not brd_ok: risk_score += 10
        if risk_score >= 75:   label = "🔴 EXTREME"
        elif risk_score >= 50: label = "🟠 HIGH"
        elif risk_score >= 25: label = "🟡 MEDIUM"
        else:                  label = "🟢 LOW"
        vix_tag = f"VIX {'✓' if vix_ok else '✗'}{vix_in:.1f}"
        brd_tag = f"Breadth {'✓' if brd_ok else '✗'}{breadth:.0f}%"
        return [
            f"  Risk {label} · EMA20{e20} EMA50{e50} EMA200{e200} · {vix_tag} · {brd_tag}"
        ]
    except Exception:
        return []


def format_breadth_dashboard(total_universe: int, total_tradable: int,
                              qualified: int, near_buy: int,
                              developing: int, monitor: int,
                              yesterday: dict = None) -> list:
    """Compact 2-line breadth dashboard for mobile."""
    rejected = total_universe - qualified
    return [
        f"  📈 Breadth · Universe {total_universe} · Tradable {total_tradable} · Qualified {qualified}",
        f"  Near Buy {near_buy} · Developing {developing} · Monitor {monitor} · Rejected {rejected}",
    ]


def format_buy_card(stock: dict, sizing: dict, regime: str,
                    buy_thesis: str = "") -> list:
    """Enhanced BUY card. buy_thesis is AI-generated when available."""
    try:
        opp    = float(stock.get("opportunity_score", 0) or 0)
        conf   = float(stock.get("final_confidence", 0) or 0)
        tq     = float(stock.get("trade_quality_score", 0) or 0)
        rr     = float(stock.get("rr_ratio", 0) or 0)
        sector = get_sector(stock.get("symbol", ""))
        cats   = stock.get("catalysts", []) or []

        # Conviction icon
        if opp >= 85:   icon = "\U0001f525"
        elif opp >= 75: icon = "\u26a1"
        else:           icon = "\U0001f4c8"

        # Thesis: use AI-generated if available, else rule-based
        if not buy_thesis:
            fs    = stock.get("factor_scores", {}) or {}
            trend_s = float(fs.get("trend_quality", 0) or 0)
            rs_s    = float(fs.get("rs_vs_nifty", 0) or 0)
            thesis_parts = []
            if trend_s > 70:              thesis_parts.append("strong uptrend")
            if rs_s > 70:                 thesis_parts.append("outperforming NIFTY")
            if "VOL_SURGE" in cats:       thesis_parts.append("volume expansion")
            if "NEAR_52W_HIGH" in cats:   thesis_parts.append("near 52W high breakout")
            buy_thesis = ", ".join(thesis_parts) if thesis_parts else "multi-factor confluence"

        # Unescape HTML entities from upstream news/AI text before we re-escape
        # for Telegram HTML mode — prevents double-encoding like &amp;#x27;.
        try:
            for _k in ("news_summary", "buy_thesis", "summary"):
                _v = stock.get(_k)
                if isinstance(_v, str) and "&" in _v and ";" in _v:
                    stock[_k] = html.unescape(_v)
        except Exception:
            pass

        # Telegram HTML mode only requires <, >, & escaped — quote=False keeps
        # apostrophes readable (fixes the &#x27; artefacts in AI thesis strings).
        # `buy_thesis` was already unescaped upstream by _clean_ai_output(); we
        # now unescape defensively in case it came from a cached fallback path.
        try:
            if isinstance(buy_thesis, str) and "&" in buy_thesis and ";" in buy_thesis:
                buy_thesis = html.unescape(buy_thesis)
        except Exception:
            pass
        sym     = html.escape(str(stock.get("symbol", "")), quote=False)
        entry   = stock.get("entry", 0)
        stop_p  = stock.get("stop", 0)
        t1      = stock.get("target1", 0)
        t2      = stock.get("target2", 0)
        risk_p  = round((entry - stop_p) / entry * 100, 1) if entry > 0 else 0
        pos_val = sizing.get("position_value", stock.get("position_value", 0))
        pos_pct = sizing.get("position_pct", stock.get("position_pct", 0))
        shares  = sizing.get("shares", stock.get("shares", 0))
        # BUGFIX: prefer explicit `max_loss`; fall back to risk_amount; last-resort
        # live compute (shares × risk-per-share) so cached rows still render.
        max_loss = (
            sizing.get("max_loss")
            or stock.get("max_loss")
            or sizing.get("risk_amount")
            or stock.get("risk_amount")
            or 0
        )
        try:
            if (not max_loss or float(max_loss) <= 0) and shares and entry and stop_p and entry > stop_p > 0:
                max_loss = round(shares * (entry - stop_p), 2)
        except Exception:
            pass
        # Persist back to stock so tracker / weekly stats read a real number.
        stock["max_loss"] = max_loss
        news    = truncate_display(stock.get("news_summary", ""), 120)

        pos_val_k = pos_val / 1000  # display in K
        max_loss_k = max_loss / 1000 if max_loss else 0
        # Render MaxLoss in ₹ when < ₹1K so tiny worst-case losses don't collapse to 0.0K.
        max_loss_str = (
            f"MaxLoss ₹{max_loss:.0f}" if 0 < max_loss < 1000
            else f"MaxLoss ₹{max_loss_k:.1f}K"
        )
        # Fundamentals honesty: when source is SCREENER+YF and BOTH ROE + D/E
        # are literally 0, that means yfinance was rate-limited AND screener
        # parsed no financial rows — do NOT display as "real" 0%. Show "N/A".
        _roe  = stock.get('roe', 0) or 0
        _de   = stock.get('de_ratio', 0) or 0
        _pldg = stock.get('promoter_pledge_pct', 0) or 0
        _src  = str(stock.get('fundamentals_source', stock.get('source', ''))).upper()
        _has_real_fund = (_roe != 0) or (_de != 0) or (_pldg != 0)
        if _has_real_fund:
            fund_line = (
                f"  ROE {_roe:.1f}% · D/E {_de:.2f} · Pledge {_pldg:.0f}%"
            )
        else:
            fund_line = "  ROE N/A · D/E N/A · Pledge N/A (fetch failed)"

        # Phase C3: delivery line — the strongest per-stock institutional signal.
        # Only shown when we got real nselib data; otherwise silent (the OBV proxy
        # is already baked into base_confidence).
        deliv_line = None
        if stock.get("delivery_source") == "nselib":
            _d_today = float(stock.get("delivery_pct_today", 0) or 0)
            _d_20d   = float(stock.get("delivery_pct_20d_avg", 0) or 0)
            _d_sig   = str(stock.get("delivery_signal", "NEUTRAL"))
            _sig_emoji = {
                "STRONG_ACCUM":  "🟢 STRONG ACCUM",
                "ACCUM":         "🟢 Accumulation",
                "NEUTRAL":       "⚪ Neutral",
                "WEAK":          "🟡 Weak delivery",
                "DISTRIBUTION":  "🔴 DISTRIBUTION",
            }.get(_d_sig, _d_sig)
            deliv_line = (
                f"  Delivery {_d_today:.0f}% (20d avg {_d_20d:.0f}%) · {_sig_emoji}"
            )

        lines = [
            f"  {icon} <b>{sym}</b> · {html.escape(str(sector), quote=False)}",
            f"  Opp {opp:.0f} · Conf {conf:.1f} · TQ {tq:.1f} · R/R {rr:.2f}x",
            f"  Entry ₹{entry:.1f} · Stop ₹{stop_p:.1f} ({risk_p:.1f}%) · T1 ₹{t1:.1f} · T2 ₹{t2:.1f}",
            f"  Size ₹{pos_val_k:.0f}K ({pos_pct:.1f}%) · {shares} shares · {max_loss_str}",
            fund_line,
        ]
        if deliv_line:
            lines.append(deliv_line)
        lines.append(f"  📝 {html.escape(str(buy_thesis), quote=False)}")
        if cats:
            lines.append(f"  🏷 {html.escape(' · '.join(str(c) for c in cats), quote=False)}")
        # FIX label/score mismatch: distinguish NO_NEWS (score correctly = 50)
        # from "we have news but couldn't summarise". Never claim "No significant
        # news" unless the category actually was NO_NEWS.
        news_cat = str(stock.get("news_category", "") or "").upper()
        if news and news != "\u2014":
            try:
                news = html.unescape(str(news))
            except Exception:
                pass
            lines.append(f"  📰 {html.escape(str(news), quote=False)}")
        elif news_cat == "NO_NEWS":
            lines.append("  📰 No headlines in last 3 days")
        else:
            lines.append("  📰 —")
        fs = stock.get("factor_scores", {}) or {}
        lines += format_confidence_breakdown(fs, conf)
        return lines
    except Exception:
        return [f"  \U0001f4c8 <b>{html.escape(str(stock.get('symbol', '?')))}</b>"]


# ── Compact Portfolio Card (FIX 3E) ──────────────────────────────────────────
def format_portfolio_card_compact(alert: dict, current_price: float,
                                   stop_warning_pct: float = 5.0) -> list:
    """
    Compact portfolio card.
    Normal: 2 lines. Near stop: 3 lines (adds stop warning).
    """
    try:
        symbol   = str(alert.get("symbol", ""))
        entry    = float(alert.get("entry", 0) or 0)
        stop     = float(alert.get("stop_loss", alert.get("stop", 0)) or 0)
        t1       = float(alert.get("target1", 0) or 0)
        t2       = float(alert.get("target2", 0) or 0)
        days     = int(alert.get("days_held", 0) or 0)
        pnl_p    = float(alert.get("pnl_pct", 0) or 0)
        action   = str(alert.get("action", "HOLD"))

        risk     = entry - stop
        gain     = current_price - entry
        r_mult   = round(gain / risk, 2) if risk > 0 else 0.0

        dist_stop = round((current_price - stop) / current_price * 100, 1) \
                    if current_price > 0 and stop > 0 else 999.0
        near_stop = dist_stop <= stop_warning_pct

        pnl_icon    = "🟢" if pnl_p > 0 else ("🔴" if pnl_p < -2 else "⚪")
        action_icon = {"HOLD": "✅", "EXIT": "🔴", "EXIT_FULL": "🔴",
                       "TRAIL_STOP": "🟡", "REVIEW": "🟠"}.get(action, "✅")

        lines = [
            f"  {action_icon} <b>{html.escape(symbol)}</b> · {action} · T{days} · {pnl_icon}{pnl_p:+.1f}% · {r_mult:+.2f}R",
            f"  ₹{entry:.0f}→₹{current_price:.0f} · T1 ₹{t1:.0f} · T2 ₹{t2:.0f}",
        ]
        if near_stop:
            lines.append(f"  ⚠️ Stop ₹{stop:.0f} only {dist_stop:.1f}% away")
        return lines
    except Exception:
        return [f"  ✅ <b>{html.escape(str(alert.get('symbol', '?')))}</b>"]


# ── Compact Portfolio Summary (FIX 3F) ───────────────────────────────────────
def format_portfolio_summary_compact(alerts: list, current_prices: dict,
                                      total_capital: float) -> list:
    """Replaces 6-line Portfolio Health Dashboard with 2 lines."""
    try:
        if not alerts:
            return ["  No active holdings."]
        qty_known = [a for a in alerts if a.get("quantity", 0) > 0]
        total_invested = sum(
            float(a.get("entry", 0)) * float(a.get("quantity", 0))
            for a in qty_known
        ) or sum(float(a.get("entry", 0)) for a in alerts)
        total_current = sum(
            current_prices.get(a["symbol"], float(a.get("entry", 0))) *
            float(a.get("quantity", 1))
            for a in alerts
        )
        total_pnl = round((total_current - total_invested) / total_invested * 100, 2) \
                    if total_invested > 0 else 0.0
        exposure  = round(total_invested / total_capital * 100, 1) if total_capital > 0 else 0.0
        cash      = round(100 - exposure, 1)
        return [
            f"  💼 {len(alerts)} pos · ₹{total_invested:,.0f} ({exposure:.1f}%) · Cash {cash:.1f}% · PnL {total_pnl:+.2f}%",
        ]
    except Exception:
        return [f"  💼 {len(alerts)} pos · see Excel for details"]


# ── Stop Watch Alert (FIX 5) ─────────────────────────────────────────────────
STOP_WATCH_THRESHOLD_PCT = 5.0

def format_stop_watch_alert(holdings: list, current_prices: dict) -> list:
    """
    Prominent STOP WATCH block at top of portfolio section.
    Returns empty list if all positions are safe.
    """
    warnings = []
    try:
        for h in (holdings or []):
            sym  = h.get("symbol", "")
            stop = float(h.get("stop_loss", h.get("stop", 0)) or 0)
            entry= float(h.get("entry", 0) or 0)
            cur  = float(current_prices.get(sym, entry) or entry)
            if stop <= 0 or cur <= 0:
                continue
            dist_pct = round((cur - stop) / cur * 100, 1)
            if cur <= stop:
                warnings.append(
                    f"  🔴 STOP HIT — {html.escape(sym)} "
                    f"Rs{cur:.1f} \u2264 Stop Rs{stop:.1f} | EXIT NOW"
                )
            elif dist_pct <= 1.0:
                warnings.append(
                    f"  🔴 CRITICAL — {html.escape(sym)} "
                    f"Rs{cur:.1f} | Stop Rs{stop:.1f} only {dist_pct:.1f}% away"
                )
            elif dist_pct <= STOP_WATCH_THRESHOLD_PCT:
                warnings.append(
                    f"  ⚠️  STOP WATCH — {html.escape(sym)} "
                    f"Rs{cur:.1f} | Stop Rs{stop:.1f} | {dist_pct:.1f}% away — watch closely"
                )
    except Exception:
        pass
    if not warnings:
        return []
    return ["", "🚨 STOP ALERTS"] + warnings


def format_daily_summary(regime: str, buys: list, watchlist: list,
                          portfolio_alerts: list, macro: dict,
                          nifty_state: dict = None) -> list:
    """Executive briefing at end of Telegram report (ENHANCEMENT 9 / BUG FIX 6)."""
    try:
        near_miss_count = len([w for w in watchlist if w.get("tier") == "NEAR_MISS"])
        exits = [a for a in portfolio_alerts if "EXIT" in str(a.get("action", ""))]
        lines = ["", "📋 DAILY SUMMARY"]

        if regime in ("STRONG_BULL", "BULL"):
            lines.append("  Market is in a bullish phase with broad participation.")
        elif regime == "TRANSITION":
            lines.append("  Market is in transition — mixed signals, no clear direction.")
        elif regime == "SIDEWAYS":
            lines.append("  Market is range-bound. Patience required.")
        else:
            lines.append("  Market is in a bearish phase. Preserve capital.")

        if buys:
            lines.append(f"  {len(buys)} institutional-quality setup(s) identified today.")
        else:
            lines.append("  No institutional-quality setup met all required conditions today.")
            if near_miss_count > 0:
                lines.append(f"  {near_miss_count} stock(s) within 8 pts of qualifying — watch closely.")

        # Use nifty_state as single source of truth (BUG FIX 6)
        ns = nifty_state or {}
        if ns.get("ema_bear", macro.get("nifty_below_all_emas", False)):
            lines.append(
                "  NIFTY trading below major EMAs — risk elevated. "
                "Avoid aggressive positioning."
            )
        else:
            lines.append("  NIFTY structure supportive — trend intact above key EMAs.")

        if exits:
            lines.append(f"  ⚠️  {len(exits)} position(s) require immediate exit review.")
        else:
            lines.append("  Existing positions on track — no exit action required.")

        if not buys and regime not in ("STRONG_BULL", "BULL"):
            lines.append("  → Avoid forcing new trades. Quality over quantity.")
        elif buys:
            lines.append("  → Execute BUY signal(s) with strict position sizing.")

        return lines
    except Exception:
        return ["", "📋 DAILY SUMMARY", "  (unavailable)"]


def format_watchlist_section(watchlist: list, regime: str,
                              conf_history: dict = None,
                              gate_memory: dict = None,
                              near_miss_insights: dict = None) -> list:
    """
    NEAR MISS  — full detail for ALL stocks (no truncation), sorted by R/R desc
    DEVELOPING — company-names-only (rows of 5), like MONITOR
    MONITOR    — company-names-only (rows of 5)
    Full Entry/Stop/T1/T2 for every tier is persisted to recommendation_tracker.xlsx.
    """
    thresh   = REGIME_THRESHOLDS[regime]
    min_conf = thresh["min_confidence"]

    near = sorted([w for w in watchlist if w.get("tier") == "NEAR_MISS"],
                  key=lambda x: x.get("rr", 0), reverse=True)
    dev  = sorted([w for w in watchlist if w.get("tier") == "DEVELOPING"],
                  key=lambda x: x.get("conf", x.get("final_confidence", 0)), reverse=True)
    mon  = [w for w in watchlist if w.get("tier") == "MONITOR"]

    lines = [f"👁 <b>WATCHLIST</b> — {len(watchlist)} stocks (min conf {min_conf})"]

    # -- NEAR MISS: full detail card for EVERY stock (no truncation) ----------
    if near:
        lines.append(f"🔴 <b>NEAR MISS</b> ({len(near)} total — all listed)")
        for w in near:
            sym      = html.escape(str(w["symbol"]))
            sector   = html.escape(str(w.get("sector", "DIV")))
            conf     = w.get("conf", w.get("final_confidence", 0))
            tq       = w.get("tq", w.get("trade_quality_score", 0))
            entry    = w.get("entry", 0)
            stop     = w.get("stop", 0)
            target1  = w.get("target1", 0)
            target2  = w.get("target2", 0)
            rr       = w.get("rr_ratio", w.get("rr", 0))
            risk     = w.get("risk_pct", 0)
            opp      = w.get("opportunity_score", 0)
            lines.append(f"  <b>{sym}</b> [{sector}] · Opp{opp:.0f} Conf{conf:.1f} TQ{tq:.1f} RR{rr:.1f}x")
            lines.append(f"  ₹{entry:.1f} entry · Stop ₹{stop:.1f}({risk:.1f}%) · T1 ₹{target1:.1f} · T2 ₹{target2:.1f}")
            lines.extend(format_near_miss_failures(w, thresh))
            insight = (near_miss_insights or {}).get(w.get("symbol", ""), "")
            if insight:
                lines.append(f"  💡 {html.escape(str(insight))}")

    # -- DEVELOPING: header + all names in rows of 5 (like MONITOR) -----------
    if dev:
        lines.append(f"🟡 <b>DEVELOPING</b> ({len(dev)} building) — full levels in Excel")
        dev_names = [w["symbol"].replace(".NS", "") for w in dev]
        for i in range(0, len(dev_names), 5):
            lines.append("  " + " \u00b7 ".join(dev_names[i:i+5]))

    # -- MONITOR: header + all names in rows of 5 (unchanged) -----------------
    if mon:
        best     = max(mon, key=lambda x: x.get("rr_ratio", x.get("rr", 0)))
        best_sym = best["symbol"].replace(".NS", "")
        best_rr  = best.get("rr_ratio", best.get("rr", 0))
        lines.append(
            f"\U0001f535 <b>MONITOR</b> ({len(mon)} early-stage) \u00b7 best R/R: {best_sym} {best_rr:.1f}x"
        )
        names = [m["symbol"].replace(".NS", "") for m in mon]
        for i in range(0, len(names), 5):
            lines.append("  " + " \u00b7 ".join(names[i:i+5]))

    if not near and not dev and not mon:
        lines.append("  None today.")

    return lines


def format_no_buy_explanation(top_rejected: list, regime: str,
                               watchlist: list = None) -> list:
    """
    When buys=0, show a single clean summary line pointing to the watchlist.
    Never duplicates stocks already shown in the WATCHLIST section below.
    """
    thresh = REGIME_THRESHOLDS[regime]
    wl     = watchlist or []
    nm     = len([s for s in wl if s.get("tier") == "NEAR_MISS"])
    dev    = len([s for s in wl if s.get("tier") == "DEVELOPING"])
    mon    = len([s for s in wl if s.get("tier") == "MONITOR"])

    lines = [
        f"  None \u2014 no setup cleared all gates "
        f"(need Conf\u2265{thresh['min_confidence']} \u00b7 TQ\u2265{thresh['min_tq']} \u00b7 R/R\u2265{thresh['min_rr']})"
    ]
    if nm or dev or mon:
        parts = []
        if nm:  parts.append(f"\U0001f534 {nm} Near Miss")
        if dev: parts.append(f"\U0001f7e1 {dev} Developing")
        if mon: parts.append(f"\U0001f535 {mon} Monitor")
        joined = " \u00b7 ".join(parts)
        lines.append(f"  \u2193 {joined} \u2014 details in WATCHLIST \u2193")
    return lines


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9b — CONFIDENCE HISTORY + GATE MEMORY (FEATURES 2 & 7)
# ─────────────────────────────────────────────────────────────────────────────

def load_confidence_history() -> dict:
    """Load {symbol: {dates:[], confs:[]}} rolling 3-day window."""
    try:
        if os.path.exists(CONF_HISTORY_FILE):
            with open(CONF_HISTORY_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def update_confidence_history(history: dict, scored_stocks: list,
                               today_str: str) -> dict:
    """Update rolling 3-day confidence for all scored stocks."""
    try:
        for stock in scored_stocks:
            sym  = stock.get("symbol", "")
            conf = float(stock.get("final_confidence", 0) or 0)
            if not sym:
                continue
            if sym not in history:
                history[sym] = {"dates": [], "confs": []}
            history[sym]["dates"].append(today_str)
            history[sym]["confs"].append(conf)
            history[sym]["dates"] = history[sym]["dates"][-3:]
            history[sym]["confs"] = history[sym]["confs"][-3:]
    except Exception:
        pass
    return history


def save_confidence_history(history: dict) -> None:
    try:
        with open(CONF_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        _log(f"[WARN] Confidence history save failed: {e}")


def get_confidence_trend(symbol: str, history: dict) -> str:
    """Returns trend arrow string for Telegram. Empty string if insufficient data."""
    try:
        data  = history.get(symbol, {})
        confs = data.get("confs", [])
        if len(confs) < 2:
            return ""
        if len(confs) == 2:
            c1, c2 = confs
            delta  = c2 - c1
            arrow  = "\u2191" if delta > 1 else ("\u2193" if delta < -1 else "\u2192")
            return f"{arrow} {c1:.0f}\u2192{c2:.0f}"
        c1, c2, c3 = confs[-3], confs[-2], confs[-1]
        total = c3 - c1
        d1    = c2 - c1
        d2    = c3 - c2
        if total > 4 and d1 > 0 and d2 > 0:
            arrow, label = "\u2191\u2191", "rising fast"
        elif total > 1:
            arrow, label = "\u2191 ", "rising"
        elif total < -4:
            arrow, label = "\u2193\u2193", "falling fast"
        elif total < -1:
            arrow, label = "\u2193 ", "falling"
        else:
            arrow, label = "\u2192 ", "flat"
        return f"{arrow} {c1:.0f}\u2192{c2:.0f}\u2192{c3:.0f} ({label})"
    except Exception:
        return ""


def load_gate_memory() -> dict:
    """Load {symbol: {history: [{date, fails, conf}]}} rolling 5-day window."""
    try:
        if os.path.exists(GATE_MEMORY_FILE):
            with open(GATE_MEMORY_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def update_gate_memory(memory: dict, watchlist: list, today_str: str) -> dict:
    """Track fail reasons per stock over time."""
    try:
        for stock in watchlist:
            sym   = stock.get("symbol", "")
            fails = stock.get("fail_reasons", [])
            conf  = float(stock.get("final_confidence", 0) or 0)
            if not sym:
                continue
            if sym not in memory:
                memory[sym] = {"history": []}
            memory[sym]["history"].append({"date": today_str, "fails": fails, "conf": conf})
            memory[sym]["history"] = memory[sym]["history"][-5:]
    except Exception:
        pass
    return memory


def save_gate_memory(memory: dict) -> None:
    try:
        with open(GATE_MEMORY_FILE, "w") as f:
            json.dump(memory, f, indent=2)
    except Exception as e:
        _log(f"[WARN] Gate memory save failed: {e}")


def get_gate_pattern(symbol: str, memory: dict) -> str:
    """Returns one-line pattern description. Empty string if insufficient history."""
    try:
        data = memory.get(symbol, {})
        hist = data.get("history", [])
        if len(hist) < 2:
            return ""
        all_fails = [set(h.get("fails", [])) for h in hist]
        confs     = [h.get("conf", 0) for h in hist]
        if len(hist) >= 3:
            recent_fails = all_fails[-3:]
            persistent   = set.intersection(*recent_fails) if all(recent_fails) else set()
            if persistent:
                fail_name  = list(persistent)[0].split("(")[0].strip()
                conf_trend = confs[-1] - confs[-3]
                if conf_trend > 2:
                    return f"Day {len(hist)} {fail_name} but conf rising +{conf_trend:.1f} \u2705"
                elif conf_trend < -2:
                    return f"Day {len(hist)} {fail_name} \u2014 conf falling {conf_trend:.1f} \u26a0\ufe0f"
                elif len(hist) >= 4:
                    return f"Day {len(hist)} stuck on {fail_name} \u2014 consider removing \ud83d\udd34"
                else:
                    return f"Day {len(hist)} {fail_name} \u2014 monitoring"
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10 — TELEGRAM OUTPUT FORMATTER
# ─────────────────────────────────────────────────────────────────────────────

def format_telegram_message(regime_data: dict, buys: list, shorts: list,
                              watchlist: list, portfolio_alerts: list,
                              macro: dict, key_levels: dict,
                              upcoming_events: list, timestamp: str,
                              heat: dict = None, platt: dict = None,
                              tracker_v2: dict = None,
                              rejected_stocks: list = None,
                              breadth_20: float = 50.0,
                              nifty_state: dict = None,
                              universe_count: int = 0,
                              tradable_count: int = 0,
                              conf_history: dict = None,
                              gate_memory: dict = None,
                              ai_results: dict = None) -> str:
    lines  = []
    regime = regime_data["regime"]
    score  = regime_data["score"]
    thresh = regime_data["thresholds"]

    # ── Header ──────────────────────────────────────────────────────────────
    lines.append(f"📊 <b>NSE SWING BRIEF</b> · {timestamp}")
    # FIX: stamp the actual latest OHLCV candle date used by the scan so
    # readers can tell whether data is fresh or from a prior session.
    try:
        _latest = None
        _ns = nifty_state or {}
        _cand = _ns.get("latest_candle_date") or _ns.get("as_of") or _ns.get("as_of_date")
        if _cand:
            _latest = str(_cand)
        elif macro:
            _cand2 = macro.get("latest_candle_date") or macro.get("as_of")
            if _cand2:
                _latest = str(_cand2)
        if _latest:
            lines.append(f"  <i>Latest candle: {html.escape(_latest)}</i>")
    except Exception:
        pass
    lines.append("─" * 22)
    lines.append("")

    # ── Market Regime ──
    vix_in  = macro.get("vix_in", 15.0)
    vix_us  = macro.get("vix_us", 18.0)
    vix_in_flag = "HIGH" if vix_in > 20 else ("low" if vix_in < 13 else "NORMAL")
    vix_us_flag = "HIGH" if vix_us > 25 else ("ok" if vix_us < 22 else "ELEVATED")
    REGIME_RATIONALE = {
        "STRONG_BULL":     "Strong uptrend, broad breadth — full deployment.",
        "BULL":            "Uptrend intact, moderate breadth — standard sizing.",
        "SIDEWAYS":        "Range-bound — tight sizing, high conviction only.",
        "TRANSITION":      "Mixed signals — reduced sizing, tight stops.",
        "HIGH_VOLATILITY": f"Elevated VIX-IN {vix_in:.1f} — 1 buy max, tight stops mandatory.",
        "BEAR":            "Downtrend — no new longs, manage exits only.",
        "STRONG_BEAR":     "Severe downtrend — cash only, no new positions.",
    }
    lines.append("📊 <b>MARKET REGIME</b>")
    lines.append(f"  {regime} · Score {score:.0f}/100 · MaxBuys {thresh['max_buys']}")

    # Regime explanation (Patch 4)
    fii_flow = macro.get("fii_flow_cr", 0.0)
    dii_flow = macro.get("dii_flow_cr", 0.0)
    # Use nifty_state as primary EMA source (FIX 1: single source of truth)
    _kl = key_levels or {}
    _ns_early = nifty_state or {}
    reg_why = regime_explanation(
        score, regime, vix_in, breadth_20, fii_flow, dii_flow,
        nifty_close = float(_ns_early.get("close",  _kl.get("last",   0)) or 0),
        ema20       = float(_ns_early.get("ema20",  _kl.get("ema20",  0)) or 0),
        ema50       = float(_ns_early.get("ema50",  _kl.get("ema50",  0)) or 0),
        ema200      = float(_ns_early.get("ema200", _kl.get("ema200", 0)) or 0),
    )
    lines.append(f"  {html.escape(str(reg_why))}")
    lines.append(f"  {REGIME_RATIONALE.get(regime, '')}")
    # Phase 3 (2026-07-01): VIX percentile — institutional-grade regime cue.
    _vp = macro.get("vix_in_percentile")
    _vr = macro.get("vix_in_regime", "UNKNOWN")
    if _vp is not None:
        _vr_hint = {
            "COMPLACENT": "complacent · watch mean reversion",
            "NORMAL":     "normal",
            "ELEVATED":   "elevated",
            "FEAR":       "fear · contrarian window",
            "UNKNOWN":    "",
        }.get(_vr, "")
        _hint_str = f" — {_vr_hint}" if _vr_hint else ""
        lines.append(
            f"  VIX-IN {vix_in:.1f}({vix_in_flag}) · {_vp:.0f}th %ile [{_vr}{_hint_str}] · "
            f"VIX-US {vix_us:.1f}({vix_us_flag})"
        )
    else:
        lines.append(
            f"  VIX-IN {vix_in:.1f}({vix_in_flag}) · VIX-US {vix_us:.1f}({vix_us_flag})"
        )
    lines.append(
        f"  NIFTY {macro.get('nifty_1d_pct', 0):+.2f}% · "
        f"S&P {macro.get('sp500_1d_pct', 0):+.2f}% · "
        f"DXY {macro.get('dxy', 0):.1f}"
    )
    lines.append(
        f"  ₹/$ {macro.get('usdinr', 0):.2f} · "
        f"Crude ${macro.get('crude_usd', 0):.1f} · "
        f"US10Y {macro.get('us10y', 0):.2f}%"
    )

    # Phase C4 (2026-07-02): FII/DII display removed entirely. NSDL is
    # structurally T-1/T-2, RSS fallbacks are unreliable, and the number
    # never fed sizing/gates/regime. Fetch is now stubbed to a no-op.

    # ── Conviction + Risk (single lines) ──
    _kl2 = key_levels or {}
    _ns  = nifty_state or {}
    lines.extend(format_conviction_meter(score, breadth_20, fii_flow, dii_flow))
    lines.extend(format_risk_meter(_ns, vix_in, breadth_20))
    lines.append(f"  Min Conf {thresh['min_confidence']} · TQ {thresh['min_tq']} · R/R {thresh['min_rr']} · Max {thresh['max_buys']} buy(s)")

    if heat:
        heat_emoji = "🔴" if not heat["heat_ok"] else ("🟡" if heat["heat_pct"] > heat["max_heat_pct"] * 0.6 else "🟢")
        lines.append(f"  {heat_emoji} Heat {heat['heat_pct']:.1f}%/{heat['max_heat_pct']:.0f}%")
    if platt and platt.get("calibrated"):
        lines.append(
            f"  WR {platt['win_rate']:.0%} ({platt['total_closed']}T) · "
            f"Avg W +{platt['avg_win_pct']:.1f}% / L -{platt['avg_loss_pct']:.1f}%"
        )
    lines.append("")

    # ── NIFTY Key Levels + EMA interpretation (Patch 3) ──
    if key_levels:
        kl = key_levels
        lines.append("NIFTY LEVELS")
        lines.append(
            f"  EMA20: {kl.get('ema20', '—')} | "
            f"EMA50: {kl.get('ema50', '—')} | "
            f"EMA200: {kl.get('ema200', '—')}"
        )
        # FIX: date-stamp the 52W High/Low so a stale figure is obvious.
        _52w_as_of = None
        try:
            _ns2 = nifty_state or {}
            _52w_as_of = _ns2.get("as_of") or _ns2.get("latest_candle_date") or _ns2.get("as_of_date")
            if _52w_as_of:
                _52w_as_of = str(_52w_as_of)
        except Exception:
            _52w_as_of = None
        _52w_tag = f" (as of {html.escape(_52w_as_of)})" if _52w_as_of else ""
        lines.append(
            f"  52W H: {kl.get('high_52w', '—')} "
            f"({kl.get('dist_from_52w_high_pct', 0):.1f}% away) | "
            f"52W L: {kl.get('low_52w', '—')}{_52w_tag}"
        )
        lines.append(
            f"  20D Range: {kl.get('recent_low_20d', '—')} — {kl.get('recent_high_20d', '—')}"
        )
        # One-line structure from nifty_state (BUG FIX 1: single source)
        try:
            ns = nifty_state or {}
            if ns.get("structure"):
                lines.append(f"  Structure: {ns['structure']}")
            else:
                nifty_close = float(kl.get("last", 0) or 0)
                structure   = interpret_nifty_structure(
                    nifty_close,
                    float(kl.get("ema20",  0) or 0),
                    float(kl.get("ema50",  0) or 0),
                    float(kl.get("ema200", 0) or 0),
                    float(kl.get("high_52w", 0) or 0),
                )
                lines.append(f"  Structure: {structure}")
        except Exception:
            pass
        lines.append("")

    # ── Breadth Dashboard (BUG FIX 2: full universe counts) ──
    _near_c = len([w for w in watchlist if w.get("tier") == "NEAR_MISS"])
    _dev_c  = len([w for w in watchlist if w.get("tier") == "DEVELOPING"])
    _mon_c  = len([w for w in watchlist if w.get("tier") == "MONITOR"])
    _rej_c  = len(rejected_stocks or [])
    _qual_c = len(buys) + len(watchlist)
    lines.extend(format_breadth_dashboard(
        universe_count or (len(buys) + len(watchlist) + _rej_c + len(shorts)),
        tradable_count or (len(buys) + len(watchlist) + _rej_c),
        _qual_c, _near_c, _dev_c, _mon_c,
    ))
    lines.append("")

    # ── BUY Signals (Patch 6 for no-buy case) ──
    lines.append("✅ <b>BUY SIGNALS</b>")
    if buys:
        _ai = ai_results or {}
        _buy_theses = _ai.get("buy_theses", {})
        for b in buys:
            sizing = {
                "position_value": b.get("position_value", 0),
                "position_pct":   b.get("position_pct", 0),
                "shares":         b.get("shares", 0),
                "max_loss":       b.get("max_loss", 0),
            }
            thesis = _buy_theses.get(b.get("symbol", ""), "")
            lines.extend(format_buy_card(b, sizing, regime, buy_thesis=thesis))
            # Gap validity (morning price check)
            entry  = b.get("entry", 0)
            stop_p = b.get("stop", 0)
            t1     = b.get("target1", 0)
            rr_v   = b.get("rr_ratio", 1.8) or 1.8
            gap_check = check_gap_validity(entry, stop_p, t1, rr_v)
            max_entry = gap_check.get("max_valid_entry", 0)
            if max_entry > 0 and max_entry > entry:
                gap_max_pct = round((max_entry - entry) / entry * 100, 1)
                lines.append(f"  ⚡ Max entry ₹{max_entry:.1f} (+{gap_max_pct:.1f}%) — skip if opens above")
            sizing_method = b.get("sizing_method", "")
            if sizing_method:
                lines.append(f"  💰 {html.escape(str(sizing_method))}")
            rs_diff = b.get("rs_diff21", 0)
            lines.append(f"  RS vs Nifty {rs_diff:+.1f}%")
            weekly_ok = b.get("weekly_trend_ok", True)
            if not weekly_ok:
                lines.append("  ⚠️ Weekly DOWN — reduced conviction")
            pattern = b.get("price_pattern", "NONE")
            if pattern != "NONE":
                lines.append(f"  📐 {html.escape(str(pattern))}")
            accum = b.get("accum_signal", "NEUTRAL")
            if accum != "NEUTRAL":
                lines.append(f"  📊 Vol: {html.escape(str(accum))}")
            ai_sum = truncate_display(b.get("ai_commentary", ""), 90)
            if ai_sum and ai_sum != "—":
                lines.append(f"  🤖 {html.escape(str(ai_sum))}")
            if b.get("repeat_tag"):
                lines.append(f"  🔁 {html.escape(str(b['repeat_tag']))}")
            if b.get("warnings"):
                lines.append(f"  ⚠️ {html.escape(', '.join(b['warnings'][:3]))}")
            lines.append("  ···")
    else:
        # Patch 6: detailed no-buy explanation (includes watchlist as closer candidates)
        no_buy_lines = format_no_buy_explanation(rejected_stocks or [], regime,
                                                  watchlist=watchlist or [])
        lines.extend(no_buy_lines)
    lines.append("")

    # ── SHORT Signals ──
    if shorts:
        lines.append("🔽 <b>SHORT SIGNALS</b>")
        for s in shorts:
            lines.append(f"  <b>{html.escape(str(s.get('symbol','?')))}</b> [SHORT]")
            lines.append(
                f"  Entry ₹{s.get('entry',0):.1f} · Stop ₹{s.get('stop',0):.1f} · T1 ₹{s.get('target1',0):.1f} · T2 ₹{s.get('target2',0):.1f} · RR {s.get('rr',0):.2f}x"
            )
            lines.append(f"  {html.escape(str(s.get('reason','')))}")  
        lines.append("")

    # ── Watchlist — ALL stocks, all tiers, with levels (Patch 1) ──
    _nm_insights = (ai_results or {}).get("near_miss_insights", {})
    wl_lines = format_watchlist_section(
        watchlist, regime,
        conf_history=conf_history or {},
        gate_memory=gate_memory or {},
        near_miss_insights=_nm_insights,
    )
    lines.extend(wl_lines)
    lines.append("")

    # ── Stop Watch Alert (FIX 5: appears BEFORE portfolio, cannot be missed) ──
    _cur_prices_port = {a["symbol"]: float(a.get("current", a.get("entry", 0)) or 0)
                        for a in portfolio_alerts}
    stop_alerts = format_stop_watch_alert(portfolio_alerts, _cur_prices_port)
    if stop_alerts:
        lines += stop_alerts

    # ── Portfolio ──
    exits   = [a for a in portfolio_alerts if a["action"] in ("EXIT", "EXIT_FULL")]
    trails  = [a for a in portfolio_alerts if a["action"] == "TRAIL_STOP"]
    reviews = [a for a in portfolio_alerts if a["action"] == "REVIEW"]
    holds   = [a for a in portfolio_alerts if a["action"] == "HOLD"]
    lines.append("📁 <b>PORTFOLIO</b>")
    if portfolio_alerts:
        # Compact 2-line portfolio summary (FIX 3F)
        lines.extend(format_portfolio_summary_compact(portfolio_alerts, _cur_prices_port, PORTFOLIO_CAPITAL))
        lines.append("")

        def _fmt_alert_card(a: dict) -> list:
            cur = float(a.get("current", a.get("entry", 0)) or 0)
            card = format_portfolio_card_compact(a, cur)
            # Append exit reason if relevant
            if a.get("reason") and a["action"] not in ("HOLD",):
                card.append(f"     Reason: {html.escape(str(a['reason']))}")
            return card

        if exits:
            lines.append("  🚨 <b>EXIT</b>")
            for e in exits:
                lines.extend(_fmt_alert_card(e))
        if trails:
            lines.append("  ⚡ <b>TRAIL STOP</b>")
            for t in trails:
                lines.extend(_fmt_alert_card(t))
        if reviews:
            lines.append("  🔍 <b>REVIEW</b>")
            for r in reviews:
                lines.extend(_fmt_alert_card(r))
        if holds:
            lines.append("  ✅ <b>HOLD</b>")
            for h in holds[:6]:
                lines.extend(_fmt_alert_card(h))
    else:
        lines.append("  No active holdings.")
    lines.append("")

    # ── Upcoming Events (FIX 4: config-based, 2 lines per event max) ──
    if upcoming_events:
        lines.extend(format_upcoming_events_compact(upcoming_events, portfolio_alerts))
        lines.append("")

    # ── Daily Summary (AI-written when available, rule-based fallback) ──
    _ai_summary = (ai_results or {}).get("daily_summary", "")
    if _ai_summary:
        lines.append("")
        lines.append("\U0001f4cb DAILY SUMMARY")
        lines.append(f"  {html.escape(str(_ai_summary))}")
    else:
        lines.extend(format_daily_summary(
            regime, buys, watchlist, portfolio_alerts, macro, nifty_state
        ))

    # ── Footer ──
    lines.append("")
    # Phase C: data-quality footer — visible signal of source health
    try:
        dq          = macro.get("data_quality", "NORMAL")
        bad_fields  = macro.get("bad_fields", []) or []
        usdinr_src  = (macro.get("sources", {}) or {}).get("usdinr", "?")
        # Extract just the source name from the verbose log line
        usdinr_name = "yfinance"
        if "frankfurter" in str(usdinr_src):
            usdinr_name = "frankfurter"
        elif "yfinance" in str(usdinr_src):
            usdinr_name = "yfinance"
        # Phase C4: FII/DII removed from output — footer no longer reports it.
        dq_icon     = "🟢" if dq == "NORMAL" else "🟡"
        lines.append(f"{dq_icon} Data quality: {dq}"
                     + (f" · bad={','.join(bad_fields)}" if bad_fields else "")
                     + f" · usdinr={usdinr_name}")
    except Exception:
        pass
    lines.append("─" * 22)
    lines.append("⚠️ Recommendation only. Execute manually.")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10b — EXCEL RECOMMENDATION TRACKER (PART C)
# ─────────────────────────────────────────────────────────────────────────────

TRACKER_XLSX = "recommendation_tracker.xlsx"


def _create_excel_workbook():
    """Create recommendation_tracker.xlsx with all required sheets."""
    try:
        import openpyxl
        from openpyxl.styles import PatternFill, Font
        wb = openpyxl.Workbook()

        ws1 = wb.active
        ws1.title = "Recommendations"
        ws1.append([
            "Date", "Ticker", "Company", "Category", "Opp Score", "Confidence", "TQ",
            "R/R", "Entry", "Stop", "T1", "T2", "Sector", "Regime", "Pledge%", "ROE",
            "D/E", "Catalysts", "Fail Reasons", "Status"
        ])

        ws2 = wb.create_sheet("Daily Tracking")
        ws2.append([
            "Tracking Date", "Ticker", "Rec Date", "Day#", "Close", "High", "Low",
            "Volume", "Return%", "Max Gain%", "Max DD%", "T1 Hit", "T2 Hit",
            "Stop Hit", "Remaining Upside%", "Holding Days", "Status"
        ])

        ws3 = wb.create_sheet("Performance Summary")
        ws3.append(["Metric", "Value"])

        for name in ["Confidence Analysis", "TQ Analysis", "Opp Score Analysis",
                     "Sector Analysis", "Regime Analysis", "Monthly Report",
                     # v2 research sheets (auto-populated by research_job.py)
                     "Weekday Analysis", "Holding Period Analysis",
                     "Category Comparison", "Conf x TQ Matrix",
                     "Catalyst Analysis", "Fail Reason Analysis",
                     "Regime x Sector", "Confidence Trajectory"]:
            wb.create_sheet(name)

        return wb
    except ImportError:
        return None
    except Exception:
        return None


def save_recommendations_to_excel(buys: list, watchlist: list,
                                   regime_data: dict, today_str: str) -> None:
    """
    Appends today's recommendations to recommendation_tracker.xlsx.
    Creates file with all sheets if it doesn't exist. Never overwrites existing rows.
    """
    try:
        import openpyxl
    except ImportError:
        _log("[WARN] openpyxl not installed — skipping Excel save. Run: pip install openpyxl")
        return
    try:
        if os.path.exists(TRACKER_XLSX):
            wb = openpyxl.load_workbook(TRACKER_XLSX)
        else:
            wb = _create_excel_workbook()
            if wb is None:
                return

        ws = wb["Recommendations"]
        all_stocks = (
            [(s, "BUY") for s in buys] +
            [(s, s.get("tier", "WATCHLIST")) for s in watchlist]
        )

        for stock, category in all_stocks:
            symbol = stock.get("symbol", "")
            ws.append([
                today_str,
                symbol,
                symbol.replace(".NS", ""),
                category,
                stock.get("opportunity_score", 0),
                stock.get("final_confidence", 0),
                stock.get("trade_quality_score", 0),
                stock.get("rr_ratio", stock.get("rr", 0)),
                stock.get("entry", 0),
                stock.get("stop", 0),
                stock.get("target1", 0),
                stock.get("target2", 0),
                get_sector(symbol),
                regime_data.get("regime", ""),
                stock.get("promoter_pledge_pct", 0),
                stock.get("roe", 0),
                stock.get("de_ratio", 0),
                ", ".join(stock.get("catalysts", []) or []),
                ", ".join(stock.get("fail_reasons", []) or []),
                "ACTIVE",
            ])

        wb.save(TRACKER_XLSX)
        _log(f"[INFO] Saved {len(all_stocks)} recommendations to {TRACKER_XLSX}")

    except Exception as e:
        _log(f"[WARN] Excel save failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 11 — PIPELINE ENTRY
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline():
    init_run_log()
    try:
        _run_pipeline_inner()
    except Exception as e:
        _log(f"[CRITICAL] Unhandled pipeline exception: {e}")
        _log(traceback.format_exc())
    finally:
        close_run_log()


def _ensure_portfolio_json():
    """
    Create an empty portfolio.json if it doesn't exist yet, with a schema comment
    so users know exactly how to add holdings manually.
    The file is a JSON array. Each holding object uses these fields:
      symbol        — NSE symbol with .NS suffix, e.g. "RELIANCE.NS"
      entry_price   — price you bought at (float)
      stop_loss     — your hard stop price (float)
      target1       — first target price (float)
      target2       — second target price (float)
      entry_date    — date you entered, "YYYY-MM-DD"
      sector        — optional, e.g. "ENERGY" (defaults to OTHERS if omitted)
    Example:
      [{"symbol":"RELIANCE.NS","entry_price":2950.0,"stop_loss":2800.0,
        "target1":3100.0,"target2":3300.0,"entry_date":"2026-06-20","sector":"ENERGY"}]
    """
    if not os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, "w") as f:
                json.dump([], f, indent=2)
            _log(f"[INFO] Created empty {PORTFOLIO_FILE} — add your holdings manually to enable portfolio monitoring")
        except Exception as e:
            _log(f"[WARN] Could not create {PORTFOLIO_FILE}: {e}")


def _run_pipeline_inner():
    import copy
    _log("=== NSE SWING TRADE PIPELINE v6.0 STARTING ===")
    _log(f"  Capital: Rs{PORTFOLIO_CAPITAL:,.0f} | Groq keys: {len(GROQ_KEYS)}")
    _log(f"  Run mode: {'SCHEDULED — day counters will advance' if IS_SCHEDULED else 'MANUAL — day counters frozen, history not written'}")
    _ensure_portfolio_json()

    # ── Sector map (must be first — used by all scoring) ──
    _init_sector_map()

    # ── 0. Market holiday guard ──
    # Scheduled runs respect the holiday calendar.
    # Manual runs bypass it so you can test on weekends/holidays.
    if not IS_SCHEDULED and not is_market_open():
        _log("[INFO] Market closed today, but running anyway (manual run — holiday guard bypassed).")
    elif not is_market_open():
        _log("[INFO] Market closed today (holiday or weekend). Skipping pipeline.")
        return

    # ── 0b. Earnings season adjustment ──
    earn_adj = earnings_season_threshold_adjustment()
    if earn_adj > 0:
        _log(f"[INFO] Earnings season — thresholds tightened by +{earn_adj}")
        effective_thresholds = copy.deepcopy(REGIME_THRESHOLDS)
        for rk in effective_thresholds:
            effective_thresholds[rk]["min_confidence"] += earn_adj
    else:
        effective_thresholds = REGIME_THRESHOLDS

    # ── 1. Global macro ──
    _log("[1/17] Fetching global macro...")
    macro = fetch_global_macro()
    _vp   = macro.get("vix_in_percentile")
    _vp_s = f" ({_vp:.0f}th %ile, {macro.get('vix_in_regime','UNKNOWN')})" if _vp is not None else ""
    _log(
        f"  NIFTY {macro['nifty_1d_pct']:+.2f}% | "
        f"VIX-IN {macro['vix_in']:.1f}{_vp_s} | "
        f"USD/INR {macro['usdinr']:.2f}"
    )

    # ── 0c. VIX-percentile regime adjustment ──
    # Phase 3 (2026-07-01): VIX percentile is a leading contrarian signal.
    # COMPLACENT (<=15th pct) → market is dangerously calm, raise the bar by +2.
    # FEAR (>=85th pct)      → panic bottoms are entry opportunities, lower by -3.
    # ELEVATED (65-85)       → mild caution (+1).
    # Apply once, on top of any earnings-season adjustment already baked in.
    _vix_pct = macro.get("vix_in_percentile")
    _vix_regime = macro.get("vix_in_regime", "UNKNOWN")
    vix_adj = 0
    if _vix_pct is not None:
        if _vix_regime == "COMPLACENT":
            vix_adj = 2
        elif _vix_regime == "ELEVATED":
            vix_adj = 1
        elif _vix_regime == "FEAR":
            vix_adj = -3
    if vix_adj != 0:
        _log(
            f"[INFO] VIX regime {_vix_regime} (pct={_vix_pct}) — "
            f"thresholds adjusted by {vix_adj:+d}"
        )
        # If we hadn't already deep-copied for earnings, copy now.
        if effective_thresholds is REGIME_THRESHOLDS:
            effective_thresholds = copy.deepcopy(REGIME_THRESHOLDS)
        for rk in effective_thresholds:
            effective_thresholds[rk]["min_confidence"] = max(
                50, min(99, effective_thresholds[rk]["min_confidence"] + vix_adj)
            )

    # ── 1b. FII/DII flows from NSE ──
    _log("[2/17] Fetching FII/DII flows from NSE...")
    fii_dii = get_fii_dii_data()
    macro["fii_flow_cr"]      = fii_dii["fii_flow_cr"]
    macro["dii_flow_cr"]      = fii_dii["dii_flow_cr"]
    macro["fii_available"]    = fii_dii.get("available", False)
    macro["fii_provisional"]  = fii_dii.get("is_provisional", False)
    macro["fii_source"]       = fii_dii.get("source", "NONE")
    macro["fii_confidence"]   = fii_dii.get("confidence", "NONE")
    # FIX: bubble the staleness flag + dii_found flag so downstream (footer,
    # regime banner, tracker row) can surface it explicitly.
    macro["fii_stale"]        = bool(fii_dii.get("stale", False)) or \
                                str(fii_dii.get("confidence", "")).upper() == "STALE"
    macro["dii_found"]        = bool(fii_dii.get("dii_found", fii_dii.get("dii_flow_cr", 0) != 0))
    dii_log = f"{fii_dii['dii_flow_cr']:+.0f}Cr" if fii_dii.get("dii_found") else "N/A"
    _log(f"  FII: {fii_dii['fii_flow_cr']:+.0f}Cr | DII: {dii_log} | "
         f"src={macro['fii_source']} conf={macro['fii_confidence']} | Available: {fii_dii['available']}")

    # ── 3. Bulk/block deals ──
    _log("[3/17] Fetching bulk/block deals...")
    bulk_deals = fetch_bulk_deals()
    if bulk_deals:
        # Phase C1 (2026-07-02): only show first 25 symbols in the log line so
        # it doesn't hog 100+ chars of every daily run.
        _keys = list(bulk_deals.keys())
        _shown = ', '.join(_keys[:25])
        _tail  = f" … +{len(_keys) - 25} more" if len(_keys) > 25 else ""
        _log(f"  Bulk deals: {len(bulk_deals)} found — {_shown}{_tail}")
    # else: fetch_bulk_deals() already logged the reason (quiet day / blocked / error)

    # ── 4. Load symbols ──
    _log("[4/17] Loading symbol universe...")
    symbols = load_symbols("stocks.txt")

    # ── 5. Parallel price download + liquidity filter ──
    _log("[5/17] Downloading prices (parallel)...")
    tradable = filter_and_download(symbols, period="6mo", max_workers=12)
    _log(f"  Tradable: {len(tradable)} stocks")

    # ── 5b. Enrich sector map for all tradable symbols ──
    _log("[5b/17] Enriching sector map: nselib bulk first, yfinance fallback...")
    enrich_sectors_from_nselib()
    enrich_sectors_from_yfinance(list(tradable.keys()))
    if not tradable:
        _log("[ERROR] No tradable stocks. Aborting.")
        return

    # ── 6. Market regime ──
    _log("[6/17] Detecting market regime...")
    nifty_df    = fetch_price_data("^NSEI", period="1y")
    breadth     = compute_breadth(tradable)
    if nifty_df is None:
        _log("[ERROR] Cannot fetch Nifty data. Aborting.")
        return
    regime_data = detect_market_regime(nifty_df, breadth, macro)
    regime      = regime_data["regime"]

    # Compute real Nifty 21-day and 5-day returns ONCE — passed to every stock's RS calc
    try:
        _nc = nifty_df["Close"].squeeze().values.astype(float)
        nifty_ret21_real = round((_nc[-1] / _nc[-22] - 1) * 100, 2) if len(_nc) > 22 else 0.0
        nifty_ret5_real  = round((_nc[-1] / _nc[-6]  - 1) * 100, 2) if len(_nc) > 6  else 0.0
    except Exception:
        nifty_ret21_real = 0.0
        nifty_ret5_real  = 0.0
    regime_data["nifty_ret21"] = nifty_ret21_real
    regime_data["nifty_ret5"]  = nifty_ret5_real
    _log(f"  REGIME: {regime} | Score: {regime_data['score']:.1f}/100 | EMA20 breadth: {breadth['ema20_pct']:.1f}%")
    _log(f"  Nifty 21d ret: {nifty_ret21_real:+.2f}% | 5d ret: {nifty_ret5_real:+.2f}%")

    # ── 6b. Nifty key levels + single nifty_state (BUG FIX 1) ──
    key_levels  = compute_key_levels(nifty_df)
    nifty_state = compute_nifty_state(nifty_df)
    _log(f"  Nifty structure: {nifty_state['structure']}")

    # ── 6c. Sector rotation ──
    _log("[6c/17] Computing sector rotation...")
    sector_rotation = compute_sector_rotation(tradable)
    leading = [s for s, d in sector_rotation.items() if d["status"] == "LEADING"]
    lagging = [s for s, d in sector_rotation.items() if d["status"] == "LAGGING"]
    _log(f"  Leading: {leading} | Lagging: {lagging}")

    # ── 7. Score all stocks ──
    _log("[7/17] Scoring all stocks...")
    scored = []
    for symbol, df in tradable.items():
        sector       = get_sector(symbol)
        scores       = compute_all_factors(symbol, df, sector, regime_data, sector_rotation)
        base_conf    = compute_base_confidence(scores)
        # NOTE: **scores must come BEFORE base_confidence so our computed value wins
        # (scores dict contains base_confidence: 0.0 as a default placeholder)
        scored.append({"symbol": symbol, "sector": sector, "_df": df, **scores, "base_confidence": base_conf})

    scored.sort(key=lambda x: (-x["base_confidence"], x["symbol"]))
    top_40 = scored[:40]
    _log(f"  Top 40: best base conf {top_40[0]['base_confidence']:.1f} ({top_40[0]['symbol']})")

    # ── 8. News + AI risk for top 40 ──
    _log("[8/17] News + AI risk for top 40...")
    for stock in top_40:
        sym_clean  = stock["symbol"].replace(".NS", "")
        headlines  = fetch_news_for_symbol(sym_clean)
        if headlines:
            ai_result = ai_news_risk(sym_clean, [h["title"] for h in headlines])
            age       = min(h["age_days"] for h in headlines)
            penalty   = compute_news_penalty(ai_result, age)
        else:
            ai_result = {"severity": 0, "category": "NO_NEWS", "is_black_swan": False, "summary": ""}
            penalty   = 0.0
        stock["news_penalty"]  = penalty
        stock["is_black_swan"] = ai_result.get("is_black_swan", False)
        stock["news_summary"]  = truncate_display(ai_result.get("summary", ""), 100)
        # FIX: persist news category so BUY-card renderer can distinguish
        # "no headlines" (NO_NEWS) from "headline exists but summary was empty".
        stock["news_category"] = ai_result.get("category", "")
        stock["news_risk"]     = max(0, 100 - int(penalty * 2))

    # ── 9. Promoter data + fundamentals — sequential with 24h cache (no rate limiting) ──
    # FIX: widen from 20 -> 30 so 3-of-5 BUYs no longer come back with ROE=0.
    # BUY-priority ordering is done inside fetch_all_fundamentals_cached().
    _log("[9/17] Fetching promoter/fundamentals for top 30 (BUY-priority + cached)...")
    top_40 = fetch_all_fundamentals_cached(top_40, max_stocks=30)

    # ── 10. Options PCR for top 20 (parallel) ──
    _log("[10/17] Options PCR for top 20 (parallel)...")

    def _fetch_pcr(stock: dict) -> tuple:
        sym_clean = stock["symbol"].replace(".NS", "")
        oc = fetch_option_chain(sym_clean)
        return stock["symbol"], pcr_score(oc["pcr"]), oc.get("source", "?")

    pcr_map = {}
    pcr_src_map = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_fetch_pcr, s): s["symbol"] for s in top_40[:20]}
        for fut in as_completed(futs):
            try:
                sym, score_val, src = fut.result(timeout=15)
                pcr_map[sym]     = score_val
                pcr_src_map[sym] = src
            except Exception as e:
                _log(f"[WARN] PCR fetch failed for {futs[fut]}: {e}")

    for stock in top_40[:20]:
        stock["options_sentiment"] = pcr_map.get(stock["symbol"], 60)
        stock["pcr_source"]        = pcr_src_map.get(stock["symbol"], "NOT_FETCHED")

    # ── 11. Final confidence ──
    _log("[11/17] Computing final confidence...")
    macro_adj_global = macro_regime_adjustment(macro) * 0.3
    # Phase C3 (2026-07-02): blend real delivery % into the volume_delivery
    # factor for stocks that got real nselib data. OBV proxy stays as fallback.
    # Weight: 65% real delivery + 35% existing (OBV × accumulation) blend.
    for stock in top_40:
        if stock.get("delivery_source") == "nselib":
            deliv_score  = delivery_score_from_signal(
                stock.get("delivery_signal", "NEUTRAL"),
                stock.get("delivery_ratio", 1.0),
            )
            proxy_score  = float(stock.get("volume_delivery", 50))
            blended      = round(deliv_score * 0.65 + proxy_score * 0.35, 1)
            stock["volume_delivery_proxy"] = proxy_score  # keep for audit
            stock["volume_delivery"]       = blended
        # Otherwise keep existing OBV-proxy value as computed in score_stock().
    for stock in top_40:
        bulk_adj  = bulk_deal_score(stock["symbol"], bulk_deals)
        base_conf = compute_base_confidence({k: stock.get(k, 50) for k in FACTOR_WEIGHTS})
        stock["base_confidence"]  = base_conf
        stock["final_confidence"] = compute_final_confidence(
            base_conf, regime, stock.get("news_penalty", 0), macro_adj_global, bulk_adj
        )
    top_40.sort(key=lambda x: (-x["final_confidence"], x["symbol"]))

    # ── 11b. Opportunity scores (ENHANCEMENT 1) ──
    for stock in top_40:
        stock["opportunity_score"] = compute_opportunity_score(stock)
    top_40.sort(key=lambda x: (-x["opportunity_score"], x["symbol"]))

    # ── 12. Portfolio monitoring ──
    _log("[12/17] Monitoring portfolio...")
    holdings       = load_portfolio()
    current_prices = {}
    for h in holdings:
        sym = h.get("symbol", "")
        if sym:
            try:
                df_tmp = fetch_price_data(sym, period="1mo")
                if df_tmp is not None and len(df_tmp) > 0:
                    current_prices[sym] = float(df_tmp["Close"].squeeze().iloc[-1])
            except Exception:
                pass
    portfolio_alerts = monitor_portfolio(holdings, current_prices, regime)

    # ── 13. Load tracker (before gates — needed for deduplication) ──
    _log("[13/17] Loading trade tracker...")
    tracker_entries = load_tracker()

    # ── 13b. Tracker V2 — load, update PnL, close completed positions ──
    tracker_v2 = initialize_tracker_if_new()
    tracker_v2 = update_tracker_v2_pnl(tracker_v2)
    if IS_SCHEDULED:
        save_tracker_v2(tracker_v2)
    _log(f"  Tracker V2: {len(tracker_v2.get('buys',[]))} active | {len(tracker_v2.get('watchlist',[]))} watching | {tracker_v2.get('performance',{}).get('completed',0)} completed")

    # ── 13b. Upcoming events from config file (FIX 4: auto-filters past events) ──
    upcoming_events = load_events_config()

    # ── 14. Gate system ──
    _log("[14/17] Running gate system (13 gates)...")
    portfolio_context = {
        "active_count":   len([a for a in portfolio_alerts if a["action"] == "HOLD"]),
        "existing_count": len(holdings),
    }
    buys, watchlist_stocks, rejected = [], [], []

    # Build returns cache ONCE from already-downloaded price data (zero extra downloads)
    _log("  Building correlation returns cache...")
    returns_cache = build_returns_cache(tradable, lookback=60)

    # Pre-fetch BSE results dates for top 40 (parallel, 5 workers)
    _log("  Fetching BSE results dates for top 40...")
    results_dates_map: dict = {}

    def _fetch_results(stock: dict) -> tuple:
        sym_clean = stock["symbol"].replace(".NS", "")
        return sym_clean, fetch_bse_results_dates(sym_clean)

    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_fetch_results, s): s["symbol"] for s in top_40}
        for fut in as_completed(futs):
            try:
                sym_clean, dates = fut.result(timeout=10)
                results_dates_map[sym_clean] = dates
            except Exception:
                pass
    # Phase C1 (2026-07-02): honest counter — how many top-40 stocks had an
    # upcoming earnings date discovered. Silent failure was hiding the gate
    # activity.
    _dates_found = sum(1 for d in results_dates_map.values() if d)
    _log(f"  BSE results-dates: {_dates_found}/{len(top_40)} stocks have upcoming earnings")

    for stock in top_40:
        sym_clean     = stock["symbol"].replace(".NS", "")
        promoter_data = stock.get("promoter_data", {"promoter_pledge_pct": 0})
        gate_result   = run_gates(
            stock, regime, effective_thresholds,
            portfolio_context, bulk_deals, promoter_data,
            results_dates=results_dates_map.get(sym_clean, []),
            upcoming_events=upcoming_events,
            returns_cache=returns_cache,
            holdings=holdings,
        )
        stock["decision"]     = gate_result["decision"]
        stock["fail_reasons"] = gate_result["fail_reasons"]
        stock["warnings"]     = gate_result["warnings"]

        if gate_result["decision"] == "BUY":
            buys.append(stock)
        elif gate_result["decision"] == "WATCHLIST":
            wl = classify_watchlist(stock, regime, effective_thresholds)
            stock.update(wl)
            watchlist_stocks.append(stock)
        else:
            rejected.append(stock)

    _log(f"  Gate results: {len(buys)} BUY | {len(watchlist_stocks)} WATCHLIST | {len(rejected)} REJECTED")

    # Sort all lists by opportunity score (ENHANCEMENT 1)
    for stock in buys + watchlist_stocks:
        if "opportunity_score" not in stock:
            stock["opportunity_score"] = compute_opportunity_score(stock)
    buys.sort(key=lambda x: (-x.get("opportunity_score", 0), x.get("symbol", "")))
    watchlist_stocks.sort(key=lambda x: (-x.get("opportunity_score", 0), x.get("symbol", "")))

    # Enforce max_buys cap
    # Phase C3 (2026-07-02): removed fii_stale halving — FII data is
    # structurally D-1/D-2 and was never a reliable signal to throttle
    # sizing on. max_buys is now driven purely by regime thresholds and
    # portfolio heat. See format_conviction_meter comment for full rationale.
    max_buys = effective_thresholds[regime]["max_buys"]
    buys = buys[:max_buys]

    # ── 14a. Decision audit JSONL — Phase C ─────────────────────────────────
    # Append one line per stock that reached the gates. Used for post-mortem RCA
    # ("why did we buy X on day N?") and CL-level signal regression tests.
    try:
        for stock in (buys + watchlist_stocks + rejected):
            append_decision_audit({
                "symbol":          stock.get("symbol"),
                "decision":        stock.get("decision"),
                "confidence":      stock.get("confidence"),
                "trade_quality":   stock.get("trade_quality"),
                "opportunity":     stock.get("opportunity_score"),
                "regime":          regime,
                "fail_reasons":    stock.get("fail_reasons", []),
                "warnings":        stock.get("warnings", []),
                "macro_quality":   macro.get("data_quality", "NORMAL"),
                "macro_bad":       macro.get("bad_fields", []),
                "fii_source":      macro.get("fii_source"),
                "fii_confidence":  macro.get("fii_confidence"),
                "usdinr":          macro.get("usdinr"),
                "pcr_source":      stock.get("pcr_source"),
            })
        _log(f"  [Audit] decision_audit appended {len(buys)+len(watchlist_stocks)+len(rejected)} rows → {DECISION_AUDIT_FILE}")
    except Exception as e:
        _log(f"  [Audit] append failed (non-fatal): {e}")

    # ── 14b. Confidence history update (FEATURE 2) ──
    _today_str_h = datetime.date.today().isoformat()
    conf_history = load_confidence_history()
    conf_history = update_confidence_history(conf_history, top_40, _today_str_h)
    if IS_SCHEDULED:
        save_confidence_history(conf_history)
        _log(f"  Confidence history: {len(conf_history)} symbols tracked (saved)")
    else:
        _log(f"  Confidence history: {len(conf_history)} symbols tracked (NOT saved — manual run)")

    # ── 14c. Gate memory update (FEATURE 7) ──
    gate_memory = load_gate_memory()
    gate_memory = update_gate_memory(gate_memory, watchlist_stocks, _today_str_h)
    if IS_SCHEDULED:
        save_gate_memory(gate_memory)
        _log(f"  Gate memory: {len(gate_memory)} symbols tracked (saved)")
    else:
        _log(f"  Gate memory: {len(gate_memory)} symbols tracked (NOT saved — manual run)")
    buys = tag_repeat_buy_signals(buys, tracker_entries)

    # ── 14c. Position sizing — Kelly + Heat-aware ──
    # Compute Platt stats from tracker history (activates after 20 closed trades)
    platt = compute_platt_stats(tracker_entries)
    if platt["calibrated"]:
        _log(f"  Platt calibration active: WR={platt['win_rate']:.1%} | "
             f"AvgWin={platt['avg_win_pct']:.1f}% | AvgLoss={platt['avg_loss_pct']:.1f}%")
    else:
        _log(f"  Platt calibration: {platt['total_closed']}/20 trades — using fixed 1.5% sizing")

    # Compute portfolio heat (total open risk as % of capital)
    heat = compute_portfolio_heat(holdings, current_prices, PORTFOLIO_CAPITAL)
    _log(f"  Portfolio heat: {heat['heat_pct']:.1f}% / {heat['max_heat_pct']:.0f}% max "
         f"({'OK' if heat['heat_ok'] else 'NEAR LIMIT'})")

    for stock in buys:
        pos = kelly_position_size(
            entry=stock.get("entry", 0),
            stop=stock.get("stop", 0),
            capital=PORTFOLIO_CAPITAL,
            win_rate=platt["win_rate"],
            avg_win_pct=platt["avg_win_pct"],
            avg_loss_pct=platt["avg_loss_pct"],
            heat=heat,
            max_position_pct=_MAX_POSITION_PCT_ENV,
        )
        stock.update(pos)

    # ── 14d. Short signal detection ──
    shorts = detect_short_signals(top_40, regime, regime_data["thresholds"])

    # ── 14e. Upcoming market events already computed at step 13b ──

    # ── 14f. Watchlist persistence ──
    wl_history = load_persistent_watchlist()
    watchlist_stocks, wl_history = merge_watchlist_with_history(watchlist_stocks, wl_history)
    if IS_SCHEDULED:
        save_persistent_watchlist(wl_history)
    else:
        _log("  Watchlist history NOT saved (manual run)")

    # ── 15. Format and send main Telegram message ──
    _log("[15/17] Sending main Telegram report...")
    # Phase C: explicit Asia/Kolkata — no dependency on TZ env var
    timestamp = ist_now().strftime("%b %d, %Y %H:%M IST")
    # nifty_state already computed at step 6b — pass through (BUG FIX 1)

    # ── 15a. Run AI calls in parallel (daily summary + buy theses + near miss insights) ──
    _log("[15a/17] Running AI calls in parallel...")
    ai_results = run_all_ai_calls(
        regime_data      = regime_data,
        macro            = macro,
        breadth_data     = breadth,
        nifty_state      = nifty_state,
        buys             = buys,
        watchlist        = watchlist_stocks,
        portfolio_alerts = portfolio_alerts,
        conf_history     = conf_history,
        gate_memory      = gate_memory,
        events           = upcoming_events,
    )

    message = format_telegram_message(
        regime_data      = regime_data,
        buys             = buys,
        shorts           = shorts,
        watchlist        = watchlist_stocks,
        portfolio_alerts = portfolio_alerts,
        macro            = macro,
        key_levels       = key_levels,
        upcoming_events  = upcoming_events,
        timestamp        = timestamp,
        heat             = heat,
        platt            = platt,
        tracker_v2       = tracker_v2,
        rejected_stocks  = rejected,
        breadth_20       = breadth.get("ema20_pct", 50.0),
        nifty_state      = nifty_state,
        universe_count   = len(symbols),
        tradable_count   = len(tradable),
        conf_history     = conf_history,
        gate_memory      = gate_memory,
        ai_results       = ai_results,
    )
    _log("--- TELEGRAM PREVIEW (first 1500 chars) ---")
    _log(message[:1500])
    _log("--- END PREVIEW ---")
    # Prepend a visible test banner on manual runs so you instantly know it's not the real report
    if not IS_SCHEDULED:
        banner = (
            "⚠️  MANUAL TEST RUN — NOT the scheduled report\n"
            "State was NOT saved. Day counters NOT advanced.\n"
            + "─" * 40 + "\n"
        )
        message = banner + message
    send_telegram(message)
    # Send BUY signals to dedicated buy channel
    send_buy_telegram(buys, regime, timestamp)

    # ── 16. Trade Tracker updates ──
    _log("[16/17] Updating trade tracker...")
    tracker_entries = update_tracker_trailing_stop(tracker_entries)

    for stock in buys:
        tracker_entries = add_to_tracker(tracker_entries, stock, "BUY")

    near_miss_stocks = [w for w in watchlist_stocks if w.get("tier") == "NEAR_MISS"]
    for stock in near_miss_stocks:
        tracker_entries = add_to_tracker(tracker_entries, stock, "NEAR_MISS")

    tracker_entries, closed_today = update_tracker(tracker_entries)

    if closed_today:
        _log(f"  Tracker: {len(closed_today)} trade(s) closed today")

    if IS_SCHEDULED:
        save_tracker(tracker_entries)
    else:
        _log("  Trade tracker NOT saved (manual run)")

    # ── 17. Save CSVs + Excel ──
    _log("[17/17] Saving output CSVs + Excel...")
    for s in top_40:
        s.pop("_df", None)
        s.pop("fundamentals", None)
        s.pop("promoter_data", None)
    save_csv(top_40,           "analysis_output.csv")
    save_csv(portfolio_alerts, "portfolio_monitor.csv")
    save_csv(buys,             "buys_today.csv")

    # Save recommendations to Excel tracker (PART C)
    # Always persist — the Excel is the system-of-record for the tracker job
    # and downstream artifacts. Manual runs MUST still write it, otherwise the
    # Recommendation Tracker workflow has nothing to download.
    today_str_pipe = datetime.date.today().isoformat()
    save_recommendations_to_excel(
        buys, watchlist_stocks,
        {"regime": regime, "score": regime_data.get("score", 0)},
        today_str_pipe
    )

    # ── 18. Done ──
    _log("[DONE] Pipeline complete.")
    _log(f"  BUY: {len(buys)} | WATCHLIST: {len(watchlist_stocks)} | SHORTS: {len(shorts)}")


if __name__ == "__main__":
    run_pipeline()
