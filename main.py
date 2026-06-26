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
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

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
# SECTION 1 — CONFIG & ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────

PORTFOLIO_CAPITAL   = float(os.getenv("CAPITAL", "500000"))
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
TRACKER_BOT_TOKEN   = os.getenv("TRACKER_BOT_TOKEN", "")
TRACKER_CHAT_ID     = os.getenv("TRACKER_CHAT_ID", "")
# Dedicated BUY-signal channel — set BUY_BOT_TOKEN + BUY_CHAT_ID in GitHub Secrets
BUY_BOT_TOKEN       = os.getenv("BUY_BOT_TOKEN", "")
BUY_CHAT_ID         = os.getenv("BUY_CHAT_ID", "")
TRACKER_FILE        = os.getenv("TRACKER_FILE", "tracker.json")
TRADE_TRACKER_V2_FILE   = os.getenv("TRADE_TRACKER_V2_FILE", "trade_tracker.json")
FUNDAMENTALS_CACHE_FILE = os.getenv("FUNDAMENTALS_CACHE_FILE", "fundamentals_cache.json")
PORTFOLIO_FILE          = os.getenv("PORTFOLIO_FILE", "portfolio.json")
WATCHLIST_FILE      = os.getenv("WATCHLIST_FILE", "watchlist_persist.json")
TELEGRAM_MAX_CHARS  = 4000

# Regime thresholds — v6.0 calibrated (Bug 2 fix)
REGIME_THRESHOLDS = {
    "STRONG_BULL":     {"min_confidence": 78, "min_tq": 72, "min_rr": 1.7, "max_buys": 5,  "max_exposure": 0.85},
    "BULL":            {"min_confidence": 82, "min_tq": 76, "min_rr": 1.8, "max_buys": 3,  "max_exposure": 0.75},
    "SIDEWAYS":        {"min_confidence": 80, "min_tq": 78, "min_rr": 2.0, "max_buys": 1,  "max_exposure": 0.50},
    "TRANSITION":      {"min_confidence": 83, "min_tq": 78, "min_rr": 2.0, "max_buys": 2,  "max_exposure": 0.55},
    "HIGH_VOLATILITY": {"min_confidence": 85, "min_tq": 80, "min_rr": 2.2, "max_buys": 1,  "max_exposure": 0.40},
    "BEAR":            {"min_confidence": 92, "min_tq": 88, "min_rr": 2.5, "max_buys": 0,  "max_exposure": 0.20},
    "STRONG_BEAR":     {"min_confidence": 99, "min_tq": 99, "min_rr": 3.0, "max_buys": 0,  "max_exposure": 0.00},
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
    if len(text) <= max_len:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


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


def send_tracker_telegram(message: str) -> None:
    if not TRACKER_BOT_TOKEN or not TRACKER_CHAT_ID:
        return
    chunks = _split_telegram_message(message, TELEGRAM_MAX_CHARS)
    for chunk in chunks:
        try:
            url = f"https://api.telegram.org/bot{TRACKER_BOT_TOKEN}/sendMessage"
            resp = requests.post(url, json={
                "chat_id": TRACKER_CHAT_ID,
                "text": _sanitize_telegram_html(chunk),
                "parse_mode": "HTML",
            }, timeout=12)
            if resp.status_code != 200:
                _log(f"[WARN] Tracker Telegram: {resp.status_code} {resp.text[:80]}")
        except Exception as e:
            _log(f"[WARN] send_tracker_telegram failed: {e}")


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
                if avg_vol < min_avg_volume or avg_val_lakhs < min_avg_value_lakhs:
                    continue
                tradable[symbol] = df
            except Exception as e:
                _log(f"[WARN] download failed for {sym}: {e}")
                failed += 1
    _log(f"Download complete: {len(tradable)} tradable, {failed} failed/illiquid")
    return tradable


_NSE_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
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


def _bhavcopy_from_file(filepath: str):
    """Load a pre-downloaded bhavcopy CSV or ZIP from disk. Returns DataFrame or None."""
    import zipfile, io as _io
    try:
        if not os.path.exists(filepath) or os.path.getsize(filepath) < 500:
            return None
        if filepath.endswith(".zip"):
            with zipfile.ZipFile(filepath) as zf:
                csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                if csv_name is None:
                    return None
                df = pd.read_csv(zf.open(csv_name))
        else:
            df = pd.read_csv(filepath)
        df.columns = [c.strip() for c in df.columns]
        _log(f"  Bhavcopy loaded from local file: {filepath}")
        return df
    except Exception as e:
        _log(f"[WARN] Could not read local bhavcopy file {filepath}: {e}")
        return None


def _bhavcopy_from_bse(date: datetime.datetime):
    """
    BSE bhavcopy fallback — disabled.
    BSE API and ZIP downloads both require browser session cookies unavailable from CI IPs.
    Delivery % defaults to 50% when NSE data is also unavailable — scoring impact is minimal.
    """
    return None


def fetch_nse_bhavcopy(date=None):
    """
    Delivery % data — tried in this priority order:
    1. Local pre-downloaded file (bhavcopy_today.csv / .zip) written by workflow step
    2. NSE archive URLs with pre-warmed session (blocked on CI IPs, works locally)
    3. BSE bhavcopy fallback — BSE servers are accessible from GitHub Actions
    Returns a DataFrame with at minimum SYMBOL and DELIV_PER columns, or None.
    """
    import zipfile, io as _io

    if date is None:
        date = datetime.datetime.today()

    # ── 1. Local pre-downloaded file ──────────────────────────────────────────
    for local_path in ("bhavcopy_today.csv", "bhavcopy_today.zip"):
        df = _bhavcopy_from_file(local_path)
        if df is not None:
            return df

    # ── 2. NSE live fetch (works locally / non-blocked IPs) ──────────────────
    session = _nse_session()
    session.headers.update({
        "Referer": "https://www.nseindia.com/all-reports",
        "X-Requested-With": "XMLHttpRequest",
    })

    nse_ok = False
    for days_back in range(0, 6):
        d = date - datetime.timedelta(days=days_back)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%d%b%Y").upper()
        date_ymd = d.strftime("%Y%m%d")

        nse_urls = [
            (f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv", "csv"),
            (f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv", "csv"),
            (f"https://nsearchives.nseindia.com/content/equities/BhavCopy_NSE_CM_0_0_0_{date_ymd}_F_0000.csv.zip", "zip"),
        ]
        for url, fmt in nse_urls:
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    if fmt == "zip":
                        with zipfile.ZipFile(_io.BytesIO(resp.content)) as zf:
                            csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                            if csv_name is None:
                                continue
                            df = pd.read_csv(zf.open(csv_name))
                    else:
                        df = pd.read_csv(StringIO(resp.text))
                    df.columns = [c.strip() for c in df.columns]
                    _log(f"  Bhavcopy loaded for {d.strftime('%d-%b-%Y')} via {url.split('/')[2]}")
                    return df
                elif resp.status_code == 404:
                    continue
                elif resp.status_code in (403, 429):
                    _log(f"[WARN] NSE bhavcopy blocked ({resp.status_code}) — likely CI IP block")
                    nse_ok = False
                    break   # all NSE sources will be blocked; skip to BSE
            except Exception as e:
                _log(f"[WARN] NSE bhavcopy fetch error: {e}")
                continue
        if not nse_ok:
            break  # don't retry other dates if NSE is blocking us

    # ── 3. BSE fallback disabled — requires browser session unavailable from CI ──
    _log("  Bhavcopy unavailable from CI — using 50% default (no impact on signals)")
    return None


def get_delivery_pct(symbol: str, bhavcopy_df) -> float:
    if bhavcopy_df is None:
        return 50.0
    try:
        clean_sym = symbol.replace(".NS", "").strip()
        cols = bhavcopy_df.columns.tolist()
        # Detect symbol column — old format uses SYMBOL, new ZIP uses TradingSymbol
        sym_col = "SYMBOL" if "SYMBOL" in cols else ("TradingSymbol" if "TradingSymbol" in cols else None)
        if sym_col is None:
            return 50.0
        row = bhavcopy_df[bhavcopy_df[sym_col].astype(str).str.strip() == clean_sym]
        if len(row) > 0:
            # Check all known delivery-percentage column names in priority order
            for col in ("DELIV_PER", "DeliveryPercentage", "% Dly Qt to Traded Qty"):
                if col in cols:
                    val = row.iloc[0][col]
                    if pd.notna(val) and str(val).strip() not in ("", "-", "nan"):
                        return float(str(val).replace(",", "").strip())
    except Exception:
        pass
    return 50.0


def fetch_option_chain(symbol_nse: str) -> dict:
    neutral = {"pcr": 1.0, "total_ce_oi": 0, "total_pe_oi": 0}
    try:
        session = _nse_session()
        url  = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol_nse}"
        resp = session.get(url, headers={
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/option-chain",
        }, timeout=12)
        if resp.status_code == 200:
            data    = resp.json()
            records = data.get("records", {}).get("data", [])
            total_ce_oi = sum(r.get("CE", {}).get("openInterest", 0) for r in records if "CE" in r)
            total_pe_oi = sum(r.get("PE", {}).get("openInterest", 0) for r in records if "PE" in r)
            pcr = round(total_pe_oi / total_ce_oi, 3) if total_ce_oi > 0 else 1.0
            return {"pcr": pcr, "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi}
        elif resp.status_code in (403, 429):
            _log(f"[WARN] NSE options chain blocked ({resp.status_code}) for {symbol_nse} — neutral PCR used")
    except Exception as e:
        _log(f"[WARN] fetch_option_chain failed for {symbol_nse}: {e}")
    return neutral


def pcr_score(pcr: float) -> int:
    if pcr >= 1.5:   return 35
    elif pcr >= 1.2: return 75
    elif pcr >= 0.9: return 60
    elif pcr >= 0.7: return 45
    return 25


def fetch_bulk_deals(days_back: int = 3) -> dict:
    result = {}
    try:
        session = _nse_session()
        resp = session.get(
            "https://www.nseindia.com/api/bulk-deals",
            headers={
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/market-data/bulk-block-deals",
            },
            timeout=12,
        )
        if resp.status_code == 200:
            deals = resp.json().get("data", [])
            for deal in deals:
                sym    = deal.get("symbol", "").strip() + ".NS"
                action = "BUY" if str(deal.get("buySell", "")).upper().startswith("B") else "SELL"
                result[sym] = action
        elif resp.status_code in (403, 429):
            _log(f"[WARN] NSE bulk deals blocked ({resp.status_code}) — skipping")
    except Exception as e:
        _log(f"[WARN] fetch_bulk_deals failed: {e}")
    return result


def bulk_deal_score(symbol: str, bulk_deals_dict: dict) -> int:
    action = bulk_deals_dict.get(symbol)
    if action == "BUY":  return 6
    elif action == "SELL": return -8
    return 0


def ownership_quality_score(promoter_data: dict) -> int:
    score    = 50
    pledge   = promoter_data.get("promoter_pledge_pct", 0)
    promoter = promoter_data.get("promoter_holding_pct", 50)
    fii      = promoter_data.get("fii_pct", 0)
    dii      = promoter_data.get("dii_pct", 0)
    if pledge > 40:   score -= 30
    elif pledge > 20: score -= 15
    elif pledge > 10: score -= 5
    if promoter > 60:   score += 15
    elif promoter > 50: score += 8
    elif promoter < 30: score -= 10
    if fii > 15:   score += 10
    elif fii > 5:  score += 5
    if dii > 10:   score += 8
    elif dii > 3:  score += 4
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
    """
    time.sleep(delay_seconds)

    data = fetch_screener_data(symbol_clean)
    if data:
        # FIX: screener HTML structure may have changed — ROE/D-E arrive as 0
        # Supplement with yfinance when key financial ratios are all zero
        if data.get("roe", 0) == 0 and data.get("de_ratio", 0) == 0:
            try:
                yf_data = fetch_yfinance_fundamentals(symbol_clean + ".NS")
                if yf_data.get("roe", 0) != 0:
                    data["roe"] = yf_data["roe"]
                if yf_data.get("de_ratio", 0) != 0:
                    data["de_ratio"] = yf_data["de_ratio"]
                if yf_data.get("roce", 0) != 0:
                    data["roce"] = yf_data["roce"]
            except Exception:
                pass
        data["source"] = "SCREENER+YF"
        _log(f"[INFO] Fundamentals (screener): {symbol_clean} "
             f"ROE={data.get('roe',0):.1f}% D/E={data.get('de_ratio',0):.2f} "
             f"Pledge={data.get('promoter_pledge_pct',0):.1f}%")
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


def fetch_all_fundamentals_cached(top_40: list, max_stocks: int = 20) -> list:
    """
    Fetches fundamentals sequentially with 24h cache.
    Adds ~50s on first run; near-instant on same-day re-runs.
    """
    cache = load_fundamentals_cache()
    _log(f"[INFO] Fundamentals cache: {len(cache)} symbols cached")
    est_sec = sum(
        0 if symbol_clean in cache else 2.5
        for symbol_clean in [s["symbol"].replace(".NS", "") for s in top_40[:max_stocks]]
    )
    _log(f"[INFO] Estimated fetch time: ~{est_sec:.0f}s for {max_stocks} stocks")

    for i, stock in enumerate(top_40[:max_stocks]):
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

    for stock in top_40[max_stocks:]:
        stock["promoter_data"]       = {**NEUTRAL_FUNDAMENTALS}
        stock["ownership_quality"]   = 50
        stock["promoter_pledge_pct"] = 0.0
        stock["roe"]                 = 0.0
        stock["de_ratio"]            = 0.0
        stock["roce"]                = 0.0
        stock["fundamentals_source"] = "NOT_FETCHED"

    save_fundamentals_cache(cache)
    fetched_ok = sum(1 for s in top_40[:max_stocks]
                     if s.get("fundamentals_source") not in ("NEUTRAL_DEFAULT", "NOT_FETCHED"))
    _log(f"  Fundamentals done: {fetched_ok}/{max_stocks} real data | "
         f"{max_stocks - fetched_ok} defaults | cache saved")
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


def fetch_global_macro() -> dict:
    macro = {
        "usdinr": 83.5, "crude_usd": 75.0, "vix_us": 18.0, "vix_in": 15.0,
        "us10y": 4.3, "sp500_1d_pct": 0.0, "china_1d_pct": 0.0,
        "gold_usd": 2300.0, "nifty_1d_pct": 0.0, "dxy": 103.0,
        "sensex_1d_pct": 0.0, "dow_1d_pct": 0.0,
        "vix_in_20d_avg": 15.0, "vix_term_ratio": 1.0,
    }
    for name, ticker in GLOBAL_TICKERS.items():
        try:
            # Fetch 30d for VIX_IN so we can compute 20d moving average
            period = "30d" if name == "VIX_IN" else "5d"
            df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=True)
            if df is not None and len(df) >= 2:
                last = float(df["Close"].squeeze().iloc[-1])
                prev = float(df["Close"].squeeze().iloc[-2])
                pct  = round((last - prev) / prev * 100, 2) if prev != 0 else 0.0
                if name == "USDINR":   macro["usdinr"]       = last
                elif name == "CRUDE":  macro["crude_usd"]    = last
                elif name == "VIX_US": macro["vix_us"]       = last
                elif name == "VIX_IN":
                    macro["vix_in"] = last
                    # VIX term structure: spot vs 20-day average
                    if len(df) >= 20:
                        vix20_avg = float(df["Close"].squeeze().tail(20).mean())
                        macro["vix_in_20d_avg"] = round(vix20_avg, 2)
                        macro["vix_term_ratio"]  = round(last / vix20_avg, 3) if vix20_avg > 0 else 1.0
                elif name == "US10Y":  macro["us10y"]        = last
                elif name == "GOLD":   macro["gold_usd"]     = last
                elif name == "DXY":    macro["dxy"]          = last
                elif name == "SP500":  macro["sp500_1d_pct"] = pct
                elif name == "DOW":    macro["dow_1d_pct"]   = pct
                elif name == "NIFTY":  macro["nifty_1d_pct"] = pct
                elif name == "SENSEX": macro["sensex_1d_pct"]= pct
        except Exception:
            continue
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


def fetch_fii_dii_flows() -> dict:
    """
    Fetches today's FII/DII provisional flows from NSE.
    Returns dict with fii_flow_cr and dii_flow_cr. Never crashes.
    """
    result = {"fii_flow_cr": 0.0, "dii_flow_cr": 0.0}
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com",
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                    timeout=10)
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        r = session.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer":    "https://www.nseindia.com",
            "Accept":     "application/json",
        }, timeout=12)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                latest   = data[0]
                fii_buy  = float(str(latest.get("fiiBuy",  "0")).replace(",", ""))
                fii_sell = float(str(latest.get("fiiSell", "0")).replace(",", ""))
                dii_buy  = float(str(latest.get("diiBuy",  "0")).replace(",", ""))
                dii_sell = float(str(latest.get("diiSell", "0")).replace(",", ""))
                result["fii_flow_cr"] = round(fii_buy - fii_sell, 2)
                result["dii_flow_cr"] = round(dii_buy - dii_sell, 2)
                return result
    except Exception as e:
        _log(f"[WARN] FII/DII NSE fetch failed: {e}")

    # Fallback: moneycontrol RSS
    try:
        if _FEEDPARSER_OK:
            import re as _re
            feed = feedparser.parse("https://www.moneycontrol.com/rss/marketstats.xml")
            for entry in feed.entries[:5]:
                title = entry.get("title", "").lower()
                if "fii" in title and "crore" in title:
                    fii_m = _re.search(r"fii.*?([\d,]+)\s*crore", title)
                    dii_m = _re.search(r"dii.*?([\d,]+)\s*crore", title)
                    if fii_m:
                        val = float(fii_m.group(1).replace(",", ""))
                        result["fii_flow_cr"] = val if "bought" in title else -val
                    if dii_m:
                        val = float(dii_m.group(1).replace(",", ""))
                        result["dii_flow_cr"] = val if "bought" in title else -val
                    break
    except Exception:
        pass

    return result


def format_fii_dii(fii: float, dii: float) -> str:
    """Format FII/DII for Telegram. Handles zero gracefully."""
    if fii == 0 and dii == 0:
        return "FII/DII: Data unavailable (post-market)"
    fii_lbl = f"Rs{abs(fii):.0f}Cr {'🟢 BUY' if fii > 0 else '🔴 SELL'}"
    dii_lbl = f"Rs{abs(dii):.0f}Cr {'🟢 BUY' if dii > 0 else '🔴 SELL'}"
    combined = fii + dii
    if fii > 500 and dii > 500:
        sentiment = "💪 Both buying"
    elif fii * dii < 0:
        sentiment = "🟡 Mixed flows"
    elif fii < -500 and dii < -500:
        sentiment = "⚠️ Both selling"
    else:
        sentiment = "➡️ Neutral"
    return f"FII {fii_lbl} | DII {dii_lbl} | {sentiment}"


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
    return result


def sector_rotation_score(sector: str, rotation: dict) -> tuple:
    data   = rotation.get(sector, {})
    status = data.get("status", "NEUTRAL")
    adj    = {"LEADING": 15, "NEUTRAL": 0, "WEAKENING": -8, "LAGGING": -15}
    return adj.get(status, 0), status


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


def get_upcoming_events(lookahead_days: int = 7) -> list:
    try:
        events = []
        today  = datetime.date.today()
        end    = today + datetime.timedelta(days=lookahead_days)
        RBI_MPC_DATES = [
            "2026-02-07","2026-04-09","2026-06-06",
            "2026-08-08","2026-10-07","2026-12-05",
        ]
        SPECIAL_DATES = {
            "2026-07-01": "Union Budget Presentation",
            "2026-03-31": "Financial Year End",
        }
        current = today
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            if date_str in RBI_MPC_DATES:
                events.append(f"RBI MPC Decision — {current.strftime('%d %b')}")
            if date_str in SPECIAL_DATES:
                events.append(f"{SPECIAL_DATES[date_str]} — {current.strftime('%d %b')}")
            if current.weekday() == 3:
                next_thu = current + datetime.timedelta(weeks=1)
                if next_thu.month != current.month:
                    events.append(f"NSE Monthly Expiry — {current.strftime('%d %b')}")
                else:
                    events.append(f"NSE Weekly Expiry — {current.strftime('%d %b')}")
            current += datetime.timedelta(days=1)
        return events
    except Exception as e:
        _log(f"[WARN] get_upcoming_events failed: {e}")
        return []


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
    if score >= 80:   regime = "STRONG_BULL"
    elif score >= 65: regime = "BULL"
    elif score >= 50: regime = "SIDEWAYS"
    elif score >= 40: regime = "TRANSITION"
    elif score >= 30: regime = "HIGH_VOLATILITY"
    elif score >= 20: regime = "BEAR"
    else:             regime = "STRONG_BEAR"

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


def compute_position_size(entry: float, stop: float, capital: float,
                           risk_per_trade: float = 0.015,
                           max_position_pct: float = 0.25) -> dict:
    """Risk-based position sizing — always returns non-zero for valid inputs."""
    try:
        if entry <= 0 or stop <= 0 or stop >= entry or capital <= 0:
            return {"shares": 0, "position_value": 0.0, "position_pct": 0.0,
                    "risk_amount": 0.0, "risk_pct": 0.0}
        risk_per_share = entry - stop
        risk_amount    = capital * risk_per_trade
        shares         = max(1, int(risk_amount / risk_per_share))
        position_value = shares * entry
        if position_value > capital * max_position_pct:
            shares         = max(1, int((capital * max_position_pct) / entry))
            position_value = shares * entry
        position_pct = (position_value / capital) * 100
        return {
            "shares":         shares,
            "position_value": round(position_value, 2),
            "position_pct":   round(position_pct, 1),
            "risk_amount":    round(risk_amount, 2),
            "risk_pct":       round(risk_per_trade * 100, 1),
        }
    except Exception:
        return {"shares": 0, "position_value": 0.0, "position_pct": 0.0,
                "risk_amount": 0.0, "risk_pct": 0.0}


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
                        heat: dict, max_position_pct: float = 0.25) -> dict:
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

        return {
            "shares":          shares,
            "position_value":  round(position_value, 2),
            "position_pct":    round(position_pct, 1),
            "risk_amount":     round(risk_amount, 2),
            "risk_pct":        round(risk_per_trade * 100, 2),
            "sizing_method":   sizing_method,
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
        "final_confidence": 0.0, "base_confidence": 0.0,
        "weekly_trend_ok": False, "price_pattern": "NONE", "rs_diff21": 0.0,
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
        if sector_rotation:
            rotation_adj, sector_status = sector_rotation_score(sector, sector_rotation)
        result["sector_strength"] = max(0, min(100, 50 + rotation_adj))
        result["sector_status"]   = sector_status

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


def build_portfolio_ai_summary(alerts: list) -> str:
    """Compact <200-token summary — safe to pass to Groq if commentary needed."""
    lines = []
    for a in alerts:
        lines.append(f"{a['symbol']}: {a['action']} ({a['reason']}) PnL={a.get('pnl_pct', 0):.1f}%")
    return "\n".join(lines[:10])


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

    # Gate 9: Sector Health (SOFT / HARD if LAGGING)
    sector_status = stock.get("sector_status", "NEUTRAL")
    if sector_status == "LAGGING":
        fail_reasons.append("SECTOR_LAGGING")
    elif sector_status == "WEAKENING":
        warnings.append("SECTOR_WEAKENING")

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

    hard_fails = [f for f in fail_reasons if "WARNING" not in f]
    if hard_fails:
        # PORTFOLIO_FULL never blocks watchlist — stock is valid, just capacity is full today
        # Strip it before deciding WATCHLIST vs REJECTED so it doesn't poison the check
        scoreable_fails = [f for f in hard_fails if "PORTFOLIO_FULL" not in f]
        soft_only = all("EVENT_BLOCK" in f or "HIGH_CORR" in f for f in scoreable_fails)
        if not scoreable_fails:
            # Only PORTFOLIO_FULL failed — valid setup, just no room
            decision = "WATCHLIST"
        elif len(scoreable_fails) <= 2 and (soft_only or all("FAIL" in f and "LAGGING" not in f for f in scoreable_fails)):
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


def classify_watchlist(stock: dict, regime: str, thresholds: dict) -> dict:
    thresh   = (thresholds or REGIME_THRESHOLDS)[regime]
    min_conf = thresh["min_confidence"]
    conf     = stock.get("final_confidence", 0)
    tq       = stock.get("trade_quality_score", 0)
    conf_gap = round(min_conf - conf, 1)

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
        "risk_pct": levels["risk_pct"],
        "current":  levels["current"],
        "fail_reasons": stock.get("fail_reasons", []),
        "warnings":     stock.get("warnings", []),
    }

    # Tier logic: relative to regime gap size (not absolute confidence)
    # NEAR_MISS  = gap <= 8  (very close — watch daily)
    # DEVELOPING = gap <= 18 (building — watch weekly)
    # MONITOR    = gap > 18  (early stage — track loosely)
    if conf_gap <= 8 and tq >= thresh["min_tq"] - 5:
        return {**base, "tier": "NEAR_MISS",
                "note": f"Needs +{conf_gap:.1f} conf. Watch for volume trigger.",
                "days_to_watch": 3, "watch_days": 3}
    elif conf_gap <= 18 and tq >= 70:
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


def format_tracker_daily(open_entries: list, closed_today: list, timestamp: str) -> str:
    lines = []
    lines.append("─" * 40)
    lines.append(f"SIGNAL TRACKER — {timestamp}")
    lines.append(f"Open: {len(open_entries)} | Closed today: {len(closed_today)}")
    lines.append("─" * 40)
    open_buys      = [e for e in open_entries if e["type"] == "BUY"       and e["status"] == "OPEN"]
    open_near_miss = [e for e in open_entries if e["type"] == "NEAR_MISS" and e["status"] == "OPEN"]

    lines.append("")
    lines.append("BUY SIGNALS TRACKING")
    if open_buys:
        for e in open_buys:
            lines.extend(_entry_block(e))
            lines.append("")
    else:
        lines.append("  No open BUY signals.")

    lines.append("")
    lines.append("NEAR MISS TRACKING")
    lines.append("  (Tracking near-miss setups for comparison)")
    if open_near_miss:
        for e in open_near_miss:
            lines.extend(_entry_block(e))
            lines.append("")
    else:
        lines.append("  No open near-miss signals.")

    if closed_today:
        lines.append("")
        lines.append("CLOSED TODAY")
        for e in closed_today:
            pnl    = e.get("final_pnl_pct", 0)
            emoji  = "✅" if pnl >= 0 else "❌"
            days   = _days_open(e)
            lines.append(f"  {emoji} <b>{e['symbol']}</b> [{e['type']}] — {e['close_reason']}")
            lines.append(f"  Entry Rs{e.get('entry', 0):.2f} → Close Rs{e.get('close_price', 0):.2f} | PnL: <b>{pnl:+.2f}%</b> in {days}d")
            lines.append(f"  Max gain: {e['max_gain_pct']:+.2f}% | Max loss: {e['max_loss_pct']:+.2f}%")
            lines.append("")

    lines.append("─" * 40)
    lines.append("Tracker only. Prices ~15min delayed.")
    lines.append("─" * 40)
    return "\n".join(lines)


def format_tracker_close_summary(closed_entries: list) -> str:
    if not closed_entries:
        return ""
    lines = []
    for e in closed_entries:
        pnl   = e.get("final_pnl_pct", 0)
        days  = (datetime.date.fromisoformat(e["close_date"]) -
                 datetime.date.fromisoformat(e["suggested_date"])).days
        emoji = "✅ WIN" if pnl >= 0 else "❌ LOSS"
        lines += [
            "─" * 40,
            f"TRADE CLOSED — {e['symbol']}",
            "─" * 40,
            f"Signal type : {e['type']}",
            f"Signal date : {e['suggested_date']}",
            f"Close date  : {e['close_date']} ({days} days held)",
            f"Outcome     : {emoji}",
            f"Reason      : {e['close_reason']}",
            f"Entry       : Rs{e.get('entry', 0):.2f}",
            f"Exit        : Rs{e.get('close_price', 0):.2f}",
            f"PnL         : {pnl:+.2f}%",
            f"Max gain    : {e['max_gain_pct']:+.2f}%",
            f"Max loss    : {e['max_loss_pct']:+.2f}%",
            "─" * 40,
        ]
    return "\n".join(lines)


def format_tracker_stats(all_entries: list) -> str:
    closed = [e for e in all_entries if e["status"] == "CLOSED"]
    if not closed:
        return "No closed trades yet."
    wins    = [e for e in closed if e.get("final_pnl_pct", 0) >= 0]
    losses  = [e for e in closed if e.get("final_pnl_pct", 0) < 0]
    avg_pnl = sum(e.get("final_pnl_pct", 0) for e in closed) / len(closed)
    avg_days = sum(
        (datetime.date.fromisoformat(e["close_date"]) -
         datetime.date.fromisoformat(e["suggested_date"])).days
        for e in closed if e.get("close_date") and e.get("suggested_date")
    ) / max(1, len(closed))
    best  = max(closed, key=lambda x: x.get("final_pnl_pct", 0))
    worst = min(closed, key=lambda x: x.get("final_pnl_pct", 0))
    lines = [
        "─" * 40,
        "SIGNAL TRACKER — LIFETIME STATS",
        "─" * 40,
        f"Total closed : {len(closed)}",
        f"Win rate     : {len(wins)}/{len(closed)} ({len(wins)/len(closed)*100:.1f}%)",
        f"Loss rate    : {len(losses)}/{len(closed)} ({len(losses)/len(closed)*100:.1f}%)",
        f"Avg PnL      : {avg_pnl:+.2f}%",
        f"Avg hold     : {avg_days:.1f} days",
        f"Best trade   : {best['symbol']} {best.get('final_pnl_pct', 0):+.2f}%",
        f"Worst trade  : {worst['symbol']} {worst.get('final_pnl_pct', 0):+.2f}%",
        "─" * 40,
    ]
    return "\n".join(lines)


def maybe_send_weekly_stats(tracker_entries: list) -> None:
    pass  # Weekly stats now stored in recommendation_tracker.xlsx via research_job.py


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
            if not hist or hist[-1].get("date") != today_str:
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


def format_tracker_for_telegram(tracker: dict) -> str:
    """Format tracker v2 for embedding in main Telegram message. Empty string if nothing to show."""
    active   = tracker.get("buys", [])
    watching = tracker.get("watchlist", [])
    perf     = tracker.get("performance", {})

    # Don't show section if nothing to track
    if not active and not watching and not perf.get("completed", 0):
        return ""

    lines = ["📈 TRADE TRACKER"]

    if active:
        lines.append("  ACTIVE:")
        for pos in active:
            hist    = pos.get("pnl_history", [])
            cur_pnl = hist[-1]["pnl"]   if hist else 0.0
            cur_px  = hist[-1]["price"] if hist else pos.get("entry", 0)
            day_n   = pos.get("days_tracked", 1)
            status  = html.escape(str(pos.get("status", "ACTIVE")))
            entry   = pos.get("entry",   0)
            stop    = pos.get("stop",    0)
            t1      = pos.get("target1", 0)
            t2      = pos.get("target2", 0)

            # Progress bar [====>......] between stop and T2
            total_range = (t2 - stop) if t2 > stop else 1
            cur_pos     = (cur_px - stop)
            fill        = int((cur_pos / total_range) * 10) if total_range > 0 else 0
            fill        = max(0, min(10, fill))
            bar         = "=" * fill + ">" + "." * (10 - fill)

            dist_stop = round((cur_px - stop) / cur_px * 100, 1) if cur_px > 0 else 0
            dist_t1   = round((t1 - cur_px) / cur_px * 100, 1) if cur_px > 0 and t1 > 0 else 0

            lines.append(
                f"  {html.escape(str(pos['symbol']))} | Day {day_n}/15 | "
                f"PnL {cur_pnl:+.1f}% | {status}"
            )
            lines.append(f"  [{bar}] Rs{cur_px:.1f}")
            lines.append(
                f"  Stop Rs{stop:.1f} ({dist_stop:.1f}% below) | "
                f"T1 Rs{t1:.1f} ({dist_t1:.1f}% away) | T2 Rs{t2:.1f}"
            )

    if watching:
        lines.append("  WATCHING:")
        for w in watching:
            tier      = html.escape(str(w.get("tier", "MONITOR")))
            day_n     = w.get("days_watching", 1)
            conf_gap  = w.get("conf_gap_at_rec", w.get("conf_gap", 0))
            direction = w.get("direction", "\u2014")  # populated by update_tracker_v2_pnl
            cur_px    = w.get("current_price", 0)
            entry     = w.get("entry", 0)
            lines.append(
                f"  {html.escape(str(w['symbol']))} [{tier}] Day {day_n}/14 | "
                f"Gap was {conf_gap:.1f} | {direction}"
            )
            if entry > 0 and cur_px > 0:
                lines.append(
                    f"    Cur Rs{cur_px:.1f} | Entry Rs{entry:.1f} | "
                    f"Stop Rs{w.get('stop', 0):.1f} | T1 Rs{w.get('target1', 0):.1f}"
                )

    if perf.get("completed", 0) > 0:
        lines.append(
            f"  SCORE: WinRate {perf.get('win_rate', 0):.0f}% | "
            f"AvgW {perf.get('avg_win', 0):+.1f}% | "
            f"AvgL {perf.get('avg_loss', 0):+.1f}% | "
            f"Completed: {perf['completed']}"
        )

    return "\n".join(lines)


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
    """Shows exactly what passed/failed for each near miss (ENHANCEMENT 3)."""
    conf = float(stock.get("final_confidence", 0) or 0)
    tq   = float(stock.get("trade_quality_score", 0) or 0)
    rr   = float(stock.get("rr_ratio", 0) or stock.get("rr", 0) or 0)
    opp  = float(stock.get("opportunity_score", 0) or 0)
    lines = [f"     Opp Score: {opp:.1f} | Failed Checks:"]
    min_c = thresh.get("min_confidence", 80)
    min_t = thresh.get("min_tq", 78)
    min_r = thresh.get("min_rr", 2.0)
    if conf >= min_c:
        lines.append(f"     ✓ Confidence {conf:.1f} — PASSED")
    else:
        lines.append(f"     ✗ Confidence {conf:.1f} — need +{min_c - conf:.1f}")
    if tq >= min_t:
        lines.append(f"     ✓ TQ {tq:.1f} — PASSED")
    else:
        lines.append(f"     ✗ TQ {tq:.1f} — need +{min_t - tq:.1f}")
    if rr >= min_r:
        lines.append(f"     ✓ R/R {rr:.2f}x — PASSED")
    else:
        lines.append(f"     ✗ R/R {rr:.2f}x — need +{min_r - rr:.2f}x")
    lines.append("     → Improve by: volume surge or price consolidation above entry")
    return lines


def format_conviction_meter(regime_score: float, breadth: float,
                             fii: float, dii: float) -> list:
    """Visual conviction bar (ENHANCEMENT 4)."""
    try:
        combined_flow = fii + dii
        conviction = round(
            regime_score * 0.5 +
            breadth * 0.3 +
            min(100, max(0, 50 + combined_flow / 200)) * 0.2
        , 1)
        filled = int(conviction / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        if conviction >= 75:   label = "🟢 Strong Buying Environment"
        elif conviction >= 55: label = "🟡 Moderate — Selective Only"
        elif conviction >= 40: label = "🟠 Weak — Caution Required"
        else:                  label = "🔴 Poor — Avoid New Positions"
        return [
            f"  Market Conviction: [{bar}] {conviction:.0f}%",
            f"  {label}",
        ]
    except Exception:
        return []


def format_risk_meter(nifty_close: float, ema20: float, ema50: float,
                       ema200: float, vix_in: float, breadth: float) -> list:
    """Shows current market risk level with reasons (ENHANCEMENT 4)."""
    try:
        risk_factors = []
        risk_score   = 0
        if nifty_close > 0 and ema20 > 0:
            if nifty_close < ema20:
                risk_factors.append("✗ Below EMA20");   risk_score += 25
            else:
                risk_factors.append("✓ Above EMA20")
        if nifty_close > 0 and ema50 > 0:
            if nifty_close < ema50:
                risk_factors.append("✗ Below EMA50");   risk_score += 25
            else:
                risk_factors.append("✓ Above EMA50")
        if nifty_close > 0 and ema200 > 0:
            if nifty_close < ema200:
                risk_factors.append("✗ Below EMA200");  risk_score += 25
            else:
                risk_factors.append("✓ Above EMA200")
        if vix_in > 20:
            risk_factors.append(f"✗ VIX elevated {vix_in:.1f}"); risk_score += 15
        else:
            risk_factors.append(f"✓ VIX normal {vix_in:.1f}")
        if breadth < 40:
            risk_factors.append(f"✗ Weak breadth {breadth:.0f}%"); risk_score += 10
        else:
            risk_factors.append(f"✓ Breadth ok {breadth:.0f}%")
        if risk_score >= 75:   risk_label = "🔴 EXTREME"
        elif risk_score >= 50: risk_label = "🟠 HIGH"
        elif risk_score >= 25: risk_label = "🟡 MEDIUM"
        else:                  risk_label = "🟢 LOW"
        lines = [f"  Market Risk: {risk_label}"]
        for f in risk_factors:
            lines.append(f"    {f}")
        return lines
    except Exception:
        return []


def format_breadth_dashboard(total_scanned: int, qualified: int,
                              near_buy: int, developing: int,
                              monitor: int, rejected: int,
                              yesterday: dict = None) -> list:
    """Universe breadth stats with delta arrows (ENHANCEMENT 5)."""
    lines = ["  Market Breadth:"]
    stats = [
        ("Scanned",    total_scanned),
        ("Qualified",  qualified),
        ("Near Buy",   near_buy),
        ("Developing", developing),
        ("Monitor",    monitor),
        ("Rejected",   rejected),
    ]
    for label, val in stats:
        if yesterday:
            prev  = yesterday.get(label.lower().replace(" ", "_"), val)
            delta = val - prev
            arrow = f" ▲+{delta}" if delta > 0 else (f" ▼{delta}" if delta < 0 else " →")
        else:
            arrow = ""
        lines.append(f"    {label:<12} {val:>6}{arrow}")
    return lines


def format_buy_card(stock: dict, sizing: dict, regime: str) -> list:
    """Enhanced BUY card with thesis, catalysts, and confidence breakdown (ENHANCEMENT 6)."""
    try:
        opp    = float(stock.get("opportunity_score", 0) or 0)
        conf   = float(stock.get("final_confidence", 0) or 0)
        tq     = float(stock.get("trade_quality_score", 0) or 0)
        rr     = float(stock.get("rr_ratio", 0) or 0)
        sector = get_sector(stock.get("symbol", ""))
        cats   = stock.get("catalysts", []) or []

        # Conviction icon
        if opp >= 85:   icon = "🔥"
        elif opp >= 75: icon = "⚡"
        else:           icon = "📈"

        # One-line thesis
        fs    = stock.get("factor_scores", {}) or {}
        trend_s = float(fs.get("trend_quality", 0) or 0)
        rs_s    = float(fs.get("rs_vs_nifty", 0) or 0)
        thesis_parts = []
        if trend_s > 70:              thesis_parts.append("strong uptrend")
        if rs_s > 70:                 thesis_parts.append("outperforming NIFTY")
        if "VOL_SURGE" in cats:       thesis_parts.append("volume expansion")
        if "NEAR_52W_HIGH" in cats:   thesis_parts.append("near 52W high breakout")
        thesis = ", ".join(thesis_parts) if thesis_parts else "multi-factor confluence"

        sym     = html.escape(str(stock.get("symbol", "")))
        entry   = stock.get("entry", 0)
        stop_p  = stock.get("stop", 0)
        t1      = stock.get("target1", 0)
        t2      = stock.get("target2", 0)
        risk_p  = round((entry - stop_p) / entry * 100, 1) if entry > 0 else 0
        pos_val = sizing.get("position_value", stock.get("position_value", 0))
        pos_pct = sizing.get("position_pct", stock.get("position_pct", 0))
        shares  = sizing.get("shares", stock.get("shares", 0))
        max_loss= sizing.get("max_loss", stock.get("max_loss", 0))
        news    = truncate_display(stock.get("news_summary", ""), 120)

        lines = [
            f"  {icon} <b>{sym}</b> [{html.escape(str(sector))}]",
            f"     Opp Score: {opp:.1f} | Conf: {conf:.1f} | TQ: {tq:.1f} | R/R: {rr:.2f}x",
            f"     Entry  Rs{entry:.2f} | Stop Rs{stop_p:.2f} ({risk_p:.1f}%)",
            f"     T1     Rs{t1:.2f} | T2 Rs{t2:.2f}",
            f"     Size   Rs{pos_val:,.0f} ({pos_pct:.1f}%) | Shares {shares} | MaxLoss Rs{max_loss:,.0f}",
            f"     ROE {stock.get('roe', 0):.1f}% | D/E {stock.get('de_ratio', 0):.2f} | Pledge {stock.get('promoter_pledge_pct', 0):.0f}%",
            f"     Thesis: {html.escape(str(thesis))}",
        ]
        if cats:
            lines.append(f"     Catalysts: {html.escape(' | '.join(str(c) for c in cats))}")
        if news and news != "—":
            lines.append(f"     News: {html.escape(str(news))}")
        # Confidence breakdown
        lines += format_confidence_breakdown(fs, conf)
        return lines
    except Exception:
        return [f"  📈 <b>{html.escape(str(stock.get('symbol', '?')))}</b>"]


def format_portfolio_card(alert: dict, current_price: float) -> list:
    """Enhanced portfolio card with Hold Score, R-multiple, ATR stop (ENHANCEMENT 7)."""
    try:
        symbol = str(alert.get("symbol", ""))
        entry  = float(alert.get("entry", 0) or 0)
        stop   = float(alert.get("stop_loss", alert.get("stop", 0)) or 0)
        t1     = float(alert.get("target1", 0) or 0)
        t2     = float(alert.get("target2", 0) or 0)
        days   = int(alert.get("days_held", 0) or 0)
        pnl_p  = float(alert.get("pnl_pct", 0) or 0)
        action = str(alert.get("action", "HOLD"))

        risk_per_share = entry - stop
        gain_per_share = current_price - entry
        r_multiple = round(gain_per_share / risk_per_share, 2) if risk_per_share > 0 else 0.0

        # ATR-based trailing stop
        try:
            df = yf.download(symbol, period="1mo", interval="1d",
                             progress=False, auto_adjust=True)
            if df is not None and len(df) >= 14:
                atr = float(np.mean(df["High"].values[-14:] - df["Low"].values[-14:]))
                atr_stop = round(current_price - atr * 2, 2)
            else:
                atr_stop = stop
        except Exception:
            atr_stop = stop

        remaining_upside = round((t2 - current_price) / current_price * 100, 1) if t2 > current_price else 0.0
        dist_stop = round((current_price - max(stop, atr_stop)) / current_price * 100, 1) if current_price > 0 else 0.0

        hold_score = 50
        if pnl_p > 0:              hold_score += 20
        if r_multiple > 1:         hold_score += 15
        if days < 15:              hold_score += 10
        if remaining_upside > 5:   hold_score += 5
        hold_score = min(100, hold_score)

        action_icon = {"HOLD": "✅", "EXIT": "🔴", "EXIT_FULL": "🔴",
                       "TRAIL_STOP": "🟡", "REVIEW": "🟠"}.get(action, "✅")

        return [
            f"  {action_icon} <b>{html.escape(symbol)}</b> | {action} | Day {days}",
            f"     Entry Rs{entry:.2f} → Now Rs{current_price:.2f} | PnL {pnl_p:+.1f}% | {r_multiple:+.2f}R",
            f"     Hold Score: {hold_score}/100 | Remaining Upside: {remaining_upside:.1f}%",
            f"     Stop Rs{stop:.2f} | ATR Stop Rs{atr_stop:.2f} | Distance: {dist_stop:.1f}%",
            f"     T1 Rs{t1:.2f} | T2 Rs{t2:.2f} | Review in: {max(0, 15 - days)} days",
        ]
    except Exception:
        return [f"  ✅ <b>{html.escape(str(alert.get('symbol', '?')))}</b>"]


def format_portfolio_dashboard(alerts: list, current_prices: dict,
                                total_capital: float) -> list:
    """Portfolio health summary with sector allocation (ENHANCEMENT 8)."""
    try:
        if not alerts:
            return ["  No active holdings."]
        qty_known = [a for a in alerts if a.get("quantity", 0) > 0]
        total_invested = sum(
            float(a.get("entry", 0)) * float(a.get("quantity", 0))
            for a in qty_known
        )
        total_current = sum(
            current_prices.get(a["symbol"], float(a.get("entry", 0))) *
            float(a.get("quantity", 0))
            for a in qty_known
        )
        total_pnl_pct = round((total_current - total_invested) / total_invested * 100, 2) \
                        if total_invested > 0 else 0.0
        exposure_pct  = round(total_invested / total_capital * 100, 1) if total_capital > 0 else 0.0
        cash_pct      = round(100 - exposure_pct, 1)

        sector_exp: dict = {}
        for a in qty_known:
            s   = get_sector(a.get("symbol", ""))
            val = float(a.get("entry", 0)) * float(a.get("quantity", 0))
            sector_exp[s] = sector_exp.get(s, 0) + val

        lines = [
            "  💼 Portfolio Health Dashboard",
            f"    Invested:  Rs{total_invested:,.0f} ({exposure_pct:.1f}%)",
            f"    Current:   Rs{total_current:,.0f} | PnL {total_pnl_pct:+.2f}%",
            f"    Cash:      {cash_pct:.1f}% available",
            f"    Positions: {len(alerts)}",
        ]
        if sector_exp:
            lines.append("    Sector Allocation:")
            for sec, val in sorted(sector_exp.items(), key=lambda x: -x[1]):
                pct = round(val / total_capital * 100, 1) if total_capital > 0 else 0.0
                lines.append(f"      {html.escape(sec):<16} {pct:.1f}%")
        return lines
    except Exception:
        return ["  💼 Portfolio Health Dashboard (unavailable)"]


def format_daily_summary(regime: str, buys: list, watchlist: list,
                          portfolio_alerts: list, macro: dict,
                          breadth: float) -> list:
    """Executive briefing at end of Telegram report (ENHANCEMENT 9)."""
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

        nifty_below = macro.get("nifty_below_all_emas", False)
        if nifty_below:
            lines.append("  NIFTY trading below all major EMAs — risk remains elevated.")
        else:
            lines.append("  NIFTY structure supportive of new positions.")

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


def format_system_snapshot(tracker_v2: dict) -> list:
    """System performance footer (ENHANCEMENT 10)."""
    try:
        tracker = tracker_v2 or {}
        perf    = tracker.get("performance", {}) or {}
        active  = tracker.get("buys", []) or []
        watching = tracker.get("watchlist", []) or []

        open_pnls = []
        for pos in active:
            hist = pos.get("pnl_history", [])
            if hist:
                open_pnls.append(hist[-1].get("pnl", 0))
        avg_open_pnl = round(sum(open_pnls) / len(open_pnls), 1) if open_pnls else 0.0

        wr = float(perf.get("win_rate", 0) or 0)
        if wr >= 60:       health = "🟢 Excellent"
        elif wr >= 45:     health = "🟡 Good"
        elif wr >= 35:     health = "🟠 Neutral"
        elif not perf.get("completed"): health = "⚪ No data yet"
        else:              health = "🔴 Weak"

        return [
            "",
            "📊 SYSTEM PERFORMANCE SNAPSHOT",
            f"  Active Positions:     {len(active)}",
            f"  Avg Open Return:      {avg_open_pnl:+.1f}%",
            f"  Watchlist Tracking:   {len(watching)} stocks",
            f"  Completed Trades:     {perf.get('completed', 0)}",
            f"  Win Rate (all-time):  {wr:.1f}%",
            f"  Avg Win:              {perf.get('avg_win', 0):+.1f}%",
            f"  Avg Loss:             {perf.get('avg_loss', 0):+.1f}%",
            f"  Strategy Health:      {health}",
        ]
    except Exception:
        return []


def format_watchlist_section(watchlist: list, regime: str) -> list:
    """
    NEAR MISS  — full detail, sorted by R/R descending (fully actionable)
    DEVELOPING — compact 1-liner each, top 5 by confidence, rest collapsed
    MONITOR    — single collapsed count line only (not actionable today)
    """
    thresh   = REGIME_THRESHOLDS[regime]
    min_conf = thresh["min_confidence"]

    near = sorted([w for w in watchlist if w.get("tier") == "NEAR_MISS"],
                  key=lambda x: x.get("rr", 0), reverse=True)
    dev  = sorted([w for w in watchlist if w.get("tier") == "DEVELOPING"],
                  key=lambda x: x.get("conf", x.get("final_confidence", 0)), reverse=True)
    mon  = [w for w in watchlist if w.get("tier") == "MONITOR"]

    lines = [f"\U0001f441 WATCHLIST \u2014 {len(watchlist)} stocks (threshold {min_conf})"]

    # -- NEAR MISS: full detail per stock, sorted by opportunity score then R/R ---------
    if near:
        lines.append(f"  \U0001f534 NEAR MISS ({len(near)} \u2014 within 8 pts of BUY, best R/R first):")
        for w in near:
            sym      = html.escape(str(w["symbol"]))
            sector   = html.escape(str(w.get("sector", "DIVERSIFIED")))
            conf     = w.get("conf", w.get("final_confidence", 0))
            tq       = w.get("tq", w.get("trade_quality_score", 0))
            entry    = w.get("entry", 0)
            stop     = w.get("stop", 0)
            target1  = w.get("target1", 0)
            target2  = w.get("target2", 0)
            rr       = w.get("rr", 0)
            risk     = w.get("risk_pct", 0)
            cur      = w.get("current", w.get("price", entry))
            opp      = w.get("opportunity_score", 0)
            lines.append(f"    <b>{sym}</b> [{sector}] | Opp {opp:.1f} | Conf {conf:.1f} | TQ {tq:.1f}")
            lines.append(f"    Entry Rs{entry:.2f} | Stop Rs{stop:.2f} ({risk:.1f}%) | T1 Rs{target1:.2f} | T2 Rs{target2:.2f} | R/R {rr:.1f}x")
            lines.append(f"    (Cur Rs{cur:.1f})")
            # Near miss failure breakdown (ENHANCEMENT 3)
            lines.extend(format_near_miss_failures(w, thresh))
            if w.get("warnings"):
                lines.append(f"    \u26a0\ufe0f  {html.escape(' | '.join(str(x) for x in w['warnings']))}")

    # -- DEVELOPING: compact 1-liner, top 5, rest collapsed ---------------
    if dev:
        shown = dev[:5]
        rest  = dev[5:]
        lines.append(f"  \U0001f7e1 DEVELOPING ({len(dev)} \u2014 building, not ready yet):")
        for w in shown:
            sym    = html.escape(str(w["symbol"]))
            sector = html.escape(str(w.get("sector", "OTHERS")))
            conf   = w.get("conf", w.get("final_confidence", 0))
            rr     = w.get("rr", 0)
            risk   = w.get("risk_pct", 0)
            lines.append(f"    {sym} [{sector}] Conf {conf:.0f} | R/R {rr:.1f}x | Risk {risk:.1f}%")
        if rest:
            rest_names = ", ".join(w["symbol"].replace(".NS", "") for w in rest)
            lines.append(f"    + {len(rest)} more: {rest_names}")

    # -- MONITOR: collapsed to one line only ------------------------------
    if mon:
        best = max(mon, key=lambda x: x.get("rr", 0))
        best_sym = best["symbol"].replace(".NS", "")
        lines.append(
            f"  \U0001f535 MONITOR ({len(mon)} early-stage) \u2014 "
            f"best R/R: {best_sym} {best.get('rr', 0):.1f}x"
        )

    if not near and not dev and not mon:
        lines.append("  None today.")

    return lines


def format_no_buy_explanation(top_rejected: list, regime: str) -> list:
    """
    When buys=0, show top 3 closest rejected stocks with exact gap to passing.
    """
    thresh = REGIME_THRESHOLDS[regime]
    lines  = ["  None — no setup cleared all gates today."]
    lines.append(
        f"  (Need: Conf≥{thresh['min_confidence']} | "
        f"TQ≥{thresh['min_tq']} | R/R≥{thresh['min_rr']})"
    )
    if not top_rejected:
        return lines
    lines.append("")
    lines.append("  Closest candidates:")
    top3 = sorted(top_rejected, key=lambda x: x.get("final_confidence", 0), reverse=True)[:3]
    for i, s in enumerate(top3):
        conf     = s.get("final_confidence", 0)
        tq       = s.get("trade_quality_score", 0)
        rr       = s.get("rr_ratio", 0)
        conf_gap = thresh["min_confidence"] - conf
        tq_gap   = max(0, thresh["min_tq"] - tq)
        rr_gap   = max(0, thresh["min_rr"] - rr)
        fails    = s.get("fail_reasons", [])
        lines.append(f"  #{i+1} {html.escape(str(s.get('symbol','?')))} [{html.escape(str(s.get('sector','?')))}]")
        lines.append(
            f"     Conf {conf:.1f} (need +{conf_gap:.1f}) | "
            f"TQ {tq:.1f} (need +{tq_gap:.1f}) | "
            f"R/R {rr:.2f}x (need +{rr_gap:.2f})"
        )
        lines.append(f"     Blockers: {html.escape(', '.join(str(f) for f in fails) if fails else 'none recorded')}")
    return lines


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
                              breadth_20: float = 50.0) -> str:
    lines  = []
    regime = regime_data["regime"]
    score  = regime_data["score"]
    thresh = regime_data["thresholds"]

    # ── Header ──
    lines.append("═" * 40)
    lines.append(f"NSE SWING BRIEF — {timestamp}")
    lines.append("═" * 40)
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
    lines.append("📊 MARKET REGIME")
    lines.append(f"  🟡 {regime} | Score: {score:.0f}/100 | MaxBuys: {thresh['max_buys']}")

    # Regime explanation (Patch 4)
    fii_flow = macro.get("fii_flow_cr", 0.0)
    dii_flow = macro.get("dii_flow_cr", 0.0)
    # Pass NIFTY EMA values so EMA structure is reflected in the Why line
    _kl = key_levels or {}
    reg_why = regime_explanation(
        score, regime, vix_in, breadth_20, fii_flow, dii_flow,
        nifty_close = float(_kl.get("last",   0) or 0),
        ema20       = float(_kl.get("ema20",  0) or 0),
        ema50       = float(_kl.get("ema50",  0) or 0),
        ema200      = float(_kl.get("ema200", 0) or 0),
    )
    lines.append(f"  Why: {html.escape(str(reg_why))}")
    lines.append(f"  {REGIME_RATIONALE.get(regime, '')}")

    vix_ratio = macro.get("vix_term_ratio", 1.0)
    lines.append(
        f"  VIX-IN {vix_in:.1f} ({vix_in_flag}) | VIX-US {vix_us:.1f} ({vix_us_flag})"
    )
    lines.append(
        f"  NIFTY {macro.get('nifty_1d_pct', 0):+.2f}% | "
        f"S&P {macro.get('sp500_1d_pct', 0):+.2f}% | "
        f"DXY {macro.get('dxy', 0):.1f}"
    )
    lines.append(
        f"  USD/INR {macro.get('usdinr', 0):.2f} | "
        f"Crude ${macro.get('crude_usd', 0):.1f} | "
        f"US10Y {macro.get('us10y', 0):.2f}%"
    )

    # FII/DII (Patch 2)
    fii_dii_line = format_fii_dii(fii_flow, dii_flow)
    lines.append(f"  {fii_dii_line}")

    # ── Market Conviction Meter + Risk Meter ──
    _kl2 = key_levels or {}
    lines.extend(format_conviction_meter(score, breadth_20, fii_flow, dii_flow))
    lines.extend(format_risk_meter(
        float(_kl2.get("last",   0) or 0),
        float(_kl2.get("ema20",  0) or 0),
        float(_kl2.get("ema50",  0) or 0),
        float(_kl2.get("ema200", 0) or 0),
        vix_in, breadth_20,
    ))
    lines.append("")

    lines.append(
        f"  Min Conf {thresh['min_confidence']} | "
        f"Min TQ {thresh['min_tq']} | "
        f"Min R/R {thresh['min_rr']} | "
        f"Max Buys {thresh['max_buys']}"
    )

    # Portfolio heat
    if heat:
        heat_emoji = "🔴" if not heat["heat_ok"] else ("🟡" if heat["heat_pct"] > heat["max_heat_pct"] * 0.6 else "🟢")
        lines.append(
            f"  {heat_emoji} Portfolio Heat: {heat['heat_pct']:.1f}% / {heat['max_heat_pct']:.0f}%"
        )

    # Platt calibration stats
    if platt and platt.get("calibrated"):
        lines.append(
            f"  📊 System WR: {platt['win_rate']:.0%} ({platt['total_closed']} trades) | "
            f"Avg W: +{platt['avg_win_pct']:.1f}% / L: -{platt['avg_loss_pct']:.1f}%"
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
        lines.append(
            f"  52W H: {kl.get('high_52w', '—')} "
            f"({kl.get('dist_from_52w_high_pct', 0):.1f}% away) | "
            f"52W L: {kl.get('low_52w', '—')}"
        )
        lines.append(
            f"  20D Range: {kl.get('recent_low_20d', '—')} — {kl.get('recent_high_20d', '—')}"
        )
        # One-line structure interpretation
        try:
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

    # ── Breadth Dashboard ──
    _near_c = len([w for w in watchlist if w.get("tier") == "NEAR_MISS"])
    _dev_c  = len([w for w in watchlist if w.get("tier") == "DEVELOPING"])
    _mon_c  = len([w for w in watchlist if w.get("tier") == "MONITOR"])
    _rej_c  = len(rejected_stocks or [])
    _tot_scanned = len(buys) + len(watchlist) + _rej_c + len(shorts)
    lines.extend(format_breadth_dashboard(
        _tot_scanned, len(buys) + len(watchlist),
        _near_c, _dev_c, _mon_c, _rej_c,
    ))
    lines.append("")

    # ── BUY Signals (Patch 6 for no-buy case) ──
    lines.append("✅ BUY SIGNALS")
    if buys:
        for b in buys:
            sizing = {
                "position_value": b.get("position_value", 0),
                "position_pct":   b.get("position_pct", 0),
                "shares":         b.get("shares", 0),
                "max_loss":       b.get("max_loss", 0),
            }
            lines.extend(format_buy_card(b, sizing, regime))
            # Gap validity (morning price check)
            entry  = b.get("entry", 0)
            stop_p = b.get("stop", 0)
            t1     = b.get("target1", 0)
            rr_v   = b.get("rr_ratio", 1.8) or 1.8
            gap_check = check_gap_validity(entry, stop_p, t1, rr_v)
            max_entry = gap_check.get("max_valid_entry", 0)
            if max_entry > 0 and max_entry > entry:
                gap_max_pct = round((max_entry - entry) / entry * 100, 1)
                lines.append(f"  ⚡ Max valid entry: Rs{max_entry:.2f} (+{gap_max_pct:.1f}%)")
                lines.append(f"     If open > Rs{max_entry:.2f} → SKIP. Wait for pullback.")
            sizing_method = b.get("sizing_method", "")
            if sizing_method:
                lines.append(f"  Sizing: {html.escape(str(sizing_method))}")
            rs_diff = b.get("rs_diff21", 0)
            lines.append(f"  RS vs Nifty (21d): {rs_diff:+.1f}%")
            weekly_ok = b.get("weekly_trend_ok", True)
            if not weekly_ok:
                lines.append("  ⚠️ Weekly trend: DOWN — reduced conviction")
            pattern = b.get("price_pattern", "NONE")
            if pattern != "NONE":
                lines.append(f"  Pattern: {html.escape(str(pattern))}")
            accum = b.get("accum_signal", "NEUTRAL")
            if accum != "NEUTRAL":
                lines.append(f"  Volume: {html.escape(str(accum))}")
            ai_sum = truncate_display(b.get("ai_commentary", ""), 90)
            if ai_sum and ai_sum != "—":
                lines.append(f"  AI: {html.escape(str(ai_sum))}")
            if b.get("repeat_tag"):
                lines.append(f"  [{html.escape(str(b['repeat_tag']))}]")
            if b.get("warnings"):
                lines.append(f"  WARN: {html.escape(', '.join(b['warnings'][:3]))}")
            lines.append("  " + "─" * 36)
    else:
        # Patch 6: detailed no-buy explanation
        no_buy_lines = format_no_buy_explanation(rejected_stocks or [], regime)
        lines.extend(no_buy_lines)
    lines.append("")

    # ── SHORT Signals ──
    if shorts:
        lines.append("SHORT SIGNALS")
        for s in shorts:
            lines.append(f"  >> {html.escape(str(s.get('symbol','?')))} [SHORT]")
            lines.append(
                f"     Entry Rs{s.get('entry',0):.2f} | Stop Rs{s.get('stop',0):.2f} | "
                f"T1 Rs{s.get('target1',0):.2f} | T2 Rs{s.get('target2',0):.2f}"
            )
            lines.append(f"     R/R {s.get('rr',0):.2f}x | {html.escape(str(s.get('reason','')))}")
        lines.append("")

    # ── Watchlist — ALL stocks, all tiers, with levels (Patch 1) ──
    wl_lines = format_watchlist_section(watchlist, regime)
    lines.extend(wl_lines)
    lines.append("")

    # ── Portfolio ──
    exits   = [a for a in portfolio_alerts if a["action"] in ("EXIT", "EXIT_FULL")]
    trails  = [a for a in portfolio_alerts if a["action"] == "TRAIL_STOP"]
    reviews = [a for a in portfolio_alerts if a["action"] == "REVIEW"]
    holds   = [a for a in portfolio_alerts if a["action"] == "HOLD"]
    lines.append("📁 PORTFOLIO")
    if portfolio_alerts:
        # Portfolio health dashboard (ENHANCEMENT 8)
        _cur_prices_port = {a["symbol"]: float(a.get("current", a.get("entry", 0)) or 0)
                            for a in portfolio_alerts}
        lines.extend(format_portfolio_dashboard(portfolio_alerts, _cur_prices_port, PORTFOLIO_CAPITAL))
        lines.append("")

        def _fmt_alert_card(a: dict) -> list:
            cur = float(a.get("current", a.get("entry", 0)) or 0)
            card = format_portfolio_card(a, cur)
            # Append exit reason if relevant
            if a.get("reason") and a["action"] not in ("HOLD",):
                card.append(f"     Reason: {html.escape(str(a['reason']))}")
            return card

        if exits:
            lines.append("  🚨 EXIT:")
            for e in exits:
                lines.extend(_fmt_alert_card(e))
        if trails:
            lines.append("  ⚡ TRAIL STOP (T1 hit):")
            for t in trails:
                lines.extend(_fmt_alert_card(t))
        if reviews:
            lines.append("  🔍 REVIEW:")
            for r in reviews:
                lines.extend(_fmt_alert_card(r))
        if holds:
            lines.append("  ✅ HOLD:")
            for h in holds[:6]:
                lines.extend(_fmt_alert_card(h))
    else:
        lines.append("  No active holdings.")
    lines.append("")

    # ── Upcoming Events ──
    if upcoming_events:
        lines.append("UPCOMING EVENTS")
        for ev in upcoming_events:
            lines.append(f"  {html.escape(str(ev))}")
        lines.append("")

    # ── System Performance Snapshot ──
    lines.extend(format_system_snapshot(tracker_v2))

    # ── Daily Summary (executive briefing) ──
    lines.extend(format_daily_summary(
        regime, buys, watchlist, portfolio_alerts, macro, breadth_20
    ))

    # ── Footer ──
    lines.append("")
    lines.append("─" * 40)
    lines.append("⚠️  Recommendation only. Execute manually.")
    lines.append("═" * 40)
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
                     "Sector Analysis", "Regime Analysis", "Monthly Report"]:
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
    _log(f"  Capital: Rs{PORTFOLIO_CAPITAL:,.0f} | Groq keys: {len(GROQ_KEYS)} | Tracker: {'configured' if TRACKER_BOT_TOKEN else 'NOT configured'}")
    _ensure_portfolio_json()

    # ── Sector map (must be first — used by all scoring) ──
    _init_sector_map()

    # ── 0. Market holiday guard ──
    if not is_market_open():
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
    _log(f"  NIFTY {macro['nifty_1d_pct']:+.2f}% | VIX-IN {macro['vix_in']:.1f} | USD/INR {macro['usdinr']:.2f}")

    # ── 1b. FII/DII flows from NSE ──
    _log("[2/17] Fetching FII/DII flows from NSE...")
    fii_dii = fetch_fii_dii_flows()
    macro["fii_flow_cr"] = fii_dii["fii_flow_cr"]
    macro["dii_flow_cr"] = fii_dii["dii_flow_cr"]
    _log(f"  FII: {fii_dii['fii_flow_cr']:+.0f}Cr | DII: {fii_dii['dii_flow_cr']:+.0f}Cr")

    # ── 3. Bulk/block deals ──
    _log("[3/17] Fetching bulk/block deals...")
    bulk_deals = fetch_bulk_deals()
    _log(f"  Bulk deals: {len(bulk_deals)} found")

    # ── 4. Load symbols ──
    _log("[4/17] Loading symbol universe...")
    symbols = load_symbols("stocks.txt")

    # ── 5. Parallel price download + liquidity filter ──
    _log("[5/17] Downloading prices (parallel)...")
    tradable = filter_and_download(symbols, period="6mo", max_workers=12)
    _log(f"  Tradable: {len(tradable)} stocks")

    # ── 5b. Enrich sector map for all tradable symbols ──
    _log("[5b/17] Enriching sector map from yfinance for unknowns...")
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

    # ── 6b. Nifty key levels ──
    key_levels = compute_key_levels(nifty_df)

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

    scored.sort(key=lambda x: x["base_confidence"], reverse=True)
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
        stock["news_risk"]     = max(0, 100 - int(penalty * 2))

    # ── 9. Promoter data + fundamentals — sequential with 24h cache (no rate limiting) ──
    _log("[9/17] Fetching promoter/fundamentals for top 20 (sequential + cached)...")
    top_40 = fetch_all_fundamentals_cached(top_40, max_stocks=20)

    # ── 10. Options PCR for top 20 (parallel) ──
    _log("[10/17] Options PCR for top 20 (parallel)...")

    def _fetch_pcr(stock: dict) -> tuple:
        sym_clean = stock["symbol"].replace(".NS", "")
        oc = fetch_option_chain(sym_clean)
        return stock["symbol"], pcr_score(oc["pcr"])

    pcr_map = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_fetch_pcr, s): s["symbol"] for s in top_40[:20]}
        for fut in as_completed(futs):
            try:
                sym, score_val = fut.result(timeout=15)
                pcr_map[sym] = score_val
            except Exception as e:
                _log(f"[WARN] PCR fetch failed for {futs[fut]}: {e}")

    for stock in top_40[:20]:
        stock["options_sentiment"] = pcr_map.get(stock["symbol"], 60)

    # ── 11. Final confidence ──
    _log("[11/17] Computing final confidence...")
    macro_adj_global = macro_regime_adjustment(macro) * 0.3
    for stock in top_40:
        bulk_adj  = bulk_deal_score(stock["symbol"], bulk_deals)
        base_conf = compute_base_confidence({k: stock.get(k, 50) for k in FACTOR_WEIGHTS})
        stock["base_confidence"]  = base_conf
        stock["final_confidence"] = compute_final_confidence(
            base_conf, regime, stock.get("news_penalty", 0), macro_adj_global, bulk_adj
        )
    top_40.sort(key=lambda x: x["final_confidence"], reverse=True)

    # ── 11b. Opportunity scores (ENHANCEMENT 1) ──
    for stock in top_40:
        stock["opportunity_score"] = compute_opportunity_score(stock)
    top_40.sort(key=lambda x: x["opportunity_score"], reverse=True)

    # ── 12. Portfolio monitoring ──
    _log("[12/17] Monitoring portfolio...")
    holdings       = load_portfolio()
    current_prices = {}
    for h in holdings:
        sym = h.get("symbol", "")
        if sym:
            try:
                df_tmp = fetch_price_data(sym, period="5d")
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
    save_tracker_v2(tracker_v2)
    _log(f"  Tracker V2: {len(tracker_v2.get('buys',[]))} active | {len(tracker_v2.get('watchlist',[]))} watching | {tracker_v2.get('performance',{}).get('completed',0)} completed")

    # ── 13b. Upcoming events (needed by Gate 13 before gate system runs) ──
    upcoming_events = get_upcoming_events(lookahead_days=7)

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
    buys.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    watchlist_stocks.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)

    # Enforce max_buys cap
    max_buys = effective_thresholds[regime]["max_buys"]
    buys = buys[:max_buys]

    # ── 14b. Tag repeat signals ──
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
            max_position_pct=0.25,
        )
        stock.update(pos)

    # ── 14d. Short signal detection ──
    shorts = detect_short_signals(top_40, regime, regime_data["thresholds"])

    # ── 14e. Upcoming market events already computed at step 13b ──

    # ── 14f. Watchlist persistence ──
    wl_history = load_persistent_watchlist()
    watchlist_stocks, wl_history = merge_watchlist_with_history(watchlist_stocks, wl_history)
    save_persistent_watchlist(wl_history)

    # ── 15. Format and send main Telegram message ──
    _log("[15/17] Sending main Telegram report...")
    timestamp = datetime.datetime.now().strftime("%b %d, %Y %H:%M IST")

    # Compute nifty_below_all_emas for daily summary
    _kl_pipe = key_levels or {}
    _nc_close = float(_kl_pipe.get("last", 0) or 0)
    macro["nifty_below_all_emas"] = (
        _nc_close > 0 and
        _nc_close < float(_kl_pipe.get("ema20", 0) or 0) and
        _nc_close < float(_kl_pipe.get("ema50", 0) or 0) and
        _nc_close < float(_kl_pipe.get("ema200", 0) or 0)
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
    )
    _log("--- TELEGRAM PREVIEW (first 1500 chars) ---")
    _log(message[:1500])
    _log("--- END PREVIEW ---")
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

    save_tracker(tracker_entries)

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
