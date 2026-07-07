"""
quality_scores.py
==================
Piotroski F-Score (0-9) and Beneish M-Score for fundamental quality filtering.

Piotroski F-Score (Joseph Piotroski, 2000):
    - 9 binary tests across Profitability, Leverage/Liquidity, Operating Efficiency
    - Score >= 7 = strong, <= 2 = weak
    - Well-known effect: high-F portfolios outperform low-F by ~7.5% p.a.

Beneish M-Score (Messod Beneish, 1999):
    - 8-variable probit model estimating earnings-manipulation likelihood
    - M-Score > -1.78 => likely manipulator (RED FLAG)
    - Famously flagged Enron in 1998 before collapse

Both scores use standard Screener.in / annual-report line items. All inputs
degrade gracefully: missing fields => sub-test scored 0 (F) or skipped (M).

Public API:
    piotroski_f_score(fundamentals: dict) -> tuple[int, dict]
    beneish_m_score(fundamentals: dict) -> tuple[float, dict]
    quality_composite(fundamentals: dict) -> dict     # convenience wrapper

Feature flag: caller controls via env `ENABLE_QUALITY_SCORE=true|false`.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(d: Dict[str, Any], *keys: str, default: Optional[float] = None) -> Optional[float]:
    """Fetch first non-null numeric value across a list of possible key spellings."""
    for k in keys:
        if k in d and d[k] is not None:
            try:
                v = float(d[k])
                if not math.isnan(v) and not math.isinf(v):
                    return v
            except (TypeError, ValueError):
                continue
    return default


def _safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den is None or den == 0:
        return None
    try:
        return num / den
    except (TypeError, ZeroDivisionError):
        return None


# ---------------------------------------------------------------------------
# Piotroski F-Score
# ---------------------------------------------------------------------------

def piotroski_f_score(f: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """
    Compute Piotroski F-Score (0-9). Returns (score, breakdown_dict).

    Expected keys (any of the synonyms in each row):
        net_income        | pat | profit_after_tax
        net_income_prev   | pat_prev
        operating_cf      | cfo | cash_from_operations
        total_assets      | total_assets_cur
        total_assets_prev
        long_term_debt        | ltd
        long_term_debt_prev   | ltd_prev
        current_assets    | ca
        current_liab      | cl | current_liabilities
        current_assets_prev
        current_liab_prev
        shares_outstanding      | shares
        shares_outstanding_prev | shares_prev
        gross_profit           | gp
        gross_profit_prev
        revenue                | sales
        revenue_prev           | sales_prev

    Missing fields => that sub-test scores 0.
    """
    b: Dict[str, Any] = {}  # breakdown

    ni       = _f(f, "net_income", "pat", "profit_after_tax", "net_profit")
    ni_prev  = _f(f, "net_income_prev", "pat_prev", "net_profit_prev")
    cfo      = _f(f, "operating_cf", "cfo", "cash_from_operations")
    ta       = _f(f, "total_assets", "total_assets_cur")
    ta_prev  = _f(f, "total_assets_prev")
    ltd      = _f(f, "long_term_debt", "ltd", "long_term_borrowings")
    ltd_prev = _f(f, "long_term_debt_prev", "ltd_prev")
    ca       = _f(f, "current_assets", "ca")
    cl       = _f(f, "current_liab", "cl", "current_liabilities")
    ca_prev  = _f(f, "current_assets_prev")
    cl_prev  = _f(f, "current_liab_prev")
    sh       = _f(f, "shares_outstanding", "shares")
    sh_prev  = _f(f, "shares_outstanding_prev", "shares_prev")
    gp       = _f(f, "gross_profit", "gp")
    gp_prev  = _f(f, "gross_profit_prev")
    rev      = _f(f, "revenue", "sales", "total_revenue")
    rev_prev = _f(f, "revenue_prev", "sales_prev")

    score = 0

    # --- PROFITABILITY (4 pts) ---
    # 1. Positive Net Income
    t1 = 1 if (ni is not None and ni > 0) else 0
    b["p1_positive_ni"] = t1; score += t1

    # 2. Positive CFO
    t2 = 1 if (cfo is not None and cfo > 0) else 0
    b["p2_positive_cfo"] = t2; score += t2

    # 3. ROA improved (NI/Avg TA current > prev)
    roa_cur  = _safe_div(ni,      ta)
    roa_prev = _safe_div(ni_prev, ta_prev)
    t3 = 1 if (roa_cur is not None and roa_prev is not None and roa_cur > roa_prev) else 0
    b["p3_roa_improved"] = t3; score += t3
    b["_roa_cur"], b["_roa_prev"] = roa_cur, roa_prev

    # 4. CFO > NI (earnings quality)
    t4 = 1 if (cfo is not None and ni is not None and cfo > ni) else 0
    b["p4_cfo_gt_ni"] = t4; score += t4

    # --- LEVERAGE / LIQUIDITY / SOURCE OF FUNDS (3 pts) ---
    # 5. Long-term debt decreased (ratio LTD/TA)
    lev_cur  = _safe_div(ltd,      ta)
    lev_prev = _safe_div(ltd_prev, ta_prev)
    t5 = 1 if (lev_cur is not None and lev_prev is not None and lev_cur < lev_prev) else 0
    b["p5_leverage_decreased"] = t5; score += t5

    # 6. Current ratio improved
    cr_cur  = _safe_div(ca,      cl)
    cr_prev = _safe_div(ca_prev, cl_prev)
    t6 = 1 if (cr_cur is not None and cr_prev is not None and cr_cur > cr_prev) else 0
    b["p6_current_ratio_improved"] = t6; score += t6

    # 7. No new shares issued (shares outstanding not up)
    t7 = 1 if (sh is not None and sh_prev is not None and sh <= sh_prev * 1.02) else 0  # 2% tolerance for buybacks/ESOPs
    b["p7_no_dilution"] = t7; score += t7

    # --- OPERATING EFFICIENCY (2 pts) ---
    # 8. Gross margin improved
    gm_cur  = _safe_div(gp,      rev)
    gm_prev = _safe_div(gp_prev, rev_prev)
    t8 = 1 if (gm_cur is not None and gm_prev is not None and gm_cur > gm_prev) else 0
    b["p8_gross_margin_improved"] = t8; score += t8

    # 9. Asset turnover improved (Rev / Avg TA)
    at_cur  = _safe_div(rev,      ta)
    at_prev = _safe_div(rev_prev, ta_prev)
    t9 = 1 if (at_cur is not None and at_prev is not None and at_cur > at_prev) else 0
    b["p9_asset_turnover_improved"] = t9; score += t9

    b["total"] = score
    b["max"] = 9
    b["interpretation"] = (
        "STRONG" if score >= 7 else
        "MODERATE" if score >= 4 else
        "WEAK"
    )
    return score, b


# ---------------------------------------------------------------------------
# Beneish M-Score
# ---------------------------------------------------------------------------

def beneish_m_score(f: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """
    Compute Beneish M-Score. Returns (score, breakdown_dict).

    Interpretation:
        M > -1.78 => LIKELY MANIPULATOR (red flag)
        M <= -1.78 => Non-manipulator

    Formula (8-variable, Beneish 1999):
        M = -4.84 + 0.92*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
            + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI

    Any missing variable => variable defaults to 1.0 (neutral) and flagged in breakdown.
    """
    b: Dict[str, Any] = {"missing_vars": []}

    # Line items (current = _t, prior = _tm1)
    rev_t   = _f(f, "revenue", "sales", "total_revenue")
    rev_tm1 = _f(f, "revenue_prev", "sales_prev")
    ar_t    = _f(f, "receivables", "accounts_receivable", "trade_receivables")
    ar_tm1  = _f(f, "receivables_prev", "accounts_receivable_prev")
    gp_t    = _f(f, "gross_profit", "gp")
    gp_tm1  = _f(f, "gross_profit_prev")
    ta_t    = _f(f, "total_assets", "total_assets_cur")
    ta_tm1  = _f(f, "total_assets_prev")
    ppe_t   = _f(f, "ppe", "net_ppe", "fixed_assets")
    ppe_tm1 = _f(f, "ppe_prev", "fixed_assets_prev")
    ca_t    = _f(f, "current_assets", "ca")
    ca_tm1  = _f(f, "current_assets_prev")
    sec_t   = _f(f, "securities", "investments")   # non-current investments
    sec_tm1 = _f(f, "securities_prev", "investments_prev")
    dep_t   = _f(f, "depreciation")
    dep_tm1 = _f(f, "depreciation_prev")
    sga_t   = _f(f, "sga", "selling_general_admin", "other_expenses")
    sga_tm1 = _f(f, "sga_prev", "other_expenses_prev")
    ni_t    = _f(f, "net_income", "pat", "profit_after_tax")
    cfo_t   = _f(f, "operating_cf", "cfo")
    ltd_t   = _f(f, "long_term_debt", "ltd")
    ltd_tm1 = _f(f, "long_term_debt_prev", "ltd_prev")
    cl_t    = _f(f, "current_liab", "cl")
    cl_tm1  = _f(f, "current_liab_prev")

    def ratio(n, d, name):
        r = _safe_div(n, d)
        if r is None:
            b["missing_vars"].append(name)
            return 1.0
        return r

    # DSRI = (AR_t/Sales_t) / (AR_tm1/Sales_tm1)
    dsri = ratio(_safe_div(ar_t, rev_t), _safe_div(ar_tm1, rev_tm1), "DSRI")

    # GMI = (GM_tm1) / (GM_t) where GM = GP/Sales
    gm_t   = _safe_div(gp_t,   rev_t)
    gm_tm1 = _safe_div(gp_tm1, rev_tm1)
    gmi = ratio(gm_tm1, gm_t, "GMI")

    # AQI = (1 - (CA+PPE+Sec)/TA)_t / same_tm1
    def _aqi_comp(ca, ppe, sec, ta):
        if ta is None or ta == 0: return None
        num = (ca or 0) + (ppe or 0) + (sec or 0)
        return 1.0 - (num / ta)
    aqi_t   = _aqi_comp(ca_t,   ppe_t,   sec_t,   ta_t)
    aqi_tm1 = _aqi_comp(ca_tm1, ppe_tm1, sec_tm1, ta_tm1)
    aqi = ratio(aqi_t, aqi_tm1, "AQI")

    # SGI = Sales_t / Sales_tm1
    sgi = ratio(rev_t, rev_tm1, "SGI")

    # DEPI = (Dep_tm1 / (Dep_tm1 + PPE_tm1)) / (Dep_t / (Dep_t + PPE_t))
    depi_num = _safe_div(dep_tm1, (dep_tm1 or 0) + (ppe_tm1 or 0)) if (dep_tm1 is not None or ppe_tm1 is not None) else None
    depi_den = _safe_div(dep_t,   (dep_t   or 0) + (ppe_t   or 0)) if (dep_t   is not None or ppe_t   is not None) else None
    depi = ratio(depi_num, depi_den, "DEPI")

    # SGAI = (SGA_t/Sales_t) / (SGA_tm1/Sales_tm1)
    sgai = ratio(_safe_div(sga_t, rev_t), _safe_div(sga_tm1, rev_tm1), "SGAI")

    # TATA = (NI - CFO) / TA
    if ni_t is None or cfo_t is None or ta_t in (None, 0):
        b["missing_vars"].append("TATA")
        tata = 0.0
    else:
        tata = (ni_t - cfo_t) / ta_t

    # LVGI = ((LTD+CL)/TA)_t / same_tm1
    def _lev(ltd, cl, ta):
        if ta is None or ta == 0: return None
        return ((ltd or 0) + (cl or 0)) / ta
    lvgi = ratio(_lev(ltd_t, cl_t, ta_t), _lev(ltd_tm1, cl_tm1, ta_tm1), "LVGI")

    m = (
        -4.84
        + 0.920 * dsri
        + 0.528 * gmi
        + 0.404 * aqi
        + 0.892 * sgi
        + 0.115 * depi
        - 0.172 * sgai
        + 4.679 * tata
        - 0.327 * lvgi
    )

    b.update({
        "DSRI": round(dsri, 4), "GMI": round(gmi, 4), "AQI": round(aqi, 4),
        "SGI": round(sgi, 4), "DEPI": round(depi, 4), "SGAI": round(sgai, 4),
        "TATA": round(tata, 4), "LVGI": round(lvgi, 4),
        "M_score": round(m, 4),
        "flag": "MANIPULATOR" if m > -1.78 else "CLEAN",
        "threshold": -1.78,
    })
    return m, b


# ---------------------------------------------------------------------------
# Composite convenience API
# ---------------------------------------------------------------------------

def quality_composite(fundamentals: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a compact dict combining both scores.

    Usage in main.py:
        q = quality_composite(fundamentals_for_symbol)
        if q["hard_reject"]:
            # gate blocks the trade
            reject_reasons.append(q["reject_reason"])
        else:
            score += q["factor_bonus"]  # +0..+1.0 into 10-factor score
    """
    try:
        f_score, f_break = piotroski_f_score(fundamentals or {})
        m_score, m_break = beneish_m_score(fundamentals or {})
    except Exception as e:
        log.warning("quality_composite failed: %s", e)
        return {
            "ok": False, "hard_reject": False, "factor_bonus": 0.0,
            "reject_reason": "", "piotroski": None, "beneish": None, "error": str(e),
        }

    # Hard gate: block only on strong evidence
    hard_reject = False
    reject_reason = ""
    if f_score <= 2:
        hard_reject = True
        reject_reason = f"Piotroski F-Score too low ({f_score}/9 = weak fundamentals)"
    elif m_score > -1.78 and len(m_break.get("missing_vars", [])) <= 2:
        # only block on Beneish if we have enough data to trust it
        hard_reject = True
        reject_reason = f"Beneish M-Score {m_score:.2f} > -1.78 (earnings manipulation risk)"

    # Soft bonus: 0..+1.0 into the 10-factor score
    # Piotroski contributes 0..0.7, Beneish contributes 0..0.3
    piotroski_bonus = max(0.0, min(1.0, (f_score - 3) / 6.0)) * 0.7
    beneish_bonus = 0.3 if m_score <= -2.22 else (0.15 if m_score <= -1.78 else 0.0)
    factor_bonus = round(piotroski_bonus + beneish_bonus, 3)

    return {
        "ok": True,
        "hard_reject": hard_reject,
        "reject_reason": reject_reason,
        "factor_bonus": factor_bonus,
        "piotroski": {"score": f_score, "breakdown": f_break},
        "beneish":   {"score": m_score, "breakdown": m_break},
    }


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # Sample: strong company (should score high F, low M)
    sample_strong = {
        "net_income": 500,      "net_income_prev": 400,
        "operating_cf": 600,
        "total_assets": 5000,   "total_assets_prev": 4800,
        "long_term_debt": 800,  "long_term_debt_prev": 900,
        "current_assets": 2000, "current_liab": 1000,
        "current_assets_prev": 1800, "current_liab_prev": 1000,
        "shares_outstanding": 100, "shares_outstanding_prev": 100,
        "gross_profit": 1500,   "gross_profit_prev": 1300,
        "revenue": 4000,        "revenue_prev": 3700,
        "receivables": 400,     "receivables_prev": 380,
        "ppe": 2500,            "ppe_prev": 2400,
        "securities": 300,      "securities_prev": 280,
        "depreciation": 200,    "depreciation_prev": 195,
        "sga": 600,             "sga_prev": 580,
    }
    q = quality_composite(sample_strong)
    print("Strong sample:", q["piotroski"]["score"], "F |",
          q["beneish"]["score"], "M | bonus=", q["factor_bonus"],
          "| hard_reject=", q["hard_reject"])

    # Sample: weak/red-flag company
    sample_weak = dict(sample_strong)
    sample_weak.update({
        "net_income": -100, "operating_cf": -200,
        "receivables": 800,  # huge jump in AR = classic manipulation flag
        "gross_profit": 800, # margin collapse
    })
    q2 = quality_composite(sample_weak)
    print("Weak sample:  ", q2["piotroski"]["score"], "F |",
          q2["beneish"]["score"], "M | bonus=", q2["factor_bonus"],
          "| hard_reject=", q2["hard_reject"], "|", q2["reject_reason"])
