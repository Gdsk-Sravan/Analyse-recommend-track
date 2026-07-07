"""
insider_feed.py
================
NSE insider-trading & bulk/block-deal disclosures signal.

Data sources:
    - nselib.capital_market.insider_trading_data()   → SAST/PIT filings
    - nselib.capital_market.bulk_deal_data()         → single trade > 0.5% of equity
    - nselib.capital_market.block_deal_data()        → negotiated large trades

Signal logic:
    - Promoter/director/KMP acquired (buy) recently → BULLISH factor bonus
    - Promoter/director/KMP disposed (sell) recently → BEARISH factor bonus
    - Large *sell* by promoter in last 30 days at aggregate > ₹1 cr → optional hard gate
    - Cluster of block-deal *buyers* on a rising trend → bullish confirmation

Cache: 6 hours (this data updates end-of-day only).

Public API:
    insider_signal(symbol: str, lookback_days: int = 30) -> dict
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_CACHE_TTL = 6 * 3600
_CACHE: Dict[str, tuple] = {}


def _parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(s).strip()[:11], fmt)
        except ValueError:
            continue
    return None


def _to_float(x, default=0.0):
    if x is None:
        return default
    try:
        s = str(x).replace(",", "").replace("₹", "").strip()
        if s in ("", "-", "nan"):
            return default
        return float(s)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _fetch_insider(symbol: str, lookback_days: int) -> List[Dict[str, Any]]:
    """Return list of insider filings for last `lookback_days` days."""
    from_dt = datetime.now() - timedelta(days=lookback_days)
    to_dt = datetime.now()
    from_str = from_dt.strftime("%d-%m-%Y")
    to_str   = to_dt.strftime("%d-%m-%Y")

    try:
        from nselib import capital_market as _cm
    except Exception:
        log.warning("insider_feed: nselib.capital_market unavailable")
        return []

    df = None
    for attempt in range(2):
        try:
            df = _cm.insider_trading_data(symbol=symbol, from_date=from_str, to_date=to_str)
            if df is not None:
                break
        except TypeError:
            # Some versions accept only the date-range (all-symbol) call
            try:
                df = _cm.insider_trading_data(from_date=from_str, to_date=to_str)
                if df is not None:
                    break
            except Exception as e:
                log.debug("insider_feed: insider_trading_data attempt %d: %s", attempt, e)
        except Exception as e:
            log.debug("insider_feed: insider_trading_data attempt %d: %s", attempt, e)
            time.sleep(1.0 * (attempt + 1))
    if df is None:
        return []

    try:
        import pandas as pd
        if not isinstance(df, pd.DataFrame) or df.empty:
            return []
        cols = {c.lower(): c for c in df.columns}

        def col(*cands):
            for c in cands:
                for k in cols:
                    if c in k:
                        return cols[k]
            return None

        c_sym    = col("symbol")
        c_name   = col("name of the acquirer", "name")
        c_cat    = col("category")
        c_type   = col("acquisition", "mode", "type")
        c_qty    = col("shares", "no. of secur", "quantity")
        c_val    = col("value", "total value")
        c_date   = col("acquisition/disposal date", "date of allotment", "date")

        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            sym = str(r[c_sym]).strip().upper() if c_sym else ""
            if sym and sym != symbol.upper():
                continue
            side_raw = str(r[c_type]).strip().upper() if c_type else ""
            side = ("BUY" if any(k in side_raw for k in ("BUY", "ACQUIS")) else
                    "SELL" if any(k in side_raw for k in ("SELL", "DISPOS")) else
                    "UNKNOWN")
            rows.append({
                "symbol": sym,
                "name": str(r[c_name]).strip() if c_name else "",
                "category": str(r[c_cat]).strip() if c_cat else "",
                "side": side,
                "qty": _to_float(r[c_qty]) if c_qty else 0.0,
                "value_inr": _to_float(r[c_val]) if c_val else 0.0,
                "date": _parse_date(str(r[c_date])) if c_date else None,
            })
        return rows
    except Exception as e:
        log.warning("insider_feed: parse failed for %s: %s", symbol, e)
        return []


def _fetch_bulk_block(symbol: str, lookback_days: int) -> List[Dict[str, Any]]:
    """Combined bulk + block deals for the symbol."""
    from_dt = datetime.now() - timedelta(days=lookback_days)
    from_str = from_dt.strftime("%d-%m-%Y")
    to_str   = datetime.now().strftime("%d-%m-%Y")

    try:
        from nselib import capital_market as _cm
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for fn_name, deal_kind in (("bulk_deal_data", "BULK"),
                                ("block_deal_data", "BLOCK")):
        fn = getattr(_cm, fn_name, None)
        if fn is None:
            continue
        df = None
        try:
            df = fn(from_date=from_str, to_date=to_str)
        except Exception as e:
            log.debug("insider_feed: %s failed: %s", fn_name, e)
            continue
        try:
            import pandas as pd
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            cols = {c.lower(): c for c in df.columns}
            def col(*cands):
                for c in cands:
                    for k in cols:
                        if c in k:
                            return cols[k]
                return None
            c_sym   = col("symbol")
            c_name  = col("client name", "party")
            c_type  = col("buy/sell", "type")
            c_qty   = col("quantity", "shares")
            c_price = col("price", "trade price")
            c_date  = col("date")
            for _, r in df.iterrows():
                sym = str(r[c_sym]).strip().upper() if c_sym else ""
                if sym != symbol.upper():
                    continue
                side_raw = str(r[c_type]).strip().upper() if c_type else ""
                side = ("BUY" if "BUY" in side_raw else
                        "SELL" if "SELL" in side_raw else "UNKNOWN")
                qty = _to_float(r[c_qty]) if c_qty else 0.0
                px  = _to_float(r[c_price]) if c_price else 0.0
                out.append({
                    "symbol": sym,
                    "deal_kind": deal_kind,
                    "party": str(r[c_name]).strip() if c_name else "",
                    "side": side,
                    "qty": qty,
                    "price": px,
                    "value_inr": qty * px,
                    "date": _parse_date(str(r[c_date])) if c_date else None,
                })
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Signal
# ---------------------------------------------------------------------------

_PROMOTER_TERMS = ("PROMOTER", "PROMOTORS", "PROMOTER GROUP",
                   "MANAGING DIRECTOR", "DIRECTOR", "KMP", "KEY MANAGERIAL")


def _is_promoter(row: Dict[str, Any]) -> bool:
    cat = row.get("category", "").upper()
    return any(t in cat for t in _PROMOTER_TERMS)


def insider_signal(symbol: str, lookback_days: int = 30) -> Dict[str, Any]:
    """
    Return compact signal used by main.py's scoring pipeline.

    Result shape:
        {ok, symbol, insider_buy_value_inr, insider_sell_value_inr,
         promoter_sell_inr_last_30d, bulk_block_buy_count, bulk_block_sell_count,
         factor_bonus, hard_reject, reject_reason}
    """
    now = time.time()
    key = f"{symbol}|{lookback_days}"
    if key in _CACHE:
        ts, data = _CACHE[key]
        if now - ts < _CACHE_TTL:
            return data

    ins = _fetch_insider(symbol, lookback_days)
    bd  = _fetch_bulk_block(symbol, lookback_days)

    buy_val  = sum(r["value_inr"] for r in ins if r["side"] == "BUY")
    sell_val = sum(r["value_inr"] for r in ins if r["side"] == "SELL")
    promoter_sell_val = sum(r["value_inr"] for r in ins
                            if r["side"] == "SELL" and _is_promoter(r))
    promoter_buy_val  = sum(r["value_inr"] for r in ins
                            if r["side"] == "BUY"  and _is_promoter(r))

    bd_buys  = sum(1 for r in bd if r["side"] == "BUY")
    bd_sells = sum(1 for r in bd if r["side"] == "SELL")

    # ── Scoring (soft) ──
    bonus = 0.0
    # Promoter buys are a strong bullish signal
    if promoter_buy_val > 1e7:      # > 1 cr
        bonus += 0.30
    elif promoter_buy_val > 1e6:    # > 10 lakh
        bonus += 0.15
    # Promoter sells modestly bearish (many are ESOP / tax-driven — soft)
    if promoter_sell_val > 5e7:     # > 5 cr in 30d = material
        bonus -= 0.30
    elif promoter_sell_val > 1e7:   # > 1 cr
        bonus -= 0.15
    # Bulk/block deals net
    if bd_buys > bd_sells + 1:
        bonus += 0.10
    elif bd_sells > bd_buys + 1:
        bonus -= 0.10

    # Hard gate: opt-in
    hard_reject = False
    reject_reason = ""
    if (os.environ.get("ENABLE_INSIDER_HARD_GATE", "false").lower() == "true"
        and promoter_sell_val > 1e7 and promoter_buy_val < promoter_sell_val * 0.1):
        hard_reject = True
        reject_reason = (f"Promoter sold ₹{promoter_sell_val/1e7:.2f} Cr "
                         f"in last {lookback_days}d")

    out = {
        "ok": True,
        "symbol": symbol,
        "lookback_days": lookback_days,
        "insider_buy_value_inr": round(buy_val, 2),
        "insider_sell_value_inr": round(sell_val, 2),
        "promoter_buy_value_inr": round(promoter_buy_val, 2),
        "promoter_sell_value_inr": round(promoter_sell_val, 2),
        "bulk_block_buy_count": bd_buys,
        "bulk_block_sell_count": bd_sells,
        "n_filings": len(ins),
        "n_bulk_block": len(bd),
        "factor_bonus": round(bonus, 3),
        "hard_reject": hard_reject,
        "reject_reason": reject_reason,
    }
    _CACHE[key] = (now, out)
    return out


if __name__ == "__main__":  # pragma: no cover
    import argparse
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("--days", type=int, default=30)
    args = ap.parse_args()
    print(json.dumps(insider_signal(args.symbol, args.days), indent=2, default=str))
