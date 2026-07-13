"""
attribution.py
===============
Post-trade factor & gate attribution for the swing trading system.

Answers the questions your rating flagged as MISSING:
    - "Which gate/factor added alpha last 30/60/90 days?"
    - "If I remove Gate-X, what P&L do I lose/gain?"
    - "Which of my 10 score factors is actually predictive?"

Inputs:
    - trade_tracker.json OR shadow_master.xlsx (the daily log)
    - Each trade row is expected to carry: symbol, entry_date, exit_date,
      entry_px, exit_px, side, factors (dict of {factor_name: value}),
      gates_passed (list[str]), regime, kelly_frac, ...

Outputs (added to Excel + optional Telegram):
    Sheet "Attribution_Factor"     — per-factor IC (info coefficient) & lift
    Sheet "Attribution_Gate"       — per-gate P&L delta if removed
    Sheet "Attribution_Regime"     — per-regime win-rate & expectancy

All computations are read-only and degrade to empty sheets if input is missing.

Public API:
    build_attribution_report(tracker_path: str, out_xlsx: str,
                             lookback_days: int = 90) -> dict
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # attribution silently degrades


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class Trade:
    symbol: str
    entry_date: datetime
    exit_date: Optional[datetime]
    entry_px: float
    exit_px: Optional[float]
    side: str = "LONG"
    pnl_pct: float = 0.0                # realised P&L% (post-cost)
    factors: Dict[str, float] = field(default_factory=dict)  # {"momentum": 0.72, ...}
    gates_passed: List[str] = field(default_factory=list)    # ["liquidity","trend",...]
    regime: str = "UNKNOWN"
    kelly_frac: float = 0.0
    status: str = "CLOSED"              # OPEN | CLOSED | STOPPED


def _to_float(x, default=None):
    try:
        v = float(x)
        return v if not (math.isnan(v) or math.isinf(v)) else default
    except (TypeError, ValueError):
        return default


def _to_date(x) -> Optional[datetime]:
    if x is None or x == "" or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, datetime):
        return x
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(x)[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(str(x))
    except Exception:
        return None


def load_trades_from_json(path: str) -> List[Trade]:
    """Load trades from trade_tracker.json (list-of-dicts or dict-of-symbol)."""
    if not os.path.exists(path):
        log.warning("attribution: %s not found", path)
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception as e:
        log.warning("attribution: cannot read %s: %s", path, e)
        return []

    rows = raw if isinstance(raw, list) else list(raw.values())
    trades: List[Trade] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        ent_dt = _to_date(r.get("entry_date") or r.get("entry_time"))
        if ent_dt is None:
            continue
        exit_dt = _to_date(r.get("exit_date") or r.get("closed_at"))
        entry_px = _to_float(r.get("entry_px") or r.get("entry_price"))
        exit_px  = _to_float(r.get("exit_px") or r.get("exit_price") or r.get("last_px"))
        if entry_px is None or entry_px <= 0:
            continue
        # Realised P&L% preferring stored field, else compute
        pnl = _to_float(r.get("pnl_pct"))
        if pnl is None and exit_px is not None:
            side = str(r.get("side", "LONG")).upper()
            pnl = (exit_px / entry_px - 1.0) * (1.0 if side == "LONG" else -1.0) * 100.0

        trades.append(Trade(
            symbol=str(r.get("symbol", "")),
            entry_date=ent_dt,
            exit_date=exit_dt,
            entry_px=entry_px,
            exit_px=exit_px,
            side=str(r.get("side", "LONG")).upper(),
            pnl_pct=_to_float(pnl, 0.0),
            factors=dict(r.get("factors") or {}),
            gates_passed=list(r.get("gates_passed") or []),
            regime=str(r.get("regime", "UNKNOWN")),
            kelly_frac=_to_float(r.get("kelly_frac"), 0.0),
            status=str(r.get("status", "CLOSED")).upper(),
        ))
    return trades


def load_trades_from_excel(path: str, sheet: str = "TradeLog") -> List[Trade]:
    if pd is None or not os.path.exists(path):
        return []
    try:
        df = pd.read_excel(path, sheet_name=sheet)
    except Exception as e:
        log.info("attribution: no sheet %s in %s (%s)", sheet, path, e)
        return []
    if df.empty:
        return []

    trades: List[Trade] = []
    for _, row in df.iterrows():
        d = row.to_dict()
        ent_dt = _to_date(d.get("entry_date") or d.get("Entry Date"))
        if ent_dt is None:
            continue
        entry_px = _to_float(d.get("entry_px") or d.get("Entry Price"))
        exit_px  = _to_float(d.get("exit_px")  or d.get("Exit Price"))
        pnl = _to_float(d.get("pnl_pct") or d.get("P&L %"))
        if pnl is None and (entry_px and exit_px):
            side = str(d.get("side", "LONG")).upper()
            pnl = (exit_px / entry_px - 1.0) * (1.0 if side == "LONG" else -1.0) * 100.0

        # gates_passed may be stored comma-separated
        gp_raw = d.get("gates_passed") or d.get("Gates") or ""
        gates_passed = [g.strip() for g in str(gp_raw).split(",") if g.strip()]

        # factors: JSON-serialised string or "k1:v1;k2:v2"
        fac_raw = d.get("factors") or d.get("Factors") or ""
        factors: Dict[str, float] = {}
        if fac_raw:
            try:
                if isinstance(fac_raw, str) and fac_raw.strip().startswith("{"):
                    factors = {k: _to_float(v, 0.0) for k, v in json.loads(fac_raw).items()}
                else:
                    for part in str(fac_raw).split(";"):
                        if ":" in part:
                            k, v = part.split(":", 1)
                            factors[k.strip()] = _to_float(v, 0.0)
            except Exception:
                pass

        trades.append(Trade(
            symbol=str(d.get("symbol") or d.get("Symbol", "")),
            entry_date=ent_dt,
            exit_date=_to_date(d.get("exit_date") or d.get("Exit Date")),
            entry_px=entry_px or 0.0,
            exit_px=exit_px,
            side=str(d.get("side", "LONG")).upper(),
            pnl_pct=_to_float(pnl, 0.0),
            factors=factors,
            gates_passed=gates_passed,
            regime=str(d.get("regime") or d.get("Regime", "UNKNOWN")),
            kelly_frac=_to_float(d.get("kelly_frac"), 0.0),
            status=str(d.get("status", "CLOSED")).upper(),
        ))
    return trades


# ---------------------------------------------------------------------------
# Attribution primitives
# ---------------------------------------------------------------------------

def _closed_only(trades: List[Trade], lookback_days: int) -> List[Trade]:
    cutoff = datetime.now() - timedelta(days=lookback_days)
    return [t for t in trades
            if t.status == "CLOSED" and t.exit_date is not None
            and t.exit_date >= cutoff]


def factor_ic(trades: List[Trade]) -> Dict[str, Dict[str, float]]:
    """
    For each factor, compute:
        - spearman-ish IC (rank correlation with pnl_pct)
        - top-quintile vs bottom-quintile P&L spread (lift)
        - hit-rate above/below median
    Returns { factor_name: {ic, lift, hits_top, hits_bot, n} }.
    """
    if not trades:
        return {}

    # gather universe of factor names
    fac_names = sorted({k for t in trades for k in t.factors.keys()})
    out: Dict[str, Dict[str, float]] = {}

    for fname in fac_names:
        pairs = [(t.factors.get(fname), t.pnl_pct)
                 for t in trades if fname in t.factors]
        pairs = [(v, p) for v, p in pairs if v is not None]
        n = len(pairs)
        if n < 10:
            out[fname] = {"n": n, "ic": 0.0, "lift": 0.0,
                          "hit_top": 0.0, "hit_bot": 0.0, "insufficient": 1.0}
            continue

        # rank correlation (spearman via numpy)
        try:
            import numpy as np
            vals = np.array([v for v, _ in pairs], dtype=float)
            pnls = np.array([p for _, p in pairs], dtype=float)
            rv = _rank(vals); rp = _rank(pnls)
            if rv.std() > 0 and rp.std() > 0:
                ic = float(((rv - rv.mean()) * (rp - rp.mean())).mean() /
                           (rv.std() * rp.std()))
            else:
                ic = 0.0
        except Exception:
            ic = 0.0

        # quintile lift
        pairs.sort(key=lambda x: x[0])
        q = max(1, n // 5)
        bot = pairs[:q]; top = pairs[-q:]
        mean_bot = sum(p for _, p in bot) / len(bot)
        mean_top = sum(p for _, p in top) / len(top)
        lift = mean_top - mean_bot

        hit_top = sum(1 for _, p in top if p > 0) / len(top)
        hit_bot = sum(1 for _, p in bot if p > 0) / len(bot)

        out[fname] = {
            "n": float(n),
            "ic": round(ic, 4),
            "lift_pct": round(lift, 3),
            "top_hit_rate": round(hit_top, 3),
            "bot_hit_rate": round(hit_bot, 3),
        }
    return out


def _rank(arr):
    import numpy as np
    order = arr.argsort()
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(len(arr), dtype=float)
    return ranks


def gate_leave_one_out(trades: List[Trade]) -> Dict[str, Dict[str, float]]:
    """
    For each gate: what happens to the *sub-population that passed it*?
    Compare: (avg P&L when gate=passed) vs (overall avg P&L).
    A positive delta means the gate is selecting winners.

    NOTE: True "remove-gate" simulation would need re-scoring every past
    candidate. This is a lightweight proxy using stored gates_passed only.
    """
    if not trades:
        return {}

    overall_pnl = sum(t.pnl_pct for t in trades) / max(1, len(trades))
    overall_wr  = sum(1 for t in trades if t.pnl_pct > 0) / max(1, len(trades))

    gates = sorted({g for t in trades for g in t.gates_passed})
    out: Dict[str, Dict[str, float]] = {}
    for g in gates:
        sub = [t for t in trades if g in t.gates_passed]
        n = len(sub)
        if n < 5:
            out[g] = {"n": float(n), "avg_pnl_pct": 0.0, "delta_vs_all": 0.0,
                      "win_rate": 0.0, "insufficient": 1.0}
            continue
        avg = sum(t.pnl_pct for t in sub) / n
        wr = sum(1 for t in sub if t.pnl_pct > 0) / n
        out[g] = {
            "n": float(n),
            "avg_pnl_pct": round(avg, 3),
            "delta_vs_all": round(avg - overall_pnl, 3),
            "win_rate": round(wr, 3),
            "wr_delta": round(wr - overall_wr, 3),
        }
    return out


def regime_breakdown(trades: List[Trade]) -> Dict[str, Dict[str, float]]:
    if not trades:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    regimes = sorted({t.regime for t in trades})
    for r in regimes:
        sub = [t for t in trades if t.regime == r]
        if not sub:
            continue
        wins = [t for t in sub if t.pnl_pct > 0]
        losses = [t for t in sub if t.pnl_pct <= 0]
        avg_win  = (sum(t.pnl_pct for t in wins) / len(wins)) if wins else 0.0
        avg_loss = (sum(t.pnl_pct for t in losses) / len(losses)) if losses else 0.0
        wr = len(wins) / len(sub) if sub else 0.0
        expectancy = wr * avg_win + (1 - wr) * avg_loss
        out[r] = {
            "n": float(len(sub)),
            "win_rate": round(wr, 3),
            "avg_win_pct": round(avg_win, 3),
            "avg_loss_pct": round(avg_loss, 3),
            "expectancy_pct": round(expectancy, 3),
            "total_pnl_pct": round(sum(t.pnl_pct for t in sub), 3),
        }
    return out


# ---------------------------------------------------------------------------
# Excel writer
# ---------------------------------------------------------------------------

def _write_sheet(writer, name: str, records: Dict[str, Dict[str, float]], key_col: str):
    if pd is None or not records:
        return
    df = pd.DataFrame([{key_col: k, **v} for k, v in records.items()])
    # sort by dominant column
    for col in ("delta_vs_all", "lift_pct", "expectancy_pct", "ic"):
        if col in df.columns:
            df = df.sort_values(col, ascending=False)
            break
    try:
        df.to_excel(writer, sheet_name=name[:31], index=False)
    except Exception as e:
        log.warning("attribution: cannot write sheet %s: %s", name, e)


def build_attribution_report(
    tracker_json: str = "trade_tracker.json",
    tracker_xlsx: str = "tracking_workbook.xlsx",
    out_xlsx: str = "tracking_workbook.xlsx",
    lookback_days: int = 90,
) -> Dict[str, Any]:
    """
    Build attribution report. Reads from JSON + Excel, writes 3 new sheets
    into out_xlsx (default: same file — appended in place via openpyxl mode='a').

    Returns a summary dict.
    """
    trades: List[Trade] = []
    trades += load_trades_from_json(tracker_json)
    trades += load_trades_from_excel(tracker_xlsx)

    # de-dup on (symbol, entry_date)
    seen = set(); unique: List[Trade] = []
    for t in trades:
        key = (t.symbol, t.entry_date)
        if key in seen:
            continue
        seen.add(key)
        unique.append(t)

    closed = _closed_only(unique, lookback_days)
    log.info("attribution: %d total trades, %d closed in last %dd",
             len(unique), len(closed), lookback_days)

    summary = {
        "total_trades": len(unique),
        "closed_in_window": len(closed),
        "lookback_days": lookback_days,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if not closed:
        return summary

    factor = factor_ic(closed)
    gate   = gate_leave_one_out(closed)
    regime = regime_breakdown(closed)

    summary.update({
        "n_factors": len(factor),
        "n_gates": len(gate),
        "n_regimes": len(regime),
        "top_factor_by_lift": _top(factor, "lift_pct"),
        "top_gate_by_delta":  _top(gate,   "delta_vs_all"),
        "best_regime":        _top(regime, "expectancy_pct"),
    })

    if pd is None:
        log.warning("attribution: pandas unavailable — skipping Excel write")
        return summary

    # Excel append mode
    try:
        if os.path.exists(out_xlsx):
            writer = pd.ExcelWriter(out_xlsx, engine="openpyxl", mode="a",
                                    if_sheet_exists="replace")
        else:
            writer = pd.ExcelWriter(out_xlsx, engine="openpyxl")
        with writer as w:
            _write_sheet(w, "Attribution_Factor", factor, "factor")
            _write_sheet(w, "Attribution_Gate",   gate,   "gate")
            _write_sheet(w, "Attribution_Regime", regime, "regime")
    except Exception as e:
        log.warning("attribution: Excel write failed: %s", e)

    return summary


def _top(d: Dict[str, Dict[str, float]], key: str) -> Optional[str]:
    if not d:
        return None
    best = max(d.items(), key=lambda kv: kv[1].get(key, -1e9))
    return f"{best[0]} ({best[1].get(key, 0):+.3f})"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--tracker-json", default="trade_tracker.json")
    ap.add_argument("--tracker-xlsx", default="shadow_master.xlsx")
    ap.add_argument("--out", default="shadow_master.xlsx")
    ap.add_argument("--lookback", type=int, default=90)
    args = ap.parse_args()

    s = build_attribution_report(args.tracker_json, args.tracker_xlsx,
                                 args.out, args.lookback)
    print(json.dumps(s, indent=2, default=str))
