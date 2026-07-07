"""
backtest_stats.py
==================
Advanced backtest statistics: Deflated Sharpe Ratio (López de Prado, 2014) and
Monte Carlo permutation tests. Guards against the biggest failure mode of
walk-forward backtests — accepting a strategy that is really just lucky.

References:
    - Bailey & López de Prado (2014), "The Deflated Sharpe Ratio: Correcting for
      Selection Bias, Backtest Overfitting, and Non-Normality"
    - Aronson (2007), "Evidence-Based Technical Analysis", ch. on Monte Carlo
    - Vince (1990), "Portfolio Management Formulas"

The module is dependency-light (numpy + stdlib). If numpy is unavailable,
functions return neutral values (no crash).

Public API:
    sharpe_ratio(returns, periods_per_year=252) -> float
    deflated_sharpe(returns, n_trials, periods_per_year=252) -> dict
    monte_carlo_permutation(trade_returns, n_iters=1000, seed=42) -> dict
    bootstrap_win_rate(trade_wins, n_iters=2000, seed=42) -> dict
    compute_all(trade_pnls_pct: list, n_trials_tested: int = 1) -> dict
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

try:
    import numpy as np
    _HAS_NP = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    _HAS_NP = False


# ---------------------------------------------------------------------------
# Basic ratios
# ---------------------------------------------------------------------------

def sharpe_ratio(returns: Sequence[float], periods_per_year: int = 252) -> float:
    """Annualised Sharpe. Returns should be per-period (e.g. per-trade or daily)."""
    if not _HAS_NP or len(returns) < 2:
        return 0.0
    r = np.asarray(returns, dtype=float)
    mu = r.mean()
    sd = r.std(ddof=1)
    if sd == 0 or math.isnan(sd):
        return 0.0
    return float(mu / sd * math.sqrt(periods_per_year))


def skew_kurt(returns: Sequence[float]) -> tuple:
    """Return (skew, excess_kurtosis). Fisher's definition; kurtosis of Normal = 0."""
    if not _HAS_NP or len(returns) < 4:
        return 0.0, 0.0
    r = np.asarray(returns, dtype=float)
    mu = r.mean(); sd = r.std(ddof=1)
    if sd == 0:
        return 0.0, 0.0
    z = (r - mu) / sd
    n = len(r)
    g1 = float((z ** 3).mean())              # sample skew
    g2 = float((z ** 4).mean() - 3.0)        # excess kurtosis
    return g1, g2


# ---------------------------------------------------------------------------
# Deflated Sharpe Ratio (Bailey & López de Prado 2014)
# ---------------------------------------------------------------------------

def _expected_max_sr(n_trials: int) -> float:
    """
    Expected maximum of N iid standard-normal Sharpe ratios.
    Approximation from Bailey-López de Prado:
        E[max] ≈ (1 - γ) * Φ⁻¹(1 - 1/N) + γ * Φ⁻¹(1 - 1/(N*e))
    where γ ≈ 0.5772 (Euler-Mascheroni).
    """
    if not _HAS_NP:
        return 0.0
    if n_trials <= 1:
        return 0.0
    from math import e, log as ln
    try:
        # Approximate inverse-normal via numpy
        from numpy.random import default_rng
        rng = default_rng(0)
        # Use erfinv-based ppf via math.erf inverse — approximate with scipy if
        # available, else use a rational approximation.
        try:
            from scipy.stats import norm  # type: ignore
            ppf = norm.ppf
        except Exception:
            ppf = _norm_ppf_approx
        gamma = 0.5772156649
        term1 = ppf(1 - 1.0 / n_trials)
        term2 = ppf(1 - 1.0 / (n_trials * e))
        return (1 - gamma) * term1 + gamma * term2
    except Exception as ex:
        log.debug("_expected_max_sr fallback: %s", ex)
        return 0.0


def _norm_ppf_approx(p: float) -> float:
    """Beasley-Springer-Moro inverse normal CDF approximation (accurate to 4e-4)."""
    if p <= 0 or p >= 1:
        return 0.0
    # Simple rational approx (Peter Acklam)
    a = [-3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
          4.374664141464968e+00,  2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00]
    pl = 0.02425; ph = 1 - pl
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p <= ph:
        q = p - 0.5; r = q*q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
               (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
            ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)


def deflated_sharpe(
    returns: Sequence[float],
    n_trials: int = 1,
    periods_per_year: int = 252,
    benchmark_sr: float = 0.0,
) -> Dict[str, Any]:
    """
    Deflated Sharpe Ratio (probability that observed SR > benchmark_sr after
    correcting for skew, kurtosis, and number of strategies tested).

    Args:
        returns:          per-period returns (per-trade OK — set periods_per_year=n_trades/yr)
        n_trials:         how many strategy variants you effectively tested
                          (parameter combos, thresholds, etc). Higher => more deflation.
        benchmark_sr:     what SR are we testing "better than"? Default 0.

    Returns dict with:
        sr, sr_annualised, expected_max_sr, DSR (probability),
        p_value, verdict
    """
    if not _HAS_NP or len(returns) < 10:
        return {"sr": 0.0, "DSR": 0.0, "verdict": "INSUFFICIENT_DATA",
                "n_returns": len(returns)}

    r = np.asarray(returns, dtype=float)
    T = len(r)
    sr_per_period = sharpe_ratio(r, periods_per_year=1)   # unannualised
    sr_ann = sharpe_ratio(r, periods_per_year=periods_per_year)

    skew, kurt = skew_kurt(r)
    exp_max = _expected_max_sr(max(1, n_trials))

    # Variance of estimated SR under non-normal returns
    # Var(SR_hat) = (1 - skew*SR + (kurt/4)*SR^2) / (T - 1)
    var_sr = (1.0 - skew * sr_per_period + (kurt / 4.0) * sr_per_period ** 2) / max(1, T - 1)
    if var_sr <= 0:
        return {"sr": sr_ann, "DSR": 0.0, "verdict": "UNSTABLE_VARIANCE"}

    # DSR = Φ( (SR - E[max]) / sqrt(Var(SR)) )
    num = sr_per_period - exp_max - benchmark_sr / math.sqrt(periods_per_year)
    z = num / math.sqrt(var_sr)
    # normal CDF
    dsr = 0.5 * (1.0 + math.erf(z / math.sqrt(2)))

    verdict = ("STRONG" if dsr >= 0.95 else
               "PROBABLE" if dsr >= 0.80 else
               "MARGINAL" if dsr >= 0.50 else
               "LUCKY")
    return {
        "sr": round(sr_per_period, 4),
        "sr_annualised": round(sr_ann, 4),
        "expected_max_sr_per_period": round(exp_max, 4),
        "n_trials_tested": int(n_trials),
        "skew": round(skew, 4),
        "excess_kurtosis": round(kurt, 4),
        "n_returns": int(T),
        "z_stat": round(z, 4),
        "DSR": round(dsr, 4),
        "p_value": round(1 - dsr, 4),
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# Monte Carlo permutation test
# ---------------------------------------------------------------------------

def monte_carlo_permutation(
    trade_returns: Sequence[float],
    n_iters: int = 1000,
    seed: int = 42,
) -> Dict[str, Any]:
    """
    Randomly shuffle sign of trade returns (or reorder) and count how often
    the shuffled series produces cumulative P&L >= observed. This is an
    Aronson-style permutation test for "is the sequence of wins/losses
    genuinely predictive, or a lucky ordering?".

    Null hypothesis: trade direction is random.
    p_value = fraction of shuffles that beat or match observed.
    """
    if not _HAS_NP or len(trade_returns) < 20:
        return {"n_iters": 0, "p_value": 1.0, "verdict": "INSUFFICIENT_DATA",
                "observed_cum_pnl": float(sum(trade_returns) if trade_returns else 0.0)}

    r = np.asarray(trade_returns, dtype=float)
    observed = float(r.sum())
    observed_max_dd = _max_drawdown(np.cumsum(r))

    rng = np.random.default_rng(seed)
    beat = 0
    dd_beat = 0
    for _ in range(n_iters):
        shuffled = rng.permutation(r)
        cum = np.cumsum(shuffled)
        if cum[-1] >= observed:
            beat += 1
        dd_shuffled = _max_drawdown(cum)
        if abs(dd_shuffled) <= abs(observed_max_dd):
            dd_beat += 1

    p_value = beat / n_iters
    verdict = ("STRONG"   if p_value <= 0.01 else
               "PROBABLE" if p_value <= 0.05 else
               "MARGINAL" if p_value <= 0.20 else
               "RANDOM")
    return {
        "n_iters": n_iters,
        "observed_cum_pnl": round(observed, 4),
        "observed_max_drawdown": round(observed_max_dd, 4),
        "p_value_pnl": round(p_value, 4),
        "p_value_drawdown_favourable": round(1 - dd_beat / n_iters, 4),
        "verdict": verdict,
    }


def _max_drawdown(cum: "np.ndarray") -> float:
    if not _HAS_NP or len(cum) == 0:
        return 0.0
    peak = np.maximum.accumulate(cum)
    dd = cum - peak
    return float(dd.min())


# ---------------------------------------------------------------------------
# Bootstrap CI on win-rate
# ---------------------------------------------------------------------------

def bootstrap_win_rate(
    trade_wins: Sequence[int],
    n_iters: int = 2000,
    seed: int = 42,
) -> Dict[str, Any]:
    """
    Given a 0/1 win indicator per trade, produce 95% CI on win-rate via
    bootstrap resampling. Answers: "how tight is my 60% win-rate estimate?"
    """
    if not _HAS_NP or len(trade_wins) < 10:
        return {"win_rate": 0.0, "ci_low": 0.0, "ci_high": 0.0, "n_trades": len(trade_wins)}
    w = np.asarray(trade_wins, dtype=int)
    n = len(w)
    rng = np.random.default_rng(seed)
    means = np.empty(n_iters)
    for i in range(n_iters):
        idx = rng.integers(0, n, size=n)
        means[i] = w[idx].mean()
    return {
        "win_rate": round(float(w.mean()), 4),
        "ci_low_2p5": round(float(np.percentile(means, 2.5)), 4),
        "ci_high_97p5": round(float(np.percentile(means, 97.5)), 4),
        "n_trades": int(n),
        "n_iters": int(n_iters),
    }


# ---------------------------------------------------------------------------
# One-shot convenience
# ---------------------------------------------------------------------------

def compute_all(
    trade_pnls_pct: List[float],
    n_trials_tested: int = 1,
    trades_per_year: int = 50,
) -> Dict[str, Any]:
    """
    Given a list of per-trade P&L% (e.g. [+1.5, -0.8, +2.1, ...]) run the
    full statistical panel. Returns a compact dict safe to JSON-serialise.

    Use `n_trials_tested` = total number of parameter combinations you swept
    (e.g. if you tested 5 confidence thresholds × 3 stop widths = 15).
    """
    if not trade_pnls_pct:
        return {"available": False, "reason": "no trades"}
    # Convert % to decimal returns for SR math
    r_dec = [x / 100.0 for x in trade_pnls_pct]

    dsr = deflated_sharpe(r_dec, n_trials=n_trials_tested,
                          periods_per_year=trades_per_year)
    mc  = monte_carlo_permutation(r_dec, n_iters=1000)
    wins = [1 if x > 0 else 0 for x in trade_pnls_pct]
    boot = bootstrap_win_rate(wins, n_iters=2000)

    verdict_map = {"STRONG": 4, "PROBABLE": 3, "MARGINAL": 2, "LUCKY": 1, "RANDOM": 0}
    overall_score = min(
        verdict_map.get(dsr.get("verdict", ""), 0),
        verdict_map.get(mc.get("verdict",  ""), 0),
    )
    overall_verdict = {4: "STRONG_EDGE", 3: "PROBABLE_EDGE",
                       2: "MARGINAL_EDGE", 1: "LUCKY", 0: "NO_EDGE"}.get(overall_score, "NO_EDGE")

    return {
        "available": True,
        "deflated_sharpe": dsr,
        "monte_carlo": mc,
        "bootstrap_win_rate": boot,
        "overall_verdict": overall_verdict,
        "n_trades": len(trade_pnls_pct),
    }


# ---------------------------------------------------------------------------
# CLI self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    import random as _r
    _r.seed(0)
    # Simulate 150 trades with slight edge (win 55% at +1.5%, lose 45% at -1%)
    trades = [+1.5 if _r.random() < 0.55 else -1.0 for _ in range(150)]
    res = compute_all(trades, n_trials_tested=20, trades_per_year=50)
    import json
    print(json.dumps(res, indent=2))
