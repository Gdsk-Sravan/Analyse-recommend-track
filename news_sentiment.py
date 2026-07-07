"""
news_sentiment.py
==================
News sentiment analysis for NSE stocks using RSS headline aggregation + Groq LLM.

Rationale:
    Piotroski/Beneish/technicals capture "steady state" quality but miss the
    day-of catalysts — fraud disclosure, downgrade, promoter arrest, tender-
    offer, block-deal buyer identity. These move prices +/- 5-15% in a single
    session and are almost always news-driven.

Approach:
    1. Fetch RSS from Moneycontrol, Economic Times, Business Standard, LiveMint
       (feedparser — already in requirements).
    2. Filter to items mentioning symbol or company keywords (last 7 days).
    3. Send batch of headlines to Groq (existing llama-3.1-8b-instant used in
       main.py) with a structured JSON schema prompt.
    4. Aggregate to a single {sentiment, materiality, confidence} triple.

Fallback: if Groq unavailable, use lightweight keyword scoring
    (positive/negative wordlists — small but non-zero signal).

Public API:
    news_sentiment_signal(symbol: str, company_name: str = None,
                          lookback_days: int = 7) -> dict

Feature flag: `ENABLE_NEWS_SENTIMENT=true|false` (default true).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Google-News RSS is a reliable aggregator with per-query filtering
# (no API key needed). We build the URL per symbol.
_GOOGLE_NEWS_TEMPLATE = ("https://news.google.com/rss/search"
                         "?q={q}+when:{days}d&hl=en-IN&gl=IN&ceid=IN:en")
# Broad market feeds as fallback (already scraped by many Indian traders)
_MARKET_FEEDS = [
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    "https://www.livemint.com/rss/markets",
]

_CACHE_TTL = 3600  # 1 hour
_CACHE: Dict[str, tuple] = {}

# Groq wire — mirrors main.py's usage
_GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
_GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant")


# ---------------------------------------------------------------------------
# Fetch headlines
# ---------------------------------------------------------------------------

def fetch_headlines(symbol: str, company_name: Optional[str] = None,
                    lookback_days: int = 7, max_items: int = 30) -> List[Dict[str, Any]]:
    """Return list of {title, link, published, source} for last `lookback_days`."""
    now = time.time()
    cache_key = f"{symbol}|{lookback_days}"
    if cache_key in _CACHE:
        ts, data = _CACHE[cache_key]
        if now - ts < _CACHE_TTL:
            return data

    try:
        import feedparser
    except Exception:
        log.warning("news_sentiment: feedparser not installed")
        return []

    query = company_name or symbol
    query_str = f'"{query}"'
    url = _GOOGLE_NEWS_TEMPLATE.format(q=quote_plus(query_str), days=lookback_days)
    items: List[Dict[str, Any]] = []
    try:
        feed = feedparser.parse(url)
        for entry in (feed.entries or [])[:max_items]:
            items.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "source": entry.get("source", {}).get("title", "google-news")
                          if isinstance(entry.get("source"), dict)
                          else "google-news",
            })
    except Exception as e:
        log.warning("news_sentiment: google-news fetch failed for %s: %s", symbol, e)

    # Fallback: pull broad market feeds and grep for symbol
    if len(items) < 3:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        patt = re.compile(rf"\b({re.escape(symbol)}|{re.escape(query)})\b", re.IGNORECASE)
        for feed_url in _MARKET_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:80]:
                    title = entry.get("title", "")
                    if not patt.search(title):
                        continue
                    pub = entry.get("published_parsed")
                    if pub:
                        pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                        if pub_dt < cutoff:
                            continue
                    items.append({
                        "title": title,
                        "link": entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "source": feed_url,
                    })
            except Exception:
                continue

    # De-dup on title
    seen = set(); dedup: List[Dict[str, Any]] = []
    for it in items:
        t = it["title"].strip().lower()
        if t and t not in seen:
            seen.add(t); dedup.append(it)
    _CACHE[cache_key] = (now, dedup)
    return dedup


# ---------------------------------------------------------------------------
# Sentiment scoring
# ---------------------------------------------------------------------------

_POS_KWS = {
    "beat", "beats", "record", "surge", "surges", "rally", "gains", "upgrade",
    "raised", "raises", "expansion", "acquires", "wins", "order", "orders",
    "profit", "outperform", "buy", "target", "bullish", "dividend", "bonus",
    "buyback", "signs", "approval", "cleared", "launches", "expansion",
}
_NEG_KWS = {
    "miss", "misses", "plunge", "plunges", "fall", "falls", "downgrade",
    "cut", "cuts", "probe", "raid", "fraud", "scam", "sebi", "penalty",
    "resignation", "resigns", "sell", "bearish", "warning", "loss", "losses",
    "default", "insolvency", "nclt", "arrest", "scandal", "irregularities",
    "restatement", "material", "block deal seller", "promoter pledge",
}
_MATERIAL_KWS = {
    "sebi", "cbi", "ed ", "raid", "fraud", "insider trading", "manipulat",
    "restatement", "resignation", "auditor", "qualified opinion", "nclt",
    "default", "downgrade", "credit rating", "moody", "crisil", "care ratings",
    "block deal", "bulk deal", "promoter pledge", "promoter sold",
}


def _keyword_score(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not items:
        return {"sentiment": "NEUTRAL", "score": 0.0, "materiality": "LOW",
                "confidence": 0.0, "engine": "keyword", "n_items": 0}
    pos = 0; neg = 0; mat = 0
    triggers: List[str] = []
    for it in items:
        t = it["title"].lower()
        for kw in _POS_KWS:
            if kw in t: pos += 1
        for kw in _NEG_KWS:
            if kw in t: neg += 1
        for kw in _MATERIAL_KWS:
            if kw in t:
                mat += 1
                triggers.append(kw)
    total = pos + neg
    if total == 0:
        return {"sentiment": "NEUTRAL", "score": 0.0, "materiality": "LOW",
                "confidence": 0.2, "engine": "keyword", "n_items": len(items)}
    score = (pos - neg) / total
    sent = ("POSITIVE" if score > 0.3 else
            "NEGATIVE" if score < -0.3 else "NEUTRAL")
    materiality = ("HIGH" if mat >= 2 else "MEDIUM" if mat == 1 else "LOW")
    conf = min(0.7, 0.3 + 0.05 * total)   # keyword score is capped at 0.7 conf
    return {
        "sentiment": sent, "score": round(score, 3),
        "materiality": materiality, "confidence": round(conf, 2),
        "engine": "keyword", "n_items": len(items),
        "triggers": triggers[:10],
    }


def _groq_score(symbol: str, items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Ask Groq to classify. Returns None on any failure (caller falls back)."""
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key or not items:
        return None
    try:
        import requests
    except Exception:
        return None

    headlines_text = "\n".join(
        f"[{i+1}] {it['title']}" for i, it in enumerate(items[:20])
    )
    prompt = f"""You are a professional Indian-equities analyst. Classify the news
for stock {symbol} based on these {min(len(items), 20)} recent headlines.

HEADLINES:
{headlines_text}

Return ONLY a JSON object with keys:
  "sentiment": "POSITIVE" | "NEGATIVE" | "NEUTRAL"
  "materiality": "HIGH" | "MEDIUM" | "LOW"  (HIGH = SEBI/fraud/downgrade/arrest/restatement/promoter-pledge/big block-deal seller)
  "confidence": 0.0 to 1.0
  "reasoning": one short sentence
Do NOT include any text outside the JSON object."""

    payload = {
        "model": _GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 300,
        "response_format": {"type": "json_object"},
    }
    try:
        r = requests.post(_GROQ_ENDPOINT,
                          headers={"Authorization": f"Bearer {api_key}",
                                   "Content-Type": "application/json"},
                          data=json.dumps(payload), timeout=20)
        if r.status_code != 200:
            log.info("news_sentiment: Groq HTTP %s: %s", r.status_code, r.text[:200])
            return None
        content = r.json()["choices"][0]["message"]["content"]
        obj = json.loads(content)
        # Sanitize
        s = str(obj.get("sentiment", "NEUTRAL")).upper()
        m = str(obj.get("materiality", "LOW")).upper()
        c = float(obj.get("confidence", 0.5) or 0.5)
        return {
            "sentiment": s if s in ("POSITIVE", "NEGATIVE", "NEUTRAL") else "NEUTRAL",
            "materiality": m if m in ("HIGH", "MEDIUM", "LOW") else "LOW",
            "confidence": max(0.0, min(1.0, c)),
            "reasoning": str(obj.get("reasoning", ""))[:250],
            "engine": "groq:" + _GROQ_MODEL,
            "n_items": len(items),
        }
    except Exception as e:
        log.info("news_sentiment: Groq call failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Unified signal
# ---------------------------------------------------------------------------

def news_sentiment_signal(
    symbol: str,
    company_name: Optional[str] = None,
    lookback_days: int = 7,
) -> Dict[str, Any]:
    """
    Return {ok, sentiment, materiality, confidence, factor_bonus,
            hard_reject, reject_reason, headlines_count, engine}.

    factor_bonus (soft): -0.5 .. +0.5 into 10-factor score
    hard_reject:  True only if materiality=HIGH AND sentiment=NEGATIVE
                  AND confidence >= 0.7 AND ENABLE_NEWS_HARD_GATE=true
    """
    items = fetch_headlines(symbol, company_name, lookback_days)
    if not items:
        return {"ok": False, "reason": "no_headlines", "factor_bonus": 0.0,
                "hard_reject": False, "reject_reason": "",
                "headlines_count": 0, "sentiment": "UNKNOWN",
                "materiality": "LOW", "confidence": 0.0}

    scored = _groq_score(symbol, items) or _keyword_score(items)

    # Soft factor bonus
    sent_sign = {"POSITIVE": +1, "NEGATIVE": -1, "NEUTRAL": 0}.get(scored["sentiment"], 0)
    mat_weight = {"HIGH": 0.50, "MEDIUM": 0.25, "LOW": 0.10}.get(scored["materiality"], 0.10)
    factor_bonus = round(sent_sign * mat_weight * scored["confidence"], 3)

    hard_reject = False
    reject_reason = ""
    if (scored["sentiment"] == "NEGATIVE"
        and scored["materiality"] == "HIGH"
        and scored["confidence"] >= 0.7
        and os.environ.get("ENABLE_NEWS_HARD_GATE", "false").lower() == "true"):
        hard_reject = True
        reject_reason = (f"Material negative news "
                         f"(conf={scored['confidence']:.2f}, "
                         f"engine={scored['engine']})")

    return {
        "ok": True,
        "symbol": symbol,
        "sentiment": scored["sentiment"],
        "materiality": scored["materiality"],
        "confidence": scored["confidence"],
        "engine": scored.get("engine"),
        "reasoning": scored.get("reasoning"),
        "headlines_count": len(items),
        "top_headline": items[0]["title"] if items else "",
        "factor_bonus": factor_bonus,
        "hard_reject": hard_reject,
        "reject_reason": reject_reason,
    }


if __name__ == "__main__":  # pragma: no cover
    import argparse
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("--name", default=None)
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()
    r = news_sentiment_signal(args.symbol, args.name, args.days)
    print(json.dumps(r, indent=2))
