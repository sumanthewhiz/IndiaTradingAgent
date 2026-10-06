from __future__ import annotations

import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, time, timedelta
from pathlib import Path

from .broker import NoRedirect
from .core import SafetyError, timestamp
from .credentials import atomic_json, read_atomic_json
from .market_data import FeedError, feed_timestamp

GLOBAL_RELEASES = {
    "fed-monetary": "https://www.federalreserve.gov/feeds/press_monetary.xml",
    "ecb-releases": "https://www.ecb.europa.eu/rss/press.html",
}
MATERIAL_POLICY = re.compile(
    r"FOMC statement|monetary policy decision|key.{0,20}interest rate|"
    r"emergency|liquidity facility|financial stability|economic projections", re.I,
)
UNAVAILABLE_CONTEXT = [
    "No licensed real-time GIFT NIFTY or US/Asian futures feed is configured.",
    "No licensed real-time crude, FX, US yields or overseas equity price feed is configured.",
    "Official central-bank releases are not a complete economic/geopolitical calendar.",
]


def fetch_global_release(url: str) -> bytes:
    if url not in GLOBAL_RELEASES.values():
        raise SafetyError("Global context source is not allowlisted.")
    request = urllib.request.Request(url, headers={"User-Agent": "IndiaTradingAgent/0.2"})
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=8) as response:
            data = response.read(512001)
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        raise FeedError(f"http_{status}", f"Official global source returned HTTP {status}.") from None
    except (urllib.error.URLError, OSError):
        raise FeedError("network_error", "Official global source could not be reached.") from None
    if len(data) > 512000:
        raise FeedError("oversized_response", "Official global context exceeded its response limit.")
    return data


def global_context(directory: Path, at: datetime, fetch=fetch_global_release) -> dict:
    cache = directory / f"global-{at.date().isoformat()}.json"
    if cache.exists():
        saved = read_atomic_json(cache)
        if 0 <= (at - timestamp(saved["checked_at"])).total_seconds() <= 1800:
            return saved
    headlines, sources = [], {}
    for source, url in GLOBAL_RELEASES.items():
        try:
            data = fetch(url)
            if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
                raise FeedError("unsafe_xml", "Global context contains unsupported XML entities.")
            root = ET.fromstring(data)
            items = root.findall(".//item")
            if root.tag != "rss" or not items or len(items) > 1000:
                raise FeedError("invalid_rss", "Global source did not provide a valid bounded RSS feed.")
            latest = None
            for item in items:
                title = " ".join((item.findtext("title") or "").split())
                published = feed_timestamp(item.findtext("pubDate") or "")
                if (published - at).total_seconds() > 60:
                    raise FeedError("future_dated", "Global source publication time is in the future.")
                latest = max(latest, published) if latest else published
                if title and 0 <= (at - published).total_seconds() <= 72 * 3600:
                    headlines.append({
                        "source": source, "at": published.isoformat(),
                        "headline": title[:500], "material_policy": bool(MATERIAL_POLICY.search(title)),
                    })
            sources[source] = {
                "healthy": True, "checked_at": at.isoformat(),
                "latest_publication": latest.isoformat(), "url": url,
                "meaning": "Official release source reached; an old last release is not a live quote.",
                "error": "",
            }
        except (SafetyError, OSError, ValueError, TypeError, ET.ParseError) as error:
            sources[source] = {
                "healthy": False, "checked_at": at.isoformat(), "url": url,
                "error": str(error) if isinstance(error, SafetyError) else type(error).__name__,
            }
    headlines.sort(key=lambda x: x["at"], reverse=True)
    recent_policy = [x for x in headlines if x["material_policy"]
                     and 0 <= (at - timestamp(x["at"])).total_seconds() <= 18 * 3600]
    result = {
        "checked_at": at.isoformat(), "sources": sources, "headlines": headlines[:30],
        "coverage": "limited official global policy releases, not a universal global-market feed",
        "gaps": UNAVAILABLE_CONTEXT,
        "opening_blackout": {
            "start": at.replace(hour=9, minute=15, second=0, microsecond=0).isoformat(),
            "end": at.replace(hour=9, minute=45, second=0, microsecond=0).isoformat(),
            "symbols": ["*"],
        } if recent_policy and at.time() < time(9, 45) else None,
        "policy_risk_reason": "Recent major global monetary-policy release; skip the opening reaction window."
        if recent_policy else "",
    }
    atomic_json(cache, result)
    return result


def premarket_candidates(quotes: dict, symbols: list[str], cash: int) -> list[dict]:
    """Previous close is used for research only, never as an executable price."""
    maximum = min(cash, 2_500_000) // 4
    from .core import paise
    candidates = []
    for symbol in symbols:
        quote = quotes.get("NSE:" + symbol)
        if quote is None:
            continue
        previous = paise(quote["ohlc"]["close"])
        if not 0 < previous <= maximum:
            continue
        candidates.append({
            "symbol": symbol, "price_paise": previous, "previous_close_paise": previous,
            "prior_quote_turnover_paise": previous * max(0, int(quote.get("volume", 0))),
        })
    return sorted(candidates, key=lambda x: (-x["prior_quote_turnover_paise"], x["symbol"]))


def rank_premarket(candidates: list[dict], histories: dict, benchmark: dict,
                   sectors: dict, excluded: set[str], catalysts: dict, at: datetime) -> list[dict]:
    ranked = []
    for item in candidates:
        symbol = item["symbol"]
        history = histories.get(symbol)
        if (symbol in excluded or not history or not 50 <= history["atr_bps"] <= 400
                or history["last_session"] != benchmark["last_session"]
                or history["average_turnover_paise"] < 1_000_000_000
                or abs(item["previous_close_paise"] / history["last_close_paise"] - 1) > 0.01):
            continue
        rel5 = history["return_5d_bps"] - benchmark["return_5d_bps"]
        rel20 = history["return_20d_bps"] - benchmark["return_20d_bps"]
        score = max(-1, min(1, rel5 / 500)) + 0.5 * max(-1, min(1, rel20 / 1000))
        aged = [x for x in catalysts.get(symbol, [])
                if 1800 <= (at - timestamp(x["at"])).total_seconds() <= 86400]
        score += 0.2 if aged else 0
        ranked.append({
            **item, "score": round(score, 4), "sector": sectors.get(symbol, "Unknown"),
            "spread_bps": None, "quote_kind": "prior close / research only",
            "historical_as_of": history["last_session"], "atr_bps": history["atr_bps"],
            "relative_5d_bps": round(rel5, 2), "relative_20d_bps": round(rel20, 2),
            "issuer_catalysts": [x["headline"] for x in aged[:3]],
            "reason": (
                f"Pre-market research: relative 5d {rel5:+.0f} bps / 20d {rel20:+.0f} bps, "
                f"ATR {history['atr_bps']:.0f} bps. Current spread/volume must qualify after 09:15."
            ),
        })
    return sorted(ranked, key=lambda x: (-x["score"], x["symbol"]))
