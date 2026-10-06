from __future__ import annotations

import csv
import http.client
import html
import io
import json
import math
import re
import time as clock
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from xml.parsers import expat
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from statistics import mean

from .broker import KiteHTTP, NoRedirect
from .core import Config, IST, Instrument, SafetyError, bps, now_ist, paise, timestamp
from .credentials import atomic_json, read_atomic_json
from .events import HIGH_IMPACT, high_impact_news
from .market import Bar, HistoryNotReady, Tape

CONSTITUENTS_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv"
NEWS_SOURCES = {
    "nse-announcements": "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
    "rbi-releases": "https://www.rbi.org.in/pressreleases_rss.xml",
}
SEED_SYMBOLS = (
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "LT", "ITC", "SBIN",
    "AXISBANK", "BHARTIARTL", "KOTAKBANK", "HINDUNILVR", "SUNPHARMA", "MARUTI",
    "M&M", "TITAN", "NTPC", "POWERGRID", "TATASTEEL", "JSWSTEEL", "ULTRACEMCO",
    "ASIANPAINT", "BAJFINANCE", "BAJAJFINSV", "HCLTECH", "TECHM", "WIPRO",
    "DRREDDY", "CIPLA", "GRASIM", "EICHERMOT", "ONGC", "COALINDIA", "BPCL",
    "APOLLOHOSP", "ADANIPORTS", "NESTLEIND", "BRITANNIA", "SBILIFE", "HDFCLIFE",
)

MAX_PUBLIC_BYTES = 2_000_000
MAX_FEED_ITEMS = 5000
WATCHLIST_VERSION = "daily-relative-strength-v1"
TRANSIENT_NEWS_CODES = {
    "incomplete_snapshot", "incomplete_response", "network_error", "timeout",
    "http_500", "http_502", "http_503", "http_504",
}


class FeedError(SafetyError):
    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 http_status: int | None = None):
        super().__init__(message)
        self.code, self.retryable, self.http_status = code, retryable, http_status


@dataclass(frozen=True)
class PublicResponse:
    status: int
    body: bytes
    etag: str = ""
    last_modified: str = ""


def public_response(url: str, headers: dict[str, str] | None = None) -> PublicResponse:
    if url not in {CONSTITUENTS_URL, *NEWS_SOURCES.values()}:
        raise FeedError("url_not_allowed", "Public source URL is not allowlisted.")
    request = urllib.request.Request(url, headers={
        "User-Agent": "IndiaTradingAgent/0.2",
        **(headers or {}),
    })
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=8) as response:
            data = response.read(MAX_PUBLIC_BYTES + 1)
            if len(data) > MAX_PUBLIC_BYTES:
                raise FeedError("oversized_response", "Publisher response exceeds the two-megabyte limit.")
            return PublicResponse(response.status, data, response.headers.get("ETag", ""),
                                  response.headers.get("Last-Modified", ""))
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        if status == 304:
            return PublicResponse(304, b"")
        if status == 429:
            raise FeedError("http_429", "Publisher rate-limited the request; wait for the next scheduled poll.",
                            http_status=status) from None
        raise FeedError(f"http_{status}", f"Publisher returned HTTP {status}.",
                        retryable=status in {500, 502, 503, 504}, http_status=status) from None
    except urllib.error.URLError as error:
        timed_out = isinstance(error.reason, TimeoutError)
        raise FeedError("timeout" if timed_out else "network_error",
                        "Publisher request timed out." if timed_out else "Publisher connection failed.",
                        retryable=True) from None
    except http.client.IncompleteRead:
        raise FeedError("incomplete_response", "Publisher response ended before the document was complete.",
                        retryable=True) from None
    except TimeoutError:
        raise FeedError("timeout", "Publisher request timed out.", retryable=True) from None
    except OSError:
        raise FeedError("network_error", "Publisher connection failed.", retryable=True) from None


def public_bytes(url: str) -> bytes:
    result = public_response(url)
    if result.status != 200:
        raise FeedError("unexpected_status", "Publisher did not return a complete document.")
    return result.body


def company_key(name: str) -> str:
    name = re.sub(r"\b(limited|ltd|private|pvt)\b\.?", "", name, flags=re.IGNORECASE)
    return re.sub(r"[^A-Z0-9]", "", name.upper())


def market_timestamp(raw: str | datetime) -> datetime:
    value = raw if isinstance(raw, datetime) else datetime.fromisoformat(raw)
    return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)


def feed_timestamp(value: str) -> datetime:
    try:
        return timestamp(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (ValueError, TypeError):
            # NSE's official RSS uses "21-Sep-2026 10:50:03" in exchange-local IST.
            parsed = datetime.strptime(value, "%d-%b-%Y %H:%M:%S")
        return parsed.replace(tzinfo=IST) if parsed.tzinfo is None else parsed.astimezone(IST)


def constituents(master: dict[str, dict], cache: Path) -> tuple[list[str], dict[str, str], dict[str, str], str]:
    rows = None
    origin = "official current NIFTY 200 constituents"
    try:
        data = public_bytes(CONSTITUENTS_URL).decode("utf-8-sig")
        parsed = list(csv.DictReader(io.StringIO(data)))
        if not 180 <= len(parsed) <= 220 or not all(
            x.get("Symbol") and x.get("Company Name") and x.get("Series") == "EQ" for x in parsed
        ) or len({x["Symbol"] for x in parsed}) != len(parsed):
            raise SafetyError("Constituent CSV format/size is invalid.")
        rows = parsed
        atomic_json(cache, {"source_url": CONSTITUENTS_URL, "fetched_at": now_ist().isoformat(), "rows": rows})
    except (SafetyError, ValueError, UnicodeError) as error:
        unavailable = str(error) if isinstance(error, SafetyError) else type(error).__name__
        if cache.exists():
            saved = read_atomic_json(cache)
            if (saved.get("source_url") == CONSTITUENTS_URL
                    and 0 <= (now_ist() - timestamp(saved["fetched_at"])).total_seconds() <= 7 * 86400):
                rows = saved["rows"]
                origin = f"cached official NIFTY 200 (at most seven days old); refresh failed: {unavailable}"
        if rows is None:
            rows = [{"Symbol": symbol, "Company Name": master.get(symbol, {}).get("name", symbol)}
                    for symbol in SEED_SYMBOLS]
            origin = f"bundled liquid-cash candidates, NOT current index membership; refresh failed: {unavailable}"
    symbols, aliases, sectors = [], {}, {}
    for row in rows:
        symbol = row["Symbol"]
        instrument = master.get(symbol)
        if (instrument is None or instrument.get("segment") != "NSE"
                or instrument.get("instrument_type") != "EQ" or int(instrument["lot_size"]) != 1
                or instrument.get("expiry")):
            continue
        symbols.append(symbol)
        sectors[symbol] = row.get("Industry") or "Unknown"
        aliases[company_key(row["Company Name"])] = symbol
        aliases[company_key(symbol)] = symbol
        if instrument.get("name"):
            aliases[company_key(instrument["name"])] = symbol
    return symbols, aliases, sectors, origin


def screen_quotes(quotes: dict, symbols: list[str], cash: int, at: datetime) -> list[dict]:
    candidates = []
    position_cap = min(cash, 2_500_000) // 4
    for symbol in symbols:
        raw = quotes.get("NSE:" + symbol)
        if not raw:
            continue
        try:
            last = paise(raw["last_price"])
            bid, ask = paise(raw["depth"]["buy"][0]["price"]), paise(raw["depth"]["sell"][0]["price"])
            low, high = paise(raw["lower_circuit_limit"]), paise(raw["upper_circuit_limit"])
            quote_at = market_timestamp(raw["timestamp"])
            volume = int(raw["volume"])
            previous_close, open_price = paise(raw["ohlc"]["close"]), paise(raw["ohlc"]["open"])
            turnover = last * volume
            if (quote_at.date() != at.date() or not -2 <= (at - quote_at).total_seconds() <= 30
                    or not 0 < low < bid <= ask < high or last > position_cap
                    or ask - bid > bps(bid, 8) or volume <= 0 or turnover < 1_000_000_000
                    or previous_close <= 0 or abs(open_price / previous_close - 1) > 0.03):
                continue
            spread_bps = (ask - bid) / bid * 10000
            change = last / previous_close - 1
            score = math.log10(turnover) + max(-1, min(1, change * 100)) - spread_bps / 10
            candidates.append({
                "symbol": symbol, "price_paise": last, "turnover_paise": turnover,
                "change_percent": round(change * 100, 3), "spread_bps": round(spread_bps, 3),
                "change_bps": round(change * 10000, 3),
                "from_open_bps": round((last / open_price - 1) * 10000, 3),
                "previous_close_paise": previous_close, "quote_at": quote_at.isoformat(),
                "score": round(score, 4),
                "reason": "Fresh cash quote; liquid turnover; narrow spread; affordable; gap below 3%",
            })
        except (KeyError, ValueError, TypeError, IndexError):
            raise SafetyError("Malformed broker quote while screening " + symbol + ".") from None
    return sorted(candidates, key=lambda x: (-x["score"], x["symbol"]))


def daily_history(http: KiteHTTP, token: int, at: datetime) -> dict:
    response = http.request("GET", f"/instruments/historical/{token}/day", query=[
        ("from", (at - timedelta(days=50)).strftime("%Y-%m-%d 00:00:00")),
        ("to", (at - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")),
    ])
    candles = response["candles"]
    if len(candles) < 21:
        raise SafetyError("Fewer than 21 completed daily candles are available.")
    ranges, prior, turnover, closes, dates = [], None, [], [], []
    for row in candles:
        when, open_, high, low, close, volume = row[:6]
        date_ = market_timestamp(when).date()
        if date_ >= at.date() or (dates and date_ <= dates[-1]):
            raise SafetyError("Daily history includes an unfinished/future candle.")
        open_, high, low, close = (paise(x) for x in (open_, high, low, close))
        if not 0 < low <= min(open_, close) <= max(open_, close) <= high or int(volume) < 0:
            raise SafetyError("Daily candle has invalid price/volume.")
        ranges.append(max(high - low, abs(high - prior), abs(low - prior)) if prior else high - low)
        turnover.append(close * int(volume))
        prior = close
        closes.append(close)
        dates.append(date_)
    if (at.date() - dates[-1]).days > 7:
        raise SafetyError("Most recent completed daily candle is more than seven days old.")
    atr_bps = sum(ranges[-14:]) / 14 / prior * 10000
    return {
        "atr_bps": round(atr_bps, 2),
        "average_turnover_paise": int(sum(turnover[-20:]) / 20),
        "sessions": len(candles), "last_close_paise": prior,
        "last_session": dates[-1].isoformat(),
        "return_5d_bps": round((closes[-1] / closes[-6] - 1) * 10000, 3),
        "return_20d_bps": round((closes[-1] / closes[-21] - 1) * 10000, 3),
        "mean_20d_close_paise": round(mean(closes[-20:])),
    }


def cached_daily_history(http: KiteHTTP, token: int, at: datetime, directory: Path) -> dict:
    path = directory / f"{at.date().isoformat()}-{token}.json"
    if path.exists():
        cached = read_atomic_json(path)
        if (cached.get("as_of") == at.date().isoformat() and cached.get("token") == token
                and cached.get("version") == WATCHLIST_VERSION):
            return cached["metrics"]
    metrics = daily_history(http, token, at)
    atomic_json(path, {"as_of": at.date().isoformat(), "token": token, "version": WATCHLIST_VERSION,
                       "metrics": metrics})
    return metrics


def rank_opportunities(candidates: list[dict], histories: dict[str, dict], benchmark: dict,
                       benchmark_change_bps: float, sectors: dict[str, str],
                       catalysts: dict[str, list[dict]], excluded: set[str], at: datetime) -> list[dict]:
    """Transparent research ranking, not a return forecast or an order trigger."""
    def clamp(value: float) -> float:
        return max(-1.0, min(1.0, value))

    ranked = []
    elapsed = max(20, min(375, at.hour * 60 + at.minute - 555)) / 375
    for item in candidates:
        symbol = item["symbol"]
        metrics = histories.get(symbol)
        if symbol in excluded or metrics is None:
            continue
        if (not 50 <= metrics["atr_bps"] <= 400
                or metrics["last_session"] != benchmark["last_session"]
                or abs(item["previous_close_paise"] / metrics["last_close_paise"] - 1) > 0.01
                or metrics["average_turnover_paise"] <= 0):
            continue
        relative_5d = metrics["return_5d_bps"] - benchmark["return_5d_bps"]
        relative_20d = metrics["return_20d_bps"] - benchmark["return_20d_bps"]
        relative_today = item["change_bps"] - benchmark_change_bps
        pace = item["turnover_paise"] / (metrics["average_turnover_paise"] * elapsed)
        catalyst_score = 0.0
        evidence = []
        for event in catalysts.get(symbol, []):
            age = (at - timestamp(event["at"])).total_seconds()
            # Never reward a headline within its event-reaction risk window.
            if 1800 <= age <= 86400:
                evidence.append(event["headline"])
                catalyst_score = max(catalyst_score, 0.35 * (1 - age / 86400))
        components = {
            "relative_today": 1.2 * clamp(relative_today / 100),
            "relative_5d": 0.7 * clamp(relative_5d / 500),
            "relative_20d": 0.4 * clamp(relative_20d / 1000),
            "intraday_direction": 0.6 * clamp(item["from_open_bps"] / 100),
            "turnover_pace": 0.6 * clamp(math.log2(max(0.125, pace))),
            "liquidity": 0.3 * clamp(math.log10(metrics["average_turnover_paise"] / 1e9)),
            "spread_cost": -item["spread_bps"] / 8,
            "aged_issuer_catalyst": catalyst_score,
        }
        ranked.append({
            **item, "sector": sectors.get(symbol, "Unknown"),
            "score": round(sum(components.values()), 4),
            "factors": {name: round(value, 4) for name, value in components.items()},
            "relative_today_bps": round(relative_today, 2),
            "relative_5d_bps": round(relative_5d, 2), "relative_20d_bps": round(relative_20d, 2),
            "turnover_pace_estimate": round(pace, 3), "atr_bps": metrics["atr_bps"],
            "historical_as_of": metrics["last_session"], "issuer_catalysts": evidence[:3],
            "reason": (
                f"Relative strength: today {relative_today:+.0f} bps, 5d {relative_5d:+.0f} bps;"
                f" turnover pace ~{pace:.2f}x (linear estimate), spread {item['spread_bps']:.1f} bps;"
                f" ATR {metrics['atr_bps']:.0f} bps. Ranked hypothesis, not expected profit."
            ),
        })
    return sorted(ranked, key=lambda x: (-x["score"], x["symbol"]))


def select_diverse(ranked: list[dict], limit: int = 5) -> list[dict]:
    selected, sector_counts = [], {}
    for item in ranked:
        sector = item["sector"]
        if sector_counts.get(sector, 0) >= 2:
            continue
        selected.append(item)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected) == limit:
            break
    return selected


def closed_intraday_bars(http: KiteHTTP, token: int, at: datetime) -> list[Bar]:
    bucket = at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0)
    opening = at.replace(hour=9, minute=15, second=0, microsecond=0)

    def read(earliest: datetime) -> list[Bar]:
        response = http.request("GET", f"/instruments/historical/{token}/5minute", query=[
            ("from", earliest.strftime("%Y-%m-%d %H:%M:%S")),
            ("to", bucket.strftime("%Y-%m-%d %H:%M:%S")),
        ])
        if not isinstance(response, dict) or not isinstance(response.get("candles"), list):
            raise SafetyError("Broker historical response has no valid candle array.")
        result = []
        for row in response["candles"]:
            if not isinstance(row, list) or len(row) not in {6, 7}:
                raise SafetyError("Broker historical candle has an invalid structure.")
            start = market_timestamp(row[0])
            if start.date() != at.date():
                raise SafetyError("Broker historical candle belongs to a different session.")
            if earliest <= start and start + timedelta(minutes=5) <= bucket:
                result.append(Bar(start, *(paise(x) for x in row[1:5]), int(row[5])))
        return result

    result = read(opening)
    try:
        Tape().seed(result, at)
    except HistoryNotReady:
        latest = result[-1].end if result else opening
        if bucket - latest > timedelta(minutes=15):
            raise
        # Retry only the missing tail once; never pad gaps or include an open candle.
        result += read(latest)
        Tape().seed(result, at)
    return result


class AutomaticNews:
    """Fixed official sources, issuer mapping and health; does not treat RSS as exhaustive news."""

    def __init__(self, config: Config, aliases: dict[str, str], fetch=None, *,
                 symbols: list[str] | None = None, transport=public_response):
        self.config, self.aliases, self.fetch = config, aliases, fetch
        self.symbols = set(config.market.symbols if symbols is None else symbols)
        self.transport = transport
        self.health: dict[str, dict] = {}
        self.excluded: set[str] = set()
        self.catalysts: dict[str, list[dict]] = {}
        self.documents: dict[str, PublicResponse] = {}
        self.poll_interval_seconds = config.news.rss_poll_seconds

    def _document(self, source: str, url: str, refresh: bool = False) -> tuple[PublicResponse, bool]:
        if self.fetch is not None:
            data = self.fetch(url)
            result = PublicResponse(200, data)
        else:
            headers = {"Cache-Control": "no-cache"} if refresh else {}
            previous = self.documents.get(source)
            if previous and not refresh:
                for header, value in (("If-None-Match", previous.etag),
                                      ("If-Modified-Since", previous.last_modified)):
                    if value and len(value) <= 1024 and not any(x in value for x in "\r\n"):
                        headers[header] = value
            result = self.transport(url, headers)
        if result.status == 304:
            previous = self.documents.get(source)
            if previous is None:
                raise FeedError("cache_miss", "Publisher returned 304 without a validated local document.")
            return previous, True
        if result.status != 200:
            raise FeedError("unexpected_status", "Publisher returned an unexpected response.")
        if len(result.body) > MAX_PUBLIC_BYTES:
            raise FeedError("oversized_response", "Publisher response exceeds the two-megabyte limit.")
        return result, False

    def _parse(self, source: str, data: bytes, at: datetime) -> tuple[list[dict], datetime, int, set, dict]:
        if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            raise FeedError("unsafe_xml", "Publisher XML contains unsupported DTD/entities.")
        try:
            root = ET.fromstring(data)
        except ET.ParseError as error:
            line, column = error.position
            incomplete_codes = {
                expat.errors.codes[expat.errors.XML_ERROR_NO_ELEMENTS],
                expat.errors.codes[expat.errors.XML_ERROR_UNCLOSED_TOKEN],
                expat.errors.codes[expat.errors.XML_ERROR_PARTIAL_CHAR],
                expat.errors.codes[expat.errors.XML_ERROR_UNCLOSED_CDATA_SECTION],
            }
            nse_rss_prefix = re.match(
                br"(?:\xef\xbb\xbf)?\s*(?:<\?xml[^?]*\?>\s*)?<rss(?:\s|>)", data[:512]
            )
            if (source == "nse-announcements" and nse_rss_prefix
                    and error.code in incomplete_codes and not data.rstrip().endswith(b"</rss>")):
                raise FeedError(
                    "incomplete_snapshot",
                    f"NSE published an unfinished RSS snapshot (line {line}, column {column});"
                    " waiting for a complete document.",
                    retryable=True, http_status=200,
                ) from None
            raise FeedError("invalid_xml",
                            f"Publisher returned incomplete or invalid RSS XML (line {line}, column {column}).",
                            retryable=True) from None
        if root.tag != "rss":
            raise FeedError("not_rss", "Publisher returned a non-RSS document, possibly an access page.")
        items = root.findall(".//item")
        if not items or len(items) > MAX_FEED_ITEMS:
            raise FeedError("item_count", "Publisher RSS is empty or exceeds the 5,000-item limit.")
        parsed = []
        for index, item in enumerate(items):
            title = (item.findtext("title") or "").strip()
            try:
                published = feed_timestamp(item.findtext("pubDate") or "")
            except (ValueError, TypeError, OverflowError):
                raise FeedError("invalid_timestamp", f"Publisher item {index + 1} has an invalid publication time.") from None
            if not title:
                raise FeedError("missing_title", f"Publisher item {index + 1} has no title.")
            parsed.append((item, title, published))
        latest = max(item[2] for item in parsed)
        maximum_age = 3 * 86400 if source == "nse-announcements" else 7 * 86400
        age = (at - latest).total_seconds()
        if age < -60:
            raise FeedError("future_dated", "Publisher RSS is future-dated; verify the local clock and source.")
        if age > maximum_age:
            raise FeedError("stale_document", "Publisher's most recent item is beyond the accepted source-age limit.")
        events, excluded, catalysts = [], set(), {}
        for item, title, published in parsed:
            description = html.unescape(re.sub(r"<[^>]*>", " ", item.findtext("description") or ""))
            text = " ".join((title + ": " + description).split())
            age = (at - published).total_seconds()
            if source == "nse-announcements":
                symbol = self.aliases.get(company_key(title))
                if not symbol or symbol not in self.symbols:
                    continue
                symbols = [symbol]
                material = re.search(
                    r"financial results|board meeting|stock split|suspension|fraud|default|insolvency|"
                    r"bankruptcy|regulatory action", text, re.I,
                )
                if 0 <= age <= 86400:
                    if material or (age < self.config.news.pause_minutes * 60 and HIGH_IMPACT.search(text)):
                        excluded.add(symbol)
                    if not material and re.search(
                        r"bagging|receiving of orders|order win|awarded.{0,30}contract|new contract", text, re.I
                    ):
                        catalysts.setdefault(symbol, []).append({
                            "at": published.isoformat(), "headline": text[:1000], "source": source,
                        })
            else:
                symbols = ["*"]
            if 0 <= age <= 3600:
                events.append({
                    "type": "news", "source": source, "at": published.isoformat(),
                    "headline": text[:1000], "symbols": symbols, "public": True,
                    "severity": "high" if high_impact_news(text, source) else "medium",
                })
        events.sort(key=lambda item: item["at"])
        events.append({"type": "heartbeat", "source": source, "at": at.isoformat()})
        return events, latest, len(items), excluded, catalysts

    def poll(self, require_all: bool = True) -> list[dict]:
        events, failures = [], []
        for source, url in NEWS_SOURCES.items():
            prior = self.health.get(source, {})
            last_error = None
            retry_error = ""
            for attempt in (1, 2, 3):
                try:
                    document, unchanged = self._document(source, url, refresh=attempt > 1)
                    at = now_ist()
                    parsed, latest, count, excluded, catalysts = self._parse(source, document.body, at)
                    self.documents[source] = document
                    if source == "nse-announcements":
                        self.excluded, self.catalysts = excluded, catalysts
                    events.extend(parsed)
                    self.health[source] = {
                        "healthy": True, "last_success": at.isoformat(), "last_attempt": at.isoformat(),
                        "latest_publication": latest.isoformat(), "items": count, "url": url,
                        "error": "", "error_code": "", "attempts": attempt,
                        "http_status": 304 if unchanged else 200, "consecutive_failures": 0,
                        "recovered_from": retry_error,
                        "last_verified_grace": False,
                    }
                    last_error = None
                    break
                except FeedError as error:
                    last_error = error
                except (TimeoutError, OSError):
                    last_error = FeedError("network_error", "Publisher connection failed.", retryable=True)
                except (SafetyError, ValueError, KeyError, TypeError) as error:
                    last_error = FeedError("source_error", f"Publisher validation failed ({type(error).__name__}).")
                if not last_error.retryable or attempt == 3:
                    break
                retry_error = last_error.code
                clock.sleep(1.0 if attempt == 1 else 3.0)
            if last_error is not None:
                self.health[source] = {
                    **prior, "healthy": False, "last_success": prior.get("last_success"),
                    "last_attempt": now_ist().isoformat(), "url": url,
                    "error": str(last_error), "error_code": last_error.code,
                    "http_status": last_error.http_status, "attempts": attempt,
                    "consecutive_failures": prior.get("consecutive_failures", 0) + 1,
                    "last_verified_grace": last_error.code in TRANSIENT_NEWS_CODES
                        and source in self.documents and prior.get("last_success") is not None,
                }
                failures.append(f"{source}: {last_error.code} ({last_error})")
        self.poll_interval_seconds = (
            min(30, self.config.news.rss_poll_seconds)
            if any(not row.get("healthy") and row.get("error_code") in TRANSIENT_NEWS_CODES
                   for row in self.health.values())
            else self.config.news.rss_poll_seconds
        )
        if failures and require_all:
            raise FeedError("required_source_unavailable", "Required news coverage unavailable: " + "; ".join(failures))
        return events
