from __future__ import annotations

import csv
import io
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path

from .broker import BrokerError, KiteHTTP, cash_instrument
from .core import Config, Instrument, SafetyError, now_ist, paise, timestamp
from .engine import MAX_TRACKED_STOCKS, TradingEngine
from .market import Bar, HistoryNotReady, Tape
from .market_data import (
    cached_daily_history, closed_intraday_bars, constituents, market_timestamp,
    rank_opportunities, screen_quotes, select_diverse,
)
from .storage import Store

SCAN_SECONDS = 60
EVENT_SCAN_SECONDS = 30
MAX_DAILY_SCANS = 600
HISTORY_LOADS_PER_SCAN = 3
ADMISSIONS_PER_SCAN = 2
MIN_TRACKING_SECONDS = 600


def restore_symbols(config: Config, store: Store, day: str) -> set[str]:
    """Restore admitted subscriptions/ownership without changing the frozen strategy configuration."""
    members = store.get("discovery_members") or {"active": {}, "admitted": {}}
    state = store.get("engine") or {}
    owned = {order["symbol"] for order in state.get("orders", [])
             if order["status"] not in {"COMPLETE", "CANCELLED", "REJECTED"}}
    if state.get("position"):
        owned.add(state["position"]["symbol"])
    extra_owned = owned - set(config.market.symbols)
    if not extra_owned <= set(members["admitted"]):
        raise SafetyError("Owned symbol is absent from the discovery-admission ledger; reconcile manually.")
    active = {symbol for symbol, item in members["active"].items() if item["day"] == day}
    result = (active | extra_owned) - set(config.market.symbols)
    if len(result | set(config.market.symbols)) > MAX_TRACKED_STOCKS:
        raise SafetyError("Restored discovery membership exceeds the bounded subscription pool.")
    return result


@dataclass(frozen=True)
class PreparedCandidate:
    instrument: Instrument
    bars: list[Bar]
    evaluated_at: datetime
    ranking: dict


class IntradayDiscovery:
    """Read-only, non-LLM scanning. One background request sequence at a time."""

    def __init__(self, config: Config, http: KiteHTTP, workspace: Path, store: Store):
        if http.allow_orders:
            raise SafetyError("Opportunity discovery must use a read-only broker client.")
        self.config, self.http, self.workspace, self.store = config, http, workspace, store
        self.master: dict[str, dict] = {}
        self.symbols: list[str] = []
        self.aliases: dict[str, str] = {}
        self.sectors: dict[str, str] = {}
        self.source = ""
        self.day = ""
        self.histories: dict[str, dict] = {}
        self.history_failures: dict[str, datetime] = {}
        self.previous_quotes: dict[str, tuple[datetime, int, int]] = {}
        budget = store.get("discovery_budget") or {}
        self.last_started = timestamp(budget["last_at"]) if budget.get("last_at") else None
        self.news_pending: set[str] = set()
        self.news_seen: set[str] = set()
        self.closed_reported = False
        self.budget_reported = False

    def notify_news(self, batch: list[dict]) -> None:
        for item in batch:
            if item.get("type") != "news":
                continue
            identity = item["source"] + "|" + item["at"] + "|" + item["headline"]
            if identity in self.news_seen:
                continue
            self.news_seen.add(identity)
            for symbol in item["symbols"]:
                if symbol in self.symbols:
                    self.news_pending.add(symbol)
        # De-duplication for discovery triggers is bounded and has no trading authority.
        if len(self.news_seen) > 10000:
            self.news_seen.clear()

    def due(self, at: datetime) -> bool:
        if not time(9, 20) <= at.time() < time.fromisoformat(self.config.market.entry_end):
            return False
        interval = EVENT_SCAN_SECONDS if self.news_pending else SCAN_SECONDS
        return self.last_started is None or (at - self.last_started).total_seconds() >= interval

    def begin(self, at: datetime) -> tuple[str, set[str]] | None:
        if at.time() >= time.fromisoformat(self.config.market.entry_end) and not self.closed_reported:
            self.closed_reported = True
            self.store.put("discovery_status", {
                **(self.store.get("discovery_status") or {}),
                "state": "entry_window_closed", "day": at.date().isoformat(), "at": at.isoformat(),
                "reason": "New-entry discovery is finished for this session; existing positions remain managed.",
            })
        if not self.due(at):
            return None
        budget = self.store.get("discovery_budget") or {}
        count = budget.get("scans", 0) if budget.get("day") == at.date().isoformat() else 0
        if count >= MAX_DAILY_SCANS:
            if not self.budget_reported:
                self.budget_reported = True
                self.store.put("discovery_status", {
                    **(self.store.get("discovery_status") or {}),
                    "state": "budget_exhausted", "day": at.date().isoformat(), "at": at.isoformat(),
                    "reason": "Daily discovery scan cap reached; existing positions remain managed.",
                })
            return None
        self.last_started = at
        self.store.put("discovery_budget", {
            "day": at.date().isoformat(), "scans": count + 1, "last_at": at.isoformat(),
        })
        names = set(self.news_pending)
        self.news_pending.clear()
        return ("issuer_news" if names else "scheduled_market_scan"), names

    def scan(self, at: datetime, cash: int, active: set[str], excluded: set[str],
             catalysts: dict, pauses: dict, news_ready: bool, trigger: str, news_symbols: set[str]) -> dict:
        if at.date().isoformat() != self.day:
            self.master = {row["tradingsymbol"]: row for row in csv.DictReader(io.StringIO(
                self.http.request("GET", "/instruments/NSE", raw=True)
            )) if row["exchange"] == "NSE"}
            self.symbols, self.aliases, self.sectors, self.source = constituents(
                self.master, self.workspace / "discovery-constituents.json"
            )
            if not self.symbols or len(self.symbols) > 220 or self.config.market.benchmark not in self.master:
                raise SafetyError("Discovery universe or benchmark metadata is unavailable.")
            self.day = at.date().isoformat()
            self.histories.clear()
            self.history_failures.clear()
            self.previous_quotes.clear()
            return {"kind": "catalogue", "day": self.day, "at": now_ist().isoformat(),
                    "source": self.source, "universe_count": len(self.symbols),
                    "universe": {symbol: int(self.master[symbol]["instrument_token"]) for symbol in self.symbols}}

        benchmark = self.config.market.benchmark
        quotes = self.http.request("GET", "/quote", query=[
            ("i", "NSE:" + symbol) for symbol in self.symbols + [benchmark]
        ])
        observed = now_ist()
        reference = quotes.get("NSE:" + benchmark)
        if (observed.date() != at.date() or not reference
                or not -1 <= (observed - market_timestamp(reference["timestamp"])).total_seconds() <= 30
                or paise(reference["ohlc"]["close"]) <= 0):
            raise SafetyError("Discovery benchmark quote is stale or outside the current session.")
        liquid, omissions = [], []
        for symbol in self.symbols:
            try:
                liquid.extend(screen_quotes(quotes, [symbol], cash, observed))
            except (SafetyError, ValueError, KeyError, TypeError) as error:
                omissions.append({"symbol": symbol, "reason": "Quote screening: " + type(error).__name__})
        # A notable move since the previous scan or new issuer news prioritizes history acquisition.
        for item in liquid:
            symbol = item["symbol"]
            prior = self.previous_quotes.get(symbol)
            move, burst = 0.0, 0.0
            if prior:
                elapsed = (observed - prior[0]).total_seconds()
                if 0 < elapsed <= 180:
                    move = (item["price_paise"] / prior[1] - 1) * 10000
                    change = max(0, item["turnover_paise"] - prior[2])
                    session_seconds = max(60, (observed - observed.replace(hour=9, minute=15, second=0, microsecond=0)).total_seconds())
                    burst = change / max(1, item["turnover_paise"] * elapsed / session_seconds)
            item["move_since_scan_bps"] = round(move, 2)
            item["turnover_burst_estimate"] = round(burst, 3)
            item["new_issuer_event"] = symbol in news_symbols
            item["discovery_priority"] = item["score"] + min(1, abs(move) / 50) + (1 if symbol in news_symbols else 0)
            self.previous_quotes[symbol] = (observed, item["price_paise"], item["turnover_paise"])
        liquid.sort(key=lambda item: (-item["discovery_priority"], item["symbol"]))
        history_reads = 0
        benchmark_history = cached_daily_history(
            self.http, int(self.master[benchmark]["instrument_token"]), observed, self.workspace / "history"
        )
        for item in liquid:
            symbol = item["symbol"]
            if symbol in excluded or symbol in self.histories:
                continue
            if self.history_failures.get(symbol, observed - timedelta(days=1)) > observed:
                continue
            token = int(self.master[symbol]["instrument_token"])
            cached = self.workspace / "history" / f"{observed.date().isoformat()}-{token}.json"
            if not cached.exists() and history_reads >= HISTORY_LOADS_PER_SCAN:
                continue
            if not cached.exists():
                history_reads += 1
            try:
                self.histories[symbol] = cached_daily_history(
                    self.http, token, observed, self.workspace / "history"
                )
            except BrokerError:
                raise
            except (SafetyError, ValueError, KeyError, TypeError) as error:
                reason = str(error) if isinstance(error, SafetyError) else type(error).__name__
                omissions.append({"symbol": symbol, "reason": reason})
                self.history_failures[symbol] = observed + timedelta(minutes=10)

        benchmark_move = (paise(reference["last_price"]) / paise(reference["ohlc"]["close"]) - 1) * 10000
        ranked = rank_opportunities(liquid, self.histories, benchmark_history, benchmark_move,
                                    self.sectors, catalysts, excluded, observed)
        for item in ranked:
            incremental = max(-0.5, min(0.5, item["move_since_scan_bps"] / 100))
            burst = min(0.3, math.log2(max(1, item["turnover_burst_estimate"])) * 0.1)
            item["score"] = round(item["score"] + incremental + burst, 4)
            item["reason"] += (
                f" Recent scan move {item['move_since_scan_bps']:+.0f} bps;"
                f" turnover acceleration estimate {item['turnover_burst_estimate']:.2f}x."
            )
        ranked.sort(key=lambda item: (-item["score"], item["symbol"]))
        wanted = select_diverse(ranked, limit=MAX_TRACKED_STOCKS)
        prepared = []
        if news_ready:
            for item in wanted:
                symbol = item["symbol"]
                if (symbol in active or symbol in excluded
                        or any(timestamp(pauses.get(key, "2000-01-01T00:00:00+05:30")) > observed
                               for key in ("*", symbol))):
                    continue
                if len(prepared) >= ADMISSIONS_PER_SCAN:
                    break
                when = now_ist()
                instrument = cash_instrument(symbol, self.master[symbol], quotes["NSE:" + symbol])
                try:
                    bars = closed_intraday_bars(self.http, instrument.token, when)
                    Tape().seed(bars, when)
                except BrokerError:
                    raise
                except HistoryNotReady as error:
                    omissions.append({"symbol": symbol, "reason": str(error), "history_pending": True})
                    continue
                except (SafetyError, ValueError, KeyError, TypeError) as error:
                    omissions.append({"symbol": symbol, "reason": "Intraday warm-up: " + type(error).__name__})
                    continue
                prepared.append(PreparedCandidate(instrument, bars, observed, item))
        return {
            "kind": "scan", "day": observed.date().isoformat(), "at": observed.isoformat(),
            "source": self.source, "universe_count": len(self.symbols), "trigger": trigger,
            "liquid_candidates": len(liquid), "history_ready": len(self.histories),
            "history_reads": history_reads, "ranked": ranked[:30],
            "wanted": [item["symbol"] for item in wanted],
            "prepared": prepared, "omissions": omissions, "news_ready": news_ready,
        }


def apply_scan(result: dict, engine: TradingEngine, at: datetime, subscribe, unsubscribe) -> dict:
    """Main-thread apply. Discovery never places orders and cannot retire owned symbols."""
    if result["day"] != engine.session.day.isoformat() or not 0 <= (at - timestamp(result["at"])).total_seconds() <= 45:
        engine.store.audit(at, "DISCOVERY_RESULT_STALE", reason="Outdated scan was not admitted.")
        return {"state": "stale_scan", "day": engine.session.day.isoformat(), "at": at.isoformat()}
    ranking = {item["symbol"]: item for item in result["ranked"]}
    admitted = []
    for prepared in result["prepared"][:ADMISSIONS_PER_SCAN]:
        symbol = prepared.instrument.symbol
        if symbol in engine.trade_symbols:
            continue
        if (engine.state["halt"] or engine.state["quarantine"] or not engine.reconciled
                or not time(9, 20) <= at.time() < time.fromisoformat(engine.config.market.entry_end)):
            break
        if (symbol in engine.discovery_excluded
                or any(timestamp(engine.state["pauses"].get(key, "2000-01-01T00:00:00+05:30")) > at
                       for key in ("*", symbol))
                or engine.discovery_universe.get(symbol) != prepared.instrument.token):
            continue
        if prepared.bars[-1].end != at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0):
            engine.store.audit(at, "DISCOVERY_CANDIDATE_DEFERRED", symbol=symbol,
                               reason="Warm-up crossed a candle boundary; retry next scan.")
            continue
        if len(engine.trade_symbols) >= MAX_TRACKED_STOCKS:
            removable = []
            for name in engine.trade_symbols - set(engine.config.market.symbols) - engine.owned_symbols():
                entry = engine.discovery_members["active"].get(name)
                if (entry and (at - timestamp(entry["admitted_at"])).total_seconds() >= MIN_TRACKING_SECONDS
                        and name not in result["wanted"]):
                    removable.append(name)
            if not removable:
                continue
            remove = min(removable, key=lambda name: (ranking.get(name, {}).get("score", -100), name))
            token = engine.instruments[remove].token
            if engine.retire_discovered(remove, at, "Higher-ranked intraday opportunity; no owned exposure."):
                unsubscribe(token)
        if engine.admit_discovered(prepared.instrument, prepared.bars, at, prepared.ranking["reason"]):
            try:
                subscribe(prepared.instrument, prepared.bars[-1].end)
            except (OSError, RuntimeError, ValueError) as error:
                engine.retire_discovered(symbol, at, "Subscription failed; no order was placed.")
                engine.store.audit(at, "DISCOVERY_SUBSCRIPTION_FAILED", symbol=symbol, error=type(error).__name__)
                continue
            admitted.append(symbol)
    view = {
        "state": "scanning", "day": result["day"], "at": at.isoformat(),
        "last_scan_at": result["at"], "source": result["source"],
        "universe_count": result["universe_count"], "liquid_candidates": result["liquid_candidates"],
        "history_ready": result["history_ready"], "trigger": result["trigger"],
        "active_symbols": sorted(engine.trade_symbols), "admitted_this_scan": admitted,
        "ranked": result["ranked"], "omissions": result["omissions"],
        "regular_interval_seconds": SCAN_SECONDS, "event_interval_seconds": EVENT_SCAN_SECONDS,
        "maximum_tracked_stocks": MAX_TRACKED_STOCKS, "news_ready": result["news_ready"],
    }
    engine.store.put("discovery_status", view)
    engine.store.audit(at, "DISCOVERY_SCAN", trigger=result["trigger"], universe=result["universe_count"],
                       liquid_candidates=result["liquid_candidates"], admitted=admitted,
                       active_symbols=view["active_symbols"], history_reads=result["history_reads"])
    return view
