from __future__ import annotations

import hashlib
import json
import queue
import signal
import threading
import time as clock
import urllib.error
import xml.etree.ElementTree as ET
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

from .broker import (
    BrokerError, KiteBroker, KiteHTTP, PaperBroker, load_kite_instruments,
)
from .core import Config, IST, Instrument, SafetyError, Session, Tick, now_ist, paise, timestamp
from .engine import CLOCK_HALT, FUTURE_TOLERANCE_SECONDS, TradingEngine
from .events import AIContextAgent, EventAgent, Inbox, KnowledgeBase, RSSCollector, clear_legacy_routine_auction_pause
from .market import HistoryNotReady
from .storage import InstanceLock, Store
from .reconciliation import LEGACY_READ_HALT, ReconciliationHealth
from .position_feed import LatestQuoteBuffer, POSITION_FEED_HALT, fetch_position_quote
from .streaming import (
    RECOVERABLE_STREAM_HALTS, StreamCommands, StreamEvent, StreamHealth,
    reactor_dispatch, safe_stream_reason,
)


def code_hash(root: Path) -> str:
    digest = hashlib.sha256()
    paths = (
        list((root / "india_trader").glob("*.py"))
        + list((root / "tests").glob("test_*.py"))
        + list((root / "knowledge").glob("*.md"))
        + list((root / "india_trader" / "web").glob("*.html"))
        + [root / "pyproject.toml"]
    )
    for path in sorted(paths):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def research_hash(config: Config) -> str:
    from dataclasses import asdict
    data = asdict(config)
    data.pop("live")
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def authorize_live(config: Config, session: Session, root: Path, accepted: bool) -> None:
    today = now_ist().date()
    if (not accepted or not config.live.enabled or not config.live.broker_user_id
            or not session.live_approved or session.day != today
            or not session.reviewed or not session.trading_day
            or session.account_id != config.live.broker_user_id
            or session.config_hash != config.fingerprint
            or not 0 < session.capital_rupees <= config.risk.capital_rupees
            or set(session.symbols) != set(config.market.symbols)):
        raise SafetyError("Live authorization failed. See the daily authorization checklist.")
    path = root / config.live.qualification_file
    evidence = json.loads(path.read_text(encoding="utf-8"))
    if evidence.get("code_hash") != code_hash(root) or evidence.get("research_hash") != research_hash(config):
        raise SafetyError("Qualification does not match the current code/strategy/risk/fee configuration.")
    approved_on = datetime.fromisoformat(evidence["approved_on"]).date()
    if not 0 <= (today - approved_on).days <= 7:
        raise SafetyError("Qualification/operator review must be renewed within seven days.")
    required = (
        "software_tests_passed", "evidence_gate_passed", "static_ip_confirmed",
        "broker_approved_self_coded_algo", "current_order_rules_confirmed",
        "current_fees_confirmed", "licensed_realtime_data", "news_coverage_reviewed",
        "dedicated_cash_account", "supervised_incident_drill_passed",
    )
    if any(evidence.get(key) is not True for key in required):
        raise SafetyError("Live qualification is incomplete; never mark a missing check as passed.")
    if not evidence.get("broker_approval_reference"):
        raise SafetyError("Record the broker's approval/reference for your applicable algo classification.")
    for item in evidence.get("reports", []):
        file = root / item["path"]
        if hashlib.sha256(file.read_bytes()).hexdigest() != item["sha256"]:
            raise SafetyError("A qualification report changed.")
    if not evidence.get("reports"):
        raise SafetyError("Live qualification requires real-data evaluation reports.")
    from .reports import evidence_gate
    reports = [json.loads((root / item["path"]).read_text(encoding="utf-8"))
               for item in evidence["reports"]]
    if not evidence_gate(reports, config, root)["passed"]:
        raise SafetyError("Evaluation reports no longer satisfy the research gate.")
    check = root / evidence["software_check_path"]
    software = json.loads(check.read_text(encoding="utf-8"))
    if (software.get("passed") is not True or software.get("code_hash") != code_hash(root)
            or software.get("tests_run", 0) < 20):
        raise SafetyError("Run the actual software tests; a manual assertion alone is insufficient.")


def decode_tick(raw: dict[str, Any], token_map: dict[int, Instrument]) -> Tick:
    instrument = token_map[int(raw["instrument_token"])]
    at = raw["exchange_timestamp"]
    if not isinstance(at, datetime):
        raise ValueError("Full-mode exchange timestamp missing.")
    # pykiteconnect uses datetime.fromtimestamp(): a naive value is HOST-local, not always IST.
    at = at.astimezone(IST)
    last = paise(raw["last_price"])
    if instrument.reference:
        tick = Tick(instrument.symbol, at, last, last, last, 0, 0, 0)
    else:
        buy, sell = raw["depth"]["buy"][0], raw["depth"]["sell"][0]
        tick = Tick(instrument.symbol, at, last, paise(buy["price"]), paise(sell["price"]),
                    int(raw["volume_traded"]), int(buy["quantity"]), int(sell["quantity"]),
                    paise(raw.get("average_traded_price", 0)))
    tick.validate()
    return tick


def recent_verified_news(health: dict, at: datetime, max_age: int) -> bool:
    return bool(
        health.get("last_verified_grace") and health.get("last_success")
        and 0 <= (at - timestamp(health["last_success"])).total_seconds() <= max_age
    )


def invalidate_failed_news(news, engine: TradingEngine, at: datetime | None = None) -> None:
    at = at or now_ist()
    for source in engine.config.news.required_sources:
        health = news.health.get(source)
        if (health is not None and not health.get("healthy")
                and not recent_verified_news(health, at, engine.config.news.heartbeat_max_seconds)):
            previous = engine.news_heartbeats.pop(source, None)
            if previous is not None and health.get("last_verified_grace"):
                engine.store.audit(at, "FEED_FRESHNESS_EXPIRED", source=source,
                                   last_verified_at=previous.isoformat(),
                                   maximum_age_seconds=engine.config.news.heartbeat_max_seconds)


def apply_managed_news(batch: list[dict], news, engine: TradingEngine,
                       event_agent: EventAgent, at: datetime) -> None:
    previous = engine.store.get("feed_health") or {}
    invalidate_failed_news(news, engine, at)
    for source in engine.config.news.required_sources:
        health = news.health.get(source, {})
        if not health.get("healthy"):
            grace = recent_verified_news(health, at, engine.config.news.heartbeat_max_seconds)
            if not grace:
                engine.news_heartbeats.pop(source, None)
            engine.store.audit(at, "FEED_DEGRADED" if grace else "FEED_UNAVAILABLE", source=source,
                               error=health.get("error", "No source health result."),
                               error_code=health.get("error_code", "missing_health"),
                               attempts=health.get("attempts", 0),
                               http_status=health.get("http_status"),
                               last_verified_at=health.get("last_success"))
        elif source in previous and not previous[source].get("healthy"):
            engine.store.audit(at, "FEED_RECOVERED", source=source,
                               attempts=health.get("attempts", 1),
                               http_status=health.get("http_status"))
        elif health.get("recovered_from"):
            engine.store.audit(at, "FEED_RETRY_RECOVERED", source=source,
                               error_code=health["recovered_from"], attempts=health["attempts"])
    for item in batch:
        # A headline can cross the one-hour reaction boundary while the other source loads.
        if item.get("type") == "news" and (at - timestamp(item["at"])).total_seconds() > 3600:
            continue
        event_agent.accept(item, engine, at)
    engine.store.put("feed_health", news.health)


@dataclass
class ManagedRun:
    directory: Path
    workspace: Path
    plan: dict
    keys: dict

    def authorize(self, root: Path, config: Config, session: Session) -> None:
        from .autonomy import POLICY_VERSION, default_auto_config, software_ready
        from .credentials import CredentialVault
        value = CredentialVault(self.directory / "credentials.dat").load()
        blackout = self.plan.get("global_context", {}).get("opening_blackout")
        expected_blackouts = [blackout] if blackout else []
        if (not value["auto_start"] or value.get("consent_version") != "auto-live-v1"
                or self.plan.get("policy") != POLICY_VERSION
                or not software_ready(root) or self.plan["day"] != now_ist().date().isoformat()
                or config not in tuple(
                    default_auto_config(self.plan["selected"], self.plan["account"],
                                        legacy_opening=opening, legacy_signals=signals,
                                        legacy_participation=participation)
                    for opening in (False, True) for signals in (False, True)
                    for participation in (False, True)
                )
                or session.account_id != self.plan["account"]
                or not session.live_approved or session.config_hash != config.fingerprint
                or session.blackouts != expected_blackouts
                or value["keys"] != self.keys
                or self.plan.get("credential_fingerprint") != hashlib.sha256(
                    json.dumps(self.keys, sort_keys=True).encode()).hexdigest()):
            raise SafetyError("Saved dashboard authorization or automatic policy validation failed.")

    def publish(self, engine: TradingEngine, news, at: datetime) -> None:
        cfg = engine.config
        news_fresh = all(
            source in engine.news_heartbeats and 0 <= (at - engine.news_heartbeats[source]).total_seconds()
            <= cfg.news.heartbeat_max_seconds for source in cfg.news.required_sources
        )
        reference = engine._fresh(cfg.market.benchmark, at)
        ready = (not engine.state["halt"] and not engine.state["quarantine"] and engine.market_stream_ready
                 and engine.position_feed_ready
                 and engine.broker_reads_ready and engine.reconciled and engine.snapshot_at is not None
                 and (at - engine.snapshot_at).total_seconds() <= 15
                 and reference is not None and news_fresh)
        pauses = engine.state["pauses"]
        global_pause = "*" in pauses and timestamp(pauses["*"]) > at
        candidate_available = not global_pause and any(
            engine._fresh(symbol, at) is not None
            and engine.permits_entry(symbol, at)
            and (symbol not in pauses or timestamp(pauses[symbol]) <= at)
            and symbol not in engine.discovery_excluded
            for symbol in engine.trade_symbols
        )
        health_snapshot = dict(news.health)
        failed_sources = [
            f"{name}: {health.get('error', 'source unavailable')}"
            for name, health in health_snapshot.items()
            if name in cfg.news.required_sources and not health.get("healthy")
            and not recent_verified_news(health, at, cfg.news.heartbeat_max_seconds)
        ]
        degraded_sources = [
            name for name, health in health_snapshot.items()
            if name in cfg.news.required_sources and not health.get("healthy")
            and recent_verified_news(health, at, cfg.news.heartbeat_max_seconds)
        ]
        ready = ready and not failed_sources
        status = "RECOVERY" if self.plan.get("recovery_only") else (
            "LIVE_DEGRADED" if degraded_sources else "LIVE") if ready else "BLOCKED"
        reason = engine.state["halt"] or (
            "Required news feed unavailable; new entries paused. " + "; ".join(failed_sources)
            if failed_sources else
            "Waiting for fresh market, broker and required news data." if not ready else
            "News retry in progress for " + ", ".join(degraded_sources) +
            "; using the last complete verified snapshot only within the existing freshness limit."
            if degraded_sources else
            "Live broker engine is monitoring; orders require all strategy and risk gates."
        )
        if engine.state["halt"] == CLOCK_HALT:
            fault = engine.state.get("clock_fault", {})
            delta = f" Last observed lead: {fault['ahead_seconds']:.3f}s." if "ahead_seconds" in fault else ""
            reason = (
                CLOCK_HALT + delta +
                " Sync Windows date/time; the future/stale-data tolerance has not been relaxed."
            )
        if engine.stream_status and (engine.state["halt"] in RECOVERABLE_STREAM_HALTS
                                     or (not engine.market_stream_ready and not engine.state["halt"])):
            reason = engine.stream_status["reason"]
        if (not engine.broker_reads_ready and engine.reconciliation_status
                and engine.state["halt"] in {"", LEGACY_READ_HALT}):
            reason = engine.reconciliation_status["reason"]
        if (not engine.position_feed_ready
                and engine.state["halt"] in {"", POSITION_FEED_HALT}):
            reason = engine.position_feed_status["reason"]
        if ready and global_pause:
            reason = (
                f"Market data is live; entries paused by an event until {timestamp(pauses['*']):%H:%M:%S} IST."
                " No entry is permitted during that pause."
            )
        summary = engine.summary(at)
        quotes = {symbol: {
            "symbol": symbol, "price_paise": quote.last, "bid_paise": quote.bid,
            "ask_paise": quote.ask, "at": quote.at.isoformat(),
        } for symbol, quote in engine.quotes.items()}
        engine.store.put("runtime", {
            **summary, "at": at.isoformat(), "status": status, "reason": reason,
            "live": ready and not self.plan.get("recovery_only", False),
            "entries_allowed": ready and engine.flat and candidate_available
            and time.fromisoformat(cfg.market.entry_start) <= at.time()
            < time.fromisoformat(cfg.market.entry_end)
            and engine.state["trades"] < cfg.risk.max_trades
            and not self.plan.get("recovery_only", False),
            "quotes": quotes, "news_health": health_snapshot,
            "active_symbols": sorted(engine.trade_symbols),
            "broker_snapshot_at": engine.snapshot_at.isoformat() if engine.snapshot_at else None,
            "model": cfg.ai.model, "config_policy": self.plan["policy"],
            "clock_fault": engine.state.get("clock_fault"),
            "stream": engine.stream_status,
            "reconciliation": engine.reconciliation_status,
            "position_feed": engine.position_feed_status,
            "participation_profile": cfg.strategy.participation_profile,
        })


def run_connected(
    root: Path, config: Config, session: Session, database: Path,
    mode: str, accepted: bool, kill_file: Path, *, managed: ManagedRun | None = None,
) -> dict[str, Any]:
    if mode not in {"shadow", "live"}:
        raise ValueError("Connected mode must be shadow or live.")
    if session.day != now_ist().date() or not session.trading_day:
        raise SafetyError("A reviewed trading-day manifest is required for today's session.")
    if now_ist().time() >= time.fromisoformat(config.market.close_at):
        raise SafetyError("Session has ended. Use the broker terminal to handle any carryover.")
    if mode == "live":
        if managed is None:
            authorize_live(config, session, root, accepted)
        else:
            managed.authorize(root, config, session)
    try:
        from kiteconnect import KiteTicker
        from twisted.internet import reactor
    except ImportError as exc:
        raise SafetyError('Streaming dependency missing: python -m pip install -e ".[kite]"') from exc

    http = (KiteHTTP(config, allow_orders=False, api_key=managed.keys["broker_api_key"],
                     access_token=managed.keys["broker_access_token"])
            if managed else KiteHTTP(config, allow_orders=False))
    profile = http.request("GET", "/user/profile")
    account = str(profile["user_id"])
    if mode == "live" and account != config.live.broker_user_id:
        raise SafetyError("Authenticated account does not match the authorized account.")
    lock_name = hashlib.sha256(account.encode()).hexdigest()[:24] + ".lock"
    account_lock = Path.home() / ".india-trader" / "locks" / lock_name
    with InstanceLock(account_lock), InstanceLock(database.with_suffix(".lock")), Store(database) as store:
        if mode == "live":
            holdings = http.request("GET", "/portfolio/holdings")
            if any(int(x.get("quantity", 0)) or int(x.get("t1_quantity", 0)) for x in holdings):
                raise SafetyError("Existing holdings detected. This version requires a dedicated empty account.")
        extra_symbols = set()
        if managed:
            from .discovery import restore_symbols
            extra_symbols = restore_symbols(config, store, session.day.isoformat())
        instruments = load_kite_instruments(http, config, extra_symbols=extra_symbols) if extra_symbols else load_kite_instruments(http, config)
        broker = (KiteBroker(http, instruments) if mode == "live"
                  else PaperBroker(config.risk.capital_rupees * 100, config.costs, store))
        engine = TradingEngine(config, session, instruments, broker, store, mode,
                               allow_daily_universe_change=managed is not None,
                               allow_signal_profile_upgrade=managed is not None,
                               enable_discovery=managed is not None)
        stream_health = StreamHealth(engine)
        reconciliation = ReconciliationHealth(engine)
        http.allow_orders = mode == "live"
        try:
            reconciliation.success(broker.snapshot(now_ist()), now_ist())
        except (SafetyError, ValueError, KeyError, TypeError) as error:
            reconciliation.failure(error, now_ist())
        if engine.state["quarantine"]:
            raise SafetyError("Startup reconciliation quarantined the account; inspect status and broker.")
        if managed:
            engine.state["broker_auth_required"] = False
            engine._save()
        if managed and engine.flat and engine.state["halt"] == "Operator kill switch.":
            engine.state["halt"] = ""
            engine._save()
            engine._check_risk(now_ist())
        if managed:
            clear_legacy_routine_auction_pause(engine, now_ist())
        if managed and managed.plan.get("recovery_only"):
            engine.halt("Recovery-only startup: close owned exposure before any further trading.", now_ist())
        store.audit(now_ist(), "RUN_START", mode=mode, code_hash=code_hash(root),
                    research_hash=research_hash(config), dataset_kind="licensed",
                    starting_equity=engine.state["day_start"],
                    instruments=[x.symbol for x in instruments.values()])
        token_map = {item.token: item for item in instruments.values()}
        ticks: queue.Queue[tuple[Tick, datetime, int]] = queue.Queue(maxsize=config.market.queue_size)
        latest_quotes = LatestQuoteBuffer()
        faults: queue.SimpleQueue[str] = queue.SimpleQueue()
        stream_events: queue.SimpleQueue[StreamEvent] = queue.SimpleQueue()
        stream_generation = [0]
        closing_stream = threading.Event()
        overflow, dirty = threading.Event(), threading.Event()
        inbox = Inbox(root / config.news.inbox)
        ai = AIContextAgent(config, store, KnowledgeBase(root / "knowledge"),
                            api_key=managed.keys["ai_api_key"] if managed else None)
        events = EventAgent(config, store, ai)
        events.allowed_symbols.update(engine.trade_symbols)
        if managed:
            from .market_data import AutomaticNews
            rss = AutomaticNews(config, managed.plan["aliases"], symbols=sorted(engine.trade_symbols))
        else:
            rss = RSSCollector(config)
        ticker = KiteTicker(http.api_key, http.access_token, reconnect=True,
                            reconnect_max_tries=10, reconnect_max_delay=30)
        stream_commands = StreamCommands(ticker, reactor_dispatch(reactor))
        position_http = KiteHTTP(config, False, api_key=http.api_key, access_token=http.access_token)
        discovery = None
        if managed and not managed.plan.get("recovery_only"):
            from .discovery import IntradayDiscovery
            discovery_http = KiteHTTP(config, False, api_key=http.api_key, access_token=http.access_token)
            discovery = IntradayDiscovery(config, discovery_http, managed.workspace, store)
            store.put("discovery_status", {
                "day": session.day.isoformat(), "state": "initializing", "at": now_ist().isoformat(),
                "active_symbols": sorted(engine.trade_symbols),
                "reason": "Preparing bounded intraday discovery beyond the initial picks.",
            })
        first_ticks: dict[str, Tick] = {}
        first_receipts: dict[str, datetime] = {}
        discard_before: dict[str, datetime] = {}
        first_lock = threading.Lock()

        def on_connect(ws, _response):
            with first_lock:
                tokens = list(token_map)
                stream_generation[0] += 1
                generation = stream_generation[0]
                first_ticks.clear()
                first_receipts.clear()
            stream_events.put(StreamEvent("connected", now_ist(), generation))
            # This callback is already on the reactor thread.
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)

        def on_ticks(_ws, batch):
            received_at = now_ist()
            with first_lock:
                mapping = dict(token_map)
                generation = stream_generation[0]
            for raw in batch:
                try:
                    if raw.get("mode") != "full":
                        continue
                    # Updates already in flight for a retired subscription are not a corrupt feed.
                    if raw.get("instrument_token") not in mapping:
                        continue
                    item = decode_tick(raw, mapping)
                    if item.at.time() < time(9, 15):
                        continue
                    with first_lock:
                        first_ticks.setdefault(item.symbol, item)
                        first_receipts.setdefault(item.symbol, received_at)
                        cutoff = discard_before.get(item.symbol)
                    if cutoff is not None and item.at < cutoff:
                        continue
                    latest_quotes.offer(item, received_at, generation)
                    ticks.put_nowait((item, received_at, generation))
                except queue.Full:
                    overflow.set()
                except (ValueError, KeyError, IndexError, TypeError):
                    faults.put("Invalid full-mode market payload.")

        def stream_event(kind: str, code=None, reason=""):
            if closing_stream.is_set():
                return
            with first_lock:
                generation = stream_generation[0]
            clean_code = code if type(code) is int else None
            clean_reason = safe_stream_reason(reason, (http.api_key, http.access_token))
            stream_events.put(StreamEvent(kind, now_ist(), generation, clean_code, clean_reason))

        def on_error(_ws, code, reason):
            stream_event("error", code, reason)

        def on_close(_ws, code, reason):
            stream_event("closed", code, reason)

        def process_stream_events():
            while True:
                try:
                    event = stream_events.get_nowait()
                except queue.Empty:
                    break
                stream_health.event(event)
                dirty.set()
                if engine.state.get("broker_auth_required"):
                    http.auth_expired = True

        def refresh_latest_quotes(at: datetime) -> None:
            latest = latest_quotes.take(stream_health.generation)
            if not stream_health.connected:
                return
            for quote, received in latest:
                if quote.symbol in engine.instruments:
                    engine.observe_stream_quote(quote, received, at)

        def subscribe_discovered(instrument: Instrument, cutoff: datetime) -> None:
            with first_lock:
                token_map[instrument.token] = instrument
                discard_before[instrument.symbol] = cutoff
            try:
                stream_commands.subscribe([instrument.token])
            except (OSError, RuntimeError, ValueError):
                with first_lock:
                    token_map.pop(instrument.token, None)
                    discard_before.pop(instrument.symbol, None)
                raise

        def unsubscribe_discovered(token: int) -> None:
            with first_lock:
                instrument = token_map.pop(token, None)
                if instrument:
                    first_ticks.pop(instrument.symbol, None)
                    first_receipts.pop(instrument.symbol, None)
                    discard_before.pop(instrument.symbol, None)
            stream_commands.unsubscribe([token])

        ticker.on_connect, ticker.on_ticks = on_connect, on_ticks
        ticker.on_error, ticker.on_close = on_error, on_close
        ticker.on_reconnect = lambda _ws, attempt: stream_event("retry", attempt)
        ticker.on_noreconnect = lambda _ws: stream_event("exhausted", None, "SDK reconnect attempts exhausted.")
        ticker.on_order_update = lambda _ws, _data: dirty.set()
        interrupted = [0]
        previous_signal = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda *_: interrupted.__setitem__(0, interrupted[0] + 1))
        last_snapshot, last_clock, last_rss, last_status = 0.0, 0.0, 0.0, 0.0
        snapshot_future = rss_future = discovery_future = position_quote_future = None
        final: dict[str, Any] = {}
        try:
            ticker.connect(threaded=True)
            if managed and not managed.plan.get("recovery_only"):
                from .market_data import closed_intraday_bars
                deadline = clock.monotonic() + 20
                complete = False
                while clock.monotonic() < deadline:
                    process_stream_events()
                    with first_lock:
                        complete = len(first_ticks) == len(instruments)
                    if complete:
                        break
                    clock.sleep(0.1)
                if not complete:
                    process_stream_events()
                    if stream_health.restart_needed:
                        # Persist the specific fault; the controller may restart only if flat.
                        raise SafetyError(engine.stream_status["reason"])
                    raise SafetyError("Not all subscribed instruments supplied an initial full quote.")
                # A bad clock must not be concealed by spending time on history requests.
                with first_lock:
                    opening_ticks, opening_receipts = dict(first_ticks), dict(first_receipts)
                for symbol, quote in opening_ticks.items():
                    if (quote.at - opening_receipts[symbol]).total_seconds() > FUTURE_TOLERANCE_SECONDS:
                        engine.on_tick(quote, opening_receipts[symbol], now_ist())
                        raise SafetyError(CLOCK_HALT)
                opening_end = now_ist().replace(hour=9, minute=20, second=0, microsecond=0)
                partial_open = [symbol for symbol, quote in opening_ticks.items()
                                if time(9, 15, 3) < quote.at.time() < time(9, 20)]
                if partial_open:
                    with first_lock:
                        for symbol in partial_open:
                            discard_before[symbol] = opening_end
                    while now_ist() < opening_end:
                        if kill_file.exists() or interrupted[0]:
                            raise SafetyError("Operator stopped opening-history warm-up; no new orders were placed.")
                        clock.sleep(0.1)
                for symbol, instrument in instruments.items():
                    observed = opening_end if symbol in partial_open else opening_ticks[symbol].at
                    if observed.time() >= time(9, 20):
                        try:
                            bars = closed_intraday_bars(http, instrument.token, observed)
                            engine.signals.tapes[symbol].seed(bars, observed)
                        except HistoryNotReady as error:
                            store.audit(now_ist(), "HISTORY_WAIT", symbol=symbol,
                                        expected_through=error.expected.isoformat(),
                                        available_through=error.available.isoformat() if error.available else None)
                            raise HistoryNotReady(error.expected, error.available, symbol) from None
                        discard_before[symbol] = bars[-1].end
            process_stream_events()
            stream_health.mark_warmed()
            with ThreadPoolExecutor(max_workers=4, thread_name_prefix="market-io") as workers:
                while True:
                    at, mono = now_ist(), clock.monotonic()
                    process_stream_events()
                    if managed and http.auth_expired:
                        engine.state["broker_auth_required"] = True
                        engine.halt("Broker session expired; official reauthentication required.", at, liquidate=False)
                        engine._save()
                        break
                    if interrupted[0] >= 2:
                        engine.halt("Forced operator shutdown; verify broker exposure immediately.", at,
                                    liquidate=False)
                        break
                    while not faults.empty():
                        engine.halt(faults.get(), at)
                    if overflow.is_set():
                        engine.halt("Market queue overflow; possible lost ticks.", at)
                    if managed:
                        invalidate_failed_news(rss, engine, at)
                    refresh_latest_quotes(now_ist())
                    if snapshot_future and snapshot_future.done():
                        try:
                            reconciliation.success(snapshot_future.result(), now_ist())
                        except (SafetyError, ValueError, KeyError, TypeError) as exc:
                            reconciliation.failure(exc, now_ist())
                        snapshot_future = None
                    # Broker writes/reconciliation can block the main thread while ticks still arrive.
                    # Refresh risk quotes before processing candles or testing position-feed freshness.
                    refresh_latest_quotes(now_ist())
                    if position_quote_future and position_quote_future.done():
                        try:
                            engine.position_feed.accept(position_quote_future.result(), now_ist())
                        except (SafetyError, OSError, ValueError, TypeError, KeyError, IndexError) as error:
                            engine.position_feed.failed(error, now_ist(), clock.monotonic())
                            if isinstance(error, BrokerError) and error.session_expired:
                                http.auth_expired = True
                        position_quote_future = None
                    for _ in range(256):
                        try:
                            item, received_at, generation = ticks.get_nowait()
                        except queue.Empty:
                            break
                        process_stream_events()
                        if generation != stream_health.generation or not stream_health.connected:
                            continue
                        if item.symbol not in engine.instruments:
                            continue
                        if item.at < discard_before.get(item.symbol, item.at):
                            continue
                        tape = engine.signals.tapes[item.symbol]
                        if (discovery and not stream_health.restart_needed and item.symbol not in config.market.symbols
                                and item.symbol != config.market.benchmark and tape.seeded and tape.latest is None
                                and tape.bars[-1].end != item.at.replace(
                                    minute=item.at.minute // 5 * 5, second=0, microsecond=0)):
                            token = engine.instruments[item.symbol].token
                            if engine.retire_discovered(item.symbol, now_ist(),
                                                        "Initial stream crossed warm-up boundary; rediscover later."):
                                unsubscribe_discovered(token)
                                continue
                        if isinstance(broker, PaperBroker):
                            broker.on_tick(item)
                        processed_at = now_ist()
                        refresh_latest_quotes(processed_at)
                        engine.on_tick(item, received_at, processed_at,
                                       evaluate_signals=not stream_health.restart_needed)
                        stream_health.observe(item, received_at, processed_at)
                    stream_health.ready(now_ist())
                    if snapshot_future is None and reconciliation.due(
                        mono, last_snapshot, dirty=dirty.is_set()
                    ):
                        if isinstance(broker, PaperBroker):
                            reconciliation.success(broker.snapshot(at), at)
                        else:
                            snapshot_future = workers.submit(broker.snapshot, at)
                        last_snapshot = mono
                        dirty.clear()
                    if rss_future and rss_future.done():
                        try:
                            batch = rss_future.result()
                            if managed:
                                apply_managed_news(batch, rss, engine, events, now_ist())
                                engine.discovery_excluded = set(rss.excluded)
                                if discovery:
                                    discovery.notify_news(batch)
                            else:
                                for event in batch:
                                    events.accept(event, engine, now_ist())
                        except (SafetyError, urllib.error.URLError, OSError, ValueError,
                                TypeError, ET.ParseError) as exc:
                            if managed:
                                engine.news_heartbeats.clear()
                                store.audit(now_ist(), "FEED_PROCESSING_ERROR", error=type(exc).__name__)
                            else:
                                engine.halt("RSS collector failed: " + type(exc).__name__, now_ist(),
                                            liquidate=False)
                        rss_future = None
                    if discovery_future is not None and discovery_future.done():
                        try:
                            result = discovery_future.result()
                            # Change collector scope only between polls; require a new full-universe
                            # verification before admitting any newly discovered trading symbol.
                            if result["kind"] != "catalogue" or rss_future is None:
                                if result["kind"] == "catalogue":
                                    engine.discovery_universe = result["universe"]
                                    rss.aliases = {**managed.plan["aliases"], **discovery.aliases}
                                    rss.symbols = set(discovery.symbols) | engine.trade_symbols
                                    events.allowed_symbols = set(rss.symbols)
                                    engine.news_heartbeats.pop("nse-announcements", None)
                                    last_rss = 0
                                    store.put("discovery_status", {
                                        "state": "ready", "day": result["day"], "at": result["at"],
                                        "universe_count": result["universe_count"], "source": result["source"],
                                        "active_symbols": sorted(engine.trade_symbols),
                                        "reason": "Broad universe loaded; fresh news and first scan pending.",
                                    })
                                else:
                                    from .discovery import apply_scan
                                    engine.discovery_excluded = set(rss.excluded)
                                    apply_scan(result, engine, now_ist(), subscribe_discovered, unsubscribe_discovered)
                                discovery_future = None
                        except (SafetyError, OSError, ValueError, TypeError, KeyError) as exc:
                            reason = str(exc) if isinstance(exc, SafetyError) else type(exc).__name__
                            store.audit(now_ist(), "DISCOVERY_ERROR", reason=reason)
                            previous = store.get("discovery_status") or {}
                            store.put("discovery_status", {
                                **previous, "state": "degraded", "day": session.day.isoformat(),
                                "at": now_ist().isoformat(), "reason": reason,
                                "active_symbols": sorted(engine.trade_symbols),
                            })
                            if isinstance(exc, BrokerError) and exc.session_expired:
                                http.auth_expired = True
                            discovery_future = None
                    if discovery and discovery_future is None and not engine.state["halt"] and not engine.state["quarantine"]:
                        started = discovery.begin(now_ist())
                        if started:
                            trigger, news_symbols = started
                            check_at = now_ist()
                            news_ready = all(
                                source in engine.news_heartbeats
                                and 0 <= (check_at - engine.news_heartbeats[source]).total_seconds()
                                <= config.news.heartbeat_max_seconds for source in config.news.required_sources
                            )
                            scan_cash = min(engine.state["capital"], engine.state["cash"])
                            discovery_future = workers.submit(
                                discovery.scan, check_at, scan_cash, set(engine.trade_symbols),
                                set(rss.excluded), deepcopy(rss.catalysts), dict(engine.state["pauses"]),
                                news_ready, trigger, news_symbols,
                            )
                    if ((config.news.rss_urls or managed) and rss_future is None
                            and mono - last_rss >= (
                                rss.poll_interval_seconds if managed else config.news.rss_poll_seconds)):
                        rss_future = workers.submit(rss.poll, require_all=False) if managed else workers.submit(rss.poll)
                        last_rss = mono
                    at, mono = now_ist(), clock.monotonic()
                    if mono - last_clock >= 1:
                        try:
                            for event in ([] if managed else inbox.read()):
                                # On restart old complete records are replayed only for de-duplication.
                                event_at = datetime.fromisoformat(event["at"].replace("Z", "+00:00"))
                                if event_at.tzinfo is None:
                                    raise SafetyError("Inbox timestamps must be timezone-aware.")
                                age = (at - event_at).total_seconds()
                                if age > (config.news.heartbeat_max_seconds if event.get("type") == "heartbeat"
                                          else 3600):
                                    continue
                                events.accept(event, engine, at)
                        except (SafetyError, ValueError, KeyError, TypeError, OSError) as exc:
                            engine.halt("News inbox failed: " + type(exc).__name__, at, liquidate=False)
                        ai.drain(engine, at)
                        refresh_latest_quotes(now_ist())
                        at, mono = now_ist(), clock.monotonic()
                        engine.timer(at, kill=kill_file.exists() or interrupted[0] > 0)
                        if position_quote_future is None and engine.position_feed.due(at, mono):
                            position = engine.position
                            engine.position_feed.started(mono)
                            position_quote_future = workers.submit(
                                fetch_position_quote, position_http, engine.instruments[position.symbol],
                                position.trade_id,
                            )
                        if discovery:
                            for symbol in list(engine.trade_symbols - set(config.market.symbols) - engine.owned_symbols()):
                                entry = engine.discovery_members["active"].get(symbol)
                                if (entry and engine.signals.tapes[symbol].latest is None
                                        and (at - timestamp(entry["admitted_at"])).total_seconds() > 30):
                                    token = engine.instruments[symbol].token
                                    if engine.retire_discovered(symbol, at, "No initial stream within 30 seconds."):
                                        unsubscribe_discovered(token)
                        if managed:
                            managed.publish(engine, rss, at)
                        last_clock = mono
                    if mono - last_status >= 30:
                        store.audit(at, "EQUITY", equity=engine.marked_equity(at))
                        print(json.dumps(engine.summary(at), sort_keys=True), flush=True)
                        last_status = mono
                    if engine.flat and (interrupted[0] or kill_file.exists()
                                        or at.time() >= time.fromisoformat(config.market.close_at)):
                        break
                    if stream_health.may_restart_flat(at):
                        store.audit(at, "STREAM_RESTART_READY", reason="Flat broker-reconciled worker will rebuild after reconnect.")
                        break
                    clock.sleep(0.05)
                final = engine.summary(now_ist())
                if managed:
                    managed.publish(engine, rss, now_ist())
        finally:
            signal.signal(signal.SIGINT, previous_signal)
            closing_stream.set()
            try:
                stream_commands.close()
            except (SafetyError, OSError, RuntimeError, ValueError) as error:
                store.audit(now_ist(), "STREAM_CLOSE_FAILED", error=type(error).__name__)
            ai.close()
            final = engine.summary(now_ist())
            store.put("last_status", final)
            store.audit(now_ist(), "RUN_END", **final)
        return final
