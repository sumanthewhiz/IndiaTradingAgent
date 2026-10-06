from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from .broker import BrokerError, KiteHTTP
from .core import Instrument, SafetyError, Tick, now_ist, paise, timestamp

if TYPE_CHECKING:
    from .engine import TradingEngine

POSITION_FEED_HALT = "Stale/missing position feed; keep broker protection until a fresh exit is possible."
SUSTAINED_OUTAGE_SECONDS = 15


class LatestQuoteBuffer:
    """Latest executable quotes are independent of the ordered indicator queue."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latest: dict[str, tuple[Tick, datetime, int]] = {}

    def offer(self, tick: Tick, received_at: datetime, generation: int) -> None:
        with self.lock:
            previous = self.latest.get(tick.symbol)
            if previous is None or generation > previous[2] or (
                generation == previous[2]
                and (tick.at, received_at) >= (previous[0].at, previous[1])
            ):
                self.latest[tick.symbol] = (tick, received_at, generation)

    def take(self, generation: int) -> list[tuple[Tick, datetime]]:
        with self.lock:
            current, self.latest = self.latest, {}
        return [(tick, received) for tick, received, epoch in current.values() if epoch == generation]


@dataclass(frozen=True)
class PositionQuote:
    trade_id: str
    tick: Tick
    received_at: datetime


def fetch_position_quote(http: KiteHTTP, instrument: Instrument, trade_id: str) -> PositionQuote:
    from .market_data import market_timestamp
    if http.allow_orders or instrument.reference:
        raise SafetyError("Position quote refresh requires a read-only cash-instrument client.")
    key = "NSE:" + instrument.symbol
    response = http.request("GET", "/quote", query=[("i", key)])
    received = now_ist()
    if not isinstance(response, dict):
        raise SafetyError("Broker position quote response is not a mapping.")
    row = response.get(key)
    if not isinstance(row, dict) or int(row.get("instrument_token", -1)) != instrument.token:
        raise SafetyError("Broker position quote did not match the owned instrument.")
    bid, ask = row["depth"]["buy"][0], row["depth"]["sell"][0]
    tick = Tick(
        instrument.symbol, market_timestamp(row["timestamp"]), paise(row["last_price"]),
        paise(bid["price"]), paise(ask["price"]), int(row["volume"]),
        int(bid["quantity"]), int(ask["quantity"]), paise(row.get("average_price", 0)),
    )
    tick.validate()
    if not instrument.lower <= tick.bid <= tick.ask <= instrument.upper or min(tick.bid_size, tick.ask_size) <= 0:
        raise SafetyError("Owned-position quote has no valid executable depth inside the instrument's bands.")
    return PositionQuote(trade_id, tick, received)


class PositionFeedHealth:
    def __init__(self, engine: TradingEngine):
        self.engine = engine
        self.next_refresh = 0.0
        self.refresh_failures = 0
        self.refresh_disabled = False
        engine.position_feed_ready = True
        engine.position_feed_status = {"state": "idle", "reason": "No owned position needs a price refresh."}

    def _reconciled(self, at: datetime) -> bool:
        engine = self.engine
        return bool(engine.reconciled and engine.broker_reads_ready and not engine.state["quarantine"]
                    and not engine.state.get("clock_fault") and not engine.state.get("broker_auth_required")
                    and engine.snapshot_at and 0 <= (at - engine.snapshot_at).total_seconds() <= 15)

    def check(self, at: datetime) -> None:
        engine = self.engine
        pos = engine.position
        previous = engine.state.get("position_feed_fault")
        if pos is None or not pos.quantity:
            if engine.flat and self._reconciled(at):
                if engine.state["halt"] == POSITION_FEED_HALT:
                    engine.state["halt"] = ""
                    engine.store.audit(at, "POSITION_FEED_RECOVERED",
                                       reason="Owned position closed; fresh broker reconciliation confirms no exposure/orders.",
                                       capital_and_trade_history_preserved=True)
                    engine.state.pop("position_feed_fault", None)
                    engine._save()
                    engine._check_risk(at)
                elif previous:
                    engine.state.pop("position_feed_fault", None)
                    engine._save()
            engine.position_feed_ready = True
            engine.position_feed_status = {"state": "idle", "reason": "No owned position requires a quote."}
            return
        quote = engine.position_quote(pos.symbol, at)
        if quote is not None:
            engine.position_feed_ready = True
            source = engine.position_quote_source(pos.symbol, at)
            engine.position_feed_status = {
                "state": "healthy", "symbol": pos.symbol, "source": source,
                "exchange_at": quote.at.isoformat(), "age_seconds": round((at-quote.at).total_seconds(), 3),
                "reason": "Fresh owned-position quote; ownership and exit checks remain enforced.",
            }
            if previous and previous.get("trade_id") == pos.trade_id and engine.state["halt"] != POSITION_FEED_HALT:
                engine.store.audit(at, "POSITION_FEED_RECOVERED", symbol=pos.symbol,
                                   reason="Fresh executable quote restored; no stale-quote forced exit.",
                                   source=source, exchange_at=quote.at.isoformat())
                engine.state.pop("position_feed_fault", None)
                engine._save()
            return

        engine.position_feed_ready = False
        stream_quote = engine.quotes.get(pos.symbol)
        age = (at-stream_quote.at).total_seconds() if stream_quote else None
        if previous is None or previous.get("trade_id") != pos.trade_id:
            previous = {
                "trade_id": pos.trade_id, "symbol": pos.symbol, "detected_at": at.isoformat(),
                "last_exchange_at": stream_quote.at.isoformat() if stream_quote else None,
                "last_received_at": engine.quote_receipts.get(pos.symbol),
                "quote_age_seconds": round(age, 3) if age is not None else None,
                "freshness_limit_seconds": engine.config.market.max_quote_age_seconds,
            }
            engine.state["position_feed_fault"] = previous
            engine.store.audit(at, "POSITION_FEED_STALE", **previous)
            engine._save()
        confirmed_protection = sum(
            order.remaining for order in engine.orders
            if order.symbol == pos.symbol and order.purpose == "protect"
            and order.status in {"OPEN", "TRIGGER PENDING"} and not order.cancel_requested
        )
        engine.position_feed_status = {
            **previous, "state": "awaiting_fresh_quote", "at": at.isoformat(),
            "last_reconciled_stop_quantity": confirmed_protection,
            "quote_age_seconds": round(age, 3) if age is not None else None,
            "reason": (
                f"Owned-position quote for {pos.symbol} is missing/stale; new entries paused."
                " Keeping existing broker protection and checking for a fresh executable quote."
            ),
        }
        elapsed = (at-timestamp(previous["detected_at"])).total_seconds()
        if elapsed >= SUSTAINED_OUTAGE_SECONDS or (age is not None and age >= SUSTAINED_OUTAGE_SECONDS):
            engine.halt(POSITION_FEED_HALT, at)

    def due(self, at: datetime, monotonic: float) -> bool:
        engine = self.engine
        return bool(
            engine.position and engine.position.quantity > 0
            and engine._fresh(engine.position.symbol, at) is None
            and not self.refresh_disabled and monotonic >= self.next_refresh
        )

    def started(self, monotonic: float) -> None:
        self.next_refresh = monotonic + 5

    def accept(self, result: PositionQuote, at: datetime) -> bool:
        engine = self.engine
        pos = engine.position
        if (pos is None or not pos.quantity or pos.trade_id != result.trade_id
                or pos.symbol != result.tick.symbol):
            engine.store.audit(at, "POSITION_QUOTE_DISCARDED", symbol=result.tick.symbol,
                               reason="Owned trade changed/closed before read-only quote arrived.")
            return False
        quote = result.tick
        if (quote.at.date() != engine.session.day
                or (quote.at-result.received_at).total_seconds() > 1
                or not -1 <= (at-quote.at).total_seconds() <= engine.config.market.max_quote_age_seconds
                or not 0 <= (at-result.received_at).total_seconds() <= engine.config.market.max_quote_age_seconds):
            raise SafetyError("Position quote refresh is stale or future-dated; exchange time was not replaced.")
        quote.validate()
        engine.exit_quotes[quote.symbol] = quote
        self.refresh_failures = 0
        engine.store.audit(at, "POSITION_QUOTE_REFRESHED", symbol=quote.symbol,
                           source="broker_readonly_quote", exchange_at=quote.at.isoformat(),
                           received_at=result.received_at.isoformat())
        self.check(at)
        engine._check_risk(at)
        engine._drive(at)
        return True

    def failed(self, error: Exception, at: datetime, monotonic: float) -> None:
        self.refresh_failures += 1
        delay = min(30, 5 * 2 ** min(self.refresh_failures-1, 3))
        if isinstance(error, BrokerError) and error.retry_after_seconds is not None:
            delay = max(delay, error.retry_after_seconds)
        if isinstance(error, BrokerError) and (error.session_expired or error.status_code in {401, 403}):
            self.refresh_disabled = True
        self.next_refresh = monotonic + delay
        self.engine.store.audit(at, "POSITION_QUOTE_REFRESH_FAILED",
                                reason=str(error) if isinstance(error, SafetyError) else type(error).__name__,
                                next_retry_seconds=delay, disabled=self.refresh_disabled)
