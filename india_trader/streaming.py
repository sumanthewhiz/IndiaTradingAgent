from __future__ import annotations

import re
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

from .core import SafetyError, Tick, timestamp

if TYPE_CHECKING:
    from .engine import TradingEngine

STREAM_HALT = "Broker market-data stream interrupted; reconnecting and revalidating."
LEGACY_STREAM_HALTS = frozenset({
    "Broker WebSocket reported an error.",
    "Broker WebSocket disconnected; recovery is risk-management-only.",
})
RECOVERABLE_STREAM_HALTS = LEGACY_STREAM_HALTS | {STREAM_HALT}
RESTART_WINDOW_SECONDS = 900
MAX_AUTOMATIC_RESTARTS = 3


def safe_stream_reason(reason: object, secrets: tuple[str, ...] = ()) -> str:
    text = reason.decode("utf-8", errors="replace") if isinstance(reason, bytes) else str(reason or "")
    for secret in sorted((value for value in secrets if value), key=len, reverse=True):
        text = text.replace(secret, "[redacted]")
    text = re.sub(r"(?:wss?|https?)://\S+", "[redacted URL]", text, flags=re.I)
    text = re.sub(
        r"\b(authorization|api_key|api_secret|access_token|request_token|password)"
        r"\b[\"']?\s*[:=]\s*[^\r\n,;]+", r"\1=[redacted]", text, flags=re.I,
    )
    return " ".join("".join(c for c in text if c.isprintable() or c.isspace()).split())[:400]


@dataclass(frozen=True)
class StreamEvent:
    kind: str
    at: datetime
    generation: int
    code: int | None = None
    reason: str = ""


def reactor_dispatch(reactor) -> Callable:
    """Run a bounded command on Twisted's owning thread, propagating its result/error."""
    from twisted.internet.defer import maybeDeferred

    def dispatch(operation: Callable):
        if not reactor.running:
            raise SafetyError("WebSocket event loop is not running.")
        future: Future = Future()

        def execute():
            if not future.set_running_or_notify_cancel():
                return
            def success(value):
                future.set_result(value)
                return value

            def failure(error):
                future.set_exception(error.value)
                return None

            maybeDeferred(operation).addCallbacks(success, failure)

        reactor.callFromThread(execute)
        try:
            return future.result(timeout=5)
        except FutureTimeout:
            future.cancel()
            raise SafetyError("WebSocket control command timed out; subscriptions require reconciliation.") from None

    return dispatch


class StreamCommands:
    def __init__(self, ticker, dispatch: Callable):
        self.ticker, self.dispatch = ticker, dispatch

    def subscribe(self, tokens: list[int]) -> None:
        def operation():
            if not self.ticker.is_connected():
                raise SafetyError("Cannot subscribe while the broker stream is disconnected.")
            complete = False
            try:
                self.ticker.subscribe(list(tokens))
                self.ticker.set_mode(self.ticker.MODE_FULL, list(tokens))
                complete = True
            finally:
                if not complete:
                    for token in tokens:
                        self.ticker.subscribed_tokens.pop(token, None)
        self.dispatch(operation)

    def unsubscribe(self, tokens: list[int]) -> None:
        def operation():
            if self.ticker.is_connected():
                self.ticker.unsubscribe(list(tokens))
            else:
                # Remove desired subscriptions even offline, before the SDK resubscribes.
                for token in tokens:
                    self.ticker.subscribed_tokens.pop(token, None)
        self.dispatch(operation)

    def close(self) -> None:
        self.dispatch(self.ticker.close)


class StreamHealth:
    """One main-thread owner. Only transient stream halts can recover; order state is untouched."""

    def __init__(self, engine: TradingEngine):
        self.engine = engine
        self.generation = 0
        self.connected_at: datetime | None = None
        self.connected = False
        self.warmed = False
        self.restart_needed = False
        self.incident_recorded = False
        self.fault_at: datetime | None = None
        self.samples: dict[str, tuple[datetime, datetime, int]] = {}
        self.engine.market_stream_ready = False
        self.engine.stream_status = {"state": "connecting", "reason": "Connecting broker market-data stream."}

    def event(self, event: StreamEvent) -> None:
        engine = self.engine
        if event.kind == "connected":
            self.generation = event.generation
            self.connected = True
            self.connected_at = event.at
            self.samples.clear()
            engine.market_stream_ready = False
            engine.reconciled = False
            engine.store.audit(event.at, "STREAM_CONNECTED", generation=self.generation)
            self._status("validating", "Broker stream connected; fresh data and broker reconciliation pending.", event.at)
            return
        if event.generation != self.generation and self.generation:
            return
        if event.kind == "retry":
            engine.store.audit(event.at, "STREAM_RETRY", attempt=event.code, generation=event.generation)
            return
        if event.kind not in {"error", "closed", "exhausted"}:
            raise ValueError("Unknown stream event.")
        self.connected = False
        self.samples.clear()
        engine.market_stream_ready = False
        engine.quotes.clear()
        engine.reconciled = False
        auth = event.code in {401, 403} or bool(re.search(
            r"TokenException|invalid.{0,30}(?:access.token|api.key)|session.{0,20}expired",
            event.reason, re.I,
        ))
        retryable = event.kind != "exhausted" and event.code in {1000, 1001, 1006, 1011, 1012, 1013} and not auth
        detail = f"WebSocket {event.code if event.code is not None else 'unknown'}"
        if event.reason:
            detail += ": " + event.reason
        engine.store.audit(event.at, "STREAM_ERROR", kind_detail=event.kind, code=event.code,
                           reason=event.reason, generation=event.generation, retryable=retryable)
        if not self.incident_recorded:
            attempts = [
                value for value in engine.state.get("stream_restart_history", [])
                if 0 <= (event.at - timestamp(value)).total_seconds() < RESTART_WINDOW_SECONDS
            ]
            attempts.append(event.at.isoformat())
            engine.state["stream_restart_history"] = attempts
            engine.state["stream_fault"] = {
                "at": event.at.isoformat(), "code": event.code, "reason": event.reason,
                "retryable": retryable, "auto_retry_allowed": retryable and len(attempts) <= MAX_AUTOMATIC_RESTARTS,
                "retry_delay_seconds": min(60, 5 * 2 ** min(len(attempts) - 1, 4)),
            }
            self.fault_at = event.at
            self.incident_recorded = True
        elif not retryable:
            engine.state["stream_fault"]["retryable"] = False
            engine.state["stream_fault"]["auto_retry_allowed"] = False
        self.restart_needed = True
        if auth:
            engine.state["broker_auth_required"] = True
        engine.halt(STREAM_HALT if retryable else detail + ". Broker/data permission review required.", event.at)
        engine._save()
        self._status("reconnecting" if retryable else "blocked", detail, event.at)

    def _status(self, state: str, reason: str, at: datetime) -> None:
        self.engine.stream_status = {
            "state": state, "reason": reason, "at": at.isoformat(),
            "connected": self.connected, "generation": self.generation,
            "last_connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_error_code": self.engine.state.get("stream_fault", {}).get("code"),
            "last_error_reason": self.engine.state.get("stream_fault", {}).get("reason", ""),
        }

    def mark_warmed(self) -> None:
        self.warmed = True

    def observe(self, tick: Tick, received_at: datetime, at: datetime) -> None:
        from .engine import FUTURE_TOLERANCE_SECONDS
        if (not self.connected or self.restart_needed or not self.warmed
                or self.connected_at is None or received_at < self.connected_at):
            return
        age = (at - tick.at).total_seconds()
        if (tick.at.date() != self.engine.session.day
                or (tick.at - received_at).total_seconds() > FUTURE_TOLERANCE_SECONDS
                or not -FUTURE_TOLERANCE_SECONDS <= age <= self.engine.config.market.max_quote_age_seconds):
            self.samples.pop(tick.symbol, None)
            return
        previous = self.samples.get(tick.symbol)
        if previous is None:
            self.samples[tick.symbol] = (received_at, received_at, 1)
        elif received_at > previous[1]:
            self.samples[tick.symbol] = (previous[0], received_at, previous[2] + 1)

    def ready(self, at: datetime) -> bool:
        engine = self.engine
        if (not self.connected or not self.warmed or self.restart_needed or engine.state["quarantine"]
                or not engine.reconciled or engine.snapshot_at is None or self.connected_at is None
                or engine.snapshot_at < self.connected_at
                or not 0 <= (at - engine.snapshot_at).total_seconds() <= 15):
            return False
        if engine.market_stream_ready:
            return True
        for symbol in engine.instruments:
            sample = self.samples.get(symbol)
            if (sample is None or sample[2] < 3 or (sample[1] - sample[0]).total_seconds() < 3
                    or engine._fresh(symbol, at) is None
                    or not 0 <= (at - sample[1]).total_seconds() <= engine.config.market.max_quote_age_seconds):
                return False
        halt = engine.state["halt"]
        if halt in RECOVERABLE_STREAM_HALTS:
            if not engine.flat:
                return False
            engine.state["halt"] = ""
            engine.state.pop("stream_fault", None)
            engine.store.audit(at, "STREAM_RECOVERED",
                               reason="Rebuilt indicator history, fresh full quotes and post-connect broker reconciliation.",
                               generation=self.generation, capital_and_orders_preserved=True)
            engine._save()
            engine._check_risk(at)
        engine.market_stream_ready = True
        self._status("healthy", "Broker stream verified with fresh quotes and reconciled ownership.", at)
        return True

    def may_restart_flat(self, at: datetime) -> bool:
        return bool(
            self.restart_needed and self.engine.flat and self.engine.reconciled
            and self.fault_at is not None and self.engine.snapshot_at is not None
            and self.engine.snapshot_at >= self.fault_at
            and 0 <= (at - self.engine.snapshot_at).total_seconds() <= 15
        )


def retry_delay(state: dict, at: datetime) -> int | None:
    fault = state.get("stream_fault") or {}
    if (state.get("halt") != STREAM_HALT or state.get("quarantine") or state.get("position")
            or any(order["status"] not in {"COMPLETE", "CANCELLED", "REJECTED"} for order in state.get("orders", []))
            or fault.get("auto_retry_allowed") is not True
            or not fault.get("at") or not 0 <= (at - timestamp(fault["at"])).total_seconds() <= RESTART_WINDOW_SECONDS):
        return None
    return int(fault["retry_delay_seconds"])
