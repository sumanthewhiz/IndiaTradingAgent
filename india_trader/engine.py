from __future__ import annotations

import uuid
from dataclasses import asdict, replace
from datetime import datetime, time, timedelta
from typing import Any

from .broker import Broker, BrokerError, BrokerRejected, KiteBroker
from .core import (
    IST, TERMINAL, Candidate, Config, Instrument, Order, Position, SafetyError, Session,
    Snapshot, Tick, bps, tick_ceil, tick_floor, timestamp,
)
from .market import Bar, SignalAgent, Tape
from .position_feed import PositionFeedHealth
from .storage import Store

CLOCK_HALT = "Exchange timestamp is in the future; check clock synchronization."
FUTURE_TOLERANCE_SECONDS = 1
MAX_TRACKED_STOCKS = 15


class TradingEngine:
    """Single writer. Signals/AI cannot bypass sizing or the execution state machine."""

    def __init__(
        self, config: Config, session: Session, instruments: dict[str, Instrument],
        broker: Broker, store: Store, mode: str, *, allow_daily_universe_change: bool = False,
        allow_signal_profile_upgrade: bool = False,
        enable_discovery: bool = False,
    ):
        config.validate()
        if mode not in {"paper", "shadow", "live"}:
            raise ValueError("Unknown engine mode.")
        self.config, self.session, self.instruments = config, session, instruments
        self.broker, self.store, self.mode = broker, store, mode
        self.signals = SignalAgent(config, instruments)
        self.discovery_enabled = enable_discovery
        self.trade_symbols = self.signals.trade_symbols
        self.discovery_universe: dict[str, int] = {}
        self.discovery_excluded: set[str] = set()
        if isinstance(broker, KiteBroker):
            broker.trade_symbols = self.trade_symbols
        self.quotes: dict[str, Tick] = {}
        self.quote_receipts: dict[str, str] = {}
        self.exit_quotes: dict[str, Tick] = {}
        self.snapshot_at: datetime | None = None
        self.broker_cash = 0
        self.reconciled = False
        self.broker_reads_ready = True  # Offline/paper callers reconcile synchronously.
        self.reconciliation_status: dict = {}
        self.market_stream_ready = True  # Offline replay has no transport; connected runtime owns this gate.
        self.stream_status: dict = {}
        self.news_heartbeats: dict[str, datetime] = {}
        self.mismatch_count = 0
        self.last_reject: tuple[str, str] | None = None
        self.last_rejection_reason = ""
        self.next_day = False
        self.state: dict[str, Any] = store.get("engine") or {
            "version": 1, "mode": mode, "config_hash": config.fingerprint,
            "day": session.day.isoformat(), "initialized": False,
            "capital": config.risk.capital_rupees * 100,
            "cash": config.risk.capital_rupees * 100,
            "day_start": config.risk.capital_rupees * 100,
            "day_peak": config.risk.capital_rupees * 100,
            "trades": 0, "consecutive_losses": 0, "buy_turnover": 0,
            "halt": "", "quarantine": False, "orders": [], "position": None, "pauses": {},
            "last_exit": {}, "completed_trades": 0,
        }
        self.state.setdefault("day_open_equity", self.state["day_start"])
        if self.state["version"] != 1 or self.state["mode"] != mode:
            raise SafetyError("State format/mode mismatch; never share paper/live databases.")
        if self.state["config_hash"] != config.fingerprint:
            saved_configuration = store.get("configuration")
            previous_config = Config.from_mapping(saved_configuration) if saved_configuration else None
            previous = asdict(previous_config) if previous_config else None
            current = asdict(config)
            eligible_upgrade = False
            if (allow_signal_profile_upgrade and previous is not None
                    and self.state["position"] is None and not self.state["quarantine"]
                    and all(x["status"] in TERMINAL for x in self.state["orders"])
                    and previous["strategy"]["enabled"] == ["orb", "vwap_pullback"]
                    and previous["strategy"]["benchmark_alignment"] == "absolute"
                    and current["strategy"]["enabled"] == ["orb", "vwap_pullback", "momentum_breakout"]
                    and current["strategy"]["benchmark_alignment"] == "relative_strength"):
                upgraded = asdict(previous_config)
                upgraded["strategy"]["enabled"] = list(current["strategy"]["enabled"])
                upgraded["strategy"]["benchmark_alignment"] = "relative_strength"
                eligible_upgrade = upgraded == current
                previous = upgraded
            if previous:
                previous["market"]["symbols"] = []
            comparable = asdict(config)
            comparable["market"]["symbols"] = []
            schema_only = previous_config == config
            if (previous and previous["market"]["entry_start"] == "09:35"
                    and previous["strategy"]["opening_range_minutes"] == 15
                    and comparable["market"]["entry_start"] == "09:25"
                    and comparable["strategy"]["opening_range_minutes"] == 5):
                previous["market"]["entry_start"] = "09:25"
                previous["strategy"]["opening_range_minutes"] = 5
            can_rotate = (
                allow_daily_universe_change and previous == comparable
                and self.state["day"] != session.day.isoformat()
                and self.state["position"] is None and not self.state["quarantine"]
                and all(x["status"] in TERMINAL for x in self.state["orders"])
            )
            if not schema_only and not can_rotate and not eligible_upgrade:
                raise SafetyError("Configuration changed for this ledger. Reconcile and review before migration.")
            self.state["config_hash"] = config.fingerprint
            store.audit(datetime.combine(session.day, time(0), IST),
                        "CONFIG_SCHEMA_NORMALIZED" if schema_only else
                        "SIGNAL_PROFILE_UPGRADED" if eligible_upgrade else "UNIVERSE_ROTATED",
                        symbols=current["market"]["symbols"])
        store.put("configuration", asdict(config))
        self.orders = [Order(**x) for x in self.state["orders"]]
        self.position = Position(**self.state["position"]) if self.state["position"] else None
        self.position_feed = PositionFeedHealth(self)
        self.discovery_members = store.get("discovery_members") or {"active": {}, "admitted": {}}
        restored = set(instruments) - set(config.market.symbols) - {config.market.benchmark}
        if restored:
            if not enable_discovery or not restored <= set(self.discovery_members["admitted"]):
                raise SafetyError("Unrecognized dynamic symbols in startup state; reconcile ownership before trading.")
            self.trade_symbols.update(restored)
        if enable_discovery:
            if len(self.trade_symbols) > MAX_TRACKED_STOCKS:
                raise SafetyError("Stored discovery membership exceeds the tracking limit.")
            self.discovery_members["active"] = {
                s: entry for s, entry in self.discovery_members["active"].items() if s in restored
            }
            store.put("discovery_members", self.discovery_members)
        self.clock_recovery_pending = self.state["halt"] == CLOCK_HALT and self.flat
        self.clock_samples: dict[str, tuple[datetime, datetime, int]] = {}
        self.next_day = self.state["day"] != session.day.isoformat()
        if self.next_day and (self.position is not None or any(x.active for x in self.orders)):
            self.state["halt"] = "Carryover exposure: recovery only, no new entries."
        self._save()

    def _save(self) -> None:
        self.state["orders"] = [asdict(x) for x in self.orders]
        self.state["position"] = asdict(self.position) if self.position else None
        self.store.put("engine", self.state)

    @property
    def flat(self) -> bool:
        return self.position is None and not any(x.active for x in self.orders)

    def owned_symbols(self) -> set[str]:
        return ({self.position.symbol} if self.position else set()) | {
            order.symbol for order in self.orders if order.active
        }

    def permits_entry(self, symbol: str, at: datetime) -> bool:
        if symbol not in self.trade_symbols:
            return False
        return replace(self.session, symbols=sorted(self.trade_symbols)).permits(symbol, at)

    def admit_discovered(self, instrument: Instrument, bars: list[Bar], at: datetime, reason: str) -> bool:
        symbol = instrument.symbol
        if (not self.discovery_enabled or self.state["halt"] or self.state["quarantine"]
                or not self.reconciled or at.date() != self.session.day
                or not time(9, 20) <= at.time() < time.fromisoformat(self.config.market.entry_end)):
            return False
        if symbol in self.trade_symbols:
            return False
        if (len(self.trade_symbols) >= MAX_TRACKED_STOCKS or symbol in self.discovery_excluded
                or instrument.reference or self.discovery_universe.get(symbol) != instrument.token
                or not 0 < instrument.lower < instrument.upper or instrument.tick <= 0
                or any(item.token == instrument.token for item in self.instruments.values())):
            return False
        if any(timestamp(self.state["pauses"].get(key, "2000-01-01T00:00:00+05:30")) > at
               for key in ("*", symbol)):
            return False
        tape = Tape()
        tape.seed(bars, at)
        entry = {"instrument": asdict(instrument), "day": self.session.day.isoformat(),
                 "admitted_at": at.isoformat(), "reason": reason}
        self.discovery_members["admitted"][symbol] = entry
        self.discovery_members["active"][symbol] = entry
        # Admission is durable before a subscription can produce a signal/order.
        self.store.put("discovery_members", self.discovery_members)
        self.instruments[symbol] = instrument
        self.signals.tapes[symbol] = tape
        self.trade_symbols.add(symbol)
        self.store.audit(at, "DISCOVERY_ADMITTED", symbol=symbol, reason=reason)
        return True

    def retire_discovered(self, symbol: str, at: datetime, reason: str) -> bool:
        if (not self.discovery_enabled or symbol in self.config.market.symbols
                or symbol in self.owned_symbols() or symbol not in self.trade_symbols):
            return False
        self.trade_symbols.discard(symbol)
        self.instruments.pop(symbol, None)
        self.signals.tapes.pop(symbol, None)
        self.quotes.pop(symbol, None)
        self.quote_receipts.pop(symbol, None)
        self.exit_quotes.pop(symbol, None)
        self.clock_samples.pop(symbol, None)
        self.discovery_members["active"].pop(symbol, None)
        self.store.put("discovery_members", self.discovery_members)
        self.store.audit(at, "DISCOVERY_RETIRED", symbol=symbol, reason=reason)
        return True

    def halt(self, reason: str, at: datetime, liquidate: bool = True) -> None:
        changed = False
        if not self.state["halt"]:
            self.state["halt"] = reason
            self.store.audit(at, "HALT", reason=reason)
            changed = True
        if liquidate and self.position:
            if not self.position.exit_reason:
                self.position.exit_reason = "HALT: " + reason
                changed = True
        if changed:
            self._save()

    def quarantine(self, reason: str, at: datetime) -> None:
        self.state["quarantine"] = True
        self.halt(reason, at, liquidate=False)
        self._save()

    def pause(self, symbols: list[str], until: datetime, at: datetime, reason: str) -> None:
        for symbol in symbols:
            old = self.state["pauses"].get(symbol)
            if old is None or timestamp(old) < until:
                self.state["pauses"][symbol] = until.isoformat()
        if self.position and ("*" in symbols or self.position.symbol in symbols):
            self.position.exit_reason = self.position.exit_reason or "event_pause"
        self.store.audit(at, "PAUSE", symbols=symbols, until=until.isoformat(), reason=reason)
        self._save()

    def heartbeat_news(self, at: datetime, source: str = "licensed-wire") -> None:
        prior = self.news_heartbeats.get(source)
        if prior is None or at > prior:
            self.news_heartbeats[source] = at

    def _fresh(self, symbol: str, at: datetime) -> Tick | None:
        quote = self.quotes.get(symbol)
        if quote is None:
            return None
        age = (at - quote.at).total_seconds()
        return quote if -FUTURE_TOLERANCE_SECONDS <= age <= self.config.market.max_quote_age_seconds else None

    def position_quote(self, symbol: str, at: datetime) -> Tick | None:
        candidates = [
            quote for quote in (self.quotes.get(symbol), self.exit_quotes.get(symbol))
            if quote is not None and quote.at.date() == self.session.day
            and -FUTURE_TOLERANCE_SECONDS <= (at-quote.at).total_seconds() <= self.config.market.max_quote_age_seconds
        ]
        return max(candidates, key=lambda quote: quote.at) if candidates else None

    def position_quote_source(self, symbol: str, at: datetime) -> str:
        quote = self.position_quote(symbol, at)
        return "broker_readonly_quote" if quote is not None and quote is self.exit_quotes.get(symbol) else "stream"

    def _reject(self, candidate: Candidate, reason: str, at: datetime) -> None:
        self.last_rejection_reason = reason
        key = (candidate.symbol, reason)
        if key != self.last_reject:
            self.store.audit(at, "CANDIDATE_REJECTED", symbol=candidate.symbol,
                             setup=candidate.setup, reason=reason)
            self.last_reject = key

    def observe_stream_quote(self, tick: Tick, received_at: datetime, at: datetime) -> bool:
        try:
            tick.validate()
        except ValueError as exc:
            self.halt(str(exc), at)
            return False
        if tick.symbol not in self.instruments:
            self.halt("Tick outside the configured/admitted instrument set.", at)
            return False
        if tick.at.date() != self.session.day:
            self.halt("Tick is outside authorized session date.", at)
            return False
        ahead = (tick.at - received_at).total_seconds()
        if ahead > FUTURE_TOLERANCE_SECONDS:
            self.clock_samples.clear()
            if self.state["halt"] != CLOCK_HALT:
                self.state["clock_fault"] = {
                    "symbol": tick.symbol, "exchange_at": tick.at.isoformat(),
                    "received_at": received_at.isoformat(), "ahead_seconds": round(ahead, 6),
                    "tolerance_seconds": FUTURE_TOLERANCE_SECONDS,
                }
                self.store.audit(at, "CLOCK_SKEW", **self.state["clock_fault"])
            # Do not let a rejected future quote contaminate candles, VWAP or exit prices.
            self.halt(CLOCK_HALT, at)
            return False
        prior = self.quotes.get(tick.symbol)
        prior_received = self.quote_receipts.get(tick.symbol)
        if (prior is None or tick.at > prior.at
                or (tick.at == prior.at and (prior_received is None
                                            or received_at >= timestamp(prior_received)))):
            self.quotes[tick.symbol] = tick
            self.quote_receipts[tick.symbol] = received_at.isoformat()
        return True

    def on_tick(self, tick: Tick, received_at: datetime, processed_at: datetime | None = None, *,
                evaluate_signals: bool = True) -> None:
        at = processed_at or received_at
        if not self.observe_stream_quote(tick, received_at, at):
            return
        try:
            if evaluate_signals:
                candidate = self.signals.ingest(tick)
            else:
                candidate = None
                self.signals.decision = None
        except (ValueError, SafetyError) as exc:
            self.halt(str(exc), at)
            return
        if self.clock_recovery_pending:
            previous = self.clock_samples.get(tick.symbol)
            if previous and received_at > previous[1]:
                first, _, count = previous
                self.clock_samples[tick.symbol] = (first, received_at, count + 1)
            elif previous is None:
                self.clock_samples[tick.symbol] = (received_at, received_at, 1)
            self._recover_clock_halt(at)
        self._check_risk(at)
        self.position_feed.check(at)
        if candidate:
            attempted = self.consider(candidate, at)
            if self.signals.decision:
                self.signals.decision["entry_attempted"] = attempted
                if attempted:
                    self.signals.decision["reason"] = (
                        f"Entry attempted via {candidate.setup}; broker status is tracked separately."
                    )
                else:
                    self.signals.decision["reason"] = "Risk gate: " + self.last_rejection_reason
        if self.signals.decision:
            decision = self.signals.decision
            self.store.audit(at, "SIGNAL_EVALUATED", **decision)
            diagnostics = self.store.get("signal_diagnostics") or {}
            if diagnostics.get("day") != self.session.day.isoformat():
                diagnostics = {"day": self.session.day.isoformat(), "symbols": {}}
            diagnostics["symbols"][tick.symbol] = decision
            self.store.put("signal_diagnostics", diagnostics)
        self._drive(at)

    def _recover_clock_halt(self, at: datetime) -> None:
        if (not self.clock_recovery_pending or self.state["halt"] != CLOCK_HALT
                or not self.flat or self.state["quarantine"] or not self.reconciled
                or self.snapshot_at is None or not 0 <= (at - self.snapshot_at).total_seconds() <= 15
                or at.date() != self.session.day):
            return
        for symbol in self.instruments:
            sample = self.clock_samples.get(symbol)
            if (sample is None or sample[2] < 3 or (sample[1] - sample[0]).total_seconds() < 3
                    or self._fresh(symbol, at) is None
                    or not 0 <= (at - sample[1]).total_seconds() <= self.config.market.max_quote_age_seconds):
                return
        self.state["halt"] = ""
        self.clock_recovery_pending = False
        self.store.audit(at, "CLOCK_RECOVERED", evidence="fresh receipt-time-validated ticks for every subscribed instrument",
                         symbols=list(self.instruments), cash_allocation_unchanged=True)
        self.state.pop("clock_fault", None)
        self._save()
        self._check_risk(at)

    def consider(self, candidate: Candidate, at: datetime) -> bool:
        cfg, risk = self.config, self.config.risk
        deny = lambda reason: self._reject(candidate, reason, at)
        quote = self._fresh(candidate.symbol, at)
        if (self.state["halt"] or not self.state["initialized"] or not self.reconciled
                or not self.broker_reads_ready or not self.position_feed_ready):
            deny("ledger/halt/reconciliation gate")
            return False
        if not self.market_stream_ready:
            deny("broker stream validation/recovery pending")
            return False
        if self.position is not None or any(x.active for x in self.orders):
            deny("one-position/in-flight-order limit")
            return False
        if candidate.setup not in cfg.strategy.enabled or candidate.symbol not in self.trade_symbols:
            deny("setup or symbol not enabled")
            return False
        if not self.permits_entry(candidate.symbol, at):
            deny("calendar/issuer review/blackout gate")
            return False
        if candidate.symbol in self.discovery_excluded:
            deny("issuer-event discovery exclusion")
            return False
        if not time.fromisoformat(cfg.market.entry_start) <= at.time() < time.fromisoformat(cfg.market.entry_end):
            deny("outside entry window")
            return False
        if (quote is None or not 0 <= (at - candidate.at).total_seconds() <= cfg.market.max_quote_age_seconds
                or self._fresh(cfg.market.benchmark, at) is None):
            deny("stale quote, benchmark or signal")
            return False
        if self.snapshot_at is None or (at - self.snapshot_at).total_seconds() > 15:
            deny("stale broker reconciliation")
            return False
        if any(
            source not in self.news_heartbeats
            or not 0 <= (at - self.news_heartbeats[source]).total_seconds()
            <= cfg.news.heartbeat_max_seconds
            for source in cfg.news.required_sources
        ):
            deny("no fresh news collector heartbeat")
            return False
        if any(timestamp(self.state["pauses"].get(key, "2000-01-01T00:00:00+05:30")) > at
               for key in ("*", candidate.symbol)):
            deny("event pause")
            return False
        last_exit = self.state["last_exit"].get(candidate.symbol)
        if last_exit and (at - timestamp(last_exit)).total_seconds() < cfg.strategy.cooldown_minutes * 60:
            deny("symbol cooldown")
            return False
        if self.state["trades"] >= risk.max_trades or self.state["consecutive_losses"] >= risk.max_consecutive_losses:
            deny("daily trade/loss count")
            return False
        instrument = self.instruments[candidate.symbol]
        if instrument.reference or quote.ask - quote.bid > bps(quote.bid, cfg.market.max_spread_bps):
            deny("reference-only instrument or spread too wide")
            return False
        if quote.ask >= instrument.upper or quote.bid <= instrument.lower:
            deny("at a circuit band")
            return False
        entry = tick_ceil(quote.ask + bps(quote.ask, cfg.execution.entry_slippage_bps), instrument.tick)
        stop = tick_floor(candidate.stop, instrument.tick)
        stop_limit = max(instrument.lower, tick_floor(
            stop - bps(stop, cfg.execution.protection_gap_bps), instrument.tick
        ))
        distance = entry - stop
        if (not instrument.lower < stop < entry < instrument.upper
                or not bps(entry, cfg.strategy.min_stop_bps)
                <= distance <= bps(entry, cfg.strategy.max_stop_bps)):
            deny("invalid structural stop or entry band")
            return False
        target = tick_floor(entry + int(distance * cfg.strategy.reward_r), instrument.tick)
        if target >= instrument.upper:
            deny("target at/beyond upper circuit")
            return False
        capital = min(self.state["capital"], self.state["cash"])
        buffer = bps(self.state["capital"], risk.cash_buffer_bps)
        spendable = max(0, min(capital - buffer, self.broker_cash - buffer,
                               bps(self.state["capital"], risk.max_position_bps)))
        turnover_left = max(0, int(self.state["capital"] * risk.max_daily_buy_turnover_multiple)
                            - self.state["buy_turnover"])
        max_qty = min(spendable // entry, turnover_left // entry,
                      quote.ask_size * cfg.market.max_depth_participation_bps // 10000,
                      quote.bid_size * cfg.market.max_depth_participation_bps // 10000)
        equity = self.marked_equity(at)
        daily_left = max(0, bps(self.state["day_start"], risk.daily_loss_bps)
                         - max(0, self.state["day_peak"] - equity))
        risk_budget = min(bps(self.state["day_start"], risk.risk_per_trade_bps), daily_left)

        def planned_loss(quantity: int) -> int:
            return quantity * (entry - stop_limit) + cfg.costs.fee("BUY", entry * quantity) + (
                cfg.costs.fee("SELL", stop_limit * quantity)
            )

        low, high = 0, max_qty
        while low < high:
            mid = (low + high + 1) // 2
            if (planned_loss(mid) <= risk_budget
                    and entry * mid + cfg.costs.fee("BUY", entry * mid) <= spendable):
                low = mid
            else:
                high = mid - 1
        quantity = low
        if quantity < 1:
            deny("minimum share fails funded-capital/liquidity/cost-aware risk budget")
            return False
        loss = planned_loss(quantity)
        costs = cfg.costs.fee("BUY", entry * quantity) + cfg.costs.fee("SELL", target * quantity)
        profit = quantity * (target - entry) - costs
        if (profit < loss * cfg.strategy.min_net_reward_r
                or profit < costs * cfg.strategy.min_profit_cost_multiple):
            deny("expected target does not clear net reward/cost hurdle")
            return False
        self.position = Position(
            candidate.symbol, candidate.setup, at.isoformat(), stop, target,
            trade_id=uuid.uuid4().hex, initial_risk=loss
        )
        self.state["trades"] += 1
        self.store.audit(at, "ENTRY_PLAN", symbol=candidate.symbol, setup=candidate.setup,
                         quantity=quantity, limit=entry, stop=stop, stop_limit=stop_limit,
                         target=target, modeled_risk=loss, modeled_target_net=profit)
        self._submit(candidate.symbol, "entry", "BUY", quantity, entry, 0, at)
        return True

    def _submit(
        self, symbol: str, purpose: str, side: str, quantity: int,
        price: int, trigger: int, at: datetime,
    ) -> None:
        if quantity <= 0:
            raise SafetyError("Non-positive order quantity.")
        tag = uuid.uuid4().hex[:8]
        while any(x.tag == tag for x in self.orders):
            tag = uuid.uuid4().hex[:8]
        order = Order(tag, symbol, purpose, side, quantity, price, trigger, at.isoformat())
        self.orders.append(order)
        # Intent/reservation is durable BEFORE any network side effect.
        self._save()
        try:
            order.order_id = self.broker.submit(order)
            order.status = "ACKNOWLEDGED"
        except BrokerRejected as exc:
            order.status = "REJECTED"
            self.halt(f"{purpose} rejected: {exc}", at)
        except BrokerError as exc:
            order.status = "UNKNOWN"
            self.halt(f"{purpose} submission uncertain: {exc}", at)
        self.store.audit(at, "ORDER_INTENT", tag=tag, purpose=purpose, side=side,
                         symbol=symbol, quantity=quantity, status=order.status)
        self.reconciled = False
        self._save()

    def _cancel(self, order: Order, at: datetime) -> None:
        if not order.active or order.cancel_requested or not order.order_id:
            return
        order.cancel_requested = True
        self._save()
        try:
            self.broker.cancel(order.order_id)
        except BrokerError as exc:
            if isinstance(exc, BrokerRejected) and exc.session_expired:
                order.cancel_requested = False
            self.halt(f"Cancellation uncertain: {exc}", at)
        self.store.audit(at, "CANCEL_REQUEST", tag=order.tag, order_id=order.order_id)
        self.reconciled = False
        # An acknowledgment is NOT proof of cancellation; snapshot confirmation is required.
        self._save()

    def reconcile(self, snapshot: Snapshot, at: datetime) -> None:
        known = {order.tag for order in self.orders}
        self.reconciled = False
        if any(order.active and timestamp(order.created) > snapshot.at for order in self.orders):
            self.store.audit(at, "STALE_SNAPSHOT_IGNORED", snapshot_started=snapshot.at.isoformat())
            return
        if snapshot.foreign_activity or any(
            record.tag not in known and (record.filled > 0 or record.status not in TERMINAL)
            for record in snapshot.orders
        ):
            self.quarantine("Unowned order/product activity: account must be exclusive to this engine.", at)
            return
        records: dict[str, list] = {}
        for record in snapshot.orders:
            records.setdefault(record.tag, []).append(record)
        # Apply buys before sells so one snapshot containing both sides cannot fabricate a short.
        for order in sorted(self.orders, key=lambda x: x.side != "BUY"):
            matches = records.get(order.tag, [])
            if len(matches) > 1:
                self.quarantine("Duplicate broker tag: do not infer idempotency.", at)
                return
            if not matches:
                if order.active:
                    if (order.order_id and order.status == "ACKNOWLEDGED"
                            and (at - timestamp(order.created)).total_seconds() < 15):
                        self.store.audit(at, "ORDER_AWAITING_VISIBILITY", tag=order.tag)
                        return
                    self.halt("Unresolved order intent; no automatic resubmission.", at)
                    self._save()
                    return
                continue
            record = matches[0]
            if (record.symbol != order.symbol or record.side != order.side
                    or record.quantity != order.quantity or record.price != order.price
                    or record.trigger != order.trigger or record.product != "CNC"
                    or record.exchange != "NSE"
                    or (order.order_id and record.order_id != order.order_id)
                    or not order.filled <= record.filled <= order.quantity):
                self.quarantine("Broker order changed unexpectedly; manual reconciliation required.", at)
                return
            total_value = record.value or record.average * record.filled
            if record.filled and (record.average <= 0 or total_value < order.value):
                self.quarantine("Invalid broker cumulative fill value.", at)
                return
            delta_quantity, delta_value = record.filled - order.filled, total_value - order.value
            fee = self.config.costs.fee(order.side, total_value)
            delta_fee = fee - order.fees
            if delta_quantity or delta_fee or delta_value:
                pos = self.position
                if pos is None or pos.symbol != order.symbol:
                    self.quarantine("Unexpected fill without an owned position.", at)
                    return
                if order.side == "BUY":
                    pos.quantity += delta_quantity
                    pos.bought += delta_quantity
                    pos.buy_value += delta_value
                    self.state["cash"] -= delta_value + delta_fee
                    self.state["buy_turnover"] += delta_value
                else:
                    if delta_quantity > pos.quantity:
                        self.quarantine("Sell exceeds owned shares; emergency manual reconciliation.", at)
                        return
                    pos.quantity -= delta_quantity
                    pos.sell_value += delta_value
                    self.state["cash"] += delta_value - delta_fee
                    if order.purpose == "protect":
                        pos.exit_reason = pos.exit_reason or "protective_stop"
                pos.fees += delta_fee
                self.store.audit(at, "FILL_DELTA", tag=order.tag, purpose=order.purpose,
                                 side=order.side, symbol=order.symbol, quantity=delta_quantity,
                                 value=delta_value, modeled_fees=delta_fee)
            changed = (order.filled, order.value, order.fees, order.status, order.order_id) != (
                record.filled, total_value, fee, record.status, record.order_id
            )
            order.filled, order.value, order.fees = record.filled, total_value, fee
            order.status, order.order_id = record.status, record.order_id
            if changed:
                self._save()
        expected = ({self.position.symbol: self.position.quantity}
                    if self.position and self.position.quantity else {})
        actual = {symbol: quantity for symbol, quantity in snapshot.positions.items() if quantity}
        if actual != expected:
            self.mismatch_count += 1
            self.store.audit(at, "RECONCILIATION_MISMATCH", expected=expected, actual=actual)
            if self.mismatch_count >= 2 or any(q < 0 for q in actual.values()):
                self.quarantine("Position mismatch: sequential API snapshots or external activity.", at)
            self._save()
            return
        self.mismatch_count = 0
        self.broker_cash, self.snapshot_at = snapshot.cash_available, snapshot.at
        self.reconciled = True
        if not self.state["initialized"]:
            if expected or any(x.active for x in self.orders):
                self.halt("Cold start requires an empty account/ledger.", at, liquidate=False)
                return
            cap = min(self.state["capital"], snapshot.cash_available)
            if self.session.capital_rupees:
                cap = min(cap, self.session.capital_rupees * 100)
            self.state.update(capital=cap, cash=cap, day_start=cap, day_peak=cap,
                              day_open_equity=cap, initialized=True)
            if cap <= 0:
                self.halt("No usable cash. Funding is never attempted.", at)
        if self.next_day and self.flat and not self.state["quarantine"]:
            self.state.update(
                day=self.session.day.isoformat(), day_start=min(self.state["cash"], self.state["capital"]),
                day_peak=min(self.state["cash"], self.state["capital"]),
                day_open_equity=self.state["cash"],
                trades=0, consecutive_losses=0, buy_turnover=0, halt="", pauses={},
            )
            self.next_day = False
            self.store.audit(at, "NEW_SESSION", frozen_capital=self.state["capital"],
                             allocated_cash=self.state["cash"])
        if self.position and not self.position.quantity and not any(x.active for x in self.orders):
            pos = self.position
            if pos.bought:
                net = pos.sell_value - pos.buy_value - pos.fees
                self.state["consecutive_losses"] = (
                    self.state["consecutive_losses"] + 1 if net <= 0 else 0
                )
                self.state["last_exit"][pos.symbol] = at.isoformat()
                self.state["completed_trades"] += 1
                self.store.audit_once("trade:" + pos.trade_id, at, "TRADE_CLOSED",
                                 symbol=pos.symbol, setup=pos.setup, trade_id=pos.trade_id,
                                 bought=pos.bought, net_paise=net, fees_paise=pos.fees,
                                 buy_value_paise=pos.buy_value, sell_value_paise=pos.sell_value,
                                 initial_risk_paise=pos.initial_risk,
                                 exit_reason=pos.exit_reason or "closed")
            self.exit_quotes.pop(pos.symbol, None)
            self.position = None
        self._save()
        self._check_risk(at)
        self.position_feed.check(at)
        self._drive(at)

    def marked_equity(self, at: datetime) -> int:
        pos = self.position
        if pos is None or not pos.quantity:
            return self.state["cash"]
        candidates = [quote for quote in (self.quotes.get(pos.symbol), self.exit_quotes.get(pos.symbol))
                      if quote is not None]
        quote = max(candidates, key=lambda value: value.at) if candidates else None
        if quote is None:
            mark = max(0, pos.stop - bps(pos.stop, self.config.execution.protection_gap_bps))
        else:
            mark = quote.bid
        proceeds = pos.quantity * mark
        return self.state["cash"] + proceeds - self.config.costs.fee("SELL", proceeds)

    def _check_risk(self, at: datetime) -> None:
        if not self.state["initialized"]:
            return
        equity = self.marked_equity(at)
        self.state["day_peak"] = max(self.state["day_peak"], equity)
        loss = max(self.state["day_start"] - equity, self.state["day_peak"] - equity)
        if loss >= bps(self.state["day_start"], self.config.risk.daily_loss_bps):
            self.halt("Daily loss/profit-giveback threshold reached.", at)
        if self.state["consecutive_losses"] >= self.config.risk.max_consecutive_losses:
            self.halt("Consecutive losing trades limit reached.", at)
        pos = self.position
        if pos and pos.quantity:
            quote = self.position_quote(pos.symbol, at)
            if quote:
                if quote.bid <= pos.stop:
                    pos.exit_reason = pos.exit_reason or "stop"
                elif quote.bid >= pos.target:
                    pos.exit_reason = pos.exit_reason or "take_profit"
            if (at - timestamp(pos.opened)).total_seconds() >= self.config.strategy.max_hold_minutes * 60:
                pos.exit_reason = pos.exit_reason or "time_stop"
            if at.time() >= time.fromisoformat(self.config.market.flatten_at):
                pos.exit_reason = pos.exit_reason or "scheduled_flatten"

    def timer(self, at: datetime, kill: bool = False) -> None:
        if kill:
            self.halt("Operator kill switch.", at)
        if at.date() != self.session.day:
            self.halt("Session date expired.", at)
        self._check_risk(at)
        self.position_feed.check(at)
        self._drive(at)

    def _drive(self, at: datetime) -> None:
        pos = self.position
        if pos is None or self.state["quarantine"] or at.time() < time(9, 15):
            return
        ownership_confirmed = (self.reconciled and self.snapshot_at is not None
                               and 0 <= (at - self.snapshot_at).total_seconds() <= 15)
        cfg = self.config.execution
        entry_orders = [x for x in self.orders if x.active and x.purpose == "entry"]
        for order in entry_orders:
            age = (at - timestamp(order.created)).total_seconds()
            if (self.state["halt"] or pos.exit_reason or order.filled
                    or age >= cfg.entry_ttl_seconds or at.time() >= time.fromisoformat(
                        self.config.market.entry_end)):
                self._cancel(order, at)
        if not self.broker_reads_ready:
            # Keep accepted stop/exit orders working until current ownership is verified.
            return
        if any(x.active and x.status in {"SUBMITTING", "UNKNOWN"} for x in self.orders):
            return
        if at.time() >= time.fromisoformat(self.config.market.close_at):
            if pos.quantity or any(x.active for x in self.orders):
                self.halt("Market closed with unresolved exposure; manual broker action required.", at,
                          liquidate=False)
            return
        quote = self.position_quote(pos.symbol, at)
        instrument = self.instruments[pos.symbol]
        guards = [x for x in self.orders if x.active and x.purpose == "protect"]
        exits = [x for x in self.orders if x.active and x.purpose == "exit"]
        if not pos.quantity:
            for order in guards:
                self._cancel(order, at)
            return
        if not pos.exit_reason:
            uncovered = pos.quantity - sum(x.remaining for x in guards + exits)
            if uncovered < 0:
                self.quarantine("Protective sell quantity exceeds ownership.", at)
                return
            if uncovered > 0:
                if not ownership_confirmed:
                    return
                stop_limit = max(instrument.lower, tick_floor(
                    pos.stop - bps(pos.stop, cfg.protection_gap_bps), instrument.tick
                ))
                self._submit(pos.symbol, "protect", "SELL", uncovered, stop_limit, pos.stop, at)
            return
        # Never cancel a native stop using a stale quote, and never create overlapping sells.
        if quote is None:
            return
        if entry_orders:
            uncovered = pos.quantity - sum(x.remaining for x in guards + exits)
            if uncovered > 0 and ownership_confirmed and quote.bid > pos.stop:
                stop_limit = max(instrument.lower, tick_floor(
                    pos.stop - bps(pos.stop, cfg.protection_gap_bps), instrument.tick
                ))
                self._submit(pos.symbol, "protect", "SELL", uncovered, stop_limit, pos.stop, at)
            return
        if not ownership_confirmed:
            return
        for order in guards:
            self._cancel(order, at)
        if guards or entry_orders:
            return
        if exits:
            order = exits[0]
            if ((at - timestamp(order.created)).total_seconds() >= cfg.exit_ttl_seconds
                    and pos.exit_reprices < cfg.max_exit_reprices):
                self._cancel(order, at)
            elif pos.exit_reprices >= cfg.max_exit_reprices:
                self.halt("Exit reprice limit reached; final bounded limit remains working.", at,
                          liquidate=False)
            return
        if pos.exit_reprices >= cfg.max_exit_reprices:
            self.quarantine("Bounded exit attempts exhausted with no working exit; contact broker.", at)
            return
        limit = max(instrument.lower, tick_floor(
            quote.bid - bps(quote.bid, cfg.exit_slippage_bps), instrument.tick
        ))
        pos.exit_reprices += 1
        self._submit(pos.symbol, "exit", "SELL", pos.quantity, limit, 0, at)

    def summary(self, at: datetime) -> dict[str, Any]:
        return {
            "mode": self.mode, "session": self.state["day"],
            "halt": self.state["halt"], "flat": self.flat,
            "quarantine": self.state["quarantine"],
            "reconciled": self.reconciled, "capital_paise": self.state["capital"],
            "allocated_cash_paise": self.state["cash"],
            "marked_equity_paise": self.marked_equity(at),
            "day_open_equity_paise": self.state["day_open_equity"],
            "entry_attempts": self.state["trades"],
            "position": asdict(self.position) if self.position else None,
            "active_orders": [asdict(x) for x in self.orders if x.active],
            "completed_trades": self.state["completed_trades"],
        }
