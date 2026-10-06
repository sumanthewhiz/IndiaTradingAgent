from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.broker import BrokerError, BrokerReadUnavailable, PaperBroker
from india_trader.core import Candidate, Config, IST, Instrument, SafetyError, Session, Tick
from india_trader.engine import CLOCK_HALT, TradingEngine
from india_trader.position_feed import (
    LatestQuoteBuffer, POSITION_FEED_HALT, PositionQuote, fetch_position_quote,
)
from india_trader.reconciliation import ReconciliationHealth
from india_trader.storage import Store

AT = datetime(2026, 9, 29, 11, 0, tzinfo=IST)


class PositionFeedTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / "state.db")
        self.config = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
        self.instruments = {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                            "INDEX": Instrument("INDEX", 2, 1, 1, 10**12, True)}
        self.session = Session(AT.date(), True, True, ["DEMO"], [])
        self.broker = PaperBroker(2500000, self.config.costs)
        self.engine = TradingEngine(self.config, self.session, self.instruments,
                                    self.broker, self.store, "paper")
        self.engine.reconcile(self.broker.snapshot(AT), AT)
        self.engine.observe_stream_quote(self.tick(AT), AT, AT)
        self.engine.observe_stream_quote(Tick("INDEX", AT, 2500000,2500000,2500000,0,0,0), AT, AT)
        self.engine.heartbeat_news(AT)

    def tearDown(self):
        self.store.__exit__()
        self.temporary.cleanup()

    def tick(self, at, price=10065, volume=1000):
        return Tick("DEMO", at, price, price-1, price+1, volume, 10000, 10000)

    def open_position(self):
        self.assertTrue(self.engine.consider(Candidate("DEMO", "orb", AT, 9979), AT))
        tick = self.tick(AT+timedelta(seconds=1), price=10066, volume=2000)
        self.broker.on_tick(tick)
        self.engine.observe_stream_quote(tick, tick.at, tick.at)
        self.engine.reconcile(self.broker.snapshot(tick.at), tick.at)
        self.engine.reconcile(self.broker.snapshot(tick.at), tick.at)
        return next(order for order in self.engine.orders if order.purpose == "protect")

    def test_brief_stale_episode_keeps_native_protection_without_immediate_exit_or_permanent_halt(self):
        guard = self.open_position()
        self.engine.timer(AT+timedelta(seconds=5))
        self.assertFalse(self.engine.position_feed_ready)
        self.assertEqual(self.engine.state["halt"], "")
        self.assertEqual(self.engine.position.exit_reason, "")
        self.assertFalse(guard.cancel_requested)
        self.assertEqual(self.engine.position_feed_status["last_reconciled_stop_quantity"], guard.quantity)
        self.assertEqual(len(self.store.events("POSITION_FEED_STALE")), 1)
        fresh = self.tick(AT+timedelta(seconds=6), volume=2200)
        self.engine.on_tick(fresh, fresh.at)
        self.assertTrue(self.engine.position_feed_ready)
        self.assertEqual(self.engine.state["halt"], "")
        self.assertEqual(self.engine.position.exit_reason, "")
        self.assertFalse(guard.cancel_requested)
        self.assertEqual(len(self.store.events("POSITION_FEED_RECOVERED")), 1)

    def test_latest_received_quote_prevents_false_alarm_after_slow_broker_operation(self):
        guard = self.open_position()
        mailbox = LatestQuoteBuffer()
        current = self.tick(AT+timedelta(seconds=6), volume=2500)
        mailbox.offer(current, current.at, 1)
        for tick, receipt in mailbox.take(1):
            self.engine.observe_stream_quote(tick, receipt, current.at)
        # Older queued candles must not overwrite the quote already received by the socket thread.
        older = self.tick(AT+timedelta(seconds=2), volume=2100)
        self.engine.on_tick(older, older.at, current.at)
        self.engine.timer(current.at)
        self.assertEqual(self.engine.quotes["DEMO"].at, current.at)
        self.assertTrue(self.engine.position_feed_ready)
        self.assertEqual(self.engine.state["halt"], "")
        self.assertEqual(self.store.events("POSITION_FEED_STALE"), [])
        self.assertFalse(guard.cancel_requested)

    def test_old_quote_received_late_is_not_made_fresh(self):
        self.open_position()
        stale = self.tick(AT+timedelta(seconds=1), volume=2000)
        self.engine.observe_stream_quote(stale, AT+timedelta(seconds=8), AT+timedelta(seconds=8))
        self.engine.timer(AT+timedelta(seconds=8))
        self.assertFalse(self.engine.position_feed_ready)
        self.assertIsNone(self.engine.position_quote("DEMO", AT+timedelta(seconds=8)))
        self.assertEqual(self.engine.position_feed_status["quote_age_seconds"], 7)

    def test_older_queued_quote_in_the_same_exchange_second_cannot_overwrite_latest_price(self):
        self.open_position()
        exchange = AT+timedelta(seconds=2)
        receipt = exchange+timedelta(milliseconds=800)
        current = self.tick(exchange, price=10067, volume=2200)
        older = self.tick(exchange, price=10065, volume=2100)
        mailbox = LatestQuoteBuffer()
        mailbox.offer(current, receipt, 1)
        mailbox.offer(older, exchange+timedelta(milliseconds=100), 1)
        self.assertEqual(mailbox.take(1)[0][0], current)
        self.engine.observe_stream_quote(current, receipt, receipt)
        self.engine.on_tick(older, exchange+timedelta(milliseconds=100), receipt)
        self.assertEqual(self.engine.quotes["DEMO"], current)
        self.assertEqual(self.engine.quote_receipts["DEMO"], receipt.isoformat())
        self.assertEqual(self.engine.signals.tapes["DEMO"].latest, older)

    def test_sustained_outage_latches_exit_intent_but_does_not_cancel_a_native_stop_on_stale_data(self):
        guard = self.open_position()
        self.engine.timer(AT+timedelta(seconds=17))
        self.assertEqual(self.engine.state["halt"], POSITION_FEED_HALT)
        self.assertTrue(self.engine.position.exit_reason)
        self.assertFalse(guard.cancel_requested)
        self.assertFalse(any(order.purpose == "exit" for order in self.engine.orders))
        self.assertIsNotNone(self.engine.state.get("position_feed_fault"))

    def test_fresh_rest_quote_supports_position_risk_but_not_entry_signals_or_indicator_history(self):
        guard = self.open_position()
        self.engine.timer(AT+timedelta(seconds=5))
        at = AT+timedelta(seconds=6)
        fetched = PositionQuote(self.engine.position.trade_id, self.tick(at, volume=2300), at)
        last_stream = self.engine.quotes["DEMO"].at
        self.assertTrue(self.engine.position_feed.accept(fetched, at))
        self.assertTrue(self.engine.position_feed_ready)
        self.assertEqual(self.engine.position_quote_source("DEMO", at), "broker_readonly_quote")
        self.assertEqual(self.engine.quotes["DEMO"].at, last_stream)
        self.assertIsNone(self.engine.signals.tapes["DEMO"].latest)
        self.assertIsNone(self.engine._fresh("DEMO", at))
        self.assertFalse(guard.cancel_requested)
        self.assertEqual(self.store.events("SIGNAL_EVALUATED"), [])

    def test_rest_refresh_for_old_trade_is_discarded(self):
        self.open_position()
        at = AT+timedelta(seconds=6)
        fetched = PositionQuote("not-the-current-trade", self.tick(at), at)
        self.assertFalse(self.engine.position_feed.accept(fetched, at))
        self.assertEqual(self.engine.exit_quotes, {})
        self.assertEqual(len(self.store.events("POSITION_QUOTE_DISCARDED")), 1)

    def test_future_or_stale_rest_data_cannot_replace_exchange_time_with_receipt_time(self):
        self.open_position()
        at = AT+timedelta(seconds=8)
        for exchange, receipt in ((at+timedelta(seconds=2), at),
                                  (at-timedelta(seconds=4), at),
                                  (at-timedelta(seconds=1), at-timedelta(seconds=5))):
            with self.subTest(exchange=exchange, receipt=receipt):
                with self.assertRaises(SafetyError):
                    self.engine.position_feed.accept(
                        PositionQuote(self.engine.position.trade_id, self.tick(exchange), receipt), at)
        self.assertEqual(self.engine.exit_quotes, {})

    def test_after_sustained_outage_an_exit_still_waits_for_confirmed_stop_cancellation(self):
        guard = self.open_position()
        at = AT+timedelta(seconds=17)
        self.engine.timer(at)
        self.assertEqual(self.engine.state["halt"], POSITION_FEED_HALT)
        self.engine.reconcile(self.broker.snapshot(at), at)
        self.engine.position_feed.accept(PositionQuote(self.engine.position.trade_id, self.tick(at), at), at)
        self.assertTrue(guard.cancel_requested)
        self.assertFalse(any(x.purpose == "exit" for x in self.engine.orders))
        self.engine.reconcile(self.broker.snapshot(at), at)
        self.assertTrue(any(x.purpose == "exit" for x in self.engine.orders))
        self.assertTrue(all(x["status"] == "CANCELLED" for x in self.broker.orders.values()
                            if x["purpose"] == "protect"))

    def test_legacy_halt_clears_only_on_fresh_flat_reconciliation_and_preserves_loss_and_attempts(self):
        self.engine.halt(POSITION_FEED_HALT, AT)
        self.engine.state.update(cash=2499000, trades=1, consecutive_losses=1)
        self.engine._save()
        self.engine.reconciled = False
        self.engine.position_feed.check(AT)
        self.assertEqual(self.engine.state["halt"], POSITION_FEED_HALT)
        self.assertTrue(ReconciliationHealth(self.engine).success(self.broker.snapshot(AT), AT))
        self.assertEqual(self.engine.state["halt"], "")
        self.assertEqual(self.engine.state["cash"], 2499000)
        self.assertEqual(self.engine.state["trades"], 1)
        self.assertEqual(self.engine.state["consecutive_losses"], 1)

    def test_fresh_exit_quote_does_not_cancel_protection_before_ownership_is_fresh_and_confirmed(self):
        guard = self.open_position()
        at = AT+timedelta(seconds=20)
        self.engine.position.exit_reason = "time_stop"
        self.engine.position_feed.accept(PositionQuote(self.engine.position.trade_id, self.tick(at), at), at)
        self.assertFalse(guard.cancel_requested)
        self.engine.snapshot_at = at
        self.engine.reconciled = False
        self.engine.position_feed.accept(PositionQuote(self.engine.position.trade_id, self.tick(at), at), at)
        self.assertFalse(guard.cancel_requested)
        self.engine.reconciled = True
        self.engine.broker_reads_ready = False
        self.engine.position_feed.accept(PositionQuote(self.engine.position.trade_id, self.tick(at), at), at)
        self.assertFalse(guard.cancel_requested)
        self.assertTrue(ReconciliationHealth(self.engine).success(self.broker.snapshot(at), at))
        self.assertTrue(guard.cancel_requested)
        self.assertFalse(any(x.purpose == "exit" for x in self.engine.orders))

    def test_snapshot_freshness_uses_batch_start_not_later_processing_time(self):
        snapshot = self.broker.snapshot(AT)
        self.assertTrue(ReconciliationHealth(self.engine).success(snapshot, AT+timedelta(seconds=14)))
        self.assertEqual(self.engine.snapshot_at, AT)
        self.engine.halt(POSITION_FEED_HALT, AT+timedelta(seconds=16))
        self.engine.position_feed.check(AT+timedelta(seconds=16))
        self.assertEqual(self.engine.state["halt"], POSITION_FEED_HALT)

    def test_delayed_quote_after_position_closes_cannot_create_a_second_sell(self):
        self.open_position()
        fresh_at = AT+timedelta(seconds=2)
        self.engine.position_feed.accept(PositionQuote(
            self.engine.position.trade_id, self.tick(fresh_at), fresh_at), fresh_at)
        at = AT+timedelta(seconds=3)
        delayed = PositionQuote(self.engine.position.trade_id, self.tick(at), at)
        stop_tick = self.tick(at, price=9978, volume=3000)
        self.broker.on_tick(stop_tick)
        self.engine.reconcile(self.broker.snapshot(at), at)
        self.assertTrue(self.engine.flat)
        orders_before = len(self.engine.orders)
        self.assertFalse(self.engine.position_feed.accept(delayed, at+timedelta(seconds=1)))
        self.assertEqual(len(self.engine.orders), orders_before)
        self.assertFalse(self.engine.exit_quotes)

    def test_secondary_clock_or_auth_fault_blocks_clearing_an_earlier_feed_halt(self):
        for fault, value in (("clock_fault", {"ahead_seconds": 2}), ("broker_auth_required", True)):
            with self.subTest(fault=fault):
                self.engine.state["halt"] = POSITION_FEED_HALT
                self.engine.state[fault] = value
                self.engine.position_feed.check(AT)
                self.assertEqual(self.engine.state["halt"], POSITION_FEED_HALT)
                self.engine.state.pop(fault)

    def test_other_halts_are_not_erased_as_position_feed_recovery(self):
        for halt in (CLOCK_HALT, "Daily loss/profit-giveback threshold reached.",
                     "Unresolved order intent; no automatic resubmission.", "Operator kill switch."):
            with self.subTest(halt=halt):
                self.engine.state["halt"] = halt
                self.engine.position_feed.check(AT)
                self.assertEqual(self.engine.state["halt"], halt)

    def test_risk_guard_reasserts_after_legacy_warning_is_cleared_flat(self):
        self.engine.halt(POSITION_FEED_HALT, AT)
        self.engine.state["cash"] -= 20000
        self.engine.position_feed.check(AT)
        self.assertEqual(self.engine.state["halt"], "Daily loss/profit-giveback threshold reached.")

    def test_bounded_read_refreshes_keep_budget_and_report_failure(self):
        self.open_position()
        at = AT+timedelta(seconds=5)
        self.assertTrue(self.engine.position_feed.due(at, 100))
        self.engine.position_feed.started(100)
        self.assertFalse(self.engine.position_feed.due(at, 104))
        error = BrokerReadUnavailable("Kite GET /quote timeout.", method="GET", endpoint="/quote", category="timeout")
        for i, delay in enumerate((5,10,20,30,30)):
            self.engine.position_feed.failed(error, at, 200+i*100)
            self.assertEqual(self.engine.position_feed.next_refresh, 200+i*100+delay)
        self.assertEqual(len(self.store.events("POSITION_QUOTE_REFRESH_FAILED")), 5)
        self.assertEqual(self.engine.position.exit_reason, "")

    def test_retry_after_and_auth_failure_do_not_poll_or_cancel_native_protection(self):
        guard = self.open_position()
        at = AT+timedelta(seconds=5)
        throttled = BrokerReadUnavailable("Rate limited.", method="GET", endpoint="/quote",
                                         category="http", status_code=429, retry_after_seconds=45)
        self.engine.position_feed.failed(throttled, at, 100)
        self.assertEqual(self.engine.position_feed.next_refresh, 145)
        denied = BrokerError("Quote permission denied.", method="GET", endpoint="/quote",
                             category="http", status_code=403)
        self.engine.position_feed.failed(denied, at, 200)
        self.assertFalse(self.engine.position_feed.due(at, 10000))
        self.assertFalse(guard.cancel_requested)
        self.assertEqual(self.engine.position.exit_reason, "")

    def test_latest_quote_buffer_is_generation_scoped_and_bounded_by_symbol(self):
        mailbox = LatestQuoteBuffer()
        mailbox.offer(self.tick(AT), AT, 1)
        mailbox.offer(self.tick(AT+timedelta(seconds=1)), AT+timedelta(seconds=1), 1)
        mailbox.offer(self.tick(AT-timedelta(seconds=2)), AT, 1)
        current = mailbox.take(1)
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0][0].at, AT+timedelta(seconds=1))
        self.assertEqual(mailbox.take(1), [])
        mailbox.offer(self.tick(AT), AT, 1)
        self.assertEqual(mailbox.take(2), [])
        mailbox.offer(self.tick(AT), AT, 2)
        mailbox.offer(self.tick(AT+timedelta(seconds=10)), AT, 1)
        self.assertEqual(mailbox.take(2)[0][0].at, AT)


class PositionQuoteTests(unittest.TestCase):
    def test_quote_fetch_uses_only_owned_symbol_get_and_preserves_exchange_timestamp(self):
        instrument = Instrument("DEMO", 123, 1, 9000, 11000)
        http = Mock()
        http.allow_orders = False
        http.request.return_value = {"NSE:DEMO": {
            "instrument_token": 123, "timestamp": "2026-09-29 11:00:00",
            "last_price": 100, "average_price": 100, "volume": 1000,
            "depth": {"buy":[{"price":99.99,"quantity":100}],
                      "sell":[{"price":100.01,"quantity":100}]},
        }}
        with patch("india_trader.position_feed.now_ist", return_value=AT+timedelta(milliseconds=200)):
            result = fetch_position_quote(http, instrument, "test-trade")
        http.request.assert_called_once_with("GET","/quote",query=[("i","NSE:DEMO")])
        self.assertEqual(result.tick.at, AT)
        self.assertEqual(result.trade_id, "test-trade")
        self.assertEqual(result.received_at, AT+timedelta(milliseconds=200))

    def test_order_enabled_client_reference_index_or_wrong_token_is_rejected(self):
        http = Mock()
        http.allow_orders = True
        instrument = Instrument("DEMO",123,1,9000,11000)
        with self.assertRaises(SafetyError):
            fetch_position_quote(http, instrument, "trade")
        http.request.assert_not_called()
        http.allow_orders = False
        with self.assertRaises(SafetyError):
            fetch_position_quote(http, replace(instrument, reference=True), "trade")
        http.request.return_value = {"NSE:DEMO":{"instrument_token":456}}
        with self.assertRaises(SafetyError):
            fetch_position_quote(http, instrument, "trade")


if __name__ == "__main__":
    unittest.main()
