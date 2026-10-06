from __future__ import annotations

import io
import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.autonomy import (
    POLICY_VERSION, PreparationBlocked, account_directory, check_flat_entry_window,
    default_auto_config, prepare_session,
)
from india_trader.broker import BrokerReadUnavailable, BrokerRejected, KiteHTTP
from india_trader.cli import main
from india_trader.core import Config, IST, Instrument, SafetyError
from india_trader.credentials import atomic_json
from india_trader.dashboard import Controller
from india_trader.market import Bar, HistoryNotReady, Tape
from india_trader.market_data import closed_intraday_bars
from india_trader.storage import Store

ROOT = Path(__file__).resolve().parents[1]
AT = datetime(2026, 9, 29, 9, 32, tzinfo=IST)
OPEN = AT.replace(hour=9, minute=15)


def row(index):
    return [(OPEN+timedelta(minutes=5*index)).isoformat(), 100, 101, 99, 100.5, 1000+index]


class HistoricalDataTests(unittest.TestCase):
    def test_complete_history_uses_one_get_and_excludes_the_open_candle(self):
        http = Mock()
        http.request.return_value = {"candles": [row(i) for i in range(4)]}
        bars = closed_intraday_bars(http, 123, AT)
        self.assertEqual(len(bars), 3)
        self.assertEqual(bars[-1].end, AT.replace(minute=30))
        self.assertEqual(bars[-1].close, 10050)
        http.request.assert_called_once()
        self.assertEqual(http.request.call_args.args, ("GET", "/instruments/historical/123/5minute"))

    def test_missing_tail_is_requested_once_and_only_real_closed_candles_are_appended(self):
        http = Mock()
        http.request.side_effect = [
            {"candles": [row(0), row(1)]}, {"candles": [row(2), row(3)]},
        ]
        bars = closed_intraday_bars(http, 123, AT)
        self.assertEqual([bar.volume for bar in bars], [1000, 1001, 1002])
        calls = http.request.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(dict(calls[1].kwargs["query"])["from"], "2026-09-29 09:25:00")
        self.assertEqual({dict(call.kwargs["query"])["to"] for call in calls}, {"2026-09-29 09:30:00"})
        self.assertTrue(all(call.args[0] == "GET" for call in calls))

    def test_missing_tail_after_refresh_remains_pending_and_reports_exact_coverage(self):
        http = Mock()
        http.request.side_effect = [{"candles": [row(0), row(1)]}, {"candles": []}]
        with self.assertRaises(HistoryNotReady) as caught:
            closed_intraday_bars(http, 123, AT)
        self.assertEqual(caught.exception.expected, AT.replace(minute=30))
        self.assertEqual(caught.exception.available, AT.replace(minute=25))
        self.assertIn("09:30", str(caught.exception))
        self.assertIn("09:25", str(caught.exception))
        self.assertEqual(http.request.call_count, 2)

    def test_empty_history_is_not_filled_with_synthetic_bars(self):
        http = Mock()
        http.request.return_value = {"candles": []}
        with self.assertRaises(HistoryNotReady) as caught:
            closed_intraday_bars(http, 123, AT)
        self.assertIsNone(caught.exception.available)
        self.assertEqual(http.request.call_count, 2)
        tape = Tape()
        with self.assertRaises(HistoryNotReady):
            tape.seed([], AT)
        self.assertFalse(tape.seeded)
        self.assertEqual(tape.bars, [])

    def test_extensive_missing_history_does_not_trigger_unbounded_tail_fetches(self):
        http = Mock()
        http.request.return_value = {"candles": [row(0)]}
        with self.assertRaises(HistoryNotReady):
            closed_intraday_bars(http, 123, AT.replace(hour=11))
        self.assertEqual(http.request.call_count, 1)

    def test_gaps_duplicates_and_invalid_prices_are_hard_errors_not_retryable_delay(self):
        for rows in ([row(0), row(2)], [row(0), row(0), row(1)],
                     [[row(0)[0], 100, 99, 98, 100, 1000]]):
            with self.subTest(rows=rows):
                http = Mock()
                http.request.return_value = {"candles": rows}
                with self.assertRaises(SafetyError) as caught:
                    closed_intraday_bars(http, 123, AT)
                self.assertNotIsInstance(caught.exception, HistoryNotReady)
                self.assertEqual(http.request.call_count, 1)

    def test_tail_with_a_missing_internal_bar_is_not_padded(self):
        http = Mock()
        http.request.side_effect = [{"candles": [row(0)]}, {"candles": [row(2)]}]
        with self.assertRaises(SafetyError) as caught:
            closed_intraday_bars(http, 123, AT)
        self.assertNotIsInstance(caught.exception, HistoryNotReady)
        self.assertEqual(http.request.call_count, 2)

    def test_bad_structure_and_wrong_session_are_not_accepted_as_history_delay(self):
        previous = row(0)
        previous[0] = (OPEN-timedelta(days=1)).isoformat()
        for response in (None, {}, {"candles": {}}, {"candles": [[1, 2]]}, {"candles": [previous]}):
            with self.subTest(response=response):
                http = Mock()
                http.request.return_value = response
                with self.assertRaises(SafetyError) as caught:
                    closed_intraday_bars(http, 123, AT)
                self.assertNotIsInstance(caught.exception, HistoryNotReady)

    def test_auth_or_transport_failure_is_not_disguised_as_an_empty_history(self):
        for failure in (
            BrokerRejected("History access denied.", 403, error_type="PermissionException"),
            BrokerReadUnavailable("History timeout.", method="GET",
                                  endpoint="/instruments/historical/123/5minute", category="timeout"),
        ):
            http = Mock()
            http.request.side_effect = failure
            with self.assertRaises(type(failure)):
                closed_intraday_bars(http, 123, AT)
            self.assertEqual(http.request.call_count, 1)

    def test_incomplete_seed_does_not_mutate_indicators(self):
        tape = Tape()
        with self.assertRaises(HistoryNotReady):
            tape.seed([Bar(OPEN, 10000, 10100, 9900, 10050, 1000)], AT)
        self.assertEqual(tape.bars, [])
        self.assertEqual((tape.value, tape.volume, tape.ema), (0, 0, 0))
        self.assertFalse(tape.seeded)

    def test_history_route_remains_read_only_and_narrow(self):
        self.assertTrue(KiteHTTP.allowed("GET", "/instruments/historical/123/5minute", False))
        for method, route in (
            ("POST", "/instruments/historical/123/5minute"), ("POST", "/orders/regular"),
            ("GET", "/instruments/historical/123/minute"), ("GET", "/bank/withdraw"),
        ):
            self.assertFalse(KiteHTTP.allowed(method, route, False))


class WarmupWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.config = default_auto_config(["DEMO"], "TEST01")
        self.workspace = account_directory(self.directory, "TEST01")
        self.workspace.mkdir(parents=True)
        self.keys = {name: "test-only-"+name for name in
                     ("ai_api_key", "broker_api_key", "broker_api_secret", "broker_access_token")}
        self.vault = Mock()
        self.vault.load.return_value = {
            "keys": self.keys, "auto_start": True, "consent_version": "auto-live-v1",
        }
        self.orders = []
        self.positions = []
        self.http = Mock()
        self.http.config = self.config
        self.http.request.side_effect = self.response
        self.plan = {
            "account": "TEST01", "policy": POLICY_VERSION, "day": AT.date().isoformat(),
            "selected": ["DEMO"], "config": asdict(self.config), "aliases": {},
            "universe_source": "test", "created_at": AT.isoformat(),
        }
        atomic_json(self.workspace/"plan.json", self.plan)
        atomic_json(self.directory/"active-account.json", {"directory": str(self.workspace)})

    def tearDown(self):
        self.temporary.cleanup()

    def response(self, method, path, **kwargs):
        self.assertEqual(method, "GET")
        return {
            "/user/profile": {"broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["NSE"],
                              "products": ["CNC"], "order_types": ["LIMIT", "SL"]},
            "/portfolio/holdings": [],
            "/orders": self.orders,
            "/portfolio/positions": {"net": self.positions},
            "/user/margins/equity": {
                "available": {"cash": 25000, "live_balance": 25000, "collateral": 0},
                "net": 25000, "utilised": {},
            },
        }[path]

    def test_flat_late_start_checks_broker_then_skips_history_and_ai_verification(self):
        with patch("india_trader.autonomy.now_ist", return_value=AT.replace(hour=14, minute=30)), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", return_value=self.http), \
             patch("india_trader.autonomy.closed_intraday_bars") as history, \
             patch("india_trader.autonomy.verify_gemini_key") as ai:
            with self.assertRaises(PreparationBlocked) as caught:
                prepare_session(ROOT, self.directory, self.vault)
        self.assertEqual(caught.exception.state, "ENTRY_WINDOW_CLOSED")
        history.assert_not_called()
        ai.assert_not_called()
        self.assertIn(("GET", "/portfolio/positions"), [call.args for call in self.http.request.call_args_list])
        self.assertEqual(json.loads((self.workspace/"plan.json").read_text()), self.plan)
        self.assertFalse((self.workspace/"live.db").exists())

    def test_before_cutoff_and_owned_exposure_do_not_take_the_flat_shortcut(self):
        with patch("india_trader.autonomy.now_ist", return_value=AT):
            check_flat_entry_window(self.http, self.config, False)
        with patch("india_trader.autonomy.now_ist", return_value=AT.replace(hour=15)):
            check_flat_entry_window(self.http, self.config, True)
        self.http.request.assert_not_called()

    def test_unexpected_broker_position_or_working_order_cannot_claim_a_flat_late_close(self):
        for kind in ("position", "order"):
            with self.subTest(kind=kind):
                self.positions = ([{"quantity": 1, "product": "CNC", "exchange": "NSE",
                                    "tradingsymbol": "DEMO"}] if kind == "position" else [])
                self.orders = ([{
                    "product": "CNC", "exchange": "NSE", "quantity": 1, "filled_quantity": 0,
                    "average_price": 0, "order_id": "test-order", "tradingsymbol": "DEMO",
                    "transaction_type": "BUY", "status": "OPEN", "price": 100,
                }] if kind == "order" else [])
                with patch("india_trader.autonomy.now_ist", return_value=AT.replace(hour=15)):
                    with self.assertRaises(PreparationBlocked) as caught:
                        check_flat_entry_window(self.http, self.config, False)
                self.assertEqual(caught.exception.state, "BLOCKED")

    def test_slow_broker_read_cannot_establish_late_session_flatness(self):
        at = AT.replace(hour=15)
        with patch("india_trader.autonomy.now_ist", side_effect=[at, at, at+timedelta(seconds=16)]):
            with self.assertRaises(PreparationBlocked) as caught:
                check_flat_entry_window(self.http, self.config, False)
        self.assertEqual(caught.exception.state, "BLOCKED")
        self.assertIn("too old", str(caught.exception))

    def test_preparation_waits_then_recovers_when_complete_real_history_arrives(self):
        at = AT.replace(hour=10, minute=32)
        bars = [Bar(OPEN+timedelta(minutes=5*i), 10000, 10100, 9900, 10050, 1000) for i in range(15)]
        instruments = {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000)}
        news = Mock()
        news.health = {}
        pending = HistoryNotReady(at.replace(minute=30), at.replace(minute=20))
        with patch("india_trader.autonomy.now_ist", return_value=at), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", return_value=self.http), \
             patch("india_trader.autonomy.verify_gemini_key"), \
             patch("india_trader.pre_market.global_context", return_value={}), \
             patch("india_trader.autonomy.AutomaticNews", return_value=news), \
             patch("india_trader.autonomy.load_kite_instruments", return_value=instruments), \
             patch("india_trader.autonomy.closed_intraday_bars", side_effect=[pending, bars]):
            with self.assertRaises(PreparationBlocked) as caught:
                prepare_session(ROOT, self.directory, self.vault)
            self.assertEqual(caught.exception.state, "WAITING_HISTORY")
            self.assertIn("DEMO", str(caught.exception))
            self.assertIn("10:30", str(caught.exception))
            self.assertEqual(json.loads((self.workspace/"plan.json").read_text()), self.plan)
            prepared = prepare_session(ROOT, self.directory, self.vault)
        self.assertEqual(len(prepared["warmup"]["DEMO"]), 15)
        self.assertFalse((self.workspace/"live.db").exists())

    def test_controller_uses_bounded_history_retry_not_a_permanent_block(self):
        error = PreparationBlocked("WAITING_HISTORY", "DEMO: need candles through 09:30.")
        controller = Controller(ROOT, self.directory, self.vault, prepare=Mock(side_effect=error))
        controller._software_check = Mock()
        controller._launch_worker = Mock()
        controller.closed = Mock()
        controller.closed.is_set.side_effect = [False, True]
        controller.wake.set()
        with patch("india_trader.dashboard.clock.monotonic", return_value=1000):
            controller._loop()
        self.assertEqual(controller.phase, "WAITING_HISTORY")
        self.assertEqual(controller.next_attempt, 1015)
        controller._launch_worker.assert_not_called()
        controller.prepare.side_effect = None
        controller.prepare.return_value = self.plan
        controller.closed.is_set.side_effect = [False, False, True]
        controller.wake.set()
        with patch("india_trader.dashboard.clock.monotonic", return_value=1016):
            controller._loop()
        controller._launch_worker.assert_called_once_with(self.plan)

    def test_worker_history_wait_is_recognized_and_reset_without_resetting_the_ledger(self):
        controller = Controller(ROOT, self.directory, self.vault)
        original = {"cash": 123000, "capital": 250000, "position": None, "orders": [], "halt": ""}
        with Store(self.workspace/"live.db") as store:
            store.put("engine", original)
        process = Mock()
        process.poll.return_value = 3
        process.stdout = io.StringIO("WAITING_HISTORY: DEMO: need completed candles through 09:30.\n")
        with patch("india_trader.dashboard.subprocess.Popen", return_value=process):
            controller._launch_worker({"database": str(self.workspace/"live.db")})
        controller.reader.join(timeout=2)
        self.assertTrue(controller.worker_waiting_for_history)
        controller.closed = Mock()
        controller.closed.is_set.side_effect = [False, True]
        controller.wake.set()
        with patch("india_trader.dashboard.clock.monotonic", return_value=1000):
            controller._loop()
        self.assertEqual(controller.phase, "WAITING_HISTORY")
        self.assertEqual(controller.next_attempt, 1015)
        with Store(self.workspace/"live.db") as store:
            self.assertEqual(store.get("engine"), original)
        process.stdout = io.StringIO("STOPPED: Synthetic corrupt candle.\n")
        process.poll.return_value = 2
        with patch("india_trader.dashboard.subprocess.Popen", return_value=process):
            controller._launch_worker({"database": str(self.workspace/"live.db")})
        controller.reader.join(timeout=2)
        self.assertFalse(controller.worker_waiting_for_history)
        controller.worker = None

    def test_cli_identifies_history_wait_separately_from_hard_startup_failures(self):
        output = io.StringIO()
        error = HistoryNotReady(AT.replace(minute=30), AT.replace(minute=25), "DEMO")
        with patch("india_trader.cli.generate_demo", side_effect=error), patch("sys.stderr", output):
            code = main(["demo", "--out", str(self.directory/"not-created")])
        self.assertEqual(code, 3)
        self.assertTrue(output.getvalue().startswith("WAITING_HISTORY: "))
        self.assertIn("DEMO", output.getvalue())
        self.assertFalse((self.directory/"not-created").exists())


if __name__ == "__main__":
    unittest.main()
