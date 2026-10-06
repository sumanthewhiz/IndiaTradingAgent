from __future__ import annotations

import http.client
import io
import json
import socket
import ssl
import tempfile
import unittest
import urllib.error
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from india_trader.broker import BrokerError, BrokerReadUnavailable, BrokerRejected, KiteHTTP, PaperBroker, SubmissionUnknown
from india_trader.core import Candidate, Config, IST, Instrument, Order, SafetyError, Session, Snapshot, Tick
from india_trader.engine import TradingEngine
from india_trader.reconciliation import LEGACY_READ_HALT, ReconciliationHealth
from india_trader.storage import Store

AT = datetime(2026, 9, 28, 10, 0, tzinfo=IST)


class ReadTransportTests(unittest.TestCase):
    def setUp(self):
        self.http = KiteHTTP(Config(), False, api_key="test-only-key", access_token="test-only-token")
        self.http.opener = Mock()

    def test_get_timeout_is_read_unavailable_with_sanitized_endpoint_not_unknown_order(self):
        self.http.opener.open.side_effect = TimeoutError("test-only-token https://private.invalid")
        with self.assertRaises(BrokerReadUnavailable) as caught:
            self.http.request("GET", "/orders")
        self.assertEqual(caught.exception.method, "GET")
        self.assertEqual(caught.exception.endpoint, "/orders")
        self.assertEqual(caught.exception.category, "timeout")
        self.assertNotIn("test-only-token", str(caught.exception))
        self.assertNotIn("private.invalid", str(caught.exception))
        self.assertNotIsInstance(caught.exception, SubmissionUnknown)
        self.http.opener.open.assert_called_once()

    def test_network_dns_and_partial_response_failures_have_bounded_safe_categories(self):
        for failure, category in (
            (urllib.error.URLError(socket.gaierror("private DNS details")), "dns_resolution"),
            (ConnectionResetError("private network details"), "connection_interrupted"),
            (http.client.RemoteDisconnected("private connection details"), "connection_interrupted"),
            (http.client.IncompleteRead(b"private-partial-data", 10), "incomplete_response"),
            (ssl.SSLEOFError("private early EOF"), "connection_interrupted"),
        ):
            with self.subTest(category=category):
                self.http.opener.open.side_effect = failure
                self.http.last_request = 0
                with self.assertRaises(BrokerReadUnavailable) as caught:
                    self.http.request("GET", "/portfolio/positions")
                self.assertEqual(caught.exception.category, category)
                self.assertNotIn("private", str(caught.exception))

    def test_tls_verification_failure_never_becomes_a_retryable_or_insecure_read(self):
        self.http.opener.open.side_effect = urllib.error.URLError(
            ssl.SSLCertVerificationError("certificate invalid"))
        with self.assertRaises(BrokerError) as caught:
            self.http.request("GET", "/orders")
        self.assertNotIsInstance(caught.exception, BrokerReadUnavailable)
        self.assertEqual(caught.exception.category, "tls_certificate")
        self.assertIn("certificate checks were not disabled", str(caught.exception))

    def test_post_and_cancel_transport_failures_remain_unknown_and_are_never_retried(self):
        self.http.allow_orders = True
        for method, path in (("POST", "/orders/regular"), ("DELETE", "/orders/regular/test123")):
            self.http.last_request = 0
            self.http.opener.open.reset_mock()
            self.http.opener.open.side_effect = TimeoutError("private")
            with self.subTest(method=method), self.assertRaises(SubmissionUnknown) as caught:
                self.http.request(method, path)
            self.assertNotIsInstance(caught.exception, BrokerReadUnavailable)
            self.assertEqual(caught.exception.method, method)
            self.assertIn("do not resubmit blindly", str(caught.exception))
            self.http.opener.open.assert_called_once()

    def test_http_read_rate_limit_retains_retry_after_without_retrying_in_transport(self):
        self.http.opener.open.side_effect = urllib.error.HTTPError(
            "https://api.kite.trade/orders", 429, "Rate limit", {"Retry-After": "40"},
            io.BytesIO(b'{"status":"error","error_type":"GeneralException","message":"Too many requests."}'),
        )
        with self.assertRaises(BrokerReadUnavailable) as caught:
            self.http.request("GET", "/orders")
        self.assertEqual(caught.exception.retry_after_seconds, 40)
        self.http.opener.open.assert_called_once()

    def test_malformed_get_response_is_hard_validation_failure_not_empty_success(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b'not valid JSON'
        self.http.opener.open.side_effect = None
        self.http.opener.open.return_value = response
        with self.assertRaises(BrokerError) as caught:
            self.http.request("GET", "/portfolio/positions")
        self.assertEqual(caught.exception.category, "invalid_response")
        self.assertNotIsInstance(caught.exception, BrokerReadUnavailable)


class ReconciliationRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name)/"state.db")
        self.config = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
        self.instruments = {"DEMO": Instrument("DEMO",1,1,9000,11000),
                            "INDEX": Instrument("INDEX",2,1,1,10**12,True)}
        self.broker = PaperBroker(2500000,self.config.costs)
        self.engine = TradingEngine(self.config,Session(AT.date(),True,True,["DEMO"],[]),
                                    self.instruments,self.broker,self.store,"paper")
        self.health = ReconciliationHealth(self.engine)
        self.health.success(self.broker.snapshot(AT),AT)

    def tearDown(self):
        self.store.__exit__()
        self.temporary.cleanup()

    def error(self, **kwargs):
        return BrokerReadUnavailable("Kite GET /orders transport failure (timeout).",
                                     method="GET",endpoint="/orders",category="timeout",**kwargs)

    def test_temporary_read_failure_pauses_entries_then_recovers_without_a_latched_halt(self):
        cash,capital = self.engine.state["cash"],self.engine.state["capital"]
        self.health.failure(self.error(),AT,monotonic=100)
        self.assertFalse(self.engine.reconciled)
        self.assertFalse(self.engine.broker_reads_ready)
        self.assertEqual(self.engine.state["halt"],"")
        self.assertFalse(self.engine.consider(Candidate("DEMO","orb",AT,9900),AT))
        self.assertFalse(self.health.due(101,0,dirty=True))
        self.assertTrue(self.health.due(102,0,dirty=True))
        self.assertTrue(self.health.success(self.broker.snapshot(AT+timedelta(seconds=3)),
                                            AT+timedelta(seconds=3)))
        self.assertTrue(self.engine.broker_reads_ready)
        self.assertEqual(self.engine.reconciliation_status["state"],"healthy")
        self.assertEqual(self.engine.state["cash"],cash)
        self.assertEqual(self.engine.state["capital"],capital)
        self.assertEqual(self.engine.state["trades"],0)
        self.assertEqual(self.broker.counter,0)
        self.assertEqual(len(self.store.events("BROKER_RECONCILIATION_RECOVERED")),1)

    def test_retry_backoff_caps_and_does_not_spin_even_on_order_update_events(self):
        expected=[2,4,8,16,30,30]
        for index,delay in enumerate(expected):
            mono=100+index*100
            self.health.failure(self.error(),AT+timedelta(seconds=index),monotonic=mono)
            self.assertEqual(self.engine.reconciliation_status["retry_delay_seconds"],delay)
            self.assertFalse(self.health.due(mono+delay-.1,0,dirty=True))
            self.assertTrue(self.health.due(mono+delay,0,dirty=True))
        self.health.failure(self.error(retry_after_seconds=60),AT,monotonic=1000)
        self.assertFalse(self.health.due(1059,0,dirty=True))
        self.assertTrue(self.health.due(1060,0,dirty=True))

    def test_exact_old_read_only_transport_halt_clears_after_fresh_consistent_flat_reconciliation(self):
        self.engine.halt(LEGACY_READ_HALT,AT)
        self.engine.state.update(cash=2495000,trades=2,consecutive_losses=1)
        self.engine._save()
        restarted = ReconciliationHealth(self.engine)
        self.assertTrue(restarted.success(self.broker.snapshot(AT),AT))
        self.assertEqual(self.engine.state["halt"],"")
        self.assertEqual(self.engine.state["cash"],2495000)
        self.assertEqual(self.engine.state["trades"],2)
        self.assertEqual(self.engine.state["consecutive_losses"],1)
        self.assertEqual(self.broker.counter,0)

    def test_risk_and_unknown_write_halts_never_clear_as_read_recovery(self):
        for reason in ("Daily loss/profit-giveback threshold reached.",
                       "entry submission uncertain: Broker transport failure; reconcile before action.",
                       "Operator kill switch."):
            self.engine.state["halt"]=reason
            health=ReconciliationHealth(self.engine)
            health.failure(self.error(),AT,monotonic=0)
            health.success(self.broker.snapshot(AT),AT)
            self.assertEqual(self.engine.state["halt"],reason)

    def test_hard_response_failure_remains_a_halt_after_later_success(self):
        self.health.failure(ValueError("bad broker quantities"),AT,monotonic=0)
        reason=self.engine.state["halt"]
        self.assertIn("ValueError",reason)
        self.assertNotIn("bad broker quantities",reason)
        self.health.success(self.broker.snapshot(AT),AT)
        self.assertEqual(self.engine.state["halt"],reason)

    def test_auth_failure_requires_login(self):
        self.health.failure(BrokerRejected("Session expired.",403,error_type="TokenException",
                                          method="GET",endpoint="/orders"),AT,monotonic=0)
        self.assertTrue(self.engine.state["broker_auth_required"])
        self.assertFalse(self.engine.broker_reads_ready)
        self.assertEqual(self.engine.reconciliation_status["state"],"blocked")

    def test_old_or_future_snapshot_never_marks_current_ownership_ready(self):
        for age in (16,-1):
            with self.subTest(age=age):
                snapshot=self.broker.snapshot(AT-timedelta(seconds=age))
                self.assertFalse(self.health.success(snapshot,AT))
                self.assertFalse(self.engine.reconciled)
                self.assertFalse(self.engine.broker_reads_ready)
                self.assertEqual(self.engine.reconciliation_status["category"],"snapshot_age")

    def test_native_stop_is_not_cancelled_during_read_outage(self):
        self.engine.quotes["DEMO"]=Tick("DEMO",AT,10065,10064,10066,1000,10000,10000)
        self.engine.quotes["INDEX"]=Tick("INDEX",AT,10000,10000,10000,0,0,0)
        self.engine.heartbeat_news(AT)
        self.assertTrue(self.engine.consider(Candidate("DEMO","orb",AT,9979),AT))
        filled_at=AT+timedelta(seconds=1)
        fill=Tick("DEMO",filled_at,10066,10065,10067,2000,10000,10000)
        self.broker.on_tick(fill)
        self.engine.quotes["DEMO"]=fill
        self.health.success(self.broker.snapshot(filled_at),filled_at)
        self.health.success(self.broker.snapshot(filled_at),filled_at)
        guard=next(x for x in self.engine.orders if x.purpose=="protect")
        self.assertTrue(guard.active)
        self.health.failure(self.error(),filled_at,monotonic=0)
        target_at=AT+timedelta(seconds=2)
        self.engine.quotes["DEMO"]=Tick("DEMO",target_at,10350,10349,10351,3000,10000,10000)
        self.engine.timer(target_at)
        self.assertFalse(guard.cancel_requested)
        self.assertFalse(any(x.purpose=="exit" for x in self.engine.orders))
        self.assertTrue(self.health.success(self.broker.snapshot(target_at),target_at))
        self.assertTrue(guard.cancel_requested)
        self.assertFalse(any(x.purpose=="exit" for x in self.engine.orders))

    def test_foreign_ownership_mismatch_is_not_hidden_by_successful_http(self):
        bad=Snapshot([],{"OTHER":1},2500000,AT)
        self.assertFalse(self.health.success(bad,AT))
        self.assertFalse(self.engine.broker_reads_ready)
        self.assertFalse(self.health.success(bad,AT))
        self.assertTrue(self.engine.state["quarantine"])
        self.assertFalse(self.engine.broker_reads_ready)

    def test_ambiguous_write_is_never_classified_as_temporary_read(self):
        self.health.failure(SubmissionUnknown("Unknown submission.",method="POST",endpoint="/orders/regular",
                                              category="timeout"),AT,monotonic=0)
        self.assertEqual(self.engine.reconciliation_status["state"],"blocked")
        self.assertTrue(self.engine.state["halt"])
        self.assertEqual(self.broker.counter,0)


if __name__ == "__main__":
    unittest.main()
