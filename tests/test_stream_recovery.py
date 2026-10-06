from __future__ import annotations

import importlib.util
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.broker import PaperBroker
from india_trader.core import Config, IST, Instrument, Order, Position, SafetyError, Session, Tick
from india_trader.engine import CLOCK_HALT, TradingEngine
from india_trader.dashboard import Controller
from india_trader.storage import Store
from india_trader.streaming import (
    LEGACY_STREAM_HALTS, STREAM_HALT, StreamCommands, StreamEvent, StreamHealth,
    reactor_dispatch, retry_delay, safe_stream_reason,
)

AT = datetime(2026, 9, 28, 12, 0, tzinfo=IST)


class StreamHealthTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.store = Store(self.directory / "state.db")
        self.config = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
        self.instruments = {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                            "INDEX": Instrument("INDEX", 2, 1, 1, 10**12, True)}
        self.broker = PaperBroker(2500000, self.config.costs)
        self.engine = TradingEngine(self.config, Session(AT.date(), True, True, ["DEMO"], []),
                                    self.instruments, self.broker, self.store, "paper")
        self.engine.reconcile(self.broker.snapshot(AT), AT)
        self.stream = StreamHealth(self.engine)

    def tearDown(self):
        self.store.__exit__()
        self.temporary.cleanup()

    def healthy_samples(self, stream=None, start=AT):
        stream = stream or self.stream
        for second in range(4):
            receipt = start + timedelta(seconds=second)
            for symbol in self.instruments:
                tick = Tick(symbol, receipt-timedelta(milliseconds=100),10000,9999,10001,100+second,1000,1000)
                self.engine.on_tick(tick, receipt, evaluate_signals=False)
                stream.observe(tick, receipt, receipt)
        return start + timedelta(seconds=3)

    def test_1006_fault_is_specific_persistent_and_blocks_entries(self):
        self.stream.event(StreamEvent("connected", AT, 1))
        self.engine.quotes["DEMO"] = Tick("DEMO", AT,10000,9999,10001,100,1000,1000)
        self.stream.event(StreamEvent("error", AT+timedelta(seconds=1), 1, 1006, "Peer dropped TCP connection."))
        self.assertFalse(self.engine.market_stream_ready)
        self.assertFalse(self.stream.connected)
        self.assertEqual(self.engine.quotes, {})
        self.assertEqual(self.engine.state["halt"], STREAM_HALT)
        self.assertEqual(self.engine.state["stream_fault"]["code"], 1006)
        self.assertTrue(self.engine.state["stream_fault"]["auto_retry_allowed"])
        self.assertIn("1006", self.engine.stream_status["reason"])
        self.assertEqual(self.store.events("STREAM_ERROR")[0]["code"], 1006)
        self.assertEqual(self.broker.counter, 0)

    def test_error_and_close_are_one_retry_incident(self):
        self.stream.event(StreamEvent("connected", AT, 1))
        self.stream.event(StreamEvent("error", AT, 1, 1006, "Dropped."))
        self.stream.event(StreamEvent("closed", AT, 1, 1006, "Dropped."))
        self.assertEqual(len(self.engine.state["stream_restart_history"]),1)
        self.assertEqual(len(self.store.events("STREAM_ERROR")),2)
        self.assertEqual(retry_delay(self.engine.state, AT),5)

    def test_flat_worker_cannot_restart_until_post_failure_broker_reconciliation(self):
        self.stream.event(StreamEvent("connected", AT, 1))
        failed = AT+timedelta(seconds=2)
        self.stream.event(StreamEvent("error",failed,1,1006,"Dropped."))
        self.assertFalse(self.stream.may_restart_flat(failed))
        self.engine.reconcile(self.broker.snapshot(failed),failed)
        self.assertTrue(self.stream.may_restart_flat(failed))
        self.engine.orders.append(Order("abcdef12","DEMO","entry","BUY",1,10000,0,failed.isoformat()))
        self.assertFalse(self.stream.may_restart_flat(failed))
        self.engine._save()
        self.assertIsNone(retry_delay(self.engine.state,failed))

    def test_legacy_halt_recovers_only_after_warmup_fresh_quotes_and_reconciliation(self):
        self.engine.halt("Broker WebSocket reported an error.", AT)
        self.engine.state.update(cash=2495000,trades=2,consecutive_losses=1)
        self.engine._save()
        self.stream.event(StreamEvent("connected",AT,1))
        end = self.healthy_samples()
        self.assertFalse(self.stream.ready(end))
        self.stream.mark_warmed()
        end = self.healthy_samples(start=AT+timedelta(seconds=5))
        self.assertFalse(self.stream.ready(end))
        self.engine.reconcile(self.broker.snapshot(end),end)
        self.assertTrue(self.stream.ready(end))
        self.assertEqual(self.engine.state["halt"],"")
        self.assertEqual(self.engine.state["cash"],2495000)
        self.assertEqual(self.engine.state["trades"],2)
        self.assertEqual(self.engine.state["consecutive_losses"],1)
        self.assertEqual(len(self.store.events("STREAM_RECOVERED")),1)
        self.assertEqual(self.broker.counter,0)

    def test_runtime_disconnect_never_reuses_old_indicator_history_to_reopen_entries(self):
        self.stream.event(StreamEvent("connected",AT,1))
        self.stream.mark_warmed()
        self.stream.event(StreamEvent("error",AT,1,1006,"Dropped."))
        self.stream.event(StreamEvent("connected",AT+timedelta(seconds=4),2))
        end=self.healthy_samples(start=AT+timedelta(seconds=5))
        self.engine.reconcile(self.broker.snapshot(end),end)
        self.assertFalse(self.stream.ready(end))
        self.assertTrue(self.stream.restart_needed)
        self.assertEqual(self.engine.state["halt"],STREAM_HALT)

    def test_risk_and_clock_halts_are_not_cleared_by_healthy_reconnect(self):
        for halt in ("Daily loss/profit-giveback threshold reached.", CLOCK_HALT, "Operator kill switch."):
            with self.subTest(halt=halt):
                self.engine.state["halt"] = halt
                stream=StreamHealth(self.engine)
                stream.event(StreamEvent("connected",AT,1));stream.mark_warmed()
                end=self.healthy_samples(stream)
                self.engine.reconcile(self.broker.snapshot(end),end)
                stream.ready(end)
                self.assertEqual(self.engine.state["halt"],halt)
        self.assertEqual(self.store.events("STREAM_RECOVERED"),[])

    def test_owned_position_and_stale_quote_prevent_halt_clear(self):
        self.engine.halt(STREAM_HALT,AT)
        self.stream.event(StreamEvent("connected",AT,1));self.stream.mark_warmed()
        end=self.healthy_samples()
        self.engine.reconcile(self.broker.snapshot(end),end)
        self.engine.position=Position("DEMO","orb",AT.isoformat(),9900,10200,quantity=1)
        self.assertFalse(self.stream.ready(end))
        self.engine.position=None
        self.engine.quotes["INDEX"]=replace(self.engine.quotes["INDEX"],at=AT-timedelta(seconds=20))
        self.assertFalse(self.stream.ready(end))
        self.assertEqual(self.engine.state["halt"],STREAM_HALT)

    def test_future_ticks_cannot_validate_reconnect(self):
        self.stream.event(StreamEvent("connected",AT,1));self.stream.mark_warmed()
        tick=Tick("DEMO",AT+timedelta(seconds=2),10000,9999,10001,100,1000,1000)
        self.stream.observe(tick,AT,AT+timedelta(seconds=10))
        self.assertNotIn("DEMO",self.stream.samples)
        self.assertFalse(self.stream.ready(AT+timedelta(seconds=10)))

    def test_auth_or_protocol_faults_are_not_automatic_transient_restarts(self):
        for code,reason in ((403,"Forbidden"),(0,"Invalid api_key or access_token"),
                            (1008,"Policy violation"),(1002,"Protocol error")):
            with self.subTest(code=code):
                engine=self.engine
                engine.state["halt"]="";engine.state.pop("stream_fault",None)
                stream=StreamHealth(engine)
                stream.event(StreamEvent("connected",AT,1))
                stream.event(StreamEvent("error",AT,1,code,reason))
                self.assertIsNone(retry_delay(engine.state,AT))
                self.assertFalse(engine.state["stream_fault"]["auto_retry_allowed"])
                if code in {0,403}:
                    self.assertTrue(engine.state["broker_auth_required"])

    def test_retry_budget_is_persistent_and_bounded(self):
        for index in range(4):
            stream=StreamHealth(self.engine)
            at=AT+timedelta(seconds=index*30)
            stream.event(StreamEvent("connected",at,1))
            stream.event(StreamEvent("error",at,1,1006,"Dropped."))
            self.engine._save()
            self.assertEqual(len(self.store.get("engine")["stream_restart_history"]),index+1)
        self.assertIsNone(retry_delay(self.engine.state,AT+timedelta(seconds=90)))
        self.assertFalse(self.engine.state["stream_fault"]["auto_retry_allowed"])

    def test_recovery_view_does_not_accept_messages_from_an_old_connection(self):
        self.stream.event(StreamEvent("connected",AT,2))
        self.stream.event(StreamEvent("error",AT,1,1006,"Old connection."))
        self.assertTrue(self.stream.connected)
        self.assertFalse(self.stream.restart_needed)
        self.assertEqual(self.engine.state["halt"],"")

    def test_controller_retries_only_flat_authorized_transport_failures(self):
        self.stream.event(StreamEvent("connected",AT,1))
        self.stream.event(StreamEvent("error",AT,1,1006,"Dropped."))
        vault=Mock()
        vault.load.return_value={"auto_start":True,"keys":{}}
        controller=Controller(self.directory,self.directory,vault)
        controller.workspace=lambda:self.directory
        process=Mock()
        process.poll.return_value=2
        controller.worker=process
        controller.closed=Mock()
        controller.closed.is_set.side_effect=[False,True]
        controller.wake.set()
        with patch("india_trader.dashboard.read_ledger",return_value=self.engine.state), \
             patch("india_trader.dashboard.now_ist",return_value=AT):
            controller._loop()
        self.assertEqual(controller.phase,"RECONNECTING")
        self.assertIn("5s",controller.reason)
        self.assertIsNone(controller.worker)
        vault.load.return_value={"auto_start":False,"keys":{}}
        controller.worker=process
        controller.closed.is_set.side_effect=[False,True]
        controller.wake.set()
        with patch("india_trader.dashboard.read_ledger",return_value=self.engine.state), \
             patch("india_trader.dashboard.now_ist",return_value=AT):
            controller._loop()
        self.assertNotEqual(controller.phase,"RECONNECTING")


class StreamControlTests(unittest.TestCase):
    def test_dynamic_subscription_and_close_execute_on_owning_thread(self):
        caller=threading.get_ident()
        executions=[]
        class Socket:
            MODE_FULL="full"
            subscribed_tokens={}
            def is_connected(self): return True
            def subscribe(self,tokens):
                executions.append(("subscribe",threading.get_ident()))
                self.subscribed_tokens.update({token:"quote" for token in tokens})
            def set_mode(self,mode,tokens):
                executions.append(("mode",threading.get_ident()))
                self.subscribed_tokens.update({token:mode for token in tokens})
            def unsubscribe(self,tokens):
                executions.append(("unsubscribe",threading.get_ident()))
                for token in tokens:self.subscribed_tokens.pop(token,None)
            def close(self): executions.append(("close",threading.get_ident()))
        ticker=Socket()
        with ThreadPoolExecutor(max_workers=1) as reactor:
            commands=StreamCommands(ticker,lambda operation:reactor.submit(operation).result(timeout=2))
            commands.subscribe([1,2]);commands.unsubscribe([2]);commands.close()
        self.assertEqual([name for name,_ in executions],["subscribe","mode","unsubscribe","close"])
        self.assertTrue(all(thread!=caller for _,thread in executions))
        self.assertEqual(len({thread for _,thread in executions}),1)

    def test_offline_retirement_removes_sdk_resubscribe_intent_without_sending(self):
        ticker=Mock()
        ticker.is_connected.return_value=False
        ticker.subscribed_tokens={1:"full",2:"full"}
        commands=StreamCommands(ticker,lambda operation:operation())
        commands.unsubscribe([1])
        self.assertEqual(ticker.subscribed_tokens,{2:"full"})
        ticker.unsubscribe.assert_not_called()
        with self.assertRaises(SafetyError):commands.subscribe([3])
        ticker.subscribe.assert_not_called()

    def test_failed_mode_change_removes_half_added_desired_subscription(self):
        ticker=Mock()
        ticker.is_connected.return_value=True
        ticker.subscribed_tokens={}
        ticker.subscribe.side_effect=lambda tokens:ticker.subscribed_tokens.update({token:"quote" for token in tokens})
        ticker.set_mode.side_effect=RuntimeError("wire failed")
        commands=StreamCommands(ticker,lambda operation:operation())
        with self.assertRaises(RuntimeError):commands.subscribe([3])
        self.assertEqual(ticker.subscribed_tokens,{})

    def test_stream_diagnostics_redact_tokens_urls_and_control_characters(self):
        message=safe_stream_reason(
            b"1006 test-api test-token wss://host?access_token=private\nAuthorization: Bearer abc\x00",
            ("test-api","test-token"),
        )
        for private in ("test-api","test-token","private","Bearer abc","\n","\x00"):
            self.assertNotIn(private,message)
        self.assertIn("[redacted]",message)
        self.assertLessEqual(len(safe_stream_reason("x"*1000)),400)

    @unittest.skipUnless(importlib.util.find_spec("twisted"),"Optional broker SDK dependency")
    def test_actual_dispatch_uses_reactor_queue_and_propagates_error(self):
        class Reactor:
            running=True
            def __init__(self,pool):self.pool=pool
            def callFromThread(self,operation):self.pool.submit(operation)
        with ThreadPoolExecutor(max_workers=1) as pool:
            dispatch=reactor_dispatch(Reactor(pool))
            self.assertNotEqual(dispatch(threading.get_ident),threading.get_ident())
            def fail():raise ValueError("synthetic failure")
            with self.assertRaises(ValueError):dispatch(fail)
        stopped=Mock()
        stopped.running=False
        with self.assertRaises(SafetyError):reactor_dispatch(stopped)(lambda:None)
        stopped.callFromThread.assert_not_called()


if __name__ == "__main__":
    unittest.main()
