from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from india_trader.autonomy import default_auto_config
from india_trader.broker import PaperBroker
from india_trader.core import IST, Instrument, SafetyError, Session, Tick
from india_trader.engine import TradingEngine
from india_trader.market import Bar, SignalAgent
from india_trader.storage import Store

AT = datetime(2026, 9, 25, 13, 35, tzinfo=IST)
INSTRUMENTS = {
    "DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
    "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True),
}


def prepared_signal(config, *, at=AT, reference_last=2499975, volume_increment=5500):
    signals = SignalAgent(config, INSTRUMENTS)
    start = at.replace(hour=9, minute=15)
    current = at - timedelta(minutes=5)
    count = int((current - start).total_seconds() // 300)
    stocks = [Bar(start+timedelta(minutes=5*i), 10000 if i == 0 else 10020,
                  10060, 9995, 10020, 1000) for i in range(count)]
    index = [Bar(start+timedelta(minutes=5*i), 2500000, 2500100, 2499900, 2499975, 0)
             for i in range(count)]
    signals.tapes["DEMO"].seed(stocks, current)
    signals.tapes["NIFTY 50"].seed(index, current)
    signals.ingest(Tick("NIFTY 50", current, reference_last, reference_last, reference_last, 0, 0, 0))
    signals.ingest(Tick("DEMO", current, 10025, 10024, 10026, count*1000, 1000, 1000, 10015))
    signals.ingest(Tick("DEMO", at-timedelta(seconds=1), 10090, 10089, 10091,
                        count*1000+volume_increment, 1000, 1000, 10015))
    signals.ingest(Tick("NIFTY 50", at, reference_last, reference_last, reference_last, 0, 0, 0))
    trigger = Tick("DEMO", at, 10091, 10090, 10092, count*1000+volume_increment+10,
                   1000, 1000, 10015)
    return signals, trigger


class OpportunityTests(unittest.TestCase):
    def test_strong_stock_can_qualify_when_benchmark_is_marginally_below_open(self):
        config = default_auto_config(["DEMO"], "TEST01")
        signals, tick = prepared_signal(config)
        candidate = signals.ingest(tick)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.setup, "momentum_breakout")
        self.assertEqual(signals.decision["alignment_path"], "stock_relative_strength")
        self.assertLess(signals.decision["benchmark_return_bps"], 0)
        self.assertFalse(signals.decision["setup_checks"]["vwap_pullback"]["pullback_touches_vwap"])
        self.assertTrue(all(signals.decision["setup_checks"]["momentum_breakout"].values()))

    def test_legacy_profile_still_requires_the_absolute_index_filter(self):
        config = default_auto_config(["DEMO"], "TEST01", legacy_signals=True)
        signals, tick = prepared_signal(config)
        self.assertIsNone(signals.ingest(tick))
        self.assertFalse(signals.decision["checks"]["market_alignment"])
        self.assertIn("market alignment", signals.decision["reason"])

    def test_sharply_falling_benchmark_does_not_pass_relative_strength_path(self):
        signals, tick = prepared_signal(default_auto_config(["DEMO"], "TEST01"),
                                        reference_last=2480000)
        self.assertIsNone(signals.ingest(tick))
        self.assertFalse(signals.decision["checks"]["market_alignment"])
        self.assertLess(signals.decision["benchmark_recent_bps"], -50)

    def test_without_confirmation_volume_there_is_no_forced_trade(self):
        signals, tick = prepared_signal(default_auto_config(["DEMO"], "TEST01"),
                                        volume_increment=500)
        self.assertIsNone(signals.ingest(tick))
        self.assertFalse(signals.decision["setup_checks"]["momentum_breakout"]["confirmation_volume"])
        self.assertIn("confirmation volume", signals.decision["reason"])

    def test_stale_benchmark_and_entry_cutoff_still_block_new_signals(self):
        config = default_auto_config(["DEMO"], "TEST01")
        signals, tick = prepared_signal(config)
        reference = signals.tapes["NIFTY 50"]
        reference.latest = replace(reference.latest, at=AT-timedelta(seconds=20))
        self.assertIsNone(signals.ingest(tick))
        self.assertFalse(signals.decision["checks"]["benchmark_fresh"])
        signals, tick = prepared_signal(config, at=AT.replace(hour=14, minute=30))
        self.assertIsNone(signals.ingest(tick))
        self.assertFalse(signals.decision["checks"]["entry_window"])

    def test_no_repeat_evaluation_on_each_tick_within_same_candle(self):
        signals, tick = prepared_signal(default_auto_config(["DEMO"], "TEST01"))
        signals.ingest(tick)
        self.assertIsNotNone(signals.decision)
        signals.ingest(replace(tick, at=tick.at+timedelta(seconds=1), volume=tick.volume+1))
        self.assertIsNone(signals.decision)

    def test_signal_qualification_does_not_skip_news_or_execution_gates(self):
        config = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                   INSTRUMENTS, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            engine.signals, tick = prepared_signal(config)
            engine.quotes["NIFTY 50"] = engine.signals.tapes["NIFTY 50"].latest
            engine.on_tick(tick, AT)
            self.assertEqual(broker.counter, 0)
            self.assertEqual(engine.state["trades"], 0)
            record = store.events("SIGNAL_EVALUATED")[-1]
            self.assertFalse(record["entry_attempted"])
            self.assertIn("news collector heartbeat", record["reason"])
            self.assertEqual(store.get("signal_diagnostics")["symbols"]["DEMO"]["reason"], record["reason"])

    def test_qualified_opportunity_can_submit_with_small_capital_without_raising_limits(self):
        baseline = default_auto_config(["DEMO"], "TEST01")
        config = replace(baseline, risk=replace(baseline.risk, capital_rupees=1000))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(100000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                   INSTRUMENTS, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            engine.signals, tick = prepared_signal(config)
            engine.quotes["NIFTY 50"] = engine.signals.tapes["NIFTY 50"].latest
            for source in config.news.required_sources:
                engine.heartbeat_news(AT, source)
            engine.on_tick(tick, AT)
            self.assertEqual(engine.state["trades"], 1)
            self.assertEqual(broker.counter, 1)
            self.assertEqual(engine.position.setup, "momentum_breakout")
            self.assertLessEqual(engine.position.initial_risk, 250)
            self.assertLessEqual(engine.orders[0].quantity * engine.orders[0].price, 25000)
            self.assertTrue(store.events("SIGNAL_EVALUATED")[-1]["entry_attempted"])


class ProfileUpgradeTests(unittest.TestCase):
    def test_explicit_flat_upgrade_preserves_cash_counters_and_opening_window(self):
        old = default_auto_config(["DEMO"], "TEST01", legacy_opening=True, legacy_signals=True)
        new = default_auto_config(["DEMO"], "TEST01", legacy_opening=True)
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, old.costs)
            session = Session(AT.date(), True, True, ["DEMO"], [])
            engine = TradingEngine(old, session, INSTRUMENTS, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            engine.state.update(cash=2495000, trades=2, consecutive_losses=1)
            engine._save()
            with self.assertRaises(SafetyError):
                TradingEngine(new, session, INSTRUMENTS, broker, store, "paper")
            upgraded = TradingEngine(new, session, INSTRUMENTS, broker, store, "paper",
                                     allow_signal_profile_upgrade=True)
            self.assertEqual(upgraded.state["cash"], 2495000)
            self.assertEqual(upgraded.state["trades"], 2)
            self.assertEqual(upgraded.state["consecutive_losses"], 1)
            self.assertEqual(upgraded.config.risk, old.risk)
            self.assertEqual(upgraded.config.market.entry_start, "09:35")
            self.assertEqual(len(store.events("SIGNAL_PROFILE_UPGRADED")), 1)

    def test_profile_upgrade_never_allows_risk_increase(self):
        old = default_auto_config(["DEMO"], "TEST01", legacy_signals=True)
        new = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, old.costs)
            session = Session(AT.date(), True, True, ["DEMO"], [])
            TradingEngine(old, session, INSTRUMENTS, broker, store, "paper")
            with self.assertRaises(SafetyError):
                TradingEngine(replace(new, risk=replace(new.risk, risk_per_trade_bps=30)),
                              session, INSTRUMENTS, broker, store, "paper",
                              allow_signal_profile_upgrade=True)

    def test_next_day_can_upgrade_signals_and_opening_together_without_refunding_losses(self):
        old = default_auto_config(["DEMO"], "TEST01", legacy_opening=True, legacy_signals=True)
        new = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, old.costs)
            session = Session(AT.date(), True, True, ["DEMO"], [])
            engine = TradingEngine(old, session, INSTRUMENTS, broker, store, "paper")
            engine.state["cash"] = 2495000
            engine._save()
            upgraded = TradingEngine(new, replace(session, day=session.day+timedelta(days=1)),
                                     INSTRUMENTS, broker, store, "paper",
                                     allow_signal_profile_upgrade=True, allow_daily_universe_change=True)
            self.assertEqual(upgraded.state["cash"], 2495000)
            self.assertEqual(upgraded.config.strategy.benchmark_alignment, "relative_strength")
            self.assertEqual(upgraded.config.market.entry_start, "09:25")


if __name__ == "__main__":
    unittest.main()
