from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from india_trader.autonomy import default_auto_config
from india_trader.broker import PaperBroker
from india_trader.core import Candidate, Config, IST, Instrument, SafetyError, Session, Tick
from india_trader.engine import TradingEngine
from india_trader.market import Bar, SignalAgent
from india_trader.storage import Store

AT = datetime(2026, 9, 25, 13, 35, tzinfo=IST)
INSTRUMENTS = {
    "DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
    "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True),
}


def prepared_signal(config, *, at=AT, reference_last=2499975, volume_increment=5500,
                    first_stock_price=10000):
    signals = SignalAgent(config, INSTRUMENTS)
    start = at.replace(hour=9, minute=15)
    current = at - timedelta(minutes=5)
    count = int((current - start).total_seconds() // 300)
    stocks = [Bar(start+timedelta(minutes=5*i), first_stock_price if i == 0 else 10020,
                  max(10060, first_stock_price) if i == 0 else 10060,
                  9995, 10020, 1000) for i in range(count)]
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

    def test_balanced_upgrade_preserves_all_financial_state_and_pauses(self):
        old = default_auto_config(["DEMO"], "TEST01", legacy_participation=True)
        new = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, old.costs)
            session = Session(AT.date(), True, True, ["DEMO"], [])
            engine = TradingEngine(old, session, INSTRUMENTS, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            engine.state.update(cash=2499900, trades=1, consecutive_losses=1,
                                pauses={"*": (AT+timedelta(minutes=5)).isoformat()})
            engine._save()
            before = {k: v for k, v in engine.state.items() if k != "config_hash"}
            with self.assertRaises(SafetyError):
                TradingEngine(new, session, INSTRUMENTS, broker, store, "paper")
            upgraded = TradingEngine(new, session, INSTRUMENTS, broker, store, "paper",
                                     allow_signal_profile_upgrade=True)
            self.assertEqual(before, {k: v for k, v in upgraded.state.items() if k != "config_hash"})
            self.assertEqual(upgraded.config.strategy.participation_profile, "balanced")
            self.assertEqual(upgraded.config.risk, old.risk)
            self.assertEqual(upgraded.config.execution, old.execution)
            self.assertEqual(upgraded.config.costs, old.costs)

    def test_balanced_upgrade_is_not_applied_to_owned_or_quarantined_state(self):
        old = default_auto_config(["DEMO"], "TEST01", legacy_participation=True)
        new = default_auto_config(["DEMO"], "TEST01")
        for state in ("owned", "quarantine"):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as temporary, \
                 Store(Path(temporary)/"state.db") as store:
                broker = PaperBroker(2500000, old.costs)
                session = Session(AT.date(), True, True, ["DEMO"], [])
                engine = TradingEngine(old, session, INSTRUMENTS, broker, store, "paper")
                engine.reconcile(broker.snapshot(AT), AT)
                if state == "owned":
                    engine.observe_stream_quote(Tick("DEMO",AT,10065,10064,10066,1000,10000,10000),AT,AT)
                    engine.observe_stream_quote(Tick("NIFTY 50",AT,2500000,2500000,2500000,0,0,0),AT,AT)
                    for source in old.news.required_sources:
                        engine.heartbeat_news(AT, source)
                    self.assertTrue(engine.consider(Candidate("DEMO","orb",AT,9979),AT))
                else:
                    engine.state["quarantine"] = True
                    engine._save()
                with self.assertRaises(SafetyError):
                    TradingEngine(new, session, INSTRUMENTS, broker, store, "paper",
                                  allow_signal_profile_upgrade=True)


class BalancedParticipationTests(unittest.TestCase):
    def test_balanced_accepts_moderate_volume_while_selective_remains_available(self):
        for selective, expected in ((True, False), (False, True)):
            with self.subTest(selective=selective):
                config = default_auto_config(["DEMO"], "TEST01", legacy_participation=selective)
                signals, tick = prepared_signal(config, volume_increment=1300)
                self.assertEqual(signals.ingest(tick) is not None, expected)
        self.assertEqual(Config().strategy.participation_profile, "selective")

    def test_balanced_volume_threshold_is_exact_not_just_nonzero_volume(self):
        config = default_auto_config(["DEMO"], "TEST01")
        for volume, expected in ((1199, False), (1200, True)):
            with self.subTest(volume=volume):
                signals, tick = prepared_signal(config, volume_increment=volume)
                self.assertEqual(signals.ingest(tick) is not None, expected)

    def test_recent_strength_can_qualify_a_recovery_below_the_session_open(self):
        balanced = default_auto_config(["DEMO"], "TEST01")
        selective = default_auto_config(["DEMO"], "TEST01", legacy_participation=True)
        old, old_tick = prepared_signal(selective, first_stock_price=10150)
        self.assertIsNone(old.ingest(old_tick))
        signals, tick = prepared_signal(balanced, first_stock_price=10150)
        self.assertIsNotNone(signals.ingest(tick))
        self.assertEqual(signals.decision["alignment_path"], "recent_relative_strength")
        self.assertGreater(signals.decision["recent_relative_strength_bps"], 10)
        self.assertTrue(signals.decision["checks"]["stock_above_vwap"])

    def test_recovery_needs_aligned_complete_history_and_does_not_buy_a_market_plunge(self):
        config = default_auto_config(["DEMO"], "TEST01")
        for invalid in ("incomplete", "plunge"):
            with self.subTest(invalid=invalid):
                signals, tick = prepared_signal(config, first_stock_price=10150,
                                                reference_last=2480000 if invalid=="plunge" else 2499975)
                if invalid == "incomplete":
                    signals.tapes["DEMO"].bars[-1].complete = False
                self.assertIsNone(signals.ingest(tick))
                self.assertFalse(signals.decision["checks"]["market_alignment"])

    def test_recent_alignment_does_not_depend_on_which_symbol_ticks_first_after_boundary(self):
        config = default_auto_config(["DEMO"], "TEST01")
        signals, tick = prepared_signal(config, first_stock_price=10150)
        reference = signals.tapes["NIFTY 50"]
        reference.bar = reference.bars.pop()
        reference.latest = replace(reference.latest, at=AT-timedelta(seconds=1))
        bars_before = list(reference.bars)
        active_before = reference.bar
        self.assertIsNotNone(signals.ingest(tick))
        self.assertEqual(signals.decision["alignment_path"], "recent_relative_strength")
        self.assertEqual(reference.bars, bars_before)
        self.assertIs(reference.bar, active_before)
        self.assertLess(reference.latest.at, AT)

    def test_momentum_uses_twelve_complete_bars_but_never_trades_an_incomplete_warmup(self):
        config = default_auto_config(["DEMO"], "TEST01")
        for minute, expected in ((10, False), (15, True)):
            with self.subTest(minute=minute):
                signals, tick = prepared_signal(config, at=AT.replace(hour=10, minute=minute),
                                                volume_increment=1300)
                self.assertEqual(signals.ingest(tick) is not None, expected)
                self.assertFalse(signals.decision["setup_checks"]["orb"]["breakout_volume"])
                self.assertEqual(signals.decision["momentum_minimum_bars"], 12)
        old, tick = prepared_signal(default_auto_config(["DEMO"],"TEST01",legacy_participation=True),
                                    at=AT.replace(hour=10,minute=15),volume_increment=1300)
        self.assertIsNone(old.ingest(tick))

    def test_cost_efficient_small_trade_can_qualify_without_increasing_cash_or_risk(self):
        results = []
        for selective in (True, False):
            config = default_auto_config(["DEMO"], "TEST01", legacy_participation=selective)
            with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
                broker = PaperBroker(100000, config.costs)
                engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                       INSTRUMENTS, broker, store, "paper")
                engine.reconcile(broker.snapshot(AT), AT)
                engine.observe_stream_quote(Tick("DEMO",AT,10065,10064,10066,1000,10000,10000),AT,AT)
                engine.observe_stream_quote(Tick("NIFTY 50",AT,2500000,2500000,2500000,0,0,0),AT,AT)
                for source in config.news.required_sources:
                    engine.heartbeat_news(AT, source)
                attempted = engine.consider(Candidate("DEMO","momentum_breakout",AT,10020),AT)
                results.append(attempted)
                if attempted:
                    plan = store.events("ENTRY_PLAN")[-1]
                    self.assertGreaterEqual(plan["modeled_target_net"], plan["modeled_risk"])
                    self.assertGreaterEqual(plan["modeled_target_net"], plan["modeled_fees"]*2)
                    self.assertLessEqual(plan["modeled_risk"], 250)
                    self.assertLessEqual(plan["quantity"]*plan["limit"] + config.costs.fee(
                        "BUY", plan["quantity"]*plan["limit"]), 25000)
                    self.assertEqual(engine.state["capital"], 100000)
                else:
                    self.assertLess(engine.last_rejection_details["net_reward_r"], 1.5)
                    self.assertGreater(engine.last_rejection_details["net_reward_r"], 1.0)
        self.assertEqual(results, [False, True])

    def test_unaffordable_share_and_fee_dominated_target_are_still_rejected_with_numbers(self):
        config = default_auto_config(["DEMO"], "TEST01")
        for price, stop, detail in ((10065,10058,"modeled_target_net_paise"),
                                   (25065,24900,"one_share_limit_paise")):
            with self.subTest(price=price), tempfile.TemporaryDirectory() as temporary, \
                 Store(Path(temporary)/"state.db") as store:
                instruments = {**INSTRUMENTS, "DEMO": Instrument("DEMO",1,1,9000,30000)}
                broker = PaperBroker(100000, config.costs)
                engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                       instruments, broker, store, "paper")
                engine.reconcile(broker.snapshot(AT), AT)
                engine.observe_stream_quote(Tick("DEMO",AT,price,price-1,price+1,1000,10000,10000),AT,AT)
                engine.observe_stream_quote(Tick("NIFTY 50",AT,2500000,2500000,2500000,0,0,0),AT,AT)
                for source in config.news.required_sources:
                    engine.heartbeat_news(AT, source)
                self.assertFalse(engine.consider(Candidate("DEMO","momentum_breakout",AT,stop),AT))
                self.assertEqual(broker.counter, 0)
                self.assertEqual(engine.state["trades"], 0)
                self.assertIn(detail, store.events("CANDIDATE_REJECTED")[-1]["details"])

    def test_balanced_paper_round_trip_requires_confirmed_protection_cancellation(self):
        config = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(100000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                   INSTRUMENTS, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            engine.observe_stream_quote(Tick("DEMO",AT,10065,10064,10066,1000,10000,10000),AT,AT)
            engine.observe_stream_quote(Tick("NIFTY 50",AT,2500000,2500000,2500000,0,0,0),AT,AT)
            for source in config.news.required_sources:
                engine.heartbeat_news(AT, source)
            self.assertTrue(engine.consider(Candidate("DEMO","momentum_breakout",AT,10020),AT))
            fill_at = AT+timedelta(seconds=1)
            fill = Tick("DEMO",fill_at,10065,10064,10066,2000,10000,10000)
            broker.on_tick(fill)
            engine.on_tick(fill, fill_at)
            engine.reconcile(broker.snapshot(fill_at), fill_at)
            engine.reconcile(broker.snapshot(fill_at), fill_at)
            guard = next(order for order in engine.orders if order.purpose == "protect")
            target = engine.position.target
            target_at = AT+timedelta(seconds=2)
            target_tick = Tick("DEMO",target_at,target+1,target,target+2,3000,10000,10000)
            broker.on_tick(target_tick)
            engine.on_tick(target_tick, target_at)
            self.assertTrue(guard.cancel_requested)
            self.assertFalse(any(order.purpose == "exit" for order in engine.orders))
            engine.reconcile(broker.snapshot(target_at), target_at)
            self.assertEqual(guard.status, "CANCELLED")
            self.assertTrue(any(order.purpose == "exit" for order in engine.orders))
            exit_at = AT+timedelta(seconds=3)
            last_tick = replace(target_tick, at=exit_at, volume=4000)
            broker.on_tick(last_tick)
            engine.on_tick(last_tick, exit_at)
            engine.reconcile(broker.snapshot(exit_at), exit_at)
            self.assertTrue(engine.flat)
            self.assertEqual(engine.state["capital"], 100000)
            self.assertEqual(engine.state["trades"], 1)
            self.assertEqual(engine.state["completed_trades"], 1)
            self.assertGreater(store.events("TRADE_CLOSED")[0]["net_paise"], 0)


if __name__ == "__main__":
    unittest.main()
