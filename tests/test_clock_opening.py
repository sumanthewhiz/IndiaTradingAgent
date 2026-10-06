from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.autonomy import POLICY_VERSION, PreparationBlocked, account_directory, default_auto_config, prepare_session
from india_trader.broker import PaperBroker
from india_trader.core import Config, IST, Instrument, SafetyError, Session, Tick
from india_trader.engine import CLOCK_HALT, TradingEngine
from india_trader.market import Bar, SignalAgent, Tape
from india_trader.pre_market import GLOBAL_RELEASES, global_context, premarket_candidates, rank_premarket
from india_trader.runtime import decode_tick
from india_trader.storage import Store

AT = datetime(2026, 9, 25, 9, 45, tzinfo=IST)


class ClockTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "state.db")
        self.config = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
        self.instruments = {
            "DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
            "INDEX": Instrument("INDEX", 2, 1, 1, 10**12, True),
        }
        self.broker = PaperBroker(2500000, self.config.costs)
        self.session = Session(AT.date(), True, True, ["DEMO"], [])
        self.engine = TradingEngine(self.config, self.session, self.instruments,
                                    self.broker, self.store, "paper")
        self.engine.reconcile(self.broker.snapshot(AT), AT)

    def tearDown(self):
        self.store.__exit__()
        self.temp.cleanup()

    def quote(self, symbol, at, volume=100):
        return Tick(symbol, at, 10000, 9999, 10001, volume, 1000, 1000)

    def restart_faulted(self):
        self.engine.halt(CLOCK_HALT, AT)
        self.engine = TradingEngine(self.config, self.session, self.instruments,
                                    self.broker, self.store, "paper")
        self.engine.reconcile(self.broker.snapshot(AT), AT)

    def samples(self, until=4):
        for second in range(until):
            received = AT + timedelta(seconds=second)
            for symbol in self.instruments:
                self.engine.on_tick(self.quote(symbol, received - timedelta(milliseconds=200), 100+second),
                                    received, received)

    def test_bad_future_tick_is_rejected_before_mutating_quotes_or_candles(self):
        self.engine.on_tick(self.quote("DEMO", AT + timedelta(seconds=2)), AT)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)
        self.assertNotIn("DEMO", self.engine.quotes)
        self.assertIsNone(self.engine.signals.tapes["DEMO"].latest)
        self.assertIsNone(self.engine.signals.tapes["DEMO"].bar)
        self.assertEqual(self.store.events("CLOCK_SKEW")[0]["ahead_seconds"], 2)
        self.assertEqual(self.broker.counter, 0)

    def test_queue_delay_cannot_make_a_future_received_tick_valid(self):
        self.engine.on_tick(self.quote("DEMO", AT + timedelta(seconds=2)),
                            AT, AT + timedelta(seconds=10))
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)
        self.assertNotIn("DEMO", self.engine.quotes)

    def test_existing_one_second_tolerance_is_not_widened(self):
        self.engine.on_tick(self.quote("DEMO", AT + timedelta(seconds=1)), AT)
        self.assertEqual(self.engine.state["halt"], "")
        self.engine.on_tick(self.quote("DEMO", AT + timedelta(seconds=1, microseconds=1)), AT)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)

    def test_correct_clock_does_not_automatically_clear_a_new_fault_in_same_run(self):
        self.engine.on_tick(self.quote("DEMO", AT + timedelta(seconds=2)), AT)
        self.samples(5)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)
        self.assertEqual(self.store.events("CLOCK_RECOVERED"), [])

    def test_restart_recovers_exact_clock_fault_after_multisymbol_fresh_samples(self):
        self.restart_faulted()
        before = {name: self.engine.state[name] for name in ("cash", "capital", "trades", "buy_turnover")}
        self.samples(3)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)
        for symbol in self.instruments:
            receipt = AT + timedelta(seconds=3)
            self.engine.on_tick(self.quote(symbol, receipt - timedelta(milliseconds=200), 103), receipt)
        self.assertEqual(self.engine.state["halt"], "")
        self.assertEqual(len(self.store.events("CLOCK_RECOVERED")), 1)
        self.assertEqual({name: self.engine.state[name] for name in before}, before)
        self.assertEqual(self.broker.counter, 0)

    def test_missing_instrument_or_stale_reconciliation_cannot_unlock(self):
        self.restart_faulted()
        for second in range(4):
            receipt = AT + timedelta(seconds=second)
            self.engine.on_tick(self.quote("DEMO", receipt, 100+second), receipt)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)
        self.engine.snapshot_at = AT - timedelta(seconds=30)
        self.samples(4)
        self.assertEqual(self.engine.state["halt"], CLOCK_HALT)

    def test_risk_halt_and_quarantine_are_not_cleared_as_clock_recovery(self):
        self.engine.halt("Daily loss/profit-giveback threshold reached.", AT)
        self.engine = TradingEngine(self.config, self.session, self.instruments,
                                    self.broker, self.store, "paper")
        self.samples()
        self.assertEqual(self.engine.state["halt"], "Daily loss/profit-giveback threshold reached.")
        self.assertEqual(self.store.events("CLOCK_RECOVERED"), [])

    def test_sdk_naive_timestamp_conversion_preserves_unix_instant(self):
        epoch = AT.timestamp()
        raw = {"instrument_token": 2, "exchange_timestamp": datetime.fromtimestamp(epoch),
               "last_price": 25000}
        decoded = decode_tick(raw, {2: self.instruments["INDEX"]})
        self.assertEqual(decoded.at.timestamp(), epoch)
        self.assertEqual(decoded.at.utcoffset(), timedelta(hours=5, minutes=30))
        raw["exchange_timestamp"] = AT.astimezone(timezone.utc)
        self.assertEqual(decode_tick(raw, {2: self.instruments["INDEX"]}).at, AT)


class OpeningTests(unittest.TestCase):
    def test_default_manual_strategy_retains_fifteen_minute_range(self):
        self.assertEqual(Config().strategy.opening_range_minutes, 15)
        self.assertEqual(Config().market.entry_start, "09:35")

    def test_auto_strategy_cannot_enter_in_the_preopen_auction(self):
        config = default_auto_config(["DEMO"], "TEST01")
        self.assertEqual(config.market.entry_start, "09:25")
        self.assertEqual(config.strategy.opening_range_minutes, 5)
        self.assertEqual(config.risk, Config().risk)
        for start in ("09:00", "09:15", "09:20"):
            with self.subTest(start=start), self.assertRaises(ValueError):
                replace(config, market=replace(config.market, entry_start=start)).validate()

    def test_five_minute_range_requires_the_next_completed_confirmation_bar(self):
        config = default_auto_config(["DEMO"], "TEST01")
        instruments = {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                       "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True)}
        signals = SignalAgent(config, instruments)
        opening = AT.replace(hour=9, minute=15)
        volume = 100
        candidates = []
        for second in range(601):
            at = opening + timedelta(seconds=second)
            signals.ingest(Tick("NIFTY 50", at, 2500000+second, 2500000+second, 2500000+second, 0, 0, 0))
            volume += 10 if second < 300 else 100
            price = 10000 + second % 11 if second < 300 else 10020 + min(second-300, 20)
            candidate = signals.ingest(Tick("DEMO", at, price, price-1, price+1, volume, 1000, 1000))
            if candidate:
                candidates.append(candidate)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].at.time().isoformat(), "09:25:00")

    def test_legacy_config_schema_migration_preserves_every_risk_counter(self):
        legacy = default_auto_config(["DEMO"], "TEST01", legacy_opening=True)
        with tempfile.TemporaryDirectory() as folder, Store(Path(folder)/"state.db") as store:
            broker = PaperBroker(2500000, legacy.costs)
            instruments = {"DEMO": Instrument("DEMO",1,1,9000,11000),
                           "NIFTY 50": Instrument("NIFTY 50",2,1,1,10**12,True)}
            session = Session(AT.date(), True, True, ["DEMO"], [])
            engine = TradingEngine(legacy, session, instruments, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            raw = asdict(legacy)
            raw["strategy"].pop("opening_range_minutes")
            store.put("configuration", raw)
            state = store.get("engine")
            state.update(config_hash="legacy-schema-hash", cash=2450000, trades=2, halt=CLOCK_HALT)
            store.put("engine", state)
            restored = TradingEngine(legacy, session, instruments, broker, store, "paper")
            self.assertEqual(restored.state["cash"], 2450000)
            self.assertEqual(restored.state["trades"], 2)
            self.assertEqual(restored.state["halt"], CLOCK_HALT)
            self.assertEqual(len(store.events("CONFIG_SCHEMA_NORMALIZED")), 1)
            with self.assertRaises(SafetyError):
                TradingEngine(default_auto_config(["DEMO"], "TEST01"), session, instruments, broker, store,
                              "paper", allow_daily_universe_change=True)
            next_day = replace(session, day=session.day+timedelta(days=1))
            upgraded = TradingEngine(default_auto_config(["DEMO"], "TEST01"), next_day, instruments, broker,
                                     store, "paper", allow_daily_universe_change=True)
            self.assertEqual(upgraded.state["cash"], 2450000)
            self.assertEqual(upgraded.config.market.entry_start, "09:25")


class GlobalContextTests(unittest.TestCase):
    def feed(self, title, published):
        return (f"<rss><channel><item><title>{title}</title><pubDate>{published.isoformat()}</pubDate>"
                "</item></channel></rss>").encode()

    def test_recent_official_policy_cue_blocks_only_opening_reaction_window(self):
        at = AT.replace(hour=8, minute=30)
        data = self.feed("Federal Reserve issues FOMC statement", at-timedelta(hours=12))
        with tempfile.TemporaryDirectory() as folder:
            context = global_context(Path(folder), at, fetch=lambda _: data)
        self.assertEqual(context["opening_blackout"]["start"], at.replace(hour=9, minute=15).isoformat())
        self.assertEqual(context["opening_blackout"]["end"], at.replace(hour=9, minute=45).isoformat())
        self.assertTrue(context["gaps"])
        self.assertTrue(all(item["healthy"] for item in context["sources"].values()))

    def test_unavailable_global_source_is_visible_and_not_synthesized(self):
        with tempfile.TemporaryDirectory() as folder:
            def unavailable(_):
                raise OSError("source down")
            context = global_context(Path(folder), AT, fetch=unavailable)
        self.assertFalse(any(item["healthy"] for item in context["sources"].values()))
        self.assertEqual(context["headlines"], [])
        self.assertIsNone(context["opening_blackout"])
        self.assertIn("limited", context["coverage"])

    def test_successful_context_is_cached_for_thirty_minutes_and_not_forever(self):
        fetch = Mock(return_value=self.feed("Routine official release", AT-timedelta(hours=2)))
        with tempfile.TemporaryDirectory() as folder:
            global_context(Path(folder), AT, fetch)
            global_context(Path(folder), AT+timedelta(minutes=5), fetch)
            self.assertEqual(fetch.call_count, len(GLOBAL_RELEASES))
            global_context(Path(folder), AT+timedelta(minutes=31), fetch)
            self.assertEqual(fetch.call_count, 2*len(GLOBAL_RELEASES))

    def test_previous_close_premarket_screen_does_not_require_or_invent_executable_depth(self):
        quote = {"ohlc": {"close": 100}, "volume": 100000}
        candidates = premarket_candidates({"NSE:DEMO": quote}, ["DEMO"], 100000)
        self.assertEqual(candidates[0]["price_paise"], 10000)
        metrics = {"atr_bps":150, "last_session":"2026-09-24","last_close_paise":10000,
                   "average_turnover_paise":2000000000,"return_5d_bps":100,"return_20d_bps":200}
        ranked = rank_premarket(candidates, {"DEMO":metrics}, metrics, {"DEMO":"IT"},set(),{},AT)
        self.assertEqual(ranked[0]["quote_kind"], "prior close / research only")
        self.assertIsNone(ranked[0]["spread_bps"])


class PreMarketLifecycleTests(unittest.TestCase):
    def test_research_precedes_market_and_cannot_launch_the_worker_before_nine_fifteen(self):
        at = [AT.replace(hour=8, minute=30)]
        keys = {"ai_api_key": "test-ai", "broker_api_key": "test-broker",
                "broker_access_token": "test-token"}
        vault = Mock()
        vault.load.return_value = {"auto_start": True, "consent_version": "auto-live-v1", "keys": keys}
        quote = {"last_price": 100, "ohlc": {"close": 100, "open": 100}, "volume": 100000}
        calls = []

        class ReadOnlyHTTP:
            def __init__(self, config, allow_orders, **kwargs):
                self.config = config
                if allow_orders:
                    raise AssertionError("Preparation must not authorize order routes.")

            def request(self, method, path, **kwargs):
                calls.append((method, path))
                if method != "GET":
                    raise AssertionError("Pre-market preparation attempted a write.")
                if path == "/user/profile":
                    return {"broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["NSE"],
                            "products": ["CNC"], "order_types": ["LIMIT", "SL"]}
                if path == "/portfolio/holdings":
                    return []
                if path == "/user/margins/equity":
                    return {"available": {"cash": 25000, "live_balance": 25000, "collateral": 0},
                            "net": 25000, "utilised": {}}
                if path == "/instruments/NSE":
                    return ("tradingsymbol,exchange,segment,instrument_type,lot_size,expiry,instrument_token,name\n"
                            "DEMO,NSE,NSE,EQ,1,,1,Demo Limited\n"
                            "NIFTY 50,NSE,INDICES,EQ,1,,2,NIFTY 50\n")
                if path == "/quote":
                    return {"NSE:DEMO": quote, "NSE:NIFTY 50": quote}
                raise AssertionError(path)

        history = {"atr_bps": 150, "last_session": "2026-09-24", "last_close_paise": 10000,
                   "average_turnover_paise": 2000000000, "return_5d_bps": 50, "return_20d_bps": 100}
        news = Mock()
        news.health, news.excluded, news.catalysts = {}, set(), {}
        instruments = {"DEMO": Instrument("DEMO",1,1,9000,11000),
                       "NIFTY 50": Instrument("NIFTY 50",2,1,1,10**12,True)}
        context = {"coverage": "test official releases", "opening_blackout": None, "gaps": ["Test gap"]}
        with tempfile.TemporaryDirectory() as temporary, \
             patch("india_trader.autonomy.now_ist", side_effect=lambda: at[0]), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", ReadOnlyHTTP), \
             patch("india_trader.autonomy.verify_gemini_key"), \
             patch("india_trader.autonomy.constituents", return_value=(["DEMO"],{},{"DEMO":"IT"},"test universe")), \
             patch("india_trader.autonomy.AutomaticNews", return_value=news), \
             patch("india_trader.autonomy.cached_daily_history", return_value=history) as histories, \
             patch("india_trader.autonomy.load_kite_instruments", return_value=instruments) as instrument_load, \
             patch("india_trader.autonomy.closed_intraday_bars") as bars, \
             patch("india_trader.pre_market.global_context", return_value=context):
            root = Path(temporary)
            with self.assertRaises(PreparationBlocked) as blocked:
                prepare_session(root, root, vault)
            self.assertEqual(blocked.exception.state, "PREMARKET_READY")
            workspace = account_directory(root, "TEST01")
            saved = json.loads((workspace/"plan.json").read_text())
            self.assertTrue(saved["selection"]["pre_market"])
            self.assertEqual(saved["config"]["market"]["entry_start"], "09:25")
            self.assertEqual(saved["global_context"], context)
            self.assertFalse((workspace/"live.db").exists())
            instrument_load.assert_not_called()
            bars.assert_not_called()
            count = histories.call_count
            at[0] = AT.replace(hour=9, minute=0)
            with self.assertRaises(PreparationBlocked) as blocked:
                prepare_session(root, root, vault)
            self.assertEqual(blocked.exception.state, "PREMARKET_READY")
            self.assertEqual(histories.call_count, count)
            at[0] = AT.replace(hour=9, minute=15)
            plan = prepare_session(root, root, vault)
            self.assertTrue(plan["selection_reused"])
            self.assertEqual(plan["selected"], ["DEMO"])
            self.assertEqual(plan["warmup"], {})
            self.assertEqual(plan["config"]["risk"]["capital_rupees"], 25000)
            bars.assert_not_called()
            instrument_load.assert_called_once()
            self.assertTrue(all(method == "GET" for method, _ in calls))


if __name__ == "__main__":
    unittest.main()
