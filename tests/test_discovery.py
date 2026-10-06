from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.autonomy import default_auto_config
from india_trader.broker import KiteBroker, PaperBroker
from india_trader.core import Candidate, Config, IST, Instrument, Order, Position, SafetyError, Session, Tick
from india_trader.discovery import (
    ADMISSIONS_PER_SCAN, IntradayDiscovery, PreparedCandidate, apply_scan, restore_symbols,
)
from india_trader.engine import MAX_TRACKED_STOCKS, TradingEngine
from india_trader.events import EventAgent
from india_trader.market import Bar
from india_trader.storage import Store

AT = datetime(2026, 9, 28, 11, 0, 10, tzinfo=IST)


def history():
    return {"atr_bps": 150, "last_session": "2026-09-25", "last_close_paise": 10000,
            "return_5d_bps": 150, "return_20d_bps": 200, "average_turnover_paise": 2000000000}


def bars(at=AT):
    start = at.replace(hour=9, minute=15, second=0, microsecond=0)
    count = int((at.replace(second=0, microsecond=0) - start).total_seconds() // 300)
    return [Bar(start+timedelta(minutes=5*i), 10000, 10020, 9990, 10010, 1000) for i in range(count)]


def row(symbol, token):
    return {"tradingsymbol": symbol, "instrument_token": str(token), "exchange": "NSE",
            "segment": "NSE", "instrument_type": "EQ", "lot_size": "1", "expiry": "", "tick_size": "0.01"}


def quote(at=AT, price=100, volume=1000000):
    return {"last_price": price, "timestamp": at.isoformat(), "volume": volume,
            "depth": {"buy": [{"price": price-.01, "quantity": 1000}],
                      "sell": [{"price": price+.01, "quantity": 1000}]},
            "lower_circuit_limit": 90, "upper_circuit_limit": 110,
            "ohlc": {"open": 100, "close": 100}}


class DiscoveryCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.store = Store(self.directory / "state.db")
        self.config = default_auto_config(["INITIAL"], "TEST01")
        self.broker = PaperBroker(2500000, self.config.costs)
        self.instruments = {"INITIAL": Instrument("INITIAL", 1, 1, 9000, 11000),
                            "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True)}
        self.session = Session(AT.date(), True, True, ["INITIAL"], [])
        self.engine = TradingEngine(self.config, self.session, self.instruments, self.broker,
                                    self.store, "paper", enable_discovery=True)
        self.engine.reconcile(self.broker.snapshot(AT), AT)
        self.engine.discovery_universe = {"NEW": 3, "OTHER": 4}

    def tearDown(self):
        self.store.__exit__()
        self.temporary.cleanup()

    def prepared(self, symbol="NEW", token=3, at=AT):
        return PreparedCandidate(Instrument(symbol, token, 1, 9000, 11000), bars(at), at,
                                 {"symbol": symbol, "score": 5.0, "reason": "Synthetic ranked opportunity"})

    def result(self, prepared=None, at=AT):
        prepared = [self.prepared(at=at)] if prepared is None else prepared
        return {"kind": "scan", "day": at.date().isoformat(), "at": at.isoformat(),
                "source": "test universe", "universe_count": 200, "trigger": "scheduled_market_scan",
                "liquid_candidates": 20, "history_ready": 15, "history_reads": 1,
                "ranked": [x.ranking for x in prepared], "wanted": [x.instrument.symbol for x in prepared],
                "prepared": prepared, "omissions": [], "news_ready": True}

    def test_new_stock_is_admitted_without_order_and_uses_all_original_risk_gates(self):
        before = {name: self.engine.state[name] for name in ("cash", "capital", "trades", "buy_turnover")}
        subscribe = Mock()
        view = apply_scan(self.result(), self.engine, AT, subscribe, Mock())
        self.assertIn("NEW", self.engine.trade_symbols)
        self.assertIn("NEW", self.engine.signals.trade_symbols)
        self.assertTrue(self.engine.permits_entry("NEW", AT))
        self.assertEqual(self.config.market.symbols, ["INITIAL"])
        self.assertEqual(self.broker.counter, 0)
        self.assertEqual(before, {name: self.engine.state[name] for name in before})
        self.assertEqual(view["admitted_this_scan"], ["NEW"])
        self.assertTrue(self.engine.signals.tapes["NEW"].seeded)
        subscribe.assert_called_once()
        self.assertFalse(self.engine.consider(Candidate("NEW", "momentum_breakout", AT, 9979), AT))
        self.assertEqual(self.broker.counter, 0)
        self.assertIn("NEW", self.store.get("discovery_members")["admitted"])

    def test_duplicate_scan_result_does_not_resubscribe_or_order_twice(self):
        subscribe = Mock()
        apply_scan(self.result(), self.engine, AT, subscribe, Mock())
        apply_scan(self.result(), self.engine, AT, subscribe, Mock())
        subscribe.assert_called_once()
        self.assertEqual(len(self.store.events("DISCOVERY_ADMITTED")), 1)
        self.assertEqual(self.broker.counter, 0)

    def test_discovered_noninitial_stock_can_place_a_qualified_paper_entry(self):
        start = AT.replace(second=0)
        self.assertTrue(self.engine.admit_discovered(self.prepared().instrument, bars(start), start, "test"))
        self.engine.signals.tapes["NIFTY 50"].seed(bars(start), start)
        self.engine.on_tick(Tick("NIFTY 50",start,10080,10080,10080,0,0,0),start)
        self.engine.on_tick(Tick("NEW",start,10025,10024,10026,100000,10000,10000,10015),start)
        end = start+timedelta(minutes=5)
        self.engine.on_tick(Tick("NEW",end-timedelta(seconds=1),10090,10089,10091,105000,10000,10000,10015),
                            end-timedelta(seconds=1))
        self.engine.reconcile(self.broker.snapshot(end),end)
        for source in self.config.news.required_sources:
            self.engine.heartbeat_news(end,source)
        self.engine.on_tick(Tick("NIFTY 50",end,10080,10080,10080,0,0,0),end)
        self.engine.on_tick(Tick("NEW",end,10091,10090,10092,105010,10000,10000,10015),end)
        self.assertEqual(self.broker.counter,1)
        self.assertEqual(self.engine.position.symbol,"NEW")
        self.assertEqual(self.engine.orders[0].symbol,"NEW")
        self.assertEqual(self.engine.state["trades"],1)
        self.assertNotIn("NEW",self.config.market.symbols)
        self.assertLessEqual(self.engine.position.initial_risk,6250)
        self.assertLessEqual(self.engine.orders[0].quantity*self.engine.orders[0].price,625000)

    def test_unknown_metadata_wrong_token_reference_and_news_exclusion_are_rejected(self):
        for instrument in (
            Instrument("UNKNOWN", 5, 1, 9000, 11000), Instrument("NEW", 99, 1, 9000, 11000),
            Instrument("NEW", 3, 1, 9000, 11000, True),
        ):
            self.assertFalse(self.engine.admit_discovered(instrument, bars(), AT, "test"))
        self.engine.discovery_excluded.add("NEW")
        self.assertFalse(self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test"))
        self.assertNotIn("NEW", self.engine.trade_symbols)

    def test_cutoff_stale_results_and_crossed_candle_boundaries_do_not_admit(self):
        subscribe = Mock()
        apply_scan(self.result(), self.engine, AT+timedelta(seconds=46), subscribe, Mock())
        self.assertNotIn("NEW", self.engine.trade_symbols)
        crossed = self.result(at=AT.replace(minute=4, second=50))
        apply_scan(crossed, self.engine, AT.replace(minute=5, second=1), subscribe, Mock())
        self.assertNotIn("NEW", self.engine.trade_symbols)
        cutoff = AT.replace(hour=14, minute=30)
        apply_scan(self.result(at=cutoff), self.engine, cutoff, subscribe, Mock())
        subscribe.assert_not_called()
        self.assertEqual(self.broker.counter, 0)

    def test_paused_or_halted_engine_cannot_admit_new_entry_symbols(self):
        self.engine.pause(["NEW"], AT+timedelta(minutes=30), AT, "issuer event")
        self.assertFalse(self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test"))
        self.engine.state["pauses"].clear()
        self.engine.halt("Daily loss/profit-giveback threshold reached.", AT)
        self.assertFalse(self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test"))

    def test_owned_or_pending_order_symbol_is_never_removed(self):
        self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test")
        self.engine.position = Position("NEW", "momentum_breakout", AT.isoformat(), 9979, 10300, quantity=1)
        self.assertFalse(self.engine.retire_discovered("NEW", AT, "test"))
        self.engine.position = None
        self.engine.orders.append(Order("abcdef12", "NEW", "protect", "SELL", 1, 9970, 9980, AT.isoformat()))
        self.assertFalse(self.engine.retire_discovered("NEW", AT, "test"))
        self.engine.orders.clear()
        self.assertTrue(self.engine.retire_discovered("NEW", AT, "test"))
        self.assertNotIn("NEW", self.engine.signals.tapes)
        self.assertNotIn("NEW", self.engine.trade_symbols)
        self.assertFalse(self.engine.retire_discovered("INITIAL", AT, "test"))

    def test_membership_restores_same_day_without_resetting_cash_or_counters(self):
        self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test")
        self.engine.state.update(cash=2450000, trades=2, consecutive_losses=1)
        self.engine._save()
        extras = restore_symbols(self.config, self.store, self.session.day.isoformat())
        self.assertEqual(extras, {"NEW"})
        restored = TradingEngine(self.config, self.session, dict(self.instruments), self.broker,
                                 self.store, "paper", enable_discovery=True)
        self.assertIn("NEW", restored.trade_symbols)
        self.assertEqual(restored.state["cash"], 2450000)
        self.assertEqual(restored.state["trades"], 2)
        self.assertEqual(restored.state["consecutive_losses"], 1)

    def test_next_day_restores_owned_discovery_symbol_even_if_new_universe_omits_it(self):
        self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test")
        next_day = (AT+timedelta(days=1)).date().isoformat()
        self.assertEqual(restore_symbols(self.config, self.store, next_day), set())
        self.engine.position = Position("NEW", "momentum_breakout", AT.isoformat(), 9900, 10300, quantity=1)
        self.engine._save()
        self.assertEqual(restore_symbols(self.config, self.store, next_day), {"NEW"})
        members = self.store.get("discovery_members")
        members["admitted"].clear()
        self.store.put("discovery_members", members)
        with self.assertRaises(SafetyError):
            restore_symbols(self.config, self.store, next_day)

    def test_broker_allows_only_actual_admissions_not_arbitrary_discovery_candidates(self):
        http = Mock()
        http.config = self.config
        http.request.return_value = {"order_id": "test-order"}
        broker = KiteBroker(http, self.engine.instruments)
        broker.trade_symbols = self.engine.trade_symbols
        self.engine.admit_discovered(self.prepared().instrument, bars(), AT, "test")
        order = Order("abcdef12", "NEW", "entry", "BUY", 1, 10000, 0, AT.isoformat())
        self.assertEqual(broker.submit(order), "test-order")  # mocked endpoint, no live order
        self.engine.retire_discovered("NEW", AT, "test")
        self.assertNotIn("NEW", broker.trade_symbols)
        http.request.assert_called_once()

    def test_tracking_limit_and_ten_minute_rotation_preserve_seed_and_owned_stocks(self):
        old = AT-timedelta(minutes=15)
        for i in range(MAX_TRACKED_STOCKS-1):
            name, token = f"D{i}", i+10
            self.engine.discovery_universe[name] = token
            self.assertTrue(self.engine.admit_discovered(
                Instrument(name,token,1,9000,11000), bars(old), old, "test"))
        self.engine.position = Position("D0", "momentum_breakout", old.isoformat(), 9900, 10300, quantity=1)
        unsubscribe = Mock()
        apply_scan(self.result(), self.engine, AT, Mock(), unsubscribe)
        self.assertEqual(len(self.engine.trade_symbols), MAX_TRACKED_STOCKS)
        self.assertIn("NEW", self.engine.trade_symbols)
        self.assertIn("D0", self.engine.trade_symbols)
        self.assertIn("INITIAL", self.engine.trade_symbols)
        unsubscribe.assert_called_once()
        self.assertEqual(self.broker.counter, 0)

    def test_news_outside_initial_watchlist_is_audited_and_paused_without_spending_ai(self):
        ai = Mock()
        events = EventAgent(self.config, self.store, ai)
        events.allowed_symbols.add("NEW")
        events.accept({"type":"news", "source":"nse-announcements", "at":AT.isoformat(),
                       "symbols":["NEW"], "headline":"New issuer earnings results", "severity":"high", "public":True},
                      self.engine, AT)
        self.assertIn("NEW", self.engine.state["pauses"])
        ai.submit.assert_not_called()
        self.assertEqual(self.broker.counter, 0)


class ScannerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.store = Store(self.directory / "state.db")
        self.config = default_auto_config(["INITIAL"], "TEST01")
        self.http = Mock()
        self.http.allow_orders = False
        self.scanner = IntradayDiscovery(self.config, self.http, self.directory, self.store)

    def tearDown(self):
        self.store.__exit__()
        self.temporary.cleanup()

    def setup_catalogue(self):
        self.scanner.day = AT.date().isoformat()
        self.scanner.master = {symbol:row(symbol, token) for symbol,token in
                               [("INITIAL",1),("NIFTY 50",2),("NEW",3),("OTHER",4),("EXTRA",5),("MORE",6)]}
        self.scanner.symbols = ["INITIAL","NEW","OTHER","EXTRA","MORE"]
        self.scanner.source = "test universe"
        self.scanner.sectors = {x: x for x in self.scanner.symbols}

    def test_scan_frequency_event_trigger_dedup_and_restart_budget(self):
        self.scanner.symbols = ["NEW"]
        self.assertEqual(self.scanner.begin(AT)[0], "scheduled_market_scan")
        self.assertIsNone(self.scanner.begin(AT+timedelta(seconds=20)))
        event = {"type":"news","source":"nse-announcements","at":AT.isoformat(),
                 "headline":"Issuer event","symbols":["NEW"]}
        self.scanner.notify_news([event])
        self.assertEqual(self.scanner.begin(AT+timedelta(seconds=30)), ("issuer_news", {"NEW"}))
        self.scanner.notify_news([event])
        self.assertIsNone(self.scanner.begin(AT+timedelta(seconds=60)))
        restarted = IntradayDiscovery(self.config,self.http,self.directory,self.store)
        self.assertIsNone(restarted.begin(AT+timedelta(seconds=50)))
        self.assertIsNotNone(restarted.begin(AT+timedelta(seconds=90)))
        self.assertIsNone(restarted.begin(AT.replace(hour=14,minute=30)))
        self.assertEqual(self.store.get("discovery_status")["state"],"entry_window_closed")
        self.assertIsNone(restarted.begin(AT.replace(hour=9,minute=0)))

    def test_daily_cap_is_persistent_and_does_not_fake_a_scan(self):
        self.store.put("discovery_budget", {"day":AT.date().isoformat(),"scans":600,"last_at":AT.isoformat()})
        self.assertIsNone(self.scanner.begin(AT+timedelta(minutes=1)))
        self.assertEqual(self.store.get("discovery_status")["state"], "budget_exhausted")
        self.http.request.assert_not_called()

    def test_readonly_scanner_refuses_order_enabled_client(self):
        self.http.allow_orders = True
        with self.assertRaises(SafetyError):
            IntradayDiscovery(self.config,self.http,self.directory,self.store)

    def test_first_scan_loads_broad_catalogue_without_admitting_unreviewed_symbols(self):
        raw = ("tradingsymbol,exchange,segment,instrument_type,lot_size,expiry,instrument_token,name,tick_size\n"
               "INITIAL,NSE,NSE,EQ,1,,1,Initial Ltd,0.01\n"
               "NEW,NSE,NSE,EQ,1,,3,New Ltd,0.01\n"
               "NIFTY 50,NSE,INDICES,EQ,1,,2,NIFTY 50,0.01\n")
        self.http.request.return_value = raw
        with patch("india_trader.discovery.constituents",return_value=(
            ["INITIAL","NEW"], {"NEW":"NEW"}, {"NEW":"IT"}, "test current universe"
        )), patch("india_trader.discovery.now_ist",return_value=AT):
            result = self.scanner.scan(AT,2500000,{"INITIAL"},set(),{},{},True,"scheduled",set())
        self.assertEqual(result["kind"],"catalogue")
        self.assertEqual(result["universe"],{"INITIAL":1,"NEW":3})
        self.assertNotIn("prepared",result)
        self.http.request.assert_called_once_with("GET","/instruments/NSE",raw=True)

    def test_scan_prepares_outside_stock_and_keeps_network_history_work_bounded(self):
        self.setup_catalogue()
        quotes = {"NSE:"+name:quote(price=100+0.1*i) for i,name in enumerate(self.scanner.symbols)}
        quotes["NSE:NIFTY 50"] = quote()
        self.http.request.return_value = quotes
        with patch("india_trader.discovery.now_ist", return_value=AT), \
             patch("india_trader.discovery.cached_daily_history", return_value=history()) as daily, \
             patch("india_trader.discovery.closed_intraday_bars", return_value=bars()) as intraday:
            result = self.scanner.scan(AT,2500000,{"INITIAL"},set(),{}, {},True,"scheduled_market_scan",{"NEW"})
        self.assertLessEqual(daily.call_count,4)  # benchmark + 3 uncached stocks
        self.assertLessEqual(intraday.call_count,ADMISSIONS_PER_SCAN)
        self.assertTrue(any(x.instrument.symbol!="INITIAL" for x in result["prepared"]))
        self.assertTrue(all(call.args[0]=="GET" for call in self.http.request.call_args_list))
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM ai_spend").fetchone()[0],0)

    def test_failed_news_prevents_candidate_preparation_but_not_readonly_market_scan(self):
        self.setup_catalogue()
        self.http.request.return_value = {"NSE:"+name:quote() for name in self.scanner.symbols+["NIFTY 50"]}
        with patch("india_trader.discovery.now_ist", return_value=AT), \
             patch("india_trader.discovery.cached_daily_history", return_value=history()), \
             patch("india_trader.discovery.closed_intraday_bars") as warm:
            result=self.scanner.scan(AT,2500000,{"INITIAL"},set(),{},{},False,"scheduled_market_scan",set())
        self.assertEqual(result["prepared"],[])
        warm.assert_not_called()
        self.assertGreater(result["liquid_candidates"],0)

    def test_current_events_exclude_new_stock_without_forcing_a_trade(self):
        self.setup_catalogue()
        self.http.request.return_value = {"NSE:"+name:quote() for name in self.scanner.symbols+["NIFTY 50"]}
        with patch("india_trader.discovery.now_ist", return_value=AT), \
             patch("india_trader.discovery.cached_daily_history", return_value=history()), \
             patch("india_trader.discovery.closed_intraday_bars",return_value=bars()):
            result=self.scanner.scan(AT,2500000,{"INITIAL"},{"NEW"},{},{},True,"issuer_news",{"NEW"})
        self.assertNotIn("NEW",[x["symbol"] for x in result["ranked"]])
        self.assertNotIn("NEW",[x.instrument.symbol for x in result["prepared"]])

    def test_bad_single_quote_is_reported_without_disabling_other_discoveries(self):
        self.setup_catalogue()
        quotes = {"NSE:"+name:quote() for name in self.scanner.symbols+["NIFTY 50"]}
        quotes["NSE:NEW"] = {"last_price":None}
        self.http.request.return_value = quotes
        with patch("india_trader.discovery.now_ist",return_value=AT), \
             patch("india_trader.discovery.cached_daily_history",return_value=history()), \
             patch("india_trader.discovery.closed_intraday_bars",return_value=bars()):
            result = self.scanner.scan(AT,2500000,{"INITIAL"},set(),{},{},True,"scheduled",set())
        self.assertTrue(any(x["symbol"]=="NEW" for x in result["omissions"]))
        self.assertNotIn("NEW",[x.instrument.symbol for x in result["prepared"]])
        self.assertGreater(result["liquid_candidates"],0)
        self.assertTrue(result["prepared"])


if __name__ == "__main__":
    unittest.main()
