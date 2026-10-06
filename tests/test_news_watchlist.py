from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
import urllib.error
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from xml.sax.saxutils import escape

from india_trader.autonomy import (
    POLICY_VERSION, PreparationBlocked, default_auto_config, prepare_session, reuse_daily_selection,
)
from india_trader.broker import PaperBroker
from india_trader.core import Config, IST, Instrument, SafetyError, Session, Tick
from india_trader.credentials import atomic_json
from india_trader.engine import TradingEngine
from india_trader.events import EventAgent
from india_trader.market import Bar
from india_trader.market_data import (
    CONSTITUENTS_URL, MAX_FEED_ITEMS, NEWS_SOURCES, WATCHLIST_VERSION,
    AutomaticNews, FeedError, PublicResponse, cached_daily_history, company_key,
    constituents, daily_history, public_response, rank_opportunities, select_diverse,
)
from india_trader.runtime import ManagedRun, apply_managed_news, invalidate_failed_news, recent_verified_news
from india_trader.storage import Store

AT = datetime(2026, 9, 24, 11, 0, tzinfo=IST)


def rss(items):
    return ("<rss><channel>" + "".join(
        "<item><title>" + escape(title) + "</title><description>" + escape(text)
        + "</description><pubDate>" + at.isoformat() + "</pubDate></item>"
        for title, text, at in items
    ) + "</channel></rss>").encode()


NSE = rss([("Demo Limited", "Routine information", AT - timedelta(minutes=10))])
RBI = rss([("RBI release", "Routine information", AT - timedelta(hours=2))])


class FeedTests(unittest.TestCase):
    def collector(self, fetch=None, transport=None, symbols=None):
        config = default_auto_config(["DEMO"], "TEST01")
        return AutomaticNews(config, {company_key("Demo Limited"): "DEMO"},
                             fetch=fetch, symbols=symbols, **({"transport": transport} if transport else {}))

    def test_transient_timeout_retries_once_then_recovers(self):
        counts = {}
        def fetch(url):
            counts[url] = counts.get(url, 0) + 1
            if url == NEWS_SOURCES["nse-announcements"] and counts[url] == 1:
                raise TimeoutError("test network timeout")
            return NSE if url == NEWS_SOURCES["nse-announcements"] else RBI
        collector = self.collector(fetch=fetch)
        with patch("india_trader.market_data.now_ist", return_value=AT), \
             patch("india_trader.market_data.clock.sleep") as sleep:
            events = collector.poll()
        self.assertEqual(collector.health["nse-announcements"]["attempts"], 2)
        self.assertTrue(collector.health["nse-announcements"]["healthy"])
        self.assertEqual(sum(x["type"] == "heartbeat" for x in events), 2)
        sleep.assert_called_once_with(1.0)

    def test_retries_are_bounded_and_healthy_source_results_are_retained(self):
        count = [0]
        def fetch(url):
            if url == NEWS_SOURCES["nse-announcements"]:
                count[0] += 1
                raise FeedError("http_503", "Publisher returned HTTP 503.", retryable=True, http_status=503)
            return RBI
        collector = self.collector(fetch=fetch)
        with patch("india_trader.market_data.now_ist", return_value=AT), patch("india_trader.market_data.clock.sleep"):
            events = collector.poll(require_all=False)
        self.assertEqual(count[0], 3)
        self.assertEqual(collector.health["nse-announcements"]["error_code"], "http_503")
        self.assertTrue(collector.health["rbi-releases"]["healthy"])
        self.assertEqual([x["source"] for x in events if x["type"] == "heartbeat"], ["rbi-releases"])

    def test_permission_rate_limit_and_access_page_are_not_retried(self):
        for failure in (
            FeedError("http_403", "Publisher returned HTTP 403.", http_status=403),
            FeedError("http_429", "Publisher rate limited request.", http_status=429),
        ):
            with self.subTest(failure=failure.code):
                collector = self.collector(fetch=Mock(side_effect=failure))
                with patch("india_trader.market_data.clock.sleep") as sleep:
                    with self.assertRaises(FeedError):
                        collector.poll()
                self.assertEqual(collector.fetch.call_count, 2)  # once for each distinct source
                sleep.assert_not_called()
        collector = self.collector(fetch=lambda _: b"<html>Access challenge</html>")
        with patch("india_trader.market_data.clock.sleep") as sleep:
            with self.assertRaises(FeedError):
                collector.poll()
        self.assertEqual(collector.health["nse-announcements"]["error_code"], "not_rss")
        sleep.assert_not_called()

    def test_conditional_304_revalidates_only_a_previously_valid_document(self):
        calls = []
        counts = {}
        def transport(url, headers):
            calls.append((url, headers))
            counts[url] = counts.get(url, 0) + 1
            if counts[url] == 1:
                return PublicResponse(200, NSE if url == NEWS_SOURCES["nse-announcements"] else RBI,
                                      '"version-1"', "Thu, 24 Sep 2026 05:00:00 GMT")
            return PublicResponse(304, b"")
        collector = self.collector(transport=transport)
        with patch("india_trader.market_data.now_ist", return_value=AT):
            collector.poll()
        later = AT + timedelta(minutes=2)
        with patch("india_trader.market_data.now_ist", return_value=later):
            events = collector.poll()
        self.assertEqual(calls[2][1]["If-None-Match"], '"version-1"')
        self.assertEqual(collector.health["nse-announcements"]["http_status"], 304)
        self.assertEqual(collector.health["nse-announcements"]["last_success"], later.isoformat())
        self.assertEqual(sum(x["type"] == "heartbeat" for x in events), 2)

    def test_observed_mid_publication_truncation_is_not_repaired_or_accepted(self):
        document = (
            b'<?xml version="1.0"?><rss><channel><item><title>Demo Limited</title>'
            b'<description>General update</description><pubDate>25-Sep-'
        )
        collector = self.collector(fetch=lambda _: document)
        with patch("india_trader.market_data.clock.sleep") as sleep:
            with self.assertRaises(FeedError):
                collector.poll()
        health = collector.health["nse-announcements"]
        self.assertFalse(health["healthy"])
        self.assertFalse(health["last_verified_grace"])
        self.assertEqual(health["error_code"], "incomplete_snapshot")
        self.assertEqual(health["http_status"], 200)
        self.assertEqual(health["attempts"], 3)
        self.assertNotIn("nse-announcements", collector.documents)
        self.assertEqual(collector.poll_interval_seconds, 30)
        self.assertEqual([x.args[0] for x in sleep.call_args_list[:2]], [1.0, 3.0])

    def test_transient_failure_preserves_only_the_original_verified_deadline(self):
        config = default_auto_config(["DEMO"], "TEST01")
        collector = self.collector(fetch=lambda url: NSE if url == NEWS_SOURCES["nse-announcements"] else RBI)
        with patch("india_trader.market_data.now_ist", return_value=AT):
            collector.poll()
        original = collector.health["nse-announcements"]["last_success"]
        collector.fetch = lambda url: b"<rss><channel><item><pubDate>25-Sep-" if url == NEWS_SOURCES["nse-announcements"] else RBI
        later = AT+timedelta(seconds=120)
        with patch("india_trader.market_data.now_ist", return_value=later), \
             patch("india_trader.market_data.clock.sleep"):
            batch = collector.poll(require_all=False)
        health = collector.health["nse-announcements"]
        self.assertFalse(health["healthy"])
        self.assertTrue(health["last_verified_grace"])
        self.assertEqual(health["last_success"], original)
        self.assertFalse(any(x["source"] == "nse-announcements" for x in batch))
        self.assertTrue(recent_verified_news(health, later, config.news.heartbeat_max_seconds))
        self.assertFalse(recent_verified_news(health, AT+timedelta(seconds=181), config.news.heartbeat_max_seconds))
        with tempfile.TemporaryDirectory() as folder, Store(Path(folder)/"state.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(AT.date(),True,True,["DEMO"],[]),
                                   {"DEMO":Instrument("DEMO",1,1,9000,11000),
                                    "NIFTY 50":Instrument("NIFTY 50",2,1,1,10**12,True)},
                                   broker,store,"paper")
            engine.reconcile(broker.snapshot(later),later)
            for source in NEWS_SOURCES:
                engine.heartbeat_news(AT,source)
            apply_managed_news(batch,collector,engine,EventAgent(config,store),later)
            self.assertEqual(engine.news_heartbeats["nse-announcements"], AT)
            self.assertEqual(store.events("FEED_DEGRADED")[0]["source"],"nse-announcements")
            engine.quotes["DEMO"]=Tick("DEMO",later,10000,9999,10001,1000,1000,1000)
            engine.quotes["NIFTY 50"]=Tick("NIFTY 50",later,2500000,2500000,2500000,0,0,0)
            managed=ManagedRun(Path(folder),Path(folder),{"policy":"test"},{})
            managed.publish(engine,collector,later)
            self.assertTrue(store.get("runtime")["live"])
            self.assertEqual(store.get("runtime")["status"],"LIVE_DEGRADED")
            expired=AT+timedelta(seconds=181)
            invalidate_failed_news(collector,engine,expired)
            self.assertNotIn("nse-announcements",engine.news_heartbeats)
            self.assertEqual(store.events("FEED_FRESHNESS_EXPIRED")[0]["source"], "nse-announcements")
            managed.publish(engine,collector,expired)
            self.assertFalse(store.get("runtime")["live"])

    def test_malformed_complete_xml_and_access_denial_never_get_recent_snapshot_grace(self):
        collector = self.collector(fetch=lambda url: NSE if url == NEWS_SOURCES["nse-announcements"] else RBI)
        with patch("india_trader.market_data.now_ist", return_value=AT):
            collector.poll()
        for failure in (
            b"<rss><channel><item><title>bad & unescaped</title></item></channel></rss>",
            FeedError("http_403", "Access denied.", http_status=403),
        ):
            def fetch(_):
                if isinstance(failure, FeedError):
                    raise failure
                return failure
            collector.fetch = fetch
            with patch("india_trader.market_data.now_ist", return_value=AT+timedelta(seconds=120)), \
                 patch("india_trader.market_data.clock.sleep"):
                collector.poll(require_all=False)
            self.assertFalse(collector.health["nse-announcements"]["last_verified_grace"])
            self.assertFalse(recent_verified_news(collector.health["nse-announcements"],
                                                 AT+timedelta(seconds=120),180))

    def test_malformed_conditional_response_retries_a_fresh_document_without_etag(self):
        phase = [0]
        requests = []
        def transport(url, headers):
            requests.append((url, dict(headers)))
            if url == NEWS_SOURCES["nse-announcements"]:
                if phase[0] == 1 and "If-None-Match" in headers:
                    return PublicResponse(200, b"<rss><channel>")
                return PublicResponse(200, NSE, '"valid-1"')
            return PublicResponse(200, RBI, '"rbi-1"')
        collector = self.collector(transport=transport)
        with patch("india_trader.market_data.now_ist", return_value=AT), \
             patch("india_trader.market_data.clock.sleep") as sleep:
            collector.poll()
            phase[0] = 1
            collector.poll()
        nse = [headers for url, headers in requests if url == NEWS_SOURCES["nse-announcements"]]
        self.assertEqual(nse[-1], {"Cache-Control": "no-cache"})
        self.assertEqual(collector.health["nse-announcements"]["attempts"], 2)
        self.assertTrue(collector.health["nse-announcements"]["healthy"])
        sleep.assert_called_once_with(1.0)

    def test_304_without_document_is_not_a_healthy_heartbeat(self):
        collector = self.collector(transport=lambda *_: PublicResponse(304, b""))
        with self.assertRaises(FeedError):
            collector.poll()
        self.assertEqual(collector.health["nse-announcements"]["error_code"], "cache_miss")
        self.assertFalse(collector.health["nse-announcements"]["healthy"])

    def test_cached_document_does_not_replace_a_failed_request_or_expired_document(self):
        responses = [PublicResponse(200, NSE), PublicResponse(200, RBI)]
        collector = self.collector(transport=Mock(side_effect=responses))
        with patch("india_trader.market_data.now_ist", return_value=AT):
            collector.poll()
        previous = collector.health["nse-announcements"]["last_success"]
        collector.transport = Mock(side_effect=FeedError("timeout", "Timed out.", retryable=True))
        with patch("india_trader.market_data.now_ist", return_value=AT + timedelta(minutes=2)), \
             patch("india_trader.market_data.clock.sleep"):
            self.assertEqual(collector.poll(require_all=False), [])
        self.assertEqual(collector.health["nse-announcements"]["last_success"], previous)
        self.assertFalse(collector.health["nse-announcements"]["healthy"])
        collector.transport = Mock(return_value=PublicResponse(304, b""))
        with patch("india_trader.market_data.now_ist", return_value=AT + timedelta(days=8)):
            with self.assertRaises(FeedError):
                collector.poll()
        self.assertEqual(collector.health["nse-announcements"]["error_code"], "stale_document")

    def test_relevant_announcements_beyond_250_are_not_lost(self):
        items = [("Unrelated Limited", "Routine", AT - timedelta(minutes=5)) for _ in range(300)]
        items.append(("Demo Limited", "Financial results", AT - timedelta(minutes=15)))
        collector = self.collector(fetch=lambda url: rss(items) if url == NEWS_SOURCES["nse-announcements"] else RBI)
        with patch("india_trader.market_data.now_ist", return_value=AT):
            events = collector.poll()
        self.assertEqual(collector.health["nse-announcements"]["items"], 301)
        self.assertEqual(len([x for x in events if x["type"] == "news"]), 1)
        self.assertIn("DEMO", collector.excluded)

    def test_invalid_xml_timestamp_and_oversize_are_explicit(self):
        documents = [
            (b"<rss><channel>", "incomplete_snapshot"),
            (b"<rss><channel><item><title>Demo</title><pubDate>invalid</pubDate></item></channel></rss>", "invalid_timestamp"),
            (b"x" * 2_000_001, "oversized_response"),
            (rss([("Demo", "Routine", AT)] * (MAX_FEED_ITEMS + 1)), "item_count"),
        ]
        for document, expected in documents:
            with self.subTest(code=expected):
                collector = self.collector(fetch=lambda _: document)
                with patch("india_trader.market_data.now_ist", return_value=AT), \
                     patch("india_trader.market_data.clock.sleep"):
                    with self.assertRaises(FeedError):
                        collector.poll()
                self.assertEqual(collector.health["nse-announcements"]["error_code"], expected)

    def test_context_is_refreshed_instead_of_excluding_stock_forever(self):
        old = rss([("Demo Limited", "Financial results", AT - timedelta(hours=23))])
        collector = self.collector(fetch=lambda url: old if url == NEWS_SOURCES["nse-announcements"] else RBI)
        with patch("india_trader.market_data.now_ist", return_value=AT):
            collector.poll()
        self.assertIn("DEMO", collector.excluded)
        with patch("india_trader.market_data.now_ist", return_value=AT + timedelta(hours=2)):
            collector.poll()
        self.assertNotIn("DEMO", collector.excluded)

    def test_runtime_keeps_healthy_macro_events_while_nse_is_unavailable(self):
        config = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as folder, Store(Path(folder) / "state.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                   {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                                    "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True)},
                                   broker, store, "paper")
            for source in NEWS_SOURCES:
                engine.heartbeat_news(AT, source)
            news = Mock()
            news.health = {
                "nse-announcements": {"healthy": False, "error_code": "http_503",
                                      "error": "Publisher HTTP 503.", "attempts": 2, "http_status": 503},
                "rbi-releases": {"healthy": True, "http_status": 200},
            }
            invalidate_failed_news(news, engine)
            self.assertNotIn("nse-announcements", engine.news_heartbeats)
            self.assertIn("rbi-releases", engine.news_heartbeats)
            batch = [
                {"type": "news", "source": "rbi-releases", "at": AT.isoformat(),
                 "symbols": ["*"], "severity": "high", "public": True, "headline": "RBI policy decision"},
                {"type": "heartbeat", "source": "rbi-releases", "at": AT.isoformat()},
            ]
            apply_managed_news(batch, news, engine, EventAgent(config, store), AT)
            self.assertNotIn("nse-announcements", engine.news_heartbeats)
            self.assertIn("rbi-releases", engine.news_heartbeats)
            self.assertIn("*", engine.state["pauses"])
            self.assertEqual(store.events("FEED_UNAVAILABLE")[0]["source"], "nse-announcements")
            self.assertEqual(store.events("FEED_UNAVAILABLE")[0]["error_code"], "http_503")
            news.health["nse-announcements"] = {"healthy": True, "http_status": 304, "attempts": 1}
            apply_managed_news([{"type": "heartbeat", "source": "nse-announcements", "at": AT.isoformat()}],
                               news, engine, EventAgent(config, store), AT)
            self.assertEqual(store.events("FEED_RECOVERED")[0]["source"], "nse-announcements")

    def test_known_failed_source_cannot_show_live_even_with_a_recent_old_heartbeat(self):
        config = default_auto_config(["DEMO"], "TEST01")
        with tempfile.TemporaryDirectory() as folder, Store(Path(folder) / "state.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, ["DEMO"], []),
                                   {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                                    "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True)},
                                   broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            for source in NEWS_SOURCES:
                engine.heartbeat_news(AT, source)
            engine.quotes["DEMO"] = Tick("DEMO", AT, 10000, 9999, 10001, 1000, 1000, 1000)
            engine.quotes["NIFTY 50"] = Tick("NIFTY 50", AT, 2500000, 2500000, 2500000, 0, 0, 0)
            news = Mock()
            news.health = {"nse-announcements": {"healthy": False, "error": "Publisher HTTP 503."}}
            ManagedRun(Path(folder), Path(folder), {"policy": "test"}, {}).publish(engine, news, AT)
            snapshot = store.get("runtime")
            self.assertFalse(snapshot["live"])
            self.assertFalse(snapshot["entries_allowed"])
            self.assertEqual(snapshot["status"], "BLOCKED")
            self.assertIn("nse-announcements", snapshot["reason"])

    def test_http_error_classification_does_not_leak_server_body(self):
        for code, expected_retry in ((403, False), (429, False), (503, True)):
            error = urllib.error.HTTPError(NEWS_SOURCES["nse-announcements"], code, "raw upstream message",
                                          {}, io.BytesIO(b"private upstream content"))
            opener = Mock()
            opener.open.side_effect = error
            with patch("india_trader.market_data.urllib.request.build_opener", return_value=opener):
                with self.assertRaises(FeedError) as caught:
                    public_response(NEWS_SOURCES["nse-announcements"])
            self.assertEqual(caught.exception.retryable, expected_retry)
            self.assertEqual(caught.exception.http_status, code)
            self.assertNotIn("private upstream", str(caught.exception))
            self.assertNotIn("raw upstream", str(caught.exception))


class RankingTests(unittest.TestCase):
    def metrics(self, momentum=0):
        return {"atr_bps": 150, "average_turnover_paise": 20_000_000_000, "last_close_paise": 10000,
                "last_session": "2026-09-23", "return_5d_bps": momentum, "return_20d_bps": momentum,
                "mean_20d_close_paise": 10000, "sessions": 30}

    def candidate(self, symbol, change=0, spread=2):
        return {"symbol": symbol, "price_paise": 10000, "previous_close_paise": 10000,
                "turnover_paise": 10_000_000_000, "change_bps": change,
                "from_open_bps": change, "spread_bps": spread, "quote_at": AT.isoformat()}

    def rank(self, items, histories=None, catalysts=None, excluded=None):
        return rank_opportunities(items, histories or {x["symbol"]: self.metrics() for x in items},
                                  self.metrics(), 0, {x["symbol"]: "Test industry" for x in items},
                                  catalysts or {}, excluded or set(), AT)

    def test_current_and_historical_strength_change_ranking_not_symbol_identity(self):
        items = [self.candidate("OLD", -50), self.candidate("NEW", 100)]
        histories = {"OLD": self.metrics(-500), "NEW": self.metrics(500)}
        self.assertEqual(self.rank(items, histories)[0]["symbol"], "NEW")
        reversed_strength = [self.candidate("OLD", 100), self.candidate("NEW", -50)]
        self.assertEqual(self.rank(reversed_strength, {"OLD": self.metrics(500), "NEW": self.metrics(-500)})[0]["symbol"], "OLD")

    def test_aged_catalyst_is_bounded_and_fresh_news_is_not_rewarded(self):
        item = self.candidate("DEMO")
        recent = {"DEMO": [{"at": (AT - timedelta(minutes=5)).isoformat(), "headline": "Order win"}]}
        older = {"DEMO": [{"at": (AT - timedelta(hours=2)).isoformat(), "headline": "Order win"}]}
        baseline = self.rank([item])[0]["score"]
        self.assertEqual(self.rank([item], catalysts=recent)[0]["score"], baseline)
        row = self.rank([item], catalysts=older)[0]
        self.assertGreater(row["score"], baseline)
        self.assertLessEqual(row["score"] - baseline, 0.35)
        self.assertEqual(self.rank([item], catalysts=older, excluded={"DEMO"}), [])

    def test_price_basis_or_history_session_mismatch_cannot_rank(self):
        item = self.candidate("DEMO")
        for changed in ({"last_close_paise": 5000}, {"last_session": "2026-09-22"},
                        {"atr_bps": 500}, {"average_turnover_paise": 0}):
            with self.subTest(changed=changed):
                self.assertEqual(self.rank([item], {"DEMO": {**self.metrics(), **changed}}), [])

    def test_cost_and_industry_diversity_are_not_ignored(self):
        items = [self.candidate("WIDE", spread=8), self.candidate("TIGHT", spread=1)]
        self.assertEqual(self.rank(items)[0]["symbol"], "TIGHT")
        ranked = [{"symbol": str(i), "sector": "Bank" if i < 4 else "IT"} for i in range(6)]
        self.assertEqual([x["symbol"] for x in select_diverse(ranked)], ["0", "1", "4", "5"])

    def test_current_official_universe_has_industry_metadata(self):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Company Name", "Industry", "Symbol", "Series", "ISIN Code"])
        writer.writeheader()
        master = {}
        for i in range(200):
            symbol = f"TEST{i}"
            writer.writerow({"Company Name": symbol + " Limited", "Industry": "Test industry",
                             "Symbol": symbol, "Series": "EQ", "ISIN Code": "test"})
            master[symbol] = {"segment": "NSE", "instrument_type": "EQ", "lot_size": 1}
        with tempfile.TemporaryDirectory() as folder, \
             patch("india_trader.market_data.public_bytes", return_value=output.getvalue().encode()):
            symbols, aliases, sectors, source = constituents(master, Path(folder) / "constituents.json")
        self.assertEqual(len(symbols), 200)
        self.assertEqual(sectors["TEST0"], "Test industry")
        self.assertEqual(aliases[company_key("TEST0 Limited")], "TEST0")
        self.assertIn("current NIFTY 200", source)

    def test_old_nifty50_cache_is_not_mislabeled_as_current_nifty200(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder) / "constituents.json"
            atomic_json(cache, {"fetched_at": AT.isoformat(), "rows": [{"Symbol": "OLD", "Company Name": "Old"}]})
            with patch("india_trader.market_data.now_ist", return_value=AT), \
                 patch("india_trader.market_data.public_bytes", side_effect=FeedError("http_403", "Unavailable.")):
                symbols, _, _, source = constituents({}, cache)
        self.assertEqual(symbols, [])
        self.assertIn("bundled", source)
        self.assertNotIn("current NIFTY 200", source)

    def test_daily_history_cache_reuses_only_same_day_and_token(self):
        http = Mock()
        with tempfile.TemporaryDirectory() as folder, \
             patch("india_trader.market_data.daily_history", return_value=self.metrics()) as read:
            cache = Path(folder)
            cached_daily_history(http, 1, AT, cache)
            cached_daily_history(http, 1, AT + timedelta(hours=1), cache)
            self.assertEqual(read.call_count, 1)
            cached_daily_history(http, 1, AT + timedelta(days=1), cache)
            cached_daily_history(http, 2, AT + timedelta(days=1), cache)
            self.assertEqual(read.call_count, 3)

    def test_daily_history_is_completed_ordered_and_has_momentum(self):
        http = Mock()
        rows = [[(AT - timedelta(days=30-i)).isoformat(), 100+i, 101+i, 99+i, 100+i, 1000]
                for i in range(30)]
        http.request.return_value = {"candles": rows}
        metrics = daily_history(http, 1, AT)
        self.assertGreater(metrics["return_5d_bps"], 0)
        self.assertGreater(metrics["return_20d_bps"], metrics["return_5d_bps"])
        self.assertEqual(metrics["last_session"], (AT - timedelta(days=1)).date().isoformat())
        for invalid in (rows[:20], rows + [[AT.isoformat(), 100, 101, 99, 100, 1000]], rows[::-1]):
            http.request.return_value = {"candles": invalid}
            with self.assertRaises(SafetyError):
                daily_history(http, 1, AT)

    def test_prior_day_selection_is_not_reused_except_for_owned_exposure(self):
        plan = {"day": (AT - timedelta(days=1)).date().isoformat()}
        self.assertFalse(reuse_daily_selection(plan, AT, False))
        self.assertTrue(reuse_daily_selection(plan, AT, True))
        self.assertTrue(reuse_daily_selection({"day": AT.date().isoformat()}, AT, False))
        with self.assertRaises(PreparationBlocked):
            reuse_daily_selection({"day": (AT + timedelta(days=1)).date().isoformat()}, AT, False)


class DailyPreparationTests(unittest.TestCase):
    def test_new_day_rebuilds_selection_without_resetting_losses_or_same_day_churn(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            account = "TEST01"
            from india_trader.autonomy import account_directory
            workspace = account_directory(directory, account)
            workspace.mkdir(parents=True)
            day0 = AT - timedelta(days=1)
            config = default_auto_config(["OLD"], account)
            instruments = {"OLD": Instrument("OLD", 1, 1, 9000, 11000),
                           "NIFTY 50": Instrument("NIFTY 50", 2, 1, 1, 10**12, True)}
            broker = PaperBroker(2500000, config.costs)
            with Store(workspace / "live.db") as store:
                engine = TradingEngine(config, Session(day0.date(), True, True, ["OLD"], []),
                                       instruments, broker, store, "live")
                engine.reconcile(broker.snapshot(day0), day0)
                engine.state["cash"] = 2450000
                engine.state["trades"] = 2
                engine._save()
            atomic_json(workspace / "plan.json", {
                "account": account, "day": day0.date().isoformat(), "policy": POLICY_VERSION,
                "selected": ["OLD"], "aliases": {}, "universe_source": "prior day",
                "created_at": day0.isoformat(), "screen": [], "history": {},
            })
            keys = {"ai_api_key": "test-ai", "broker_api_key": "test-key", "broker_access_token": "test-token"}
            vault = Mock()
            vault.load.return_value = {"keys": keys, "auto_start": True, "consent_version": "auto-live-v1"}
            quote = {"last_price": 100, "timestamp": AT.isoformat(), "volume": 1_000_000,
                     "depth": {"buy": [{"price": 99.99}], "sell": [{"price": 100.01}]},
                     "lower_circuit_limit": 90, "upper_circuit_limit": 110, "ohlc": {"open": 100, "close": 100}}
            http = Mock()
            def request(method, path, **kwargs):
                self.assertEqual(method, "GET")
                if path == "/user/profile":
                    return {"broker": "ZERODHA", "user_id": account, "exchanges": ["NSE"],
                            "products": ["CNC"], "order_types": ["LIMIT", "SL"]}
                if path == "/portfolio/holdings":
                    return []
                if path == "/user/margins/equity":
                    return {"available": {"cash": 25000, "live_balance": 25000, "collateral": 0}, "utilised": {}, "net": 25000}
                if path == "/instruments/NSE":
                    return ("tradingsymbol,exchange,segment,instrument_type,lot_size,expiry,instrument_token,name\n"
                            "NEW,NSE,NSE,EQ,1,,3,New Limited\nNIFTY 50,NSE,INDICES,EQ,1,,2,NIFTY 50\n")
                if path == "/quote":
                    return {"NSE:NEW": quote, "NSE:NIFTY 50": quote}
                raise AssertionError(path)
            http.request.side_effect = request
            news = Mock()
            news.excluded, news.catalysts, news.health = set(), {}, {}
            metrics = RankingTests().metrics()
            opening = AT.replace(hour=9, minute=15)
            bars = [Bar(opening + timedelta(minutes=5*i), 10000, 10020, 9990, 10010, 1000)
                    for i in range(21)]
            new_instruments = {"NEW": Instrument("NEW", 3, 1, 9000, 11000), "NIFTY 50": instruments["NIFTY 50"]}
            with patch("india_trader.autonomy.now_ist", return_value=AT), \
                 patch("india_trader.autonomy.software_ready", return_value=True), \
                 patch("india_trader.autonomy.KiteHTTP", return_value=http), \
                 patch("india_trader.autonomy.verify_gemini_key"), \
                 patch("india_trader.pre_market.global_context", return_value={"opening_blackout": None}), \
                 patch("india_trader.autonomy.constituents", return_value=(["NEW"], {}, {"NEW": "IT"}, "fresh universe")) as universe, \
                 patch("india_trader.autonomy.AutomaticNews", return_value=news), \
                 patch("india_trader.autonomy.cached_daily_history", return_value=metrics) as historical, \
                 patch("india_trader.autonomy.load_kite_instruments", return_value=new_instruments), \
                 patch("india_trader.autonomy.closed_intraday_bars", return_value=bars):
                plan = prepare_session(directory, directory, vault)
                self.assertEqual(plan["selected"], ["NEW"])
                self.assertFalse(plan["selection_reused"])
                self.assertEqual(plan["cash_available_paise"], 2450000)
                self.assertEqual(plan["selection"]["session_day"], AT.date().isoformat())
                self.assertTrue((workspace / "watchlists" / f"{AT.date().isoformat()}.json").exists())
                universe.assert_called_once()
                reads = historical.call_count
                again = prepare_session(directory, directory, vault)
                self.assertTrue(again["selection_reused"])
                self.assertEqual(again["selection"]["refreshed_at"], plan["selection"]["refreshed_at"])
                self.assertEqual(historical.call_count, reads)
                universe.assert_called_once()
            with Store(workspace / "live.db") as store:
                self.assertEqual(store.get("engine")["cash"], 2450000)
                self.assertEqual(store.get("engine")["trades"], 2)
                rotated = TradingEngine(default_auto_config(["NEW"], account),
                                        Session(AT.date(), True, True, ["NEW"], []),
                                        new_instruments, broker, store, "live", allow_daily_universe_change=True)
                rotated.reconcile(broker.snapshot(AT), AT)
                self.assertEqual(rotated.state["cash"], 2450000)
                self.assertEqual(rotated.state["capital"], 2500000)
                self.assertEqual(rotated.state["trades"], 0)
                self.assertEqual(rotated.state["day"], AT.date().isoformat())


if __name__ == "__main__":
    unittest.main()
