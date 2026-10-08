from __future__ import annotations

import io
import json
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.broker import PaperBroker
from india_trader.ai_provider import GEMINI_ENDPOINT, parse_pause
from india_trader.core import AIConfig, Config, IST, NewsConfig, Session
from india_trader.engine import TradingEngine
from india_trader.events import (
    AIContextAgent, EventAgent, KnowledgeBase, clear_legacy_routine_auction_pause,
    high_impact_news, routine_auction_result, routine_rbi_release,
)
from india_trader.storage import Store

AT = datetime(2026, 9, 25, 11, 0, tzinfo=IST)


class OptionalAITests(unittest.TestCase):
    def test_standard_rbi_operations_do_not_trigger_a_market_wide_pause_from_boilerplate(self):
        notices = (
            "Money Market Operations as on September 24, 2026: RBI Operations Amount Current Rate.",
            "RBI to conduct Overnight Variable Rate Reverse Repo (VRRR) auction under LAF on September 25, 2026:",
            "Result of the 29-day Variable Rate Reverse Repo (VRRR) auction held on September 25, 2026: Rate 5.49.",
            "Underwriting Auction for sale of Government Securities for INR 36000 crore on September 25, 2026: Standard RBI underwriting procedures.",
        )
        for notice in notices:
            with self.subTest(notice=notice):
                self.assertTrue(routine_rbi_release(notice, "rbi-releases"))
                self.assertFalse(high_impact_news(notice, "rbi-releases"))
                self.assertFalse(routine_rbi_release(notice, "licensed-wire"))

    def test_rbi_policy_shocks_and_distress_override_a_routine_title(self):
        prefix = "Money Market Operations as on September 24, 2026: "
        for event in ("Emergency liquidity measures", "MPC statement", "CRR change",
                      "Repo rate cut", "Bank defaulted", "Bond default", "Earthquake", "Trading suspension"):
            with self.subTest(event=event):
                self.assertFalse(routine_rbi_release(prefix+event, "rbi-releases"))
                self.assertTrue(high_impact_news(prefix+event, "rbi-releases"))

    def test_routine_release_records_context_without_consuming_ai_or_adding_a_pause(self):
        config = Config(news=NewsConfig(allowed_sources=["rbi-releases"], required_sources=["rbi-releases"]))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(AT.date(), True, True, config.market.symbols, []),
                                   {}, broker, store, "paper")
            engine.reconcile(broker.snapshot(AT), AT)
            ai = Mock()
            agent = EventAgent(config, store, ai)
            raw = {
                "type": "news", "source": "rbi-releases", "at": AT.isoformat(), "symbols": ["*"],
                "headline": "Money Market Operations as on September 24, 2026: RBI Operations.",
                "severity": "medium", "public": True,
            }
            agent.accept(raw, engine, AT)
            self.assertEqual(engine.state["pauses"], {})
            self.assertTrue(store.events("NEWS")[-1]["routine_operational"])
            ai.submit.assert_not_called()
            agent.accept({**raw, "severity": "critical", "headline": raw["headline"]+" Emergency."}, engine, AT)
            self.assertIn("*", engine.state["pauses"])
            ai.submit.assert_called_once()

    def test_republished_description_does_not_restart_the_same_event_reaction_window(self):
        config = Config(news=NewsConfig(allowed_sources=["rbi-releases"], required_sources=["rbi-releases"]))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            engine = TradingEngine(config,Session(AT.date(),True,True,config.market.symbols,[]),
                                   {},PaperBroker(2500000,config.costs),store,"paper")
            agent = EventAgent(config, store)
            raw = {"type":"news","source":"rbi-releases","at":AT.isoformat(),"symbols":["*"],
                   "headline":"RBI monetary policy decision:","severity":"high","public":True}
            agent.accept(raw, engine, AT+timedelta(minutes=5))
            original = engine.state["pauses"]["*"]
            agent.accept({**raw,"headline":raw["headline"]+" Full statement arrived."},
                         engine, AT+timedelta(minutes=12))
            self.assertEqual(original, engine.state["pauses"]["*"])
            self.assertEqual(original, (AT+timedelta(minutes=30)).isoformat())

    def test_expired_material_reaction_is_not_restarted_but_critical_news_still_pauses(self):
        config = Config(news=NewsConfig(allowed_sources=["rbi-releases"], required_sources=["rbi-releases"]))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            engine = TradingEngine(config,Session(AT.date(),True,True,config.market.symbols,[]),
                                   {},PaperBroker(2500000,config.costs),store,"paper")
            agent = EventAgent(config, store)
            raw = {"type":"news","source":"rbi-releases","at":AT.isoformat(),"symbols":["*"],
                   "headline":"RBI monetary policy statement:","severity":"high","public":True}
            at = AT+timedelta(minutes=40)
            agent.accept(raw, engine, at)
            self.assertEqual(engine.state["pauses"], {})
            self.assertEqual(len(store.events("NEWS_REACTION_WINDOW_ELAPSED")), 1)
            agent.accept({**raw,"headline":"RBI emergency restriction.","severity":"critical"}, engine, at)
            self.assertEqual(engine.state["pauses"]["*"], (at+timedelta(minutes=30)).isoformat())

    def test_routine_nil_devolvement_auction_is_not_earnings_or_policy_news(self):
        text = "Government Stock - Auction Results: Cut-off: 7.2% GS 2035 Devolvement on Primary Dealers NIL"
        self.assertTrue(routine_auction_result(text, "rbi-releases"))
        self.assertFalse(high_impact_news(text, "rbi-releases"))
        self.assertTrue(high_impact_news(text.replace("Dealers NIL", "Dealers INR 500 crore"), "rbi-releases"))
        self.assertTrue(high_impact_news(text, "licensed-wire"))
        self.assertTrue(high_impact_news("RBI monetary policy decision on repo rate", "rbi-releases"))
        self.assertTrue(high_impact_news("Company financial results announced", "nse-announcements"))

    def test_only_traceable_routine_auction_pause_is_removed_without_changing_risk(self):
        text = "Government Stock - Auction Results: Cut-off: 7.2% GS 2035 Devolvement on Primary Dealers NIL"
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            config = Config()
            broker = PaperBroker(2500000,config.costs)
            engine = TradingEngine(config,Session(AT.date(),True,True,config.market.symbols,[]),{},broker,store,"paper")
            engine.reconcile(broker.snapshot(AT),AT)
            store.audit(AT,"NEWS",source="rbi-releases",headline=text,severity="high",symbols=["*"])
            engine.pause(["*"],AT+timedelta(minutes=30),AT,"deterministic_event_pause")
            cash = engine.state["cash"]
            self.assertTrue(clear_legacy_routine_auction_pause(engine,AT+timedelta(minutes=1)))
            self.assertEqual(engine.state["pauses"],{})
            self.assertEqual(engine.state["cash"],cash)
            self.assertEqual(engine.state["trades"],0)
            self.assertEqual(len(store.events("PAUSE_CLASSIFICATION_CORRECTED")),1)
            later=AT+timedelta(minutes=2)
            engine.pause(["*"],AT+timedelta(minutes=45),later,"ai_additional_pause")
            self.assertFalse(clear_legacy_routine_auction_pause(engine,later))

    def test_truncated_old_news_cannot_prove_that_clearing_a_pause_is_safe(self):
        prefix = "Money Market Operations as on September 24, 2026: "
        text = (prefix+"operational detail "*100)[:1000]
        self.assertTrue(routine_rbi_release(text, "rbi-releases"))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            config = Config()
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config,Session(AT.date(),True,True,config.market.symbols,[]),
                                   {},broker,store,"paper")
            engine.reconcile(broker.snapshot(AT),AT)
            store.audit(AT,"NEWS",source="rbi-releases",headline=text,severity="high",symbols=["*"])
            engine.pause(["*"],AT+timedelta(minutes=30),AT,"deterministic_event_pause")
            self.assertFalse(clear_legacy_routine_auction_pause(engine,AT+timedelta(minutes=1)))
            self.assertIn("*", engine.state["pauses"])

    def test_malformed_provider_objects_raise_handled_validation_errors(self):
        for endpoint, body in (
            (GEMINI_ENDPOINT, b"[]"),
            (GEMINI_ENDPOINT, b'{"candidates":[null]}'),
            (GEMINI_ENDPOINT, b'{"candidates":[{"finishReason":"STOP","content":{"parts":[null]}}]}'),
            ("https://api.openai.com/v1/chat/completions", b'{"choices":[null]}'),
        ):
            with self.subTest(endpoint=endpoint, body=body), self.assertRaises(ValueError):
                parse_pause(endpoint, body)

    def test_failed_optional_model_records_no_verdict_without_clearing_material_news_pause(self):
        config = Config(ai=AIConfig(enabled=True, share_public_news=True, model="test-only-model"))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            engine = TradingEngine(config, Session(AT.date(), True, True, config.market.symbols, []),
                                   {}, PaperBroker(2500000,config.costs),store,"paper")
            ai = AIContextAgent(config,store,KnowledgeBase(Path(temporary)),api_key="test-only-secret")
            body = io.BytesIO(b'{"message":"test-only-secret private provider details"}')
            failure = urllib.error.HTTPError("https://api.openai.com/v1/chat/completions",429,"quota",{},body)
            opener = Mock()
            opener.open.side_effect = failure
            try:
                with patch("india_trader.events.urllib.request.build_opener",return_value=opener):
                    EventAgent(config,store,ai).accept({
                        "type":"news","source":"licensed-wire","at":AT.isoformat(),
                        "symbols":["*"],"severity":"high","public":True,"headline":"Material monetary policy decision",
                    },engine,AT)
                    self.assertIsNotNone(ai.future)
                    ai.future.result(timeout=5)
                before = engine.state["pauses"]["*"]
                ai.drain(engine,AT)
                result = store.events("AI_RESULT")[-1]
                self.assertEqual(result["pause_minutes"],0)
                self.assertIn("ai_failed_no_verdict",result["reason"])
                self.assertIn("HTTP 429",result["reason"])
                self.assertNotIn("test-only-secret",json.dumps(result))
                self.assertNotIn("private provider",json.dumps(result))
                self.assertEqual(engine.state["pauses"]["*"],before)
                self.assertEqual(before,(AT+timedelta(minutes=30)).isoformat())
                self.assertEqual(len(store.events("PAUSE")),1)
                self.assertEqual(store.db.execute("SELECT calls FROM ai_spend").fetchone()[0],1)
                opener.open.assert_called_once()
                self.assertTrue(body.closed)
            finally:
                ai.close()

    def test_medium_event_does_not_get_an_invented_pause_on_model_failure(self):
        config = Config(ai=AIConfig(enabled=True, share_public_news=True, model="test-only-model"))
        with tempfile.TemporaryDirectory() as temporary, Store(Path(temporary)/"state.db") as store:
            engine = TradingEngine(config,Session(AT.date(),True,True,config.market.symbols,[]),
                                   {},PaperBroker(2500000,config.costs),store,"paper")
            ai = AIContextAgent(config,store,KnowledgeBase(Path(temporary)),api_key="test-only-secret")
            opener = Mock()
            opener.open.side_effect = TimeoutError("test provider timed out")
            try:
                with patch("india_trader.events.urllib.request.build_opener",return_value=opener):
                    EventAgent(config,store,ai).accept({
                        "type":"news","source":"licensed-wire","at":AT.isoformat(),
                        "symbols":["*"],"severity":"medium","public":True,"headline":"Routine investor information update",
                    },engine,AT)
                    ai.future.result(timeout=5)
                ai.drain(engine,AT)
                self.assertEqual(engine.state["pauses"],{})
                self.assertIn("TimeoutError",store.events("AI_RESULT")[-1]["reason"])
                self.assertEqual(store.events("ORDER_INTENT"),[])
                self.assertEqual(store.db.execute("SELECT calls FROM ai_spend").fetchone()[0],1)
            finally:
                ai.close()


if __name__ == "__main__":
    unittest.main()
