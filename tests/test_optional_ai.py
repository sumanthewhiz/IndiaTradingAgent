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
from india_trader.core import AIConfig, Config, IST, Session
from india_trader.engine import TradingEngine
from india_trader.events import (
    AIContextAgent, EventAgent, KnowledgeBase, clear_legacy_routine_auction_pause,
    high_impact_news, routine_auction_result,
)
from india_trader.storage import Store

AT = datetime(2026, 9, 25, 11, 0, tzinfo=IST)


class OptionalAITests(unittest.TestCase):
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
