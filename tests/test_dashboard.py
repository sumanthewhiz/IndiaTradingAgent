from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.ai_provider import (
    GEMINI_ENDPOINT, GEMINI_MODEL, parse_pause, provider_kind, request_headers, request_payload,
)
from india_trader.autonomy import (
    POLICY_VERSION, PreparationBlocked, account_directory, broker_login_url, default_auto_config,
    broker_connection, prepare_session, record_broker_connection, validate_broker_capabilities,
)
from india_trader.broker import KiteBroker, PaperBroker
from india_trader.core import AIConfig, Config, IST, Instrument, SafetyError, Session, Tick
from india_trader.credentials import CredentialVault, WindowsProtector, atomic_json
from india_trader.dashboard import Controller, DashboardServer
from india_trader.engine import TradingEngine
from india_trader.market import Bar, Tape
from india_trader.market_data import AutomaticNews, NEWS_SOURCES, company_key, feed_timestamp, screen_quotes
from india_trader.storage import Store
from india_trader.runtime import ManagedRun

ROOT = Path(__file__).resolve().parent.parent
FAKE_KEYS = {
    "ai_api_key": "test-only-gemini-key", "broker_api_key": "test-only-kite-key",
    "broker_api_secret": "test-only-kite-secret", "broker_access_token": "test-only-access-token",
}


class TestOnlyProtector:
    """Test double only. Production never uses this reversible test encoding."""
    def protect(self, data):
        return base64.b64encode(data)

    def unprotect(self, data):
        return base64.b64decode(data)


class VaultTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.vault = CredentialVault(self.directory / "keys.dat", TestOnlyProtector())

    def tearDown(self):
        self.temp.cleanup()

    def test_no_credentials_means_no_auto_start(self):
        self.assertFalse(self.vault.load()["auto_start"])
        self.assertFalse(any(self.vault.public_status()["saved"].values()))

    def test_requires_explicit_live_authorization(self):
        with self.assertRaises(SafetyError):
            self.vault.save(FAKE_KEYS, False)
        self.assertFalse(self.vault.path.exists())

    def test_saved_keys_persist_without_plaintext_or_api_readback(self):
        self.vault.save(FAKE_KEYS, True)
        restored = CredentialVault(self.vault.path, TestOnlyProtector())
        self.assertEqual(restored.load()["keys"], FAKE_KEYS)
        self.assertTrue(restored.load()["auto_start"])
        public = json.dumps(restored.public_status())
        for value in FAKE_KEYS.values():
            self.assertNotIn(value, public)
            self.assertNotIn(value.encode(), self.vault.path.read_bytes())

    def test_blank_update_keeps_saved_keys(self):
        self.vault.save(FAKE_KEYS, True)
        self.vault.save({"ai_api_key": ""}, True)
        self.assertEqual(self.vault.load()["keys"], FAKE_KEYS)

    def test_no_password_or_extra_config_fields(self):
        with self.assertRaises(SafetyError):
            self.vault.save({**FAKE_KEYS, "bank_password": "test"}, True)

    def test_missing_keys_do_not_create_vault(self):
        with self.assertRaises(SafetyError):
            self.vault.save({"ai_api_key": "test"}, True)
        self.assertFalse(self.vault.path.exists())

    def test_header_injection_is_rejected(self):
        with self.assertRaises(SafetyError):
            self.vault.save({**FAKE_KEYS, "ai_api_key": "test\r\nheader:value"}, True)

    def test_corrupted_vault_is_not_reset(self):
        self.vault.path.write_bytes(b"invalid")
        with self.assertRaises(SafetyError):
            self.vault.load()
        self.assertEqual(self.vault.path.read_bytes(), b"invalid")

    def test_stop_and_token_renewal_keep_credentials(self):
        self.vault.save(FAKE_KEYS, True)
        self.vault.set_auto_start(False)
        self.vault.set_access_token("test-only-new-access")
        self.assertFalse(self.vault.load()["auto_start"])
        self.assertEqual(self.vault.load()["keys"]["ai_api_key"], FAKE_KEYS["ai_api_key"])
        self.assertEqual(self.vault.load()["keys"]["broker_access_token"], "test-only-new-access")

    @unittest.skipUnless(os.name == "nt", "Windows DPAPI")
    def test_real_windows_dpapi_round_trip(self):
        vault = CredentialVault(self.directory / "dpapi.dat", WindowsProtector())
        vault.save(FAKE_KEYS, True)
        self.assertNotIn(FAKE_KEYS["ai_api_key"].encode(), vault.path.read_bytes())
        self.assertEqual(CredentialVault(vault.path).load()["keys"], FAKE_KEYS)


class GeminiTests(unittest.TestCase):
    def test_native_payload_is_bounded_and_has_no_tools(self):
        config = AIConfig(model=GEMINI_MODEL, endpoint=GEMINI_ENDPOINT, max_output_tokens=2048)
        data = json.loads(request_payload(config, [
            {"role": "system", "content": "Classify uncertainty."},
            {"role": "user", "content": "Public headline."},
        ]))
        self.assertEqual(data["generationConfig"]["maxOutputTokens"], 2048)
        self.assertEqual(data["generationConfig"]["thinkingConfig"]["thinkingLevel"], "low")
        self.assertEqual(data["generationConfig"]["responseMimeType"], "application/json")
        self.assertNotIn("tools", data)
        self.assertNotIn("api_key", data)

    def test_gemini_headers_do_not_put_key_in_url(self):
        headers = request_headers(GEMINI_ENDPOINT, "test-only-key")
        self.assertEqual(headers["x-goog-api-key"], "test-only-key")
        self.assertNotIn("Authorization", headers)
        self.assertNotIn("test-only-key", GEMINI_ENDPOINT)

    def test_native_endpoint_model_must_match(self):
        config = AIConfig(model="gemini-other", endpoint=GEMINI_ENDPOINT)
        with self.assertRaises(SafetyError):
            request_payload(config, [{"content":"a"},{"content":"b"}])

    def test_compatibility_request_maps_budget_and_low_reasoning(self):
        config = AIConfig(model=GEMINI_MODEL,
                          endpoint="https://generativelanguage.googleapis.com/v1beta/openai/chat/completions")
        data = json.loads(request_payload(config, [{"content":"a"},{"content":"b"}]))
        self.assertEqual(data["max_tokens"], config.max_output_tokens)
        self.assertEqual(data["reasoning_effort"], "low")
        self.assertNotIn("max_completion_tokens", data)

    def test_openai_request_behavior_is_preserved(self):
        data = json.loads(request_payload(AIConfig(model="example"), [{"content":"a"},{"content":"b"}]))
        self.assertEqual(data["max_completion_tokens"], 128)
        self.assertNotIn("reasoning_effort", data)

    def test_untrusted_ai_endpoints_rejected(self):
        for url in (
            "http://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
            "https://generativelanguage.googleapis.com.evil.example/v1beta/openai/chat/completions",
            "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions?key=test",
            "https://test@generativelanguage.googleapis.com/v1beta/openai/chat/completions",
            "https://generativelanguage.googleapis.com/v1beta/models/../../funds:generateContent",
            "https://api.openai.com/v1/chat/completions#fragment",
        ):
            with self.subTest(url=url), self.assertRaises(SafetyError):
                provider_kind(url)

    def test_only_bounded_pause_outputs_accepted(self):
        for value in ({"pause_minutes": 15}, {"pause_minutes": 0}, {"pause_minutes": 60}):
            body = {"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":json.dumps(value)}]}}]}
            self.assertEqual(parse_pause(GEMINI_ENDPOINT, json.dumps(body).encode()), value["pause_minutes"])
        for value in ({"pause_minutes": True}, {"pause_minutes": 61}, {"pause_minutes": -1},
                      {"pause_minutes": 0, "order": "BUY"}):
            body = {"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":json.dumps(value)}]}}]}
            with self.assertRaises(ValueError):
                parse_pause(GEMINI_ENDPOINT, json.dumps(body).encode())

    def test_thinking_or_truncation_is_not_a_success(self):
        body = {"candidates":[{"finishReason":"MAX_TOKENS","content":{"parts":[{"text":'{"pause_minutes":0}'}]}}]}
        with self.assertRaises(ValueError):
            parse_pause(GEMINI_ENDPOINT, json.dumps(body).encode())


class WarmupTests(unittest.TestCase):
    def setUp(self):
        self.at = datetime(2026, 9, 21, 9, 32, tzinfo=IST)
        self.bars = [Bar(self.at.replace(hour=9, minute=15) + timedelta(minutes=5*i),
                         10000, 10020, 9990, 10010, 1000) for i in range(3)]

    def test_real_closed_history_enables_mid_session_warmup(self):
        tape = Tape()
        tape.seed(self.bars, self.at)
        tape.push(Tick("DEMO", self.at, 10010, 10009, 10011, 10000, 1000, 1000, 10005))
        self.assertTrue(tape.complete_opening)
        self.assertEqual(tape.first_price, 10000)
        self.assertEqual(tape.vwap, 10005)
        self.assertFalse(tape.bar.complete)

    def test_future_or_incomplete_history_is_rejected(self):
        with self.assertRaises(SafetyError):
            Tape().seed(self.bars + [replace(self.bars[-1], start=self.at.replace(minute=30))], self.at)
        with self.assertRaises(SafetyError):
            Tape().seed(self.bars[1:], self.at)

    def test_gap_between_seed_and_stream_fails_closed(self):
        tape = Tape()
        tape.seed(self.bars, self.at)
        with self.assertRaises(SafetyError):
            tape.push(Tick("DEMO", self.at.replace(minute=36), 10010, 10009, 10011, 10000, 10, 10))

    def test_partial_startup_bar_cannot_become_a_trade_signal(self):
        tape = Tape()
        tape.seed(self.bars, self.at)
        tape.push(Tick("DEMO", self.at, 10010, 10009, 10011, 10000, 1000, 1000))
        finished = tape.push(Tick("DEMO", self.at.replace(minute=35), 10020, 10019, 10021, 11000, 1000, 1000))
        self.assertFalse(finished.complete)
        self.assertTrue(tape.bar.complete)

    def test_daily_universe_rotation_preserves_cash_and_risk(self):
        with tempfile.TemporaryDirectory() as directory, Store(Path(directory)/"risk.db") as store:
            first = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
            session = Session(self.at.date(), True, True, ["DEMO"], [])
            instruments = {"DEMO":Instrument("DEMO",1,1,9000,11000),
                           "INDEX":Instrument("INDEX",2,1,1,10**12,True)}
            broker = PaperBroker(2500000, first.costs)
            engine = TradingEngine(first, session, instruments, broker, store, "live")
            engine.reconcile(broker.snapshot(self.at), self.at)
            engine.state["cash"] = 2450000
            engine._save()
            second = replace(first, market=replace(first.market, symbols=["OTHER"]))
            next_session = replace(session, day=session.day+timedelta(days=1), symbols=["OTHER"])
            rotated = TradingEngine(second, next_session,
                                    {"OTHER":Instrument("OTHER",3,1,9000,11000),"INDEX":instruments["INDEX"]},
                                    broker, store, "live", allow_daily_universe_change=True)
            self.assertEqual(rotated.state["cash"],2450000)
            self.assertEqual(rotated.state["capital"],2500000)


class SourceTests(unittest.TestCase):
    def test_official_local_ist_feed_dates(self):
        nse = feed_timestamp("21-Sep-2026 10:50:03")
        rbi = feed_timestamp("Mon, 21 Sep 2026 10:20:00")
        self.assertEqual(nse.utcoffset(), timedelta(hours=5,minutes=30))
        self.assertEqual(rbi.hour,10)

    def test_issuer_matching_ignores_irrelevant_announcements(self):
        config = default_auto_config(["HDFCBANK"], "TEST01")
        nse = b"""<rss><channel><item><title>HDFC Bank Limited</title><description>Financial results</description><pubDate>21-Sep-2026 10:50:03</pubDate></item><item><title>Unrelated Company Limited</title><description>Financial results</description><pubDate>21-Sep-2026 10:50:03</pubDate></item></channel></rss>"""
        rbi = b"<rss><channel><item><title>Routine release</title><description>Archive item</description><pubDate>Fri, 18 Sep 2026 10:20:00</pubDate></item></channel></rss>"
        feeds = {NEWS_SOURCES["nse-announcements"]:nse,NEWS_SOURCES["rbi-releases"]:rbi}
        collector = AutomaticNews(config, {company_key("HDFC Bank Ltd."):"HDFCBANK"}, fetch=feeds.__getitem__)
        with patch("india_trader.market_data.now_ist", return_value=datetime(2026,9,21,10,51,tzinfo=IST)):
            events=collector.poll()
        news=[x for x in events if x["type"]=="news"]
        self.assertEqual(len(news),1)
        self.assertEqual(news[0]["symbols"],["HDFCBANK"])
        self.assertIn("HDFCBANK",collector.excluded)
        self.assertEqual(sum(x["type"]=="heartbeat" for x in events),2)

    def test_publisher_html_does_not_count_as_a_healthy_feed(self):
        collector=AutomaticNews(default_auto_config(["HDFCBANK"],"TEST01"),{},fetch=lambda _:b"<html>challenge</html>")
        with self.assertRaises(SafetyError):
            collector.poll()
        self.assertTrue(all(not x["healthy"] for x in collector.health.values()))

    def test_empty_or_stale_required_feeds_block_entries(self):
        for data in (
            b"<rss><channel/></rss>",
            b"<rss><channel><item><title>Old</title><pubDate>01-Jan-2020 10:00:00</pubDate></item></channel></rss>",
        ):
            collector = AutomaticNews(default_auto_config(["HDFCBANK"], "TEST01"), {}, fetch=lambda _: data)
            with self.assertRaises(SafetyError):
                collector.poll()

    def test_liquidity_screen_does_not_chase_large_gaps_or_use_stale_quotes(self):
        at=datetime(2026,9,21,10,30,tzinfo=IST)
        quote={"last_price":100,"timestamp":at.isoformat(),"volume":200000,
               "depth":{"buy":[{"price":99.99}],"sell":[{"price":100.01}]},
               "lower_circuit_limit":90,"upper_circuit_limit":110,
               "ohlc":{"close":99.50,"open":100}}
        self.assertEqual(screen_quotes({"NSE:DEMO":quote},["DEMO"],2500000,at)[0]["symbol"],"DEMO")
        quote["ohlc"]["open"]=110
        self.assertEqual(screen_quotes({"NSE:DEMO":quote},["DEMO"],2500000,at),[])
        quote["ohlc"]["open"]=100
        quote["timestamp"]=(at-timedelta(days=1)).isoformat()
        self.assertEqual(screen_quotes({"NSE:DEMO":quote},["DEMO"],2500000,at),[])


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.directory=Path(self.temp.name)
        self.vault=CredentialVault(self.directory/"credentials.dat",TestOnlyProtector())
        self.controllers=[]

    def tearDown(self):
        for controller in self.controllers:
            controller.close()
        self.temp.cleanup()

    def create(self, prepare):
        controller=Controller(ROOT,self.directory,self.vault,prepare=prepare)
        controller._software_check=Mock()
        self.controllers.append(controller)
        return controller

    def test_keys_missing_never_start_preparation(self):
        prepare=Mock()
        controller=self.create(prepare)
        controller.start()
        controller.wake.set()
        self.assertFalse(prepare.called)
        self.assertEqual(controller.snapshot()["state"],"WAITING_CONFIG")

    def test_saved_authorization_autostarts_after_controller_restart(self):
        called=threading.Event()
        def prepare(*args):
            called.set()
            raise PreparationBlocked("MARKET_CLOSED","Test-only closed market.")
        self.vault.save(FAKE_KEYS,True)
        first=self.create(prepare)
        first.start();first.wake.set()
        self.assertTrue(called.wait(3))
        first.close()
        called.clear()
        second=self.create(prepare)
        second.start();second.wake.set()
        self.assertTrue(called.wait(3))
        self.assertTrue(second.snapshot()["credentials"]["saved"]["ai_api_key"])

    def test_explicit_stop_survives_restart(self):
        self.vault.save(FAKE_KEYS,True)
        prepare=Mock()
        first=self.create(prepare)
        first.stop();first.close()
        second=self.create(prepare)
        second.start();second.wake.set()
        self.assertTrue((self.directory/"PAUSED").exists())
        self.assertFalse(self.vault.load()["auto_start"])
        self.assertFalse(prepare.called)

    def test_wrong_broker_callback_state_never_exchanges_credentials(self):
        self.vault.save(FAKE_KEYS,True)
        controller=self.create(Mock())
        url=controller.begin_login()
        self.assertTrue(url.startswith("https://kite.zerodha.com/connect/login?"))
        self.assertNotIn(FAKE_KEYS["broker_api_secret"],url)
        with patch("india_trader.dashboard.exchange_broker_token") as exchange:
            with self.assertRaises(SafetyError):
                controller.finish_login("wrong-state","test-only-request-token")
            exchange.assert_not_called()

    def test_live_badge_requires_running_worker_and_fresh_runtime(self):
        self.vault.save(FAKE_KEYS,True)
        controller=self.create(Mock())
        workspace=account_directory(self.directory,"TEST01")
        workspace.mkdir(parents=True)
        atomic_json(self.directory/"active-account.json",{"directory":str(workspace)})
        atomic_json(workspace/"plan.json",{"account":"TEST01","universe_source":"test","screen":[]})
        with Store(workspace/"live.db") as store:
            config=Config(market=replace(Config().market,symbols=["DEMO"],benchmark="INDEX"))
            broker=PaperBroker(2500000,config.costs)
            TradingEngine(config,Session(datetime.now(IST).date(),True,True,["DEMO"],[]),
                          {"DEMO":Instrument("DEMO",1,1,9000,11000),"INDEX":Instrument("INDEX",2,1,1,10**12,True)},
                          broker,store,"live")
        runtime = {
            "at":datetime.now(IST).isoformat(),"status":"LIVE","reason":"Test data",
            "live":True,"entries_allowed":True,"news_health":{},"quotes":{},
        }
        with Store(workspace/"live.db") as store:
            store.put("runtime", runtime)
        (workspace/"runtime.json").write_text("Legacy runtime JSON is no longer the live snapshot.")
        self.assertFalse(controller.snapshot()["live"])
        controller.worker=Mock()
        controller.worker.poll.return_value=None
        self.assertTrue(controller.snapshot()["live"])
        runtime["at"]=(datetime.now(IST)-timedelta(minutes=1)).isoformat()
        with Store(workspace/"live.db") as store:
            store.put("runtime", runtime)
        self.assertFalse(controller.snapshot()["live"])
        self.assertEqual(controller.snapshot()["state"],"BLOCKED")
        controller.worker=None

    def test_runtime_entry_marker_respects_event_pauses(self):
        config = default_auto_config(["DEMO"], "TEST01")
        at = datetime(2026, 9, 21, 10, 30, tzinfo=IST)
        instruments = {"DEMO":Instrument("DEMO",1,1,9000,11000),
                       "NIFTY 50":Instrument("NIFTY 50",2,1,1,10**12,True)}
        with Store(self.directory / "marker.db") as store:
            broker = PaperBroker(2500000, config.costs)
            engine = TradingEngine(config, Session(at.date(),True,True,["DEMO"],[]),
                                   instruments,broker,store,"live")
            engine.reconcile(broker.snapshot(at),at)
            engine.quotes["DEMO"] = Tick("DEMO",at,10000,9999,10001,10000,1000,1000)
            engine.quotes["NIFTY 50"] = Tick("NIFTY 50",at,2500000,2500000,2500000,0,0,0)
            for source in config.news.required_sources:
                engine.heartbeat_news(at,source)
            managed = ManagedRun(self.directory,self.directory,{"policy":POLICY_VERSION},FAKE_KEYS)
            news = Mock()
            news.health = {}
            managed.publish(engine,news,at)
            self.assertTrue(store.get("runtime")["entries_allowed"])
            engine.pause(["*"],at+timedelta(minutes=30),at,"test uncertainty")
            managed.publish(engine,news,at)
            view = store.get("runtime")
            self.assertTrue(view["live"])
            self.assertFalse(view["entries_allowed"])

    def test_verified_account_is_visible_without_a_plan_or_trade_ledger(self):
        self.vault.save(FAKE_KEYS, True)
        now = datetime(2026, 9, 24, 11, 0, tzinfo=IST)
        record_broker_connection(self.directory, FAKE_KEYS,
                                 {"broker": "ZERODHA", "user_id": "TEST01"}, now)
        controller = self.create(Mock())
        controller._set("NEEDS_CASH", "Synthetic insufficient-cash condition.")
        with patch("india_trader.dashboard.now_ist", return_value=now):
            view = controller.snapshot()
        self.assertEqual(view["account"]["id_masked"], "****ST01")
        self.assertEqual(view["account"]["authentication_status"], "verified")
        self.assertIsNone(view["metrics"])
        self.assertFalse(view["live"])
        self.assertFalse(view["worker_running"])
        self.assertFalse((self.directory / "active-account.json").exists())
        self.assertNotIn("session_fingerprint", json.dumps(view))
        saved = (self.directory / "broker-connection.json").read_text()
        self.assertNotIn("TEST01", saved)
        for key in FAKE_KEYS.values():
            self.assertNotIn(key, saved)
            self.assertNotIn(key, json.dumps(view))

    def test_identity_does_not_survive_a_different_broker_key_or_token(self):
        now = datetime(2026, 9, 24, 11, 0, tzinfo=IST)
        record_broker_connection(self.directory, FAKE_KEYS,
                                 {"broker": "ZERODHA", "user_id": "TEST01"}, now)
        for changed in ({"broker_access_token": "test-only-other-token"},
                        {"broker_api_key": "test-only-other-key"}, {"broker_access_token": ""}):
            with self.subTest(changed=changed):
                self.assertIsNone(broker_connection(self.directory, {**FAKE_KEYS, **changed}, now))
        same_session = {**FAKE_KEYS, "ai_api_key": "test-only-different-ai"}
        self.assertIsNotNone(broker_connection(self.directory, same_session, now))

    def test_account_metadata_does_not_claim_valid_auth_after_six_am_or_rejection(self):
        self.vault.save(FAKE_KEYS, True)
        now = datetime(2026, 9, 24, 11, 0, tzinfo=IST)
        record_broker_connection(self.directory, FAKE_KEYS,
                                 {"broker": "ZERODHA", "user_id": "TEST01"}, now)
        expiry = now.replace(hour=6) + timedelta(days=1)
        self.assertEqual(broker_connection(self.directory, FAKE_KEYS, expiry)["authentication_status"],
                         "reauthentication_required")
        controller = self.create(Mock())
        controller._set("BROKER_LOGIN_REQUIRED", "Broker session revoked.")
        with patch("india_trader.dashboard.now_ist", return_value=now):
            self.assertEqual(controller.snapshot()["account"]["authentication_status"],
                             "reauthentication_required")

    def test_forgetting_keys_also_removes_verified_identity(self):
        self.vault.save(FAKE_KEYS, True)
        record_broker_connection(self.directory, FAKE_KEYS,
                                 {"broker": "ZERODHA", "user_id": "TEST01"}, datetime.now(IST))
        controller = self.create(Mock())
        controller.forget()
        self.assertFalse((self.directory / "broker-connection.json").exists())
        self.assertIsNone(controller.snapshot()["account"])


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.vault = CredentialVault(self.directory / "credentials.dat", TestOnlyProtector())
        self.vault.save(FAKE_KEYS, True)
        self.at = datetime(2026, 9, 21, 10, 30, tzinfo=IST)

    def tearDown(self):
        self.temp.cleanup()

    def test_mf_only_profile_identifies_nse_not_order_types_as_missing(self):
        profile = {"broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["MF"],
                   "products": ["CNC"], "order_types": ["LIMIT", "SL"]}
        with self.assertRaises(PreparationBlocked) as error:
            validate_broker_capabilities(profile)
        self.assertEqual(error.exception.state, "BLOCKED")
        self.assertIn("Missing broker permissions: NSE cash exchange.", str(error.exception))
        self.assertIn("enabled exchanges: MF", str(error.exception))
        self.assertIn("enable/reactivate NSE equity", str(error.exception))
        self.assertNotIn("CNC product", str(error.exception))
        self.assertNotIn("SL orders", str(error.exception))

    def test_supported_profile_retains_all_required_checks(self):
        profile = {"broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["MF", "NSE"],
                   "products": ["CNC"], "order_types": ["LIMIT", "SL"]}
        validate_broker_capabilities(profile)
        for field, code, label in (("products", "CNC", "CNC product"),
                                  ("order_types", "LIMIT", "LIMIT orders"),
                                  ("order_types", "SL", "SL orders")):
            broken = {key: list(value) if isinstance(value, list) else value for key, value in profile.items()}
            broken[field].remove(code)
            with self.subTest(code=code), self.assertRaises(PreparationBlocked) as error:
                validate_broker_capabilities(broken)
            self.assertIn(label, str(error.exception))
            self.assertNotIn("NSE cash exchange", str(error.exception))

    def test_invalid_capability_shape_or_identity_does_not_pass(self):
        valid = {"broker": "ZERODHA", "exchanges": ["NSE"], "products": ["CNC"],
                 "order_types": ["LIMIT", "SL"]}
        for invalid in ({**valid, "broker": "OTHER"}, {**valid, "exchanges": "NSE"},
                        {**valid, "order_types": None}, {**valid, "products": ["CNC", 1]}):
            with self.subTest(profile=invalid), self.assertRaises(PreparationBlocked):
                validate_broker_capabilities(invalid)

    def test_mf_only_account_stops_before_other_preparation_calls(self):
        http = Mock()
        http.request.return_value = {
            "broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["MF"],
            "products": ["CNC"], "order_types": ["LIMIT", "SL"],
        }
        with patch("india_trader.autonomy.now_ist", return_value=self.at), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", return_value=http) as client, \
             patch("india_trader.autonomy.verify_gemini_key") as ai:
            with self.assertRaises(PreparationBlocked):
                prepare_session(ROOT, self.directory, self.vault)
            client.assert_called_once()
            self.assertFalse(client.call_args.args[1])
            http.request.assert_called_once_with("GET", "/user/profile")
            ai.assert_not_called()
        self.assertFalse((self.directory / "active-account.json").exists())

    def test_saved_credentials_do_not_bypass_software_gate(self):
        with patch("india_trader.autonomy.now_ist", return_value=self.at), \
             patch("india_trader.autonomy.software_ready", return_value=False), \
             patch("india_trader.autonomy.KiteHTTP") as http:
            with self.assertRaises(PreparationBlocked) as error:
                prepare_session(ROOT, self.directory, self.vault)
            self.assertEqual(error.exception.state, "CHECKS_REQUIRED")
            http.assert_not_called()

    def test_closed_market_makes_no_broker_or_ai_calls(self):
        with patch("india_trader.autonomy.now_ist", return_value=self.at.replace(hour=20)), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP") as http, \
             patch("india_trader.autonomy.verify_gemini_key") as ai:
            with self.assertRaises(PreparationBlocked) as error:
                prepare_session(ROOT, self.directory, self.vault)
            self.assertEqual(error.exception.state, "MARKET_CLOSED")
            http.assert_not_called()
            ai.assert_not_called()

    def test_cash_loaded_before_start_is_bounded_by_broker_available_cash(self):
        raw = {"available": {"cash": 25000, "intraday_payin": 25000,
                             "live_balance": 24000, "collateral": 0},
               "utilised": {}, "net": 23000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 2300000)

    def test_intraday_payin_is_not_added_again_or_used_to_cover_adhoc_margin(self):
        raw = {"available": {"cash": 10000, "intraday_payin": 5000,
                             "live_balance": 15000, "collateral": 0, "adhoc_margin": 5000},
               "utilised": {}, "net": 15000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 1000000)

    def test_split_payin_uses_opening_balance_including_negative_carryover(self):
        raw = {"enabled": True, "available": {
            "cash": 0, "opening_balance": -50.25, "intraday_payin": 1250,
            "live_balance": 1199.75, "collateral": 0, "adhoc_margin": 0,
        }, "utilised": {"debits": 0, "payout": 0}, "net": 1199.75}
        self.assertEqual(KiteBroker.conservative_cash(raw), 119975)

    def test_deposit_not_added_twice_when_cash_already_includes_it(self):
        raw = {"available": {"cash": 1250, "opening_balance": 250, "intraday_payin": 1000,
                             "live_balance": 2000, "collateral": 0},
               "utilised": {}, "net": 2000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 125000)

    def test_opening_plus_payin_does_not_override_blocked_funds_or_withdrawals(self):
        raw = {"available": {"cash": 0, "opening_balance": 10000, "intraday_payin": 1000,
                             "live_balance": 6500, "collateral": 0},
               "utilised": {"debits": 4000, "payout": 500}, "net": 6500}
        self.assertEqual(KiteBroker.conservative_cash(raw), 650000)
        raw["net"] = 6000
        self.assertEqual(KiteBroker.conservative_cash(raw), 600000)

    def test_margin_cannot_make_encumbered_cash_spendable(self):
        raw = {"available": {"cash": 10000, "opening_balance": 10000, "intraday_payin": 0,
                             "live_balance": 5000, "collateral": 0, "adhoc_margin": 5000},
               "utilised": {"debits": 10000}, "net": 5000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 0)

    def test_nonpositive_live_funds_or_negative_carryover_produce_no_cash(self):
        raw = {"available": {"cash": 0, "opening_balance": -2000, "intraday_payin": 1000,
                             "live_balance": 1000, "collateral": 0}, "utilised": {}, "net": 1000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 0)
        raw["available"]["opening_balance"] = 2000
        for live, net in ((0, 3000), (3000, 0), (-100, 3000), (3000, -100)):
            with self.subTest(live=live, net=net):
                raw["available"]["live_balance"], raw["net"] = live, net
                self.assertEqual(KiteBroker.conservative_cash(raw), 0)

    def test_deposit_without_opening_balance_does_not_invent_extra_cash(self):
        raw = {"available": {"cash": 0, "intraday_payin": 5000, "live_balance": 5000, "collateral": 0},
               "utilised": {}, "net": 5000}
        self.assertEqual(KiteBroker.conservative_cash(raw), 0)

    def test_disabled_or_malformed_cash_payload_fails_closed(self):
        raw = {"enabled": False, "available": {
            "cash": 0, "opening_balance": 1000, "intraday_payin": 1000,
            "live_balance": 2000, "collateral": 0,
        }, "utilised": {}, "net": 2000}
        with self.assertRaises(SafetyError):
            KiteBroker.conservative_cash(raw)
        raw["enabled"] = True
        raw["available"]["opening_balance"] = "NaN"
        with self.assertRaises(ValueError):
            KiteBroker.conservative_cash(raw)

    def test_insufficient_cash_diagnostic_does_not_hide_authenticated_identity(self):
        http = Mock()
        http.request.side_effect = [
            {"broker": "ZERODHA", "user_id": "TEST01", "exchanges": ["NSE"],
             "products": ["CNC"], "order_types": ["LIMIT", "SL"]},
            [],
            {"available": {"cash": 0, "opening_balance": -10, "intraday_payin": 100,
                           "live_balance": 90, "collateral": 0}, "utilised": {}, "net": 90},
        ]
        with patch("india_trader.autonomy.now_ist", return_value=self.at), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", return_value=http), \
             patch("india_trader.autonomy.verify_gemini_key"), \
             patch("india_trader.pre_market.global_context", return_value={"opening_blackout": None}):
            with self.assertRaises(PreparationBlocked) as error:
                prepare_session(ROOT, self.directory, self.vault)
        self.assertEqual(error.exception.state, "NEEDS_CASH")
        self.assertIn("INR 90.00", str(error.exception))
        self.assertIsNotNone(broker_connection(self.directory, FAKE_KEYS, self.at))
        self.assertFalse((self.directory / "active-account.json").exists())
        self.assertTrue(all(call.args[0] == "GET" for call in http.request.call_args_list))

    def test_valid_mocked_sources_produce_automatic_plan_without_trade_calls(self):
        calls = []
        at = self.at
        quote = {"last_price":100,"timestamp":at.isoformat(),"volume":200000,
                 "depth":{"buy":[{"price":99.99}],"sell":[{"price":100.01}]},
                 "lower_circuit_limit":90,"upper_circuit_limit":110,
                 "ohlc":{"close":99.50,"open":100}}

        class ReadOnlyHTTP:
            def __init__(self, config, allow_orders, **kwargs):
                self.config = config
                assert not allow_orders

            def request(self, method, path, **kwargs):
                calls.append((method, path))
                assert method == "GET"
                if path == "/user/profile":
                    return {"broker":"ZERODHA","user_id":"TEST01","exchanges":["NSE"],
                            "products":["CNC"],"order_types":["LIMIT","SL"]}
                if path == "/portfolio/holdings":
                    return []
                if path == "/user/margins/equity":
                    return {"available":{"cash":0,"opening_balance":-50,"intraday_payin":25050,
                                         "live_balance":25000,"collateral":0},
                            "utilised":{},"net":25000}
                if path == "/instruments/NSE":
                    return ("tradingsymbol,exchange,segment,instrument_type,lot_size,expiry,instrument_token,name\n"
                            "DEMO,NSE,NSE,EQ,1,,1,Demo Limited\nNIFTY 50,NSE,INDICES,EQ,1,,2,NIFTY 50\n")
                if path == "/quote":
                    return {"NSE:DEMO":quote, "NSE:NIFTY 50":quote}
                raise AssertionError(path)

        news = Mock()
        news.excluded = set()
        news.catalysts = {}
        news.health = {name:{"healthy":True} for name in NEWS_SOURCES}
        instruments = {"DEMO":Instrument("DEMO",1,1,9000,11000),
                       "NIFTY 50":Instrument("NIFTY 50",2,1,1,10**12,True)}
        opening = at.replace(hour=9, minute=15)
        bars = [Bar(opening+timedelta(minutes=5*i),10000,10020,9990,10010,1000) for i in range(15)]
        with patch("india_trader.autonomy.now_ist", return_value=at), \
             patch("india_trader.autonomy.software_ready", return_value=True), \
             patch("india_trader.autonomy.KiteHTTP", ReadOnlyHTTP), \
             patch("india_trader.autonomy.verify_gemini_key") as ai, \
             patch("india_trader.pre_market.global_context", return_value={"opening_blackout": None}), \
             patch("india_trader.autonomy.constituents", return_value=(["DEMO"],{"DEMO":"DEMO"},{"DEMO":"Test sector"},"test source")), \
             patch("india_trader.autonomy.AutomaticNews", return_value=news), \
             patch("india_trader.autonomy.cached_daily_history", return_value={
                 "atr_bps":150, "last_close_paise":9950, "last_session":"2026-09-18",
                 "average_turnover_paise":1000000000, "return_5d_bps":100, "return_20d_bps":200}), \
             patch("india_trader.autonomy.load_kite_instruments", return_value=instruments), \
             patch("india_trader.autonomy.closed_intraday_bars", return_value=bars):
            plan = prepare_session(ROOT, self.directory, self.vault)
        self.assertEqual(plan["selected"], ["DEMO"])
        self.assertEqual(plan["config"]["risk"]["capital_rupees"], 25000)
        self.assertTrue(plan["config"]["live"]["enabled"])
        self.assertFalse(plan["recovery_only"])
        self.assertNotIn(FAKE_KEYS["ai_api_key"], json.dumps(plan))
        self.assertTrue(all(method=="GET" for method, _ in calls))
        self.assertTrue((self.directory/"active-account.json").exists())
        ai.assert_called_once_with(FAKE_KEYS["ai_api_key"])

    @unittest.skipUnless(os.name == "nt", "Production managed authorization uses Windows DPAPI")
    def test_managed_authorization_cannot_raise_limits_or_use_replaced_keys(self):
        real = CredentialVault(self.directory / "credentials.dat")
        self.vault.forget()
        real.save(FAKE_KEYS, True)
        config = default_auto_config(["DEMO"], "TEST01")
        at = datetime.now(IST)
        plan = {
            "policy":POLICY_VERSION, "day":at.date().isoformat(),"selected":["DEMO"],
            "account":"TEST01","credential_fingerprint":hashlib.sha256(json.dumps(FAKE_KEYS,sort_keys=True).encode()).hexdigest(),
        }
        session = Session(at.date(),True,True,["DEMO"],[],True,"TEST01",25000,config.fingerprint)
        managed = ManagedRun(self.directory,self.directory/"accounts"/"test",plan,dict(FAKE_KEYS))
        with patch("india_trader.autonomy.software_ready",return_value=True):
            managed.authorize(ROOT,config,session)
            bigger = replace(config,risk=replace(config.risk,capital_rupees=100000))
            with self.assertRaises(SafetyError):
                managed.authorize(ROOT,bigger,replace(session,config_hash=bigger.fingerprint))
            real.set_access_token("test-only-new-token")
            with self.assertRaises(SafetyError):
                managed.authorize(ROOT,config,session)


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.directory=Path(self.temp.name)
        vault=CredentialVault(self.directory/"credentials.dat",TestOnlyProtector())
        self.controller=Controller(ROOT,self.directory,vault,prepare=Mock())
        self.server=DashboardServer(("127.0.0.1",0),self.controller,
                                    ROOT/"india_trader"/"web"/"dashboard.html","test-only-ui-capability")
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.origin=self.server.origin
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join(3)
        self.controller.close();self.temp.cleanup()

    def request(self,path="/api/status",body=None,authorized=True,origin=None,host=None):
        headers={}
        if authorized:headers["X-Dashboard-Token"]="test-only-ui-capability"
        if body is not None:
            headers["Content-Type"]="application/json"
            headers["Origin"]=self.origin if origin is None else origin
        elif origin is not None:
            headers["Origin"]=origin
        if host:headers["Host"]=host
        request=urllib.request.Request(self.origin+path,headers=headers,
                    data=json.dumps(body).encode() if body is not None else None)
        try:
            response=self.opener.open(request,timeout=3)
        except urllib.error.HTTPError as exc:
            response=exc
        with response:
            return response.status,response.headers,response.read()

    def test_status_requires_local_session_capability(self):
        self.assertEqual(self.request(authorized=False)[0],403)
        status,_,body=self.request()
        self.assertEqual(status,200)
        self.assertFalse(json.loads(body)["live"])
        self.assertEqual(json.loads(body)["state"],"WAITING_CONFIG")

    def test_csrf_and_dns_rebinding_are_rejected(self):
        body={"keys":FAKE_KEYS,"authorize_live":True}
        self.assertEqual(self.request("/api/config",body,origin="https://untrusted.example")[0],403)
        self.assertEqual(self.request(host="untrusted.example")[0],403)
        self.assertFalse(self.controller.vault.path.exists())

    def test_saved_keys_are_not_returned_by_config_or_status(self):
        code,_,body=self.request("/api/config",{"keys":FAKE_KEYS,"authorize_live":True})
        self.assertEqual(code,200)
        for path in ("/api/config","/api/status"):
            code,_,body=self.request(path)
            self.assertEqual(code,200)
            for key in FAKE_KEYS.values():
                self.assertNotIn(key.encode(),body)
        self.assertTrue(self.controller.vault.public_status()["auto_start"])

    def test_status_reports_verified_account_while_cash_gate_is_blocked(self):
        self.controller.vault.save(FAKE_KEYS, True)
        record_broker_connection(self.directory, FAKE_KEYS,
                                 {"broker": "ZERODHA", "user_id": "TEST01"}, datetime.now(IST))
        self.controller._set("NEEDS_CASH", "Synthetic low-cash test.")
        code, _, body = self.request()
        view = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(view["state"], "NEEDS_CASH")
        self.assertEqual(view["account"]["id_masked"], "****ST01")
        self.assertEqual(view["account"]["authentication_status"], "verified")
        self.assertFalse(view["live"])
        self.assertFalse(view["worker_running"])
        self.assertIsNone(view["metrics"])
        self.assertNotIn("session_fingerprint", view["account"])

    def test_page_has_nonce_csp_no_remote_assets_and_theme(self):
        code,headers,body=self.request("/",authorized=False)
        self.assertEqual(code,200)
        self.assertIn("frame-ancestors 'none'",headers["Content-Security-Policy"])
        self.assertIn("script-src 'nonce-",headers["Content-Security-Policy"])
        self.assertNotIn(b"__NONCE__",body)
        self.assertIn(b'scoutTheme',body)
        self.assertIn(b'--cp-bg: #f7f4ef;',body)
        self.assertNotIn(b'<script src="https://',body)
        self.assertEqual(headers["Cache-Control"],"no-store")

    def test_no_arbitrary_file_or_secret_routes(self):
        for path in ("/credentials.dat","/api/keys","/../../credentials.dat","/api/fund-transfer"):
            self.assertEqual(self.request(path)[0],404)

    def test_callback_cannot_be_forged(self):
        with patch("india_trader.dashboard.exchange_broker_token") as exchange:
            code,_,_=self.request("/broker/callback?state=bad&request_token=test",authorized=False)
            self.assertEqual(code,400)
            exchange.assert_not_called()

    def test_stop_and_forget_are_explicit_and_persistent(self):
        self.request("/api/config",{"keys":FAKE_KEYS,"authorize_live":True})
        self.assertEqual(self.request("/api/stop",{})[0],200)
        self.assertTrue((self.directory/"PAUSED").exists())
        self.assertFalse(self.controller.vault.load()["auto_start"])
        self.assertEqual(self.request("/api/forget",{"confirm":True})[0],200)
        self.assertFalse(self.controller.vault.path.exists())


if __name__ == "__main__":
    unittest.main()
