from __future__ import annotations

import errno
import io
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.cli import failure_message
from india_trader.broker import PaperBroker
from india_trader.core import Config, IST, Instrument, SafetyError, Session
from india_trader.credentials import (
    atomic_bytes, atomic_json, read_atomic_json,
)
from india_trader.dashboard import Controller
from india_trader.engine import TradingEngine
from india_trader.runtime import ManagedRun
from india_trader.storage import Store


class AtomicStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.target = self.directory / "runtime.json"

    def tearDown(self):
        self.temp.cleanup()

    def engine(self, store):
        config = Config(market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"))
        at = datetime(2026, 9, 24, 10, 0, tzinfo=IST)
        broker = PaperBroker(2500000, config.costs)
        engine = TradingEngine(config, Session(at.date(), True, True, ["DEMO"], []),
                               {"DEMO": Instrument("DEMO", 1, 1, 9000, 11000),
                                "INDEX": Instrument("INDEX", 2, 1, 1, 10**12, True)},
                               broker, store, "paper")
        engine.reconcile(broker.snapshot(at), at)
        managed = ManagedRun(self.directory, self.directory, {"policy": "test-policy"}, {})
        news = Mock()
        news.health = {}
        return engine, managed, news, at

    def test_runtime_publication_ignores_locked_legacy_json_and_updates_ledger(self):
        atomic_json(self.target, {"sequence": 1, "payload": "old"})
        with Store(self.directory / "state.db") as store:
            engine, managed, news, at = self.engine(store)
            with self.target.open("rb") as reader:
                managed.publish(engine, news, at)
                self.assertEqual(store.get("runtime")["at"], at.isoformat())
                self.assertFalse(store.get("runtime")["live"])
                self.assertEqual(json.loads(reader.read()), {"sequence": 1, "payload": "old"})

    def test_metadata_reader_closes_its_handle(self):
        atomic_json(self.target, {"sequence": 1})
        self.assertEqual(read_atomic_json(self.target), {"sequence": 1})
        self.target.unlink()
        self.assertFalse(self.target.exists())

    def test_missing_file_is_not_replaced_by_empty_success(self):
        with self.assertRaises(FileNotFoundError):
            read_atomic_json(self.target)

    def test_invalid_json_and_nonobject_state_are_reported(self):
        for content, error in ((b"not-json", ValueError), (b"[]", SafetyError), (b"null", SafetyError)):
            with self.subTest(content=content):
                atomic_bytes(self.target, content)
                with self.assertRaises(error):
                    read_atomic_json(self.target)

    @unittest.skipUnless(os.name == "nt", "Windows sharing semantics")
    def test_transient_uncooperative_reader_is_retried(self):
        atomic_json(self.target, {"sequence": 1})
        opened = threading.Event()

        def brief_reader():
            with self.target.open("rb"):
                opened.set()
                threading.Event().wait(0.05)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(brief_reader)
            self.assertTrue(opened.wait(2))
            atomic_json(self.target, {"sequence": 2})
            future.result(timeout=2)
        self.assertEqual(read_atomic_json(self.target), {"sequence": 2})
        self.assertEqual(list(self.directory.glob("runtime.json.*")), [])

    @unittest.skipUnless(os.name == "nt", "Windows sharing semantics")
    def test_persistent_lock_fails_bounded_and_preserves_previous_snapshot(self):
        atomic_json(self.target, {"sequence": 1})
        with self.target.open("rb") as reader, patch("india_trader.credentials.clock.sleep") as sleep:
            with self.assertRaises(PermissionError):
                atomic_json(self.target, {"sequence": 2})
            self.assertEqual(json.loads(reader.read()), {"sequence": 1})
        self.assertEqual(sleep.call_count, 5)
        self.assertAlmostEqual(sum(call.args[0] for call in sleep.call_args_list), 0.31)
        self.assertEqual(read_atomic_json(self.target), {"sequence": 1})
        self.assertEqual(list(self.directory.glob("runtime.json.*")), [])

    @unittest.skipUnless(os.name == "nt", "Windows retry classification")
    def test_nonsharing_permission_failure_is_not_retried(self):
        atomic_json(self.target, {"sequence": 1})
        error = PermissionError(errno.EACCES, "Test-only denial.")
        with patch("india_trader.credentials.os.replace", side_effect=error) as replace, \
             patch("india_trader.credentials.clock.sleep") as sleep:
            with self.assertRaises(PermissionError):
                atomic_json(self.target, {"sequence": 2})
        replace.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(read_atomic_json(self.target), {"sequence": 1})

    def test_disk_failure_is_not_hidden_by_retry_or_partial_overwrite(self):
        atomic_json(self.target, {"sequence": 1})
        with patch("india_trader.credentials.os.replace", side_effect=OSError(errno.ENOSPC, "disk full")) as replace, \
             patch("india_trader.credentials.clock.sleep") as sleep:
            with self.assertRaises(OSError):
                atomic_json(self.target, {"sequence": 2})
        replace.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(read_atomic_json(self.target), {"sequence": 1})
        self.assertEqual(list(self.directory.glob("runtime.json.*")), [])

    def test_concurrent_status_reads_never_observe_partial_json(self):
        database = self.directory / "state.db"
        stop = threading.Event()
        started = threading.Event()

        def read_until_stopped():
            count = 0
            started.set()
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
                while not stop.is_set():
                    row = connection.execute("SELECT value FROM state WHERE key='runtime'").fetchone()
                    value = json.loads(row[0])
                    self.assertEqual(value["config_policy"], "test-policy")
                    self.assertEqual(value["capital_paise"], 2500000)
                    self.assertFalse(value["live"])
                    count += 1
            return count

        with Store(database) as store, ThreadPoolExecutor(max_workers=1) as pool:
            engine, managed, news, at = self.engine(store)
            managed.publish(engine, news, at)
            reader = pool.submit(read_until_stopped)
            try:
                self.assertTrue(started.wait(2))
                for sequence in range(1, 61):
                    managed.publish(engine, news, at + timedelta(seconds=sequence))
            finally:
                stop.set()
            self.assertGreater(reader.result(timeout=5), 0)
            self.assertEqual(store.get("runtime")["at"], (at + timedelta(seconds=60)).isoformat())

    def test_permission_failure_message_names_only_the_local_leaf(self):
        error = PermissionError(errno.EACCES, "untrusted full error text")
        error.filename = str(self.directory / "temporary-secret-path")
        error.filename2 = str(self.directory / "runtime.json")
        message = failure_message(error)
        self.assertIn("runtime.json", message)
        self.assertIn("PermissionError", message)
        self.assertNotIn(str(self.directory), message)
        self.assertNotIn("untrusted full error text", message)
        self.assertNotIn("temporary-secret-path", message)

    def test_controller_surfaces_redacted_current_run_failure_and_clears_old_one(self):
        vault = Mock()
        vault.load.return_value = {"keys": {"broker_api_key": "test-only-sensitive-key"}, "auto_start": True}
        controller = Controller(self.directory, self.directory, vault)
        workspace = self.directory / "accounts" / "synthetic"
        workspace.mkdir(parents=True)
        original = {"cash": 123000, "capital": 250000, "position": None, "orders": [], "halt": ""}
        with Store(workspace / "live.db") as store:
            store.put("engine", original)
            store.put("runtime", {"live": True, "reason": "stale previous run"})
        process = Mock()
        process.stdout = io.StringIO(
            "An ordinary startup line.\n"
            "STOPPED: Local file access blocked for runtime.json. test-only-sensitive-key\n"
        )
        process.poll.return_value = 2
        with patch("india_trader.dashboard.subprocess.Popen", return_value=process):
            controller._launch_worker({"database": str(workspace / "live.db")})
        with Store(workspace / "live.db") as store:
            self.assertEqual(store.get("runtime"), {})
            self.assertEqual(store.get("engine"), original)
        controller.reader.join(timeout=2)
        controller.closed = Mock()
        controller.closed.is_set.side_effect = [False, True]
        controller.wake.set()
        controller._loop()
        self.assertIn("Worker stopped: Local file access blocked for runtime.json", controller.reason)
        self.assertNotIn("test-only-sensitive-key", controller.reason)
        self.assertIn("[redacted]", controller.reason)
        self.assertNotIn("test-only-sensitive-key", (workspace / "worker.log").read_text())
        next_process = Mock()
        next_process.stdout = io.StringIO("")
        next_process.poll.return_value = 2
        with patch("india_trader.dashboard.subprocess.Popen", return_value=next_process):
            controller._launch_worker({"database": str(workspace / "live.db")})
        controller.reader.join(timeout=2)
        self.assertEqual(controller.worker_failure, "")
        controller.worker = None


if __name__ == "__main__":
    unittest.main()
