from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import signal
import sqlite3
import subprocess
import sys
import threading
import time as clock
import urllib.parse
import webbrowser
from collections import Counter
from datetime import datetime, timedelta
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .ai_provider import GEMINI_MODEL
from .autonomy import (
    POLICY_VERSION, PreparationBlocked, broker_connection, broker_login_url, exchange_broker_token,
    has_exposure, prepare_session, read_ledger, software_ready,
)
from .core import Config, SafetyError, bps, now_ist, timestamp
from .broker import BrokerError
from .credentials import (
    CredentialVault, WindowsProtector, application_directory, atomic_bytes,
    read_atomic_json,
)
from .storage import InstanceLock, Store
from .streaming import retry_delay


def participation_summary(events: list[dict]) -> dict:
    signals = [event for event in events if event["kind"] == "SIGNAL_EVALUATED"]
    failures: Counter[str] = Counter()
    rejections: Counter[str] = Counter()
    candidates = 0
    latest_risk = None
    for event in signals:
        if not event.get("checks", {}).get("entry_window", False):
            continue
        failures.update(name for name, passed in event["checks"].items() if not passed)
        candidates += bool(event.get("setup"))
        if event.get("reason", "").startswith("Risk gate: "):
            rejections[event["reason"].removeprefix("Risk gate: ")] += 1
            if event.get("risk_details"):
                latest_risk = {key: event[key] for key in ("symbol", "bar_end", "reason", "risk_details")}
    return {
        "evaluated_bars": len(signals),
        "entry_window_bars": sum(bool(item.get("checks", {}).get("entry_window")) for item in signals),
        "qualified_signals": candidates,
        "entry_plans": sum(item["kind"] == "ENTRY_PLAN" for item in events),
        "top_signal_blocks": [{"reason": reason.replace("_", " "), "count": count}
                              for reason, count in failures.most_common(4)],
        "risk_rejections": [{"reason": reason, "count": count} for reason, count in rejections.most_common(4)],
        "latest_risk_details": latest_risk,
    }


class Controller:
    def __init__(self, root: Path, directory: Path, vault: CredentialVault, *, prepare=prepare_session):
        self.root, self.directory, self.vault, self.prepare = root, directory, vault, prepare
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.closed = threading.Event()
        self.worker: subprocess.Popen | None = None
        self.reader: threading.Thread | None = None
        self.worker_failure = ""
        self.worker_waiting_for_history = False
        self.phase, self.reason = "WAITING_CONFIG", "Save your AI and broker API credentials to begin."
        self.next_attempt = 0.0
        self.blocked_day = ""
        self.pending_login: tuple[str, datetime] | None = None
        self.state_thread = threading.Thread(target=self._loop, name="dashboard-controller", daemon=True)
        self.directory.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        self.state_thread.start()

    def _set(self, state: str, reason: str) -> None:
        with self.lock:
            self.phase, self.reason = state, reason

    def workspace(self) -> Path | None:
        path = self.directory / "active-account.json"
        if not path.exists():
            return None
        item = read_atomic_json(path)
        workspace = Path(item["directory"]).resolve()
        if workspace.parent != (self.directory / "accounts").resolve():
            raise SafetyError("Account workspace is outside the protected application directory.")
        return workspace

    def busy(self) -> bool:
        with self.lock:
            running = self.worker is not None and self.worker.poll() is None
        workspace = self.workspace()
        return running or (workspace is not None and has_exposure(read_ledger(workspace / "live.db")))

    def running(self) -> bool:
        with self.lock:
            return self.worker is not None and self.worker.poll() is None

    def save_credentials(self, body: dict) -> dict:
        if body.get("authorize_live") is not True or not isinstance(body.get("keys"), dict):
            raise SafetyError("Use Save & authorize live startup to accept the displayed cash-only limits.")
        if self.running():
            raise SafetyError("Stop and confirm flatness before replacing credentials.")
        if self.busy():
            previous = self.vault.load()["keys"]
            changed = {name for name, value in body["keys"].items()
                       if value and value != previous.get(name)}
            if changed - {"broker_api_secret", "broker_access_token"}:
                raise SafetyError("Only broker session credentials may change while owned exposure needs recovery.")
            new_token = body["keys"].get("broker_access_token")
            if new_token:
                from .broker import KiteHTTP
                from .core import Config
                http = KiteHTTP(Config(), False, api_key=previous["broker_api_key"], access_token=new_token)
                account = http.request("GET", "/user/profile")["user_id"]
                plan = read_atomic_json(self.workspace() / "plan.json")
                if account != plan["account"]:
                    raise SafetyError("Recovery credentials must belong to the original broker account.")
        result = self.vault.save(body["keys"], True)
        paused = self.directory / "PAUSED"
        if paused.exists():
            paused.unlink()
        with self.lock:
            self.next_attempt = 0
        self._set("VALIDATING", "Credentials encrypted. Validating keys and preparing the market automatically.")
        self.wake.set()
        return result

    def resume(self, authorize_live: bool) -> None:
        if authorize_live is not True:
            raise SafetyError("Explicit authorization is required to resume live operation.")
        value = self.vault.load()
        if not value["keys"].get("ai_api_key") or not value["keys"].get("broker_api_key"):
            raise SafetyError("Save complete credentials first.")
        self.vault.set_auto_start(True)
        paused = self.directory / "PAUSED"
        if paused.exists():
            paused.unlink()
        with self.lock:
            self.next_attempt = 0
        self.wake.set()

    def stop(self) -> None:
        (self.directory / "PAUSED").write_text(now_ist().isoformat(), encoding="utf-8")
        workspace = self.workspace()
        if workspace:
            (workspace / "HALT").write_text("Dashboard stop " + now_ist().isoformat(), encoding="utf-8")
        try:
            self.vault.set_auto_start(False)
        except SafetyError:
            self._set("STOPPING", "Stop file written; encrypted vault also requires repair.")
            self.wake.set()
            return
        self._set("STOPPING" if self.busy() else "STOPPED",
                  "Stop requested. Open positions are not considered closed until broker reconciliation confirms it.")
        self.wake.set()

    def forget(self) -> None:
        if self.busy():
            raise SafetyError("Cannot remove recovery credentials while the engine or owned exposure is active.")
        self.vault.forget()
        connection = self.directory / "broker-connection.json"
        if connection.exists():
            connection.unlink()
        self.pending_login = None
        self._set("WAITING_CONFIG", "Saved credentials removed. No live startup is authorized.")
        self.wake.set()

    def begin_login(self) -> str:
        if self.running():
            raise SafetyError("Wait for the running engine to stop before renewing its broker session.")
        keys = self.vault.load()["keys"]
        if not keys.get("broker_api_key") or not keys.get("broker_api_secret"):
            raise SafetyError("Save the Kite API key and API secret before broker sign-in.")
        state = secrets.token_urlsafe(32)
        with self.lock:
            self.pending_login = (state, now_ist() + timedelta(minutes=5))
        return broker_login_url(keys["broker_api_key"], state)

    def finish_login(self, state: str, request_token: str) -> None:
        with self.lock:
            pending = self.pending_login
            if (pending is None or now_ist() > pending[1]
                    or not hmac.compare_digest(state, pending[0])):
                raise SafetyError("Broker callback state is invalid or expired. Start sign-in from this dashboard again.")
            self.pending_login = None
        if self.running():
            raise SafetyError("Broker credentials cannot change while the engine is active.")
        keys = self.vault.load()["keys"]
        expected = None
        if self.busy():
            expected = read_atomic_json(self.workspace() / "plan.json")["account"]
        token = exchange_broker_token(keys["broker_api_key"], keys["broker_api_secret"], request_token, expected)
        self.vault.set_access_token(token)
        with self.lock:
            self.next_attempt = 0
        self._set("VALIDATING", "Broker session saved securely. Continuing automatic preparation.")
        self.wake.set()

    def _software_check(self) -> None:
        if software_ready(self.root):
            return
        self._set("CHECKING_SOFTWARE", "Running offline regression checks for this code version; no orders are placed.")
        result = subprocess.run(
            [sys.executable, "-m", "india_trader", "self-test", "--out", "data\\software-check.json"],
            cwd=self.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=180, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0 or not software_ready(self.root):
            raise PreparationBlocked("CHECKS_REQUIRED", "Offline software checks failed. Live startup was blocked.")

    def _launch_worker(self, plan: dict) -> None:
        workspace = Path(plan["database"]).parent
        kill = workspace / "HALT"
        if kill.exists():
            kill.unlink()
        # Reset presentation only. The trading/risk ledger must survive restarts.
        database = workspace / "live.db"
        if database.exists():
            with Store(database) as store:
                store.put("runtime", {})
        with self.lock:
            self.worker_failure = ""
            self.worker_waiting_for_history = False
        process = subprocess.Popen(
            [sys.executable, "-m", "india_trader", "managed-worker",
             "--state-dir", str(self.directory), "--workspace", str(workspace)],
            cwd=self.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        with self.lock:
            self.worker = process
        redact = [value for value in self.vault.load()["keys"].values() if value]

        def consume():
            with (workspace / "worker.log").open("a", encoding="utf-8") as log:
                for line in process.stdout:
                    for value in redact:
                        line = line.replace(value, "[redacted]")
                    if line.startswith(("STOPPED: ", "WAITING_HISTORY: ")):
                        marker, detail = line.split(": ", 1)
                        detail = " ".join(detail.split())[:600]
                        with self.lock:
                            self.worker_failure = detail
                            self.worker_waiting_for_history = marker == "WAITING_HISTORY"
                    log.write(line[:16384])
                    log.flush()

        self.reader = threading.Thread(target=consume, name="redacted-worker-log", daemon=True)
        self.reader.start()
        self._set("STARTING", "Warming indicators and connecting the live feed. No LIVE badge until data and broker gates pass.")

    def _loop(self) -> None:
        while not self.closed.is_set():
            self.wake.wait(1)
            self.wake.clear()
            try:
                with self.lock:
                    process = self.worker
                if process is not None:
                    code = process.poll()
                    if code is None:
                        continue
                    if self.reader:
                        self.reader.join(timeout=2)
                    with self.lock:
                        self.worker = None
                        self.next_attempt = clock.monotonic() + 60
                    state = read_ledger(self.workspace() / "live.db") if self.workspace() else None
                    stream_delay = retry_delay(state, now_ist()) if state else None
                    if state and state.get("broker_auth_required"):
                        self._set("BROKER_LOGIN_REQUIRED", "Broker session expired. Sign in again; saved keys and owned exposure are retained.")
                        self.next_attempt = float("inf")
                    elif has_exposure(state):
                        self._set("BLOCKED", "Worker ended with unresolved exposure. Inspect the broker; do not reset the ledger.")
                        self.next_attempt = float("inf")
                    elif (stream_delay is not None and self.vault.load()["auto_start"]
                          and not (self.directory / "PAUSED").exists()):
                        self._set("RECONNECTING",
                                  f"Broker market-data connection interrupted; retrying a flat, reconciled worker"
                                  f" in {stream_delay}s with fresh indicator history.")
                        self.next_attempt = clock.monotonic() + stream_delay
                    elif state and state.get("halt") and state["halt"] != "Operator kill switch.":
                        self._set("BLOCKED", state["halt"])
                        self.next_attempt = float("inf")
                        self.blocked_day = now_ist().date().isoformat()
                    elif code and self.worker_waiting_for_history:
                        self._set("WAITING_HISTORY", self.worker_failure + " Automatic recheck in 15 seconds.")
                        self.next_attempt = clock.monotonic() + 15
                    elif code:
                        with self.lock:
                            failure = self.worker_failure
                        self._set("BLOCKED", "Worker stopped: " + failure if failure else
                                  "Worker could not complete startup/run. Check the local redacted worker log.")
                    else:
                        self._set("MARKET_CLOSED", "Session completed flat. Waiting for the next authorized market session.")
                    continue
                if (self.directory / "PAUSED").exists():
                    self._set("STOPPED", "Stopped by you. Saved credentials will not override an explicit stop.")
                    continue
                if self.blocked_day and self.blocked_day != now_ist().date().isoformat():
                    self.blocked_day = ""
                    self.next_attempt = 0
                credentials = self.vault.load()
                if not credentials["keys"].get("ai_api_key") or not credentials["keys"].get("broker_api_key"):
                    self._set("WAITING_CONFIG", "Enter and save your Gemini and broker API credentials.")
                    continue
                if not credentials["auto_start"]:
                    self._set("STOPPED", "Automatic trading is paused. Credentials remain encrypted for your next start.")
                    continue
                if clock.monotonic() < self.next_attempt:
                    continue
                self._software_check()
                plan = self.prepare(self.root, self.directory, self.vault, self._set)
                if self.closed.is_set() or not self.vault.load()["auto_start"]:
                    continue
                self._launch_worker(plan)
            except PreparationBlocked as exc:
                self._set(exc.state, str(exc))
                delay = 60 if exc.state in {"MARKET_CLOSED", "WAITING_MARKET", "ENTRY_WINDOW_CLOSED"} else 300
                if exc.state == "WAITING_HISTORY":
                    delay = 15
                if exc.state == "PREMARKET_READY":
                    current = now_ist()
                    opens = current.replace(hour=9, minute=15, second=0, microsecond=0)
                    delay = min(300, max(1, (opens - current).total_seconds()))
                self.next_attempt = clock.monotonic() + delay
            except BrokerError as exc:
                self._set("BROKER_LOGIN_REQUIRED" if exc.session_expired else "BLOCKED", str(exc))
                self.next_attempt = clock.monotonic() + 300
            except (SafetyError, OSError, ValueError, TypeError, KeyError, sqlite3.Error, subprocess.SubprocessError) as exc:
                message = str(exc) if isinstance(exc, SafetyError) else type(exc).__name__
                self._set("BLOCKED", message)
                self.next_attempt = clock.monotonic() + 120

    def snapshot(self) -> dict:
        at = now_ist()
        credentials = self.vault.load()
        identity = broker_connection(self.directory, credentials["keys"], at)
        account_ref = identity.pop("account_ref") if identity else None
        with self.lock:
            phase, reason = self.phase, self.reason
            running = self.worker is not None and self.worker.poll() is None
        view = {
            "at": at.isoformat(), "state": phase, "reason": reason, "live": False,
            "entries_allowed": False, "worker_running": running, "credentials": self.vault.public_status(),
            "model": GEMINI_MODEL, "account": identity, "metrics": None, "positions": [],
            "orders": [], "transactions": [], "trades": [], "activity": [], "equity": [],
            "watchlist": [], "feeds": {}, "ai": {"calls": 0, "tokens": 0, "estimated_micros": 0},
            "watchlist_refresh": None,
            "global_context": None,
            "signal_diagnostics": {},
            "discovery": None,
            "stream": None,
            "reconciliation": None,
            "position_feed": None,
            "participation": None,
            "policy": {"allocation_cap_rupees": 25000, "position_percent": 25, "cash_buffer_percent": 10,
                       "risk_per_trade_percent": 0.25, "daily_loss_percent": 0.75,
                       "max_entry_attempts": 3, "max_consecutive_losses": 2,
                       "ai_calls_per_day": 2, "ai_tokens_per_day": 8000, "ai_usd_per_day": 0.10},
        }
        if identity and phase == "BROKER_LOGIN_REQUIRED":
            identity["authentication_status"] = "reauthentication_required"
        workspace = self.workspace()
        if workspace is None:
            return view
        plan = read_atomic_json(workspace / "plan.json")
        if account_ref is not None and account_ref != hashlib.sha256(plan["account"].encode()).hexdigest():
            return view
        view.update(watchlist=plan.get("screen", []), watchlist_refresh=plan.get("selection"),
                    global_context=plan.get("global_context"), feeds=plan.get("news_health", {}),
                    universe_source=plan.get("universe_source"))
        if not (workspace / "live.db").exists():
            return view
        day = at.date().isoformat()
        with closing(sqlite3.connect((workspace / "live.db").as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
            connection.execute("BEGIN")
            row = connection.execute("SELECT value FROM state WHERE key='engine'").fetchone()
            if not row:
                return view
            engine = json.loads(row[0])
            runtime_row = connection.execute("SELECT value FROM state WHERE key='runtime'").fetchone()
            runtime = json.loads(runtime_row[0]) if runtime_row else {}
            signal_row = connection.execute("SELECT value FROM state WHERE key='signal_diagnostics'").fetchone()
            signal_diagnostics = json.loads(signal_row[0]) if signal_row else {}
            discovery_row = connection.execute("SELECT value FROM state WHERE key='discovery_status'").fetchone()
            discovery_status = json.loads(discovery_row[0]) if discovery_row else None
            if discovery_status and discovery_status.get("day") != day:
                discovery_status = None
            rows = connection.execute(
                "SELECT at,kind,payload FROM audit WHERE substr(at,1,10)=? ORDER BY id", (day,)
            ).fetchall()
            ai = connection.execute("SELECT calls,tokens,micros FROM ai_spend WHERE day=?", (day,)).fetchone()
        fresh = bool(runtime.get("at") and 0 <= (at - timestamp(runtime["at"])).total_seconds() <= 15)
        if running and fresh:
            view.update(state=runtime["status"], reason=runtime["reason"],
                        live=bool(runtime["live"]), entries_allowed=bool(runtime["entries_allowed"]))
        elif running and runtime:
            view.update(state="BLOCKED", reason="Engine heartbeat is stale. Verify broker protection and exposure.")
        if not self.vault.load()["auto_start"] and running:
            view.update(state="STOPPING", reason="Stopping entries and waiting for confirmed exits.", live=False,
                        entries_allowed=False)
        events = [{"at": at_, "kind": kind, **json.loads(payload)} for at_, kind, payload in rows]
        participation = participation_summary(events)
        config = Config.from_mapping(plan["config"]) if "config" in plan else None
        if config:
            participation.update(
                profile=runtime.get("participation_profile", config.strategy.participation_profile),
                position_cap_paise=bps(engine["capital"], config.risk.max_position_bps),
                modeled_risk_per_trade_paise=bps(engine["day_start"], config.risk.risk_per_trade_bps),
                daily_loss_limit_paise=bps(engine["day_start"], config.risk.daily_loss_bps),
                min_net_reward_r=config.strategy.min_net_reward_r,
                min_profit_cost_multiple=config.strategy.min_profit_cost_multiple,
            )
        trades = [x for x in events if x["kind"] == "TRADE_CLOSED"]
        realized = sum(x["net_paise"] for x in trades)
        metrics_current = engine["day"] == day
        total = runtime.get("marked_equity_paise", engine["cash"]) - engine.get("day_open_equity", engine["day_start"])
        fees = sum(x.get("modeled_fees", 0) for x in events if x["kind"] == "FILL_DELTA")
        view.update({
            "account": {
                **(identity or {"id_masked": "****" + plan["account"][-4:],
                                  "verified_at": None, "authentication_status": "not_verified"}),
                "last_update": runtime.get("at"), "snapshot_fresh": fresh and running,
            },
            "metrics": {
                "realized_net_paise": realized, "unrealized_net_paise": total - realized if metrics_current else None,
                "total_net_paise": total if metrics_current else None, "modeled_fees_paise": fees,
                "allocated_capital_paise": engine["capital"], "cash_paise": engine["cash"],
                "entry_attempts": engine["trades"] if metrics_current else 0, "completed_trades": len(trades),
            },
            "positions": [engine["position"]] if engine.get("position") else [],
            "orders": [x for x in engine["orders"] if x["created"][:10] == day],
            "transactions": [x for x in events if x["kind"] == "FILL_DELTA"][-100:],
            "trades": trades[-100:], "activity": [x for x in events if x["kind"] != "EQUITY"][-75:],
            "equity": [x for x in events if x["kind"] == "EQUITY"][-400:],
            "watchlist": plan.get("screen", []), "quotes": runtime.get("quotes", {}),
            "watchlist_refresh": plan.get("selection"),
            "discovery": discovery_status,
            "signal_diagnostics": signal_diagnostics.get("symbols", {})
                if signal_diagnostics.get("day") == day else {},
            "feeds": runtime.get("news_health", plan.get("news_health", {})),
            "stream": runtime.get("stream"),
            "reconciliation": runtime.get("reconciliation"),
            "position_feed": runtime.get("position_feed"),
            "participation": participation,
            "universe_source": plan["universe_source"], "model": runtime.get("model", GEMINI_MODEL),
        })
        if ai:
            view["ai"] = {"calls": ai[0], "tokens": ai[1], "estimated_micros": ai[2]}
        if discovery_status:
            if runtime.get("active_symbols"):
                discovery_status["active_symbols"] = runtime["active_symbols"]
            ranked = {item["symbol"]: item for item in discovery_status.get("ranked", [])}
            initial = {item["symbol"]: item for item in plan.get("screen", [])}
            view["watchlist"] = [
                {**(ranked.get(symbol) or initial.get(symbol) or {
                    "symbol": symbol, "spread_bps": None, "score": None,
                    "reason": "Dynamically admitted; awaiting current ranking snapshot.",
                }), "origin": "initial" if symbol in plan["selected"] else "intraday_discovery"}
                for symbol in discovery_status.get("active_symbols", plan["selected"])
            ]
        return view

    def close(self) -> None:
        self.closed.set()
        self.wake.set()
        workspace = self.workspace()
        if workspace and self.worker and self.worker.poll() is None:
            (workspace / "HALT").write_text("Dashboard shutdown " + now_ist().isoformat(), encoding="utf-8")
            # Do not blindly terminate an order manager with unresolved real exposure.
            self.worker.wait()
        if self.state_thread.is_alive():
            self.state_thread.join(timeout=30)


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, address, controller: Controller, template: Path, access_token: str):
        self.controller, self.template, self.access_token = controller, template, access_token
        super().__init__(address, DashboardHandler)
        self.authority = f"127.0.0.1:{self.server_address[1]}"
        self.origin = "http://" + self.authority


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "IndiaTradingAgent"
    sys_version = ""
    protocol_version = "HTTP/1.0"

    def log_message(self, *_):
        # Callback query strings can carry a short-lived broker request token.
        return

    def _reply(self, status: int, value: dict) -> None:
        body = json.dumps(value, ensure_ascii=True, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(body)

    def _host_ok(self) -> bool:
        return self.headers.get("Host") == self.server.authority

    def _authorized(self, mutating: bool = False) -> bool:
        origin = self.headers.get("Origin")
        return (
            self._host_ok()
            and (origin == self.server.origin if mutating else origin in (None, self.server.origin))
            and hmac.compare_digest(self.headers.get("X-Dashboard-Token", ""), self.server.access_token)
        )

    def do_GET(self):
        path = urllib.parse.urlparse(self.path)
        if not self._host_ok():
            self._reply(403, {"error": "Invalid dashboard host."})
            return
        if path.path == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path.path == "/":
            nonce = secrets.token_urlsafe(24)
            body = self.server.template.read_text(encoding="utf-8").replace("__NONCE__", nonce).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy",
                f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
                "connect-src 'self'; img-src 'self' data:; base-uri 'none'; "
                "frame-ancestors 'none'; form-action 'self'")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(body)
            return
        if path.path == "/broker/callback":
            values = urllib.parse.parse_qs(path.query)
            try:
                self.server.controller.finish_login(values.get("state", [""])[0],
                                                    values.get("request_token", [""])[0])
            except (SafetyError, OSError, ValueError, KeyError):
                self._reply(400, {"error": "Broker sign-in could not be completed. Return to the dashboard and start again."})
                return
            self.send_response(303)
            self.send_header("Location", self.server.origin + "/#access=" + self.server.access_token)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            return
        if not self._authorized():
            self._reply(403, {"error": "Open the authorized dashboard using the local launcher."})
            return
        if path.path == "/api/status":
            try:
                self._reply(200, self.server.controller.snapshot())
            except (SafetyError, OSError, ValueError, KeyError, sqlite3.Error):
                self._reply(503, {"error": "State could not be read safely. No LIVE state is asserted."})
        elif path.path == "/api/config":
            self._reply(200, self.server.controller.vault.public_status())
        else:
            self._reply(404, {"error": "Not found."})

    def do_POST(self):
        if not self._authorized(mutating=True):
            self._reply(403, {"error": "Dashboard authorization/origin rejected."})
            return
        if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            self._reply(415, {"error": "Use application/json."})
            return
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if not 0 <= length <= 40000 or self.headers.get("Transfer-Encoding"):
                self._reply(413, {"error": "Invalid request size/encoding."})
                return
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("Expected an object.")
            path = urllib.parse.urlparse(self.path).path
            controller = self.server.controller
            if path == "/api/config":
                result = controller.save_credentials(body)
            elif path == "/api/stop":
                controller.stop()
                result = {"requested": "stop", "flat": False}
            elif path == "/api/resume":
                controller.resume(body.get("authorize_live") is True)
                result = {"requested": "resume"}
            elif path == "/api/forget":
                if body.get("confirm") is not True:
                    raise SafetyError("Credential removal must be confirmed.")
                controller.forget()
                result = {"forgotten": True}
            elif path == "/api/broker/login":
                result = {"url": controller.begin_login()}
            else:
                self._reply(404, {"error": "Not found."})
                return
            self._reply(200, result)
        except (SafetyError, ValueError, OSError, KeyError, sqlite3.Error) as exc:
            reason = str(exc) if isinstance(exc, SafetyError) else "Request could not be processed safely."
            self._reply(400, {"error": reason})


def serve_dashboard(root: Path, port: int = 8787, open_browser: bool = True) -> None:
    directory = application_directory()
    protector = WindowsProtector()
    session_file = directory / "ui-session.dat"
    lock = InstanceLock(directory / "dashboard.lock")
    try:
        lock.__enter__()
    except SafetyError:
        if open_browser and session_file.exists():
            saved = json.loads(protector.unprotect(session_file.read_bytes()))
            webbrowser.open(saved["url"], new=2)
            print("Opened the already-running local dashboard.")
            return
        raise
    try:
        access = secrets.token_urlsafe(32)
        controller = Controller(root, directory, CredentialVault(directory / "credentials.dat"))
        server = DashboardServer(("127.0.0.1", port), controller,
                                 root / "india_trader" / "web" / "dashboard.html", access)
        url = server.origin + "/#access=" + access
        atomic_bytes(session_file, protector.protect(json.dumps({"url": url}).encode()))
        controller.start()
        if open_browser:
            webbrowser.open(url, new=2)
        print(f"Dashboard listening at {server.origin}. Re-run the launcher to open an authorized tab.", flush=True)
        print("No trading starts without saved explicit authorization and healthy broker/data checks.", flush=True)
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            print("Stopping the dashboard; waiting for any active order manager to reconcile flat.", flush=True)
        finally:
            server.server_close()
            controller.close()
    finally:
        lock.__exit__()
