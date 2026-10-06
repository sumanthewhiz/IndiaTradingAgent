from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys
import unittest
from contextlib import closing
from pathlib import Path

from .core import Config, SafetyError, Session, now_ist
from .market import HistoryNotReady
from .replay import generate_demo, replay
from .reports import build_report, qualification
from .runtime import code_hash, research_hash, run_connected
from .storage import Store

ROOT = Path(__file__).resolve().parent.parent


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Paper-first Indian cash-equity event agent. No profit guarantee or bank-transfer capability."
    )
    commands = result.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Offline synthetic exercise, never live evidence")
    demo.add_argument("--out", type=Path, required=True)
    tests = commands.add_parser("self-test", help="Run the bundled tests and write a source-bound result")
    tests.add_argument("--out", type=Path, default=Path("data\\software-check.json"))
    doctor = commands.add_parser("doctor", help="Read-only local configuration/credential-presence check")
    doctor.add_argument("--config", type=Path, default=Path("config.example.toml"))
    replay_parser = commands.add_parser("replay", help="One-session tick replay; no network")
    replay_parser.add_argument("--config", type=Path, required=True)
    replay_parser.add_argument("--session", type=Path, required=True)
    replay_parser.add_argument("--ticks", type=Path, required=True)
    replay_parser.add_argument("--instruments", type=Path, required=True)
    replay_parser.add_argument("--events", type=Path)
    replay_parser.add_argument("--db", type=Path, required=True)
    replay_parser.add_argument("--report", type=Path, required=True)
    replay_parser.add_argument("--out-of-sample", action="store_true")
    replay_parser.add_argument("--operating-cost-rupees", type=float)
    run = commands.add_parser("run", help="Broker WebSocket with shadow fills or explicitly authorized live orders")
    run.add_argument("--config", type=Path, required=True)
    run.add_argument("--session", type=Path, required=True)
    run.add_argument("--mode", choices=["shadow", "live"], default="shadow")
    run.add_argument("--db", type=Path, required=True)
    run.add_argument("--kill-file", type=Path, default=Path("data\\HALT"))
    run.add_argument("--accept-live-risk", action="store_true")
    halt = commands.add_parser("halt", help="Latch local emergency stop; daemon must observe this file")
    halt.add_argument("--kill-file", type=Path, default=Path("data\\HALT"))
    status = commands.add_parser("status", help="Read stored status without broker credentials")
    status.add_argument("--db", type=Path, required=True)
    report = commands.add_parser("report", help="Export a completed session")
    report.add_argument("--config", type=Path, required=True)
    report.add_argument("--db", type=Path, required=True)
    report.add_argument("--day", required=True)
    report.add_argument("--out", type=Path, required=True)
    report.add_argument("--operating-cost-rupees", type=float)
    qualify = commands.add_parser("qualify", help="Evaluate real-data gates; operator attestations remain false")
    qualify.add_argument("--config", type=Path, required=True)
    qualify.add_argument("--reports", nargs="+", required=True)
    qualify.add_argument("--software-check", type=Path, default=Path("data\\software-check.json"))
    qualify.add_argument("--out", type=Path, default=Path("data\\qualification.json"))
    dashboard = commands.add_parser("dashboard", help="Local dashboard: encrypted keys and automatic guarded live startup")
    dashboard.add_argument("--port", type=int, default=8787)
    dashboard.add_argument("--no-browser", action="store_true")
    worker = commands.add_parser("managed-worker", help=argparse.SUPPRESS)
    worker.add_argument("--state-dir", type=Path, required=True)
    worker.add_argument("--workspace", type=Path, required=True)
    return result


def emit(value) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


def failure_message(exc: Exception) -> str:
    if isinstance(exc, PermissionError):
        target = exc.filename2 or exc.filename
        name = Path(target).name if target else "local state file"
        return (
            f"Local file access blocked for {name} (PermissionError)."
            " The file may be held by another process or its permissions may prevent access."
        )
    return str(exc) if isinstance(exc, (SafetyError, ValueError)) else type(exc).__name__


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "dashboard":
            from .dashboard import serve_dashboard
            if not 1024 <= args.port <= 65535:
                raise SafetyError("Choose a non-privileged local port.")
            serve_dashboard(ROOT, args.port, not args.no_browser)
            return 0
        if args.command == "managed-worker":
            from .credentials import CredentialVault, read_atomic_json
            from .runtime import ManagedRun
            workspace = args.workspace.resolve()
            if workspace.parent != (args.state_dir / "accounts").resolve():
                raise SafetyError("Managed workspace must be inside the account state directory.")
            if (args.state_dir / "PAUSED").exists():
                raise SafetyError("Dashboard operation is explicitly paused.")
            plan = read_atomic_json(workspace / "plan.json")
            config = Config.from_mapping(plan["config"])
            keys = CredentialVault(args.state_dir / "credentials.dat").load()["keys"]
            context = plan.get("global_context", {})
            blackout = context.get("opening_blackout")
            session = Session(now_ist().date(), True, True, plan["selected"], [blackout] if blackout else [],
                              live_approved=True, account_id=plan["account"],
                              capital_rupees=config.risk.capital_rupees, config_hash=config.fingerprint)
            result = run_connected(ROOT, config, session, workspace / "live.db", "live", True,
                                   workspace / "HALT", managed=ManagedRun(
                                       args.state_dir, workspace, plan, keys))
            emit(result)
            return 0 if result["flat"] else 1
        if args.command == "demo":
            config, session, ticks, master, events = generate_demo(args.out)
            result = replay(ROOT, config, session, ticks, master, events, args.out / "paper.db",
                            args.out / "report.json", False, 0)
            emit(result)
            return 0 if result["flat"] and result["trade_count"] >= 1 and not result["halt"] else 1
        if args.command == "self-test":
            suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_*.py")
            test_result = unittest.TextTestRunner(verbosity=2).run(suite)
            result = {"passed": test_result.wasSuccessful(), "tests_run": test_result.testsRun,
                      "failures": len(test_result.failures), "errors": len(test_result.errors),
                      "code_hash": code_hash(ROOT), "run_at": now_ist().isoformat()}
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
            emit(result)
            return 0 if result["passed"] else 1
        if args.command == "halt":
            args.kill_file.parent.mkdir(parents=True, exist_ok=True)
            args.kill_file.write_text("HALT " + now_ist().isoformat(), encoding="utf-8")
            emit({"kill_file": str(args.kill_file.resolve()),
                  "notice": "Stop requested, NOT proof of exit. Check daemon and broker positions/orders."})
            return 0
        if args.command == "status":
            uri = args.db.resolve().as_uri() + "?mode=ro"
            with closing(sqlite3.connect(uri, uri=True)) as connection:
                row = connection.execute("SELECT value FROM state WHERE key='engine'").fetchone()
            emit(json.loads(row[0]) if row else {"status": "No engine state."})
            return 0
        config = Config.load(args.config)
        if args.command == "doctor":
            emit({
                "config_valid": True, "config_hash": config.fingerprint,
                "research_hash": research_hash(config), "code_hash": code_hash(ROOT),
                "live_enabled": config.live.enabled,
                "capital_rupees": config.risk.capital_rupees,
                "KITE_API_KEY_present": bool(os.environ.get("KITE_API_KEY")),
                "KITE_ACCESS_TOKEN_present": bool(os.environ.get("KITE_ACCESS_TOKEN")),
                "AI_enabled": config.ai.enabled,
                "AI_key_present": bool(os.environ.get(config.ai.api_key_env)),
                "required_news_sources": config.news.required_sources,
                "warning": "Presence is not authentication, data coverage, broker approval or strategy validation.",
            })
            return 0
        if args.command == "replay":
            result = replay(ROOT, config, Session.load(args.session), args.ticks, args.instruments,
                            args.events, args.db, args.report, args.out_of_sample,
                            args.operating_cost_rupees)
            emit(result)
            return 0 if result["flat"] and not result["quarantine"] else 1
        if args.command == "run":
            result = run_connected(ROOT, config, Session.load(args.session), args.db,
                                   args.mode, args.accept_live_risk, args.kill_file)
            emit(result)
            return 0 if result["flat"] else 1
        if args.command == "report":
            if not args.db.exists():
                raise SafetyError("Report database does not exist.")
            with Store(args.db) as store:
                result = build_report(store, config, args.day, args.operating_cost_rupees)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
            emit(result)
            return 0
        if args.command == "qualify":
            paths = sorted({Path(path) for pattern in args.reports for path in glob.glob(pattern)})
            if not paths:
                raise SafetyError("No evaluation reports matched.")
            result = qualification(ROOT, config, paths, args.software_check, args.out)
            emit(result)
            return 0 if result["evidence_gate_passed"] and result["software_tests_passed"] else 1
        raise SafetyError("Unhandled command.")
    except HistoryNotReady as exc:
        print(f"WAITING_HISTORY: {failure_message(exc)}", file=sys.stderr)
        return 3
    except (SafetyError, ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        # Provider exception bodies/URLs can contain tokens; report a bounded local error only.
        message = failure_message(exc)
        print(f"STOPPED: {message}", file=sys.stderr)
        return 2
