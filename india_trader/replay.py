from __future__ import annotations

import csv
import json
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

from .broker import PaperBroker
from .core import Config, IST, Instrument, NewsConfig, SafetyError, Session, Tick, file_hash, paise, rupees, timestamp
from .engine import TradingEngine
from .events import EventAgent
from .runtime import code_hash, research_hash
from .storage import InstanceLock, Store

COLUMNS = ["timestamp", "symbol", "last", "bid", "ask", "volume", "bid_size", "ask_size"]


def read_ticks(path: Path) -> Iterator[Tick]:
    previous = None
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != COLUMNS:
            raise ValueError(f"Tick columns/order must be {COLUMNS}. Prices are rupees.")
        for row in reader:
            at = timestamp(row["timestamp"])
            if previous is not None and at < previous:
                raise ValueError("Tick file must be globally time ordered.")
            previous = at
            tick = Tick(row["symbol"], at, paise(row["last"]), paise(row["bid"]),
                        paise(row["ask"]), int(row["volume"]), int(row["bid_size"]),
                        int(row["ask_size"]))
            tick.validate()
            yield tick


def load_replay_instruments(path: Path, config: Config) -> tuple[dict[str, Instrument], str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw["dataset_kind"] not in {"synthetic", "licensed"}:
        raise ValueError("dataset_kind must be synthetic or licensed (operator declaration).")
    instruments = {x["symbol"]: Instrument(**x) for x in raw["instruments"]}
    if set(instruments) != set(config.market.symbols) | {config.market.benchmark}:
        raise ValueError("Replay instrument master must match the exact configured universe.")
    for symbol, item in instruments.items():
        if item.tick <= 0 or not 0 < item.lower < item.upper:
            raise ValueError("Invalid tick/price-band metadata.")
        if item.reference != (symbol == config.market.benchmark):
            raise ValueError("Exactly the configured benchmark must be reference-only.")
    return instruments, raw["dataset_kind"]


def replay(
    root: Path, config: Config, session: Session, ticks_path: Path, instruments_path: Path,
    events_path: Path | None, database: Path, report_path: Path, out_of_sample: bool,
    operating_cost_rupees: float | None,
) -> dict:
    from .reports import build_report

    if database.exists():
        raise SafetyError("Replay requires a NEW database; never mix runs or replay into a live ledger.")
    instruments, kind = load_replay_instruments(instruments_path, config)
    events = []
    if events_path:
        if events_path.stat().st_size > 10_000_000:
            raise ValueError("Split event history into per-session files under 10 MB.")
        for line in events_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                events.append(json.loads(line))
        events.sort(key=lambda x: timestamp(x["at"]))
    source_files = [ticks_path, instruments_path] + ([events_path] if events_path else [])
    provenance = [{"path": str(path), "sha256": file_hash(path)}
                  for path in source_files]
    with InstanceLock(database.with_suffix(".lock")), Store(database) as store:
        broker = PaperBroker(config.risk.capital_rupees * 100, config.costs, store)
        engine = TradingEngine(config, session, instruments, broker, store, "paper")
        event_agent = EventAgent(config, store)
        initial = datetime.combine(session.day, datetime.min.time(), IST)
        engine.reconcile(broker.snapshot(initial), initial)
        store.audit(initial, "RUN_START", mode="paper", dataset_kind=kind,
                    code_hash=code_hash(root), research_hash=research_hash(config),
                    starting_equity=engine.state["day_start"], provenance=provenance,
                    out_of_sample=out_of_sample)
        event_index, last_minute = 0, None
        last_reconcile = initial
        last = None
        for tick in read_ticks(ticks_path):
            if tick.at.date() != session.day:
                raise SafetyError("Replay is one authorized session per file.")
            while event_index < len(events) and timestamp(events[event_index]["at"]) <= tick.at:
                event_agent.accept(events[event_index], engine, tick.at)
                event_index += 1
            broker.on_tick(tick)
            engine.on_tick(tick, tick.at)
            if (broker.dirty or (tick.at - last_reconcile).total_seconds()
                    >= config.execution.reconcile_seconds):
                engine.reconcile(broker.snapshot(tick.at), tick.at)
                last_reconcile = tick.at
            engine.timer(tick.at)
            minute = tick.at.replace(second=0, microsecond=0)
            if minute != last_minute:
                store.audit(tick.at, "EQUITY", equity=engine.marked_equity(tick.at))
                last_minute = minute
            last = tick.at
        if last is None:
            raise ValueError("Tick history is empty.")
        # No invented closing fill: unfinished positions remain unfinished in the report.
        final = engine.summary(last)
        store.put("last_status", final)
        store.audit(last, "RUN_END", **final)
        result = build_report(store, config, session.day.isoformat(), operating_cost_rupees)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
        return result


def generate_demo(out: Path) -> tuple[Config, Session, Path, Path, Path]:
    out.mkdir(parents=True, exist_ok=True)
    config = Config(
        market=replace(Config().market, symbols=["DEMO"], benchmark="INDEX"),
        news=NewsConfig(allowed_sources=["demo"], required_sources=["demo"]),
    )
    start = datetime(2026, 9, 21, 9, 15, tzinfo=IST)
    session = Session(start.date(), True, True, ["DEMO"], [], capital_rupees=25000)
    ticks_path, master_path, events_path = (
        out / "synthetic_ticks.csv", out / "synthetic_instruments.json", out / "synthetic_events.jsonl"
    )
    if any(path.exists() for path in (ticks_path, master_path, events_path)):
        raise SafetyError("Choose a new demo output directory; existing artifacts are not overwritten.")
    with ticks_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        volume = 1000
        for second in range(27 * 60 + 1):
            at = start + timedelta(seconds=second)
            index = 2_000_000 + second
            writer.writerow([at.isoformat(), "INDEX", rupees(index), rupees(index),
                             rupees(index), 0, 0, 0])
            if second < 900:
                price = 10000 + second % 21
                volume += 200
            elif second < 1200:
                price = 9980 + (second - 900) * 80 // 299
                volume += 2000
            else:
                price = min(10400, 10065 + second - 1200)
                volume += 400
            writer.writerow([at.isoformat(), "DEMO", rupees(price), rupees(price - 1),
                             rupees(price + 1), volume, 10000, 10000])
    master_path.write_text(json.dumps({
        "dataset_kind": "synthetic",
        "instruments": [
            asdict(Instrument("DEMO", 1, 1, 9000, 11000)),
            asdict(Instrument("INDEX", 2, 1, 1, 10**12, True)),
        ],
    }, indent=2), encoding="utf-8")
    with events_path.open("w", encoding="utf-8") as handle:
        for minute in range(28):
            handle.write(json.dumps({
                "type": "heartbeat", "source": "demo",
                "at": (start + timedelta(minutes=minute)).isoformat(),
            }) + "\n")
    return config, session, ticks_path, master_path, events_path
