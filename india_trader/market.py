from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from .core import Candidate, Config, Instrument, SafetyError, Tick


class HistoryNotReady(SafetyError):
    def __init__(self, expected: datetime, available: datetime | None, symbol: str = ""):
        self.expected, self.available, self.symbol = expected, available, symbol
        latest = available.strftime("%H:%M IST") if available else "unavailable"
        label = f" for {symbol}" if symbol else ""
        super().__init__(
            f"Historical warm-up pending{label}: need completed candles through "
            f"{expected:%H:%M} IST; broker coverage ends {latest}."
        )


@dataclass
class Bar:
    start: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int
    count: int = 1
    complete: bool = True

    @property
    def end(self) -> datetime:
        return self.start + timedelta(minutes=5)


class Tape:
    def __init__(self):
        self.latest: Tick | None = None
        self.bar: Bar | None = None
        self.bars: list[Bar] = []
        self.value = 0
        self.volume = 0
        self.first_price = 0
        self.ema = 0.0
        self.complete_opening = False
        self.seeded = False
        self.provider_vwap = 0

    @property
    def vwap(self) -> float:
        if self.provider_vwap:
            return float(self.provider_vwap)
        return self.value / self.volume if self.volume else 0.0

    def seed(self, bars: list[Bar], at: datetime) -> None:
        if self.latest or self.bar or self.bars:
            raise SafetyError("Historical warm-up must precede all streaming ticks.")
        expected = at.replace(hour=9, minute=15, second=0, microsecond=0)
        bucket = at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0)
        if not bars:
            raise HistoryNotReady(bucket, None)
        for bar in bars:
            if (bar.start != expected or bar.end > bucket or not bar.complete
                    or not 0 < bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
                    or bar.volume < 0):
                raise SafetyError("Historical warm-up contains gaps, invalid prices or a future/open candle.")
            expected = bar.end
        if expected != bucket:
            raise HistoryNotReady(bucket, expected)
        self.bars = list(bars)
        self.first_price = bars[0].open
        self.complete_opening = True
        self.seeded = True
        for bar in bars:
            self.value += (bar.high + bar.low + bar.close) * bar.volume // 3
            self.volume += bar.volume
            self.ema = bar.close if not self.ema else bar.close * 2 / 21 + self.ema * 19 / 21

    def push(self, tick: Tick) -> Bar | None:
        tick.validate()
        if self.latest and tick.at < self.latest.at:
            raise SafetyError("Out-of-order market timestamp.")
        if self.latest == tick:
            return None
        if self.latest and tick.at.date() != self.latest.at.date():
            raise SafetyError("Market session changed; start a new authorized run.")
        if self.latest and tick.volume < self.latest.volume:
            raise SafetyError("Cumulative market volume decreased.")
        delta = tick.volume - self.latest.volume if self.latest else 0
        bucket = tick.at.replace(minute=tick.at.minute // 5 * 5, second=0, microsecond=0)
        finished = None
        if self.bar and bucket > self.bar.start:
            if bucket - self.bar.start != timedelta(minutes=5):
                raise SafetyError("Missing five-minute bar; do not fabricate market history.")
            finished = self.bar
            self.bars.append(finished)
            if finished.complete:
                self.ema = finished.close if not self.ema else (
                    finished.close * 2 / 21 + self.ema * 19 / 21
                )
            self.bar = None
        if self.bar is None:
            if self.seeded and self.latest is None and self.bars[-1].end != bucket:
                raise SafetyError("Streaming began after the warmed-up interval; refresh warm-up.")
            complete = not (self.seeded and self.latest is None and (tick.at - bucket).total_seconds() > 3)
            self.bar = Bar(bucket, tick.last, tick.last, tick.last, tick.last, delta, complete=complete)
        else:
            self.bar.high = max(self.bar.high, tick.last)
            self.bar.low = min(self.bar.low, tick.last)
            self.bar.close = tick.last
            self.bar.volume += delta
            self.bar.count += 1
        if self.latest is None and not self.seeded:
            self.first_price = tick.last
            self.complete_opening = (
                tick.at.time() >= time(9, 15) and tick.at.time() <= time(9, 15, 3)
            )
        self.latest = tick
        if tick.session_vwap:
            self.provider_vwap = tick.session_vwap
        self.value += tick.last * delta
        self.volume += delta
        return finished


class SignalAgent:
    def __init__(self, config: Config, instruments: dict[str, Instrument]):
        self.config = config
        self.instruments = instruments
        self.tapes = {symbol: Tape() for symbol in instruments}
        self.trade_symbols = set(config.market.symbols)
        self.decision: dict | None = None

    def ingest(self, tick: Tick) -> Candidate | None:
        self.decision = None
        if tick.symbol not in self.tapes:
            raise SafetyError("Tick outside the configured universe.")
        tape = self.tapes[tick.symbol]
        bar = tape.push(tick)
        if bar is None or tick.symbol not in self.trade_symbols:
            return None
        cfg = self.config.strategy
        reference = self.tapes[self.config.market.benchmark]
        ref = reference.latest
        size = self.instruments[tick.symbol].tick
        history = tape.bars
        recent = history[-3:]
        reference_recent = [x for x in reference.bars if x.complete][-3:]
        benchmark_return = (ref.last / reference.first_price - 1) * 10000 if ref and reference.first_price else None
        stock_return = (bar.close / tape.first_price - 1) * 10000 if tape.first_price else None
        relative_strength = stock_return - benchmark_return if stock_return is not None and benchmark_return is not None else None
        reference_15m = ((ref.last / reference_recent[0].open - 1) * 10000
                         if ref and len(reference_recent) == 3 else None)
        index_positive = benchmark_return is not None and benchmark_return > 0
        relative_path = (
            cfg.benchmark_alignment == "relative_strength"
            and relative_strength is not None and relative_strength >= 20
            and stock_return > 0 and bar.close > tape.vwap and bar.close > tape.ema
            and len(recent) == 3 and all(x.complete for x in recent)
            and recent[-1].close > recent[0].close
            and reference_15m is not None and reference_15m >= -50
        )
        gates = {
            "complete_bar": bar.complete,
            "opening_history_ready": tape.complete_opening,
            "entry_window": time.fromisoformat(self.config.market.entry_start) <= tick.at.time()
                            < time.fromisoformat(self.config.market.entry_end),
            "benchmark_fresh": bool(ref and reference.complete_opening
                                    and abs((tick.at - ref.at).total_seconds())
                                    <= self.config.market.max_quote_age_seconds),
            "stock_above_vwap": tape.vwap > 0 and bar.close > tape.vwap,
            "market_alignment": index_positive or relative_path,
        }
        self.decision = {
            "symbol": tick.symbol, "bar_start": bar.start.isoformat(), "bar_end": bar.end.isoformat(),
            "observed_at": tick.at.isoformat(), "bar_close_paise": bar.close,
            "session_vwap_paise": round(tape.vwap, 2), "ema20_paise": round(tape.ema, 2),
            "benchmark_return_bps": round(benchmark_return, 2) if benchmark_return is not None else None,
            "stock_relative_strength_bps": round(relative_strength, 2) if relative_strength is not None else None,
            "benchmark_recent_bps": round(reference_15m, 2) if reference_15m is not None else None,
            "alignment_path": "index_above_open" if index_positive else
                              "stock_relative_strength" if relative_path else "not_aligned",
            "checks": gates, "setup_checks": {}, "entry_attempted": False,
        }
        opening_count = cfg.opening_range_minutes // 5
        opening_end = tick.at.replace(hour=9, minute=15, second=0, microsecond=0) + timedelta(
            minutes=cfg.opening_range_minutes
        )
        confirmation_start = opening_end + timedelta(minutes=5)
        candidates = []
        if "orb" in cfg.enabled:
            opening = [x for x in history if time(9, 15) <= x.start.time() < opening_end.time() and x.complete]
            high = max((x.high for x in opening), default=0)
            average_volume = sum(x.volume for x in opening) / opening_count
            checks = {
                "morning_window": confirmation_start.time() <= tick.at.time() <= time(10, 30),
                "complete_opening_range": len(opening) == opening_count and len(history) >= opening_count + 1,
                "new_range_breakout": len(history) >= 2 and history[-2].close <= high < bar.close,
                "breakout_volume": average_volume > 0 and bar.volume >= average_volume * cfg.volume_ratio,
            }
            self.decision["setup_checks"]["orb"] = checks
            if all(checks.values()):
                candidates.append(Candidate(tick.symbol, "orb", tick.at, min(bar.low, high) - size))
        previous = history[-4:-1]
        prior_volume = sum(x.volume for x in previous) / 3
        warm = sum(x.complete for x in history) >= 20
        if "vwap_pullback" in cfg.enabled:
            checks = {
                "twenty_bar_history": warm,
                "prior_uptrend": len(previous) == 3
                    and all(x.complete and x.close > tape.vwap for x in previous)
                    and previous[-1].close > previous[0].close,
                "pullback_touches_vwap": bar.low <= tape.vwap <= bar.close,
                "bullish_rejection": bar.close > bar.open,
                "above_ema20": bar.close > tape.ema,
                "confirmation_volume": prior_volume > 0 and bar.volume >= prior_volume * cfg.volume_ratio,
            }
            self.decision["setup_checks"]["vwap_pullback"] = checks
            if all(checks.values()):
                candidates.append(Candidate(tick.symbol, "vwap_pullback", tick.at, bar.low - size))
        if "momentum_breakout" in cfg.enabled:
            base_high = max((x.high for x in previous), default=0)
            checks = {
                "twenty_bar_history": warm,
                "complete_three_bar_base": len(previous) == 3 and all(x.complete for x in previous),
                "close_breaks_recent_high": bar.close > base_high > 0,
                "bullish_breakout": bar.close > bar.open,
                "above_ema20": bar.close > tape.ema,
                "confirmation_volume": prior_volume > 0 and bar.volume >= prior_volume * cfg.volume_ratio,
            }
            self.decision["setup_checks"]["momentum_breakout"] = checks
            if all(checks.values()):
                candidates.append(Candidate(tick.symbol, "momentum_breakout", tick.at, min(bar.low, base_high) - size))
        failed = [name.replace("_", " ") for name, passed in gates.items() if not passed]
        if failed:
            self.decision["reason"] = "No entry: " + ", ".join(failed) + "."
            return None
        if candidates:
            chosen = candidates[0]
            self.decision["setup"] = chosen.setup
            self.decision["reason"] = f"{chosen.setup} signal qualifies; awaiting execution/risk checks."
            return chosen
        failures = [
            name + ": " + ", ".join(key.replace("_", " ") for key, passed in checks.items() if not passed)
            for name, checks in self.decision["setup_checks"].items()
        ]
        self.decision["reason"] = "No qualifying setup. " + "; ".join(failures)
        return None
