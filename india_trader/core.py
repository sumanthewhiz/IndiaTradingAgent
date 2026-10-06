from __future__ import annotations

import hashlib
import json
import math
import re
import tomllib
from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any, TypeVar

IST = timezone(timedelta(hours=5, minutes=30), "IST")
TERMINAL = frozenset({"COMPLETE", "CANCELLED", "REJECTED"})


class SafetyError(RuntimeError):
    pass


def now_ist() -> datetime:
    return datetime.now(IST)


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamps must contain a timezone offset.")
    return result.astimezone(IST)


def paise(value: Any) -> int:
    try:
        amount = Decimal(str(value))
        if not amount.is_finite():
            raise ValueError("Non-finite monetary value.")
        return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except InvalidOperation:
        raise ValueError("Invalid or out-of-range monetary value.") from None


def rupees(value: int) -> str:
    return f"{Decimal(value) / 100:.2f}"


def bps(value: int, rate: float) -> int:
    return int(
        (Decimal(value) * Decimal(str(rate)) / 10000).to_integral_value(
            rounding=ROUND_CEILING
        )
    )


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tick_floor(value: int, tick: int) -> int:
    return value // tick * tick


def tick_ceil(value: int, tick: int) -> int:
    return (value + tick - 1) // tick * tick


@dataclass(frozen=True)
class RiskConfig:
    capital_rupees: int = 25000
    cash_buffer_bps: int = 1000
    max_position_bps: int = 2500
    risk_per_trade_bps: int = 25
    daily_loss_bps: int = 75
    max_trades: int = 3
    max_consecutive_losses: int = 2
    max_daily_buy_turnover_multiple: float = 2.0


@dataclass(frozen=True)
class MarketConfig:
    symbols: list[str] = field(default_factory=lambda: ["RELIANCE", "ICICIBANK"])
    benchmark: str = "NIFTY 50"
    entry_start: str = "09:35"
    entry_end: str = "14:30"
    flatten_at: str = "15:10"
    close_at: str = "15:30"
    max_quote_age_seconds: int = 3
    max_spread_bps: int = 8
    max_depth_participation_bps: int = 1000
    queue_size: int = 4096


@dataclass(frozen=True)
class StrategyConfig:
    enabled: list[str] = field(default_factory=lambda: ["orb"])
    reward_r: float = 3.0
    min_net_reward_r: float = 1.5
    min_profit_cost_multiple: float = 3.0
    volume_ratio: float = 1.5
    min_stop_bps: int = 10
    max_stop_bps: int = 100
    cooldown_minutes: int = 30
    max_hold_minutes: int = 45
    opening_range_minutes: int = 15
    benchmark_alignment: str = "absolute"


@dataclass(frozen=True)
class ExecutionConfig:
    entry_ttl_seconds: int = 8
    exit_ttl_seconds: int = 10
    protection_gap_bps: int = 30
    entry_slippage_bps: int = 3
    exit_slippage_bps: int = 15
    request_interval_seconds: float = 1.05
    reconcile_seconds: int = 5
    max_exit_reprices: int = 3


@dataclass(frozen=True)
class CostConfig:
    # Modeling assumptions, not a representation of current broker tariffs.
    brokerage_bps: float = 3.0
    brokerage_cap_rupees: int = 20
    sell_stt_bps: float = 2.5
    transaction_bps: float = 0.30
    sebi_bps: float = 0.01
    buy_stamp_bps: float = 0.30
    gst_percent: float = 18.0
    extra_buffer_bps: float = 2.0

    def fee(self, side: str, notional: int) -> int:
        if notional <= 0:
            return 0
        brokerage = min(bps(notional, self.brokerage_bps),
                        self.brokerage_cap_rupees * 100)
        taxable = brokerage + bps(notional, self.transaction_bps + self.sebi_bps)
        tax = bps(notional, self.sell_stt_bps if side == "SELL" else self.buy_stamp_bps)
        return taxable + bps(taxable, self.gst_percent * 100) + tax + bps(
            notional, self.extra_buffer_bps
        )


@dataclass(frozen=True)
class NewsConfig:
    inbox: str = "data\\events.jsonl"
    allowed_sources: list[str] = field(default_factory=lambda: ["operator", "licensed-wire"])
    required_sources: list[str] = field(default_factory=lambda: ["licensed-wire"])
    heartbeat_max_seconds: int = 180
    pause_minutes: int = 30
    rss_urls: list[str] = field(default_factory=list)
    rss_poll_seconds: int = 120


@dataclass(frozen=True)
class AIConfig:
    enabled: bool = False
    share_public_news: bool = False
    endpoint: str = "https://api.openai.com/v1/chat/completions"
    model: str = ""
    api_key_env: str = "TRADER_AI_API_KEY"
    max_calls_per_day: int = 2
    max_tokens_per_day: int = 4000
    max_cost_usd_per_day: float = 0.10
    input_usd_per_million: float = 3.0
    output_usd_per_million: float = 15.0
    max_input_bytes: int = 1400
    max_output_tokens: int = 128
    cooldown_seconds: int = 1800
    timeout_seconds: int = 8


@dataclass(frozen=True)
class LiveConfig:
    enabled: bool = False
    broker_user_id: str = ""
    algo_id: str = ""
    qualification_file: str = "data\\qualification.json"


T = TypeVar("T")


def _section(cls: type[T], raw: dict[str, Any]) -> T:
    allowed = {f.name for f in fields(cls)}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"Unknown {cls.__name__} keys: {sorted(unknown)}")
    defaults = cls()
    for key, value in raw.items():
        default = getattr(defaults, key)
        expected = type(default)
        if expected is float:
            valid = type(value) in (float, int) and math.isfinite(value)
        else:
            valid = type(value) is expected
        if not valid:
            raise ValueError(f"{cls.__name__}.{key} has the wrong type.")
        if isinstance(value, list) and not all(type(x) is str for x in value):
            raise ValueError(f"{cls.__name__}.{key} must contain strings.")
    return cls(**raw)


@dataclass(frozen=True)
class Config:
    risk: RiskConfig = field(default_factory=RiskConfig)
    market: MarketConfig = field(default_factory=MarketConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    costs: CostConfig = field(default_factory=CostConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    live: LiveConfig = field(default_factory=LiveConfig)

    @classmethod
    def load(cls, path: Path) -> Config:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> Config:
        types = {
            "risk": RiskConfig, "market": MarketConfig, "strategy": StrategyConfig,
            "execution": ExecutionConfig, "costs": CostConfig, "news": NewsConfig,
            "ai": AIConfig, "live": LiveConfig,
        }
        if set(raw) - set(types):
            raise ValueError("Unknown top-level configuration section.")
        result = cls(**{key: _section(kind, raw.get(key, {}))
                        for key, kind in types.items()})
        result.validate()
        return result

    def validate(self) -> None:
        r, m, s, e, a = self.risk, self.market, self.strategy, self.execution, self.ai
        checks = [
            (1000 <= r.capital_rupees <= 10000000, "capital_rupees out of range"),
            (100 <= r.cash_buffer_bps <= 9000, "cash buffer must be 1%-90%"),
            (100 <= r.max_position_bps <= 10000 - r.cash_buffer_bps, "position cap"),
            (1 <= r.risk_per_trade_bps <= 50, "per-trade modeled risk cannot exceed 0.5%"),
            (r.risk_per_trade_bps <= r.daily_loss_bps <= 200, "daily loss cap"),
            (1 <= r.max_trades <= 10, "max_trades must be 1-10"),
            (1 <= r.max_consecutive_losses <= 3, "consecutive loss limit"),
            (0 < r.max_daily_buy_turnover_multiple <= 5, "turnover limit"),
            (1 <= len(m.symbols) <= 30 and len(set(m.symbols)) == len(m.symbols),
             "Use 1-30 distinct cash symbols"),
            (all(re.fullmatch(r"[A-Z0-9&-]{1,30}", x) for x in m.symbols), "symbol syntax"),
            (m.benchmark not in m.symbols and bool(m.benchmark), "benchmark must not be traded"),
            (1 <= m.max_quote_age_seconds <= 10, "quote age limit"),
            (1 <= m.max_spread_bps <= 30, "spread cap"),
            (1 <= m.max_depth_participation_bps <= 2500, "depth participation"),
            (64 <= m.queue_size <= 100000, "queue_size"),
            (time(9, 25) <= time.fromisoformat(m.entry_start)
             < time.fromisoformat(m.entry_end) < time.fromisoformat(m.flatten_at)
             < time.fromisoformat(m.close_at) <= time(15, 30), "session times"),
            (set(s.enabled) <= {"orb", "vwap_pullback", "momentum_breakout"}, "unsupported setup"),
            (s.opening_range_minutes in (5, 15), "opening range must be 5 or 15 minutes"),
            (s.benchmark_alignment in {"absolute", "relative_strength"}, "benchmark alignment"),
            (1.5 <= s.reward_r <= 5 and 1 <= s.min_net_reward_r <= s.reward_r, "reward ratios"),
            (s.min_profit_cost_multiple >= 2, "cost hurdle"),
            (1 <= s.volume_ratio <= 5, "volume_ratio"),
            (1 <= s.min_stop_bps < s.max_stop_bps <= 300, "stop distance"),
            (10 <= s.cooldown_minutes <= 180 and 5 <= s.max_hold_minutes <= 90,
             "cooldown/holding period"),
            (1 <= e.entry_ttl_seconds <= 30 and 2 <= e.exit_ttl_seconds <= 60, "order TTL"),
            (5 <= e.protection_gap_bps <= 100, "stop-limit gap"),
            (0 <= e.entry_slippage_bps <= 10 and 1 <= e.exit_slippage_bps <= 50, "slippage caps"),
            (1 <= e.request_interval_seconds <= 5 and 2 <= e.reconcile_seconds <= 30,
             "API rate/reconciliation"),
            (1 <= e.max_exit_reprices <= 5, "exit reprice limit"),
            (30 <= self.news.heartbeat_max_seconds <= 600, "news freshness"),
            (5 <= self.news.pause_minutes <= 120, "news pause"),
            (bool(self.news.allowed_sources), "allowed news sources"),
            (bool(self.news.required_sources) and set(self.news.required_sources)
             <= set(self.news.allowed_sources), "required news sources must be approved"),
            (60 <= self.news.rss_poll_seconds <= 1800, "RSS polling interval"),
            (0 <= a.max_calls_per_day <= 20 and 0 <= a.max_tokens_per_day <= 100000, "AI quota"),
            (128 <= a.max_input_bytes <= 8000 and 32 <= a.max_output_tokens <= 4096, "AI payload"),
            (60 <= a.cooldown_seconds and 1 <= a.timeout_seconds <= 20, "AI timing"),
            (0 <= a.max_cost_usd_per_day <= 10, "AI cost cap"),
            (not a.enabled or (a.share_public_news and bool(a.model)
                               and a.input_usd_per_million > 0
                               and a.output_usd_per_million > 0), "AI opt-in, model and pricing"),
        ]
        for okay, reason in checks:
            if not okay:
                raise ValueError(f"Invalid configuration: {reason}.")
        for name, value in asdict(self.costs).items():
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"Invalid fee: {name}")
        if self.costs.extra_buffer_bps < 1:
            raise ValueError("Maintain at least one basis point of fee uncertainty reserve.")

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class Instrument:
    symbol: str
    token: int
    tick: int
    lower: int
    upper: int
    reference: bool = False


@dataclass(frozen=True)
class Tick:
    symbol: str
    at: datetime
    last: int
    bid: int
    ask: int
    volume: int
    bid_size: int
    ask_size: int
    session_vwap: int = 0

    def validate(self) -> None:
        if self.at.tzinfo is None:
            raise ValueError("Tick has no timezone.")
        if min(self.last, self.bid, self.ask) <= 0 or self.bid > self.ask:
            raise ValueError("Invalid price/depth.")
        if min(self.volume, self.bid_size, self.ask_size, self.session_vwap) < 0:
            raise ValueError("Negative quantity.")


@dataclass(frozen=True)
class Candidate:
    symbol: str
    setup: str
    at: datetime
    stop: int


@dataclass
class Order:
    tag: str
    symbol: str
    purpose: str
    side: str
    quantity: int
    price: int
    trigger: int
    created: str
    order_id: str = ""
    status: str = "SUBMITTING"
    filled: int = 0
    value: int = 0
    fees: int = 0
    cancel_requested: bool = False

    @property
    def active(self) -> bool:
        return self.status not in TERMINAL

    @property
    def remaining(self) -> int:
        return self.quantity - self.filled


@dataclass(frozen=True)
class BrokerOrder:
    order_id: str
    tag: str
    symbol: str
    side: str
    quantity: int
    filled: int
    average: int
    status: str
    price: int
    trigger: int
    product: str = "CNC"
    exchange: str = "NSE"
    value: int = 0


@dataclass(frozen=True)
class Snapshot:
    orders: list[BrokerOrder]
    positions: dict[str, int]
    cash_available: int
    at: datetime
    foreign_activity: bool = False


@dataclass
class Position:
    symbol: str
    setup: str
    opened: str
    stop: int
    target: int
    trade_id: str = ""
    quantity: int = 0
    bought: int = 0
    buy_value: int = 0
    sell_value: int = 0
    fees: int = 0
    initial_risk: int = 0
    exit_reason: str = ""
    exit_reprices: int = 0


@dataclass(frozen=True)
class Session:
    day: date
    trading_day: bool
    reviewed: bool
    symbols: list[str]
    blackouts: list[dict[str, Any]]
    live_approved: bool = False
    account_id: str = ""
    capital_rupees: int = 0
    config_hash: str = ""

    @classmethod
    def load(cls, path: Path) -> Session:
        raw = json.loads(path.read_text(encoding="utf-8"))
        expected = {f.name for f in fields(cls)}
        if set(raw) - expected:
            raise ValueError("Unknown session field.")
        raw["day"] = date.fromisoformat(raw["day"])
        result = cls(**raw)
        if any(type(x) is not bool for x in (
            result.trading_day, result.reviewed, result.live_approved
        )):
            raise ValueError("Session boolean fields must be true/false.")
        if not isinstance(result.symbols, list) or not all(
            type(x) is str for x in result.symbols
        ):
            raise ValueError("Session symbols must be a list of strings.")
        if type(result.capital_rupees) is not int or result.capital_rupees < 0:
            raise ValueError("Session capital must be nonnegative integer rupees.")
        if not isinstance(result.blackouts, list):
            raise ValueError("Session blackouts must be a list.")
        for blackout in result.blackouts:
            start, end = timestamp(blackout["start"]), timestamp(blackout["end"])
            if start.date() != result.day or end <= start:
                raise ValueError("Invalid scheduled blackout.")
            symbols = blackout["symbols"]
            if not isinstance(symbols, list) or not symbols or not all(
                isinstance(x, str) and x in set(result.symbols) | {"*"} for x in symbols
            ):
                raise ValueError("Blackout symbols must be approved symbols or '*'.")
        return result

    def permits(self, symbol: str, at: datetime) -> bool:
        if (at.astimezone(IST).date() != self.day or not self.trading_day
                or not self.reviewed or symbol not in self.symbols):
            return False
        return not any(
            timestamp(x["start"]) <= at <= timestamp(x["end"])
            and (symbol in x["symbols"] or "*" in x["symbols"])
            for x in self.blackouts
        )
