from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, replace
from contextlib import closing
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .ai_provider import GEMINI_ENDPOINT, GEMINI_MODEL, verify_gemini_key
from .broker import BrokerError, KiteBroker, KiteHTTP, NoRedirect, load_kite_instruments
from .core import AIConfig, Config, IST, LiveConfig, NewsConfig, SafetyError, Session, TERMINAL, now_ist, paise, rupees, timestamp
from .credentials import CredentialVault, atomic_json, read_atomic_json
from .market_data import (
    NEWS_SOURCES, WATCHLIST_VERSION, AutomaticNews, cached_daily_history, closed_intraday_bars,
    constituents, market_timestamp, rank_opportunities, screen_quotes, select_diverse,
)

POLICY_VERSION = "cash-only-autonomous-v1"


class PreparationBlocked(SafetyError):
    def __init__(self, state: str, reason: str):
        super().__init__(reason)
        self.state = state


def validate_broker_capabilities(profile: dict) -> None:
    if not isinstance(profile, dict) or profile.get("broker") != "ZERODHA":
        raise PreparationBlocked("BLOCKED", "Kite did not report the expected Zerodha broker identity.")
    for field in ("exchanges", "products", "order_types"):
        values = profile.get(field)
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise PreparationBlocked(
                "BLOCKED", f"Kite returned invalid or missing {field}; trading permissions cannot be verified."
            )
    required = (
        ("exchanges", "NSE", "NSE cash exchange"),
        ("products", "CNC", "CNC product"),
        ("order_types", "LIMIT", "LIMIT orders"),
        ("order_types", "SL", "SL orders"),
    )
    missing = [label for field, code, label in required if code not in profile[field]]
    if missing:
        reason = "Missing broker permissions: " + ", ".join(missing) + "."
        if "NSE" not in profile["exchanges"]:
            enabled = ", ".join(profile["exchanges"]) or "none"
            reason += (
                f" Kite reports enabled exchanges: {enabled}."
                " Confirm the intended client account and ask Zerodha to enable/reactivate NSE equity trading;"
                " then sign in again. An API subscription alone does not activate exchange access."
            )
        else:
            reason += " Confirm the required cash-trading permissions with Zerodha, then sign in again."
        raise PreparationBlocked("BLOCKED", reason)


def default_auto_config(symbols: list[str], account: str, *, legacy_opening: bool = False,
                        legacy_signals: bool = False) -> Config:
    config = Config(
        market=replace(Config().market, symbols=list(symbols), entry_start="09:35" if legacy_opening else "09:25"),
        strategy=replace(Config().strategy, enabled=["orb", "vwap_pullback"] + (
            [] if legacy_signals else ["momentum_breakout"]),
            opening_range_minutes=15 if legacy_opening else 5,
            benchmark_alignment="absolute" if legacy_signals else "relative_strength"),
        news=NewsConfig(
            inbox="unused-managed-inbox", allowed_sources=list(NEWS_SOURCES),
            required_sources=list(NEWS_SOURCES), rss_poll_seconds=120,
        ),
        ai=AIConfig(
            enabled=True, share_public_news=True, endpoint=GEMINI_ENDPOINT,
            model=GEMINI_MODEL, api_key_env="GEMINI_API_KEY", max_calls_per_day=2,
            max_tokens_per_day=8000, max_cost_usd_per_day=0.10,
            input_usd_per_million=2.0, output_usd_per_million=12.0,
            max_input_bytes=1400, max_output_tokens=2048, cooldown_seconds=1800,
            timeout_seconds=20,
        ),
        live=LiveConfig(enabled=True, broker_user_id=account),
    )
    config.validate()
    return config


def read_ledger(path: Path) -> dict | None:
    if not path.exists():
        return None
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
        row = connection.execute("SELECT value FROM state WHERE key='engine'").fetchone()
    return json.loads(row[0]) if row else None


def has_exposure(state: dict | None) -> bool:
    if state is None:
        return False
    return state.get("position") is not None or any(
        item["status"] not in {"COMPLETE", "CANCELLED", "REJECTED"} for item in state.get("orders", [])
    )


def account_directory(directory: Path, account: str) -> Path:
    return directory / "accounts" / hashlib.sha256(account.encode()).hexdigest()[:24]


def broker_session_fingerprint(keys: dict) -> str:
    identity = [keys.get("broker_api_key", ""), keys.get("broker_access_token", "")]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def record_broker_connection(directory: Path, keys: dict, profile: dict, at: datetime) -> None:
    account = profile.get("user_id") if isinstance(profile, dict) else None
    if (not isinstance(profile, dict) or profile.get("broker") != "ZERODHA"
            or not isinstance(account, str) or not account.strip()
            or not keys.get("broker_api_key") or not keys.get("broker_access_token")):
        raise PreparationBlocked("BLOCKED", "Kite did not return a verifiable broker account identity.")
    expires = at.astimezone(IST).replace(hour=6, minute=0, second=0, microsecond=0)
    if expires <= at:
        expires += timedelta(days=1)
    atomic_json(directory / "broker-connection.json", {
        "account_ref": hashlib.sha256(account.encode()).hexdigest(),
        "id_masked": "****" + account[-4:], "verified_at": at.isoformat(),
        "session_expires_at": expires.isoformat(),
        "session_fingerprint": broker_session_fingerprint(keys),
    })


def broker_connection(directory: Path, keys: dict, at: datetime) -> dict | None:
    path = directory / "broker-connection.json"
    if not path.exists() or not keys.get("broker_api_key") or not keys.get("broker_access_token"):
        return None
    saved = read_atomic_json(path)
    if saved.get("session_fingerprint") != broker_session_fingerprint(keys):
        return None
    valid = timestamp(saved["verified_at"]) <= at < timestamp(saved["session_expires_at"])
    return {
        "account_ref": saved["account_ref"], "id_masked": saved["id_masked"],
        "verified_at": saved["verified_at"], "session_expires_at": saved["session_expires_at"],
        "authentication_status": "verified" if valid else "reauthentication_required",
        "last_update": None, "snapshot_fresh": False,
    }


def software_ready(root: Path) -> bool:
    from .runtime import code_hash
    path = root / "data" / "software-check.json"
    if not path.exists():
        return False
    value = json.loads(path.read_text(encoding="utf-8"))
    return value.get("passed") is True and value.get("tests_run", 0) >= 50 and value.get("code_hash") == code_hash(root)


def reuse_daily_selection(plan: dict | None, at: datetime, exposure: bool) -> bool:
    if plan is None:
        return False
    plan_day = date.fromisoformat(plan["day"])
    if plan_day > at.date():
        raise PreparationBlocked("BLOCKED", "Saved watchlist is future-dated; check the clock before trading.")
    return exposure or plan_day == at.date()


def check_flat_entry_window(http: KiteHTTP, config: Config, exposure: bool) -> None:
    if exposure or now_ist().time() < time.fromisoformat(config.market.entry_end):
        return
    started = now_ist()
    snapshot = KiteBroker(http, {}).snapshot(started)
    if not 0 <= (now_ist() - snapshot.at).total_seconds() <= 15:
        raise PreparationBlocked("BLOCKED", "Broker snapshot is too old to confirm a flat late-session startup.")
    if snapshot.foreign_activity or any(snapshot.positions.values()) or any(
        order.status not in TERMINAL for order in snapshot.orders
    ):
        raise PreparationBlocked(
            "BLOCKED", "Broker exposure or working orders remain after the new-entry cutoff; reconcile before proceeding.",
        )
    raise PreparationBlocked(
        "ENTRY_WINDOW_CLOSED",
        f"New entries ended at {config.market.entry_end} IST. Broker reports no open positions or working orders."
        " Entry-history warm-up is unnecessary; waiting for the next authorized session.",
    )


def prepare_session(root: Path, directory: Path, vault: CredentialVault, progress=lambda *_: None) -> dict:
    at = now_ist()
    value = vault.load()
    keys = value["keys"]
    if not value["auto_start"] or value.get("consent_version") != "auto-live-v1":
        raise PreparationBlocked("STOPPED", "Automatic live operation has not been authorized.")
    if not keys.get("ai_api_key") or not keys.get("broker_api_key"):
        raise PreparationBlocked("WAITING_CONFIG", "Save your Gemini and broker API credentials.")
    if not keys.get("broker_access_token"):
        raise PreparationBlocked("BROKER_LOGIN_REQUIRED", "Sign in on Zerodha to obtain today's broker session.")
    if not software_ready(root):
        raise PreparationBlocked("CHECKS_REQUIRED", "The current code must pass its local software checks before starting.")
    if at.weekday() >= 5 or not time(8, 30) <= at.time() < time(15, 25):
        raise PreparationBlocked("MARKET_CLOSED", "Waiting for weekday pre-market preparation from 08:30 IST; continuous trading starts at 09:15.")
    progress("VALIDATING", "Checking broker identity, cash permissions and current session.")
    http = KiteHTTP(Config(), False, api_key=keys["broker_api_key"], access_token=keys["broker_access_token"])
    try:
        profile = http.request("GET", "/user/profile")
    except BrokerError as exc:
        if exc.session_expired:
            raise PreparationBlocked("BROKER_LOGIN_REQUIRED", str(exc)) from None
        raise
    record_broker_connection(directory, keys, profile, now_ist())
    validate_broker_capabilities(profile)
    account = str(profile["user_id"])
    workspace = account_directory(directory, account)
    workspace.mkdir(parents=True, exist_ok=True)
    database, plan_path = workspace / "live.db", workspace / "plan.json"
    prior = read_ledger(database)
    exposure = has_exposure(prior)
    saved_plan = read_atomic_json(plan_path) if plan_path.exists() else None
    if exposure and saved_plan is None:
        raise PreparationBlocked("BLOCKED", "An existing exposure ledger has no matching plan. Reconcile manually.")
    if prior and prior.get("quarantine"):
        raise PreparationBlocked("BLOCKED", "This account is quarantined for reconciliation; it was not reset.")
    if saved_plan and (saved_plan.get("account") != account or saved_plan.get("policy") != POLICY_VERSION):
        raise PreparationBlocked("BLOCKED", "Saved account/policy does not match. Reconcile before migrating.")
    holdings = http.request("GET", "/portfolio/holdings")
    if any(int(x.get("quantity", 0)) or int(x.get("t1_quantity", 0)) for x in holdings):
        raise PreparationBlocked("BLOCKED", "Use a dedicated cash account without unrelated delivery holdings.")
    saved_config = Config.from_mapping(saved_plan["config"]) if saved_plan and "config" in saved_plan else Config()
    check_flat_entry_window(http, saved_config, exposure)
    if not exposure:
        progress("VALIDATING", "Verifying Gemini key/model without a billable generation.")
        verify_gemini_key(keys["ai_api_key"])
    cash = KiteBroker.conservative_cash(http.request("GET", "/user/margins/equity"))
    if cash < 100000 and not exposure:
        raise PreparationBlocked(
            "NEEDS_CASH",
            f"Usable funded cash is INR {rupees(cash)}; startup requires INR 1,000.00."
            " This excludes collateral and ad-hoc margin and respects broker-blocked funds."
            " No funds will be transferred.",
        )
    if prior:
        cash = min(cash, int(prior["capital"]), max(0, int(prior["cash"]))) if not exposure else cash
    from .pre_market import global_context, premarket_candidates, rank_premarket
    context = global_context(workspace / "context", at)
    reused_selection = reuse_daily_selection(saved_plan, at, exposure)
    if reused_selection:
        selected = saved_plan["selected"]
        aliases = saved_plan["aliases"]
        origin = saved_plan["universe_source"]
        history = saved_plan.get("history", {})
        diagnostics = saved_plan.get("screen", [])
        selection = saved_plan.get("selection", {
            "session_day": saved_plan["day"], "refreshed_at": saved_plan["created_at"],
            "version": "legacy", "universe_source": origin,
        })
    else:
        progress("PREPARING", "Refreshing today's NIFTY 200 cash-stock universe and live liquidity screen.")
        master = {x["tradingsymbol"]: x for x in csv.DictReader(io.StringIO(
            http.request("GET", "/instruments/NSE", raw=True)
        )) if x["exchange"] == "NSE"}
        symbols, aliases, sectors, origin = constituents(master, directory / "constituents-cache.json")
        quotes = http.request("GET", "/quote", query=[("i", "NSE:" + x) for x in symbols])
        early = at.time() < time(9, 15)
        ranked = premarket_candidates(quotes, symbols, cash) if early else screen_quotes(quotes, symbols, cash, now_ist())
        if not ranked:
            raise PreparationBlocked("WAITING_MARKET", "No fresh, liquid, affordable candidate clears screening; this may be a holiday.")
        scan_config = default_auto_config([x["symbol"] for x in ranked[:25]], account)
        news = AutomaticNews(scan_config, aliases, symbols=symbols)
        progress("PREPARING", "Checking official issuer/macro feeds and event exclusions.")
        news.poll()
        benchmark_symbol = scan_config.market.benchmark
        if benchmark_symbol not in master:
            raise PreparationBlocked("BLOCKED", "The broker instrument master lacks the configured reference index.")
        benchmark_history = cached_daily_history(
            http, int(master[benchmark_symbol]["instrument_token"]), now_ist(), workspace / "history"
        )
        history, omissions = {}, []
        pool = [item for item in ranked if item["symbol"] not in news.excluded][:30]
        progress("PREPARING", "Ranking today's candidates against 5/20-session history, benchmark and issuer events.")
        for candidate in pool:
            symbol = candidate["symbol"]
            try:
                history[symbol] = cached_daily_history(
                    http, int(master[symbol]["instrument_token"]), now_ist(), workspace / "history"
                )
            except BrokerError:
                raise
            except SafetyError as error:
                omissions.append({"symbol": symbol, "reason": str(error)})
        refresh_symbols = [item["symbol"] for item in pool]
        refreshed_at = now_ist()
        if early:
            opportunities = rank_premarket(pool, history, benchmark_history, sectors,
                                          news.excluded, news.catalysts, refreshed_at)
        else:
            refreshed = http.request("GET", "/quote", query=[
                ("i", "NSE:" + x) for x in refresh_symbols + [benchmark_symbol]
            ])
            refreshed_at = now_ist()
            benchmark_quote = refreshed.get("NSE:" + benchmark_symbol)
            if (not benchmark_quote
                    or not -2 <= (refreshed_at - market_timestamp(benchmark_quote["timestamp"])).total_seconds() <= 30
                    or paise(benchmark_quote["ohlc"]["close"]) <= 0):
                raise PreparationBlocked("WAITING_MARKET", "A fresh reference-index quote is required for daily ranking.")
            benchmark_change = (
                paise(benchmark_quote["last_price"]) / paise(benchmark_quote["ohlc"]["close"]) - 1
            ) * 10000
            # Recheck events after history acquisition; cached candles cannot substitute for current inputs.
            news.poll()
            opportunities = rank_opportunities(
                screen_quotes(refreshed, refresh_symbols, cash, refreshed_at), history, benchmark_history,
                benchmark_change, sectors, news.catalysts, news.excluded, refreshed_at,
            )
        diagnostics = select_diverse(opportunities)
        selected = [item["symbol"] for item in diagnostics]
        if not selected:
            raise PreparationBlocked("WAITING_MARKET", "No candidate clears history, volatility, event and liquidity checks.")
        selection = {
            "session_day": refreshed_at.date().isoformat(), "refreshed_at": refreshed_at.isoformat(),
            "version": WATCHLIST_VERSION, "universe_source": origin,
            "universe_count": len(symbols), "liquid_affordable_count": len(ranked),
            "history_reviewed_count": len(history), "benchmark": benchmark_symbol,
            "benchmark_history_as_of": benchmark_history["last_session"],
            "event_excluded": sorted(news.excluded), "history_omissions": omissions,
            "ranked_candidates": opportunities,
            "pre_market": early,
            "reason": "Daily fresh ranking; maximum two names per industry. Scores are not predicted gains.",
        }
    config = Config.from_mapping(saved_plan["config"]) if reused_selection and "config" in saved_plan else default_auto_config(selected, account)
    if not exposure:
        for legacy_opening in (False, True):
            if config == default_auto_config(selected, account, legacy_opening=legacy_opening, legacy_signals=True):
                # Adopt only the published signal-profile upgrade; no risk/size/time reset.
                config = default_auto_config(selected, account, legacy_opening=legacy_opening)
                break
    check_flat_entry_window(http, config, exposure)
    news = AutomaticNews(config, aliases)
    if not exposure:
        progress("PREPARING", "Verifying current official news coverage.")
        news.poll()
    http.config = config
    pre_market = now_ist().time() < time(9, 15)
    warmup = {}
    if not pre_market:
        instruments = load_kite_instruments(http, config)
        progress("PREPARING", "Loading completed intraday candles; no fabricated opening history.")
    if not exposure and not pre_market:
        from .market import HistoryNotReady, Tape
        for symbol, instrument in instruments.items():
            check_flat_entry_window(http, config, exposure)
            when = now_ist()
            if when.time() >= time(9, 20):
                try:
                    bars = closed_intraday_bars(http, instrument.token, when)
                    Tape().seed(bars, when)
                except HistoryNotReady as error:
                    raise PreparationBlocked(
                        "WAITING_HISTORY", f"{symbol}: {error} Automatic recheck in 15 seconds; entries remain paused.",
                    ) from None
                warmup[symbol] = [{**asdict(x), "start": x.start.isoformat()} for x in bars]
    plan = {
        "policy": POLICY_VERSION, "day": at.date().isoformat(), "account": account,
        "selected": selected, "aliases": aliases, "universe_source": origin,
        "history": history, "screen": diagnostics, "warmup": warmup,
        "recovery_only": exposure, "created_at": now_ist().isoformat(),
        "selection": selection, "selection_reused": reused_selection,
        "global_context": context,
        "cash_available_paise": cash, "news_health": news.health,
        "database": str(database), "config": asdict(config),
        "credential_fingerprint": hashlib.sha256(json.dumps(keys, sort_keys=True).encode()).hexdigest(),
    }
    atomic_json(plan_path, plan)
    if not reused_selection:
        atomic_json(workspace / "watchlists" / f"{plan['day']}.json", {
            "selected": selected, "selection": selection,
        })
    atomic_json(directory / "active-account.json", {"directory": str(workspace), "account_suffix": account[-4:]})
    if pre_market:
        raise PreparationBlocked(
            "PREMARKET_READY",
            f"Pre-market research ready. Continuous-market worker starts at 09:15;"
            f" earliest confirmed entry window {config.market.entry_start} IST. No auction orders.",
        )
    return plan


def broker_login_url(api_key: str, state: str) -> str:
    return "https://kite.zerodha.com/connect/login?" + urllib.parse.urlencode({
        "v": "3", "api_key": api_key, "redirect_params": urllib.parse.urlencode({"state": state}),
    })


def exchange_broker_token(api_key: str, api_secret: str, request_token: str,
                          expected_account: str | None = None) -> str:
    if not request_token or len(request_token) > 4096:
        raise SafetyError("Broker callback did not contain a valid request token.")
    checksum = hashlib.sha256((api_key + request_token + api_secret).encode()).hexdigest()
    request = urllib.request.Request("https://api.kite.trade/session/token", method="POST",
        data=urllib.parse.urlencode({"api_key": api_key, "request_token": request_token, "checksum": checksum}).encode(),
        headers={"X-Kite-Version": "3", "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=10) as response:
            result = json.loads(response.read(65536))
        if result.get("status") != "success" or not result["data"].get("access_token"):
            raise SafetyError("Broker did not issue an access token.")
        if expected_account is not None and result["data"].get("user_id") != expected_account:
            raise SafetyError("Sign in to the original broker account to recover its owned exposure.")
        return result["data"]["access_token"]
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        raise SafetyError("Broker token exchange failed. Sign in again; no password or OTP was stored.") from None
