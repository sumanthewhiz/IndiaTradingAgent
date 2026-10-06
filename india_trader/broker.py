from __future__ import annotations

import csv
import http.client
import io
import json
import os
import re
import socket
import ssl
import threading
import time as clock
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from .core import (
    TERMINAL, BrokerOrder, Config, CostConfig, Instrument, Order, SafetyError,
    Snapshot, Tick, paise, rupees,
)
from .storage import Store


class BrokerError(SafetyError):
    def __init__(self, message: str, status_code: int | None = None, *,
                 error_type: str | None = None, endpoint: str | None = None,
                 method: str | None = None, category: str | None = None,
                 retry_after_seconds: float | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.endpoint = endpoint
        self.method = method
        self.category = category
        self.retry_after_seconds = retry_after_seconds

    @property
    def session_expired(self) -> bool:
        return self.status_code == 401 or (
            self.status_code == 403 and self.error_type == "TokenException"
        )


class BrokerRejected(BrokerError):
    pass


class SubmissionUnknown(BrokerError):
    pass


class BrokerReadUnavailable(BrokerError):
    """An explicitly read-only request may be retried; no order side effect is implied."""


class Broker(Protocol):
    def submit(self, order: Order) -> str: ...
    def cancel(self, order_id: str) -> None: ...
    def snapshot(self, at: datetime) -> Snapshot: ...


class PaperBroker:
    """Orders can fill only on a later tick, at executable bid/ask, with partial fills."""

    def __init__(self, capital: int, costs: CostConfig, store: Store | None = None):
        self.costs, self.store = costs, store
        existing = store.get("paper_broker") if store else None
        self.cash = existing["cash"] if existing else capital
        self.positions: dict[str, int] = existing["positions"] if existing else {}
        self.orders: dict[str, dict[str, Any]] = existing["orders"] if existing else {}
        self.counter = existing["counter"] if existing else 0
        self.dirty = True
        self._save()

    def _save(self) -> None:
        if self.store:
            self.store.put("paper_broker", {
                "cash": self.cash, "positions": self.positions,
                "orders": self.orders, "counter": self.counter,
            })

    def submit(self, order: Order) -> str:
        if any(x["tag"] == order.tag for x in self.orders.values()):
            raise BrokerRejected("Duplicate client reference in paper broker.")
        self.counter += 1
        identifier = f"P{self.counter:08d}"
        item = asdict(order)
        item.update(order_id=identifier, status="TRIGGER PENDING" if order.trigger else "OPEN")
        item["triggered"] = False
        self.orders[identifier] = item
        self.dirty = True
        self._save()
        return identifier

    def cancel(self, order_id: str) -> None:
        order = self.orders[order_id]
        if order["status"] not in TERMINAL:
            order["status"] = "CANCELLED"
            self.dirty = True
        self._save()

    def on_tick(self, tick: Tick) -> None:
        changed = False
        available = {"BUY": max(0, tick.ask_size // 10),
                     "SELL": max(0, tick.bid_size // 10)}
        for order in self.orders.values():
            if order["symbol"] != tick.symbol or order["status"] in TERMINAL:
                continue
            if tick.at <= datetime.fromisoformat(order["created"]):
                continue
            side = order["side"]
            if order["trigger"]:
                if tick.last <= order["trigger"] and not order["triggered"]:
                    order["triggered"] = True
                    changed = True
                if not order["triggered"]:
                    continue
                order["status"] = "OPEN"
            executable = tick.ask <= order["price"] if side == "BUY" else tick.bid >= order["price"]
            if not executable:
                continue
            quantity = min(order["quantity"] - order["filled"], available[side])
            if quantity <= 0:
                continue
            price = tick.ask if side == "BUY" else tick.bid
            total_value = order["value"] + quantity * price
            total_fee = self.costs.fee(side, total_value)
            delta_fee = total_fee - order["fees"]
            if side == "BUY" and quantity * price + delta_fee > self.cash:
                order["status"] = "REJECTED"
                changed = True
                continue
            if side == "SELL" and quantity > self.positions.get(tick.symbol, 0):
                order["status"] = "REJECTED"
                changed = True
                continue
            order["filled"] += quantity
            changed = True
            order["value"] = total_value
            order["fees"] = total_fee
            sign = 1 if side == "BUY" else -1
            self.positions[tick.symbol] = self.positions.get(tick.symbol, 0) + sign * quantity
            self.cash -= sign * quantity * price + delta_fee
            available[side] -= quantity
            if order["filled"] == order["quantity"]:
                order["status"] = "COMPLETE"
        if changed:
            self.dirty = True
            self._save()

    def snapshot(self, at: datetime) -> Snapshot:
        orders = [
            BrokerOrder(
                x["order_id"], x["tag"], x["symbol"], x["side"],
                x["quantity"], x["filled"],
                int(Decimal(x["value"]) / x["filled"] + Decimal("0.5")) if x["filled"] else 0,
                x["status"], x["price"], x["trigger"], value=x["value"],
            )
            for x in self.orders.values()
        ]
        blocked = sum(
            (x["quantity"] - x["filled"]) * x["price"]
            for x in self.orders.values() if x["side"] == "BUY" and x["status"] not in TERMINAL
        )
        self.dirty = False
        return Snapshot(orders, {s: q for s, q in self.positions.items() if q},
                        max(0, self.cash - blocked), at)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise BrokerError("HTTP redirect refused; credentials were not forwarded.")


class KiteHTTP:
    """Fixed broker origin and route allowlist. No transfers, funding or arbitrary URLs."""

    def __init__(self, config: Config, allow_orders: bool, *,
                 api_key: str | None = None, access_token: str | None = None):
        self.api_key = api_key if api_key is not None else os.environ.get("KITE_API_KEY", "")
        self.access_token = access_token if access_token is not None else os.environ.get("KITE_ACCESS_TOKEN", "")
        if not self.api_key or not self.access_token:
            raise SafetyError("Set KITE_API_KEY and KITE_ACCESS_TOKEN in this process.")
        self.config, self.allow_orders = config, allow_orders
        self.auth_expired = False
        self.opener = urllib.request.build_opener(NoRedirect)
        self.lock = threading.Lock()
        self.last_request = 0.0

    @staticmethod
    def allowed(method: str, path: str, allow_orders: bool) -> bool:
        if method == "GET":
            if re.fullmatch(r"/instruments/historical/[0-9]+/(5minute|day)", path):
                return True
            return path in {
                "/user/profile", "/user/margins/equity", "/orders", "/portfolio/positions",
                "/portfolio/holdings", "/instruments/NSE", "/quote",
            }
        if not allow_orders:
            return False
        return (
            (method == "POST" and path == "/orders/regular")
            or (method == "DELETE" and re.fullmatch(r"/orders/regular/[A-Za-z0-9]+", path) is not None)
        )

    def _error(self, response: urllib.error.HTTPError, method: str, path: str) -> BrokerError:
        error_type = None
        detail = "Broker error detail unavailable."
        retry_after = (response.headers or {}).get("Retry-After", "")
        retry_after_seconds = (
            float(retry_after) if isinstance(retry_after, str) and re.fullmatch(r"\d{1,5}", retry_after) else None
        )
        try:
            content = response.read(16385)
            if len(content) <= 16384:
                payload = json.loads(content)
                if isinstance(payload, dict):
                    candidate = payload.get("error_type")
                    if isinstance(candidate, str) and re.fullmatch(r"[A-Za-z]{1,64}", candidate):
                        error_type = candidate
                    message = payload.get("message")
                    if isinstance(message, str):
                        for secret in sorted((self.api_key, self.access_token), key=len, reverse=True):
                            if secret:
                                message = message.replace(secret, "[redacted]")
                        message = re.sub(r"https?://\S+", "[redacted URL]", message, flags=re.I)
                        message = re.sub(
                            r"\b(authorization|api_key|api_secret|access_token|request_token|password)"
                            r"\b[\"']?\s*[:=]\s*[^\r\n,;]+",
                            r"\1=[redacted]", message, flags=re.I,
                        )
                        printable = "".join(char for char in message if char.isprintable() or char.isspace())
                        detail = " ".join(printable.split())[:300] or "Broker error detail unavailable."
        except (ValueError, UnicodeError, OSError, http.client.HTTPException):
            detail = "Broker error detail unavailable."
        finally:
            response.close()
        classification = f", {error_type}" if error_type else ""
        message = f"Kite {method} {path} rejected (HTTP {response.code}{classification})."
        if detail:
            message += " " + detail
        if response.code == 401 or (response.code == 403 and error_type == "TokenException"):
            message += " Broker session expired or invalid; sign in with Zerodha again."
        elif response.code == 403:
            if re.search(r"\b(?:IP|whitelist|whitelisted)\b", detail, re.I):
                message += (
                    " Verify the trading host's outbound static IP against the Kite developer-profile whitelist."
                    " No order was retried."
                )
            elif method == "GET" and (path == "/quote" or path.startswith("/instruments/historical/")):
                message += (
                    " Market-data access was denied for this Kite app. In My Apps, verify the paid"
                    " Connect subscription with live and historical data is active for the same API key."
                    " IP whitelisting does not grant market-data access."
                )
            else:
                message += " Confirm this app's account/API permissions with Zerodha; not every 403 is an IP rejection."
        rejected = response.code in {400, 401, 403, 404, 405, 422}
        transient_read = method == "GET" and response.code in {408, 429, 500, 502, 503, 504}
        if transient_read:
            message += " Read-only broker data is unavailable; pause entries and recheck with backoff."
        elif not rejected:
            message += " Request outcome may be unknown; reconcile before further action."
        cls = BrokerReadUnavailable if transient_read else BrokerRejected if rejected else SubmissionUnknown
        return cls(message, response.code, error_type=error_type, endpoint=path, method=method,
                   category="http_error", retry_after_seconds=retry_after_seconds)

    @staticmethod
    def _transport_error(error: Exception, method: str, path: str) -> BrokerError:
        cause = error.reason if isinstance(error, urllib.error.URLError) else error
        if isinstance(cause, ssl.SSLCertVerificationError):
            category = "tls_certificate"
        elif isinstance(cause, (ssl.SSLEOFError, ssl.SSLZeroReturnError)):
            category = "connection_interrupted"
        elif isinstance(cause, ssl.SSLError):
            category = "tls_error"
        elif isinstance(cause, TimeoutError):
            category = "timeout"
        elif isinstance(cause, socket.gaierror):
            category = "dns_resolution"
        elif isinstance(cause, http.client.IncompleteRead):
            category = "incomplete_response"
        elif isinstance(cause, (ConnectionError, http.client.RemoteDisconnected)):
            category = "connection_interrupted"
        else:
            category = "transport"
        message = f"Kite {method} {path} transport failure ({category})."
        retryable = method == "GET" and category not in {"tls_certificate", "tls_error"}
        if retryable:
            message += " Read-only data unavailable; no order was submitted by this request."
            cls = BrokerReadUnavailable
        elif method == "GET":
            message += " Verify broker connectivity/TLS configuration; certificate checks were not disabled."
            cls = BrokerError
        else:
            message += " Order outcome may be unknown; reconcile before action and do not resubmit blindly."
            cls = SubmissionUnknown
        return cls(message, endpoint=path, method=method, category=category)

    def request(
        self, method: str, path: str, data: dict[str, Any] | None = None,
        query: list[tuple[str, str]] | None = None, raw: bool = False,
    ) -> Any:
        if not self.allowed(method, path, self.allow_orders):
            raise SafetyError("Broker route is not on the read/trade-only allowlist.")
        url = "https://api.kite.trade" + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        body = urllib.parse.urlencode(data).encode() if data is not None else None
        request = urllib.request.Request(url, data=body, method=method, headers={
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "IndiaTradingAgent/0.1",
        })
        with self.lock:
            delay = self.config.execution.request_interval_seconds - (
                clock.monotonic() - self.last_request
            )
            if delay > 0:
                clock.sleep(delay)
            self.last_request = clock.monotonic()
            try:
                with self.opener.open(request, timeout=5) as response:
                    content = response.read(16_000_001)
            except urllib.error.HTTPError as exc:
                failure = self._error(exc, method, path)
                if failure.session_expired:
                    self.auth_expired = True
                # A timeout/5xx may follow acceptance; never retry an order submission.
                raise failure from None
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as error:
                raise self._transport_error(error, method, path) from None
        if len(content) > 16_000_000:
            raise BrokerError("Broker response exceeded size limit.")
        try:
            if raw:
                return content.decode("utf-8-sig")
            result = json.loads(content)
        except (UnicodeError, ValueError):
            cls = BrokerError if method == "GET" else SubmissionUnknown
            raise cls(f"Malformed Kite {method} {path} response; reconcile before action.",
                      method=method, endpoint=path, category="invalid_response") from None
        if not isinstance(result, dict) or result.get("status") != "success" or "data" not in result:
            cls = BrokerError if method == "GET" else SubmissionUnknown
            raise cls(f"Unexpected Kite {method} {path} response; reconcile before action.",
                      method=method, endpoint=path, category="invalid_response")
        return result["data"]


class KiteBroker:
    def __init__(self, http: KiteHTTP, instruments: dict[str, Instrument]):
        self.http, self.instruments = http, instruments
        self.trade_symbols = set(http.config.market.symbols)

    def submit(self, order: Order) -> str:
        instrument = self.instruments[order.symbol]
        if (instrument.reference or order.symbol not in self.trade_symbols
                or order.side not in {"BUY", "SELL"} or type(order.quantity) is not int
                or order.quantity <= 0 or not re.fullmatch(r"[a-f0-9]{8}", order.tag)
                or not instrument.lower <= order.price <= instrument.upper
                or order.price % instrument.tick):
            raise BrokerRejected("Order failed cash-instrument/price/quantity validation.")
        if order.trigger and (
            order.side != "SELL" or order.purpose != "protect"
            or not order.price <= order.trigger <= instrument.upper
            or order.trigger % instrument.tick
        ):
            raise BrokerRejected("Invalid protective stop-limit.")
        payload: dict[str, Any] = {
            "exchange": "NSE", "tradingsymbol": order.symbol,
            "transaction_type": order.side, "quantity": order.quantity,
            "product": "CNC", "order_type": "SL" if order.trigger else "LIMIT",
            "price": rupees(order.price), "validity": "DAY", "tag": order.tag,
        }
        if order.trigger:
            payload["trigger_price"] = rupees(order.trigger)
        if self.http.config.live.algo_id:
            payload["algo_id"] = self.http.config.live.algo_id
        result = self.http.request("POST", "/orders/regular", data=payload)
        identifier = result.get("order_id") if isinstance(result, dict) else None
        if not identifier:
            raise SubmissionUnknown("Order acknowledgment has no ID.")
        return str(identifier)

    def cancel(self, order_id: str) -> None:
        self.http.request("DELETE", f"/orders/regular/{order_id}")

    @staticmethod
    def conservative_cash(raw: dict[str, Any]) -> int:
        if raw.get("enabled") is False:
            raise SafetyError("The broker equity segment is disabled.")
        available, utilised = raw["available"], raw["utilised"]
        if paise(available["collateral"]) != 0:
            raise SafetyError("Pledged collateral is unsupported; use a dedicated cash-only account.")
        if any(paise(utilised.get(x, 0)) != 0 for x in ("span", "exposure", "option_premium")):
            raise SafetyError("Derivative/margin usage detected.")
        funded_cash = paise(available["cash"])
        if "opening_balance" in available and "intraday_payin" in available:
            # Some Kite responses leave cash at zero and report today's deposits separately.
            # Use an alternative funding basis, never cash + pay-in (which can double count).
            opening_and_payin = paise(available["opening_balance"]) + paise(available["intraday_payin"])
            funded_cash = max(funded_cash, opening_and_payin)
        adhoc = max(0, paise(available.get("adhoc_margin", 0)))
        return max(0, min(funded_cash, paise(available["live_balance"]) - adhoc,
                          paise(raw["net"]) - adhoc))

    def snapshot(self, at: datetime) -> Snapshot:
        raw_orders = self.http.request("GET", "/orders")
        positions = self.http.request("GET", "/portfolio/positions")
        margins = self.http.request("GET", "/user/margins/equity")
        parsed: list[BrokerOrder] = []
        foreign = False
        for item in raw_orders:
            if item["product"] != "CNC" or item["exchange"] != "NSE":
                foreign = True
            quantity, filled = int(item["quantity"]), int(item["filled_quantity"])
            average = item["average_price"]
            parsed.append(BrokerOrder(
                str(item["order_id"]), item.get("tag") or "", item["tradingsymbol"],
                item["transaction_type"], quantity, filled, paise(average), item["status"],
                paise(item["price"]), paise(item.get("trigger_price", 0)),
                item["product"], item["exchange"], paise(Decimal(str(average)) * filled),
            ))
        net: dict[str, int] = {}
        for item in positions["net"]:
            quantity = int(item["quantity"])
            if not quantity:
                continue
            if item["exchange"] != "NSE" or item["product"] != "CNC":
                foreign = True
            symbol = item["tradingsymbol"]
            net[symbol] = net.get(symbol, 0) + quantity
        return Snapshot(parsed, net, self.conservative_cash(margins), at, foreign)


def cash_instrument(symbol: str, row: dict, quote: dict) -> Instrument:
    if (row["exchange"] != "NSE" or row["segment"] != "NSE" or row["instrument_type"] != "EQ"
            or int(row["lot_size"]) != 1 or row.get("expiry") or row["tradingsymbol"] != symbol):
        raise SafetyError("Only current NSE cash EQ shares with lot size one are supported.")
    step = paise(row["tick_size"])
    low, high = paise(quote["lower_circuit_limit"]), paise(quote["upper_circuit_limit"])
    if step <= 0 or not 0 < low < high:
        raise SafetyError("Missing/invalid daily tick size or price bands.")
    return Instrument(symbol, int(row["instrument_token"]), step, low, high)


def load_kite_instruments(http: KiteHTTP, config: Config, *, extra_symbols: set[str] | None = None) -> dict[str, Instrument]:
    required = set(config.market.symbols) | {config.market.benchmark} | (extra_symbols or set())
    master = csv.DictReader(io.StringIO(http.request("GET", "/instruments/NSE", raw=True)))
    rows = {row["tradingsymbol"]: row for row in master
            if row["tradingsymbol"] in required and row["exchange"] == "NSE"}
    if set(rows) != required:
        raise SafetyError("Allowlist/benchmark missing from today's broker instrument master.")
    quotes = http.request("GET", "/quote", query=[("i", "NSE:" + x) for x in sorted(required)])
    result: dict[str, Instrument] = {}
    for symbol, row in rows.items():
        reference = symbol == config.market.benchmark
        quote = quotes["NSE:" + symbol]
        if reference:
            result[symbol] = Instrument(symbol, int(row["instrument_token"]), 1, 1, 10**12, True)
            continue
        result[symbol] = cash_instrument(symbol, row, quote)
    return result
