from __future__ import annotations

import time as clock
from datetime import datetime
from typing import TYPE_CHECKING

from .broker import BrokerError, BrokerReadUnavailable
from .core import SafetyError, Snapshot

if TYPE_CHECKING:
    from .engine import TradingEngine

SNAPSHOT_ENDPOINTS = frozenset({"/orders", "/portfolio/positions", "/user/margins/equity"})
LEGACY_READ_HALT = "Broker reconciliation failed: Broker transport failure; reconcile before action."
MAX_SNAPSHOT_AGE = 15


class ReconciliationHealth:
    """Transient GET failures gate readiness, not permanent trading strategy state."""

    def __init__(self, engine: TradingEngine):
        self.engine = engine
        self.failures = 0
        self.retry_not_before = 0.0
        self.last_success: datetime | None = None
        self.legacy_recovery = engine.state["halt"] == LEGACY_READ_HALT
        engine.broker_reads_ready = False
        engine.reconciliation_status = {
            "state": "checking", "reason": "Verifying broker orders, positions and funded cash.",
        }

    def failure(self, error: Exception, at: datetime, *, monotonic: float | None = None) -> None:
        engine = self.engine
        engine.reconciled = False
        engine.broker_reads_ready = False
        self.failures += 1
        temporary = isinstance(error, BrokerReadUnavailable) and error.method == "GET" and error.endpoint in SNAPSHOT_ENDPOINTS
        reason = str(error) if isinstance(error, SafetyError) else type(error).__name__
        delay = min(30, 2 ** min(self.failures, 5))
        if isinstance(error, BrokerError) and error.retry_after_seconds is not None:
            delay = max(delay, error.retry_after_seconds)
        mono = clock.monotonic() if monotonic is None else monotonic
        self.retry_not_before = mono + delay
        engine.reconciliation_status = {
            "state": "retrying" if temporary else "blocked", "at": at.isoformat(),
            "reason": reason, "last_success_at": self.last_success.isoformat() if self.last_success else None,
            "consecutive_failures": self.failures, "retry_delay_seconds": delay,
            "endpoint": error.endpoint if isinstance(error, BrokerError) else None,
            "category": error.category if isinstance(error, BrokerError) else type(error).__name__,
        }
        engine.store.audit(at, "BROKER_READ_UNAVAILABLE" if temporary else "BROKER_RECONCILIATION_FAILED",
                           **{key: value for key, value in engine.reconciliation_status.items() if key != "at"})
        if not temporary:
            engine.halt("Broker reconciliation failed: " + reason, at, liquidate=False)
        if isinstance(error, BrokerError) and error.session_expired:
            engine.state["broker_auth_required"] = True
            engine._save()

    def success(self, snapshot: Snapshot, at: datetime) -> bool:
        engine = self.engine
        if not 0 <= (at - snapshot.at).total_seconds() <= MAX_SNAPSHOT_AGE:
            self.failure(BrokerReadUnavailable(
                "Broker snapshot is stale or future-dated; re-read before new actions.",
                method="GET", endpoint="/orders", category="snapshot_age",
            ), at)
            return False
        # Do not cancel native protection while the batch is only partly applied.
        engine.broker_reads_ready = False
        try:
            engine.reconcile(snapshot, at)
        except (SafetyError, ValueError, KeyError, TypeError) as error:
            self.failure(error, at)
            return False
        if not engine.reconciled or engine.state["quarantine"]:
            engine.reconciliation_status = {
                "state": "reconciling", "at": at.isoformat(),
                "reason": engine.state["halt"] or "Broker reads succeeded; ownership reconciliation is not yet complete.",
                "last_success_at": self.last_success.isoformat() if self.last_success else None,
            }
            return False
        recovering = self.failures > 0
        self.last_success = at
        self.failures = 0
        self.retry_not_before = 0.0
        engine.broker_reads_ready = True
        if (self.legacy_recovery and engine.state["halt"] == LEGACY_READ_HALT
                and engine.flat and not engine.state["quarantine"]):
            engine.state["halt"] = ""
            self.legacy_recovery = False
            recovering = True
            engine._save()
        if recovering:
            engine.store.audit(at, "BROKER_RECONCILIATION_RECOVERED",
                               reason="Fresh orders, positions and cash agree with owned ledger.",
                               capital_and_order_state_preserved=True)
        engine.reconciliation_status = {
            "state": "healthy", "at": at.isoformat(), "last_success_at": at.isoformat(),
            "reason": "Broker orders, positions and cash reconciled.", "consecutive_failures": 0,
        }
        engine._check_risk(at)
        engine.position_feed.check(at)
        engine._drive(at)
        return True

    def due(self, monotonic: float, last_started: float, *, dirty: bool) -> bool:
        interval = 2 if dirty else self.engine.config.execution.reconcile_seconds
        return monotonic >= self.retry_not_before and monotonic - last_started >= interval
