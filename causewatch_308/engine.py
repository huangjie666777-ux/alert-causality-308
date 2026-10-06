"""Glue between the scraper, the alert state machine, suppression and store.

Every round is planned in memory first (state machine plan plus suppression
overlay derived from the active alerts of all targets), then persisted in a
single transaction; the in-memory state is committed only after the write
succeeds.  A failed persistence therefore never advances memory and never
loses a firing/suppression event -- the next round simply plans them again.
"""

from __future__ import annotations

import logging
import time

from .config import AppConfig, TargetConfig
from .deps import KIND_SUPPRESSED, KIND_UNSUPPRESSED, SuppressionTracker
from .labels import canonical_key
from .scraper import ScrapeResult
from .state import KIND_FIRING, KIND_RESOLVED, AlertStateMachine
from .store import Store

log = logging.getLogger(__name__)


class AlertEngine:
    def __init__(self, config: AppConfig, store: Store):
        self._state_machine = AlertStateMachine(config.rules)
        self._suppression = SuppressionTracker(config.dependencies, config.rules)
        self._store = store

    def handle_round(self, target: TargetConfig, result: ScrapeResult) -> None:
        now_mono = time.monotonic()
        now_wall = time.time()
        if result.ok:
            plan = self._state_machine.plan_round(
                target.id, result.samples, now_mono
            )
        else:
            plan = self._state_machine.plan_failure(target.id, now_mono)
            log.warning("scrape failed for target %s: %s", target.id, result.error)
        # Recompute suppression from the active alerts of every target.
        snapshot = self._state_machine.snapshot_all(
            now_mono, now_wall, states=plan.states
        )
        suppression = self._suppression.compute(snapshot)
        for alert in snapshot:
            alert.update(
                suppression.annotations[
                    (alert["rule_id"], canonical_key(alert["labels"]))
                ]
            )
        events = [*plan.events, *suppression.events]
        self._store.save_round(
            target.id,
            ok=result.ok,
            error=result.error,
            samples=result.samples if result.ok else None,
            events=events,
            active_alerts=snapshot,
            duration_ms=result.duration_ms,
            now_mono=now_mono,
            now_wall=now_wall,
        )
        # Persistence succeeded: only now advance the in-memory state.
        self._state_machine.commit(plan)
        self._suppression.commit(suppression)
        for event in events:
            if event.kind == KIND_FIRING:
                log.info(
                    "ALERT FIRING rule=%s labels=%s value=%s threshold=%s",
                    event.rule.id,
                    event.labels,
                    event.value,
                    event.rule.threshold,
                )
            elif event.kind == KIND_RESOLVED:
                log.info(
                    "alert resolved rule=%s labels=%s reason=%s",
                    event.rule.id,
                    event.labels,
                    event.reason,
                )
            elif event.kind == KIND_SUPPRESSED:
                log.info(
                    "alert suppressed rule=%s labels=%s",
                    event.rule.id,
                    event.labels,
                )
            elif event.kind == KIND_UNSUPPRESSED:
                log.info(
                    "alert unsuppressed rule=%s labels=%s",
                    event.rule.id,
                    event.labels,
                )
