"""Glue between the scraper, the state machine, suppression and the store.

Every round is evaluated against in-memory state first, then persisted in
one transaction.  If the persist fails, the in-memory state machine and the
suppression engine are rolled back to their pre-round checkpoints, so a
lost write never advances memory (which would silently drop events such as
a firing that was computed but never stored).
"""

from __future__ import annotations

import logging
import time

from .config import AppConfig, TargetConfig
from .scraper import ScrapeResult
from .state import (
    KIND_FIRING,
    KIND_RESOLVED,
    KIND_SUPPRESSED,
    KIND_UNSUPPRESSED,
    AlertStateMachine,
)
from .store import Store
from .suppress import SuppressionEngine

log = logging.getLogger(__name__)


class AlertEngine:
    def __init__(self, config: AppConfig, store: Store):
        self._state_machine = AlertStateMachine(config.rules)
        self._suppression = SuppressionEngine(config.rules)
        self._store = store

    def handle_round(self, target: TargetConfig, result: ScrapeResult) -> None:
        now_mono = time.monotonic()
        now_wall = time.time()
        sm_checkpoint = self._state_machine.checkpoint()
        supp_checkpoint = self._suppression.checkpoint()
        try:
            if result.ok:
                events = self._state_machine.process_round(
                    target.id, result.samples, now_mono
                )
            else:
                events = self._state_machine.process_failure(target.id, now_mono)
                log.warning(
                    "scrape failed for target %s: %s", target.id, result.error
                )
            # Suppression is recomputed globally after every round: a round
            # of one target can (un)suppress alerts of every other target.
            active = self._state_machine.snapshot(None, now_mono, now_wall)
            events = events + self._suppression.recompute(active)
            self._store.save_round(
                target.id,
                ok=result.ok,
                error=result.error,
                samples=result.samples if result.ok else None,
                events=events,
                active_alerts=active,
                duration_ms=result.duration_ms,
                now_mono=now_mono,
                now_wall=now_wall,
            )
        except Exception:
            self._state_machine.restore(sm_checkpoint)
            self._suppression.restore(supp_checkpoint)
            raise
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
                    "alert suppressed rule=%s labels=%s sources=%s",
                    event.rule.id,
                    event.labels,
                    (event.details or {}).get("sources"),
                )
            elif event.kind == KIND_UNSUPPRESSED:
                log.info(
                    "alert unsuppressed rule=%s labels=%s",
                    event.rule.id,
                    event.labels,
                )
