"""Glue between the scraper, the alert state machine and the store."""

from __future__ import annotations

import logging
import time

from .config import AppConfig, TargetConfig
from .scraper import ScrapeResult
from .state import KIND_FIRING, AlertStateMachine
from .store import Store

log = logging.getLogger(__name__)


class AlertEngine:
    def __init__(self, config: AppConfig, store: Store):
        self._state_machine = AlertStateMachine(config.rules)
        self._store = store

    def handle_round(self, target: TargetConfig, result: ScrapeResult) -> None:
        now_mono = time.monotonic()
        now_wall = time.time()
        if result.ok:
            events = self._state_machine.process_round(
                target.id, result.samples, now_mono
            )
        else:
            events = self._state_machine.process_failure(target.id, now_mono)
            log.warning("scrape failed for target %s: %s", target.id, result.error)
        snapshot = self._state_machine.snapshot(target.id, now_mono, now_wall)
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
        for event in events:
            if event.kind == KIND_FIRING:
                log.info(
                    "ALERT FIRING rule=%s labels=%s value=%s threshold=%s",
                    event.rule.id,
                    event.labels,
                    event.value,
                    event.rule.threshold,
                )
            else:
                log.info(
                    "alert resolved rule=%s labels=%s reason=%s",
                    event.rule.id,
                    event.labels,
                    event.reason,
                )
