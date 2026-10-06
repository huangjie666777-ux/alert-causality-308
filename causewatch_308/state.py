"""Alert state machine.

Alerts are identified by (rule id, complete label set); label ordering does
not change identity.  A series whose value is strictly greater than the rule
threshold enters ``pending``; it only becomes ``firing`` after the condition
has held continuously for the rule's duration on a monotonic clock (a zero
duration fires immediately).  Recovery, series disappearance and scrape
failures clear ``pending`` silently and resolve a ``firing`` alert exactly
once with a reason.  Re-exceeding the threshold restarts the timer from
scratch: pending time never accumulates across failed or healthy rounds.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import RuleConfig
from .labels import sorted_items
from .parser import Sample

KIND_FIRING = "firing"
KIND_RESOLVED = "resolved"
KIND_SUPPRESSED = "suppressed"
KIND_UNSUPPRESSED = "unsuppressed"

REASON_RECOVERED = "recovered"
REASON_SERIES_MISSING = "series_missing"
REASON_SCRAPE_FAILED = "scrape_failed"
REASON_RESTART = "restart"

_STATE_PENDING = "pending"
_STATE_FIRING = "firing"


@dataclass
class AlertEvent:
    kind: str  # KIND_FIRING | KIND_RESOLVED | KIND_SUPPRESSED | KIND_UNSUPPRESSED
    rule: RuleConfig
    labels: dict[str, str]
    value: float | None
    reason: str | None
    details: dict | None = None  # e.g. {"sources": [...]} on suppressed events


@dataclass
class _AlertState:
    rule: RuleConfig
    labels: dict[str, str]
    state: str  # _STATE_PENDING | _STATE_FIRING
    since: float  # monotonic timestamp of the start of the current streak
    value: float


class AlertStateMachine:
    """In-memory, per-target alert evaluation driven by scrape rounds."""

    def __init__(self, rules: tuple[RuleConfig, ...] | list[RuleConfig]):
        self._rules_by_target: dict[str, list[RuleConfig]] = {}
        for rule in rules:
            self._rules_by_target.setdefault(rule.target_id, []).append(rule)
        self._states: dict[tuple[str, tuple[tuple[str, str], ...]], _AlertState] = {}

    def checkpoint(self) -> dict:
        """Snapshot the in-memory states so a failed persist can roll back.

        The engine checkpoints before evaluating a round and restores the
        checkpoint when the database write fails, so a lost write never
        advances the state machine (which would silently drop events).
        """
        return {
            key: _AlertState(s.rule, dict(s.labels), s.state, s.since, s.value)
            for key, s in self._states.items()
        }

    def restore(self, checkpoint: dict) -> None:
        """Roll the in-memory states back to a previous checkpoint."""
        self._states = {
            key: _AlertState(s.rule, dict(s.labels), s.state, s.since, s.value)
            for key, s in checkpoint.items()
        }

    def process_round(
        self, target_id: str, samples: list[Sample], now: float
    ) -> list[AlertEvent]:
        """Evaluate a successful scrape round for one target."""
        events: list[AlertEvent] = []
        seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        for rule in self._rules_by_target.get(target_id, []):
            for sample in samples:
                if sample.metric != rule.metric:
                    continue
                if not all(
                    sample.labels.get(k) == v for k, v in rule.labels.items()
                ):
                    continue
                key = (rule.id, sorted_items(sample.labels))
                seen.add(key)
                state = self._states.get(key)
                if sample.value > rule.threshold:
                    if state is None:
                        if rule.duration_seconds <= 0:
                            self._states[key] = _AlertState(
                                rule, dict(sample.labels), _STATE_FIRING, now, sample.value
                            )
                            events.append(
                                AlertEvent(KIND_FIRING, rule, dict(sample.labels), sample.value, None)
                            )
                        else:
                            self._states[key] = _AlertState(
                                rule, dict(sample.labels), _STATE_PENDING, now, sample.value
                            )
                    elif state.state == _STATE_PENDING:
                        state.value = sample.value
                        if now - state.since >= rule.duration_seconds:
                            state.state = _STATE_FIRING
                            events.append(
                                AlertEvent(KIND_FIRING, rule, dict(sample.labels), sample.value, None)
                            )
                    else:
                        state.value = sample.value
                else:
                    if state is not None:
                        if state.state == _STATE_FIRING:
                            events.append(
                                AlertEvent(
                                    KIND_RESOLVED, rule, dict(sample.labels),
                                    sample.value, REASON_RECOVERED,
                                )
                            )
                        del self._states[key]
        # Series that vanished from the exposition clear/resolve their alerts.
        for key, state in list(self._states.items()):
            if state.rule.target_id != target_id or key in seen:
                continue
            if state.state == _STATE_FIRING:
                events.append(
                    AlertEvent(KIND_RESOLVED, state.rule, state.labels, None, REASON_SERIES_MISSING)
                )
            del self._states[key]
        return events

    def process_failure(self, target_id: str, now: float) -> list[AlertEvent]:
        """Evaluate a failed scrape round for one target."""
        del now  # failure handling does not depend on the clock
        events: list[AlertEvent] = []
        for key, state in list(self._states.items()):
            if state.rule.target_id != target_id:
                continue
            if state.state == _STATE_FIRING:
                events.append(
                    AlertEvent(KIND_RESOLVED, state.rule, state.labels, None, REASON_SCRAPE_FAILED)
                )
            del self._states[key]
        return events

    def snapshot(
        self, target_id: str | None, now_mono: float, now_wall: float
    ) -> list[dict]:
        """Return active alerts as persistable dicts (all targets if None)."""
        out = []
        for state in self._states.values():
            if target_id is not None and state.rule.target_id != target_id:
                continue
            out.append(
                {
                    "rule_id": state.rule.id,
                    "target_id": state.rule.target_id,
                    "metric": state.rule.metric,
                    "labels": state.labels,
                    "state": state.state,
                    "since_mono": state.since,
                    "since_wall": now_wall - (now_mono - state.since),
                    "value": state.value,
                    "threshold": state.rule.threshold,
                }
            )
        return out
