"""Alert state machine.

Alerts are identified by (rule id, complete label set); label ordering does
not change identity.  A series whose value is strictly greater than the rule
threshold enters ``pending``; it only becomes ``firing`` after the condition
has held continuously for the rule's duration on a monotonic clock (a zero
duration fires immediately).  Recovery, series disappearance and scrape
failures clear ``pending`` silently and resolve a ``firing`` alert exactly
once with a reason.  Re-exceeding the threshold restarts the timer from
scratch: pending time never accumulates across failed or healthy rounds.

Round evaluation is split into a pure planning step (``plan_round`` /
``plan_failure``) and ``commit``: the engine persists the planned events and
snapshot first and only commits the in-memory state when the write succeeds,
so a failed persistence never advances memory and never loses an event.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import RuleConfig
from .labels import sorted_items
from .parser import Sample

KIND_FIRING = "firing"
KIND_RESOLVED = "resolved"

REASON_RECOVERED = "recovered"
REASON_SERIES_MISSING = "series_missing"
REASON_SCRAPE_FAILED = "scrape_failed"
REASON_RESTART = "restart"

_STATE_PENDING = "pending"
_STATE_FIRING = "firing"


@dataclass
class AlertEvent:
    kind: str  # KIND_FIRING | KIND_RESOLVED | suppressed/unsuppressed (deps)
    rule: RuleConfig
    labels: dict[str, str]
    value: float | None
    reason: str | None


@dataclass
class _AlertState:
    rule: RuleConfig
    labels: dict[str, str]
    state: str  # _STATE_PENDING | _STATE_FIRING
    since: float  # monotonic timestamp of the start of the current streak
    value: float


@dataclass
class StatePlan:
    """Tentative result of evaluating one round: new states plus events."""

    states: dict[tuple[str, tuple[tuple[str, str], ...]], _AlertState]
    events: list[AlertEvent]


class AlertStateMachine:
    """In-memory, per-target alert evaluation driven by scrape rounds."""

    def __init__(self, rules: tuple[RuleConfig, ...] | list[RuleConfig]):
        self._rules_by_target: dict[str, list[RuleConfig]] = {}
        for rule in rules:
            self._rules_by_target.setdefault(rule.target_id, []).append(rule)
        self._states: dict[tuple[str, tuple[tuple[str, str], ...]], _AlertState] = {}

    def commit(self, plan: StatePlan) -> None:
        """Advance the in-memory state to a previously computed plan."""
        self._states = plan.states

    def plan_round(
        self, target_id: str, samples: list[Sample], now: float
    ) -> StatePlan:
        """Evaluate a successful scrape round without mutating the machine."""
        states = dict(self._states)
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
                state = states.get(key)
                if sample.value > rule.threshold:
                    if state is None:
                        if rule.duration_seconds <= 0:
                            states[key] = _AlertState(
                                rule, dict(sample.labels), _STATE_FIRING, now, sample.value
                            )
                            events.append(
                                AlertEvent(KIND_FIRING, rule, dict(sample.labels), sample.value, None)
                            )
                        else:
                            states[key] = _AlertState(
                                rule, dict(sample.labels), _STATE_PENDING, now, sample.value
                            )
                    elif state.state == _STATE_PENDING:
                        if now - state.since >= rule.duration_seconds:
                            states[key] = _AlertState(
                                rule, state.labels, _STATE_FIRING, state.since, sample.value
                            )
                            events.append(
                                AlertEvent(KIND_FIRING, rule, dict(sample.labels), sample.value, None)
                            )
                        else:
                            states[key] = _AlertState(
                                rule, state.labels, _STATE_PENDING, state.since, sample.value
                            )
                    else:
                        states[key] = _AlertState(
                            rule, state.labels, _STATE_FIRING, state.since, sample.value
                        )
                else:
                    if state is not None:
                        if state.state == _STATE_FIRING:
                            events.append(
                                AlertEvent(
                                    KIND_RESOLVED, rule, dict(sample.labels),
                                    sample.value, REASON_RECOVERED,
                                )
                            )
                        del states[key]
        # Series that vanished from the exposition clear/resolve their alerts.
        for key, state in list(states.items()):
            if state.rule.target_id != target_id or key in seen:
                continue
            if state.state == _STATE_FIRING:
                events.append(
                    AlertEvent(KIND_RESOLVED, state.rule, state.labels, None, REASON_SERIES_MISSING)
                )
            del states[key]
        return StatePlan(states, events)

    def plan_failure(self, target_id: str, now: float) -> StatePlan:
        """Evaluate a failed scrape round without mutating the machine."""
        del now  # failure handling does not depend on the clock
        states = dict(self._states)
        events: list[AlertEvent] = []
        for key, state in list(states.items()):
            if state.rule.target_id != target_id:
                continue
            if state.state == _STATE_FIRING:
                events.append(
                    AlertEvent(KIND_RESOLVED, state.rule, state.labels, None, REASON_SCRAPE_FAILED)
                )
            del states[key]
        return StatePlan(states, events)

    def process_round(
        self, target_id: str, samples: list[Sample], now: float
    ) -> list[AlertEvent]:
        """Evaluate a successful scrape round for one target."""
        plan = self.plan_round(target_id, samples, now)
        self.commit(plan)
        return plan.events

    def process_failure(self, target_id: str, now: float) -> list[AlertEvent]:
        """Evaluate a failed scrape round for one target."""
        plan = self.plan_failure(target_id, now)
        self.commit(plan)
        return plan.events

    def snapshot_all(
        self,
        now_mono: float,
        now_wall: float,
        states: dict | None = None,
    ) -> list[dict]:
        """Return the active alerts of every target as persistable dicts.

        ``states`` may be a tentative plan's state map so the snapshot can be
        persisted before the plan is committed.
        """
        states = self._states if states is None else states
        out = []
        for state in states.values():
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

    def snapshot(
        self, target_id: str, now_mono: float, now_wall: float
    ) -> list[dict]:
        """Return the active alerts of one target as persistable dicts."""
        out = []
        for state in self._states.values():
            if state.rule.target_id != target_id:
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
