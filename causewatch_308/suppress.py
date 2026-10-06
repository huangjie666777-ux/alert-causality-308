"""Dependency-based suppression of firing alerts and root-cause tracing.

Rules may declare upstream dependencies (see :mod:`causewatch_308.config`).
After every scrape round the suppression engine recomputes the suppression
state from the *global* snapshot of active alerts (all targets):

* two firing instances are linked when every correlation label of the
  dependency exists and is equal on both instances -- a missing label never
  matches, label order is irrelevant and the complete label set still
  defines instance identity;
* only ``firing`` instances suppress or carry a suppression state;
  ``pending`` instances never suppress and are never reported suppressed;
* a suppressed upstream keeps suppressing its own downstreams, so
  suppression propagates transitively along the instance graph;
* a firing instance stays suppressed until *all* of its upstream sources
  are gone; its own recovery never emits an "unsuppressed" event.

The first transition into and out of suppression of a still-firing instance
appends exactly one ``suppressed`` / ``unsuppressed`` event; steady state
emits nothing.  Suppression never touches the state machine: downstream
timers keep running and the original firing/resolved events are preserved.
"""

from __future__ import annotations

import copy

from .config import RuleConfig
from .labels import sorted_items
from .state import KIND_SUPPRESSED, KIND_UNSUPPRESSED, AlertEvent

# Instance identity: rule id + complete label set (order-independent).
_InstanceKey = tuple[str, tuple[tuple[str, str], ...]]


def _key(rule_id: str, labels: dict[str, str]) -> _InstanceKey:
    return (rule_id, sorted_items(labels))


def _linked(
    down_labels: dict[str, str], up_labels: dict[str, str], correlation: tuple[str, ...]
) -> bool:
    """Two instances link iff every correlation label exists and is equal."""
    return all(
        name in down_labels
        and name in up_labels
        and down_labels[name] == up_labels[name]
        for name in correlation
    )


def _ref(alert: dict) -> dict:
    """Public reference to one alert instance (rule, target, full labels)."""
    return {
        "rule_id": alert["rule_id"],
        "target_id": alert["target_id"],
        "metric": alert["metric"],
        "labels": dict(alert["labels"]),
    }


class SuppressionEngine:
    """Tracks which firing instances are currently suppressed.

    The engine is in-memory only; the store mirrors its output every round
    and a restart clears it together with the leftover alert states.
    """

    def __init__(self, rules: list[RuleConfig] | tuple[RuleConfig, ...]):
        self._rules_by_id = {r.id: r for r in rules}
        self._deps_by_rule = {r.id: r.depends_on for r in rules if r.depends_on}
        # key -> {"sources": [instance refs], "root_causes": [instance refs]}
        self._suppressed: dict[_InstanceKey, dict] = {}

    def checkpoint(self) -> dict:
        """Snapshot the suppression map for rollback on persist failure."""
        return copy.deepcopy(self._suppressed)

    def restore(self, checkpoint: dict) -> None:
        """Roll the suppression map back to a previous checkpoint."""
        self._suppressed = copy.deepcopy(checkpoint)

    def recompute(self, active: list[dict]) -> list[AlertEvent]:
        """Recompute suppression from the global active-alert snapshot.

        Annotates every dict in ``active`` with ``suppressed`` (bool),
        ``suppressed_by`` (direct upstream sources) and ``root_causes``
        (unsuppressed ancestors reached along current instance links), and
        returns the transition events of this round.
        """
        firing = [a for a in active if a["state"] == "firing"]
        inst_by_key = {_key(a["rule_id"], a["labels"]): a for a in firing}
        by_rule: dict[str, list[dict]] = {}
        for a in firing:
            by_rule.setdefault(a["rule_id"], []).append(a)

        # Instance graph: downstream key -> sorted upstream instance keys.
        sources: dict[_InstanceKey, list[_InstanceKey]] = {}
        for a in firing:
            deps = self._deps_by_rule.get(a["rule_id"])
            if not deps:
                continue
            matched: set[_InstanceKey] = set()
            for dep in deps:
                for up in by_rule.get(dep.rule_id, ()):
                    if _linked(a["labels"], up["labels"], dep.labels):
                        matched.add(_key(up["rule_id"], up["labels"]))
            if matched:
                sources[_key(a["rule_id"], a["labels"])] = sorted(matched)

        new_suppressed: dict[_InstanceKey, dict] = {}
        for key, src_keys in sources.items():
            new_suppressed[key] = {
                "sources": [_ref(inst_by_key[k]) for k in src_keys],
                "root_causes": [
                    _ref(inst_by_key[k]) for k in self._roots(key, sources)
                ],
            }

        events: list[AlertEvent] = []
        for key in sorted(new_suppressed):
            if key not in self._suppressed:
                events.append(
                    self._event(
                        KIND_SUPPRESSED,
                        inst_by_key[key],
                        {"sources": new_suppressed[key]["sources"]},
                    )
                )
        for key in sorted(self._suppressed):
            # Released only when every upstream source is gone *and* the
            # instance is still firing; a resolved instance leaves silently.
            if key not in new_suppressed and key in inst_by_key:
                events.append(self._event(KIND_UNSUPPRESSED, inst_by_key[key], None))
        self._suppressed = new_suppressed

        for a in active:
            info = (
                new_suppressed.get(_key(a["rule_id"], a["labels"]))
                if a["state"] == "firing"
                else None
            )
            a["suppressed"] = info is not None
            a["suppressed_by"] = list(info["sources"]) if info else []
            a["root_causes"] = list(info["root_causes"]) if info else []
        return events

    def _event(self, kind: str, alert: dict, details: dict | None) -> AlertEvent:
        return AlertEvent(
            kind,
            self._rules_by_id[alert["rule_id"]],
            dict(alert["labels"]),
            alert["value"],
            None,
            details,
        )

    @staticmethod
    def _roots(
        start: _InstanceKey, sources: dict[_InstanceKey, list[_InstanceKey]]
    ) -> list[_InstanceKey]:
        """Unsuppressed ancestors reachable from ``start`` along the links."""
        roots: set[_InstanceKey] = set()
        seen: set[_InstanceKey] = set()
        stack = [start]
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            srcs = sources.get(key)
            if srcs:
                stack.extend(srcs)
            elif key != start:
                roots.add(key)
        return sorted(roots)
