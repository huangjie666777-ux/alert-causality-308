"""Dependency-driven suppression of connected firing alerts.

Rule dependencies (upstream rule -> downstream rule plus a non-empty list of
correlation labels) are validated at startup; this module derives the
*instance*-level suppression after every round from the active alerts of all
targets:

* two firing instances are linked exactly when every correlation label
  exists with the same value on both sides -- a missing label never matches,
  label ordering is irrelevant and identity stays the complete label set;
* any firing upstream instance suppresses its linked downstream instances,
  and a suppressed instance keeps propagating suppression further
  downstream; ``pending`` instances never suppress and are never suppressed;
* suppression neither stops downstream timers nor removes the original
  firing/resolved events -- it is an overlay on top of the state machine.

The tracker only emits an event on a transition: the first time a still
firing instance becomes suppressed (``suppressed``) and the first time it is
released while still firing (``unsuppressed``).  With several upstreams the
release happens only once *all* of them are gone; a downstream that resolves
on its own produces no release event.  Like the state machine, the tracker
computes a plan that is committed only after successful persistence.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import DependencyConfig, RuleConfig
from .labels import canonical_key
from .state import AlertEvent

KIND_SUPPRESSED = "suppressed"
KIND_UNSUPPRESSED = "unsuppressed"

_FIRING = "firing"

_InstanceKey = tuple[str, str]  # (rule id, canonical key of the complete label set)


def _instance_key(instance: dict) -> _InstanceKey:
    return (instance["rule_id"], canonical_key(instance["labels"]))


def _ref(instance: dict) -> dict:
    """A persistable reference to an alert instance (rule/target/labels)."""
    return {
        "rule_id": instance["rule_id"],
        "target_id": instance["target_id"],
        "metric": instance["metric"],
        "labels": dict(sorted(instance["labels"].items())),
    }


def _labels_match(link_labels: tuple[str, ...], upstream: dict, downstream: dict) -> bool:
    """All correlation labels must exist and be equal on both instances."""
    return all(
        name in upstream and name in downstream and upstream[name] == downstream[name]
        for name in link_labels
    )


@dataclass
class SuppressionPlan:
    """Tentative suppression overlay for one round."""

    annotations: dict[_InstanceKey, dict]  # every active instance -> annotation
    suppressed: dict[_InstanceKey, tuple[_InstanceKey, ...]]  # new committed map
    events: list[AlertEvent]


class SuppressionTracker:
    """Tracks which firing instances are suppressed by upstream firings."""

    def __init__(
        self,
        dependencies: tuple[DependencyConfig, ...] | list[DependencyConfig],
        rules: tuple[RuleConfig, ...] | list[RuleConfig],
    ):
        self._rules_by_id = {r.id: r for r in rules}
        # downstream rule id -> [(upstream rule id, correlation labels)]
        self._links: dict[str, list[tuple[str, tuple[str, ...]]]] = {}
        for dep in dependencies:
            self._links.setdefault(dep.downstream, []).append(
                (dep.upstream, tuple(dep.labels))
            )
        # committed map: suppressed instance key -> sorted direct suppressors
        self._suppressed: dict[_InstanceKey, tuple[_InstanceKey, ...]] = {}

    def compute(self, instances: list[dict]) -> SuppressionPlan:
        """Derive the suppression overlay from all active alert instances."""
        firing: dict[_InstanceKey, dict] = {}
        for instance in instances:
            if instance["state"] == _FIRING:
                firing[_instance_key(instance)] = instance

        # Direct instance-level edges upstream -> downstream.
        edges: dict[_InstanceKey, set[_InstanceKey]] = {}
        for down_key, down in firing.items():
            for upstream_rule, link_labels in self._links.get(down["rule_id"], ()):
                for up_key, up in firing.items():
                    if up["rule_id"] != upstream_rule:
                        continue
                    if _labels_match(link_labels, up["labels"], down["labels"]):
                        edges.setdefault(down_key, set()).add(up_key)

        # Root causes: the unsuppressed firing ancestors along current edges.
        # The rule graph is acyclic, so the instance graph is acyclic too.
        roots_cache: dict[_InstanceKey, frozenset[_InstanceKey]] = {}

        def roots_of(key: _InstanceKey) -> frozenset[_InstanceKey]:
            cached = roots_cache.get(key)
            if cached is not None:
                return cached
            ups = edges.get(key)
            if not ups:
                roots_cache[key] = frozenset((key,))
                return roots_cache[key]
            found: set[_InstanceKey] = set()
            for up in ups:
                found |= roots_of(up)
            roots_cache[key] = frozenset(found)
            return roots_cache[key]

        annotations: dict[_InstanceKey, dict] = {}
        for instance in instances:
            key = _instance_key(instance)
            ups = edges.get(key)
            if ups:
                annotations[key] = {
                    "suppressed": True,
                    "suppressed_by": [_ref(firing[k]) for k in sorted(ups)],
                    "root_causes": [_ref(firing[k]) for k in sorted(roots_of(key))],
                }
            else:
                annotations[key] = {
                    "suppressed": False,
                    "suppressed_by": [],
                    "root_causes": [],
                }

        new_suppressed = {key: tuple(sorted(ups)) for key, ups in edges.items()}
        events: list[AlertEvent] = []
        for key in sorted(new_suppressed):
            if key not in self._suppressed:
                instance = firing[key]
                events.append(
                    AlertEvent(
                        KIND_SUPPRESSED,
                        self._rules_by_id[instance["rule_id"]],
                        dict(instance["labels"]),
                        instance["value"],
                        None,
                    )
                )
        for key in sorted(self._suppressed):
            if key not in new_suppressed and key in firing:
                # Released while still firing; a resolved downstream produces
                # no release event.
                instance = firing[key]
                events.append(
                    AlertEvent(
                        KIND_UNSUPPRESSED,
                        self._rules_by_id[instance["rule_id"]],
                        dict(instance["labels"]),
                        instance["value"],
                        None,
                    )
                )
        return SuppressionPlan(annotations, new_suppressed, events)

    def commit(self, plan: SuppressionPlan) -> None:
        """Advance the committed suppression map to a computed plan."""
        self._suppressed = plan.suppressed
