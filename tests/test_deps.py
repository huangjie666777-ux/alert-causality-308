from causewatch_308.config import DependencyConfig, RuleConfig
from causewatch_308.deps import KIND_SUPPRESSED, KIND_UNSUPPRESSED, SuppressionTracker


def make_rule(rule_id, target_id="t1", metric="m"):
    return RuleConfig(rule_id, target_id, metric, {}, 10.0, 0.0)


def instance(rule_id, labels, state="firing", value=42.0, target_id="t1", metric="m"):
    return {
        "rule_id": rule_id,
        "target_id": target_id,
        "metric": metric,
        "labels": labels,
        "state": state,
        "since_mono": 0.0,
        "since_wall": 1000.0,
        "value": value,
        "threshold": 10.0,
    }


def tracker(*deps, rules=("up", "mid", "down")):
    return SuppressionTracker(list(deps), [make_rule(r) for r in rules])


def dep(upstream, downstream, labels=("room",)):
    return DependencyConfig(upstream, downstream, tuple(labels))


def annotation(plan, rule_id, labels):
    from causewatch_308.labels import canonical_key

    return plan.annotations[(rule_id, canonical_key(labels))]


def test_matching_correlation_labels_link_instances():
    t = tracker(dep("up", "down"))
    up = instance("up", {"room": "a", "host": "h1"})
    down = instance("down", {"host": "h1", "room": "a"})  # label order irrelevant
    plan = t.compute([up, down])
    ann = annotation(plan, "down", {"host": "h1", "room": "a"})
    assert ann["suppressed"] is True
    assert [r["rule_id"] for r in ann["suppressed_by"]] == ["up"]
    assert ann["suppressed_by"][0]["labels"] == {"host": "h1", "room": "a"}
    assert [r["rule_id"] for r in ann["root_causes"]] == ["up"]
    assert [e.kind for e in plan.events] == [KIND_SUPPRESSED]
    assert plan.events[0].labels == {"host": "h1", "room": "a"}

    t.commit(plan)
    # steady state: no repeated events while nothing changes
    plan2 = t.compute([up, down])
    assert plan2.events == []


def test_missing_correlation_label_never_matches():
    t = tracker(dep("up", "down"))
    up = instance("up", {"room": "a"})
    down = instance("down", {"zone": "1"})  # no "room" label
    plan = t.compute([up, down])
    assert annotation(plan, "down", {"zone": "1"})["suppressed"] is False
    assert plan.events == []


def test_different_label_values_do_not_match():
    t = tracker(dep("up", "down"))
    plan = t.compute(
        [instance("up", {"room": "a"}), instance("down", {"room": "b"})]
    )
    assert annotation(plan, "down", {"room": "b"})["suppressed"] is False


def test_complete_label_set_distinguishes_identity():
    t = tracker(dep("up", "down"))
    up = instance("up", {"room": "a"})
    down_a = instance("down", {"room": "a", "host": "h1"})
    down_b = instance("down", {"room": "a", "host": "h2"})
    plan = t.compute([up, down_a, down_b])
    assert annotation(plan, "down", {"room": "a", "host": "h1"})["suppressed"] is True
    assert annotation(plan, "down", {"room": "a", "host": "h2"})["suppressed"] is True
    # one suppressed event per distinct instance
    assert len([e for e in plan.events if e.kind == KIND_SUPPRESSED]) == 2


def test_pending_upstream_cannot_suppress():
    t = tracker(dep("up", "down"))
    plan = t.compute(
        [
            instance("up", {"room": "a"}, state="pending"),
            instance("down", {"room": "a"}),
        ]
    )
    assert annotation(plan, "down", {"room": "a"})["suppressed"] is False
    assert plan.events == []


def test_pending_downstream_is_not_suppressed():
    t = tracker(dep("up", "down"))
    plan = t.compute(
        [
            instance("up", {"room": "a"}),
            instance("down", {"room": "a"}, state="pending"),
        ]
    )
    assert annotation(plan, "down", {"room": "a"})["suppressed"] is False
    assert plan.events == []


def test_suppression_propagates_through_suppressed_instances():
    t = tracker(dep("up", "mid"), dep("mid", "down"))
    up = instance("up", {"room": "a"})
    mid = instance("mid", {"room": "a"})
    down = instance("down", {"room": "a"})
    plan = t.compute([up, mid, down])
    assert annotation(plan, "mid", {"room": "a"})["suppressed"] is True
    # mid is itself suppressed but still propagates suppression downstream
    down_ann = annotation(plan, "down", {"room": "a"})
    assert down_ann["suppressed"] is True
    assert [r["rule_id"] for r in down_ann["suppressed_by"]] == ["mid"]
    # root cause traces back to the unsuppressed origin
    assert [r["rule_id"] for r in down_ann["root_causes"]] == ["up"]


def test_diamond_root_causes():
    t = tracker(
        dep("up", "mid"),
        dep("up", "down"),
        dep("mid", "down"),
    )
    up = instance("up", {"room": "a"})
    mid = instance("mid", {"room": "a"})
    down = instance("down", {"room": "a"})
    plan = t.compute([up, mid, down])
    down_ann = annotation(plan, "down", {"room": "a"})
    assert [r["rule_id"] for r in down_ann["suppressed_by"]] == ["mid", "up"]
    assert [r["rule_id"] for r in down_ann["root_causes"]] == ["up"]


def test_release_requires_all_upstreams_gone():
    t = tracker(dep("up", "down"), dep("mid", "down"))
    up = instance("up", {"room": "a"})
    mid = instance("mid", {"room": "a"})
    down = instance("down", {"room": "a"})
    t.commit(t.compute([up, mid, down]))

    # one upstream resolves: still suppressed, no release event
    plan = t.compute([mid, down])
    assert annotation(plan, "down", {"room": "a"})["suppressed"] is True
    assert [r["rule_id"] for r in annotation(plan, "down", {"room": "a"})["suppressed_by"]] == ["mid"]
    assert plan.events == []
    t.commit(plan)

    # last upstream resolves: released exactly once
    plan = t.compute([down])
    assert annotation(plan, "down", {"room": "a"})["suppressed"] is False
    assert [e.kind for e in plan.events] == [KIND_UNSUPPRESSED]
    t.commit(plan)
    assert t.compute([down]).events == []


def test_resolved_downstream_produces_no_release_event():
    t = tracker(dep("up", "down"))
    up = instance("up", {"room": "a"})
    down = instance("down", {"room": "a"})
    t.commit(t.compute([up, down]))
    # downstream recovers on its own while upstream still firing: no events
    plan = t.compute([up])
    assert plan.events == []


def test_refire_while_upstream_firing_suppresses_again():
    t = tracker(dep("up", "down"))
    up = instance("up", {"room": "a"})
    down = instance("down", {"room": "a"})
    t.commit(t.compute([up, down]))
    t.commit(t.compute([up]))  # downstream resolved silently
    plan = t.compute([up, down])  # downstream fires again
    assert [e.kind for e in plan.events] == [KIND_SUPPRESSED]


def test_no_dependencies_keeps_legacy_behaviour():
    t = tracker()
    plan = t.compute([instance("up", {"room": "a"}), instance("down", {"room": "a"})])
    assert plan.events == []
    assert all(not a["suppressed"] for a in plan.annotations.values())
