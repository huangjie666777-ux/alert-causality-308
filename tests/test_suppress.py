from causewatch_308.config import DependencyConfig, RuleConfig
from causewatch_308.suppress import SuppressionEngine


def make_rule(rule_id, deps=(), target_id="t1", metric="m"):
    return RuleConfig(
        id=rule_id,
        target_id=target_id,
        metric=metric,
        labels={},
        threshold=1.0,
        duration_seconds=0.0,
        depends_on=tuple(
            DependencyConfig(rule_id=up, labels=tuple(labels)) for up, labels in deps
        ),
    )


def alert(rule_id, labels, state="firing", value=5.0, target_id="t1", metric="m"):
    return {
        "rule_id": rule_id,
        "target_id": target_id,
        "metric": metric,
        "labels": dict(labels),
        "state": state,
        "since_mono": 0.0,
        "since_wall": 1000.0,
        "value": value,
        "threshold": 1.0,
    }


def pair(deps=(("up", ["room"]),)):
    up = make_rule("up")
    down = make_rule("down", deps=deps)
    return SuppressionEngine([up, down])


def by_rule(active, rule_id):
    return next(a for a in active if a["rule_id"] == rule_id)


def test_upstream_firing_suppresses_linked_downstream():
    eng = pair()
    active = [alert("up", {"room": "a"}), alert("down", {"room": "a"}, value=9.0)]
    events = eng.recompute(active)
    assert [e.kind for e in events] == ["suppressed"]
    event = events[0]
    assert event.rule.id == "down"
    assert event.labels == {"room": "a"}
    assert event.value == 9.0
    assert event.reason is None
    assert event.details == {
        "sources": [
            {
                "rule_id": "up",
                "target_id": "t1",
                "metric": "m",
                "labels": {"room": "a"},
            }
        ]
    }
    down = by_rule(active, "down")
    assert down["suppressed"] is True
    assert [s["rule_id"] for s in down["suppressed_by"]] == ["up"]
    assert [r["rule_id"] for r in down["root_causes"]] == ["up"]
    up = by_rule(active, "up")
    assert up["suppressed"] is False
    assert up["suppressed_by"] == [] and up["root_causes"] == []


def test_pending_upstream_does_not_suppress():
    eng = pair()
    active = [alert("up", {"room": "a"}, state="pending"), alert("down", {"room": "a"})]
    assert eng.recompute(active) == []
    assert by_rule(active, "down")["suppressed"] is False


def test_pending_downstream_is_not_suppressed():
    eng = pair()
    active = [alert("up", {"room": "a"}), alert("down", {"room": "a"}, state="pending")]
    assert eng.recompute(active) == []
    assert by_rule(active, "down")["suppressed"] is False


def test_missing_correlation_label_never_matches():
    eng = pair()
    # label missing on the downstream instance
    active = [alert("up", {"room": "a"}), alert("down", {"zone": "1"})]
    assert eng.recompute(active) == []
    # label missing on the upstream instance
    active = [alert("up", {"zone": "1"}), alert("down", {"room": "a"})]
    assert eng.recompute(active) == []


def test_label_value_mismatch_does_not_link():
    eng = pair()
    active = [alert("up", {"room": "a"}), alert("down", {"room": "b"})]
    assert eng.recompute(active) == []
    assert by_rule(active, "down")["suppressed"] is False


def test_label_order_is_irrelevant():
    eng = pair(deps=(("up", ["room", "device"]),))
    active = [
        alert("up", {"room": "a", "device": "d1", "extra": "x"}),
        alert("down", {"device": "d1", "room": "a"}),
    ]
    events = eng.recompute(active)
    assert [e.kind for e in events] == ["suppressed"]


def test_extra_labels_do_not_affect_linking_but_define_identity():
    eng = pair()
    active = [
        alert("up", {"room": "a", "zone": "1"}),
        alert("up", {"room": "a", "zone": "2"}),
        alert("down", {"room": "a", "extra": "x"}),
    ]
    events = eng.recompute(active)
    assert [e.kind for e in events] == ["suppressed"]
    down = by_rule(active, "down")
    # both upstream instances are distinct identities and both are sources
    assert len(down["suppressed_by"]) == 2
    assert {tuple(s["labels"].items()) for s in down["suppressed_by"]} == {
        (("room", "a"), ("zone", "1")),
        (("room", "a"), ("zone", "2")),
    }
    # dropping one of the two sources keeps the instance suppressed
    events = eng.recompute(active[:1] + active[2:])
    assert events == []
    assert by_rule(active, "down")["suppressed"] is True


def test_sustained_suppression_emits_no_duplicate_events():
    eng = pair()
    eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})])
    assert eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})]) == []


def test_release_when_all_upstreams_gone():
    eng = pair()
    eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})])
    events = eng.recompute([alert("down", {"room": "a"}, value=7.0)])
    assert [e.kind for e in events] == ["unsuppressed"]
    assert events[0].rule.id == "down"
    assert events[0].value == 7.0
    assert events[0].details is None


def test_multiple_upstreams_release_only_when_all_gone():
    rules = [
        make_rule("up1"),
        make_rule("up2"),
        make_rule("down", deps=(("up1", ["room"]), ("up2", ["room"]))),
    ]
    eng = SuppressionEngine(rules)
    base = [
        alert("up1", {"room": "a"}),
        alert("up2", {"room": "a"}),
        alert("down", {"room": "a"}),
    ]
    events = eng.recompute([dict(a) for a in base])
    assert [e.kind for e in events] == ["suppressed"]
    # one upstream leaves: still suppressed, no event
    assert eng.recompute([alert("up2", {"room": "a"}), alert("down", {"room": "a"})]) == []
    # the last one leaves: released
    events = eng.recompute([alert("down", {"room": "a"})])
    assert [e.kind for e in events] == ["unsuppressed"]


def test_downstream_recovery_while_suppressed_is_silent():
    eng = pair()
    eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})])
    # downstream resolved (no longer active) while upstream still firing
    assert eng.recompute([alert("up", {"room": "a"})]) == []


def test_transitive_suppression_and_root_causes():
    rules = [
        make_rule("a"),
        make_rule("b", deps=(("a", ["room"]),)),
        make_rule("c", deps=(("b", ["room"]),)),
    ]
    eng = SuppressionEngine(rules)
    active = [
        alert("a", {"room": "x"}),
        alert("b", {"room": "x"}),
        alert("c", {"room": "x"}),
    ]
    events = eng.recompute(active)
    assert sorted(e.kind for e in events) == ["suppressed", "suppressed"]
    c = by_rule(active, "c")
    assert [s["rule_id"] for s in c["suppressed_by"]] == ["b"]
    assert [r["rule_id"] for r in c["root_causes"]] == ["a"]
    b = by_rule(active, "b")
    assert b["suppressed"] is True  # suppressed upstream keeps propagating
    # the root leaves: b is released, c stays suppressed with b as new root
    active = [alert("b", {"room": "x"}), alert("c", {"room": "x"})]
    events = eng.recompute(active)
    assert [(e.kind, e.rule.id) for e in events] == [("unsuppressed", "b")]
    c = by_rule(active, "c")
    assert c["suppressed"] is True
    assert [r["rule_id"] for r in c["root_causes"]] == ["b"]


def test_new_firing_downstream_is_suppressed_immediately():
    eng = pair()
    eng.recompute([alert("up", {"room": "a"})])
    events = eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})])
    assert [e.kind for e in events] == ["suppressed"]


def test_instances_of_other_rooms_are_independent():
    eng = pair()
    active = [
        alert("up", {"room": "a"}),
        alert("down", {"room": "a"}),
        alert("down", {"room": "b"}),
    ]
    eng.recompute(active)
    downs = [a for a in active if a["rule_id"] == "down"]
    assert {a["labels"]["room"]: a["suppressed"] for a in downs} == {
        "a": True,
        "b": False,
    }


def test_checkpoint_and_restore():
    eng = pair()
    eng.recompute([alert("up", {"room": "a"}), alert("down", {"room": "a"})])
    checkpoint = eng.checkpoint()
    events = eng.recompute([alert("down", {"room": "a"})])
    assert [e.kind for e in events] == ["unsuppressed"]
    eng.restore(checkpoint)
    # after rollback the release is computed again (not lost)
    events = eng.recompute([alert("down", {"room": "a"})])
    assert [e.kind for e in events] == ["unsuppressed"]
