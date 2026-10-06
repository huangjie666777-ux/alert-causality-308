from causewatch_308.config import RuleConfig
from causewatch_308.parser import Sample
from causewatch_308.state import AlertStateMachine


def make_rule(**overrides):
    params = dict(
        id="r1",
        target_id="t1",
        metric="temp",
        labels={"room": "a"},
        threshold=10.0,
        duration_seconds=5.0,
    )
    params.update(overrides)
    return RuleConfig(**params)


def sample(value, labels=None, metric="temp"):
    return Sample(metric, labels if labels is not None else {"room": "a"}, value)


def kinds(events):
    return [e.kind for e in events]


def test_below_threshold_no_alert():
    sm = AlertStateMachine([make_rule()])
    assert sm.process_round("t1", [sample(5.0)], now=0.0) == []
    assert sm.snapshot("t1", 0.0, 1000.0) == []


def test_pending_then_firing_after_duration():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    assert sm.process_round("t1", [sample(20.0)], now=0.0) == []
    assert sm.process_round("t1", [sample(20.0)], now=4.9) == []
    events = sm.process_round("t1", [sample(20.0)], now=5.0)
    assert kinds(events) == ["firing"]
    assert events[0].value == 20.0
    # sustained anomaly does not re-trigger
    assert sm.process_round("t1", [sample(30.0)], now=6.0) == []
    assert sm.process_round("t1", [sample(40.0)], now=100.0) == []


def test_zero_duration_fires_immediately():
    sm = AlertStateMachine([make_rule(duration_seconds=0.0)])
    events = sm.process_round("t1", [sample(20.0)], now=0.0)
    assert kinds(events) == ["firing"]


def test_recovery_clears_pending_silently():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    assert sm.process_round("t1", [sample(1.0)], now=1.0) == []
    assert sm.snapshot("t1", 2.0, 1000.0) == []


def test_recovery_resolves_firing_once():
    sm = AlertStateMachine([make_rule(duration_seconds=0.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    events = sm.process_round("t1", [sample(1.0)], now=1.0)
    assert kinds(events) == ["resolved"]
    assert events[0].reason == "recovered"
    assert sm.process_round("t1", [sample(1.0)], now=2.0) == []


def test_series_missing_clears_pending():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    assert sm.process_round("t1", [], now=1.0) == []
    assert sm.snapshot("t1", 2.0, 1000.0) == []


def test_series_missing_resolves_firing():
    sm = AlertStateMachine([make_rule(duration_seconds=0.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    events = sm.process_round("t1", [], now=1.0)
    assert kinds(events) == ["resolved"]
    assert events[0].reason == "series_missing"


def test_scrape_failure_clears_pending():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    assert sm.process_failure("t1", now=1.0) == []
    assert sm.snapshot("t1", 2.0, 1000.0) == []


def test_scrape_failure_resolves_firing():
    sm = AlertStateMachine([make_rule(duration_seconds=0.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)
    events = sm.process_failure("t1", now=1.0)
    assert kinds(events) == ["resolved"]
    assert events[0].reason == "scrape_failed"


def test_pending_does_not_accumulate_across_failures():
    sm = AlertStateMachine([make_rule(duration_seconds=10.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)   # pending since t=0
    sm.process_failure("t1", now=5.0)                  # pending cleared
    sm.process_round("t1", [sample(20.0)], now=7.0)   # pending since t=7
    assert sm.process_round("t1", [sample(20.0)], now=16.9) == []
    events = sm.process_round("t1", [sample(20.0)], now=17.0)
    assert kinds(events) == ["firing"]


def test_reexceed_restarts_timer():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    sm.process_round("t1", [sample(20.0)], now=0.0)   # pending since t=0
    sm.process_round("t1", [sample(1.0)], now=4.0)    # recovered, cleared
    sm.process_round("t1", [sample(20.0)], now=6.0)   # pending since t=6
    assert sm.process_round("t1", [sample(20.0)], now=10.9) == []
    events = sm.process_round("t1", [sample(20.0)], now=11.0)
    assert kinds(events) == ["firing"]


def test_label_order_does_not_change_identity():
    sm = AlertStateMachine([make_rule(duration_seconds=5.0)])
    sm.process_round("t1", [Sample("temp", {"room": "a", "zone": "1"}, 20.0)], now=0.0)
    events = sm.process_round(
        "t1", [Sample("temp", {"zone": "1", "room": "a"}, 20.0)], now=5.0
    )
    assert kinds(events) == ["firing"]
    assert events[0].labels == {"room": "a", "zone": "1"}


def test_distinct_label_sets_are_independent_alerts():
    sm = AlertStateMachine([make_rule(labels={}, duration_seconds=0.0)])
    events = sm.process_round(
        "t1",
        [Sample("temp", {"room": "a"}, 20.0), Sample("temp", {"room": "b"}, 30.0)],
        now=0.0,
    )
    assert kinds(events) == ["firing", "firing"]
    # only one series recovers
    events = sm.process_round(
        "t1",
        [Sample("temp", {"room": "a"}, 1.0), Sample("temp", {"room": "b"}, 30.0)],
        now=1.0,
    )
    assert kinds(events) == ["resolved"]
    assert events[0].labels == {"room": "a"}
    active = sm.snapshot("t1", 1.0, 1000.0)
    assert len(active) == 1
    assert active[0]["labels"] == {"room": "b"}


def test_rule_label_filter_must_match():
    sm = AlertStateMachine([make_rule(labels={"room": "a"}, duration_seconds=0.0)])
    # series exists but with a different room label: no alert
    assert sm.process_round("t1", [Sample("temp", {"room": "b"}, 99.0)], now=0.0) == []


def test_targets_are_isolated():
    rule_a = make_rule(id="ra", target_id="ta", duration_seconds=0.0)
    rule_b = make_rule(id="rb", target_id="tb", duration_seconds=0.0)
    sm = AlertStateMachine([rule_a, rule_b])
    sm.process_round("ta", [sample(20.0)], now=0.0)
    # failure of tb must not touch ta's firing alert
    assert sm.process_failure("tb", now=1.0) == []
    assert len(sm.snapshot("ta", 1.0, 1000.0)) == 1
