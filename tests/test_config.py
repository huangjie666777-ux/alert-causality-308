import pytest

from causewatch_308.config import ConfigError, parse_config


def make_rule(rule_id, **overrides):
    rule = {
        "id": rule_id,
        "target_id": "t1",
        "metric": "temp",
        "labels": {},
        "threshold": 10,
        "duration_seconds": 0,
    }
    rule.update(overrides)
    return rule


def make_config(rules, targets=None):
    return {
        "port": 8080,
        "sqlite_path": "data/test.db",
        "targets": targets
        or [
            {
                "id": "t1",
                "url": "http://127.0.0.1:9101/metrics",
                "interval_seconds": 1,
                "timeout_seconds": 2,
                "max_response_bytes": 1048576,
            }
        ],
        "rules": rules,
    }


def test_no_dependencies_is_backward_compatible():
    config = parse_config(make_config([make_rule("r1")]))
    assert config.rules[0].depends_on == ()


def test_valid_dependency_parsed():
    config = parse_config(
        make_config(
            [
                make_rule("up"),
                make_rule(
                    "down",
                    depends_on=[{"rule_id": "up", "labels": ["room", "device"]}],
                ),
            ]
        )
    )
    deps = config.rules[1].depends_on
    assert len(deps) == 1
    assert deps[0].rule_id == "up"
    assert deps[0].labels == ("room", "device")


def test_cross_target_dependency_valid():
    targets = [
        {
            "id": "t1",
            "url": "http://127.0.0.1:9101/metrics",
            "interval_seconds": 1,
            "timeout_seconds": 2,
            "max_response_bytes": 1048576,
        },
        {
            "id": "t2",
            "url": "http://127.0.0.1:9102/metrics",
            "interval_seconds": 1,
            "timeout_seconds": 2,
            "max_response_bytes": 1048576,
        },
    ]
    rules = [
        make_rule("up", target_id="t1"),
        make_rule(
            "down",
            target_id="t2",
            depends_on=[{"rule_id": "up", "labels": ["room"]}],
        ),
    ]
    config = parse_config(make_config(rules, targets=targets))
    assert config.rules[1].depends_on[0].rule_id == "up"


def test_diamond_dependency_valid():
    rules = [
        make_rule("top"),
        make_rule("mid1", depends_on=[{"rule_id": "top", "labels": ["room"]}]),
        make_rule("mid2", depends_on=[{"rule_id": "top", "labels": ["room"]}]),
        make_rule(
            "bottom",
            depends_on=[
                {"rule_id": "mid1", "labels": ["room"]},
                {"rule_id": "mid2", "labels": ["room"]},
            ],
        ),
    ]
    config = parse_config(make_config(rules))
    assert len(config.rules[3].depends_on) == 2


@pytest.mark.parametrize(
    "depends_on, message",
    [
        ({"rule_id": "up"}, "must be an array"),
        (["up"], "must be an object"),
        ([{"rule_id": "up", "labels": ["room"], "extra": 1}], "unknown key"),
        ([{"labels": ["room"]}], "non-empty string"),
        ([{"rule_id": "", "labels": ["room"]}], "non-empty string"),
        ([{"rule_id": "up", "labels": []}], "non-empty array"),
        ([{"rule_id": "up", "labels": "room"}], "non-empty array"),
        ([{"rule_id": "up", "labels": ["9bad"]}], "invalid label name"),
        ([{"rule_id": "up", "labels": [1]}], "invalid label name"),
        ([{"rule_id": "up", "labels": ["room", "room"]}], "duplicate label"),
    ],
)
def test_malformed_dependency_rejected(depends_on, message):
    rules = [make_rule("up"), make_rule("down", depends_on=depends_on)]
    with pytest.raises(ConfigError, match=message):
        parse_config(make_config(rules))


def test_self_dependency_rejected():
    rules = [make_rule("r1", depends_on=[{"rule_id": "r1", "labels": ["room"]}])]
    with pytest.raises(ConfigError, match="cannot depend on itself"):
        parse_config(make_config(rules))


def test_unknown_rule_reference_rejected():
    rules = [make_rule("down", depends_on=[{"rule_id": "ghost", "labels": ["r"]}])]
    with pytest.raises(ConfigError, match="unknown rule 'ghost'"):
        parse_config(make_config(rules))


def test_duplicate_edge_rejected():
    rules = [
        make_rule("up"),
        make_rule(
            "down",
            depends_on=[
                {"rule_id": "up", "labels": ["room"]},
                {"rule_id": "up", "labels": ["device"]},
            ],
        ),
    ]
    with pytest.raises(ConfigError, match="duplicate dependency edge"):
        parse_config(make_config(rules))


def test_two_node_cycle_rejected():
    rules = [
        make_rule("a", depends_on=[{"rule_id": "b", "labels": ["room"]}]),
        make_rule("b", depends_on=[{"rule_id": "a", "labels": ["room"]}]),
    ]
    with pytest.raises(ConfigError, match="dependency cycle detected: a -> b -> a"):
        parse_config(make_config(rules))


def test_three_node_cycle_rejected():
    rules = [
        make_rule("a", depends_on=[{"rule_id": "c", "labels": ["room"]}]),
        make_rule("b", depends_on=[{"rule_id": "a", "labels": ["room"]}]),
        make_rule("c", depends_on=[{"rule_id": "b", "labels": ["room"]}]),
    ]
    with pytest.raises(ConfigError, match="dependency cycle detected"):
        parse_config(make_config(rules))
