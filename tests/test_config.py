import pytest

from causewatch_308.config import ConfigError, parse_config


def base_config(**overrides):
    data = {
        "port": 8080,
        "sqlite_path": "x.db",
        "targets": [
            {
                "id": "t1",
                "url": "http://127.0.0.1:9101/metrics",
                "interval_seconds": 1,
                "timeout_seconds": 2,
                "max_response_bytes": 1024,
            }
        ],
        "rules": [
            {
                "id": "up",
                "target_id": "t1",
                "metric": "m1",
                "labels": {},
                "threshold": 1,
                "duration_seconds": 0,
            },
            {
                "id": "mid",
                "target_id": "t1",
                "metric": "m2",
                "labels": {},
                "threshold": 1,
                "duration_seconds": 0,
            },
            {
                "id": "down",
                "target_id": "t1",
                "metric": "m3",
                "labels": {},
                "threshold": 1,
                "duration_seconds": 0,
            },
        ],
    }
    data.update(overrides)
    return data


def dep(upstream="up", downstream="down", labels=("room",)):
    return {"upstream": upstream, "downstream": downstream, "labels": list(labels)}


def test_no_dependencies_is_backward_compatible():
    config = parse_config(base_config())
    assert config.dependencies == ()


def test_valid_dependencies():
    config = parse_config(
        base_config(dependencies=[dep(), dep("mid", "down", ("room", "zone"))])
    )
    assert len(config.dependencies) == 2
    first = config.dependencies[0]
    assert (first.upstream, first.downstream, first.labels) == ("up", "down", ("room",))
    assert config.rules_by_id["down"].metric == "m3"


def test_unknown_upstream_rejected():
    with pytest.raises(ConfigError, match="references unknown rule 'ghost'"):
        parse_config(base_config(dependencies=[dep("ghost", "down")]))


def test_unknown_downstream_rejected():
    with pytest.raises(ConfigError, match="references unknown rule 'ghost'"):
        parse_config(base_config(dependencies=[dep("up", "ghost")]))


def test_self_dependency_rejected():
    with pytest.raises(ConfigError, match="cannot depend on itself"):
        parse_config(base_config(dependencies=[dep("up", "up")]))


def test_duplicate_edge_rejected_even_with_different_labels():
    with pytest.raises(ConfigError, match="duplicate edge"):
        parse_config(
            base_config(dependencies=[dep(), dep("up", "down", ("zone",))])
        )


def test_direct_cycle_rejected():
    with pytest.raises(ConfigError, match="cycle"):
        parse_config(
            base_config(dependencies=[dep("up", "down"), dep("down", "up")])
        )


def test_indirect_cycle_rejected():
    with pytest.raises(ConfigError, match="cycle"):
        parse_config(
            base_config(
                dependencies=[
                    dep("up", "mid"),
                    dep("mid", "down"),
                    dep("down", "up"),
                ]
            )
        )


def test_diamond_without_cycle_is_allowed():
    config = parse_config(
        base_config(
            dependencies=[
                dep("up", "mid"),
                dep("up", "down"),
                dep("mid", "down"),
            ]
        )
    )
    assert len(config.dependencies) == 3


def test_empty_labels_rejected():
    with pytest.raises(ConfigError, match="non-empty array"):
        parse_config(base_config(dependencies=[dep(labels=[])]))


def test_labels_must_be_an_array():
    with pytest.raises(ConfigError, match="non-empty array"):
        parse_config(
            base_config(
                dependencies=[{"upstream": "up", "downstream": "down", "labels": "room"}]
            )
        )


def test_invalid_label_name_rejected():
    with pytest.raises(ConfigError, match="invalid label name"):
        parse_config(base_config(dependencies=[dep(labels=["1bad"])]))


def test_duplicate_label_name_rejected():
    with pytest.raises(ConfigError, match="duplicate label name"):
        parse_config(base_config(dependencies=[dep(labels=["room", "room"])]))


def test_unknown_dependency_key_rejected():
    bad = dep()
    bad["since"] = 1
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(base_config(dependencies=[bad]))


def test_dependencies_must_be_an_array():
    with pytest.raises(ConfigError, match="must be an array"):
        parse_config(base_config(dependencies={"upstream": "up"}))
