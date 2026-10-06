"""JSON configuration loading and startup validation.

The configuration file describes the local listen address, the SQLite path,
the scrape targets and the alert rules.  Everything is validated eagerly at
startup so a misconfigured process fails fast with a clear message instead of
misbehaving at runtime.  There is intentionally no PromQL, no notification
and no frontend configuration.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


class ConfigError(ValueError):
    """Raised when the configuration file is invalid."""


_METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_TOP_KEYS = {"host", "port", "sqlite_path", "targets", "rules"}
_TARGET_KEYS = {"id", "url", "interval_seconds", "timeout_seconds", "max_response_bytes"}
_RULE_KEYS = {
    "id", "target_id", "metric", "labels", "threshold", "duration_seconds",
    "depends_on",
}
_DEPENDENCY_KEYS = {"rule_id", "labels"}


@dataclass(frozen=True)
class DependencyConfig:
    """One upstream rule this rule depends on, plus the correlation labels.

    Two firing instances are linked only when every correlation label is
    present and equal on both instances; the label list must be non-empty.
    """

    rule_id: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class TargetConfig:
    id: str
    url: str
    interval_seconds: float
    timeout_seconds: float
    max_response_bytes: int


@dataclass(frozen=True)
class RuleConfig:
    id: str
    target_id: str
    metric: str
    labels: dict[str, str]
    threshold: float
    duration_seconds: float
    depends_on: tuple[DependencyConfig, ...] = ()


@dataclass(frozen=True)
class AppConfig:
    host: str
    port: int
    sqlite_path: Path
    targets: tuple[TargetConfig, ...]
    rules: tuple[RuleConfig, ...]

    @property
    def targets_by_id(self) -> dict[str, TargetConfig]:
        return {t.id: t for t in self.targets}


def _err(path: str, msg: str) -> ConfigError:
    return ConfigError(f"{path}: {msg}")


def _check_unknown_keys(obj: dict, allowed: set[str], path: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise _err(path, f"unknown key(s): {', '.join(unknown)}")


def _require_str(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _err(path, "must be a non-empty string")
    return value


def _require_number(value: object, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _err(path, "must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise _err(path, "must be a finite number")
    return result


def _parse_target(raw: object, path: str) -> TargetConfig:
    if not isinstance(raw, dict):
        raise _err(path, "must be an object")
    _check_unknown_keys(raw, _TARGET_KEYS, path)
    target_id = _require_str(raw.get("id"), f"{path}.id")
    url = _require_str(raw.get("url"), f"{path}.url")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise _err(f"{path}.url", "must be an absolute http(s) URL")
    interval = _require_number(raw.get("interval_seconds"), f"{path}.interval_seconds")
    if interval <= 0:
        raise _err(f"{path}.interval_seconds", "must be > 0")
    timeout = _require_number(raw.get("timeout_seconds"), f"{path}.timeout_seconds")
    if timeout <= 0:
        raise _err(f"{path}.timeout_seconds", "must be > 0")
    limit = raw.get("max_response_bytes")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise _err(f"{path}.max_response_bytes", "must be a positive integer")
    return TargetConfig(
        id=target_id,
        url=url,
        interval_seconds=interval,
        timeout_seconds=timeout,
        max_response_bytes=limit,
    )


def _parse_dependency(raw: object, path: str, own_rule_id: str) -> DependencyConfig:
    if not isinstance(raw, dict):
        raise _err(path, "must be an object")
    _check_unknown_keys(raw, _DEPENDENCY_KEYS, path)
    rule_id = _require_str(raw.get("rule_id"), f"{path}.rule_id")
    if rule_id == own_rule_id:
        raise _err(f"{path}.rule_id", "a rule cannot depend on itself")
    labels = raw.get("labels")
    if not isinstance(labels, list) or not labels:
        raise _err(f"{path}.labels", "must be a non-empty array of label names")
    clean: list[str] = []
    for i, name in enumerate(labels):
        if not isinstance(name, str) or not _LABEL_NAME_RE.match(name):
            raise _err(f"{path}.labels[{i}]", f"invalid label name {name!r}")
        if name in clean:
            raise _err(f"{path}.labels", f"duplicate label name {name!r}")
        clean.append(name)
    return DependencyConfig(rule_id=rule_id, labels=tuple(clean))


def _parse_rule(raw: object, path: str, target_ids: set[str]) -> RuleConfig:
    if not isinstance(raw, dict):
        raise _err(path, "must be an object")
    _check_unknown_keys(raw, _RULE_KEYS, path)
    rule_id = _require_str(raw.get("id"), f"{path}.id")
    target_id = _require_str(raw.get("target_id"), f"{path}.target_id")
    if target_id not in target_ids:
        raise _err(f"{path}.target_id", f"references unknown target {target_id!r}")
    metric = _require_str(raw.get("metric"), f"{path}.metric")
    if not _METRIC_NAME_RE.match(metric):
        raise _err(f"{path}.metric", f"invalid metric name {metric!r}")
    labels = raw.get("labels", {})
    if not isinstance(labels, dict):
        raise _err(f"{path}.labels", "must be an object of string equality filters")
    clean_labels: dict[str, str] = {}
    for key, value in labels.items():
        if not isinstance(key, str) or not _LABEL_NAME_RE.match(key):
            raise _err(f"{path}.labels", f"invalid label name {key!r}")
        if not isinstance(value, str):
            raise _err(f"{path}.labels.{key}", "label filter values must be strings")
        clean_labels[key] = value
    threshold = _require_number(raw.get("threshold"), f"{path}.threshold")
    duration = _require_number(raw.get("duration_seconds"), f"{path}.duration_seconds")
    if duration < 0:
        raise _err(f"{path}.duration_seconds", "must be >= 0")
    raw_deps = raw.get("depends_on", [])
    if not isinstance(raw_deps, list):
        raise _err(f"{path}.depends_on", "must be an array")
    deps = tuple(
        _parse_dependency(d, f"{path}.depends_on[{i}]", rule_id)
        for i, d in enumerate(raw_deps)
    )
    dep_rule_ids = [d.rule_id for d in deps]
    if len(set(dep_rule_ids)) != len(dep_rule_ids):
        raise _err(f"{path}.depends_on", "duplicate dependency edge(s)")
    return RuleConfig(
        id=rule_id,
        target_id=target_id,
        metric=metric,
        labels=clean_labels,
        threshold=threshold,
        duration_seconds=duration,
        depends_on=deps,
    )


def _check_dependency_graph(rules: tuple[RuleConfig, ...]) -> None:
    """Validate cross-rule references and reject dependency cycles."""
    rule_ids = {r.id for r in rules}
    for rule in rules:
        for dep in rule.depends_on:
            if dep.rule_id not in rule_ids:
                raise _err(
                    "rules",
                    f"rule {rule.id!r} depends on unknown rule {dep.rule_id!r}",
                )
    graph = {r.id: [d.rule_id for d in r.depends_on] for r in rules}
    state: dict[str, int] = {}  # 1 = on the current DFS path, 2 = fully explored
    stack: list[str] = []

    def visit(node: str) -> None:
        state[node] = 1
        stack.append(node)
        for nxt in graph[node]:
            if state.get(nxt) == 1:
                cycle = " -> ".join(stack[stack.index(nxt):] + [nxt])
                raise _err("rules", f"dependency cycle detected: {cycle}")
            if state.get(nxt) != 2:
                visit(nxt)
        stack.pop()
        state[node] = 2

    for rule_id in graph:
        if state.get(rule_id) != 2:
            visit(rule_id)


def parse_config(data: object) -> AppConfig:
    """Validate a decoded JSON document and return the typed configuration."""
    if not isinstance(data, dict):
        raise ConfigError("top level: must be a JSON object")
    _check_unknown_keys(data, _TOP_KEYS, "top level")

    host = data.get("host", "0.0.0.0")
    if not isinstance(host, str) or not host:
        raise _err("host", "must be a non-empty string")

    port = data.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        raise _err("port", "must be an integer between 1 and 65535")

    sqlite_path = _require_str(data.get("sqlite_path"), "sqlite_path")

    raw_targets = data.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise _err("targets", "must be a non-empty array")
    targets = tuple(_parse_target(t, f"targets[{i}]") for i, t in enumerate(raw_targets))
    target_ids = [t.id for t in targets]
    if len(set(target_ids)) != len(target_ids):
        raise _err("targets", "target ids must be unique")

    raw_rules = data.get("rules", [])
    if not isinstance(raw_rules, list):
        raise _err("rules", "must be an array")
    id_set = set(target_ids)
    rules = tuple(_parse_rule(r, f"rules[{i}]", id_set) for i, r in enumerate(raw_rules))
    rule_ids = [r.id for r in rules]
    if len(set(rule_ids)) != len(rule_ids):
        raise _err("rules", "rule ids must be unique")
    _check_dependency_graph(rules)

    return AppConfig(
        host=host,
        port=port,
        sqlite_path=Path(sqlite_path),
        targets=targets,
        rules=rules,
    )


def load_config(path: str | Path) -> AppConfig:
    """Read the JSON configuration file and validate it."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read configuration file {path}: {exc}") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc
    return parse_config(data)
