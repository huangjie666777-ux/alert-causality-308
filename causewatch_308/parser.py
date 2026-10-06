"""Parser for the Prometheus text exposition format, version 0.0.4.

Supported subset and strictness rules (see README):

* gauge (and untyped) samples are kept; samples of other declared types are
  still fully validated, then skipped;
* label values support the ``\\n``, ``\\"`` and ``\\\\`` escapes;
* explicit timestamps are rejected;
* malformed lines, duplicate series and non-finite values (NaN, +Inf, -Inf)
  are rejected.

Any violation raises :class:`ParseError`, which fails the whole scrape round:
no partial set of samples is ever published.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from .labels import sorted_items


class ParseError(ValueError):
    """Raised when an exposition cannot be parsed."""


@dataclass
class Sample:
    metric: str
    labels: dict[str, str]
    value: float


_METRIC_NAME_RE = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_LABEL_NAME_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
_VALUE_RE = re.compile(
    r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
    r"|[+-]?[Nn][Aa][Nn]"
    r"|[+-]?[Ii][Nn][Ff]"
)
_KNOWN_TYPES = {"counter", "gauge", "histogram", "summary", "untyped"}
_KEPT_TYPES = {"gauge", "untyped"}
_ESCAPES = {"n": "\n", '"': '"', "\\": "\\"}


def parse_text(text: str) -> list[Sample]:
    """Parse a whole exposition, returning the kept gauge/untyped samples."""
    types: dict[str, str] = {}
    helps: set[str] = set()
    sampled: set[str] = set()
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    kept: list[Sample] = []

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            _parse_comment(line, lineno, types, helps, sampled)
            continue
        sample = _parse_sample(line, lineno)
        key = (sample.metric, sorted_items(sample.labels))
        if key in seen:
            raise ParseError(
                f"line {lineno}: duplicate series for metric {sample.metric!r}"
            )
        seen.add(key)
        sampled.add(sample.metric)
        if types.get(sample.metric, "untyped") in _KEPT_TYPES:
            kept.append(sample)
    return kept


def _check_metric_name(name: str, lineno: int) -> None:
    if not _METRIC_NAME_RE.fullmatch(name):
        raise ParseError(f"line {lineno}: invalid metric name {name!r}")


def _parse_comment(
    line: str,
    lineno: int,
    types: dict[str, str],
    helps: set[str],
    sampled: set[str],
) -> None:
    body = line[1:].strip()
    if not body:
        return
    parts = body.split(None, 1)
    directive = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    if directive == "HELP":
        tokens = rest.split(None, 1)
        if not tokens:
            raise ParseError(f"line {lineno}: malformed HELP directive")
        name = tokens[0]
        _check_metric_name(name, lineno)
        if name in helps:
            raise ParseError(f"line {lineno}: duplicate HELP for metric {name!r}")
        if name in sampled:
            raise ParseError(
                f"line {lineno}: HELP for metric {name!r} appears after samples"
            )
        helps.add(name)
    elif directive == "TYPE":
        tokens = rest.split()
        if len(tokens) != 2:
            raise ParseError(f"line {lineno}: malformed TYPE directive")
        name, typ = tokens
        _check_metric_name(name, lineno)
        if typ not in _KNOWN_TYPES:
            raise ParseError(f"line {lineno}: unknown metric type {typ!r}")
        if name in types:
            raise ParseError(f"line {lineno}: duplicate TYPE for metric {name!r}")
        if name in sampled:
            raise ParseError(
                f"line {lineno}: TYPE for metric {name!r} appears after samples"
            )
        types[name] = typ
    # Any other comment (including unknown directives) is ignored.


def _parse_sample(line: str, lineno: int) -> Sample:
    match = _METRIC_NAME_RE.match(line)
    if match is None:
        raise ParseError(f"line {lineno}: invalid metric name")
    name = match.group(0)
    i = match.end()
    labels: dict[str, str] = {}
    if i < len(line) and line[i] == "{":
        labels, i = _parse_labels(line, i, lineno)
    rest = line[i:]
    if rest and not rest[0].isspace():
        raise ParseError(f"line {lineno}: malformed sample line")
    tail = rest.strip()
    if tail.startswith("{"):
        raise ParseError(f"line {lineno}: unexpected '{{' after metric name")
    tokens = tail.split()
    if not tokens:
        raise ParseError(f"line {lineno}: missing sample value")
    if len(tokens) == 2:
        raise ParseError(
            f"line {lineno}: explicit timestamps are not supported"
        )
    if len(tokens) > 2:
        raise ParseError(f"line {lineno}: malformed sample line")
    value = _parse_value(tokens[0], lineno)
    return Sample(name, labels, value)


def _parse_labels(line: str, i: int, lineno: int) -> tuple[dict[str, str], int]:
    # line[i] == "{"
    i += 1
    labels: dict[str, str] = {}
    n = len(line)
    if i < n and line[i] == "}":
        return labels, i + 1
    while True:
        match = _LABEL_NAME_RE.match(line, i)
        if match is None:
            raise ParseError(f"line {lineno}: invalid label name")
        lname = match.group(0)
        i = match.end()
        if i >= n or line[i] != "=":
            raise ParseError(
                f"line {lineno}: expected '=' after label name {lname!r}"
            )
        i += 1
        if i >= n or line[i] != '"':
            raise ParseError(
                f"line {lineno}: expected quoted value for label {lname!r}"
            )
        value, i = _parse_label_value(line, i, lineno)
        if lname in labels:
            raise ParseError(f"line {lineno}: duplicate label {lname!r}")
        labels[lname] = value
        if i >= n:
            raise ParseError(f"line {lineno}: unterminated label set")
        char = line[i]
        if char == ",":
            i += 1
            continue
        if char == "}":
            return labels, i + 1
        raise ParseError(f"line {lineno}: expected ',' or '}}' in label set")


def _parse_label_value(line: str, i: int, lineno: int) -> tuple[str, int]:
    # line[i] == '"'
    i += 1
    out: list[str] = []
    n = len(line)
    while True:
        if i >= n:
            raise ParseError(f"line {lineno}: unterminated label value")
        char = line[i]
        if char == '"':
            return "".join(out), i + 1
        if char == "\\":
            i += 1
            if i >= n:
                raise ParseError(
                    f"line {lineno}: unterminated escape in label value"
                )
            esc = line[i]
            if esc not in _ESCAPES:
                raise ParseError(
                    f"line {lineno}: invalid escape sequence '\\{esc}' in label value"
                )
            out.append(_ESCAPES[esc])
            i += 1
        else:
            out.append(char)
            i += 1


def _parse_value(token: str, lineno: int) -> float:
    if not _VALUE_RE.fullmatch(token):
        raise ParseError(f"line {lineno}: invalid sample value {token!r}")
    value = float(token)
    if not math.isfinite(value):
        raise ParseError(f"line {lineno}: non-finite sample value {token!r}")
    return value
