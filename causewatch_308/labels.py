"""Label-set canonicalisation shared by parser, state machine and store.

Alert identity is built from the *complete* label set of a series.  Sorting
the items makes the identity independent of the order labels appeared in the
exposition, so ``{a="1",b="2"}`` and ``{b="2",a="1"}`` are the same alert.
"""

from __future__ import annotations

import json


def sorted_items(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """Return the label set as a sorted tuple of items (hashable identity)."""
    return tuple(sorted(labels.items()))


def canonical_key(labels: dict[str, str]) -> str:
    """Return a stable string key for a label set (used as DB key)."""
    return json.dumps(sorted(labels.items()), ensure_ascii=False, separators=(",", ":"))


def to_json(labels: dict[str, str]) -> str:
    """Serialise a label set as a sorted JSON object for storage/display."""
    return json.dumps(dict(sorted(labels.items())), ensure_ascii=False, sort_keys=True)
