"""SQLite persistence for rounds, samples, alert states, suppression, events.

Each scrape round is persisted in a single transaction: the round row, the
replacement of the target's samples (successful rounds only), the mirror of
the *global* active alert states (including the suppression snapshot:
``suppressed`` flag, direct sources and root causes) and all events of the
round (firing/resolved/suppressed/unsuppressed) are written atomically.
Event ids come from an AUTOINCREMENT primary key, so they are stable and
monotonically increasing, which is what the id-based pagination of the
events API relies on.

Databases created by older versions are migrated in place: missing columns
(``alerts.suppressed*``, ``events.details``) are added with defaults that
match the pre-suppression behaviour.

On startup, history (rounds/samples/events) is preserved, every pending
alert is dropped and every previously firing alert is resolved exactly once
with reason ``restart`` -- downtime never accumulates into alert durations.
The suppression snapshot is discarded together with the alert mirror.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .labels import canonical_key, to_json
from .state import KIND_FIRING, KIND_RESOLVED, REASON_RESTART

SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id TEXT NOT NULL,
    ts REAL NOT NULL,
    ok INTEGER NOT NULL,
    error TEXT,
    sample_count INTEGER NOT NULL,
    duration_ms REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rounds_target ON rounds(target_id, id);

CREATE TABLE IF NOT EXISTS samples (
    target_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    labels_key TEXT NOT NULL,
    labels TEXT NOT NULL,
    value REAL NOT NULL,
    round_id INTEGER NOT NULL,
    scraped_at REAL NOT NULL,
    PRIMARY KEY (target_id, metric, labels_key)
);

CREATE TABLE IF NOT EXISTS alerts (
    rule_id TEXT NOT NULL,
    labels_key TEXT NOT NULL,
    target_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    labels TEXT NOT NULL,
    state TEXT NOT NULL,
    since_mono REAL NOT NULL,
    since_wall REAL NOT NULL,
    value REAL,
    threshold REAL NOT NULL,
    suppressed INTEGER NOT NULL DEFAULT 0,
    suppressed_by TEXT NOT NULL DEFAULT '[]',
    root_causes TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (rule_id, labels_key)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT,
    rule_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    labels TEXT NOT NULL,
    value REAL,
    threshold REAL NOT NULL,
    details TEXT
);
"""

# Columns added after the initial schema, applied to old databases.
_MIGRATIONS = {
    "alerts": {
        "suppressed": "ALTER TABLE alerts ADD COLUMN suppressed"
        " INTEGER NOT NULL DEFAULT 0",
        "suppressed_by": "ALTER TABLE alerts ADD COLUMN suppressed_by"
        " TEXT NOT NULL DEFAULT '[]'",
        "root_causes": "ALTER TABLE alerts ADD COLUMN root_causes"
        " TEXT NOT NULL DEFAULT '[]'",
    },
    "events": {
        "details": "ALTER TABLE events ADD COLUMN details TEXT",
    },
}


class Store:
    def __init__(self, path: str | Path):
        self._path = str(path)
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._migrate()

    def close(self) -> None:
        self._conn.close()

    def _migrate(self) -> None:
        """Add columns missing from databases created by older versions."""
        with self._conn:
            for table, columns in _MIGRATIONS.items():
                existing = {
                    row[1]
                    for row in self._conn.execute(f"PRAGMA table_info({table})")
                }
                for name, ddl in columns.items():
                    if name not in existing:
                        self._conn.execute(ddl)

    # ------------------------------------------------------------------
    # startup recovery
    # ------------------------------------------------------------------
    def startup_recovery(self, now_wall: float | None = None) -> int:
        """Resolve leftover firing alerts as 'restart', drop pending ones."""
        now_wall = time.time() if now_wall is None else now_wall
        with self._conn:  # single transaction
            rows = self._conn.execute(
                "SELECT * FROM alerts WHERE state = ?", (KIND_FIRING,)
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    "INSERT INTO events (ts, kind, reason, rule_id, target_id,"
                    " metric, labels, value, threshold)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        now_wall,
                        KIND_RESOLVED,
                        REASON_RESTART,
                        row["rule_id"],
                        row["target_id"],
                        row["metric"],
                        row["labels"],
                        row["value"],
                        row["threshold"],
                    ),
                )
            self._conn.execute("DELETE FROM alerts")
        return len(rows)

    # ------------------------------------------------------------------
    # round persistence (single transaction)
    # ------------------------------------------------------------------
    def save_round(
        self,
        target_id: str,
        *,
        ok: bool,
        error: str | None,
        samples,
        events,
        active_alerts,
        duration_ms: float,
        now_mono: float,
        now_wall: float,
    ) -> None:
        """Persist one round atomically.

        ``active_alerts`` is the *global* mirror of every active alert (all
        targets), annotated by the suppression engine; the alerts table is
        fully replaced each round so suppression changes caused by another
        target's round are reflected immediately.
        """
        del now_mono  # monotonic values are carried inside active_alerts
        with self._conn:  # single transaction for the whole round
            cur = self._conn.execute(
                "INSERT INTO rounds (target_id, ts, ok, error, sample_count,"
                " duration_ms) VALUES (?,?,?,?,?,?)",
                (
                    target_id,
                    now_wall,
                    int(ok),
                    error,
                    len(samples) if ok else 0,
                    duration_ms,
                ),
            )
            round_id = cur.lastrowid
            if ok:
                self._conn.execute(
                    "DELETE FROM samples WHERE target_id = ?", (target_id,)
                )
                self._conn.executemany(
                    "INSERT INTO samples (target_id, metric, labels_key,"
                    " labels, value, round_id, scraped_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    [
                        (
                            target_id,
                            s.metric,
                            canonical_key(s.labels),
                            to_json(s.labels),
                            s.value,
                            round_id,
                            now_wall,
                        )
                        for s in samples
                    ],
                )
            self._conn.execute("DELETE FROM alerts")
            self._conn.executemany(
                "INSERT INTO alerts (rule_id, labels_key, target_id, metric,"
                " labels, state, since_mono, since_wall, value, threshold,"
                " suppressed, suppressed_by, root_causes)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        a["rule_id"],
                        canonical_key(a["labels"]),
                        a["target_id"],
                        a["metric"],
                        to_json(a["labels"]),
                        a["state"],
                        a["since_mono"],
                        a["since_wall"],
                        a["value"],
                        a["threshold"],
                        int(a.get("suppressed", False)),
                        json.dumps(a.get("suppressed_by", []), ensure_ascii=False),
                        json.dumps(a.get("root_causes", []), ensure_ascii=False),
                    )
                    for a in active_alerts
                ],
            )
            self._conn.executemany(
                "INSERT INTO events (ts, kind, reason, rule_id, target_id,"
                " metric, labels, value, threshold, details)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        now_wall,
                        e.kind,
                        e.reason,
                        e.rule.id,
                        e.rule.target_id,
                        e.rule.metric,
                        to_json(e.labels),
                        e.value,
                        e.rule.threshold,
                        json.dumps(e.details, ensure_ascii=False)
                        if e.details is not None
                        else None,
                    )
                    for e in events
                ],
            )

    # ------------------------------------------------------------------
    # queries used by the HTTP API
    # ------------------------------------------------------------------
    def targets_health(self) -> dict[str, dict]:
        latest = self._conn.execute(
            "SELECT r.* FROM rounds r"
            " JOIN (SELECT target_id, MAX(id) AS max_id FROM rounds"
            "       GROUP BY target_id) m"
            "   ON r.target_id = m.target_id AND r.id = m.max_id"
        ).fetchall()
        out: dict[str, dict] = {}
        for row in latest:
            failures = self._conn.execute(
                "SELECT COUNT(*) AS c FROM rounds WHERE target_id = ? AND ok = 0"
                " AND id > COALESCE("
                "   (SELECT MAX(id) FROM rounds WHERE target_id = ? AND ok = 1), 0)",
                (row["target_id"], row["target_id"]),
            ).fetchone()["c"]
            out[row["target_id"]] = {
                "ok": bool(row["ok"]),
                "error": row["error"],
                "last_scrape_at": row["ts"],
                "duration_ms": row["duration_ms"],
                "sample_count": row["sample_count"],
                "consecutive_failures": failures,
            }
        return out

    def latest_samples(self, target_id: str) -> dict | None:
        rows = self._conn.execute(
            "SELECT metric, labels, value, round_id, scraped_at FROM samples"
            " WHERE target_id = ? ORDER BY metric, labels_key",
            (target_id,),
        ).fetchall()
        if not rows:
            return None
        return {
            "round_id": rows[0]["round_id"],
            "scraped_at": rows[0]["scraped_at"],
            "samples": [
                {
                    "metric": r["metric"],
                    "labels": json.loads(r["labels"]),
                    "value": r["value"],
                }
                for r in rows
            ],
        }

    @staticmethod
    def _alert_from_row(r: sqlite3.Row) -> dict:
        return {
            "rule_id": r["rule_id"],
            "target_id": r["target_id"],
            "metric": r["metric"],
            "labels": json.loads(r["labels"]),
            "state": r["state"],
            "since": r["since_wall"],
            "value": r["value"],
            "threshold": r["threshold"],
            "suppressed": bool(r["suppressed"]),
            "suppressed_by": json.loads(r["suppressed_by"]),
            "root_causes": json.loads(r["root_causes"]),
        }

    def active_alerts(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM alerts ORDER BY rule_id, labels_key"
        ).fetchall()
        return [self._alert_from_row(r) for r in rows]

    def actionable_firings(self) -> list[dict]:
        """Firing alerts that are not suppressed (safe to act on)."""
        rows = self._conn.execute(
            "SELECT * FROM alerts WHERE state = ? AND suppressed = 0"
            " ORDER BY rule_id, labels_key",
            (KIND_FIRING,),
        ).fetchall()
        return [self._alert_from_row(r) for r in rows]

    def events_after(self, after_id: int, limit: int) -> tuple[list[dict], bool]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?",
            (after_id, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        events = [
            {
                "id": r["id"],
                "ts": r["ts"],
                "kind": r["kind"],
                "reason": r["reason"],
                "rule_id": r["rule_id"],
                "target_id": r["target_id"],
                "metric": r["metric"],
                "labels": json.loads(r["labels"]),
                "value": r["value"],
                "threshold": r["threshold"],
                "details": json.loads(r["details"])
                if r["details"] is not None
                else None,
            }
            for r in rows
        ]
        return events, has_more
