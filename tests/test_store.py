import sqlite3

import pytest

from causewatch_308.config import RuleConfig
from causewatch_308.parser import Sample
from causewatch_308.state import AlertEvent
from causewatch_308.store import Store


def make_rule(**overrides):
    params = dict(
        id="r1",
        target_id="t1",
        metric="temp",
        labels={"room": "a"},
        threshold=10.0,
        duration_seconds=0.0,
    )
    params.update(overrides)
    return RuleConfig(**params)


def alert_dict(**overrides):
    data = dict(
        rule_id="r1",
        target_id="t1",
        metric="temp",
        labels={"room": "a"},
        state="firing",
        since_mono=1.0,
        since_wall=1000.0,
        value=42.0,
        threshold=10.0,
    )
    data.update(overrides)
    return data


def save_ok(store, samples, events=(), alerts=(), target_id="t1", wall=1000.0):
    store.save_round(
        target_id,
        ok=True,
        error=None,
        samples=samples,
        events=events,
        active_alerts=alerts,
        duration_ms=1.5,
        now_mono=1.0,
        now_wall=wall,
    )


def test_round_samples_saved_and_replaced(tmp_path):
    store = Store(tmp_path / "t.db")
    save_ok(store, [Sample("temp", {"room": "a"}, 1.0)])
    data = store.latest_samples("t1")
    assert data["samples"] == [{"metric": "temp", "labels": {"room": "a"}, "value": 1.0}]
    # next successful round replaces the samples of the target
    save_ok(store, [Sample("temp", {"room": "a"}, 2.0), Sample("hum", {}, 50.0)])
    data = store.latest_samples("t1")
    assert [s["metric"] for s in data["samples"]] == ["hum", "temp"]
    assert store.targets_health()["t1"]["ok"] is True
    store.close()


def test_failed_round_keeps_previous_samples_and_marks_health(tmp_path):
    store = Store(tmp_path / "t.db")
    save_ok(store, [Sample("temp", {}, 1.0)])
    store.save_round(
        "t1",
        ok=False,
        error="timeout after 1s",
        samples=None,
        events=[],
        active_alerts=[],
        duration_ms=1000.0,
        now_mono=2.0,
        now_wall=1001.0,
    )
    data = store.latest_samples("t1")
    assert data["samples"] == [{"metric": "temp", "labels": {}, "value": 1.0}]
    health = store.targets_health()["t1"]
    assert health["ok"] is False
    assert health["error"] == "timeout after 1s"
    assert health["consecutive_failures"] == 1
    save_ok(store, [Sample("temp", {}, 3.0)])
    assert store.targets_health()["t1"]["consecutive_failures"] == 0
    store.close()


def test_events_paginated_by_id(tmp_path):
    store = Store(tmp_path / "t.db")
    rule = make_rule()
    events = [
        AlertEvent("firing", rule, {"room": "a"}, 42.0, None),
        AlertEvent("resolved", rule, {"room": "a"}, 5.0, "recovered"),
        AlertEvent("firing", rule, {"room": "a"}, 43.0, None),
    ]
    save_ok(store, [Sample("temp", {"room": "a"}, 42.0)], events=events)
    page1, more1 = store.events_after(0, 2)
    assert [e["kind"] for e in page1] == ["firing", "resolved"]
    assert more1 is True
    page2, more2 = store.events_after(page1[-1]["id"], 2)
    assert [e["kind"] for e in page2] == ["firing"]
    assert more2 is False
    ids = [e["id"] for e in page1 + page2]
    assert ids == sorted(ids) and len(set(ids)) == 3
    store.close()


def test_restart_resolves_firing_once_and_clears_pending(tmp_path):
    path = tmp_path / "t.db"
    store = Store(path)
    rule = make_rule()
    save_ok(
        store,
        [Sample("temp", {"room": "a"}, 42.0)],
        events=[AlertEvent("firing", rule, {"room": "a"}, 42.0, None)],
        alerts=[
            alert_dict(state="firing"),
            alert_dict(rule_id="r2", state="pending", labels={"room": "b"}),
        ],
    )
    store.close()

    # restart: history preserved, firing resolved once as 'restart', all cleared
    store = Store(path)
    assert store.startup_recovery(now_wall=2000.0) == 1
    assert store.active_alerts() == []
    events, _ = store.events_after(0, 100)
    assert [(e["kind"], e["reason"]) for e in events] == [
        ("firing", None),
        ("resolved", "restart"),
    ]
    # a second startup does not duplicate the resolution
    assert store.startup_recovery(now_wall=3000.0) == 0
    events, _ = store.events_after(0, 100)
    assert len(events) == 2
    store.close()


def test_active_alerts_mirrored_per_round(tmp_path):
    store = Store(tmp_path / "t.db")
    save_ok(store, [Sample("temp", {"room": "a"}, 42.0)], alerts=[alert_dict()])
    assert len(store.active_alerts()) == 1
    # next round without the alert mirrors the empty state
    save_ok(store, [Sample("temp", {"room": "a"}, 1.0)], alerts=[])
    assert store.active_alerts() == []
    store.close()


def test_suppression_snapshot_round_trip(tmp_path):
    store = Store(tmp_path / "t.db")
    source = {
        "rule_id": "up",
        "target_id": "t1",
        "metric": "temp",
        "labels": {"room": "a"},
    }
    save_ok(
        store,
        [Sample("temp", {"room": "a"}, 42.0)],
        alerts=[
            alert_dict(rule_id="up"),
            alert_dict(
                rule_id="down",
                labels={"room": "a"},
                suppressed=True,
                suppressed_by=[source],
                root_causes=[source],
            ),
        ],
    )
    alerts = {a["rule_id"]: a for a in store.active_alerts()}
    assert alerts["up"]["suppressed"] is False
    assert alerts["up"]["suppressed_by"] == []
    assert alerts["up"]["root_causes"] == []
    assert alerts["down"]["suppressed"] is True
    assert alerts["down"]["suppressed_by"] == [source]
    assert alerts["down"]["root_causes"] == [source]
    # only the unsuppressed firing alert is actionable
    assert [a["rule_id"] for a in store.actionable_firings()] == ["up"]
    store.close()


def test_actionable_firings_excludes_pending_and_suppressed(tmp_path):
    store = Store(tmp_path / "t.db")
    save_ok(
        store,
        [Sample("temp", {"room": "a"}, 42.0)],
        alerts=[
            alert_dict(rule_id="firing-free"),
            alert_dict(rule_id="firing-suppressed", suppressed=True),
            alert_dict(rule_id="pending-one", state="pending"),
        ],
    )
    assert [a["rule_id"] for a in store.actionable_firings()] == ["firing-free"]
    store.close()


def test_event_details_round_trip(tmp_path):
    store = Store(tmp_path / "t.db")
    rule = make_rule()
    details = {
        "sources": [
            {
                "rule_id": "up",
                "target_id": "t1",
                "metric": "temp",
                "labels": {"room": "a"},
            }
        ]
    }
    events = [
        AlertEvent("suppressed", rule, {"room": "a"}, 42.0, None, details),
        AlertEvent("unsuppressed", rule, {"room": "a"}, 42.0, None),
    ]
    save_ok(store, [Sample("temp", {"room": "a"}, 42.0)], events=events)
    stored, _ = store.events_after(0, 10)
    assert [e["kind"] for e in stored] == ["suppressed", "unsuppressed"]
    assert stored[0]["details"] == details
    assert stored[0]["reason"] is None
    assert stored[1]["details"] is None
    store.close()


def test_restart_clears_suppression_without_unsuppress_event(tmp_path):
    path = tmp_path / "t.db"
    store = Store(path)
    rule = make_rule()
    save_ok(
        store,
        [Sample("temp", {"room": "a"}, 42.0)],
        events=[AlertEvent("firing", rule, {"room": "a"}, 42.0, None)],
        alerts=[
            alert_dict(
                state="firing",
                suppressed=True,
                suppressed_by=[{"rule_id": "up", "target_id": "t1",
                                "metric": "temp", "labels": {"room": "a"}}],
                root_causes=[{"rule_id": "up", "target_id": "t1",
                              "metric": "temp", "labels": {"room": "a"}}],
            )
        ],
    )
    store.close()

    store = Store(path)
    assert store.startup_recovery(now_wall=2000.0) == 1
    assert store.active_alerts() == []
    events, _ = store.events_after(0, 100)
    # the leftover suppressed firing resolves as 'restart'; no unsuppressed
    # event is ever emitted for it, and history is preserved
    assert [(e["kind"], e["reason"]) for e in events] == [
        ("firing", None),
        ("resolved", "restart"),
    ]
    store.close()


def test_old_database_is_migrated(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id TEXT NOT NULL, ts REAL NOT NULL, ok INTEGER NOT NULL,
            error TEXT, sample_count INTEGER NOT NULL, duration_ms REAL NOT NULL
        );
        CREATE TABLE samples (
            target_id TEXT NOT NULL, metric TEXT NOT NULL,
            labels_key TEXT NOT NULL, labels TEXT NOT NULL, value REAL NOT NULL,
            round_id INTEGER NOT NULL, scraped_at REAL NOT NULL,
            PRIMARY KEY (target_id, metric, labels_key)
        );
        CREATE TABLE alerts (
            rule_id TEXT NOT NULL, labels_key TEXT NOT NULL,
            target_id TEXT NOT NULL, metric TEXT NOT NULL, labels TEXT NOT NULL,
            state TEXT NOT NULL, since_mono REAL NOT NULL,
            since_wall REAL NOT NULL, value REAL, threshold REAL NOT NULL,
            PRIMARY KEY (rule_id, labels_key)
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
            kind TEXT NOT NULL, reason TEXT, rule_id TEXT NOT NULL,
            target_id TEXT NOT NULL, metric TEXT NOT NULL, labels TEXT NOT NULL,
            value REAL, threshold REAL NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO alerts (rule_id, labels_key, target_id, metric, labels,"
        " state, since_mono, since_wall, value, threshold)"
        " VALUES ('r1', '[[\"room\",\"a\"]]', 't1', 'temp',"
        " '{\"room\": \"a\"}', 'firing', 1.0, 1000.0, 42.0, 10.0)"
    )
    conn.execute(
        "INSERT INTO events (ts, kind, reason, rule_id, target_id, metric,"
        " labels, value, threshold)"
        " VALUES (1000.0, 'firing', NULL, 'r1', 't1', 'temp',"
        " '{\"room\": \"a\"}', 42.0, 10.0)"
    )
    conn.commit()
    conn.close()

    store = Store(path)  # migrates the old schema in place
    alerts = store.active_alerts()
    assert len(alerts) == 1
    assert alerts[0]["suppressed"] is False
    assert alerts[0]["suppressed_by"] == []
    assert alerts[0]["root_causes"] == []
    events, _ = store.events_after(0, 10)
    assert events[0]["kind"] == "firing"
    assert events[0]["details"] is None
    # startup recovery still works on the migrated database
    assert store.startup_recovery(now_wall=2000.0) == 1
    events, _ = store.events_after(0, 10)
    assert [(e["kind"], e["reason"]) for e in events] == [
        ("firing", None),
        ("resolved", "restart"),
    ]
    # and new suppression data can be written to the migrated database
    save_ok(
        store,
        [Sample("temp", {"room": "a"}, 42.0)],
        alerts=[alert_dict(suppressed=True, suppressed_by=[], root_causes=[])],
    )
    assert store.active_alerts()[0]["suppressed"] is True
    store.close()
