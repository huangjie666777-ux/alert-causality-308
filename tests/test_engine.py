"""Engine-level tests: persistence atomicity, suppression flow, migration."""

import sqlite3

import pytest

from causewatch_308.config import (
    AppConfig,
    DependencyConfig,
    RuleConfig,
    TargetConfig,
)
from causewatch_308.engine import AlertEngine
from causewatch_308.parser import Sample
from causewatch_308.scraper import ScrapeResult
from causewatch_308.store import Store


def make_config(tmp_path, rules, dependencies=()):
    return AppConfig(
        host="127.0.0.1",
        port=8080,
        sqlite_path=tmp_path / "engine.db",
        targets=(
            TargetConfig("t1", "http://127.0.0.1:1/metrics", 1.0, 1.0, 65536),
        ),
        rules=tuple(rules),
        dependencies=tuple(dependencies),
    )


def ok_result(*samples):
    return ScrapeResult(True, list(samples), None, 1.0)


def failed_result():
    return ScrapeResult(False, None, "connection error: refused", 1.0)


def kinds_and_reasons(store):
    events, _ = store.events_after(0, 500)
    return [(e["kind"], e["reason"]) for e in events]


def test_failed_persistence_does_not_advance_memory(tmp_path):
    config = make_config(
        tmp_path, [RuleConfig("r1", "t1", "temp", {}, 10.0, 0.0)]
    )
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    hot = ok_result(Sample("temp", {"room": "a"}, 50.0))

    original_save = store.save_round

    def broken_save(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    store.save_round = broken_save
    with pytest.raises(sqlite3.OperationalError):
        engine.handle_round(target, hot)
    # nothing was persisted and memory did not advance
    assert store.active_alerts() == []
    assert store.events_after(0, 100)[0] == []

    # the next round re-plans the same firing instead of losing it
    store.save_round = original_save
    engine.handle_round(target, hot)
    assert kinds_and_reasons(store) == [("firing", None)]
    alerts = store.active_alerts()
    assert len(alerts) == 1 and alerts[0]["state"] == "firing"
    assert alerts[0]["suppressed"] is False

    # a further identical round does not duplicate the firing event
    engine.handle_round(target, hot)
    assert kinds_and_reasons(store) == [("firing", None)]
    store.close()


def test_failed_persistence_does_not_advance_suppression(tmp_path):
    config = make_config(
        tmp_path,
        [
            RuleConfig("up", "t1", "m1", {}, 10.0, 0.0),
            RuleConfig("down", "t1", "m2", {}, 10.0, 0.0),
        ],
        [DependencyConfig("up", "down", ("room",))],
    )
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    both = ok_result(
        Sample("m1", {"room": "a"}, 50.0), Sample("m2", {"room": "a"}, 50.0)
    )

    original_save = store.save_round

    def broken_save(*args, **kwargs):
        raise sqlite3.OperationalError("boom")

    store.save_round = broken_save
    with pytest.raises(sqlite3.OperationalError):
        engine.handle_round(target, both)

    store.save_round = original_save
    engine.handle_round(target, both)
    # the whole round (firings + suppression) is replayed exactly once
    assert kinds_and_reasons(store) == [
        ("firing", None),
        ("firing", None),
        ("suppressed", None),
    ]
    engine.handle_round(target, both)
    assert len(kinds_and_reasons(store)) == 3
    store.close()


def suppression_config(tmp_path, up_duration=0.0, down_duration=0.0):
    return make_config(
        tmp_path,
        [
            RuleConfig("up", "t1", "m1", {}, 10.0, up_duration),
            RuleConfig("down", "t1", "m2", {}, 10.0, down_duration),
        ],
        [DependencyConfig("up", "down", ("room",))],
    )


def test_suppression_flow_end_to_end_in_memory(tmp_path):
    config = suppression_config(tmp_path)
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]

    # downstream fires alone first: actionable
    engine.handle_round(target, ok_result(Sample("m2", {"room": "a"}, 50.0)))
    alerts = {a["rule_id"]: a for a in store.active_alerts()}
    assert alerts["down"]["suppressed"] is False

    # upstream fires: downstream becomes suppressed, original events kept
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    alerts = {a["rule_id"]: a for a in store.active_alerts()}
    assert alerts["up"]["suppressed"] is False
    down = alerts["down"]
    assert down["suppressed"] is True
    assert [r["rule_id"] for r in down["suppressed_by"]] == ["up"]
    assert [r["rule_id"] for r in down["root_causes"]] == ["up"]
    assert down["suppressed_by"][0]["labels"] == {"room": "a"}
    assert down["suppressed_by"][0]["target_id"] == "t1"
    assert kinds_and_reasons(store) == [
        ("firing", None),       # down fires
        ("firing", None),       # up fires
        ("suppressed", None),   # down suppressed by up
    ]

    # steady state: no duplicate suppression events
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    assert len(kinds_and_reasons(store)) == 3

    # upstream recovers: downstream is re-exposed while still firing
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 1.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    alerts = {a["rule_id"]: a for a in store.active_alerts()}
    assert "up" not in alerts
    assert alerts["down"]["suppressed"] is False
    assert kinds_and_reasons(store)[-2:] == [
        ("resolved", "recovered"),
        ("unsuppressed", None),
    ]
    store.close()


def test_downstream_recovery_while_suppressed_has_no_release_event(tmp_path):
    config = suppression_config(tmp_path)
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    # downstream recovers on its own while upstream keeps firing
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 1.0)
        ),
    )
    assert kinds_and_reasons(store) == [
        ("firing", None),
        ("firing", None),
        ("suppressed", None),
        ("resolved", "recovered"),  # downstream's own recovery, no release
    ]
    store.close()


def test_pending_downstream_timer_keeps_running(tmp_path):
    config = suppression_config(tmp_path, down_duration=10.0)
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    # upstream firing while downstream is still pending
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    alerts = {a["rule_id"]: a for a in store.active_alerts()}
    assert alerts["down"]["state"] == "pending"
    assert alerts["down"]["suppressed"] is False
    assert kinds_and_reasons(store) == [("firing", None)]
    store.close()


def test_scrape_failure_resolves_without_release_event(tmp_path):
    config = suppression_config(tmp_path)
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    engine.handle_round(target, failed_result())
    # both alerts resolve as scrape_failed; no unsuppressed event for down
    assert kinds_and_reasons(store) == [
        ("firing", None),
        ("firing", None),
        ("suppressed", None),
        ("resolved", "scrape_failed"),
        ("resolved", "scrape_failed"),
    ]
    assert store.active_alerts() == []
    store.close()


def test_restart_clears_suppression_without_release_event(tmp_path):
    config = suppression_config(tmp_path)
    store = Store(config.sqlite_path)
    engine = AlertEngine(config, store)
    target = config.targets[0]
    engine.handle_round(
        target,
        ok_result(
            Sample("m1", {"room": "a"}, 99.0), Sample("m2", {"room": "a"}, 50.0)
        ),
    )
    store.close()

    # restart: history kept, firing alerts resolved as 'restart', suppression gone
    store = Store(config.sqlite_path)
    assert store.startup_recovery() == 2
    assert store.active_alerts() == []
    assert kinds_and_reasons(store) == [
        ("firing", None),
        ("firing", None),
        ("suppressed", None),
        ("resolved", "restart"),
        ("resolved", "restart"),
    ]
    store.close()


def test_old_database_is_migrated_in_place(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id TEXT NOT NULL,
            ts REAL NOT NULL,
            ok INTEGER NOT NULL,
            error TEXT,
            sample_count INTEGER NOT NULL,
            duration_ms REAL NOT NULL
        );
        CREATE TABLE samples (
            target_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            labels_key TEXT NOT NULL,
            labels TEXT NOT NULL,
            value REAL NOT NULL,
            round_id INTEGER NOT NULL,
            scraped_at REAL NOT NULL,
            PRIMARY KEY (target_id, metric, labels_key)
        );
        CREATE TABLE alerts (
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
            PRIMARY KEY (rule_id, labels_key)
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            kind TEXT NOT NULL,
            reason TEXT,
            rule_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            metric TEXT NOT NULL,
            labels TEXT NOT NULL,
            value REAL,
            threshold REAL NOT NULL
        );
        INSERT INTO alerts (rule_id, labels_key, target_id, metric, labels,
                            state, since_mono, since_wall, value, threshold)
        VALUES ('r1', '[["room","a"]]', 't1', 'temp', '{"room":"a"}',
                'firing', 1.0, 1000.0, 42.0, 10.0);
        INSERT INTO events (ts, kind, reason, rule_id, target_id, metric,
                            labels, value, threshold)
        VALUES (1000.0, 'firing', NULL, 'r1', 't1', 'temp',
                '{"room":"a"}', 42.0, 10.0);
        """
    )
    conn.close()

    store = Store(path)  # migrates the old schema on open
    alert = store.active_alerts()[0]
    assert alert["rule_id"] == "r1"
    assert alert["suppressed"] is False
    assert alert["suppressed_by"] == []
    assert alert["root_causes"] == []
    # restart settlement works on the migrated database
    assert store.startup_recovery(now_wall=2000.0) == 1
    assert store.active_alerts() == []
    events, _ = store.events_after(0, 100)
    assert [(e["kind"], e["reason"]) for e in events] == [
        ("firing", None),
        ("resolved", "restart"),
    ]
    store.close()
