"""End-to-end tests: real HTTP servers, scheduler, engine, store and API."""

import asyncio
import sqlite3
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import unused_port

from causewatch_308.config import (
    AppConfig,
    DependencyConfig,
    RuleConfig,
    TargetConfig,
)
from causewatch_308.engine import AlertEngine
from causewatch_308.scraper import ScrapeScheduler
from causewatch_308.server import make_app
from causewatch_308.store import Store


async def wait_for(cond, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


def make_config(port, tmp_path, *, interval=0.05, timeout=1.0, limit=65536,
                threshold=10.0, duration=0.15):
    return AppConfig(
        host="127.0.0.1",
        port=unused_port(),
        sqlite_path=tmp_path / "it.db",
        targets=(
            TargetConfig("t1", f"http://127.0.0.1:{port}/metrics",
                         interval, timeout, limit),
        ),
        rules=(
            RuleConfig("r1", "t1", "temp", {"room": "a"}, threshold, duration),
        ),
    )


async def start_metrics_server(state, mode):
    async def metrics(request):
        if mode["name"] == "slow":
            await asyncio.sleep(0.5)
        if mode["name"] == "big":
            return web.Response(text="big_metric 1\n" * 5000)
        if mode["name"] == "bad":
            return web.Response(text="this is not a valid exposition\n")
        return web.Response(
            text=f'# TYPE temp gauge\ntemp{{room="a"}} {state["value"]}\n'
        )

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    port = unused_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner, port


def test_trigger_and_recovery_end_to_end(tmp_path):
    asyncio.run(_trigger_and_recovery(tmp_path))


async def _trigger_and_recovery(tmp_path):
    state = {"value": 5.0}
    mode = {"name": "ok"}
    runner, port = await start_metrics_server(state, mode)
    config = make_config(port, tmp_path)
    store = Store(config.sqlite_path)
    store.startup_recovery()
    engine = AlertEngine(config, store)
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()
    try:
        await wait_for(lambda: store.targets_health().get("t1", {}).get("ok"))
        assert store.active_alerts() == []

        state["value"] = 50.0
        await wait_for(lambda: any(a["state"] == "pending" for a in store.active_alerts()))
        await wait_for(lambda: any(a["state"] == "firing" for a in store.active_alerts()))
        events, _ = store.events_after(0, 100)
        assert [(e["kind"], e["reason"]) for e in events] == [("firing", None)]
        assert events[0]["labels"] == {"room": "a"}

        state["value"] = 1.0
        await wait_for(lambda: store.active_alerts() == [])
        events, _ = store.events_after(0, 100)
        assert [(e["kind"], e["reason"]) for e in events] == [
            ("firing", None),
            ("resolved", "recovered"),
        ]
    finally:
        await scheduler.stop()
        store.close()
        await runner.cleanup()


def test_failed_rounds_resolve_and_do_not_publish_partial_samples(tmp_path):
    asyncio.run(_failed_rounds(tmp_path))


async def _failed_rounds(tmp_path):
    state = {"value": 50.0}
    mode = {"name": "ok"}
    runner, port = await start_metrics_server(state, mode)
    config = make_config(port, tmp_path, duration=0.0, limit=4096, timeout=0.3)
    store = Store(config.sqlite_path)
    store.startup_recovery()
    engine = AlertEngine(config, store)
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()
    try:
        # zero duration: fires on the first successful round
        await wait_for(lambda: any(a["state"] == "firing" for a in store.active_alerts()))
        assert store.latest_samples("t1")["samples"][0]["value"] == 50.0

        # oversized response: whole round fails, firing resolves, samples stay
        mode["name"] = "big"
        await wait_for(lambda: store.targets_health()["t1"]["consecutive_failures"] >= 1)
        await wait_for(lambda: store.active_alerts() == [])
        events, _ = store.events_after(0, 100)
        assert ("resolved", "scrape_failed") in [(e["kind"], e["reason"]) for e in events]
        assert "exceeds limit" in store.targets_health()["t1"]["error"]
        # no partial samples of the failed round were published
        assert store.latest_samples("t1")["samples"][0]["metric"] == "temp"

        # invalid payload also fails the round
        mode["name"] = "bad"
        await wait_for(lambda: "parse error" in (store.targets_health()["t1"]["error"] or ""))

        # slow endpoint hits the per-target timeout
        mode["name"] = "slow"
        await wait_for(lambda: "timeout" in (store.targets_health()["t1"]["error"] or ""))

        # recovery: healthy rounds resume and the alert fires again
        mode["name"] = "ok"
        await wait_for(lambda: store.targets_health()["t1"]["ok"])
        await wait_for(lambda: any(a["state"] == "firing" for a in store.active_alerts()))
    finally:
        await scheduler.stop()
        store.close()
        await runner.cleanup()


def test_http_api(tmp_path):
    asyncio.run(_http_api(tmp_path))


async def _http_api(tmp_path):
    state = {"value": 50.0}
    mode = {"name": "ok"}
    metrics_runner, metrics_port = await start_metrics_server(state, mode)
    config = make_config(metrics_port, tmp_path, duration=0.0)
    store = Store(config.sqlite_path)
    store.startup_recovery()
    engine = AlertEngine(config, store)
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()

    api_runner = web.AppRunner(make_app(config, store))
    await api_runner.setup()
    api_port = unused_port()
    await web.TCPSite(api_runner, "127.0.0.1", api_port).start()
    base = f"http://127.0.0.1:{api_port}"
    try:
        await wait_for(lambda: any(a["state"] == "firing" for a in store.active_alerts()))
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/api/targets") as resp:
                assert resp.status == 200
                body = await resp.json()
                target = body["targets"][0]
                assert target["id"] == "t1"
                assert target["health"]["ok"] is True
                assert target["rules"] == ["r1"]

            async with session.get(f"{base}/api/targets/t1/samples") as resp:
                assert resp.status == 200
                body = await resp.json()
                assert body["samples"][0]["value"] == 50.0

            async with session.get(f"{base}/api/targets/nope/samples") as resp:
                assert resp.status == 404

            async with session.get(f"{base}/api/alerts") as resp:
                body = await resp.json()
                assert body["alerts"][0]["state"] == "firing"
                assert body["alerts"][0]["labels"] == {"room": "a"}

            async with session.get(f"{base}/api/events?limit=1") as resp:
                page1 = await resp.json()
                assert len(page1["events"]) == 1
                assert page1["events"][0]["kind"] == "firing"

            state["value"] = 0.0
            await wait_for(lambda: store.active_alerts() == [])
            async with session.get(
                f"{base}/api/events?after_id={page1['next_after_id']}&limit=10"
            ) as resp:
                page2 = await resp.json()
                assert page2["events"][0]["kind"] == "resolved"
                assert page2["events"][0]["reason"] == "recovered"
                assert page2["has_more"] is False

            async with session.get(f"{base}/api/events?limit=0") as resp:
                assert resp.status == 400
    finally:
        await scheduler.stop()
        await api_runner.cleanup()
        store.close()
        await metrics_runner.cleanup()


async def start_two_metric_server(state):
    async def metrics(request):
        return web.Response(
            text=(
                '# TYPE temp gauge\ntemp{room="a"} %s\n'
                '# TYPE hum gauge\nhum{room="a"} %s\n'
            )
            % (state["temp"], state["hum"])
        )

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    port = unused_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner, port


def make_dep_config(port, tmp_path, *, interval=0.05):
    return AppConfig(
        host="127.0.0.1",
        port=unused_port(),
        sqlite_path=tmp_path / "dep.db",
        targets=(
            TargetConfig("t1", f"http://127.0.0.1:{port}/metrics",
                         interval, 1.0, 65536),
        ),
        rules=(
            RuleConfig("temp-high", "t1", "temp", {"room": "a"}, 10.0, 0.0),
            RuleConfig(
                "hum-high", "t1", "hum", {"room": "a"}, 10.0, 0.0,
                (DependencyConfig("temp-high", ("room",)),),
            ),
        ),
    )


def test_suppression_end_to_end(tmp_path):
    asyncio.run(_suppression_end_to_end(tmp_path))


async def _suppression_end_to_end(tmp_path):
    state = {"temp": 1.0, "hum": 1.0}
    metrics_runner, metrics_port = await start_two_metric_server(state)
    config = make_dep_config(metrics_port, tmp_path)
    store = Store(config.sqlite_path)
    store.startup_recovery()
    engine = AlertEngine(config, store)
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()

    api_runner = web.AppRunner(make_app(config, store))
    await api_runner.setup()
    api_port = unused_port()
    await web.TCPSite(api_runner, "127.0.0.1", api_port).start()
    base = f"http://127.0.0.1:{api_port}"
    try:
        await wait_for(lambda: store.targets_health().get("t1", {}).get("ok"))
        import aiohttp

        async with aiohttp.ClientSession() as session:
            # downstream fires alone: fully actionable
            state["hum"] = 50.0
            await wait_for(lambda: len(store.actionable_firings()) == 1)
            async with session.get(f"{base}/api/firing") as resp:
                body = await resp.json()
                assert [a["rule_id"] for a in body["alerts"]] == ["hum-high"]
                assert body["alerts"][0]["suppressed"] is False

            # upstream fires too: downstream becomes suppressed
            state["temp"] = 50.0
            await wait_for(
                lambda: any(
                    a["rule_id"] == "hum-high" and a["suppressed"]
                    for a in store.active_alerts()
                )
            )
            async with session.get(f"{base}/api/firing") as resp:
                body = await resp.json()
                assert [a["rule_id"] for a in body["alerts"]] == ["temp-high"]
            async with session.get(f"{base}/api/alerts") as resp:
                body = await resp.json()
                alerts = {a["rule_id"]: a for a in body["alerts"]}
                hum = alerts["hum-high"]
                assert hum["state"] == "firing"
                assert hum["suppressed"] is True
                assert hum["suppressed_by"] == [
                    {
                        "rule_id": "temp-high",
                        "target_id": "t1",
                        "metric": "temp",
                        "labels": {"room": "a"},
                    }
                ]
                assert hum["root_causes"] == hum["suppressed_by"]
                assert alerts["temp-high"]["suppressed"] is False
            # the original firing events of both alerts are preserved and the
            # suppression transition was appended exactly once
            events, _ = store.events_after(0, 100)
            assert [(e["kind"], e["rule_id"]) for e in events] == [
                ("firing", "hum-high"),
                ("firing", "temp-high"),
                ("suppressed", "hum-high"),
            ]
            assert events[2]["details"]["sources"][0]["rule_id"] == "temp-high"

            # upstream recovers: downstream is exposed again
            state["temp"] = 1.0
            await wait_for(
                lambda: any(
                    e["kind"] == "unsuppressed" for e in store.events_after(0, 100)[0]
                )
            )
            async with session.get(f"{base}/api/firing") as resp:
                body = await resp.json()
                assert [a["rule_id"] for a in body["alerts"]] == ["hum-high"]
            events, _ = store.events_after(0, 100)
            assert [(e["kind"], e["rule_id"]) for e in events] == [
                ("firing", "hum-high"),
                ("firing", "temp-high"),
                ("suppressed", "hum-high"),
                ("resolved", "temp-high"),
                ("unsuppressed", "hum-high"),
            ]

            # downstream recovers: plain resolved event, no unsuppressed one
            state["hum"] = 1.0
            await wait_for(lambda: store.active_alerts() == [])
            events, _ = store.events_after(0, 100)
            assert [(e["kind"], e["rule_id"]) for e in events][-1] == (
                "resolved",
                "hum-high",
            )
            assert [e["kind"] for e in events].count("unsuppressed") == 1
    finally:
        await scheduler.stop()
        await api_runner.cleanup()
        store.close()
        await metrics_runner.cleanup()


def test_failed_persistence_does_not_advance_memory(tmp_path):
    asyncio.run(_failed_persistence(tmp_path))


async def _failed_persistence(tmp_path):
    state = {"value": 50.0}
    mode = {"name": "ok"}
    runner, port = await start_metrics_server(state, mode)
    config = make_config(port, tmp_path, duration=0.0)
    store = Store(config.sqlite_path)
    store.startup_recovery()
    engine = AlertEngine(config, store)

    # sabotage the very first round persist
    real_save_round = store.save_round
    calls = {"n": 0}

    def flaky_save_round(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("injected persist failure")
        return real_save_round(*args, **kwargs)

    store.save_round = flaky_save_round
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()
    try:
        await wait_for(lambda: calls["n"] >= 2)
        # the failed round did not advance the in-memory state machine, so
        # the retry records the firing exactly once
        await wait_for(
            lambda: any(a["state"] == "firing" for a in store.active_alerts())
        )
        events, _ = store.events_after(0, 100)
        assert [(e["kind"], e["reason"]) for e in events] == [("firing", None)]
    finally:
        await scheduler.stop()
        store.close()
        await runner.cleanup()
