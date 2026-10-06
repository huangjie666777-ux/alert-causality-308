"""End-to-end suppression tests: real HTTP servers, scheduler, engine, API."""

import asyncio

import aiohttp
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
from tests.test_integration import wait_for


async def start_metrics_server(state):
    async def metrics(request):
        return web.Response(
            text=(
                "# TYPE temp gauge\n"
                f'temp{{room="a"}} {state["temp"]}\n'
                "# TYPE humidity gauge\n"
                f'humidity{{room="a"}} {state["humidity"]}\n'
            )
        )

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    port = unused_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner, port


def make_config(port, tmp_path):
    return AppConfig(
        host="127.0.0.1",
        port=unused_port(),
        sqlite_path=tmp_path / "e2e.db",
        targets=(
            TargetConfig(
                "t1", f"http://127.0.0.1:{port}/metrics", 0.05, 1.0, 65536
            ),
        ),
        rules=(
            RuleConfig("temp-high", "t1", "temp", {"room": "a"}, 10.0, 0.1),
            RuleConfig("hum-high", "t1", "humidity", {"room": "a"}, 10.0, 0.1),
        ),
        dependencies=(DependencyConfig("temp-high", "hum-high", ("room",)),),
    )


def test_cascading_suppression_and_reexposure(tmp_path):
    asyncio.run(_cascading_suppression(tmp_path))


async def _cascading_suppression(tmp_path):
    state = {"temp": 0.0, "humidity": 0.0}
    metrics_runner, metrics_port = await start_metrics_server(state)
    config = make_config(metrics_port, tmp_path)
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

    async def alerts_body():
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/api/alerts") as resp:
                assert resp.status == 200
                return await resp.json()

    def hum_alert(body):
        return next(a for a in body["alerts"] if a["rule_id"] == "hum-high")

    try:
        # downstream fires alone: actionable
        state["humidity"] = 50.0
        await wait_for(
            lambda: any(
                a["rule_id"] == "hum-high" and a["state"] == "firing"
                for a in store.active_alerts()
            )
        )
        body = await alerts_body()
        assert [a["rule_id"] for a in body["actionable"]] == ["hum-high"]
        assert hum_alert(body)["suppressed"] is False

        # upstream fires: downstream is suppressed and drops out of actionable
        state["temp"] = 99.0
        await wait_for(
            lambda: any(
                a["rule_id"] == "hum-high" and a["suppressed"]
                for a in store.active_alerts()
            )
        )
        body = await alerts_body()
        assert [a["rule_id"] for a in body["actionable"]] == ["temp-high"]
        hum = hum_alert(body)
        assert hum["state"] == "firing"
        assert hum["suppressed"] is True
        assert [r["rule_id"] for r in hum["suppressed_by"]] == ["temp-high"]
        assert [r["rule_id"] for r in hum["root_causes"]] == ["temp-high"]
        source = hum["suppressed_by"][0]
        assert source["target_id"] == "t1"
        assert source["labels"] == {"room": "a"}

        events, _ = store.events_after(0, 100)
        kinds = [(e["kind"], e["rule_id"]) for e in events]
        assert kinds == [
            ("firing", "hum-high"),
            ("firing", "temp-high"),
            ("suppressed", "hum-high"),
        ]

        # upstream recovers: downstream is re-exposed while still firing
        state["temp"] = 0.0
        await wait_for(
            lambda: any(
                a["rule_id"] == "hum-high" and not a["suppressed"]
                for a in store.active_alerts()
            )
        )
        body = await alerts_body()
        assert [a["rule_id"] for a in body["actionable"]] == ["hum-high"]
        events, _ = store.events_after(0, 100)
        kinds = [(e["kind"], e["rule_id"]) for e in events]
        assert kinds[-2:] == [
            ("resolved", "temp-high"),
            ("unsuppressed", "hum-high"),
        ]
    finally:
        await scheduler.stop()
        await api_runner.cleanup()
        store.close()
        await metrics_runner.cleanup()
