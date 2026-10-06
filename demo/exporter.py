"""Demo metrics exporter used by the README walkthrough.

Serves a couple of gauges in the Prometheus 0.0.4 text format on /metrics
and lets you change their values through /set?metric=...&value=... so alert
triggering and recovery can be demonstrated with curl.
"""

from __future__ import annotations

import sys

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

registry = CollectorRegistry()
GAUGES = {
    "demo_temperature_celsius": Gauge(
        "demo_temperature_celsius",
        "Demo room temperature in celsius",
        ["room"],
        registry=registry,
    ),
    "demo_humidity_percent": Gauge(
        "demo_humidity_percent",
        "Demo room humidity in percent",
        ["room"],
        registry=registry,
    ),
}
GAUGES["demo_temperature_celsius"].labels(room="server").set(21.0)
GAUGES["demo_humidity_percent"].labels(room="server").set(45.0)


async def metrics(request: web.Request) -> web.Response:
    return web.Response(body=generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def set_value(request: web.Request) -> web.Response:
    name = request.query.get("metric", "")
    if name not in GAUGES:
        return web.json_response(
            {"error": f"unknown metric {name!r}", "known": sorted(GAUGES)}, status=400
        )
    try:
        value = float(request.query.get("value", ""))
    except ValueError:
        return web.json_response({"error": "value must be a number"}, status=400)
    GAUGES[name].labels(room="server").set(value)
    return web.json_response({"ok": True, "metric": name, "value": value})


async def index(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "endpoints": {
                "/metrics": "Prometheus 0.0.4 exposition",
                "/set?metric=<name>&value=<number>": "update a gauge",
            },
            "metrics": sorted(GAUGES),
        }
    )


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9101
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/set", set_value)
    app.router.add_post("/set", set_value)
    web.run_app(app, host="127.0.0.1", port=port, print=None)


if __name__ == "__main__":
    main()
