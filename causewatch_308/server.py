"""HTTP query API: target health, latest samples, alerts, firings, events."""

from __future__ import annotations

from aiohttp import web

from .config import AppConfig
from .store import Store

_MAX_LIMIT = 500
CONFIG_KEY = web.AppKey("config", AppConfig)
STORE_KEY = web.AppKey("store", Store)


def _error(status: int, message: str) -> web.Response:
    return web.json_response({"error": message}, status=status)


async def list_targets(request: web.Request) -> web.Response:
    config: AppConfig = request.app[CONFIG_KEY]
    store: Store = request.app[STORE_KEY]
    health = store.targets_health()
    rules_by_target: dict[str, list[str]] = {}
    for rule in config.rules:
        rules_by_target.setdefault(rule.target_id, []).append(rule.id)
    targets = [
        {
            "id": t.id,
            "url": t.url,
            "interval_seconds": t.interval_seconds,
            "timeout_seconds": t.timeout_seconds,
            "max_response_bytes": t.max_response_bytes,
            "rules": rules_by_target.get(t.id, []),
            "health": health.get(t.id),
        }
        for t in config.targets
    ]
    return web.json_response({"targets": targets})


async def target_samples(request: web.Request) -> web.Response:
    config: AppConfig = request.app[CONFIG_KEY]
    store: Store = request.app[STORE_KEY]
    target_id = request.match_info["target_id"]
    if target_id not in config.targets_by_id:
        return _error(404, f"unknown target {target_id!r}")
    data = store.latest_samples(target_id)
    if data is None:
        data = {"round_id": None, "scraped_at": None, "samples": []}
    data["target_id"] = target_id
    return web.json_response(data)


async def list_alerts(request: web.Request) -> web.Response:
    store: Store = request.app[STORE_KEY]
    return web.json_response({"alerts": store.active_alerts()})


async def list_firing(request: web.Request) -> web.Response:
    """Actionable alerts: firing instances that are not suppressed."""
    store: Store = request.app[STORE_KEY]
    return web.json_response({"alerts": store.actionable_firings()})


async def list_events(request: web.Request) -> web.Response:
    store: Store = request.app[STORE_KEY]
    try:
        after_id = int(request.query.get("after_id", "0"))
        limit = int(request.query.get("limit", "50"))
    except ValueError:
        return _error(400, "after_id and limit must be integers")
    if after_id < 0:
        return _error(400, "after_id must be >= 0")
    if not (1 <= limit <= _MAX_LIMIT):
        return _error(400, f"limit must be between 1 and {_MAX_LIMIT}")
    events, has_more = store.events_after(after_id, limit)
    return web.json_response(
        {
            "events": events,
            "next_after_id": events[-1]["id"] if events else after_id,
            "has_more": has_more,
        }
    )


def make_app(config: AppConfig, store: Store) -> web.Application:
    app = web.Application()
    app[CONFIG_KEY] = config
    app[STORE_KEY] = store
    app.router.add_get("/api/targets", list_targets)
    app.router.add_get("/api/targets/{target_id}/samples", target_samples)
    app.router.add_get("/api/alerts", list_alerts)
    app.router.add_get("/api/firing", list_firing)
    app.router.add_get("/api/events", list_events)
    return app
