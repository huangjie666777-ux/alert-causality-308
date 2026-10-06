"""Entry point: wires config, store, engine, scheduler and the HTTP server."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from aiohttp import web

from .config import ConfigError, load_config
from .engine import AlertEngine
from .scraper import ScrapeScheduler
from .server import make_app
from .store import Store

log = logging.getLogger("alert-causality-308")


async def run(config) -> None:
    store = Store(config.sqlite_path)
    recovered = store.startup_recovery()
    if recovered:
        log.info(
            "resolved %d previously firing alert(s) with reason 'restart'",
            recovered,
        )
    engine = AlertEngine(config, store)
    scheduler = ScrapeScheduler(config.targets, engine.handle_round)
    await scheduler.start()

    runner = web.AppRunner(make_app(config, store))
    await runner.setup()
    site = web.TCPSite(runner, config.host, config.port)
    await site.start()
    log.info("HTTP API listening on %s:%d", config.host, config.port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    log.info("shutting down")
    await scheduler.stop()  # cancel scrape tasks, close the HTTP client session
    await runner.cleanup()  # stop the API server and its connections
    store.close()
    log.info("shutdown complete")


def main(argv=None) -> None:
    arg_parser = argparse.ArgumentParser(
        prog="alert-causality-308",
        description="Metrics scraping and sustained-threshold alerting backend",
    )
    arg_parser.add_argument(
        "--config", default="config.json", help="path to the JSON configuration"
    )
    arg_parser.add_argument(
        "--check", action="store_true", help="validate configuration and exit"
    )
    arg_parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logging"
    )
    args = arg_parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2)
    if args.check:
        print(
            f"configuration OK: {len(config.targets)} target(s),"
            f" {len(config.rules)} rule(s)"
        )
        return
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
