"""Concurrent periodic scraping.

One asyncio task per target: rounds of the same target never overlap (the
task awaits each round before sleeping for the next interval), and targets
never block each other (independent tasks sharing one connection pool).
Timeouts, oversized responses, HTTP errors and parse errors all fail the
round as a whole; a failed round publishes no samples at all.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import aiohttp

from .config import TargetConfig
from .parser import ParseError, Sample, parse_text

log = logging.getLogger(__name__)

_CHUNK_SIZE = 65536


@dataclass
class ScrapeResult:
    ok: bool
    samples: list[Sample] | None
    error: str | None
    duration_ms: float


async def scrape_once(
    session: aiohttp.ClientSession, target: TargetConfig
) -> ScrapeResult:
    start = time.monotonic()

    def elapsed() -> float:
        return (time.monotonic() - start) * 1000.0

    def fail(message: str) -> ScrapeResult:
        return ScrapeResult(False, None, message, elapsed())

    try:
        timeout = aiohttp.ClientTimeout(total=target.timeout_seconds)
        async with session.get(target.url, timeout=timeout) as resp:
            if resp.status != 200:
                return fail(f"unexpected HTTP status {resp.status}")
            if (
                resp.content_length is not None
                and resp.content_length > target.max_response_bytes
            ):
                return fail(
                    f"response too large: Content-Length {resp.content_length}"
                    f" exceeds limit of {target.max_response_bytes} bytes"
                )
            chunks: list[bytes] = []
            total = 0
            async for chunk in resp.content.iter_chunked(_CHUNK_SIZE):
                total += len(chunk)
                if total > target.max_response_bytes:
                    return fail(
                        f"response exceeds limit of"
                        f" {target.max_response_bytes} bytes"
                    )
                chunks.append(chunk)
        try:
            text = b"".join(chunks).decode("utf-8")
        except UnicodeDecodeError as exc:
            return fail(f"response is not valid UTF-8: {exc}")
        samples = parse_text(text)
        return ScrapeResult(True, samples, None, elapsed())
    except TimeoutError:
        return fail(f"timeout after {target.timeout_seconds}s")
    except aiohttp.ClientError as exc:
        return fail(f"connection error: {exc}")
    except ParseError as exc:
        return fail(f"parse error: {exc}")


class ScrapeScheduler:
    """Runs one scrape loop per target and fans results out to a callback."""

    def __init__(self, targets, on_round):
        # on_round(target, result) is a synchronous callable; round
        # processing (state machine + SQLite) is fast and never awaits.
        self._targets = targets
        self._on_round = on_round
        self._session: aiohttp.ClientSession | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        for target in self._targets:
            self._tasks.append(
                asyncio.create_task(self._run(target), name=f"scrape:{target.id}")
            )

    async def _run(self, target: TargetConfig) -> None:
        while True:
            result = await scrape_once(self._session, target)
            try:
                self._on_round(target, result)
            except Exception:
                log.exception("round processing failed for target %s", target.id)
            await asyncio.sleep(target.interval_seconds)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None
