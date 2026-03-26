from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    status_code: Optional[int]
    error_name: Optional[str]
    error_detail: Optional[str]
    latency_ms: Optional[int]


class ContentMismatchError(RuntimeError):
    pass


def _classify_exception(exc: BaseException) -> tuple[str, str]:
    return type(exc).__name__, str(exc)[:500]


def _decode_best_effort(body: bytes, *, charset: str | None) -> str:
    enc = charset or "utf-8"
    return body.decode(enc, errors="ignore")


async def probe_once(
    session: aiohttp.ClientSession,
    *,
    url: str,
    expect_substring: str,
    timeout_seconds: float,
    connect_timeout_seconds: float,
    max_bytes: int,
) -> ProbeResult:
    start = time.perf_counter()
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=connect_timeout_seconds)
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            status = int(resp.status)
            # read up to max_bytes without forcing full download
            body = await resp.content.read(max_bytes)

            latency_ms = int((time.perf_counter() - start) * 1000)

            if not (200 <= status < 400):
                return ProbeResult(
                    ok=False,
                    status_code=status,
                    error_name="BadStatus",
                    error_detail=f"HTTP {status}",
                    latency_ms=latency_ms,
                )

            if expect_substring:
                text = _decode_best_effort(body, charset=resp.charset)
                if expect_substring not in text:
                    raise ContentMismatchError(f"Expected substring not found: {expect_substring!r}")

            return ProbeResult(ok=True, status_code=status, error_name=None, error_detail=None, latency_ms=latency_ms)
    except (aiohttp.ClientError, asyncio.TimeoutError, ContentMismatchError) as exc:
        latency_ms = int((time.perf_counter() - start) * 1000)
        err_name, detail = _classify_exception(exc)
        return ProbeResult(ok=False, status_code=None, error_name=err_name, error_detail=detail, latency_ms=latency_ms)

