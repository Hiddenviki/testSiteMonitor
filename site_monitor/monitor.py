from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp

from . import db as db_mod
from .config import AppConfig
from .probe import ProbeResult, probe_once
from .time_utils import epoch_to_local_str, utc_now_epoch


@dataclass
class SiteState:
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    open_incident_id: Optional[int] = None


def _fmt_down(org_name: str, ts_utc: int, tz_name: str, error_name: str) -> str:
    # Required format: "Org – time – error"
    return f"{org_name} – {epoch_to_local_str(ts_utc, tz_name)} – {error_name}"


def _fmt_up(org_name: str, ts_utc: int, tz_name: str) -> str:
    # Required format: "Org – time"
    return f"{org_name} – {epoch_to_local_str(ts_utc, tz_name)}"


async def _check_site_once(
    *,
    session: aiohttp.ClientSession,
    config: AppConfig,
    site,
) -> tuple[int, ProbeResult]:
    ts_utc = utc_now_epoch()
    res = await probe_once(
        session,
        url=site.url,
        expect_substring=site.expect,
        timeout_seconds=config.probe.timeout_seconds,
        connect_timeout_seconds=config.probe.connect_timeout_seconds,
        max_bytes=config.probe.max_response_bytes_to_check,
    )
    return ts_utc, res


def _persist_check(conn, *, site_id: int, ts_utc: int, res: ProbeResult) -> None:
    db_mod.insert_check(
        conn,
        site_id=site_id,
        ts_utc=ts_utc,
        ok=res.ok,
        status_code=res.status_code,
        error_name=res.error_name,
        error_detail=res.error_detail,
        latency_ms=res.latency_ms,
    )


def _update_incident_state(
    *,
    conn,
    logger,
    config: AppConfig,
    site,
    ts_utc: int,
    res: ProbeResult,
    st: SiteState,
) -> None:
    if res.ok:
        st.consecutive_successes += 1
        st.consecutive_failures = 0

        if (
            st.open_incident_id is not None
            and st.consecutive_successes >= config.probe.confirm_up_after_consecutive_successes
        ):
            db_mod.end_incident(conn, incident_id=st.open_incident_id, up_ts_utc=ts_utc)
            logger.info(_fmt_up(site.name, ts_utc, config.timezone))
            st.open_incident_id = None
        return

    st.consecutive_failures += 1
    st.consecutive_successes = 0

    if st.open_incident_id is None and st.consecutive_failures >= config.probe.confirm_down_after_consecutive_failures:
        err = res.error_name or "Error"
        incident_id = db_mod.start_incident(conn, site_id=site.id, down_ts_utc=ts_utc, error_name=err)
        st.open_incident_id = incident_id
        logger.error(_fmt_down(site.name, ts_utc, config.timezone, err))


async def run_monitor_iteration(
    *,
    config: AppConfig,
    conn,
    session: aiohttp.ClientSession,
    sites,
    state: dict[int, SiteState],
    logger,
) -> None:
    for site in sites:
        ts_utc, res = await _check_site_once(session=session, config=config, site=site)
        _persist_check(conn, site_id=site.id, ts_utc=ts_utc, res=res)
        _update_incident_state(conn=conn, logger=logger, config=config, site=site, ts_utc=ts_utc, res=res, st=state[site.id])


async def run_monitor(
    *,
    config: AppConfig,
    db_path: Path,
    logger,
) -> None:
    conn = db_mod.connect(db_path)
    db_mod.init_schema(conn)
    db_mod.upsert_sites(conn, [s.__dict__ for s in config.sites])
    sites = db_mod.get_sites(conn)

    state: dict[int, SiteState] = {}
    for s in sites:
        state[s.id] = SiteState(open_incident_id=db_mod.get_open_incident_id(conn, s.id))

    headers = {"User-Agent": config.probe.user_agent}
    async with aiohttp.ClientSession(headers=headers) as session:
        logger.info(f"Monitoring started. Sites: {len(sites)}. Interval: {config.probe.interval_seconds}s")
        while True:
            await run_monitor_iteration(
                config=config,
                conn=conn,
                session=session,
                sites=sites,
                state=state,
                logger=logger,
            )
            await asyncio.sleep(config.probe.interval_seconds)

