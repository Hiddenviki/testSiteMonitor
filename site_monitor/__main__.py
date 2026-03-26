from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import aiohttp

from . import db as db_mod
from .config import load_config
from .logging_utils import setup_logging
from .monitor import SiteState, run_monitor, run_monitor_iteration
from .report import build_daily_report, print_report, write_csv


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="site_monitor", description="Website availability monitor (test task).")
    p.add_argument("--config", default="sites.json", help="Path to sites.json")
    p.add_argument("--db", default="data/monitor.sqlite3", help="Path to SQLite DB")
    p.add_argument("--log", default="logs/monitor.log", help="Path to log file")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose console output")

    sub = p.add_subparsers(dest="cmd", required=True)

    mon = sub.add_parser("monitor", help="Run continuous monitoring loop")
    mon.add_argument("--once", action="store_true", help="Run single check iteration and exit")

    rep = sub.add_parser("report", help="Build daily report")
    rep.add_argument("--date", default=str(date.today()), help="Day in YYYY-MM-DD (local timezone from config)")
    rep.add_argument("--csv", default="", help="Optional path to write CSV report")

    return p.parse_args()


async def _run_monitor_cmd(args: argparse.Namespace) -> None:
    config = load_config(Path(args.config))
    logger = setup_logging(Path(args.log), verbose=args.verbose)

    if args.once:
        conn = db_mod.connect(Path(args.db))
        db_mod.init_schema(conn)
        db_mod.upsert_sites(conn, [s.__dict__ for s in config.sites])
        sites = db_mod.get_sites(conn)

        state = {s.id: SiteState(open_incident_id=db_mod.get_open_incident_id(conn, s.id)) for s in sites}

        headers = {"User-Agent": config.probe.user_agent}
        async with aiohttp.ClientSession(headers=headers) as session:
            logger.info(f"Monitoring started. Sites: {len(sites)}. Interval: {config.probe.interval_seconds}s")
            await run_monitor_iteration(
                config=config,
                conn=conn,
                session=session,
                sites=sites,
                state=state,
                logger=logger,
            )
    else:
        await run_monitor(config=config, db_path=Path(args.db), logger=logger)


def _run_report_cmd(args: argparse.Namespace) -> None:
    config = load_config(Path(args.config))
    day = date.fromisoformat(args.date)
    rows = build_daily_report(db_path=Path(args.db), tz_name=config.timezone, day=day)
    print_report(rows)
    if args.csv:
        write_csv(rows, Path(args.csv))


def main() -> None:
    args = _parse_args()
    if args.cmd == "monitor":
        import asyncio

        asyncio.run(_run_monitor_cmd(args))
    elif args.cmd == "report":
        _run_report_cmd(args)
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

