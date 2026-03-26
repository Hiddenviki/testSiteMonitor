from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from . import db as db_mod
from .time_utils import DayWindow, day_window_utc


@dataclass(frozen=True)
class ReportRow:
    name: str
    uptime_percent: float
    downtime_seconds: int


def _clip_interval(down: int, up: int, window: DayWindow) -> int:
    start = max(down, window.day_start_utc)
    end = min(up, window.day_end_utc_exclusive)
    return max(0, end - start)


def build_daily_report(*, db_path: Path, tz_name: str, day: date) -> list[ReportRow]:
    conn = db_mod.connect(db_path)
    db_mod.init_schema(conn)
    sites = db_mod.get_sites(conn)

    window = day_window_utc(day, tz_name)
    day_seconds = max(1, window.day_end_utc_exclusive - window.day_start_utc)

    rows: list[ReportRow] = []
    for s in sites:
        downtime = 0
        for down_ts, up_ts in db_mod.iter_incidents_overlapping_day(
            conn, site_id=s.id, day_start_utc=window.day_start_utc, day_end_utc=window.day_end_utc_exclusive - 1
        ):
            up = up_ts if up_ts is not None else window.day_end_utc_exclusive
            downtime += _clip_interval(down_ts, up, window)

        uptime = (max(0, day_seconds - downtime) / day_seconds) * 100.0
        rows.append(ReportRow(name=s.name, uptime_percent=uptime, downtime_seconds=int(downtime)))

    rows.sort(key=lambda r: r.name)
    return rows


def format_seconds(total_seconds: int) -> str:
    s = int(max(0, total_seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def print_report(rows: list[ReportRow]) -> None:
    headers = ["Наименование организации", "Uptime, %", "Общее время недоступности сайта за период"]
    org_w = max([len(headers[0]), *[len(r.name) for r in rows]] or [len(headers[0])])
    up_w = len(headers[1])
    down_w = len(headers[2])

    def pad(s: str, w: int) -> str:
        return s + " " * max(0, w - len(s))

    print(f"{pad(headers[0], org_w)} | {pad(headers[1], up_w)} | {headers[2]}")
    print(f"{'-' * org_w}-+-{'-' * up_w}-+-{'-' * down_w}")
    for r in rows:
        uptime_str = f"{r.uptime_percent:.2f}%"
        print(f"{pad(r.name, org_w)} | {pad(uptime_str, up_w)} | {format_seconds(r.downtime_seconds)}")


def write_csv(rows: list[ReportRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "uptime_percent", "downtime_seconds", "downtime_hhmmss"])
        for r in rows:
            w.writerow([r.name, f"{r.uptime_percent:.2f}", r.downtime_seconds, format_seconds(r.downtime_seconds)])

