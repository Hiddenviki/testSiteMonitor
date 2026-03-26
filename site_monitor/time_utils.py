from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class DayWindow:
    day_start_utc: int
    day_end_utc_exclusive: int


def utc_now_epoch() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def epoch_to_local_str(ts_utc: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    dt = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(tz)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def day_window_utc(day: date, tz_name: str) -> DayWindow:
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(day, time(0, 0, 0), tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return DayWindow(
        day_start_utc=int(start_local.astimezone(timezone.utc).timestamp()),
        day_end_utc_exclusive=int(end_local.astimezone(timezone.utc).timestamp()),
    )

