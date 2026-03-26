from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ProbeConfig:
    timeout_seconds: float
    connect_timeout_seconds: float
    interval_seconds: float
    user_agent: str
    confirm_down_after_consecutive_failures: int
    confirm_up_after_consecutive_successes: int
    max_response_bytes_to_check: int


@dataclass(frozen=True)
class SiteConfig:
    name: str
    url: str
    expect: str


@dataclass(frozen=True)
class AppConfig:
    timezone: str
    sites: list[SiteConfig]
    probe: ProbeConfig


def _require(obj: dict[str, Any], key: str, typ: type, *, where: str) -> Any:
    if key not in obj:
        raise ValueError(f"Missing key '{key}' in {where}")
    val = obj[key]
    if not isinstance(val, typ):
        raise ValueError(f"Key '{key}' must be {typ.__name__} in {where}")
    return val


def load_config(path: Path) -> AppConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config must be a JSON object")

    timezone = _require(raw, "timezone", str, where="config")
    sites_raw = _require(raw, "sites", list, where="config")
    probe_raw = _require(raw, "probe", dict, where="config")

    sites: list[SiteConfig] = []
    for i, s in enumerate(sites_raw):
        if not isinstance(s, dict):
            raise ValueError(f"sites[{i}] must be an object")
        name = _require(s, "name", str, where=f"sites[{i}]")
        url = _require(s, "url", str, where=f"sites[{i}]")
        expect = _require(s, "expect", str, where=f"sites[{i}]")
        sites.append(SiteConfig(name=name, url=url, expect=expect))

    probe = ProbeConfig(
        timeout_seconds=float(_require(probe_raw, "timeout_seconds", (int, float), where="probe")),
        connect_timeout_seconds=float(
            _require(probe_raw, "connect_timeout_seconds", (int, float), where="probe")
        ),
        interval_seconds=float(_require(probe_raw, "interval_seconds", (int, float), where="probe")),
        user_agent=_require(probe_raw, "user_agent", str, where="probe"),
        confirm_down_after_consecutive_failures=int(
            _require(probe_raw, "confirm_down_after_consecutive_failures", int, where="probe")
        ),
        confirm_up_after_consecutive_successes=int(
            _require(probe_raw, "confirm_up_after_consecutive_successes", int, where="probe")
        ),
        max_response_bytes_to_check=int(_require(probe_raw, "max_response_bytes_to_check", int, where="probe")),
    )

    if probe.confirm_down_after_consecutive_failures < 1:
        raise ValueError("confirm_down_after_consecutive_failures must be >= 1")
    if probe.confirm_up_after_consecutive_successes < 1:
        raise ValueError("confirm_up_after_consecutive_successes must be >= 1")
    if probe.interval_seconds <= 0:
        raise ValueError("interval_seconds must be > 0")
    if probe.max_response_bytes_to_check <= 0:
        raise ValueError("max_response_bytes_to_check must be > 0")

    return AppConfig(timezone=timezone, sites=sites, probe=probe)

