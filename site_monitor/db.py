from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class SiteRow:
    id: int
    name: str
    url: str
    expect: str


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            expect TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL,
            ts_utc INTEGER NOT NULL,
            ok INTEGER NOT NULL,
            status_code INTEGER,
            error_name TEXT,
            error_detail TEXT,
            latency_ms INTEGER,
            FOREIGN KEY(site_id) REFERENCES sites(id)
        );
        CREATE INDEX IF NOT EXISTS idx_checks_site_ts ON checks(site_id, ts_utc);

        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL,
            down_ts_utc INTEGER NOT NULL,
            up_ts_utc INTEGER,
            error_name TEXT NOT NULL,
            FOREIGN KEY(site_id) REFERENCES sites(id)
        );
        CREATE INDEX IF NOT EXISTS idx_incidents_site_down ON incidents(site_id, down_ts_utc);
        CREATE INDEX IF NOT EXISTS idx_incidents_site_up ON incidents(site_id, up_ts_utc);
        """
    )
    conn.commit()


def upsert_sites(conn: sqlite3.Connection, sites: Iterable[dict]) -> None:
    conn.executemany(
        """
        INSERT INTO sites(name, url, expect)
        VALUES(?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET url=excluded.url, expect=excluded.expect
        """,
        [(s["name"], s["url"], s.get("expect", "")) for s in sites],
    )
    conn.commit()


def get_sites(conn: sqlite3.Connection) -> list[SiteRow]:
    cur = conn.execute("SELECT id, name, url, expect FROM sites ORDER BY name ASC;")
    return [SiteRow(*row) for row in cur.fetchall()]


def insert_check(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    ts_utc: int,
    ok: bool,
    status_code: Optional[int],
    error_name: Optional[str],
    error_detail: Optional[str],
    latency_ms: Optional[int],
) -> None:
    conn.execute(
        """
        INSERT INTO checks(site_id, ts_utc, ok, status_code, error_name, error_detail, latency_ms)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            ts_utc,
            1 if ok else 0,
            status_code,
            error_name,
            error_detail,
            latency_ms,
        ),
    )
    conn.commit()


def get_open_incident_id(conn: sqlite3.Connection, site_id: int) -> Optional[int]:
    cur = conn.execute(
        """
        SELECT id FROM incidents
        WHERE site_id=? AND up_ts_utc IS NULL
        ORDER BY down_ts_utc DESC
        LIMIT 1
        """,
        (site_id,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def start_incident(conn: sqlite3.Connection, *, site_id: int, down_ts_utc: int, error_name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO incidents(site_id, down_ts_utc, up_ts_utc, error_name)
        VALUES(?, ?, NULL, ?)
        """,
        (site_id, down_ts_utc, error_name),
    )
    conn.commit()
    return int(cur.lastrowid)


def end_incident(conn: sqlite3.Connection, *, incident_id: int, up_ts_utc: int) -> None:
    conn.execute(
        "UPDATE incidents SET up_ts_utc=? WHERE id=?;",
        (up_ts_utc, incident_id),
    )
    conn.commit()


def iter_incidents_overlapping_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    day_start_utc: int,
    day_end_utc: int,
):
    cur = conn.execute(
        """
        SELECT down_ts_utc, up_ts_utc
        FROM incidents
        WHERE site_id=?
          AND down_ts_utc <= ?
          AND (up_ts_utc IS NULL OR up_ts_utc >= ?)
        ORDER BY down_ts_utc ASC
        """,
        (site_id, day_end_utc, day_start_utc),
    )
    for down_ts_utc, up_ts_utc in cur.fetchall():
        yield int(down_ts_utc), (int(up_ts_utc) if up_ts_utc is not None else None)

