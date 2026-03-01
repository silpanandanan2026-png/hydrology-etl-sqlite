from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Any


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_station (
    station_key      INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id       TEXT NOT NULL UNIQUE,
    station_label    TEXT NOT NULL,
    river_name       TEXT,
    lat              REAL,
    long             REAL,
    status           TEXT,
    date_opened      TEXT
);

CREATE TABLE IF NOT EXISTS dim_parameter (
    parameter_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    measure_id        TEXT NOT NULL UNIQUE,
    label             TEXT,
    parameter_name    TEXT NOT NULL,
    unit_name         TEXT,
    observed_property TEXT,
    period_seconds    INTEGER,
    period_name       TEXT,
    value_type        TEXT,
    observation_type  TEXT
);

CREATE TABLE IF NOT EXISTS fact_measurement (
    measurement_key      INTEGER PRIMARY KEY AUTOINCREMENT,
    station_key          INTEGER NOT NULL,
    parameter_key        INTEGER NOT NULL,
    source_measure_id    TEXT NOT NULL,
    reading_datetime_utc TEXT NOT NULL,
    reading_date         TEXT NOT NULL,
    value                REAL,
    quality              TEXT,
    completeness         TEXT,
    qcode                TEXT,
    load_utc             TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_key) REFERENCES dim_station(station_key),
    FOREIGN KEY (parameter_key) REFERENCES dim_parameter(parameter_key),
    UNIQUE(source_measure_id, reading_datetime_utc)
);

CREATE INDEX IF NOT EXISTS idx_fact_measurement_date ON fact_measurement(reading_date);
CREATE INDEX IF NOT EXISTS idx_fact_measurement_station ON fact_measurement(station_key);
CREATE INDEX IF NOT EXISTS idx_fact_measurement_param ON fact_measurement(parameter_key);
"""


def connect(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cn = sqlite3.connect(path)
    cn.row_factory = sqlite3.Row
    return cn


def init_db(cn: sqlite3.Connection) -> None:
    cn.executescript(SCHEMA_SQL)
    cn.commit()


def upsert_station(cn: sqlite3.Connection, row: dict[str, Any]) -> int:
    cn.execute(
        """
        INSERT INTO dim_station(station_id, station_label, river_name, lat, long, status, date_opened)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station_id) DO UPDATE SET
            station_label=excluded.station_label,
            river_name=excluded.river_name,
            lat=excluded.lat,
            long=excluded.long,
            status=excluded.status,
            date_opened=excluded.date_opened
        """,
        (
            row["station_id"],
            row["station_label"],
            row.get("river_name"),
            row.get("lat"),
            row.get("long"),
            row.get("status"),
            row.get("date_opened"),
        ),
    )
    cn.commit()
    return get_station_key(cn, row["station_id"])


def upsert_parameters(cn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    for r in rows:
        cn.execute(
            """
            INSERT INTO dim_parameter(
                measure_id, label, parameter_name, unit_name, observed_property,
                period_seconds, period_name, value_type, observation_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(measure_id) DO UPDATE SET
                label=excluded.label,
                parameter_name=excluded.parameter_name,
                unit_name=excluded.unit_name,
                observed_property=excluded.observed_property,
                period_seconds=excluded.period_seconds,
                period_name=excluded.period_name,
                value_type=excluded.value_type,
                observation_type=excluded.observation_type
            """,
            (
                r["measure_id"],
                r.get("label"),
                r["parameter_name"],
                r.get("unit_name"),
                r.get("observed_property"),
                r.get("period_seconds"),
                r.get("period_name"),
                r.get("value_type"),
                r.get("observation_type"),
            ),
        )
    cn.commit()

    mapping: dict[str, int] = {}
    cur = cn.execute("SELECT measure_id, parameter_key FROM dim_parameter")
    for row in cur.fetchall():
        mapping[str(row["measure_id"])] = int(row["parameter_key"])
    return mapping


def insert_facts(
    cn: sqlite3.Connection,
    *,
    station_key: int,
    parameter_key_by_measure_id: dict[str, int],
    rows: Iterable[dict[str, Any]],
) -> int:
    inserted = 0
    for r in rows:
        parameter_key = parameter_key_by_measure_id[r["measure_id"]]
        cur = cn.execute(
            """
            INSERT OR IGNORE INTO fact_measurement(
                station_key, parameter_key, source_measure_id,
                reading_datetime_utc, reading_date, value, quality, completeness, qcode
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                station_key,
                parameter_key,
                r["measure_id"],
                r["reading_datetime_utc"],
                r["reading_date"],
                r.get("value"),
                r.get("quality"),
                r.get("completeness"),
                r.get("qcode"),
            ),
        )
        inserted += cur.rowcount
    cn.commit()
    return inserted


def get_station_key(cn: sqlite3.Connection, station_id: str) -> int:
    cur = cn.execute("SELECT station_key FROM dim_station WHERE station_id = ?", (station_id,))
    row = cur.fetchone()
    if not row:
        raise KeyError(f"station_id not found in dim_station: {station_id}")
    return int(row["station_key"])


def query_summary(cn: sqlite3.Connection) -> dict[str, Any]:
    summary = {}
    summary["dim_station_rows"] = _scalar(cn, "SELECT COUNT(*) FROM dim_station")
    summary["dim_parameter_rows"] = _scalar(cn, "SELECT COUNT(*) FROM dim_parameter")
    summary["fact_measurement_rows"] = _scalar(cn, "SELECT COUNT(*) FROM fact_measurement")

    summary["latest_by_parameter"] = [
        dict(row)
        for row in cn.execute(
            """
            SELECT p.parameter_name,
                   p.unit_name,
                   COUNT(*) AS row_count,
                   MAX(f.reading_datetime_utc) AS latest_reading_utc
            FROM fact_measurement f
            JOIN dim_parameter p ON p.parameter_key = f.parameter_key
            GROUP BY p.parameter_name, p.unit_name
            ORDER BY p.parameter_name, p.unit_name
            """
        ).fetchall()
    ]
    return summary


def _scalar(cn: sqlite3.Connection, sql: str) -> int:
    cur = cn.execute(sql)
    row = cur.fetchone()
    return int(row[0]) if row else 0
