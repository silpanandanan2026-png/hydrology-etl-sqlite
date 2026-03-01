from __future__ import annotations

from typing import Any


def build_star_rows(
    station: dict[str, Any],
    measures: list[dict[str, Any]],
    facts: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Create row dictionaries suitable for SQLite insertion."""
    station_row = {
        "station_id": station.get("station_id"),
        "station_label": station.get("station_label"),
        "river_name": station.get("river_name"),
        "lat": _to_float(station.get("lat")),
        "long": _to_float(station.get("long")),
        "status": station.get("status"),
        "date_opened": station.get("date_opened"),
    }

    parameter_rows = []
    for m in measures:
        parameter_rows.append(
            {
                "measure_id": m.get("measure_id"),
                "label": m.get("label"),
                "parameter_name": m.get("parameter_name"),
                "unit_name": m.get("unit_name"),
                "observed_property": m.get("observed_property"),
                "period_seconds": _to_int(m.get("period_seconds")),
                "period_name": m.get("period_name"),
                "value_type": m.get("value_type"),
                "observation_type": m.get("observation_type"),
            }
        )

    fact_rows = []
    for f in facts:
        fact_rows.append(
            {
                "measure_id": f.get("measure_id"),
                "reading_datetime_utc": f.get("reading_datetime"),
                "reading_date": f.get("reading_date"),
                "value": _to_float(f.get("value")),
                "quality": f.get("quality"),
                "completeness": f.get("completeness"),
                "qcode": f.get("qcode"),
            }
        )

    return station_row, parameter_rows, fact_rows


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
