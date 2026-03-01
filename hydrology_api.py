from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
import requests


def _json_get(session: requests.Session, url: str, params: dict[str, Any], timeout: int) -> dict[str, Any]:
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _as_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _clean_resource_name(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("label") or value.get("notation") or value.get("value") or value.get("@id")
    if isinstance(value, str):
        if value.startswith("http"):
            return value.rstrip("/").split("/")[-1]
        return value
    return str(value)


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_dt_to_iso(value: str) -> str:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return str(value)


def _parse_dt_to_date(value: str) -> str:
    text = _parse_dt_to_iso(value)
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        return str(value)[:10]


def find_station_by_label(
    session: requests.Session,
    *,
    api_base: str,
    station_label: str,
    timeout: int,
) -> dict[str, Any]:
    url = f"{api_base}/id/stations.json"
    payload = _json_get(session, url, params={"search": station_label, "_limit": 50}, timeout=timeout)
    stations = _as_list(payload)

    exact = [s for s in stations if str(s.get("label", "")).strip().lower() == station_label.strip().lower()]
    chosen = exact[0] if exact else (stations[0] if stations else None)
    if not chosen:
        raise ValueError(f"Station not found for label: {station_label}")

    station = {
        "station_id": chosen.get("notation") or chosen.get("stationReference") or chosen.get("@id"),
        "station_label": chosen.get("label", station_label),
        "river_name": chosen.get("riverName"),
        "lat": chosen.get("lat"),
        "long": chosen.get("long"),
        "status": _clean_resource_name(chosen.get("status")),
        "date_opened": chosen.get("dateOpened"),
        "raw": chosen,
    }
    if not station["station_id"]:
        raise ValueError(f"Station found but identifier missing: {chosen}")
    return station


def list_station_measures(
    session: requests.Session,
    *,
    api_base: str,
    station_id: str,
    timeout: int,
) -> list[dict[str, Any]]:
    url = f"{api_base}/id/stations/{station_id}/measures.json"
    payload = _json_get(session, url, params={"_limit": 200}, timeout=timeout)
    measures = _as_list(payload)

    parsed: list[dict[str, Any]] = []
    for m in measures:
        measure_id = m.get("notation") or m.get("@id")
        if not measure_id:
            continue
        parsed.append(
            {
                "measure_id": measure_id if isinstance(measure_id, str) else str(measure_id),
                "label": m.get("label"),
                "parameter_name": m.get("parameterName") or m.get("parameter"),
                "unit_name": m.get("unitName") or _clean_resource_name(m.get("unit")),
                "observed_property": _clean_resource_name(m.get("observedProperty")),
                "period_seconds": _safe_int(m.get("period")),
                "period_name": m.get("periodName"),
                "value_type": m.get("valueType"),
                "observation_type": _clean_resource_name(m.get("observationType")),
                "raw": m,
            }
        )
    if not parsed:
        raise ValueError(f"No measures returned for station_id={station_id}")
    return parsed


def choose_target_measures(
    measures: list[dict[str, Any]],
    targets: tuple[tuple[str, str | None], ...],
) -> list[dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for target_param, target_unit in targets:
        candidates = [
            m
            for m in measures
            if str(m.get("parameter_name", "")).strip().lower() == target_param.strip().lower()
        ]

        if target_unit:
            exact_unit = [
                m
                for m in candidates
                if str(m.get("unit_name", "")).strip().lower() == target_unit.strip().lower()
            ]
            if exact_unit:
                pick = exact_unit[0]
            elif candidates:
                pick = candidates[0]
            else:
                raise ValueError(f"No measure found for parameter='{target_param}' (unit='{target_unit}')")
        else:
            if not candidates:
                raise ValueError(f"No measure found for parameter='{target_param}'")
            pick = candidates[0]

        if pick["measure_id"] not in seen_ids:
            chosen.append(pick)
            seen_ids.add(pick["measure_id"])

    if len(chosen) != len(targets):
        raise ValueError(f"Expected {len(targets)} measures, selected {len(chosen)}")
    return chosen


def fetch_latest_n_readings_for_measure(
    session: requests.Session,
    *,
    api_base: str,
    measure_id: str,
    latest_n: int,
    timeout: int,
) -> list[dict[str, Any]]:
    """
    Fetch by increasing lookback windows, sort client-side, keep latest_n.
    This is robust even if API default ordering changes.
    """
    windows_days = [30, 90, 365, 3650]
    all_rows: list[dict[str, Any]] = []
    url = f"{api_base}/id/measures/{measure_id}/readings.json"

    for days in windows_days:
        min_date = (date.today() - timedelta(days=days)).isoformat()
        payload = _json_get(session, url, params={"mineq-date": min_date}, timeout=timeout)
        rows = _as_list(payload)
        if rows:
            all_rows = rows
            if len(rows) >= latest_n:
                break

    if not all_rows:
        return []

    parsed: list[dict[str, Any]] = []
    for r in all_rows:
        dt_text = r.get("dateTime") or r.get("date")
        if not dt_text:
            continue
        parsed.append(
            {
                "reading_datetime": _parse_dt_to_iso(dt_text),
                "reading_date": str(r.get("date") or _parse_dt_to_date(dt_text)),
                "value": _safe_float(r.get("value")),
                "quality": r.get("quality"),
                "completeness": r.get("completeness"),
                "qcode": r.get("qcode"),
                "raw": r,
            }
        )

    parsed.sort(key=lambda x: x["reading_datetime"] or "", reverse=True)
    return parsed[:latest_n]


def extract_station_and_readings(
    session: requests.Session,
    *,
    api_base: str,
    station_label: str,
    targets: tuple[tuple[str, str | None], ...],
    latest_n: int,
    timeout: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    station = find_station_by_label(session, api_base=api_base, station_label=station_label, timeout=timeout)
    measures = list_station_measures(session, api_base=api_base, station_id=station["station_id"], timeout=timeout)
    chosen_measures = choose_target_measures(measures, targets)

    facts: list[dict[str, Any]] = []
    for measure in chosen_measures:
        rows = fetch_latest_n_readings_for_measure(
            session,
            api_base=api_base,
            measure_id=measure["measure_id"],
            latest_n=latest_n,
            timeout=timeout,
        )
        for row in rows:
            facts.append({**row, "measure_id": measure["measure_id"]})

    facts.sort(key=lambda x: (x["reading_datetime"] or "", x["measure_id"]), reverse=True)
    return station, chosen_measures, facts
