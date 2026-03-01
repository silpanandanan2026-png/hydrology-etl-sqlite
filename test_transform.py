from src.hydrology_api import choose_target_measures
from src.transform import build_star_rows


def test_choose_target_measures_exact_unit_match():
    measures = [
        {"measure_id": "m1", "parameter_name": "Conductivity", "unit_name": "µS/cm"},
        {"measure_id": "m2", "parameter_name": "Dissolved Oxygen", "unit_name": "%"},
        {"measure_id": "m3", "parameter_name": "Dissolved Oxygen", "unit_name": "mg/L"},
    ]
    targets = (("Conductivity", "µS/cm"), ("Dissolved Oxygen", "mg/L"))
    chosen = choose_target_measures(measures, targets)
    assert [m["measure_id"] for m in chosen] == ["m1", "m3"]


def test_build_star_rows_shapes():
    station = {
        "station_id": "E64999A",
        "station_label": "HIPPER_PARK ROAD BRIDGE_E_202312",
        "river_name": "HIPPER",
        "lat": "53.23",
        "long": "-1.43",
        "status": "Active",
        "date_opened": "2023-12-01",
    }
    measures = [
        {
            "measure_id": "E64999A-cond-i-subdaily-uS",
            "label": "Conductivity",
            "parameter_name": "Conductivity",
            "unit_name": "µS/cm",
            "observed_property": "conductivity",
            "period_seconds": "900",
            "period_name": "sub-daily",
            "value_type": "instantaneous",
            "observation_type": "Measured",
        }
    ]
    facts = [
        {
            "measure_id": "E64999A-cond-i-subdaily-uS",
            "reading_datetime": "2026-02-14T10:00:00+00:00",
            "reading_date": "2026-02-14",
            "value": "123.4",
            "quality": "Good",
            "completeness": None,
            "qcode": None,
        }
    ]

    station_row, parameter_rows, fact_rows = build_star_rows(station, measures, facts)
    assert station_row["station_id"] == "E64999A"
    assert isinstance(station_row["lat"], float)
    assert parameter_rows[0]["period_seconds"] == 900
    assert fact_rows[0]["value"] == 123.4
