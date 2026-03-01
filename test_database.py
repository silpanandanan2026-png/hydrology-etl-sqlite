from pathlib import Path

from src.database import connect, init_db, insert_facts, query_summary, upsert_parameters, upsert_station


def test_database_load_and_dedup(tmp_path: Path):
    db_path = tmp_path / "test.db"
    with connect(db_path) as cn:
        init_db(cn)
        station_key = upsert_station(
            cn,
            {
                "station_id": "E64999A",
                "station_label": "HIPPER_PARK ROAD BRIDGE_E_202312",
                "river_name": "HIPPER",
                "lat": 53.2,
                "long": -1.4,
                "status": "Active",
                "date_opened": "2023-12-01",
            },
        )
        parameter_map = upsert_parameters(
            cn,
            [
                {
                    "measure_id": "m_cond",
                    "label": "Conductivity",
                    "parameter_name": "Conductivity",
                    "unit_name": "µS/cm",
                    "observed_property": "conductivity",
                    "period_seconds": 900,
                    "period_name": "sub-daily",
                    "value_type": "instantaneous",
                    "observation_type": "Measured",
                }
            ],
        )

        rows = [
            {
                "measure_id": "m_cond",
                "reading_datetime_utc": "2026-02-14T10:00:00+00:00",
                "reading_date": "2026-02-14",
                "value": 123.4,
                "quality": "Good",
                "completeness": None,
                "qcode": None,
            },
            {
                "measure_id": "m_cond",
                "reading_datetime_utc": "2026-02-14T10:00:00+00:00",
                "reading_date": "2026-02-14",
                "value": 123.4,
                "quality": "Good",
                "completeness": None,
                "qcode": None,
            },
        ]
        inserted = insert_facts(cn, station_key=station_key, parameter_key_by_measure_id=parameter_map, rows=rows)
        summary = query_summary(cn)

    assert inserted == 1
    assert summary["dim_station_rows"] == 1
    assert summary["dim_parameter_rows"] == 1
    assert summary["fact_measurement_rows"] == 1
