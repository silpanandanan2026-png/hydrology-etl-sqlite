from src.pipeline import run_pipeline
from src.settings import Settings


def test_run_pipeline_with_mocked_extraction(monkeypatch, tmp_path):
    station = {
        "station_id": "E64999A",
        "station_label": "HIPPER_PARK ROAD BRIDGE_E_202312",
        "river_name": "HIPPER",
        "lat": 53.23,
        "long": -1.43,
        "status": "Active",
        "date_opened": "2023-12-01",
    }
    measures = [
        {
            "measure_id": "m_cond",
            "label": "Sub-daily Conductivity (µS/cm) time series",
            "parameter_name": "Conductivity",
            "unit_name": "µS/cm",
            "observed_property": "conductivity",
            "period_seconds": 900,
            "period_name": "sub-daily",
            "value_type": "instantaneous",
            "observation_type": "Measured",
        },
        {
            "measure_id": "m_do",
            "label": "Sub-daily Dissolved Oxygen (mg/L) time series",
            "parameter_name": "Dissolved Oxygen",
            "unit_name": "mg/L",
            "observed_property": "dissolved-oxygen",
            "period_seconds": 900,
            "period_name": "sub-daily",
            "value_type": "instantaneous",
            "observation_type": "Measured",
        },
    ]
    facts = [
        {
            "measure_id": "m_cond",
            "reading_datetime": "2026-02-14T10:00:00+00:00",
            "reading_date": "2026-02-14",
            "value": 111.1,
            "quality": "Good",
            "completeness": None,
            "qcode": None,
        },
        {
            "measure_id": "m_do",
            "reading_datetime": "2026-02-14T10:15:00+00:00",
            "reading_date": "2026-02-14",
            "value": 8.2,
            "quality": "Good",
            "completeness": None,
            "qcode": None,
        },
    ]

    def fake_extract(*args, **kwargs):
        return station, measures, facts

    monkeypatch.setattr("src.pipeline.extract_station_and_readings", fake_extract)

    settings = Settings(sqlite_path=tmp_path / "out.db", latest_n=10)
    result = run_pipeline(settings)
    assert result["inserted_fact_rows"] == 2
    assert result["summary"]["fact_measurement_rows"] == 2
