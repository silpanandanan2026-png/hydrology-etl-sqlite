from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

from src.database import connect, init_db, insert_facts, query_summary, upsert_parameters, upsert_station
from src.hydrology_api import extract_station_and_readings
from src.settings import Settings
from src.transform import build_star_rows


def run_pipeline(settings: Settings) -> dict:
    settings.ensure_output_dir()

    with requests.Session() as session:
        station, measures, facts = extract_station_and_readings(
            session,
            api_base=settings.api_base,
            station_label=settings.station_label,
            targets=settings.target_parameters,
            latest_n=settings.latest_n,
            timeout=settings.timeout_seconds,
        )

    station_row, parameter_rows, fact_rows = build_star_rows(station, measures, facts)

    with connect(settings.sqlite_path) as cn:
        init_db(cn)
        station_key = upsert_station(cn, station_row)
        parameter_key_by_measure_id = upsert_parameters(cn, parameter_rows)
        inserted_rows = insert_facts(
            cn,
            station_key=station_key,
            parameter_key_by_measure_id=parameter_key_by_measure_id,
            rows=fact_rows,
        )
        summary = query_summary(cn)

    return {
        "db_path": str(settings.sqlite_path),
        "station": station_row,
        "selected_measures": parameter_rows,
        "fetched_fact_rows": len(fact_rows),
        "inserted_fact_rows": inserted_rows,
        "summary": summary,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hydrology take-home ETL: API -> transform -> SQLite star schema"
    )
    parser.add_argument(
        "--db-path",
        default=str(Settings().sqlite_path),
        help="Path to SQLite database file (default: output/hydrology_hipper.db)",
    )
    parser.add_argument(
        "--latest-n",
        type=int,
        default=Settings().latest_n,
        help="Number of most recent readings to download per parameter (default: 10)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = Settings(sqlite_path=Path(args.db_path), latest_n=int(args.latest_n))
    result = run_pipeline(settings)

    print("\n=== Pipeline run completed ===")
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    print("\nTip: run the same command again to show idempotent loading (duplicate rows are ignored).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
