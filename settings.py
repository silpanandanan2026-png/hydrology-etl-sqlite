from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    station_label: str = "HIPPER_PARK ROAD BRIDGE_E_202312"
    # We intentionally specify unit for dissolved oxygen because the station can have multiple DO measures.
    target_parameters: tuple[tuple[str, str | None], ...] = (
        ("Conductivity", "µS/cm"),
        ("Dissolved Oxygen", "mg/L"),
    )
    latest_n: int = 10
    sqlite_path: Path = Path("output") / "hydrology_hipper.db"
    api_base: str = "https://environment.data.gov.uk/hydrology"
    timeout_seconds: int = 30

    def ensure_output_dir(self) -> None:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
