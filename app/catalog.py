from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from threading import Lock

from .models import Soda

LOGGER = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "soda"


def _parse_float(value: str | None, default: float | None = None, *, field_name: str) -> float | None:
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


def _parse_int(value: str | None, default: int = 1, *, field_name: str) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_tags(value: str | None) -> tuple[str, ...]:
    if value is None or value.strip() == "":
        return ()
    return tuple(part.strip() for part in re.split(r"\s*[|;,]\s*", value) if part.strip())


class SodaCatalog:
    def __init__(self, csv_path: str) -> None:
        self.csv_path = Path(csv_path)
        self._lock = Lock()
        self._mtime_ns: int | None = None
        self._sodas: list[Soda] = []
        self._warnings: list[str] = []

    def refresh(self, *, force: bool = False) -> None:
        with self._lock:
            path = self.csv_path
            if not path.exists():
                self._sodas = []
                self._warnings = [f"CSV file not found at {path}."]
                self._mtime_ns = None
                LOGGER.warning("Soda CSV not found: %s", path)
                return

            current_mtime_ns = path.stat().st_mtime_ns
            if not force and self._mtime_ns == current_mtime_ns:
                return

            sodas: list[Soda] = []
            warnings: list[str] = []

            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    self._sodas = []
                    self._warnings = ["CSV file is missing a header row."]
                    self._mtime_ns = current_mtime_ns
                    LOGGER.warning("Soda CSV has no header row: %s", path)
                    return

                for row_number, raw_row in enumerate(reader, start=2):
                    row = {
                        (key or "").strip().lower(): (value or "").strip()
                        for key, value in raw_row.items()
                    }

                    try:
                        soda = self._parse_row(row, row_number)
                    except ValueError as exc:
                        warning = f"Row {row_number}: {exc}"
                        warnings.append(warning)
                        LOGGER.warning("Skipping CSV row %s: %s", row_number, exc)
                        continue

                    if soda is not None:
                        sodas.append(soda)

            self._sodas = sodas
            self._warnings = warnings
            self._mtime_ns = current_mtime_ns
            LOGGER.info("Loaded %s sodas from %s", len(sodas), path)

    def list_sodas(self) -> list[Soda]:
        self.refresh()
        return list(self._sodas)

    def get_by_id(self, soda_id: str) -> Soda | None:
        self.refresh()
        for soda in self._sodas:
            if soda.id == soda_id:
                return soda
        return None

    @property
    def warnings(self) -> list[str]:
        self.refresh()
        return list(self._warnings)

    def _parse_row(self, row: dict[str, str], row_number: int) -> Soda | None:
        name = row.get("name", "").strip()
        if not name:
            raise ValueError("missing required field 'name'")

        enabled = _parse_bool(row.get("enabled"), True)
        if not enabled:
            return None

        brand = row.get("brand", "").strip()
        caffeine_mg = _parse_float(row.get("caffeine_mg"), 0.0, field_name="caffeine_mg") or 0.0
        sugar_g = _parse_float(row.get("sugar_g"), None, field_name="sugar_g")
        category = row.get("category", "").strip() or "General"
        is_diet = _parse_bool(row.get("is_diet"), False)
        is_caffeine_free = _parse_bool(row.get("is_caffeine_free"), False) or caffeine_mg <= 0
        tags = _parse_tags(row.get("tags"))
        priority = max(_parse_int(row.get("priority"), 1, field_name="priority"), 1)
        slug_source = f"{brand}-{name}-{row_number}"

        return Soda(
            id=_slugify(slug_source),
            name=name,
            brand=brand,
            caffeine_mg=caffeine_mg,
            sugar_g=sugar_g,
            category=category,
            is_diet=is_diet,
            is_caffeine_free=is_caffeine_free,
            tags=tags,
            priority=priority,
            enabled=True,
        )
