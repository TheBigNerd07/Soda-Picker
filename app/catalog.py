from __future__ import annotations

import csv
import io
import logging
import re
from pathlib import Path
from threading import Lock

from .models import CatalogDiagnostics, Soda

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"name"}
OPTIONAL_COLUMNS = {
    "brand",
    "caffeine_mg",
    "sugar_g",
    "category",
    "is_diet",
    "is_caffeine_free",
    "tags",
    "priority",
    "enabled",
}
CANONICAL_COLUMN_ORDER = (
    "name",
    "brand",
    "caffeine_mg",
    "sugar_g",
    "category",
    "is_diet",
    "is_caffeine_free",
    "tags",
    "priority",
    "enabled",
)
DEFAULT_ESTIMATED_CAFFEINE_MG = 35.0


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
        self._diagnostics = CatalogDiagnostics()

    def refresh(self, *, force: bool = False) -> None:
        with self._lock:
            path = self.csv_path
            if not path.exists():
                self._sodas = []
                self._diagnostics = CatalogDiagnostics(
                    warnings=(f"CSV file not found at {path}.",),
                )
                self._mtime_ns = None
                LOGGER.warning("Soda CSV not found: %s", path)
                return

            current_mtime_ns = path.stat().st_mtime_ns
            if not force and self._mtime_ns == current_mtime_ns:
                return

            sodas, diagnostics = self._load_text(path.read_text(encoding="utf-8"), source_label=str(path))
            self._sodas = sodas
            self._diagnostics = diagnostics
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

    def preview_upload(self, raw_text: str) -> tuple[list[Soda], CatalogDiagnostics]:
        return self._load_text(raw_text, source_label="uploaded CSV")

    def replace_with_text(self, raw_text: str) -> tuple[list[Soda], CatalogDiagnostics]:
        sodas, diagnostics = self._load_text(raw_text, source_label=str(self.csv_path))
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.csv_path.write_text(raw_text, encoding="utf-8")
        self._sodas = sodas
        self._diagnostics = diagnostics
        self._mtime_ns = self.csv_path.stat().st_mtime_ns
        return sodas, diagnostics

    def add_soda(
        self,
        *,
        name: str,
        brand: str = "",
        category: str = "",
        contains_caffeine: bool,
        is_diet: bool = False,
        tags: tuple[str, ...] = (),
        priority: int = 1,
    ) -> Soda:
        cleaned_name = name.strip()
        if not cleaned_name:
            raise ValueError("Soda name is required.")

        cleaned_brand = brand.strip()
        display_name = f"{cleaned_brand} {cleaned_name}".strip().lower()
        self.refresh()
        if any(soda.display_name.lower() == display_name for soda in self._sodas):
            raise ValueError("That soda is already in the catalog.")

        headers, rows = self._read_rows_for_update()
        merged_headers = list(headers)
        for field_name in CANONICAL_COLUMN_ORDER:
            if field_name not in merged_headers:
                merged_headers.append(field_name)

        rows.append(
            {
                "name": cleaned_name,
                "brand": cleaned_brand,
                "caffeine_mg": "" if contains_caffeine else "0",
                "sugar_g": "",
                "category": category.strip() or "General",
                "is_diet": "true" if is_diet else "false",
                "is_caffeine_free": "false" if contains_caffeine else "true",
                "tags": "|".join(tag for tag in tags if tag),
                "priority": str(max(priority, 1)),
                "enabled": "true",
            }
        )

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=merged_headers, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in merged_headers})

        sodas, diagnostics = self.replace_with_text(output.getvalue())
        if not sodas:
            raise RuntimeError("Catalog write succeeded but no sodas were loaded back.")
        return sodas[-1]

    @property
    def diagnostics(self) -> CatalogDiagnostics:
        self.refresh()
        return self._diagnostics

    @property
    def warnings(self) -> tuple[str, ...]:
        self.refresh()
        return self._diagnostics.warnings

    def _load_text(self, raw_text: str, *, source_label: str) -> tuple[list[Soda], CatalogDiagnostics]:
        warnings: list[str] = []
        total_rows = 0
        disabled_rows = 0
        invalid_rows = 0
        loaded_sodas: list[Soda] = []

        reader = csv.DictReader(io.StringIO(raw_text))
        header_fields = tuple((field or "").strip().lower() for field in (reader.fieldnames or ()))
        if not header_fields:
            diagnostics = CatalogDiagnostics(warnings=(f"{source_label} is missing a header row.",))
            return [], diagnostics

        missing_required = sorted(REQUIRED_COLUMNS.difference(header_fields))
        if missing_required:
            diagnostics = CatalogDiagnostics(
                header_fields=header_fields,
                warnings=(f"Missing required column(s): {', '.join(missing_required)}.",),
            )
            return [], diagnostics

        missing_optional = tuple(sorted(OPTIONAL_COLUMNS.difference(header_fields)))
        if missing_optional:
            warnings.append(f"Missing optional columns: {', '.join(missing_optional)}.")

        for row_number, raw_row in enumerate(reader, start=2):
            total_rows += 1
            normalized_row = {
                (key or "").strip().lower(): (value or "").strip()
                for key, value in raw_row.items()
            }
            try:
                soda = self._parse_row(normalized_row, row_number)
            except ValueError as exc:
                warning = f"Row {row_number}: {exc}"
                warnings.append(warning)
                invalid_rows += 1
                LOGGER.warning("Skipping CSV row %s: %s", row_number, exc)
                continue

            if soda is None:
                disabled_rows += 1
                continue

            loaded_sodas.append(soda)

        duplicate_names = self._find_duplicates(loaded_sodas)
        if duplicate_names:
            warnings.append(f"Duplicate soda names detected: {', '.join(duplicate_names)}.")

        diagnostics = CatalogDiagnostics(
            total_rows=total_rows,
            loaded_rows=len(loaded_sodas),
            disabled_rows=disabled_rows,
            invalid_rows=invalid_rows,
            header_fields=header_fields,
            missing_optional_columns=missing_optional,
            duplicate_names=duplicate_names,
            warnings=tuple(warnings),
        )
        return loaded_sodas, diagnostics

    def _parse_row(self, row: dict[str, str], row_number: int) -> Soda | None:
        name = row.get("name", "").strip()
        if not name:
            raise ValueError("missing required field 'name'")

        enabled = _parse_bool(row.get("enabled"), True)
        if not enabled:
            return None

        brand = row.get("brand", "").strip()
        has_caffeine_value = "caffeine_mg" in row and row.get("caffeine_mg", "").strip() != ""
        has_caffeine_free_value = "is_caffeine_free" in row and row.get("is_caffeine_free", "").strip() != ""
        parsed_caffeine = _parse_float(row.get("caffeine_mg"), None, field_name="caffeine_mg")
        explicit_caffeine_free = _parse_bool(row.get("is_caffeine_free"), False)
        caffeine_is_estimated = False
        if parsed_caffeine is not None:
            caffeine_mg = parsed_caffeine
            is_caffeine_free = explicit_caffeine_free or caffeine_mg <= 0
        elif has_caffeine_free_value and not explicit_caffeine_free:
            caffeine_mg = DEFAULT_ESTIMATED_CAFFEINE_MG
            is_caffeine_free = False
            caffeine_is_estimated = True
        else:
            caffeine_mg = 0.0
            is_caffeine_free = True if not has_caffeine_value else explicit_caffeine_free
        sugar_g = _parse_float(row.get("sugar_g"), None, field_name="sugar_g")
        category = row.get("category", "").strip() or "General"
        is_diet = _parse_bool(row.get("is_diet"), False)
        tags = _parse_tags(row.get("tags"))
        priority = max(_parse_int(row.get("priority"), 1, field_name="priority"), 1)
        slug_source = f"{brand}-{name}-{row_number}"

        return Soda(
            id=_slugify(slug_source),
            name=name,
            brand=brand,
            caffeine_mg=caffeine_mg,
            caffeine_is_estimated=caffeine_is_estimated,
            sugar_g=sugar_g,
            category=category,
            is_diet=is_diet,
            is_caffeine_free=is_caffeine_free,
            tags=tags,
            priority=priority,
            enabled=True,
            row_number=row_number,
        )

    @staticmethod
    def _find_duplicates(sodas: list[Soda]) -> tuple[str, ...]:
        seen: dict[str, str] = {}
        duplicates: list[str] = []
        for soda in sodas:
            normalized = soda.display_name.lower()
            if normalized in seen:
                duplicates.append(soda.display_name)
            else:
                seen[normalized] = soda.display_name
        return tuple(sorted(set(duplicates)))

    def _read_rows_for_update(self) -> tuple[list[str], list[dict[str, str]]]:
        path = self.csv_path
        if not path.exists():
            return list(CANONICAL_COLUMN_ORDER), []

        raw_text = path.read_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(raw_text))
        headers = [((field or "").strip().lower()) for field in (reader.fieldnames or ())]
        headers = [field for field in headers if field]
        if not headers:
            raise ValueError("The current CSV is missing a header row.")
        if any(column not in headers for column in REQUIRED_COLUMNS):
            raise ValueError("The current CSV is missing required columns, so Soda Picker cannot append to it safely.")

        rows: list[dict[str, str]] = []
        for raw_row in reader:
            rows.append(
                {
                    (key or "").strip().lower(): (value or "").strip()
                    for key, value in raw_row.items()
                }
            )
        return headers, rows
