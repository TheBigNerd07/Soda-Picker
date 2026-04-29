from __future__ import annotations

from dataclasses import dataclass

from .models import CatalogItem, SodaState


@dataclass(frozen=True)
class FountainPreset:
    key: str
    label: str
    description: str
    soda_names: tuple[str, ...]


@dataclass(frozen=True)
class FountainCompanyGroup:
    key: str
    label: str
    items: tuple[CatalogItem, ...]

    @property
    def count(self) -> int:
        return len(self.items)


FOUNTAIN_PRESETS = (
    FountainPreset(
        key="coke-fountain",
        label="Coke fountain preset",
        description="Common Coca-Cola restaurant fountain mix.",
        soda_names=(
            "Coca-Cola",
            "Cherry Coke",
            "Vanilla Coke",
            "Sprite",
            "Fanta Orange",
            "Barq's Root Beer",
            "Canada Dry Ginger Ale",
        ),
    ),
    FountainPreset(
        key="pepsi-fountain",
        label="Pepsi fountain preset",
        description="Common Pepsi restaurant fountain mix.",
        soda_names=(
            "Pepsi",
            "Wild Cherry Pepsi",
            "Pepsi Vanilla",
            "Starry",
            "Mountain Dew",
            "Mug Root Beer",
        ),
    ),
)

_PARENT_COMPANY_ORDER = {
    "Coca-Cola Company": 0,
    "PepsiCo": 1,
    "Keurig Dr Pepper": 2,
    "Independent / Regional": 3,
}

_PARENT_COMPANY_ALIASES = {
    "coca-cola": "Coca-Cola Company",
    "coke": "Coca-Cola Company",
    "sprite": "Coca-Cola Company",
    "fanta": "Coca-Cola Company",
    "barq's": "Coca-Cola Company",
    "barqs": "Coca-Cola Company",
    "mello yello": "Coca-Cola Company",
    "surge": "Coca-Cola Company",
    "pibb xtra": "Coca-Cola Company",
    "mr pibb": "Coca-Cola Company",
    "fresca": "Coca-Cola Company",
    "pepsico": "PepsiCo",
    "pepsi": "PepsiCo",
    "mountain dew": "PepsiCo",
    "mug": "PepsiCo",
    "starry": "PepsiCo",
    "sierra mist": "PepsiCo",
    "slice": "PepsiCo",
    "brisk": "PepsiCo",
    "keurig dr pepper": "Keurig Dr Pepper",
    "dr pepper": "Keurig Dr Pepper",
    "7up": "Keurig Dr Pepper",
    "a&w": "Keurig Dr Pepper",
    "sunkist": "Keurig Dr Pepper",
    "crush": "Keurig Dr Pepper",
    "squirt": "Keurig Dr Pepper",
    "sun drop": "Keurig Dr Pepper",
    "rc cola": "Keurig Dr Pepper",
    "royal crown": "Keurig Dr Pepper",
    "diet rite": "Keurig Dr Pepper",
    "canada dry": "Keurig Dr Pepper",
    "seagram's": "Keurig Dr Pepper",
    "vernors": "Keurig Dr Pepper",
    "schweppes": "Keurig Dr Pepper",
    "big red": "Keurig Dr Pepper",
    "big blue": "Keurig Dr Pepper",
}


def list_fountain_presets() -> tuple[FountainPreset, ...]:
    return FOUNTAIN_PRESETS


def resolve_fountain_preset(key: str) -> FountainPreset | None:
    cleaned = key.strip().lower()
    for preset in FOUNTAIN_PRESETS:
        if preset.key == cleaned:
            return preset
    return None


def build_preset_soda_ids(catalog_items: list[CatalogItem], preset_key: str) -> tuple[str, ...]:
    preset = resolve_fountain_preset(preset_key)
    if preset is None:
        return ()

    target_names = {name.strip().casefold() for name in preset.soda_names if name.strip()}
    soda_ids: list[str] = []
    for item in catalog_items:
        labels = {
            item.soda.name.strip().casefold(),
            item.soda.display_name.strip().casefold(),
        }
        if labels.intersection(target_names):
            soda_ids.append(item.id)
    return tuple(soda_ids)


def group_catalog_items_by_company(catalog_items: list[CatalogItem]) -> tuple[FountainCompanyGroup, ...]:
    grouped: dict[str, list[CatalogItem]] = {}
    for item in sorted(catalog_items, key=lambda catalog_item: catalog_item.display_name.lower()):
        label = _parent_company_label(item)
        grouped.setdefault(label, []).append(item)

    ordered = sorted(
        grouped.items(),
        key=lambda entry: (_PARENT_COMPANY_ORDER.get(entry[0], 99), entry[0].lower()),
    )
    return tuple(
        FountainCompanyGroup(
            key=_normalize_group_key(label),
            label=label,
            items=tuple(items),
        )
        for label, items in ordered
    )


def apply_location_inventory(catalog_items: list[CatalogItem], available_soda_ids: tuple[str, ...]) -> list[CatalogItem]:
    available_set = set(available_soda_ids)
    scoped_items: list[CatalogItem] = []
    for item in catalog_items:
        scoped_items.append(
            CatalogItem(
                soda=item.soda,
                state=SodaState(
                    soda_id=item.id,
                    is_available=item.id in available_set,
                    preference=item.state.preference,
                    temp_ban_until=item.state.temp_ban_until,
                ),
            )
        )
    return scoped_items


def _parent_company_label(item: CatalogItem) -> str:
    brand = item.soda.brand.strip().lower()
    if brand in _PARENT_COMPANY_ALIASES:
        return _PARENT_COMPANY_ALIASES[brand]
    if brand in {"coca-cola company", "pepsi", "pepsico", "keurig dr pepper"}:
        return _PARENT_COMPANY_ALIASES.get(brand, item.soda.brand.strip())
    if item.soda.brand.strip():
        return "Independent / Regional"
    return "Other"


def _normalize_group_key(label: str) -> str:
    return (
        label.strip()
        .lower()
        .replace(" ", "-")
        .replace("/", "-")
    )
