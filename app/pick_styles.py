from __future__ import annotations

import re
from dataclasses import dataclass

from .models import CatalogItem


def normalize_pick_style_key(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return cleaned.strip("-")


def _display_label(value: str) -> str:
    return value.replace("-", " ").strip().title()


@dataclass(frozen=True)
class PickStyleOption:
    value: str
    label: str
    description: str
    kind: str = "any"
    match_key: str = ""
    search_terms: tuple[str, ...] = ()

    @property
    def is_any(self) -> bool:
        return self.kind == "any"

    def matches_fields(
        self,
        *,
        name: str = "",
        brand: str = "",
        category: str = "",
        tags: tuple[str, ...] = (),
    ) -> bool:
        if self.is_any:
            return True
        if self.kind == "caffeine_free":
            haystack = _search_blob_parts(name=name, brand=brand, category=category, tags=tags)
            return any(
                term in haystack
                for term in ("caffeine-free", "caffeine-free-only", "zero-caffeine", "decaf", "no-caffeine")
            )
        if self.kind == "category":
            return normalize_pick_style_key(category) == self.match_key

        haystack = _search_blob_parts(name=name, brand=brand, category=category, tags=tags)
        return any(term in haystack for term in self.search_terms)

    def matches(self, item: CatalogItem) -> bool:
        if self.kind == "caffeine_free":
            return item.soda.is_caffeine_free or item.soda.caffeine_mg <= 0
        return self.matches_fields(
            name=item.soda.name,
            brand=item.soda.brand,
            category=item.soda.category,
            tags=item.soda.tags,
        )


@dataclass(frozen=True)
class PickStyleGroup:
    label: str
    options: tuple[PickStyleOption, ...]


ANY_PICK_STYLE = PickStyleOption(
    value="any",
    label="Any soda",
    description="Let the picker use the full safe catalog.",
)

_FEATURED_PICK_STYLES = (
    PickStyleOption(
        value="mood:fruit-forward",
        label="Fruit-forward",
        description="Lean toward fruit sodas, berries, cherry, tropical, and juicy flavors.",
        kind="mood",
        search_terms=(
            "fruit",
            "orange",
            "grape",
            "strawberry",
            "berry",
            "cherry",
            "tropical",
            "grapefruit",
            "pineapple",
            "mango",
        ),
    ),
    PickStyleOption(
        value="mood:crisp-bright",
        label="Crisp and bright",
        description="Bias toward lemon-lime, citrus, grapefruit, and ginger styles.",
        kind="mood",
        search_terms=(
            "lemon-lime",
            "citrus",
            "grapefruit",
            "ginger",
            "ginger-ale",
            "lime",
            "lemon",
        ),
    ),
    PickStyleOption(
        value="mood:dessert-creamy",
        label="Dessert and creamy",
        description="Aim for root beer, cream soda, vanilla, and softer dessert profiles.",
        kind="mood",
        search_terms=("root-beer", "cream-soda", "vanilla", "cream"),
    ),
    PickStyleOption(
        value="mood:caffeine-free",
        label="Caffeine-free only",
        description="Keep the picker inside the caffeine-free lane.",
        kind="caffeine_free",
    ),
)


def _search_blob_parts(
    *,
    name: str = "",
    brand: str = "",
    category: str = "",
    tags: tuple[str, ...] = (),
) -> str:
    parts = [name, brand, category, *tags]
    return " ".join(normalize_pick_style_key(part) for part in parts if part).strip()


def _has_match(option: PickStyleOption, catalog_items: list[CatalogItem]) -> bool:
    return any(option.matches(item) for item in catalog_items)


def featured_pick_styles(
    catalog_items: list[CatalogItem],
    *,
    include_caffeine_free: bool = True,
) -> tuple[PickStyleOption, ...]:
    return tuple(
        option
        for option in _FEATURED_PICK_STYLES
        if (include_caffeine_free or option.kind != "caffeine_free") and _has_match(option, catalog_items)
    )


def build_pick_style_groups(catalog_items: list[CatalogItem]) -> tuple[PickStyleGroup, ...]:
    groups: list[PickStyleGroup] = [
        PickStyleGroup(label="Picker", options=(ANY_PICK_STYLE,)),
    ]

    featured = featured_pick_styles(catalog_items)
    if featured:
        groups.append(PickStyleGroup(label="Featured moods", options=featured))

    categories: dict[str, PickStyleOption] = {}
    for item in catalog_items:
        category = item.soda.category.strip()
        if not category:
            continue
        key = normalize_pick_style_key(category)
        if not key or key in categories:
            continue
        categories[key] = PickStyleOption(
            value=f"category:{key}",
            label=_display_label(category),
            description=f"Only pull from {category.replace('-', ' ')} sodas when it still fits the rules.",
            kind="category",
            match_key=key,
        )

    if categories:
        groups.append(
            PickStyleGroup(
                label="Catalog categories",
                options=tuple(sorted(categories.values(), key=lambda option: option.label.lower())),
            )
        )

    return tuple(groups)


def resolve_pick_style_option(value: str | None, catalog_items: list[CatalogItem]) -> PickStyleOption:
    if not value:
        return ANY_PICK_STYLE

    for group in build_pick_style_groups(catalog_items):
        for option in group.options:
            if option.value == value:
                return option
    return ANY_PICK_STYLE
