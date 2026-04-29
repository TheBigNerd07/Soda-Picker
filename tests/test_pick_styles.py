from __future__ import annotations

import unittest

from app.models import CatalogItem, Soda, SodaState
from app.pick_styles import build_pick_style_groups, resolve_pick_style_option


class PickStyleTests(unittest.TestCase):
    def build_item(self, *, soda_id: str, name: str, category: str) -> CatalogItem:
        return CatalogItem(
            soda=Soda(id=soda_id, name=name, category=category, caffeine_mg=0, is_caffeine_free=True),
            state=SodaState(soda_id=soda_id),
        )

    def test_pinned_categories_are_promoted_and_removed_from_general_list(self) -> None:
        catalog_items = [
            self.build_item(soda_id="cola", name="Cola", category="cola"),
            self.build_item(soda_id="orange", name="Orange Soda", category="orange"),
            self.build_item(soda_id="root", name="Root Beer", category="root-beer"),
        ]

        groups = build_pick_style_groups(
            catalog_items,
            pinned_category_keys=("orange", "cola"),
        )

        self.assertEqual(groups[1].label, "Pinned categories")
        self.assertEqual(
            tuple(option.match_key for option in groups[1].options),
            ("orange", "cola"),
        )
        category_group = next(group for group in groups if group.label == "Catalog categories")
        self.assertEqual(tuple(option.match_key for option in category_group.options), ("root-beer",))

    def test_resolve_pick_style_option_supports_pinned_categories(self) -> None:
        catalog_items = [
            self.build_item(soda_id="orange", name="Orange Soda", category="orange"),
        ]

        option = resolve_pick_style_option(
            "category:orange",
            catalog_items,
            pinned_category_keys=("orange",),
        )
        self.assertEqual(option.match_key, "orange")


if __name__ == "__main__":
    unittest.main()
