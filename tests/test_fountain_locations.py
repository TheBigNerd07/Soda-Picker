from __future__ import annotations

import unittest

from app.fountain_locations import (
    apply_location_inventory,
    build_preset_soda_ids,
    group_catalog_items_by_company,
)
from app.models import CatalogItem, Soda, SodaState


class FountainLocationTests(unittest.TestCase):
    def build_item(self, *, soda_id: str, name: str, brand: str = "", available: bool = True) -> CatalogItem:
        return CatalogItem(
            soda=Soda(id=soda_id, name=name, brand=brand, caffeine_mg=0, is_caffeine_free=True, category="cola"),
            state=SodaState(soda_id=soda_id, is_available=available),
        )

    def test_coke_preset_matches_common_catalog_items(self) -> None:
        catalog_items = [
            self.build_item(soda_id="coke", name="Coca-Cola", brand="Coca-Cola"),
            self.build_item(soda_id="sprite", name="Sprite", brand="Coca-Cola"),
            self.build_item(soda_id="pepsi", name="Pepsi", brand="PepsiCo"),
        ]

        matched = build_preset_soda_ids(catalog_items, "coke-fountain")
        self.assertEqual(matched, ("coke", "sprite"))

    def test_location_inventory_scopes_availability_without_touching_preferences(self) -> None:
        catalog_items = [
            CatalogItem(
                soda=Soda(id="coke", name="Coca-Cola", caffeine_mg=34, category="cola"),
                state=SodaState(soda_id="coke", is_available=False, preference="favorite"),
            ),
            CatalogItem(
                soda=Soda(id="sprite", name="Sprite", caffeine_mg=0, is_caffeine_free=True, category="lemon-lime"),
                state=SodaState(soda_id="sprite", is_available=True, preference="neutral"),
            ),
        ]

        scoped = apply_location_inventory(catalog_items, ("coke",))
        self.assertTrue(scoped[0].state.is_available)
        self.assertEqual(scoped[0].state.preference, "favorite")
        self.assertFalse(scoped[1].state.is_available)

    def test_company_grouping_clusters_parent_companies_for_location_pickers(self) -> None:
        catalog_items = [
            self.build_item(soda_id="coke", name="Coca-Cola", brand="Coca-Cola"),
            self.build_item(soda_id="sprite", name="Sprite", brand="Sprite"),
            self.build_item(soda_id="pepsi", name="Pepsi", brand="Pepsi"),
            self.build_item(soda_id="mug", name="Mug Root Beer", brand="Mug"),
            self.build_item(soda_id="drpepper", name="Dr Pepper", brand="Dr Pepper"),
            self.build_item(soda_id="jarritos", name="Jarritos Mandarin", brand="Jarritos"),
        ]

        groups = group_catalog_items_by_company(catalog_items)

        self.assertEqual(
            [group.label for group in groups],
            [
                "Coca-Cola Company",
                "PepsiCo",
                "Keurig Dr Pepper",
                "Independent / Regional",
            ],
        )
        self.assertEqual([item.id for item in groups[0].items], ["coke", "sprite"])
        self.assertEqual([item.id for item in groups[1].items], ["mug", "pepsi"])


if __name__ == "__main__":
    unittest.main()
