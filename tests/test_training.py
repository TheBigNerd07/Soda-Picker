from __future__ import annotations

import unittest
from datetime import date

from app.models import CatalogItem, PassportEntry, Soda, SodaState
from app.training import (
    PassportTrainingProfile,
    build_training_adjustment,
    build_training_menu,
    build_training_mode_options,
    profile_from_storage,
    serialize_training_form,
)


class PassportTrainingTests(unittest.TestCase):
    def build_item(self, soda: Soda) -> CatalogItem:
        return CatalogItem(soda=soda, state=SodaState(soda_id=soda.id))

    def test_training_menu_uses_passport_and_catalog_overlap(self) -> None:
        catalog_items = [
            self.build_item(Soda(id="cola", name="Classic Cola", category="cola", priority=3, caffeine_mg=34)),
            self.build_item(Soda(id="orange", name="Orange Crush", category="orange", priority=2, caffeine_mg=0)),
        ]
        passport_entries = [
            PassportEntry(
                id=1,
                soda_name="Inca Kola",
                brand="Coca-Cola",
                country="Peru",
                region="",
                city="Lima",
                category="cola",
                tried_on=date(2026, 3, 1),
                where_tried="Corner store",
                rating=5,
                would_try_again=True,
                notes="Bright but still cola-like",
            ),
            PassportEntry(
                id=2,
                soda_name="Orange Crush",
                brand="Crush",
                country="United States",
                region="",
                city="Chicago",
                category="orange",
                tried_on=date(2026, 3, 4),
                where_tried="Diner",
                rating=4,
                would_try_again=True,
                notes="Juicy fruit soda",
            ),
        ]
        profile = profile_from_storage(
            {
                "boost_categories": "cola",
                "boost_moods": "mood:fruit-forward",
            }
        )

        menu = build_training_menu(
            passport_entries=passport_entries,
            catalog_items=catalog_items,
            profile=profile,
        )

        self.assertEqual(menu.passport_entry_count, 2)
        self.assertIn("Cola", [option.label for option in menu.category_options])
        self.assertIn("Orange", [option.label for option in menu.category_options])
        self.assertIn("Fruit-forward", [option.label for option in menu.mood_options])
        self.assertEqual(menu.boost_labels, ("Cola", "Fruit-forward"))

    def test_serialize_training_form_drops_conflicts_and_unknown_values(self) -> None:
        payload = serialize_training_form(
            boost_category_keys=["cola", "cola", "unknown"],
            avoid_category_keys=["cola", "orange"],
            boost_mood_values=["mood:fruit-forward"],
            avoid_mood_values=["mood:fruit-forward", "mood:dessert-creamy"],
            mode="trained",
            strength="assertive",
            allowed_category_keys={"cola", "orange"},
            allowed_mood_values={"mood:fruit-forward", "mood:dessert-creamy"},
        )

        self.assertEqual(payload["mode"], "trained")
        self.assertEqual(payload["boost_categories"], "cola")
        self.assertEqual(payload["avoid_categories"], "orange")
        self.assertEqual(payload["boost_moods"], "mood:fruit-forward")
        self.assertEqual(payload["avoid_moods"], "mood:dessert-creamy")
        self.assertEqual(payload["strength"], "assertive")

    def test_mode_resolution_falls_back_to_classic_without_training_signal(self) -> None:
        auto_profile = PassportTrainingProfile(mode="auto")
        forced_profile = PassportTrainingProfile(mode="trained")
        active_profile = PassportTrainingProfile(mode="auto", boost_category_keys=("cola",))

        self.assertEqual(auto_profile.effective_mode, "classic")
        self.assertFalse(auto_profile.uses_training)
        self.assertEqual(forced_profile.effective_mode, "classic")
        self.assertFalse(forced_profile.uses_training)
        self.assertEqual(active_profile.effective_mode, "trained")
        self.assertTrue(active_profile.uses_training)

    def test_mode_options_include_classic_auto_and_trained(self) -> None:
        values = [row[0] for row in build_training_mode_options()]
        self.assertEqual(values, ["auto", "classic", "trained"])

    def test_training_adjustment_boosts_matching_item(self) -> None:
        profile = PassportTrainingProfile(
            boost_category_keys=("cola",),
            boost_mood_values=("mood:fruit-forward",),
            strength="assertive",
        )

        cola_adjustment = build_training_adjustment(
            self.build_item(Soda(id="cola", name="Cherry Cola", brand="Example", category="cola")),
            profile,
        )
        plain_adjustment = build_training_adjustment(
            self.build_item(Soda(id="root", name="Root Beer", brand="Example", category="root-beer")),
            profile,
        )

        self.assertGreater(cola_adjustment.multiplier, plain_adjustment.multiplier)
        self.assertTrue(cola_adjustment.nudges)


if __name__ == "__main__":
    unittest.main()
