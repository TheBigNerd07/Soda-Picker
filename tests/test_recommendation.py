from __future__ import annotations

import unittest
from datetime import date, datetime
from random import Random

from app.config import Settings
from app.models import CatalogItem, Soda, SodaState
from app.pick_styles import resolve_pick_style_option
from app.recommendation import RecommendationEngine
from app.training import PassportTrainingProfile


class RecommendationEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            timezone_name="America/Los_Angeles",
            no_soda_before="10:30",
            weekend_no_soda_before="11:00",
            daily_caffeine_limit_mg=160,
            weekend_daily_caffeine_limit_mg=180,
            caffeine_cutoff_hour=15,
            weekend_caffeine_cutoff_hour=16,
            bedtime_hour=23,
            weekend_bedtime_hour=23,
            latest_caffeine_hours_before_bed=6,
            duplicate_lookback=4,
        )
        self.engine = RecommendationEngine()

    def build_item(
        self,
        soda: Soda,
        *,
        is_available: bool = True,
        preference: str = "neutral",
        temp_ban_until: date | None = None,
    ) -> CatalogItem:
        return CatalogItem(
            soda=soda,
            state=SodaState(
                soda_id=soda.id,
                is_available=is_available,
                preference=preference,
                temp_ban_until=temp_ban_until,
            ),
        )

    def test_pre_1030_blocks_recommendation(self) -> None:
        local_now = datetime(2026, 3, 13, 9, 45, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(1),
        )

        self.assertIsNone(result.soda)
        self.assertEqual(result.status, "blocked")
        self.assertIn("Soda time starts", result.reason)

    def test_after_window_returns_recommendation(self) -> None:
        local_now = datetime(2026, 3, 13, 11, 15, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(2),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.status, "ready")
        self.assertEqual(result.soda.soda.name, "Cola")

    def test_daily_caffeine_limit_prefers_caffeine_free(self) -> None:
        local_now = datetime(2026, 3, 13, 16, 0, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=1)),
                self.build_item(Soda(id="root", name="Root Beer", caffeine_mg=0, is_caffeine_free=True, priority=1)),
            ],
            daily_caffeine_total=160,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(3),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Root Beer")
        self.assertLessEqual(result.projected_total_mg, 160)

    def test_out_of_stock_item_is_not_recommended(self) -> None:
        local_now = datetime(2026, 3, 13, 11, 30, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=4), is_available=False),
                self.build_item(Soda(id="sprite", name="Sprite", caffeine_mg=0, is_caffeine_free=True, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(4),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Sprite")
        self.assertTrue(any(note.soda_name == "Cola" for note in result.rejected_options))

    def test_night_mode_prefers_caffeine_free(self) -> None:
        local_now = datetime(2026, 3, 13, 22, 0, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=1)),
                self.build_item(Soda(id="sprite", name="Sprite", caffeine_mg=0, is_caffeine_free=True, priority=1)),
            ],
            daily_caffeine_total=30,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(5),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Sprite")

    def test_weekend_rules_shift_no_soda_before(self) -> None:
        local_now = datetime(2026, 3, 14, 10, 45, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(6),
        )

        self.assertEqual(result.status, "blocked")
        self.assertIn("11:00 AM", result.reason)

    def test_pick_style_prefers_matching_mood(self) -> None:
        local_now = datetime(2026, 3, 13, 12, 15, tzinfo=self.settings.timezone)
        catalog_items = [
            self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, category="cola", priority=4)),
            self.build_item(
                Soda(
                    id="orange",
                    name="Orange Crush",
                    caffeine_mg=0,
                    category="orange",
                    tags=("fruit",),
                    is_caffeine_free=True,
                    priority=1,
                )
            ),
        ]

        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=catalog_items,
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            pick_style=resolve_pick_style_option("mood:fruit-forward", catalog_items),
            rng=Random(7),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Orange Crush")
        self.assertFalse(result.pick_style_fallback_used)
        self.assertIn("fruit-forward", result.reason.lower())

    def test_pick_style_falls_back_when_matching_lane_is_blocked(self) -> None:
        local_now = datetime(2026, 3, 13, 16, 30, tzinfo=self.settings.timezone)
        catalog_items = [
            self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, category="cola", priority=4)),
            self.build_item(
                Soda(
                    id="root",
                    name="Root Beer",
                    caffeine_mg=0,
                    category="root-beer",
                    is_caffeine_free=True,
                    priority=1,
                )
            ),
        ]

        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=catalog_items,
            daily_caffeine_total=160,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            pick_style=resolve_pick_style_option("category:cola", catalog_items),
            rng=Random(8),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Root Beer")
        self.assertTrue(result.pick_style_fallback_used)
        self.assertIn("fell back to the broader safe list", result.reason)

    def test_passport_training_can_bias_the_safe_list(self) -> None:
        local_now = datetime(2026, 3, 13, 12, 15, tzinfo=self.settings.timezone)
        catalog_items = [
            self.build_item(Soda(id="root", name="Root Beer", caffeine_mg=0, category="root-beer", priority=1)),
            self.build_item(Soda(id="orange", name="Orange Crush", caffeine_mg=0, category="orange", priority=1)),
        ]

        result = self.engine.recommend(
            rules=self.settings.effective_rules(local_now),
            catalog_items=catalog_items,
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            training_profile=PassportTrainingProfile(
                boost_category_keys=("orange",),
                boost_mood_values=("mood:fruit-forward",),
                strength="assertive",
            ),
            rng=Random(9),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Orange Crush")
        self.assertIn("passport training", result.reason.lower())

    def test_setting_can_block_diet_sodas(self) -> None:
        local_now = datetime(2026, 3, 13, 12, 0, tzinfo=self.settings.timezone)
        settings = self.settings.with_overrides({"allow_diet_sodas": "false"})
        result = self.engine.recommend(
            rules=settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="diet-cola", name="Diet Cola", caffeine_mg=38, is_diet=True, priority=3)),
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, is_diet=False, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(10),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Cola")
        self.assertTrue(any(note.reason == "diet and zero-sugar sodas are turned off" for note in result.rejected_options))

    def test_setting_can_block_full_sugar_sodas(self) -> None:
        local_now = datetime(2026, 3, 13, 12, 0, tzinfo=self.settings.timezone)
        settings = self.settings.with_overrides({"allow_full_sugar_sodas": "false"})
        result = self.engine.recommend(
            rules=settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="diet-cola", name="Diet Cola", caffeine_mg=38, is_diet=True, priority=1)),
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, is_diet=False, priority=3)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(11),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.soda.name, "Diet Cola")
        self.assertTrue(any(note.reason == "full-sugar sodas are turned off" for note in result.rejected_options))

    def test_turning_off_both_sugar_modes_blocks_recommendation(self) -> None:
        local_now = datetime(2026, 3, 13, 12, 0, tzinfo=self.settings.timezone)
        settings = self.settings.with_overrides(
            {
                "allow_diet_sodas": "false",
                "allow_full_sugar_sodas": "false",
            }
        )
        result = self.engine.recommend(
            rules=settings.effective_rules(local_now),
            catalog_items=[
                self.build_item(Soda(id="diet-cola", name="Diet Cola", caffeine_mg=38, is_diet=True, priority=1)),
                self.build_item(Soda(id="cola", name="Cola", caffeine_mg=38, is_diet=False, priority=1)),
            ],
            daily_caffeine_total=0,
            recent_soda_ids=[],
            today_entries=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(12),
        )

        self.assertEqual(result.status, "blocked")
        self.assertIn("Both diet and full-sugar sodas are turned off", result.reason)


if __name__ == "__main__":
    unittest.main()
