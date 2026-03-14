from __future__ import annotations

import unittest
from datetime import date, datetime
from random import Random

from app.config import Settings
from app.models import CatalogItem, Soda, SodaState
from app.recommendation import RecommendationEngine


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


if __name__ == "__main__":
    unittest.main()
