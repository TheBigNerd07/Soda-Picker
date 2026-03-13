from __future__ import annotations

import unittest
from datetime import datetime
from random import Random

from app.config import Settings
from app.models import Soda
from app.recommendation import RecommendationEngine


class RecommendationEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            timezone_name="America/Los_Angeles",
            no_soda_before="10:30",
            daily_caffeine_limit_mg=160,
            caffeine_cutoff_hour=15,
        )
        self.engine = RecommendationEngine()

    def test_pre_1030_blocks_recommendation(self) -> None:
        local_now = datetime(2026, 3, 13, 9, 45, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            settings=self.settings,
            sodas=[
                Soda(id="cola", name="Cola", caffeine_mg=38, priority=1),
            ],
            daily_caffeine_total=0,
            recent_consumed_ids=[],
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
            settings=self.settings,
            sodas=[
                Soda(id="cola", name="Cola", caffeine_mg=38, priority=1),
            ],
            daily_caffeine_total=0,
            recent_consumed_ids=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(2),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.status, "ready")
        self.assertEqual(result.soda.name, "Cola")

    def test_daily_caffeine_limit_blocks_caffeinated_only_choices(self) -> None:
        local_now = datetime(2026, 3, 13, 16, 0, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            settings=self.settings,
            sodas=[
                Soda(id="cola", name="Cola", caffeine_mg=38, priority=1),
                Soda(id="root", name="Root Beer", caffeine_mg=0, is_caffeine_free=True, priority=1),
            ],
            daily_caffeine_total=160,
            recent_consumed_ids=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(3),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.name, "Root Beer")
        self.assertIn("160", result.reason)

    def test_late_day_bias_prefers_caffeine_free_when_available(self) -> None:
        local_now = datetime(2026, 3, 13, 21, 0, tzinfo=self.settings.timezone)
        result = self.engine.recommend(
            settings=self.settings,
            sodas=[
                Soda(id="cola", name="Cola", caffeine_mg=38, priority=1),
                Soda(id="sprite", name="Sprite", caffeine_mg=0, is_caffeine_free=True, priority=1),
            ],
            daily_caffeine_total=30,
            recent_consumed_ids=[],
            local_now=local_now,
            chaos_mode=False,
            rng=Random(4),
        )

        self.assertIsNotNone(result.soda)
        self.assertEqual(result.soda.name, "Sprite")
        self.assertEqual(result.status, "ready")


if __name__ == "__main__":
    unittest.main()
