from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from app.database import Database
from app.models import CatalogItem, RecommendationResult, Soda, SodaState


class DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "soda-picker.db"
        self.database = Database(str(self.db_path))
        self.database.initialize()
        self.local_now = datetime.fromisoformat("2026-03-13T12:30:00-07:00")
        self.admin = self.database.create_user(username="admin", password="secret123", is_admin=True)
        self.user = self.database.create_user(username="casey", password="soda456", is_admin=False)
        self.other_user = self.database.create_user(username="riley", password="bubbles789", is_admin=False)

    def test_manual_entry_can_be_logged_updated_and_deleted(self) -> None:
        self.database.log_manual_entry(
            self.user.id,
            name="Cold Brew",
            caffeine_mg=95,
            local_now=self.local_now,
            notes="Morning boost",
        )

        entries = self.database.get_today_entries(self.user.id, self.local_now)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.entry_type, "manual")
        self.assertEqual(entry.soda_name, "Cold Brew")

        updated_time = self.local_now.replace(hour=14, minute=15)
        self.assertTrue(
            self.database.update_entry(
                self.user.id,
                entry.id,
                soda_name="Cold Brew Large",
                brand="",
                caffeine_mg=110,
                local_now=updated_time,
                reason="Adjusted after mistake",
                notes="Fixed size",
            )
        )

        updated = self.database.get_entry(self.user.id, entry.id)
        self.assertIsNotNone(updated)
        self.assertEqual(updated.soda_name, "Cold Brew Large")
        self.assertEqual(updated.caffeine_mg, 110)

        self.assertTrue(self.database.delete_entry(self.user.id, entry.id))
        self.assertEqual(self.database.get_today_entries(self.user.id, self.local_now), [])

    def test_soda_state_and_setting_overrides_persist(self) -> None:
        self.database.save_soda_state(
            self.user.id,
            soda_id="cola-1",
            is_available=False,
            preference="favorite",
            temp_ban_until=date(2026, 3, 20),
        )
        states = self.database.get_soda_state_map(self.user.id)
        self.assertFalse(states["cola-1"].is_available)
        self.assertEqual(states["cola-1"].preference, "favorite")

        self.database.set_setting_overrides(
            self.user.id,
            {
                "no_soda_before": "11:00",
                "reminder_enabled": "true",
            }
        )
        overrides = self.database.get_setting_overrides(self.user.id)
        self.assertEqual(overrides["no_soda_before"], "11:00")
        self.assertEqual(overrides["reminder_enabled"], "true")
        self.assertEqual(self.database.get_setting_overrides(self.other_user.id), {})

    def test_recommendation_history_tracks_logged_state(self) -> None:
        soda = Soda(id="cola-1", name="Cola", brand="Example", caffeine_mg=38)
        item = CatalogItem(soda=soda, state=SodaState(soda_id=soda.id))
        recommendation_id = self.database.log_recommendation(
            self.user.id,
            RecommendationResult(
                status="ready",
                headline="Fizz forecast",
                reason="Fits the rules",
                soda=item,
                projected_total_mg=38,
            ),
            self.local_now,
        )

        self.database.log_catalog_consumption(
            self.user.id,
            soda,
            self.local_now,
            reason="Fits the rules",
            recommendation_id=recommendation_id,
        )

        history = self.database.get_recent_recommendations(self.user.id, limit=5)
        self.assertEqual(len(history), 1)
        self.assertTrue(history[0].was_logged)
        self.assertEqual(self.database.get_recent_recommendations(self.other_user.id, limit=5), [])

    def test_passport_entries_can_be_logged_updated_exported_and_deleted(self) -> None:
        self.database.add_passport_entry(
            self.user.id,
            soda_name="Inca Kola",
            brand="Coca-Cola",
            country="Peru",
            region="Lima Province",
            city="Lima",
            category="Fruit soda",
            tried_on=date(2026, 3, 1),
            where_tried="Corner store",
            rating=4,
            would_try_again=True,
            notes="Bright bubblegum thing",
        )
        self.database.add_passport_entry(
            self.user.id,
            soda_name="Ramune",
            brand="Hata",
            country="Japan",
            region="Tokyo",
            city="Tokyo",
            category="Marble bottle",
            tried_on=date(2026, 3, 10),
            where_tried="Arcade",
            rating=None,
            would_try_again=False,
            notes="Fun bottle",
        )

        entries = self.database.list_passport_entries(self.user.id, limit=10)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].soda_name, "Ramune")
        self.assertEqual(entries[1].country, "Peru")

        summary = self.database.get_passport_summary(self.user.id)
        self.assertEqual(summary.total_entries, 2)
        self.assertEqual(summary.unique_sodas, 2)
        self.assertEqual(summary.countries_count, 2)
        self.assertEqual(summary.latest_country, "Japan")

        ramune = entries[0]
        self.assertTrue(
            self.database.update_passport_entry(
                self.user.id,
                ramune.id,
                soda_name="Ramune",
                brand="Hata",
                country="Japan",
                region="Tokyo",
                city="Akihabara",
                category="Lemon-lime",
                tried_on=date(2026, 3, 10),
                where_tried="Arcade",
                rating=5,
                would_try_again=True,
                notes="Better than expected",
            )
        )

        updated = self.database.get_passport_entry(self.user.id, ramune.id)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.city, "Akihabara")
        self.assertEqual(updated.rating, 5)
        self.assertTrue(updated.would_try_again)

        exported = self.database.export_passport_csv(self.user.id)
        self.assertIn("soda_name,brand,country", exported)
        self.assertIn("Ramune,Hata,Japan", exported)

        self.assertTrue(self.database.delete_passport_entry(self.user.id, ramune.id))
        remaining = self.database.list_passport_entries(self.user.id, limit=10)
        self.assertEqual(len(remaining), 1)

    def test_wishlist_entries_can_be_added_updated_exported_and_deleted(self) -> None:
        first_id = self.database.add_wishlist_entry(
            self.user.id,
            soda_name="Green River",
            brand="Sprecher",
            country="United States",
            category="Lime soda",
            source_type="manual",
            source_ref="",
            priority=5,
            status="active",
            notes="Find it on the next Midwest trip",
        )
        second_id = self.database.add_wishlist_entry(
            self.user.id,
            soda_name="Beverly",
            brand="Coca-Cola",
            country="Italy",
            category="Bitter soda",
            source_type="passport",
            source_ref="11",
            priority=2,
            status="found",
            notes="Already had a can once",
        )

        entries = self.database.list_wishlist_entries(self.user.id, limit=10)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].id, first_id)
        self.assertEqual(entries[0].status, "active")
        self.assertEqual(entries[1].id, second_id)

        existing = self.database.find_active_wishlist_entry(self.user.id, soda_name="Green River", brand="Sprecher")
        self.assertIsNotNone(existing)
        assert existing is not None
        self.assertEqual(existing.priority, 5)

        summary = self.database.get_wishlist_summary(self.user.id)
        self.assertEqual(summary.total_entries, 2)
        self.assertEqual(summary.active_entries, 1)
        self.assertEqual(summary.found_entries, 1)
        self.assertEqual(summary.high_priority_entries, 1)

        self.assertTrue(
            self.database.update_wishlist_entry(
                self.user.id,
                first_id,
                soda_name="Green River",
                brand="Sprecher",
                country="United States",
                category="Lime soda",
                priority=4,
                status="found",
                notes="Tracked down already",
            )
        )

        updated = self.database.get_wishlist_entry(self.user.id, first_id)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.status, "found")
        self.assertEqual(updated.priority, 4)

        exported = self.database.export_wishlist_csv(self.user.id)
        self.assertIn("soda_name,brand,country", exported)
        self.assertIn("Green River,Sprecher,United States", exported)

        self.assertTrue(self.database.delete_wishlist_entry(self.user.id, second_id))
        remaining = self.database.list_wishlist_entries(self.user.id, limit=10)
        self.assertEqual(len(remaining), 1)

    def test_accounts_authenticate_and_keep_user_data_separate(self) -> None:
        self.assertIsNotNone(self.database.authenticate_user("admin", "secret123"))
        self.assertIsNotNone(self.database.authenticate_user("casey", "soda456"))
        self.assertIsNone(self.database.authenticate_user("casey", "wrong-password"))
        self.assertEqual(self.database.count_admin_users(), 1)

        self.database.log_manual_entry(
            self.user.id,
            name="Tea",
            caffeine_mg=40,
            local_now=self.local_now,
        )
        self.database.log_manual_entry(
            self.other_user.id,
            name="Espresso",
            caffeine_mg=80,
            local_now=self.local_now,
        )

        user_entries = self.database.get_recent_entries(self.user.id, limit=10)
        other_entries = self.database.get_recent_entries(self.other_user.id, limit=10)
        self.assertEqual([entry.soda_name for entry in user_entries], ["Tea"])
        self.assertEqual([entry.soda_name for entry in other_entries], ["Espresso"])

        updated = self.database.update_user(self.other_user.id, password="new-pass-123", is_admin=True)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertTrue(updated.is_admin)
        self.assertEqual(self.database.count_admin_users(), 2)
        self.assertIsNotNone(self.database.authenticate_user("riley", "new-pass-123"))


if __name__ == "__main__":
    unittest.main()
