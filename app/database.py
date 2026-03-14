from __future__ import annotations

import csv
import io
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from .models import (
    PREFERENCE_CHOICES,
    PREFERENCE_NEUTRAL,
    ConsumptionEntry,
    PassportEntry,
    PassportSummary,
    RecommendationHistoryEntry,
    RecommendationResult,
    Soda,
    SodaState,
    WISHLIST_STATUS_ACTIVE,
    WISHLIST_STATUS_CHOICES,
    WishlistEntry,
    WishlistSummary,
)


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS consumption_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soda_id TEXT NOT NULL,
                    soda_name TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    caffeine_mg REAL NOT NULL,
                    consumed_at_utc TEXT NOT NULL,
                    consumed_at_local TEXT NOT NULL,
                    local_date TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    chaos_mode INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self._ensure_column(connection, "consumption_log", "entry_type", "TEXT NOT NULL DEFAULT 'catalog'")
            self._ensure_column(connection, "consumption_log", "notes", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "consumption_log", "recommendation_id", "INTEGER")
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_consumption_log_local_date
                ON consumption_log (local_date)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_consumption_log_consumed_at_utc
                ON consumption_log (consumed_at_utc DESC)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS soda_state (
                    soda_id TEXT PRIMARY KEY,
                    is_available INTEGER NOT NULL DEFAULT 1,
                    preference TEXT NOT NULL DEFAULT 'neutral',
                    temp_ban_until TEXT NOT NULL DEFAULT ''
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS setting_override (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS recommendation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soda_id TEXT NOT NULL,
                    soda_name TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    caffeine_mg REAL NOT NULL DEFAULT 0,
                    recommended_at_utc TEXT NOT NULL,
                    recommended_at_local TEXT NOT NULL,
                    local_date TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    chaos_mode INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'ready',
                    projected_total_mg REAL,
                    was_logged INTEGER NOT NULL DEFAULT 0,
                    rejection_summary TEXT NOT NULL DEFAULT ''
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_recommendation_history_local_date
                ON recommendation_history (local_date)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_recommendation_history_created
                ON recommendation_history (recommended_at_utc DESC)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS soda_passport (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soda_name TEXT NOT NULL,
                    brand TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT '',
                    region TEXT NOT NULL DEFAULT '',
                    city TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    tried_on TEXT NOT NULL,
                    where_tried TEXT NOT NULL DEFAULT '',
                    rating INTEGER,
                    would_try_again INTEGER NOT NULL DEFAULT 0,
                    notes TEXT NOT NULL DEFAULT '',
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_soda_passport_tried_on
                ON soda_passport (tried_on DESC, created_at_utc DESC)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS soda_wishlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soda_name TEXT NOT NULL,
                    brand TEXT NOT NULL DEFAULT '',
                    country TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    source_type TEXT NOT NULL DEFAULT 'manual',
                    source_ref TEXT NOT NULL DEFAULT '',
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'active',
                    notes TEXT NOT NULL DEFAULT '',
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_soda_wishlist_status_updated
                ON soda_wishlist (status, updated_at_utc DESC)
                """
            )

    def get_today_entries(self, local_now: datetime) -> list[ConsumptionEntry]:
        return self.get_entries_for_date(local_now.date())

    def get_entries_for_date(self, local_date: date) -> list[ConsumptionEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM consumption_log
                WHERE local_date = ?
                ORDER BY consumed_at_utc DESC
                """,
                (local_date.isoformat(),),
            ).fetchall()
        return [self._map_consumption_entry(row) for row in rows]

    def get_recent_entries(self, *, limit: int = 50) -> list[ConsumptionEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM consumption_log
                ORDER BY consumed_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._map_consumption_entry(row) for row in rows]

    def get_entry(self, entry_id: int) -> ConsumptionEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM consumption_log
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()
        if row is None:
            return None
        return self._map_consumption_entry(row)

    def get_today_caffeine_total(self, local_now: datetime) -> float:
        local_date = local_now.date().isoformat()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(caffeine_mg), 0) AS total
                FROM consumption_log
                WHERE local_date = ?
                """,
                (local_date,),
            ).fetchone()
        return float(row["total"])

    def get_recent_consumed_catalog_ids(self, *, limit: int = 6) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT soda_id
                FROM consumption_log
                WHERE entry_type = 'catalog'
                ORDER BY consumed_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [row["soda_id"] for row in rows if row["soda_id"]]

    def get_recent_recommended_ids(self, *, limit: int = 6) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT soda_id
                FROM recommendation_history
                WHERE soda_id != ''
                ORDER BY recommended_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [row["soda_id"] for row in rows if row["soda_id"]]

    def log_catalog_consumption(
        self,
        soda: Soda,
        local_now: datetime,
        *,
        reason: str = "",
        chaos_mode: bool = False,
        notes: str = "",
        recommendation_id: int | None = None,
    ) -> None:
        self._insert_consumption(
            soda_id=soda.id,
            soda_name=soda.name,
            brand=soda.brand,
            caffeine_mg=soda.caffeine_mg,
            local_now=local_now,
            entry_type="catalog",
            reason=reason,
            chaos_mode=chaos_mode,
            notes=notes,
            recommendation_id=recommendation_id,
        )

    def log_manual_entry(
        self,
        *,
        name: str,
        caffeine_mg: float,
        local_now: datetime,
        reason: str = "Manual caffeine entry",
        notes: str = "",
    ) -> None:
        manual_id = f"manual-{int(local_now.timestamp())}"
        self._insert_consumption(
            soda_id=manual_id,
            soda_name=name,
            brand="",
            caffeine_mg=caffeine_mg,
            local_now=local_now,
            entry_type="manual",
            reason=reason,
            chaos_mode=False,
            notes=notes,
            recommendation_id=None,
        )

    def update_entry(
        self,
        entry_id: int,
        *,
        soda_name: str,
        brand: str,
        caffeine_mg: float,
        local_now: datetime,
        reason: str,
        notes: str,
    ) -> bool:
        existing = self.get_entry(entry_id)
        if existing is None:
            return False

        utc_now = local_now.astimezone(timezone.utc)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE consumption_log
                SET soda_name = ?,
                    brand = ?,
                    caffeine_mg = ?,
                    consumed_at_utc = ?,
                    consumed_at_local = ?,
                    local_date = ?,
                    reason = ?,
                    notes = ?
                WHERE id = ?
                """,
                (
                    soda_name.strip(),
                    brand.strip(),
                    caffeine_mg,
                    utc_now.isoformat(),
                    local_now.isoformat(),
                    local_now.date().isoformat(),
                    reason.strip(),
                    notes.strip(),
                    entry_id,
                ),
            )

        if existing.recommendation_id is not None:
            self._recompute_recommendation_logged(existing.recommendation_id)
        return True

    def delete_entry(self, entry_id: int) -> bool:
        existing = self.get_entry(entry_id)
        if existing is None:
            return False

        with self._connect() as connection:
            connection.execute("DELETE FROM consumption_log WHERE id = ?", (entry_id,))

        if existing.recommendation_id is not None:
            self._recompute_recommendation_logged(existing.recommendation_id)
        return True

    def get_soda_state_map(self) -> dict[str, SodaState]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT soda_id, is_available, preference, temp_ban_until
                FROM soda_state
                """
            ).fetchall()

        state_map: dict[str, SodaState] = {}
        for row in rows:
            temp_ban_until = row["temp_ban_until"].strip()
            state_map[row["soda_id"]] = SodaState(
                soda_id=row["soda_id"],
                is_available=bool(row["is_available"]),
                preference=row["preference"] if row["preference"] in PREFERENCE_CHOICES else PREFERENCE_NEUTRAL,
                temp_ban_until=date.fromisoformat(temp_ban_until) if temp_ban_until else None,
            )
        return state_map

    def save_soda_state(
        self,
        *,
        soda_id: str,
        is_available: bool,
        preference: str,
        temp_ban_until: date | None,
    ) -> None:
        normalized_preference = preference if preference in PREFERENCE_CHOICES else PREFERENCE_NEUTRAL
        temp_ban_text = temp_ban_until.isoformat() if temp_ban_until else ""
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO soda_state (soda_id, is_available, preference, temp_ban_until)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(soda_id) DO UPDATE SET
                    is_available = excluded.is_available,
                    preference = excluded.preference,
                    temp_ban_until = excluded.temp_ban_until
                """,
                (soda_id, int(is_available), normalized_preference, temp_ban_text),
            )

    def get_setting_overrides(self) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM setting_override").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def set_setting_overrides(self, overrides: dict[str, str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            for key, value in overrides.items():
                if value == "":
                    connection.execute("DELETE FROM setting_override WHERE key = ?", (key,))
                    continue

                connection.execute(
                    """
                    INSERT INTO setting_override (key, value, updated_at_utc)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (key, value, now),
                )

    def clear_setting_overrides(self) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM setting_override")

    def log_recommendation(self, recommendation: RecommendationResult, local_now: datetime) -> int:
        utc_now = local_now.astimezone(timezone.utc)
        item = recommendation.soda
        soda_id = item.id if item is not None else ""
        soda_name = item.soda.name if item is not None else recommendation.headline
        brand = item.soda.brand if item is not None else ""
        caffeine_mg = item.soda.caffeine_mg if item is not None else 0.0
        rejection_summary = " | ".join(
            f"{detail.soda_name}: {detail.reason}" for detail in recommendation.rejected_options
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO recommendation_history (
                    soda_id,
                    soda_name,
                    brand,
                    caffeine_mg,
                    recommended_at_utc,
                    recommended_at_local,
                    local_date,
                    reason,
                    chaos_mode,
                    status,
                    projected_total_mg,
                    was_logged,
                    rejection_summary
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    soda_id,
                    soda_name,
                    brand,
                    caffeine_mg,
                    utc_now.isoformat(),
                    local_now.isoformat(),
                    local_now.date().isoformat(),
                    recommendation.reason,
                    int(recommendation.chaos_mode),
                    recommendation.status,
                    recommendation.projected_total_mg,
                    0,
                    rejection_summary,
                ),
            )
        return int(cursor.lastrowid)

    def mark_recommendation_logged(self, recommendation_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE recommendation_history SET was_logged = 1 WHERE id = ?",
                (recommendation_id,),
            )

    def get_recent_recommendations(self, *, limit: int = 12) -> list[RecommendationHistoryEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_id, soda_name, brand, caffeine_mg, recommended_at_local,
                       reason, chaos_mode, status, projected_total_mg, was_logged,
                       rejection_summary
                FROM recommendation_history
                ORDER BY recommended_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            RecommendationHistoryEntry(
                id=row["id"],
                soda_id=row["soda_id"],
                soda_name=row["soda_name"],
                brand=row["brand"],
                caffeine_mg=float(row["caffeine_mg"]),
                recommended_at_local=datetime.fromisoformat(row["recommended_at_local"]),
                reason=row["reason"],
                chaos_mode=bool(row["chaos_mode"]),
                status=row["status"],
                projected_total_mg=row["projected_total_mg"],
                was_logged=bool(row["was_logged"]),
                rejection_summary=row["rejection_summary"],
            )
            for row in rows
        ]

    def add_passport_entry(
        self,
        *,
        soda_name: str,
        brand: str,
        country: str,
        region: str,
        city: str,
        category: str,
        tried_on: date,
        where_tried: str,
        rating: int | None,
        would_try_again: bool,
        notes: str,
    ) -> None:
        created_at_utc = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO soda_passport (
                    soda_name,
                    brand,
                    country,
                    region,
                    city,
                    category,
                    tried_on,
                    where_tried,
                    rating,
                    would_try_again,
                    notes,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    soda_name.strip(),
                    brand.strip(),
                    country.strip(),
                    region.strip(),
                    city.strip(),
                    category.strip(),
                    tried_on.isoformat(),
                    where_tried.strip(),
                    rating,
                    int(would_try_again),
                    notes.strip(),
                    created_at_utc,
                ),
            )

    def list_passport_entries(self, *, limit: int = 250) -> list[PassportEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM soda_passport
                ORDER BY tried_on DESC, created_at_utc DESC, soda_name COLLATE NOCASE ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._map_passport_entry(row) for row in rows]

    def get_passport_entry(self, entry_id: int) -> PassportEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM soda_passport
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()
        if row is None:
            return None
        return self._map_passport_entry(row)

    def update_passport_entry(
        self,
        entry_id: int,
        *,
        soda_name: str,
        brand: str,
        country: str,
        region: str,
        city: str,
        category: str,
        tried_on: date,
        where_tried: str,
        rating: int | None,
        would_try_again: bool,
        notes: str,
    ) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE soda_passport
                SET soda_name = ?,
                    brand = ?,
                    country = ?,
                    region = ?,
                    city = ?,
                    category = ?,
                    tried_on = ?,
                    where_tried = ?,
                    rating = ?,
                    would_try_again = ?,
                    notes = ?
                WHERE id = ?
                """,
                (
                    soda_name.strip(),
                    brand.strip(),
                    country.strip(),
                    region.strip(),
                    city.strip(),
                    category.strip(),
                    tried_on.isoformat(),
                    where_tried.strip(),
                    rating,
                    int(would_try_again),
                    notes.strip(),
                    entry_id,
                ),
            )
        return cursor.rowcount > 0

    def delete_passport_entry(self, entry_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM soda_passport WHERE id = ?", (entry_id,))
        return cursor.rowcount > 0

    def get_passport_summary(self) -> PassportSummary:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_entries,
                    COUNT(DISTINCT LOWER(TRIM(COALESCE(brand, '') || '|' || COALESCE(soda_name, '')))) AS unique_sodas,
                    COUNT(DISTINCT NULLIF(TRIM(country), '')) AS countries_count
                FROM soda_passport
                """
            ).fetchone()
            latest_country_row = connection.execute(
                """
                SELECT country
                FROM soda_passport
                WHERE TRIM(country) != ''
                ORDER BY tried_on DESC, created_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
        return PassportSummary(
            total_entries=int(row["total_entries"]),
            unique_sodas=int(row["unique_sodas"]),
            countries_count=int(row["countries_count"]),
            latest_country=latest_country_row["country"] if latest_country_row else "",
        )

    def find_active_wishlist_entry(self, *, soda_name: str, brand: str = "") -> WishlistEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM soda_wishlist
                WHERE LOWER(TRIM(soda_name)) = LOWER(TRIM(?))
                  AND LOWER(TRIM(brand)) = LOWER(TRIM(?))
                  AND status = ?
                ORDER BY updated_at_utc DESC
                LIMIT 1
                """,
                (soda_name.strip(), brand.strip(), WISHLIST_STATUS_ACTIVE),
            ).fetchone()
        if row is None:
            return None
        return self._map_wishlist_entry(row)

    def add_wishlist_entry(
        self,
        *,
        soda_name: str,
        brand: str,
        country: str,
        category: str,
        source_type: str,
        source_ref: str,
        priority: int,
        status: str,
        notes: str,
    ) -> int:
        normalized_status = status if status in WISHLIST_STATUS_CHOICES else WISHLIST_STATUS_ACTIVE
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO soda_wishlist (
                    soda_name,
                    brand,
                    country,
                    category,
                    source_type,
                    source_ref,
                    priority,
                    status,
                    notes,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    soda_name.strip(),
                    brand.strip(),
                    country.strip(),
                    category.strip(),
                    source_type.strip() or "manual",
                    source_ref.strip(),
                    max(1, min(priority, 5)),
                    normalized_status,
                    notes.strip(),
                    now,
                    now,
                ),
            )
        return int(cursor.lastrowid)

    def list_wishlist_entries(
        self,
        *,
        limit: int = 250,
        include_archived: bool = True,
    ) -> list[WishlistEntry]:
        filters: list[str] = []
        params: list[object] = []
        if not include_archived:
            filters.append("status != ?")
            params.append("archived")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM soda_wishlist
                {where_clause}
                ORDER BY
                    CASE status
                        WHEN 'active' THEN 0
                        WHEN 'found' THEN 1
                        ELSE 2
                    END ASC,
                    priority DESC,
                    updated_at_utc DESC,
                    soda_name COLLATE NOCASE ASC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._map_wishlist_entry(row) for row in rows]

    def get_wishlist_entry(self, entry_id: int) -> WishlistEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM soda_wishlist
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()
        if row is None:
            return None
        return self._map_wishlist_entry(row)

    def update_wishlist_entry(
        self,
        entry_id: int,
        *,
        soda_name: str,
        brand: str,
        country: str,
        category: str,
        priority: int,
        status: str,
        notes: str,
    ) -> bool:
        normalized_status = status if status in WISHLIST_STATUS_CHOICES else WISHLIST_STATUS_ACTIVE
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE soda_wishlist
                SET soda_name = ?,
                    brand = ?,
                    country = ?,
                    category = ?,
                    priority = ?,
                    status = ?,
                    notes = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    soda_name.strip(),
                    brand.strip(),
                    country.strip(),
                    category.strip(),
                    max(1, min(priority, 5)),
                    normalized_status,
                    notes.strip(),
                    now,
                    entry_id,
                ),
            )
        return cursor.rowcount > 0

    def delete_wishlist_entry(self, entry_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM soda_wishlist WHERE id = ?", (entry_id,))
        return cursor.rowcount > 0

    def get_wishlist_summary(self) -> WishlistSummary:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_entries,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_entries,
                    SUM(CASE WHEN status = 'found' THEN 1 ELSE 0 END) AS found_entries,
                    SUM(CASE WHEN status = 'archived' THEN 1 ELSE 0 END) AS archived_entries,
                    SUM(CASE WHEN status = 'active' AND priority >= 4 THEN 1 ELSE 0 END) AS high_priority_entries
                FROM soda_wishlist
                """
            ).fetchone()
        return WishlistSummary(
            total_entries=int(row["total_entries"]),
            active_entries=int(row["active_entries"] or 0),
            found_entries=int(row["found_entries"] or 0),
            archived_entries=int(row["archived_entries"] or 0),
            high_priority_entries=int(row["high_priority_entries"] or 0),
        )

    def export_consumption_csv(self) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, local_date, reason, notes, chaos_mode, recommendation_id
                FROM consumption_log
                ORDER BY consumed_at_utc DESC
                """
            ).fetchall()

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "id",
                "entry_type",
                "soda_id",
                "soda_name",
                "brand",
                "caffeine_mg",
                "consumed_at_local",
                "local_date",
                "reason",
                "notes",
                "chaos_mode",
                "recommendation_id",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["entry_type"],
                    row["soda_id"],
                    row["soda_name"],
                    row["brand"],
                    row["caffeine_mg"],
                    row["consumed_at_local"],
                    row["local_date"],
                    row["reason"],
                    row["notes"],
                    row["chaos_mode"],
                    row["recommendation_id"],
                ]
            )
        return buffer.getvalue()

    def export_passport_csv(self) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM soda_passport
                ORDER BY tried_on DESC, created_at_utc DESC, soda_name COLLATE NOCASE ASC
                """
            ).fetchall()

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "id",
                "soda_name",
                "brand",
                "country",
                "region",
                "city",
                "category",
                "tried_on",
                "where_tried",
                "rating",
                "would_try_again",
                "notes",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["soda_name"],
                    row["brand"],
                    row["country"],
                    row["region"],
                    row["city"],
                    row["category"],
                    row["tried_on"],
                    row["where_tried"],
                    row["rating"],
                    row["would_try_again"],
                    row["notes"],
                ]
            )
        return buffer.getvalue()

    def export_wishlist_csv(self) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM soda_wishlist
                ORDER BY
                    CASE status
                        WHEN 'active' THEN 0
                        WHEN 'found' THEN 1
                        ELSE 2
                    END ASC,
                    priority DESC,
                    updated_at_utc DESC,
                    soda_name COLLATE NOCASE ASC
                """
            ).fetchall()

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "id",
                "soda_name",
                "brand",
                "country",
                "category",
                "source_type",
                "source_ref",
                "priority",
                "status",
                "notes",
                "updated_at_utc",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["soda_name"],
                    row["brand"],
                    row["country"],
                    row["category"],
                    row["source_type"],
                    row["source_ref"],
                    row["priority"],
                    row["status"],
                    row["notes"],
                    row["updated_at_utc"],
                ]
            )
        return buffer.getvalue()

    def export_recommendation_csv(self) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_id, soda_name, brand, caffeine_mg, recommended_at_local,
                       local_date, reason, chaos_mode, status, projected_total_mg,
                       was_logged, rejection_summary
                FROM recommendation_history
                ORDER BY recommended_at_utc DESC
                """
            ).fetchall()

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "id",
                "soda_id",
                "soda_name",
                "brand",
                "caffeine_mg",
                "recommended_at_local",
                "local_date",
                "reason",
                "chaos_mode",
                "status",
                "projected_total_mg",
                "was_logged",
                "rejection_summary",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["soda_id"],
                    row["soda_name"],
                    row["brand"],
                    row["caffeine_mg"],
                    row["recommended_at_local"],
                    row["local_date"],
                    row["reason"],
                    row["chaos_mode"],
                    row["status"],
                    row["projected_total_mg"],
                    row["was_logged"],
                    row["rejection_summary"],
                ]
            )
        return buffer.getvalue()

    def create_database_backup(self, backup_dir: str) -> Path:
        backup_root = Path(backup_dir)
        backup_root.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        backup_path = backup_root / f"soda-picker-{timestamp}.sqlite3"
        with self._connect() as source, sqlite3.connect(backup_path) as destination:
            source.backup(destination)
        return backup_path

    def _insert_consumption(
        self,
        *,
        soda_id: str,
        soda_name: str,
        brand: str,
        caffeine_mg: float,
        local_now: datetime,
        entry_type: str,
        reason: str,
        chaos_mode: bool,
        notes: str,
        recommendation_id: int | None,
    ) -> None:
        utc_now = local_now.astimezone(timezone.utc)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO consumption_log (
                    soda_id,
                    soda_name,
                    brand,
                    caffeine_mg,
                    consumed_at_utc,
                    consumed_at_local,
                    local_date,
                    reason,
                    chaos_mode,
                    entry_type,
                    notes,
                    recommendation_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    soda_id,
                    soda_name,
                    brand,
                    caffeine_mg,
                    utc_now.isoformat(),
                    local_now.isoformat(),
                    local_now.date().isoformat(),
                    reason.strip(),
                    int(chaos_mode),
                    entry_type,
                    notes.strip(),
                    recommendation_id,
                ),
            )
        if recommendation_id is not None:
            self.mark_recommendation_logged(recommendation_id)

    def _recompute_recommendation_logged(self, recommendation_id: int) -> None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS entry_count
                FROM consumption_log
                WHERE recommendation_id = ?
                """,
                (recommendation_id,),
            ).fetchone()
            was_logged = 1 if row["entry_count"] else 0
            connection.execute(
                """
                UPDATE recommendation_history
                SET was_logged = ?
                WHERE id = ?
                """,
                (was_logged, recommendation_id),
            )

    def _map_consumption_entry(self, row: sqlite3.Row) -> ConsumptionEntry:
        return ConsumptionEntry(
            id=row["id"],
            entry_type=row["entry_type"],
            soda_id=row["soda_id"],
            soda_name=row["soda_name"],
            brand=row["brand"],
            caffeine_mg=float(row["caffeine_mg"]),
            consumed_at_local=datetime.fromisoformat(row["consumed_at_local"]),
            recommendation_id=row["recommendation_id"],
            reason=row["reason"],
            chaos_mode=bool(row["chaos_mode"]),
            notes=row["notes"],
        )

    def _map_passport_entry(self, row: sqlite3.Row) -> PassportEntry:
        return PassportEntry(
            id=row["id"],
            soda_name=row["soda_name"],
            brand=row["brand"],
            country=row["country"],
            region=row["region"],
            city=row["city"],
            category=row["category"],
            tried_on=date.fromisoformat(row["tried_on"]),
            where_tried=row["where_tried"],
            rating=int(row["rating"]) if row["rating"] is not None else None,
            would_try_again=bool(row["would_try_again"]),
            notes=row["notes"],
        )

    def _map_wishlist_entry(self, row: sqlite3.Row) -> WishlistEntry:
        return WishlistEntry(
            id=row["id"],
            soda_name=row["soda_name"],
            brand=row["brand"],
            country=row["country"],
            category=row["category"],
            source_type=row["source_type"],
            source_ref=row["source_ref"],
            priority=int(row["priority"]),
            status=row["status"] if row["status"] in WISHLIST_STATUS_CHOICES else WISHLIST_STATUS_ACTIVE,
            notes=row["notes"],
            updated_at_utc=datetime.fromisoformat(row["updated_at_utc"]) if row["updated_at_utc"] else None,
        )

    def _ensure_column(self, connection: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = {row["name"] for row in rows}
        if column_name not in columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection
