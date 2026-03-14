from __future__ import annotations

import csv
import io
import re
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

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
    UserAccount,
    UserAuthRecord,
    WISHLIST_STATUS_ACTIVE,
    WISHLIST_STATUS_CHOICES,
    WishlistEntry,
    WishlistSummary,
)
from .security import hash_password, password_matches

if TYPE_CHECKING:
    from .config import Settings

LOCAL_MODE_USERNAME = "__local__"
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,32}$")
USER_CONSUMPTION_TABLE = "user_consumption_log"
USER_SODA_STATE_TABLE = "user_soda_state"
USER_SETTING_OVERRIDE_TABLE = "user_setting_override"
USER_RECOMMENDATION_TABLE = "user_recommendation_history"
USER_PASSPORT_TABLE = "user_soda_passport"
USER_WISHLIST_TABLE = "user_soda_wishlist"
USER_OWNED_TABLES = (
    USER_CONSUMPTION_TABLE,
    USER_SODA_STATE_TABLE,
    USER_SETTING_OVERRIDE_TABLE,
    USER_RECOMMENDATION_TABLE,
    USER_PASSPORT_TABLE,
    USER_WISHLIST_TABLE,
)


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)

    def initialize(self, settings: Settings | None = None) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            self._initialize_user_accounts(connection)
            local_user = self._ensure_local_user(connection)
            bootstrap_user = self._ensure_bootstrap_admin(connection, settings)
            owner_user = bootstrap_user or local_user

            self._initialize_user_tables(connection)
            self._migrate_legacy_tables(connection, owner_user_id=owner_user.id)

            if (
                bootstrap_user is not None
                and bootstrap_user.id != local_user.id
                and self._should_adopt_local_data(
                    connection,
                    source_user_id=local_user.id,
                    target_user_id=bootstrap_user.id,
                )
            ):
                self._reassign_user_data(
                    connection,
                    from_user_id=local_user.id,
                    to_user_id=bootstrap_user.id,
                )

            if settings is not None and settings.access_control_enabled and not self._has_loginable_users(connection):
                raise ValueError(
                    "Access control is enabled but no app accounts can log in. "
                    "Set ACCESS_CONTROL_USERNAME and ACCESS_CONTROL_PASSWORD to bootstrap the first admin."
                )

    def get_local_user(self) -> UserAccount:
        user = self.get_user_by_username(LOCAL_MODE_USERNAME)
        if user is None:
            raise RuntimeError("The local fallback user is missing.")
        return user

    def get_user_by_id(self, user_id: int) -> UserAccount | None:
        with self._connect() as connection:
            return self._get_user_by_id(connection, user_id)

    def get_user_by_username(self, username: str) -> UserAccount | None:
        cleaned = username.strip()
        if not cleaned:
            return None
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, username, is_admin, created_at_utc, updated_at_utc
                FROM user_account
                WHERE username = ?
                """,
                (cleaned,),
            ).fetchone()
        if row is None:
            return None
        return self._map_user_account(row)

    def get_user_auth_record(self, username: str) -> UserAuthRecord | None:
        cleaned = username.strip()
        if not cleaned:
            return None
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, username, password_hash, is_admin, created_at_utc, updated_at_utc
                FROM user_account
                WHERE username = ?
                """,
                (cleaned,),
            ).fetchone()
        if row is None:
            return None
        return UserAuthRecord(
            user=self._map_user_account(row),
            password_hash=row["password_hash"] or "",
        )

    def authenticate_user(self, username: str, password: str) -> UserAccount | None:
        record = self.get_user_auth_record(username)
        if record is None or not record.password_hash:
            return None
        if not password_matches(password, record.password_hash):
            return None
        return record.user

    def user_exists(self, username: str) -> bool:
        record = self.get_user_auth_record(username)
        return record is not None and bool(record.password_hash)

    def list_users(self, *, include_local: bool = False) -> list[UserAccount]:
        filters: list[str] = []
        params: list[object] = []
        if not include_local:
            filters.append("username != ?")
            params.append(LOCAL_MODE_USERNAME)

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, username, is_admin, created_at_utc, updated_at_utc
                FROM user_account
                {where_clause}
                ORDER BY is_admin DESC, username COLLATE NOCASE ASC
                """,
                tuple(params),
            ).fetchall()
        return [self._map_user_account(row) for row in rows]

    def count_users(self, *, include_local: bool = False) -> int:
        filters: list[str] = []
        params: list[object] = []
        if not include_local:
            filters.append("username != ?")
            params.append(LOCAL_MODE_USERNAME)

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        with self._connect() as connection:
            row = connection.execute(
                f"SELECT COUNT(*) AS total FROM user_account {where_clause}",
                tuple(params),
            ).fetchone()
        return int(row["total"])

    def count_admin_users(self, *, include_local: bool = False) -> int:
        with self._connect() as connection:
            return self._admin_count(connection, include_local=include_local)

    def create_user(self, *, username: str, password: str, is_admin: bool) -> UserAccount:
        normalized_username = self._normalize_username(username)
        normalized_password = self._normalize_password(password)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO user_account (username, password_hash, is_admin, created_at_utc, updated_at_utc)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_username,
                        hash_password(normalized_password),
                        int(is_admin),
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("That username already exists.") from exc

            row = connection.execute(
                """
                SELECT id, username, is_admin, created_at_utc, updated_at_utc
                FROM user_account
                WHERE id = ?
                """,
                (int(cursor.lastrowid),),
            ).fetchone()
        if row is None:
            raise RuntimeError("Failed to read back the new user account.")
        return self._map_user_account(row)

    def update_user(self, user_id: int, *, password: str | None = None, is_admin: bool | None = None) -> UserAccount | None:
        with self._connect() as connection:
            existing = self._get_user_with_password(connection, user_id)
            if existing is None:
                return None

            if existing["username"] == LOCAL_MODE_USERNAME and is_admin is False:
                raise ValueError("The local fallback account must stay an admin.")

            new_is_admin = bool(existing["is_admin"]) if is_admin is None else bool(is_admin)
            if bool(existing["is_admin"]) and not new_is_admin and self._admin_count(connection, include_local=False) <= 1:
                raise ValueError("Soda Picker needs at least one admin account.")

            new_password_hash = existing["password_hash"] or ""
            if password is not None and password.strip():
                new_password_hash = hash_password(self._normalize_password(password))

            updated_at = datetime.now(timezone.utc).isoformat()
            connection.execute(
                """
                UPDATE user_account
                SET password_hash = ?, is_admin = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (new_password_hash, int(new_is_admin), updated_at, user_id),
            )
            updated = self._get_user_by_id(connection, user_id)
        return updated

    def delete_user(self, user_id: int, *, acting_user_id: int | None = None) -> bool:
        with self._connect() as connection:
            existing = self._get_user_by_id(connection, user_id)
            if existing is None:
                return False

            if existing.username == LOCAL_MODE_USERNAME:
                raise ValueError("The local fallback account cannot be deleted.")
            if acting_user_id is not None and acting_user_id == user_id:
                raise ValueError("You cannot delete the account you are signed in with.")
            if existing.is_admin and self._admin_count(connection, include_local=False) <= 1:
                raise ValueError("Soda Picker needs at least one admin account.")

            cursor = connection.execute("DELETE FROM user_account WHERE id = ?", (user_id,))
        return cursor.rowcount > 0

    def get_today_entries(self, user_id: int, local_now: datetime) -> list[ConsumptionEntry]:
        return self.get_entries_for_date(user_id, local_now.date())

    def get_entries_for_date(self, user_id: int, local_date: date) -> list[ConsumptionEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ? AND local_date = ?
                ORDER BY consumed_at_utc DESC
                """,
                (user_id, local_date.isoformat()),
            ).fetchall()
        return [self._map_consumption_entry(row) for row in rows]

    def get_recent_entries(self, user_id: int, *, limit: int = 50) -> list[ConsumptionEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ?
                ORDER BY consumed_at_utc DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [self._map_consumption_entry(row) for row in rows]

    def get_entry(self, user_id: int, entry_id: int) -> ConsumptionEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, reason, chaos_mode, notes, recommendation_id
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ? AND id = ?
                """,
                (user_id, entry_id),
            ).fetchone()
        if row is None:
            return None
        return self._map_consumption_entry(row)

    def get_today_caffeine_total(self, user_id: int, local_now: datetime) -> float:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT COALESCE(SUM(caffeine_mg), 0) AS total
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ? AND local_date = ?
                """,
                (user_id, local_now.date().isoformat()),
            ).fetchone()
        return float(row["total"])

    def get_recent_consumed_catalog_ids(self, user_id: int, *, limit: int = 6) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT soda_id
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ? AND entry_type = 'catalog'
                ORDER BY consumed_at_utc DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [row["soda_id"] for row in rows if row["soda_id"]]

    def get_recent_recommended_ids(self, user_id: int, *, limit: int = 6) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT soda_id
                FROM {USER_RECOMMENDATION_TABLE}
                WHERE user_id = ? AND soda_id != ''
                ORDER BY recommended_at_utc DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [row["soda_id"] for row in rows if row["soda_id"]]

    def log_catalog_consumption(
        self,
        user_id: int,
        soda: Soda,
        local_now: datetime,
        *,
        reason: str = "",
        chaos_mode: bool = False,
        notes: str = "",
        recommendation_id: int | None = None,
    ) -> None:
        self._insert_consumption(
            user_id=user_id,
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
        user_id: int,
        *,
        name: str,
        caffeine_mg: float,
        local_now: datetime,
        reason: str = "Manual caffeine entry",
        notes: str = "",
    ) -> None:
        manual_id = f"manual-{int(local_now.timestamp())}"
        self._insert_consumption(
            user_id=user_id,
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
        user_id: int,
        entry_id: int,
        *,
        soda_name: str,
        brand: str,
        caffeine_mg: float,
        local_now: datetime,
        reason: str,
        notes: str,
    ) -> bool:
        existing = self.get_entry(user_id, entry_id)
        if existing is None:
            return False

        utc_now = local_now.astimezone(timezone.utc)
        with self._connect() as connection:
            connection.execute(
                f"""
                UPDATE {USER_CONSUMPTION_TABLE}
                SET soda_name = ?,
                    brand = ?,
                    caffeine_mg = ?,
                    consumed_at_utc = ?,
                    consumed_at_local = ?,
                    local_date = ?,
                    reason = ?,
                    notes = ?
                WHERE user_id = ? AND id = ?
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
                    user_id,
                    entry_id,
                ),
            )

        if existing.recommendation_id is not None:
            self._recompute_recommendation_logged(user_id, existing.recommendation_id)
        return True

    def delete_entry(self, user_id: int, entry_id: int) -> bool:
        existing = self.get_entry(user_id, entry_id)
        if existing is None:
            return False

        with self._connect() as connection:
            connection.execute(
                f"DELETE FROM {USER_CONSUMPTION_TABLE} WHERE user_id = ? AND id = ?",
                (user_id, entry_id),
            )

        if existing.recommendation_id is not None:
            self._recompute_recommendation_logged(user_id, existing.recommendation_id)
        return True

    def get_soda_state_map(self, user_id: int) -> dict[str, SodaState]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT soda_id, is_available, preference, temp_ban_until
                FROM {USER_SODA_STATE_TABLE}
                WHERE user_id = ?
                """,
                (user_id,),
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
        user_id: int,
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
                f"""
                INSERT INTO {USER_SODA_STATE_TABLE} (user_id, soda_id, is_available, preference, temp_ban_until)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, soda_id) DO UPDATE SET
                    is_available = excluded.is_available,
                    preference = excluded.preference,
                    temp_ban_until = excluded.temp_ban_until
                """,
                (user_id, soda_id, int(is_available), normalized_preference, temp_ban_text),
            )

    def get_setting_overrides(self, user_id: int) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT key, value FROM {USER_SETTING_OVERRIDE_TABLE} WHERE user_id = ?",
                (user_id,),
            ).fetchall()
        return {row["key"]: row["value"] for row in rows}

    def set_setting_overrides(self, user_id: int, overrides: dict[str, str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            for key, value in overrides.items():
                if value == "":
                    connection.execute(
                        f"DELETE FROM {USER_SETTING_OVERRIDE_TABLE} WHERE user_id = ? AND key = ?",
                        (user_id, key),
                    )
                    continue

                connection.execute(
                    f"""
                    INSERT INTO {USER_SETTING_OVERRIDE_TABLE} (user_id, key, value, updated_at_utc)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id, key) DO UPDATE SET
                        value = excluded.value,
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (user_id, key, value, now),
                )

    def clear_setting_overrides(self, user_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                f"DELETE FROM {USER_SETTING_OVERRIDE_TABLE} WHERE user_id = ?",
                (user_id,),
            )

    def log_recommendation(self, user_id: int, recommendation: RecommendationResult, local_now: datetime) -> int:
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
                f"""
                INSERT INTO {USER_RECOMMENDATION_TABLE} (
                    user_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
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

    def mark_recommendation_logged(self, user_id: int, recommendation_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                f"UPDATE {USER_RECOMMENDATION_TABLE} SET was_logged = 1 WHERE user_id = ? AND id = ?",
                (user_id, recommendation_id),
            )

    def get_recent_recommendations(self, user_id: int, *, limit: int = 12) -> list[RecommendationHistoryEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_id, soda_name, brand, caffeine_mg, recommended_at_local,
                       reason, chaos_mode, status, projected_total_mg, was_logged,
                       rejection_summary
                FROM {USER_RECOMMENDATION_TABLE}
                WHERE user_id = ?
                ORDER BY recommended_at_utc DESC
                LIMIT ?
                """,
                (user_id, limit),
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
        user_id: int,
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
    ) -> int:
        created_at_utc = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                f"""
                INSERT INTO {USER_PASSPORT_TABLE} (
                    user_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
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
        return int(cursor.lastrowid)

    def list_passport_entries(self, user_id: int, *, limit: int = 250) -> list[PassportEntry]:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM {USER_PASSPORT_TABLE}
                WHERE user_id = ?
                ORDER BY tried_on DESC, created_at_utc DESC, soda_name COLLATE NOCASE ASC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        return [self._map_passport_entry(row) for row in rows]

    def get_passport_entry(self, user_id: int, entry_id: int) -> PassportEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM {USER_PASSPORT_TABLE}
                WHERE user_id = ? AND id = ?
                """,
                (user_id, entry_id),
            ).fetchone()
        if row is None:
            return None
        return self._map_passport_entry(row)

    def update_passport_entry(
        self,
        user_id: int,
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
                f"""
                UPDATE {USER_PASSPORT_TABLE}
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
                WHERE user_id = ? AND id = ?
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
                    user_id,
                    entry_id,
                ),
            )
        return cursor.rowcount > 0

    def delete_passport_entry(self, user_id: int, entry_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                f"DELETE FROM {USER_PASSPORT_TABLE} WHERE user_id = ? AND id = ?",
                (user_id, entry_id),
            )
        return cursor.rowcount > 0

    def get_passport_summary(self, user_id: int) -> PassportSummary:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS total_entries,
                    COUNT(DISTINCT LOWER(TRIM(COALESCE(brand, '') || '|' || COALESCE(soda_name, '')))) AS unique_sodas,
                    COUNT(DISTINCT NULLIF(TRIM(country), '')) AS countries_count
                FROM {USER_PASSPORT_TABLE}
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            latest_country_row = connection.execute(
                f"""
                SELECT country
                FROM {USER_PASSPORT_TABLE}
                WHERE user_id = ? AND TRIM(country) != ''
                ORDER BY tried_on DESC, created_at_utc DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
        return PassportSummary(
            total_entries=int(row["total_entries"]),
            unique_sodas=int(row["unique_sodas"]),
            countries_count=int(row["countries_count"]),
            latest_country=latest_country_row["country"] if latest_country_row else "",
        )

    def find_active_wishlist_entry(self, user_id: int, *, soda_name: str, brand: str = "") -> WishlistEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM {USER_WISHLIST_TABLE}
                WHERE user_id = ?
                  AND LOWER(TRIM(soda_name)) = LOWER(TRIM(?))
                  AND LOWER(TRIM(brand)) = LOWER(TRIM(?))
                  AND status = ?
                ORDER BY updated_at_utc DESC
                LIMIT 1
                """,
                (user_id, soda_name.strip(), brand.strip(), WISHLIST_STATUS_ACTIVE),
            ).fetchone()
        if row is None:
            return None
        return self._map_wishlist_entry(row)

    def add_wishlist_entry(
        self,
        user_id: int,
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
                f"""
                INSERT INTO {USER_WISHLIST_TABLE} (
                    user_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
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
        user_id: int,
        *,
        limit: int = 250,
        include_archived: bool = True,
    ) -> list[WishlistEntry]:
        filters = ["user_id = ?"]
        params: list[object] = [user_id]
        if not include_archived:
            filters.append("status != ?")
            params.append("archived")

        where_clause = "WHERE " + " AND ".join(filters)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM {USER_WISHLIST_TABLE}
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

    def get_wishlist_entry(self, user_id: int, entry_id: int) -> WishlistEntry | None:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM {USER_WISHLIST_TABLE}
                WHERE user_id = ? AND id = ?
                """,
                (user_id, entry_id),
            ).fetchone()
        if row is None:
            return None
        return self._map_wishlist_entry(row)

    def update_wishlist_entry(
        self,
        user_id: int,
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
                f"""
                UPDATE {USER_WISHLIST_TABLE}
                SET soda_name = ?,
                    brand = ?,
                    country = ?,
                    category = ?,
                    priority = ?,
                    status = ?,
                    notes = ?,
                    updated_at_utc = ?
                WHERE user_id = ? AND id = ?
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
                    user_id,
                    entry_id,
                ),
            )
        return cursor.rowcount > 0

    def delete_wishlist_entry(self, user_id: int, entry_id: int) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                f"DELETE FROM {USER_WISHLIST_TABLE} WHERE user_id = ? AND id = ?",
                (user_id, entry_id),
            )
        return cursor.rowcount > 0

    def get_wishlist_summary(self, user_id: int) -> WishlistSummary:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS total_entries,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_entries,
                    SUM(CASE WHEN status = 'found' THEN 1 ELSE 0 END) AS found_entries,
                    SUM(CASE WHEN status = 'archived' THEN 1 ELSE 0 END) AS archived_entries,
                    SUM(CASE WHEN status = 'active' AND priority >= 4 THEN 1 ELSE 0 END) AS high_priority_entries
                FROM {USER_WISHLIST_TABLE}
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
        return WishlistSummary(
            total_entries=int(row["total_entries"]),
            active_entries=int(row["active_entries"] or 0),
            found_entries=int(row["found_entries"] or 0),
            archived_entries=int(row["archived_entries"] or 0),
            high_priority_entries=int(row["high_priority_entries"] or 0),
        )

    def export_consumption_csv(self, user_id: int) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, entry_type, soda_id, soda_name, brand, caffeine_mg,
                       consumed_at_local, local_date, reason, notes, chaos_mode, recommendation_id
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ?
                ORDER BY consumed_at_utc DESC
                """,
                (user_id,),
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

    def export_passport_csv(self, user_id: int) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, region, city, category,
                       tried_on, where_tried, rating, would_try_again, notes
                FROM {USER_PASSPORT_TABLE}
                WHERE user_id = ?
                ORDER BY tried_on DESC, created_at_utc DESC, soda_name COLLATE NOCASE ASC
                """,
                (user_id,),
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

    def export_wishlist_csv(self, user_id: int) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_name, brand, country, category, source_type, source_ref,
                       priority, status, notes, updated_at_utc
                FROM {USER_WISHLIST_TABLE}
                WHERE user_id = ?
                ORDER BY
                    CASE status
                        WHEN 'active' THEN 0
                        WHEN 'found' THEN 1
                        ELSE 2
                    END ASC,
                    priority DESC,
                    updated_at_utc DESC,
                    soda_name COLLATE NOCASE ASC
                """,
                (user_id,),
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

    def export_recommendation_csv(self, user_id: int) -> str:
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, soda_id, soda_name, brand, caffeine_mg, recommended_at_local,
                       local_date, reason, chaos_mode, status, projected_total_mg,
                       was_logged, rejection_summary
                FROM {USER_RECOMMENDATION_TABLE}
                WHERE user_id = ?
                ORDER BY recommended_at_utc DESC
                """,
                (user_id,),
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
        user_id: int,
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
                f"""
                INSERT INTO {USER_CONSUMPTION_TABLE} (
                    user_id,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
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
            self.mark_recommendation_logged(user_id, recommendation_id)

    def _recompute_recommendation_logged(self, user_id: int, recommendation_id: int) -> None:
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT COUNT(*) AS entry_count
                FROM {USER_CONSUMPTION_TABLE}
                WHERE user_id = ? AND recommendation_id = ?
                """,
                (user_id, recommendation_id),
            ).fetchone()
            was_logged = 1 if row["entry_count"] else 0
            connection.execute(
                f"""
                UPDATE {USER_RECOMMENDATION_TABLE}
                SET was_logged = ?
                WHERE user_id = ? AND id = ?
                """,
                (was_logged, user_id, recommendation_id),
            )

    def _initialize_user_accounts(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user_account (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL COLLATE NOCASE UNIQUE,
                password_hash TEXT NOT NULL DEFAULT '',
                is_admin INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )

    def _initialize_user_tables(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_CONSUMPTION_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                soda_id TEXT NOT NULL,
                soda_name TEXT NOT NULL,
                brand TEXT NOT NULL,
                caffeine_mg REAL NOT NULL,
                consumed_at_utc TEXT NOT NULL,
                consumed_at_local TEXT NOT NULL,
                local_date TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                chaos_mode INTEGER NOT NULL DEFAULT 0,
                entry_type TEXT NOT NULL DEFAULT 'catalog',
                notes TEXT NOT NULL DEFAULT '',
                recommendation_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_consumption_log_local_date
            ON {USER_CONSUMPTION_TABLE} (user_id, local_date)
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_consumption_log_consumed
            ON {USER_CONSUMPTION_TABLE} (user_id, consumed_at_utc DESC)
            """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_SODA_STATE_TABLE} (
                user_id INTEGER NOT NULL,
                soda_id TEXT NOT NULL,
                is_available INTEGER NOT NULL DEFAULT 1,
                preference TEXT NOT NULL DEFAULT 'neutral',
                temp_ban_until TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (user_id, soda_id),
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_SETTING_OVERRIDE_TABLE} (
                user_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                PRIMARY KEY (user_id, key),
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_RECOMMENDATION_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
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
                rejection_summary TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_recommendation_history_local_date
            ON {USER_RECOMMENDATION_TABLE} (user_id, local_date)
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_recommendation_history_created
            ON {USER_RECOMMENDATION_TABLE} (user_id, recommended_at_utc DESC)
            """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_PASSPORT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
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
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_soda_passport_tried_on
            ON {USER_PASSPORT_TABLE} (user_id, tried_on DESC, created_at_utc DESC)
            """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_WISHLIST_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
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
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES user_account(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_user_soda_wishlist_status_updated
            ON {USER_WISHLIST_TABLE} (user_id, status, updated_at_utc DESC)
            """
        )

    def _migrate_legacy_tables(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        self._migrate_consumption_log(connection, owner_user_id=owner_user_id)
        self._migrate_soda_state(connection, owner_user_id=owner_user_id)
        self._migrate_setting_override(connection, owner_user_id=owner_user_id)
        self._migrate_recommendation_history(connection, owner_user_id=owner_user_id)
        self._migrate_passport_entries(connection, owner_user_id=owner_user_id)
        self._migrate_wishlist_entries(connection, owner_user_id=owner_user_id)

    def _migrate_consumption_log(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "consumption_log"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_CONSUMPTION_TABLE):
            return

        columns = self._table_columns(connection, legacy_table)
        entry_type_expr = "entry_type" if "entry_type" in columns else "'catalog'"
        notes_expr = "notes" if "notes" in columns else "''"
        recommendation_expr = "recommendation_id" if "recommendation_id" in columns else "NULL"
        connection.execute(
            f"""
            INSERT INTO {USER_CONSUMPTION_TABLE} (
                user_id,
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
            SELECT
                ?,
                soda_id,
                soda_name,
                brand,
                caffeine_mg,
                consumed_at_utc,
                consumed_at_local,
                local_date,
                reason,
                chaos_mode,
                {entry_type_expr},
                {notes_expr},
                {recommendation_expr}
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _migrate_soda_state(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "soda_state"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_SODA_STATE_TABLE):
            return

        columns = self._table_columns(connection, legacy_table)
        temp_ban_expr = "temp_ban_until" if "temp_ban_until" in columns else "''"
        connection.execute(
            f"""
            INSERT INTO {USER_SODA_STATE_TABLE} (user_id, soda_id, is_available, preference, temp_ban_until)
            SELECT ?, soda_id, is_available, preference, {temp_ban_expr}
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _migrate_setting_override(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "setting_override"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_SETTING_OVERRIDE_TABLE):
            return

        columns = self._table_columns(connection, legacy_table)
        updated_expr = "updated_at_utc" if "updated_at_utc" in columns else f"'{datetime.now(timezone.utc).isoformat()}'"
        connection.execute(
            f"""
            INSERT INTO {USER_SETTING_OVERRIDE_TABLE} (user_id, key, value, updated_at_utc)
            SELECT ?, key, value, {updated_expr}
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _migrate_recommendation_history(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "recommendation_history"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_RECOMMENDATION_TABLE):
            return

        columns = self._table_columns(connection, legacy_table)
        projected_expr = "projected_total_mg" if "projected_total_mg" in columns else "NULL"
        was_logged_expr = "was_logged" if "was_logged" in columns else "0"
        rejection_expr = "rejection_summary" if "rejection_summary" in columns else "''"
        connection.execute(
            f"""
            INSERT INTO {USER_RECOMMENDATION_TABLE} (
                user_id,
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
            SELECT
                ?,
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
                {projected_expr},
                {was_logged_expr},
                {rejection_expr}
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _migrate_passport_entries(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "soda_passport"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_PASSPORT_TABLE):
            return

        connection.execute(
            f"""
            INSERT INTO {USER_PASSPORT_TABLE} (
                user_id,
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
            SELECT
                ?,
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
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _migrate_wishlist_entries(self, connection: sqlite3.Connection, *, owner_user_id: int) -> None:
        legacy_table = "soda_wishlist"
        if not self._should_copy_legacy_table(connection, legacy_table, USER_WISHLIST_TABLE):
            return

        connection.execute(
            f"""
            INSERT INTO {USER_WISHLIST_TABLE} (
                user_id,
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
            SELECT
                ?,
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
            FROM {legacy_table}
            """,
            (owner_user_id,),
        )

    def _should_copy_legacy_table(self, connection: sqlite3.Connection, legacy_table: str, new_table: str) -> bool:
        return (
            self._table_exists(connection, legacy_table)
            and self._table_exists(connection, new_table)
            and self._table_has_rows(connection, legacy_table)
            and not self._table_has_rows(connection, new_table)
        )

    def _should_adopt_local_data(self, connection: sqlite3.Connection, *, source_user_id: int, target_user_id: int) -> bool:
        if not self._user_has_any_data(connection, source_user_id):
            return False
        if self._user_has_any_data(connection, target_user_id):
            return False

        for table_name in USER_OWNED_TABLES:
            row = connection.execute(
                f"SELECT COUNT(*) AS total FROM {table_name} WHERE user_id != ?",
                (source_user_id,),
            ).fetchone()
            if int(row["total"]):
                return False
        return True

    def _reassign_user_data(self, connection: sqlite3.Connection, *, from_user_id: int, to_user_id: int) -> None:
        for table_name in USER_OWNED_TABLES:
            connection.execute(
                f"UPDATE {table_name} SET user_id = ? WHERE user_id = ?",
                (to_user_id, from_user_id),
            )

    def _user_has_any_data(self, connection: sqlite3.Connection, user_id: int) -> bool:
        for table_name in USER_OWNED_TABLES:
            row = connection.execute(
                f"SELECT COUNT(*) AS total FROM {table_name} WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if int(row["total"]):
                return True
        return False

    def _ensure_local_user(self, connection: sqlite3.Connection) -> UserAccount:
        existing = self._get_user_by_username(connection, LOCAL_MODE_USERNAME)
        if existing is not None:
            return existing

        now = datetime.now(timezone.utc).isoformat()
        cursor = connection.execute(
            """
            INSERT INTO user_account (username, password_hash, is_admin, created_at_utc, updated_at_utc)
            VALUES (?, '', 1, ?, ?)
            """,
            (LOCAL_MODE_USERNAME, now, now),
        )
        created = self._get_user_by_id(connection, int(cursor.lastrowid))
        if created is None:
            raise RuntimeError("Failed to create the local fallback user.")
        return created

    def _ensure_bootstrap_admin(self, connection: sqlite3.Connection, settings: Settings | None) -> UserAccount | None:
        if settings is None:
            return None

        username = settings.access_control_username.strip()
        password = settings.access_control_password
        if bool(username) != bool(password):
            raise ValueError(
                "Set both ACCESS_CONTROL_USERNAME and ACCESS_CONTROL_PASSWORD to bootstrap the first admin, or leave both blank."
            )
        if not username:
            return None

        normalized_username = self._normalize_username(username)
        existing = self._get_user_by_username(connection, normalized_username)
        if existing is not None:
            return existing

        now = datetime.now(timezone.utc).isoformat()
        cursor = connection.execute(
            """
            INSERT INTO user_account (username, password_hash, is_admin, created_at_utc, updated_at_utc)
            VALUES (?, ?, 1, ?, ?)
            """,
            (normalized_username, hash_password(self._normalize_password(password)), now, now),
        )
        created = self._get_user_by_id(connection, int(cursor.lastrowid))
        if created is None:
            raise RuntimeError("Failed to create the bootstrap admin account.")
        return created

    def _has_loginable_users(self, connection: sqlite3.Connection) -> bool:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM user_account
            WHERE username != ? AND password_hash != ''
            """,
            (LOCAL_MODE_USERNAME,),
        ).fetchone()
        return int(row["total"]) > 0

    def _admin_count(self, connection: sqlite3.Connection, *, include_local: bool) -> int:
        if include_local:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM user_account WHERE is_admin = 1"
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM user_account
                WHERE is_admin = 1 AND username != ?
                """,
                (LOCAL_MODE_USERNAME,),
            ).fetchone()
        return int(row["total"])

    def _get_user_by_id(self, connection: sqlite3.Connection, user_id: int) -> UserAccount | None:
        row = connection.execute(
            """
            SELECT id, username, is_admin, created_at_utc, updated_at_utc
            FROM user_account
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        return self._map_user_account(row)

    def _get_user_by_username(self, connection: sqlite3.Connection, username: str) -> UserAccount | None:
        row = connection.execute(
            """
            SELECT id, username, is_admin, created_at_utc, updated_at_utc
            FROM user_account
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
        if row is None:
            return None
        return self._map_user_account(row)

    def _get_user_with_password(self, connection: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, username, password_hash, is_admin, created_at_utc, updated_at_utc
            FROM user_account
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()

    def _normalize_username(self, username: str) -> str:
        cleaned = username.strip()
        if not cleaned:
            raise ValueError("Username is required.")
        if cleaned == LOCAL_MODE_USERNAME:
            raise ValueError("That username is reserved.")
        if not USERNAME_PATTERN.fullmatch(cleaned):
            raise ValueError("Usernames can only use letters, numbers, dots, dashes, and underscores.")
        return cleaned

    @staticmethod
    def _normalize_password(password: str) -> str:
        if not password or not password.strip():
            raise ValueError("Password is required.")
        return password

    def _map_user_account(self, row: sqlite3.Row) -> UserAccount:
        return UserAccount(
            id=int(row["id"]),
            username=row["username"],
            is_admin=bool(row["is_admin"]),
            created_at_utc=datetime.fromisoformat(row["created_at_utc"]) if row["created_at_utc"] else None,
            updated_at_utc=datetime.fromisoformat(row["updated_at_utc"]) if row["updated_at_utc"] else None,
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

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_has_rows(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()
        return int(row["total"]) > 0

    def _table_columns(self, connection: sqlite3.Connection, table_name: str) -> set[str]:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
