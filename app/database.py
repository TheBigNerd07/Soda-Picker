from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import ConsumptionEntry, Soda


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

    def get_today_entries(self, local_now: datetime) -> list[ConsumptionEntry]:
        local_date = local_now.date().isoformat()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, soda_id, soda_name, brand, caffeine_mg, consumed_at_local, reason, chaos_mode
                FROM consumption_log
                WHERE local_date = ?
                ORDER BY consumed_at_utc DESC
                """,
                (local_date,),
            ).fetchall()

        return [
            ConsumptionEntry(
                id=row["id"],
                soda_id=row["soda_id"],
                soda_name=row["soda_name"],
                brand=row["brand"],
                caffeine_mg=row["caffeine_mg"],
                consumed_at_local=datetime.fromisoformat(row["consumed_at_local"]),
                reason=row["reason"],
                chaos_mode=bool(row["chaos_mode"]),
            )
            for row in rows
        ]

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

    def get_recent_consumed_ids(self, limit: int = 3) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT soda_id
                FROM consumption_log
                ORDER BY consumed_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [row["soda_id"] for row in rows]

    def log_consumption(
        self,
        soda: Soda,
        local_now: datetime,
        *,
        reason: str = "",
        chaos_mode: bool = False,
    ) -> None:
        local_date = local_now.date().isoformat()
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
                    chaos_mode
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    soda.id,
                    soda.name,
                    soda.brand,
                    soda.caffeine_mg,
                    utc_now.isoformat(),
                    local_now.isoformat(),
                    local_date,
                    reason,
                    int(chaos_mode),
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection
