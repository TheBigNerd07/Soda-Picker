from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import format_clock_time


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_hhmm(value: str) -> time:
    cleaned = value.strip()
    try:
        hour_text, minute_text = cleaned.split(":", maxsplit=1)
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError as exc:
        raise ValueError(f"Invalid HH:MM time value: {value!r}") from exc

    if hour not in range(24) or minute not in range(60):
        raise ValueError(f"Invalid HH:MM time value: {value!r}")

    return time(hour=hour, minute=minute)


@dataclass(slots=True)
class Settings:
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    timezone_name: str = "America/Los_Angeles"
    no_soda_before: str = "10:30"
    daily_caffeine_limit_mg: int = 160
    caffeine_cutoff_hour: int = 15
    csv_path: str = "/data/sample_sodas.csv"
    database_path: str = "/data/soda_picker.db"
    chaos_mode_default: bool = False
    log_level: str = "INFO"
    timezone: ZoneInfo = field(init=False, repr=False)
    no_soda_before_time: time = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.app_host = self.app_host.strip() or "0.0.0.0"
        self.log_level = self.log_level.strip().upper() or "INFO"

        if self.app_port <= 0 or self.app_port > 65535:
            raise ValueError("APP_PORT must be between 1 and 65535.")
        if self.daily_caffeine_limit_mg < 0:
            raise ValueError("DAILY_CAFFEINE_LIMIT_MG must be 0 or greater.")
        if self.caffeine_cutoff_hour not in range(24):
            raise ValueError("CAFFEINE_CUTOFF_HOUR must be between 0 and 23.")

        try:
            self.timezone = ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown timezone: {self.timezone_name!r}") from exc

        self.no_soda_before_time = parse_hhmm(self.no_soda_before)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_host=os.getenv("APP_HOST", "0.0.0.0"),
            app_port=int(os.getenv("APP_PORT", "8000")),
            timezone_name=os.getenv("TZ", "America/Los_Angeles"),
            no_soda_before=os.getenv("NO_SODA_BEFORE", "10:30"),
            daily_caffeine_limit_mg=int(os.getenv("DAILY_CAFFEINE_LIMIT_MG", "160")),
            caffeine_cutoff_hour=int(os.getenv("CAFFEINE_CUTOFF_HOUR", "15")),
            csv_path=os.getenv("CSV_PATH", "/data/sample_sodas.csv"),
            database_path=os.getenv("DATABASE_PATH", "/data/soda_picker.db"),
            chaos_mode_default=parse_bool(os.getenv("CHAOS_MODE_DEFAULT"), False),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    def local_now(self) -> datetime:
        return datetime.now(self.timezone)

    @property
    def no_soda_before_minutes(self) -> int:
        return (self.no_soda_before_time.hour * 60) + self.no_soda_before_time.minute

    @property
    def no_soda_before_display(self) -> str:
        return format_clock_time(self.no_soda_before_time)

    def display_items(self) -> list[tuple[str, str]]:
        return [
            ("APP_HOST", self.app_host),
            ("APP_PORT", str(self.app_port)),
            ("TZ", self.timezone_name),
            ("NO_SODA_BEFORE", self.no_soda_before),
            ("DAILY_CAFFEINE_LIMIT_MG", str(self.daily_caffeine_limit_mg)),
            ("CAFFEINE_CUTOFF_HOUR", str(self.caffeine_cutoff_hour)),
            ("CSV_PATH", self.csv_path),
            ("DATABASE_PATH", self.database_path),
            ("CHAOS_MODE_DEFAULT", "true" if self.chaos_mode_default else "false"),
            ("LOG_LEVEL", self.log_level),
        ]
