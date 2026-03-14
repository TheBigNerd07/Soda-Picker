from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import format_clock_time

PERSISTABLE_SETTING_FIELDS = {
    "no_soda_before",
    "weekend_no_soda_before",
    "daily_caffeine_limit_mg",
    "weekend_daily_caffeine_limit_mg",
    "caffeine_cutoff_hour",
    "weekend_caffeine_cutoff_hour",
    "bedtime_hour",
    "weekend_bedtime_hour",
    "latest_caffeine_hours_before_bed",
    "duplicate_lookback",
    "chaos_mode_default",
    "reminder_enabled",
    "reminder_time",
}

ENV_ONLY_FIELDS = {
    "app_host",
    "app_port",
    "timezone_name",
    "csv_path",
    "database_path",
    "backup_dir",
    "log_level",
    "basic_auth_username",
    "basic_auth_password",
    "trusted_hosts",
    "rate_limit_requests",
    "rate_limit_window_seconds",
    "access_control_mode",
    "access_control_username",
    "access_control_password",
    "access_control_secret",
    "access_control_session_days",
}


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


def _int_or_default(value: str | int | None, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class RuntimeRules:
    is_weekend: bool
    no_soda_before_time: time
    daily_caffeine_limit_mg: int
    caffeine_cutoff_hour: int
    bedtime_hour: int
    latest_caffeine_hours_before_bed: int
    duplicate_lookback: int
    reminder_enabled: bool
    reminder_time: time
    effective_caffeine_stop_hour: int

    @property
    def no_soda_before_minutes(self) -> int:
        return (self.no_soda_before_time.hour * 60) + self.no_soda_before_time.minute

    @property
    def reminder_time_display(self) -> str:
        return format_clock_time(self.reminder_time)

    @property
    def no_soda_before_display(self) -> str:
        return format_clock_time(self.no_soda_before_time)

    @property
    def effective_cutoff_display(self) -> str:
        return format_clock_time(time(self.effective_caffeine_stop_hour, 0))

    @property
    def bedtime_display(self) -> str:
        return format_clock_time(time(self.bedtime_hour, 0))

    @property
    def rules_label(self) -> str:
        if self.is_weekend:
            return "Weekend rules"
        return "Weekday rules"


@dataclass
class Settings:
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    timezone_name: str = "America/Los_Angeles"
    no_soda_before: str = "10:30"
    weekend_no_soda_before: str = ""
    daily_caffeine_limit_mg: int = 160
    weekend_daily_caffeine_limit_mg: int = 180
    caffeine_cutoff_hour: int = 15
    weekend_caffeine_cutoff_hour: int = 16
    bedtime_hour: int = 23
    weekend_bedtime_hour: int = 23
    latest_caffeine_hours_before_bed: int = 6
    duplicate_lookback: int = 4
    csv_path: str = "/data/sample_sodas.csv"
    database_path: str = "/data/soda_picker.db"
    backup_dir: str = "/data/backups"
    chaos_mode_default: bool = False
    reminder_enabled: bool = False
    reminder_time: str = "10:30"
    log_level: str = "INFO"
    basic_auth_username: str = ""
    basic_auth_password: str = ""
    trusted_hosts: str = ""
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60
    access_control_mode: str = "off"
    access_control_username: str = ""
    access_control_password: str = ""
    access_control_secret: str = ""
    access_control_session_days: int = 30
    timezone: ZoneInfo = field(init=False, repr=False)
    no_soda_before_time: time = field(init=False, repr=False)
    weekend_no_soda_before_time: time = field(init=False, repr=False)
    reminder_time_value: time = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.app_host = self.app_host.strip() or "0.0.0.0"
        self.log_level = self.log_level.strip().upper() or "INFO"
        self.weekend_no_soda_before = self.weekend_no_soda_before.strip() or self.no_soda_before
        self.reminder_time = self.reminder_time.strip() or self.no_soda_before
        self.access_control_mode = self.access_control_mode.strip().lower() or "off"

        if self.app_port <= 0 or self.app_port > 65535:
            raise ValueError("APP_PORT must be between 1 and 65535.")
        if self.daily_caffeine_limit_mg < 0 or self.weekend_daily_caffeine_limit_mg < 0:
            raise ValueError("Daily caffeine limits must be 0 or greater.")
        if self.caffeine_cutoff_hour not in range(24) or self.weekend_caffeine_cutoff_hour not in range(24):
            raise ValueError("Caffeine cutoff hours must be between 0 and 24.")
        if self.bedtime_hour not in range(24) or self.weekend_bedtime_hour not in range(24):
            raise ValueError("Bedtime hours must be between 0 and 23.")
        if self.latest_caffeine_hours_before_bed < 0 or self.latest_caffeine_hours_before_bed > 12:
            raise ValueError("LATEST_CAFFEINE_HOURS_BEFORE_BED must be between 0 and 12.")
        if self.duplicate_lookback < 1 or self.duplicate_lookback > 12:
            raise ValueError("DUPLICATE_LOOKBACK must be between 1 and 12.")
        if self.rate_limit_requests < 0 or self.rate_limit_window_seconds < 0:
            raise ValueError("Rate limit values must be 0 or greater.")
        if self.access_control_mode not in {"off", "writes", "all"}:
            raise ValueError("ACCESS_CONTROL_MODE must be one of: off, writes, all.")
        if self.access_control_session_days < 1 or self.access_control_session_days > 90:
            raise ValueError("ACCESS_CONTROL_SESSION_DAYS must be between 1 and 90.")

        try:
            self.timezone = ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown timezone: {self.timezone_name!r}") from exc

        self.no_soda_before_time = parse_hhmm(self.no_soda_before)
        self.weekend_no_soda_before_time = parse_hhmm(self.weekend_no_soda_before)
        self.reminder_time_value = parse_hhmm(self.reminder_time)
        if self.access_control_enabled:
            if not self.access_control_secret:
                raise ValueError("ACCESS_CONTROL_SECRET is required when access control is enabled.")
            if bool(self.access_control_username) != bool(self.access_control_password):
                raise ValueError(
                    "Set both ACCESS_CONTROL_USERNAME and ACCESS_CONTROL_PASSWORD to bootstrap the first admin, or leave both blank."
                )
        if self.basic_auth_enabled and self.access_control_enabled:
            raise ValueError("Choose either BASIC_AUTH_* or ACCESS_CONTROL_*; do not enable both.")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_host=os.getenv("APP_HOST", "0.0.0.0"),
            app_port=int(os.getenv("APP_PORT", "8000")),
            timezone_name=os.getenv("TZ", "America/Los_Angeles"),
            no_soda_before=os.getenv("NO_SODA_BEFORE", "10:30"),
            weekend_no_soda_before=os.getenv("WEEKEND_NO_SODA_BEFORE", ""),
            daily_caffeine_limit_mg=int(os.getenv("DAILY_CAFFEINE_LIMIT_MG", "160")),
            weekend_daily_caffeine_limit_mg=int(os.getenv("WEEKEND_DAILY_CAFFEINE_LIMIT_MG", os.getenv("DAILY_CAFFEINE_LIMIT_MG", "160"))),
            caffeine_cutoff_hour=int(os.getenv("CAFFEINE_CUTOFF_HOUR", "15")),
            weekend_caffeine_cutoff_hour=int(os.getenv("WEEKEND_CAFFEINE_CUTOFF_HOUR", os.getenv("CAFFEINE_CUTOFF_HOUR", "15"))),
            bedtime_hour=int(os.getenv("BEDTIME_HOUR", "23")),
            weekend_bedtime_hour=int(os.getenv("WEEKEND_BEDTIME_HOUR", os.getenv("BEDTIME_HOUR", "23"))),
            latest_caffeine_hours_before_bed=int(os.getenv("LATEST_CAFFEINE_HOURS_BEFORE_BED", "6")),
            duplicate_lookback=int(os.getenv("DUPLICATE_LOOKBACK", "4")),
            csv_path=os.getenv("CSV_PATH", "/data/sample_sodas.csv"),
            database_path=os.getenv("DATABASE_PATH", "/data/soda_picker.db"),
            backup_dir=os.getenv("BACKUP_DIR", "/data/backups"),
            chaos_mode_default=parse_bool(os.getenv("CHAOS_MODE_DEFAULT"), False),
            reminder_enabled=parse_bool(os.getenv("REMINDER_ENABLED"), False),
            reminder_time=os.getenv("REMINDER_TIME", os.getenv("NO_SODA_BEFORE", "10:30")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            basic_auth_username=os.getenv("BASIC_AUTH_USERNAME", ""),
            basic_auth_password=os.getenv("BASIC_AUTH_PASSWORD", ""),
            trusted_hosts=os.getenv("TRUSTED_HOSTS", ""),
            rate_limit_requests=int(os.getenv("RATE_LIMIT_REQUESTS", "120")),
            rate_limit_window_seconds=int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")),
            access_control_mode=os.getenv("ACCESS_CONTROL_MODE", "off"),
            access_control_username=os.getenv("ACCESS_CONTROL_USERNAME", ""),
            access_control_password=os.getenv("ACCESS_CONTROL_PASSWORD", ""),
            access_control_secret=os.getenv("ACCESS_CONTROL_SECRET", ""),
            access_control_session_days=int(os.getenv("ACCESS_CONTROL_SESSION_DAYS", "30")),
        )

    def local_now(self) -> datetime:
        return datetime.now(self.timezone)

    def effective_rules(self, local_now: datetime) -> RuntimeRules:
        is_weekend = local_now.weekday() >= 5
        no_soda_before_time = self.weekend_no_soda_before_time if is_weekend else self.no_soda_before_time
        daily_limit = self.weekend_daily_caffeine_limit_mg if is_weekend else self.daily_caffeine_limit_mg
        cutoff_hour = self.weekend_caffeine_cutoff_hour if is_weekend else self.caffeine_cutoff_hour
        bedtime_hour = self.weekend_bedtime_hour if is_weekend else self.bedtime_hour
        bedtime_cutoff = max(0, bedtime_hour - self.latest_caffeine_hours_before_bed)
        effective_cutoff_hour = min(cutoff_hour, bedtime_cutoff)

        return RuntimeRules(
            is_weekend=is_weekend,
            no_soda_before_time=no_soda_before_time,
            daily_caffeine_limit_mg=daily_limit,
            caffeine_cutoff_hour=cutoff_hour,
            bedtime_hour=bedtime_hour,
            latest_caffeine_hours_before_bed=self.latest_caffeine_hours_before_bed,
            duplicate_lookback=self.duplicate_lookback,
            reminder_enabled=self.reminder_enabled,
            reminder_time=self.reminder_time_value,
            effective_caffeine_stop_hour=effective_cutoff_hour,
        )

    def with_overrides(self, overrides: dict[str, str]) -> "Settings":
        values: dict[str, Any] = {}
        for key, raw_value in overrides.items():
            if key not in PERSISTABLE_SETTING_FIELDS:
                continue
            if key in {"daily_caffeine_limit_mg", "weekend_daily_caffeine_limit_mg", "caffeine_cutoff_hour", "weekend_caffeine_cutoff_hour", "bedtime_hour", "weekend_bedtime_hour", "latest_caffeine_hours_before_bed", "duplicate_lookback"}:
                values[key] = _int_or_default(raw_value, getattr(self, key))
            elif key in {"chaos_mode_default", "reminder_enabled"}:
                values[key] = parse_bool(raw_value, getattr(self, key))
            else:
                values[key] = raw_value

        return replace(self, **values)

    @property
    def trusted_hosts_list(self) -> list[str]:
        return [host.strip() for host in self.trusted_hosts.split(",") if host.strip()]

    @property
    def basic_auth_enabled(self) -> bool:
        return bool(self.basic_auth_username and self.basic_auth_password)

    @property
    def access_control_enabled(self) -> bool:
        return self.access_control_mode != "off"

    @property
    def access_control_mode_label(self) -> str:
        if self.access_control_mode == "writes":
            return "Protect writes and admin pages"
        if self.access_control_mode == "all":
            return "Login required for the whole app"
        return "Disabled"

    def display_items(self, override_keys: set[str] | None = None) -> list[tuple[str, str, str]]:
        override_keys = override_keys or set()
        items = [
            ("APP_HOST", self.app_host, "env"),
            ("APP_PORT", str(self.app_port), "env"),
            ("TZ", self.timezone_name, "env"),
            ("NO_SODA_BEFORE", self.no_soda_before, "override" if "no_soda_before" in override_keys else "env"),
            ("WEEKEND_NO_SODA_BEFORE", self.weekend_no_soda_before, "override" if "weekend_no_soda_before" in override_keys else "env"),
            ("DAILY_CAFFEINE_LIMIT_MG", str(self.daily_caffeine_limit_mg), "override" if "daily_caffeine_limit_mg" in override_keys else "env"),
            ("WEEKEND_DAILY_CAFFEINE_LIMIT_MG", str(self.weekend_daily_caffeine_limit_mg), "override" if "weekend_daily_caffeine_limit_mg" in override_keys else "env"),
            ("CAFFEINE_CUTOFF_HOUR", str(self.caffeine_cutoff_hour), "override" if "caffeine_cutoff_hour" in override_keys else "env"),
            ("WEEKEND_CAFFEINE_CUTOFF_HOUR", str(self.weekend_caffeine_cutoff_hour), "override" if "weekend_caffeine_cutoff_hour" in override_keys else "env"),
            ("BEDTIME_HOUR", str(self.bedtime_hour), "override" if "bedtime_hour" in override_keys else "env"),
            ("WEEKEND_BEDTIME_HOUR", str(self.weekend_bedtime_hour), "override" if "weekend_bedtime_hour" in override_keys else "env"),
            ("LATEST_CAFFEINE_HOURS_BEFORE_BED", str(self.latest_caffeine_hours_before_bed), "override" if "latest_caffeine_hours_before_bed" in override_keys else "env"),
            ("DUPLICATE_LOOKBACK", str(self.duplicate_lookback), "override" if "duplicate_lookback" in override_keys else "env"),
            ("CSV_PATH", self.csv_path, "env"),
            ("DATABASE_PATH", self.database_path, "env"),
            ("BACKUP_DIR", self.backup_dir, "env"),
            ("CHAOS_MODE_DEFAULT", "true" if self.chaos_mode_default else "false", "override" if "chaos_mode_default" in override_keys else "env"),
            ("REMINDER_ENABLED", "true" if self.reminder_enabled else "false", "override" if "reminder_enabled" in override_keys else "env"),
            ("REMINDER_TIME", self.reminder_time, "override" if "reminder_time" in override_keys else "env"),
            ("LOG_LEVEL", self.log_level, "env"),
            ("BASIC_AUTH_ENABLED", "true" if self.basic_auth_enabled else "false", "env"),
            ("ACCESS_CONTROL_MODE", self.access_control_mode, "env"),
            ("ACCESS_CONTROL_USERNAME", self.access_control_username or "unset", "env"),
            ("ACCESS_CONTROL_SESSION_DAYS", str(self.access_control_session_days), "env"),
            ("TRUSTED_HOSTS", self.trusted_hosts or "disabled", "env"),
            ("RATE_LIMIT_REQUESTS", str(self.rate_limit_requests), "env"),
            ("RATE_LIMIT_WINDOW_SECONDS", str(self.rate_limit_window_seconds), "env"),
        ]
        return items


def normalize_override_payload(form_data: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key in PERSISTABLE_SETTING_FIELDS:
        if key in {"chaos_mode_default", "reminder_enabled"}:
            normalized[key] = "true" if parse_bool(form_data.get(key), False) else "false"
        else:
            normalized[key] = (form_data.get(key) or "").strip()
    return normalized
