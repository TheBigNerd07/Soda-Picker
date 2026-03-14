from __future__ import annotations

from .config import Settings, normalize_override_payload
from .database import Database


class SettingsStore:
    def __init__(self, base_settings: Settings, database: Database) -> None:
        self.base_settings = base_settings
        self.database = database

    def current(self) -> Settings:
        return self.base_settings.with_overrides(self.database.get_setting_overrides())

    def override_keys(self) -> set[str]:
        return set(self.database.get_setting_overrides().keys())

    def save(self, form_data: dict[str, str]) -> Settings:
        normalized = normalize_override_payload(form_data)
        validated = self.base_settings.with_overrides(normalized)
        self.database.set_setting_overrides(normalized)
        return validated

    def reset(self) -> Settings:
        self.database.clear_setting_overrides()
        return self.base_settings
