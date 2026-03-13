from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time


def format_clock_time(value: datetime | time) -> str:
    rendered = value.strftime("%I:%M %p")
    return rendered.lstrip("0")


@dataclass(frozen=True, slots=True)
class Soda:
    id: str
    name: str
    brand: str = ""
    caffeine_mg: float = 0.0
    sugar_g: float | None = None
    category: str = "General"
    is_diet: bool = False
    is_caffeine_free: bool = False
    tags: tuple[str, ...] = field(default_factory=tuple)
    priority: int = 1
    enabled: bool = True

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.name.lower():
            return f"{self.brand} {self.name}"
        return self.name

    @property
    def caffeine_label(self) -> str:
        if self.caffeine_mg <= 0:
            return "Caffeine-free"
        return f"{self.caffeine_mg:g} mg caffeine"

    @property
    def sugar_label(self) -> str:
        if self.sugar_g is None:
            return "Sugar n/a"
        return f"{self.sugar_g:g} g sugar"

    @property
    def badge_labels(self) -> tuple[str, ...]:
        badges: list[str] = []
        if self.is_caffeine_free or self.caffeine_mg <= 0:
            badges.append("Caffeine-Free")
        elif self.caffeine_mg <= 20:
            badges.append("Low Caffeine")
        else:
            badges.append(f"{self.caffeine_mg:g} mg")

        if self.is_diet:
            badges.append("Diet")

        if self.category:
            badges.append(self.category.title())

        return tuple(badges)


@dataclass(frozen=True, slots=True)
class ConsumptionEntry:
    id: int
    soda_id: str
    soda_name: str
    brand: str
    caffeine_mg: float
    consumed_at_local: datetime
    reason: str = ""
    chaos_mode: bool = False

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.soda_name.lower():
            return f"{self.brand} {self.soda_name}"
        return self.soda_name

    @property
    def time_label(self) -> str:
        return format_clock_time(self.consumed_at_local)


@dataclass(frozen=True, slots=True)
class RecommendationResult:
    status: str
    headline: str
    reason: str
    soda: Soda | None = None
    chaos_mode: bool = False
