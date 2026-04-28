from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time

PREFERENCE_NEUTRAL = "neutral"
PREFERENCE_FAVORITE = "favorite"
PREFERENCE_DISLIKE = "dislike"
PREFERENCE_CHOICES = {
    PREFERENCE_NEUTRAL,
    PREFERENCE_FAVORITE,
    PREFERENCE_DISLIKE,
}
WISHLIST_STATUS_ACTIVE = "active"
WISHLIST_STATUS_FOUND = "found"
WISHLIST_STATUS_ARCHIVED = "archived"
WISHLIST_STATUS_CHOICES = {
    WISHLIST_STATUS_ACTIVE,
    WISHLIST_STATUS_FOUND,
    WISHLIST_STATUS_ARCHIVED,
}
RECOMMENDATION_FEEDBACK_OPTIONS = (
    ("good_pick", "Good pick"),
    ("bad_pick", "Bad pick"),
    ("too_sweet", "Too sweet"),
    ("too_much_caffeine", "Too much caffeine"),
    ("not_in_the_mood", "Not in the mood"),
)
RECOMMENDATION_FEEDBACK_LABELS = dict(RECOMMENDATION_FEEDBACK_OPTIONS)
RECOMMENDATION_FEEDBACK_CHOICES = {value for value, _label in RECOMMENDATION_FEEDBACK_OPTIONS}


def format_clock_time(value: datetime | time) -> str:
    rendered = value.strftime("%I:%M %p")
    return rendered.lstrip("0")


def format_calendar_date(value: date | datetime) -> str:
    if isinstance(value, datetime):
        target = value.date()
    else:
        target = value
    return target.strftime("%b %d, %Y")


@dataclass(frozen=True)
class Soda:
    id: str
    name: str
    brand: str = ""
    caffeine_mg: float = 0.0
    caffeine_is_estimated: bool = False
    sugar_g: float | None = None
    category: str = "General"
    is_diet: bool = False
    is_caffeine_free: bool = False
    tags: tuple[str, ...] = field(default_factory=tuple)
    priority: int = 1
    enabled: bool = True
    row_number: int = 0

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.name.lower():
            return f"{self.brand} {self.name}"
        return self.name

    @property
    def caffeine_label(self) -> str:
        if self.caffeine_mg <= 0:
            return "Caffeine-free"
        if self.caffeine_is_estimated:
            return "Contains caffeine"
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
        elif self.caffeine_is_estimated:
            badges.append("Caffeinated")
        elif self.caffeine_mg <= 20:
            badges.append("Low Caffeine")
        else:
            badges.append(f"{self.caffeine_mg:g} mg")

        if self.is_diet:
            badges.append("Diet")

        if self.category:
            badges.append(self.category.title())

        return tuple(badges)


@dataclass(frozen=True)
class SodaState:
    soda_id: str
    is_available: bool = True
    preference: str = PREFERENCE_NEUTRAL
    temp_ban_until: date | None = None

    def is_temporarily_banned(self, local_now: datetime) -> bool:
        return self.temp_ban_until is not None and self.temp_ban_until >= local_now.date()

    @property
    def preference_label(self) -> str:
        return self.preference.replace("_", " ").title()


@dataclass(frozen=True)
class CatalogItem:
    soda: Soda
    state: SodaState

    @property
    def id(self) -> str:
        return self.soda.id

    @property
    def display_name(self) -> str:
        return self.soda.display_name

    def state_badges(self, local_now: datetime) -> tuple[str, ...]:
        badges: list[str] = []
        if not self.state.is_available:
            badges.append("Out of Stock")
        if self.state.preference == PREFERENCE_FAVORITE:
            badges.append("Favorite")
        elif self.state.preference == PREFERENCE_DISLIKE:
            badges.append("Disliked")
        if self.state.is_temporarily_banned(local_now):
            until = format_calendar_date(self.state.temp_ban_until)
            badges.append(f"Banned Until {until}")
        return tuple(badges)


@dataclass(frozen=True)
class ConsumptionEntry:
    id: int
    entry_type: str
    soda_id: str
    soda_name: str
    brand: str
    caffeine_mg: float
    consumed_at_local: datetime
    recommendation_id: int | None = None
    reason: str = ""
    chaos_mode: bool = False
    notes: str = ""

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.soda_name.lower():
            return f"{self.brand} {self.soda_name}"
        return self.soda_name

    @property
    def time_label(self) -> str:
        return format_clock_time(self.consumed_at_local)

    @property
    def source_label(self) -> str:
        if self.entry_type == "manual":
            return "Manual caffeine"
        if self.entry_type == "manual_soda":
            return "Manual soda"
        return "Catalog soda"

    @property
    def entry_type_label(self) -> str:
        return self.source_label

    @property
    def datetime_local_input_value(self) -> str:
        return self.consumed_at_local.strftime("%Y-%m-%dT%H:%M")


@dataclass(frozen=True)
class RejectionDetail:
    soda_id: str
    soda_name: str
    reason: str


@dataclass(frozen=True)
class RecommendationResult:
    status: str
    headline: str
    reason: str
    soda: CatalogItem | None = None
    chaos_mode: bool = False
    projected_total_mg: float | None = None
    remaining_budget_mg: float | None = None
    rejected_options: tuple[RejectionDetail, ...] = ()
    effective_cutoff_label: str = ""
    bedtime_label: str = ""
    rules_summary: str = ""
    pick_style_value: str = "any"
    pick_style_label: str = "Any soda"
    pick_style_fallback_used: bool = False


@dataclass(frozen=True)
class RecommendationHistoryEntry:
    id: int
    soda_id: str
    soda_name: str
    brand: str
    caffeine_mg: float
    recommended_at_local: datetime
    reason: str
    chaos_mode: bool
    status: str
    projected_total_mg: float | None
    was_logged: bool
    rejection_summary: str = ""
    feedback: str = ""

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.soda_name.lower():
            return f"{self.brand} {self.soda_name}"
        return self.soda_name

    @property
    def time_label(self) -> str:
        return format_clock_time(self.recommended_at_local)

    @property
    def feedback_label(self) -> str:
        return RECOMMENDATION_FEEDBACK_LABELS.get(self.feedback, "")


@dataclass(frozen=True)
class UserAccount:
    id: int
    username: str
    is_admin: bool = False
    created_at_utc: datetime | None = None
    updated_at_utc: datetime | None = None

    @property
    def role_label(self) -> str:
        return "Admin" if self.is_admin else "User"

    @property
    def created_label(self) -> str:
        if self.created_at_utc is None:
            return "Recently"
        return f"{format_calendar_date(self.created_at_utc)} {format_clock_time(self.created_at_utc)}"


@dataclass(frozen=True)
class UserAuthRecord:
    user: UserAccount
    password_hash: str


@dataclass(frozen=True)
class PassportEntry:
    id: int
    soda_name: str
    brand: str
    country: str
    region: str
    city: str
    category: str
    tried_on: date
    where_tried: str
    contains_caffeine: bool = False
    rating: int | None = None
    would_try_again: bool = False
    notes: str = ""
    created_at_utc: datetime | None = None

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.soda_name.lower():
            return f"{self.brand} {self.soda_name}"
        return self.soda_name

    @property
    def tried_on_label(self) -> str:
        return format_calendar_date(self.tried_on)

    @property
    def origin_label(self) -> str:
        parts = [part for part in (self.city, self.region, self.country) if part]
        return ", ".join(parts) or "Origin not set"

    @property
    def rating_label(self) -> str:
        if self.rating is None:
            return "Unrated"
        return f"{self.rating}/5"

    @property
    def caffeine_label(self) -> str:
        return "Contains caffeine" if self.contains_caffeine else "No caffeine noted"


@dataclass(frozen=True)
class PassportSummary:
    total_entries: int = 0
    unique_sodas: int = 0
    countries_count: int = 0
    latest_country: str = ""


@dataclass(frozen=True)
class PassportBreakdownItem:
    label: str
    count: int


@dataclass(frozen=True)
class PassportInsights:
    countries: tuple[PassportBreakdownItem, ...] = ()
    brands: tuple[PassportBreakdownItem, ...] = ()
    categories: tuple[PassportBreakdownItem, ...] = ()


@dataclass(frozen=True)
class PassportDuplicateGroup:
    entries: tuple[PassportEntry, ...] = ()

    @property
    def display_name(self) -> str:
        if not self.entries:
            return "Duplicate entries"
        return self.entries[0].display_name

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def ids_csv(self) -> str:
        return ",".join(str(entry.id) for entry in self.entries)


@dataclass(frozen=True)
class WishlistEntry:
    id: int
    soda_name: str
    brand: str
    country: str
    category: str
    source_type: str
    source_ref: str
    priority: int = 3
    status: str = WISHLIST_STATUS_ACTIVE
    notes: str = ""
    updated_at_utc: datetime | None = None

    @property
    def display_name(self) -> str:
        if self.brand and self.brand.lower() not in self.soda_name.lower():
            return f"{self.brand} {self.soda_name}"
        return self.soda_name

    @property
    def status_label(self) -> str:
        return self.status.replace("_", " ").title()

    @property
    def source_label(self) -> str:
        if self.source_type == "catalog":
            return "Catalog"
        if self.source_type == "passport":
            return "Passport"
        return "Manual"

    @property
    def updated_label(self) -> str:
        if self.updated_at_utc is None:
            return "Recently"
        return f"{format_calendar_date(self.updated_at_utc)} {format_clock_time(self.updated_at_utc)}"


@dataclass(frozen=True)
class WishlistSummary:
    total_entries: int = 0
    active_entries: int = 0
    found_entries: int = 0
    archived_entries: int = 0
    high_priority_entries: int = 0


@dataclass(frozen=True)
class CatalogDiagnostics:
    total_rows: int = 0
    loaded_rows: int = 0
    disabled_rows: int = 0
    invalid_rows: int = 0
    header_fields: tuple[str, ...] = ()
    missing_optional_columns: tuple[str, ...] = ()
    duplicate_names: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackupFile:
    name: str
    path: str
    created_at: datetime
    size_bytes: int

    @property
    def size_label(self) -> str:
        size = float(self.size_bytes)
        units = ["B", "KB", "MB", "GB"]
        unit = units[0]
        for unit in units:
            if size < 1024 or unit == units[-1]:
                break
            size /= 1024
        return f"{size:.1f} {unit}"

    @property
    def created_label(self) -> str:
        return f"{format_calendar_date(self.created_at)} {format_clock_time(self.created_at)}"
