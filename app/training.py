from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from .models import CatalogItem, PassportEntry
from .pick_styles import featured_pick_styles, normalize_pick_style_key

TRAINING_KEY_BOOST_CATEGORIES = "boost_categories"
TRAINING_KEY_AVOID_CATEGORIES = "avoid_categories"
TRAINING_KEY_BOOST_MOODS = "boost_moods"
TRAINING_KEY_AVOID_MOODS = "avoid_moods"
TRAINING_KEY_STRENGTH = "strength"
TRAINING_KEY_MODE = "mode"
TRAINING_STORAGE_KEYS = (
    TRAINING_KEY_MODE,
    TRAINING_KEY_BOOST_CATEGORIES,
    TRAINING_KEY_AVOID_CATEGORIES,
    TRAINING_KEY_BOOST_MOODS,
    TRAINING_KEY_AVOID_MOODS,
    TRAINING_KEY_STRENGTH,
)
TRAINING_MODE_AUTO = "auto"
TRAINING_MODE_CLASSIC = "classic"
TRAINING_MODE_TRAINED = "trained"
TRAINING_MODE_CHOICES = (
    TRAINING_MODE_AUTO,
    TRAINING_MODE_CLASSIC,
    TRAINING_MODE_TRAINED,
)
TRAINING_MODE_LABELS = {
    TRAINING_MODE_AUTO: "Auto",
    TRAINING_MODE_CLASSIC: "Classic",
    TRAINING_MODE_TRAINED: "Passport-trained",
}
TRAINING_MODE_HINTS = {
    TRAINING_MODE_AUTO: "Use training when this account has saved passport preferences, otherwise fall back to classic.",
    TRAINING_MODE_CLASSIC: "Always use the original rules and weighting only.",
    TRAINING_MODE_TRAINED: "Use your saved passport training whenever it exists; if there is no signal yet, it behaves like classic.",
}
DEFAULT_TRAINING_STRENGTH = "balanced"
TRAINING_STRENGTH_CHOICES = ("subtle", "balanced", "assertive")
TRAINING_STRENGTH_LABELS = {
    "subtle": "Subtle",
    "balanced": "Balanced",
    "assertive": "Assertive",
}
TRAINING_STRENGTH_HINTS = {
    "subtle": "Keep the passport influence light.",
    "balanced": "Use passport answers as a steady nudge.",
    "assertive": "Let passport training steer the safe list more strongly.",
}
TRAINING_FACTORS = {
    "subtle": {"category_boost": 1.12, "category_avoid": 0.88, "mood_boost": 1.08, "mood_avoid": 0.9},
    "balanced": {"category_boost": 1.28, "category_avoid": 0.7, "mood_boost": 1.18, "mood_avoid": 0.8},
    "assertive": {"category_boost": 1.46, "category_avoid": 0.52, "mood_boost": 1.3, "mood_avoid": 0.68},
}
_TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class PassportTrainingOption:
    key: str
    label: str
    detail: str


@dataclass(frozen=True)
class PassportTrainingMenu:
    passport_entry_count: int
    category_options: tuple[PassportTrainingOption, ...] = ()
    mood_options: tuple[PassportTrainingOption, ...] = ()
    boost_labels: tuple[str, ...] = ()
    avoid_labels: tuple[str, ...] = ()

    @property
    def has_passport_entries(self) -> bool:
        return self.passport_entry_count > 0

    @property
    def has_questions(self) -> bool:
        return bool(self.category_options or self.mood_options)


@dataclass(frozen=True)
class PassportTrainingProfile:
    mode: str = TRAINING_MODE_AUTO
    boost_category_keys: tuple[str, ...] = ()
    avoid_category_keys: tuple[str, ...] = ()
    boost_mood_values: tuple[str, ...] = ()
    avoid_mood_values: tuple[str, ...] = ()
    strength: str = DEFAULT_TRAINING_STRENGTH

    @property
    def normalized_mode(self) -> str:
        if self.mode in TRAINING_MODE_CHOICES:
            return self.mode
        return TRAINING_MODE_AUTO

    @property
    def mode_label(self) -> str:
        return TRAINING_MODE_LABELS[self.normalized_mode]

    @property
    def normalized_strength(self) -> str:
        if self.strength in TRAINING_STRENGTH_CHOICES:
            return self.strength
        return DEFAULT_TRAINING_STRENGTH

    @property
    def strength_label(self) -> str:
        return TRAINING_STRENGTH_LABELS[self.normalized_strength]

    @property
    def factors(self) -> dict[str, float]:
        return TRAINING_FACTORS[self.normalized_strength]

    @property
    def is_active(self) -> bool:
        return bool(
            self.boost_category_keys
            or self.avoid_category_keys
            or self.boost_mood_values
            or self.avoid_mood_values
        )

    @property
    def effective_mode(self) -> str:
        if self.normalized_mode == TRAINING_MODE_CLASSIC:
            return TRAINING_MODE_CLASSIC
        if self.normalized_mode == TRAINING_MODE_TRAINED and self.is_active:
            return TRAINING_MODE_TRAINED
        if self.normalized_mode == TRAINING_MODE_AUTO and self.is_active:
            return TRAINING_MODE_TRAINED
        return TRAINING_MODE_CLASSIC

    @property
    def effective_mode_label(self) -> str:
        return TRAINING_MODE_LABELS[self.effective_mode]

    @property
    def uses_training(self) -> bool:
        return self.effective_mode == TRAINING_MODE_TRAINED


@dataclass(frozen=True)
class TrainingAdjustment:
    multiplier: float = 1.0
    nudges: tuple[str, ...] = ()
    penalties: tuple[str, ...] = ()


def profile_from_storage(values: dict[str, str] | None) -> PassportTrainingProfile:
    values = values or {}
    return PassportTrainingProfile(
        mode=(values.get(TRAINING_KEY_MODE, "") or TRAINING_MODE_AUTO).strip().lower(),
        boost_category_keys=_split_values(values.get(TRAINING_KEY_BOOST_CATEGORIES, "")),
        avoid_category_keys=_split_values(values.get(TRAINING_KEY_AVOID_CATEGORIES, "")),
        boost_mood_values=_split_values(values.get(TRAINING_KEY_BOOST_MOODS, "")),
        avoid_mood_values=_split_values(values.get(TRAINING_KEY_AVOID_MOODS, "")),
        strength=(values.get(TRAINING_KEY_STRENGTH, "") or DEFAULT_TRAINING_STRENGTH).strip().lower(),
    )


def serialize_training_form(
    *,
    boost_category_keys: Iterable[str],
    avoid_category_keys: Iterable[str],
    boost_mood_values: Iterable[str],
    avoid_mood_values: Iterable[str],
    mode: str,
    strength: str,
    allowed_category_keys: set[str],
    allowed_mood_values: set[str],
) -> dict[str, str]:
    cleaned_boost_categories = _clean_values(boost_category_keys, allowed=allowed_category_keys)
    cleaned_avoid_categories = _clean_values(
        avoid_category_keys,
        allowed=allowed_category_keys,
        blocked=set(cleaned_boost_categories),
    )
    cleaned_boost_moods = _clean_values(boost_mood_values, allowed=allowed_mood_values)
    cleaned_avoid_moods = _clean_values(
        avoid_mood_values,
        allowed=allowed_mood_values,
        blocked=set(cleaned_boost_moods),
    )
    normalized_strength = strength.strip().lower()
    if normalized_strength not in TRAINING_STRENGTH_CHOICES:
        normalized_strength = DEFAULT_TRAINING_STRENGTH
    normalized_mode = mode.strip().lower()
    if normalized_mode not in TRAINING_MODE_CHOICES:
        normalized_mode = TRAINING_MODE_AUTO

    return {
        TRAINING_KEY_MODE: normalized_mode,
        TRAINING_KEY_BOOST_CATEGORIES: ",".join(cleaned_boost_categories),
        TRAINING_KEY_AVOID_CATEGORIES: ",".join(cleaned_avoid_categories),
        TRAINING_KEY_BOOST_MOODS: ",".join(cleaned_boost_moods),
        TRAINING_KEY_AVOID_MOODS: ",".join(cleaned_avoid_moods),
        TRAINING_KEY_STRENGTH: normalized_strength,
    }


def build_training_menu(
    *,
    passport_entries: list[PassportEntry],
    catalog_items: list[CatalogItem],
    profile: PassportTrainingProfile,
) -> PassportTrainingMenu:
    category_options = _build_category_options(passport_entries=passport_entries, catalog_items=catalog_items)
    mood_options = _build_mood_options(passport_entries=passport_entries, catalog_items=catalog_items)
    option_labels = {option.key: option.label for option in (*category_options, *mood_options)}
    boost_labels = tuple(
        option_labels[key]
        for key in (*profile.boost_category_keys, *profile.boost_mood_values)
        if key in option_labels
    )
    avoid_labels = tuple(
        option_labels[key]
        for key in (*profile.avoid_category_keys, *profile.avoid_mood_values)
        if key in option_labels
    )
    return PassportTrainingMenu(
        passport_entry_count=len(passport_entries),
        category_options=category_options,
        mood_options=mood_options,
        boost_labels=boost_labels,
        avoid_labels=avoid_labels,
    )


def build_training_strength_options() -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (value, TRAINING_STRENGTH_LABELS[value], TRAINING_STRENGTH_HINTS[value])
        for value in TRAINING_STRENGTH_CHOICES
    )


def build_training_mode_options() -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (value, TRAINING_MODE_LABELS[value], TRAINING_MODE_HINTS[value])
        for value in TRAINING_MODE_CHOICES
    )


def build_training_adjustment(item: CatalogItem, profile: PassportTrainingProfile) -> TrainingAdjustment:
    if not profile.is_active:
        return TrainingAdjustment()

    multiplier = 1.0
    nudges: list[str] = []
    penalties: list[str] = []
    factors = profile.factors

    category_key = normalize_pick_style_key(item.soda.category)
    category_label = item.soda.category.strip().title() or item.display_name
    if category_key:
        if category_key in profile.boost_category_keys:
            multiplier *= factors["category_boost"]
            nudges.append(f"your passport training favors {category_label.lower()} picks")
        elif category_key in profile.avoid_category_keys:
            multiplier *= factors["category_avoid"]
            penalties.append(f"passport training cools off {category_label.lower()} picks")

    matched_boost_mood = None
    matched_avoid_mood = None
    for option in featured_pick_styles([item], include_caffeine_free=False):
        if option.value in profile.boost_mood_values and matched_boost_mood is None:
            matched_boost_mood = option.label
        if option.value in profile.avoid_mood_values and matched_avoid_mood is None:
            matched_avoid_mood = option.label

    if matched_boost_mood is not None:
        multiplier *= factors["mood_boost"]
        nudges.append(f"your passport training leans {matched_boost_mood.lower()}")
    if matched_avoid_mood is not None:
        multiplier *= factors["mood_avoid"]
        penalties.append(f"passport training tones down {matched_avoid_mood.lower()} picks")

    return TrainingAdjustment(
        multiplier=multiplier,
        nudges=tuple(nudges),
        penalties=tuple(penalties),
    )


def _build_category_options(
    *,
    passport_entries: list[PassportEntry],
    catalog_items: list[CatalogItem],
) -> tuple[PassportTrainingOption, ...]:
    catalog_categories: dict[str, str] = {}
    for item in catalog_items:
        key = normalize_pick_style_key(item.soda.category)
        if key and key not in catalog_categories:
            catalog_categories[key] = item.soda.category.strip().title()

    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "positive": 0, "negative": 0})
    for entry in passport_entries:
        key = normalize_pick_style_key(entry.category)
        if not key or key not in catalog_categories:
            continue
        stats = counts[key]
        stats["count"] += 1
        if _is_positive_entry(entry):
            stats["positive"] += 1
        if _is_negative_entry(entry):
            stats["negative"] += 1

    ranked = sorted(
        counts.items(),
        key=lambda item: (-item[1]["positive"], -item[1]["count"], catalog_categories[item[0]].lower()),
    )
    return tuple(
        PassportTrainingOption(
            key=key,
            label=catalog_categories[key],
            detail=_format_option_detail(stats),
        )
        for key, stats in ranked[:8]
    )


def _build_mood_options(
    *,
    passport_entries: list[PassportEntry],
    catalog_items: list[CatalogItem],
) -> tuple[PassportTrainingOption, ...]:
    mood_options = featured_pick_styles(catalog_items, include_caffeine_free=False)
    if not mood_options:
        return ()

    ranked_rows: list[tuple[int, int, str, str, str]] = []
    for option in mood_options:
        count = 0
        positive = 0
        negative = 0
        for entry in passport_entries:
            if not option.matches_fields(
                name=entry.soda_name,
                brand=entry.brand,
                category=entry.category,
                tags=_entry_tags(entry),
            ):
                continue
            count += 1
            if _is_positive_entry(entry):
                positive += 1
            if _is_negative_entry(entry):
                negative += 1
        if count:
            ranked_rows.append((positive, count, option.value, option.label, _format_option_detail({"count": count, "positive": positive, "negative": negative})))

    ranked_rows.sort(key=lambda row: (-row[0], -row[1], row[3].lower()))
    return tuple(
        PassportTrainingOption(
            key=value,
            label=label,
            detail=detail,
        )
        for _, _, value, label, detail in ranked_rows[:6]
    )


def _entry_tags(entry: PassportEntry) -> tuple[str, ...]:
    raw_text = " ".join(part for part in (entry.notes, entry.where_tried) if part).lower()
    tokens = [token for token in _TOKEN_SPLIT.split(raw_text) if token]
    return tuple(tokens)


def _clean_values(
    values: Iterable[str],
    *,
    allowed: set[str],
    blocked: set[str] | None = None,
) -> tuple[str, ...]:
    blocked = blocked or set()
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen or normalized in blocked or normalized not in allowed:
            continue
        cleaned.append(normalized)
        seen.add(normalized)
    return tuple(cleaned)


def _split_values(value: str) -> tuple[str, ...]:
    parts: list[str] = []
    seen: set[str] = set()
    for part in value.split(","):
        normalized = part.strip()
        if not normalized or normalized in seen:
            continue
        parts.append(normalized)
        seen.add(normalized)
    return tuple(parts)


def _is_positive_entry(entry: PassportEntry) -> bool:
    return entry.would_try_again or (entry.rating is not None and entry.rating >= 4)


def _is_negative_entry(entry: PassportEntry) -> bool:
    return (entry.rating is not None and entry.rating <= 2) and not entry.would_try_again


def _format_option_detail(stats: dict[str, int]) -> str:
    count = stats["count"]
    pieces = [f"{count} passport entr{'y' if count == 1 else 'ies'}"]
    positive = stats["positive"]
    negative = stats["negative"]
    if positive:
        pieces.append(f"{positive} strong keeper{'s' if positive != 1 else ''}")
    if negative:
        pieces.append(f"{negative} low rating{'s' if negative != 1 else ''}")
    return ", ".join(pieces)
