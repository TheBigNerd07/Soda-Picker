from __future__ import annotations

from datetime import datetime
from random import Random

from .config import Settings
from .models import RecommendationResult, Soda, format_clock_time


class RecommendationEngine:
    def recommend(
        self,
        *,
        settings: Settings,
        sodas: list[Soda],
        daily_caffeine_total: float,
        recent_consumed_ids: list[str],
        local_now: datetime,
        chaos_mode: bool = False,
        rng: Random | None = None,
    ) -> RecommendationResult:
        rng = rng or Random()

        if self._minutes_since_midnight(local_now) < settings.no_soda_before_minutes:
            return RecommendationResult(
                status="blocked",
                headline="Still too early for soda",
                reason=(
                    f"It’s {format_clock_time(local_now)} in {settings.timezone_name}. "
                    f"Soda time starts at {settings.no_soda_before_display}."
                ),
                chaos_mode=chaos_mode,
            )

        if not sodas:
            return RecommendationResult(
                status="empty",
                headline="No sodas loaded",
                reason="Load a CSV into the mounted data directory so I have something fizzy to pick from.",
                chaos_mode=chaos_mode,
            )

        stage = self._day_stage(local_now, settings.caffeine_cutoff_hour)
        candidates: list[tuple[Soda, float]] = []

        for soda in sodas:
            weight = max(float(soda.priority), 1.0)
            weight *= self._repeat_penalty(soda, recent_consumed_ids)
            weight *= self._caffeine_budget_factor(soda, daily_caffeine_total, settings, chaos_mode)
            weight *= self._time_of_day_factor(soda, stage, settings, chaos_mode)

            if chaos_mode and weight > 0:
                weight = (weight ** 0.68) * (0.9 + (rng.random() * 0.55))

            if weight > 0:
                candidates.append((soda, weight))

        if not candidates:
            return RecommendationResult(
                status="blocked",
                headline="No safe pick right now",
                reason=self._no_safe_pick_reason(stage, daily_caffeine_total, settings),
                chaos_mode=chaos_mode,
            )

        choice = self._weighted_choice(candidates, rng)
        if choice is None:
            return RecommendationResult(
                status="blocked",
                headline="No safe pick right now",
                reason=self._no_safe_pick_reason(stage, daily_caffeine_total, settings),
                chaos_mode=chaos_mode,
            )

        reason = self._build_reason(
            soda=choice,
            stage=stage,
            daily_caffeine_total=daily_caffeine_total,
            settings=settings,
            chaos_mode=chaos_mode,
        )
        return RecommendationResult(
            status="ready",
            headline="Fizz forecast",
            reason=reason,
            soda=choice,
            chaos_mode=chaos_mode,
        )

    @staticmethod
    def _minutes_since_midnight(local_now: datetime) -> int:
        return (local_now.hour * 60) + local_now.minute

    @staticmethod
    def _day_stage(local_now: datetime, cutoff_hour: int) -> str:
        if local_now.hour >= 20:
            return "night"
        if local_now.hour >= cutoff_hour:
            return "late"
        return "day"

    @staticmethod
    def _repeat_penalty(soda: Soda, recent_consumed_ids: list[str]) -> float:
        if not recent_consumed_ids:
            return 1.0
        if soda.id == recent_consumed_ids[0]:
            return 0.18
        if soda.id in recent_consumed_ids:
            return 0.55
        return 1.0

    @staticmethod
    def _caffeine_budget_factor(
        soda: Soda,
        daily_caffeine_total: float,
        settings: Settings,
        chaos_mode: bool,
    ) -> float:
        if soda.caffeine_mg <= 0:
            return 1.0

        if daily_caffeine_total >= settings.daily_caffeine_limit_mg:
            return 0.0

        projected_total = daily_caffeine_total + soda.caffeine_mg
        if projected_total > settings.daily_caffeine_limit_mg:
            return 0.2 if chaos_mode else 0.08

        if projected_total > settings.daily_caffeine_limit_mg * 0.85:
            return 0.55

        if projected_total > settings.daily_caffeine_limit_mg * 0.7:
            return 0.8

        return 1.0

    @staticmethod
    def _time_of_day_factor(
        soda: Soda,
        stage: str,
        settings: Settings,
        chaos_mode: bool,
    ) -> float:
        caffeine_mg = soda.caffeine_mg
        is_caffeine_free = soda.is_caffeine_free or caffeine_mg <= 0

        if stage == "day":
            if caffeine_mg >= settings.daily_caffeine_limit_mg:
                return 0.45
            return 1.0 if not is_caffeine_free else 0.95

        if stage == "late":
            if is_caffeine_free:
                return 1.9
            if caffeine_mg <= 20:
                return 1.25
            if caffeine_mg <= 40:
                return 0.7
            return 0.2 if chaos_mode else 0.08

        if is_caffeine_free:
            return 2.6
        if caffeine_mg <= 10:
            return 0.35
        if caffeine_mg <= 20 and chaos_mode:
            return 0.12
        return 0.03

    @staticmethod
    def _weighted_choice(candidates: list[tuple[Soda, float]], rng: Random) -> Soda | None:
        total_weight = sum(weight for _, weight in candidates)
        if total_weight <= 0:
            return None

        threshold = rng.uniform(0, total_weight)
        running_total = 0.0
        for soda, weight in candidates:
            running_total += weight
            if running_total >= threshold:
                return soda
        return candidates[-1][0]

    @staticmethod
    def _no_safe_pick_reason(stage: str, daily_caffeine_total: float, settings: Settings) -> str:
        if daily_caffeine_total >= settings.daily_caffeine_limit_mg:
            return (
                f"You already had {daily_caffeine_total:g} mg today, and nothing loaded fits a caffeine-free fallback."
            )
        if stage == "night":
            return "It’s late and the current catalog does not have a gentle enough night pick."
        if stage == "late":
            return "Later-day rules knocked out the current options. Add a few lighter or caffeine-free sodas."
        return "The loaded catalog does not have an option that fits your current rules."

    @staticmethod
    def _build_reason(
        *,
        soda: Soda,
        stage: str,
        daily_caffeine_total: float,
        settings: Settings,
        chaos_mode: bool,
    ) -> str:
        if daily_caffeine_total >= settings.daily_caffeine_limit_mg and soda.caffeine_mg <= 0:
            return f"You already had {daily_caffeine_total:g} mg today, so this keeps it caffeine-free."

        if stage == "night":
            if soda.caffeine_mg <= 0:
                return "Night pick: keeping it caffeine-free."
            return f"It’s late, so this is one of the gentlest remaining options at {soda.caffeine_mg:g} mg."

        if stage == "late":
            if soda.caffeine_mg <= 0:
                return "Low caffeine pick for later in the day."
            if daily_caffeine_total > 0:
                return f"You already had {daily_caffeine_total:g} mg today, so this is a safer choice."
            return f"Later-day pick with a lighter {soda.caffeine_mg:g} mg caffeine hit."

        if chaos_mode:
            return "Random fun pick within your current limits."

        if daily_caffeine_total > 0 and soda.caffeine_mg <= 20:
            return f"You already had {daily_caffeine_total:g} mg today, so this is a safer choice."

        if soda.priority > 1:
            return "Higher-priority pick with plenty of room in your caffeine budget."

        return "Balanced random pick within your current limits."
