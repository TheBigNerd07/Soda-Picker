from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from random import Random

from .config import RuntimeRules
from .models import (
    CatalogItem,
    ConsumptionEntry,
    PREFERENCE_DISLIKE,
    PREFERENCE_FAVORITE,
    RecommendationResult,
    RejectionDetail,
    format_clock_time,
)
from .pick_styles import ANY_PICK_STYLE, PickStyleOption
from .training import PassportTrainingProfile, build_training_adjustment


@dataclass
class _CandidateEvaluation:
    item: CatalogItem
    weight: float
    reasons: list[str] = field(default_factory=list)
    nudges: list[str] = field(default_factory=list)
    blocked: bool = False


class RecommendationEngine:
    def recommend(
        self,
        *,
        rules: RuntimeRules,
        catalog_items: list[CatalogItem],
        daily_caffeine_total: float,
        recent_soda_ids: list[str],
        today_entries: list[ConsumptionEntry],
        local_now: datetime,
        chaos_mode: bool = False,
        pick_style: PickStyleOption = ANY_PICK_STYLE,
        training_profile: PassportTrainingProfile | None = None,
        rng: Random | None = None,
    ) -> RecommendationResult:
        rng = rng or Random()
        training_profile = training_profile or PassportTrainingProfile()

        if self._minutes_since_midnight(local_now) < rules.no_soda_before_minutes:
            return RecommendationResult(
                status="blocked",
                headline="Still too early for soda",
                reason=(
                    f"It’s {format_clock_time(local_now)}. "
                    f"Soda time starts at {rules.no_soda_before_display}."
                ),
                chaos_mode=chaos_mode,
                effective_cutoff_label=rules.effective_cutoff_display,
                bedtime_label=rules.bedtime_display,
                rules_summary=self._build_rules_summary(rules),
                pick_style_value=pick_style.value,
                pick_style_label=pick_style.label,
            )

        if not catalog_items:
            return RecommendationResult(
                status="empty",
                headline="No sodas loaded",
                reason="Load a CSV into the mounted data directory so there is something fizzy to pick from.",
                chaos_mode=chaos_mode,
                effective_cutoff_label=rules.effective_cutoff_display,
                bedtime_label=rules.bedtime_display,
                rules_summary=self._build_rules_summary(rules),
                pick_style_value=pick_style.value,
                pick_style_label=pick_style.label,
            )

        stage = self._day_stage(local_now, rules)
        evaluations: list[_CandidateEvaluation] = []
        for item in catalog_items:
            evaluations.append(
                self._score_item(
                    item=item,
                    rules=rules,
                    daily_caffeine_total=daily_caffeine_total,
                    recent_soda_ids=recent_soda_ids,
                    local_now=local_now,
                    stage=stage,
                    chaos_mode=chaos_mode,
                    training_profile=training_profile,
                )
            )

        candidates, pick_style_fallback_used = self._filter_candidates_for_pick_style(
            evaluations=evaluations,
            pick_style=pick_style,
        )
        if not candidates:
            rejected = self._build_rejections(evaluations, exclude_id=None)
            return RecommendationResult(
                status="blocked",
                headline="No safe pick right now",
                reason=self._build_no_pick_reason(
                    daily_caffeine_total,
                    rules,
                    stage,
                    evaluations,
                    local_now,
                    pick_style=pick_style,
                ),
                chaos_mode=chaos_mode,
                rejected_options=rejected,
                effective_cutoff_label=rules.effective_cutoff_display,
                bedtime_label=rules.bedtime_display,
                rules_summary=self._build_rules_summary(rules),
                pick_style_value=pick_style.value,
                pick_style_label=pick_style.label,
            )

        choice = self._weighted_choice(candidates, rng)
        projected_total = daily_caffeine_total + choice.item.soda.caffeine_mg
        remaining_budget = rules.daily_caffeine_limit_mg - projected_total
        rejected = self._build_rejections(evaluations, exclude_id=choice.item.id)

        return RecommendationResult(
            status="ready",
            headline="Fizz forecast",
            reason=self._build_reason(
                choice=choice,
                stage=stage,
                rules=rules,
                daily_caffeine_total=daily_caffeine_total,
                chaos_mode=chaos_mode,
                pick_style=pick_style,
                pick_style_fallback_used=pick_style_fallback_used,
            ),
            soda=choice.item,
            chaos_mode=chaos_mode,
            projected_total_mg=projected_total,
            remaining_budget_mg=remaining_budget,
            rejected_options=rejected,
            effective_cutoff_label=rules.effective_cutoff_display,
            bedtime_label=rules.bedtime_display,
            rules_summary=self._build_rules_summary(rules),
            pick_style_value=pick_style.value,
            pick_style_label=pick_style.label,
            pick_style_fallback_used=pick_style_fallback_used,
        )

    @staticmethod
    def _minutes_since_midnight(local_now: datetime) -> int:
        return (local_now.hour * 60) + local_now.minute

    @staticmethod
    def _day_stage(local_now: datetime, rules: RuntimeRules) -> str:
        if local_now.hour >= max(20, rules.bedtime_hour - 1):
            return "night"
        if local_now.hour >= rules.effective_caffeine_stop_hour:
            return "late"
        return "day"

    def _score_item(
        self,
        *,
        item: CatalogItem,
        rules: RuntimeRules,
        daily_caffeine_total: float,
        recent_soda_ids: list[str],
        local_now: datetime,
        stage: str,
        chaos_mode: bool,
        training_profile: PassportTrainingProfile,
    ) -> _CandidateEvaluation:
        evaluation = _CandidateEvaluation(item=item, weight=max(float(item.soda.priority), 1.0))

        if not item.state.is_available:
            evaluation.weight = 0
            evaluation.blocked = True
            evaluation.reasons.append("not marked in stock")
            return evaluation

        if item.state.is_temporarily_banned(local_now):
            evaluation.weight = 0
            evaluation.blocked = True
            until = item.state.temp_ban_until.strftime("%b %d")
            evaluation.reasons.append(f"temporarily banned until {until}")
            return evaluation

        if item.soda.is_diet and not rules.allow_diet_sodas:
            evaluation.weight = 0
            evaluation.blocked = True
            evaluation.reasons.append("diet and zero-sugar sodas are turned off")
            return evaluation

        if not item.soda.is_diet and not rules.allow_full_sugar_sodas:
            evaluation.weight = 0
            evaluation.blocked = True
            evaluation.reasons.append("full-sugar sodas are turned off")
            return evaluation

        if item.state.preference == PREFERENCE_FAVORITE:
            evaluation.weight *= 1.7
        elif item.state.preference == PREFERENCE_DISLIKE:
            evaluation.weight *= 0.38
            evaluation.reasons.append("marked as a dislike")

        recent_window = recent_soda_ids[: rules.duplicate_lookback]
        if recent_window:
            if item.id == recent_window[0]:
                evaluation.weight *= 0.05
                evaluation.reasons.append("showed up on the last pick")
            elif item.id in recent_window:
                evaluation.weight *= 0.2
                evaluation.reasons.append("still inside the no-repeat window")

        caffeine_mg = item.soda.caffeine_mg
        if caffeine_mg > 0:
            projected_total = daily_caffeine_total + caffeine_mg
            if daily_caffeine_total >= rules.daily_caffeine_limit_mg:
                evaluation.weight = 0
                evaluation.blocked = True
                evaluation.reasons.append("today's caffeine limit is already used up")
                return evaluation

            if projected_total > rules.daily_caffeine_limit_mg:
                evaluation.weight *= 0.08 if not chaos_mode else 0.15
                evaluation.reasons.append("would push the day over your caffeine cap")
            elif projected_total > rules.daily_caffeine_limit_mg * 0.85:
                evaluation.weight *= 0.45
                evaluation.reasons.append("would take you close to the daily limit")
            elif projected_total > rules.daily_caffeine_limit_mg * 0.7:
                evaluation.weight *= 0.75
                evaluation.reasons.append("uses a big chunk of today's caffeine budget")

            if stage == "late":
                if caffeine_mg <= 20:
                    evaluation.weight *= 0.85
                    evaluation.reasons.append("late-day caffeine keeps getting nudged down")
                elif caffeine_mg <= 40:
                    evaluation.weight *= 0.35
                    evaluation.reasons.append("later-day rules prefer lighter caffeine")
                else:
                    evaluation.weight *= 0.08 if not chaos_mode else 0.16
                    evaluation.reasons.append("high caffeine is heavily penalized this late")
            elif stage == "night":
                evaluation.weight *= 0.02
                evaluation.reasons.append("night mode strongly prefers caffeine-free soda")
        else:
            if stage == "late":
                evaluation.weight *= 1.75
            elif stage == "night":
                evaluation.weight *= 2.4

        training_adjustment = build_training_adjustment(item, training_profile)
        if training_adjustment.multiplier != 1.0:
            evaluation.weight *= training_adjustment.multiplier
        evaluation.nudges.extend(training_adjustment.nudges)
        evaluation.reasons.extend(training_adjustment.penalties)

        if chaos_mode and evaluation.weight > 0:
            evaluation.weight *= 1.15

        return evaluation

    @staticmethod
    def _filter_candidates_for_pick_style(
        *,
        evaluations: list[_CandidateEvaluation],
        pick_style: PickStyleOption,
    ) -> tuple[list[_CandidateEvaluation], bool]:
        candidates = [evaluation for evaluation in evaluations if evaluation.weight > 0]
        if pick_style.is_any or not candidates:
            return candidates, False

        matching = [evaluation for evaluation in candidates if pick_style.matches(evaluation.item)]
        if matching:
            return matching, False
        return candidates, True

    @staticmethod
    def _weighted_choice(candidates: list[_CandidateEvaluation], rng: Random) -> _CandidateEvaluation:
        total_weight = sum(candidate.weight for candidate in candidates)
        threshold = rng.uniform(0, total_weight)
        running_total = 0.0
        for candidate in candidates:
            running_total += candidate.weight
            if running_total >= threshold:
                return candidate
        return candidates[-1]

    @staticmethod
    def _build_rejections(
        evaluations: list[_CandidateEvaluation],
        *,
        exclude_id: str | None,
    ) -> tuple[RejectionDetail, ...]:
        ranked = sorted(
            (
                evaluation
                for evaluation in evaluations
                if evaluation.item.id != exclude_id and evaluation.reasons
            ),
            key=lambda evaluation: (-evaluation.item.soda.priority, evaluation.item.display_name.lower()),
        )
        notes: list[RejectionDetail] = []
        for evaluation in ranked[:3]:
            notes.append(
                RejectionDetail(
                    soda_id=evaluation.item.id,
                    soda_name=evaluation.item.display_name,
                    reason=evaluation.reasons[0],
                )
            )
        return tuple(notes)

    @staticmethod
    def _build_reason(
        *,
        choice: _CandidateEvaluation,
        stage: str,
        rules: RuntimeRules,
        daily_caffeine_total: float,
        chaos_mode: bool,
        pick_style: PickStyleOption,
        pick_style_fallback_used: bool,
    ) -> str:
        item = choice.item
        base_reason: str
        if chaos_mode:
            base_reason = "Random fun pick within your current safety and timing limits."
        elif item.state.preference == PREFERENCE_FAVORITE:
            base_reason = "Favorite available, in stock, and still inside the current rules."
        elif item.soda.caffeine_mg <= 0 and stage in {"late", "night"}:
            base_reason = "Lower-risk pick for later in the day."
        elif daily_caffeine_total > 0 and item.soda.caffeine_mg <= 20:
            base_reason = f"You already had {daily_caffeine_total:g} mg today, so this keeps things gentler."
        elif stage == "late":
            base_reason = (
                f"The caffeine window starts tightening around {rules.effective_cutoff_display}, "
                "so this came out as a safer late-day option."
            )
        elif item.soda.priority > 1:
            base_reason = "Higher-priority pick that still fits your current caffeine budget."
        else:
            base_reason = "Balanced random pick within your current limits."

        if choice.nudges:
            base_reason = f"{base_reason} {choice.nudges[0].capitalize()}."

        if pick_style.is_any:
            return base_reason
        if pick_style_fallback_used:
            return (
                f"Nothing safe matched your {pick_style.label.lower()} pick, "
                f"so this fell back to the broader safe list. {base_reason}"
            )
        return f"Matched your {pick_style.label.lower()} pick. {base_reason}"

    @staticmethod
    def _build_no_pick_reason(
        daily_caffeine_total: float,
        rules: RuntimeRules,
        stage: str,
        evaluations: list[_CandidateEvaluation],
        local_now: datetime,
        *,
        pick_style: PickStyleOption,
    ) -> str:
        if not pick_style.is_any:
            matching_evaluations = [evaluation for evaluation in evaluations if pick_style.matches(evaluation.item)]
            if matching_evaluations and all(evaluation.weight <= 0 for evaluation in matching_evaluations):
                return (
                    f"Nothing in your {pick_style.label.lower()} lane survived the current inventory, "
                    "timing, and caffeine rules."
                )
        if not rules.allow_diet_sodas and not rules.allow_full_sugar_sodas:
            return "Both diet and full-sugar sodas are turned off in your settings."
        if not rules.allow_diet_sodas and all(evaluation.item.soda.is_diet for evaluation in evaluations):
            return "Only diet or zero-sugar sodas are available right now, and you turned those off."
        if not rules.allow_full_sugar_sodas and all(not evaluation.item.soda.is_diet for evaluation in evaluations):
            return "Only full-sugar sodas are available right now, and you turned those off."
        if all(not evaluation.item.state.is_available for evaluation in evaluations):
            return "Everything in the catalog is currently marked out of stock."
        if all(evaluation.item.state.is_temporarily_banned(local_now) for evaluation in evaluations):
            return "Everything is temporarily banned right now."
        if daily_caffeine_total >= rules.daily_caffeine_limit_mg:
            return (
                f"You already hit {daily_caffeine_total:g} mg today, "
                "and there is no caffeine-free option currently available."
            )
        if stage == "night":
            return "It is night mode now, and nothing available is gentle enough."
        return "The current inventory, bans, and caffeine rules knocked everything out."

    @staticmethod
    def _build_rules_summary(rules: RuntimeRules) -> str:
        return (
            f"{rules.rules_label}: soda starts at {rules.no_soda_before_display}, "
            f"caffeine gets squeezed after {rules.effective_cutoff_display}, bedtime is {rules.bedtime_display}."
        )
