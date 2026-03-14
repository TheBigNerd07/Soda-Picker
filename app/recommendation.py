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


@dataclass
class _CandidateEvaluation:
    item: CatalogItem
    weight: float
    reasons: list[str] = field(default_factory=list)
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
        rng: Random | None = None,
    ) -> RecommendationResult:
        rng = rng or Random()

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
                )
            )

        candidates = [evaluation for evaluation in evaluations if evaluation.weight > 0]
        if not candidates:
            rejected = self._build_rejections(evaluations, exclude_id=None)
            return RecommendationResult(
                status="blocked",
                headline="No safe pick right now",
                reason=self._build_no_pick_reason(daily_caffeine_total, rules, stage, evaluations, local_now),
                chaos_mode=chaos_mode,
                rejected_options=rejected,
                effective_cutoff_label=rules.effective_cutoff_display,
                bedtime_label=rules.bedtime_display,
                rules_summary=self._build_rules_summary(rules),
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
            ),
            soda=choice.item,
            chaos_mode=chaos_mode,
            projected_total_mg=projected_total,
            remaining_budget_mg=remaining_budget,
            rejected_options=rejected,
            effective_cutoff_label=rules.effective_cutoff_display,
            bedtime_label=rules.bedtime_display,
            rules_summary=self._build_rules_summary(rules),
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

        if chaos_mode and evaluation.weight > 0:
            evaluation.weight *= 1.15

        return evaluation

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
    ) -> str:
        item = choice.item
        if chaos_mode:
            return "Random fun pick within your current safety and timing limits."
        if item.state.preference == PREFERENCE_FAVORITE:
            return "Favorite available, in stock, and still inside the current rules."
        if item.soda.caffeine_mg <= 0 and stage in {"late", "night"}:
            return "Lower-risk pick for later in the day."
        if daily_caffeine_total > 0 and item.soda.caffeine_mg <= 20:
            return f"You already had {daily_caffeine_total:g} mg today, so this keeps things gentler."
        if stage == "late":
            return (
                f"The caffeine window starts tightening around {rules.effective_cutoff_display}, "
                "so this came out as a safer late-day option."
            )
        if item.soda.priority > 1:
            return "Higher-priority pick that still fits your current caffeine budget."
        return "Balanced random pick within your current limits."

    @staticmethod
    def _build_no_pick_reason(
        daily_caffeine_total: float,
        rules: RuntimeRules,
        stage: str,
        evaluations: list[_CandidateEvaluation],
        local_now: datetime,
    ) -> str:
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
