from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ScoringCurve:
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "business_score": 0.10,
            "team_score": 0.16,
            "funding_score": 0.12,
            "traction_score": 0.22,
            "market_score": 0.14,
            "thesis_fit_score": 0.20,
            "evidence_score": 0.06,
        }
    )
    strong_recommend_threshold: float = 90.0
    recommend_threshold: float = 82.0
    watchlist_threshold: float = 75.0
    track_only_threshold: float = 60.0
    push_threshold: float = 82.0

    def normalized_weights(self, override: dict[str, float] | None = None) -> dict[str, float]:
        raw = dict(self.weights)
        if override:
            for key, value in override.items():
                raw[str(key)] = float(value)
        total = sum(max(0.0, float(v)) for v in raw.values()) or 1.0
        return {key: round(max(0.0, float(value)) / total, 6) for key, value in raw.items()}

    def compute_raw_score(self, component_scores: dict[str, float], *, override_weights: dict[str, float] | None = None) -> float:
        weights = self.normalized_weights(override_weights)
        weighted_five = 0.0
        for key, weight in weights.items():
            weighted_five += float(component_scores.get(key, 0.0) or 0.0) * weight
        return round(max(0.0, min(100.0, weighted_five * 20.0)), 2)

    def recommendation_band(self, final_score: float) -> str:
        if final_score >= self.strong_recommend_threshold:
            return "Strong Recommend"
        if final_score >= self.recommend_threshold:
            return "Recommend"
        if final_score >= self.watchlist_threshold:
            return "Watchlist"
        if final_score >= self.track_only_threshold:
            return "Track Only"
        return "Reject"

    def hard_gate(
        self,
        *,
        entity_type: str,
        verification_status: str,
        source_hits: int,
        dd_status: str,
        mvp_mode: bool = False,
    ) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        status_normalized = str(verification_status or "").lower()
        dd_normalized = str(dd_status or "").lower()
        if str(entity_type or "").lower() != "company":
            reasons.append("entity_type != company")
        allowed_statuses = {"verified", "likely_company"} if mvp_mode else {"verified"}
        if status_normalized not in allowed_statuses:
            reasons.append(
                "verification_status not in verified/likely_company"
                if mvp_mode
                else "verification_status != verified"
            )
        if int(source_hits or 0) < 2:
            reasons.append("source_hits < 2")
        if mvp_mode and status_normalized == "likely_company":
            if dd_normalized != "dd_partial":
                reasons.append("likely_company requires dd_partial")
        elif dd_normalized not in {"dd_partial", "dd_done"}:
            reasons.append("dd_status below dd_partial")
        return len(reasons) == 0, reasons

    def should_push_recommendation(self, *, final_score: float, hard_gate_passed: bool) -> bool:
        return hard_gate_passed and final_score >= self.push_threshold

    def should_watchlist(self, *, final_score: float, hard_gate_passed: bool) -> bool:
        return hard_gate_passed and self.watchlist_threshold <= final_score < self.push_threshold

    def to_dict(self) -> dict[str, Any]:
        return {
            "weights": dict(self.weights),
            "strong_recommend_threshold": self.strong_recommend_threshold,
            "recommend_threshold": self.recommend_threshold,
            "watchlist_threshold": self.watchlist_threshold,
            "track_only_threshold": self.track_only_threshold,
            "push_threshold": self.push_threshold,
        }
