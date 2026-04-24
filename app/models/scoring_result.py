from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ScoringResult:
    lead_id: int
    company_name: str
    normalized_name: str = ""
    official_domain: str = ""
    source_hits: int = 0
    completeness_score: float = 0.0
    dd_status: str = "dd_pending_review"

    business_score: float = 0.0
    team_score: float = 0.0
    funding_score: float = 0.0
    traction_score: float = 0.0
    market_score: float = 0.0
    thesis_fit_score: float = 0.0
    evidence_score: float = 0.0
    raw_score: float = 0.0
    confidence_multiplier: float = 1.0
    boost_score: float = 0.0
    penalty_score: float = 0.0
    final_score: float = 0.0

    recommendation_band: str = "Reject"
    recommendation_reason: str = ""
    score_reason: str = ""
    thesis_fit_breakdown: dict[str, Any] = field(default_factory=dict)
    policy_version: int = 1
    matched_policy_rules: list[str] = field(default_factory=list)
    hard_gate_passed: bool = False
    hard_gate_reasons: list[str] = field(default_factory=list)

    component_reasons: dict[str, list[str]] = field(default_factory=dict)
    evidence_snapshot: dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "lead_id": self.lead_id,
            "company_name": self.company_name,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "source_hits": int(self.source_hits),
            "completeness_score": float(self.completeness_score),
            "dd_status": self.dd_status,
            "business_score": float(self.business_score),
            "team_score": float(self.team_score),
            "funding_score": float(self.funding_score),
            "traction_score": float(self.traction_score),
            "market_score": float(self.market_score),
            "thesis_fit_score": float(self.thesis_fit_score),
            "evidence_score": float(self.evidence_score),
            "raw_score": float(self.raw_score),
            "confidence_multiplier": float(self.confidence_multiplier),
            "boost_score": float(self.boost_score),
            "penalty_score": float(self.penalty_score),
            "final_score": float(self.final_score),
            "recommendation_band": self.recommendation_band,
            "recommendation_reason": self.recommendation_reason,
            "score_reason": self.score_reason or self.recommendation_reason,
            "thesis_fit_breakdown": self.thesis_fit_breakdown,
            "policy_version": int(self.policy_version),
            "matched_policy_rules": list(self.matched_policy_rules),
            "hard_gate_passed": bool(self.hard_gate_passed),
            "hard_gate_reasons": list(self.hard_gate_reasons),
            "component_reasons": self.component_reasons,
            "evidence_snapshot": self.evidence_snapshot,
            "updated_at": self.updated_at,
            "base_score": float(self.raw_score),
            "thesis_fit": float(self.thesis_fit_score),
            "evidence_strength": float(self.evidence_score),
        }
