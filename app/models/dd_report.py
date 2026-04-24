from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.models.dd_question import DDQuestion


def _clean_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, (tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


@dataclass(slots=True)
class DDProfile:
    fields: dict[str, Any] = field(default_factory=dict)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "fields": self.fields,
            "evidence": self.evidence,
            "missing_fields": self.missing_fields,
            "confidence": float(self.confidence),
        }


@dataclass(slots=True)
class DDOverall:
    dd_status: str
    completeness_score: float
    source_hits: int
    summary: str
    missing_dimensions: list[str] = field(default_factory=list)
    confidence: float = 0.0
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "dd_status": self.dd_status,
            "completeness_score": float(self.completeness_score),
            "source_hits": int(self.source_hits),
            "summary": self.summary,
            "missing_dimensions": self.missing_dimensions,
            "confidence": float(self.confidence),
            "generated_at": self.generated_at,
        }


@dataclass(slots=True)
class DDReport:
    lead_id: int
    company_name: str
    normalized_name: str
    official_domain: str
    source_hits: int
    dd_status: str
    completeness_score: float
    business_profile: DDProfile
    team_profile: DDProfile
    funding_profile: DDProfile
    traction_profile: DDProfile
    market_position: DDProfile
    dd_overall: DDOverall
    business_summary: str = ""
    team_summary: str = ""
    funding_summary: str = ""
    traction_summary: str = ""
    industry_position: str = ""
    questions: list[DDQuestion] = field(default_factory=list)
    evidence_json: dict[str, Any] = field(default_factory=dict)

    def legacy_summary_map(self) -> dict[str, str]:
        return {
            "business_summary": self.business_summary or self._summary_from_fields(
                self.business_profile,
                ["one_liner", "products_services", "target_customers", "use_cases", "official_domain"],
            ),
            "team_summary": self.team_summary or self._summary_from_fields(
                self.team_profile,
                ["founders", "key_people", "prior_companies", "research_background"],
            ),
            "funding_summary": self.funding_summary or self._summary_from_fields(
                self.funding_profile,
                ["funding_rounds", "total_raised", "valuation", "notable_investors", "founded_year", "headquarters"],
            ),
            "traction_summary": self.traction_summary or self._summary_from_fields(
                self.traction_profile,
                ["customers", "partners", "product_launches", "revenue_signals", "deployment_signals"],
            ),
            "industry_position": self.industry_position or self._summary_from_fields(
                self.market_position,
                ["sub_sector", "competitors", "leader_signals", "crowdedness", "is_new_category"],
            ),
        }

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "lead_id": self.lead_id,
            "company_name": self.company_name,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "source_hits": int(self.source_hits),
            "dd_status": self.dd_status,
            "completeness_score": float(self.completeness_score),
            "business_profile": self.business_profile.to_dict(),
            "team_profile": self.team_profile.to_dict(),
            "funding_profile": self.funding_profile.to_dict(),
            "traction_profile": self.traction_profile.to_dict(),
            "market_position": self.market_position.to_dict(),
            "dd_overall": self.dd_overall.to_dict(),
            "questions": [question.to_dict() if isinstance(question, DDQuestion) else dict(question) for question in self.questions],
            "evidence_json": self.evidence_json,
        }
        payload.update(self.legacy_summary_map())
        return payload

    def _summary_from_fields(self, profile: DDProfile, field_order: list[str]) -> str:
        parts: list[str] = []
        for field_name in field_order:
            value = profile.fields.get(field_name)
            if not value:
                continue
            if isinstance(value, list):
                cleaned = _clean_list(value)
                if cleaned:
                    parts.append(", ".join(cleaned[:4]))
            else:
                text = str(value).strip()
                if text:
                    parts.append(text)
            if len(parts) >= 2:
                break
        return " | ".join(parts)
