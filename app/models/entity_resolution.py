from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class EntityResolution:
    raw_title: str
    candidate_name: str
    normalized_name: str
    entity_type: str
    official_domain: str
    verification_status: str
    verification_score: float
    reject_reason: str
    source: str = ""
    url: str = ""
    snippet: str = ""
    query: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_title": self.raw_title,
            "candidate_name": self.candidate_name,
            "normalized_name": self.normalized_name,
            "entity_type": self.entity_type,
            "official_domain": self.official_domain,
            "verification_status": self.verification_status,
            "verification_score": float(self.verification_score),
            "reject_reason": self.reject_reason,
            "source": self.source,
            "url": self.url,
            "snippet": self.snippet,
            "query": self.query,
            "evidence": self.evidence,
        }

    @property
    def is_verified(self) -> bool:
        return self.verification_status == "verified" and self.entity_type == "company"

    @property
    def is_pending_review(self) -> bool:
        return self.verification_status == "pending_review"

    @property
    def is_likely_company(self) -> bool:
        return self.verification_status == "likely_company" and self.entity_type == "company"

    @property
    def is_rejected(self) -> bool:
        return self.verification_status == "rejected"
