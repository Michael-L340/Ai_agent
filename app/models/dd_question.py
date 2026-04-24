from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DDQuestion:
    lead_id: int
    company_key: str
    company_name: str
    normalized_name: str
    official_domain: str
    dimension: str
    question_type: str
    prompt: str
    scope: str = "lead"
    scope_key: str = ""
    missing_fields: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    dedupe_key: str = ""
    status: str = "open"
    answer_text: str = ""
    answer_feedback_id: int | None = None
    published_at: str = ""
    created_at: str = ""
    updated_at: str = ""
    resolved_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "scope_key": self.scope_key,
            "lead_id": self.lead_id,
            "company_key": self.company_key,
            "company_name": self.company_name,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "dimension": self.dimension,
            "question_type": self.question_type,
            "prompt": self.prompt,
            "missing_fields": list(self.missing_fields),
            "details": self.details,
            "dedupe_key": self.dedupe_key,
            "status": self.status,
            "answer_text": self.answer_text,
            "answer_feedback_id": self.answer_feedback_id,
            "published_at": self.published_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "resolved_at": self.resolved_at,
        }
