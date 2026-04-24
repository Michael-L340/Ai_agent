from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class DDFeedback:
    scope: str
    scope_key: str
    dimension: str
    feedback_kind: str
    content: str
    lead_id: int | None = None
    company_key: str = ""
    company_name: str = ""
    normalized_name: str = ""
    official_domain: str = ""
    source_question_id: int | None = None
    parsed: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "scope_key": self.scope_key,
            "dimension": self.dimension,
            "feedback_kind": self.feedback_kind,
            "content": self.content,
            "lead_id": self.lead_id,
            "company_key": self.company_key,
            "company_name": self.company_name,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "source_question_id": self.source_question_id,
            "parsed": self.parsed,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

