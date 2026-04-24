from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PlannerFeedbackEvent:
    feedback_type: str
    target: str
    value: str
    status: str = "active"
    source_feedback_id: int | None = None
    merged: bool = False
    merge_summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
