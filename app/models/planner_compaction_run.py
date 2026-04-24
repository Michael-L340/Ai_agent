from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PlannerCompactionRun:
    promoted_themes: list[str] = field(default_factory=list)
    decayed_themes: list[str] = field(default_factory=list)
    merged_topics: list[dict[str, Any]] = field(default_factory=list)
    archived_preferences: list[str] = field(default_factory=list)
    source_policy_changes: list[dict[str, Any]] = field(default_factory=list)
    summary: str = ""
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
