from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PlannerLongMemory:
    sub_sectors: list[str]
    signal_dictionary: list[str]
    negative_filters: list[str]
    source_policy: dict[str, Any]
    human_preferences: list[str]
    version: int = 1
    updated_at: str = ""
    thesis_weights: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        # keep legacy aliases for compatibility with existing planner consumers
        payload["focus_sectors"] = list(self.sub_sectors)
        payload["sensitive_keywords"] = list(self.signal_dictionary)
        payload["channel_status"] = dict(self.source_policy.get("channel_status", {}))
        return payload


@dataclass
class PlannerTheme:
    theme: str
    priority: int
    keywords: list[str] = field(default_factory=list)
    source_suggestions: list[str] = field(default_factory=list)
    days_active: int = 1
    promote_candidate: bool = False
    recency_score: float = 0.0
    source_diversity_score: float = 0.0
    commercial_signal_score: float = 0.0
    human_preference_score: float = 0.0
    new_theme_score: float = 0.0
    promotion_reason: str = ""
    evidence_summary: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PlannerShortMemory:
    today: str
    daily_strategy: str
    emerging_themes: list[dict[str, Any]]
    priority: list[str]
    keywords: list[str]
    source_suggestions: dict[str, list[str]]
    days_active: dict[str, int]
    promote_candidate: list[str]
    query_boost_terms: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PlannerFeedbackMemoryItem:
    feedback_type: str
    target: str
    value: str
    status: str = "active"
    source_feedback_id: int | None = None
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PlannerCompactionResult:
    promoted_themes: list[str] = field(default_factory=list)
    decayed_themes: list[str] = field(default_factory=list)
    merged_topics: list[dict[str, Any]] = field(default_factory=list)
    archived_preferences: list[str] = field(default_factory=list)
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
