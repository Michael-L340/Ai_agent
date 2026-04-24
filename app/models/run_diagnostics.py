from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from app.models.run_stage_result import RunStageResult


@dataclass
class RunDiagnostics:
    run_status: str
    new_data_fetched: bool
    used_existing_pool_only: bool
    failure_summary: str
    unavailable_sources: list[str] = field(default_factory=list)
    source_status_by_channel: dict[str, dict[str, Any]] = field(default_factory=dict)
    action_suggestions: list[str] = field(default_factory=list)
    stage_results: list[RunStageResult | dict[str, Any]] = field(default_factory=list)
    recommendation_blockers: list[str] = field(default_factory=list)
    lead_status_by_verification: dict[str, int] = field(default_factory=dict)
    scoring_skip_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["stage_results"] = [
            item.to_dict() if isinstance(item, RunStageResult) else dict(item)
            for item in self.stage_results
        ]
        return data
