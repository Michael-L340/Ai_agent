from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class RunStageResult:
    stage_name: str
    status: str
    started_at: datetime
    ended_at: datetime
    duration_sec: float
    input_count: int = 0
    output_count: int = 0
    error_type: str = ""
    error_message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
