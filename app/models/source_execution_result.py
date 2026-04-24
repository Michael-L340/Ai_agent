from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class SourceExecutionResult:
    source_name: str
    status: str
    request_attempted: bool
    request_succeeded: bool
    items_received: int
    failure_stage: str = ""
    failure_code: str = ""
    http_status: int | None = None
    retry_after_sec: int | None = None
    provider_message: str = ""
    retryable: bool = False
    action_hint: str = ""
    items: list[dict[str, str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
