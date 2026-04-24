from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CommandRequest(BaseModel):
    command: str = Field(..., description="run_cycle / refresh_strategy / update_channel")
    data: dict[str, Any] = Field(default_factory=dict)


class FeedbackRequest(BaseModel):
    lead_id: int | None = None
    verdict: str = Field(..., description="like / dislike / neutral")
    content: str = ""
    feedback_type: str = "lead_feedback"


class ChatRequest(BaseModel):
    message: str = Field(..., description="Natural language text from human.")


class OpenClawInboundRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str | None = None
    action: str | None = None
    message: str | None = None
    text: str | None = None
    content: str | None = None
    session_key: str | None = Field(default=None, alias="sessionKey")
    channel_id: str | None = Field(default=None, alias="channelId")
    sender: str | None = None
    from_: str | None = Field(default=None, alias="from")
    metadata: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)


class OpenClawOutboxAckRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    event_ids: list[int] = Field(default_factory=list, alias="eventIds")


class LeadRecommendation(BaseModel):
    lead_id: int
    company_name: str
    final_score: float
    summary: str
    reasons: str
    sources: str


class RunCycleResult(BaseModel):
    run_at: datetime
    searched_items: int
    new_leads: int
    dd_done: int
    scored: int
    recommended: int


class SearchItem(BaseModel):
    source: str
    query: str
    title: str
    snippet: str
    url: str


class LeadRecord(BaseModel):
    id: int
    company_name: str
    status: str
    sources: str
    description: str
