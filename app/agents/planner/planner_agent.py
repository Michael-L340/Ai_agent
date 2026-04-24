from __future__ import annotations

import json
from typing import Any

from app.core.config import Settings
from app.core.db import Database
from app.services.planner_memory_manager import PlannerMemoryManager, PLANNER_LONG_KEY


class PlannerAgent:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self.memory = PlannerMemoryManager(db, settings.daily_strategy_file)

    def bootstrap_if_needed(self) -> dict[str, Any]:
        return self.memory.bootstrap_long_memory()

    def refresh_short_memory(self) -> dict[str, Any]:
        return self.memory.refresh_short_memory()

    def apply_feedback_learning(self) -> dict[str, Any]:
        feedbacks = self.db.list_recent_feedback(limit=100)
        self.memory.ingest_feedback(feedbacks)
        return self.memory.apply_feedback_to_long_memory()

    def get_search_plan(self) -> dict[str, Any]:
        self.bootstrap_if_needed()
        self.db.get_long_memory(PLANNER_LONG_KEY)  # ensure compatibility path warmed
        self.apply_feedback_learning()
        return self.memory.build_search_plan()

    def update_channel_status(self, channel: str, enabled: bool) -> dict[str, Any]:
        memory = self.memory.get_long_memory()
        channel_status = dict(memory.get("channel_status", {"brave": True, "bocha": True}))
        if channel not in channel_status:
            raise ValueError(f"Unknown channel: {channel}")

        channel_status[channel] = enabled
        memory["channel_status"] = channel_status
        source_policy = dict(memory.get("source_policy", {}))
        source_policy["channel_status"] = channel_status
        source_policy["preferred_sources"] = [name for name, is_enabled in channel_status.items() if is_enabled]
        memory["source_policy"] = source_policy
        self.db.set_long_memory(PLANNER_LONG_KEY, memory)
        return memory

    def compress_long_memory(self) -> dict[str, Any]:
        memory = self.memory.get_long_memory()
        compaction = self.memory.compact_memories()
        memory["compaction_result"] = compaction
        return memory

    def get_memory_snapshot(self) -> dict[str, Any]:
        self.bootstrap_if_needed()
        return self.memory.memory_snapshot()

    @staticmethod
    def _extract_boost_terms(content: str) -> list[str]:
        # If daily report is JSON, read explicit strategy terms first.
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict) and isinstance(parsed.get("boost_terms"), list):
                return [str(x).strip() for x in parsed["boost_terms"] if str(x).strip()][:8]
        except Exception:
            pass

        candidates = []
        for token in ["保险", "金融", "医疗", "agent", "compliance", "B2B", "客户", "营收"]:
            if token.lower() in content.lower():
                candidates.append(token)
        return candidates[:8]

