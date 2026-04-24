from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.config import ROOT_DIR
from app.core.db import Database, normalize_text_content
from app.models.planner_compaction_run import PlannerCompactionRun
from app.models.planner_feedback_event import PlannerFeedbackEvent
from app.models.planner_memory import (
    PlannerFeedbackMemoryItem,
    PlannerLongMemory,
    PlannerShortMemory,
    PlannerTheme,
)
from app.services.theme_detector import ThemeDetector


PLANNER_LONG_KEY = "planner_long_memory"
CORE_SUB_SECTORS = [
    "Agent runtime security",
    "Prompt injection defense",
    "LLM red teaming automation",
    "AI data leakage prevention",
]
VALID_PLANNER_FEEDBACK_TYPES = {
    "prefer_topic",
    "pause_source",
    "resume_source",
    "boost_signal",
    "deprioritize_pattern",
    "promote_theme",
    "reject_theme",
}


class PlannerMemoryManager:
    def __init__(self, db: Database, daily_strategy_file: str, *, lookback_days: int = 7):
        self.db = db
        self.daily_strategy_file = daily_strategy_file
        self.lookback_days = lookback_days
        self.theme_detector = ThemeDetector()

    def bootstrap_long_memory(self) -> dict[str, Any]:
        existing = self.db.get_long_memory(PLANNER_LONG_KEY)
        if existing:
            normalized = self._normalize_long_memory(existing)
            self.db.set_long_memory(PLANNER_LONG_KEY, normalized)
            return normalized

        memory = PlannerLongMemory(
            sub_sectors=list(CORE_SUB_SECTORS),
            signal_dictionary=[
                "enterprise customer",
                "B2B",
                "paid pilot",
                "ARR",
                "SOC2",
                "compliance",
                "financial customer",
                "commercialization",
            ],
            negative_filters=[
                "google",
                "microsoft",
                "amazon",
                "meta",
                "openai",
                "tencent",
                "alibaba",
                "bytedance",
            ],
            source_policy={
                "channel_status": {"brave": True, "bocha": True},
                "preferred_sources": ["brave", "bocha"],
                "paused_sources": [],
                "boosted_signals": [],
                "deprioritized_patterns": [],
                "promoted_themes": [],
                "rejected_themes": [],
            },
            human_preferences=[],
            version=1,
            updated_at=datetime.now(UTC).isoformat(),
            thesis_weights={
                "team": 0.25,
                "product": 0.25,
                "traction": 0.30,
                "market": 0.20,
            },
        ).to_dict()
        self.db.set_long_memory(PLANNER_LONG_KEY, memory)
        return memory

    def get_long_memory(self) -> dict[str, Any]:
        return self._normalize_long_memory(self.db.get_long_memory(PLANNER_LONG_KEY) or self.bootstrap_long_memory())

    def refresh_short_memory(self) -> dict[str, Any]:
        long_memory = self.apply_feedback_to_long_memory()
        strategy_file = Path(self.daily_strategy_file)
        if not strategy_file.is_absolute():
            strategy_file = ROOT_DIR / strategy_file
        today = datetime.now().date().isoformat()
        content = strategy_file.read_text(encoding="utf-8").strip() if strategy_file.exists() else (
            "Today prioritize agent security, enterprise deployment, and early commercial signals."
        )

        previous = self.db.get_latest_short_memory() or {}
        previous_days_active = previous.get("days_active", {}) if isinstance(previous, dict) else {}
        recent_context = self._load_recent_context(days=self.lookback_days)
        recent_strategies = recent_context["strategies"]
        if not any(str(item.get("memory_date") or "") == today for item in recent_strategies):
            recent_strategies = [
                {
                    "memory_date": today,
                    "created_at": datetime.now(UTC).isoformat(),
                    "strategy_value": {"daily_strategy": content},
                },
                *recent_strategies,
            ]

        detected = self.theme_detector.detect(
            long_memory=long_memory,
            recent_strategies=recent_strategies,
            recent_messages=recent_context["messages"],
            recent_signals=recent_context["signals"],
            recent_feedback=recent_context["feedback"],
            days=self.lookback_days,
        )

        source_policy = dict(long_memory.get("source_policy", {}))
        preferred_sources = list(source_policy.get("preferred_sources") or ["brave", "bocha"])
        rejected_themes = {str(x).strip().lower() for x in source_policy.get("rejected_themes", [])}
        explicit_theme_feedback = self._active_explicit_theme_feedback()

        filtered: list[dict[str, Any]] = []
        for item in detected:
            theme_name = str(item["theme"]).strip()
            if theme_name.lower() in rejected_themes:
                continue
            suggestions = [src for src in item["source_suggestions"] if src in preferred_sources] or list(preferred_sources)
            enriched = dict(item)
            enriched["source_suggestions"] = suggestions
            filtered.append(enriched)

        for theme_name, feedback_type in explicit_theme_feedback.items():
            if theme_name.lower() in rejected_themes:
                continue
            if any(str(item.get("theme") or "").strip().lower() == theme_name.lower() for item in filtered):
                continue
            synthetic = {
                "theme": theme_name,
                "priority": 5 if feedback_type == "promote_theme" else 4,
                "keywords": [theme_name],
                "source_suggestions": list(preferred_sources),
                "recency_score": 3.0,
                "source_diversity_score": 1.0,
                "commercial_signal_score": 1.0,
                "human_preference_score": 5.0,
                "new_theme_score": 5.0 if theme_name.lower() not in {s.lower() for s in long_memory.get("sub_sectors", [])} else 2.0,
                "promote_candidate": feedback_type == "promote_theme",
                "promotion_reason": "explicit human feedback",
                "evidence_summary": [f"human feedback: {feedback_type}"],
            }
            filtered.append(synthetic)

        themes: list[dict[str, Any]] = []
        days_active_map: dict[str, int] = {}
        source_suggestions: dict[str, list[str]] = {}
        promote_candidates: list[str] = []
        ordered_keywords: list[str] = []

        filtered.sort(
            key=lambda item: (
                -int(bool(item.get("promote_candidate"))),
                -int(item.get("priority", 0)),
                str(item.get("theme") or ""),
            )
        )

        for item in filtered:
            theme_name = str(item["theme"]).strip()
            days_active = int(previous_days_active.get(theme_name, 0) or 0) + 1
            theme = PlannerTheme(
                theme=theme_name,
                priority=int(item["priority"]),
                keywords=list(item["keywords"]),
                source_suggestions=list(item["source_suggestions"]),
                days_active=days_active,
                promote_candidate=bool(item["promote_candidate"]),
                recency_score=float(item["recency_score"]),
                source_diversity_score=float(item["source_diversity_score"]),
                commercial_signal_score=float(item["commercial_signal_score"]),
                human_preference_score=float(item["human_preference_score"]),
                new_theme_score=float(item["new_theme_score"]),
                promotion_reason=str(item["promotion_reason"]),
                evidence_summary=list(item["evidence_summary"]),
            )
            theme_dict = theme.to_dict()
            themes.append(theme_dict)
            days_active_map[theme_name] = days_active
            source_suggestions[theme_name] = list(theme.source_suggestions)
            ordered_keywords.extend(theme.keywords)
            if theme.promote_candidate:
                promote_candidates.append(theme_name)

        boosted_signals = list(source_policy.get("boosted_signals", []))
        ordered_keywords.extend(boosted_signals)

        short_memory = PlannerShortMemory(
            today=today,
            daily_strategy=content,
            emerging_themes=themes,
            priority=[item["theme"] for item in sorted(themes, key=lambda x: (-int(x["priority"]), x["theme"]))],
            keywords=self._dedupe(ordered_keywords, limit=30),
            source_suggestions=source_suggestions,
            days_active=days_active_map,
            promote_candidate=promote_candidates,
            query_boost_terms=self._dedupe(
                [item["theme"] for item in themes[:8]] + boosted_signals,
                limit=12,
            ),
        ).to_dict()
        self.db.set_short_memory(today, short_memory)
        return short_memory

    def get_short_memory(self) -> dict[str, Any]:
        return self.db.get_latest_short_memory() or self.refresh_short_memory()

    def ingest_feedback(self, feedback_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ingested: list[dict[str, Any]] = []
        for fb in feedback_rows:
            fb = dict(fb)
            feedback_id = int(fb.get("id") or 0)
            if feedback_id and self.db.has_planner_feedback_event_source(feedback_id):
                continue

            event = self._build_feedback_event(fb)
            if not event:
                continue
            event_dict = event.to_dict()
            event_id = self.db.add_planner_feedback_event(event_dict)
            event_dict["id"] = event_id
            ingested.append(event_dict)
        return ingested

    def apply_feedback_to_long_memory(self) -> dict[str, Any]:
        memory = self.get_long_memory()
        events = self.db.list_planner_feedback_events(limit=500)
        if not events:
            return memory

        events_sorted = sorted(
            [dict(item) for item in events],
            key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)),
        )
        latest_conflict: dict[tuple[str, str], int] = {}
        for item in events_sorted:
            conflict_key = self._feedback_conflict_key(item)
            latest_conflict[conflict_key] = int(item.get("id") or 0)

        sub_sectors = list(memory.get("sub_sectors", []))
        human_preferences = list(memory.get("human_preferences", []))
        signal_dictionary = list(memory.get("signal_dictionary", []))
        negative_filters = list(memory.get("negative_filters", []))
        source_policy = dict(memory.get("source_policy", {}))
        channel_status = dict(source_policy.get("channel_status", {"brave": True, "bocha": True}))
        paused_sources = set(source_policy.get("paused_sources", []))
        boosted_signals = list(source_policy.get("boosted_signals", []))
        deprioritized_patterns = list(source_policy.get("deprioritized_patterns", []))
        promoted_themes = list(source_policy.get("promoted_themes", []))
        rejected_themes = list(source_policy.get("rejected_themes", []))

        feedback_snapshot: list[dict[str, Any]] = []
        for item in events_sorted:
            event_id = int(item.get("id") or 0)
            feedback_type = str(item.get("feedback_type") or "").strip().lower()
            value = normalize_text_content(item.get("value") or "").strip()
            conflict_key = self._feedback_conflict_key(item)
            status = "active" if latest_conflict.get(conflict_key) == event_id else "archived"

            if status == "active":
                if feedback_type == "prefer_topic":
                    human_preferences.append(value)
                elif feedback_type == "pause_source":
                    channel_status[value] = False
                    paused_sources.add(value)
                elif feedback_type == "resume_source":
                    channel_status[value] = True
                    paused_sources.discard(value)
                elif feedback_type == "boost_signal":
                    signal_dictionary.append(value)
                    boosted_signals.append(value)
                elif feedback_type == "deprioritize_pattern":
                    negative_filters.append(value)
                    deprioritized_patterns.append(value)
                elif feedback_type == "promote_theme":
                    sub_sectors.append(value)
                    human_preferences.append(value)
                    promoted_themes.append(value)
                    if value.lower() in {x.lower() for x in rejected_themes}:
                        rejected_themes = [x for x in rejected_themes if x.lower() != value.lower()]
                elif feedback_type == "reject_theme":
                    rejected_themes.append(value)
                    sub_sectors = [x for x in sub_sectors if x.lower() != value.lower()]
                    human_preferences = [x for x in human_preferences if x.lower() != value.lower()]
                    promoted_themes = [x for x in promoted_themes if x.lower() != value.lower()]

            merge_summary = self._merge_summary(feedback_type, value, status)
            feedback_snapshot.append(
                PlannerFeedbackMemoryItem(
                    feedback_type=feedback_type,
                    target=str(item.get("target") or ""),
                    value=value,
                    status=status,
                    source_feedback_id=item.get("source_feedback_id"),
                    created_at=str(item.get("created_at") or ""),
                    updated_at=datetime.now(UTC).isoformat(),
                ).to_dict()
            )
            self.db.mark_planner_feedback_event_merged(event_id, merge_summary=merge_summary)

        channel_status = {name: bool(enabled) for name, enabled in channel_status.items()}
        preferred_sources = [name for name, enabled in channel_status.items() if enabled]
        source_policy["channel_status"] = channel_status
        source_policy["preferred_sources"] = preferred_sources
        source_policy["paused_sources"] = self._dedupe(list(paused_sources), limit=20)
        source_policy["boosted_signals"] = self._dedupe(boosted_signals, limit=50)
        source_policy["deprioritized_patterns"] = self._dedupe(deprioritized_patterns, limit=50)
        source_policy["promoted_themes"] = self._dedupe(promoted_themes, limit=50)
        source_policy["rejected_themes"] = self._dedupe(rejected_themes, limit=50)

        memory["sub_sectors"] = self._dedupe(sub_sectors, limit=50)
        memory["focus_sectors"] = list(memory["sub_sectors"])
        memory["human_preferences"] = self._dedupe(human_preferences, limit=50)
        memory["signal_dictionary"] = self._dedupe(signal_dictionary, limit=80)
        memory["sensitive_keywords"] = list(memory["signal_dictionary"])
        memory["negative_filters"] = self._dedupe(negative_filters, limit=80)
        memory["source_policy"] = source_policy
        memory["channel_status"] = channel_status
        memory["updated_at"] = datetime.now(UTC).isoformat()

        self.db.replace_planner_feedback_memory(feedback_snapshot)
        self.db.set_long_memory(PLANNER_LONG_KEY, memory)
        return memory

    def compact_memories(self) -> dict[str, Any]:
        long_memory = self.apply_feedback_to_long_memory()
        short_memory = self.get_short_memory()
        feedback_items = self.db.list_planner_feedback_memory(limit=500)
        source_policy = dict(long_memory.get("source_policy", {}))

        sub_sectors = list(long_memory.get("sub_sectors", []))
        current_themes = {
            str(item.get("theme") or "").strip()
            for item in short_memory.get("emerging_themes", [])
            if str(item.get("theme") or "").strip()
        }
        protected = {item.lower() for item in CORE_SUB_SECTORS}
        protected |= {item.lower() for item in long_memory.get("human_preferences", [])}
        protected |= {item.lower() for item in source_policy.get("promoted_themes", [])}
        protected |= {item.lower() for item in source_policy.get("rejected_themes", [])}

        promoted_themes: list[str] = []
        merged_topics: list[dict[str, Any]] = []
        for theme in short_memory.get("emerging_themes", []):
            theme_name = str(theme.get("theme") or "").strip()
            if not theme_name:
                continue
            existing = next((s for s in sub_sectors if s.lower() == theme_name.lower()), None)
            if theme.get("promote_candidate") and not existing:
                sub_sectors.append(theme_name)
                promoted_themes.append(theme_name)
            elif existing:
                merged_topics.append(
                    {
                        "theme": theme_name,
                        "merged_into": existing,
                        "reason": str(theme.get("promotion_reason") or "duplicate normalized topic"),
                    }
                )

        explicit_promoted = [item["value"] for item in feedback_items if item["feedback_type"] == "promote_theme" and item["status"] == "active"]
        for theme_name in explicit_promoted:
            if theme_name.lower() not in {s.lower() for s in sub_sectors}:
                sub_sectors.append(theme_name)
            promoted_themes.append(theme_name)

        decayed_themes: list[str] = []
        retained_sub_sectors: list[str] = []
        for theme_name in sub_sectors:
            if (
                theme_name.lower() not in protected
                and theme_name not in current_themes
                and theme_name not in promoted_themes
            ):
                decayed_themes.append(theme_name)
                continue
            retained_sub_sectors.append(theme_name)

        for theme_name in source_policy.get("rejected_themes", []):
            if theme_name and theme_name.lower() not in {item.lower() for item in decayed_themes}:
                decayed_themes.append(theme_name)

        archived_preferences = [
            item["value"]
            for item in feedback_items
            if item["status"] == "archived"
        ]

        source_policy_changes = [
            {
                "source": source_name,
                "enabled": bool(enabled),
            }
            for source_name, enabled in dict(source_policy.get("channel_status", {})).items()
        ]

        long_memory["sub_sectors"] = self._dedupe(retained_sub_sectors, limit=50)
        long_memory["focus_sectors"] = list(long_memory["sub_sectors"])
        long_memory["signal_dictionary"] = self._dedupe(long_memory.get("signal_dictionary", []), limit=80)
        long_memory["sensitive_keywords"] = list(long_memory["signal_dictionary"])
        long_memory["human_preferences"] = self._dedupe(long_memory.get("human_preferences", []), limit=50)
        long_memory["negative_filters"] = self._dedupe(long_memory.get("negative_filters", []), limit=80)
        long_memory["updated_at"] = datetime.now(UTC).isoformat()
        self.db.set_long_memory(PLANNER_LONG_KEY, long_memory)

        run = PlannerCompactionRun(
            promoted_themes=self._dedupe(promoted_themes, limit=50),
            decayed_themes=self._dedupe(decayed_themes, limit=50),
            merged_topics=merged_topics,
            archived_preferences=self._dedupe(archived_preferences, limit=100),
            source_policy_changes=source_policy_changes,
            summary=(
                f"promoted={len(promoted_themes)}, decayed={len(decayed_themes)}, "
                f"merged={len(merged_topics)}, archived={len(archived_preferences)}"
            ),
            created_at=datetime.now(UTC).isoformat(),
        ).to_dict()
        self.db.add_planner_compaction(run)
        self.db.add_planner_compaction_run(run)
        return run

    def memory_snapshot(self) -> dict[str, Any]:
        return {
            "long_memory": self.get_long_memory(),
            "short_memory": self.get_short_memory(),
            "feedback_memory": self.db.list_planner_feedback_memory(limit=500),
            "feedback_events": self.db.list_planner_feedback_events(limit=500),
            "compaction": self.db.get_latest_planner_compaction_run() or self.db.get_latest_planner_compaction() or {},
        }

    def build_search_plan(self) -> dict[str, Any]:
        long_memory = self.apply_feedback_to_long_memory()
        short_memory = self.refresh_short_memory()

        base_queries: list[str] = []
        rejected_themes = {str(x).strip().lower() for x in long_memory.get("source_policy", {}).get("rejected_themes", [])}

        for sector in long_memory.get("sub_sectors", []):
            if sector.lower() in rejected_themes:
                continue
            base_queries.extend(
                [
                    f"{sector} startup funding",
                    f"{sector} enterprise customers",
                ]
            )

        for theme in short_memory.get("priority", []):
            if str(theme).strip().lower() in rejected_themes:
                continue
            base_queries.append(f"AI security startup {theme}")

        for term in short_memory.get("query_boost_terms", []):
            base_queries.append(f"AI security startup {term}")

        for preference in long_memory.get("human_preferences", []):
            if preference.lower() in rejected_themes:
                continue
            base_queries.append(f"{preference} AI startup")

        deduped: list[str] = []
        seen: set[str] = set()
        for query in base_queries:
            key = query.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(query)

        source_policy = long_memory.get("source_policy", {})
        channel_status = dict(source_policy.get("channel_status", {"brave": True, "bocha": True}))
        preferred_sources = list(source_policy.get("preferred_sources") or [name for name, enabled in channel_status.items() if enabled])
        source_suggestions = {
            theme: [src for src in suggestions if src in preferred_sources] or list(preferred_sources)
            for theme, suggestions in dict(short_memory.get("source_suggestions", {})).items()
            if theme.lower() not in rejected_themes
        }

        return {
            "queries": deduped[:16],
            "channel_status": channel_status,
            "negative_filters": self._dedupe(
                list(long_memory.get("negative_filters", [])) + list(source_policy.get("deprioritized_patterns", [])),
                limit=80,
            ),
            "sensitive_keywords": long_memory.get("signal_dictionary", []),
            "human_preferences": long_memory.get("human_preferences", []),
            "source_suggestions": source_suggestions,
        }

    def _load_recent_context(self, *, days: int) -> dict[str, Any]:
        return {
            "strategies": self.db.list_recent_short_memories(days=days, limit=14),
            "messages": self.db.list_recent_conversation_messages(days=days, limit=200, direction="inbound"),
            "signals": self.db.list_recent_signals(days=days, limit=300),
            "feedback": self.db.list_planner_feedback_memory(limit=500),
        }

    def _normalize_long_memory(self, memory: dict[str, Any]) -> dict[str, Any]:
        payload = dict(memory or {})
        sub_sectors = payload.get("sub_sectors") or payload.get("focus_sectors") or []
        signal_dictionary = payload.get("signal_dictionary") or payload.get("sensitive_keywords") or []
        source_policy = dict(payload.get("source_policy") or {})
        channel_status = dict(source_policy.get("channel_status") or payload.get("channel_status") or {"brave": True, "bocha": True})
        source_policy.setdefault("channel_status", channel_status)
        source_policy.setdefault("preferred_sources", [name for name, enabled in channel_status.items() if enabled])
        source_policy.setdefault("paused_sources", [name for name, enabled in channel_status.items() if not enabled])
        source_policy.setdefault("boosted_signals", [])
        source_policy.setdefault("deprioritized_patterns", [])
        source_policy.setdefault("promoted_themes", [])
        source_policy.setdefault("rejected_themes", [])
        return PlannerLongMemory(
            sub_sectors=self._dedupe(sub_sectors, limit=50),
            signal_dictionary=self._dedupe(signal_dictionary, limit=80),
            negative_filters=self._dedupe(payload.get("negative_filters", []), limit=80),
            source_policy=source_policy,
            human_preferences=self._dedupe(payload.get("human_preferences", []), limit=50),
            version=int(payload.get("version") or 1),
            updated_at=str(payload.get("updated_at") or ""),
            thesis_weights=dict(payload.get("thesis_weights", {})),
        ).to_dict()

    def _build_feedback_event(self, row: dict[str, Any]) -> PlannerFeedbackEvent | None:
        content = normalize_text_content(row.get("content") or "").strip()
        if not content:
            return None
        feedback_type = normalize_text_content(row.get("feedback_type") or "").strip().lower()
        verdict = normalize_text_content(row.get("verdict") or "").strip().lower()
        source_feedback_id = int(row.get("id") or 0) or None

        if feedback_type in VALID_PLANNER_FEEDBACK_TYPES:
            target, value = self._parse_explicit_feedback_value(feedback_type, content)
        else:
            target, value = self._parse_feedback_target_value(content=content, verdict=verdict)
            feedback_type = self._coerce_legacy_feedback_type(target=target, verdict=verdict, content=content)

        if not value:
            return None

        return PlannerFeedbackEvent(
            feedback_type=feedback_type,
            target=target,
            value=value[:160],
            status="active",
            source_feedback_id=source_feedback_id,
            merged=False,
            merge_summary="",
            metadata={"verdict": verdict, "original_content": content},
            created_at=str(row.get("created_at") or ""),
            updated_at=datetime.now(UTC).isoformat(),
        )

    @staticmethod
    def _parse_explicit_feedback_value(feedback_type: str, content: str) -> tuple[str, str]:
        lowered = content.lower().strip()
        if feedback_type in {"pause_source", "resume_source"}:
            for source_name in ("brave", "bocha"):
                if source_name in lowered:
                    return "source_policy", source_name
            return "source_policy", lowered.split()[-1] if lowered.split() else ""
        if feedback_type == "boost_signal":
            return "signal_dictionary", content
        if feedback_type == "deprioritize_pattern":
            return "negative_filters", content
        if feedback_type == "promote_theme":
            return "promote_theme", PlannerMemoryManager._extract_topic_phrase(content)
        if feedback_type == "reject_theme":
            return "reject_theme", PlannerMemoryManager._extract_topic_phrase(content)
        return "sub_sectors", PlannerMemoryManager._extract_topic_phrase(content)

    @staticmethod
    def _coerce_legacy_feedback_type(*, target: str, verdict: str, content: str) -> str:
        lower = content.lower()
        if target == "source_policy":
            if "pause" in lower or "暂停" in content:
                return "pause_source"
            return "resume_source"
        if target == "sub_sectors":
            return "prefer_topic"
        if target == "negative_filters":
            return "deprioritize_pattern"
        if verdict == "prefer_sector":
            return "prefer_topic"
        return "prefer_topic"

    @staticmethod
    def _parse_feedback_target_value(*, content: str, verdict: str) -> tuple[str, str]:
        lower = content.lower()
        if "pause bocha" in lower or "暂停bocha" in content or "resume bocha" in lower or "恢复bocha" in content:
            return "source_policy", "bocha"
        if "pause brave" in lower or "暂停brave" in content or "resume brave" in lower or "恢复brave" in content:
            return "source_policy", "brave"
        if "大公司" in content or "big company" in lower:
            return "negative_filters", content[:120]
        if "关注" in content or "focus" in lower or verdict == "prefer_sector":
            return "sub_sectors", PlannerMemoryManager._extract_topic_phrase(content)[:120]
        return "sub_sectors", PlannerMemoryManager._extract_topic_phrase(content)[:120]

    @staticmethod
    def _extract_topic_phrase(content: str) -> str:
        text = normalize_text_content(content).strip()
        patterns = [
            r"(?:更关注|关注|多关注)\s*(.+?)(?:方向|赛道|主题)?$",
            r"(?:promote|reject|prefer)\s+(.+?)(?:\s+theme|\s+topic|\s+sector)?$",
            r"(?:theme|topic|sector)\s*[:：]?\s*(.+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                candidate = match.group(1).strip(" ：:，,。.!? ")
                if candidate:
                    return candidate
        return text

    def _active_explicit_theme_feedback(self) -> dict[str, str]:
        feedback_items = self.db.list_planner_feedback_memory(status="active", limit=500)
        latest: dict[str, str] = {}
        for item in reversed(feedback_items):
            feedback_type = str(item.get("feedback_type") or "").lower()
            if feedback_type not in {"prefer_topic", "promote_theme"}:
                continue
            value = str(item.get("value") or "").strip()
            if value:
                latest[value] = feedback_type
        return latest

    @staticmethod
    def _feedback_conflict_key(item: dict[str, Any]) -> tuple[str, str]:
        feedback_type = str(item.get("feedback_type") or "").lower()
        value = str(item.get("value") or "").strip().lower()
        if feedback_type in {"pause_source", "resume_source"}:
            return ("source_policy", value)
        if feedback_type in {"prefer_topic", "promote_theme", "reject_theme"}:
            return ("theme", value)
        if feedback_type == "boost_signal":
            return ("signal", value)
        if feedback_type == "deprioritize_pattern":
            return ("pattern", value)
        return (feedback_type, value)

    @staticmethod
    def _merge_summary(feedback_type: str, value: str, status: str) -> str:
        if status != "active":
            return f"{feedback_type} archived due to newer conflicting feedback"
        return f"{feedback_type} applied to {value}"

    @staticmethod
    def _dedupe(values: list[Any], *, limit: int) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            item = normalize_text_content(value).strip()
            key = item.lower()
            if not item or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped[:limit]
