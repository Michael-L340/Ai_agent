from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.core.db import normalize_text_content


COMMERCIAL_MARKERS = (
    "enterprise customer",
    "enterprise customers",
    "b2b",
    "paid pilot",
    "arr",
    "funding",
    "seed",
    "series a",
    "customer",
    "customers",
    "partner",
    "partners",
    "launch",
    "deployment",
    "production deployment",
    "commercialization",
    "付费",
    "客户",
    "融资",
    "落地",
)


@dataclass(frozen=True)
class ThemeRule:
    theme: str
    keywords: tuple[str, ...]
    source_suggestions: tuple[str, ...]


class ThemeDetector:
    RULES: tuple[ThemeRule, ...] = (
        ThemeRule(
            theme="agent security",
            keywords=("agent security", "agent runtime security", "runtime protection", "runtime monitoring"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="prompt injection defense",
            keywords=("prompt injection", "prompt attacks", "guardrail", "guardrails"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="llm red teaming",
            keywords=("llm red teaming", "red teaming", "red team"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="insurance ai security",
            keywords=("insurance", "insurtech", "保险"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="financial services ai security",
            keywords=("financial services", "banking", "fintech", "金融"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="healthcare ai security",
            keywords=("healthcare", "medical", "医院", "医疗"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="ai compliance automation",
            keywords=("compliance", "soc2", "governance", "审计", "合规"),
            source_suggestions=("brave", "bocha"),
        ),
        ThemeRule(
            theme="enterprise deployment",
            keywords=("enterprise", "deployment", "production", "pilot", "客户"),
            source_suggestions=("brave", "bocha"),
        ),
    )

    def detect(
        self,
        *,
        long_memory: dict[str, Any],
        recent_strategies: list[dict[str, Any]],
        recent_messages: list[dict[str, Any]],
        recent_signals: list[dict[str, Any]],
        recent_feedback: list[dict[str, Any]],
        days: int = 7,
    ) -> list[dict[str, Any]]:
        occurrences = self._collect_occurrences(
            recent_strategies=recent_strategies,
            recent_messages=recent_messages,
            recent_signals=recent_signals,
            recent_feedback=recent_feedback,
        )
        existing_themes = {str(theme).strip().lower() for theme in long_memory.get("sub_sectors", [])}
        now = datetime.now(UTC)

        detected: list[dict[str, Any]] = []
        for rule in self.RULES:
            matched = [item for item in occurrences if any(keyword in item["text"] for keyword in rule.keywords)]
            if not matched:
                continue

            source_types = {item["source_type"] for item in matched}
            latest_seen = max(item["seen_at"] for item in matched if item["seen_at"] is not None)
            commercial_hits = sum(self._commercial_hits(item["text"]) for item in matched)
            human_hits = sum(1 for item in matched if item["source_type"] in {"feedback", "messages"})

            recency_score = self._recency_score(now=now, latest_seen=latest_seen, max_days=max(days, 1))
            source_diversity_score = min(5.0, float(len(source_types)))
            commercial_signal_score = min(5.0, float(commercial_hits))
            human_preference_score = min(5.0, float(human_hits * 2))
            new_theme_score = self._new_theme_score(rule.theme, existing_themes)

            weighted = (
                (recency_score * 0.25)
                + (source_diversity_score * 0.20)
                + (commercial_signal_score * 0.25)
                + (human_preference_score * 0.15)
                + (new_theme_score * 0.15)
            )
            priority = max(1, min(5, int(round(weighted))))
            promote_candidate = weighted >= 3.6 and source_diversity_score >= 2.0 and new_theme_score >= 3.0

            suggestions = sorted(set(rule.source_suggestions) | {item["source_name"] for item in matched if item["source_name"]})
            evidence_summary = []
            for item in matched[:5]:
                summary = item["summary"]
                if summary and summary not in evidence_summary:
                    evidence_summary.append(summary)

            detected.append(
                {
                    "theme": rule.theme,
                    "priority": priority,
                    "keywords": list(dict.fromkeys(rule.keywords)),
                    "source_suggestions": suggestions or list(rule.source_suggestions),
                    "recency_score": round(recency_score, 2),
                    "source_diversity_score": round(source_diversity_score, 2),
                    "commercial_signal_score": round(commercial_signal_score, 2),
                    "human_preference_score": round(human_preference_score, 2),
                    "new_theme_score": round(new_theme_score, 2),
                    "promote_candidate": promote_candidate,
                    "promotion_reason": self._promotion_reason(
                        theme=rule.theme,
                        promote_candidate=promote_candidate,
                        source_types=source_types,
                        weighted_score=weighted,
                        commercial_signal_score=commercial_signal_score,
                    ),
                    "evidence_summary": evidence_summary,
                }
            )

        detected.sort(
            key=lambda item: (
                -float(item["promote_candidate"]),
                -float(item["priority"]),
                -float(item["commercial_signal_score"]),
                item["theme"],
            )
        )
        return detected

    def _collect_occurrences(
        self,
        *,
        recent_strategies: list[dict[str, Any]],
        recent_messages: list[dict[str, Any]],
        recent_signals: list[dict[str, Any]],
        recent_feedback: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []

        for row in recent_strategies:
            strategy = row.get("strategy_value") or {}
            text = normalize_text_content(
                " ".join(
                    [
                        str(strategy.get("daily_strategy") or ""),
                        " ".join(str(x) for x in strategy.get("keywords", []) if str(x).strip()),
                        " ".join(str(x) for x in strategy.get("priority", []) if str(x).strip()),
                    ]
                )
            ).lower()
            if text:
                items.append(
                    {
                        "source_type": "strategy",
                        "source_name": "daily_strategy",
                        "text": text,
                        "summary": str(strategy.get("daily_strategy") or ""),
                        "seen_at": self._coerce_datetime(row.get("created_at")) or self._coerce_date(row.get("memory_date")),
                    }
                )

        for row in recent_messages:
            content = normalize_text_content(row.get("content") or "")
            if content:
                items.append(
                    {
                        "source_type": "messages",
                        "source_name": str(row.get("source") or "messages"),
                        "text": content.lower(),
                        "summary": content,
                        "seen_at": self._coerce_datetime(row.get("created_at")),
                    }
                )

        for row in recent_signals:
            title = normalize_text_content(row.get("title") or "")
            snippet = normalize_text_content(row.get("snippet") or "")
            query = normalize_text_content(row.get("query") or "")
            combined = " ".join(part for part in [title, snippet, query] if part).strip()
            if combined:
                items.append(
                    {
                        "source_type": "signals",
                        "source_name": str(row.get("source") or "signals"),
                        "text": combined.lower(),
                        "summary": title or snippet or query,
                        "seen_at": self._coerce_datetime(row.get("fetched_at")),
                    }
                )

        for row in recent_feedback:
            value = normalize_text_content(row.get("value") or row.get("content") or "")
            if value:
                items.append(
                    {
                        "source_type": "feedback",
                        "source_name": str(row.get("feedback_type") or "feedback"),
                        "text": value.lower(),
                        "summary": value,
                        "seen_at": self._coerce_datetime(row.get("updated_at") or row.get("created_at")),
                    }
                )
        return items

    @staticmethod
    def _commercial_hits(text: str) -> int:
        lowered = str(text or "").lower()
        return sum(1 for marker in COMMERCIAL_MARKERS if marker in lowered)

    @staticmethod
    def _new_theme_score(theme: str, existing_themes: set[str]) -> float:
        return 1.0 if theme.lower() in existing_themes else 5.0

    @staticmethod
    def _recency_score(*, now: datetime, latest_seen: datetime | None, max_days: int) -> float:
        if latest_seen is None:
            return 1.0
        delta_days = max(0.0, (now - latest_seen).total_seconds() / 86400.0)
        if delta_days <= 1:
            return 5.0
        if delta_days <= 2:
            return 4.0
        if delta_days <= 4:
            return 3.0
        if delta_days <= max_days:
            return 2.0
        return 1.0

    @staticmethod
    def _promotion_reason(
        *,
        theme: str,
        promote_candidate: bool,
        source_types: set[str],
        weighted_score: float,
        commercial_signal_score: float,
    ) -> str:
        if promote_candidate:
            return (
                f"{theme} is eligible for promotion because it appears across "
                f"{len(source_types)} source types, shows commercial signals ({commercial_signal_score:.1f}/5), "
                f"and reached weighted score {weighted_score:.2f}."
            )
        return (
            f"{theme} is still short-term because weighted score is {weighted_score:.2f} "
            f"or source diversity is not high enough."
        )

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except Exception:
            return None

    @staticmethod
    def _coerce_date(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text).replace(tzinfo=UTC)
        except Exception:
            return None
