from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.core.db import Database
from app.models.dd_feedback import DDFeedback
from app.models.dd_question import DDQuestion


DIMENSION_KEYWORDS: dict[str, tuple[str, ...]] = {
    "entity": ("公司名", "公司名字", "公司主体", "主体", "entity", "name", "命名", "归一", "识别"),
    "business": ("业务", "产品", "服务", "做什么", "business", "product", "service", "one-liner"),
    "team": ("团队", "创始人", "founder", "leadership", "team", "人物", "背景"),
    "funding": ("融资", "估值", "raised", "investor", "valuation", "funding", "资金"),
    "traction": ("客户", "合作", "收入", "部署", "落地", "增长", "traction", "revenue", "customer"),
    "market": ("竞争", "赛道", "行业地位", "market", "competition", "crowded", "市场"),
}

FIELD_KEYWORDS: dict[str, str] = {
    "one_liner": "business_profile",
    "products_services": "business_profile",
    "target_customers": "business_profile",
    "use_cases": "business_profile",
    "founders": "team_profile",
    "key_people": "team_profile",
    "prior_companies": "team_profile",
    "research_background": "team_profile",
    "founded_year": "funding_profile",
    "headquarters": "funding_profile",
    "funding_rounds": "funding_profile",
    "total_raised": "funding_profile",
    "valuation": "funding_profile",
    "notable_investors": "funding_profile",
    "customers": "traction_profile",
    "partners": "traction_profile",
    "product_launches": "traction_profile",
    "revenue_signals": "traction_profile",
    "deployment_signals": "traction_profile",
    "sub_sector": "market_position",
    "is_new_category": "market_position",
    "competitors": "market_position",
    "leader_signals": "market_position",
    "crowdedness": "market_position",
}

DIMENSION_ALIASES: dict[str, str] = {
    "entity": "entity",
    "business": "business_profile",
    "business_profile": "business_profile",
    "team": "team_profile",
    "team_profile": "team_profile",
    "funding": "funding_profile",
    "funding_profile": "funding_profile",
    "traction": "traction_profile",
    "traction_profile": "traction_profile",
    "market": "market_position",
    "market_position": "market_position",
}


@dataclass(slots=True)
class DDFeedbackContext:
    lead_id: int | None
    company_key: str
    normalized_name: str
    official_domain: str
    focus_dimensions: list[str] = field(default_factory=list)
    avoid_dimensions: list[str] = field(default_factory=list)
    focus_fields_by_dimension: dict[str, list[str]] = field(default_factory=dict)
    blocked_fields_by_dimension: dict[str, list[str]] = field(default_factory=dict)
    confirmed_entity_name: str = ""
    feedback_entries: list[dict[str, Any]] = field(default_factory=list)
    open_questions: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lead_id": self.lead_id,
            "company_key": self.company_key,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "focus_dimensions": list(self.focus_dimensions),
            "avoid_dimensions": list(self.avoid_dimensions),
            "focus_fields_by_dimension": self.focus_fields_by_dimension,
            "blocked_fields_by_dimension": self.blocked_fields_by_dimension,
            "confirmed_entity_name": self.confirmed_entity_name,
            "feedback_entries": self.feedback_entries,
            "open_questions": self.open_questions,
        }


class DDMemoryStore:
    def __init__(self, db: Database):
        self.db = db

    def build_context(self, lead: dict[str, Any]) -> DDFeedbackContext:
        lead_id = int(lead.get("id") or lead.get("lead_id") or 0) or None
        normalized_name = self._pick_text(lead.get("normalized_name"), lead.get("company_name"), lead.get("candidate_name"))
        company_key = self.db._company_key_from_name(
            normalized_name or self._pick_text(lead.get("company_name"), lead.get("candidate_name"))
        )
        official_domain = self._pick_text(lead.get("official_domain"))

        entries: list[dict[str, Any]] = []
        if lead_id is not None:
            entries.extend(self.db.list_dd_feedback_memory(lead_id=lead_id, limit=100))
        if company_key:
            entries.extend(self.db.list_dd_feedback_memory(company_key=company_key, limit=100))
        entries.extend(self.db.list_dd_feedback_memory(scope="global", limit=100))

        seen_ids: set[int] = set()
        unique_entries: list[dict[str, Any]] = []
        for row in entries:
            row_id = int(row.get("id") or 0)
            if row_id and row_id in seen_ids:
                continue
            if row_id:
                seen_ids.add(row_id)
            unique_entries.append(row)

        focus_dimensions: list[str] = []
        avoid_dimensions: list[str] = []
        focus_fields_by_dimension: dict[str, list[str]] = {}
        blocked_fields_by_dimension: dict[str, list[str]] = {}
        confirmed_entity_name = ""
        open_questions = self.db.list_dd_questions(
            lead_id=lead_id,
            company_key=company_key,
            status="open",
            limit=20,
        )

        for row in unique_entries:
            parsed = self._feedback_payload(row)
            for dimension in parsed.get("focus_dimensions", []):
                dimension = str(dimension).strip()
                if dimension and dimension not in focus_dimensions:
                    focus_dimensions.append(dimension)
            for dimension in parsed.get("avoid_dimensions", []):
                dimension = str(dimension).strip()
                if dimension and dimension not in avoid_dimensions:
                    avoid_dimensions.append(dimension)

            for dimension, fields in (parsed.get("focus_fields_by_dimension", {}) or {}).items():
                dimension = str(dimension).strip()
                if not dimension:
                    continue
                focus_fields_by_dimension.setdefault(dimension, [])
                for field_name in fields or []:
                    field_name = str(field_name).strip()
                    if field_name and field_name not in focus_fields_by_dimension[dimension]:
                        focus_fields_by_dimension[dimension].append(field_name)

            for dimension, fields in (parsed.get("blocked_fields_by_dimension", {}) or {}).items():
                dimension = str(dimension).strip()
                if not dimension:
                    continue
                blocked_fields_by_dimension.setdefault(dimension, [])
                for field_name in fields or []:
                    field_name = str(field_name).strip()
                    if field_name and field_name not in blocked_fields_by_dimension[dimension]:
                        blocked_fields_by_dimension[dimension].append(field_name)

            candidate_name = str(parsed.get("confirmed_entity_name") or "").strip()
            if candidate_name and not confirmed_entity_name:
                confirmed_entity_name = candidate_name

        return DDFeedbackContext(
            lead_id=lead_id,
            company_key=company_key,
            normalized_name=normalized_name,
            official_domain=official_domain,
            focus_dimensions=focus_dimensions,
            avoid_dimensions=avoid_dimensions,
            focus_fields_by_dimension=focus_fields_by_dimension,
            blocked_fields_by_dimension=blocked_fields_by_dimension,
            confirmed_entity_name=confirmed_entity_name,
            feedback_entries=unique_entries,
            open_questions=open_questions,
        )

    def record_feedback(
        self,
        *,
        scope: str,
        content: str,
        lead_id: int | None = None,
        company_name: str = "",
        normalized_name: str = "",
        official_domain: str = "",
        dimension: str = "entity",
        feedback_kind: str = "note",
        source_question_id: int | None = None,
        parsed: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope = (scope or "global").strip().lower() or "global"
        content = str(content or "").strip()
        parsed = self._normalize_feedback_payload(dict(parsed or self.parse_feedback_text(content)))
        normalized_name = self._pick_text(normalized_name, company_name)
        company_key = self.db._company_key_from_name(normalized_name or company_name)
        scope_key = self._scope_key(scope=scope, lead_id=lead_id, company_key=company_key)

        if scope == "lead":
            scope_key = self._scope_key(scope=scope, lead_id=lead_id, company_key=company_key)
        elif scope == "company":
            scope_key = self._scope_key(scope=scope, lead_id=lead_id, company_key=company_key)
        else:
            scope_key = "global"

        now = datetime.now(UTC).isoformat()
        feedback = DDFeedback(
            scope=scope,
            scope_key=scope_key,
            dimension=dimension,
            feedback_kind=feedback_kind,
            content=content,
            lead_id=lead_id,
            company_key=company_key,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            source_question_id=source_question_id,
            parsed=parsed,
            created_at=now,
            updated_at=now,
        )
        feedback_id = self.db.add_dd_feedback_memory(feedback.to_dict())
        self.db.touch_leads_for_dd_feedback(scope=scope, lead_id=lead_id, company_key=company_key)
        return {"ok": True, "feedback_id": feedback_id, "feedback": feedback.to_dict()}

    def record_question(self, question: DDQuestion) -> DDQuestion:
        payload = question.to_dict()
        self.db.add_dd_question(payload)
        return question

    def list_open_questions(self, *, lead_id: int | None = None, company_key: str = "", limit: int = 20) -> list[dict[str, Any]]:
        return self.db.list_dd_questions(lead_id=lead_id, company_key=company_key, status="open", limit=limit)

    def answer_question(self, *, question_id: int, answer_text: str) -> dict[str, Any]:
        question = self.db.get_dd_question(question_id)
        if not question:
            return {"ok": False, "reason": "question_not_found"}

        parsed = self.parse_feedback_text(answer_text)
        feedback_result = self.record_feedback(
            scope=str(question.get("scope") or "lead"),
            content=answer_text,
            lead_id=int(question.get("lead_id") or 0) or None,
            company_name=str(question.get("company_name") or ""),
            normalized_name=str(question.get("normalized_name") or ""),
            official_domain=str(question.get("official_domain") or ""),
            dimension=str(question.get("dimension") or "entity"),
            feedback_kind="question_answer",
            source_question_id=question_id,
            parsed=parsed,
        )
        feedback_id = int(feedback_result.get("feedback_id") or 0)
        self.db.resolve_dd_question(question_id=question_id, answer_text=answer_text, answer_feedback_id=feedback_id)
        return {
            "ok": True,
            "question": question,
            "feedback": feedback_result.get("feedback", {}),
            "feedback_id": feedback_id,
        }

    def parse_feedback_text(self, text: str) -> dict[str, Any]:
        raw = self._clean_text(text)
        lowered = raw.lower()
        focus_dimensions: list[str] = []
        avoid_dimensions: list[str] = []
        focus_fields_by_dimension: dict[str, list[str]] = {}
        blocked_fields_by_dimension: dict[str, list[str]] = {}

        positive_markers = ("重点", "优先", "继续", "多看", "补", "加大", "focus", "focus on", "prefer", "more")
        negative_markers = ("不要", "不补", "别补", "少看", "暂时不看", "忽略", "avoid", "skip", "less")

        for dimension, keywords in DIMENSION_KEYWORDS.items():
            matched = any(keyword.lower() in lowered for keyword in keywords)
            if not matched:
                continue
            if any(marker in lowered for marker in negative_markers):
                if dimension not in avoid_dimensions:
                    avoid_dimensions.append(dimension)
            elif any(marker in lowered for marker in positive_markers):
                if dimension not in focus_dimensions:
                    focus_dimensions.append(dimension)

        field_hits: dict[str, tuple[str, ...]] = {
            "customers": ("客户", "customer", "clients", "users"),
            "partners": ("合作", "partner", "partners", "生态"),
            "product_launches": ("产品发布", "launch", "上线", "发布"),
            "revenue_signals": ("收入", "revenue", "arr", "付费", "收费"),
            "deployment_signals": ("部署", "落地", "pilot", "production", "上线"),
            "valuation": ("估值", "valuation", "valued"),
            "total_raised": ("融资额", "raised", "融资", "募资", "金额"),
            "founded_year": ("成立", "found", "founded"),
            "headquarters": ("总部", "headquarters", "based in"),
            "founders": ("创始人", "founder", "co-founder"),
            "key_people": ("核心成员", "key people", "leadership", "团队"),
            "sub_sector": ("赛道", "sub-sector", "subsector", "细分"),
            "competitors": ("竞争", "competitor", "rival"),
            "one_liner": ("一句话", "one-liner"),
            "products_services": ("产品", "service", "platform"),
            "target_customers": ("客户", "customer", "enterprise"),
            "use_cases": ("场景", "use case", "use cases"),
        }
        for field_name, keywords in field_hits.items():
            if not any(keyword.lower() in lowered for keyword in keywords):
                continue
            dimension = FIELD_KEYWORDS.get(field_name, "entity")
            if any(marker in lowered for marker in negative_markers):
                blocked_fields_by_dimension.setdefault(dimension, [])
                if field_name not in blocked_fields_by_dimension[dimension]:
                    blocked_fields_by_dimension[dimension].append(field_name)
            else:
                focus_fields_by_dimension.setdefault(dimension, [])
                if field_name not in focus_fields_by_dimension[dimension]:
                    focus_fields_by_dimension[dimension].append(field_name)
                if dimension not in focus_dimensions:
                    focus_dimensions.append(dimension)

        confirmed_entity_name = self._extract_confirmed_entity_name(raw)

        return {
            "raw_text": raw,
            "focus_dimensions": [self._normalize_dimension_key(item) for item in focus_dimensions],
            "avoid_dimensions": [self._normalize_dimension_key(item) for item in avoid_dimensions],
            "focus_fields_by_dimension": {
                self._normalize_dimension_key(dimension): list(fields)
                for dimension, fields in focus_fields_by_dimension.items()
            },
            "blocked_fields_by_dimension": {
                self._normalize_dimension_key(dimension): list(fields)
                for dimension, fields in blocked_fields_by_dimension.items()
            },
            "confirmed_entity_name": confirmed_entity_name,
        }

    def _feedback_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        parsed = row.get("parsed_json") or {}
        if isinstance(parsed, str):
            try:
                import json

                parsed = json.loads(parsed)
            except Exception:
                parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
        parsed.setdefault("focus_dimensions", [])
        parsed.setdefault("avoid_dimensions", [])
        parsed.setdefault("focus_fields_by_dimension", {})
        parsed.setdefault("blocked_fields_by_dimension", {})
        parsed.setdefault("confirmed_entity_name", "")
        return self._normalize_feedback_payload(parsed)

    def _normalize_feedback_payload(self, parsed: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(parsed or {})
        normalized["focus_dimensions"] = [
            self._normalize_dimension_key(item) for item in normalized.get("focus_dimensions", [])
        ]
        normalized["avoid_dimensions"] = [
            self._normalize_dimension_key(item) for item in normalized.get("avoid_dimensions", [])
        ]
        normalized["focus_fields_by_dimension"] = {
            self._normalize_dimension_key(dimension): [
                str(field).strip() for field in fields if str(field).strip()
            ]
            for dimension, fields in (normalized.get("focus_fields_by_dimension", {}) or {}).items()
        }
        normalized["blocked_fields_by_dimension"] = {
            self._normalize_dimension_key(dimension): [
                str(field).strip() for field in fields if str(field).strip()
            ]
            for dimension, fields in (normalized.get("blocked_fields_by_dimension", {}) or {}).items()
        }
        normalized["confirmed_entity_name"] = str(normalized.get("confirmed_entity_name") or "").strip()
        normalized["raw_text"] = str(normalized.get("raw_text") or "").strip()
        return normalized

    def _scope_key(self, *, scope: str, lead_id: int | None, company_key: str) -> str:
        if scope == "lead" and lead_id:
            return f"lead:{lead_id}"
        if scope == "company" and company_key:
            return f"company:{company_key}"
        return "global"

    def _normalize_dimension_key(self, value: str) -> str:
        key = str(value or "").strip().lower()
        return DIMENSION_ALIASES.get(key, key)

    def _extract_confirmed_entity_name(self, text: str) -> str:
        patterns = [
            r"(?:company name|company subject|company|entity|subject|公司名|公司名字|公司主体|主体)\s*(?:is|are|=|:|是|为|叫|应是|应该是|其实是|就是)\s*([A-Z][A-Za-z0-9& ._\-/]{1,120})",
            r"([A-Z][A-Za-z0-9& ._\-/]{1,120})\s*(?:is|are|=|:|是|为|叫)\s*(?:the\s*)?(?:company name|company subject|company|entity|subject|公司名|公司名字|公司主体|主体)",
            r"(?:confirm|confirmed|确认|确定)\s*(?:company name|company subject|公司名|公司主体)\s*(?:is|are|=|:|是|为|叫)\s*([A-Z][A-Za-z0-9& ._\-/]{1,120})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            groups = [group for group in match.groups() if group]
            if not groups:
                continue
            candidate = self._clean_text(groups[-1])
            if candidate:
                return candidate
        return ""

    @staticmethod
    def _clean_text(text: str) -> str:
        cleaned = str(text or "").strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = cleaned.strip(" -_：:，,。！？?!")
        return cleaned

    @staticmethod
    def _pick_text(*values: Any) -> str:
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return ""
