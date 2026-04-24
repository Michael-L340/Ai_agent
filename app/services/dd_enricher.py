from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from hashlib import sha1
import re
from typing import Any, Callable

from app.clients.bocha_client import BochaSearchClient
from app.clients.brave_client import BraveSearchClient
from app.models.dd_question import DDQuestion
from app.models.dd_report import DDOverall, DDProfile, DDReport
from app.services.source_extractors import (
    DD_DIMENSION_FIELDS,
    build_dimension_queries,
    extract_dimension_profile,
    fetch_official_pages,
)


DimensionName = str
SearchFunc = Callable[[str, int], list[dict[str, str]]]
PageFetcher = Callable[[str], list[dict[str, Any]]]


class DDEnricher:
    """
    Convert a discovered lead into a structured five-dimensional DD report.

    Core rules:
    - Only use search evidence and official-site evidence; do not invent revenue, valuation, customers, or other missing facts.
    - Use normalized_name + official_domain as the primary subject key for dimension-specific queries.
    - Produce five dimensions with fields / evidence / missing_fields / confidence.
    """

    DIMENSIONS: tuple[DimensionName, ...] = (
        "business_profile",
        "team_profile",
        "funding_profile",
        "traction_profile",
        "market_position",
    )

    def __init__(
        self,
        brave_client: BraveSearchClient,
        bocha_client: BochaSearchClient,
        *,
        mvp_mode: bool = False,
        page_fetcher: PageFetcher | None = None,
        memory: Any | None = None,
        search_limit_per_query: int = 5,
        queries_per_dimension: int = 2,
        max_official_pages: int = 6,
    ):
        self.brave_client = brave_client
        self.bocha_client = bocha_client
        self.page_fetcher = page_fetcher or self._default_page_fetcher
        self.memory = memory
        self.mvp_mode = bool(mvp_mode)
        self.search_limit_per_query = max(1, int(search_limit_per_query))
        self.queries_per_dimension = max(1, int(queries_per_dimension))
        self.max_official_pages = max(1, int(max_official_pages))

    def enrich(self, lead: dict[str, Any]) -> DDReport:
        if not isinstance(lead, dict):
            lead = dict(lead)
        lead_id = int(lead.get("id") or lead.get("lead_id") or 0)
        company_name = self._pick_text(
            lead.get("normalized_name"),
            lead.get("company_name"),
            lead.get("candidate_name"),
            lead.get("raw_title"),
        )
        normalized_name = self._pick_text(
            lead.get("normalized_name"),
            lead.get("candidate_name"),
            lead.get("company_name"),
            company_name,
        )
        official_domain = self._normalize_domain(lead.get("official_domain") or "")

        feedback_context = self._feedback_context_from_lead(lead)
        confirmed_entity_name = str(feedback_context.get("confirmed_entity_name") or "").strip()
        if confirmed_entity_name:
            company_name = confirmed_entity_name
            normalized_name = confirmed_entity_name
        queries_by_dimension = build_dimension_queries(
            company_name,
            normalized_name,
            official_domain,
            feedback_hints=feedback_context,
        )
        search_hits_by_dimension: dict[str, list[dict[str, Any]]] = {}
        for dimension in self.DIMENSIONS:
            queries = queries_by_dimension.get(dimension, [])[: self._dimension_query_limit(dimension, feedback_context)]
            search_hits_by_dimension[dimension] = self._collect_search_hits(dimension, queries)

        official_pages = self._fetch_official_pages(official_domain)

        profiles: dict[str, DDProfile] = {}
        dimension_scores: dict[str, float] = {}
        dimension_source_sets: dict[str, set[str]] = {}
        for dimension in self.DIMENSIONS:
            profile_payload = extract_dimension_profile(
                dimension=dimension,
                company_name=company_name,
                normalized_name=normalized_name,
                official_domain=official_domain,
                search_hits=search_hits_by_dimension.get(dimension, []),
                official_pages=official_pages,
                blocked_fields=set(feedback_context.get("blocked_fields_by_dimension", {}).get(dimension, [])),
            )
            profile = DDProfile(
                fields=profile_payload["fields"],
                evidence=profile_payload["evidence"],
                missing_fields=profile_payload["missing_fields"],
                confidence=float(profile_payload["confidence"] or 0.0),
            )
            profiles[dimension] = profile
            dimension_scores[dimension] = self._dimension_score(profile, dimension)
            dimension_source_sets[dimension] = {
                str(item.get("source") or "").strip()
                for item in profile.evidence
                if str(item.get("source") or "").strip()
            }
            dimension_source_sets[dimension].update(
                {
                    str(item.get("source") or "").strip()
                    for item in search_hits_by_dimension.get(dimension, [])
                    if str(item.get("source") or "").strip()
                }
            )

        questions = self._build_questions(
            lead=lead,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            profiles=profiles,
            search_hits_by_dimension=search_hits_by_dimension,
            official_pages=official_pages,
            feedback_context=feedback_context,
        )

        completeness_score = round(sum(dimension_scores.values()), 2)
        filled_dimensions = [dimension for dimension, score in dimension_scores.items() if score >= 10.0]
        missing_dimensions = [dimension for dimension, score in dimension_scores.items() if score < 10.0]
        source_hits = len(
            {
                source
                for sources in dimension_source_sets.values()
                for source in sources
                if source
            }
        )
        dd_status = self._determine_status(completeness_score, missing_dimensions, profiles, questions)
        dd_overall = DDOverall(
            dd_status=dd_status,
            completeness_score=completeness_score,
            source_hits=source_hits,
            summary=self._build_overall_summary(company_name, filled_dimensions, missing_dimensions, official_domain),
            missing_dimensions=missing_dimensions,
            confidence=round(sum(profile.confidence for profile in profiles.values()) / max(1, len(profiles)), 2),
            generated_at=datetime.now(UTC).isoformat(),
        )

        evidence_json = {
            "lead_id": lead_id,
            "company_name": company_name,
            "normalized_name": normalized_name,
            "official_domain": official_domain,
            "dimension_queries": queries_by_dimension,
            "search_hits_by_dimension": search_hits_by_dimension,
            "official_pages": [
                {
                    "source": page.get("source", "official_page"),
                    "title": str(page.get("title") or "").strip(),
                    "url": str(page.get("url") or "").strip(),
                    "text": str(page.get("text") or "").strip()[:1500],
                }
                for page in official_pages
            ],
            "dimension_scores": dimension_scores,
            "dd_status": dd_status,
            "completeness_score": completeness_score,
            "source_hits": source_hits,
            "feedback_context": feedback_context,
            "questions": [question.to_dict() for question in questions],
        }

        return DDReport(
            lead_id=lead_id,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            source_hits=source_hits,
            dd_status=dd_status,
            completeness_score=completeness_score,
            business_profile=profiles["business_profile"],
            team_profile=profiles["team_profile"],
            funding_profile=profiles["funding_profile"],
            traction_profile=profiles["traction_profile"],
            market_position=profiles["market_position"],
            dd_overall=dd_overall,
            questions=questions,
            evidence_json=evidence_json,
        )

    def _collect_search_hits(self, dimension: str, queries: list[str]) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        for query in queries:
            brave_results = self.brave_client.search(query=query, limit=self.search_limit_per_query) or []
            bocha_results = self.bocha_client.search(query=query, limit=self.search_limit_per_query) or []
            hits.extend(self._normalize_hits("brave", query, brave_results))
            hits.extend(self._normalize_hits("bocha", query, bocha_results))
        return self._dedupe_hits(hits)

    def _fetch_official_pages(self, official_domain: str) -> list[dict[str, Any]]:
        if not official_domain:
            return []
        try:
            pages = self.page_fetcher(official_domain)
        except Exception:
            return []
        cleaned: list[dict[str, Any]] = []
        for page in pages[: self.max_official_pages]:
            cleaned.append(
                {
                    "source": "official_page",
                    "url": str(page.get("url") or "").strip(),
                    "title": str(page.get("title") or "").strip(),
                    "text": str(page.get("text") or "").strip(),
                }
            )
        return cleaned

    def _normalize_hits(self, source: str, query: str, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in raw_items:
            title = self._pick_text(item.get("title"), item.get("name"))
            snippet = self._pick_text(item.get("snippet"), item.get("summary"), item.get("description"))
            url = self._pick_text(item.get("url"), item.get("link"), item.get("source"))
            if not title or not url:
                continue
            normalized.append(
                {
                    "source": source,
                    "query": query,
                    "title": title,
                    "snippet": snippet,
                    "url": url,
                }
            )
        return normalized

    def _dedupe_hits(self, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for item in hits:
            key = (
                str(item.get("source") or "").strip().lower(),
                str(item.get("url") or "").strip().lower(),
                str(item.get("title") or "").strip().lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _dimension_score(self, profile: DDProfile, dimension: str) -> float:
        field_names = DD_DIMENSION_FIELDS[dimension]
        filled = 0
        for field_name in field_names:
            value = profile.fields.get(field_name)
            if self._is_missing(value):
                continue
            filled += 1
        coverage = filled / max(1, len(field_names))
        return round(min(20.0, coverage * 20.0), 2)

    def _determine_status(
        self,
        completeness_score: float,
        missing_dimensions: list[str],
        profiles: dict[str, DDProfile],
        questions: list[DDQuestion],
    ) -> str:
        has_evidence = any(profile.evidence for profile in profiles.values())
        severe_subject_conflict = any(
            str(question.question_type or "").lower() == "subject_conflict"
            or str(question.dimension or "").lower() == "entity"
            for question in questions
        )
        if questions and (not self.mvp_mode or severe_subject_conflict):
            return "dd_waiting_human"
        strong_dimensions = sum(1 for profile in profiles.values() if profile.confidence >= 40.0)
        if completeness_score >= 80.0 and not missing_dimensions and strong_dimensions >= 4:
            return "dd_done"
        if has_evidence:
            return "dd_partial"
        return "dd_pending_review"

    def _feedback_context_from_lead(self, lead: dict[str, Any]) -> dict[str, Any]:
        if self.memory is None:
            return {}
        try:
            context = self.memory.build_context(lead)
            return context.to_dict() if hasattr(context, "to_dict") else dict(context)
        except Exception:
            return {}

    def _dimension_query_limit(self, dimension: str, feedback_context: dict[str, Any]) -> int:
        focus_dimensions = {str(item).strip() for item in feedback_context.get("focus_dimensions", []) if str(item).strip()}
        avoid_dimensions = {str(item).strip() for item in feedback_context.get("avoid_dimensions", []) if str(item).strip()}
        if dimension in focus_dimensions:
            return max(self.queries_per_dimension + 1, 2)
        if dimension in avoid_dimensions:
            return max(1, self.queries_per_dimension - 1)
        return self.queries_per_dimension

    def _build_questions(
        self,
        *,
        lead: dict[str, Any],
        company_name: str,
        normalized_name: str,
        official_domain: str,
        profiles: dict[str, DDProfile],
        search_hits_by_dimension: dict[str, list[dict[str, Any]]],
        official_pages: list[dict[str, Any]],
        feedback_context: dict[str, Any],
    ) -> list[DDQuestion]:
        lead_id = int(lead.get("id") or lead.get("lead_id") or 0)
        if not lead_id:
            return []

        questions: list[DDQuestion] = []
        current_subject = normalized_name or company_name
        confirmed_entity_name = str(feedback_context.get("confirmed_entity_name") or "").strip()
        has_any_evidence = any(search_hits_by_dimension.values()) or bool(official_pages) or any(
            profile.evidence for profile in profiles.values()
        )
        if not has_any_evidence:
            return []
        if confirmed_entity_name:
            return []
        if not confirmed_entity_name:
            conflict_candidate = self._detect_subject_conflict(current_subject, search_hits_by_dimension, official_pages)
        else:
            conflict_candidate = ""
        if conflict_candidate and self._normalize_name_key(conflict_candidate) != self._normalize_name_key(current_subject):
            questions.append(
                self._make_question(
                    lead_id=lead_id,
                    company_name=company_name,
                    normalized_name=normalized_name,
                    official_domain=official_domain,
                    dimension="entity",
                    question_type="subject_conflict",
                    prompt=(
                        f"我在搜索里反复看到另一个主体 “{conflict_candidate}”，"
                        f"但当前 lead 名称是 “{current_subject}”。请确认这条记录真正对应的公司主体。"
                    ),
                    missing_fields=["normalized_name"],
                    details={
                        "conflict_candidate": conflict_candidate,
                        "confirmed_entity_name": confirmed_entity_name,
                    },
                )
            )
            return questions[:1]

        focus_fields_by_dimension = feedback_context.get("focus_fields_by_dimension", {}) or {}
        avoid_dimensions = {str(item).strip() for item in feedback_context.get("avoid_dimensions", []) if str(item).strip()}
        for dimension in self.DIMENSIONS:
            if dimension in avoid_dimensions:
                continue
            profile = profiles.get(dimension)
            if not profile or profile.confidence >= 15.0:
                continue

            missing_fields = list(profile.missing_fields or [])
            focus_fields = [str(item).strip() for item in focus_fields_by_dimension.get(dimension, []) if str(item).strip()]
            if focus_fields:
                narrowed = [field for field in missing_fields if field in focus_fields]
                if narrowed:
                    missing_fields = narrowed

            if not missing_fields:
                continue

            questions.append(
                self._make_question(
                    lead_id=lead_id,
                    company_name=company_name,
                    normalized_name=normalized_name,
                    official_domain=official_domain,
                    dimension=dimension,
                    question_type="missing_fields",
                    prompt=self._build_question_prompt(dimension, missing_fields, current_subject),
                    missing_fields=missing_fields[:3],
                    details={
                        "confidence": profile.confidence,
                        "evidence_count": len(profile.evidence),
                        "focus_fields": focus_fields,
                        "feedback_context": feedback_context,
                    },
                )
            )
            if len(questions) >= 2:
                break

        return questions

    def _build_question_prompt(self, dimension: str, missing_fields: list[str], company_name: str) -> str:
        dimension_labels = {
            "business_profile": "业务概况",
            "team_profile": "团队背景",
            "funding_profile": "融资概况",
            "traction_profile": "业务进展",
            "market_position": "行业地位",
        }
        missing_text = "、".join(missing_fields[:3]) if missing_fields else "关键字段"
        return f"{company_name} 的 {dimension_labels.get(dimension, dimension)} 里还缺少 {missing_text}。如果你有更准的资料，请直接补一句。"

    def _make_question(
        self,
        *,
        lead_id: int,
        company_name: str,
        normalized_name: str,
        official_domain: str,
        dimension: str,
        question_type: str,
        prompt: str,
        missing_fields: list[str],
        details: dict[str, Any],
    ) -> DDQuestion:
        now = datetime.now(UTC).isoformat()
        return DDQuestion(
            lead_id=lead_id,
            company_key=self._normalize_name_key(normalized_name or company_name),
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            dimension=dimension,
            question_type=question_type,
            prompt=prompt,
            scope="lead",
            scope_key=f"lead:{lead_id}",
            missing_fields=missing_fields,
            details=details,
            dedupe_key=self._question_dedupe_key(lead_id, dimension, question_type, prompt),
            status="open",
            created_at=now,
            updated_at=now,
        )

    def _question_dedupe_key(self, lead_id: int, dimension: str, question_type: str, prompt: str) -> str:
        payload = f"{lead_id}|{dimension}|{question_type}|{prompt}".encode("utf-8")
        return sha1(payload).hexdigest()

    def _detect_subject_conflict(
        self,
        current_subject: str,
        search_hits_by_dimension: dict[str, list[dict[str, Any]]],
        official_pages: list[dict[str, Any]],
    ) -> str:
        candidates: Counter[str] = Counter()
        official_candidates: Counter[str] = Counter()
        for hits in search_hits_by_dimension.values():
            for item in hits:
                for text in [str(item.get("title") or ""), str(item.get("snippet") or "")]:
                    for candidate in self._extract_company_candidates(text):
                        candidates[candidate] += 1
        for page in official_pages:
            for text in [str(page.get("title") or ""), str(page.get("text") or "")]:
                for candidate in self._extract_company_candidates(text):
                    candidates[candidate] += 1
                    official_candidates[candidate] += 1

        current_key = self._normalize_name_key(current_subject)
        for candidate, count in candidates.most_common(10):
            # In MVP mode, only treat subject conflict as severe when the alternate
            # subject appears repeatedly and is also supported by official-page text.
            if count < 3:
                continue
            if official_candidates.get(candidate, 0) < 1:
                continue
            candidate_key = self._normalize_name_key(candidate)
            if not candidate_key or candidate_key == current_key:
                continue
            if self._looks_like_generic_noise(candidate):
                continue
            return candidate
        return ""

    def _extract_company_candidates(self, text: str) -> list[str]:
        text = re.sub(r"\s+", " ", str(text or "").strip())
        if not text:
            return []
        candidates: list[str] = []
        for pattern in [
            r"(?:About|Team|Customers|Press|Funding|Security Solution for)\s+([A-Z][A-Za-z0-9&]+(?:\s+[A-Z][A-Za-z0-9&]+){0,3})",
            r"([A-Z][A-Za-z0-9&]+(?:\s+[A-Z][A-Za-z0-9&]+){1,3})",
        ]:
            for match in re.finditer(pattern, text):
                candidate = self._clean_candidate(match.group(1))
                if candidate:
                    candidates.append(candidate)
        return self._dedupe_strings(candidates)

    def _looks_like_generic_noise(self, candidate: str) -> bool:
        lowered = self._normalize_name_key(candidate)
        if lowered in {
            "what",
            "guide",
            "report",
            "analysis",
            "security",
            "runtime",
            "market",
            "startup",
            "startups",
            "customers",
            "company",
            "team",
            "product",
            "platform",
        } or len(lowered) <= 2:
            return True
        tokens = [token for token in re.split(r"[^a-z0-9]+", str(candidate or "").strip().lower()) if token]
        generic_tokens = {
            "ai",
            "agent",
            "security",
            "runtime",
            "platform",
            "solution",
            "solutions",
            "startup",
            "startups",
            "company",
            "companies",
            "team",
            "teams",
            "product",
            "products",
            "service",
            "services",
            "market",
            "analysis",
            "guide",
            "report",
            "customers",
            "customer",
            "what",
            "is",
            "the",
            "new",
            "hot",
            "frontier",
            "llm",
            "model",
            "models",
            "enterprise",
            "enterprises",
            "investor",
            "investors",
            "funding",
            "launch",
            "launches",
            "red",
            "teaming",
        }
        if tokens and all(token in generic_tokens for token in tokens):
            return True
        return False

    def _clean_candidate(self, text: str) -> str:
        cleaned = re.sub(r"\s+", " ", str(text or "").strip())
        cleaned = cleaned.strip(" -_：:，,。！？?!")
        cleaned = re.sub(
            r"\b(for|launch|launches|launching|raises|raised|raise|announces|announced|completes|completed|closes|secures|secured|debuts|unveils|ships)\b.*$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip()
        return cleaned

    def _normalize_name_key(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

    @staticmethod
    def _dedupe_strings(values: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            key = str(value).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(str(value).strip())
        return out

    def _build_overall_summary(
        self,
        company_name: str,
        filled_dimensions: list[str],
        missing_dimensions: list[str],
        official_domain: str,
    ) -> str:
        if not filled_dimensions:
            if official_domain:
                return f"No reliable DD evidence yet for {company_name}; only the official domain {official_domain} is known."
            return f"No reliable DD evidence yet for {company_name}."
        filled_text = ", ".join(filled_dimensions)
        missing_text = ", ".join(missing_dimensions) if missing_dimensions else "none"
        if official_domain:
            return f"{company_name}: evidence collected for {filled_text}; official domain {official_domain}; weaker dimensions: {missing_text}."
        return f"{company_name}: evidence collected for {filled_text}; weaker dimensions: {missing_text}."

    def _pick_text(self, *values: Any) -> str:
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _normalize_domain(self, domain: str) -> str:
        text = str(domain or "").strip().lower()
        if not text:
            return ""
        if text.startswith("https://"):
            text = text[8:]
        elif text.startswith("http://"):
            text = text[7:]
        if text.startswith("www."):
            text = text[4:]
        return text.strip("/")

    @staticmethod
    def _default_page_fetcher(official_domain: str) -> list[dict[str, Any]]:
        return fetch_official_pages(official_domain)

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, bool):
            return False
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (list, tuple, set)):
            return len(value) == 0
        return False
