from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI

from app.core.config import Settings


class LLMClient:
    CONTENT_TITLE_MARKERS = (
        "what is",
        "how to",
        "why ",
        "new frontier",
        "startups to know",
        "market report",
        "funding data",
        "listicle",
        "explainer",
        "analysis",
        "research",
        "guide",
        "tutorial",
        "podcast",
        "webinar",
        "article",
        "blog",
        "github",
        "open source",
        "playbook for",
        "actually adopting",
        "manage genai risks",
        "get contacts",
        "funded by",
        "buys",
        "acquires",
        "acquisition",
        "deal push",
        "startups in",
        "cybersecurity startups",
        "agentic security startups",
        "cool ",
    )

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: OpenAI | None = None
        if settings.openai_api_key:
            kwargs: dict[str, Any] = {"api_key": settings.openai_api_key}
            if settings.openai_base_url:
                kwargs["base_url"] = settings.openai_base_url
            self.client = OpenAI(**kwargs)

        self.positive_keywords = [
            "agent security",
            "ai security",
            "llm security",
            "model security",
            "red teaming",
            "guardrail",
            "prompt injection",
            "安全代理",
            "ai安全",
            "模型安全",
            "智能体安全",
            "红队",
            "越狱",
            "数据泄露",
            "风险控制",
            "b端",
            "企业客户",
        ]
        self.negative_keywords = [
            "microsoft",
            "google",
            "meta",
            "amazon",
            "apple",
            "腾讯",
            "阿里",
            "字节",
            "openai",
        ]

    def classify_relevance(self, title: str, snippet: str) -> dict[str, Any]:
        text = f"{title} {snippet}".lower()

        positive_hits = sum(1 for kw in self.positive_keywords if kw in text)
        negative_hits = sum(1 for kw in self.negative_keywords if kw in text)

        relevant = positive_hits >= 1 and negative_hits == 0
        tags = [kw for kw in self.positive_keywords if kw in text][:5]

        company_name = self.extract_company_name(title=title, snippet=snippet)

        # Use rule result first for stability; LLM can refine when available.
        if self.client:
            try:
                llm_result = self._classify_with_llm(title, snippet)
                if isinstance(llm_result, dict):
                    relevant = bool(llm_result.get("relevant", relevant))
                    tags = llm_result.get("tags", tags) or tags
                    company_name = llm_result.get("company_name", company_name) or company_name
            except Exception:
                pass

        return {
            "relevant": relevant,
            "tags": tags,
            "company_name": company_name,
            "reason": f"positive_hits={positive_hits}, negative_hits={negative_hits}",
        }

    def extract_company_name(self, title: str, snippet: str) -> str:
        title = re.sub(r"\s+", " ", str(title or "").strip())
        snippet = re.sub(r"\s+", " ", str(snippet or "").strip())
        if any(marker in f"{title} {snippet}".lower() for marker in self.CONTENT_TITLE_MARKERS):
            return ""

        prefixes = (
            r"^(.+?)\s+for\s+.+$",
            r"^(.+?)\s+(?:launch|launches|launching|raises|raised|raise|announces|announced|completes|completed|closes|secures|secured|debuts|unveils|ships)\b.*$",
            r"^(.+?)\s+(?:startup|company|platform|solution|software|service)\b.*$",
        )
        for pattern in prefixes:
            match = re.match(pattern, title, flags=re.IGNORECASE)
            if match:
                candidate = self._clean_company_candidate(match.group(1))
                if candidate:
                    return candidate

        for sep in ["|", "—", "-", ":", "：", "–"]:
            if sep in title:
                candidate = self._clean_company_candidate(title.split(sep)[0])
                if candidate:
                    return candidate

        pattern = re.compile(
            r"(?:[A-Z][A-Za-z0-9&]+|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9&]+|[A-Z]{2,})){0,3}"
        )
        candidates = [self._clean_company_candidate(match) for match in pattern.findall(title)]
        candidates = [candidate for candidate in candidates if candidate]
        if candidates:
            return max(candidates, key=lambda item: len(item.split()))

        if snippet:
            snippet_candidates = [self._clean_company_candidate(match) for match in pattern.findall(snippet)]
            snippet_candidates = [candidate for candidate in snippet_candidates if candidate]
            if snippet_candidates:
                return max(snippet_candidates, key=lambda item: len(item.split()))

        fallback = title.strip()[:50]
        return fallback if fallback else snippet.strip()[:50]

    def resolve_entity_candidate(
        self,
        raw_title: str,
        snippet: str,
        url: str,
        candidate_name: str = "",
        evidence_text: str = "",
    ) -> dict[str, Any]:
        if not self.client:
            return {}

        prompt = (
            "You are a strict company-entity verifier for AI security investment research.\n"
            "Analyze the input and return a JSON object with these keys exactly:\n"
            "raw_title, candidate_name, normalized_name, entity_type, official_domain,\n"
            "verification_status, verification_score, reject_reason.\n"
            "Rules:\n"
            "- If the item is a listicle, report, guide, explainer, blog post, GitHub page, market summary, or generic marketing page, set entity_type to content and verification_status to rejected.\n"
            "- If the title mixes a company name with a descriptor, strip the descriptor and normalize to the standard company name.\n"
            "- For ambiguous cases, use verification_status pending_review.\n"
            "- If the title reads like an article headline or question, reject it even if it contains a company-like word.\n"
            "- verification_score must be a number from 0 to 100.\n"
            "- Do not invent evidence.\n"
            "Examples:\n"
            "- 'Protect AI for AI Agent Security' -> normalized_name 'Protect AI', entity_type 'company'.\n"
            "- 'Lakera: The AI' -> normalized_name 'Lakera', entity_type 'company'.\n"
            "- 'Actually Adopting AI' -> entity_type 'content', verification_status 'rejected'.\n"
            "- 'Runtime: The new frontier of AI agent security' -> entity_type 'content', verification_status 'rejected'.\n"
            "- '10 Hot AI Security Startups To Know In 2025' -> entity_type 'content', verification_status 'rejected'.\n"
            "- 'What Is LLM Red Teaming?' -> entity_type 'content', verification_status 'rejected'.\n"
            "Input:\n"
            f"raw_title: {raw_title}\n"
            f"snippet: {snippet}\n"
            f"url: {url}\n"
            f"candidate_name: {candidate_name}\n"
            f"evidence: {evidence_text}\n"
        )

        response = self.client.chat.completions.create(
            model=self.settings.openai_model,
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a careful entity resolver. Return JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        parsed: dict[str, Any] = json.loads(content)
        if not isinstance(parsed.get("verification_score"), (int, float)):
            parsed["verification_score"] = 0
        return parsed

    def _clean_company_candidate(self, text: str) -> str:
        cleaned = re.sub(r"\s+", " ", str(text or "").strip())
        cleaned = cleaned.strip(" .,/;:-—–：|")
        cleaned = re.sub(r"\b(for|launch|launches|launching|raises|raised|raise|announces|announced|completes|completed|closes|secures|secured|debuts|unveils|ships)\b.*$", "", cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    def build_dd_summary(
        self,
        company_name: str,
        evidence_by_source: dict[str, list[dict[str, str]]],
    ) -> dict[str, str]:
        flattened = []
        for source, items in evidence_by_source.items():
            for item in items:
                flattened.append(f"[{source}] {item.get('title', '')} | {item.get('snippet', '')}")

        evidence_text = "\n".join(flattened[:20])

        default_summary = {
            "business_summary": f"{company_name} 可能聚焦 AI/Agent 安全相关产品，建议持续验证其具体应用场景与客户类型。",
            "team_summary": "公开信息有限，需进一步补充核心团队履历与安全背景。",
            "funding_summary": "暂未形成完整融资画像，建议持续跟踪融资节点与投资方质量。",
            "traction_summary": "可见到早期信号，但商业化验证仍需关注客户数量、付费和续约。",
            "industry_position": "属于 AI 安全细分赛道候选标的，需判断是否红海或存在先发龙头。",
        }

        if not self.client:
            return default_summary

        prompt = (
            "你是投研DD分析助手。根据证据给出JSON，字段必须完整："
            "business_summary, team_summary, funding_summary, traction_summary, industry_position。"
            "每个字段1-2句，避免编造。\n"
            f"公司：{company_name}\n"
            f"证据：\n{evidence_text}"
        )

        try:
            response = self.client.chat.completions.create(
                model=self.settings.openai_model,
                temperature=0.2,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "你是严谨的投资研究员，禁止编造信息。"},
                    {"role": "user", "content": prompt},
                ],
            )
            content = response.choices[0].message.content or "{}"
            parsed = json.loads(content)
            for key in default_summary:
                if key not in parsed or not parsed[key]:
                    parsed[key] = default_summary[key]
            return parsed
        except Exception:
            return default_summary

    def _classify_with_llm(self, title: str, snippet: str) -> dict[str, Any]:
        if not self.client:
            return {}

        prompt = (
            "判断这条信息是否属于值得关注的AI安全创业公司线索，并提取公司名。"
            "返回JSON字段：relevant(bool), company_name(str), tags(list[str])。"
            f"\n标题：{title}\n摘要：{snippet}"
        )

        response = self.client.chat.completions.create(
            model=self.settings.openai_model,
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "你是AI安全投研筛选器。"},
                {"role": "user", "content": prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        parsed: dict[str, Any] = json.loads(content)
        if not isinstance(parsed.get("tags"), list):
            parsed["tags"] = []
        return parsed

