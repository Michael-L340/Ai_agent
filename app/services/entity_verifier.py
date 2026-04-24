from __future__ import annotations

import re
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from app.clients.bocha_client import BochaSearchClient
from app.clients.brave_client import BraveSearchClient
from app.clients.llm_client import LLMClient
from app.core.config import Settings
from app.models.entity_resolution import EntityResolution
from app.services.company_name_memory import CompanyNameFeedbackStore


class EntityVerifier:
    CONTENT_MARKERS = (
        "what is",
        "how to",
        "why ",
        "new frontier",
        "guide",
        "tutorial",
        "cheat sheet",
        "checklist",
        "market report",
        "funding data",
        "startups to know",
        "top ",
        "hot ",
        "actually adopting",
        "manage genai risks",
        "get contacts",
        "listicle",
        "explainer",
        "primer",
        "analysis",
        "research",
        "whitepaper",
        "report",
        "article",
        "blog",
        "podcast",
        "webinar",
        "github",
        "open source",
        "documentation",
        "wiki",
        "github.com",
        "playbook for",
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
    SINGLE_TOKEN_BLOCKLIST = {
        "actually",
        "article",
        "blog",
        "in",
        "of",
        "and",
        "or",
        "the",
        "on",
        "at",
        "by",
        "a",
        "an",
        "guide",
        "hot",
        "most",
        "non",
        "new",
        "podcast",
        "report",
        "research",
        "runtime",
        "top",
        "competitors",
        "team",
        "investors",
        "tutorial",
        "webinar",
        "what",
        "why",
        "how",
        "get",
        "manage",
        "contact",
        "contacts",
        "analysis",
        "explainer",
    }
    COMPANY_HINT_WORDS = (
        "startup",
        "company",
        "inc",
        "corp",
        "co",
        "ai",
        "labs",
        "security",
        "agent",
        "platform",
        "solution",
        "software",
        "systems",
        "tech",
        "technologies",
        "network",
        "cloud",
    )
    MVP_RELEVANCE_MARKERS = (
        "ai security",
        "agent security",
        "cybersecurity",
        "genai security",
        "llm security",
        "runtime protection",
        "runtime security",
        "prompt injection",
        "ai runtime",
        "model security",
        "red teaming",
    )
    GENERIC_NAME_TOKENS = {
        "ai",
        "security",
        "agent",
        "agents",
        "startup",
        "startups",
        "company",
        "platform",
        "solution",
        "solutions",
        "software",
        "systems",
        "market",
        "report",
        "data",
        "guide",
        "what",
        "is",
        "how",
        "to",
        "for",
        "the",
        "new",
        "hot",
        "top",
        "know",
        "launch",
        "launches",
        "launching",
        "raises",
        "raised",
        "funding",
        "customers",
        "customer",
        "customer",
        "customer",
        "research",
        "article",
        "blog",
        "open",
        "source",
        "github",
        "red",
        "teaming",
        "red-teaming",
        "llm",
        "model",
        "market",
        "funding",
        "dataset",
        "guide",
        "security",
    }
    BLOCKED_NAMES = {
        "microsoft",
        "google",
        "meta",
        "amazon",
        "apple",
        "openai",
        "github",
        "linkedin",
        "crunchbase",
        "pitchbook",
        "the information",
        "theinformation",
        "wikipedia",
        "reddit",
        "youtube",
        "x",
        "twitter",
        "ibm",
        "crowdstrike",
        "sequoia",
        "crn",
        "fortune",
        "cnbc",
        "a16z",
        "yc",
        "y combinator",
        "reuters",
        "bloomberg",
        "forbes",
        "wired",
        "venturebeat",
        "businesswire",
        "prnewswire",
        "globenewswire",
    }
    GENERIC_DOMAINS = (
        "a16z.news",
        "businesswire.com",
        "bloomberg.com",
        "cnbc.com",
        "linkedin.com",
        "crunchbase.com",
        "forbes.com",
        "globenewswire.com",
        "pitchbook.com",
        "theinformation.com",
        "merriam-webster.com",
        "techcrunch.com",
        "siliconangle.com",
        "most.org",
        "news.ycombinator.com",
        "medium.com",
        "prnewswire.com",
        "reuters.com",
        "substack.com",
        "venturebeat.com",
        "wired.com",
        "wikipedia.org",
        "youtube.com",
        "reddit.com",
        "x.com",
        "twitter.com",
        "github.com",
    )
    TITLE_CANDIDATE_RE = re.compile(
        r"(?:[A-Z][A-Za-z0-9&]+|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9&]+|[A-Z]{2,})){0,3}"
    )

    def __init__(self, settings: Settings, llm: LLMClient | None = None):
        self.settings = settings
        self.llm = llm
        self.brave_client = BraveSearchClient(settings)
        self.bocha_client = BochaSearchClient(settings)
        self.feedback_store = CompanyNameFeedbackStore()
        self._cache: dict[str, EntityResolution] = {}

    def resolve(
        self,
        raw_title: str,
        snippet: str,
        url: str,
        *,
        source: str = "",
        query: str = "",
    ) -> EntityResolution:
        feedback = self.feedback_store.analyze(raw_title=raw_title, snippet=snippet, url=url)
        cache_key = self._cache_key(raw_title, url, feedback.get("fingerprint", ""))
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            return EntityResolution(**cached.to_dict())

        original_title = self._clean_text(raw_title)
        original_snippet = self._clean_text(snippet)
        cleaned_title_value = feedback.get("cleaned_title")
        cleaned_snippet_value = feedback.get("cleaned_snippet")
        cleaned_title = self._clean_text(original_title if cleaned_title_value is None else cleaned_title_value)
        cleaned_snippet = self._clean_text(original_snippet if cleaned_snippet_value is None else cleaned_snippet_value)
        alias_match = feedback.get("alias_match") or {}
        alias_name = self._normalize_company_name(str(alias_match.get("normalized_name") or "").strip())
        alias_domain = self._clean_text(str(alias_match.get("official_domain") or "")).lower().strip()

        candidate_name = alias_name or self._extract_candidate_name(cleaned_title, cleaned_snippet, url)
        if not candidate_name:
            blocked_phrases = feedback.get("blocked_phrases") or []
            if blocked_phrases and not alias_name:
                reject_reason = "blocked by human feedback"
            else:
                reject_reason = "no company-like entity detected"
            resolution = self._reject_content(
                raw_title=original_title,
                snippet=cleaned_snippet,
                url=url,
                source=source,
                query=query,
                reject_reason=reject_reason,
            )
            self._cache[cache_key] = resolution
            return EntityResolution(**resolution.to_dict())

        heuristic = EntityResolution(
            raw_title=original_title,
            candidate_name=candidate_name,
            normalized_name=self._normalize_company_name(candidate_name),
            entity_type="company",
            official_domain=alias_domain,
            verification_status="pending_review",
            verification_score=28.0 + self._candidate_score(candidate_name, cleaned_title, url),
            reject_reason="",
            source=source,
            url=url,
            snippet=cleaned_snippet,
            query=query,
            evidence={
                "human_feedback": feedback,
            },
        )

        if self._looks_like_content_page(cleaned_title, url):
            resolution = self._reject_content(
                raw_title=cleaned_title,
                snippet=cleaned_snippet,
                url=url,
                source=source,
                query=query,
                reject_reason="content page marker detected",
                candidate_name="",
            )
            self._cache[cache_key] = resolution
            return EntityResolution(**resolution.to_dict())

        llm_hint = self._resolve_with_llm(
            raw_title=cleaned_title,
            snippet=cleaned_snippet,
            url=url,
            candidate_name=candidate_name,
        )
        if llm_hint:
            heuristic = self._merge_llm_hint(heuristic, llm_hint)

        evidence = self._search_evidence(heuristic.normalized_name or heuristic.candidate_name)
        evidence["human_feedback"] = feedback
        final = self._finalize_resolution(heuristic, evidence)
        self._cache[cache_key] = final
        return EntityResolution(**final.to_dict())

    def _cache_key(self, raw_title: str, url: str, feedback_fingerprint: str = "") -> str:
        return f"{self._clean_text(raw_title).lower()}||{self._clean_text(url).lower()}||{feedback_fingerprint}"

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip())

    def _looks_like_content_page(self, title: str, url: str) -> bool:
        text = f"{title} {url}".lower()
        return any(marker in text for marker in self.CONTENT_MARKERS)

    def _reject_content(
        self,
        *,
        raw_title: str,
        snippet: str,
        url: str,
        source: str,
        query: str,
        reject_reason: str,
        candidate_name: str = "",
    ) -> EntityResolution:
        return EntityResolution(
            raw_title=raw_title,
            candidate_name=candidate_name,
            normalized_name="",
            entity_type="content",
            official_domain="",
            verification_status="rejected",
            verification_score=0.0,
            reject_reason=reject_reason,
            source=source,
            url=url,
            snippet=snippet,
            query=query,
            evidence={},
        )

    def _extract_candidate_name(self, title: str, snippet: str, url: str) -> str:
        if not title:
            return ""

        candidates: list[str] = []
        split_patterns = (
            r"^(.+?)\s+for\s+.+$",
            r"^(.+?)\s+(?:launch|launches|launching|raises|raised|raise|announces|announced|closes|clo[sz]ed|secures|secured|completes|completed|debuts|unveils|ships|rolls out)\b.*$",
            r"^(.+?)\s+(?:startup|company|platform|solution|software|service)\b.*$",
        )
        for pattern in split_patterns:
            match = re.match(pattern, title, flags=re.IGNORECASE)
            if match:
                candidate = self._normalize_company_name(match.group(1))
                if candidate:
                    candidates.append(candidate)

        for separator in ["|", "?", "?", "-", ":", "?"]:
            if separator in title:
                left = self._normalize_company_name(title.split(separator)[0])
                if left:
                    candidates.append(left)

        for chunk in self.TITLE_CANDIDATE_RE.findall(title):
            candidate = self._normalize_company_name(chunk)
            if candidate:
                candidates.append(candidate)

        if not candidates and snippet:
            for chunk in self.TITLE_CANDIDATE_RE.findall(snippet):
                candidate = self._normalize_company_name(chunk)
                if candidate:
                    candidates.append(candidate)

        unique_candidates = []
        seen = set()
        for candidate in candidates:
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            unique_candidates.append(candidate)

        if not unique_candidates:
            return ""

        best = max(unique_candidates, key=lambda item: self._candidate_score(item, title, url))
        score = self._candidate_score(best, title, url)
        if score < 10:
            return ""
        if self._is_blocked_name(best) or self._looks_like_content_candidate(best, title=title, snippet=snippet, url=url):
            return ""
        return best

    def _normalize_company_name(self, candidate: str) -> str:
        text = self._clean_text(candidate)
        text = re.sub(r"^[\-\|\:\uFF1A\u2014\u2013\s]+", "", text)
        text = re.sub(r"[\-\|\:\uFF1A\u2014\u2013\s]+$", "", text)
        text = re.sub(r"\s+", " ", text).strip(" .,/;")
        return text

    def _candidate_score(self, candidate: str, title: str, url: str) -> int:
        candidate = self._normalize_company_name(candidate)
        if not candidate:
            return -100

        lowered = candidate.lower()
        tokens = [token for token in re.split(r"[\s&/_-]+", lowered) if token]
        generic_hits = sum(1 for token in tokens if token in self.GENERIC_NAME_TOKENS)
        score = 0

        if any(blocked in lowered for blocked in self.BLOCKED_NAMES):
            score -= 60

        if 1 <= len(tokens) <= 3:
            score += 18
        elif len(tokens) == 4:
            score += 8
        else:
            score -= 8

        if len(tokens) == 1:
            score += 5

        score -= generic_hits * 10

        if lowered in title.lower():
            score += 8

        if self._candidate_from_prefix(title, candidate):
            score += 8

        if self._url_domain_is_generic(url):
            score -= 10

        if re.search(r"\b(company|startup|platform|security|labs|ai)\b", lowered):
            score += 4

        if lowered.startswith(("what ", "how ", "why ", "top ", "hot ")):
            score -= 20

        if self._looks_like_content_candidate(candidate, title=title, snippet="", url=url):
            score -= 40

        return score

    def _candidate_from_prefix(self, title: str, candidate: str) -> bool:
        lowered_title = title.lower()
        lowered_candidate = candidate.lower()
        candidate_pos = lowered_title.find(lowered_candidate)
        if candidate_pos < 0:
            return False
        prefix = lowered_title[:candidate_pos].strip()
        return bool(prefix) and len(prefix.split()) <= 4

    def _is_blocked_name(self, name: str) -> bool:
        lowered = name.lower()
        return any(blocked == lowered or blocked in lowered for blocked in self.BLOCKED_NAMES)

    def _looks_like_content_candidate(self, candidate: str, *, title: str, snippet: str, url: str) -> bool:
        lowered_candidate = self._normalize_company_name(candidate).lower()
        if not lowered_candidate:
            return True

        if self._is_blocked_name(lowered_candidate):
            return True

        text = f"{title} {snippet} {url} {lowered_candidate}".lower()
        if any(marker in text for marker in self.CONTENT_MARKERS):
            return True

        tokens = [token for token in re.split(r"[\s&/_-]+", lowered_candidate) if token]
        if not tokens:
            return True

        if len(tokens) == 1 and tokens[0] in self.SINGLE_TOKEN_BLOCKLIST:
            return True

        content_hits = sum(1 for token in tokens if token in self.SINGLE_TOKEN_BLOCKLIST)
        if content_hits >= 2 and len(tokens) <= 4:
            return True

        generic_hits = sum(1 for token in tokens if token in self.GENERIC_NAME_TOKENS)
        if generic_hits == len(tokens):
            return True

        if lowered_candidate.startswith(("what ", "how ", "why ", "top ", "hot ", "most ", "new ")):
            return True

        return False

    def _url_domain_is_generic(self, url: str) -> bool:
        domain = self._extract_domain(url)
        if not domain:
            return False
        return any(domain == g or domain.endswith(f".{g}") or domain.endswith(g) for g in self.GENERIC_DOMAINS)

    def _extract_domain(self, url: str) -> str:
        parsed = urlparse(url or "")
        netloc = parsed.netloc.lower().strip()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc

    def _resolve_with_llm(
        self,
        *,
        raw_title: str,
        snippet: str,
        url: str,
        candidate_name: str,
    ) -> dict[str, Any]:
        if not self.llm or not self.llm.client:
            return {}
        try:
            return self.llm.resolve_entity_candidate(
                raw_title=raw_title,
                snippet=snippet,
                url=url,
                candidate_name=candidate_name,
            )
        except Exception:
            return {}

    def _merge_llm_hint(
        self,
        resolution: EntityResolution,
        hint: dict[str, Any],
    ) -> EntityResolution:
        merged = EntityResolution(**resolution.to_dict())
        normalized_name = self._normalize_company_name(str(hint.get("normalized_name") or "").strip())
        candidate_name = self._normalize_company_name(str(hint.get("candidate_name") or "").strip())
        if candidate_name and len(candidate_name) >= len(merged.candidate_name):
            merged.candidate_name = candidate_name
        if normalized_name and len(normalized_name) >= 2:
            merged.normalized_name = normalized_name

        entity_type = str(hint.get("entity_type") or "").strip().lower()
        if entity_type in {"company", "content", "unknown"} and merged.entity_type != "company":
            merged.entity_type = entity_type

        official_domain = str(hint.get("official_domain") or "").strip().lower()
        if official_domain:
            merged.official_domain = official_domain

        reject_reason = str(hint.get("reject_reason") or "").strip()
        if reject_reason and merged.verification_status != "verified":
            merged.reject_reason = reject_reason

        try:
            hint_score = float(hint.get("verification_score") or 0.0)
            if hint_score > merged.verification_score:
                merged.verification_score = hint_score
        except Exception:
            pass

        hint_status = str(hint.get("verification_status") or "").strip().lower()
        if hint_status in {"verified", "likely_company", "pending_review", "rejected"} and merged.verification_status != "verified":
            merged.verification_status = hint_status

        return merged

    def _search_evidence(self, candidate_name: str) -> dict[str, Any]:
        candidate = self._normalize_company_name(candidate_name)
        if not candidate:
            return {"searches": {}, "official_domain": "", "domain_scores": {}, "reason": "empty candidate"}

        queries = [
            candidate,
            f'"{candidate}" official site',
        ]
        brave_hits: list[dict[str, str]] = []
        bocha_hits: list[dict[str, str]] = []

        for query in queries:
            brave_hits.extend(self.brave_client.search(query, limit=3))
            bocha_hits.extend(self.bocha_client.search(query, limit=3))

        all_hits = []
        for source_name, hits in (("brave", brave_hits), ("bocha", bocha_hits)):
            for item in hits:
                all_hits.append(
                    {
                        "source": source_name,
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                        "url": item.get("url", ""),
                        "domain": self._extract_domain(item.get("url", "")),
                    }
                )

        domain_scores: Counter[str] = Counter()
        candidate_slug = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        candidate_tokens = [token for token in re.split(r"[\s&/_-]+", candidate.lower()) if token]

        for item in all_hits:
            domain = item["domain"]
            if not domain or self._is_generic_domain(domain):
                continue
            text = f"{item['title']} {item['snippet']}".lower()
            score = 0
            if candidate_slug and candidate_slug in re.sub(r"[^a-z0-9]+", "", domain.lower()):
                score += 40
            if candidate.lower() in text:
                score += 20
            if any(token in text for token in candidate_tokens):
                score += 10
            if self._result_looks_company_like(item["title"], item["snippet"], candidate):
                score += 15
            domain_scores[domain] += score

        official_domain = ""
        if domain_scores:
            official_domain = max(domain_scores.items(), key=lambda pair: pair[1])[0]

        return {
            "searches": {
                "brave": brave_hits,
                "bocha": bocha_hits,
            },
            "official_domain": official_domain,
            "domain_scores": dict(domain_scores),
            "reason": "live search evidence collected",
        }

    def _is_generic_domain(self, domain: str) -> bool:
        lowered = domain.lower()
        return any(lowered == item or lowered.endswith(f".{item}") or lowered.endswith(item) for item in self.GENERIC_DOMAINS)

    def _official_domain_matches_candidate(self, domain: str, candidate: str) -> bool:
        if not domain or not candidate:
            return False
        candidate_slug = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        domain_slug = re.sub(r"[^a-z0-9]+", "", domain.lower())
        if not candidate_slug or not domain_slug:
            return False
        return candidate_slug in domain_slug or domain_slug in candidate_slug

    def _result_looks_company_like(self, title: str, snippet: str, candidate: str) -> bool:
        text = f"{title} {snippet}".lower()
        candidate_lower = candidate.lower()
        return candidate_lower in text and not self._looks_like_content_page(title, "")

    def _fails_mvp_single_token_guard(self, candidate: str) -> bool:
        tokens = [token for token in re.split(r"[\s&/_-]+", self._normalize_company_name(candidate)) if token]
        if len(tokens) != 1:
            return False
        token = tokens[0]
        if token.lower() in self.SINGLE_TOKEN_BLOCKLIST:
            return True
        if len(token) <= 4:
            return True
        if token.isupper():
            return True
        return False

    def _looks_mvp_relevant(self, *, title: str, snippet: str, query: str) -> bool:
        text = f"{title} {snippet} {query}".lower()
        return any(marker in text for marker in self.MVP_RELEVANCE_MARKERS)

    def _qualifies_for_likely_company(
        self,
        resolution: EntityResolution,
        evidence: dict[str, Any],
    ) -> bool:
        candidate = self._normalize_company_name(resolution.normalized_name or resolution.candidate_name)
        if resolution.entity_type != "company" or not candidate:
            return False
        if self._looks_like_content_page(resolution.raw_title, resolution.url):
            return False
        if self._looks_like_content_candidate(candidate, title=resolution.raw_title, snippet=resolution.snippet, url=resolution.url):
            return False
        if self._fails_mvp_single_token_guard(candidate):
            return False
        if self._is_blocked_name(candidate):
            return False
        official_domain = str(evidence.get("official_domain") or resolution.official_domain or "").strip().lower()
        if official_domain and self._is_generic_domain(official_domain):
            return False
        if not self._looks_mvp_relevant(title=resolution.raw_title, snippet=resolution.snippet, query=resolution.query):
            return False
        return True

    def _finalize_resolution(
        self,
        resolution: EntityResolution,
        evidence: dict[str, Any],
    ) -> EntityResolution:
        final = EntityResolution(**resolution.to_dict())
        final.evidence = evidence

        if final.entity_type != "company":
            final.verification_status = "rejected"
            final.verification_score = 0.0
            final.reject_reason = final.reject_reason or "non-company entity"
            return final

        official_domain = str(evidence.get("official_domain") or "").strip().lower()
        final.official_domain = official_domain

        if official_domain and self._is_generic_domain(official_domain):
            final.verification_status = "rejected"
            final.verification_score = min(final.verification_score, 30.0)
            final.reject_reason = final.reject_reason or "generic publisher domain"
            return final

        searches = evidence.get("searches", {})
        brave_hits = searches.get("brave", []) if isinstance(searches, dict) else []
        bocha_hits = searches.get("bocha", []) if isinstance(searches, dict) else []
        all_hits = [*brave_hits, *bocha_hits]

        if official_domain:
            final.verification_score += 25
        if brave_hits:
            final.verification_score += 8
        if bocha_hits:
            final.verification_score += 8
        if brave_hits and bocha_hits:
            final.verification_score += 12

        candidate_lower = final.normalized_name.lower()
        exact_hits = 0
        domain_mentions = 0
        for item in all_hits:
            text = f"{item.get('title', '')} {item.get('snippet', '')}".lower()
            if candidate_lower and candidate_lower in text:
                exact_hits += 1
            item_domain = str(item.get("domain") or self._extract_domain(item.get("url", ""))).lower()
            if official_domain and item_domain == official_domain:
                domain_mentions += 1
        if exact_hits:
            final.verification_score += min(15, exact_hits * 3)
        if domain_mentions:
            final.verification_score += min(10, domain_mentions * 5)

        if final.official_domain and self._official_domain_matches_candidate(final.official_domain, final.normalized_name) and final.verification_score >= 70.0:
            final.verification_status = "verified"
            final.reject_reason = ""
            final.verification_score = min(100.0, final.verification_score)
            return final

        if bool(getattr(self.settings, "mvp_mode", False)) and self._qualifies_for_likely_company(final, evidence):
            final.verification_status = "likely_company"
            final.reject_reason = ""
            final.verification_score = min(100.0, max(final.verification_score, 58.0 if not final.official_domain else 65.0))
            return final

        if final.official_domain or exact_hits >= 1:
            final.verification_status = "pending_review"
            final.reject_reason = final.reject_reason or "needs human review"
            final.verification_score = min(100.0, max(final.verification_score, 45.0))
            return final

        final.verification_status = "rejected"
        final.reject_reason = final.reject_reason or "insufficient live evidence"
        final.verification_score = min(100.0, max(final.verification_score, 20.0))
        return final
