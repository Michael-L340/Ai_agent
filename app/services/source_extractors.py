from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


DD_DIMENSION_FIELDS: dict[str, list[str]] = {
    "business_profile": [
        "one_liner",
        "products_services",
        "target_customers",
        "use_cases",
        "official_domain",
    ],
    "team_profile": [
        "founders",
        "key_people",
        "prior_companies",
        "research_background",
    ],
    "funding_profile": [
        "founded_year",
        "headquarters",
        "funding_rounds",
        "total_raised",
        "valuation",
        "notable_investors",
    ],
    "traction_profile": [
        "customers",
        "partners",
        "product_launches",
        "revenue_signals",
        "deployment_signals",
    ],
    "market_position": [
        "sub_sector",
        "is_new_category",
        "competitors",
        "leader_signals",
        "crowdedness",
    ],
}

OFFICIAL_PAGE_PATHS: dict[str, list[str]] = {
    "business_profile": ["/about", "/about-us", "/company", "/product", "/products", "/solutions", "/customers"],
    "team_profile": ["/team", "/about", "/company", "/leadership"],
    "funding_profile": ["/press", "/news", "/investors", "/about", "/company"],
    "traction_profile": ["/customers", "/case-studies", "/product", "/solutions", "/press"],
    "market_position": ["/blog", "/press", "/resources", "/solutions", "/company"],
}

BUSINESS_VERB_HINTS = (
    "provides",
    "providing",
    "offers",
    "offering",
    "builds",
    "building",
    "protects",
    "protecting",
    "secures",
    "securing",
    "monitors",
    "monitoring",
    "detects",
    "detecting",
    "helps",
    "helping",
    "platform",
    "solution",
    "product",
    "service",
)

TARGET_CUSTOMER_HINTS = (
    "enterprise",
    "enterprises",
    "security teams",
    "developers",
    "devops",
    "ai teams",
    "ai builders",
    "banks",
    "financial institutions",
    "healthcare",
    "marketplaces",
    "smb",
    "smbs",
    "startups",
    "customers",
    "organizations",
    "teams",
)

USE_CASE_HINTS = (
    "prompt injection",
    "red teaming",
    "runtime protection",
    "model monitoring",
    "agent security",
    "guardrail",
    "data leakage",
    "pii",
    "policy enforcement",
    "compliance",
    "hallucination",
    "evaluation",
)

TEAM_HINTS = (
    "founder",
    "co-founder",
    "ceo",
    "cto",
    "leadership",
    "team",
    "research",
    "scientist",
    "professor",
    "phd",
    "ex-",
    "formerly",
    "previously",
)

FUNDING_HINTS = (
    "seed",
    "pre-seed",
    "series a",
    "series b",
    "series c",
    "angel",
    "funding",
    "raised",
    "raise",
    "investor",
    "investors",
    "valuation",
    "founded",
    "headquartered",
    "based in",
)

TRACTION_HINTS = (
    "customer",
    "customers",
    "partner",
    "partners",
    "launched",
    "launch",
    "deploy",
    "deployed",
    "production",
    "pilot",
    "revenue",
    "arr",
    "contract",
    "beta",
    "ga",
)

MARKET_HINTS = (
    "competitor",
    "competitors",
    "category",
    "sub-sector",
    "sub sector",
    "new category",
    "new frontier",
    "crowded",
    "leader",
    "leading",
    "first",
    "only",
    "agent security",
    "ai security",
    "llm security",
    "runtime protection",
    "prompt security",
)


@dataclass(slots=True)
class EvidenceBlob:
    source: str
    dimension: str
    title: str
    snippet: str
    url: str
    reason: str
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "dimension": self.dimension,
            "title": self.title,
            "snippet": self.snippet,
            "url": self.url,
            "reason": self.reason,
            "confidence": float(self.confidence),
        }


def build_dimension_queries(
    company_name: str,
    normalized_name: str,
    official_domain: str,
    *,
    feedback_hints: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    subject = normalized_name or company_name
    domain_hint = f"site:{official_domain}" if official_domain else ""
    feedback_hints = feedback_hints or {}
    focus_dimensions = {str(item).strip() for item in feedback_hints.get("focus_dimensions", []) if str(item).strip()}
    avoid_dimensions = {str(item).strip() for item in feedback_hints.get("avoid_dimensions", []) if str(item).strip()}
    focus_fields_by_dimension = {
        str(dimension): [str(field).strip() for field in fields if str(field).strip()]
        for dimension, fields in (feedback_hints.get("focus_fields_by_dimension", {}) or {}).items()
    }
    queries = {
        "business_profile": [
            subject,
            f"{subject} product platform",
            f"{subject} AI security platform",
            f"{domain_hint} about {subject}".strip(),
            f"{domain_hint} product {subject}".strip(),
        ],
        "team_profile": [
            f"{subject} founders team",
            f"{subject} leadership",
            f"{domain_hint} team {subject}".strip(),
            f"{domain_hint} founders {subject}".strip(),
        ],
        "funding_profile": [
            f"{subject} funding investors",
            f"{subject} raised seed series",
            f"{domain_hint} press funding {subject}".strip(),
            f"{domain_hint} investors {subject}".strip(),
        ],
        "traction_profile": [
            f"{subject} customers partners",
            f"{subject} launch deploy revenue",
            f"{domain_hint} customers {subject}".strip(),
            f"{domain_hint} case studies {subject}".strip(),
        ],
        "market_position": [
            f"{subject} competitors category",
            f"{subject} AI security market position",
            f"{domain_hint} competitors {subject}".strip(),
            f"{domain_hint} blog {subject}".strip(),
        ],
    }
    focus_query_variants: dict[str, dict[str, list[str]]] = {
        "business_profile": {
            "one_liner": [f"{subject} what does it do", f"{subject} overview"],
            "products_services": [f"{subject} platform", f"{subject} product", f"{subject} service"],
            "target_customers": [f"{subject} customers", f"{subject} enterprise customers", f"{subject} users"],
            "use_cases": [f"{subject} use cases", f"{subject} prompt injection", f"{subject} runtime protection"],
        },
        "team_profile": {
            "founders": [f"{subject} founders", f"{subject} cofounders"],
            "key_people": [f"{subject} leadership", f"{subject} team"],
            "prior_companies": [f"{subject} founders OpenAI", f"{subject} former companies"],
            "research_background": [f"{subject} researchers", f"{subject} phd"],
        },
        "funding_profile": {
            "founded_year": [f"{subject} founded", f"{subject} founded year"],
            "headquarters": [f"{subject} based in", f"{subject} headquarters"],
            "funding_rounds": [f"{subject} seed funding", f"{subject} series a"],
            "total_raised": [f"{subject} raised", f"{subject} funding"],
            "valuation": [f"{subject} valuation", f"{subject} valued at"],
            "notable_investors": [f"{subject} investors", f"{subject} backed by"],
        },
        "traction_profile": {
            "customers": [f"{subject} customers", f"{subject} customer case study", f"{subject} enterprise customers"],
            "partners": [f"{subject} partners", f"{subject} integrations"],
            "product_launches": [f"{subject} launch", f"{subject} product launch"],
            "revenue_signals": [f"{subject} revenue", f"{subject} ARR"],
            "deployment_signals": [f"{subject} deployed", f"{subject} production"],
        },
        "market_position": {
            "sub_sector": [f"{subject} agent security", f"{subject} AI security"],
            "is_new_category": [f"{subject} new category", f"{subject} first in market"],
            "competitors": [f"{subject} competitors", f"{subject} rivals"],
            "leader_signals": [f"{subject} leader", f"{subject} recognized"],
            "crowdedness": [f"{subject} crowded market", f"{subject} competitive landscape"],
        },
    }

    result: dict[str, list[str]] = {}
    for dimension, queries_list in queries.items():
        if dimension in avoid_dimensions and dimension not in focus_dimensions:
            queries_list = queries_list[:2]
        dimension_focus_fields = focus_fields_by_dimension.get(dimension, [])
        for field_name in dimension_focus_fields:
            queries_list.extend(focus_query_variants.get(dimension, {}).get(field_name, []))
        result[dimension] = [q for q in _dedupe_keep_order(queries_list) if q]
    return result


def build_official_page_urls(official_domain: str) -> list[str]:
    domain = _normalize_domain(official_domain)
    if not domain:
        return []

    urls: list[str] = []
    for paths in OFFICIAL_PAGE_PATHS.values():
        for path in paths:
            urls.append(urljoin(f"https://{domain}", path))
    return _dedupe_keep_order(urls)


def fetch_official_pages(official_domain: str, *, timeout: int = 8, max_pages: int = 6) -> list[dict[str, Any]]:
    urls = build_official_page_urls(official_domain)[:max_pages]
    pages: list[dict[str, Any]] = []
    for url in urls:
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0 (AI Invest Agent DD Enricher)"},
            )
            if response.status_code >= 400:
                continue
            content_type = response.headers.get("content-type", "").lower()
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                continue
            text = extract_text_from_html(response.text)
            if not text:
                continue
            pages.append(
                {
                    "source": "official_page",
                    "url": url,
                    "title": _extract_html_title(response.text) or url,
                    "text": text,
                }
            )
        except Exception:
            continue
    return pages


def extract_dimension_profile(
    *,
    dimension: str,
    company_name: str,
    normalized_name: str,
    official_domain: str,
    search_hits: list[dict[str, Any]],
    official_pages: list[dict[str, Any]],
    blocked_fields: set[str] | None = None,
) -> dict[str, Any]:
    blocked_fields = {str(item).strip() for item in (blocked_fields or set()) if str(item).strip()}
    fields: dict[str, Any] = _empty_fields_for_dimension(dimension, official_domain=official_domain)
    evidence: list[dict[str, Any]] = []
    texts = _blob_texts(search_hits, official_pages)

    if dimension == "business_profile":
        _extract_business_profile(fields, evidence, texts, company_name, normalized_name, official_domain, blocked_fields)
    elif dimension == "team_profile":
        _extract_team_profile(fields, evidence, texts, company_name, normalized_name, blocked_fields)
    elif dimension == "funding_profile":
        _extract_funding_profile(fields, evidence, texts, blocked_fields)
    elif dimension == "traction_profile":
        _extract_traction_profile(fields, evidence, texts, blocked_fields)
    elif dimension == "market_position":
        _extract_market_profile(fields, evidence, texts, blocked_fields)

    effective_fields = [field for field in DD_DIMENSION_FIELDS[dimension] if field not in blocked_fields]
    missing_fields = [field for field in effective_fields if _is_missing(fields.get(field))]
    confidence = _compute_confidence(fields, evidence, dimension, blocked_fields=blocked_fields)
    return {
        "fields": fields,
        "evidence": evidence,
        "missing_fields": missing_fields,
        "confidence": confidence,
    }


def extract_text_from_html(html: str) -> str:
    if not html:
        return ""

    stripped = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    stripped = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", stripped)
    stripped = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", stripped)
    stripped = re.sub(r"(?is)<svg[^>]*>.*?</svg>", " ", stripped)
    stripped = re.sub(r"(?is)<[^>]+>", " ", stripped)
    stripped = html_lib.unescape(stripped)
    stripped = re.sub(r"\s+", " ", stripped)
    return stripped.strip()


def _extract_html_title(html: str) -> str:
    match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html or "")
    if not match:
        return ""
    title = html_lib.unescape(match.group(1))
    return re.sub(r"\s+", " ", title).strip()


def _normalize_domain(domain: str) -> str:
    lowered = str(domain or "").strip().lower()
    if not lowered:
        return ""
    if lowered.startswith("https://"):
        lowered = lowered[8:]
    elif lowered.startswith("http://"):
        lowered = lowered[7:]
    if lowered.startswith("www."):
        lowered = lowered[4:]
    return lowered.strip("/")


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = str(item).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(str(item))
    return out


def _empty_fields_for_dimension(dimension: str, *, official_domain: str = "") -> dict[str, Any]:
    if dimension == "business_profile":
        return {
            "one_liner": "",
            "products_services": [],
            "target_customers": [],
            "use_cases": [],
            "official_domain": official_domain,
        }
    if dimension == "team_profile":
        return {
            "founders": [],
            "key_people": [],
            "prior_companies": [],
            "research_background": [],
        }
    if dimension == "funding_profile":
        return {
            "founded_year": "",
            "headquarters": "",
            "funding_rounds": [],
            "total_raised": "",
            "valuation": "",
            "notable_investors": [],
        }
    if dimension == "traction_profile":
        return {
            "customers": [],
            "partners": [],
            "product_launches": [],
            "revenue_signals": [],
            "deployment_signals": [],
        }
    if dimension == "market_position":
        return {
            "sub_sector": [],
            "is_new_category": None,
            "competitors": [],
            "leader_signals": [],
            "crowdedness": "",
        }
    return {}


def _blob_texts(search_hits: list[dict[str, Any]], official_pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blobs: list[dict[str, Any]] = []
    for item in search_hits:
        title = str(item.get("title") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        url = str(item.get("url") or "").strip()
        text = " ".join(part for part in [snippet, title] if part).strip()
        blobs.append({"source": item.get("source", "search"), "title": title, "snippet": snippet, "url": url, "text": text})
    for page in official_pages:
        blobs.append(
            {
                "source": page.get("source", "official_page"),
                "title": str(page.get("title") or "").strip(),
                "snippet": str(page.get("text") or "")[:500].strip(),
                "url": str(page.get("url") or "").strip(),
                "text": str(page.get("text") or "").strip(),
            }
        )
    return blobs


def _append_unique(container: list[str], values: list[str]) -> None:
    existing = {value.lower() for value in container}
    for value in values:
        cleaned = _clean_value(value)
        if not cleaned:
            continue
        if cleaned.lower() in existing:
            continue
        container.append(cleaned)
        existing.add(cleaned.lower())


def _clean_value(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -_,.;:!?/")


def _first_match(patterns: list[str], text: str, flags: int = re.IGNORECASE) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=flags)
        if match:
            groups = [group for group in match.groups() if group]
            if groups:
                return _clean_value(groups[0])
    return ""


def _extract_business_profile(
    fields: dict[str, Any],
    evidence: list[dict[str, Any]],
    texts: list[dict[str, Any]],
    company_name: str,
    normalized_name: str,
    official_domain: str,
    blocked_fields: set[str],
) -> None:
    company = normalized_name or company_name
    for blob in texts:
        text = blob["text"]
        lowered = text.lower()
        if company and company.lower() not in lowered and not any(hint in lowered for hint in BUSINESS_VERB_HINTS):
            continue

        sentence = _first_match(
            [
                rf"({re.escape(company)}[^.?!:;]{{0,160}}(?:provides|offers|builds|protects|secures|monitors|detects|helps|delivers|platform|solution|service)[^.?!]*)",
                r"([^.?!:;]{0,200}(?:provides|offers|builds|protects|secures|monitors|detects|helps|delivers)[^.?!]*)",
            ],
            text,
        )
        if sentence and not fields["one_liner"]:
            fields["one_liner"] = sentence

        if "products_services" not in blocked_fields:
            products = _collect_keyword_phrases(text, ["platform", "solution", "product", "service", "runtime", "guardrail", "protection", "monitoring", "testing"])
            _append_unique(fields["products_services"], products)

        if "target_customers" not in blocked_fields:
            customers = _collect_keyword_phrases(text, list(TARGET_CUSTOMER_HINTS))
            _append_unique(fields["target_customers"], customers)

        if "use_cases" not in blocked_fields:
            use_cases = _collect_keyword_phrases(text, list(USE_CASE_HINTS))
            _append_unique(fields["use_cases"], use_cases)

        if any(part for part in [fields["one_liner"], fields["products_services"], fields["target_customers"], fields["use_cases"]] if part):
            evidence.append(
                EvidenceBlob(
                    source=blob["source"],
                    dimension="business_profile",
                    title=blob["title"] or company,
                    snippet=blob["snippet"],
                    url=blob["url"],
                    reason="business signal matched",
                    confidence=70.0,
                ).to_dict()
            )

    fields["official_domain"] = official_domain
    if official_domain and not fields["one_liner"]:
        fields["one_liner"] = f"{company} operates on {official_domain}; more evidence is needed to confirm the exact product scope."


def _extract_team_profile(fields: dict[str, Any], evidence: list[dict[str, Any]], texts: list[dict[str, Any]], company_name: str, normalized_name: str, blocked_fields: set[str]) -> None:
    for blob in texts:
        text = blob["text"]
        lowered = text.lower()
        if not any(hint in lowered for hint in TEAM_HINTS):
            continue

        if "founders" not in blocked_fields:
            founder_names = _collect_person_names(text, ["founder", "co-founder", "founders", "cofounder", "ceo", "cto"])
            _append_unique(fields["founders"], founder_names)

        if "key_people" not in blocked_fields:
            key_people = _collect_person_names(text, ["leadership", "team", "executive", "cto", "ceo", "chief", "head of", "research"])
            _append_unique(fields["key_people"], key_people)

        if "prior_companies" not in blocked_fields:
            prior_companies = _collect_prior_companies(text)
            _append_unique(fields["prior_companies"], prior_companies)

        if "research_background" not in blocked_fields:
            research_background = _collect_research_background(text)
            _append_unique(fields["research_background"], research_background)

        if any(fields[field] for field in fields):
            evidence.append(
                EvidenceBlob(
                    source=blob["source"],
                    dimension="team_profile",
                    title=blob["title"] or (normalized_name or company_name),
                    snippet=blob["snippet"],
                    url=blob["url"],
                    reason="team signal matched",
                    confidence=65.0,
                ).to_dict()
            )


def _extract_funding_profile(fields: dict[str, Any], evidence: list[dict[str, Any]], texts: list[dict[str, Any]], blocked_fields: set[str]) -> None:
    for blob in texts:
        text = blob["text"]
        lowered = text.lower()
        if not any(hint in lowered for hint in FUNDING_HINTS):
            continue

        if "founded_year" not in blocked_fields and not fields["founded_year"]:
            founded_year = _first_match([r"(?:founded|founded in|established|launched in)\s+(20\d{2})"], text)
            if founded_year:
                fields["founded_year"] = founded_year

        if "headquarters" not in blocked_fields and not fields["headquarters"]:
            headquarters = _first_match([r"(?:headquartered in|based in)\s+([A-Z][A-Za-z0-9 ,.-]{2,60})"], text)
            if headquarters:
                fields["headquarters"] = headquarters

        if "funding_rounds" not in blocked_fields:
            round_text = _collect_keyword_phrases(text, ["seed", "pre-seed", "series a", "series b", "series c", "angel"])
            _append_unique(fields["funding_rounds"], round_text)

        if "total_raised" not in blocked_fields and not fields["total_raised"]:
            total_raised = _first_match(
                [
                    r"(\$?\d+(?:\.\d+)?\s?(?:m|million|bn|billion))",
                    r"(raised\s+\$?\d+(?:\.\d+)?\s?(?:m|million|bn|billion))",
                ],
                text,
            )
            if total_raised:
                fields["total_raised"] = total_raised

        if "valuation" not in blocked_fields and not fields["valuation"]:
            valuation = _first_match([r"(?:valuation|valued at)\s+([^\.;,]{2,80})"], text)
            if valuation:
                fields["valuation"] = valuation

        if "notable_investors" not in blocked_fields:
            investors = _collect_people_like_phrases(text, ["led by", "backed by", "investors include", "from", "with"])
            _append_unique(fields["notable_investors"], investors)

        if any(fields[field] for field in fields):
            evidence.append(
                EvidenceBlob(
                    source=blob["source"],
                    dimension="funding_profile",
                    title=blob["title"],
                    snippet=blob["snippet"],
                    url=blob["url"],
                    reason="funding signal matched",
                    confidence=65.0,
                ).to_dict()
            )


def _extract_traction_profile(fields: dict[str, Any], evidence: list[dict[str, Any]], texts: list[dict[str, Any]], blocked_fields: set[str]) -> None:
    for blob in texts:
        text = blob["text"]
        lowered = text.lower()
        if not any(hint in lowered for hint in TRACTION_HINTS):
            continue

        if "customers" not in blocked_fields:
            customers = _collect_people_like_phrases(text, ["used by", "customers include", "customer include", "adopted by", "trusted by"])
            _append_unique(fields["customers"], customers)

        if "partners" not in blocked_fields:
            partners = _collect_people_like_phrases(text, ["partner with", "partnership with", "integrates with", "integrated with"])
            _append_unique(fields["partners"], partners)

        if "product_launches" not in blocked_fields:
            launches = _collect_keyword_phrases(text, ["launched", "launch", "release", "released", "beta", "ga", "general availability"])
            _append_unique(fields["product_launches"], launches)

        if "revenue_signals" not in blocked_fields:
            revenue = _collect_keyword_phrases(text, ["arr", "revenue", "paid", "subscription", "contract", "billing"])
            _append_unique(fields["revenue_signals"], revenue)

        if "deployment_signals" not in blocked_fields:
            deploy = _collect_keyword_phrases(text, ["deployed", "deployment", "production", "pilot", "rolled out", "rollout", "live"])
            _append_unique(fields["deployment_signals"], deploy)

        if any(fields[field] for field in fields):
            evidence.append(
                EvidenceBlob(
                    source=blob["source"],
                    dimension="traction_profile",
                    title=blob["title"],
                    snippet=blob["snippet"],
                    url=blob["url"],
                    reason="traction signal matched",
                    confidence=60.0,
                ).to_dict()
            )


def _extract_market_profile(fields: dict[str, Any], evidence: list[dict[str, Any]], texts: list[dict[str, Any]], blocked_fields: set[str]) -> None:
    for blob in texts:
        text = blob["text"]
        lowered = text.lower()
        if not any(hint in lowered for hint in MARKET_HINTS):
            continue

        if "sub_sector" not in blocked_fields:
            subsector = _collect_keyword_phrases(
                text,
                [
                    "agent security",
                    "ai security",
                    "llm security",
                    "runtime protection",
                    "prompt security",
                    "red teaming",
                    "model security",
                    "guardrail",
                    "ai runtime protection",
                ],
            )
            _append_unique(fields["sub_sector"], subsector)

        if fields["is_new_category"] is None:
            if any(token in lowered for token in ["new category", "new frontier", "first", "only", "pioneer"]):
                fields["is_new_category"] = True
            elif any(token in lowered for token in ["competitor", "crowded", "crowdedness", "competitive"]):
                fields["is_new_category"] = False

        if "competitors" not in blocked_fields:
            competitors = _collect_competitor_phrases(text)
            _append_unique(fields["competitors"], competitors)

        if "leader_signals" not in blocked_fields:
            leader_signals = _collect_keyword_phrases(text, ["leader", "leading", "top", "trusted by", "recognized", "market leader"])
            _append_unique(fields["leader_signals"], leader_signals)

        if "crowdedness" not in blocked_fields and not fields["crowdedness"]:
            crowdedness = _infer_crowdedness(text)
            if crowdedness:
                fields["crowdedness"] = crowdedness

        if any(fields[field] for field in fields if field != "is_new_category") or fields["is_new_category"] is not None:
            evidence.append(
                EvidenceBlob(
                    source=blob["source"],
                    dimension="market_position",
                    title=blob["title"],
                    snippet=blob["snippet"],
                    url=blob["url"],
                    reason="market signal matched",
                    confidence=60.0,
                ).to_dict()
            )


def _collect_keyword_phrases(text: str, keywords: list[str]) -> list[str]:
    lowered = text.lower()
    matches: list[str] = []
    for keyword in keywords:
        if keyword in lowered:
            matches.append(keyword)
    return _dedupe_keep_order(matches)


def _collect_person_names(text: str, triggers: list[str]) -> list[str]:
    results: list[str] = []
    for trigger in triggers:
        pattern = rf"(?:{re.escape(trigger)})[:\s\-]+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){{1,3}})"
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = _clean_value(match.group(1))
            if candidate:
                candidate = re.sub(r"\b(?:and|with|from|at|the)\b.*$", "", candidate, flags=re.IGNORECASE).strip()
            if candidate and _looks_like_person_name(candidate):
                results.append(candidate)
    return _dedupe_keep_order(results)


def _collect_prior_companies(text: str) -> list[str]:
    patterns = [
        r"(?:ex-|formerly at|previously at|from)\s+([A-Z][A-Za-z0-9&]+(?:\s+[A-Z][A-Za-z0-9&]+){0,3})",
    ]
    results: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = _clean_value(match.group(1))
            if candidate:
                results.append(candidate)
    return _dedupe_keep_order(results)


def _collect_research_background(text: str) -> list[str]:
    keywords = ["phd", "professor", "researcher", "scientist", "stanford", "mit", "berkeley", "oxford", "cambridge", "eth"]
    lowered = text.lower()
    results = [word for word in keywords if word in lowered]
    return _dedupe_keep_order(results)


def _collect_people_like_phrases(text: str, triggers: list[str]) -> list[str]:
    results: list[str] = []
    for trigger in triggers:
        pattern = rf"(?:{re.escape(trigger)})[:\s]+([^.;\n]{{2,120}})"
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = _clean_value(match.group(1))
            if candidate:
                candidate = re.sub(r"^(?:and|the|a|an|customers?|partners?|employees?|team)\s+", "", candidate, flags=re.IGNORECASE).strip()
            if candidate:
                results.append(candidate)
    return _dedupe_keep_order(results)


def _looks_like_person_name(value: str) -> bool:
    parts = [part for part in re.split(r"\s+", str(value or "").strip()) if part]
    if len(parts) < 2:
        return False
    stopwords = {"and", "with", "from", "at", "the", "team", "customers", "founders"}
    for part in parts:
        normalized = part.strip(",.;:!?")
        if not normalized or normalized.lower() in stopwords:
            return False
        if normalized.isupper():
            continue
        if not normalized[0].isupper():
            return False
    return True


def _collect_competitor_phrases(text: str) -> list[str]:
    results: list[str] = []
    for pattern in [
        r"(?:competitors?|rivals?|alternatives?)[:\s]+([^.;\n]{2,160})",
        r"(?:against|vs\.?)\s+([^.;\n]{2,120})",
    ]:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = _clean_value(match.group(1))
            if candidate:
                results.append(candidate)
    return _dedupe_keep_order(results)


def _infer_crowdedness(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ["crowded", "competitive", "many competitors", "fragmented"]):
        return "high"
    if any(word in lowered for word in ["emerging", "new category", "few competitors", "early market"]):
        return "low"
    if any(word in lowered for word in ["category", "market", "competitive landscape"]):
        return "medium"
    return ""


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


def _compute_confidence(fields: dict[str, Any], evidence: list[dict[str, Any]], dimension: str, *, blocked_fields: set[str] | None = None) -> float:
    blocked_fields = blocked_fields or set()
    field_names = [field for field in DD_DIMENSION_FIELDS[dimension] if field not in blocked_fields]
    if not field_names:
        return 0.0
    filled = sum(1 for field in field_names if not _is_missing(fields.get(field)))
    coverage = filled / max(1, len(field_names))
    evidence_factor = min(1.0, len(evidence) / max(1, len(field_names)))
    confidence = (coverage * 70.0) + (evidence_factor * 30.0)
    return round(min(100.0, confidence), 2)

