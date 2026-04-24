from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Mapping


DEFAULT_SCORING_WEIGHTS: dict[str, float] = {
    "business_score": 0.10,
    "team_score": 0.16,
    "funding_score": 0.12,
    "traction_score": 0.22,
    "market_score": 0.14,
    "thesis_fit_score": 0.20,
    "evidence_score": 0.06,
}


DIMENSION_KEYWORDS: dict[str, tuple[str, ...]] = {
    "business_score": ("business", "业务", "产品", "服务", "one liner", "use case", "use cases", "product"),
    "team_score": ("team", "团队", "founder", "创始", "key person", "research", "background", "cto"),
    "funding_score": ("funding", "融资", "investor", "估值", "raised", "round", "seed", "series", "valuation"),
    "traction_score": ("traction", "客户", "customer", "customers", "revenue", "收入", "deploy", "deployment", "pilot", "paid"),
    "market_score": ("market", "市场", "赛道", "competitor", "competitors", "crowded", "category", "sector", "sub-sector"),
    "thesis_fit_score": ("agent security", "ai security", "llm security", "prompt injection", "red team", "thesis", "方向", "偏好"),
    "evidence_score": ("evidence", "证据", "confidence", "source", "completeness", "可信", "可靠", "source hits"),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _company_key(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return text


def _text_terms(text: str, *, limit: int = 8) -> list[str]:
    text = (text or "").strip().lower()
    if not text:
        return []
    tokens: list[str] = []
    for raw in re.split(r"[\s,.;:!?()【】\[\]{}<>/\\|]+", text):
        token = raw.strip("'\"“”‘’")
        if len(token) >= 3:
            tokens.append(token)
    if not tokens:
        tokens = [text[:80]]

    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
        if len(result) >= limit:
            break
    return result


def _dimension_hits(text: str) -> list[str]:
    haystack = (text or "").lower()
    hits: list[str] = []
    for dimension, keywords in DIMENSION_KEYWORDS.items():
        if any(keyword.lower() in haystack for keyword in keywords):
            hits.append(dimension)
    return hits


@dataclass(slots=True)
class ScoringPolicyRule:
    rule_id: str
    kind: str
    scope: str = "global"
    scope_key: str = "global"
    lead_id: int | None = None
    company_key: str = ""
    company_name: str = ""
    normalized_name: str = ""
    official_domain: str = ""
    field: str = "any"
    term: str = ""
    match_mode: str = "contains"
    delta: float = 0.0
    reason: str = ""
    feedback_id: int | None = None
    feedback_type: str = ""
    verdict: str = ""
    active: bool = True
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "kind": self.kind,
            "scope": self.scope,
            "scope_key": self.scope_key,
            "lead_id": self.lead_id,
            "company_key": self.company_key,
            "company_name": self.company_name,
            "normalized_name": self.normalized_name,
            "official_domain": self.official_domain,
            "field": self.field,
            "term": self.term,
            "match_mode": self.match_mode,
            "delta": float(self.delta),
            "reason": self.reason,
            "feedback_id": self.feedback_id,
            "feedback_type": self.feedback_type,
            "verdict": self.verdict,
            "active": self.active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ScoringPolicyRule":
        payload = dict(data or {})
        return cls(
            rule_id=str(payload.get("rule_id") or ""),
            kind=str(payload.get("kind") or "boost"),
            scope=str(payload.get("scope") or "global"),
            scope_key=str(payload.get("scope_key") or "global"),
            lead_id=int(payload["lead_id"]) if payload.get("lead_id") not in (None, "") else None,
            company_key=str(payload.get("company_key") or ""),
            company_name=str(payload.get("company_name") or ""),
            normalized_name=str(payload.get("normalized_name") or ""),
            official_domain=str(payload.get("official_domain") or ""),
            field=str(payload.get("field") or "any"),
            term=str(payload.get("term") or ""),
            match_mode=str(payload.get("match_mode") or "contains"),
            delta=float(payload.get("delta") or 0.0),
            reason=str(payload.get("reason") or ""),
            feedback_id=int(payload["feedback_id"]) if payload.get("feedback_id") not in (None, "") else None,
            feedback_type=str(payload.get("feedback_type") or ""),
            verdict=str(payload.get("verdict") or ""),
            active=bool(payload.get("active", True)),
            created_at=str(payload.get("created_at") or ""),
            updated_at=str(payload.get("updated_at") or ""),
        )

    def matches(self, candidate: Mapping[str, Any]) -> bool:
        if not self.active:
            return False

        candidate_text = self._candidate_text(candidate)
        candidate_lead_id = self._candidate_int(candidate, "lead_id")
        candidate_company_key = self._candidate_str(candidate, "company_key") or _company_key(
            self._candidate_str(candidate, "normalized_name") or self._candidate_str(candidate, "company_name")
        )
        candidate_normalized = self._candidate_str(candidate, "normalized_name")
        candidate_company = self._candidate_str(candidate, "company_name")
        candidate_domain = self._candidate_str(candidate, "official_domain")

        scope = (self.scope or "global").lower()
        if scope == "lead" and self.lead_id is not None and candidate_lead_id != self.lead_id:
            return False
        if scope == "company" and self.company_key:
            if self.company_key != candidate_company_key and self.company_key not in {
                _company_key(candidate_company),
                _company_key(candidate_normalized),
            }:
                return False
        if self.normalized_name:
            normalized_key = _company_key(self.normalized_name)
            if normalized_key and normalized_key not in {
                candidate_company_key,
                _company_key(candidate_company),
                _company_key(candidate_normalized),
            }:
                return False
        if self.official_domain and self.official_domain.lower() != candidate_domain.lower():
            if self.match_mode == "exact":
                return False

        if not self.term:
            return True

        field_value = candidate_text
        if self.field and self.field != "any":
            field_value = self._candidate_str(candidate, self.field)
            if not field_value:
                field_value = candidate_text

        term = self.term.lower().strip()
        haystack = field_value.lower()
        if self.match_mode == "exact":
            return haystack == term
        return term in haystack

    @staticmethod
    def _candidate_text(candidate: Mapping[str, Any]) -> str:
        fields = [
            candidate.get("company_name"),
            candidate.get("normalized_name"),
            candidate.get("candidate_name"),
            candidate.get("raw_title"),
            candidate.get("description"),
            candidate.get("business_summary"),
            candidate.get("team_summary"),
            candidate.get("funding_summary"),
            candidate.get("traction_summary"),
            candidate.get("industry_position"),
            candidate.get("official_domain"),
            candidate.get("sources"),
        ]
        return " ".join(str(value or "") for value in fields).strip().lower()

    @staticmethod
    def _candidate_str(candidate: Mapping[str, Any], key: str) -> str:
        value = candidate.get(key)
        return str(value or "").strip()

    @staticmethod
    def _candidate_int(candidate: Mapping[str, Any], key: str) -> int:
        value = candidate.get(key)
        try:
            return int(value)
        except Exception:
            return 0


@dataclass(slots=True)
class ScoringPolicy:
    policy_key: str = "default"
    version: int = 1
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_SCORING_WEIGHTS))
    boost_rules: list[ScoringPolicyRule] = field(default_factory=list)
    penalty_rules: list[ScoringPolicyRule] = field(default_factory=list)
    updated_at: str = ""
    source_feedback_id: int | None = None
    source_feedback_type: str = ""
    source_verdict: str = ""
    source_scope: str = ""
    source_scope_key: str = ""
    source_lead_id: int | None = None
    source_company_key: str = ""
    source_content: str = ""
    change_summary: str = ""

    @classmethod
    def default(cls, policy_key: str = "default") -> "ScoringPolicy":
        return cls(policy_key=policy_key, version=1, weights=dict(DEFAULT_SCORING_WEIGHTS), updated_at=_now_iso())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ScoringPolicy":
        payload = dict(data or {})
        weights = payload.get("weights") or payload.get("weights_json") or DEFAULT_SCORING_WEIGHTS
        boost_rules = payload.get("boost_rules") or payload.get("boost_rules_json") or []
        penalty_rules = payload.get("penalty_rules") or payload.get("penalty_rules_json") or []
        if isinstance(weights, str):
            try:
                weights = json.loads(weights)
            except Exception:
                weights = DEFAULT_SCORING_WEIGHTS
        if isinstance(boost_rules, str):
            try:
                boost_rules = json.loads(boost_rules)
            except Exception:
                boost_rules = []
        if isinstance(penalty_rules, str):
            try:
                penalty_rules = json.loads(penalty_rules)
            except Exception:
                penalty_rules = []

        return cls(
            policy_key=str(payload.get("policy_key") or "default"),
            version=int(payload.get("version") or 1),
            weights={str(k): float(v) for k, v in dict(weights).items()},
            boost_rules=[ScoringPolicyRule.from_dict(item) for item in boost_rules if isinstance(item, Mapping)],
            penalty_rules=[ScoringPolicyRule.from_dict(item) for item in penalty_rules if isinstance(item, Mapping)],
            updated_at=str(payload.get("updated_at") or ""),
            source_feedback_id=int(payload["source_feedback_id"]) if payload.get("source_feedback_id") not in (None, "") else None,
            source_feedback_type=str(payload.get("source_feedback_type") or ""),
            source_verdict=str(payload.get("source_verdict") or ""),
            source_scope=str(payload.get("source_scope") or ""),
            source_scope_key=str(payload.get("source_scope_key") or ""),
            source_lead_id=int(payload["source_lead_id"]) if payload.get("source_lead_id") not in (None, "") else None,
            source_company_key=str(payload.get("source_company_key") or ""),
            source_content=str(payload.get("source_content") or ""),
            change_summary=str(payload.get("change_summary") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_key": self.policy_key,
            "version": int(self.version),
            "weights": self.normalized_weights(),
            "boost_rules": [rule.to_dict() for rule in self.boost_rules],
            "penalty_rules": [rule.to_dict() for rule in self.penalty_rules],
            "updated_at": self.updated_at,
            "source_feedback_id": self.source_feedback_id,
            "source_feedback_type": self.source_feedback_type,
            "source_verdict": self.source_verdict,
            "source_scope": self.source_scope,
            "source_scope_key": self.source_scope_key,
            "source_lead_id": self.source_lead_id,
            "source_company_key": self.source_company_key,
            "source_content": self.source_content,
            "change_summary": self.change_summary,
        }

    def normalized_weights(self) -> dict[str, float]:
        weights = {key: max(0.03, float(value)) for key, value in self.weights.items()}
        for key in DEFAULT_SCORING_WEIGHTS:
            weights.setdefault(key, DEFAULT_SCORING_WEIGHTS[key])

        total = sum(weights.values()) or 1.0
        return {key: round(float(value) / total, 6) for key, value in weights.items()}

    def apply_feedback(self, feedback: Mapping[str, Any]) -> tuple["ScoringPolicy", dict[str, Any]]:
        payload = dict(feedback or {})
        verdict = str(payload.get("verdict") or payload.get("feedback_kind") or "like").strip().lower()
        feedback_type = str(payload.get("feedback_type") or "scoring_feedback").strip().lower()
        content = str(payload.get("content") or "").strip()
        lead_id = int(payload["lead_id"]) if payload.get("lead_id") not in (None, "") else None
        company_name = str(payload.get("company_name") or "").strip()
        normalized_name = str(payload.get("normalized_name") or company_name).strip()
        official_domain = str(payload.get("official_domain") or "").strip()
        company_key = str(payload.get("company_key") or _company_key(normalized_name or company_name)).strip()
        scope = str(payload.get("scope") or "").strip().lower()
        if not scope:
            scope = "lead" if lead_id is not None else "global"
        scope_key = str(payload.get("scope_key") or "").strip()
        if not scope_key:
            if scope == "lead" and lead_id is not None:
                scope_key = f"lead:{lead_id}"
            elif scope == "company" and company_key:
                scope_key = f"company:{company_key}"
            else:
                scope_key = "global"

        dimensions = _dimension_hits(content)
        if verdict == "prefer_sector":
            for dim in ("thesis_fit_score", "market_score"):
                if dim not in dimensions:
                    dimensions.append(dim)
        if not dimensions and verdict != "wrong_entity":
            dimensions = ["thesis_fit_score", "evidence_score"] if verdict in {"like", "prefer_sector"} else ["evidence_score"]

        weight_updates = self._build_weight_updates(verdict=verdict, dimensions=dimensions)
        weights = dict(self.weights)
        for key, delta in weight_updates.items():
            weights[key] = weights.get(key, DEFAULT_SCORING_WEIGHTS.get(key, 0.0)) + delta

        if verdict == "prefer_sector":
            weights["thesis_fit_score"] = weights.get("thesis_fit_score", DEFAULT_SCORING_WEIGHTS["thesis_fit_score"]) + 0.02
            weights["market_score"] = weights.get("market_score", DEFAULT_SCORING_WEIGHTS["market_score"]) + 0.015
        elif verdict == "like":
            weights["traction_score"] = weights.get("traction_score", DEFAULT_SCORING_WEIGHTS["traction_score"]) + 0.005
        elif verdict == "dislike":
            weights["evidence_score"] = weights.get("evidence_score", DEFAULT_SCORING_WEIGHTS["evidence_score"]) + 0.005
        elif verdict == "skip":
            weights["evidence_score"] = weights.get("evidence_score", DEFAULT_SCORING_WEIGHTS["evidence_score"]) + 0.008

        boost_rules, penalty_rules, rule_notes = self._build_rules(
            verdict=verdict,
            content=content,
            lead_id=lead_id,
            scope=scope,
            scope_key=scope_key,
            company_key=company_key,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            feedback_type=feedback_type,
        )

        updated = ScoringPolicy(
            policy_key=self.policy_key,
            version=self.version + 1,
            weights=weights,
            boost_rules=[*self.boost_rules, *boost_rules],
            penalty_rules=[*self.penalty_rules, *penalty_rules],
            updated_at=_now_iso(),
            source_feedback_id=int(payload["feedback_id"]) if payload.get("feedback_id") not in (None, "") else None,
            source_feedback_type=feedback_type,
            source_verdict=verdict,
            source_scope=scope,
            source_scope_key=scope_key,
            source_lead_id=lead_id,
            source_company_key=company_key,
            source_content=content,
            change_summary=self._build_change_summary(verdict=verdict, dimensions=dimensions, rule_notes=rule_notes),
        )
        updated.weights = updated.normalized_weights()

        event = {
            "policy_key": updated.policy_key,
            "version": updated.version,
            "feedback_id": updated.source_feedback_id,
            "lead_id": lead_id,
            "company_key": company_key,
            "company_name": company_name,
            "normalized_name": normalized_name,
            "official_domain": official_domain,
            "feedback_type": feedback_type,
            "verdict": verdict,
            "scope": scope,
            "scope_key": scope_key,
            "content": content,
            "change_json": {
                "weight_updates": weight_updates,
                "boost_rules_added": [rule.to_dict() for rule in boost_rules],
                "penalty_rules_added": [rule.to_dict() for rule in penalty_rules],
                "dimensions": dimensions,
                "rule_notes": rule_notes,
            },
            "change_summary": updated.change_summary,
            "created_at": updated.updated_at,
        }
        return updated, event

    @staticmethod
    def _build_weight_updates(*, verdict: str, dimensions: list[str]) -> dict[str, float]:
        if not dimensions:
            dimensions = ["thesis_fit_score", "evidence_score"]
        base_delta = {
            "like": 0.015,
            "dislike": 0.012,
            "skip": 0.010,
            "prefer_sector": 0.020,
            "wrong_entity": 0.0,
        }.get(verdict, 0.010)
        updates: dict[str, float] = {}
        for dimension in dimensions:
            updates[dimension] = updates.get(dimension, 0.0) + base_delta
        return updates

    @staticmethod
    def _build_change_summary(*, verdict: str, dimensions: list[str], rule_notes: list[str]) -> str:
        dimension_bits = ", ".join(sorted(set(dimensions))) if dimensions else "no inferred dimensions"
        rule_bits = "; ".join(rule_notes[:4]) if rule_notes else "no rule updates"
        return f"{verdict}: {dimension_bits}; {rule_bits}"

    def _build_rules(
        self,
        *,
        verdict: str,
        content: str,
        lead_id: int | None,
        scope: str,
        scope_key: str,
        company_key: str,
        company_name: str,
        normalized_name: str,
        official_domain: str,
        feedback_type: str,
    ) -> tuple[list[ScoringPolicyRule], list[ScoringPolicyRule], list[str]]:
        boost_rules: list[ScoringPolicyRule] = []
        penalty_rules: list[ScoringPolicyRule] = []
        rule_notes: list[str] = []

        def emit_rule(
            *,
            kind: str,
            scope_value: str,
            scope_key_value: str,
            delta: float,
            term: str,
            field: str,
            match_mode: str,
            suffix: str | int,
            reason: str,
        ) -> ScoringPolicyRule:
            return ScoringPolicyRule(
                rule_id=self._rule_id(kind, verdict, scope_key_value, term, suffix),
                kind=kind,
                scope=scope_value,
                scope_key=scope_key_value,
                lead_id=lead_id,
                company_key=company_key,
                company_name=company_name,
                normalized_name=normalized_name,
                official_domain=official_domain,
                field=field,
                term=term,
                match_mode=match_mode,
                delta=delta,
                reason=reason,
                feedback_type=feedback_type,
                verdict=verdict,
                active=True,
                created_at=_now_iso(),
                updated_at=_now_iso(),
            )

        scope_variants: list[tuple[str, str]] = [(scope, scope_key)]
        if scope == "lead" and company_key:
            scope_variants.append(("company", f"company:{company_key}"))

        subject_terms = _text_terms(content, limit=6)
        subject_terms = [term for term in subject_terms if term not in {"lead", "company", "sector"}]
        focus_phrases = [
            phrase
            for phrase in DIMENSION_KEYWORDS.get("thesis_fit_score", ())
            if phrase and phrase.lower() in content.lower()
        ]
        for phrase in focus_phrases:
            if phrase not in subject_terms:
                subject_terms.append(phrase)
        if normalized_name:
            subject_terms.append(normalized_name)
        if company_name and company_name not in subject_terms:
            subject_terms.append(company_name)

        if verdict in {"like", "prefer_sector"}:
            for idx, term in enumerate(subject_terms[:4]):
                for scope_value, scope_key_value in scope_variants:
                    rule = emit_rule(
                        kind="boost",
                        scope_value=scope_value,
                        scope_key_value=scope_key_value,
                        delta=2.0 if verdict == "prefer_sector" else 1.25,
                        term=term,
                        field="search_text",
                        match_mode="contains",
                        suffix=f"{idx}-{scope_value}",
                        reason=f"human {verdict} feedback",
                    )
                    boost_rules.append(rule)
                rule_notes.append(f"boost:{term}")
        elif verdict in {"dislike", "skip"}:
            for idx, term in enumerate(subject_terms[:4]):
                for scope_value, scope_key_value in scope_variants:
                    rule = emit_rule(
                        kind="penalty",
                        scope_value=scope_value,
                        scope_key_value=scope_key_value,
                        delta=1.5 if verdict == "dislike" else 1.0,
                        term=term,
                        field="search_text",
                        match_mode="contains",
                        suffix=f"{idx}-{scope_value}",
                        reason=f"human {verdict} feedback",
                    )
                    penalty_rules.append(rule)
                rule_notes.append(f"penalty:{term}")
        elif verdict == "wrong_entity":
            for field, term in [
                ("normalized_name", normalized_name),
                ("company_name", company_name),
                ("official_domain", official_domain),
            ]:
                if not term:
                    continue
                for scope_value, scope_key_value in scope_variants:
                    rule = emit_rule(
                        kind="penalty",
                        scope_value=scope_value,
                        scope_key_value=scope_key_value,
                        delta=20.0,
                        term=term,
                        field=field,
                        match_mode="exact",
                        suffix=f"{field}-{scope_value}",
                        reason="human flagged wrong entity",
                    )
                    penalty_rules.append(rule)
                rule_notes.append(f"block:{field}:{term}")

        return boost_rules, penalty_rules, rule_notes

    @staticmethod
    def _rule_id(kind: str, verdict: str, scope_key: str, term: str, suffix: str | int) -> str:
        raw = f"{kind}|{verdict}|{scope_key}|{term}|{suffix}"
        return sha1(raw.encode("utf-8")).hexdigest()[:16]
