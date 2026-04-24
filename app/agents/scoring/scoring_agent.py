from __future__ import annotations

import json
import re
import time
from typing import Any

from app.core.config import Settings
from app.core.db import Database
from app.models.scoring_curve import ScoringCurve
from app.models.scoring_policy import ScoringPolicy
from app.models.scoring_result import ScoringResult


class ScoringAgent:
    BIG_COMPANY_HINTS = {
        "microsoft",
        "google",
        "meta",
        "amazon",
        "apple",
        "openai",
        "ibm",
        "crowdstrike",
        "sequoia",
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

    CONTENT_HINTS = {
        "what is",
        "how to",
        "guide",
        "report",
        "analysis",
        "research",
        "startups to know",
        "market data",
        "funding data",
        "listicle",
        "explainer",
        "blog",
        "github",
        "open source",
        "webinar",
        "podcast",
        "article",
    }

    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self.curve = ScoringCurve()
        self.mvp_mode = bool(getattr(settings, "mvp_mode", False))

    def run(
        self,
        search_plan: dict,
        *,
        limit: int = 100,
        deadline_ts: float | None = None,
    ) -> dict[str, int | bool]:
        rows = self.db.get_scoring_candidates(limit=limit)
        policy = ScoringPolicy.from_dict(self.db.get_scoring_policy())
        long_memory = self.db.get_long_memory("planner_long_memory") or {}
        short_memory = self.db.get_latest_short_memory() or {}
        scored = 0
        recommended = 0
        watchlist = 0
        track_only = 0
        rejected = 0
        timed_out = False

        for row in rows:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                timed_out = True
                break
            result = self._score_candidate(
                row,
                search_plan,
                long_memory=long_memory,
                short_memory=short_memory,
                policy=policy,
            )

            self.db.upsert_score(
                lead_id=result.lead_id,
                base_score=result.raw_score,
                thesis_fit=result.thesis_fit_score * 20.0,
                evidence_strength=result.evidence_score * 20.0,
                final_score=result.final_score,
                score_reason=result.recommendation_reason,
                result=result,
            )

            if (
                self.curve.should_push_recommendation(
                    final_score=result.final_score,
                    hard_gate_passed=result.hard_gate_passed,
                )
                and result.recommendation_band in {"Strong Recommend", "Recommend"}
            ):
                self.db.update_lead_status(result.lead_id, "recommended")
                recommended += 1
            elif self.curve.should_watchlist(
                final_score=result.final_score,
                hard_gate_passed=result.hard_gate_passed,
            ):
                self.db.update_lead_status(result.lead_id, "watchlist")
                watchlist += 1
            elif result.recommendation_band == "Reject":
                self.db.update_lead_status(result.lead_id, "rejected")
                rejected += 1
            else:
                self.db.update_lead_status(result.lead_id, "watchlist")
                track_only += 1

            scored += 1

        return {
            "scored": scored,
            "recommended": recommended,
            "watchlist": watchlist,
            "track_only": track_only,
            "rejected": rejected,
            "input_count": len(rows),
            "remaining_count": max(0, len(rows) - scored),
            "timed_out": timed_out,
        }

    def _score_candidate(
        self,
        row: dict[str, Any],
        search_plan: dict[str, Any],
        *,
        long_memory: dict[str, Any],
        short_memory: dict[str, Any],
        policy: ScoringPolicy,
    ) -> ScoringResult:
        lead_id = self._row_int(row, "lead_id", 0)
        company_name = self._row_str(row, "company_name", "")
        normalized_name = self._row_str(row, "normalized_name", self._row_str(row, "candidate_name", company_name))
        official_domain = self._row_str(row, "official_domain", "")
        entity_type = self._row_str(row, "entity_type", "unknown")
        verification_status = self._row_str(row, "verification_status", "pending_review")
        source_hits = self._row_int(row, "source_hits", 0)
        completeness_score = float(self._row_value(row, "completeness_score", None) or self._legacy_completeness(row) or 0.0)
        dd_status = self._row_str(row, "dd_status", "dd_pending_review")

        business_profile = self._profile_dict(self._row_value(row, "business_profile_json", None))
        team_profile = self._profile_dict(self._row_value(row, "team_profile_json", None))
        funding_profile = self._profile_dict(self._row_value(row, "funding_profile_json", None))
        traction_profile = self._profile_dict(self._row_value(row, "traction_profile_json", None))
        market_position = self._profile_dict(self._row_value(row, "market_position_json", None))
        dd_overall = self._profile_dict(self._row_value(row, "dd_overall_json", None))
        questions = self._profile_list(self._row_value(row, "questions_json", None))
        evidence_json = self._profile_dict(self._row_value(row, "evidence_json", None))

        business_score, business_notes = self._score_business_profile(business_profile)
        team_score, team_notes = self._score_team_profile(team_profile)
        funding_score, funding_notes = self._score_funding_profile(funding_profile)
        traction_score, traction_notes = self._score_traction_profile(traction_profile)
        market_score, market_notes = self._score_market_profile(market_position)
        thesis_fit_score, thesis_fit_breakdown, thesis_notes = self._score_thesis_fit(
            row,
            business_profile,
            traction_profile,
            market_position,
            search_plan,
            long_memory=long_memory,
            short_memory=short_memory,
        )
        evidence_score, evidence_confidence, evidence_notes = self._score_evidence(
            dd_overall,
            source_hits,
            questions,
            evidence_json,
        )
        hard_gate_passed, hard_gate_reasons = self.curve.hard_gate(
            entity_type=entity_type,
            verification_status=verification_status,
            source_hits=source_hits,
            dd_status=dd_status,
            mvp_mode=self.mvp_mode,
        )
        candidate_ctx = self._build_candidate_context(
            row=row,
            lead_id=lead_id,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            business_profile=business_profile,
            team_profile=team_profile,
            funding_profile=funding_profile,
            traction_profile=traction_profile,
            market_position=market_position,
            dd_overall=dd_overall,
        )
        candidate_ctx["thesis_fit_breakdown"] = thesis_fit_breakdown
        policy_boost_score, policy_penalty_score, policy_notes = self._apply_scoring_policy(policy, candidate_ctx)
        policy_weights = policy.normalized_weights()

        raw_score = self.curve.compute_raw_score(
            {
                "business_score": business_score,
                "team_score": team_score,
                "funding_score": funding_score,
                "traction_score": traction_score,
                "market_score": market_score,
                "thesis_fit_score": thesis_fit_score,
                "evidence_score": evidence_score,
            },
            override_weights=policy_weights,
        )

        confidence_multiplier = self._confidence_multiplier(
            evidence_confidence=evidence_confidence,
            completeness_score=completeness_score,
            source_hits=source_hits,
            dd_status=dd_status,
            verification_status=verification_status,
        )
        penalty_score, penalty_notes = self._penalty_score(
            row=row,
            dd_status=dd_status,
            source_hits=source_hits,
            completeness_score=completeness_score,
            questions=questions,
            dd_overall=dd_overall,
        )
        penalty_score = round(min(25.0, penalty_score + policy_penalty_score), 2)
        boost_score = round(min(25.0, policy_boost_score), 2)
        final_score = round(max(0.0, min(100.0, raw_score * confidence_multiplier - penalty_score + boost_score)), 2)
        recommendation_band = self._recommendation_band(
            final_score,
            hard_gate_passed=hard_gate_passed,
            verification_status=verification_status,
            entity_type=entity_type,
        )
        recommendation_reason = self._recommendation_reason(
            row=row,
            business_score=business_score,
            team_score=team_score,
            funding_score=funding_score,
            traction_score=traction_score,
            market_score=market_score,
            thesis_fit_score=thesis_fit_score,
            evidence_score=evidence_score,
            raw_score=raw_score,
            confidence_multiplier=confidence_multiplier,
            boost_score=boost_score,
            penalty_score=penalty_score,
            final_score=final_score,
            recommendation_band=recommendation_band,
            business_notes=business_notes,
            team_notes=team_notes,
            funding_notes=funding_notes,
            traction_notes=traction_notes,
            market_notes=market_notes,
            thesis_notes=thesis_notes,
            evidence_notes=evidence_notes,
            penalty_notes=penalty_notes,
            completeness_score=completeness_score,
            source_hits=source_hits,
            dd_status=dd_status,
            thesis_fit_breakdown=thesis_fit_breakdown,
            hard_gate_passed=hard_gate_passed,
            hard_gate_reasons=hard_gate_reasons,
            policy_version=policy.version,
            policy_notes=policy_notes,
        )

        component_reasons = {
            "business_score": business_notes,
            "team_score": team_notes,
            "funding_score": funding_notes,
            "traction_score": traction_notes,
            "market_score": market_notes,
            "thesis_fit_score": thesis_notes,
            "thesis_fit_breakdown": [f"{key}={value:.2f}" for key, value in thesis_fit_breakdown.items()],
            "evidence_score": evidence_notes,
            "penalty_score": penalty_notes,
            "hard_gate": ["passed"] if hard_gate_passed else list(hard_gate_reasons),
            "policy_rules": policy_notes,
            "policy_boost_score": [f"{boost_score:.2f} final boost"] if boost_score else [],
            "policy_penalty_score": [f"{policy_penalty_score:.2f} raw penalty"] if policy_penalty_score else [],
            "needs_human_review": [str(verification_status).lower() == "likely_company"],
        }

        return ScoringResult(
            lead_id=lead_id,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            source_hits=source_hits,
            completeness_score=completeness_score,
            dd_status=dd_status,
            business_score=business_score,
            team_score=team_score,
            funding_score=funding_score,
            traction_score=traction_score,
            market_score=market_score,
            thesis_fit_score=thesis_fit_score,
            evidence_score=evidence_score,
            raw_score=raw_score,
            confidence_multiplier=confidence_multiplier,
            boost_score=boost_score,
            penalty_score=penalty_score,
            final_score=final_score,
            recommendation_band=recommendation_band,
            recommendation_reason=recommendation_reason,
            score_reason=recommendation_reason,
            thesis_fit_breakdown=thesis_fit_breakdown,
            policy_version=policy.version,
            matched_policy_rules=policy_notes,
            hard_gate_passed=hard_gate_passed,
            hard_gate_reasons=hard_gate_reasons,
            component_reasons=component_reasons,
            evidence_snapshot={
                "dd_overall": dd_overall,
                "questions": questions,
                "evidence_json": evidence_json,
                "policy": policy.to_dict(),
                "policy_context": candidate_ctx,
                "thesis_fit_breakdown": thesis_fit_breakdown,
                "hard_gate": {"passed": hard_gate_passed, "reasons": hard_gate_reasons},
            },
        )

    def _score_business_profile(self, profile: dict[str, Any]) -> tuple[float, list[str]]:
        fields = self._profile_fields(profile)
        score = 0.0
        notes: list[str] = []

        if self._has_text(fields.get("one_liner")):
            score += 1.25
            notes.append("one_liner present")
        if self._has_items(fields.get("products_services")):
            score += 1.0
            notes.append("products_services present")
        if self._has_items(fields.get("target_customers")):
            score += 1.0
            notes.append("target_customers present")
        if self._has_items(fields.get("use_cases")):
            score += 1.0
            notes.append("use_cases present")
        if self._has_text(fields.get("official_domain")):
            score += 0.75
            notes.append("official_domain present")
        if self._has_text(fields.get("one_liner")) and any(
            token in str(fields.get("one_liner") or "").lower()
            for token in ["enterprise", "security", "platform", "product", "service", "ai"]
        ):
            score += 0.5
            notes.append("one_liner contains product/security signal")

        return min(5.0, round(score, 2)), notes

    def _score_team_profile(self, profile: dict[str, Any]) -> tuple[float, list[str]]:
        fields = self._profile_fields(profile)
        score = 0.0
        notes: list[str] = []

        if self._has_items(fields.get("founders")):
            score += 2.0
            notes.append("founders identified")
        if self._has_items(fields.get("key_people")):
            score += 1.0
            notes.append("key_people identified")
        if self._has_items(fields.get("prior_companies")):
            score += 1.0
            notes.append("prior_companies identified")
        if self._has_items(fields.get("research_background")):
            score += 1.0
            notes.append("research_background identified")

        return min(5.0, round(score, 2)), notes

    def _score_funding_profile(self, profile: dict[str, Any]) -> tuple[float, list[str]]:
        fields = self._profile_fields(profile)
        score = 0.0
        notes: list[str] = []

        if self._has_text(fields.get("founded_year")):
            score += 0.75
            notes.append("founded_year present")
        if self._has_text(fields.get("headquarters")):
            score += 0.5
            notes.append("headquarters present")
        if self._has_items(fields.get("funding_rounds")):
            score += 1.25
            notes.append("funding_rounds identified")
        if self._has_text(fields.get("total_raised")):
            score += 1.0
            notes.append("total_raised present")
        if self._has_text(fields.get("valuation")):
            score += 0.75
            notes.append("valuation present")
        if self._has_items(fields.get("notable_investors")):
            score += 0.75
            notes.append("notable_investors identified")

        return min(5.0, round(score, 2)), notes

    def _score_traction_profile(self, profile: dict[str, Any]) -> tuple[float, list[str]]:
        fields = self._profile_fields(profile)
        score = 0.0
        notes: list[str] = []

        if self._has_items(fields.get("customers")):
            score += 2.0
            notes.append("customers identified")
        if self._has_items(fields.get("partners")):
            score += 0.75
            notes.append("partners identified")
        if self._has_items(fields.get("product_launches")):
            score += 1.0
            notes.append("product_launches identified")
        if self._has_items(fields.get("revenue_signals")):
            score += 0.75
            notes.append("revenue_signals identified")
        if self._has_items(fields.get("deployment_signals")):
            score += 0.5
            notes.append("deployment_signals identified")

        return min(5.0, round(score, 2)), notes

    def _score_market_profile(self, profile: dict[str, Any]) -> tuple[float, list[str]]:
        fields = self._profile_fields(profile)
        score = 0.0
        notes: list[str] = []

        if self._has_items(fields.get("sub_sector")):
            score += 1.4
            notes.append("sub_sector identified")
        if fields.get("is_new_category") is not None:
            score += 0.8
            notes.append("category signal available")
        if self._has_items(fields.get("competitors")):
            score += 0.9
            notes.append("competitors identified")
        if self._has_items(fields.get("leader_signals")):
            score += 0.9
            notes.append("leader_signals identified")
        if self._has_text(fields.get("crowdedness")):
            score += 1.0
            notes.append("crowdedness assessed")

        return min(5.0, round(score, 2)), notes

    def _score_thesis_fit(
        self,
        row: dict[str, Any],
        business_profile: dict[str, Any],
        traction_profile: dict[str, Any],
        market_position: dict[str, Any],
        search_plan: dict[str, Any],
        *,
        long_memory: dict[str, Any],
        short_memory: dict[str, Any],
    ) -> tuple[float, dict[str, float], list[str]]:
        searchable_text = " ".join(
            [
                self._row_str(row, "company_name", ""),
                self._row_str(row, "description", ""),
                self._row_str(row, "thesis_tags", ""),
                self._join_profile_text(business_profile),
                self._join_profile_text(traction_profile),
                self._join_profile_text(market_position),
            ]
        ).lower()
        notes: list[str] = []

        long_terms = self._extract_long_memory_terms(long_memory)
        short_terms = self._extract_short_memory_terms(short_memory)
        keyword_terms = self._collect_focus_terms(search_plan)
        human_pref_terms = self._collect_human_preference_terms(search_plan, long_memory)
        commercial_terms = self._collect_commercial_terms(search_plan, long_memory)

        long_hits = self._matched_terms(long_terms, searchable_text)
        short_hits = self._matched_terms(short_terms, searchable_text)
        keyword_hits = self._matched_terms(keyword_terms, searchable_text)
        human_hits = self._matched_terms(human_pref_terms, searchable_text)
        commercial_hits = self._matched_terms(commercial_terms, searchable_text)

        long_memory_match = self._bounded_match_score(len(long_hits), step=1.25)
        short_theme_match = self._bounded_match_score(len(short_hits), step=1.25)
        keyword_match = self._bounded_match_score(len(keyword_hits), step=0.65)
        human_preference_match = self._bounded_match_score(len(human_hits), step=2.0)
        commercial_signal_match = self._bounded_match_score(len(commercial_hits), step=1.1)

        if self._has_items(business_profile.get("target_customers")) or self._has_items(traction_profile.get("customers")):
            commercial_signal_match = min(5.0, round(commercial_signal_match + 0.6, 2))
            notes.append("customer-facing evidence reinforces commercial fit")

        breakdown = {
            "long_memory_match": round(long_memory_match, 2),
            "short_theme_match": round(short_theme_match, 2),
            "keyword_match": round(keyword_match, 2),
            "commercial_signal_match": round(commercial_signal_match, 2),
            "human_preference_match": round(human_preference_match, 2),
        }

        if long_hits:
            notes.append(f"long memory hits: {', '.join(long_hits[:4])}")
        if short_hits:
            notes.append(f"short theme hits: {', '.join(short_hits[:4])}")
        if keyword_hits:
            notes.append(f"keyword hits: {', '.join(keyword_hits[:5])}")
        if human_hits:
            notes.append(f"human preference hits: {', '.join(human_hits[:4])}")
        if commercial_hits:
            notes.append(f"commercial signals: {', '.join(commercial_hits[:4])}")

        thesis_fit_score = round(sum(breakdown.values()) / len(breakdown), 2)
        return min(5.0, thesis_fit_score), breakdown, notes or ["no thesis-fit signals matched"]

    def _score_evidence(
        self,
        dd_overall: dict[str, Any],
        source_hits: int,
        questions: list[dict[str, Any]],
        evidence_json: dict[str, Any],
    ) -> tuple[float, float, list[str]]:
        completeness_score = float(dd_overall.get("completeness_score") or 0.0)
        dd_status = str(dd_overall.get("dd_status") or "dd_pending_review")
        missing_dimensions = dd_overall.get("missing_dimensions") or []
        confidence = float(dd_overall.get("confidence") or 0.0)
        dimension_scores = self._profile_dict(evidence_json.get("dimension_scores"))

        score = 0.0
        notes: list[str] = []

        if source_hits >= 1:
            score += 1.0
            notes.append("at least one source hit")
        if source_hits >= 2:
            score += 0.9
            notes.append("two-source coverage")
        if source_hits >= 3:
            score += 0.6
            notes.append("multi-source coverage")
        if completeness_score >= 20:
            score += min(1.4, completeness_score / 100.0 * 2.0)
            notes.append(f"completeness {completeness_score:.1f}")
        if dd_status == "dd_done":
            score += 0.7
            notes.append("dd_done status")
        elif dd_status == "dd_partial":
            score += 0.4
            notes.append("dd_partial status")
        if confidence >= 60:
            score += 0.4
            notes.append(f"dd confidence {confidence:.1f}")
        if dimension_scores:
            score += min(0.6, len([k for k, v in dimension_scores.items() if float(v or 0) > 0]) * 0.12)
            notes.append("dimension scores present")
        if missing_dimensions:
            score -= min(0.6, len(missing_dimensions) * 0.15)
            notes.append(f"missing_dimensions={len(missing_dimensions)}")
        if questions:
            score -= min(0.5, len(questions) * 0.1)
            notes.append(f"open_questions={len(questions)}")

        evidence_confidence = confidence if confidence > 0 else min(100.0, round((max(0.0, score) / 5.0) * 100.0, 2))
        return max(0.0, min(5.0, round(score, 2))), evidence_confidence, notes

    def _confidence_multiplier(
        self,
        *,
        evidence_confidence: float,
        completeness_score: float,
        source_hits: int,
        dd_status: str,
        verification_status: str,
    ) -> float:
        evidence_factor = min(1.0, max(0.0, evidence_confidence / 100.0))
        completeness_factor = min(1.0, max(0.0, completeness_score / 100.0))
        source_factor = min(1.0, max(0.0, source_hits / 4.0))
        status_bonus = {
            "dd_done": 0.04,
            "dd_partial": 0.0,
            "dd_pending_review": -0.03,
            "dd_waiting_human": -0.06,
        }.get(dd_status, 0.0)
        multiplier = 0.62 + (0.16 * source_factor) + (0.16 * completeness_factor) + (0.10 * evidence_factor) + status_bonus
        if self.mvp_mode and str(verification_status or "").lower() == "likely_company":
            multiplier -= 0.05
        return round(max(0.60, min(1.10, multiplier)), 2)

    def _penalty_score(
        self,
        *,
        row: dict[str, Any],
        dd_status: str,
        source_hits: int,
        completeness_score: float,
        questions: list[dict[str, Any]],
        dd_overall: dict[str, Any],
    ) -> tuple[float, list[str]]:
        penalty = 0.0
        notes: list[str] = []

        company_name = self._row_str(row, "company_name", "")
        normalized_name = self._row_str(row, "normalized_name", self._row_str(row, "candidate_name", company_name))
        raw_title = self._row_str(row, "raw_title", "")
        official_domain = self._row_str(row, "official_domain", "")
        reject_reason = self._row_str(row, "reject_reason", "")
        verification_status = self._row_str(row, "verification_status", "").lower()
        verification_score = float(self._row_value(row, "verification_score", 0.0) or 0.0)

        subject_text = f"{company_name} {normalized_name} {raw_title} {official_domain} {reject_reason}".lower()
        if any(term in subject_text for term in self.BIG_COMPANY_HINTS):
            penalty += 8.0
            notes.append("big-company or publisher noise detected")
        if any(term in subject_text for term in self.CONTENT_HINTS):
            penalty += 6.0
            notes.append("content-like subject detected")
        if verification_status != "verified":
            if self.mvp_mode and verification_status == "likely_company":
                penalty += 0.75
                notes.append("verification_status=likely_company")
            else:
                penalty += 8.0
                notes.append(f"verification_status={verification_status or 'unknown'}")
        if verification_score and verification_score < 80.0:
            penalty += 2.0
            notes.append(f"verification_score={verification_score:.1f}")
        if dd_status == "dd_waiting_human":
            penalty += 10.0
            notes.append("awaiting human confirmation")
        if source_hits < 2:
            penalty += 4.0
            notes.append(f"source_hits={source_hits}")
        if completeness_score < 30.0:
            penalty += 4.0
            notes.append(f"low completeness={completeness_score:.1f}")
        elif completeness_score < 60.0:
            penalty += 2.0
            notes.append(f"moderate completeness={completeness_score:.1f}")

        missing_dimensions = dd_overall.get("missing_dimensions") or []
        if len(missing_dimensions) >= 3:
            penalty += 4.0
            notes.append(f"many missing dimensions={len(missing_dimensions)}")
        elif len(missing_dimensions) == 2:
            penalty += 2.0
            notes.append("two missing dimensions")

        subject_conflict = any(
            str(question.get("question_type") or "").lower() == "subject_conflict"
            or str(question.get("dimension") or "").lower() == "entity"
            for question in questions
        )
        if subject_conflict:
            penalty += 6.0
            notes.append("subject conflict signal")
        elif questions:
            penalty += min(3.0, len(questions) * 0.8)
            notes.append(f"open questions={len(questions)}")

        return min(25.0, round(penalty, 2)), notes

    def _recommendation_band(
        self,
        final_score: float,
        *,
        hard_gate_passed: bool,
        verification_status: str,
        entity_type: str,
    ) -> str:
        allowed_statuses = {"verified", "likely_company"} if self.mvp_mode else {"verified"}
        if str(entity_type or "").lower() != "company" or str(verification_status or "").lower() not in allowed_statuses:
            return "Reject"
        band = self.curve.recommendation_band(final_score)
        if not hard_gate_passed and band in {"Strong Recommend", "Recommend", "Watchlist"}:
            return "Track Only"
        return band

    def _recommendation_reason(
        self,
        *,
        row: dict[str, Any],
        business_score: float,
        team_score: float,
        funding_score: float,
        traction_score: float,
        market_score: float,
        thesis_fit_score: float,
        evidence_score: float,
        raw_score: float,
        confidence_multiplier: float,
        boost_score: float,
        penalty_score: float,
        final_score: float,
        recommendation_band: str,
        business_notes: list[str],
        team_notes: list[str],
        funding_notes: list[str],
        traction_notes: list[str],
        market_notes: list[str],
        thesis_notes: list[str],
        evidence_notes: list[str],
        penalty_notes: list[str],
        completeness_score: float,
        source_hits: int,
        dd_status: str,
        thesis_fit_breakdown: dict[str, float],
        hard_gate_passed: bool,
        hard_gate_reasons: list[str],
        policy_version: int,
        policy_notes: list[str],
    ) -> str:
        lead_name = self._row_str(row, "normalized_name", self._row_str(row, "company_name", ""))
        verification_status = self._row_str(row, "verification_status", "pending_review").lower()
        top_components = sorted(
            [
                ("traction", traction_score, traction_notes),
                ("business", business_score, business_notes),
                ("thesis", thesis_fit_score, thesis_notes),
                ("market", market_score, market_notes),
                ("team", team_score, team_notes),
                ("funding", funding_score, funding_notes),
                ("evidence", evidence_score, evidence_notes),
            ],
            key=lambda item: item[1],
            reverse=True,
        )
        positive_bits: list[str] = []
        for label, value, notes in top_components[:3]:
            note = notes[0] if notes else ""
            if note:
                positive_bits.append(f"{label} {value:.1f}/5 ({note})")
            else:
                positive_bits.append(f"{label} {value:.1f}/5")

        reason_parts = []
        if positive_bits:
            reason_parts.append("; ".join(positive_bits))
        reason_parts.append(f"raw={raw_score:.2f}")
        reason_parts.append(f"confidence x{confidence_multiplier:.2f}")
        reason_parts.append(f"boost={boost_score:.2f}")
        reason_parts.append(f"penalty={penalty_score:.2f}")
        reason_parts.append(f"final={final_score:.2f}")
        reason_parts.append(f"band={recommendation_band}")
        reason_parts.append(f"completeness={completeness_score:.1f}")
        reason_parts.append(f"source_hits={source_hits}")
        reason_parts.append(f"dd_status={dd_status}")
        reason_parts.append(f"policy_v{policy_version}")
        reason_parts.append(
            "thesis_fit="
            + ",".join(f"{key}:{value:.1f}" for key, value in thesis_fit_breakdown.items())
        )
        reason_parts.append("hard_gate=pass" if hard_gate_passed else f"hard_gate=blocked({'; '.join(hard_gate_reasons)})")
        if self.mvp_mode and verification_status == "likely_company":
            reason_parts.append("主体未完全验证，仍需人工复核")

        if penalty_notes:
            reason_parts.append("penalties: " + "; ".join(penalty_notes[:4]))
        if policy_notes:
            reason_parts.append("policy_rules: " + "; ".join(policy_notes[:4]))
        elif recommendation_band == "Reject":
            reason_parts.append("penalties: evidence too weak for confidence")

        return f"{lead_name}: " + " | ".join(reason_parts)

    def _build_candidate_context(
        self,
        *,
        row: dict[str, Any],
        lead_id: int,
        company_name: str,
        normalized_name: str,
        official_domain: str,
        business_profile: dict[str, Any],
        team_profile: dict[str, Any],
        funding_profile: dict[str, Any],
        traction_profile: dict[str, Any],
        market_position: dict[str, Any],
        dd_overall: dict[str, Any],
    ) -> dict[str, Any]:
        company_key = self._row_str(row, "company_key", "")
        if not company_key:
            company_key = self.db._company_key_from_name(normalized_name or company_name)
        candidate_text = " ".join(
            [
                company_name,
                normalized_name,
                self._row_str(row, "candidate_name", ""),
                self._row_str(row, "raw_title", ""),
                self._row_str(row, "description", ""),
                self._row_str(row, "thesis_tags", ""),
                self._row_str(row, "sources", ""),
                self._row_str(row, "business_summary", ""),
                self._row_str(row, "team_summary", ""),
                self._row_str(row, "funding_summary", ""),
                self._row_str(row, "traction_summary", ""),
                self._row_str(row, "industry_position", ""),
                official_domain,
                self._join_profile_text(business_profile),
                self._join_profile_text(team_profile),
                self._join_profile_text(funding_profile),
                self._join_profile_text(traction_profile),
                self._join_profile_text(market_position),
                self._join_profile_text(dd_overall),
            ]
        ).strip().lower()
        return {
            "lead_id": lead_id,
            "company_name": company_name,
            "normalized_name": normalized_name,
            "candidate_name": self._row_str(row, "candidate_name", ""),
            "official_domain": official_domain,
            "company_key": company_key,
            "raw_title": self._row_str(row, "raw_title", ""),
            "description": self._row_str(row, "description", ""),
            "thesis_tags": self._row_str(row, "thesis_tags", ""),
            "sources": self._row_str(row, "sources", ""),
            "business_summary": self._row_str(row, "business_summary", ""),
            "team_summary": self._row_str(row, "team_summary", ""),
            "funding_summary": self._row_str(row, "funding_summary", ""),
            "traction_summary": self._row_str(row, "traction_summary", ""),
            "industry_position": self._row_str(row, "industry_position", ""),
            "dd_status": self._row_str(row, "dd_status", "dd_pending_review"),
            "candidate_text": candidate_text,
            "search_text": candidate_text,
            "policy_version": 0,
        }

    def _apply_scoring_policy(self, policy: ScoringPolicy, candidate: dict[str, Any]) -> tuple[float, float, list[str]]:
        boost_score = 0.0
        penalty_score = 0.0
        notes: list[str] = []

        for rule in policy.boost_rules:
            if rule.matches(candidate):
                delta = max(0.0, float(rule.delta))
                boost_score += delta
                note = f"boost[{rule.scope}:{rule.term or rule.field}] +{delta:.2f}"
                if rule.reason:
                    note += f" ({rule.reason})"
                notes.append(note)

        for rule in policy.penalty_rules:
            if rule.matches(candidate):
                delta = max(0.0, float(rule.delta))
                penalty_score += delta
                note = f"penalty[{rule.scope}:{rule.term or rule.field}] +{delta:.2f}"
                if rule.reason:
                    note += f" ({rule.reason})"
                notes.append(note)

        return round(min(25.0, boost_score), 2), round(min(25.0, penalty_score), 2), notes

    def _collect_focus_terms(self, search_plan: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for query in search_plan.get("queries", []):
            for token in re.split(r"\s+", str(query).lower()):
                token = token.strip(" ,.;:!?\"'()[]{}")
                if len(token) >= 3:
                    terms.append(token)
        for key in ("sensitive_keywords", "human_preferences"):
            for item in search_plan.get(key, []):
                token = str(item).lower().strip()
                if len(token) >= 3:
                    terms.append(token)
        seen: set[str] = set()
        deduped: list[str] = []
        for term in terms:
            if term in seen:
                continue
            seen.add(term)
            deduped.append(term)
        return deduped[:40]

    def _extract_long_memory_terms(self, long_memory: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for item in long_memory.get("sub_sectors", []) or []:
            token = str(item or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        return self._dedupe_terms(terms, limit=20)

    def _extract_short_memory_terms(self, short_memory: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for theme in short_memory.get("emerging_themes", []) or []:
            if isinstance(theme, dict):
                theme_name = str(theme.get("theme") or "").strip().lower()
                if len(theme_name) >= 3:
                    terms.append(theme_name)
                for keyword in theme.get("keywords", []) or []:
                    token = str(keyword or "").strip().lower()
                    if len(token) >= 3:
                        terms.append(token)
        for keyword in short_memory.get("keywords", []) or []:
            token = str(keyword or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        return self._dedupe_terms(terms, limit=24)

    def _collect_human_preference_terms(self, search_plan: dict[str, Any], long_memory: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for item in search_plan.get("human_preferences", []) or []:
            token = str(item or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        source_policy = long_memory.get("human_preferences", []) or []
        for item in source_policy:
            token = str(item or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        return self._dedupe_terms(terms, limit=16)

    def _collect_commercial_terms(self, search_plan: dict[str, Any], long_memory: dict[str, Any]) -> list[str]:
        terms = [
            "enterprise customer",
            "enterprise deployment",
            "paid pilot",
            "arr",
            "revenue",
            "customers",
            "partners",
            "production deployment",
            "b2b",
        ]
        for item in search_plan.get("sensitive_keywords", []) or []:
            token = str(item or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        for item in long_memory.get("signal_dictionary", []) or []:
            token = str(item or "").strip().lower()
            if len(token) >= 3:
                terms.append(token)
        return self._dedupe_terms(terms, limit=24)

    @staticmethod
    def _matched_terms(terms: list[str], searchable_text: str) -> list[str]:
        hits: list[str] = []
        haystack = searchable_text.lower()
        for term in terms:
            token = str(term or "").strip().lower()
            if len(token) < 3:
                continue
            if token in haystack and token not in hits:
                hits.append(token)
        return hits

    @staticmethod
    def _bounded_match_score(match_count: int, *, step: float) -> float:
        return min(5.0, round(max(0.0, float(match_count)) * float(step), 2))

    @staticmethod
    def _dedupe_terms(terms: list[str], *, limit: int) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for term in terms:
            token = str(term or "").strip().lower()
            if len(token) < 3 or token in seen:
                continue
            seen.add(token)
            result.append(token)
            if len(result) >= limit:
                break
        return result

    def _profile_dict(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except Exception:
                return {}
            return dict(parsed) if isinstance(parsed, dict) else {}
        return {}

    def _profile_list(self, value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [dict(item) if isinstance(item, dict) else {"value": item} for item in value]
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except Exception:
                return []
            if isinstance(parsed, list):
                return [dict(item) if isinstance(item, dict) else {"value": item} for item in parsed]
        return []

    @staticmethod
    def _profile_fields(profile: dict[str, Any]) -> dict[str, Any]:
        fields = profile.get("fields") if isinstance(profile, dict) else {}
        return dict(fields) if isinstance(fields, dict) else {}

    @staticmethod
    def _has_text(value: Any) -> bool:
        return bool(str(value or "").strip())

    @staticmethod
    def _has_items(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return bool(value)

    @staticmethod
    def _join_profile_text(profile: dict[str, Any]) -> str:
        fields = profile.get("fields") if isinstance(profile, dict) else {}
        if not isinstance(fields, dict):
            return ""
        pieces: list[str] = []
        for value in fields.values():
            if isinstance(value, (list, tuple, set)):
                pieces.extend(str(item) for item in value if str(item).strip())
            elif value is not None:
                text = str(value).strip()
                if text:
                    pieces.append(text)
        return " ".join(pieces)

    @staticmethod
    def _legacy_completeness(row: dict[str, Any]) -> float:
        summaries = [
            ScoringAgent._row_value(row, "business_summary", None),
            ScoringAgent._row_value(row, "team_summary", None),
            ScoringAgent._row_value(row, "funding_summary", None),
            ScoringAgent._row_value(row, "traction_summary", None),
            ScoringAgent._row_value(row, "industry_position", None),
        ]
        filled = sum(1 for summary in summaries if str(summary or "").strip())
        return float(filled * 20)

    @staticmethod
    def _row_value(row: Any, key: str, default: Any = None) -> Any:
        if isinstance(row, dict):
            return row.get(key, default)
        try:
            if hasattr(row, "keys"):
                keys = row.keys()
                if key in keys:
                    return row[key]
        except Exception:
            pass
        try:
            return row[key]
        except Exception:
            return default

    @classmethod
    def _row_str(cls, row: Any, key: str, default: str = "") -> str:
        value = cls._row_value(row, key, default)
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    @classmethod
    def _row_int(cls, row: Any, key: str, default: int = 0) -> int:
        value = cls._row_value(row, key, default)
        try:
            return int(value)
        except Exception:
            return default
