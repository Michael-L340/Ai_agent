from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.scoring.scoring_agent import ScoringAgent
from app.core.db import Database


class ScoringCurveThesisFitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()
        self.scoring_agent = ScoringAgent(self.db, SimpleNamespace(recommend_score_threshold=75.0))
        self.db.set_long_memory(
            "planner_long_memory",
            {
                "sub_sectors": ["agent security", "runtime protection", "insurance ai security"],
                "signal_dictionary": ["enterprise customer", "paid pilot", "enterprise deployment"],
                "negative_filters": ["what is", "guide"],
                "source_policy": {"channel_status": {"brave": True, "bocha": True}},
                "human_preferences": ["insurance ai security", "agent security"],
                "version": 1,
            },
        )
        self.db.set_short_memory(
            "2026-04-23",
            {
                "emerging_themes": [
                    {
                        "theme": "insurance ai security",
                        "priority": 5,
                        "keywords": ["insurance ai security", "runtime protection"],
                        "source_suggestions": ["brave", "bocha"],
                        "days_active": 3,
                        "promote_candidate": True,
                    }
                ],
                "priority": ["insurance ai security", "agent security"],
                "keywords": ["insurance ai security", "agent security", "enterprise customer", "paid pilot"],
                "source_suggestions": {"insurance ai security": ["brave", "bocha"]},
                "days_active": {"insurance ai security": 3},
                "promote_candidate": ["insurance ai security"],
            },
        )

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _insert_lead(self, company_name: str, *, raw_title: str, official_domain: str, source: str = "brave") -> int:
        lead_id, created = self.db.upsert_lead(
            company_name=company_name,
            source=source,
            description="AI security platform for enterprise insurance teams",
            thesis_tags="agent security, runtime protection, insurance ai security",
            resolution={
                "raw_title": raw_title,
                "candidate_name": company_name,
                "normalized_name": company_name,
                "entity_type": "company",
                "official_domain": official_domain,
                "verification_status": "verified",
                "verification_score": 96.0,
                "reject_reason": "",
            },
        )
        self.assertTrue(created)
        return int(lead_id)

    def _insert_dd_report(
        self,
        lead_id: int,
        *,
        company_name: str,
        source_hits: int = 3,
        dd_status: str = "dd_partial",
        raw_business: str = "Enterprise AI security platform for runtime protection.",
        raw_customers: list[str] | None = None,
    ) -> None:
        customers = raw_customers or ["Fortune 500 insurers"]
        report = {
            "lead_id": lead_id,
            "company_name": company_name,
            "normalized_name": company_name,
            "official_domain": f"{company_name.lower().replace(' ', '')}.com",
            "source_hits": source_hits,
            "dd_status": dd_status,
            "completeness_score": 84.0,
            "business_profile": {
                "fields": {
                    "one_liner": raw_business,
                    "products_services": ["runtime protection", "ai security platform"],
                    "target_customers": ["insurance enterprises", "security teams"],
                    "use_cases": ["agent security", "runtime protection"],
                    "official_domain": f"{company_name.lower().replace(' ', '')}.com",
                },
                "evidence": [{"source": "official_page", "reason": "about"}],
                "missing_fields": [],
                "confidence": 88.0,
            },
            "team_profile": {
                "fields": {
                    "founders": ["Jane Doe"],
                    "key_people": ["Alice CTO"],
                    "prior_companies": ["OpenAI"],
                    "research_background": ["MIT"],
                },
                "evidence": [{"source": "official_page", "reason": "team"}],
                "missing_fields": [],
                "confidence": 80.0,
            },
            "funding_profile": {
                "fields": {
                    "founded_year": "2023",
                    "headquarters": "New York",
                    "funding_rounds": ["seed"],
                    "total_raised": "$4M",
                    "valuation": "",
                    "notable_investors": ["A16Z"],
                },
                "evidence": [{"source": "brave", "reason": "funding"}],
                "missing_fields": ["valuation"],
                "confidence": 72.0,
            },
            "traction_profile": {
                "fields": {
                    "customers": customers,
                    "partners": ["cloud marketplace"],
                    "product_launches": ["general availability"],
                    "revenue_signals": ["paid pilot"],
                    "deployment_signals": ["production deployment"],
                },
                "evidence": [{"source": "official_page", "reason": "customers"}],
                "missing_fields": [],
                "confidence": 82.0,
            },
            "market_position": {
                "fields": {
                    "sub_sector": ["agent security", "runtime protection"],
                    "is_new_category": True,
                    "competitors": ["point solutions"],
                    "leader_signals": ["recognized leader"],
                    "crowdedness": "medium",
                },
                "evidence": [{"source": "bocha", "reason": "market map"}],
                "missing_fields": [],
                "confidence": 78.0,
            },
            "dd_overall": {
                "dd_status": dd_status,
                "completeness_score": 84.0,
                "source_hits": source_hits,
                "summary": "Structured DD exists.",
                "missing_dimensions": [],
                "confidence": 81.0,
                "generated_at": "2026-04-23T12:00:00+00:00",
            },
            "questions": [],
            "business_summary": raw_business,
            "team_summary": "Founders identified.",
            "funding_summary": "Seed round and investors identified.",
            "traction_summary": "Customers and paid pilot signals present.",
            "industry_position": "Agent security and runtime protection.",
            "evidence_json": {
                "dimension_scores": {
                    "business_profile": 4.5,
                    "team_profile": 4.0,
                    "funding_profile": 3.5,
                    "traction_profile": 4.7,
                    "market_position": 4.1,
                }
            },
        }
        self.db.upsert_dd_report(report=report)

    def test_high_thesis_fit_company_is_recommended(self) -> None:
        lead_id = self._insert_lead(
            "Protect AI",
            raw_title="Protect AI for AI Agent Security",
            official_domain="protectai.com",
        )
        self._insert_dd_report(lead_id, company_name="Protect AI")

        stats = self.scoring_agent.run(
            search_plan={
                "queries": ["insurance ai security startup funding", "agent security runtime protection"],
                "sensitive_keywords": ["enterprise customer", "paid pilot"],
                "human_preferences": ["insurance ai security"],
            }
        )

        self.assertEqual(stats["scored"], 1)
        record = self.db.get_company_analysis_for_lead(lead_id)
        self.assertIsNotNone(record)
        record = dict(record or {})
        self.assertGreaterEqual(float(record["final_score"]), 82.0)
        self.assertIn(record["recommendation_band"], {"Recommend", "Strong Recommend"})
        self.assertIsInstance(record.get("thesis_fit_breakdown"), dict)
        self.assertGreater(float(record["thesis_fit_breakdown"]["human_preference_match"]), 0.0)
        recommendations = self.db.get_recommendations(min_score=0, limit=10)
        self.assertEqual(len(recommendations), 1)

    def test_article_like_entity_is_blocked_by_hard_gate(self) -> None:
        lead_id = self._insert_lead(
            "Trent AI",
            raw_title="AI agent security startup Trent AI launch",
            official_domain="trentai.com",
        )
        self._insert_dd_report(
            lead_id,
            company_name="Trent AI",
            source_hits=1,
            dd_status="dd_pending_review",
            raw_business="Launch article about an AI security startup.",
            raw_customers=[],
        )

        self.scoring_agent.run(
            search_plan={
                "queries": ["agent security startup funding"],
                "sensitive_keywords": ["enterprise customer"],
                "human_preferences": ["agent security"],
            }
        )

        record = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        score_breakdown = dict(record.get("score_breakdown") or {})
        hard_gate = score_breakdown.get("evidence_snapshot", {}).get("hard_gate") or {}
        self.assertFalse(bool(hard_gate.get("passed")))
        self.assertIn(record.get("recommendation_band"), {"Reject", "Track Only"})
        self.assertEqual(len(self.db.get_recommendations(min_score=0, limit=10)), 0)

    def test_prefer_sector_feedback_increases_thesis_fit(self) -> None:
        lead_id = self._insert_lead(
            "Capsule Security",
            raw_title="Capsule Security raises new round",
            official_domain="capsulesecurity.io",
        )
        self._insert_dd_report(lead_id, company_name="Capsule Security")

        base_stats = self.scoring_agent.run(
            search_plan={
                "queries": ["agent security startup funding"],
                "sensitive_keywords": ["enterprise customer"],
                "human_preferences": [],
            }
        )
        self.assertEqual(base_stats["scored"], 1)
        base_record = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        base_thesis = float(base_record["thesis_fit_score"])

        self.db.update_scoring_policy_from_feedback(
            {
                "feedback_id": 1,
                "verdict": "prefer_sector",
                "feedback_type": "scoring_feedback",
                "content": "insurance ai security",
                "lead_id": lead_id,
                "company_name": "Capsule Security",
                "normalized_name": "Capsule Security",
                "official_domain": "capsulesecurity.io",
                "company_key": "capsule-security",
                "scope": "company",
                "scope_key": "company:capsule-security",
            }
        )

        self.scoring_agent.run(
            search_plan={
                "queries": ["insurance ai security startup funding", "agent security startup funding"],
                "sensitive_keywords": ["enterprise customer"],
                "human_preferences": ["insurance ai security"],
            }
        )
        updated_record = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        self.assertGreaterEqual(float(updated_record["thesis_fit_score"]), base_thesis)
        self.assertGreaterEqual(int(updated_record["policy_version"] or 0), 2)

    def test_wrong_entity_feedback_adds_penalty(self) -> None:
        lead_id = self._insert_lead(
            "Artemis",
            raw_title="AI security startup Artemis exits stealth",
            official_domain="artemis.ai",
        )
        self._insert_dd_report(lead_id, company_name="Artemis")

        self.scoring_agent.run(
            search_plan={
                "queries": ["agent security startup funding"],
                "sensitive_keywords": ["enterprise customer"],
                "human_preferences": ["agent security"],
            }
        )
        baseline = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        baseline_penalty = float(baseline["penalty_score"])

        self.db.update_scoring_policy_from_feedback(
            {
                "feedback_id": 2,
                "verdict": "wrong_entity",
                "feedback_type": "scoring_feedback",
                "content": "主体错了，不是这个公司",
                "lead_id": lead_id,
                "company_name": "Artemis",
                "normalized_name": "Artemis",
                "official_domain": "artemis.ai",
                "company_key": "artemis",
                "scope": "company",
                "scope_key": "company:artemis",
            }
        )

        self.scoring_agent.run(
            search_plan={
                "queries": ["agent security startup funding"],
                "sensitive_keywords": ["enterprise customer"],
                "human_preferences": ["agent security"],
            }
        )
        updated = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        self.assertGreater(float(updated["penalty_score"]), baseline_penalty)
        matched_rules = updated.get("matched_policy_rules") or []
        self.assertTrue(matched_rules)


if __name__ == "__main__":
    unittest.main()
