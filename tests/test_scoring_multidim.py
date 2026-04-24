from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.scoring.scoring_agent import ScoringAgent
from app.core.db import Database


class ScoringMultiDimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _insert_lead(self, company_name: str, *, official_domain: str = "protectai.com") -> int:
        lead_id, created = self.db.upsert_lead(
            company_name=company_name,
            source="brave",
            description="AI security platform for enterprise teams",
            thesis_tags="ai security, enterprise, agent security",
            resolution={
                "raw_title": f"{company_name} security platform",
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

    def _insert_dd_report(self, lead_id: int) -> None:
        report = {
            "lead_id": lead_id,
            "company_name": "Protect AI",
            "normalized_name": "Protect AI",
            "official_domain": "protectai.com",
            "source_hits": 4,
            "dd_status": "dd_partial",
            "completeness_score": 88.0,
            "business_profile": {
                "fields": {
                    "one_liner": "Protect AI provides AI security for enterprise teams.",
                    "products_services": ["AI security platform", "runtime protection"],
                    "target_customers": ["enterprise teams", "security teams"],
                    "use_cases": ["prompt injection defense", "agent security"],
                    "official_domain": "protectai.com",
                },
                "evidence": [{"source": "official_page", "reason": "about page"}],
                "missing_fields": [],
                "confidence": 92.0,
            },
            "team_profile": {
                "fields": {
                    "founders": ["Jane Doe", "John Roe"],
                    "key_people": ["CTO Alice Smith"],
                    "prior_companies": ["OpenAI"],
                    "research_background": ["MIT"],
                },
                "evidence": [{"source": "official_page", "reason": "team page"}],
                "missing_fields": [],
                "confidence": 88.0,
            },
            "funding_profile": {
                "fields": {
                    "founded_year": "2022",
                    "headquarters": "San Francisco",
                    "funding_rounds": ["seed"],
                    "total_raised": "$7M",
                    "valuation": "",
                    "notable_investors": ["Sequoia", "A16Z"],
                },
                "evidence": [{"source": "brave", "reason": "funding article"}],
                "missing_fields": ["valuation"],
                "confidence": 74.0,
            },
            "traction_profile": {
                "fields": {
                    "customers": ["Fortune 500 enterprises"],
                    "partners": ["security integrations"],
                    "product_launches": ["runtime protection platform"],
                    "revenue_signals": ["paid pilot"],
                    "deployment_signals": ["production deployment"],
                },
                "evidence": [{"source": "official_page", "reason": "customers page"}],
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
                "evidence": [{"source": "bocha", "reason": "category article"}],
                "missing_fields": [],
                "confidence": 79.0,
            },
            "dd_overall": {
                "dd_status": "dd_partial",
                "completeness_score": 88.0,
                "source_hits": 4,
                "summary": "Strong AI security company with enterprise traction and clear product surface.",
                "missing_dimensions": [],
                "confidence": 83.0,
                "generated_at": "2026-04-22T20:34:02+00:00",
            },
            "questions": [],
            "business_summary": "Protect AI provides AI security for enterprise teams.",
            "team_summary": "Founders and CTO identified.",
            "funding_summary": "Seed funding, $7M raised.",
            "traction_summary": "Fortune 500 enterprise customers and paid pilot signal.",
            "industry_position": "Agent security and runtime protection.",
            "evidence_json": {
                "dimension_scores": {
                    "business_profile": 4.8,
                    "team_profile": 4.4,
                    "funding_profile": 3.4,
                    "traction_profile": 4.6,
                    "market_position": 4.2,
                },
                "questions": [],
            },
        }
        self.db.upsert_dd_report(report=report)

    def test_scoring_result_contains_all_components(self) -> None:
        lead_id = self._insert_lead("Protect AI")
        self._insert_dd_report(lead_id)

        scoring_agent = ScoringAgent(self.db, SimpleNamespace(recommend_score_threshold=75.0))
        stats = scoring_agent.run(
            search_plan={
                "queries": ["AI security enterprise customers", "agent security runtime protection"],
                "sensitive_keywords": ["enterprise customer", "paid pilot", "ARR"],
                "human_preferences": ["agent security"],
            }
        )

        self.assertEqual(stats["scored"], 1)
        record = self.db.get_company_analysis_for_lead(lead_id)
        self.assertIsNotNone(record)
        record = dict(record or {})

        for key in [
            "business_score",
            "team_score",
            "funding_score",
            "traction_score",
            "market_score",
            "thesis_fit_score",
            "evidence_score",
            "raw_score",
            "confidence_multiplier",
            "penalty_score",
            "final_score",
            "recommendation_band",
            "recommendation_reason",
            "score_breakdown_json",
        ]:
            self.assertIn(key, record)

        self.assertGreaterEqual(float(record["business_score"]), 0.0)
        self.assertLessEqual(float(record["business_score"]), 5.0)
        self.assertGreaterEqual(float(record["team_score"]), 0.0)
        self.assertLessEqual(float(record["team_score"]), 5.0)
        self.assertGreaterEqual(float(record["final_score"]), 0.0)
        self.assertLessEqual(float(record["final_score"]), 100.0)
        self.assertIn(record["recommendation_band"], {"Strong Recommend", "Recommend", "Watchlist", "Track Only", "Reject"})
        self.assertTrue(str(record["recommendation_reason"]).strip())

        recommendations = self.db.get_recommendations(min_score=0, limit=10)
        self.assertEqual(len(recommendations), 1)
        rec = dict(recommendations[0] or {})
        self.assertIn("recommendation_band", rec)
        self.assertIn("recommendation_reason", rec)


if __name__ == "__main__":
    unittest.main()
