from __future__ import annotations

import unittest
from datetime import UTC, datetime
from types import SimpleNamespace

from app.agents.interaction.interaction_agent import InteractionAgent


def _sample_profile(fields: dict[str, object], *, confidence: float = 50.0) -> dict[str, object]:
    missing_fields = [key for key, value in fields.items() if value in ("", [], None)]
    return {
        "fields": fields,
        "evidence": [{"source": "brave", "reason": "sample evidence"}],
        "missing_fields": missing_fields,
        "confidence": confidence,
    }


def _sample_record() -> dict[str, object]:
    return {
        "lead_id": 73,
        "company_name": "Protect AI",
        "normalized_name": "Protect AI",
        "raw_title": "Protect AI for AI Agent Security",
        "official_domain": "protectai.com",
        "verification_status": "verified",
        "entity_type": "company",
        "status": "dd_partial",
        "stage": "scoring",
        "source_hits": 3,
        "dd_status": "dd_partial",
        "completeness_score": 68.33,
        "dd_updated_at": datetime(2026, 4, 22, 20, 34, 2, tzinfo=UTC),
        "score_updated_at": datetime(2026, 4, 22, 20, 34, 10, tzinfo=UTC),
        "base_score": 72.0,
        "thesis_fit": 80.0,
        "evidence_strength": 85.0,
        "final_score": 78.5,
        "score_reason": "AI security focus + enterprise signal + early traction",
        "business_profile": _sample_profile(
            {
                "one_liner": "Protect AI provides AI security for enterprise teams.",
                "products_services": ["AI security platform", "runtime protection"],
                "target_customers": ["enterprise teams"],
                "use_cases": ["prompt injection defense"],
                "official_domain": "protectai.com",
            },
            confidence=90.0,
        ),
        "team_profile": _sample_profile(
            {
                "founders": ["Jane Doe"],
                "key_people": [],
                "prior_companies": ["OpenAI"],
                "research_background": [],
            },
            confidence=72.0,
        ),
        "funding_profile": _sample_profile(
            {
                "founded_year": "2022",
                "headquarters": "San Francisco",
                "funding_rounds": ["seed"],
                "total_raised": "",
                "valuation": "",
                "notable_investors": ["Sequoia"],
            },
            confidence=60.0,
        ),
        "traction_profile": _sample_profile(
            {
                "customers": ["Fortune 500 enterprises"],
                "partners": [],
                "product_launches": ["runtime protection platform"],
                "revenue_signals": [],
                "deployment_signals": [],
            },
            confidence=58.0,
        ),
        "market_position": _sample_profile(
            {
                "sub_sector": ["agent security", "runtime protection"],
                "is_new_category": None,
                "competitors": [],
                "leader_signals": ["leader"],
                "crowdedness": "medium",
            },
            confidence=66.0,
        ),
        "dd_overall": {
            "dd_status": "dd_partial",
            "completeness_score": 68.33,
            "source_hits": 3,
            "summary": "Protect AI: evidence collected for business_profile, team_profile, funding_profile, market_position; weaker dimensions: traction_profile.",
            "missing_dimensions": ["traction_profile"],
            "confidence": 71.0,
            "generated_at": "2026-04-22T20:34:02+00:00",
        },
        "business_summary": "Protect AI provides AI security for enterprise teams",
        "team_summary": "Jane Doe | OpenAI",
        "funding_summary": "seed | Sequoia",
        "traction_summary": "Fortune 500 enterprises | runtime protection platform",
        "industry_position": "agent security, runtime protection",
    }


class InteractionAnalysisRenderTests(unittest.TestCase):
    def setUp(self):
        self.agent = InteractionAgent(SimpleNamespace(), SimpleNamespace(recommend_score_threshold=75.0, webhook_url="", webhook_timeout_seconds=10))

    def test_render_company_analysis_includes_structured_dd_and_score(self):
        record = _sample_record()
        result = self.agent.render_company_analysis(record, query="Protect AI")

        self.assertTrue(result["ok"])
        self.assertIn("业务概况", result["reply"])
        self.assertIn("团队背景", result["reply"])
        self.assertIn("融资概况", result["reply"])
        self.assertIn("业务进展", result["reply"])
        self.assertIn("行业地位", result["reply"])
        self.assertIn("score_reason", result["reply"])
        self.assertIn("recommendation", result["reply"])
        self.assertEqual(result["data"]["dd_status"], "dd_partial")
        self.assertEqual(result["data"]["final_score"], 78.5)
        self.assertIn("business_profile", result["data"])
        self.assertIn("dd_overall", result["data"])

    def test_render_dd_report_includes_structured_dd_sections(self):
        record = _sample_record()
        result = self.agent.render_dd_report(record, query="lead 73")

        self.assertTrue(result["ok"])
        self.assertIn("业务概况", result["reply"])
        self.assertIn("团队背景", result["reply"])
        self.assertIn("融资概况", result["reply"])
        self.assertIn("业务进展", result["reply"])
        self.assertIn("行业地位", result["reply"])
        self.assertEqual(result["data"]["company_name"], "Protect AI")
        self.assertEqual(result["data"]["dd_status"], "dd_partial")
        self.assertIn("dd_overall", result["data"])
        self.assertIn("market_position", result["data"])


if __name__ == "__main__":
    unittest.main()
