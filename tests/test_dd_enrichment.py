from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.dd.dd_agent import DDAgent
from app.core.db import Database
from app.services.dd_enricher import DDEnricher


class FakeSearchClient:
    def __init__(self, items_by_query: dict[str, list[dict[str, str]]], default_items: list[dict[str, str]] | None = None):
        self.items_by_query = items_by_query
        self.default_items = default_items or []

    def search(self, query: str, limit: int = 10) -> list[dict[str, str]]:
        items = self.items_by_query.get(query, self.default_items)
        return list(items)[:limit]


def _protect_ai_brave_items() -> list[dict[str, str]]:
    return [
        {
            "title": "Protect AI for AI Agent Security",
            "snippet": "Protect AI provides an AI security platform for enterprise AI teams and customers. Founded in 2022 in San Francisco. Customers include Fortune 500 enterprises.",
            "url": "https://protectai.com/news/protect-ai-agent-security",
        },
        {
            "title": "Protect AI raises seed funding for runtime protection",
            "snippet": "Investors include Sequoia and A16Z. The company focuses on prompt injection, runtime monitoring, and guardrails for AI agents.",
            "url": "https://protectai.com/news/seed",
        },
    ]


def _protect_ai_bocha_items() -> list[dict[str, str]]:
    return [
        {
            "title": "Protect AI team and customers",
            "snippet": "Founders: Jane Doe, former OpenAI, and John Smith, MIT. Partners include cloud security teams. Product launches include runtime protection and agent monitoring.",
            "url": "https://protectai.com/blog/team-customers",
        },
        {
            "title": "Protect AI market position",
            "snippet": "The company competes in the agent security and LLM security category and is recognized as a leader in runtime protection.",
            "url": "https://protectai.com/blog/market-position",
        },
    ]


def _official_pages() -> list[dict[str, str]]:
    return [
        {
            "source": "official_page",
            "url": "https://protectai.com/about",
            "title": "About Protect AI",
            "text": (
                "Protect AI provides AI security for enterprise teams. "
                "Use cases include prompt injection defense, runtime monitoring, and compliance. "
                "Founded in 2022 and based in San Francisco."
            ),
        },
        {
            "source": "official_page",
            "url": "https://protectai.com/team",
            "title": "Team",
            "text": "Founders: Jane Doe and John Smith. Jane Doe was formerly at OpenAI. John Smith previously at MIT.",
        },
        {
            "source": "official_page",
            "url": "https://protectai.com/customers",
            "title": "Customers",
            "text": "Customers include Fortune 500 enterprises and security teams. Partners include cloud platforms.",
        },
        {
            "source": "official_page",
            "url": "https://protectai.com/press",
            "title": "Press",
            "text": "Raised a seed round led by Sequoia and A16Z. Product launch: AI agent runtime protection platform.",
        },
    ]


class DDEnrichmentTests(unittest.TestCase):
    def test_structured_enrichment_has_five_dimensions(self):
        brave = FakeSearchClient({"Protect AI AI security": _protect_ai_brave_items()})
        bocha = FakeSearchClient({"Protect AI AI security": _protect_ai_bocha_items()})
        enricher = DDEnricher(
            brave,
            bocha,
            page_fetcher=lambda domain: _official_pages(),
            search_limit_per_query=5,
            queries_per_dimension=1,
            max_official_pages=4,
        )

        lead = {
            "id": 1,
            "company_name": "Protect AI",
            "normalized_name": "Protect AI",
            "official_domain": "protectai.com",
        }
        report = enricher.enrich(lead)
        payload = report.to_dict()

        self.assertEqual(report.normalized_name, "Protect AI")
        self.assertEqual(report.official_domain, "protectai.com")
        self.assertIn(report.dd_status, {"dd_done", "dd_partial", "dd_pending_review"})
        self.assertGreaterEqual(report.completeness_score, 0)
        self.assertLessEqual(report.completeness_score, 100)
        self.assertTrue(report.business_profile.evidence)
        self.assertTrue(report.team_profile.evidence)
        self.assertTrue(report.funding_profile.evidence)
        self.assertTrue(report.traction_profile.evidence)
        self.assertTrue(report.market_position.evidence)
        self.assertIn("business_profile", payload)
        self.assertIn("team_profile", payload)
        self.assertIn("funding_profile", payload)
        self.assertIn("traction_profile", payload)
        self.assertIn("market_position", payload)
        self.assertIn("dd_overall", payload)
        self.assertIn("completeness_score", payload)
        self.assertIn("dd_status", payload)
        self.assertGreaterEqual(len(report.business_profile.missing_fields), 0)

    def test_pending_review_when_no_evidence(self):
        brave = FakeSearchClient({})
        bocha = FakeSearchClient({})
        enricher = DDEnricher(
            brave,
            bocha,
            page_fetcher=lambda domain: [],
            search_limit_per_query=5,
            queries_per_dimension=1,
            max_official_pages=4,
        )
        lead = {
            "id": 2,
            "company_name": "Unknown Security",
            "normalized_name": "Unknown Security",
            "official_domain": "",
        }
        report = enricher.enrich(lead)
        self.assertEqual(report.dd_status, "dd_pending_review")
        self.assertEqual(report.completeness_score, 0.0)
        self.assertTrue(all(not profile.evidence for profile in [
            report.business_profile,
            report.team_profile,
            report.funding_profile,
            report.traction_profile,
            report.market_position,
        ]))

    def test_database_round_trip_and_full_cycle_dd(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_db_path = Path(tmpdir) / "agent_local.db"
            original_path = db_module.LOCAL_SQLITE_PATH
            db_module.LOCAL_SQLITE_PATH = temp_db_path
            try:
                db = Database("")
                db.init_schema()

                lead_id, created = db.upsert_lead(
                    company_name="Protect AI",
                    source="brave",
                    description="Protect AI provides AI security for enterprise AI teams.",
                    thesis_tags="ai security,enterprise",
                    resolution={
                        "raw_title": "Protect AI for AI Agent Security",
                        "candidate_name": "Protect AI",
                        "normalized_name": "Protect AI",
                        "entity_type": "company",
                        "official_domain": "protectai.com",
                        "verification_status": "verified",
                        "verification_score": 98.0,
                        "reject_reason": "",
                    },
                )
                self.assertTrue(created)

                brave = FakeSearchClient({"Protect AI AI security": _protect_ai_brave_items()})
                bocha = FakeSearchClient({"Protect AI AI security": _protect_ai_bocha_items()})
                enricher = DDEnricher(
                    brave,
                    bocha,
                    page_fetcher=lambda domain: _official_pages(),
                    search_limit_per_query=5,
                    queries_per_dimension=1,
                    max_official_pages=4,
                )
                agent = DDAgent(db, brave, bocha, SimpleNamespace(settings=None), enricher=enricher)
                stats = agent.run(limit=10)

                self.assertEqual(stats["processed"], 1)
                record = db.get_dd_report_for_lead(lead_id)
                self.assertIsNotNone(record)
                self.assertIn("business_profile", record)
                self.assertIn("dd_overall", record)
                self.assertIn(record["dd_status"], {"dd_done", "dd_partial", "dd_pending_review"})
                self.assertGreaterEqual(float(record["completeness_score"] or 0), 0.0)
                self.assertEqual(record["company_name"], "Protect AI")
                self.assertTrue(record["business_profile"]["fields"]["one_liner"])

                analysis = db.get_company_analysis_for_lead(lead_id)
                self.assertIsNotNone(analysis)
                self.assertIn("final_score", analysis or {})
                self.assertIn("business_profile", analysis or {})
            finally:
                db_module.LOCAL_SQLITE_PATH = original_path


if __name__ == "__main__":
    unittest.main()
