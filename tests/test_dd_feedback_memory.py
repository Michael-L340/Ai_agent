from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.dd.dd_agent import DDAgent
from app.core.db import Database
from app.services.dd_enricher import DDEnricher
from app.services.dd_memory import DDMemoryStore


class FakeSearchClient:
    def __init__(self, default_items: list[dict[str, str]] | None = None):
        self.default_items = default_items or []

    def search(self, query: str, limit: int = 10) -> list[dict[str, str]]:
        return list(self.default_items)[:limit]


def _official_pages() -> list[dict[str, str]]:
    return [
        {
            "source": "official_page",
            "url": "https://protectai.com/about",
            "title": "About Protect AI",
            "text": "Protect AI provides AI security for enterprise teams. Founded in 2022 and based in San Francisco.",
        },
        {
            "source": "official_page",
            "url": "https://protectai.com/customers",
            "title": "Customers",
            "text": "Customers include Fortune 500 enterprises and security teams.",
        },
        {
            "source": "official_page",
            "url": "https://protectai.com/press",
            "title": "Press",
            "text": "Raised a seed round led by Sequoia and A16Z. Product launch: runtime protection platform. Valued at $100M in early coverage.",
        },
    ]


def _search_items(company_name: str) -> list[dict[str, str]]:
    return [
        {
            "title": f"{company_name} for AI Agent Security",
            "snippet": f"{company_name} provides AI security for enterprise teams and customers.",
            "url": "https://protectai.com/news/overview",
        },
        {
            "title": f"{company_name} raises seed funding for runtime protection",
            "snippet": "Investors include Sequoia and A16Z. The company focuses on prompt injection, runtime monitoring, and guardrails for AI agents.",
            "url": "https://protectai.com/news/seed",
        },
    ]


class DDFeedbackMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _insert_lead(self, company_name: str, *, raw_title: str | None = None) -> int:
        lead_id, created = self.db.upsert_lead(
            company_name=company_name,
            source="brave",
            description=f"{company_name} is an AI security company.",
            thesis_tags="ai security,enterprise",
            resolution={
                "raw_title": raw_title or company_name,
                "candidate_name": company_name,
                "normalized_name": company_name,
                "entity_type": "company",
                "official_domain": "protectai.com",
                "verification_status": "verified",
                "verification_score": 98.0,
                "reject_reason": "",
            },
        )
        self.assertTrue(created)
        return int(lead_id)

    def test_feedback_changes_dimension_queries_and_blocks_fields(self):
        lead_id = self._insert_lead("Protect AI")
        memory = DDMemoryStore(self.db)
        memory.record_feedback(
            scope="lead",
            lead_id=lead_id,
            company_name="Protect AI",
            normalized_name="Protect AI",
            official_domain="protectai.com",
            dimension="traction_profile",
            feedback_kind="note",
            parsed={
                "focus_dimensions": ["traction_profile"],
                "avoid_dimensions": ["funding_profile"],
                "focus_fields_by_dimension": {"traction_profile": ["customers"]},
                "blocked_fields_by_dimension": {"funding_profile": ["valuation"]},
                "confirmed_entity_name": "",
            },
            content="重点补客户，不补估值",
        )

        brave = FakeSearchClient(_search_items("Protect AI"))
        bocha = FakeSearchClient(_search_items("Protect AI"))
        enricher = DDEnricher(
            brave,
            bocha,
            page_fetcher=lambda domain: _official_pages(),
            memory=memory,
            search_limit_per_query=3,
            queries_per_dimension=1,
            max_official_pages=3,
        )

        report = enricher.enrich(self.db.get_lead_by_id(lead_id) or {})
        traction_queries = report.evidence_json["dimension_queries"]["traction_profile"]

        self.assertTrue(any("customers" in query.lower() for query in traction_queries))
        self.assertEqual(report.funding_profile.fields.get("valuation"), "")
        self.assertIn("traction_profile", report.to_dict())
        self.assertIn("dd_overall", report.to_dict())

    def test_subject_conflict_generates_question_and_answer_resumes_enrich(self):
        lead_id = self._insert_lead("Protect AI for AI Agent Security", raw_title="Protect AI for AI Agent Security")
        memory = DDMemoryStore(self.db)
        brave = FakeSearchClient(_search_items("Protect AI"))
        bocha = FakeSearchClient(_search_items("Protect AI"))
        enricher = DDEnricher(
            brave,
            bocha,
            page_fetcher=lambda domain: _official_pages(),
            memory=memory,
            search_limit_per_query=3,
            queries_per_dimension=1,
            max_official_pages=3,
        )
        agent = DDAgent(self.db, brave, bocha, SimpleNamespace(settings=None), enricher=enricher, dd_memory=memory)

        first_run = agent.run_for_lead(self.db.get_lead_by_id(lead_id) or {})
        record = self.db.get_dd_report_for_lead(lead_id)
        questions = self.db.list_dd_questions(lead_id=lead_id, status="open", limit=10)

        self.assertEqual(first_run["dd_waiting_human"], 1)
        self.assertEqual(record["dd_status"], "dd_waiting_human")
        self.assertGreaterEqual(len(questions), 1)
        self.assertEqual(questions[0]["dimension"], "entity")
        self.assertIn("subject_conflict", str(questions[0].get("question_type") or ""))

        question_id = int(questions[0]["id"])
        answer_result = memory.answer_question(question_id=question_id, answer_text="公司名是 Protect AI")
        self.assertTrue(answer_result["ok"])

        rerun = agent.run_for_lead(self.db.get_lead_by_id(lead_id) or {})
        rerun_report = enricher.enrich(self.db.get_lead_by_id(lead_id) or {})
        record_after = self.db.get_dd_report_for_lead(lead_id)
        open_questions_after = self.db.list_dd_questions(lead_id=lead_id, status="open", limit=10)

        self.assertEqual(rerun["dd_waiting_human"], 0)
        self.assertEqual(rerun_report.company_name, "Protect AI")
        self.assertIn(record_after["dd_status"], {"dd_partial", "dd_pending_review", "dd_done"})
        self.assertEqual(len(open_questions_after), 0)
        self.assertEqual(self.db.get_dd_question(question_id)["status"], "resolved")


if __name__ == "__main__":
    unittest.main()
