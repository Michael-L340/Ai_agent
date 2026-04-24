from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.scoring.scoring_agent import ScoringAgent
from app.core.config import Settings
from app.core.db import Database
from app.models.dd_question import DDQuestion
from app.models.dd_report import DDProfile
from app.services.dd_enricher import DDEnricher
from app.services.entity_verifier import EntityVerifier


class MVPModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("", mvp_mode=True)
        self.db.init_schema()
        self.db.set_long_memory(
            "planner_long_memory",
            {
                "sub_sectors": ["agent security", "runtime protection", "ai security"],
                "signal_dictionary": ["enterprise customer", "production deployment", "paid pilot"],
                "negative_filters": ["what is", "guide"],
                "source_policy": {"channel_status": {"brave": True, "bocha": True}},
                "human_preferences": ["agent security", "runtime protection"],
                "version": 1,
            },
        )
        self.db.set_short_memory(
            "2026-04-24",
            {
                "emerging_themes": [
                    {
                        "theme": "agent security",
                        "priority": 5,
                        "keywords": ["agent security", "runtime protection", "enterprise customer"],
                        "source_suggestions": ["brave", "bocha"],
                        "days_active": 3,
                        "promote_candidate": True,
                    }
                ],
                "priority": ["agent security"],
                "keywords": ["agent security", "runtime protection", "enterprise customer", "production deployment"],
                "source_suggestions": {"agent security": ["brave", "bocha"]},
                "days_active": {"agent security": 3},
                "promote_candidate": ["agent security"],
            },
        )

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _insert_likely_lead(
        self,
        *,
        company_name: str,
        raw_title: str,
        official_domain: str = "",
        dd_status: str = "dd_partial",
    ) -> int:
        lead_id, _ = self.db.upsert_lead(
            company_name=company_name,
            source="bocha",
            description="AI security company for enterprise agent runtime protection",
            thesis_tags="agent security, runtime protection",
            resolution={
                "raw_title": raw_title,
                "candidate_name": company_name,
                "normalized_name": company_name,
                "entity_type": "company",
                "official_domain": official_domain,
                "verification_status": "likely_company",
                "verification_score": 62.0,
                "reject_reason": "",
            },
        )
        self.db.upsert_dd_report(
            report={
                "lead_id": int(lead_id),
                "company_name": company_name,
                "normalized_name": company_name,
                "official_domain": official_domain,
                "source_hits": 3,
                "dd_status": dd_status,
                "completeness_score": 92.0,
                "business_profile": {
                    "fields": {
                        "one_liner": "Enterprise agent runtime security platform",
                        "products_services": ["runtime protection"],
                        "target_customers": ["enterprise security teams"],
                        "use_cases": ["agent security"],
                        "official_domain": official_domain,
                    },
                    "evidence": [{"source": "bocha", "reason": "title"}],
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
                    "confidence": 85.0,
                },
                "funding_profile": {
                    "fields": {
                        "founded_year": "2024",
                        "headquarters": "New York",
                        "funding_rounds": ["seed"],
                        "total_raised": "$3M",
                        "valuation": "",
                        "notable_investors": ["A16Z"],
                    },
                    "evidence": [{"source": "bocha", "reason": "funding"}],
                    "missing_fields": ["valuation"],
                    "confidence": 78.0,
                },
                "traction_profile": {
                    "fields": {
                        "customers": ["enterprise customers"],
                        "partners": ["cloud marketplace"],
                        "product_launches": ["general availability"],
                        "revenue_signals": ["paid pilot"],
                        "deployment_signals": ["production deployment"],
                    },
                    "evidence": [{"source": "official_page", "reason": "customers"}],
                    "missing_fields": [],
                    "confidence": 90.0,
                },
                "market_position": {
                    "fields": {
                        "sub_sector": ["agent security", "runtime protection"],
                        "is_new_category": True,
                        "competitors": ["point solutions"],
                        "leader_signals": ["recognized leader"],
                        "crowdedness": "medium",
                    },
                    "evidence": [{"source": "bocha", "reason": "market"}],
                    "missing_fields": [],
                    "confidence": 84.0,
                },
                "dd_overall": {
                    "dd_status": dd_status,
                    "completeness_score": 92.0,
                    "source_hits": 3,
                    "summary": "MVP DD partial",
                    "missing_dimensions": [],
                    "confidence": 90.0,
                    "generated_at": "2026-04-24T00:00:00+00:00",
                },
                "questions": [],
                "business_summary": "Enterprise runtime security company",
                "team_summary": "Founder identified",
                "funding_summary": "Seed round signal",
                "traction_summary": "Enterprise customer signal",
                "industry_position": "Agent security",
                "evidence_json": {"dimension_scores": {"business_profile": 18.0}},
            }
        )
        return int(lead_id)

    def test_entity_verifier_promotes_relevant_candidate_to_likely_company_in_mvp(self) -> None:
        settings = Settings(mvp_mode=True)
        verifier = EntityVerifier(settings, llm=None)
        verifier._search_evidence = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
            "searches": {"brave": [], "bocha": []},
            "official_domain": "",
            "domain_scores": {},
            "reason": "test",
        }
        result = verifier.resolve(
            raw_title="AI agent security startup Trent AI launch",
            snippet="Trent AI builds agent security software for enterprise AI teams.",
            url="https://example.com/trent-ai-news",
            source="bocha",
            query="agent security startup funding",
        )
        self.assertEqual(result.normalized_name, "Trent AI")
        self.assertEqual(result.verification_status, "likely_company")
        self.assertEqual(result.entity_type, "company")

    def test_dd_enricher_uses_dd_partial_for_missing_fields_in_mvp(self) -> None:
        enricher = DDEnricher(
            brave_client=SimpleNamespace(search=lambda *_args, **_kwargs: []),
            bocha_client=SimpleNamespace(search=lambda *_args, **_kwargs: []),
            mvp_mode=True,
        )
        profiles = {
            "business_profile": DDProfile(fields={"one_liner": "runtime security"}, evidence=[{"source": "bocha"}], missing_fields=[], confidence=55.0),
            "team_profile": DDProfile(fields={}, evidence=[], missing_fields=["founders"], confidence=0.0),
            "funding_profile": DDProfile(fields={}, evidence=[], missing_fields=["valuation"], confidence=0.0),
            "traction_profile": DDProfile(fields={"customers": ["enterprise"]}, evidence=[{"source": "official_page"}], missing_fields=[], confidence=55.0),
            "market_position": DDProfile(fields={}, evidence=[], missing_fields=["competitors"], confidence=0.0),
        }
        questions = [
            DDQuestion(
                lead_id=1,
                company_key="trentai",
                company_name="Trent AI",
                normalized_name="Trent AI",
                official_domain="trentai.com",
                dimension="funding_profile",
                question_type="missing_fields",
                prompt="Need funding detail",
                missing_fields=["valuation"],
                details={},
                dedupe_key="q1",
                status="open",
                created_at="2026-04-24T00:00:00+00:00",
                updated_at="2026-04-24T00:00:00+00:00",
            )
        ]
        status = enricher._determine_status(48.0, ["team_profile", "funding_profile"], profiles, questions)
        self.assertEqual(status, "dd_partial")

    def test_scoring_and_recommendations_accept_likely_company_in_mvp(self) -> None:
        lead_id = self._insert_likely_lead(
            company_name="Trent AI",
            raw_title="AI agent security startup Trent AI launch",
            official_domain="trentai.com",
        )
        scorer = ScoringAgent(self.db, SimpleNamespace(recommend_score_threshold=75.0, mvp_mode=True))
        stats = scorer.run(
            search_plan={
                "queries": ["agent security startup funding", "runtime protection enterprise customer"],
                "sensitive_keywords": ["enterprise customer", "production deployment"],
                "human_preferences": ["agent security"],
            }
        )
        self.assertEqual(stats["scored"], 1)
        record = dict(self.db.get_company_analysis_for_lead(lead_id) or {})
        self.assertEqual(record.get("verification_status"), "likely_company")
        self.assertGreaterEqual(float(record.get("final_score") or 0.0), 82.0)
        self.assertIn("likely_company", str(record.get("recommendation_reason") or ""))
        recommendations = self.db.get_recommendations(min_score=0, limit=10)
        self.assertEqual(len(recommendations), 1)
        self.assertEqual(str(recommendations[0].get("verification_status")), "likely_company")
        self.assertTrue(bool(recommendations[0].get("needs_human_review")))
        self.assertEqual(str(recommendations[0].get("confidence")), "medium")

    def test_scoring_ready_requires_dd_partial_for_likely_company(self) -> None:
        likely_partial = self._insert_likely_lead(
            company_name="MVP Partial Co",
            raw_title="AI agent security startup MVP Partial Co launch",
            official_domain="mvppartial.example",
            dd_status="dd_partial",
        )
        self._insert_likely_lead(
            company_name="MVP Done Co",
            raw_title="AI agent security startup MVP Done Co launch",
            official_domain="mvpdone.example",
            dd_status="dd_done",
        )

        candidates = self.db.get_scoring_candidates(limit=20)
        lead_ids = {int(row.get("lead_id") or 0) for row in candidates}
        self.assertIn(likely_partial, lead_ids)
        self.assertEqual(sum(1 for row in candidates if str(row.get("verification_status")) == "likely_company"), 1)


if __name__ == "__main__":
    unittest.main()
