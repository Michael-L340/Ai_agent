from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.interaction.interaction_agent import InteractionAgent
from app.agents.scoring.scoring_agent import ScoringAgent
from app.models.scoring_policy import DEFAULT_SCORING_WEIGHTS, ScoringPolicy
from app.core.db import Database


class ScoringPolicyFeedbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()
        self.interaction = InteractionAgent(self.db, SimpleNamespace())
        self.scoring = ScoringAgent(self.db, SimpleNamespace(recommend_score_threshold=75.0))

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
            "completeness_score": 84.0,
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
                "completeness_score": 84.0,
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

    def test_policy_updates_persist_and_trace(self) -> None:
        lead_id = self._insert_lead("Protect AI")
        self._insert_dd_report(lead_id)

        initial = self.db.get_scoring_policy()
        self.assertEqual(initial["version"], 1)

        feedbacks = [
            ("like", "更喜欢 traction 和 customers", "scoring_feedback"),
            ("dislike", "不喜欢，太像内容页", "scoring_feedback"),
            ("skip", "跳过，因为没有客户", "scoring_feedback"),
            ("wrong_entity", "主体错了，不是公司", "scoring_feedback"),
            ("prefer_sector", "我更关注 agent security 赛道", "scoring_feedback"),
        ]

        for verdict, content, feedback_type in feedbacks:
            self.interaction.receive_feedback(
                verdict=verdict,
                content=content,
                feedback_type=feedback_type,
                lead_id=lead_id,
            )

        policy = self.db.get_scoring_policy()
        self.assertGreaterEqual(int(policy["version"]), 6)
        self.assertGreater(policy["weights"]["traction_score"], DEFAULT_SCORING_WEIGHTS["traction_score"])
        self.assertGreater(policy["weights"]["thesis_fit_score"], DEFAULT_SCORING_WEIGHTS["thesis_fit_score"])
        self.assertGreater(policy["weights"]["market_score"], DEFAULT_SCORING_WEIGHTS["market_score"])

        penalty_rules = list(policy["penalty_rules"])
        boost_rules = list(policy["boost_rules"])
        self.assertTrue(any(rule["verdict"] == "wrong_entity" for rule in penalty_rules))
        self.assertTrue(any("agent security" in str(rule.get("term") or "").lower() for rule in boost_rules))

        events = self.db.list_scoring_policy_events(limit=20)
        verdicts = {str(event["verdict"]).lower() for event in events}
        self.assertTrue({"like", "dislike", "skip", "wrong_entity", "prefer_sector"}.issubset(verdicts))
        self.assertGreaterEqual(len(events), 5)

    def test_policy_affects_future_scoring(self) -> None:
        lead_id = self._insert_lead("Protect AI")
        self._insert_dd_report(lead_id)

        row = self.db.get_scoring_candidates(limit=10)[0]
        search_plan = {
            "queries": ["AI security enterprise customers", "agent security runtime protection"],
            "sensitive_keywords": ["enterprise customer", "paid pilot", "ARR"],
            "human_preferences": ["agent security"],
        }
        long_memory = self.db.get_long_memory("planner_long_memory") or {}
        short_memory = self.db.get_latest_short_memory() or {}

        baseline_policy = ScoringPolicy.from_dict(self.db.get_scoring_policy())
        baseline = self.scoring._score_candidate(
            row,
            search_plan,
            long_memory=long_memory,
            short_memory=short_memory,
            policy=baseline_policy,
        )

        self.interaction.receive_feedback(
            verdict="prefer_sector",
            content="我更关注 agent security 赛道",
            feedback_type="scoring_feedback",
            lead_id=lead_id,
        )

        updated_policy = ScoringPolicy.from_dict(self.db.get_scoring_policy())
        updated = self.scoring._score_candidate(
            row,
            search_plan,
            long_memory=long_memory,
            short_memory=short_memory,
            policy=updated_policy,
        )

        self.assertGreater(updated.final_score, baseline.final_score)
        self.assertIn("policy_v", updated.recommendation_reason)


if __name__ == "__main__":
    unittest.main()
