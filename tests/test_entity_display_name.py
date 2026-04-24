from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.interaction.interaction_agent import InteractionAgent
from app.core.db import Database


class EntityDisplayNameTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()
        self.agent = InteractionAgent(
            self.db,
            SimpleNamespace(recommend_score_threshold=75.0, webhook_url="", webhook_timeout_seconds=10),
        )

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _insert_lead(self, *, raw_title: str, normalized_name: str, verification_status: str = "verified") -> int:
        lead_id, _ = self.db.upsert_lead(
            company_name=raw_title,
            source="brave",
            description="AI security company",
            thesis_tags="ai security",
            resolution={
                "raw_title": raw_title,
                "candidate_name": normalized_name,
                "normalized_name": normalized_name,
                "entity_type": "company",
                "official_domain": f"{normalized_name.lower().replace(' ', '')}.ai",
                "verification_status": verification_status,
                "verification_score": 95.0 if verification_status == "verified" else 55.0,
                "reject_reason": "",
            },
        )
        self.db.upsert_score(
            int(lead_id),
            base_score=80.0,
            thesis_fit=80.0,
            evidence_strength=80.0,
            final_score=82.0,
            score_reason="test score",
            result={
                "business_score": 4.0,
                "team_score": 4.0,
                "funding_score": 4.0,
                "traction_score": 4.0,
                "market_score": 4.0,
                "thesis_fit_score": 4.0,
                "evidence_score": 4.0,
                "raw_score": 80.0,
                "confidence_multiplier": 1.0,
                "penalty_score": 0.0,
                "final_score": 82.0,
                "recommendation_band": "Recommend",
                "recommendation_reason": "display-name test",
            },
        )
        self.db.upsert_dd_report(
            report={
                "lead_id": int(lead_id),
                "company_name": normalized_name,
                "normalized_name": normalized_name,
                "official_domain": f"{normalized_name.lower().replace(' ', '')}.ai",
                "source_hits": 2,
                "dd_status": "dd_partial",
                "completeness_score": 80.0,
                "business_profile": {"fields": {"one_liner": "AI security company", "official_domain": f"{normalized_name.lower().replace(' ', '')}.ai"}},
                "team_profile": {"fields": {"founders": ["Founder"]}},
                "funding_profile": {"fields": {"funding_rounds": ["seed"]}},
                "traction_profile": {"fields": {"customers": ["enterprise"]}},
                "market_position": {"fields": {"sub_sector": ["ai security"]}},
                "dd_overall": {
                    "dd_status": "dd_partial",
                    "completeness_score": 80.0,
                    "source_hits": 2,
                    "missing_dimensions": [],
                    "confidence": 75.0,
                },
                "questions": [],
                "business_summary": "AI security company",
                "team_summary": "Founder identified",
                "funding_summary": "Seed",
                "traction_summary": "Enterprise customer",
                "industry_position": "AI security",
                "evidence_json": {},
            }
        )
        return int(lead_id)

    def test_recommendations_use_normalized_display_name_and_keep_raw_title(self) -> None:
        self._insert_lead(
            raw_title="Protect AI for AI Agent Security",
            normalized_name="Protect AI",
        )
        self._insert_lead(
            raw_title="AI agent security startup Trent AI launch",
            normalized_name="Trent AI",
        )
        self._insert_lead(
            raw_title="AI security startup Artemis exits stealth",
            normalized_name="Artemis",
        )

        rows = self.db.get_recommendations(min_score=75.0, limit=10)
        by_display = {str(row["display_name"]): dict(row) for row in rows}

        self.assertIn("Protect AI", by_display)
        self.assertIn("Trent AI", by_display)
        self.assertIn("Artemis", by_display)
        self.assertEqual(by_display["Protect AI"]["raw_title"], "Protect AI for AI Agent Security")
        self.assertEqual(by_display["Trent AI"]["raw_title"], "AI agent security startup Trent AI launch")
        self.assertEqual(by_display["Artemis"]["raw_title"], "AI security startup Artemis exits stealth")

        items = self.agent.list_recommendations(threshold=75.0)
        item_names = [item["display_name"] for item in items]
        self.assertIn("Protect AI", item_names)
        self.assertIn("Trent AI", item_names)
        self.assertIn("Artemis", item_names)
        self.assertNotIn("Protect AI for AI Agent Security", item_names)
        self.assertNotIn("AI agent security startup Trent AI launch", item_names)
        self.assertNotIn("AI security startup Artemis exits stealth", item_names)

    def test_pending_review_lead_is_not_formally_recommended(self) -> None:
        self._insert_lead(
            raw_title="AI security startup Artemis exits stealth",
            normalized_name="Artemis",
            verification_status="pending_review",
        )

        rows = self.db.get_recommendations(min_score=75.0, limit=10)
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
