from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import app.core.db as db_module
from app.core.db import Database
from app.models.dd_question import DDQuestion
from app.models.dd_report import DDOverall, DDProfile, DDReport


class DDDatetimeSerializationTests(unittest.TestCase):
    def test_upsert_dd_report_normalizes_nested_datetimes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_db_path = Path(tmpdir) / "agent_local.db"
            original_path = db_module.LOCAL_SQLITE_PATH
            db_module.LOCAL_SQLITE_PATH = temp_db_path
            try:
                db = Database("")
                db.init_schema()

                lead_id, _ = db.upsert_lead(
                    company_name="Protect AI",
                    source="bocha",
                    description="Protect AI provides agent security tooling.",
                    thesis_tags="agent security",
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

                now = datetime.now(UTC)
                report = DDReport(
                    lead_id=lead_id,
                    company_name="Protect AI",
                    normalized_name="Protect AI",
                    official_domain="protectai.com",
                    source_hits=2,
                    dd_status="dd_partial",
                    completeness_score=62.5,
                    business_profile=DDProfile(
                        fields={"one_liner": "Protect AI secures enterprise agents."},
                        evidence=[{"source": "official", "seen_at": now}],
                        missing_fields=[],
                        confidence=0.9,
                    ),
                    team_profile=DDProfile(fields={}, evidence=[], missing_fields=["founders"], confidence=0.2),
                    funding_profile=DDProfile(fields={}, evidence=[], missing_fields=["valuation"], confidence=0.1),
                    traction_profile=DDProfile(fields={"customers": ["Enterprise teams"]}, evidence=[], missing_fields=[], confidence=0.5),
                    market_position=DDProfile(fields={}, evidence=[], missing_fields=["competitors"], confidence=0.2),
                    dd_overall=DDOverall(
                        dd_status="dd_partial",
                        completeness_score=62.5,
                        source_hits=2,
                        summary="Evidence exists but DD is incomplete.",
                        missing_dimensions=["team_profile", "funding_profile"],
                        confidence=0.55,
                        generated_at=now.isoformat(),
                    ),
                    questions=[
                        DDQuestion(
                            lead_id=lead_id,
                            company_key="protect ai",
                            company_name="Protect AI",
                            normalized_name="Protect AI",
                            official_domain="protectai.com",
                            dimension="entity",
                            question_type="subject_conflict",
                            prompt="Is the company name Protect AI?",
                            details={"observed_at": now},
                            created_at=now,  # intentionally raw datetime
                            updated_at=now,  # intentionally raw datetime
                        )
                    ],
                    evidence_json={"official_pages": [{"url": "https://protectai.com/about", "fetched_at": now}]},
                )

                db.upsert_dd_report(report=report)
                stored = db.get_dd_report_for_lead(lead_id)

                self.assertIsNotNone(stored)
                question = stored["questions"][0]
                evidence_json = db._json_loads(stored.get("evidence_json"), default={})
                self.assertIsInstance(question["created_at"], str)
                self.assertIsInstance(question["updated_at"], str)
                self.assertIsInstance(question["details"]["observed_at"], str)
                self.assertIsInstance(stored["business_profile"]["evidence"][0]["seen_at"], str)
                self.assertIsInstance(evidence_json["official_pages"][0]["fetched_at"], str)
            finally:
                db_module.LOCAL_SQLITE_PATH = original_path

    def test_add_dd_question_normalizes_nested_datetimes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_db_path = Path(tmpdir) / "agent_local.db"
            original_path = db_module.LOCAL_SQLITE_PATH
            db_module.LOCAL_SQLITE_PATH = temp_db_path
            try:
                db = Database("")
                db.init_schema()

                lead_id, _ = db.upsert_lead(
                    company_name="Protect AI",
                    source="bocha",
                    description="Protect AI provides agent security tooling.",
                    thesis_tags="agent security",
                    resolution={
                        "raw_title": "Protect AI",
                        "candidate_name": "Protect AI",
                        "normalized_name": "Protect AI",
                        "entity_type": "company",
                        "official_domain": "protectai.com",
                        "verification_status": "verified",
                        "verification_score": 98.0,
                        "reject_reason": "",
                    },
                )

                now = datetime.now(UTC)
                question_id = db.add_dd_question(
                    {
                        "lead_id": lead_id,
                        "company_key": "protect ai",
                        "company_name": "Protect AI",
                        "normalized_name": "Protect AI",
                        "official_domain": "protectai.com",
                        "scope": "lead",
                        "scope_key": f"lead:{lead_id}",
                        "dimension": "entity",
                        "question_type": "subject_conflict",
                        "prompt": "Please confirm the company name.",
                        "missing_fields": ["normalized_name"],
                        "details": {
                            "feedback_context": {
                                "open_questions": [{"created_at": now}],
                                "observed_at": now,
                            }
                        },
                        "created_at": now,
                    }
                )

                self.assertGreater(question_id, 0)
                stored = db.list_dd_questions(lead_id=lead_id, status="open", limit=10)
                self.assertEqual(len(stored), 1)
                details = stored[0]["details_json"]
                self.assertIsInstance(details["feedback_context"]["observed_at"], str)
                self.assertIsInstance(details["feedback_context"]["open_questions"][0]["created_at"], str)
            finally:
                db_module.LOCAL_SQLITE_PATH = original_path


if __name__ == "__main__":
    unittest.main()
