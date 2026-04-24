from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import app.core.db as db_module
from app.agents.searching.searching_agents import BaseSearchingAgent
from app.core.config import Settings
from app.core.db import Database
from app.core.runtime import AgentRuntime
from app.models.entity_resolution import EntityResolution


class _DummyLLM:
    def __init__(self):
        self.settings = Settings()

    def classify_relevance(self, *, title: str, snippet: str) -> dict[str, object]:
        return {"relevant": True, "company_name": title, "tags": ["agent security"]}


class RunCycleTimeoutDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def test_run_cycle_returns_stage_diagnostics_for_partial_success(self) -> None:
        runtime = AgentRuntime(Settings())
        runtime.init()

        runtime.planner.apply_feedback_learning = lambda: {}
        runtime.planner.get_search_plan = lambda: {
            "queries": ["agent runtime security startup funding", "agent runtime security enterprise customer"],
            "channel_status": {"brave": True, "bocha": False},
            "negative_filters": [],
        }
        runtime.brave_search_agent.fetch = lambda queries, **kwargs: {
            "searched_items": 4,
            "items": [
                {
                    "source": "brave",
                    "query": queries[0],
                    "title": "Capsule Security raises seed round",
                    "snippet": "Enterprise runtime security startup with new customers.",
                    "url": "https://example.com/capsule-security-seed",
                }
            ]
            * 4,
            "timed_out": False,
            "queries_total": len(queries),
            "queries_attempted": len(queries),
            "source_result": {
                "source_name": "brave",
                "status": "success",
                "request_attempted": True,
                "request_succeeded": True,
                "items_received": 4,
                "failure_stage": "",
                "failure_code": "",
                "http_status": None,
                "retry_after_sec": None,
                "provider_message": "",
                "retryable": False,
                "action_hint": "",
            },
        }
        runtime.brave_search_agent.verify_and_store = lambda **kwargs: {
            "matched_items": 4,
            "verified_items": 1,
            "pending_review_items": 1,
            "rejected_items": 2,
            "new_leads": 2,
            "processed_items": 4,
            "timed_out": False,
            "capped": False,
            "remaining_items": 0,
        }
        runtime.dd_agent.run = lambda limit=30, deadline_ts=None: {
            "processed": 2,
            "dd_done": 0,
            "dd_partial": 1,
            "dd_pending_review": 1,
            "dd_waiting_human": 0,
            "questions_generated": 0,
            "input_count": 6,
            "remaining_count": 4,
            "timed_out": True,
        }
        runtime.scoring_agent.run = lambda search_plan, **kwargs: {
            "scored": 0,
            "recommended": 0,
            "watchlist": 0,
            "track_only": 0,
            "rejected": 0,
            "input_count": 0,
            "remaining_count": 0,
            "timed_out": False,
        }
        runtime.interaction.publish_dd_questions = lambda: 0
        runtime.interaction.publish_recommendations = lambda threshold: 0
        runtime.db.get_recommendation_blocker_counts = lambda: {
            "verified_company_count": 1,
            "dd_ready_count": 0,
            "waiting_human_count": 2,
            "scored_ready_count": 0,
            "watchlist_count": 0,
            "hard_gate_blocked_count": 0,
            "push_ready_count": 0,
        }

        result = runtime.run_full_cycle()

        self.assertEqual(result["run_status"], "partial_success")
        self.assertTrue(result["new_data_fetched"])
        self.assertFalse(result["used_existing_pool_only"])
        self.assertEqual(result["searched_items"], 4)
        self.assertEqual(result["new_leads"], 2)
        self.assertEqual(result["recommended"], 0)
        self.assertIn("partially completed", result["failure_summary"])

        stage_results = {item["stage_name"]: item for item in result["stage_results"]}
        self.assertEqual(
            list(stage_results.keys()),
            ["planner", "searching", "entity_verification", "dd", "scoring", "recommendation"],
        )
        self.assertEqual(stage_results["planner"]["status"], "success")
        self.assertEqual(stage_results["searching"]["status"], "success")
        self.assertEqual(stage_results["entity_verification"]["status"], "success")
        self.assertEqual(stage_results["dd"]["status"], "partial_success")
        self.assertEqual(stage_results["scoring"]["status"], "skipped")
        self.assertEqual(stage_results["recommendation"]["status"], "success")
        self.assertEqual(
            result["scoring_skip_reasons"],
            ["no_dd_ready_leads", "all_waiting_human"],
        )
        self.assertEqual(
            stage_results["scoring"]["details"].get("skip_reasons"),
            ["no_dd_ready_leads", "all_waiting_human"],
        )
        self.assertEqual(result["lead_status_by_verification"]["verified"], 1)
        self.assertEqual(result["lead_status_by_verification"]["pending_review"], 1)
        self.assertEqual(result["lead_status_by_verification"]["rejected"], 2)
        self.assertTrue(result["recommendation_blockers"])

    def test_generic_single_token_entities_are_blocked_before_verified_company(self) -> None:
        settings = Settings()
        db = Database(settings.database_url)
        db.init_schema()
        agent = BaseSearchingAgent(
            source_name="brave",
            search_func=lambda query, count: None,
            db=db,
            llm=_DummyLLM(),
        )

        blocked_names = ["GenAI", "MCP", "Firewall", "Light", "Closing"]

        for blocked_name in blocked_names:
            agent.entity_verifier.resolve = lambda **kwargs: EntityResolution(
                raw_title=f"{blocked_name} raises seed round",
                candidate_name=blocked_name,
                normalized_name=blocked_name,
                entity_type="company",
                official_domain=f"{blocked_name.lower()}.example.com",
                verification_status="verified",
                verification_score=96.0,
                reject_reason="",
                source="brave",
                url=f"https://example.com/{blocked_name.lower()}",
                snippet="Enterprise AI security startup",
                query="agent security startup",
            )
            stats = agent.verify_and_store(
                items=[
                    {
                        "source": "brave",
                        "query": "agent security startup",
                        "title": f"{blocked_name} raises seed round",
                        "snippet": "Enterprise AI security startup",
                        "url": f"https://example.com/{blocked_name.lower()}",
                    }
                ],
                negative_filters=[],
            )
            self.assertEqual(stats["verified_items"], 0, msg=blocked_name)
            self.assertEqual(stats["rejected_items"], 1, msg=blocked_name)
            self.assertEqual(stats["matched_items"], 0, msg=blocked_name)

        self.assertEqual(len(db.list_leads(limit=20)), 0)


if __name__ == "__main__":
    unittest.main()
