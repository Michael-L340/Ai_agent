from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

import app.core.db as db_module
from app.clients.bocha_client import BochaSearchClient
from app.clients.brave_client import BraveSearchClient
from app.core.config import Settings
from app.core.runtime import AgentRuntime


class _DummyResponse:
    def __init__(self, status_code: int, *, text: str = "", json_payload: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self.text = text
        self._json_payload = json_payload or {}
        self.headers = headers or {}

    def json(self):
        return self._json_payload


class RunFailureReasonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def test_brave_429_maps_to_rate_limited(self) -> None:
        settings = Settings()
        settings.brave_api_key = "dummy-key"
        client = BraveSearchClient(settings)
        with patch("app.clients.brave_client.requests.get", return_value=_DummyResponse(
            429,
            text='{"error":"too many requests"}',
            headers={"Retry-After": "120"},
        )):
            result = client.execute_search("agent security startup", 2)

        self.assertEqual(result.failure_code, "rate_limited")
        self.assertEqual(result.http_status, 429)
        self.assertEqual(result.retry_after_sec, 120)
        self.assertTrue(result.retryable)

    def test_bocha_network_blocked_maps(self) -> None:
        settings = Settings()
        settings.bocha_api_key = "dummy-key"
        client = BochaSearchClient(settings)
        error = requests.exceptions.ConnectionError(
            "Failed to establish a new connection: [WinError 10013] access permissions do not allow this socket operation"
        )
        with patch("app.clients.bocha_client.requests.post", side_effect=error):
            result = client.execute_search("agent security startup", 2)

        self.assertEqual(result.failure_code, "network_blocked")
        self.assertEqual(result.failure_stage, "connect")
        self.assertFalse(result.retryable)

    def test_bocha_nested_success_response_parses_items(self) -> None:
        settings = Settings()
        settings.bocha_api_key = "dummy-key"
        client = BochaSearchClient(settings)
        payload = {
            "code": 200,
            "data": {
                "webPages": {
                    "value": [
                        {
                            "name": "Protect AI raises new round",
                            "url": "https://example.com/protect-ai",
                            "snippet": "Enterprise AI security platform.",
                        }
                    ]
                }
            },
        }
        with patch("app.clients.bocha_client.requests.post", return_value=_DummyResponse(
            200,
            text='{"code":200}',
            json_payload=payload,
        )):
            result = client.execute_search("agent security startup", 1)

        self.assertEqual(result.status, "success")
        self.assertEqual(result.items_received, 1)
        self.assertEqual(result.items[0]["title"], "Protect AI raises new round")

    def test_run_full_cycle_reports_existing_pool_only(self) -> None:
        runtime = AgentRuntime(Settings())
        runtime.init()

        runtime.planner.apply_feedback_learning = lambda: {}
        runtime.planner.get_search_plan = lambda: {
            "queries": ["agent security startup funding"],
            "channel_status": {"brave": True, "bocha": True},
            "negative_filters": [],
        }
        runtime.brave_search_agent.fetch = lambda queries, **kwargs: {
            "searched_items": 0,
            "items": [],
            "timed_out": False,
            "queries_total": len(queries),
            "queries_attempted": len(queries),
            "source_result": {
                "source_name": "brave",
                "status": "failed",
                "request_attempted": True,
                "request_succeeded": False,
                "items_received": 0,
                "failure_stage": "http_response",
                "failure_code": "rate_limited",
                "http_status": 429,
                "retry_after_sec": 60,
                "provider_message": "too many requests",
                "retryable": True,
                "action_hint": "Wait for the provider retry window, then rerun this source.",
            },
        }
        runtime.bocha_search_agent.fetch = lambda queries, **kwargs: {
            "searched_items": 0,
            "items": [],
            "timed_out": False,
            "queries_total": len(queries),
            "queries_attempted": len(queries),
            "source_result": {
                "source_name": "bocha",
                "status": "failed",
                "request_attempted": True,
                "request_succeeded": False,
                "items_received": 0,
                "failure_stage": "http_response",
                "failure_code": "quota_exhausted",
                "http_status": 403,
                "retry_after_sec": None,
                "provider_message": "quota exhausted",
                "retryable": False,
                "action_hint": "Top up or upgrade the bocha package before rerunning.",
            },
        }
        runtime.brave_search_agent.verify_and_store = lambda **kwargs: {
            "matched_items": 0,
            "verified_items": 0,
            "pending_review_items": 0,
            "rejected_items": 0,
            "new_leads": 0,
            "processed_items": 0,
            "timed_out": False,
            "capped": False,
            "remaining_items": 0,
        }
        runtime.bocha_search_agent.verify_and_store = lambda **kwargs: {
            "matched_items": 0,
            "verified_items": 0,
            "pending_review_items": 0,
            "rejected_items": 0,
            "new_leads": 0,
            "processed_items": 0,
            "timed_out": False,
            "capped": False,
            "remaining_items": 0,
        }
        runtime.dd_agent.run = lambda limit=50, deadline_ts=None: {
            "processed": 0,
            "dd_done": 0,
            "dd_partial": 0,
            "dd_pending_review": 0,
            "dd_waiting_human": 0,
            "questions_generated": 0,
            "input_count": 0,
            "remaining_count": 0,
            "timed_out": False,
        }
        runtime.interaction.publish_dd_questions = lambda: 0
        runtime.scoring_agent.run = lambda search_plan, **kwargs: {
            "scored": 3,
            "recommended": 1,
            "watchlist": 0,
            "track_only": 0,
            "rejected": 0,
            "input_count": 3,
            "remaining_count": 0,
            "timed_out": False,
        }
        runtime.interaction.publish_recommendations = lambda threshold: 1

        result = runtime.run_full_cycle()

        self.assertEqual(result["run_status"], "existing_pool_only")
        self.assertFalse(result["new_data_fetched"])
        self.assertTrue(result["used_existing_pool_only"])
        self.assertIn("No new data was fetched", result["failure_summary"])
        self.assertIn("brave", result["unavailable_sources"])
        self.assertIn("bocha", result["unavailable_sources"])
        self.assertIn("source_status_by_channel", result)
        self.assertTrue(result["action_suggestions"])


if __name__ == "__main__":
    unittest.main()
