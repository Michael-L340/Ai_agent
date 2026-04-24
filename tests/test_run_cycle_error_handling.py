from __future__ import annotations

import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import router
from app.core.interaction_router import HumanMessageRouter


class _ExplodingRuntime:
    def __init__(self):
        self.settings = type("Settings", (), {"recommend_score_threshold": 75.0})()
        self.db = type("DB", (), {"log_conversation_message": lambda *args, **kwargs: None})()

    def run_full_cycle(self):
        raise RuntimeError("boom")

    def handle_human_message(self, message: str, **kwargs):
        raise RuntimeError("chat boom")


class _RouterRuntime:
    def __init__(self):
        self.db = type("DB", (), {"log_conversation_message": lambda *args, **kwargs: None})()

    def run_full_cycle(self):
        raise RuntimeError("router boom")


class RunCycleErrorHandlingTests(unittest.TestCase):
    def test_router_returns_structured_error_for_run_cycle_exception(self):
        router_runtime = _RouterRuntime()
        message_router = HumanMessageRouter(router_runtime)

        result = message_router.handle("跑一轮")

        self.assertFalse(result["ok"])
        self.assertEqual(result["action"], "run_cycle")
        self.assertEqual(result["data"]["run_status"], "internal_error")
        self.assertIn("router boom", result["data"]["failure_summary"])
        self.assertIn("action_suggestions", result["data"])

    def test_interaction_chat_does_not_return_http_500_on_runtime_exception(self):
        app = FastAPI()
        app.include_router(router)
        app.state.runtime = _ExplodingRuntime()
        client = TestClient(app)

        response = client.post("/interaction/chat", json={"message": "跑一轮"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["action"], "interaction_chat_error")
        self.assertEqual(payload["data"]["run_status"], "internal_error")
        self.assertIn("chat boom", payload["data"]["failure_summary"])

    def test_interaction_command_run_cycle_does_not_return_http_500_on_exception(self):
        app = FastAPI()
        app.include_router(router)
        app.state.runtime = _ExplodingRuntime()
        client = TestClient(app)

        response = client.post("/interaction/command", json={"command": "run_cycle", "data": {}})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["result"]["run_status"], "internal_error")
        self.assertIn("boom", payload["result"]["failure_summary"])


if __name__ == "__main__":
    unittest.main()
