from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.core.db as db_module
from app.agents.interaction.interaction_agent import InteractionAgent
from app.api.routes import router
from app.core.db import Database
from app.core.interaction_router import HumanMessageRouter
from app.services.dd_memory import DDMemoryStore


class _FakeRuntime:
    def __init__(self, db: Database):
        self.db = db
        self.settings = SimpleNamespace(
            recommend_score_threshold=75.0,
            openclaw_webhook_secret="",
            webhook_url="",
            webhook_timeout_seconds=5,
        )
        self.interaction = InteractionAgent(db, self.settings, dd_memory=DDMemoryStore(db))
        self.message_router = HumanMessageRouter(self)

    def handle_human_message(
        self,
        message: str,
        *,
        source: str = "direct",
        session_key: str | None = None,
        channel_id: str | None = None,
        sender: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        return self.message_router.handle(
            message=message,
            source=source,
            session_key=session_key,
            channel_id=channel_id,
            sender=sender,
            metadata=metadata,
        )

    def run_full_cycle(self) -> dict:
        return {
            "run_at": "2026-04-23T00:00:00+00:00",
            "searched_items": 0,
            "new_leads": 0,
            "dd_done": 0,
            "dd_waiting_human": 0,
            "dd_questions": 0,
            "scored": 0,
            "recommended": 0,
        }

    def refresh_strategy(self) -> dict:
        return {"today": "2026-04-23"}

    def update_channel(self, channel: str, enabled: bool) -> dict:
        return {"channel": channel, "enabled": enabled}

    def compress_memory(self) -> dict:
        return {"ok": True}

    def list_pending_outbox(self, limit: int = 50):
        return []

    def ack_outbox_events(self, event_ids: list[int]) -> int:
        return 0

    def list_conversation_messages(self, limit: int = 100):
        return self.db.list_conversation_messages(limit=limit)


class InteractionUtf8Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()
        self.runtime = _FakeRuntime(self.db)

        self.app = FastAPI()
        self.app.include_router(router)
        self.app.state.runtime = self.runtime
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.client.close()
        self.tmpdir.cleanup()

    def test_chat_utf8_smoke_and_db_roundtrip(self) -> None:
        samples = [
            ("我更关注 agent security 赛道", "feedback_prefer_sector", "赛道偏好"),
            ("主体错了，不是这个公司", "feedback_wrong_entity", "主体纠错"),
            ("先跳过这个", "feedback_skip", "跳过原因"),
        ]

        for message, expected_action, expected_reply_fragment in samples:
            response = self.client.post("/interaction/chat", json={"message": message})
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["action"], expected_action)
            self.assertIn(expected_reply_fragment, payload["reply"])

        feedback_rows = self.db.list_recent_feedback(limit=10)
        stored_contents = [row["content"] for row in feedback_rows]
        self.assertIn("我更关注 agent security 赛道", stored_contents)
        self.assertIn("主体错了，不是这个公司", stored_contents)
        self.assertIn("先跳过这个", stored_contents)

        messages = self.db.list_conversation_messages(limit=20)
        inbound_texts = [row["content"] for row in messages if row["direction"] == "inbound"]
        outbound_texts = [row["content"] for row in messages if row["direction"] == "outbound"]
        self.assertIn("我更关注 agent security 赛道", inbound_texts)
        self.assertTrue(any("赛道偏好" in text for text in outbound_texts))

    def test_mojibake_input_is_repaired_before_feedback_write(self) -> None:
        original = "我更关注 agent security 赛道"
        garbled = original.encode("utf-8").decode("latin-1")

        response = self.client.post("/interaction/chat", json={"message": garbled})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "feedback_prefer_sector")

        feedback_rows = self.db.list_recent_feedback(limit=5)
        self.assertEqual(feedback_rows[0]["content"], original)

        messages = self.db.list_conversation_messages(limit=5)
        inbound = next(row for row in messages if row["direction"] == "inbound")
        self.assertEqual(inbound["content"], original)

    def test_feedback_api_keeps_chinese_content(self) -> None:
        content = "我更关注 agent security 赛道"
        response = self.client.post(
            "/interaction/feedback",
            json={
                "lead_id": None,
                "verdict": "prefer_sector",
                "content": content,
                "feedback_type": "scoring_feedback",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ok"], True)

        feedback_rows = self.db.list_recent_feedback(limit=5)
        self.assertEqual(feedback_rows[0]["content"], content)


if __name__ == "__main__":
    unittest.main()
