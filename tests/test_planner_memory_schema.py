from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import app.core.db as db_module
from app.agents.planner.planner_agent import PlannerAgent
from app.core.db import Database


class PlannerMemorySchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_path = db_module.LOCAL_SQLITE_PATH
        db_module.LOCAL_SQLITE_PATH = Path(self.tmpdir.name) / "agent_local.db"
        self.db = Database("")
        self.db.init_schema()
        self.strategy_path = Path(self.tmpdir.name) / "daily_strategy.txt"
        self.strategy_path.write_text(
            "Today prioritize insurance AI security, agent security, enterprise deployment, and early funding signals.",
            encoding="utf-8",
        )
        self.settings = SimpleNamespace(daily_strategy_file=str(self.strategy_path))
        self.agent = PlannerAgent(self.db, self.settings)

    def tearDown(self) -> None:
        db_module.LOCAL_SQLITE_PATH = self.original_path
        self.tmpdir.cleanup()

    def _seed_recent_context(self) -> None:
        self.db.log_conversation_message(
            direction="inbound",
            source="direct",
            content="Recently insurance AI security is hot and enterprise customers are deploying agent security products.",
            sender="michael",
        )
        self.db.add_signal(
            lead_id=None,
            source="brave",
            query="insurance AI security startup funding",
            title="Insurance AI security startup wins enterprise customers",
            snippet="Paid pilot and production deployment across insurers.",
            url="https://example.org/insurance-ai-security",
            raw={},
        )
        self.db.add_signal(
            lead_id=None,
            source="bocha",
            query="agent security enterprise customers",
            title="Agent security platform raises seed funding",
            snippet="Runtime protection startup with B2B traction and customer deployment.",
            url="https://example.org/agent-security",
            raw={},
        )

    def test_planner_outputs_layered_memory_snapshot(self) -> None:
        self.agent.bootstrap_if_needed()
        self._seed_recent_context()
        self.db.add_feedback(verdict="prefer_sector", feedback_type="prefer_topic", content="insurance AI security")
        self.agent.apply_feedback_learning()
        short_memory = self.agent.refresh_short_memory()
        compressed = self.agent.compress_long_memory()
        snapshot = self.agent.get_memory_snapshot()

        self.assertIn("long_memory", snapshot)
        self.assertIn("short_memory", snapshot)
        self.assertIn("feedback_memory", snapshot)
        self.assertIn("feedback_events", snapshot)
        self.assertIn("compaction", snapshot)

        long_memory = snapshot["long_memory"]
        self.assertIn("sub_sectors", long_memory)
        self.assertIn("signal_dictionary", long_memory)
        self.assertIn("negative_filters", long_memory)
        self.assertIn("source_policy", long_memory)
        self.assertIn("human_preferences", long_memory)
        self.assertIn("version", long_memory)

        self.assertIn("emerging_themes", short_memory)
        self.assertIn("priority", short_memory)
        self.assertIn("keywords", short_memory)
        self.assertIn("source_suggestions", short_memory)
        self.assertIn("days_active", short_memory)
        self.assertIn("promote_candidate", short_memory)

        feedback_memory = snapshot["feedback_memory"]
        self.assertGreaterEqual(len(feedback_memory), 1)
        self.assertIn("feedback_type", feedback_memory[0])
        self.assertIn("target", feedback_memory[0])
        self.assertIn("value", feedback_memory[0])
        self.assertIn("status", feedback_memory[0])

        compaction = snapshot["compaction"]
        self.assertIn("promoted_themes", compaction)
        self.assertIn("decayed_themes", compaction)
        self.assertIn("merged_topics", compaction)
        self.assertIn("archived_preferences", compaction)
        self.assertIn("source_policy_changes", compaction)
        self.assertIn("compaction_result", compressed)
        self.assertIn("summary", compressed["compaction_result"])

    def test_new_short_term_theme_has_scores_and_source_suggestions(self) -> None:
        self.agent.bootstrap_if_needed()
        self._seed_recent_context()
        self.db.add_feedback(verdict="prefer_sector", feedback_type="prefer_topic", content="insurance AI security")
        self.agent.apply_feedback_learning()
        short_memory = self.agent.refresh_short_memory()

        themes = {item["theme"]: item for item in short_memory["emerging_themes"]}
        self.assertIn("insurance ai security", themes)

        insurance_theme = themes["insurance ai security"]
        for key in [
            "recency_score",
            "source_diversity_score",
            "commercial_signal_score",
            "human_preference_score",
            "new_theme_score",
            "source_suggestions",
            "promote_candidate",
            "promotion_reason",
        ]:
            self.assertIn(key, insurance_theme)

        self.assertTrue(insurance_theme["source_suggestions"])
        self.assertGreaterEqual(float(insurance_theme["source_diversity_score"]), 2.0)
        self.assertGreaterEqual(float(insurance_theme["commercial_signal_score"]), 1.0)

    def test_structured_feedback_changes_source_policy_and_search_plan(self) -> None:
        self.agent.bootstrap_if_needed()
        self._seed_recent_context()
        self.db.add_feedback(verdict="note", feedback_type="pause_source", content="pause bocha")
        self.db.add_feedback(verdict="note", feedback_type="boost_signal", content="enterprise deployment")
        self.db.add_feedback(verdict="note", feedback_type="deprioritize_pattern", content="what is")
        self.db.add_feedback(verdict="note", feedback_type="prefer_topic", content="insurance AI security")

        plan = self.agent.get_search_plan()
        memory = self.agent.get_memory_snapshot()["long_memory"]
        source_policy = memory["source_policy"]

        self.assertFalse(plan["channel_status"]["bocha"])
        self.assertFalse(source_policy["channel_status"]["bocha"])
        self.assertIn("enterprise deployment", [item.lower() for item in memory["signal_dictionary"]])
        self.assertIn("what is", [item.lower() for item in plan["negative_filters"]])
        self.assertIn("insurance ai security ai startup", [query.lower() for query in plan["queries"]])
        self.assertIn("insurance ai security", {k.lower(): v for k, v in plan["source_suggestions"].items()})
        self.assertEqual(plan["source_suggestions"]["insurance ai security"], ["brave"])

    def test_promote_and_reject_theme_affect_long_memory_and_output(self) -> None:
        self.agent.bootstrap_if_needed()
        self._seed_recent_context()
        self.db.add_feedback(verdict="note", feedback_type="promote_theme", content="promote healthcare AI security")
        self.db.add_feedback(verdict="note", feedback_type="reject_theme", content="reject prompt injection defense")
        self.agent.apply_feedback_learning()
        short_memory = self.agent.refresh_short_memory()
        compaction_memory = self.agent.compress_long_memory()
        compaction = compaction_memory["compaction_result"]
        long_memory = self.agent.get_memory_snapshot()["long_memory"]

        self.assertIn("healthcare ai security", [item.lower() for item in long_memory["sub_sectors"]])
        self.assertNotIn("prompt injection defense", [item.lower() for item in long_memory["sub_sectors"]])
        self.assertIn("healthcare ai security", [item.lower() for item in compaction["promoted_themes"]])
        self.assertIn("prompt injection defense", [item.lower() for item in compaction["decayed_themes"]])
        self.assertNotIn(
            "prompt injection defense",
            [str(item["theme"]).lower() for item in short_memory["emerging_themes"]],
        )

    def test_compaction_run_is_auditable(self) -> None:
        self.agent.bootstrap_if_needed()
        self._seed_recent_context()
        self.db.add_feedback(verdict="note", feedback_type="pause_source", content="pause bocha")
        self.db.add_feedback(verdict="note", feedback_type="resume_source", content="resume bocha")
        self.db.add_feedback(verdict="note", feedback_type="prefer_topic", content="insurance AI security")
        self.agent.apply_feedback_learning()
        self.agent.refresh_short_memory()
        self.agent.compress_long_memory()
        snapshot = self.agent.get_memory_snapshot()

        events = snapshot["feedback_events"]
        self.assertTrue(events)
        self.assertTrue(any(item["merged"] for item in events))
        self.assertTrue(any(item["status"] == "archived" for item in snapshot["feedback_memory"]))
        self.assertIn("source_policy_changes", snapshot["compaction"])


if __name__ == "__main__":
    unittest.main()
