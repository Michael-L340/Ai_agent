from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

from app.agents.dd.dd_agent import DDAgent
from app.agents.interaction.interaction_agent import InteractionAgent
from app.agents.planner.planner_agent import PlannerAgent
from app.agents.scoring.scoring_agent import ScoringAgent
from app.agents.searching.searching_agents import BochaSearchingAgent, BraveSearchingAgent
from app.clients.bocha_client import BochaSearchClient
from app.clients.brave_client import BraveSearchClient
from app.clients.llm_client import LLMClient
from app.core.config import Settings
from app.core.db import Database
from app.core.interaction_router import HumanMessageRouter
from app.services.dd_memory import DDMemoryStore
from app.services.network_diagnostics import NetworkDiagnostics
from app.models.run_diagnostics import RunDiagnostics
from app.models.run_stage_result import RunStageResult


class AgentRuntime:
    STAGE_TIMEOUT_SECONDS = {
        "planner": 10.0,
        "searching": 70.0,
        "entity_verification": 35.0,
        "dd": 45.0,
        "scoring": 20.0,
        "recommendation": 10.0,
    }
    MAX_ENTITY_VERIFICATION_ITEMS = 60
    MAX_DD_ITEMS_PER_CYCLE = 6
    MAX_SCORING_ITEMS_PER_CYCLE = 60

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = Database(settings.database_url, mvp_mode=bool(getattr(settings, "mvp_mode", False)))
        self.llm = LLMClient(settings)

        self.brave_client = BraveSearchClient(settings)
        self.bocha_client = BochaSearchClient(settings)
        self.dd_memory = DDMemoryStore(self.db)
        self.network_diagnostics = NetworkDiagnostics()

        self.planner = PlannerAgent(self.db, settings)
        self.interaction = InteractionAgent(self.db, settings, dd_memory=self.dd_memory)
        self.message_router = HumanMessageRouter(self)
        self.dd_agent = DDAgent(
            self.db,
            self.brave_client,
            self.bocha_client,
            self.llm,
            dd_memory=self.dd_memory,
        )
        self.brave_search_agent = BraveSearchingAgent(self.brave_client, self.db, self.llm)
        self.bocha_search_agent = BochaSearchingAgent(self.bocha_client, self.db, self.llm)
        self.scoring_agent = ScoringAgent(self.db, settings)

    def init(self) -> None:
        self.db.init_schema()
        if not self.settings.demo_mode:
            self.db.purge_demo_data()
        self.planner.bootstrap_if_needed()
        self.planner.refresh_short_memory()

    def run_full_cycle(self) -> dict[str, Any]:
        run_at = datetime.now(timezone.utc)
        stage_results: list[RunStageResult] = []
        source_status_by_channel: dict[str, dict[str, Any]] = {}
        lead_status_by_verification = {"verified": 0, "likely_company": 0, "pending_review": 0, "rejected": 0}

        searched_items = 0
        new_leads = 0
        likely_company = 0
        dd_done = 0
        dd_partial = 0
        dd_pending_review = 0
        dd_waiting_human = 0
        dd_questions = 0
        scored = 0
        recommended = 0
        dd_questions_published = 0
        scoring_skip_reasons: list[str] = []

        search_plan: dict[str, Any] = {}
        channel_status: dict[str, Any] = {}
        queries: list[str] = []
        negative_filters: list[str] = []
        raw_items_by_channel: dict[str, list[dict[str, str]]] = {}

        planner_started_at, planner_started_perf = self._stage_start()
        try:
            self.planner.apply_feedback_learning()
            search_plan = self.planner.get_search_plan()
            channel_status = dict(search_plan.get("channel_status", {}))
            queries = list(search_plan.get("queries", []))
            negative_filters = list(search_plan.get("negative_filters", []))
            stage_results.append(
                self._stage_success(
                    stage_name="planner",
                    started_at=planner_started_at,
                    started_perf=planner_started_perf,
                    input_count=0,
                    output_count=len(queries),
                    details={
                        "query_count": len(queries),
                        "enabled_sources": [
                            name for name, enabled in channel_status.items() if enabled
                        ],
                    },
                )
            )
        except Exception as exc:
            stage_results.append(
                self._stage_failure(
                    stage_name="planner",
                    started_at=planner_started_at,
                    started_perf=planner_started_perf,
                    input_count=0,
                    output_count=0,
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                )
            )
            diagnostics = self._build_run_diagnostics(
                searched_items=searched_items,
                new_leads=new_leads,
                dd_stats={
                    "processed": 0,
                    "dd_done": 0,
                    "dd_partial": 0,
                    "dd_pending_review": 0,
                    "dd_waiting_human": 0,
                    "questions_generated": 0,
                },
                score_stats={
                    "scored": 0,
                    "recommended": 0,
                    "watchlist": 0,
                    "track_only": 0,
                    "rejected": 0,
                },
                recommended=0,
                source_status_by_channel=source_status_by_channel,
                stage_results=stage_results,
                lead_status_by_verification=lead_status_by_verification,
            )
            return {
                "run_at": run_at,
                "searched_items": 0,
                "new_leads": 0,
                "likely_company": 0,
                "dd_done": 0,
                "dd_partial": 0,
                "dd_pending_review": 0,
                "dd_waiting_human": 0,
                "dd_questions": 0,
                "dd_questions_published": 0,
                "scored": 0,
                "recommended": 0,
                "scoring_skip_reasons": [],
                **diagnostics.to_dict(),
            }

        searching_started_at, searching_started_perf = self._stage_start()
        searching_timed_out = False
        enabled_sources = [
            ("brave", self.brave_search_agent),
            ("bocha", self.bocha_search_agent),
        ]
        enabled_count = sum(1 for name, _ in enabled_sources if channel_status.get(name, True))
        per_source_budget = (
            self.STAGE_TIMEOUT_SECONDS["searching"] / max(1, enabled_count)
            if enabled_count
            else self.STAGE_TIMEOUT_SECONDS["searching"]
        )
        search_output_count = 0

        try:
            for source_name, agent in enabled_sources:
                if not channel_status.get(source_name, True):
                    source_status_by_channel[source_name] = {
                        "source_name": source_name,
                        "status": "disabled",
                        "request_attempted": False,
                        "request_succeeded": False,
                        "items_received": 0,
                        "failure_stage": "planner",
                        "failure_code": "",
                        "http_status": None,
                        "retry_after_sec": None,
                        "provider_message": "",
                        "retryable": False,
                        "action_hint": f"Enable the {source_name} channel in planner source_policy to use it.",
                    }
                    raw_items_by_channel[source_name] = []
                    continue

                fetch_result = agent.fetch(
                    queries=queries,
                    deadline_ts=time.monotonic() + per_source_budget,
                )
                searched_items += int(fetch_result["searched_items"])
                raw_items_by_channel[source_name] = list(fetch_result["items"])
                source_status_by_channel[source_name] = dict(fetch_result["source_result"])
                searching_timed_out = searching_timed_out or bool(fetch_result["timed_out"])

            search_output_count = sum(len(items) for items in raw_items_by_channel.values())
            source_statuses = [str(item.get("status") or "") for item in source_status_by_channel.values()]
            source_failures = [status for status in source_statuses if status in {"failed", "timeout"}]
            source_partials = [status for status in source_statuses if status == "partial_success"]
            source_successes = [status for status in source_statuses if status == "success"]

            if searching_timed_out and search_output_count > 0:
                searching_stage_status = "partial_success"
                searching_error_message = "Searching hit its stage deadline before all source queries completed."
                searching_error_type = "TimeoutError"
            elif searching_timed_out:
                searching_stage_status = "timeout"
                searching_error_message = "Searching hit its stage deadline before any source returned usable items."
                searching_error_type = "TimeoutError"
            elif source_successes and (source_failures or source_partials):
                searching_stage_status = "partial_success"
                searching_error_message = "At least one source succeeded, but other sources failed or returned partial data."
                searching_error_type = ""
            elif source_failures and not source_successes and not source_partials:
                searching_stage_status = "failed"
                searching_error_message = "All enabled sources failed before returning usable search results."
                searching_error_type = "SourceExecutionError"
            elif len(queries) * max(1, enabled_count) == 0:
                searching_stage_status = "skipped"
                searching_error_message = ""
                searching_error_type = ""
            else:
                searching_stage_status = "success"
                searching_error_message = ""
                searching_error_type = ""

            stage_results.append(
                self._stage_success(
                    stage_name="searching",
                    started_at=searching_started_at,
                    started_perf=searching_started_perf,
                    input_count=len(queries) * max(1, enabled_count),
                    output_count=search_output_count,
                    status=searching_stage_status,
                    error_type=searching_error_type,
                    error_message=searching_error_message,
                    details={
                        "searched_items": searched_items,
                        "queries": len(queries),
                        "channel_status": channel_status,
                        "source_items": {
                            source_name: len(items) for source_name, items in raw_items_by_channel.items()
                        },
                    },
                )
            )
        except Exception as exc:
            stage_results.append(
                self._stage_failure(
                    stage_name="searching",
                    started_at=searching_started_at,
                    started_perf=searching_started_perf,
                    input_count=len(queries) * max(1, enabled_count),
                    output_count=sum(len(items) for items in raw_items_by_channel.values()),
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    details={
                        "searched_items": searched_items,
                        "queries": len(queries),
                        "channel_status": channel_status,
                    },
                )
            )

        entity_started_at, entity_started_perf = self._stage_start()
        entity_stats = {
            "matched_items": 0,
            "verified_items": 0,
            "likely_company_items": 0,
            "pending_review_items": 0,
            "rejected_items": 0,
            "new_leads": 0,
            "processed_items": 0,
            "timed_out": False,
            "capped": False,
            "remaining_items": 0,
        }
        entity_input_count = search_output_count
        entity_deadline = time.monotonic() + self.STAGE_TIMEOUT_SECONDS["entity_verification"]

        if entity_input_count > 0:
            try:
                for source_name, agent in enabled_sources:
                    remaining_limit = self.MAX_ENTITY_VERIFICATION_ITEMS - int(entity_stats["processed_items"])
                    if remaining_limit <= 0:
                        entity_stats["capped"] = True
                        break
                    items = raw_items_by_channel.get(source_name, [])
                    if not items:
                        continue
                    verify_result = agent.verify_and_store(
                        items=items,
                        negative_filters=negative_filters,
                        deadline_ts=entity_deadline,
                        item_limit=remaining_limit,
                    )
                    for key in (
                        "matched_items",
                        "verified_items",
                        "likely_company_items",
                        "pending_review_items",
                        "rejected_items",
                        "new_leads",
                        "processed_items",
                        "remaining_items",
                    ):
                        entity_stats[key] = int(entity_stats.get(key, 0) or 0) + int(verify_result.get(key, 0) or 0)
                    entity_stats["timed_out"] = bool(entity_stats["timed_out"] or verify_result.get("timed_out"))
                    entity_stats["capped"] = bool(entity_stats["capped"] or verify_result.get("capped"))

                new_leads += int(entity_stats["new_leads"])
                likely_company += int(entity_stats.get("likely_company_items", 0))
                lead_status_by_verification = {
                    "verified": int(entity_stats["verified_items"]),
                    "likely_company": int(entity_stats.get("likely_company_items", 0)),
                    "pending_review": int(entity_stats["pending_review_items"]),
                    "rejected": int(entity_stats["rejected_items"]),
                }
                entity_error_message = ""
                if entity_stats["timed_out"]:
                    entity_error_message = "Entity verification stage hit its timeout before finishing all items."
                elif entity_stats["capped"]:
                    entity_error_message = "Entity verification hit the per-run item cap."
                stage_results.append(
                    self._stage_from_flags(
                        stage_name="entity_verification",
                        started_at=entity_started_at,
                        started_perf=entity_started_perf,
                        input_count=entity_input_count,
                        output_count=int(entity_stats["processed_items"]),
                        timed_out=bool(entity_stats["timed_out"] or entity_stats["capped"]),
                        has_partial_output=int(entity_stats["processed_items"]) > 0,
                        error_message=entity_error_message,
                        details={
                            "matched_items": entity_stats["matched_items"],
                            "verified_items": entity_stats["verified_items"],
                            "likely_company_items": entity_stats.get("likely_company_items", 0),
                            "pending_review_items": entity_stats["pending_review_items"],
                            "rejected_items": entity_stats["rejected_items"],
                            "new_leads": entity_stats["new_leads"],
                            "remaining_items": entity_stats["remaining_items"],
                            "capped": bool(entity_stats["capped"]),
                        },
                    )
                )
            except Exception as exc:
                stage_results.append(
                    self._stage_failure(
                        stage_name="entity_verification",
                        started_at=entity_started_at,
                        started_perf=entity_started_perf,
                        input_count=entity_input_count,
                        output_count=int(entity_stats["processed_items"]),
                        error_type=exc.__class__.__name__,
                        error_message=str(exc),
                        details={
                            "matched_items": entity_stats["matched_items"],
                            "verified_items": entity_stats["verified_items"],
                            "likely_company_items": entity_stats.get("likely_company_items", 0),
                            "pending_review_items": entity_stats["pending_review_items"],
                            "rejected_items": entity_stats["rejected_items"],
                            "new_leads": entity_stats["new_leads"],
                            "remaining_items": entity_stats["remaining_items"],
                            "capped": bool(entity_stats["capped"]),
                        },
                    )
                )
        else:
            stage_results.append(
                self._stage_success(
                    stage_name="entity_verification",
                    started_at=entity_started_at,
                    started_perf=entity_started_perf,
                    input_count=0,
                    output_count=0,
                    status="skipped",
                    details={"reason": "No search items were available for entity verification."},
                )
            )

        dd_started_at, dd_started_perf = self._stage_start()
        dd_stats = {
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
        try:
            dd_stats = self.dd_agent.run(
                limit=self.MAX_DD_ITEMS_PER_CYCLE,
                deadline_ts=time.monotonic() + self.STAGE_TIMEOUT_SECONDS["dd"],
            )
            dd_done += int(dd_stats["dd_done"])
            dd_partial += int(dd_stats.get("dd_partial", 0))
            dd_pending_review += int(dd_stats.get("dd_pending_review", 0))
            dd_waiting_human += int(dd_stats.get("dd_waiting_human", 0))
            dd_questions += int(dd_stats.get("questions_generated", 0))
            dd_stage_status = self._timed_stage_status(
                processed=int(dd_stats.get("processed", 0)),
                total=int(dd_stats.get("input_count", 0)),
                timed_out=bool(dd_stats.get("timed_out")),
            )
            dd_stage_error = "DD stage hit its timeout before finishing all queued leads." if dd_stage_status in {"timeout", "partial_success"} and dd_stats.get("timed_out") else ""
            stage_results.append(
                self._stage_success(
                    stage_name="dd",
                    started_at=dd_started_at,
                    started_perf=dd_started_perf,
                    input_count=int(dd_stats.get("input_count", 0)),
                    output_count=int(dd_stats.get("processed", 0)),
                    status=dd_stage_status,
                    error_type="TimeoutError" if dd_stage_error else "",
                    error_message=dd_stage_error,
                    details={
                        "dd_done": dd_stats.get("dd_done", 0),
                        "dd_partial": dd_stats.get("dd_partial", 0),
                        "dd_pending_review": dd_stats.get("dd_pending_review", 0),
                        "dd_waiting_human": dd_stats.get("dd_waiting_human", 0),
                        "questions_generated": dd_stats.get("questions_generated", 0),
                        "remaining_count": dd_stats.get("remaining_count", 0),
                    },
                )
            )
        except Exception as exc:
            stage_results.append(
                self._stage_failure(
                    stage_name="dd",
                    started_at=dd_started_at,
                    started_perf=dd_started_perf,
                    input_count=int(dd_stats.get("input_count", 0)),
                    output_count=int(dd_stats.get("processed", 0)),
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    details={"remaining_count": dd_stats.get("remaining_count", 0)},
                )
            )

        scoring_started_at, scoring_started_perf = self._stage_start()
        score_stats = {
            "scored": 0,
            "recommended": 0,
            "watchlist": 0,
            "track_only": 0,
            "rejected": 0,
            "input_count": 0,
            "remaining_count": 0,
            "timed_out": False,
        }
        try:
            score_stats = self.scoring_agent.run(
                search_plan=search_plan,
                limit=self.MAX_SCORING_ITEMS_PER_CYCLE,
                deadline_ts=time.monotonic() + self.STAGE_TIMEOUT_SECONDS["scoring"],
            )
            scored = int(score_stats["scored"])
            scoring_stage_status = self._timed_stage_status(
                processed=int(score_stats.get("scored", 0)),
                total=int(score_stats.get("input_count", 0)),
                timed_out=bool(score_stats.get("timed_out")),
            )
            scoring_stage_error = "Scoring stage hit its timeout before finishing all candidates." if scoring_stage_status in {"timeout", "partial_success"} and score_stats.get("timed_out") else ""
            stage_results.append(
                self._stage_success(
                    stage_name="scoring",
                    started_at=scoring_started_at,
                    started_perf=scoring_started_perf,
                    input_count=int(score_stats.get("input_count", 0)),
                    output_count=scored,
                    status=scoring_stage_status,
                    error_type="TimeoutError" if scoring_stage_error else "",
                    error_message=scoring_stage_error,
                    details={
                        "recommended": score_stats.get("recommended", 0),
                        "watchlist": score_stats.get("watchlist", 0),
                        "track_only": score_stats.get("track_only", 0),
                        "rejected": score_stats.get("rejected", 0),
                        "remaining_count": score_stats.get("remaining_count", 0),
                        "skip_reasons": [],
                    },
                )
            )
        except Exception as exc:
            stage_results.append(
                self._stage_failure(
                    stage_name="scoring",
                    started_at=scoring_started_at,
                    started_perf=scoring_started_perf,
                    input_count=int(score_stats.get("input_count", 0)),
                    output_count=int(score_stats.get("scored", 0)),
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    details={"remaining_count": score_stats.get("remaining_count", 0)},
                )
            )

        blocker_counts = self.db.get_recommendation_blocker_counts()
        scoring_skip_reasons = self._derive_scoring_skip_reasons(
            score_stats=score_stats,
            dd_stats=dd_stats,
            lead_status_by_verification=lead_status_by_verification,
            blocker_counts=blocker_counts,
        )
        for stage in stage_results:
            if stage.stage_name == "scoring":
                stage.details = dict(stage.details or {})
                stage.details["skip_reasons"] = list(scoring_skip_reasons)
                break

        recommendation_started_at, recommendation_started_perf = self._stage_start()
        recommendation_error_type = ""
        recommendation_error_message = ""
        recommendation_status = "success"
        try:
            dd_questions_published = self.interaction.publish_dd_questions()
            recommended = self.interaction.publish_recommendations(
                threshold=self.settings.recommend_score_threshold
            )
        except Exception as exc:
            recommendation_status = "failed"
            recommendation_error_type = exc.__class__.__name__
            recommendation_error_message = str(exc)
        stage_results.append(
            self._stage_success(
                stage_name="recommendation",
                started_at=recommendation_started_at,
                started_perf=recommendation_started_perf,
                input_count=scored,
                output_count=recommended,
                status=recommendation_status,
                error_type=recommendation_error_type,
                error_message=recommendation_error_message,
                details={
                    "dd_questions_published": dd_questions_published,
                    "recommendation_threshold": self.settings.recommend_score_threshold,
                },
            )
        )

        diagnostics = self._build_run_diagnostics(
            searched_items=searched_items,
            new_leads=new_leads,
            dd_stats=dd_stats,
            score_stats=score_stats,
            recommended=recommended,
            source_status_by_channel=source_status_by_channel,
            stage_results=stage_results,
            lead_status_by_verification=lead_status_by_verification,
            blocker_counts=blocker_counts,
            scoring_skip_reasons=scoring_skip_reasons,
        )

        return {
            "run_at": run_at,
            "searched_items": searched_items,
            "new_leads": new_leads,
            "likely_company": likely_company,
            "dd_done": dd_done,
            "dd_partial": dd_partial,
            "dd_pending_review": dd_pending_review,
            "dd_waiting_human": dd_waiting_human,
            "dd_questions": dd_questions,
            "dd_questions_published": dd_questions_published,
            "scored": scored,
            "recommended": recommended,
            "scoring_skip_reasons": scoring_skip_reasons,
            **diagnostics.to_dict(),
        }

    def refresh_strategy(self) -> dict[str, Any]:
        return self.planner.refresh_short_memory()

    def update_channel(self, channel: str, enabled: bool) -> dict[str, Any]:
        return self.planner.update_channel_status(channel=channel, enabled=enabled)

    def compress_memory(self) -> dict[str, Any]:
        return self.planner.compress_long_memory()

    def handle_human_message(
        self,
        message: str,
        *,
        source: str = "direct",
        session_key: str | None = None,
        channel_id: str | None = None,
        sender: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.message_router.handle(
            message=message,
            source=source,
            session_key=session_key,
            channel_id=channel_id,
            sender=sender,
            metadata=metadata,
        )

    def list_pending_outbox(self, limit: int = 50):
        return self.db.list_pending_outbox(limit=limit)

    def ack_outbox_events(self, event_ids: list[int]) -> int:
        return self.db.mark_outbox_sent(event_ids)

    def list_conversation_messages(self, limit: int = 100):
        return self.db.list_conversation_messages(limit=limit)

    def rerun_dd_for_lead(self, lead_id: int) -> dict[str, Any]:
        lead = self.db.get_lead_by_id(lead_id)
        if not lead:
            return {"ok": False, "reason": "lead_not_found", "lead_id": lead_id}
        stats = self.dd_agent.run_for_lead(dict(lead))
        questions_published = self.interaction.publish_dd_questions()
        return {
            "ok": True,
            "lead_id": lead_id,
            "dd_status": stats.get("dd_status"),
            "dd_done": stats.get("dd_done", 0),
            "dd_partial": stats.get("dd_partial", 0),
            "dd_pending_review": stats.get("dd_pending_review", 0),
            "dd_waiting_human": stats.get("dd_waiting_human", 0),
            "questions_generated": stats.get("questions_generated", 0),
            "questions_published": questions_published,
        }

    def _build_run_diagnostics(
        self,
        *,
        searched_items: int,
        new_leads: int,
        dd_stats: dict[str, Any],
        score_stats: dict[str, Any],
        recommended: int,
        source_status_by_channel: dict[str, dict[str, Any]],
        stage_results: list[RunStageResult],
        lead_status_by_verification: dict[str, int] | None = None,
        blocker_counts: dict[str, int] | None = None,
        scoring_skip_reasons: list[str] | None = None,
    ) -> RunDiagnostics:
        lead_status_by_verification = lead_status_by_verification or {
            "verified": 0,
            "likely_company": 0,
            "pending_review": 0,
            "rejected": 0,
        }
        new_data_fetched = any(int(item.get("items_received") or 0) > 0 for item in source_status_by_channel.values())
        used_existing_pool_only = (not new_data_fetched) and (
            int(score_stats.get("scored", 0) or 0) > 0
            or int(recommended or 0) > 0
            or int(dd_stats.get("processed", 0) or 0) > 0
        )
        blocker_counts = blocker_counts or self.db.get_recommendation_blocker_counts()
        scoring_skip_reasons = list(scoring_skip_reasons or [])

        unavailable_sources = [
            name
            for name, item in source_status_by_channel.items()
            if str(item.get("status") or "") in {"failed", "partial_success", "timeout"}
            and str(item.get("failure_code") or "")
        ]

        action_suggestions: list[str] = []
        failure_bits: list[str] = []
        for name, item in source_status_by_channel.items():
            if item.get("failure_code"):
                failure_bits.append(self.network_diagnostics.summarize_source_failure(name, item))
            hint = str(item.get("action_hint") or "").strip()
            if hint and hint not in action_suggestions:
                action_suggestions.append(hint)

        timed_out_stages = [stage for stage in stage_results if stage.status == "timeout"]
        failed_stages = [stage for stage in stage_results if stage.status == "failed"]
        partial_stages = [stage for stage in stage_results if stage.status == "partial_success"]

        if used_existing_pool_only:
            run_status = "existing_pool_only"
        elif failed_stages or timed_out_stages:
            made_progress = new_data_fetched or new_leads > 0 or int(dd_stats.get("processed", 0) or 0) > 0 or int(score_stats.get("scored", 0) or 0) > 0
            run_status = "degraded" if made_progress else "failed"
        elif partial_stages:
            run_status = "partial_success"
        elif new_data_fetched:
            run_status = "success"
        elif unavailable_sources:
            run_status = "failed"
        else:
            run_status = "no_data"

        recommendation_blockers = self._recommendation_blockers(
            recommended=recommended,
            searched_items=searched_items,
            new_leads=new_leads,
            dd_stats=dd_stats,
            score_stats=score_stats,
            stage_results=stage_results,
            lead_status_by_verification=lead_status_by_verification,
            blocker_counts=blocker_counts,
        )

        if run_status == "success":
            failure_summary = "Fetched new data successfully and completed this cycle."
        elif run_status == "partial_success":
            failure_summary = self._partial_failure_summary(
                failure_bits=failure_bits,
                stage_results=stage_results,
                searched_items=searched_items,
                new_leads=new_leads,
            )
        elif run_status == "degraded":
            failure_summary = self._degraded_failure_summary(
                stage_results=stage_results,
                searched_items=searched_items,
                new_leads=new_leads,
                failure_bits=failure_bits,
            )
        elif run_status == "existing_pool_only":
            suffix = "; ".join(failure_bits[:3]) if failure_bits else "no new source data was fetched"
            failure_summary = (
                "No new data was fetched; this run mainly refreshed the existing lead pool. "
                f"Source status: {suffix}."
            )
        elif run_status == "failed":
            if failure_bits:
                failure_summary = "This run did not fetch new data because: " + "; ".join(failure_bits[:3])
            else:
                failure_summary = self._degraded_failure_summary(
                    stage_results=stage_results,
                    searched_items=searched_items,
                    new_leads=new_leads,
                    failure_bits=[],
                )
        else:
            failure_summary = "This run did not fetch new data and did not process the existing pool."

        if not action_suggestions and run_status in {"failed", "existing_pool_only"}:
            action_suggestions.append("Check source_status_by_channel for the failing provider and rerun after fixing the issue.")
        if recommended == 0 and recommendation_blockers:
            action_suggestions.extend(
                blocker
                for blocker in recommendation_blockers
                if blocker not in action_suggestions
            )

        return RunDiagnostics(
            run_status=run_status,
            new_data_fetched=new_data_fetched,
            used_existing_pool_only=used_existing_pool_only,
            failure_summary=failure_summary,
            unavailable_sources=unavailable_sources,
            source_status_by_channel=source_status_by_channel,
            action_suggestions=action_suggestions,
            stage_results=stage_results,
            recommendation_blockers=recommendation_blockers,
            lead_status_by_verification=lead_status_by_verification,
            scoring_skip_reasons=scoring_skip_reasons,
        )

    @staticmethod
    def _stage_start() -> tuple[datetime, float]:
        return datetime.now(timezone.utc), time.monotonic()

    def _stage_success(
        self,
        *,
        stage_name: str,
        started_at: datetime,
        started_perf: float,
        input_count: int,
        output_count: int,
        status: str = "success",
        error_type: str = "",
        error_message: str = "",
        details: dict[str, Any] | None = None,
    ) -> RunStageResult:
        ended_at = datetime.now(timezone.utc)
        return RunStageResult(
            stage_name=stage_name,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            duration_sec=round(max(0.0, time.monotonic() - started_perf), 3),
            input_count=input_count,
            output_count=output_count,
            error_type=error_type,
            error_message=error_message,
            details=details or {},
        )

    def _stage_failure(
        self,
        *,
        stage_name: str,
        started_at: datetime,
        started_perf: float,
        input_count: int,
        output_count: int,
        error_type: str,
        error_message: str,
        status: str = "failed",
        details: dict[str, Any] | None = None,
    ) -> RunStageResult:
        return self._stage_success(
            stage_name=stage_name,
            started_at=started_at,
            started_perf=started_perf,
            input_count=input_count,
            output_count=output_count,
            status=status,
            error_type=error_type,
            error_message=error_message,
            details=details or {},
        )

    def _stage_from_flags(
        self,
        *,
        stage_name: str,
        started_at: datetime,
        started_perf: float,
        input_count: int,
        output_count: int,
        timed_out: bool,
        has_partial_output: bool,
        error_message: str = "",
        details: dict[str, Any] | None = None,
    ) -> RunStageResult:
        if timed_out and has_partial_output:
            status = "partial_success"
            error_type = "TimeoutError"
        elif timed_out:
            status = "timeout"
            error_type = "TimeoutError"
        elif input_count == 0:
            status = "skipped"
            error_type = ""
        else:
            status = "success"
            error_type = ""
        return self._stage_success(
            stage_name=stage_name,
            started_at=started_at,
            started_perf=started_perf,
            input_count=input_count,
            output_count=output_count,
            status=status,
            error_type=error_type,
            error_message=error_message,
            details=details or {},
        )

    @staticmethod
    def _timed_stage_status(*, processed: int, total: int, timed_out: bool) -> str:
        if timed_out and processed > 0:
            return "partial_success"
        if timed_out:
            return "timeout"
        if total == 0:
            return "skipped"
        return "success"

    def _partial_failure_summary(
        self,
        *,
        failure_bits: list[str],
        stage_results: list[RunStageResult],
        searched_items: int,
        new_leads: int,
    ) -> str:
        stage_bits = [
            f"{stage.stage_name}={stage.status}"
            for stage in stage_results
            if stage.status in {"partial_success", "timeout"}
        ]
        prefix = (
            f"Fetched some new data (searched_items={searched_items}, new_leads={new_leads}), "
            "but one or more stages only partially completed."
        )
        suffix_parts = []
        if stage_bits:
            suffix_parts.append("stages: " + ", ".join(stage_bits[:4]))
        if failure_bits:
            suffix_parts.append("sources: " + "; ".join(failure_bits[:3]))
        return prefix + (" " + " | ".join(suffix_parts) if suffix_parts else "")

    def _degraded_failure_summary(
        self,
        *,
        stage_results: list[RunStageResult],
        searched_items: int,
        new_leads: int,
        failure_bits: list[str],
    ) -> str:
        stage_failures = [
            f"{stage.stage_name}={stage.status}"
            for stage in stage_results
            if stage.status in {"failed", "timeout", "partial_success"}
        ]
        if searched_items > 0 or new_leads > 0:
            prefix = (
                f"Searching partially succeeded (searched_items={searched_items}, new_leads={new_leads}), "
                "but downstream stages did not fully complete."
            )
        else:
            prefix = "This run did not complete successfully."
        suffix_parts = []
        if stage_failures:
            suffix_parts.append("stages: " + ", ".join(stage_failures[:5]))
        if failure_bits:
            suffix_parts.append("sources: " + "; ".join(failure_bits[:3]))
        return prefix + (" " + " | ".join(suffix_parts) if suffix_parts else "")

    def _recommendation_blockers(
        self,
        *,
        recommended: int,
        searched_items: int,
        new_leads: int,
        dd_stats: dict[str, Any],
        score_stats: dict[str, Any],
        stage_results: list[RunStageResult],
        lead_status_by_verification: dict[str, int],
        blocker_counts: dict[str, int],
    ) -> list[str]:
        if recommended > 0:
            return []

        blockers: list[str] = []
        upstream_problem = any(
            stage.stage_name in {"searching", "entity_verification", "dd", "scoring"}
            and stage.status in {"failed", "timeout", "partial_success"}
            for stage in stage_results
        )
        if upstream_problem:
            blockers.append("Upstream stages timed out or failed, so the recommendation chain did not fully complete.")
        eligible_company_count = int(lead_status_by_verification.get("verified", 0) or 0)
        if bool(getattr(self.settings, "mvp_mode", False)):
            eligible_company_count += int(lead_status_by_verification.get("likely_company", 0) or 0)
        if eligible_company_count == 0 and searched_items > 0:
            blockers.append(
                "This run produced no verified company or likely_company candidate, so recommendation had no company-safe input."
                if bool(getattr(self.settings, "mvp_mode", False))
                else "This run produced no verified company, so there is no company-safe recommendation candidate."
            )
        if int(dd_stats.get("processed", 0) or 0) == 0 and int(blocker_counts.get("dd_ready_count", 0) or 0) == 0:
            blockers.append("DD did not complete or did not reach a score-ready state.")
        if int(score_stats.get("scored", 0) or 0) == 0:
            blockers.append("Scoring did not finish, so there are no new score results ready for recommendation.")
        elif int(blocker_counts.get("push_ready_count", 0) or 0) == 0:
            if int(blocker_counts.get("watchlist_count", 0) or 0) > 0:
                blockers.append("Some companies reached watchlist, but their score did not meet the proactive recommendation threshold.")
            if int(blocker_counts.get("hard_gate_blocked_count", 0) or 0) > 0:
                blockers.append("Some candidates existed, but hard gates were not met (for example source_hits, dd_status, or verified company requirements).")
        if not blockers and new_leads == 0:
            blockers.append("No promotable new leads were fetched in this run, so recommendation remained empty.")
        return blockers

    def _derive_scoring_skip_reasons(
        self,
        *,
        score_stats: dict[str, Any],
        dd_stats: dict[str, Any],
        lead_status_by_verification: dict[str, int],
        blocker_counts: dict[str, int],
    ) -> list[str]:
        if int(score_stats.get("scored", 0) or 0) > 0:
            return []

        reasons: list[str] = []
        verified_count = int(lead_status_by_verification.get("verified", 0) or 0)
        likely_count = int(lead_status_by_verification.get("likely_company", 0) or 0) if bool(getattr(self.settings, "mvp_mode", False)) else 0
        if (verified_count + likely_count) == 0:
            reasons.append("no_verified_company")

        dd_ready_count = int(blocker_counts.get("dd_ready_count", 0) or 0)
        waiting_human_count = int(blocker_counts.get("waiting_human_count", 0) or 0)
        if dd_ready_count == 0:
            reasons.append("no_dd_ready_leads")
        if waiting_human_count > 0 and dd_ready_count == 0:
            reasons.append("all_waiting_human")

        hard_gate_blocked_count = int(blocker_counts.get("hard_gate_blocked_count", 0) or 0)
        if hard_gate_blocked_count > 0 and dd_ready_count > 0:
            reasons.append("all_rejected_by_gate")

        # keep deterministic order while removing duplicates
        deduped: list[str] = []
        for item in reasons:
            if item not in deduped:
                deduped.append(item)
        return deduped
