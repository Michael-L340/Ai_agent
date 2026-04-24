from __future__ import annotations

import time
from typing import Callable

from app.clients.brave_client import BraveSearchClient, normalize_search_items
from app.clients.bocha_client import BochaSearchClient
from app.clients.llm_client import LLMClient
from app.core.db import Database
from app.models.source_execution_result import SourceExecutionResult
from app.services.entity_verifier import EntityVerifier


class BaseSearchingAgent:
    BLOCKED_SINGLE_TOKEN_COMPANIES = {
        "closing",
        "firewall",
        "genai",
        "light",
        "mcp",
    }

    def __init__(
        self,
        source_name: str,
        search_func: Callable[[str, int], SourceExecutionResult],
        db: Database,
        llm: LLMClient,
    ):
        self.source_name = source_name
        self.search_func = search_func
        self.db = db
        self.llm = llm
        self.entity_verifier = EntityVerifier(llm.settings, llm)

    def fetch(
        self,
        queries: list[str],
        *,
        per_query_limit: int = 8,
        deadline_ts: float | None = None,
    ) -> dict[str, object]:
        searched_items = 0
        execution_results: list[SourceExecutionResult] = []
        normalized_items: list[dict[str, str]] = []
        timed_out = False

        for query in queries:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                timed_out = True
                break

            execution = self.search_func(query, per_query_limit)
            execution_results.append(execution)
            normalized = normalize_search_items(self.source_name, query, execution.items)
            searched_items += len(normalized)
            normalized_items.extend(normalized)

        return {
            "searched_items": searched_items,
            "items": normalized_items,
            "timed_out": timed_out,
            "queries_total": len(queries),
            "queries_attempted": len(execution_results),
            "source_result": self._aggregate_execution_results(
                execution_results,
                timed_out=timed_out,
                total_queries=len(queries),
            ),
        }

    def verify_and_store(
        self,
        *,
        items: list[dict[str, str]],
        negative_filters: list[str],
        deadline_ts: float | None = None,
        item_limit: int | None = None,
    ) -> dict[str, int | bool]:
        matched_items = 0
        verified_items = 0
        likely_company_items = 0
        pending_review_items = 0
        rejected_items = 0
        new_leads = 0
        processed_items = 0
        timed_out = False
        capped = False

        negative_terms = {term.strip().lower() for term in negative_filters if term.strip()}

        for item in items:
            if item_limit is not None and processed_items >= item_limit:
                capped = True
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                timed_out = True
                break

            processed_items += 1
            title = item["title"]
            snippet = item["snippet"]
            url = item["url"]
            query = item["query"]
            cls = self.llm.classify_relevance(title=title, snippet=snippet)

            resolution = self.entity_verifier.resolve(
                raw_title=title,
                snippet=snippet,
                url=url,
                source=self.source_name,
                query=query,
            )

            resolution = self._apply_minimum_entity_guards(
                resolution=resolution,
                negative_terms=negative_terms,
            )

            if resolution.is_verified:
                verified_items += 1
            elif resolution.is_likely_company:
                likely_company_items += 1
            elif resolution.is_pending_review:
                pending_review_items += 1
            else:
                rejected_items += 1

            if resolution.is_rejected or resolution.entity_type != "company":
                self.db.add_signal(
                    lead_id=None,
                    source=self.source_name,
                    query=query,
                    title=title,
                    snippet=snippet,
                    url=url,
                    raw={"entity_resolution": resolution.to_dict(), "relevant": False},
                )
                continue

            matched_items += 1
            thesis_tags = ",".join(cls.get("tags", []))

            lead_id, created = self.db.upsert_lead(
                company_name=resolution.normalized_name or resolution.candidate_name or title,
                source=self.source_name,
                description=snippet,
                thesis_tags=thesis_tags,
                resolution=resolution.to_dict(),
            )
            if created:
                new_leads += 1

            self.db.add_signal(
                lead_id=lead_id,
                source=self.source_name,
                query=query,
                title=title,
                snippet=snippet,
                url=url,
                raw={"entity_resolution": resolution.to_dict(), "relevant": True},
            )

        return {
            "matched_items": matched_items,
            "verified_items": verified_items,
            "likely_company_items": likely_company_items,
            "pending_review_items": pending_review_items,
            "rejected_items": rejected_items,
            "new_leads": new_leads,
            "processed_items": processed_items,
            "timed_out": timed_out,
            "capped": capped,
            "remaining_items": max(0, len(items) - processed_items),
        }

    def run(self, queries: list[str], negative_filters: list[str]) -> dict[str, int]:
        fetch_result = self.fetch(queries)
        verify_result = self.verify_and_store(
            items=list(fetch_result["items"]),
            negative_filters=negative_filters,
        )

        return {
            "searched_items": int(fetch_result["searched_items"]),
            "matched_items": int(verify_result["matched_items"]),
            "verified_items": int(verify_result["verified_items"]),
            "likely_company_items": int(verify_result.get("likely_company_items", 0)),
            "pending_review_items": int(verify_result["pending_review_items"]),
            "rejected_items": int(verify_result["rejected_items"]),
            "new_leads": int(verify_result["new_leads"]),
            "source_result": fetch_result["source_result"],
        }

    def _aggregate_execution_results(
        self,
        results: list[SourceExecutionResult],
        *,
        timed_out: bool = False,
        total_queries: int = 0,
    ) -> dict[str, str | int | bool | None]:
        if not results:
            return SourceExecutionResult(
                source_name=self.source_name,
                status="timeout" if timed_out else "skipped",
                request_attempted=False,
                request_succeeded=False,
                items_received=0,
                failure_stage="stage_timeout" if timed_out else "planner",
                failure_code="connect_timeout" if timed_out else "unknown_error",
                provider_message=(
                    f"{self.source_name} search stage timed out before any query completed."
                    if timed_out
                    else "No search queries were issued for this source."
                ),
                retryable=timed_out,
                action_hint=(
                    f"Reduce query volume or rerun {self.source_name} after reviewing stage timeout settings."
                    if timed_out
                    else f"Check planner output for {self.source_name}."
                ),
                metadata={"queries_total": total_queries},
            ).to_dict()

        total_items = sum(int(item.items_received) for item in results)
        attempted = any(item.request_attempted for item in results)
        succeeded = any(item.request_succeeded for item in results)
        successes = [item for item in results if item.request_succeeded]
        failures = [item for item in results if item.failure_code]

        status = "success"
        if timed_out and successes:
            status = "partial_success"
        elif timed_out:
            status = "timeout"
        elif successes and failures:
            status = "partial_success"
        elif successes:
            status = "success"
        elif attempted:
            status = "failed"
        else:
            status = "skipped"

        primary_failure = failures[0] if failures else None
        return SourceExecutionResult(
            source_name=self.source_name,
            status=status,
            request_attempted=attempted,
            request_succeeded=succeeded,
            items_received=total_items,
            failure_stage=primary_failure.failure_stage if primary_failure else "",
            failure_code=primary_failure.failure_code if primary_failure else "",
            http_status=primary_failure.http_status if primary_failure else None,
            retry_after_sec=primary_failure.retry_after_sec if primary_failure else None,
            provider_message=primary_failure.provider_message if primary_failure else "",
            retryable=(
                True
                if timed_out and not primary_failure
                else (primary_failure.retryable if primary_failure else False)
            ),
            action_hint=(
                f"{self.source_name} search hit the per-stage timeout before completing all queries."
                if timed_out and not primary_failure
                else (primary_failure.action_hint if primary_failure else "")
            ),
            metadata={
                "queries_attempted": len(results),
                "queries_total": total_queries,
                "successful_requests": len(successes),
                "failed_requests": len(failures),
                "timed_out": timed_out,
            },
        ).to_dict()

    def _apply_minimum_entity_guards(
        self,
        *,
        resolution,
        negative_terms: set[str],
    ):
        if any(
            term in resolution.normalized_name.lower()
            or term in resolution.candidate_name.lower()
            or term in resolution.raw_title.lower()
            for term in negative_terms
        ):
            resolution.reject_reason = "blocked by planner negative filter"
            resolution.verification_status = "rejected"
            resolution.verification_score = min(resolution.verification_score, 25.0)

        name = (resolution.normalized_name or resolution.candidate_name or "").strip().lower()
        if name in self.BLOCKED_SINGLE_TOKEN_COMPANIES:
            resolution.entity_type = "content"
            resolution.verification_status = "rejected"
            resolution.verification_score = min(resolution.verification_score, 10.0)
            resolution.reject_reason = "generic single-token candidate blocked"

        return resolution


class BraveSearchingAgent(BaseSearchingAgent):
    def __init__(
        self,
        brave_client: BraveSearchClient,
        db: Database,
        llm: LLMClient,
    ):
        super().__init__("brave", brave_client.execute_search, db, llm)


class BochaSearchingAgent(BaseSearchingAgent):
    def __init__(
        self,
        bocha_client: BochaSearchClient,
        db: Database,
        llm: LLMClient,
    ):
        super().__init__("bocha", bocha_client.execute_search, db, llm)
