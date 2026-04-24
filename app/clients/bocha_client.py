from __future__ import annotations

from typing import Any

import requests

from app.core.config import Settings
from app.models.source_execution_result import SourceExecutionResult
from app.services.network_diagnostics import NetworkDiagnostics


class BochaSearchClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.diagnostics = NetworkDiagnostics()

    def search(self, query: str, limit: int = 10) -> list[dict[str, str]]:
        return self.execute_search(query, limit).items

    def execute_search(self, query: str, limit: int = 10) -> SourceExecutionResult:
        if self.settings.demo_mode:
            return SourceExecutionResult(
                source_name="bocha",
                status="skipped",
                request_attempted=False,
                request_succeeded=False,
                items_received=0,
                failure_stage="client",
                failure_code="demo_mode",
                provider_message="demo_mode is enabled; live Bocha requests are skipped.",
                retryable=False,
                action_hint="Disable demo_mode to use live Bocha search.",
                metadata={"query": query, "limit": limit},
            )

        if not self.settings.bocha_api_key:
            return SourceExecutionResult(
                source_name="bocha",
                status="failed",
                request_attempted=False,
                request_succeeded=False,
                items_received=0,
                failure_stage="client",
                failure_code="unauthorized",
                provider_message="Bocha API key is not configured.",
                retryable=False,
                action_hint="Set BOCHA_API_KEY before rerunning.",
                metadata={"query": query, "limit": limit},
            )

        headers = {
            "Content-Type": "application/json",
        }
        if self.settings.bocha_api_key:
            headers["Authorization"] = f"Bearer {self.settings.bocha_api_key}"

        payload = {
            "query": query,
            "freshness": "oneYear",
            "summary": True,
            "count": limit,
        }

        try:
            response = requests.post(
                self.settings.bocha_search_url,
                headers=headers,
                json=payload,
                timeout=15,
            )
            if response.status_code >= 400:
                failure = self.diagnostics.classify_http_failure(
                    source_name="bocha",
                    status_code=response.status_code,
                    body=response.text,
                    headers=dict(response.headers),
                )
                return SourceExecutionResult(
                    source_name="bocha",
                    status="failed",
                    request_attempted=True,
                    request_succeeded=False,
                    items_received=0,
                    metadata={"query": query, "limit": limit},
                    **failure,
                )

            try:
                data = response.json()
            except Exception as exc:
                return SourceExecutionResult(
                    source_name="bocha",
                    status="failed",
                    request_attempted=True,
                    request_succeeded=False,
                    items_received=0,
                    failure_stage="parse",
                    failure_code="parse_error",
                    http_status=response.status_code,
                    provider_message=str(exc),
                    retryable=False,
                    action_hint="Inspect the Bocha response body and parser compatibility.",
                    metadata={"query": query, "limit": limit, "response_text": response.text[:500]},
                )

            parsed = self._parse_mixed_response(data)
            return SourceExecutionResult(
                source_name="bocha",
                status="success",
                request_attempted=True,
                request_succeeded=True,
                items_received=len(parsed),
                items=parsed,
                metadata={"query": query, "limit": limit},
            )
        except Exception as exc:
            failure = self.diagnostics.classify_exception(exc, source_name="bocha")
            return SourceExecutionResult(
                source_name="bocha",
                status="failed",
                request_attempted=True,
                request_succeeded=False,
                items_received=0,
                metadata={"query": query, "limit": limit},
                **failure,
            )

    def _parse_mixed_response(self, data: dict[str, Any]) -> list[dict[str, str]]:
        candidates = self._resolve_candidates(data)

        parsed: list[dict[str, str]] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("name") or "").strip()
            snippet = str(item.get("snippet") or item.get("summary") or item.get("description") or "").strip()
            url = str(item.get("url") or item.get("link") or item.get("source") or "").strip()
            if not title or not url:
                continue
            parsed.append({"title": title, "snippet": snippet, "url": url})
        return parsed

    @staticmethod
    def _resolve_candidates(data: dict[str, Any]) -> list[dict[str, Any]]:
        direct_candidates = (
            data.get("results")
            or data.get("items")
            or data.get("webPages", {}).get("value")
        )
        if isinstance(direct_candidates, list):
            return direct_candidates

        nested_data = data.get("data")
        if isinstance(nested_data, dict):
            nested_candidates = (
                nested_data.get("results")
                or nested_data.get("items")
                or nested_data.get("webPages", {}).get("value")
            )
            if isinstance(nested_candidates, list):
                return nested_candidates

        return []
