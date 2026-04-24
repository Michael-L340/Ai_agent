from __future__ import annotations

from typing import Any

import requests

from app.core.config import Settings
from app.models.source_execution_result import SourceExecutionResult
from app.services.network_diagnostics import NetworkDiagnostics


class BraveSearchClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.diagnostics = NetworkDiagnostics()

    def search(self, query: str, limit: int = 10) -> list[dict[str, str]]:
        return self.execute_search(query, limit).items

    def execute_search(self, query: str, limit: int = 10) -> SourceExecutionResult:
        if self.settings.demo_mode:
            return SourceExecutionResult(
                source_name="brave",
                status="skipped",
                request_attempted=False,
                request_succeeded=False,
                items_received=0,
                failure_stage="client",
                failure_code="demo_mode",
                provider_message="demo_mode is enabled; live Brave requests are skipped.",
                retryable=False,
                action_hint="Disable demo_mode to use live Brave search.",
                metadata={"query": query, "limit": limit},
            )

        if not self.settings.brave_api_key:
            return SourceExecutionResult(
                source_name="brave",
                status="failed",
                request_attempted=False,
                request_succeeded=False,
                items_received=0,
                failure_stage="client",
                failure_code="unauthorized",
                provider_message="Brave API key is not configured.",
                retryable=False,
                action_hint="Set BRAVE_API_KEY before rerunning.",
                metadata={"query": query, "limit": limit},
            )

        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.settings.brave_api_key,
        }
        params = {
            "q": query,
            "count": limit,
            "country": "us",
            "search_lang": "en",
            "safesearch": "moderate",
            "spellcheck": 1,
        }

        try:
            response = requests.get(
                self.settings.brave_search_url,
                headers=headers,
                params=params,
                timeout=15,
            )
            if response.status_code >= 400:
                failure = self.diagnostics.classify_http_failure(
                    source_name="brave",
                    status_code=response.status_code,
                    body=response.text,
                    headers=dict(response.headers),
                )
                return SourceExecutionResult(
                    source_name="brave",
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
                    source_name="brave",
                    status="failed",
                    request_attempted=True,
                    request_succeeded=False,
                    items_received=0,
                    failure_stage="parse",
                    failure_code="parse_error",
                    http_status=response.status_code,
                    provider_message=str(exc),
                    retryable=False,
                    action_hint="Inspect the Brave response body and parser compatibility.",
                    metadata={"query": query, "limit": limit, "response_text": response.text[:500]},
                )

            results = data.get("web", {}).get("results", []) or data.get("results", [])
            parsed: list[dict[str, str]] = []
            for item in results:
                parsed.append(
                    {
                        "title": str(item.get("title", "")).strip(),
                        "snippet": str(
                            item.get("description", "") or item.get("snippet", "")
                        ).strip(),
                        "url": str(item.get("url", "")).strip(),
                    }
                )
            return SourceExecutionResult(
                source_name="brave",
                status="success",
                request_attempted=True,
                request_succeeded=True,
                items_received=len(parsed),
                items=parsed,
                metadata={"query": query, "limit": limit},
            )
        except Exception as exc:
            failure = self.diagnostics.classify_exception(exc, source_name="brave")
            return SourceExecutionResult(
                source_name="brave",
                status="failed",
                request_attempted=True,
                request_succeeded=False,
                items_received=0,
                metadata={"query": query, "limit": limit},
                **failure,
            )


def normalize_search_items(source: str, query: str, raw_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in raw_items:
        title = str(item.get("title", "")).strip()
        snippet = str(item.get("snippet", "")).strip()
        url = str(item.get("url", "")).strip()
        if not title or not url:
            continue
        normalized.append(
            {
                "source": source,
                "query": query,
                "title": title,
                "snippet": snippet,
                "url": url,
            }
        )
    return normalized

