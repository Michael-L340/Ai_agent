from __future__ import annotations

import re
from typing import Any

import requests


class NetworkDiagnostics:
    RATE_LIMIT_CODES = {429}
    QUOTA_EXHAUSTED_CODES = {402}

    def classify_http_failure(
        self,
        *,
        source_name: str,
        status_code: int,
        body: str = "",
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = str(body or "")
        lowered = text.lower()
        header_map = {str(k).lower(): v for k, v in (headers or {}).items()}
        retry_after = self._parse_retry_after(header_map.get("retry-after"))

        if status_code in self.RATE_LIMIT_CODES:
            return self._result(
                failure_code="rate_limited",
                failure_stage="http_response",
                http_status=status_code,
                retry_after_sec=retry_after,
                provider_message=text[:500],
                retryable=True,
                action_hint="Wait for the provider retry window, then rerun this source.",
            )

        if status_code in self.QUOTA_EXHAUSTED_CODES or "usage limit exceeded" in lowered or "quota" in lowered:
            return self._result(
                failure_code="quota_exhausted",
                failure_stage="http_response",
                http_status=status_code,
                provider_message=text[:500],
                retryable=False,
                action_hint=f"Increase or reset the {source_name} quota before rerunning.",
            )

        if status_code == 401:
            return self._result(
                failure_code="unauthorized",
                failure_stage="http_response",
                http_status=status_code,
                provider_message=text[:500],
                retryable=False,
                action_hint=f"Check the {source_name} API key or token.",
            )

        if status_code == 403:
            failure_code = "quota_exhausted" if any(
                phrase in lowered for phrase in ("quota", "not enough money", "insufficient", "package quota")
            ) else "forbidden"
            action_hint = (
                f"Top up or upgrade the {source_name} package before rerunning."
                if failure_code == "quota_exhausted"
                else f"Check whether the {source_name} account is allowed to use this endpoint."
            )
            return self._result(
                failure_code=failure_code,
                failure_stage="http_response",
                http_status=status_code,
                provider_message=text[:500],
                retryable=False,
                action_hint=action_hint,
            )

        if 500 <= status_code <= 599:
            return self._result(
                failure_code="upstream_5xx",
                failure_stage="http_response",
                http_status=status_code,
                provider_message=text[:500],
                retryable=True,
                action_hint=f"Retry {source_name} later; the provider returned a server error.",
            )

        if 400 <= status_code <= 499:
            return self._result(
                failure_code="bad_response",
                failure_stage="http_response",
                http_status=status_code,
                provider_message=text[:500],
                retryable=False,
                action_hint=f"Inspect the {source_name} request payload and credentials.",
            )

        return self._result(
            failure_code="unknown_error",
            failure_stage="http_response",
            http_status=status_code,
            provider_message=text[:500],
            retryable=False,
            action_hint=f"Inspect the {source_name} response body for more details.",
        )

    def classify_exception(self, exc: Exception, *, source_name: str) -> dict[str, Any]:
        message = str(exc)
        lowered = message.lower()

        if isinstance(exc, requests.exceptions.ConnectTimeout):
            return self._result(
                failure_code="connect_timeout",
                failure_stage="connect",
                provider_message=message,
                retryable=True,
                action_hint=f"Retry {source_name}; the upstream connection timed out.",
            )
        if isinstance(exc, requests.exceptions.ReadTimeout):
            return self._result(
                failure_code="read_timeout",
                failure_stage="read",
                provider_message=message,
                retryable=True,
                action_hint=f"Retry {source_name}; the upstream read timed out.",
            )
        if isinstance(exc, requests.exceptions.SSLError):
            return self._result(
                failure_code="ssl_error",
                failure_stage="tls",
                provider_message=message,
                retryable=False,
                action_hint=f"Inspect TLS / certificate handling for {source_name}.",
            )
        if isinstance(exc, requests.exceptions.ConnectionError):
            if "10013" in lowered or "access permissions" in lowered or "访问权限" in message:
                return self._result(
                    failure_code="network_blocked",
                    failure_stage="connect",
                    provider_message=message,
                    retryable=False,
                    action_hint=f"The current Python process cannot open outbound sockets to {source_name}; use the allowed runtime or adjust firewall/security policy.",
                )
            if "name or service not known" in lowered or "nodename nor servname provided" in lowered or "getaddrinfo failed" in lowered:
                return self._result(
                    failure_code="dns_error",
                    failure_stage="dns",
                    provider_message=message,
                    retryable=True,
                    action_hint=f"Check DNS resolution for the {source_name} host.",
                )
            if "connection refused" in lowered:
                return self._result(
                    failure_code="connection_refused",
                    failure_stage="connect",
                    provider_message=message,
                    retryable=True,
                    action_hint=f"Check whether the {source_name} endpoint is reachable and accepting connections.",
                )
            return self._result(
                failure_code="network_blocked",
                failure_stage="connect",
                provider_message=message,
                retryable=True,
                action_hint=f"Check network policy, firewall, or upstream reachability for {source_name}.",
            )

        return self._result(
            failure_code="unknown_error",
            failure_stage="client",
            provider_message=message,
            retryable=False,
            action_hint=f"Inspect the {source_name} client exception details.",
        )

    @staticmethod
    def _parse_retry_after(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def summarize_source_failure(source_name: str, result: dict[str, Any]) -> str:
        failure_code = result.get("failure_code") or "unknown_error"
        http_status = result.get("http_status")
        provider_message = str(result.get("provider_message") or "").strip()
        if http_status:
            return f"{source_name} failed with {failure_code} (HTTP {http_status})"
        if provider_message:
            first_sentence = re.split(r"[\\r\\n]+", provider_message)[0][:120]
            return f"{source_name} failed with {failure_code}: {first_sentence}"
        return f"{source_name} failed with {failure_code}"

    @staticmethod
    def _result(**kwargs: Any) -> dict[str, Any]:
        return {
            "failure_code": kwargs.get("failure_code", "unknown_error"),
            "failure_stage": kwargs.get("failure_stage", ""),
            "http_status": kwargs.get("http_status"),
            "retry_after_sec": kwargs.get("retry_after_sec"),
            "provider_message": kwargs.get("provider_message", ""),
            "retryable": bool(kwargs.get("retryable", False)),
            "action_hint": kwargs.get("action_hint", ""),
        }
