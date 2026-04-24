from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.core.db import normalize_json_payload, normalize_text_content
from app.core.schemas import (
    ChatRequest,
    CommandRequest,
    FeedbackRequest,
    OpenClawInboundRequest,
    OpenClawOutboxAckRequest,
)

router = APIRouter()


def _runtime(request: Request):
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status_code=500, detail="Runtime is not initialized")
    return runtime


def _verify_openclaw_auth(request: Request) -> None:
    runtime = _runtime(request)
    secret = runtime.settings.openclaw_webhook_secret.strip()
    if not secret:
        return

    auth_header = request.headers.get("authorization", "").strip()
    token = ""
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    else:
        token = request.headers.get("x-openclaw-token", "").strip()

    if token != secret:
        raise HTTPException(status_code=401, detail="Invalid OpenClaw token")


def _extract_openclaw_text(payload: OpenClawInboundRequest) -> str:
    candidates: list[Any] = [
        payload.content,
        payload.message,
        payload.text,
    ]

    context = payload.context or {}
    if isinstance(context, dict):
        candidates.extend(
            [
                context.get("bodyForAgent"),
                context.get("content"),
                context.get("text"),
                context.get("message"),
            ]
        )

    extras = payload.model_extra or {}
    if isinstance(extras, dict):
        candidates.extend(
            [
                extras.get("bodyForAgent"),
                extras.get("content"),
                extras.get("text"),
                extras.get("message"),
            ]
        )

    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return normalize_text_content(candidate).strip()

    return ""


def _extract_openclaw_metadata(payload: OpenClawInboundRequest) -> dict[str, Any]:
    context = payload.context if isinstance(payload.context, dict) else {}
    extras = payload.model_extra if isinstance(payload.model_extra, dict) else {}
    metadata: dict[str, Any] = {}

    if isinstance(payload.metadata, dict):
        metadata.update(payload.metadata)
    if isinstance(context.get("metadata"), dict):
        metadata.update(context["metadata"])
    if isinstance(extras.get("metadata"), dict):
        metadata.update(extras["metadata"])

    return normalize_json_payload(metadata)


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/interaction/chat")
def interaction_chat(payload: ChatRequest, request: Request) -> dict:
    runtime = _runtime(request)
    message = normalize_text_content(payload.message)
    try:
        result = runtime.handle_human_message(message, source="direct")
    except Exception as exc:
        failure_summary = f"interaction_chat raised {exc.__class__.__name__}: {normalize_text_content(str(exc)) or exc.__class__.__name__}"
        return normalize_json_payload(
            {
                "ok": False,
                "reply": f"这轮没有正常完成。原因：{failure_summary}",
                "action": "interaction_chat_error",
                "data": {
                    "run_status": "internal_error",
                    "new_data_fetched": False,
                    "used_existing_pool_only": False,
                    "failure_summary": failure_summary,
                    "unavailable_sources": [],
                    "source_status_by_channel": {},
                    "action_suggestions": [
                        "Inspect server logs and rerun after fixing the runtime exception."
                    ],
                },
            }
        )
    return normalize_json_payload({"ok": bool(result.get("ok", True)), **result})


@router.post("/openclaw/inbox")
def openclaw_inbox(payload: OpenClawInboundRequest, request: Request) -> dict:
    _verify_openclaw_auth(request)
    runtime = _runtime(request)

    text = _extract_openclaw_text(payload)
    if not text:
        raise HTTPException(status_code=400, detail="OpenClaw payload does not contain text")

    session_key = payload.session_key or ""
    channel_id = payload.channel_id or ""
    sender = payload.sender or payload.from_ or ""
    metadata = _extract_openclaw_metadata(payload)

    result = runtime.handle_human_message(
        text,
        source="openclaw",
        session_key=session_key or None,
        channel_id=channel_id or None,
        sender=sender or None,
        metadata=metadata,
    )

    return normalize_json_payload({
        "ok": True,
        "source": "openclaw",
        "session_key": session_key,
        "channel_id": channel_id,
        "sender": sender,
        **result,
    })


@router.get("/openclaw/outbox")
def openclaw_outbox(request: Request, limit: int = 50) -> dict:
    _verify_openclaw_auth(request)
    runtime = _runtime(request)
    rows = runtime.list_pending_outbox(limit=limit)
    items = []
    for row in rows:
        payload = {}
        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            payload = {"raw": row["payload"]}
        items.append(
            {
                "id": row["id"],
                "event_type": row["event_type"],
                "payload": payload,
                "created_at": row["created_at"],
                "sent": bool(row["sent"]),
            }
        )
    return normalize_json_payload({"ok": True, "items": items})


@router.post("/openclaw/outbox/ack")
def openclaw_outbox_ack(payload: OpenClawOutboxAckRequest, request: Request) -> dict:
    _verify_openclaw_auth(request)
    runtime = _runtime(request)
    count = runtime.ack_outbox_events(payload.event_ids)
    return normalize_json_payload({"ok": True, "updated": count})


@router.get("/openclaw/messages")
def openclaw_messages(request: Request, limit: int = 100) -> dict:
    _verify_openclaw_auth(request)
    runtime = _runtime(request)
    rows = runtime.list_conversation_messages(limit=limit)
    items = []
    for row in rows:
        payload = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            payload = {"raw": row["payload_json"]}
        items.append(
            {
                "id": row["id"],
                "direction": row["direction"],
                "source": row["source"],
                "session_key": row["session_key"],
                "channel_id": row["channel_id"],
                "sender": row["sender"],
                "content": row["content"],
                "action": row["action"],
                "payload": payload,
                "created_at": row["created_at"],
            }
        )
    return normalize_json_payload({"ok": True, "items": items})


@router.post("/interaction/command")
def interaction_command(payload: CommandRequest, request: Request) -> dict:
    runtime = _runtime(request)

    if payload.command == "run_cycle":
        try:
            result = runtime.run_full_cycle()
            result["run_at"] = result["run_at"].isoformat()
            return normalize_json_payload({"ok": True, "result": result})
        except Exception as exc:
            failure_summary = f"run_cycle raised {exc.__class__.__name__}: {normalize_text_content(str(exc)) or exc.__class__.__name__}"
            return normalize_json_payload(
                {
                    "ok": False,
                    "result": {
                        "run_at": "",
                        "searched_items": 0,
                        "new_leads": 0,
                        "dd_done": 0,
                        "dd_waiting_human": 0,
                        "dd_questions": 0,
                        "dd_questions_published": 0,
                        "scored": 0,
                        "recommended": 0,
                        "run_status": "internal_error",
                        "new_data_fetched": False,
                        "used_existing_pool_only": False,
                        "failure_summary": failure_summary,
                        "unavailable_sources": [],
                        "source_status_by_channel": {},
                        "action_suggestions": [
                            "Inspect server logs and rerun after fixing the runtime exception."
                        ],
                    },
                }
            )

    if payload.command == "refresh_strategy":
        result = runtime.refresh_strategy()
        return normalize_json_payload({"ok": True, "result": result})

    if payload.command == "update_channel":
        channel = str(payload.data.get("channel", "")).strip().lower()
        if channel not in {"brave", "bocha"}:
            raise HTTPException(status_code=400, detail="channel must be brave or bocha")
        enabled = bool(payload.data.get("enabled", True))
        result = runtime.update_channel(channel=channel, enabled=enabled)
        return normalize_json_payload({"ok": True, "result": result})

    if payload.command == "compress_memory":
        result = runtime.compress_memory()
        return normalize_json_payload({"ok": True, "result": result})

    raise HTTPException(status_code=400, detail=f"Unknown command: {payload.command}")


@router.post("/interaction/feedback")
def interaction_feedback(payload: FeedbackRequest, request: Request) -> dict:
    runtime = _runtime(request)
    runtime.interaction.receive_feedback(
        verdict=normalize_text_content(payload.verdict),
        content=normalize_text_content(payload.content),
        feedback_type=normalize_text_content(payload.feedback_type),
        lead_id=payload.lead_id,
    )
    return normalize_json_payload({"ok": True})


@router.get("/interaction/recommendations")
def interaction_recommendations(request: Request) -> dict:
    runtime = _runtime(request)
    threshold = runtime.settings.recommend_score_threshold
    items = runtime.interaction.list_recommendations(threshold=threshold)
    return normalize_json_payload({"ok": True, "threshold": threshold, "items": items})


@router.get("/interaction/leads")
def interaction_leads(request: Request) -> dict:
    runtime = _runtime(request)
    rows = runtime.db.list_leads(limit=100)
    items = []
    for row in rows:
        items.append(
            {
                "id": row["id"],
                "display_name": row.get("display_name") or row["company_name"],
                "company_name": row["company_name"],
                "normalized_name": row.get("normalized_name") or "",
                "raw_title": row.get("raw_title") or "",
                "status": row["status"],
                "stage": row["stage"],
                "sources": row["sources"],
                "description": row["description"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
            }
        )
    return normalize_json_payload({"ok": True, "items": items})
