from __future__ import annotations

import time
from typing import Any

from app.clients.bocha_client import BochaSearchClient
from app.clients.brave_client import BraveSearchClient
from app.clients.llm_client import LLMClient
from app.core.db import Database
from app.services.dd_memory import DDMemoryStore
from app.services.dd_enricher import DDEnricher


class DDAgent:
    def __init__(
        self,
        db: Database,
        brave_client: BraveSearchClient,
        bocha_client: BochaSearchClient,
        llm: LLMClient,
        *,
        enricher: DDEnricher | None = None,
        dd_memory: DDMemoryStore | None = None,
    ):
        self.db = db
        self.brave_client = brave_client
        self.bocha_client = bocha_client
        self.llm = llm
        self.dd_memory = dd_memory or DDMemoryStore(db)
        self.enricher = enricher or DDEnricher(
            brave_client,
            bocha_client,
            memory=self.dd_memory,
            mvp_mode=bool(getattr(getattr(brave_client, "settings", None), "mvp_mode", False)),
        )
        if hasattr(self.enricher, "memory") and getattr(self.enricher, "memory", None) is None:
            setattr(self.enricher, "memory", self.dd_memory)

    def run_for_lead(self, lead: dict[str, Any]) -> dict[str, int | str]:
        lead_dict = dict(lead) if not isinstance(lead, dict) else lead
        report = self.enricher.enrich(lead_dict)
        self.db.upsert_dd_report(report=report)
        for question in getattr(report, "questions", []) or []:
            self.dd_memory.record_question(question)

        return {
            "processed": 1,
            "dd_done": int(report.dd_status == "dd_done"),
            "dd_partial": int(report.dd_status == "dd_partial"),
            "dd_pending_review": int(report.dd_status == "dd_pending_review"),
            "dd_waiting_human": int(report.dd_status == "dd_waiting_human"),
            "questions_generated": len(getattr(report, "questions", []) or []),
            "dd_status": report.dd_status,
        }

    def run(self, limit: int = 30, *, deadline_ts: float | None = None) -> dict[str, int | bool]:
        leads = self.db.get_leads_without_dd(limit=limit)
        processed = 0
        done = 0
        partial = 0
        pending = 0
        waiting_human = 0
        questions_generated = 0
        timed_out = False

        for lead in leads:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                timed_out = True
                break
            stats = self.run_for_lead(dict(lead) if not isinstance(lead, dict) else lead)
            processed += int(stats["processed"])
            done += int(stats["dd_done"])
            partial += int(stats["dd_partial"])
            pending += int(stats["dd_pending_review"])
            waiting_human += int(stats.get("dd_waiting_human") or 0)
            questions_generated += int(stats.get("questions_generated") or 0)

        return {
            "processed": processed,
            "dd_done": done,
            "dd_partial": partial,
            "dd_pending_review": pending,
            "dd_waiting_human": waiting_human,
            "questions_generated": questions_generated,
            "input_count": len(leads),
            "remaining_count": max(0, len(leads) - processed),
            "timed_out": timed_out,
        }
