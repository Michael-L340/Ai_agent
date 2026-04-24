from __future__ import annotations

from typing import Any

import requests

from app.core.config import Settings
from app.core.db import Database, normalize_text_content
from app.services.company_name_memory import CompanyNameFeedbackStore
from app.services.dd_memory import DDMemoryStore


class InteractionAgent:
    """
    交互 Agent 负责两件事：
    1) 接收外部指令/反馈并写入系统。
    2) 面向人类整理输出，必要时把 DD + 评分 + 推荐理由一起汇总。
    """

    def __init__(self, db: Database, settings: Settings, *, dd_memory: DDMemoryStore | None = None):
        self.db = db
        self.settings = settings
        self.entity_name_feedback = CompanyNameFeedbackStore()
        self.dd_memory = dd_memory or DDMemoryStore(db)

    def receive_feedback(
        self,
        verdict: str,
        content: str,
        feedback_type: str = "lead_feedback",
        lead_id: int | None = None,
    ) -> None:
        verdict = normalize_text_content(verdict).strip()
        content = normalize_text_content(content)
        feedback_type = normalize_text_content(feedback_type).strip() or "lead_feedback"
        feedback_id = self.db.add_feedback(
            verdict=verdict,
            feedback_type=feedback_type,
            content=content,
            lead_id=lead_id,
        )
        feedback_kind = str(verdict or "").strip().lower()
        if feedback_kind in {"like", "dislike", "skip", "wrong_entity", "prefer_sector"}:
            lead = self.db.get_lead_by_id(lead_id) if lead_id else None
            policy_feedback = {
                "feedback_id": feedback_id,
                "lead_id": lead_id,
                "verdict": feedback_kind,
                "feedback_type": feedback_type or "scoring_feedback",
                "content": content,
                "scope": "lead" if lead_id else "global",
                "scope_key": f"lead:{lead_id}" if lead_id else "global",
            }
            if lead:
                policy_feedback.update(
                    {
                        "company_name": lead.get("company_name") or "",
                        "normalized_name": lead.get("normalized_name") or lead.get("candidate_name") or lead.get("company_name") or "",
                        "official_domain": lead.get("official_domain") or "",
                        "company_key": lead.get("company_key") or "",
                    }
                )
            try:
                self.db.update_scoring_policy_from_feedback(policy_feedback)
            except Exception:
                pass

    def record_dd_feedback(
        self,
        *,
        raw_text: str,
        scope: str = "global",
        lead_id: int | None = None,
        company_name: str = "",
        normalized_name: str = "",
        official_domain: str = "",
        dimension: str = "entity",
        feedback_kind: str = "note",
        source_question_id: int | None = None,
    ) -> dict[str, Any]:
        raw_text = normalize_text_content(raw_text)
        company_name = normalize_text_content(company_name)
        normalized_name = normalize_text_content(normalized_name)
        official_domain = normalize_text_content(official_domain)
        dimension = normalize_text_content(dimension) or "entity"
        feedback_kind = normalize_text_content(feedback_kind) or "note"
        parsed = self.dd_memory.parse_feedback_text(raw_text)
        result = self.dd_memory.record_feedback(
            scope=scope,
            content=raw_text,
            lead_id=lead_id,
            company_name=company_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            dimension=dimension,
            feedback_kind=feedback_kind,
            source_question_id=source_question_id,
            parsed=parsed,
        )
        scope_text = {
            "lead": f"lead {lead_id}" if lead_id else "当前 lead",
            "company": company_name or normalized_name or "该公司",
            "global": "全局规则",
        }.get(scope, "全局规则")
        reply = f"已记住这条 DD 反馈（{scope_text}，{dimension}）。后续 DD enrich 会优先参考这条记忆。"
        return {
            "ok": True,
            "reply": reply,
            "action": "record_dd_feedback",
            "data": result,
        }

    def answer_dd_question(self, question_id: int, answer_text: str) -> dict[str, Any]:
        answer_text = normalize_text_content(answer_text)
        result = self.dd_memory.answer_question(question_id=question_id, answer_text=answer_text)
        if not result.get("ok"):
            return {
                "ok": False,
                "reply": f"我没找到问题 {question_id}，请先查看待确认问题。",
                "action": "answer_dd_question",
                "data": result,
            }

        question = result.get("question") or {}
        lead_id = question.get("lead_id")
        company_name = question.get("company_name") or question.get("normalized_name") or ""
        reply = (
            f"已记录问题 {question_id} 的答案，相关 DD 记忆已更新。"
            f"{f' 该 lead（{lead_id}）会在下一轮 full cycle 重新 enrich。' if lead_id else ''}"
        )
        return {
            "ok": True,
            "reply": reply,
            "action": "answer_dd_question",
            "data": {
                "question_id": question_id,
                "lead_id": lead_id,
                "company_name": company_name,
                "result": result,
            },
        }

    def list_dd_questions(
        self,
        *,
        lead_id: int | None = None,
        company_query: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        if lead_id is not None:
            items = self.db.list_dd_questions(lead_id=lead_id, status="open", limit=limit)
        else:
            company_query = (company_query or "").strip()
            if company_query:
                matches = [dict(row) for row in self.db.find_leads_by_company_query(company_query, limit=5)]
                chosen = matches[0] if matches else None
                if chosen:
                    items = self.db.list_dd_questions(lead_id=int(chosen["id"]), status="open", limit=limit)
                else:
                    company_key = self.db._company_key_from_name(company_query)
                    items = self.db.list_dd_questions(company_key=company_key, status="open", limit=limit)
            else:
                items = self.db.list_dd_questions(status="open", limit=limit)

        if not items:
            return {
                "ok": True,
                "reply": "当前没有待确认的 DD 问题。",
                "action": "list_dd_questions",
                "data": {"items": []},
            }

        lines = [f"当前有 {len(items)} 个待确认 DD 问题："]
        for item in items[:10]:
            lines.append(
                f"- question {item['id']}: lead {item['lead_id']} | {item['company_name']} | {item['dimension']} | {item['prompt']}"
            )
        return {
            "ok": True,
            "reply": "\n".join(lines),
            "action": "list_dd_questions",
            "data": {"items": items},
        }

    def remember_entity_name_feedback(
        self,
        *,
        source_text: str,
        is_company: bool,
        canonical_name: str = "",
        official_domain: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        source_text = normalize_text_content(source_text)
        canonical_name = normalize_text_content(canonical_name)
        official_domain = normalize_text_content(official_domain)
        note = normalize_text_content(note)
        if is_company:
            entry = self.entity_name_feedback.record_company_alias(
                source_text=source_text,
                normalized_name=canonical_name,
                official_domain=official_domain,
                note=note,
            )
            verdict = "alias"
            reply = (
                f"已记住：'{source_text}' 对应的公司名是 '{canonical_name}'。"
                " 后续我会优先归一到这个标准名。"
            )
        else:
            entry = self.entity_name_feedback.record_not_company(
                source_text,
                note=note,
            )
            verdict = "not_company"
            reply = (
                f"已记住：'{source_text}' 不是公司名。"
                " 后续我会优先把它当作噪音字段过滤。"
            )

        self.db.add_feedback(
            verdict=verdict,
            feedback_type="entity_name_feedback",
            content=note or source_text,
            lead_id=None,
        )

        return {
            "ok": True,
            "reply": reply,
            "action": "remember_entity_name_feedback",
            "data": {
                "source_text": source_text,
                "is_company": is_company,
                "canonical_name": canonical_name,
                "official_domain": official_domain,
                "feedback_file": str(self.entity_name_feedback.path),
                "entry": entry,
            },
        }

    def list_recommendations(self, threshold: float) -> list[dict[str, Any]]:
        rows = self.db.get_recommendations(min_score=threshold, limit=30)
        data: list[dict[str, Any]] = []
        for row in rows:
            data.append(
                {
                    "lead_id": row["lead_id"],
                    "display_name": row.get("display_name") or row.get("normalized_name") or row["company_name"],
                    "company_name": row["company_name"],
                    "normalized_name": row.get("normalized_name") or "",
                    "raw_title": row.get("raw_title") or "",
                    "verification_status": row.get("verification_status") or "pending_review",
                    "confidence": row.get("confidence") or ("medium" if str(row.get("verification_status") or "") == "likely_company" else "high"),
                    "needs_human_review": bool(row.get("needs_human_review")) or str(row.get("verification_status") or "") == "likely_company",
                    "final_score": round(float(row["final_score"]), 2),
                    "recommendation_band": row["recommendation_band"] if "recommendation_band" in row.keys() else "",
                    "recommendation_reason": row["recommendation_reason"] if "recommendation_reason" in row.keys() else "",
                    "summary": row["business_summary"],
                    "reasons": row["score_reason"],
                    "sources": row["sources"],
                }
            )
        return data

    def get_company_analysis(
        self,
        *,
        company_query: str | None = None,
        lead_id: int | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        if lead_id is not None:
            record = self.db.get_company_analysis_for_lead(lead_id)
            if not record:
                return {
                    "ok": True,
                    "reply": f"我找不到 lead {lead_id} 的分析记录。",
                    "action": "get_company_analysis",
                    "data": {"lead_id": lead_id, "items": []},
                }
            if not self.db._lead_is_active_company(record):
                return self._format_not_company_response(
                    query=f"lead {lead_id}",
                    record=record,
                    reason="这条记录不是当前可用的公司主体",
                )
            return self.render_company_analysis(record, query=f"lead {lead_id}")

        query = (company_query or "").strip()
        if not query:
            return {
                "ok": True,
                "reply": "我想查完整分析，但没识别出公司名。你可以直接说：‘查看 Capsule Security 的完整分析’ 或 ‘lead 73 的完整分析’。",
                "action": "get_company_analysis",
                "data": {"items": []},
            }

        candidates = [dict(row) for row in self.db.find_leads_by_company_query(query, limit=limit)]
        if not candidates:
            return {
                "ok": True,
                "reply": f"我没有找到与 ‘{query}’ 相符的公司主体。",
                "action": "get_company_analysis",
                "data": {"query": query, "items": []},
            }

        active_candidates = [row for row in candidates if self.db._lead_is_active_company(row)]
        chosen_pool = active_candidates or candidates
        chosen = chosen_pool[0]
        record = self.db.get_company_analysis_for_lead(int(chosen["id"]))
        if not record:
            return {
                "ok": True,
                "reply": f"我找到了 {query}，但没有可用的分析记录。",
                "action": "get_company_analysis",
                "data": {"query": query, "items": candidates},
            }

        if not self.db._lead_is_active_company(record):
            return self._format_not_company_response(
                query=query,
                record=record,
                reason="匹配到了候选项，但它不是当前可用公司主体",
                alternatives=[self._company_brief(row) for row in candidates[1:4]],
            )

        result = self.render_company_analysis(record, query=query)
        if len(candidates) > 1:
            alternates = [self._company_brief(row) for row in candidates[1:4]]
            if alternates:
                result["reply"] += "\n\n其他相近候选：\n" + "\n".join(f"- {item}" for item in alternates)
        return result

    def render_dd_report(self, record: dict[str, Any], *, query: str) -> dict[str, Any]:
        company_name = str(record.get("display_name") or record.get("normalized_name") or record.get("company_name") or "")
        raw_title = str(record.get("raw_title") or "")
        official_domain = str(record.get("official_domain") or "")
        verification_status = str(record.get("verification_status") or "pending_review")
        entity_type = str(record.get("entity_type") or "unknown")
        source_hits = int(record.get("source_hits") or 0)
        dd_status = str(record.get("dd_status") or "dd_pending_review")
        completeness_score = float(record.get("completeness_score") or 0.0)
        dd_updated_at = record.get("dd_updated_at")
        dd_updated_text = dd_updated_at.isoformat() if hasattr(dd_updated_at, "isoformat") else str(dd_updated_at or "n/a")

        dd_overall = self._profile_dict(record.get("dd_overall"))
        dd_overall_status = str(dd_overall.get("dd_status") or dd_status)
        dd_overall_summary = str(dd_overall.get("summary") or "")
        missing_dimensions = dd_overall.get("missing_dimensions") or []
        confidence = dd_overall.get("confidence")

        reply_lines = [
            f"公司：{company_name}",
            f"- lead_id: {record.get('lead_id')}",
            f"- query: {query}",
            f"- raw_title: {raw_title or 'n/a'}",
            f"- official_domain: {official_domain or 'n/a'}",
            f"- verification_status: {verification_status}",
            f"- entity_type: {entity_type}",
            f"- source_hits: {source_hits}",
            f"- dd_status: {dd_status}",
            f"- completeness_score: {completeness_score:.2f}/100",
            f"- dd_updated_at: {dd_updated_text}",
            "",
            f"总览：{dd_overall_summary or 'n/a'}",
            f"- missing_dimensions: {', '.join(missing_dimensions) if missing_dimensions else 'none'}",
            f"- dd_overall_status: {dd_overall_status}",
            f"- dd_overall_confidence: {float(confidence or 0.0):.2f}",
            "",
        ]
        reply_lines.extend(self._render_dd_sections(record))
        reply_lines.extend(self._render_dd_questions(record))

        return {
            "ok": True,
            "reply": "\n".join(reply_lines).strip(),
            "action": "get_dd_report",
            "data": {
                "lead_id": record.get("lead_id"),
                "display_name": company_name,
                "company_name": company_name,
                "normalized_name": record.get("normalized_name"),
                "verification_status": verification_status,
                "confidence": record.get("confidence") or ("medium" if verification_status == "likely_company" else "high"),
                "needs_human_review": bool(record.get("needs_human_review")) or verification_status == "likely_company",
                "entity_type": entity_type,
                "source_hits": source_hits,
                "dd_status": dd_status,
                "completeness_score": completeness_score,
                "raw_title": raw_title,
                "official_domain": official_domain,
                "business_profile": self._profile_dict(record.get("business_profile")),
                "team_profile": self._profile_dict(record.get("team_profile")),
                "funding_profile": self._profile_dict(record.get("funding_profile")),
                "traction_profile": self._profile_dict(record.get("traction_profile")),
                "market_position": self._profile_dict(record.get("market_position")),
                "dd_overall": dd_overall or {},
                "questions": self._profile_list(record.get("questions")),
                "business_summary": str(record.get("business_summary") or ""),
                "team_summary": str(record.get("team_summary") or ""),
                "funding_summary": str(record.get("funding_summary") or ""),
                "traction_summary": str(record.get("traction_summary") or ""),
                "industry_position": str(record.get("industry_position") or ""),
                "dd_updated_at": dd_updated_text,
            },
        }

    def render_company_analysis(self, record: dict[str, Any], *, query: str) -> dict[str, Any]:
        company_name = str(record.get("display_name") or record.get("normalized_name") or record.get("company_name") or "")
        raw_title = str(record.get("raw_title") or "")
        official_domain = str(record.get("official_domain") or "")
        verification_status = str(record.get("verification_status") or "pending_review")
        entity_type = str(record.get("entity_type") or "unknown")
        status = str(record.get("status") or "")
        stage = str(record.get("stage") or "")
        source_hits = int(record.get("source_hits") or 0)
        completeness_score = float(record.get("completeness_score") or 0.0)
        dd_status = str(record.get("dd_status") or "dd_pending_review")

        base_score = record.get("base_score")
        thesis_fit = record.get("thesis_fit")
        evidence_strength = record.get("evidence_strength")
        business_score = record.get("business_score")
        team_score = record.get("team_score")
        funding_score = record.get("funding_score")
        traction_score = record.get("traction_score")
        market_score = record.get("market_score")
        thesis_fit_score = record.get("thesis_fit_score")
        evidence_score = record.get("evidence_score")
        raw_score = record.get("raw_score")
        confidence_multiplier = record.get("confidence_multiplier")
        boost_score = record.get("boost_score")
        penalty_score = record.get("penalty_score")
        thesis_fit_breakdown = self._profile_dict(record.get("thesis_fit_breakdown"))
        matched_policy_rules = record.get("matched_policy_rules") or []
        policy_version = record.get("policy_version")
        recommendation_band = str(record.get("recommendation_band") or "")
        recommendation_reason = str(record.get("recommendation_reason") or "")
        final_score = record.get("final_score")
        score_reason = str(record.get("score_reason") or "")
        score_updated_at = record.get("score_updated_at")
        dd_updated_at = record.get("dd_updated_at")

        dd_overall = self._profile_dict(record.get("dd_overall"))
        dd_overall_summary = str(dd_overall.get("summary") or "")
        dd_overall_missing = dd_overall.get("missing_dimensions") or []
        dd_overall_confidence = float(dd_overall.get("confidence") or 0.0)

        score_text = "尚未评分"
        recommendation_text = "尚未进入推荐池"
        if final_score is not None:
            final_score_value = float(final_score)
            base_score_value = float(base_score or 0)
            thesis_fit_value = float(thesis_fit or 0)
            evidence_strength_value = float(evidence_strength or 0)
            score_text = (
                f"final={final_score_value:.2f} | base={base_score_value:.2f} | "
                f"thesis={thesis_fit_value:.2f} | evidence={evidence_strength_value:.2f}"
            )
            if recommendation_band in {"Strong Recommend", "Recommend"}:
                recommendation_text = "已进入推荐池（正式推荐）"
            elif recommendation_band == "Watchlist":
                recommendation_text = "已进入 watchlist（不主动推送）"
            else:
                recommendation_text = f"未进入正式推荐池（band={recommendation_band or 'n/a'}，source_hits={source_hits}）"
        elif source_hits >= 2:
            recommendation_text = "已有较完整 DD，但尚未看到评分记录"

        breakdown_lines = []
        if any(
            value is not None
            for value in [
                business_score,
                team_score,
                funding_score,
                traction_score,
                market_score,
                thesis_fit_score,
                evidence_score,
                raw_score,
                confidence_multiplier,
                penalty_score,
            ]
        ):
            breakdown_lines.extend(
                [
                    f"- business_score: {float(business_score or 0):.2f}/5",
                    f"- team_score: {float(team_score or 0):.2f}/5",
                    f"- funding_score: {float(funding_score or 0):.2f}/5",
                    f"- traction_score: {float(traction_score or 0):.2f}/5",
                    f"- market_score: {float(market_score or 0):.2f}/5",
                    f"- thesis_fit_score: {float(thesis_fit_score or 0):.2f}/5",
                f"- evidence_score: {float(evidence_score or 0):.2f}/5",
                f"- raw_score: {float(raw_score or 0):.2f}",
                f"- confidence_multiplier: {float(confidence_multiplier or 0):.2f}",
                f"- boost_score: {float(boost_score or 0):.2f}",
                f"- penalty_score: {float(penalty_score or 0):.2f}",
                f"- recommendation_band: {recommendation_band or 'n/a'}",
                f"- policy_version: {int(policy_version or 0)}",
                f"- recommendation_reason: {recommendation_reason or 'n/a'}",
                "",
            ]
        )
            if thesis_fit_breakdown:
                breakdown_lines.extend(
                    [
                        "- thesis_fit_breakdown:",
                        *[
                            f"  - {key}: {float(value or 0):.2f}/5"
                            for key, value in thesis_fit_breakdown.items()
                        ],
                        "",
                    ]
                )
            if matched_policy_rules:
                breakdown_lines.extend(
                    [
                        "- matched_policy_rules:",
                        *[f"  - {str(item)}" for item in matched_policy_rules],
                        "",
                    ]
                )

        dd_time = dd_updated_at.isoformat() if hasattr(dd_updated_at, "isoformat") else str(dd_updated_at or "n/a")
        score_time = score_updated_at.isoformat() if hasattr(score_updated_at, "isoformat") else str(score_updated_at or "n/a")

        reply_lines = [
            f"公司：{company_name}",
            f"- lead_id: {record.get('lead_id')}",
            f"- query: {query}",
            f"- raw_title: {raw_title or 'n/a'}",
            f"- official_domain: {official_domain or 'n/a'}",
            f"- verification_status: {verification_status}",
            f"- entity_type: {entity_type}",
            f"- status: {status or 'n/a'}",
            f"- stage: {stage or 'n/a'}",
            f"- source_hits: {source_hits}",
            f"- dd_status: {dd_status}",
            f"- completeness_score: {completeness_score:.2f}/100",
            f"- dd_updated_at: {dd_time}",
            f"- score_updated_at: {score_time}",
            f"- score: {score_text}",
            f"- recommendation: {recommendation_text}",
            f"- score_reason: {score_reason or 'n/a'}",
            "",
            f"DD总览：{dd_overall_summary or 'n/a'}",
            f"- missing_dimensions: {', '.join(dd_overall_missing) if dd_overall_missing else 'none'}",
            f"- dd_overall_confidence: {dd_overall_confidence:.2f}",
            "",
        ]
        reply_lines.extend(breakdown_lines)
        reply_lines.extend(self._render_dd_sections(record))
        reply_lines.extend(self._render_dd_questions(record))

        return {
            "ok": True,
            "reply": "\n".join(reply_lines).strip(),
            "action": "get_company_analysis",
            "data": {
                "lead_id": record.get("lead_id"),
                "display_name": company_name,
                "company_name": company_name,
                "normalized_name": record.get("normalized_name"),
                "verification_status": verification_status,
                "entity_type": entity_type,
                "status": status,
                "stage": stage,
                "source_hits": source_hits,
                "dd_status": dd_status,
                "completeness_score": completeness_score,
                "raw_title": raw_title,
                "official_domain": official_domain,
                "business_profile": self._profile_dict(record.get("business_profile")),
                "team_profile": self._profile_dict(record.get("team_profile")),
                "funding_profile": self._profile_dict(record.get("funding_profile")),
                "traction_profile": self._profile_dict(record.get("traction_profile")),
                "market_position": self._profile_dict(record.get("market_position")),
                "dd_overall": dd_overall or {},
                "questions": self._profile_list(record.get("questions")),
                "business_score": business_score,
                "team_score": team_score,
                "funding_score": funding_score,
                "traction_score": traction_score,
                "market_score": market_score,
                "thesis_fit_score": thesis_fit_score,
                "evidence_score": evidence_score,
                "raw_score": raw_score,
                "confidence_multiplier": confidence_multiplier,
                "penalty_score": penalty_score,
                "recommendation_band": recommendation_band,
                "recommendation_reason": recommendation_reason,
                "business_summary": str(record.get("business_summary") or ""),
                "team_summary": str(record.get("team_summary") or ""),
                "funding_summary": str(record.get("funding_summary") or ""),
                "traction_summary": str(record.get("traction_summary") or ""),
                "industry_position": str(record.get("industry_position") or ""),
                "dd_updated_at": dd_time,
                "score_updated_at": score_time,
                "base_score": base_score,
                "thesis_fit": thesis_fit,
                "evidence_strength": evidence_strength,
                "final_score": final_score,
                "score_reason": score_reason,
                "recommendation": recommendation_text,
            },
        }

    def publish_recommendations(self, threshold: float) -> int:
        payload = {
            "event": "recommendations",
            "items": self.list_recommendations(threshold),
        }
        self.publish_event("recommendations", payload)
        return len(payload["items"])

    def publish_dd_questions(self, limit: int = 20) -> int:
        items = [
            row
            for row in self.db.list_dd_questions(status="open", limit=limit)
            if not str(row.get("published_at") or "").strip()
        ]
        if not items:
            return 0
        payload = {
            "event": "dd_questions",
            "items": items,
        }
        self.publish_event("dd_questions", payload)
        self.db.mark_dd_questions_published([int(item["id"]) for item in items if item.get("id") is not None])
        return len(items)

    def publish_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.settings.webhook_url:
            try:
                requests.post(
                    self.settings.webhook_url,
                    json=payload,
                    timeout=self.settings.webhook_timeout_seconds,
                ).raise_for_status()
                return
            except Exception:
                pass

        self.db.save_outbox_event(event_type=event_type, payload=payload)

    def _format_not_company_response(
        self,
        *,
        query: str,
        record: dict[str, Any],
        reason: str,
        alternatives: list[str] | None = None,
    ) -> dict[str, Any]:
        raw_title = str(record.get("raw_title") or "")
        normalized_name = str(record.get("display_name") or record.get("normalized_name") or record.get("company_name") or "")
        verification_status = str(record.get("verification_status") or "pending_review")
        reject_reason = str(record.get("reject_reason") or "")
        reply = (
            f"我找到了最接近的主体，但它目前不是可用公司：{normalized_name or raw_title or 'n/a'}\n"
            f"- query: {query}\n"
            f"- reason: {reason}\n"
            f"- verification_status: {verification_status}\n"
            f"- raw_title: {raw_title or 'n/a'}\n"
            f"- reject_reason: {reject_reason or 'n/a'}\n\n"
            "如果你愿意，我可以继续帮你找更准确的公司名，或者你直接给我 lead id。"
        )
        if alternatives:
            reply += "\n\n其他相近候选：\n" + "\n".join(f"- {item}" for item in alternatives)
        return {
            "ok": True,
            "reply": reply,
            "action": "get_company_analysis",
            "data": {
                "query": query,
                "items": [],
                "reason": reason,
                "verification_status": verification_status,
            },
        }

    def _company_brief(self, row: dict[str, Any]) -> str:
        company_name = str(row.get("display_name") or row.get("normalized_name") or row.get("company_name") or "")
        status = str(row.get("verification_status") or "pending_review")
        stage = str(row.get("stage") or "")
        lead_id = row.get("id")
        return f"{company_name} | {status} | {stage} | lead {lead_id}"

    def _render_dd_sections(self, record: dict[str, Any]) -> list[str]:
        sections: list[str] = []
        structured_sections = [
            ("业务概况", "business_profile", [
                ("one_liner", "one_liner"),
                ("products_services", "products_services"),
                ("target_customers", "target_customers"),
                ("use_cases", "use_cases"),
                ("official_domain", "official_domain"),
            ]),
            ("团队背景", "team_profile", [
                ("founders", "founders"),
                ("key_people", "key_people"),
                ("prior_companies", "prior_companies"),
                ("research_background", "research_background"),
            ]),
            ("融资概况", "funding_profile", [
                ("founded_year", "founded_year"),
                ("headquarters", "headquarters"),
                ("funding_rounds", "funding_rounds"),
                ("total_raised", "total_raised"),
                ("valuation", "valuation"),
                ("notable_investors", "notable_investors"),
            ]),
            ("业务进展", "traction_profile", [
                ("customers", "customers"),
                ("partners", "partners"),
                ("product_launches", "product_launches"),
                ("revenue_signals", "revenue_signals"),
                ("deployment_signals", "deployment_signals"),
            ]),
            ("行业地位", "market_position", [
                ("sub_sector", "sub_sector"),
                ("is_new_category", "is_new_category"),
                ("competitors", "competitors"),
                ("leader_signals", "leader_signals"),
                ("crowdedness", "crowdedness"),
            ]),
        ]

        for label, key, fields in structured_sections:
            profile = self._profile_dict(record.get(key))
            if not profile:
                continue
            field_values = profile.get("fields") or {}
            evidence = profile.get("evidence") or []
            missing = profile.get("missing_fields") or []
            confidence = float(profile.get("confidence") or 0.0)
            sections.append(f"{label}:")
            for field_name, display_label in fields:
                sections.append(f"- {display_label}: {self._format_profile_value(field_values.get(field_name))}")
            sections.append(f"- evidence_count: {len(evidence)}")
            sections.append(f"- missing_fields: {', '.join(missing) if missing else 'none'}")
            sections.append(f"- confidence: {confidence:.2f}")
            sections.append("")

        return sections

    def _render_dd_questions(self, record: dict[str, Any]) -> list[str]:
        lead_id = record.get("lead_id")
        company_key = str(record.get("company_key") or "")
        questions = []
        if hasattr(self.db, "list_dd_questions"):
            questions = self.db.list_dd_questions(lead_id=lead_id, company_key=company_key, status="open", limit=5)
        if not questions:
            questions = self._profile_list(record.get("questions"))
        if not questions:
            return []

        lines = ["", "待确认问题："]
        for item in questions[:5]:
            prompt = str(item.get("prompt") or "").strip()
            dimension = str(item.get("dimension") or "entity")
            status = str(item.get("status") or "open")
            missing_fields = item.get("missing_fields") or []
            missing_text = ", ".join([str(field).strip() for field in missing_fields if str(field).strip()]) or "none"
            lines.append(f"- question {item.get('id', 'n/a')} | {dimension} | {status} | {prompt}")
            lines.append(f"  missing_fields: {missing_text}")
        return lines

    @staticmethod
    def _profile_dict(profile: Any) -> dict[str, Any]:
        return dict(profile) if isinstance(profile, dict) else {}

    @staticmethod
    def _profile_list(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [dict(item) if isinstance(item, dict) else {"value": item} for item in value]
        return []

    @staticmethod
    def _format_profile_value(value: Any) -> str:
        if value is None:
            return "n/a"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(items) if items else "n/a"
        text = str(value).strip()
        return text or "n/a"

    @staticmethod
    def _is_verified_company(row: dict[str, Any]) -> bool:
        status = str(row.get("verification_status") or "pending_review").strip().lower()
        entity_type = str(row.get("entity_type") or "unknown").strip().lower()
        return status == "verified" and entity_type == "company"
