from __future__ import annotations

import re
from typing import Any

from app.core.db import normalize_json_payload, normalize_text_content


class HumanMessageRouter:
    """统一处理人类消息入口。"""

    def __init__(self, runtime: Any):
        self.runtime = runtime

    def handle(
        self,
        message: str,
        *,
        source: str = "direct",
        session_key: str | None = None,
        channel_id: str | None = None,
        sender: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raw_text = normalize_text_content(message).strip()
        metadata = normalize_json_payload(metadata or {})

        self.runtime.db.log_conversation_message(
            direction="inbound",
            source=source,
            session_key=session_key or "",
            channel_id=channel_id or "",
            sender=sender or "",
            content=raw_text,
            action="received",
            payload={
                "message": raw_text,
                "source": source,
                "session_key": session_key,
                "channel_id": channel_id,
                "sender": sender,
                "metadata": metadata,
            },
        )

        result = self._dispatch(raw_text)

        self.runtime.db.log_conversation_message(
            direction="outbound",
            source=source,
            session_key=session_key or "",
            channel_id=channel_id or "",
            sender="agent",
            content=normalize_text_content(result.get("reply", "")),
            action=str(result.get("action", "reply")),
            payload=normalize_json_payload(result),
        )

        return normalize_json_payload(result)

    def _dispatch(self, raw_text: str) -> dict[str, Any]:
        text = raw_text.lower().strip()

        if not raw_text:
            return {"ok": True, "reply": self._help_text(), "action": "help", "data": {}}

        if self._looks_like_run_cycle(text):
            try:
                result = self.runtime.run_full_cycle()
                run_at = result["run_at"].isoformat()
                reply = (
                    f"已完成一轮。时间：{run_at}，"
                    f"检索：{result['searched_items']}，"
                    f"新线索：{result['new_leads']}，"
                    f"DD完成：{result['dd_done']}，"
                    f"DD等待人工：{result.get('dd_waiting_human', 0)}，"
                    f"DD问题：{result.get('dd_questions', 0)}，"
                    f"打分：{result['scored']}，"
                    f"推荐：{result['recommended']}。"
                )
                result["run_at"] = run_at
                return {"ok": True, "reply": reply, "action": "run_cycle", "data": result}
            except Exception as exc:
                result = self._internal_run_cycle_error(exc)
                return {
                    "ok": False,
                    "reply": (
                        "这轮没有正常完成。"
                        f"原因：{result['failure_summary']}"
                    ),
                    "action": "run_cycle",
                    "data": result,
                }

        if self._looks_like_dd_question_answer(text):
            return self._handle_dd_question_answer(raw_text)

        if self._looks_like_dd_question_list(text):
            return self._handle_dd_question_list(raw_text)

        if self._looks_like_dd_feedback(text):
            return self._handle_dd_feedback(raw_text)

        if self._looks_like_entity_name_feedback(text):
            return self._handle_entity_name_feedback(raw_text)

        if self._looks_like_full_analysis_request(text):
            return self._handle_company_analysis_lookup(raw_text)

        if self._looks_like_dd_request(text):
            return self._handle_dd_lookup(raw_text)

        if self._contains_any(text, ["查看推荐", "推荐列表", "show recommendations", "recommendation"]):
            threshold = self.runtime.settings.recommend_score_threshold
            items = self.runtime.interaction.list_recommendations(threshold=threshold)
            if not items:
                return {
                    "ok": True,
                    "reply": "目前还没有达到阈值的推荐项目。你可以先说‘跑一轮’。",
                    "action": "list_recommendations",
                    "data": {"threshold": threshold, "items": []},
                }

            top = items[:5]
            lines = [f"已找到 {len(items)} 个推荐，先看前 {len(top)} 个："]
            for item in top:
                lines.append(f"- lead {item['lead_id']}: {item['display_name']}（{item['final_score']}）")
            return {
                "ok": True,
                "reply": "\n".join(lines),
                "action": "list_recommendations",
                "data": {"threshold": threshold, "items": items},
            }

        if self._contains_any(text, ["查看线索", "线索列表", "show leads", "leads"]):
            rows = self.runtime.db.list_leads(limit=20)
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
                    }
                )

            if not items:
                return {
                    "ok": True,
                    "reply": "现在还没有线索，先说‘跑一轮’。",
                    "action": "list_leads",
                    "data": {"items": []},
                }

            lines = [f"最近 {len(items)} 条线索："]
            for item in items[:10]:
                lines.append(f"- lead {item['id']}: {item['display_name']} | {item['status']} | {item['sources']}")
            return {"ok": True, "reply": "\n".join(lines), "action": "list_leads", "data": {"items": items}}

        if ("关闭" in text or "暂停" in text) and "bocha" in text:
            result = self.runtime.update_channel(channel="bocha", enabled=False)
            return {
                "ok": True,
                "reply": "已关闭 bocha 渠道。",
                "action": "update_channel",
                "data": {"channel": "bocha", "enabled": False, "memory": result},
            }

        if ("开启" in text or "恢复" in text) and "bocha" in text:
            result = self.runtime.update_channel(channel="bocha", enabled=True)
            return {
                "ok": True,
                "reply": "已开启 bocha 渠道。",
                "action": "update_channel",
                "data": {"channel": "bocha", "enabled": True, "memory": result},
            }

        if ("关闭" in text or "暂停" in text) and "brave" in text:
            result = self.runtime.update_channel(channel="brave", enabled=False)
            return {
                "ok": True,
                "reply": "已关闭 brave 渠道。",
                "action": "update_channel",
                "data": {"channel": "brave", "enabled": False, "memory": result},
            }

        if ("开启" in text or "恢复" in text) and "brave" in text:
            result = self.runtime.update_channel(channel="brave", enabled=True)
            return {
                "ok": True,
                "reply": "已开启 brave 渠道。",
                "action": "update_channel",
                "data": {"channel": "brave", "enabled": True, "memory": result},
            }

        if self._contains_any(text, ["刷新策略", "更新策略", "refresh strategy"]):
            result = self.runtime.refresh_strategy()
            return {
                "ok": True,
                "reply": f"策略已刷新。今天的日期是 {result.get('today', 'unknown')}。",
                "action": "refresh_strategy",
                "data": result,
            }

        if self._contains_any(text, ["压缩记忆", "整理记忆", "compress memory"]):
            result = self.runtime.compress_memory()
            return {
                "ok": True,
                "reply": "长期记忆已压缩去重。",
                "action": "compress_memory",
                "data": result,
            }

        if self._contains_any(text, ["不喜欢", "不值得", "别推荐", "dislike"]):
            lead_id = self._extract_lead_id(raw_text)
            self.runtime.interaction.receive_feedback(
                verdict="dislike",
                content=raw_text,
                feedback_type="lead_feedback",
                lead_id=lead_id,
            )
            return {
                "ok": True,
                "reply": "收到，我已经记下你的负反馈了。",
                "action": "feedback_dislike",
                "data": {"lead_id": lead_id},
            }

        if self._contains_any(text, ["喜欢", "值得跟进", "继续跟踪", "like"]):
            lead_id = self._extract_lead_id(raw_text)
            self.runtime.interaction.receive_feedback(
                verdict="like",
                content=raw_text,
                feedback_type="lead_feedback",
                lead_id=lead_id,
            )
            return {
                "ok": True,
                "reply": "收到，我已经记下你的正反馈了。",
                "action": "feedback_like",
                "data": {"lead_id": lead_id},
            }

        if self._contains_any(text, ["跳过", "先跳过", "skip"]):
            lead_id = self._extract_lead_id(raw_text)
            self.runtime.interaction.receive_feedback(
                verdict="skip",
                content=raw_text,
                feedback_type="scoring_feedback",
                lead_id=lead_id,
            )
            return {
                "ok": True,
                "reply": "收到，我已经记下这条跳过原因了。",
                "action": "feedback_skip",
                "data": {"lead_id": lead_id},
            }

        if self._contains_any(text, ["主体错了", "wrong entity", "识别错了", "不是这个主体"]):
            lead_id = self._extract_lead_id(raw_text)
            self.runtime.interaction.receive_feedback(
                verdict="wrong_entity",
                content=raw_text,
                feedback_type="scoring_feedback",
                lead_id=lead_id,
            )
            return {
                "ok": True,
                "reply": "收到，我已经把这个主体纠错记到评分偏好了。",
                "action": "feedback_wrong_entity",
                "data": {"lead_id": lead_id},
            }

        if self._contains_any(text, ["prefer sector", "偏好赛道", "更关注", "重点关注", "prefer_sector"]):
            lead_id = self._extract_lead_id(raw_text)
            self.runtime.interaction.receive_feedback(
                verdict="prefer_sector",
                content=raw_text,
                feedback_type="scoring_feedback",
                lead_id=lead_id,
            )
            return {
                "ok": True,
                "reply": "收到，我已经把这个赛道偏好记到评分策略里了。",
                "action": "feedback_prefer_sector",
                "data": {"lead_id": lead_id},
            }

        return {
            "ok": True,
            "reply": f"我还没理解这句话：{raw_text}\n\n{self._help_text()}",
            "action": "help",
            "data": {},
        }

    def _internal_run_cycle_error(self, exc: Exception) -> dict[str, Any]:
        message = normalize_text_content(str(exc) or exc.__class__.__name__).strip() or exc.__class__.__name__
        hint = "Inspect server logs and rerun after fixing the runtime exception."
        return {
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
            "failure_summary": f"run_cycle raised {exc.__class__.__name__}: {message}",
            "unavailable_sources": [],
            "source_status_by_channel": {},
            "action_suggestions": [hint],
        }

    def _handle_company_analysis_lookup(self, raw_text: str) -> dict[str, Any]:
        lead_id = self._extract_lead_id(raw_text)
        if lead_id is not None:
            return self.runtime.interaction.get_company_analysis(lead_id=lead_id)

        company_query = self._extract_company_query(raw_text)
        return self.runtime.interaction.get_company_analysis(company_query=company_query)

    def _handle_dd_lookup(self, raw_text: str) -> dict[str, Any]:
        lead_id = self._extract_lead_id(raw_text)
        if lead_id is not None:
            record = self.runtime.db.get_dd_report_for_lead(lead_id)
            if not record:
                return {
                    "ok": True,
                    "reply": f"我找到了 lead {lead_id}，但没有对应的 DD 记录。",
                    "action": "get_dd_report",
                    "data": {"lead_id": lead_id, "items": []},
                }
            return self._format_dd_response(record, query=f"lead {lead_id}")

        company_query = self._extract_company_query(raw_text)
        matches = [dict(row) for row in self.runtime.db.find_leads_by_company_query(company_query, limit=5)]
        if not matches:
            return {
                "ok": True,
                "reply": f"我没有找到与 ‘{company_query}’ 相符的公司主体。",
                "action": "get_dd_report",
                "data": {"query": company_query, "items": []},
            }

        active_candidates = [row for row in matches if self._is_active_company_row(row)]
        chosen_pool = active_candidates or matches
        chosen = chosen_pool[0]
        record = self.runtime.db.get_dd_report_for_lead(int(chosen["id"]))
        if not record:
            return {
                "ok": True,
                "reply": self._format_no_dd_response(chosen, company_query),
                "action": "get_dd_report",
                "data": {"query": company_query, "items": matches},
            }

        result = self._format_dd_response(record, query=company_query)
        if len(matches) > 1:
            alternates = [self._company_brief(row) for row in matches[1:4]]
            if alternates:
                result["reply"] += "\n\n其他相近候选：\n" + "\n".join(f"- {item}" for item in alternates)
        return result

    def _handle_dd_question_list(self, raw_text: str) -> dict[str, Any]:
        lead_id = self._extract_lead_id(raw_text)
        if lead_id is not None:
            return self.runtime.interaction.list_dd_questions(lead_id=lead_id)

        company_query = self._extract_company_query(raw_text)
        if company_query:
            return self.runtime.interaction.list_dd_questions(company_query=company_query)

        return self.runtime.interaction.list_dd_questions()

    def _handle_dd_question_answer(self, raw_text: str) -> dict[str, Any]:
        question_id = self._extract_dd_question_id(raw_text)
        if question_id is None:
            return {
                "ok": True,
                "reply": (
                    "我识别到了你在回答 DD 问题，但没找到问题编号。"
                    "你可以直接说：‘问题 12 的答案是客户是 Fortune 500 企业’。"
                ),
                "action": "answer_dd_question",
                "data": {"question_id": None},
            }

        answer = raw_text.strip()
        result = self.runtime.interaction.answer_dd_question(question_id, answer)
        data = dict(result.get("data") or {})
        lead_id = data.get("lead_id")
        rerun = None
        if lead_id:
            try:
                rerun = self.runtime.rerun_dd_for_lead(int(lead_id))
            except Exception as exc:
                rerun = {"ok": False, "reason": "rerun_failed", "error": str(exc)}
        reply = result.get("reply", "")
        if rerun and rerun.get("ok"):
            reply += f" 已重新触发 lead {lead_id} 的 DD enrich，当前状态：{rerun.get('dd_status', 'unknown')}。"
        return {
            "ok": True,
            "reply": reply,
            "action": "answer_dd_question",
            "data": {"answer_result": result, "rerun": rerun},
        }

    def _handle_dd_feedback(self, raw_text: str) -> dict[str, Any]:
        scope, lead_id, company_query = self._resolve_dd_feedback_scope(raw_text)
        target = self._resolve_dd_feedback_target(lead_id=lead_id, company_query=company_query)
        result = self.runtime.interaction.record_dd_feedback(
            raw_text=raw_text,
            scope=scope,
            lead_id=target.get("lead_id"),
            company_name=target.get("company_name", ""),
            normalized_name=target.get("normalized_name", ""),
            official_domain=target.get("official_domain", ""),
            dimension=target.get("dimension", "entity"),
            feedback_kind="note",
        )
        reply = result.get("reply", "已记下这条 DD 反馈。")
        if target.get("lead_id") and scope in {"lead", "company"}:
            try:
                rerun = self.runtime.rerun_dd_for_lead(int(target["lead_id"]))
                if rerun.get("ok"):
                    reply += f" 已按 lead {target['lead_id']} 重新触发 enrich。"
            except Exception:
                pass
        return {
            "ok": True,
            "reply": reply,
            "action": "record_dd_feedback",
            "data": {
                "scope": scope,
                "lead_id": target.get("lead_id"),
                "company_name": target.get("company_name", ""),
                "normalized_name": target.get("normalized_name", ""),
                "official_domain": target.get("official_domain", ""),
                "result": result,
            },
        }

    def _format_dd_response(self, record: dict[str, Any], *, query: str) -> dict[str, Any]:
        return self.runtime.interaction.render_dd_report(record, query=query)

    def _format_no_dd_response(self, row: dict[str, Any], query: str) -> dict[str, Any]:
        company_name = str(row.get("display_name") or row.get("normalized_name") or row.get("company_name") or "")
        return {
            "ok": True,
            "reply": (
                f"我找到了公司 {company_name}，但当前还没有 DD 报告。\n"
                f"- query: {query}\n"
                f"- lead_id: {row.get('id')}\n"
                f"- status: {row.get('verification_status')}\n"
                f"- stage: {row.get('stage')}\n\n"
                "你可以让我先跑一轮，或者你也可以指定其他公司。"
            ),
            "action": "get_dd_report",
            "data": {"query": query, "lead_id": row.get("id"), "items": []},
        }

    def _looks_like_run_cycle(self, text: str) -> bool:
        return bool(re.search(r"\brun[_ ]?cycle\b", text)) or any(
            phrase in text for phrase in ["跑一轮", "扫描一次", "执行一轮", "运行一轮"]
        )

    def _looks_like_full_analysis_request(self, text: str) -> bool:
        return any(
            phrase in text
            for phrase in [
                "完整分析",
                "综合分析",
                "全量分析",
                "公司分析",
                "推荐理由",
                "评分理由",
                "完整报告",
                "full analysis",
                "why recommended",
                "score reason",
                "analysis summary",
            ]
        )

    def _looks_like_dd_request(self, text: str) -> bool:
        if re.search(r"\bdd\b", text):
            return True
        return any(
            phrase in text
            for phrase in [
                "尽调",
                "尽职调查",
                "尽调报告",
                "调研",
                "总结",
                "分析",
                "详情",
            ]
        )

    def _extract_company_query(self, raw_text: str) -> str:
        text = raw_text.strip()
        text = re.sub(
            r"(?i)\b(show|view|get|give me|please|summarize|summarise|analyze|analyse|check|lookup|search|detail|details|full|complete|overall|comprehensive|for|the)\b",
            " ",
            text,
        )
        text = re.sub(
            r"(查看|给我|帮我|请|总结一下|总结|分析一下|分析|解释一下|解释|看一下|看看|查询|查一下|查查|调出|调取|展示|完整|完整分析|综合分析|全量分析|公司分析|推荐理由|评分理由)",
            " ",
            text,
        )
        text = re.sub(r"(?i)\b(dd|diligence|report|analysis|score|reason)\b", " ", text)
        text = re.sub(r"(尽调|尽职调查|调研|报告|详情|情况|资料|摘要|完整报告)", " ", text)
        text = re.sub(r"(?i)\blead\s*[:#]?\s*\d+\b", " ", text)
        text = text.replace("的", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip(" -_：:，,。！？?!")

    def _looks_like_entity_name_feedback(self, text: str) -> bool:
        if "公司名" not in text and "公司名字" not in text and "公司主体" not in text:
            return False
        return (
            "不是公司名" in text
            or "不是公司名字" in text
            or "不是公司主体" in text
            or "公司名是" in text
            or "公司名字是" in text
            or "应该是" in text
            or "其实是" in text
        )

    def _looks_like_dd_feedback(self, text: str) -> bool:
        triggers = [
            "尽调反馈",
            "重点补",
            "优先补",
            "不补",
            "少看",
            "全局规则",
            "公司级",
            "lead级",
            "项目级",
            "反馈给dd",
            "记住这条反馈",
        ]
        lowered = text.lower()
        return any(trigger.lower() in lowered for trigger in triggers)

    def _looks_like_dd_question_list(self, text: str) -> bool:
        triggers = ["待确认问题", "问题列表", "查看问题", "dd问题", "未确认问题", "待确认dd"]
        lowered = text.lower()
        return any(trigger.lower() in lowered for trigger in triggers)

    def _looks_like_dd_question_answer(self, text: str) -> bool:
        return self._extract_dd_question_id(text) is not None and any(
            phrase in text
            for phrase in [
                "答案",
                "回答",
                "回复",
                "确认",
                "补充",
                "是",
                "不是",
            ]
        )

    def _handle_entity_name_feedback(self, raw_text: str) -> dict[str, Any]:
        parsed = self._parse_entity_name_feedback(raw_text)
        if not parsed:
            return {
                "ok": True,
                "reply": (
                    "我识别到了公司名纠错，但没解析成功。\n"
                    "你可以直接用这两种格式：\n"
                    "- 10 Hot AI Security Startups To Know In 2025 不是公司名\n"
                    "- Protect AI for AI Agent Security 的公司名是 Protect AI"
                ),
                "action": "remember_entity_name_feedback",
                "data": {"parsed": None},
            }

        return self.runtime.interaction.remember_entity_name_feedback(
            source_text=parsed["source_text"],
            is_company=parsed["is_company"] == "true",
            canonical_name=parsed.get("canonical_name", ""),
            official_domain=parsed.get("official_domain", ""),
            note=raw_text,
        )

    def _extract_dd_question_id(self, text: str) -> int | None:
        patterns = [
            r"(?:问题|question|ddq)\s*[:#-]?\s*(\d+)",
            r"answer\s+question\s*(\d+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            try:
                return int(match.group(1))
            except Exception:
                return None
        return None

    def _resolve_dd_feedback_scope(self, raw_text: str) -> tuple[str, int | None, str]:
        lowered = raw_text.lower()
        lead_id = self._extract_lead_id(raw_text)
        company_query = self._extract_company_query(raw_text)

        if "全局" in raw_text or "global" in lowered:
            return "global", lead_id, company_query
        if lead_id is not None or "lead" in lowered or "项目" in raw_text:
            return "lead", lead_id, company_query
        if any(marker in raw_text for marker in ["公司级", "公司层面", "这家公司", "该公司"]) or company_query:
            return "company", lead_id, company_query
        return "global", lead_id, company_query

    def _resolve_dd_feedback_target(self, *, lead_id: int | None, company_query: str) -> dict[str, Any]:
        if lead_id is not None:
            lead = self.runtime.db.get_lead_by_id(lead_id)
            if lead:
                return {
                    "lead_id": lead_id,
                    "company_name": str(lead.get("company_name") or ""),
                    "normalized_name": str(
                        lead.get("normalized_name")
                        or lead.get("candidate_name")
                        or lead.get("company_name")
                        or ""
                    ),
                    "official_domain": str(lead.get("official_domain") or ""),
                    "dimension": "entity",
                }

        if company_query:
            candidates = [dict(row) for row in self.runtime.db.find_leads_by_company_query(company_query, limit=5)]
            if candidates:
                chosen = candidates[0]
                return {
                    "lead_id": int(chosen.get("id") or 0) or None,
                    "company_name": str(chosen.get("company_name") or ""),
                    "normalized_name": str(
                        chosen.get("normalized_name")
                        or chosen.get("candidate_name")
                        or chosen.get("company_name")
                        or ""
                    ),
                    "official_domain": str(chosen.get("official_domain") or ""),
                    "dimension": "entity",
                }

        return {
            "lead_id": lead_id,
            "company_name": "",
            "normalized_name": "",
            "official_domain": "",
            "dimension": "entity",
        }

    def _parse_entity_name_feedback(self, raw_text: str) -> dict[str, str] | None:
        text = raw_text.strip()
        text = re.sub(r"^[\s`'\"“”]+|[\s`'\"“”]+$", "", text)

        negative_patterns = [
            r"^(?:记住|记一下|记录|帮我记住|请记住|把)?\s*[`'\"“”]?(.+?)[`'\"“”]?\s*(?:不是|不算|别是|不属于)\s*(?:公司名|公司名字|公司主体|公司)\s*$",
            r"^(?:记住|记一下|记录|帮我记住|请记住|把)?\s*(.+?)\s*(?:不是公司名|不是公司名字|不是公司主体|不是公司)\s*$",
        ]
        for pattern in negative_patterns:
            match = re.match(pattern, text, flags=re.IGNORECASE)
            if match:
                source_text = self._strip_quotes(match.group(1))
                if source_text:
                    return {"is_company": "false", "source_text": source_text}

        positive_patterns = [
            r"^(?:记住|记一下|记录|帮我记住|请记住|把)?\s*[`'\"“”]?(.+?)[`'\"“”]?\s*(?:的)?公司(?:名|名字)\s*是\s*[`'\"“”]?(.+?)[`'\"“”]?\s*$",
            r"^(?:记住|记一下|记录|帮我记住|请记住|把)?\s*[`'\"“”]?(.+?)[`'\"“”]?\s*(?:其实是|应当是|应该是|就是)\s*[`'\"“”]?(.+?)[`'\"“”]?\s*$",
        ]
        for pattern in positive_patterns:
            match = re.match(pattern, text, flags=re.IGNORECASE)
            if match:
                source_text = self._strip_quotes(match.group(1))
                canonical_name = self._strip_quotes(match.group(2))
                if source_text and canonical_name:
                    return {
                        "is_company": "true",
                        "source_text": source_text,
                        "canonical_name": canonical_name,
                    }

        return None

    @staticmethod
    def _strip_quotes(value: str) -> str:
        text = str(value or "").strip()
        text = re.sub(r"^[\s`'\"“”]+", "", text)
        text = re.sub(r"[\s`'\"“”]+$", "", text)
        return text.strip(" -_：:，,。！？?!")

    @staticmethod
    def _extract_lead_id(text: str) -> int | None:
        match = re.search(r"(?:lead|项目|id)\s*[:：]?\s*(\d+)", text, flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None

    def _company_brief(self, row: dict[str, Any]) -> str:
        company_name = str(row.get("display_name") or row.get("normalized_name") or row.get("company_name") or "")
        status = str(row.get("verification_status") or "pending_review")
        stage = str(row.get("stage") or "")
        lead_id = row.get("id")
        return f"{company_name} | {status} | {stage} | lead {lead_id}"

    def _is_active_company_row(self, row: dict[str, Any]) -> bool:
        return self.runtime.db._lead_is_active_company(row)

    @staticmethod
    def _contains_any(text: str, phrases: list[str]) -> bool:
        return any(phrase in text for phrase in phrases)

    @staticmethod
    def _help_text() -> str:
        return (
            "我可以执行这些操作：\n"
            "1) 跑一轮 / 扫描一次\n"
            "2) 查看推荐\n"
            "3) 查看线索\n"
            "4) 查看某公司的 DD：例如‘查看 Capsule Security 的 DD’、‘lead 73 的 DD’\n"
            "5) 查看某公司的完整分析：例如‘查看 Capsule Security 的完整分析’、‘lead 73 的完整分析’\n"
            "6) 记住公司名纠错：例如‘10 Hot AI Security Startups To Know In 2025 不是公司名’\n"
            "7) 记住公司名归一：例如‘Protect AI for AI Agent Security 的公司名是 Protect AI’\n"
            "8) DD反馈：例如‘lead 73 重点补客户，不补估值’、‘全局规则：优先补团队，不补 valuation’\n"
            "9) 查看待确认问题：例如‘查看待确认问题’\n"
            "10) 回答 DD 问题：例如‘问题 12 的答案是客户是 Fortune 500 企业’\n"
            "11) 关闭 bocha / 开启 bocha / 关闭 brave / 开启 brave\n"
            "12) 刷新策略 / 压缩记忆\n"
            "13) 反馈：例如‘我不喜欢 lead 3，明显是大公司’\n"
            "14) 跳过 / 主体错了 / 偏好赛道：例如‘跳过 lead 8，因为没有客户’、‘主体错了’、‘我更关注 agent security’"
        )
