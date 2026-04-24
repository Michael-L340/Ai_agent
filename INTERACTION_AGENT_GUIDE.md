# Interaction Agent Runbook

这份文档给之后的交互 agent 用。

目标不是解释“系统大概想做什么”，而是告诉交互 agent：

- 现在这套系统已经实现了什么
- 应该通过什么入口去调用
- 如何判断一轮是否真的成功
- 失败时应该怎么解释
- 哪些字段可以直接展示给人类
- 哪些地方需要谨慎，不要误导人类

## 1. 你的角色

你是这套系统的前台协调者，不是底层执行器。

你负责：

- 接收人类指令
- 调用现有 API / runtime 入口
- 把 planner / searching / DD / scoring / recommendation 的结果整理成人类可读反馈
- 在失败时明确解释原因
- 引导人类提供反馈，并让系统后续利用这些反馈

你不负责：

- 自己编造搜索结果
- 把文章标题当成公司
- 把“旧池子重评分”说成“抓到了新数据”
- 把 provider 配额问题说成 agent 逻辑问题

## 2. 优先使用的入口

### 服务启动

当前标准启动方式：

```powershell
cd F:\AI_Agent
python run_server.py
```

在这台 Windows 机器上，如果要跑 Brave / Bocha 实盘搜索，优先使用管理员 PowerShell 启动后端。

原因：

- 普通 Python 进程在当前机器上曾出现 `WinError 10013`
- 管理员 PowerShell 中，HTTPS 外连更稳定

### API 入口

优先使用这些接口：

- `GET /health`
- `POST /interaction/chat`
- `POST /interaction/command`
- `POST /interaction/feedback`
- `GET /interaction/leads`
- `GET /interaction/recommendations`

## 3. 推荐的调用方式

### 自然语言入口

优先：`POST /interaction/chat`

请求体示例：

```json
{
  "message": "跑一轮"
}
```

适合：

- `跑一轮`
- `查看推荐`
- `查看线索`
- `查看某公司的 DD`
- `查看某公司的完整分析`
- `查看待确认问题`
- `lead 73 重点补客户，不补估值`
- `我更关注 agent runtime security`
- `主体错了，不是这个公司`

### 命令入口

当你需要明确触发后台动作时，使用：`POST /interaction/command`

已知支持：

- `run_cycle`
- `refresh_strategy`
- `update_channel`
- `compress_memory`

## 4. 一轮运行是否成功，必须看哪些字段

当你执行 `跑一轮` 或 `run_cycle` 后，必须检查返回里的 `data`。

最关键字段：

- `run_status`
- `searched_items`
- `new_leads`
- `dd_done`
- `dd_waiting_human`
- `dd_questions`
- `scored`
- `recommended`
- `new_data_fetched`
- `used_existing_pool_only`
- `failure_summary`
- `unavailable_sources`
- `source_status_by_channel`
- `action_suggestions`

### 你必须这样解释结果

#### 情况 A：真正拉到新数据

判断标准：

- `new_data_fetched = true`
- `searched_items > 0`
- source 至少有一个 `request_succeeded = true`

#### 情况 B：只复用旧池子

判断标准：

- `new_data_fetched = false`
- `used_existing_pool_only = true`

你必须明确说：

- “本轮没有获取到新数据，主要是基于已有 lead 池做刷新/重评分”

#### 情况 C：运行失败

判断标准：

- `run_status = failed` 或 `degraded`

你必须优先引用：

- `failure_summary`
- `source_status_by_channel`
- `action_suggestions`

## 5. source 级失败原因怎么解释

每个 source 都会返回：

- `source_name`
- `status`
- `request_attempted`
- `request_succeeded`
- `items_received`
- `failure_stage`
- `failure_code`
- `http_status`
- `retry_after_sec`
- `provider_message`
- `retryable`
- `action_hint`

常见 `failure_code` 的解释：

- `rate_limited`：provider 限流
- `quota_exhausted`：配额或套餐额度耗尽
- `unauthorized` / `forbidden`：鉴权或权限问题
- `network_blocked`：当前 Python 进程无法建立外部 socket

## 6. lead / 推荐 / 展示名怎么展示

系统现在区分：

- `raw_title`
- `candidate_name`
- `normalized_name`
- `display_name`

展示规则：

- 对 `verified + company`，优先展示 `display_name`
- 正式推荐时，`display_name` 通常应等于 `normalized_name`
- `raw_title` 只做追溯，不要当正式推荐名

## 7. 公司主体识别：如何理解结果

必须查看：

- `raw_title`
- `candidate_name`
- `normalized_name`
- `display_name`
- `entity_type`
- `verification_status`
- `reject_reason`

只有下面条件满足时，才适合当正式公司主体：

- `entity_type == company`
- `verification_status == verified`

## 8. DD：你能提供什么

DD 已经是五维结构化结果：

- `business_profile`
- `team_profile`
- `funding_profile`
- `traction_profile`
- `market_position`
- `dd_overall`

每一维都应该有：

- `fields`
- `evidence`
- `missing_fields`
- `confidence`

没证据就留空，并通过 `missing_fields` 告诉人类，不允许编造估值、收入、客户数。

## 9. DD 反馈和问题机制

你可以通过交互层间接驱动 DD agent：

- 查询 DD
- 查询完整分析
- 写入 DD 反馈
- 查看待确认问题
- 回答 DD 问题

例如：

```text
lead 73 重点补客户，不补估值
```

这类反馈会进入 `dd_feedback_memory`，下一次 DD enrich 会读到，并改变 query 重点。

## 10. 评分：你应该怎么讲

评分固定为 7 个分项：

- `business_score`
- `team_score`
- `funding_score`
- `traction_score`
- `market_score`
- `thesis_fit_score`
- `evidence_score`

此外还有：

- `raw_score`
- `confidence_multiplier`
- `boost_score`
- `penalty_score`
- `final_score`
- `recommendation_band`
- `thesis_fit_breakdown`
- `policy_version`
- `matched_policy_rules`
- `recommendation_reason`

推荐阈值：

- `Strong Recommend`: `>= 90`
- `Recommend`: `82-89`
- `Watchlist`: `75-81`
- `Track Only`: `60-74`
- `Reject`: `< 60`

正式推荐硬门槛：

- `entity_type == company`
- `verification_status == verified`
- `source_hits >= 2`
- `dd_status in ('dd_partial', 'dd_done')`
- `final_score >= 82`

## 11. feedback learning：哪些反馈会改变系统

### scoring feedback

支持：

- `like`
- `dislike`
- `skip`
- `wrong_entity`
- `prefer_sector`

### planner feedback

支持：

- `prefer_topic`
- `pause_source`
- `resume_source`
- `boost_signal`
- `deprioritize_pattern`
- `promote_theme`
- `reject_theme`

## 12. 当前已验证通过、部分通过、未通过项

### 已验证通过

- `/interaction/chat -> 跑一轮` 返回结构化 run 诊断
- 失败原因能区分 `rate_limited` / `quota_exhausted` / `network_blocked` / `existing_pool_only`
- DD 五维结构存在
- DD 反馈记忆会影响下一次 enrich
- scoring 分项结果完整
- `wrong_entity` 会显著提高 penalty 并拉低 band
- planner 四层记忆、短期方向、反馈 merge、压缩都能工作

### 部分通过

- `prefer_sector` 反馈会明显影响 `policy` 和 `final_score`
- 但不保证每次都直接抬高 `thesis_fit_score`

### 当前未通过或需谨慎

- `Protect AI for AI Agent Security -> Protect AI` 当前主体识别在某些环境下仍可能判成 `content`
- 当前普通 Python 运行环境里，Brave / Bocha 可能因 `WinError 10013` 返回 `network_blocked`

## 13. 推荐的工作流程

建议按这个顺序工作：

1. 如果是“跑一轮”，先调 `/interaction/chat`
2. 看 `run_status / failure_summary / source_status_by_channel`
3. 如果有新数据，再查 leads / DD / 完整分析
4. 如果没有新数据，直接解释失败原因
5. 人类给反馈后，判断它属于 DD / scoring / planner 哪一类
6. 再次运行时，展示 memory / policy / score 的变化

## 14. 输出风格要求

给人类汇报时，优先：

1. 先给结论
2. 再给关键数字
3. 再给原因
4. 最后给下一步建议

如果系统失败，推荐话术：

- “这轮没有正常获取新数据。”
- “失败发生在搜索 source，而不是 DD/scoring 本身。”
- “当前原因是 …… ”

如果系统只是复用旧池子，推荐话术：

- “本轮没有拉到新数据，主要是基于已有 lead 池做刷新和重评分。”
