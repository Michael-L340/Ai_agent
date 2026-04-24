# AI Invest Agent (Brave + Bocha 双源版 MVP)

这是一个适合笔试演示的最小可用版本，目标是：
- 7x24 自动扫描 AI Agent 安全公司线索
- 用 Brave + Bocha 双源做检索
- 经过 Planner -> Searching -> DD -> Scoring -> Interaction 的完整流程

## 0. 交互 Agent 手册

如果后续要换一个新的交互 agent 聊天框来接管系统，请优先阅读：

- [INTERACTION_AGENT_GUIDE.md](/F:/AI_Agent/INTERACTION_AGENT_GUIDE.md)

这份手册会说明：
- 应该通过哪些 API/命令与系统交互
- 如何判断一轮是否真的成功
- 失败时该如何解释
- 哪些字段可以展示给人类
- 当前已验证通过、部分通过、未通过的能力边界

## 1. 架构说明（已实现）

1. `Interaction Agent`
- 唯一对外入口（接收指令、接收反馈、输出推荐）
- 统一对外输出到 `webhook` 或 `outbox`
- 现在也支持 `OpenClaw` 的专用 webhook 入口
- 还能记录“某字段不是公司名”以及“某字段对应的标准公司名”这类纠错记忆，后续主体核验会优先参考
- 推荐列表、线索列表、DD/完整分析会优先展示 `display_name`
- `pending_review` 和 `rejected` 对象不会进入正式推荐池

2. `Planner Agent`
- 长期记忆：赛道、关键词、负面过滤词、渠道开关
- 短期记忆：每天读取日报策略（默认 `F:\AI_Agent\data\daily_strategy.txt`）
- 反馈学习：根据人类反馈更新偏好和渠道开关
- 记忆压缩：定期去重压缩
- 现在已升级成四层 memory schema：
  - `planner_long_memory`：`sub_sectors`、`signal_dictionary`、`negative_filters`、`source_policy`、`human_preferences`、`version`
  - `planner_short_memory`：`emerging_themes`、`priority`、`keywords`、`source_suggestions`、`days_active`、`promote_candidate`
  - `planner_feedback_memory`：`feedback_type`、`target`、`value`、`status`
  - `planner_compaction`：`promoted_themes`、`decayed_themes`、`merged_topics`、`archived_preferences`
- 现有 `run_full_cycle` 不需要改入口，planner 会在内部通过 memory manager 读写这四层结构
- planner 现在会读取最近 N 天的：
  - daily strategy
  - conversation messages
  - signal logs
  - human feedback
- 然后生成新的 `emerging_themes`，并为每个 theme 计算：
  - `recency_score`
  - `source_diversity_score`
  - `commercial_signal_score`
  - `human_preference_score`
  - `new_theme_score`
- theme 会自动附带 `source_suggestions` 和 `promotion_reason`
- 满足规则的 theme 会在 compaction 时晋升到 `long_memory.sub_sectors`
- planner 现在还支持结构化 feedback merge，支持以下 `feedback_type`：
  - `prefer_topic`
  - `pause_source`
  - `resume_source`
  - `boost_signal`
  - `deprioritize_pattern`
  - `promote_theme`
  - `reject_theme`
- 在生成 search plan 之前，planner 会先 merge 最新反馈，再生成：
  - `queries`
  - `channel_status`
  - `negative_filters`
  - `sensitive_keywords`
  - `human_preferences`
  - `source_suggestions`
- compaction 现在会输出并持久化：
  - `promoted_themes`
  - `decayed_themes`
  - `merged_topics`
  - `archived_preferences`
  - `source_policy_changes`
- planner 的反馈事件和压缩结果现在都可追踪、可审计

3. `Searching Agents`
- `brave_search_agent`
- `bocha_search_agent`
- 按 Planner 的 query 检索并做主体识别（LLM/规则）
- leads 汇总到一个主表
- lead 会同时保留 `raw_title`、`candidate_name`、`normalized_name` 和 `display_name`
- 对 `verified + company` 的对象，`display_name` 会优先等于 `normalized_name`
- 原始标题只做追溯和辅助显示，不再作为推荐主名称
- `MVP_MODE=true` 时，entity verification 额外支持 `likely_company`
  - `verified`：强证据公司
  - `likely_company`：看起来像公司，但证据还不完整
  - `pending_review`：不确定
  - `rejected`：明显不是公司
- 在 MVP 模式下，只要候选名存在、标题/摘要与 AI security / agent security / GenAI security 相关、且不是内容页/榜单页/guide/report/GitHub/单个泛词主体，就允许先进入 `likely_company`

4. `DD Agent`
- 对 leads 补充业务、团队、融资、进展、行业地位
- 输出五维结构化 DD：`business_profile`、`team_profile`、`funding_profile`、`traction_profile`、`market_position`
- 每个维度都带 `fields`、`evidence`、`missing_fields`、`confidence`
- `dd_overall` 里会记录 `dd_status`、`completeness_score`、`source_hits`、`missing_dimensions`
- 双源规则：`brave + bocha` 都有有效信息 -> 高置信；否则待复核
- 在同一轮 `full cycle` 里，search 阶段产出 lead 之后，会立刻处理这些尚未完成 DD 的 lead；历史未处理的 lead 仍会在全流程里批量补扫
- `MVP_MODE=true` 时，DD 对 `likely_company` 也会继续执行
  - 信息不完整时优先生成 `dd_partial`
  - 缺失字段会进入 `missing_fields`
  - 只有主体冲突特别严重时才会进入 `dd_waiting_human`

5. `Scoring Agent`
- 可解释的多维评分系统，直接消费 DD 的五维结构化结果
- 评分策略支持 human feedback 轻量更新：`like`、`dislike`、`skip`、`wrong_entity`、`prefer_sector`
- `ScoringPolicy` 会把权重、boost_rules、penalty_rules 和变更事件一起落库，后续评分会读取最新 policy
- 分项评分包含：
  - `business_score`
  - `team_score`
  - `funding_score`
  - `traction_score`
  - `market_score`
  - `thesis_fit_score`
  - `evidence_score`
- 先算 `raw_score`，再乘 `confidence_multiplier`，最后减去 `penalty_score` 得到 `final_score`
- 现在的最终公式是：
  - `final_score = raw_score * confidence_multiplier - penalty_score + boost_score`
- `recommendation_band` 分为：
  - `Strong Recommend`
  - `Recommend`
  - `Watchlist`
  - `Track Only`
  - `Reject`
- band 阈值现在固定为：
  - `Strong Recommend`: `>= 90`
  - `Recommend`: `82–89`
  - `Watchlist`: `75–81`
  - `Track Only`: `60–74`
  - `Reject`: `< 60`
- `thesis_fit_score` 已拆成 5 个可解释子项：
  - `long_memory_match`
  - `short_theme_match`
  - `keyword_match`
  - `commercial_signal_match`
  - `human_preference_match`
- formal recommendation 保留硬门槛：
  - `entity_type == company`
  - `verification_status == verified`
  - `source_hits >= 2`
  - `dd_status in ('dd_partial', 'dd_done')`
- 只有 `final_score >= 82` 且硬门槛通过的对象，才会通过 Interaction 主动推送给人类
- `75–81` 的对象会进入 `watchlist`，但不主动推送
- `recommendation_reason` 会解释为什么推荐或不推荐
- 推荐结果仍然会通过 Interaction 对外输出，也能继续消费完整分析接口
- `MVP_MODE=true` 时：
  - `likely_company` 也允许进入评分
  - `confidence_multiplier` 会略低于 `verified`
  - 推荐理由中会明确追加：`该公司为 likely_company，仍需人工复核`
  - 推荐结果会附带：
    - `verification_status = likely_company`
    - `confidence = medium`
    - `needs_human_review = true`
  - score-ready 规则会放宽为：
    - `verified` + `dd_status in ('dd_partial', 'dd_done')`
    - `likely_company` + `dd_status = 'dd_partial'`
  - `dd_waiting_human` 仍不会进入评分

6. `SQL Storage`
- 主数据和消息日志都落在 PostgreSQL 里
- 线索、评分、反馈、长短期记忆、OpenClaw 会话消息都可查
- 数据库连接会优先读取 `F:\AI_Agent\postgresSQL\.env`，也支持根目录 `.env`

## 1.1 代码目录（按功能分类）

为了便于阅读和维护，agent 代码已经按功能拆到这些目录里：

```text
app/agents/
  interaction/interaction_agent.py
  planner/planner_agent.py
  searching/searching_agents.py
  dd/dd_agent.py
  scoring/scoring_agent.py
```

现在默认策略文件也已经改成项目根目录绝对路径：

- `F:\AI_Agent\data\daily_strategy.txt`
- 如果你想换成别的策略文件，也可以通过环境变量覆盖 `daily_strategy_file`

## 2. 技术栈

- Python
- FastAPI
- APScheduler
- PostgreSQL / Neon
- Requests
- OpenAI SDK（可选，不填 key 会自动走规则版）

## 3. 快速启动

### 3.1 安装依赖

```bash
pip install -r requirements.txt
```

### 3.2 Windows 环境说明

如果你在这台 Windows 机器上跑实盘搜索（Brave / Bocha），当前环境下建议用**管理员 PowerShell**启动后端：

```powershell
cd F:\AI_Agent
python run_server.py
```

推荐使用下面这套“固定动作”，避免端口被旧进程占用：

```powershell
# 1) 用管理员身份打开 PowerShell
# 2) 进入项目目录
cd F:\AI_Agent

# 3) （可选）先确认 8000 是否已有旧进程
Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue | Format-Table -AutoSize

# 4) 启动服务
python run_server.py
```

看到如下日志表示启动成功：

- `Application startup complete.`
- `Uvicorn running on http://0.0.0.0:8000`

再新开一个 PowerShell（普通权限即可）做健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

原因：
- 这台机器的普通 PowerShell / 普通 Python 进程对外部 HTTPS 连接曾出现 `WinError 10013`
- 在管理员 PowerShell 中，`api.search.brave.com:443` 和 `api.bochaai.com:443` 已确认可连通

注意：
- 这是当前机器的**环境约束**，不是系统设计本身的必需条件
- 如果后续防火墙 / 安全策略已放通普通 Python 进程，就不必强制管理员启动

### 3.3 API 配额说明

当前 Brave / Bocha 都已经关闭 demo fallback，系统只会使用真实 API。

如果出现：
- `searched_items = 0`
- `new_leads = 0`

不一定是代码坏了，也可能是：
- Brave 月额度已用尽（例如 `402 USAGE_LIMIT_EXCEEDED`）
- Bocha 余额或套餐额度不足（例如 `403 not enough money or package quota`）

也就是说：
- “没有新数据” 可能是 API 配额问题
- 不是一定是搜索逻辑、DD 或评分逻辑出错

### 3.4 运行失败原因返回

系统现在会在 source 级和 run 级都返回结构化诊断，不再只说“没有新数据”。

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

支持的典型 `failure_code` 包括：
- `rate_limited`
- `quota_exhausted`
- `unauthorized`
- `forbidden`
- `dns_error`
- `connection_refused`
- `connect_timeout`
- `read_timeout`
- `ssl_error`
- `upstream_5xx`
- `bad_response`
- `parse_error`
- `network_blocked`
- `demo_mode`
- `unknown_error`

`run_full_cycle()` 最终会额外返回：
- `run_status`
- `new_data_fetched`
- `used_existing_pool_only`
- `failure_summary`
- `unavailable_sources`
- `source_status_by_channel`
- `action_suggestions`
- `stage_results`
- `recommendation_blockers`
- `lead_status_by_verification`
- `scoring_skip_reasons`

当本轮评分被跳过时，`scoring_skip_reasons` 会返回明确原因码，当前支持：
- `no_verified_company`
- `no_dd_ready_leads`
- `all_waiting_human`
- `all_rejected_by_gate`

### 3.5 MVP 模式

系统现在支持一个“先稳定产出候选”的 MVP fallback：

- 配置项：`MVP_MODE=true`
- 默认：当前代码默认开启

启用后，系统的目标从“只收最强验证公司”调整为：

1. 先把更像公司的候选稳定拉进来
2. 允许它们继续进入 DD
3. 允许它们进入 scoring
4. 推荐时明确标记仍需人工复核

这意味着：

- `verified` 仍然是高置信公司
- `likely_company` 是“看起来像公司，但证据还不完整”的候选
- `pending_review` / `rejected` 仍不会进入正式推荐

MVP 模式依然保留拒绝规则：

- 文章标题
- 榜单页
- GitHub 项目页
- `What is` / `guide` / `report`
- 单个泛词主体，如：
  - `GenAI`
  - `MCP`
  - `Firewall`
  - `Light`
  - `Closing`

`stage_results` 会按阶段拆开返回：
- `planner`
- `searching`
- `entity_verification`
- `dd`
- `scoring`
- `recommendation`

每个阶段都会记录：
- `stage_name`
- `status`
- `started_at`
- `ended_at`
- `duration_sec`
- `input_count`
- `output_count`
- `error_type`
- `error_message`

其中 `status` 可能是：
- `success`
- `partial_success`
- `failed`
- `timeout`
- `skipped`

这意味着：
- 就算本轮只跑到 searching 或 entity_verification，也会把已经完成的阶段、耗时和失败点一起返回
- 不会再等到整个 `run_cycle` 超过上层超时之后，只有一个“没新数据”或 HTTP 500

这样你可以直接区分：
- 真正拉到了新数据
- 只是复用了旧 lead 池重评分
- 某个 source 被限流
- 某个 source 配额耗尽
- 当前 Python 进程被网络/安全策略拦住

另外，`/interaction/chat` 和 `/interaction/command` 的 `run_cycle` 现在都加了结构化异常兜底：

- 如果内部 run_cycle 分支抛异常，不会再直接返回裸 `500 Internal Server Error`
- 会改为返回可读的 `internal_error` 结果，包含：
  - `run_status`
  - `failure_summary`
  - `action_suggestions`

这样即使某个旧进程或异常分支没有正常跑完，也能告诉你“为什么失败”，而不是只给一个 HTTP 500。

### 3.5 run_cycle 分阶段诊断与降级返回

`run_cycle` 现在按阶段收口，不再要求所有阶段都一次性完全跑完才返回。

目前内置了分阶段预算和批处理上限：
- `searching`: 70 秒预算
- `entity_verification`: 35 秒预算
- `dd`: 45 秒预算
- `scoring`: 20 秒预算
- 每轮 entity verification 最多处理 60 条
- 每轮 DD 最多处理 6 条
- 每轮 scoring 最多处理 60 条

如果出现下面这些情况：
- 外部 API 被限流
- API 配额耗尽
- 网络被拦
- entity verification / DD / scoring 来不及在本轮收口

系统会返回：
- 哪个阶段最先失败或超时
- 哪个阶段只部分完成
- searching 是否已经抓到新数据
- recommendation 为什么是 0

例如：
- `run_status = failed`
  - 说明本轮没有正常拿到新数据，通常是上游 source 全部失败
- `run_status = partial_success`
  - 说明本轮部分成功，有新数据或部分阶段完成，但还有阶段没完全收口
- `run_status = existing_pool_only`
  - 说明没有拿到新数据，但基于已有 lead 池做了 DD / scoring / recommendation 刷新

### 3.2 配置环境变量

```bash
copy .env.example .env
```

你至少需要检查这些字段：
- `DATABASE_URL`（PostgreSQL/Neon 连接串，已经可以放在 `F:\AI_Agent\postgresSQL\.env`）
- `DEMO_MODE=false`（实盘模式，不再生成 demo 假数据；如果没有 API key，搜索会返回空结果）
- 启动时会自动清理历史 demo 污染的 `example.com` 旧记录，避免推荐列表混入假数据
- `BRAVE_API_KEY`（接真实 Brave 时填写）
- `BOCHA_API_KEY`、`BOCHA_SEARCH_URL`（接真实 Bocha 时填写）
- `OPENAI_API_KEY`（可选）
- `OPENCLAW_WEBHOOK_SECRET`（可选，OpenClaw 访问我们的 webhook 时用）

### 3.2.1 Brave / Bocha API 更换位置（相对路径）

如果你要替换 API key 或接口地址，优先改环境变量文件，不要改业务代码。

1. 环境变量文件（推荐）
- `./.env`
- `./postgresSQL/.env`

说明：
- 系统会先读 `./.env`，再读 `./postgresSQL/.env`（后者同名字段会覆盖前者）
- 常改字段：
  - `BRAVE_API_KEY`
  - `BRAVE_SEARCH_URL`（默认 Brave 官方搜索接口）
  - `BOCHA_API_KEY`
  - `BOCHA_SEARCH_URL`（默认 `https://api.bochaai.com/v1/web-search`）
  - `DEMO_MODE=false`
  - `MVP_MODE=true`

2. 配置读取代码位置（排查用）
- `./app/core/config.py`

3. Brave / Bocha 客户端代码位置（排查用）
- `./app/clients/brave_client.py`
- `./app/clients/bocha_client.py`

如果只是换 key 或换 URL，通常不需要改上述 `.py` 文件；改 `.env` 后重启服务即可生效。

### 3.3 启动服务

```bash
python run_server.py
```

默认地址：`http://127.0.0.1:8000`

## 4. API 演示

### 4.1 健康检查

```bash
curl http://127.0.0.1:8000/health
```

### 4.2 对话式交互（推荐）

你可以像聊天一样发一句自然语言给交互 Agent：

```bash
curl -X POST http://127.0.0.1:8000/interaction/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"跑一轮"}'
```

更多例子：
- `{"message":"查看推荐"}`
- `{"message":"查看线索"}`
- `{"message":"查看 Capsule Security 的 DD"}`
- `{"message":"查看 Capsule Security 的完整分析"}`
- `{"message":"关闭 bocha"}`
- `{"message":"开启 brave"}`
- `{"message":"我不喜欢 lead 3，明显是大公司"}`

如果你更想像 IM 一样连续聊天，可以运行：

```bash
python chat_cli.py
```

### 4.3 OpenClaw 接入方式

OpenClaw 可以把人类消息转给这个后端的 `/openclaw/inbox`，然后把返回的 `reply` 再发回给人类。

如果你在 Telegram 里发的是执行型命令，比如 `run_cycle`、`查看推荐`、`查看线索`，OpenClaw 这边应该把它们当成对本机后端的动作请求，而不是普通聊天回复。

对应关系可以先记成这样：

- `run_cycle` / `跑一轮` -> `POST /interaction/command`
- `查看推荐` -> `GET /interaction/recommendations`
- `查看线索` -> `GET /interaction/leads`
- `查看某公司的 DD` / `lead 73 的 DD` -> 交互层直接按公司名或 lead id 查询 `dd_reports` 并总结
- `查看某公司的完整分析` / `lead 73 的完整分析` -> 交互层按公司名或 lead id 同时汇总 DD、评分和推荐理由
- 如果后端没启动，就明确返回“后端未连接”，不要假装已经执行

最简单的请求长这样：

```bash
curl -X POST http://127.0.0.1:8000/openclaw/inbox \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <你的OPENCLAW_WEBHOOK_SECRET>" \
  -d '{"message":"查看推荐","sessionKey":"main"}'
```

如果你想让 OpenClaw 轮询我们这边的待发消息，可以用：

```bash
curl http://127.0.0.1:8000/openclaw/outbox
```

如果 OpenClaw 已经发出消息了，还可以回传确认：

```bash
curl -X POST http://127.0.0.1:8000/openclaw/outbox/ack \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <你的OPENCLAW_WEBHOOK_SECRET>" \
  -d '{"eventIds":[1,2,3]}'
```

如果你重启电脑后想重新把整套链路拉起来，请看这份一步一步的恢复指南：

- [OpenClaw 重启恢复指南](F:/AI_Agent/openclaw_reboot_guide.md)

这份指南会告诉你重启后要先开 OpenClaw gateway，再开 `python run_server.py`，最后去 Telegram 发 `run_cycle`。

如果你想让 OpenClaw 记住你的名字、语言偏好和开场白，可以编辑工作区根目录下的：

- `IDENTITY.md`
- `SOUL.md`
- `USER.md`

OpenClaw 会把这些 bootstrap 文件注入到每次运行里，所以这比把欢迎语写死在某个聊天窗口更稳。  
其中 `IDENTITY.md` 负责项目上下文，`SOUL.md` 负责对话风格和开场白，`USER.md` 负责你的偏好。

如果你要让 `run_cycle` 真正变成“执行动作”而不是“文字聊天”，还要把执行约定写进 OpenClaw 实际读取的 workspace 里，让它知道：

- `run_cycle` 优先走 `POST /openclaw/inbox`
- 只有必要时才走 `POST /interaction/command`
- 执行成功后要返回 `run_at`、`searched_items`、`new_leads`、`dd_done`、`scored`、`recommended`

如果你在里面看到 `chmod` 后权限还是 `777`，那通常不是命令错了，而是 OpenClaw 装在 Windows 挂载盘上。  
这时最稳的做法是把 OpenClaw 迁到 WSL 自己的目录（比如 `~/.openclaw`）再启动。

### 4.4 手动跑一轮全流程

```bash
curl -X POST http://127.0.0.1:8000/interaction/command \
  -H "Content-Type: application/json" \
  -d '{"command":"run_cycle","data":{}}'
```

这条全流程现在的实际顺序是：

- 先搜索并识别出新 lead
- 立刻进入 DD 阶段，处理这些尚未完成 DD 的 lead
- 再统一做评分和推荐
- 这样新线索不会等到下一轮才进入分析

### 4.5 查看推荐结果

```bash
curl http://127.0.0.1:8000/interaction/recommendations
```

### 4.6 查看某家公司 DD

你可以直接在聊天里说：

```text
查看 Capsule Security 的 DD
```

或者：

```text
lead 73 的 DD
```

交互层会先找最匹配的公司主体，再把 `dd_reports` 里的这些字段汇总给你：

- `business_profile`
- `team_profile`
- `funding_profile`
- `traction_profile`
- `market_position`
- `business_summary`
- `team_summary`
- `funding_summary`
- `traction_summary`
- `industry_position`

回复里会按人话把五维 DD 展开成这些区块：

- 业务概况
- 团队背景
- 融资概况
- 业务进展
- 行业地位

如果这条 DD 只有单一来源，系统会明确提醒你：这更适合当作初步参考，不是最终投资结论。

`dd_overall` 里会同时告诉你：

- `dd_status`：`dd_done` / `dd_partial` / `dd_pending_review`
- `completeness_score`：五个维度各 20 分，总分 100
- `missing_dimensions`：还缺哪些维度
- `source_hits`：当前 DD 一共用了多少个不同来源

### 4.7 查看某家公司完整分析

你可以直接在聊天里说：

```text
查看 Capsule Security 的完整分析
```

或者：

```text
lead 73 的完整分析
```

完整分析会在 DD 的基础上，再补上：

- `business_score`
- `team_score`
- `funding_score`
- `traction_score`
- `market_score`
- `thesis_fit_score`
- `evidence_score`
- `raw_score`
- `confidence_multiplier`
- `penalty_score`
- `final_score`
- `recommendation_band`
- `recommendation_reason`
- 是否进入推荐池

回复里也会继续展开五维 DD，让你能直接看到：

- 业务概况
- 团队背景
- 融资概况
- 业务进展
- 行业地位

如果系统匹配到的主体其实是内容页、榜单页或非可用公司，它会直接提示你，不会假装成公司。

### 4.8 查看评分解释

新的评分结果会记录在 `scores` 表里，且支持完整追溯：

- 7 个分项分数都在 `0-5`
- `raw_score` 先汇总分项权重
- `confidence_multiplier` 会结合 `evidence`、`completeness`、`source_hits`
- `penalty_score` 会对噪音、大公司、主体冲突、证据不稳做减分
- `final_score` 是最终可解释分
- `recommendation_reason` 会保留最关键的正负向理由

如果你要看某个 lead 的评分解释，可以直接在交互层问：

```text
查看 lead 73 的完整分析
```

### 4.8 提交人类反馈

```bash
curl -X POST http://127.0.0.1:8000/interaction/feedback \
  -H "Content-Type: application/json" \
  -d '{"lead_id":1,"verdict":"dislike","content":"明显是大公司，不要再推荐","feedback_type":"lead_feedback"}'
```

### 4.9 记录公司名纠错记忆

你也可以直接在聊天里发这两种格式：

```text
10 Hot AI Security Startups To Know In 2025 不是公司名
```

或者：

```text
Protect AI for AI Agent Security 的公司名是 Protect AI
```

系统会把这类纠错记忆写入：

- `F:\AI_Agent\data\company_name_feedback.json`

后续在搜索和主体核验时，会优先参考这份文件，尽量把噪音字段过滤掉，并把标题里的公司名归一成标准名。

### 4.10 记录 DD 反馈与待确认问题

DD 也支持单独的 human-in-the-loop 反馈记忆。

你可以直接发：

```text
lead 73 重点补客户，不补估值
```

或者：

```text
全局规则：优先补团队，不补 valuation
```

这些反馈会写入 `dd_feedback_memory`，并在下一次 DD enrich 前自动读取，影响查询重点和字段提取优先级。

当 DD 遇到主体冲突、低置信度或关键字段缺失时，系统会自动生成待人工确认问题，并把 lead 标成：

- `dd_waiting_human`

你也可以查看问题列表：

```text
查看 lead 73 的待确认问题
```

或者直接回答某个问题：

```text
问题 12 的答案是公司名是 Protect AI
```

回答后，系统会把答案写回 DD 反馈记忆，并重新触发对应 lead 的 DD enrich。

### 4.11 关闭某个渠道

```bash
curl -X POST http://127.0.0.1:8000/interaction/command \
  -H "Content-Type: application/json" \
  -d '{"command":"update_channel","data":{"channel":"bocha","enabled":false}}'
```

### 4.12 反馈评分偏好

如果你想让后续评分更偏向某个赛道、某个 lead，或者明确告诉系统某类结果不值得看，可以直接发：

```text
我更关注 agent security 赛道
```

```text
lead 73 这个项目不值得看，太像内容页了
```

```text
leaderboard 类结果先跳过
```

这些反馈会更新 `scoring_policy`，后续评分会读取最新 policy，并在 `recommendation_reason` 里保留 policy 版本和规则命中记录，方便回溯。

### 4.13 DD 阶段 datetime 序列化修复说明

如果某轮 `run_cycle` 在 DD 阶段报：

```text
TypeError: Object of type datetime is not JSON serializable
```

当前代码已经在 `dd_reports` 落库入口统一做了递归 JSON 规范化：

- 所有 DD profile / overall / questions / evidence payload
- 遇到 `datetime`
- 会统一转成 ISO 8601 字符串

另外，DD 问题落库 `dd_questions` 现在也会对：

- `missing_fields`
- `details`

做同样的递归规范化，避免 `feedback_context`、`open_questions` 等嵌套结构里混入 `datetime` 时再次触发 JSON 序列化错误。

这次修复只收敛了 DD 阶段的序列化问题，没有改 planner / searching / scoring 主逻辑。

注意：

- 如果你本机 `8000` 端口上仍然跑着旧进程，必须重启 `run_server.py` 后，修复才会生效。
- 修复生效后，如果 `scoring = 0`，应该返回新的明确原因，例如：
  - `dd_waiting_human`
  - `verified company 不足`
  - `score-ready 对象为空`
  而不是再因为 DD 序列化直接崩掉。

## 5. 定时任务

系统启动后自动开启调度：
- 每 `FULL_CYCLE_MINUTES` 分钟：跑一轮全链路
- 每天 09:05：刷新短期策略
- 每周日 03:30：压缩长期记忆

## 6. 数据库表

PostgreSQL：

- 连接串来自 `DATABASE_URL`
- 主项目会自动创建这些表

核心表：
- `leads`
- `signals`
- `dd_reports`
- `scores`
- `feedback`
- `memory_long`
- `memory_short`
- `outbox`
- `conversation_messages`

`postgresSQL/` 目录里的小项目现在相当于数据库连接示例和环境变量兼容层。主项目会直接复用它的 `DATABASE_URL` 读取方式，所以你不用把两个项目拆成两套数据库配置。



