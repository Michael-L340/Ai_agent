# OpenClaw 重启恢复指南

这份文档是给“电脑重启以后，怎么把整套工作流重新拉起来”准备的。  
你只要按顺序做，就能把 **OpenClaw + Telegram + 这个 AI Agent 后端** 重新恢复。

## 先记住 4 件事

- OpenClaw 负责把 Telegram 消息送到后端，不是用来直接读你工作区文件的。
- 后端启动文件是 `run_server.py`，不是 `run.py`。
- 正确的启动顺序是：先 OpenClaw gateway，再后端，再 Telegram 测试消息。
- 如果数据库暂时连不上，后端会自动回退到本地 SQLite，不会直接卡死。

## 1. 重启前你要知道什么

### 1.1 项目目录
- 项目目录：`F:\AI_Agent`

### 1.2 OpenClaw 配置文件
- Windows 路径：`\\wsl$\Ubuntu\root\.openclaw\openclaw.json`
- WSL 路径：`/root/.openclaw/openclaw.json`

### 1.3 Telegram bot token
- 这是你在 `@BotFather` 创建 bot 后拿到的 token
- 如果这个 token 曾经在截图里暴露过，建议你重新生成一个新的
- token 不要公开发给别人，也不要随便贴到群里

### 1.4 后端入口
- 启动命令：`python run_server.py`
- 默认地址：`http://127.0.0.1:8000`

## 2. 重启后的正确顺序

### 第 1 步：打开 PowerShell

建议你先开两个 PowerShell 窗口：

- 窗口 A：专门跑 OpenClaw gateway
- 窗口 B：专门跑后端 `run_server.py`

这样最不容易乱。

### 第 2 步：启动 OpenClaw gateway

如果你的 OpenClaw 还是装在 WSL 里，先用这一条：

```powershell
wsl -d Ubuntu -e bash -lc "openclaw gateway --port 18789"
```

如果以后你已经把 OpenClaw 改成 Windows 原生安装，并且 `openclaw` 已经在 PATH 里，就直接用：

```powershell
openclaw gateway --port 18789
```

如果你不确定是否已经成功启动，不要马上关掉这个窗口。  
等它继续输出日志，看到类似下面的内容才算比较稳：

- `gateway ready`
- `canvas host mounted`
- `starting channels and sidecars`

### 第 3 步：检查 gateway 是否正常

再开一个新的 PowerShell 窗口，检查 gateway 状态：

```powershell
wsl -d Ubuntu -e bash -lc "openclaw gateway status"
wsl -d Ubuntu -e bash -lc "openclaw gateway health --url ws://127.0.0.1:18789"
wsl -d Ubuntu -e bash -lc "openclaw channels status --probe"
```

如果你已经切成 Windows 原生安装，就把前面的 `wsl -d Ubuntu -e bash -lc` 去掉。

### 第 4 步：启动 AI Agent 后端

再开一个新的 PowerShell 窗口，进入项目目录：

```powershell
cd F:\AI_Agent
python run_server.py
```

如果 `python` 不认识，可以试：

```powershell
py run_server.py
```

后端启动后，再检查健康状态：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

如果返回结果里有 `status = ok`，说明后端已经正常了。

### 第 5 步：去 Telegram 发第一条测试消息

打开 Telegram，直接给 bot 发：

```text
run_cycle
```

或者中文：

```text
跑一轮
```

这一步会让系统完整走一遍：

- Planner 生成搜索计划
- Searching agents 检索
- DD agent 补充信息
- Scoring agent 打分
- Interaction agent 回消息

### 第 6 步：查看推荐和线索

你可以继续发：

```text
show recommendations
show leads
```

或者中文：

```text
查看推荐
查看线索
```

### 第 7 步：给系统反馈

比如你觉得某条不值得看，可以直接发：

```text
我不喜欢 lead 1，因为明显是大公司
```

或者：

```text
我喜欢 lead 2，因为有 B2B 付费信号
```

这些反馈会进入 Planner 的长期记忆，下一轮会参考你的偏好。

### 第 8 步：控制渠道开关

如果你想临时关闭某个检索渠道，可以发：

```text
关闭 bocha
关闭 brave
```

如果你想恢复它们，可以发：

```text
开启 bocha
开启 brave
```

### 第 9 步：刷新策略或压缩记忆

如果你想让系统重新整理短期策略，可以发：

```text
刷新策略
```

如果你想让系统整理长期记忆，可以发：

```text
压缩记忆
```

## 3. 如果 Telegram 没反应，按这个顺序排查

### 情况 A：先看 OpenClaw gateway 还在不在

如果 gateway 窗口已经报错或者自己关掉了，先重启 gateway。

### 情况 B：检查 Telegram 配置还在不在

打开这个文件：

- `\\wsl$\Ubuntu\root\.openclaw\openclaw.json`

确认里面还有：

- `channels.telegram`
- `enabled: true`
- `botToken`

### 情况 C：重新看配对状态

如果你怀疑 Telegram 需要重新配对，可以在 OpenClaw 终端里运行：

```powershell
wsl -d Ubuntu -e bash -lc "openclaw pairing list telegram"
```

如果看到配对码，再执行：

```powershell
wsl -d Ubuntu -e bash -lc "openclaw pairing approve telegram <CODE>"
```

一般来说，只要 token 和配置没有变，不需要每次重启都重新配对。  
但如果你刚换过 token、改过配置，或者 OpenClaw 提示需要重新配对，就按上面来。

### 情况 D：没有新消息，就不会有新配对请求

如果 `pairing list telegram` 显示：

```text
No pending telegram pairing requests.
```

先回 Telegram 再发一条新消息，然后再查一次。  
旧消息通常不会重新生成请求。

## 4. 常见问题

### 问题 1：`openclaw` 找不到

说明当前环境里没有把它放进 PATH。  
先用你已经跑通的那条完整命令，不要卡在这一步。

### 问题 2：`chmod` 以后还是显示 `777`

这通常说明 OpenClaw 安装在 Windows 挂载盘上，比如 `/mnt/f/openclaw`。  
这不是你命令写错了，而是 WSL 对 Windows 挂载盘的权限改动不会真正生效。

如果你遇到插件加载被拦，最稳的办法是把 OpenClaw 迁到 WSL 自己的文件系统里，比如 `/root/.openclaw`。

### 问题 3：后端起不来

先看 `python run_server.py` 的报错。

如果数据库暂时连不上，当前代码会自动回退到本地 SQLite，文件位置是：

- `F:\AI_Agent\data\agent_local.db`

### 问题 4：想跳过 Telegram，直接测后端

你可以直接发 HTTP 请求测试：

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/interaction/command `
  -ContentType "application/json" `
  -Body '{"command":"run_cycle","data":{}}'
```

或者直接看推荐结果：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/interaction/recommendations
```

## 5. 一句话版流程

如果你只想记最短版本，就记这 4 步：

1. 启动 OpenClaw gateway
2. 启动 `python run_server.py`
3. 用 `Invoke-RestMethod http://127.0.0.1:8000/health` 确认后端正常
4. 去 Telegram 发 `run_cycle`

## 6. 这份文档的作用

把它当成“电脑重启后的恢复清单”就行。  
以后你每次重新开机，只要从上到下照着做，就能把工作流恢复回来。
