# PostgreSQL on Neon

这是 `F:\AI_Agent\postgresSQL` 里的独立子项目，用来连接 Neon 上的 PostgreSQL，并演示最基础的增删查改。

## 当前版本

- `v0.2`：兼容根目录 `.env` 和子项目 `.env`
- `v0.1`：初始化项目骨架
- 目标：先跑通 Python 连接 Neon，然后对 `notes` 表做 CRUD

## 你现在只需要准备什么

1. 在 Neon 创建一个免费 PostgreSQL 项目
2. 从 Neon Dashboard 复制连接串
3. 把连接串放进本目录的 `.env`
4. 如果你已经放在 `F:\AI_Agent\.env`，这个项目也能直接读取

## 你需要用的库

- `psycopg[binary]`
- `python-dotenv`

## 目录结构

- `README.md`：使用说明和版本说明
- `.env.example`：环境变量模板
- `requirements.txt`：Python 依赖
- `src/db.py`：数据库连接封装
- `src/main.py`：CRUD 命令行入口
- `logs/2026-04-21_v0.1_initial_setup.md`：本版本日志
- `logs/2026-04-21_v0.2_env_fallback.md`：本版本日志

## 快速开始

### 1. 安装依赖

```bash
cd F:\AI_Agent\postgresSQL
pip install -r requirements.txt
```

### 2. 创建 `.env`

把 `.env.example` 复制成 `.env`，然后填入 Neon 给你的连接串。

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require
```

如果你已经把它写进了 `F:\AI_Agent\.env`，也可以直接继续用，不需要重复复制。

### 3. 初始化表

```bash
python -m src.main init
```

### 4. 增删查改

```bash
python -m src.main create --title "hello" --body "first row"
python -m src.main list
python -m src.main update --id 1 --title "updated" --body "changed"
python -m src.main delete --id 1
```

## 关于连接串

- 如果你用的是 Neon 默认连接，通常可以直接用于这个项目
- 如果连接失败，再切换到 Neon 的 pooler connection string

## 版本日志

- `logs/2026-04-21_v0.1_initial_setup.md`
- `logs/2026-04-21_v0.2_env_fallback.md`

## 下一步

- 我拿到你的 `DATABASE_URL` 之后，可以继续帮你把这个项目真正连上 Neon，并验证一次 `init -> create -> list -> update -> delete`
