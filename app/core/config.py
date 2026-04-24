from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[2]

# Support both locations:
# - F:\AI_Agent\.env
# - F:\AI_Agent\postgresSQL\.env
# The subproject file can override the root file when both define the same key.
load_dotenv(ROOT_DIR / ".env", override=False)
load_dotenv(ROOT_DIR / "postgresSQL" / ".env", override=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "ai-invest-agent"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    timezone: str = "Asia/Shanghai"
    database_url: str = ""

    full_cycle_minutes: int = 30
    recommend_score_threshold: float = 75.0
    daily_strategy_file: str = str(ROOT_DIR / "data" / "daily_strategy.txt")

    webhook_url: str = ""
    webhook_timeout_seconds: int = 10

    openclaw_webhook_secret: str = ""
    openclaw_webhook_path: str = "/openclaw/inbox"
    openclaw_outbox_path: str = "/openclaw/outbox"

    openai_api_key: str = ""
    openai_base_url: str = ""
    openai_model: str = "gpt-4o-mini"

    brave_api_key: str = ""
    brave_search_url: str = "https://api.search.brave.com/res/v1/web/search"

    bocha_api_key: str = ""
    bocha_search_url: str = "https://api.bochaai.com/v1/web-search"

    demo_mode: bool = False
    mvp_mode: bool = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
