from __future__ import annotations

from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.core.config import Settings
from app.core.runtime import AgentRuntime


def build_scheduler(runtime: AgentRuntime, settings: Settings) -> BackgroundScheduler:
    try:
        tz = ZoneInfo(settings.timezone)
    except Exception:
        tz = ZoneInfo("UTC")

    scheduler = BackgroundScheduler(timezone=tz)

    scheduler.add_job(
        func=runtime.run_full_cycle,
        trigger=IntervalTrigger(minutes=settings.full_cycle_minutes, timezone=tz),
        id="full_cycle",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=runtime.refresh_strategy,
        trigger=CronTrigger(hour=9, minute=5, timezone=tz),
        id="refresh_strategy_daily",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=runtime.compress_memory,
        trigger=CronTrigger(day_of_week="sun", hour=3, minute=30, timezone=tz),
        id="compress_memory_weekly",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    return scheduler

