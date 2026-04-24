from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from app.core.runtime import AgentRuntime
from app.core.scheduler import build_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    runtime = AgentRuntime(settings)
    runtime.init()

    scheduler = build_scheduler(runtime, settings)
    scheduler.start()

    app.state.runtime = runtime
    app.state.scheduler = scheduler
    yield

    scheduler.shutdown(wait=False)


app = FastAPI(title="AI Invest Agent (Brave + Bocha)", lifespan=lifespan)
app.include_router(router)

