from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import router
from .service import SERVICE

@asynccontextmanager
async def lifespan(_app: FastAPI):
    SERVICE.bootstrap()
    yield


app = FastAPI(title="fau_BOT Core", version="1.0.0", lifespan=lifespan)
app.include_router(router, prefix="")


@app.get("/")
def health_root():
    return {"service": "fau_bot_core", "ok": True}
