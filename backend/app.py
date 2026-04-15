from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from backend.storage import DEFAULT_RULES, SentinelStore


load_dotenv()
LOGGER = logging.getLogger("binance-sentinel.app")


def _load_database_url() -> str:
    return os.getenv("DATABASE_URL", "sqlite:///./sentinel.db")


def _load_allowed_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "*")
    if raw.strip() == "*":
        return ["*"]
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_frontend_dist_dir() -> Path | None:
    candidates: list[Path] = []
    custom = os.getenv("FRONTEND_DIST_DIR")
    if custom:
        candidates.append(Path(custom))

    project_root = Path(__file__).resolve().parents[1]
    candidates.append(project_root / "frontend" / "dist")
    candidates.append(Path("/app/frontend_dist"))

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists() and (candidate / "index.html").is_file():
            return candidate
    return None


store = SentinelStore(_load_database_url())
store.bootstrap()
frontend_dist_dir = _resolve_frontend_dist_dir()
embedded_worker_task: asyncio.Task[None] | None = None

app = FastAPI(title="币安永续异动哨兵 API", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_load_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RulesPayload(BaseModel):
    funding_abs_rate_floor: float = Field(default=DEFAULT_RULES["funding_abs_rate_floor"], ge=0)
    funding_prealert_score: float = Field(default=DEFAULT_RULES["funding_prealert_score"], ge=0)
    funding_alert_score: float = Field(default=DEFAULT_RULES["funding_alert_score"], ge=0)
    funding_cooldown_minutes: int = Field(default=DEFAULT_RULES["funding_cooldown_minutes"], ge=1)
    wall_min_notional_usd: float = Field(default=DEFAULT_RULES["wall_min_notional_usd"], ge=0)
    wall_distance_limit_bps: float = Field(default=DEFAULT_RULES["wall_distance_limit_bps"], ge=1)
    wall_candidate_score: float = Field(default=DEFAULT_RULES["wall_candidate_score"], ge=0)
    wall_alert_score: float = Field(default=DEFAULT_RULES["wall_alert_score"], ge=0)
    wall_cooldown_minutes: int = Field(default=DEFAULT_RULES["wall_cooldown_minutes"], ge=1)
    depth_confirm_seconds: float = Field(default=DEFAULT_RULES["depth_confirm_seconds"], ge=0.5)
    dashboard_flush_top_n: int = Field(default=DEFAULT_RULES["dashboard_flush_top_n"], ge=5, le=100)


async def _run_embedded_worker() -> None:
    from backend.worker import SentinelWorker, load_settings

    worker = SentinelWorker(load_settings())
    try:
        await worker.run()
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.exception("Embedded worker crashed")


@app.on_event("startup")
async def startup_embedded_worker() -> None:
    global embedded_worker_task

    if not _env_flag("ENABLE_EMBEDDED_WORKER") or embedded_worker_task is not None:
        return
    embedded_worker_task = asyncio.create_task(_run_embedded_worker())


@app.on_event("shutdown")
async def shutdown_embedded_worker() -> None:
    global embedded_worker_task

    if embedded_worker_task is None:
        return
    embedded_worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await embedded_worker_task
    embedded_worker_task = None


@app.get("/health")
def health() -> dict[str, Any]:
    overview = store.build_overview()
    return {
        "status": "ok",
        "generated_at": overview["generated_at"],
        "worker": overview["health"]["worker"],
    }


@app.get("/api/v1/symbols")
def list_symbols() -> list[dict[str, Any]]:
    return store.list_symbols()


@app.get("/api/v1/dashboard/overview")
def dashboard_overview() -> dict[str, Any]:
    return store.build_overview()


@app.get("/api/v1/funding/history")
def funding_history(
    symbol: str | None = None,
    limit: int = Query(default=120, ge=30, le=720),
) -> dict[str, Any]:
    resolved_symbol = symbol or store.resolve_chart_symbol()
    if not resolved_symbol:
        return {"symbol": None, "points": []}
    return {
        "symbol": resolved_symbol,
        "points": store.list_funding_history(symbol=resolved_symbol, limit=limit),
    }


@app.get("/api/v1/alerts")
def list_alerts(
    limit: int = Query(default=50, ge=1, le=200),
    symbol: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    return store.list_alerts(limit=limit, symbol=symbol, category=category)


@app.get("/api/v1/alerts/{alert_id}")
def get_alert(alert_id: int) -> dict[str, Any]:
    alert = store.get_alert(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.get("/api/v1/intel/feed")
def intel_feed(
    limit: int = Query(default=40, ge=1, le=200),
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    return store.list_intel(limit=limit, symbol=symbol)


@app.get("/api/v1/rules")
def get_rules() -> dict[str, Any]:
    return store.get_rules()


@app.put("/api/v1/rules")
def update_rules(payload: RulesPayload) -> dict[str, Any]:
    return store.update_rules(payload.model_dump())


@app.websocket("/ws/dashboard")
async def dashboard_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(store.build_overview())
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return
    except RuntimeError:
        return


if frontend_dist_dir and (frontend_dist_dir / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=frontend_dist_dir / "assets"), name="frontend-assets")


if frontend_dist_dir:

    @app.get("/", include_in_schema=False)
    async def frontend_index() -> FileResponse:
        return FileResponse(frontend_dist_dir / "index.html")


    @app.get("/{frontend_path:path}", include_in_schema=False)
    async def frontend_catchall(frontend_path: str) -> FileResponse:
        if frontend_path.startswith(("api/", "ws/", "health")):
            raise HTTPException(status_code=404, detail="Not found")
        candidate = frontend_dist_dir / frontend_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(frontend_dist_dir / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.app:app", host="0.0.0.0", port=8000, reload=False)
