from __future__ import annotations

import asyncio
import os
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.storage import DEFAULT_RULES, SentinelStore


load_dotenv()


def _load_database_url() -> str:
    return os.getenv("DATABASE_URL", "sqlite:///./sentinel.db")


def _load_allowed_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "*")
    if raw.strip() == "*":
        return ["*"]
    return [item.strip() for item in raw.split(",") if item.strip()]


store = SentinelStore(_load_database_url())
store.bootstrap()

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.app:app", host="0.0.0.0", port=8000, reload=False)
