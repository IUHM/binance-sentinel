from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from backend.engine import (
    FundingObservation,
    OrderbookSnapshot,
    compute_funding_signal,
    detect_orderbook_wall,
    link_relevant_intel,
    render_alert_message,
)
from backend.integrations import BinanceFuturesClient, IntelCollector, TelegramNotifier
from backend.storage import SentinelStore

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger("binance-sentinel.worker")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@dataclass(slots=True)
class WorkerSettings:
    database_url: str
    rest_base_url: str
    ws_base_url: str
    symbol_refresh_seconds: int
    intel_poll_seconds: int
    funding_history_sample_seconds: int
    funding_history_retention_hours: int
    x_source_template: str | None
    x_handles: list[str]
    official_rss_urls: list[str]
    media_rss_urls: list[str]
    telegram_bot_token: str | None
    telegram_chat_id: str | None


def load_settings() -> WorkerSettings:
    def split_env(name: str) -> list[str]:
        return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]

    return WorkerSettings(
        database_url=os.getenv("DATABASE_URL", "sqlite:///./sentinel.db"),
        rest_base_url=os.getenv("BINANCE_REST_BASE_URL", "https://fapi.binance.com"),
        ws_base_url=os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com"),
        symbol_refresh_seconds=int(os.getenv("SYMBOL_REFRESH_SECONDS", "1800")),
        intel_poll_seconds=int(os.getenv("INTEL_POLL_SECONDS", "60")),
        funding_history_sample_seconds=int(os.getenv("FUNDING_HISTORY_SAMPLE_SECONDS", "120")),
        funding_history_retention_hours=int(os.getenv("FUNDING_HISTORY_RETENTION_HOURS", "72")),
        x_source_template=os.getenv("X_SOURCE_TEMPLATE") or None,
        x_handles=split_env("X_WHITELIST"),
        official_rss_urls=split_env("OFFICIAL_RSS_URLS"),
        media_rss_urls=split_env("MEDIA_RSS_URLS"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
    )


class SentinelWorker:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.store = SentinelStore(settings.database_url)
        self.store.bootstrap()
        self.client = BinanceFuturesClient(settings.rest_base_url, settings.ws_base_url)
        feed_configs = [
            {
                "name": "Binance Official",
                "source_type": "official",
                "source_score": 0.98,
                "kind": "binance_announcements",
            }
        ] + [
            {
                "name": f"Official-{index + 1}",
                "source_type": "official",
                "source_score": 0.95,
                "kind": "rss",
                "url": url,
            }
            for index, url in enumerate(settings.official_rss_urls)
        ] + [
            {
                "name": f"Media-{index + 1}",
                "source_type": "media",
                "source_score": 0.72,
                "kind": "rss",
                "url": url,
            }
            for index, url in enumerate(settings.media_rss_urls)
        ]
        self.intel_collector = IntelCollector(
            feed_configs=feed_configs,
            x_source_template=settings.x_source_template,
            x_handles=settings.x_handles,
        )
        self.notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
        self.symbol_meta: dict[str, dict[str, Any]] = {}
        self.symbols: list[str] = []
        self.funding_history: dict[str, deque[FundingObservation]] = defaultdict(lambda: deque(maxlen=96))
        self.depth_history: dict[str, deque[OrderbookSnapshot]] = defaultdict(lambda: deque(maxlen=18))
        self.latest_open_interest: dict[str, float] = {}
        self.open_interest_updated: dict[str, datetime] = {}
        self.liquidation_pressure: dict[str, float] = defaultdict(float)
        self.pending_wall_tasks: dict[str, asyncio.Task[None]] = {}
        self.intel_cache: deque[dict[str, Any]] = deque(maxlen=300)
        self.last_snapshot_flush: dict[str, datetime] = {}
        self.last_funding_history_flush_at: datetime | None = None
        self.last_market_event_at: datetime | None = None

    async def bootstrap(self) -> None:
        await self.refresh_symbols()
        if not self.intel_cache:
            for intel in self.store.list_intel(limit=60):
                self.intel_cache.appendleft(intel)
        self._update_heartbeat(status="bootstrapped")

    async def refresh_symbols(self) -> None:
        exchange_info = await self.client.fetch_exchange_info()
        symbols = [
            item
            for item in exchange_info.get("symbols", [])
            if item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
            and item.get("status") == "TRADING"
            and item.get("marginAsset") == "USDT"
        ]
        self.symbol_meta = {item["symbol"]: item for item in symbols}
        self.symbols = sorted(self.symbol_meta)
        self.store.upsert_symbols(symbols)
        LOGGER.info("Tracking %s USDT perpetual symbols", len(self.symbols))

    async def run(self) -> None:
        while True:
            try:
                await self.bootstrap()
                break
            except Exception as exc:
                LOGGER.exception("Bootstrap failed, retrying: %s", exc)
                self._update_heartbeat(status="reconnecting")
                await asyncio.sleep(10)
        await asyncio.gather(
            self.market_loop(),
            self.symbol_refresh_loop(),
            self.intel_loop(),
            self.heartbeat_loop(),
        )

    async def symbol_refresh_loop(self) -> None:
        while True:
            try:
                await self.refresh_symbols()
            except Exception as exc:
                LOGGER.exception("Symbol refresh failed: %s", exc)
            await asyncio.sleep(self.settings.symbol_refresh_seconds)

    async def intel_loop(self) -> None:
        while True:
            try:
                collected = await self.intel_collector.collect(self.symbol_meta)
                inserted = self.store.save_intel_items(collected)
                for item in inserted:
                    self.intel_cache.appendleft(item)
                    await self._maybe_emit_intel_alert(item)
            except Exception as exc:
                LOGGER.exception("Intel loop failed: %s", exc)
            await asyncio.sleep(self.settings.intel_poll_seconds)

    async def heartbeat_loop(self) -> None:
        while True:
            self._update_heartbeat(status="live")
            await asyncio.sleep(5)

    def _update_heartbeat(self, status: str) -> None:
        self.store.set_state(
            "worker_heartbeat",
            {
                "status": status,
                "updated_at": utcnow().isoformat(),
                "tracked_symbols": len(self.symbols),
                "pending_confirmations": len(self.pending_wall_tasks),
                "last_market_event_at": self.last_market_event_at.isoformat() if self.last_market_event_at else None,
            },
        )

    async def market_loop(self) -> None:
        while True:
            try:
                if not self.symbols:
                    await asyncio.sleep(5)
                    continue
                async for kind, payload in self.client.stream_market(self.symbols):
                    self.last_market_event_at = utcnow()
                    if kind == "mark_price_batch":
                        await self.handle_mark_price_batch(payload)
                    elif kind == "force_order":
                        self.handle_force_order(payload)
                    elif kind == "partial_depth":
                        self.handle_partial_depth(payload)
            except Exception as exc:
                LOGGER.exception("Market loop crashed, reconnecting: %s", exc)
                self._update_heartbeat(status="reconnecting")
                await asyncio.sleep(3)

    async def handle_mark_price_batch(self, payload: list[dict[str, Any]]) -> None:
        rules = self.store.get_rules()
        batch: list[FundingObservation] = []
        for item in payload:
            symbol = item.get("s")
            if symbol not in self.symbol_meta:
                continue
            observation = FundingObservation(
                symbol=symbol,
                funding_rate=float(item.get("r", 0.0)),
                mark_price=float(item.get("p", 0.0)),
                observed_at=utcnow(),
                open_interest_value=self.latest_open_interest.get(symbol, 0.0),
                liquidation_value=self.liquidation_pressure.get(symbol, 0.0),
            )
            self.funding_history[symbol].append(observation)
            batch.append(observation)

        if not batch:
            return

        candidates: list[dict[str, Any]] = []
        signal_by_symbol: dict[str, dict[str, Any]] = {}
        for observation in batch:
            signal = compute_funding_signal(list(self.funding_history[observation.symbol]), batch, rules)
            if signal is None:
                continue
            if signal["score"] >= rules["funding_prealert_score"]:
                await self.refresh_open_interest(observation.symbol)
                latest = self.funding_history[observation.symbol][-1]
                latest.open_interest_value = self.latest_open_interest.get(observation.symbol, 0.0)
                signal = compute_funding_signal(list(self.funding_history[observation.symbol]), batch, rules) or signal
            candidates.append(signal)
            signal_by_symbol[observation.symbol] = signal

        top_rows = sorted(candidates, key=lambda item: item["score"], reverse=True)[: rules["dashboard_flush_top_n"]]
        self.store.upsert_snapshots(
            [
                {
                    "symbol": item["symbol"],
                    "funding_rate": item["funding_rate"],
                    "funding_zscore": item["funding_zscore"],
                    "funding_slope": item["funding_slope"],
                    "funding_percentile": item["funding_percentile"],
                    "funding_score": item["score"],
                    "mark_price": item["mark_price"],
                    "open_interest_value": item["open_interest_value"],
                    "liquidation_pressure": item["liquidation_pressure"],
                    "metrics_json": json_dumps(item),
                }
                for item in top_rows
            ]
        )
        if (
            self.last_funding_history_flush_at is None
            or (utcnow() - self.last_funding_history_flush_at).total_seconds()
            >= self.settings.funding_history_sample_seconds
        ):
            self.store.append_funding_history(
                [
                    {
                        "symbol": observation.symbol,
                        "funding_rate": observation.funding_rate,
                        "funding_score": signal_by_symbol.get(observation.symbol, {}).get("score", 0.0),
                        "mark_price": observation.mark_price,
                        "open_interest_value": observation.open_interest_value,
                        "liquidation_pressure": observation.liquidation_value,
                        "observed_at": observation.observed_at,
                    }
                    for observation in batch
                ],
                retention_hours=self.settings.funding_history_retention_hours,
            )
            self.last_funding_history_flush_at = utcnow()

        for signal in candidates:
            if signal["score"] < rules["funding_alert_score"]:
                continue
            await self.emit_alert("funding", signal["symbol"], signal)

        for symbol in list(self.liquidation_pressure):
            self.liquidation_pressure[symbol] *= 0.82

    async def refresh_open_interest(self, symbol: str) -> None:
        previous = self.open_interest_updated.get(symbol)
        if previous and (utcnow() - previous).total_seconds() < 300:
            return
        snapshot = await self.client.fetch_open_interest_snapshot(symbol)
        self.latest_open_interest[symbol] = snapshot["open_interest_value"]
        self.open_interest_updated[symbol] = utcnow()

    def handle_force_order(self, payload: dict[str, Any]) -> None:
        order = payload.get("o", {})
        symbol = order.get("s")
        if symbol not in self.symbol_meta:
            return
        notional = float(order.get("ap", 0.0)) * float(order.get("z", 0.0))
        self.liquidation_pressure[symbol] = self.liquidation_pressure.get(symbol, 0.0) * 0.7 + notional

    def handle_partial_depth(self, payload: dict[str, Any]) -> None:
        symbol = payload.get("s")
        if symbol not in self.symbol_meta:
            return
        bids = [(float(price), float(quantity)) for price, quantity in payload.get("b", []) if float(quantity) > 0]
        asks = [(float(price), float(quantity)) for price, quantity in payload.get("a", []) if float(quantity) > 0]
        snapshot = OrderbookSnapshot(symbol=symbol, observed_at=utcnow(), bids=bids, asks=asks)
        history = list(self.depth_history[symbol])
        rules = self.store.get_rules()
        signal = detect_orderbook_wall(snapshot, history, rules)
        self.depth_history[symbol].append(snapshot)

        if signal is None:
            return

        should_flush = False
        last_flush = self.last_snapshot_flush.get(symbol)
        if last_flush is None or (utcnow() - last_flush).total_seconds() >= 15:
            should_flush = True
        if signal["score"] >= rules["wall_candidate_score"]:
            should_flush = True
        if should_flush:
            self.last_snapshot_flush[symbol] = utcnow()
            self.store.upsert_snapshots(
                [
                    {
                        "symbol": signal["symbol"],
                        "wall_side": signal["wall_side"],
                        "wall_price": signal["wall_price"],
                        "wall_distance_bps": signal["wall_distance_bps"],
                        "wall_notional": signal["wall_notional"],
                        "wall_persistence": signal["wall_persistence"],
                        "wall_score": signal["score"],
                        "metrics_json": json_dumps(signal),
                    }
                ]
            )

        if signal["score"] < rules["wall_candidate_score"] or symbol in self.pending_wall_tasks:
            return

        task = asyncio.create_task(self.confirm_wall(signal))
        self.pending_wall_tasks[symbol] = task
        task.add_done_callback(lambda _: self.pending_wall_tasks.pop(symbol, None))

    async def confirm_wall(self, signal: dict[str, Any]) -> None:
        rules = self.store.get_rules()
        confirmed = await self.client.confirm_wall(
            symbol=signal["symbol"],
            side=signal["wall_side"],
            reference_price=signal["wall_price"],
            min_notional=rules["wall_min_notional_usd"],
            confirm_seconds=rules["depth_confirm_seconds"],
            distance_limit_bps=rules["wall_distance_limit_bps"],
        )
        if confirmed is None:
            return
        merged = dict(signal)
        merged.update(confirmed)
        self.store.upsert_snapshots(
            [
                {
                    "symbol": merged["symbol"],
                    "wall_side": merged["wall_side"],
                    "wall_price": merged["wall_price"],
                    "wall_distance_bps": merged["wall_distance_bps"],
                    "wall_notional": merged["wall_notional"],
                    "wall_persistence": merged["wall_persistence"],
                    "wall_score": merged["score"],
                    "metrics_json": json_dumps(merged),
                }
            ]
        )
        if merged["score"] >= rules["wall_alert_score"]:
            await self.emit_alert("orderbook", merged["symbol"], merged)

    async def emit_alert(self, category: str, symbol: str, metrics: dict[str, Any]) -> None:
        rules = self.store.get_rules()
        related_intel = link_relevant_intel(symbol, utcnow(), list(self.intel_cache))
        message = render_alert_message(category, symbol, metrics, related_intel)
        wall_side = metrics.get("wall_side", "unknown")
        wall_side_label = "买墙" if wall_side == "bid" else "卖墙"
        headline = (
            f"{symbol} 资金费率异常"
            if category == "funding"
            else f"{symbol} 出现大额{wall_side_label}"
        )
        cooldown = rules["funding_cooldown_minutes"] if category == "funding" else rules["wall_cooldown_minutes"]
        alert = {
            "dedupe_key": f"{category}:{symbol}:{metrics.get('wall_side', '')}",
            "symbol": symbol,
            "category": category,
            "severity": metrics.get("severity", "medium"),
            "headline": headline,
            "message": message,
            "score": metrics.get("score", 0.0),
            "triggered_at": utcnow(),
            "metrics": metrics,
            "related_intel": related_intel,
        }
        saved = self.store.save_alert(alert, cooldown_minutes=cooldown)
        if saved is not None:
            await self.notifier.send(saved)

    async def _maybe_emit_intel_alert(self, item: dict[str, Any]) -> None:
        if item["source_type"] not in {"official", "media"} or not item["symbols"]:
            return
        if item["source_score"] < 0.85:
            return
        for symbol in item["symbols"][:3]:
            alert = {
                "dedupe_key": f"intel:{item['fingerprint']}:{symbol}",
                "symbol": symbol,
                "category": "intel",
                "severity": "medium" if item["source_type"] == "media" else "high",
                "headline": f"{symbol} 出现最新催化情报",
                "message": item["title"],
                "score": item["source_score"] * 100,
                "triggered_at": utcnow(),
                "metrics": {"summary": item["summary"], "source_name": item["source_name"]},
                "related_intel": [item],
            }
            saved = self.store.save_alert(alert, cooldown_minutes=60)
            if saved is not None:
                await self.notifier.send(saved)


async def main() -> None:
    worker = SentinelWorker(load_settings())
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
