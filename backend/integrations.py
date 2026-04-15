from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterator
from xml.etree import ElementTree

import httpx
import websockets

LOGGER = logging.getLogger("binance-sentinel.integrations")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(raw: str | None) -> datetime:
    if not raw:
        return utcnow()
    for parser in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
    ):
        try:
            return datetime.strptime(raw, parser)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return utcnow()


class BinanceFuturesClient:
    def __init__(self, rest_base_url: str, ws_base_url: str, timeout_seconds: float = 10.0) -> None:
        self.rest_base_url = rest_base_url.rstrip("/")
        self.ws_base_url = ws_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def fetch_exchange_info(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(f"{self.rest_base_url}/fapi/v1/exchangeInfo")
            response.raise_for_status()
            return response.json()

    async def fetch_depth_snapshot(self, symbol: str, limit: int = 1000) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(
                f"{self.rest_base_url}/fapi/v1/depth",
                params={"symbol": symbol, "limit": limit},
            )
            response.raise_for_status()
            return response.json()

    async def fetch_open_interest_snapshot(self, symbol: str) -> dict[str, float]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(
                f"{self.rest_base_url}/futures/data/openInterestHist",
                params={"symbol": symbol, "period": "5m", "limit": 2},
            )
            response.raise_for_status()
            payload = response.json()
        if not payload:
            return {"open_interest_value": 0.0, "open_interest_change_ratio": 0.0}
        latest = payload[-1]
        previous_value = float(payload[-2]["sumOpenInterestValue"]) if len(payload) > 1 else 0.0
        latest_value = float(latest["sumOpenInterestValue"])
        change_ratio = (latest_value - previous_value) / previous_value if previous_value else 0.0
        return {
            "open_interest_value": latest_value,
            "open_interest_change_ratio": change_ratio,
        }

    async def stream_market(self, symbols: list[str]) -> AsyncIterator[tuple[str, Any]]:
        stream_uri = f"{self.ws_base_url}/ws"
        subscriptions = ["!markPrice@arr@1s", "!forceOrder@arr"] + [
            f"{symbol.lower()}@depth20@500ms" for symbol in symbols
        ]
        async with websockets.connect(
            stream_uri,
            max_size=None,
            ping_interval=120,
            ping_timeout=120,
        ) as websocket:
            for index in range(0, len(subscriptions), 180):
                request_id = index // 180 + 1
                chunk = subscriptions[index : index + 180]
                await websocket.send(
                    json.dumps(
                        {
                            "method": "SUBSCRIBE",
                            "params": chunk,
                            "id": request_id,
                        }
                    )
                )
                await asyncio.sleep(0.2)
            async for raw in websocket:
                payload = json.loads(raw)
                if isinstance(payload, dict) and payload.get("result") is None and "id" in payload:
                    continue
                if isinstance(payload, list):
                    yield ("mark_price_batch", payload)
                    continue
                if payload.get("e") == "forceOrder":
                    yield ("force_order", payload)
                    continue
                if payload.get("e") == "depthUpdate":
                    yield ("partial_depth", payload)

    async def confirm_wall(
        self,
        symbol: str,
        side: str,
        reference_price: float,
        min_notional: float,
        confirm_seconds: float,
        distance_limit_bps: float,
    ) -> dict[str, Any] | None:
        stream_uri = f"{self.ws_base_url}/ws/{symbol.lower()}@depth@100ms"
        snapshot = await self.fetch_depth_snapshot(symbol)
        local_book = {
            "bids": {float(price): float(quantity) for price, quantity in snapshot.get("bids", [])},
            "asks": {float(price): float(quantity) for price, quantity in snapshot.get("asks", [])},
        }
        last_update_id = snapshot.get("lastUpdateId", 0)
        previous_final_id = last_update_id
        observed = 0
        confirmed_frames = 0
        best_notional = 0.0
        best_distance = distance_limit_bps
        deadline = asyncio.get_running_loop().time() + confirm_seconds

        async with websockets.connect(
            stream_uri,
            max_size=None,
            ping_interval=120,
            ping_timeout=120,
        ) as websocket:
            while asyncio.get_running_loop().time() < deadline:
                timeout = max(0.1, deadline - asyncio.get_running_loop().time())
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    break
                payload = json.loads(raw)
                if payload.get("u", 0) < last_update_id:
                    continue
                if previous_final_id and payload.get("pu") not in (previous_final_id, None):
                    LOGGER.warning("Depth gap detected for %s during confirmation", symbol)
                    return None
                self._apply_depth(local_book, payload)
                previous_final_id = payload.get("u", previous_final_id)
                observed += 1
                candidate = self._summarise_book(local_book, side, reference_price, distance_limit_bps)
                if candidate is None:
                    continue
                best_notional = max(best_notional, candidate["wall_notional"])
                best_distance = min(best_distance, candidate["wall_distance_bps"])
                if candidate["wall_notional"] >= min_notional * 0.85:
                    confirmed_frames += 1

        if observed == 0:
            return None
        persistence = confirmed_frames / observed
        score = min((best_notional / max(min_notional, 1.0)) * 28.0 + persistence * 48.0, 100.0)
        if persistence < 0.35:
            return None
        return {
            "wall_notional": round(best_notional, 2),
            "wall_distance_bps": round(best_distance, 2),
            "wall_persistence": round(persistence, 4),
            "confirm_frames": observed,
            "score": round(score, 2),
            "severity": "critical" if score >= 85 else "high" if score >= 70 else "medium",
        }

    def _apply_depth(self, local_book: dict[str, dict[float, float]], payload: dict[str, Any]) -> None:
        for side_key, book_key in (("b", "bids"), ("a", "asks")):
            book = local_book[book_key]
            for price_raw, quantity_raw in payload.get(side_key, []):
                price = float(price_raw)
                quantity = float(quantity_raw)
                if quantity == 0.0:
                    book.pop(price, None)
                else:
                    book[price] = quantity

    def _summarise_book(
        self,
        local_book: dict[str, dict[float, float]],
        side: str,
        reference_price: float,
        distance_limit_bps: float,
    ) -> dict[str, Any] | None:
        bids = sorted(local_book["bids"].items(), key=lambda item: item[0], reverse=True)
        asks = sorted(local_book["asks"].items(), key=lambda item: item[0])
        if not bids or not asks:
            return None
        mid = (bids[0][0] + asks[0][0]) / 2
        levels = bids[:30] if side == "bid" else asks[:30]
        best: dict[str, Any] | None = None
        for price, quantity in levels:
            distance_bps = abs(price - mid) / mid * 10000 if mid else 0.0
            if distance_bps > distance_limit_bps:
                continue
            if abs(price - reference_price) / reference_price > 0.0025:
                continue
            notional = price * quantity
            if best is None or notional > best["wall_notional"]:
                best = {
                    "wall_notional": notional,
                    "wall_distance_bps": distance_bps,
                }
        return best


class IntelCollector:
    def __init__(
        self,
        feed_configs: list[dict[str, Any]],
        x_source_template: str | None = None,
        x_handles: list[str] | None = None,
    ) -> None:
        self.feed_configs = feed_configs
        self.x_source_template = x_source_template
        self.x_handles = x_handles or []
        self.common_ticker_stoplist = {
            "ONE",
            "GAS",
            "BIG",
            "CAT",
            "ACE",
            "FUN",
            "ARK",
            "ID",
            "AI",
        }

    async def collect(self, symbol_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        configs = list(self.feed_configs)
        if self.x_source_template:
            for handle in self.x_handles:
                configs.append(
                    {
                        "name": f"X:{handle}",
                        "source_type": "x",
                        "source_score": 0.55,
                        "kind": "rss",
                        "url": self.x_source_template.format(handle=handle),
                    }
                )
        collected: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            for config in configs:
                try:
                    kind = config.get("kind", "rss")
                    if kind == "rss":
                        collected.extend(await self._fetch_rss(client, config, symbol_index))
                    elif kind == "binance_announcements":
                        collected.extend(await self._fetch_binance_announcements(client, config, symbol_index))
                except Exception as exc:
                    LOGGER.warning("Intel source failed: %s (%s)", config.get("name"), exc)
        return collected

    async def _fetch_rss(
        self,
        client: httpx.AsyncClient,
        config: dict[str, Any],
        symbol_index: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        response = await client.get(config["url"])
        response.raise_for_status()
        root = ElementTree.fromstring(response.text)
        items = root.findall(".//item")
        results: list[dict[str, Any]] = []
        for item in items[:12]:
            title = (item.findtext("title") or "").strip()
            summary = (item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            published_at = _coerce_datetime(item.findtext("pubDate") or item.findtext("published"))
            symbols = self._detect_symbols(" ".join([title, summary]), symbol_index)
            fingerprint = hashlib.sha1(
                f"{config['name']}|{link}|{title}".encode("utf-8"),
            ).hexdigest()
            results.append(
                {
                    "fingerprint": fingerprint,
                    "source_name": config["name"],
                    "source_type": config["source_type"],
                    "title": title,
                    "summary": self._strip_html(summary),
                    "url": link,
                    "published_at": published_at,
                    "source_score": float(config.get("source_score", 0.5)),
                    "symbols": symbols,
                }
            )
        return results

    def _detect_symbols(self, text: str, symbol_index: dict[str, dict[str, Any]]) -> list[str]:
        upper = text.upper()
        hits: list[str] = []
        for symbol, meta in symbol_index.items():
            base_asset = str(meta.get("base_asset") or meta.get("baseAsset") or "").upper()
            if not base_asset:
                continue
            if symbol in upper:
                hits.append(symbol)
                continue
            if base_asset in self.common_ticker_stoplist:
                continue
            patterns = (
                rf"\${re.escape(base_asset)}\b",
                rf"\b{re.escape(base_asset)}\s+(?:TOKEN|COIN|CHAIN|NETWORK)\b",
                rf"\b{re.escape(base_asset)}USDT\b",
            )
            if any(re.search(pattern, upper) for pattern in patterns):
                hits.append(symbol)
        return sorted(set(hits))

    async def _fetch_binance_announcements(
        self,
        client: httpx.AsyncClient,
        config: dict[str, Any],
        symbol_index: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        url = config.get(
            "url",
            "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=10",
        )
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
        catalogs = payload.get("data", {}).get("catalogs", [])
        articles = catalogs[0].get("articles", []) if catalogs else []
        results: list[dict[str, Any]] = []
        for article in articles[:12]:
            title = str(article.get("title", "")).strip()
            code = str(article.get("code", "")).strip()
            release_ms = int(article.get("releaseDate", 0) or 0)
            published_at = datetime.fromtimestamp(release_ms / 1000, tz=timezone.utc) if release_ms else utcnow()
            url = f"https://www.binance.com/en/support/announcement/{code}" if code else ""
            symbols = self._detect_symbols(title, symbol_index)
            fingerprint = hashlib.sha1(
                f"{config['name']}|{code}|{title}".encode("utf-8"),
            ).hexdigest()
            results.append(
                {
                    "fingerprint": fingerprint,
                    "source_name": config["name"],
                    "source_type": config["source_type"],
                    "title": title,
                    "summary": title,
                    "url": url,
                    "published_at": published_at,
                    "source_score": float(config.get("source_score", 0.95)),
                    "symbols": symbols,
                }
            )
        return results

    def _strip_html(self, value: str) -> str:
        value = re.sub(r"<[^>]+>", " ", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()


class TelegramNotifier:
    def __init__(self, bot_token: str | None, chat_id: str | None) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    async def send(self, alert: dict[str, Any]) -> None:
        if not self.bot_token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        message = self._format_message(alert)
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                url,
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "disable_web_page_preview": False,
                },
            )
            response.raise_for_status()

    def _format_message(self, alert: dict[str, Any]) -> str:
        severity_map = {
            "critical": "紧急",
            "high": "高优先级",
            "medium": "中优先级",
            "low": "低优先级",
        }
        related = alert.get("related_intel", [])
        cause_line = ""
        if related:
            top = related[0]
            cause_line = f"\n关联原因: {top.get('title')} ({top.get('source_name')})"
        return (
            f"[{severity_map.get(alert['severity'], alert['severity'])}] {alert['headline']}\n"
            f"{alert['message']}{cause_line}\n"
            f"评分: {alert.get('score', 0.0):.2f}\n"
            f"触发时间: {alert['triggered_at']}"
        )
