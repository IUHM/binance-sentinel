from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, pstdev
from typing import Any, Iterable


@dataclass(slots=True)
class FundingObservation:
    symbol: str
    funding_rate: float
    mark_price: float
    observed_at: datetime
    open_interest_value: float = 0.0
    liquidation_value: float = 0.0


@dataclass(slots=True)
class OrderbookSnapshot:
    symbol: str
    observed_at: datetime
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]

    @property
    def mid_price(self) -> float:
        best_bid = self.bids[0][0] if self.bids else 0.0
        best_ask = self.asks[0][0] if self.asks else 0.0
        if best_bid and best_ask:
            return (best_bid + best_ask) / 2
        return best_bid or best_ask or 0.0


def _zscore(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    spread = pstdev(values)
    if spread == 0:
        return 0.0
    return (values[-1] - mean(values)) / spread


def _percentile_rank(value: float, universe: Iterable[float]) -> float:
    values = sorted(universe)
    if not values:
        return 0.0
    below = sum(1 for item in values if item <= value)
    return below / len(values)


def _severity_from_score(score: float) -> str:
    if score >= 85:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def compute_funding_signal(
    history: list[FundingObservation],
    cross_section: list[FundingObservation],
    rules: dict[str, Any],
) -> dict[str, Any] | None:
    if not history:
        return None
    current = history[-1]
    abs_history = [abs(item.funding_rate) for item in history[-48:]]
    abs_current = abs(current.funding_rate)
    zscore = abs(_zscore(abs_history))
    baseline = history[-6] if len(history) >= 6 else history[0]
    slope = current.funding_rate - baseline.funding_rate
    universe = [abs(item.funding_rate) for item in cross_section]
    percentile = _percentile_rank(abs_current, universe)
    oi_samples = [item.open_interest_value for item in history[-12:] if item.open_interest_value > 0]
    oi_baseline = mean(oi_samples[:-1]) if len(oi_samples) > 1 else (oi_samples[0] if oi_samples else 0.0)
    oi_change_ratio = 0.0
    if oi_baseline > 0 and current.open_interest_value > 0:
        oi_change_ratio = (current.open_interest_value - oi_baseline) / oi_baseline
    liquidation_boost = min(current.liquidation_value / 500000.0, 1.0)

    abs_rate_points = min(abs_current * 90000.0, 45.0)
    slope_points = min(abs(slope) * 150000.0, 18.0)
    zscore_points = min(zscore * 8.0, 18.0)
    percentile_points = percentile * 16.0
    oi_points = max(0.0, min(oi_change_ratio * 24.0, 8.0))
    liquidation_points = liquidation_boost * 8.0
    score = abs_rate_points + slope_points + zscore_points + percentile_points + oi_points + liquidation_points

    should_surface = (
        abs_current >= float(rules["funding_abs_rate_floor"])
        or zscore >= 2.0
        or percentile >= 0.965
        or score >= float(rules["funding_prealert_score"])
    )
    if not should_surface:
        return None

    return {
        "symbol": current.symbol,
        "funding_rate": current.funding_rate,
        "mark_price": current.mark_price,
        "open_interest_value": current.open_interest_value,
        "liquidation_pressure": current.liquidation_value,
        "funding_zscore": round(zscore, 4),
        "funding_slope": round(slope, 8),
        "funding_percentile": round(percentile, 4),
        "oi_change_ratio": round(oi_change_ratio, 4),
        "score": round(score, 2),
        "severity": _severity_from_score(score),
    }


def detect_orderbook_wall(
    current: OrderbookSnapshot,
    history: list[OrderbookSnapshot],
    rules: dict[str, Any],
) -> dict[str, Any] | None:
    mid = current.mid_price
    if mid <= 0:
        return None
    max_distance_bps = float(rules["wall_distance_limit_bps"])
    min_notional = float(rules["wall_min_notional_usd"])

    best_candidate: dict[str, Any] | None = None
    for side, levels in (("bid", current.bids), ("ask", current.asks)):
        for price, quantity in levels:
            if price <= 0 or quantity <= 0:
                continue
            distance_bps = abs(price - mid) / mid * 10000
            if distance_bps > max_distance_bps:
                continue
            notional = price * quantity
            if notional < min_notional * 0.35:
                continue
            proximity = max(0.15, 1.0 - distance_bps / max_distance_bps)
            weighted_notional = notional * proximity
            if best_candidate is None or weighted_notional > best_candidate["weighted_notional"]:
                best_candidate = {
                    "wall_side": side,
                    "wall_price": price,
                    "wall_notional": notional,
                    "wall_quantity": quantity,
                    "wall_distance_bps": distance_bps,
                    "weighted_notional": weighted_notional,
                }

    if best_candidate is None:
        return None

    comparison_window = history[-10:]
    reference_notional: list[float] = []
    persistence_hits = 0
    for snapshot in comparison_window:
        side_levels = snapshot.bids if best_candidate["wall_side"] == "bid" else snapshot.asks
        matching_level = None
        max_notional = 0.0
        snapshot_mid = snapshot.mid_price or mid
        for price, quantity in side_levels:
            distance_bps = abs(price - snapshot_mid) / snapshot_mid * 10000 if snapshot_mid else 0.0
            if distance_bps <= max_distance_bps:
                max_notional = max(max_notional, price * quantity)
            if abs(price - best_candidate["wall_price"]) / best_candidate["wall_price"] <= 0.0015:
                matching_level = max(matching_level or 0.0, price * quantity)
        reference_notional.append(max_notional)
        persistence_threshold = min(best_candidate["wall_notional"] * 0.45, min_notional)
        if matching_level and matching_level >= persistence_threshold:
            persistence_hits += 1

    baseline = mean(reference_notional) if reference_notional else min_notional
    ratio = best_candidate["wall_notional"] / max(baseline, 1.0)
    persistence = persistence_hits / max(len(comparison_window), 1)

    bid_total = sum(price * quantity for price, quantity in current.bids[:5])
    ask_total = sum(price * quantity for price, quantity in current.asks[:5])
    imbalance = bid_total / max(ask_total, 1.0)
    if best_candidate["wall_side"] == "ask":
        imbalance = ask_total / max(bid_total, 1.0)

    ratio_points = min(max(math.log2(max(ratio, 1.0)), 0.0) * 12.0, 30.0)
    absolute_points = min(best_candidate["wall_notional"] / min_notional * 10.0, 20.0)
    persistence_points = persistence * 28.0
    imbalance_points = min(max(imbalance - 1.0, 0.0) * 12.0, 12.0)
    proximity_points = max(0.0, 10.0 - best_candidate["wall_distance_bps"] / 15.0)
    score = ratio_points + absolute_points + persistence_points + imbalance_points + proximity_points

    if score < float(rules["wall_candidate_score"]):
        return None

    return {
        "symbol": current.symbol,
        "wall_side": best_candidate["wall_side"],
        "wall_price": round(best_candidate["wall_price"], 8),
        "wall_quantity": round(best_candidate["wall_quantity"], 6),
        "wall_notional": round(best_candidate["wall_notional"], 2),
        "wall_distance_bps": round(best_candidate["wall_distance_bps"], 2),
        "wall_persistence": round(persistence, 4),
        "wall_ratio": round(ratio, 4),
        "orderbook_imbalance": round(imbalance, 4),
        "score": round(score, 2),
        "severity": _severity_from_score(score),
    }


def link_relevant_intel(
    symbol: str,
    now: datetime,
    intel_items: Iterable[dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for item in intel_items:
        if symbol not in item.get("symbols", []):
            continue
        published_raw = item.get("published_at")
        if not published_raw:
            continue
        published_at = (
            published_raw
            if isinstance(published_raw, datetime)
            else datetime.fromisoformat(published_raw)
        )
        age = abs((now - published_at).total_seconds()) / 60.0
        if age > 60:
            continue
        recency = max(0.0, 1.0 - age / 60.0)
        confidence = item.get("source_score", 0.5) * 0.7 + recency * 0.3
        ranked.append(
            {
                "source_name": item.get("source_name"),
                "source_type": item.get("source_type"),
                "title": item.get("title"),
                "url": item.get("url"),
                "published_at": published_at.isoformat(),
                "confidence": round(confidence, 4),
            }
        )
    ranked.sort(key=lambda entry: entry["confidence"], reverse=True)
    return ranked[:limit]


def render_alert_message(category: str, symbol: str, metrics: dict[str, Any], related_intel: list[dict[str, Any]]) -> str:
    if category == "funding":
        direction = "上升" if metrics.get("funding_rate", 0.0) >= 0 else "下降"
        lines = [
            f"{symbol} 资金费率异常{direction}",
            f"费率: {metrics.get('funding_rate', 0.0):.6f}",
            f"评分: {metrics.get('score', 0.0):.2f}",
            f"Z 分数: {metrics.get('funding_zscore', 0.0):.2f}",
            f"持仓价值: {metrics.get('open_interest_value', 0.0):,.0f}",
        ]
    elif category == "orderbook":
        side_label = "买墙" if metrics.get("wall_side") == "bid" else "卖墙"
        lines = [
            f"{symbol} 发现大额{side_label}",
            f"挂单名义价值: {metrics.get('wall_notional', 0.0):,.0f} USDT",
            f"距离中间价: {metrics.get('wall_distance_bps', 0.0):.1f} bps",
            f"持续度: {metrics.get('wall_persistence', 0.0):.0%}",
            f"评分: {metrics.get('score', 0.0):.2f}",
        ]
    else:
        lines = [
            f"{symbol} 出现情报催化",
            metrics.get("summary", ""),
        ]
    if related_intel:
        top = related_intel[0]
        lines.append(f"关联原因: {top['title']} ({top['source_name']})")
    return "\n".join(line for line in lines if line)
