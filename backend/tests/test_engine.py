from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.engine import FundingObservation, OrderbookSnapshot, compute_funding_signal, detect_orderbook_wall
from backend.storage import DEFAULT_RULES


def test_funding_signal_surfaces_extreme_rate() -> None:
    now = datetime.now(timezone.utc)
    history = [
        FundingObservation("BTCUSDT", 0.00012, 68000, now - timedelta(minutes=8)),
        FundingObservation("BTCUSDT", 0.00016, 68100, now - timedelta(minutes=4)),
        FundingObservation("BTCUSDT", 0.00058, 68500, now, open_interest_value=220_000_000),
    ]
    cross = history + [
        FundingObservation("ETHUSDT", 0.00009, 3500, now),
        FundingObservation("SOLUSDT", -0.00011, 160, now),
    ]

    signal = compute_funding_signal(history, cross, DEFAULT_RULES)

    assert signal is not None
    assert signal["score"] >= DEFAULT_RULES["funding_prealert_score"]
    assert signal["severity"] in {"medium", "high", "critical"}


def test_orderbook_wall_detects_persistent_bid_wall() -> None:
    now = datetime.now(timezone.utc)
    history = [
        OrderbookSnapshot(
            "BTCUSDT",
            now - timedelta(seconds=5 - index),
            bids=[(68000 - index, 8 + index), (67990, 1.2)],
            asks=[(68010, 1.1), (68020, 0.9)],
        )
        for index in range(5)
    ]
    current = OrderbookSnapshot(
        "BTCUSDT",
        now,
        bids=[(68002, 18), (67998, 2.5), (67990, 1.1)],
        asks=[(68010, 1.0), (68015, 0.9), (68020, 0.8)],
    )

    signal = detect_orderbook_wall(current, history, DEFAULT_RULES)

    assert signal is not None
    assert signal["wall_side"] == "bid"
    assert signal["wall_notional"] > DEFAULT_RULES["wall_min_notional_usd"]
    assert signal["score"] >= DEFAULT_RULES["wall_candidate_score"]
