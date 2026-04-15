from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.storage import SentinelStore


def test_funding_history_returns_ascending_points(tmp_path) -> None:
    database_path = tmp_path / "sentinel.db"
    store = SentinelStore(f"sqlite:///{database_path}")
    store.bootstrap()

    now = datetime.now(timezone.utc)
    store.append_funding_history(
        [
            {
                "symbol": "BTCUSDT",
                "funding_rate": 0.00012,
                "funding_score": 40,
                "mark_price": 68000,
                "observed_at": now - timedelta(minutes=4),
            },
            {
                "symbol": "BTCUSDT",
                "funding_rate": 0.00028,
                "funding_score": 72,
                "mark_price": 68150,
                "observed_at": now - timedelta(minutes=2),
            },
        ],
        retention_hours=72,
    )

    points = store.list_funding_history("BTCUSDT", limit=10)

    assert [point["funding_rate"] for point in points] == [0.00012, 0.00028]
    assert store.resolve_chart_symbol() == "BTCUSDT"
