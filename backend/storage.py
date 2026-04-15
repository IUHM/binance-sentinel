from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from sqlalchemy import DateTime, Float, Integer, String, Text, create_engine, delete, desc, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item),
    )


def _json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


class Base(DeclarativeBase):
    pass


class SymbolModel(Base):
    __tablename__ = "symbols"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    base_asset: Mapped[str] = mapped_column(String(24), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(24), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    contract_type: Mapped[str] = mapped_column(String(24), nullable=False)
    onboard_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SnapshotModel(Base):
    __tablename__ = "snapshots"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    funding_rate: Mapped[float] = mapped_column(Float, default=0.0)
    funding_zscore: Mapped[float] = mapped_column(Float, default=0.0)
    funding_slope: Mapped[float] = mapped_column(Float, default=0.0)
    funding_percentile: Mapped[float] = mapped_column(Float, default=0.0)
    funding_score: Mapped[float] = mapped_column(Float, default=0.0)
    mark_price: Mapped[float] = mapped_column(Float, default=0.0)
    open_interest_value: Mapped[float] = mapped_column(Float, default=0.0)
    liquidation_pressure: Mapped[float] = mapped_column(Float, default=0.0)
    wall_side: Mapped[str] = mapped_column(String(8), default="")
    wall_price: Mapped[float] = mapped_column(Float, default=0.0)
    wall_distance_bps: Mapped[float] = mapped_column(Float, default=0.0)
    wall_notional: Mapped[float] = mapped_column(Float, default=0.0)
    wall_persistence: Mapped[float] = mapped_column(Float, default=0.0)
    wall_score: Mapped[float] = mapped_column(Float, default=0.0)
    metrics_json: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class FundingHistoryPointModel(Base):
    __tablename__ = "funding_history_points"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    funding_rate: Mapped[float] = mapped_column(Float, default=0.0)
    funding_score: Mapped[float] = mapped_column(Float, default=0.0)
    mark_price: Mapped[float] = mapped_column(Float, default=0.0)
    open_interest_value: Mapped[float] = mapped_column(Float, default=0.0)
    liquidation_pressure: Mapped[float] = mapped_column(Float, default=0.0)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class IntelItemModel(Base):
    __tablename__ = "intel_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_type: Mapped[str] = mapped_column(String(24), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(Text, default="")
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source_score: Mapped[float] = mapped_column(Float, default=0.0)
    symbols_json: Mapped[str] = mapped_column(Text, default="[]")
    raw_json: Mapped[str] = mapped_column(Text, default="{}")


class AlertModel(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dedupe_key: Mapped[str] = mapped_column(String(128), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    category: Mapped[str] = mapped_column(String(24), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    headline: Mapped[str] = mapped_column(String(160), nullable=False)
    message: Mapped[str] = mapped_column(Text, default="")
    score: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(24), default="open")
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    metrics_json: Mapped[str] = mapped_column(Text, default="{}")
    related_intel_json: Mapped[str] = mapped_column(Text, default="[]")
    raw_json: Mapped[str] = mapped_column(Text, default="{}")


class StateModel(Base):
    __tablename__ = "state"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value_json: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


DEFAULT_RULES: dict[str, Any] = {
    "funding_abs_rate_floor": 0.00025,
    "funding_prealert_score": 52.0,
    "funding_alert_score": 67.0,
    "funding_cooldown_minutes": 5,
    "wall_min_notional_usd": 250000.0,
    "wall_distance_limit_bps": 140.0,
    "wall_candidate_score": 55.0,
    "wall_alert_score": 70.0,
    "wall_cooldown_minutes": 3,
    "depth_confirm_seconds": 2.5,
    "dashboard_flush_top_n": 40,
}


class SentinelStore:
    def __init__(self, database_url: str) -> None:
        connect_args: dict[str, Any] = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self.engine = create_engine(database_url, future=True, connect_args=connect_args)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False, autoflush=False)

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def init_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def bootstrap(self) -> None:
        self.init_schema()
        if self.get_state("rules") is None:
            self.set_state("rules", DEFAULT_RULES)

    def set_state(self, key: str, value: Any) -> None:
        with self.session_scope() as session:
            existing = session.get(StateModel, key)
            payload = _json_dumps(value)
            if existing is None:
                session.add(StateModel(key=key, value_json=payload))
            else:
                existing.value_json = payload
                existing.updated_at = utcnow()

    def get_state(self, key: str) -> Any | None:
        with self.session_scope() as session:
            state = session.get(StateModel, key)
            if state is None:
                return None
            return _json_loads(state.value_json, None)

    def upsert_symbols(self, symbols: list[dict[str, Any]]) -> None:
        with self.session_scope() as session:
            for item in symbols:
                existing = session.get(SymbolModel, item["symbol"])
                onboard_date = None
                onboard_ts = item.get("onboardDate")
                if onboard_ts:
                    onboard_date = datetime.fromtimestamp(onboard_ts / 1000, tz=timezone.utc)
                if existing is None:
                    session.add(
                        SymbolModel(
                            symbol=item["symbol"],
                            base_asset=item["baseAsset"],
                            quote_asset=item["quoteAsset"],
                            status=item["status"],
                            contract_type=item["contractType"],
                            onboard_date=onboard_date,
                            raw_json=_json_dumps(item),
                        )
                    )
                    continue
                existing.base_asset = item["baseAsset"]
                existing.quote_asset = item["quoteAsset"]
                existing.status = item["status"]
                existing.contract_type = item["contractType"]
                existing.onboard_date = onboard_date
                existing.raw_json = _json_dumps(item)
                existing.updated_at = utcnow()

    def list_symbols(self) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            rows = session.scalars(select(SymbolModel).order_by(SymbolModel.symbol.asc())).all()
            return [
                {
                    "symbol": row.symbol,
                    "base_asset": row.base_asset,
                    "quote_asset": row.quote_asset,
                    "status": row.status,
                    "contract_type": row.contract_type,
                    "onboard_date": row.onboard_date.isoformat() if row.onboard_date else None,
                    "updated_at": row.updated_at.isoformat(),
                }
                for row in rows
            ]

    def get_rules(self) -> dict[str, Any]:
        rules = self.get_state("rules")
        merged = dict(DEFAULT_RULES)
        if rules:
            merged.update(rules)
        return merged

    def update_rules(self, rules: dict[str, Any]) -> dict[str, Any]:
        merged = dict(DEFAULT_RULES)
        merged.update(rules)
        self.set_state("rules", merged)
        return merged

    def save_intel_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        inserted: list[dict[str, Any]] = []
        with self.session_scope() as session:
            for item in items:
                existing = session.scalar(select(IntelItemModel).where(IntelItemModel.fingerprint == item["fingerprint"]))
                if existing is not None:
                    continue
                row = IntelItemModel(
                    fingerprint=item["fingerprint"],
                    source_name=item["source_name"],
                    source_type=item["source_type"],
                    title=item["title"],
                    summary=item.get("summary", ""),
                    url=item.get("url", ""),
                    published_at=item["published_at"],
                    source_score=float(item.get("source_score", 0.0)),
                    symbols_json=_json_dumps(item.get("symbols", [])),
                    raw_json=_json_dumps(item),
                )
                session.add(row)
                session.flush()
                inserted.append(self._intel_row_to_dict(row))
        return inserted

    def list_intel(self, limit: int = 40, symbol: str | None = None) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            rows = session.scalars(
                select(IntelItemModel).order_by(desc(IntelItemModel.published_at)).limit(limit)
            ).all()
            results = [self._intel_row_to_dict(row) for row in rows]
            if symbol is None:
                return results
            return [item for item in results if symbol in item["symbols"]]

    def upsert_snapshots(self, snapshots: list[dict[str, Any]]) -> None:
        if not snapshots:
            return
        with self.session_scope() as session:
            for item in snapshots:
                existing = session.get(SnapshotModel, item["symbol"])
                if existing is None:
                    existing = SnapshotModel(symbol=item["symbol"])
                    session.add(existing)
                for key, value in item.items():
                    if key == "symbol":
                        continue
                    setattr(existing, key, value)
                existing.updated_at = utcnow()

    def list_snapshots(self) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            rows = session.scalars(select(SnapshotModel)).all()
            return [self._snapshot_row_to_dict(row) for row in rows]

    def append_funding_history(self, points: list[dict[str, Any]], retention_hours: int = 72) -> None:
        if not points:
            return
        cutoff = utcnow() - timedelta(hours=retention_hours)
        with self.session_scope() as session:
            for item in points:
                session.add(
                    FundingHistoryPointModel(
                        symbol=item["symbol"],
                        funding_rate=float(item.get("funding_rate", 0.0)),
                        funding_score=float(item.get("funding_score", 0.0)),
                        mark_price=float(item.get("mark_price", 0.0)),
                        open_interest_value=float(item.get("open_interest_value", 0.0)),
                        liquidation_pressure=float(item.get("liquidation_pressure", 0.0)),
                        observed_at=item.get("observed_at", utcnow()),
                    )
                )
            session.execute(
                delete(FundingHistoryPointModel).where(FundingHistoryPointModel.observed_at < cutoff),
            )

    def list_funding_history(self, symbol: str, limit: int = 120) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            rows = session.scalars(
                select(FundingHistoryPointModel)
                .where(FundingHistoryPointModel.symbol == symbol)
                .order_by(desc(FundingHistoryPointModel.observed_at))
                .limit(limit)
            ).all()
            return [
                self._funding_point_to_dict(row)
                for row in reversed(rows)
            ]

    def resolve_chart_symbol(self) -> str | None:
        with self.session_scope() as session:
            top_snapshot = session.scalar(
                select(SnapshotModel.symbol)
                .order_by(desc(SnapshotModel.funding_score), desc(func.abs(SnapshotModel.funding_rate)))
                .limit(1)
            )
            if top_snapshot:
                return top_snapshot
            latest_history_symbol = session.scalar(
                select(FundingHistoryPointModel.symbol)
                .order_by(desc(FundingHistoryPointModel.observed_at), desc(FundingHistoryPointModel.funding_score))
                .limit(1)
            )
            if latest_history_symbol:
                return latest_history_symbol
            return session.scalar(select(SymbolModel.symbol).order_by(SymbolModel.symbol.asc()).limit(1))

    def save_alert(self, alert: dict[str, Any], cooldown_minutes: int) -> dict[str, Any] | None:
        with self.session_scope() as session:
            previous = session.scalar(
                select(AlertModel)
                .where(AlertModel.dedupe_key == alert["dedupe_key"])
                .order_by(desc(AlertModel.triggered_at))
                .limit(1)
            )
            if previous is not None:
                age_seconds = (utcnow() - previous.triggered_at).total_seconds()
                if age_seconds < cooldown_minutes * 60 and previous.score >= float(alert.get("score", 0.0)):
                    return None
            row = AlertModel(
                dedupe_key=alert["dedupe_key"],
                symbol=alert["symbol"],
                category=alert["category"],
                severity=alert["severity"],
                headline=alert["headline"],
                message=alert["message"],
                score=float(alert.get("score", 0.0)),
                status=alert.get("status", "open"),
                triggered_at=alert.get("triggered_at", utcnow()),
                metrics_json=_json_dumps(alert.get("metrics", {})),
                related_intel_json=_json_dumps(alert.get("related_intel", [])),
                raw_json=_json_dumps(alert),
            )
            session.add(row)
            session.flush()
            return self._alert_row_to_dict(row)

    def list_alerts(
        self,
        limit: int = 100,
        symbol: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.session_scope() as session:
            stmt = select(AlertModel).order_by(desc(AlertModel.triggered_at)).limit(limit)
            if symbol:
                stmt = stmt.where(AlertModel.symbol == symbol)
            if category:
                stmt = stmt.where(AlertModel.category == category)
            rows = session.scalars(stmt).all()
            return [self._alert_row_to_dict(row) for row in rows]

    def get_alert(self, alert_id: int) -> dict[str, Any] | None:
        with self.session_scope() as session:
            row = session.get(AlertModel, alert_id)
            if row is None:
                return None
            return self._alert_row_to_dict(row)

    def build_overview(self) -> dict[str, Any]:
        with self.session_scope() as session:
            snapshots = [self._snapshot_row_to_dict(row) for row in session.scalars(select(SnapshotModel)).all()]
            alerts = [
                self._alert_row_to_dict(row)
                for row in session.scalars(
                    select(AlertModel).order_by(desc(AlertModel.triggered_at)).limit(12)
                ).all()
            ]
            intel = [
                self._intel_row_to_dict(row)
                for row in session.scalars(
                    select(IntelItemModel).order_by(desc(IntelItemModel.published_at)).limit(12)
                ).all()
            ]
            heartbeat = session.get(StateModel, "worker_heartbeat")
            symbol_count = session.scalar(select(func.count()).select_from(SymbolModel)) or 0
        top_funding = sorted(
            snapshots,
            key=lambda item: (item["funding_score"], abs(item["funding_rate"])),
            reverse=True,
        )[:12]
        top_walls = sorted(
            [item for item in snapshots if item["wall_score"] > 0],
            key=lambda item: item["wall_score"],
            reverse=True,
        )[:12]
        worker_state = _json_loads(heartbeat.value_json, {}) if heartbeat else {}
        worker_age = None
        if worker_state.get("updated_at"):
            try:
                worker_age = max(0.0, (utcnow() - datetime.fromisoformat(worker_state["updated_at"])).total_seconds())
            except ValueError:
                worker_age = None
        return {
            "generated_at": utcnow().isoformat(),
            "monitored_symbol_count": symbol_count,
            "top_funding": top_funding,
            "top_walls": top_walls,
            "latest_alerts": alerts,
            "latest_intel": intel,
            "health": {
                "database": "ok",
                "worker": worker_state.get("status", "unknown"),
                "worker_age_seconds": worker_age,
                "pending_confirmations": worker_state.get("pending_confirmations", 0),
                "tracked_symbols": worker_state.get("tracked_symbols", symbol_count),
            },
        }

    def _snapshot_row_to_dict(self, row: SnapshotModel) -> dict[str, Any]:
        return {
            "symbol": row.symbol,
            "funding_rate": row.funding_rate,
            "funding_zscore": row.funding_zscore,
            "funding_slope": row.funding_slope,
            "funding_percentile": row.funding_percentile,
            "funding_score": row.funding_score,
            "mark_price": row.mark_price,
            "open_interest_value": row.open_interest_value,
            "liquidation_pressure": row.liquidation_pressure,
            "wall_side": row.wall_side,
            "wall_price": row.wall_price,
            "wall_distance_bps": row.wall_distance_bps,
            "wall_notional": row.wall_notional,
            "wall_persistence": row.wall_persistence,
            "wall_score": row.wall_score,
            "metrics": _json_loads(row.metrics_json, {}),
            "updated_at": row.updated_at.isoformat(),
        }

    def _funding_point_to_dict(self, row: FundingHistoryPointModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "symbol": row.symbol,
            "funding_rate": row.funding_rate,
            "funding_score": row.funding_score,
            "mark_price": row.mark_price,
            "open_interest_value": row.open_interest_value,
            "liquidation_pressure": row.liquidation_pressure,
            "observed_at": row.observed_at.isoformat(),
        }

    def _intel_row_to_dict(self, row: IntelItemModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "fingerprint": row.fingerprint,
            "source_name": row.source_name,
            "source_type": row.source_type,
            "title": row.title,
            "summary": row.summary,
            "url": row.url,
            "published_at": row.published_at.isoformat(),
            "discovered_at": row.discovered_at.isoformat(),
            "source_score": row.source_score,
            "symbols": _json_loads(row.symbols_json, []),
        }

    def _alert_row_to_dict(self, row: AlertModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "dedupe_key": row.dedupe_key,
            "symbol": row.symbol,
            "category": row.category,
            "severity": row.severity,
            "headline": row.headline,
            "message": row.message,
            "score": row.score,
            "status": row.status,
            "triggered_at": row.triggered_at.isoformat(),
            "metrics": _json_loads(row.metrics_json, {}),
            "related_intel": _json_loads(row.related_intel_json, []),
        }
