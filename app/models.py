"""SQLAlchemy models — simplified Health Connect storage."""

import uuid

from sqlalchemy import Column, String, Date, DateTime, func
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class HealthConnectDaily(Base):
    """
    Canonical daily summary — one row per (device_id, date).
    Upserted by daily endpoint. Newer collected_at wins.
    No unique constraints — handled in application code.
    """
    __tablename__ = "health_connect_daily"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    raw_data = Column(JSONB, nullable=False)


class HealthConnectIntradayLog(Base):
    """
    Append-only intraday sync log.
    Every sync gets a row. Query with ORDER BY collected_at DESC for latest.
    No unique constraints — pure append.
    """
    __tablename__ = "health_connect_intraday_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    raw_data = Column(JSONB, nullable=False)
