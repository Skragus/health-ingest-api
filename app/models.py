"""SQLAlchemy models — simplified Health Connect storage."""

import uuid

from sqlalchemy import Column, String, Date, DateTime, func, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class HealthConnectDaily(Base):
    """
    Canonical daily summary — one row per (device_id, date).
    Upserted by daily endpoint. Newer collected_at wins.
    """
    __tablename__ = "health_connect_daily"

    # Composite PK: device + date
    device_id = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    
    # Timestamps
    collected_at = Column(DateTime(timezone=True), nullable=False, index=True)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Raw payload — everything lives here
    raw_data = Column(JSONB, nullable=False)

    __table_args__ = (
        Index("ix_daily_date", "date"),
        Index("ix_daily_collected", "collected_at"),
    )


class HealthConnectIntradayLog(Base):
    """
    Append-only intraday sync log.
    Every sync gets a row. Query with ORDER BY collected_at DESC for latest.
    """
    __tablename__ = "health_connect_intraday_logs"

    # Auto-generated UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    device_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    collected_at = Column(DateTime(timezone=True), nullable=False, index=True)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Raw payload — everything lives here
    raw_data = Column(JSONB, nullable=False)

    __table_args__ = (
        Index("ix_logs_device_date", "device_id", "date"),
        Index("ix_logs_collected", "collected_at"),
    )
