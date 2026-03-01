"""SQLAlchemy models — simplified Health Connect storage."""

import uuid

from sqlalchemy import Column, String, Date, DateTime, func
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class HealthConnectDaily(Base):
    """
    Raw Health Connect daily snapshot (v3 schema).
    One row per (device_id, date, collected_at) — newest wins.
    """
    __tablename__ = "health_connect_daily"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    schema_version = Column(String, nullable=False, default="3")
    source_app = Column(String, nullable=False, default="health_connect")
    raw_json = Column(JSONB, nullable=False)
    payload_hash = Column(String(64), nullable=True)
    record_type = Column(String(), nullable=False, default="daily")


class HealthConnectIntradayLog(Base):
    """
    Raw Health Connect intraday sync log (v3 schema).
    Every sync gets a row. Query with ORDER BY collected_at DESC for latest.
    """
    __tablename__ = "health_connect_intraday_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    schema_version = Column(String, nullable=False, default="3")
    source_app = Column(String, nullable=False, default="health_connect")
    raw_json = Column(JSONB, nullable=False)
    payload_hash = Column(String(64), nullable=True)
    record_type = Column(String(), nullable=False, default="intraday")
