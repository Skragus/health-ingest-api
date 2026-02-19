import uuid

from sqlalchemy import Column, Integer, String, Date, DateTime, UniqueConstraint, func, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class HealthConnectDaily(Base):
    """
    Summary table: One row per day, always the current best state.
    Updated by both intraday and daily syncs throughout the day.
    Stores raw JSON payload for flexibility — all metrics live in raw_data.
    """
    __tablename__ = "health_connect_daily"

    # Identity
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    
    # Temporal
    date = Column(Date, nullable=False)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Raw payload — everything from the Android app lives here
    raw_data = Column(JSONB, nullable=False)
    
    # Metadata
    source_type = Column(String, nullable=False, server_default="daily")
    schema_version = Column(Integer, nullable=False, default=1)
    
    # Metadata about the source
    source = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "date",
            "schema_version",
            name="uq_health_connect_daily_device_date_version",
        ),
        Index("ix_health_connect_daily_date", "date"),
        Index("ix_health_connect_daily_device_date", "device_id", "date"),
    )


class HealthConnectIntradayLog(Base):
    """
    History table: Every sync gets a row, append-only.
    Captures granular time-series data for trend analysis.
    Stores raw JSON payload for flexibility.
    """
    __tablename__ = "health_connect_intraday_logs"

    # Identity
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False, index=True)
    
    # Temporal
    date = Column(Date, nullable=False, index=True)
    collected_at = Column(DateTime(timezone=True), nullable=False, index=True)
    received_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Raw payload — everything from the Android app lives here
    raw_data = Column(JSONB, nullable=False)
    
    # Metadata
    source_type = Column(String, nullable=False)
    schema_version = Column(Integer, nullable=False, default=1)
    
    # Metadata about the source
    source = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "date",
            "collected_at",
            name="uq_intraday_device_date_collected",
        ),
        Index("ix_intraday_logs_date_collected", "date", "collected_at"),
        Index("ix_intraday_logs_device_date", "device_id", "date"),
    )


# Legacy model (to be deprecated after migration)
class ShealthDaily(Base):
    __tablename__ = "shealth_daily"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    schema_version = Column(Integer, nullable=False, default=1)

    steps_total = Column(Integer, nullable=False)
    sleep_sessions = Column(JSONB, nullable=True)
    heart_rate_summary = Column(JSONB, nullable=True)
    body_metrics = Column(JSONB, nullable=True)
    nutrition_summary = Column(JSONB, nullable=True)
    exercise_sessions = Column(JSONB, nullable=True)
    source = Column(JSONB, nullable=False)

    source_type = Column(String, nullable=False, server_default="daily")

    collected_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "device_id",
            "date",
            "schema_version",
            name="uq_device_date_version",
        ),
    )
