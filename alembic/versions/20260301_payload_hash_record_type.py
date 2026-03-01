"""Add payload_hash and record_type to Health Connect tables

Revision ID: 20260301_payload_hash_record_type
Revises: 20260211_source_type
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260301_payload_hash_record_type"
down_revision: Union[str, None] = "20260211_source_type"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "health_connect_daily",
        sa.Column("payload_hash", sa.String(64), nullable=True),
    )
    op.add_column(
        "health_connect_daily",
        sa.Column("record_type", sa.String(), nullable=False, server_default="daily"),
    )

    op.add_column(
        "health_connect_intraday_logs",
        sa.Column("payload_hash", sa.String(64), nullable=True),
    )
    op.add_column(
        "health_connect_intraday_logs",
        sa.Column("record_type", sa.String(), nullable=False, server_default="intraday"),
    )


def downgrade() -> None:
    op.drop_column("health_connect_intraday_logs", "record_type")
    op.drop_column("health_connect_intraday_logs", "payload_hash")
    op.drop_column("health_connect_daily", "record_type")
    op.drop_column("health_connect_daily", "payload_hash")
