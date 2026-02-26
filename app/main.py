"""SH-APK-API — Health Connect Ingestion Layer (Simplified v2)"""

import json
import logging
import asyncio
import httpx
from datetime import date as py_date, datetime

from fastapi import Depends, FastAPI, Header, HTTPException, status, Path, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.config import settings
from app.database import Base, engine, get_db
from app.models import HealthConnectDaily, HealthConnectIntradayLog
from app.schemas import DailyIngestRequest, IngestResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("health-ingest")

app = FastAPI(title="Health Connect Ingest API", version="2.0.0")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return x_api_key


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_payload(payload: DailyIngestRequest) -> DailyIngestRequest:
    """Validate payload fields and null out garbage data."""
    
    # Body metrics validation
    if payload.body_metrics:
        weight = payload.body_metrics.weight_kg
        if weight is not None and (weight < 30 or weight > 300):
            logger.warning(f"Rejecting invalid weight: {weight} kg")
            payload.body_metrics.weight_kg = None
        
        body_fat = payload.body_metrics.body_fat_percentage
        if body_fat is not None and (body_fat < 3 or body_fat > 70):
            logger.warning(f"Rejecting invalid body fat: {body_fat}%")
            payload.body_metrics.body_fat_percentage = None
    
    # Heart rate validation
    if payload.heart_rate_summary:
        for field in ["min_bpm", "max_bpm", "avg_bpm", "resting_bpm"]:
            value = getattr(payload.heart_rate_summary, field, None)
            if value is not None and (value < 30 or value > 250):
                logger.warning(f"Rejecting invalid heart rate ({field}): {value} bpm")
                setattr(payload.heart_rate_summary, field, None)
    
    # Nutrition validation
    if payload.nutrition_summary:
        calories = payload.nutrition_summary.calories_total
        if calories is not None and (calories < 0 or calories > 10000):
            logger.warning(f"Rejecting invalid calories: {calories}")
            payload.nutrition_summary.calories_total = None
        
        for macro in ["protein_grams", "carbs_grams", "fat_grams"]:
            value = getattr(payload.nutrition_summary, macro, None)
            if value is not None and value < 0:
                logger.warning(f"Rejecting invalid macro ({macro}): {value}g")
                setattr(payload.nutrition_summary, macro, None)
    
    return payload


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

async def _send_notification(sync_type: str, payload: DailyIngestRequest):
    """Send formatted sync notification to Telegram."""
    try:
        lines = [f"✅ {sync_type.title()} Sync", f"📅 {payload.date}"]
        
        if payload.steps_total is not None:
            lines.append(f"🚶 {payload.steps_total:,} steps")
        
        if payload.body_metrics and payload.body_metrics.weight_kg:
            weight_line = f"⚖️ {payload.body_metrics.weight_kg:.1f} kg"
            if payload.body_metrics.body_fat_percentage:
                weight_line += f" ({payload.body_metrics.body_fat_percentage:.1f}% BF)"
            lines.append(weight_line)
        
        if payload.exercise_sessions:
            lines.append(f"💪 {len(payload.exercise_sessions)} workout(s)")
            for ex in payload.exercise_sessions:
                lines.append(f"   • {ex.title or 'Workout'} ({ex.duration_minutes} min)")
        
        if payload.nutrition_summary and payload.nutrition_summary.calories_total:
            food_line = f"🍽️ {payload.nutrition_summary.calories_total} cal"
            if payload.nutrition_summary.protein_grams:
                food_line += f" ({payload.nutrition_summary.protein_grams:.1f}g protein)"
            lines.append(food_line)
        
        url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={
                "chat_id": settings.TELEGRAM_CHAT_ID,
                "text": "\n".join(lines),
                "parse_mode": "HTML"
            }, timeout=5.0)
        
        logger.info(f"Telegram notification sent for {sync_type} sync")
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")


# ---------------------------------------------------------------------------
# Debug / Schema Discovery
# ---------------------------------------------------------------------------

@app.post("/v1/ingest/debug")
async def ingest_debug(
    payload: dict = Body(...),
    _: str = Depends(verify_api_key),
):
    """Raw schema capture for Health Connect development.
    
    Logs full JSON payload and returns it for inspection.
    Use this to discover actual field structure from watchdogbridge.
    """
    raw_json = json.dumps(payload, indent=2, default=str)
    logger.info(f"RAW HEALTH CONNECT PAYLOAD:\n{raw_json}")
    
    # Also save to a file for easy retrieval
    debug_file = f"/tmp/health_connect_debug_{payload.get('date', 'unknown')}_{datetime.now().isoformat()}.json"
    try:
        with open(debug_file, 'w') as f:
            f.write(raw_json)
    except Exception as e:
        logger.warning(f"Could not write debug file: {e}")
    
    return {
        "status": "debug_logged",
        "payload": payload,
        "size_bytes": len(raw_json),
        "top_level_keys": list(payload.keys()),
    }

@app.post("/v1/ingest/daily", response_model=IngestResponse)
async def ingest_daily(
    payload: DailyIngestRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Canonical daily ingestion — simple insert for backfill.
    
    Temporarily simplified: always inserts. Upsert logic to be restored.
    """
    logger.info(f"Daily ingest: {payload.date} from {payload.source.device_id}")
    
    payload = _validate_payload(payload)
    raw_payload = json.dumps(payload.model_dump(mode="json"))
    
    # Simple insert for now
    await db.execute(
        text("""
            INSERT INTO health_connect_daily (id, device_id, date, collected_at, raw_data)
            VALUES (gen_random_uuid(), :device_id, :date, :collected_at, :raw_data)
        """),
        {
            "device_id": payload.source.device_id,
            "date": payload.date,
            "collected_at": payload.source.collected_at,
            "raw_data": raw_payload,
        }
    )
    await db.commit()
    
    asyncio.create_task(_send_notification("daily", payload))
    logger.info(f"Inserted daily record for {payload.date}")
    
    return IngestResponse(inserted=True)


@app.post("/v1/ingest/intraday", response_model=IngestResponse)
async def ingest_intraday(
    payload: DailyIngestRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Intraday snapshot ingestion — append-only to logs table.
    
    Creates full audit trail of every sync. Does NOT touch daily table.
    Query with ORDER BY collected_at DESC LIMIT 1 for latest snapshot.
    """
    logger.info(f"Intraday ingest: {payload.date} from {payload.source.device_id}")
    
    payload = _validate_payload(payload)
    raw_payload = json.dumps(payload.model_dump(mode="json"))
    
    # Pure append — no conflict resolution, no constraints needed
    result = await db.execute(
        text("""
            INSERT INTO health_connect_intraday_logs
                (id, device_id, date, collected_at, raw_data)
            VALUES
                (gen_random_uuid(), :device_id, :date, :collected_at, :raw_data)
            RETURNING id
        """),
        {
            "device_id": payload.source.device_id,
            "date": payload.date,
            "collected_at": payload.source.collected_at,
            "raw_data": raw_payload,
        }
    )
    await db.commit()
    
    inserted_id = result.scalar()
    
    asyncio.create_task(_send_notification("intraday", payload))
    
    return IngestResponse(inserted=True, id=inserted_id)


# ---------------------------------------------------------------------------
# Query Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    """Health check with DB connectivity test."""
    await db.execute(text("SELECT 1"))
    return {"status": "ok", "version": "2.0.0"}


@app.get("/v1/records/latest")
async def get_latest_record(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Get latest canonical daily record.
    
    Returns the most recent daily record. Does NOT fall back to intraday —
    intraday is an audit log, not a source of truth.
    """
    result = await db.execute(
        text("""
            SELECT device_id, date, collected_at, received_at, raw_data
            FROM health_connect_daily
            ORDER BY date DESC, collected_at DESC
            LIMIT 1
        """)
    )
    row = result.mappings().first()
    
    if not row:
        raise HTTPException(status_code=404, detail="No daily records found")
    
    return {
        "device_id": row["device_id"],
        "date": row["date"].isoformat(),
        "collected_at": row["collected_at"].isoformat(),
        "received_at": row["received_at"].isoformat(),
        "data": row["raw_data"],
    }


@app.get("/v1/records/{date}")
async def get_record_by_date(
    date: str = Path(..., description="YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Get canonical daily record for specific date."""
    result = await db.execute(
        text("""
            SELECT device_id, date, collected_at, received_at, raw_data
            FROM health_connect_daily
            WHERE date = :date
            LIMIT 1
        """),
        {"date": date},
    )
    row = result.mappings().first()
    
    if not row:
        raise HTTPException(status_code=404, detail=f"No record for {date}")
    
    return {
        "device_id": row["device_id"],
        "date": row["date"].isoformat(),
        "collected_at": row["collected_at"].isoformat(),
        "received_at": row["received_at"].isoformat(),
        "data": row["raw_data"],
    }


@app.get("/v1/records")
async def list_records(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """List canonical daily records within date range."""
    result = await db.execute(
        text("""
            SELECT device_id, date, collected_at, received_at, raw_data
            FROM health_connect_daily
            WHERE date >= :start_date AND date <= :end_date
            ORDER BY date ASC
        """),
        {"start_date": start_date, "end_date": end_date},
    )
    rows = result.mappings().all()
    
    return {
        "count": len(rows),
        "records": [
            {
                "device_id": r["device_id"],
                "date": r["date"].isoformat(),
                "collected_at": r["collected_at"].isoformat(),
                "received_at": r["received_at"].isoformat(),
                "data": r["raw_data"],
            }
            for r in rows
        ],
    }


@app.get("/v1/logs")
async def get_intraday_logs(
    date: py_date | None = None,
    device_id: str | None = None,
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Query intraday audit logs (append-only stream).
    
    Use for debugging sync issues or building time-series visualizations.
    Results ordered by collected_at DESC (newest first).
    """
    conditions = []
    params = {"limit": limit}
    
    if date:
        conditions.append("date = :date")
        params["date"] = date
    
    if device_id:
        conditions.append("device_id = :device_id")
        params["device_id"] = device_id
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    
    result = await db.execute(
        text(f"""
            SELECT id, device_id, date, collected_at, received_at, raw_data
            FROM health_connect_intraday_logs
            {where_clause}
            ORDER BY collected_at DESC
            LIMIT :limit
        """),
        params,
    )
    rows = result.mappings().all()
    
    return {
        "count": len(rows),
        "logs": [
            {
                "id": str(r["id"]),
                "device_id": r["device_id"],
                "date": r["date"].isoformat(),
                "collected_at": r["collected_at"].isoformat(),
                "received_at": r["received_at"].isoformat(),
                "data": r["raw_data"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    """Create tables on startup (dev mode). Use Alembic in production."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables created (if not exists)")
