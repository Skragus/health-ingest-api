import logging
import asyncio
import httpx
import uuid
from datetime import date as py_date

from fastapi import Depends, FastAPI, Header, HTTPException, status, Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, case

from app.config import settings
from app.database import Base, engine, get_db
from app.models import ShealthDaily, HealthConnectDaily, HealthConnectIntradayLog
from app.schemas import DailyIngestRequest, IngestResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("shealth-ingest")

# ---------------------------------------------------------------------------
# Telegram Notification
# ---------------------------------------------------------------------------
async def send_telegram_notification(sync_type: str, payload: DailyIngestRequest):
    """Send formatted sync notification to Telegram."""
    try:
        # Build message
        msg = f"✅ {sync_type.title()} Sync\n"
        msg += f"📅 {payload.date}\n"
        
        if payload.steps_total and payload.steps_total > 0:
            msg += f"🚶 {payload.steps_total:,} steps\n"
        
        if payload.body_metrics:
            msg += f"⚖️ {payload.body_metrics.weight_kg:.1f} kg"
            if payload.body_metrics.body_fat_percentage:
                msg += f" ({payload.body_metrics.body_fat_percentage:.1f}% BF)"
            msg += "\n"
        
        if payload.exercise_sessions:
            msg += f"💪 {len(payload.exercise_sessions)} workout(s)\n"
            for ex in payload.exercise_sessions:
                msg += f"   • {ex.title} ({ex.duration_minutes} min)\n"
        
        if payload.nutrition_summary:
            msg += f"🍽️ {payload.nutrition_summary.calories_total} cal"
            if payload.nutrition_summary.protein_grams:
                msg += f" ({payload.nutrition_summary.protein_grams:.1f}g protein)"
            msg += "\n"
        
        # Send to Telegram
        url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={
                "chat_id": settings.TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            }, timeout=5.0)
        
        logger.info(f"Sent Telegram notification for {sync_type} sync")
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")
        # Don't raise - notification failure shouldn't break the sync

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Samsung Health Truth Layer")


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------
async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return x_api_key


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    await db.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/debug/status")
async def debug_status(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Debug endpoint to check database status and recent ingests."""
    try:
        # Total record count
        count_result = await db.execute(
            text("SELECT COUNT(*) as total FROM shealth_daily")
        )
        total_records = count_result.scalar()

        # Count by source type
        type_result = await db.execute(
            text("""
                SELECT source_type, COUNT(*) as count 
                FROM shealth_daily 
                GROUP BY source_type
            """)
        )
        by_type = {row[0]: row[1] for row in type_result.fetchall()}

        # Last 10 records
        recent_result = await db.execute(
            text("""
                SELECT date, device_id, steps_total, source_type, 
                       collected_at, received_at
                FROM shealth_daily 
                ORDER BY received_at DESC 
                LIMIT 10
            """)
        )
        recent_records = [
            {
                "date": str(row[0]),
                "device_id": row[1],
                "steps_total": row[2],
                "source_type": row[3],
                "collected_at": row[4].isoformat() if row[4] else None,
                "received_at": row[5].isoformat() if row[5] else None,
            }
            for row in recent_result.fetchall()
        ]

        # Last ingest timestamp
        last_ingest = recent_records[0]["received_at"] if recent_records else None

        return {
            "status": "ok",
            "total_records": total_records,
            "by_source_type": by_type,
            "last_ingest_at": last_ingest,
            "recent_ingests": recent_records,
        }
    except Exception as e:
        logger.error("Debug status failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Shared upsert helper
# ---------------------------------------------------------------------------
def _serialize_optional(obj):
    """model_dump a Pydantic object if present, else None."""
    return obj.model_dump(mode="json") if obj else None


def _validate_body_metrics(metrics):
    """Validate body metrics and reject garbage data."""
    if not metrics:
        return None
    
    data = metrics.model_dump(mode="json") if hasattr(metrics, 'model_dump') else metrics
    
    weight = data.get('weight_kg')
    body_fat = data.get('body_fat_percentage')
    
    # Validate weight (30-300 kg is reasonable human range)
    if weight is not None and (weight < 30 or weight > 300):
        logger.warning(f"Rejecting invalid weight: {weight} kg")
        return None
    
    # Validate body fat percentage (3-70% is reasonable range)
    if body_fat is not None and (body_fat < 3 or body_fat > 70):
        logger.warning(f"Rejecting invalid body fat: {body_fat}%")
        return None
    
    return data


def _validate_heart_rate_summary(summary):
    """Validate heart rate summary and reject garbage data."""
    if not summary:
        return None
    
    data = summary.model_dump(mode="json") if hasattr(summary, 'model_dump') else summary
    
    # Check common heart rate fields
    for field in ['min_bpm', 'max_bpm', 'avg_bpm', 'resting_bpm']:
        value = data.get(field)
        if value is not None and (value < 30 or value > 250):
            logger.warning(f"Rejecting invalid heart rate ({field}): {value} bpm")
            return None
    
    return data


def _validate_nutrition_summary(summary):
    """Validate nutrition summary and reject garbage data."""
    if not summary:
        return None
    
    data = summary.model_dump(mode="json") if hasattr(summary, 'model_dump') else summary
    
    total_calories = data.get('total_calories')
    
    # Validate calories (0-10000 per day is reasonable)
    if total_calories is not None and (total_calories < 0 or total_calories > 10000):
        logger.warning(f"Rejecting invalid calories: {total_calories}")
        return None
    
    # Validate macros are non-negative
    for macro in ['protein_g', 'carbs_g', 'fat_g']:
        value = data.get(macro)
        if value is not None and value < 0:
            logger.warning(f"Rejecting invalid macro ({macro}): {value}g")
            return None
    
    return data


async def _upsert_shealth(
    payload: DailyIngestRequest,
    source_type: str,
    db: AsyncSession,
):
    """Build and execute an idempotent upsert for shealth_daily (Legacy) 
    AND the new HealthConnectDaily / HealthConnectIntradayLog structure.
    
    New approach: Store raw JSON payload for flexibility while keeping
    core fields queryable in the database."""
    
    # Build the raw payload from the entire request
    # This captures everything Android sends, including new fields
    raw_payload = payload.model_dump(mode="json")
    
    # Core fields for querying (extracted for SQL access)
    core_data = {
        "device_id": payload.source.device_id,
        "date": payload.date,
        "schema_version": payload.schema_version,
        "steps_total": payload.steps_total,
        "source_type": source_type,
        "collected_at": payload.source.collected_at,
        "received_at": func.now(),
        "raw_data": raw_payload,  # Everything lives here
        "source": payload.source.model_dump(mode="json"),
    }

    # 1. LEGACY: Upsert to shealth_daily
    stmt_legacy = insert(ShealthDaily).values(**ingest_data)
    stmt_legacy = stmt_legacy.on_conflict_do_update(
        constraint="uq_device_date_version",
        set_={
            "steps_total": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.steps_total), else_=ShealthDaily.steps_total),
            "sleep_sessions": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.sleep_sessions), else_=ShealthDaily.sleep_sessions),
            "heart_rate_summary": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.heart_rate_summary), else_=ShealthDaily.heart_rate_summary),
            "body_metrics": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.body_metrics), else_=ShealthDaily.body_metrics),
            "nutrition_summary": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.nutrition_summary), else_=ShealthDaily.nutrition_summary),
            "exercise_sessions": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.exercise_sessions), else_=ShealthDaily.exercise_sessions),
            "source": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.source), else_=ShealthDaily.source),
            "source_type": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.source_type), else_=ShealthDaily.source_type),
            "collected_at": case((stmt_legacy.excluded.collected_at > ShealthDaily.collected_at, stmt_legacy.excluded.collected_at), else_=ShealthDaily.collected_at),
        },
    )

    # 2. NEW: Upsert to health_connect_daily (raw_data approach)
    stmt_daily = insert(HealthConnectDaily).values(**core_data)
    stmt_daily = stmt_daily.on_conflict_do_update(
        constraint="uq_health_connect_daily_device_date_version",
        set_={
            "steps_total": case((stmt_daily.excluded.collected_at > HealthConnectDaily.collected_at, stmt_daily.excluded.steps_total), else_=HealthConnectDaily.steps_total),
            "raw_data": case((stmt_daily.excluded.collected_at > HealthConnectDaily.collected_at, stmt_daily.excluded.raw_data), else_=HealthConnectDaily.raw_data),
            "source": case((stmt_daily.excluded.collected_at > HealthConnectDaily.collected_at, stmt_daily.excluded.source), else_=HealthConnectDaily.source),
            "source_type": case((stmt_daily.excluded.collected_at > HealthConnectDaily.collected_at, stmt_daily.excluded.source_type), else_=HealthConnectDaily.source_type),
            "collected_at": case((stmt_daily.excluded.collected_at > HealthConnectDaily.collected_at, stmt_daily.excluded.collected_at), else_=HealthConnectDaily.collected_at),
            "received_at": func.now(),  # Always update received_at on upsert
        },
    )

    # 3. NEW: Append to health_connect_intraday_logs (NO UPSERT, just INSERT)
    stmt_log = insert(HealthConnectIntradayLog).values(**core_data)

    try:
        await db.execute(stmt_legacy)
        await db.execute(stmt_daily)
        await db.execute(stmt_log)
        await db.commit()
        logger.info(
            "Ingest OK [%s]: device=%s date=%s steps=%d",
            source_type,
            payload.source.device_id,
            payload.date,
            payload.steps_total,
        )
        return IngestResponse()
    except Exception as e:
        logger.error("Ingest failed [%s]: %s", source_type, e)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error during sync",
        )


# ---------------------------------------------------------------------------
# Daily — canonical/final reconciliation for past dates
# ---------------------------------------------------------------------------
@app.post("/v1/ingest/shealth/daily", response_model=IngestResponse)
async def ingest_daily(
    payload: DailyIngestRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    logger.info("Received DAILY sync request for date=%s from device=%s", payload.date, payload.source.device_id)
    result = await _upsert_shealth(payload, source_type="daily", db=db)
    
    # Send Telegram notification asynchronously (don't block response)
    asyncio.create_task(send_telegram_notification("daily", payload))
    
    return result


# ---------------------------------------------------------------------------
# Intraday — provisional/hot cumulative snapshot for today
# ---------------------------------------------------------------------------
@app.post("/v1/ingest/shealth/intraday", response_model=IngestResponse)
async def ingest_intraday(
    payload: DailyIngestRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    logger.info("Received INTRADAY sync request for date=%s from device=%s", payload.date, payload.source.device_id)
    result = await _upsert_shealth(payload, source_type="intraday", db=db)
    
    # Send Telegram notification asynchronously (don't block response)
    asyncio.create_task(send_telegram_notification("intraday", payload))
    
    return result


# ---------------------------------------------------------------------------
# Startup — create tables (dev only; use Alembic in prod)
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ---------------------------------------------------------------------------
# Agent endpoints — for external API / AI agent queries
# ---------------------------------------------------------------------------
@app.get("/health/connect/latest")
async def get_latest_health_record(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Retrieve the absolute latest Health Connect record, prioritizing higher step counts for today."""
    try:
        # 1. Get the latest record from the summary table
        daily_query = await db.execute(
            text("SELECT * FROM health_connect_daily ORDER BY date DESC, collected_at DESC LIMIT 1")
        )
        daily_record = daily_query.mappings().first()
        
        if not daily_record:
            return {"status": "no_data", "message": "Database is empty"}
        
        record_dict = dict(daily_record)
        record_dict["id"] = str(record_dict["id"])
        
        # Expand raw_data into the response for backwards compatibility
        # This merges the full payload (body_metrics, nutrition, etc.) into the response
        raw_data = record_dict.pop("raw_data", {})
        if raw_data:
            # Merge raw_data fields, but don't overwrite core fields
            for key, value in raw_data.items():
                if key not in record_dict or record_dict[key] is None:
                    record_dict[key] = value
        
        # 2. Get the highest step count from intraday logs for the same date
        intraday_query = await db.execute(
            text("""
                SELECT MAX(steps_total) as max_steps, MAX(collected_at) as max_collected
                FROM health_connect_intraday_logs 
                WHERE date = :date
            """),
            {"date": record_dict["date"]}
        )
        log_data = intraday_query.mappings().first()
        
        if log_data and log_data["max_steps"] is not None:
            # Use whichever step count is higher
            if log_data["max_steps"] > record_dict["steps_total"]:
                record_dict["steps_total"] = log_data["max_steps"]
            
            # Use whichever timestamp is newer
            if log_data["max_collected"] > record_dict["collected_at"]:
                record_dict["collected_at"] = log_data["max_collected"]
                
        return record_dict
    except Exception as e:
        logger.error(f"Failed to fetch latest record: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


from datetime import date as py_date

# ... existing imports ...

@app.get("/health/connect/range")
async def get_health_connect_range(
    start: py_date,
    end: py_date,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Retrieve Health Connect records within a date range (YYYY-MM-DD)."""
    try:
        query = await db.execute(
            text("""
                SELECT * FROM health_connect_daily 
                WHERE date >= :start AND date <= :end 
                ORDER BY date ASC
            """),
            {"start": start, "end": end}
        )
        records = query.mappings().all()
        
        results = []
        for row in records:
            d = dict(row)
            d["id"] = str(d["id"])
            # Expand raw_data into the response
            raw_data = d.pop("raw_data", {})
            if raw_data:
                for key, value in raw_data.items():
                    if key not in d or d[key] is None:
                        d[key] = value
            results.append(d)
            
        return results
    except Exception as e:
        logger.error(f"Failed to fetch range {start} to {end}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


@app.get("/health/connect/by-date/{date}")
async def get_health_connect_by_date(
    date: str = Path(..., description="Date in YYYY-MM-DD format"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Retrieve the Health Connect record for a specific date."""
    try:
        query = await db.execute(
            text("SELECT * FROM health_connect_daily WHERE date = :date ORDER BY collected_at DESC LIMIT 1"),
            {"date": date}
        )
        record = query.mappings().first()
        
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No record found for date {date}"
            )
            
        record_dict = dict(record)
        record_dict["id"] = str(record_dict["id"])
        # Expand raw_data into the response
        raw_data = record_dict.pop("raw_data", {})
        if raw_data:
            for key, value in raw_data.items():
                if key not in record_dict or record_dict[key] is None:
                    record_dict[key] = value
        return record_dict
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch date {date}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


@app.get("/health/connect/{record_id}")
async def get_health_connect_record(
    record_id: uuid.UUID = Path(..., description="UUID of the Health Connect record"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """Retrieve a specific Health Connect record by its UUID."""
    try:
        # Try summary table first, then logs
        query = await db.execute(
            text("SELECT * FROM health_connect_daily WHERE id = :id"),
            {"id": record_id}
        )
        record = query.mappings().first()
        
        if not record:
            query = await db.execute(
                text("SELECT * FROM health_connect_intraday_logs WHERE id = :id"),
                {"id": record_id}
            )
            record = query.mappings().first()
        
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Record {record_id} not found"
            )
        
        # Convert UUID to string for JSON serialization
        record_dict = dict(record)
        record_dict["id"] = str(record_dict["id"])
        
        # Expand raw_data into the response
        raw_data = record_dict.pop("raw_data", {})
        if raw_data:
            for key, value in raw_data.items():
                if key not in record_dict or record_dict[key] is None:
                    record_dict[key] = value
        
        return record_dict
    except Exception as e:
        logger.error(f"Failed to fetch record {record_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
