from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class SourceSchema(BaseModel):
    device_id: str
    collected_at: datetime

    model_config = {"extra": "allow"}


class BodyMetricsSchema(BaseModel):
    # Core fields we expect
    weight_kg: Optional[float] = None
    body_fat_percentage: Optional[float] = None
    
    # Allow any additional fields from smart scales
    model_config = {"extra": "allow"}


class NutritionSummarySchema(BaseModel):
    calories_total: Optional[int] = None
    protein_grams: Optional[float] = None
    
    # Allow additional nutrition fields (carbs, fat, etc.)
    model_config = {"extra": "allow"}


class ExerciseSessionSchema(BaseModel):
    start_time: str
    end_time: str
    duration_minutes: int
    title: Optional[str] = None
    notes: Optional[str] = None
    
    # Allow additional exercise data (calories, heart rate zones, etc.)
    model_config = {"extra": "allow"}


class HeartRateSummarySchema(BaseModel):
    avg_hr: Optional[int] = None
    min_hr: Optional[int] = None
    max_hr: Optional[int] = None
    resting_hr: Optional[int] = None
    
    # Allow additional heart rate fields (zones, variability, etc.)
    model_config = {"extra": "allow"}


class SleepSessionSchema(BaseModel):
    start_time: str
    end_time: str
    duration_minutes: int
    
    # Allow additional sleep data (stages, efficiency, etc.)
    model_config = {"extra": "allow"}


class DailyIngestRequest(BaseModel):
    schema_version: int = Field(default=1)
    date: date
    steps_total: int
    source: SourceSchema
    
    # All metrics fields — optional and extensible
    sleep_sessions: Optional[Union[Dict[str, Any], List[Any]]] = None
    heart_rate_summary: Optional[HeartRateSummarySchema] = None
    body_metrics: Optional[BodyMetricsSchema] = None
    nutrition_summary: Optional[NutritionSummarySchema] = None
    exercise_sessions: Optional[List[ExerciseSessionSchema]] = None
    
    # Catch-all for any additional fields Android sends
    model_config = {"extra": "allow"}

    @field_validator("steps_total")
    @classmethod
    def validate_steps(cls, v: int) -> int:
        if v < 0:
            raise ValueError("steps_total cannot be negative")
        return v

    @field_validator("date")
    @classmethod
    def validate_date_not_future(cls, v: date) -> date:
        if v > datetime.now(timezone.utc).date():
            raise ValueError("Date cannot be in the future")
        return v


class IngestResponse(BaseModel):
    status: str = "ok"
    upserted: bool = True
