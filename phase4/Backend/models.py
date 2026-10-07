from typing import Optional

from pydantic import BaseModel, Field
from datetime import datetime

class NetworkSummaryResponse(BaseModel):
    as_of: datetime
    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: int

class ActivityPoint(BaseModel):
    timestamp: datetime
    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    internet_activity: float
    total_sms: float
    total_calls: float
    total_activity: float
    internet_share: float

class GridActivityResponse(BaseModel):
    grid_id: int
    as_of: datetime
    date: Optional[datetime] = None
    hour_value: Optional[int] = None
    data: list[ActivityPoint]

class HotspotItem(BaseModel):
    grid_id: int
    timestamp: datetime
    total_activity: float
    sms_activity: float
    call_activity: float
    internet_activity: float
    status: str
    severity: str
    reason: str
    risk: Optional[float] = None
    risk_label: Optional[str] = None
    model_version: Optional[str] = None


class AlertItem(BaseModel):
    grid_id: int
    timestamp: datetime
    total_activity: float
    sms_activity: float
    call_activity: float
    internet_activity: float
    status: str
    severity: str
    reason: str
    risk: Optional[float] = None
    risk_label: Optional[str] = None
    model_version: Optional[str] = None


class NetworkItemsResponse(BaseModel):
    as_of: datetime
    limit: int
    data: list[HotspotItem]


class HotspotItem(BaseModel):
    grid_id: int
    timestamp: datetime
    total_activity: float
    sms_activity: float
    call_activity: float
    internet_activity: float
    status: str
    severity: str
    reason: str
    risk: Optional[float] = None
    risk_label: Optional[str] = None
    model_version: Optional[str] = None


class AlertItem(BaseModel):
    grid_id: int
    timestamp: datetime
    total_activity: float
    sms_activity: float
    call_activity: float
    internet_activity: float
    status: str
    severity: str
    reason: str
    risk: Optional[float] = None
    risk_label: Optional[str] = None
    model_version: Optional[str] = None


class NetworkItemsResponse(BaseModel):
    as_of: datetime
    limit: int
    data: list[HotspotItem]


class GridFeatureValues(BaseModel):
    avg_activity: float
    activity_growth: float
    active_hours: float
    peak_ratio: float
    variability: float
    internet_share: float


class GridFeatureResponse(BaseModel):
    grid_id: int
    feature_timestamp: datetime
    features: GridFeatureValues
    data_quality: str
    feature_freshness: str

class RiskPredictionRequest(BaseModel):
    grid_id: int = Field(..., ge=1, le=10000)
    as_of: Optional[str] = None
 
 
class RiskPredictionResponse(BaseModel):
    grid_id: int
    as_of: Optional[str]
    risk_score: float
    risk_level: str
    model_version: str
    explanation_note: str