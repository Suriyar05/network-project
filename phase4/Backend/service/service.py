from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from schemas import DimTime, FactNetworkActivity

def get_as_of(db: Session, as_of: Optional[datetime]) -> datetime:
    if as_of:
        return as_of

    result = db.query(func.max(DimTime.timestamp)).scalar()

    if result is None:
        raise HTTPException(
            status_code=500,
            detail="Analytics data source unavailable"
        )

    return result


def get_activity_rows(db: Session, as_of: datetime, limit: int):
    return (
        db.query(
            FactNetworkActivity.grid_id,
            DimTime.timestamp,
            FactNetworkActivity.sms_in,
            FactNetworkActivity.sms_out,
            FactNetworkActivity.call_in,
            FactNetworkActivity.call_out,
            FactNetworkActivity.internet_activity,
            FactNetworkActivity.total_activity
        )
        .join(
            DimTime,
            FactNetworkActivity.time_key == DimTime.time_key
        )
        .filter(DimTime.timestamp <= as_of)
        .order_by(FactNetworkActivity.total_activity.desc())
        .limit(limit)
        .all()
    )


def build_item(row, severity: str, reason: str):
    sms = float(row.sms_in or 0) + float(row.sms_out or 0)
    calls = float(row.call_in or 0) + float(row.call_out or 0)
    internet = float(row.internet_activity or 0)
    total = float(row.total_activity or 0)

    return {
        "grid_id": int(row.grid_id),
        "timestamp": row.timestamp,
        "total_activity": total,
        "sms_activity": sms,
        "call_activity": calls,
        "internet_activity": internet,
        "status": "hotspot" if severity != "low" else "normal",
        "severity": severity,
        "reason": reason,
        "risk": None,
        "risk_label": None,
        "model_version": None
    }

