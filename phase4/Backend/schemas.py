from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, BigInteger, String, Index
from sqlalchemy.orm import relationship
from database import Base

class DimTime(Base):
    __tablename__ = "dim_time"

    time_key = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, nullable=False, unique=True, index=True)
    date_value = Column(Date, nullable=False)
    year_value = Column(Integer, nullable=False)
    month_value = Column(Integer, nullable=False)
    day_value = Column(Integer, nullable=False)
    hour_value = Column(Integer, nullable=False)

    network_activities = relationship("FactNetworkActivity", back_populates="time")


class DimGrid(Base):
    __tablename__ = "dim_grid"

    grid_id = Column(Integer, primary_key=True)
    centroid_latitude = Column(Float, nullable=True)
    centroid_longitude = Column(Float, nullable=True)
    geometry_reference = Column(String(255), nullable=True)

    network_activities = relationship("FactNetworkActivity", back_populates="grid")


class FactNetworkActivity(Base):
    __tablename__ = "fact_network_activity"

    activity_id = Column(BigInteger, primary_key=True, autoincrement=True)
    grid_id = Column(Integer, ForeignKey("dim_grid.grid_id"), nullable=False)
    time_key = Column(BigInteger, ForeignKey("dim_time.time_key"), nullable=False)
    sms_in = Column(Float, nullable=False)
    sms_out = Column(Float, nullable=False)
    call_in = Column(Float, nullable=False)
    call_out = Column(Float, nullable=False)
    internet_activity = Column(Float, nullable=False)
    total_sms = Column(Float, nullable=False)
    total_calls = Column(Float, nullable=False)
    total_activity = Column(Float, nullable=False)
    internet_share = Column(Float, nullable=False)

    time = relationship("DimTime", back_populates="network_activities")
    grid = relationship("DimGrid", back_populates="network_activities")


Index("idx_fact_network_activity_time", FactNetworkActivity.time_key)
Index("idx_fact_network_activity_grid", FactNetworkActivity.grid_id)
Index("idx_fact_network_activity_grid_time", FactNetworkActivity.grid_id, FactNetworkActivity.time_key)
