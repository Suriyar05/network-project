import logging
from pathlib import Path
import pandas as pd
import os
 
log_dir = Path("../log")
log_dir.mkdir(parents=True, exist_ok=True)
 
log_file = log_dir / "usage_processor.log"
 
logging.basicConfig(
    filename="../log/usage_processor.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
 
logger = logging.getLogger(__name__)
 
class UsageProcessor:
    def __init__(self, dataframe):
        self.file_path = None
        self.df = None
        self.grid_hourly = None
        self.daily_summary = None
        self.grid_summary = None
 
        try:
            if isinstance(dataframe, str):
                self.file_path = dataframe
                logger.info("File path set: %s", dataframe)
 
            elif isinstance(dataframe, pd.DataFrame):
                self.df = dataframe.copy()
                logger.info("DataFrame initialized. Shape: %s", self.df.shape)
 
            else:
                raise TypeError("Expected file path or pandas DataFrame")
 
        except Exception:
            logger.exception("Error initializing UsageProcessor")
            raise
 
    def load_data(self):
        try:
            if self.file_path:
                logger.info("Loading data from: %s", self.file_path)
                self.df = pd.read_csv(self.file_path)
                logger.info("Data loaded successfully. Shape: %s", self.df.shape)
 
            return self.df
 
        except Exception:
            logger.exception("Error loading data")
            raise
 
        finally:
            assert self.df is not None
            assert isinstance(self.df, pd.DataFrame)
            assert len(self.df) > 0
 
    def clean_data(self):
        required_columns = [
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]
 
        column_mapping = {
            "datetime": "timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet"
        }
 
        self.df = self.df.rename(columns=column_mapping)
 
        self.df["timestamp"] = pd.to_datetime(
            self.df["timestamp"],
            errors="coerce"
        )
 
        logger.info(
            "Missing timestamps: %d",
            self.df["timestamp"].isna().sum()
        )
 
        logger.info(
            "Missing grid values: %d",
            self.df["grid_id"].isna().sum()
        )
 
        before = len(self.df)
 
        self.df = self.df.dropna(
            subset=["timestamp", "grid_id"]
        )
 
        logger.info(
            "Dropped %d rows due to missing timestamp/grid_id",
            before - len(self.df)
        )
 
        activity_cols = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]
 
        logger.info(
            "Missing activity values:\n%s",
            self.df[activity_cols].isna().sum()
        )
 
        logger.info(
            "Duplicated rows: %d",
            self.df.duplicated().sum()
        )
 
        logger.info(
            "Negative activity values:\n%s",
            (self.df[activity_cols] < 0).sum()
        )
 
        assert set(required_columns).issubset(self.df.columns)
        assert self.df["timestamp"].notna().all()
        assert self.df["grid_id"].notna().all()
 
        return self.df
 
    def derive_time_features(self):
        assert pd.api.types.is_datetime64_any_dtype(
            self.df["timestamp"]
        )
 
        self.df['date'] = self.df['timestamp'].dt.date
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.day_name()
 
        assert "date" in self.df.columns
        assert "hour" in self.df.columns
        assert "day_of_week" in self.df.columns
        assert self.df["hour"].between(0, 23).all()
 
        logger.info("date, hour, day_of_week columns created")
        logger.info(f"Distinct timestamps count: {self.df['timestamp'].nunique()}")
        assert self.df["timestamp"].nunique() == 24
 
        logger.info(f"Check timeintervals : {self.df['timestamp'].dropna().sort_values().unique()}")
 
        timestamps = (
            self.df["timestamp"]
            .drop_duplicates()
            .sort_values()
        )
 
        intervals = timestamps.diff().dropna()
 
        assert intervals.eq(pd.Timedelta(hours=1)).all()
 
        return self.df
   
    def aggregate_to_grid_time(self):
        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"
        ]
 
        self.grid_hourly = (
            self.df
            .groupby(["date", "hour", "grid_id"])[activity_columns]
            .sum()
            .reset_index()
        )
 
        logger.info(
            "Grid-hourly data created: %d rows",
            len(self.grid_hourly)
        )
        print(self.grid_hourly.shape[0])
 
        return self.grid_hourly
 
    def derive_activity_features(self):
        self.grid_hourly["total_sms"] = (
            self.grid_hourly["sms_in"] +
            self.grid_hourly["sms_out"]
        )
 
        self.grid_hourly["total_calls"] = (
            self.grid_hourly["call_in"] +
            self.grid_hourly["call_out"]
        )
 
        self.grid_hourly["total_activity"] = (
            self.grid_hourly["total_sms"] +
            self.grid_hourly["total_calls"] +
            self.grid_hourly["internet"]
        )
 
        assert "total_sms" in self.grid_hourly.columns
        assert "total_calls" in self.grid_hourly.columns
        assert "total_activity" in self.grid_hourly.columns
 
        return self.grid_hourly
 
    def compute_kpis(self):
        self.daily_summary = self.grid_hourly.groupby("date").agg(
            total_sms = ("total_sms","sum"),
            total_calls = ("total_calls","sum"),
            total_activity = ("total_activity","sum"),
            total_internet = ("internet","sum"),
            unique_grids = ("grid_id","nunique"),
 
        )
 
       
        self.grid_summary = self.grid_hourly.groupby("grid_id").agg(
            total_sms = ("total_sms","sum"),
            total_calls = ("total_calls","sum"),
            total_activity = ("total_activity","sum"),
            total_internet = ("internet","sum"),
            active_hours = ("hour","nunique"),
 
        )
       
        assert self.daily_summary is not None
        assert self.grid_summary is not None
 
        assert len(self.daily_summary) > 0
        assert len(self.grid_summary) > 0
 
        logger.info("KPI Computation : ", {
            "daily_summary" : self.daily_summary,
            "grid_summary" : self.grid_summary
        })
 
        assert "total_activity" in self.daily_summary.columns
        assert "total_activity" in self.grid_summary.columns
 
        return {
            "daily_summary" : self.daily_summary,
            "grid_summary" : self.grid_summary
        }
 
    def export_summary(self, grid_hourly_path, daily_path, grid_path):
        self.grid_hourly.to_csv(grid_hourly_path, index=False)
        self.grid_summary.to_csv(grid_path,index=False)
        self.daily_summary.to_csv(daily_path,index=False)
 
        assert Path(grid_hourly_path).exists()
        assert Path(grid_path).exists()
        assert Path(daily_path).exists()
 
        logger.info("Exported grid-hourly data to %s", grid_hourly_path)
        logger.info("Exported grid-summary data to %s", grid_path)
        logger.info("Exported daily-summary data to %s", daily_path)
 
if __name__ == "__main__":
    usage_processor = UsageProcessor("../../Dataset/archive (1)/sms-call-internet-mi-2013-11-01.csv")
    usage_processor.load_data()
 
    cleaned_df = usage_processor.clean_data()
    time_features_df = usage_processor.derive_time_features() # updated clean df
 
    grid_hourly_grid_df = usage_processor.aggregate_to_grid_time()
    drive_grid_hour_df = usage_processor.derive_activity_features()
 
    kpis = usage_processor.compute_kpis()
 
    #Export
    output_dir = "../../Dataset/landing/"
    grid_hourly_file_path = output_dir + "grid_hourly.csv"
    daily_file_path = output_dir + "daily_summary.csv"
    grid_summary_file_path = output_dir + "grid_summary.csv"
 
    usage_processor.export_summary(
        grid_hourly_path = grid_hourly_file_path,
        grid_path = grid_summary_file_path,
        daily_path = daily_file_path
    )
 