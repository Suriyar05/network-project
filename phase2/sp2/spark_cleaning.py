from pathlib import Path
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    dayofweek,
    lit,
    when
)
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = PROJECT_ROOT / "logs"

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_DIR / "sp2_cleaning.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK NETWORK CLEANER
# ============================================================

class SparkNetworkCleaner:

    """
    SP2 - Cleaning and Standardization.

    Input grain:
        datetime + CellID + countrycode

    Curated grain:
        timestamp + grid_id + country_code

    Important:
        SP2 does NOT aggregate country-code rows.

    Aggregation happens in the next processing stage.
    """

    # ========================================================
    # RAW COLUMNS
    # ========================================================

    RAW_COLUMNS = [
        "datetime",
        "CellID",
        "countrycode",
        "smsin",
        "smsout",
        "callin",
        "callout",
        "internet"
    ]

    # ========================================================
    # ACTIVITY COLUMNS
    # ========================================================

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(
        self,
        input_path=None,
        spark=None
    ):

        self.spark = spark

        if input_path is None:

            self.input_path = str(
                RAW_DIR /
                "sms-call-internet-mi-*.csv"
            )

        else:

            self.input_path = input_path

        self.raw_df = None

        self.canonical_df = None

        self.clean_network_df = None

        self.rejected_df = None

        self.input_row_count = 0

        self.output_row_count = 0

        self.rejected_row_count = 0

        self.null_handled_count = 0

        self.null_counts_by_column = {}

    # ========================================================
    # 1. CREATE SPARK SESSION
    # ========================================================

    def create_spark_session(self):

        if self.spark is not None:

            logger.info(
                "Using existing SparkSession."
            )

            return self.spark

        self.spark = (
            SparkSession
            .builder
            .appName(
                "SP2-Cleaning"
            )
            .master("local[*]")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel(
            "WARN"
        )

        logger.info(
            "SparkSession created successfully."
        )

        logger.info(
            "Spark version: %s",
            self.spark.version
        )

        return self.spark

    # ========================================================
    # 2. LOAD RAW DATA
    # ========================================================

    def load_data(self):

        if self.spark is None:

            self.create_spark_session()

        logger.info(
            "Loading raw files:"
        )

        logger.info(
            "%s",
            self.input_path
        )

        self.raw_df = (
            self.spark
            .read
            .option(
                "header",
                True
            )
            .option(
                "inferSchema",
                True
            )
            .csv(
                self.input_path
            )
        )

        self.input_row_count = (
            self.raw_df.count()
        )

        logger.info(
            "Input rows: %d",
            self.input_row_count
        )

        logger.info(
            "Raw columns: %s",
            self.raw_df.columns
        )

        return self.raw_df

    # ========================================================
    # 3. VALIDATE RAW COLUMNS
    # ========================================================

    def validate_input_columns(self):

        if self.raw_df is None:

            raise RuntimeError(
                "Run load_data() first."
            )

        missing_columns = [
            column
            for column in self.RAW_COLUMNS
            if column not in self.raw_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Missing raw columns: "
                f"{missing_columns}"
            )

        logger.info(
            "Raw column validation: PASS"
        )

    # ========================================================
    # 4. RENAME TO CANONICAL NAMES
    # ========================================================

    def rename_columns(self):

        if self.raw_df is None:

            raise RuntimeError(
                "Run load_data() first."
            )

        logger.info(
            "Renaming raw columns."
        )

        self.canonical_df = (
            self.raw_df

            .withColumnRenamed(
                "CellID",
                "grid_id"
            )

            .withColumnRenamed(
                "countrycode",
                "country_code"
            )

            .withColumnRenamed(
                "smsin",
                "sms_in"
            )

            .withColumnRenamed(
                "smsout",
                "sms_out"
            )

            .withColumnRenamed(
                "callin",
                "call_in"
            )

            .withColumnRenamed(
                "callout",
                "call_out"
            )

            .withColumnRenamed(
                "internet",
                "internet_activity"
            )
        )

        logger.info(
            "Raw-to-canonical renaming completed."
        )

        return self.canonical_df

    # ========================================================
    # 5. CREATE TIMESTAMP
    # ========================================================

    def create_timestamp(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run rename_columns() first."
            )

        logger.info(
            "Creating timestamp from datetime."
        )

        self.canonical_df = (
            self.canonical_df
            .withColumn(
                "timestamp",
                to_timestamp(
                    col("datetime")
                )
            )
        )

        logger.info(
            "Timestamp created successfully."
        )

        return self.canonical_df

    # ========================================================
    # 6. CAST DATA TYPES
    # ========================================================

    def cast_data_types(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run create_timestamp() first."
            )

        logger.info(
            "Casting data types."
        )

        self.canonical_df = (
            self.canonical_df

            .withColumn(
                "timestamp",
                col("timestamp")
                .cast(TimestampType())
            )

            .withColumn(
                "grid_id",
                col("grid_id")
                .cast(IntegerType())
            )

            .withColumn(
                "country_code",
                col("country_code")
                .cast(IntegerType())
            )

            .withColumn(
                "sms_in",
                col("sms_in")
                .cast(DoubleType())
            )

            .withColumn(
                "sms_out",
                col("sms_out")
                .cast(DoubleType())
            )

            .withColumn(
                "call_in",
                col("call_in")
                .cast(DoubleType())
            )

            .withColumn(
                "call_out",
                col("call_out")
                .cast(DoubleType())
            )

            .withColumn(
                "internet_activity",
                col("internet_activity")
                .cast(DoubleType())
            )
        )

        logger.info(
            "Data type casting completed."
        )

        return self.canonical_df

    # ========================================================
    # 7. PROFILE ACTIVITY NULLS
    # ========================================================

    def profile_nulls(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run cast_data_types() first."
            )

        logger.info(
            "Profiling activity nulls."
        )

        self.null_counts_by_column = {}

        self.null_handled_count = 0

        for column in self.ACTIVITY_COLUMNS:

            null_count = (
                self.canonical_df
                .filter(
                    col(column).isNull()
                )
                .count()
            )

            self.null_counts_by_column[
                column
            ] = null_count

            self.null_handled_count += (
                null_count
            )

            logger.info(
                "Nulls in %s: %d",
                column,
                null_count
            )

        logger.info(
            "Total activity nulls: %d",
            self.null_handled_count
        )

        return self.null_counts_by_column

    # ========================================================
    # 8. QUARANTINE INVALID RECORDS
    # ========================================================

    def quarantine_invalid_records(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run cast_data_types() first."
            )

        logger.info(
            "Checking invalid records."
        )

        missing_grid = (
            col("grid_id").isNull()
        )

        missing_timestamp = (
            col("timestamp").isNull()
        )

        negative_activity = (
            (col("sms_in") < 0)
            |
            (col("sms_out") < 0)
            |
            (col("call_in") < 0)
            |
            (col("call_out") < 0)
            |
            (col("internet_activity") < 0)
        )

        rejected_condition = (
            missing_grid
            |
            missing_timestamp
            |
            negative_activity
        )

        # ----------------------------------------------------
        # Save rejected records
        # ----------------------------------------------------

        self.rejected_df = (
            self.canonical_df
            .filter(
                rejected_condition
            )
        )

        self.rejected_row_count = (
            self.rejected_df.count()
        )

        # ----------------------------------------------------
        # Keep valid records
        # ----------------------------------------------------

        self.canonical_df = (
            self.canonical_df
            .filter(
                ~rejected_condition
            )
        )

        logger.info(
            "Rejected rows: %d",
            self.rejected_row_count
        )

        logger.info(
            "Quarantine completed."
        )

        return self.rejected_df

    # ========================================================
    # 9. CURATED NULL → ZERO
    # ========================================================

    def apply_curated_null_rule(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run quarantine_invalid_records() first."
            )

        logger.info(
            "Applying curated-layer null-to-zero rule."
        )

        for column in self.ACTIVITY_COLUMNS:

            self.canonical_df = (
                self.canonical_df
                .withColumn(
                    column,
                    when(
                        col(column).isNull(),
                        lit(0.0)
                    )
                    .otherwise(
                        col(column)
                    )
                    .cast(DoubleType())
                )
            )

        self.clean_network_df = (
            self.canonical_df
        )

        logger.info(
            "Curated null-to-zero rule applied."
        )

        logger.info(
            "Activity nulls handled: %d",
            self.null_handled_count
        )

        return self.clean_network_df

    # ========================================================
    # 10. VALIDATE GRID IDs
    # ========================================================

    def validate_grid_ids(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run apply_curated_null_rule() first."
            )

        invalid_count = (
            self.clean_network_df
            .filter(
                (col("grid_id") < 1)
                |
                (col("grid_id") > 10000)
            )
            .count()
        )

        if invalid_count > 0:

            raise ValueError(
                f"Found {invalid_count} invalid grid IDs."
            )

        logger.info(
            "Grid ID validation: PASS"
        )

    # ========================================================
    # 11. VALIDATE NEGATIVE ACTIVITY
    # ========================================================

    def validate_negative_activity(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run apply_curated_null_rule() first."
            )

        negative_count = (
            self.clean_network_df
            .filter(
                (col("sms_in") < 0)
                |
                (col("sms_out") < 0)
                |
                (col("call_in") < 0)
                |
                (col("call_out") < 0)
                |
                (col("internet_activity") < 0)
            )
            .count()
        )

        if negative_count > 0:

            raise ValueError(
                f"Negative activity remains: "
                f"{negative_count}"
            )

        logger.info(
            "Negative activity validation: PASS"
        )

    # ========================================================
    # 12. DERIVE ACTIVITY FEATURES
    # ========================================================

    def derive_activity_features(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run apply_curated_null_rule() first."
            )

        logger.info(
            "Creating activity features."
        )

        self.clean_network_df = (
            self.clean_network_df

            .withColumn(
                "total_sms",
                col("sms_in")
                +
                col("sms_out")
            )

            .withColumn(
                "total_calls",
                col("call_in")
                +
                col("call_out")
            )

            .withColumn(
                "total_activity",
                col("total_sms")
                +
                col("total_calls")
                +
                col("internet_activity")
            )
        )

        logger.info(
            "total_sms created."
        )

        logger.info(
            "total_calls created."
        )

        logger.info(
            "total_activity created."
        )

        return self.clean_network_df

    # ========================================================
    # 13. DERIVE TIME FEATURES
    # ========================================================

    def derive_time_features(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run derive_activity_features() first."
            )

        logger.info(
            "Creating date, hour and day_of_week."
        )

        self.clean_network_df = (
            self.clean_network_df

            .withColumn(
                "date",
                to_date(
                    col("timestamp")
                )
            )

            .withColumn(
                "hour",
                hour(
                    col("timestamp")
                )
            )

            .withColumn(
                "day_of_week",
                dayofweek(
                    col("timestamp")
                )
            )
        )

        logger.info(
            "Time features created."
        )

        return self.clean_network_df

    # ========================================================
    # 14. VERIFY HOURLY CADENCE
    # ========================================================

    def verify_hourly_cadence(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run derive_time_features() first."
            )

        logger.info(
            "Verifying hourly cadence."
        )

        distinct_dates = (
            self.clean_network_df
            .select("date")
            .distinct()
            .count()
        )

        distinct_timestamps = (
            self.clean_network_df
            .select("timestamp")
            .distinct()
            .count()
        )

        expected_timestamps = (
            distinct_dates * 24
        )

        logger.info(
            "Distinct dates: %d",
            distinct_dates
        )

        logger.info(
            "Distinct timestamps: %d",
            distinct_timestamps
        )

        logger.info(
            "Expected timestamps: %d",
            expected_timestamps
        )

        if (
            distinct_timestamps
            != expected_timestamps
        ):

            raise ValueError(
                "Hourly cadence validation failed."
            )

        logger.info(
            "Hourly cadence validation: PASS"
        )

    # ========================================================
    # 15. GENERATE REPORT
    # ========================================================

    def generate_report(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Cleaning has not completed."
            )

        self.output_row_count = (
            self.clean_network_df.count()
        )

        logger.info(
            "============================================"
        )

        logger.info(
            "SP2 CLEANING REPORT"
        )

        logger.info(
            "============================================"
        )

        logger.info(
            "Input rows: %d",
            self.input_row_count
        )

        logger.info(
            "Rejected rows: %d",
            self.rejected_row_count
        )

        logger.info(
            "Activity nulls handled: %d",
            self.null_handled_count
        )

        logger.info(
            "Output rows: %d",
            self.output_row_count
        )

        return {
            "input_rows":
                self.input_row_count,

            "rejected_rows":
                self.rejected_row_count,

            "activity_nulls_handled":
                self.null_handled_count,

            "output_rows":
                self.output_row_count
        }

    # ========================================================
    # 16. EXPORT CLEAN DATA
    # ========================================================

    def export_clean_data(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Nothing to export."
            )

        output_path = (
            OUTPUT_DIR /
            "clean_network"
        )

        logger.info(
            "Writing clean data to: %s",
            output_path
        )

        (
            self.clean_network_df
            .write
            .mode("overwrite")
            .parquet(
                str(output_path)
            )
        )

        logger.info(
            "Clean data exported successfully."
        )

        return output_path

    # ========================================================
    # 17. EXPORT REJECTED DATA
    # ========================================================

    def export_rejected_data(self):

        if self.rejected_df is None:

            raise RuntimeError(
                "No rejected data available."
            )

        output_path = (
            OUTPUT_DIR /
            "rejected_records"
        )

        logger.info(
            "Writing rejected records to: %s",
            output_path
        )

        (
            self.rejected_df
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(output_path)
            )
        )

        logger.info(
            "Rejected records exported."
        )

        return output_path

    # ========================================================
    # 18. EXPORT REPORT
    # ========================================================

    def export_report(
        self,
        report
    ):

        report_path = (
            OUTPUT_DIR /
            "sp2_cleaning_report.txt"
        )

        with open(
            report_path,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP2 CLEANING AND STANDARDIZATION REPORT\n"
            )

            file.write(
                "=" * 55
                + "\n\n"
            )

            file.write(
                f"Input rows: "
                f"{report['input_rows']}\n"
            )

            file.write(
                f"Rejected rows: "
                f"{report['rejected_rows']}\n"
            )

            file.write(
                f"Activity nulls handled: "
                f"{report['activity_nulls_handled']}\n"
            )

            file.write(
                f"Output rows: "
                f"{report['output_rows']}\n"
            )

            file.write(
                "\nNULL POLICY\n"
            )

            file.write(
                "-" * 30
                + "\n"
            )

            file.write(
                "Blank/null activity measures are "
                "converted to zero only in the "
                "curated layer.\n"
            )

            file.write(
                "The raw DataFrame remains unchanged.\n"
            )

            file.write(
                "\nACTIVITY FEATURES\n"
            )

            file.write(
                "-" * 30
                + "\n"
            )

            file.write(
                "total_sms = sms_in + sms_out\n"
            )

            file.write(
                "total_calls = call_in + call_out\n"
            )

            file.write(
                "total_activity = total_sms + "
                "total_calls + internet_activity\n"
            )

        logger.info(
            "Report written to: %s",
            report_path
        )

        return report_path

    # ========================================================
    # 19. COMPLETE PIPELINE
    # ========================================================

    def run(self):

        logger.info(
            "============================================"
        )

        logger.info(
            "SP2 CLEANING STARTED"
        )

        logger.info(
            "============================================"
        )

        # 1
        self.create_spark_session()

        # 2
        self.load_data()

        # 3
        self.validate_input_columns()

        # 4
        self.rename_columns()

        # 5
        self.create_timestamp()

        # 6
        self.cast_data_types()

        # 7
        self.profile_nulls()

        # 8
        self.quarantine_invalid_records()

        # 9
        self.apply_curated_null_rule()

        # 10
        self.validate_grid_ids()

        # 11
        self.validate_negative_activity()

        # 12
        self.derive_activity_features()

        # 13
        self.derive_time_features()

        # 14
        self.verify_hourly_cadence()

        # 15
        report = (
            self.generate_report()
        )

        # 16
        self.export_clean_data()

        # 17
        self.export_rejected_data()

        # 18
        self.export_report(
            report
        )

        logger.info(
            "============================================"
        )

        logger.info(
            "SP2 CLEANING COMPLETED SUCCESSFULLY"
        )

        logger.info(
            "============================================"
        )

        return self.clean_network_df

    # ========================================================
    # STOP SPARK
    # ========================================================

    def stop(self):

        if self.spark is not None:

            self.spark.stop()

            logger.info(
                "SparkSession stopped."
            )