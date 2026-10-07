
import os
from pathlib import Path
import logging

# ============================================================
# HADOOP / WINUTILS CONFIGURATION
# ============================================================
#
# Project structure:
#
# Network operations and predictive intelligence system
# │
# ├── winutils
# │   └── hadoop-win-utils
# │       └── bin
# │           └── winutils.exe
# │
# └── phase 2
#     └── sp3
#         └── spark_aggregation.py
#
# ============================================================

CURRENT_FILE = Path(__file__).resolve()

PHASE2_ROOT = CURRENT_FILE.parents[1]

PROJECT_ROOT = PHASE2_ROOT.parent

HADOOP_HOME = (
    PROJECT_ROOT
    / "winutils"
    / "hadoop-win-utils"
)

WINUTILS_EXE = (
    HADOOP_HOME
    / "bin"
    / "winutils.exe"
)

os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
os.environ["hadoop.home.dir"] = str(HADOOP_HOME)

# Tell Python/Spark which Hadoop executable to use.
os.environ["PATH"] = (
    str(HADOOP_HOME / "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)


# ============================================================
# SPARK
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PROJECT PATHS
# ============================================================

# phase 2 is the project root for SP3 processing
PROJECT_ROOT = PHASE2_ROOT

INPUT_PATH = (
    PROJECT_ROOT
    / "output"
    / "clean_network"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "output"
    / "sp3"
)

LOG_DIR = (
    PROJECT_ROOT
    / "logs"
)

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

LOG_FILE = (
    LOG_DIR
    / "sp3_aggregation.log"
)

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# HADOOP VALIDATION
# ============================================================

def validate_hadoop_configuration():

    logger.info(
        "HADOOP_HOME: %s",
        HADOOP_HOME
    )

    logger.info(
        "winutils.exe: %s",
        WINUTILS_EXE
    )

    if not HADOOP_HOME.exists():

        raise FileNotFoundError(
            "HADOOP_HOME directory not found:\n"
            f"{HADOOP_HOME}"
        )

    if not WINUTILS_EXE.exists():

        raise FileNotFoundError(
            "winutils.exe not found:\n"
            f"{WINUTILS_EXE}"
        )

    logger.info(
        "Hadoop configuration: PASS"
    )


# ============================================================
# SPARK AGGREGATION
# ============================================================

class SparkNetworkAggregation:

    """
    SP3 - Network Activity Aggregations.

    Input grain:
        timestamp + grid_id + country_code

    Output grain:
        timestamp + grid_id

    SP3 collapses country-code records into
    one canonical grid/hour record.
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(self):

        self.spark = None

        self.clean_network_df = None

        self.hourly_grid_summary = None
        self.daily_traffic_summary = None
        self.hotspot_ranking = None
        self.peak_activity_hour = None

        self.input_row_count = 0
        self.output_row_count = 0
        self.day_count = 0

    # ========================================================
    # 1. CREATE SPARK SESSION
    # ========================================================

    def create_spark_session(self):

        logger.info(
            "Creating SparkSession..."
        )

        # ----------------------------------------------------
        # Validate Hadoop before Spark starts
        # ----------------------------------------------------

        validate_hadoop_configuration()

        # ----------------------------------------------------
        # Create local Spark session
        # ----------------------------------------------------

        self.spark = (
            SparkSession
            .builder
            .appName(
                "SP3-Network-Activity-Aggregation"
            )
            .master("local[*]")
            .config(
                "spark.driver.host",
                "127.0.0.1"
            )
            .config(
                "spark.driver.bindAddress",
                "127.0.0.1"
            )
            .config(
                "spark.sql.warehouse.dir",
                str(
                    PROJECT_ROOT
                    / "spark-warehouse"
                )
            )
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

    # ========================================================
    # 2. LOAD SP2 CLEAN DATA
    # ========================================================

    def load_clean_data(self):

        if self.spark is None:

            raise RuntimeError(
                "Create SparkSession before loading data."
            )

        if not INPUT_PATH.exists():

            raise FileNotFoundError(
                "SP2 cleaned dataset was not found:\n"
                f"{INPUT_PATH}\n\n"
                "Run SP2 first and make sure it exports "
                "clean_network to output/clean_network."
            )

        logger.info(
            "Loading SP2 clean dataset..."
        )

        self.clean_network_df = (
            self.spark
            .read
            .parquet(
                str(INPUT_PATH)
            )
        )

        self.input_row_count = (
            self.clean_network_df.count()
        )

        logger.info(
            "SP2 clean rows: %d",
            self.input_row_count
        )

        logger.info(
            "SP2 schema:"
        )

        self.clean_network_df.printSchema()

        return self.clean_network_df

    # ========================================================
    # 3. VALIDATE INPUT
    # ========================================================

    def validate_input(self):

        required_columns = [
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in self.clean_network_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "SP2 clean DataFrame is missing "
                f"required columns: {missing_columns}"
            )

        logger.info(
            "SP3 input column validation: PASS"
        )

    # ========================================================
    # 4. DETERMINE NUMBER OF DAILY FILES
    # ========================================================

    def determine_day_count(self):

        distinct_dates = (
            self.clean_network_df
            .select("date")
            .distinct()
            .count()
        )

        if distinct_dates == 0:

            raise ValueError(
                "No dates found in clean dataset."
            )

        self.day_count = (
            distinct_dates
        )

        logger.info(
            "Distinct days in dataset: %d",
            self.day_count
        )

        return self.day_count

    # ========================================================
    # 5. COUNTRY CODE -> GRID/HOUR AGGREGATION
    # ========================================================

    def aggregate_to_grid_hour(self):

        """
        Mandatory SP3 grain transition.

        Before:

            timestamp + grid_id + country_code

        After:

            timestamp + grid_id

        Country-code activity values are summed.
        """

        logger.info(
            "Starting country-code -> grid/hour aggregation..."
        )

        self.hourly_grid_summary = (

            self.clean_network_df

            .groupBy(
                "timestamp",
                "grid_id"
            )

            .agg(

                F.sum(
                    "sms_in"
                ).alias(
                    "sms_in"
                ),

                F.sum(
                    "sms_out"
                ).alias(
                    "sms_out"
                ),

                F.sum(
                    "call_in"
                ).alias(
                    "call_in"
                ),

                F.sum(
                    "call_out"
                ).alias(
                    "call_out"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                )
            )
        )

        logger.info(
            "Country-code rows consolidated."
        )

        return self.hourly_grid_summary

    # ========================================================
    # 6. DERIVE ACTIVITY KPIs
    # ========================================================

    def derive_activity_kpis(self):

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Run aggregate_to_grid_hour() first."
            )

        logger.info(
            "Deriving activity KPIs..."
        )

        # ----------------------------------------------------
        # TOTAL SMS
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "total_sms",
                F.col("sms_in")
                +
                F.col("sms_out")
            )
        )

        # ----------------------------------------------------
        # TOTAL CALLS
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "total_calls",
                F.col("call_in")
                +
                F.col("call_out")
            )
        )

        # ----------------------------------------------------
        # TOTAL ACTIVITY
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "total_activity",
                F.col("total_sms")
                +
                F.col("total_calls")
                +
                F.col("internet_activity")
            )
        )

        # ----------------------------------------------------
        # DATE
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "date",
                F.to_date(
                    F.col("timestamp")
                )
            )
        )

        # ----------------------------------------------------
        # HOUR
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "hour",
                F.hour(
                    F.col("timestamp")
                )
            )
        )

        # ----------------------------------------------------
        # DAY OF WEEK
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .withColumn(
                "day_of_week",
                F.dayofweek(
                    F.col("timestamp")
                )
            )
        )

        logger.info(
            "Derived total_sms."
        )

        logger.info(
            "Derived total_calls."
        )

        logger.info(
            "Derived total_activity."
        )

        logger.info(
            "Derived date, hour and day_of_week."
        )

        return self.hourly_grid_summary

    # ========================================================
    # 7. SELECT CANONICAL HOURLY COLUMNS
    # ========================================================

    def create_canonical_hourly_summary(self):

        """
        Create the canonical downstream SP3 dataset.

        Grain:
            exactly one row per grid_id + timestamp
        """

        self.hourly_grid_summary = (

            self.hourly_grid_summary

            .select(
                "timestamp",
                "grid_id",
                "date",
                "hour",
                "day_of_week",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "total_sms",
                "total_calls",
                "internet_activity",
                "total_activity"
            )

            .orderBy(
                "timestamp",
                "grid_id"
            )
        )

        logger.info(
            "Canonical hourly_grid_summary created."
        )

        return self.hourly_grid_summary

    # ========================================================
    # 8. ZERO DUPLICATE GRAIN ASSERTION
    # ========================================================

    def validate_hourly_grain(self):

        logger.info(
            "Validating hourly_grid_summary grain..."
        )

        duplicate_count = (

            self.hourly_grid_summary

            .groupBy(
                "grid_id",
                "timestamp"
            )

            .count()

            .filter(
                F.col("count") > 1
            )

            .count()
        )

        logger.info(
            "Duplicate grid/hour groups: %d",
            duplicate_count
        )

        assert (
            duplicate_count == 0
        ), (
            "SP3 FAILED: hourly_grid_summary "
            "contains duplicate grid_id + timestamp records."
        )

        logger.info(
            "VALIDATION zero duplicates: PASS"
        )

    # ========================================================
    # 9. ROW COUNT VALIDATION
    # ========================================================

    def validate_row_count(self):

        self.output_row_count = (
            self.hourly_grid_summary.count()
        )

        logger.info(
            "Clean network rows: %d",
            self.input_row_count
        )

        logger.info(
            "Hourly grid summary rows: %d",
            self.output_row_count
        )

        assert (
            self.output_row_count
            <
            self.input_row_count
        ), (
            "SP3 FAILED: aggregation did not reduce "
            "the row count."
        )

        maximum_rows = (
            self.day_count
            *
            24
            *
            10000
        )

        logger.info(
            "Maximum allowed grid/hour rows: %d",
            maximum_rows
        )

        assert (
            self.output_row_count
            <=
            maximum_rows
        ), (
            "SP3 FAILED: hourly_grid_summary "
            "exceeds D x 24 x 10000."
        )

        logger.info(
            "VALIDATION row count: PASS"
        )

    # ========================================================
    # 10. COUNTRY CODE MUST NOT EXIST
    # ========================================================

    def validate_country_code_removed(self):

        assert (
            "country_code"
            not in
            self.hourly_grid_summary.columns
        ), (
            "SP3 FAILED: country_code still exists "
            "in hourly_grid_summary."
        )

        logger.info(
            "VALIDATION country_code removed: PASS"
        )

    # ========================================================
    # 11. TIMESTAMP VALIDATION
    # ========================================================

    def validate_timestamp_count(self):

        timestamp_count = (

            self.hourly_grid_summary

            .select(
                "timestamp"
            )

            .distinct()

            .count()
        )

        expected_timestamp_count = (
            self.day_count
            *
            24
        )

        logger.info(
            "Distinct timestamps: %d",
            timestamp_count
        )

        logger.info(
            "Expected timestamps: %d",
            expected_timestamp_count
        )

        assert (
            timestamp_count
            ==
            expected_timestamp_count
        ), (
            "SP3 FAILED: unexpected hourly timestamp count."
        )

        logger.info(
            "VALIDATION timestamp count: PASS"
        )

    # ========================================================
    # 12. HAND-CHECK ONE GRID/HOUR
    # ========================================================

    def hand_check_grid_hour(self):

        logger.info(
            "Performing hand-check of one grid/hour..."
        )

        example = (

            self.hourly_grid_summary

            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity"
            )

            .orderBy(
                "timestamp",
                "grid_id"
            )

            .first()
        )

        if example is None:

            raise ValueError(
                "No hourly grid record available for hand-check."
            )

        timestamp = (
            example["timestamp"]
        )

        grid_id = (
            example["grid_id"]
        )

        original = (

            self.clean_network_df

            .filter(
                (
                    F.col("timestamp")
                    ==
                    timestamp
                )
                &
                (
                    F.col("grid_id")
                    ==
                    grid_id
                )
            )

            .agg(

                F.sum(
                    "sms_in"
                ).alias(
                    "sms_in"
                ),

                F.sum(
                    "sms_out"
                ).alias(
                    "sms_out"
                ),

                F.sum(
                    "call_in"
                ).alias(
                    "call_in"
                ),

                F.sum(
                    "call_out"
                ).alias(
                    "call_out"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                )
            )

            .first()
        )

        assert (
            float(example["sms_in"])
            ==
            float(original["sms_in"])
        )

        assert (
            float(example["sms_out"])
            ==
            float(original["sms_out"])
        )

        assert (
            float(example["call_in"])
            ==
            float(original["call_in"])
        )

        assert (
            float(example["call_out"])
            ==
            float(original["call_out"])
        )

        assert (
            float(example["internet_activity"])
            ==
            float(original["internet_activity"])
        )

        logger.info(
            "HAND CHECK: PASS"
        )

        logger.info(
            "Grid: %s",
            grid_id
        )

        logger.info(
            "Timestamp: %s",
            timestamp
        )

        logger.info(
            "SMS in: %.4f",
            float(example["sms_in"])
        )

        logger.info(
            "SMS out: %.4f",
            float(example["sms_out"])
        )

        logger.info(
            "Call in: %.4f",
            float(example["call_in"])
        )

        logger.info(
            "Call out: %.4f",
            float(example["call_out"])
        )

        logger.info(
            "Internet: %.4f",
            float(example["internet_activity"])
        )

    # ========================================================
    # 13. DAILY TRAFFIC SUMMARY
    # ========================================================

    def create_daily_traffic_summary(self):

        logger.info(
            "Creating daily traffic summary..."
        )

        self.daily_traffic_summary = (

            self.hourly_grid_summary

            .groupBy(
                "date",
                "grid_id"
            )

            .agg(

                F.sum(
                    "total_sms"
                ).alias(
                    "total_sms"
                ),

                F.sum(
                    "total_calls"
                ).alias(
                    "total_calls"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                ),

                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                F.count(
                    "*"
                ).alias(
                    "active_hours"
                )
            )

            .orderBy(
                "date",
                F.desc(
                    "total_activity"
                )
            )
        )

        logger.info(
            "Daily traffic summary created."
        )

        return self.daily_traffic_summary

    # ========================================================
    # 14. TOP TEN HOTSPOTS
    # ========================================================

    def create_hotspot_ranking(self):

        logger.info(
            "Creating top ten high-activity grid ranking..."
        )

        self.hotspot_ranking = (

            self.daily_traffic_summary

            .groupBy(
                "grid_id"
            )

            .agg(

                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                F.sum(
                    "total_sms"
                ).alias(
                    "total_sms"
                ),

                F.sum(
                    "total_calls"
                ).alias(
                    "total_calls"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                )
            )

            .orderBy(
                F.desc(
                    "total_activity"
                )
            )

            .limit(10)
        )

        logger.info(
            "Top ten high-activity grids:"
        )

        self.hotspot_ranking.show(
            truncate=False
        )

        return self.hotspot_ranking

    # ========================================================
    # 15. PEAK ACTIVITY HOUR
    # ========================================================

    def create_peak_activity_hour(self):

        logger.info(
            "Calculating peak activity hour..."
        )

        self.peak_activity_hour = (

            self.hourly_grid_summary

            .groupBy(
                "timestamp"
            )

            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )

            .orderBy(
                F.desc(
                    "total_activity"
                )
            )

            .limit(1)
        )

        logger.info(
            "Peak activity hour:"
        )

        self.peak_activity_hour.show(
            truncate=False
        )

        return self.peak_activity_hour

    # ========================================================
    # 16. INTERNET SHARE
    # ========================================================

    def add_internet_share(self):

        logger.info(
            "Calculating internet share..."
        )

        self.hourly_grid_summary = (

            self.hourly_grid_summary

            .withColumn(
                "internet_share",

                F.when(
                    F.col("total_activity") > 0,

                    F.col("internet_activity")
                    /
                    F.col("total_activity")
                )

                .otherwise(
                    F.lit(0.0)
                )
            )
        )

        logger.info(
            "Internet share added."
        )

        return self.hourly_grid_summary

    # ========================================================
    # 17. FINAL SCHEMA VALIDATION
    # ========================================================

    def validate_final_schema(self):

        required_output_columns = [

            "timestamp",
            "grid_id",
            "date",
            "hour",
            "day_of_week",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "total_sms",
            "total_calls",
            "internet_activity",
            "total_activity",
            "internet_share"
        ]

        missing_columns = [

            column
            for column in required_output_columns

            if column
            not in
            self.hourly_grid_summary.columns
        ]

        assert not missing_columns, (
            "SP3 FAILED: missing output columns: "
            f"{missing_columns}"
        )

        assert (
            "country_code"
            not in
            self.hourly_grid_summary.columns
        )

        logger.info(
            "VALIDATION final schema: PASS"
        )

    # ========================================================
    # 18. EXPORT HOURLY SUMMARY
    # ========================================================

    def export_hourly_summary(self):

        output_path = (
            OUTPUT_DIR
            / "hourly_grid_summary"
        )

        logger.info(
            "Writing hourly_grid_summary..."
        )

        (
            self.hourly_grid_summary

            .write

            .mode(
                "overwrite"
            )

            .option(
                "header",
                True
            )

            .csv(
                str(output_path)
            )
        )

        logger.info(
            "hourly_grid_summary exported to: %s",
            output_path
        )

        return output_path

    # ========================================================
    # 19. EXPORT DAILY SUMMARY
    # ========================================================

    def export_daily_summary(self):

        output_path = (
            OUTPUT_DIR
            / "daily_traffic_summary"
        )

        logger.info(
            "Writing daily_traffic_summary..."
        )

        (
            self.daily_traffic_summary

            .write

            .mode(
                "overwrite"
            )

            .option(
                "header",
                True
            )

            .csv(
                str(output_path)
            )
        )

        logger.info(
            "daily_traffic_summary exported to: %s",
            output_path
        )

        return output_path

    # ========================================================
    # 20. EXPORT HOTSPOT RANKING
    # ========================================================

    def export_hotspot_ranking(self):

        output_path = (
            OUTPUT_DIR
            / "hotspot_ranking"
        )

        logger.info(
            "Writing hotspot_ranking..."
        )

        (
            self.hotspot_ranking

            .write

            .mode(
                "overwrite"
            )

            .option(
                "header",
                True
            )

            .csv(
                str(output_path)
            )
        )

        logger.info(
            "hotspot_ranking exported to: %s",
            output_path
        )

        return output_path

    # ========================================================
    # 21. RUN SP3
    # ========================================================

    def run(self):

        try:

            logger.info(
                "============================================================"
            )

            logger.info(
                "SP3 NETWORK ACTIVITY AGGREGATION STARTED"
            )

            logger.info(
                "Project root: %s",
                PROJECT_ROOT
            )

            logger.info(
                "SP2 input: %s",
                INPUT_PATH
            )

            logger.info(
                "SP3 output: %s",
                OUTPUT_DIR
            )

            logger.info(
                "HADOOP_HOME: %s",
                HADOOP_HOME
            )

            logger.info(
                "winutils.exe: %s",
                WINUTILS_EXE
            )

            logger.info(
                "============================================================"
            )

            # ------------------------------------------------
            # CREATE SPARK
            # ------------------------------------------------

            self.create_spark_session()

            # ------------------------------------------------
            # LOAD
            # ------------------------------------------------

            self.load_clean_data()

            # ------------------------------------------------
            # VALIDATE INPUT
            # ------------------------------------------------

            self.validate_input()

            self.determine_day_count()

            # ------------------------------------------------
            # AGGREGATE
            # ------------------------------------------------

            self.aggregate_to_grid_hour()

            self.derive_activity_kpis()

            self.create_canonical_hourly_summary()

            self.add_internet_share()

            # ------------------------------------------------
            # VALIDATE
            # ------------------------------------------------

            self.validate_hourly_grain()

            self.validate_row_count()

            self.validate_country_code_removed()

            self.validate_timestamp_count()

            self.hand_check_grid_hour()

            self.validate_final_schema()

            # ------------------------------------------------
            # ADDITIONAL ANALYTICS
            # ------------------------------------------------

            self.create_daily_traffic_summary()

            self.create_hotspot_ranking()

            self.create_peak_activity_hour()

            # ------------------------------------------------
            # EXPORT
            # ------------------------------------------------

            self.export_hourly_summary()

            self.export_daily_summary()

            self.export_hotspot_ranking()

            # ------------------------------------------------
            # FINAL STATUS
            # ------------------------------------------------

            logger.info(
                "============================================================"
            )

            logger.info(
                "ALL SP3 ACCEPTANCE CRITERIA PASSED"
            )

            logger.info(
                "SP3 PROCESSING COMPLETED SUCCESSFULLY."
            )

            logger.info(
                "============================================================"
            )

            return self.hourly_grid_summary

        except Exception:

            logger.exception(
                "SP3 processing failed."
            )

            raise

        finally:

            if self.spark is not None:

                self.spark.stop()

                logger.info(
                    "SparkSession stopped."
                )
