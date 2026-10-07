
import os
import sys
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PHASE2_ROOT = PROJECT_ROOT / "phase 2"

DEFAULT_INPUT_PATH = PHASE2_ROOT / "data" / "raw"
DEFAULT_OUTPUT_PATH = PHASE2_ROOT / "data" / "processed"
DEFAULT_REFERENCE_PATH = PHASE2_ROOT / "data" / "reference"

LOG_DIR = PHASE2_ROOT / "logs"

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# HADOOP / WINUTILS
# ============================================================

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


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_DIR / "sp7.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("SP7")


# ============================================================
# HADOOP CONFIGURATION
# ============================================================

def configure_hadoop():

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

    os.environ["HADOOP_HOME"] = str(
        HADOOP_HOME
    )

    os.environ["hadoop.home.dir"] = str(
        HADOOP_HOME
    )

    os.environ["PATH"] = (
        str(HADOOP_HOME / "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
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
        "Python executable: %s",
        sys.executable
    )

    logger.info(
        "Hadoop configuration: PASS"
    )


# ============================================================
# SP7 TELECOM ETL
# ============================================================

class TelecomSparkETL:

    def __init__(
        self,
        spark,
        input_path,
        output_path,
        reference_path
    ):

        self.spark = spark

        self.input_path = Path(
            input_path
        )

        self.output_path = Path(
            output_path
        )

        self.reference_path = Path(
            reference_path
        )

        self.geojson_path = (
            self.reference_path
            / "milano-grid.geojson"
        )

        self.raw_df = None
        self.clean_df = None
        self.hourly_grid_summary = None
        self.enriched_df = None

        self.input_rows = 0
        self.rejected_rows = 0
        self.nulls_handled = 0
        self.output_rows = 0

        self.start_time = None
        self.end_time = None

    # ========================================================
    # READ RAW
    # ========================================================

    def read_raw(self):

        logger.info(
            "Reading raw network activity data."
        )

        if not self.input_path.exists():

            raise FileNotFoundError(
                "Input directory does not exist:\n"
                f"{self.input_path}"
            )

        files = sorted(
            self.input_path.glob(
                "sms-call-internet-mi-*.csv"
            )
        )

        if not files:

            raise FileNotFoundError(
                "NO INPUT FILES FOUND.\n"
                f"Input directory:\n{self.input_path}\n"
                "Expected files matching:\n"
                "sms-call-internet-mi-*.csv"
            )

        logger.info(
            "Input files discovered: %d",
            len(files)
        )

        for file in files:

            logger.info(
                "Input file: %s",
                file.name
            )

        self.raw_df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(
                [
                    str(file)
                    for file in files
                ]
            )
        )

        self.input_rows = (
            self.raw_df.count()
        )

        logger.info(
            "INPUT_ROWS=%d",
            self.input_rows
        )

        logger.info(
            "Raw schema:"
        )

        self.raw_df.printSchema()

        return self.raw_df

    # ========================================================
    # CLEAN
    # ========================================================

    def clean(self):

        logger.info(
            "Starting cleaning stage."
        )

        df = self.raw_df

        required_columns = [
            "datetime",
            "CellID",
            "countrycode",
            "smsin",
            "smsout",
            "callin",
            "callout",
            "internet"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Missing required raw columns: "
                f"{missing_columns}"
            )

        # ----------------------------------------------------
        # Rename columns
        # ----------------------------------------------------

        df = (
            df
            .withColumnRenamed(
                "datetime",
                "timestamp"
            )
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

        # ----------------------------------------------------
        # Cast timestamp
        # ----------------------------------------------------

        df = df.withColumn(
            "timestamp",
            F.to_timestamp(
                F.col("timestamp")
            )
        )

        # ----------------------------------------------------
        # Cast IDs
        # ----------------------------------------------------

        df = df.withColumn(
            "grid_id",
            F.col("grid_id").cast(
                IntegerType()
            )
        )

        df = df.withColumn(
            "country_code",
            F.col("country_code").cast(
                IntegerType()
            )
        )

        # ----------------------------------------------------
        # Cast activity columns
        # ----------------------------------------------------

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        for column in activity_columns:

            df = df.withColumn(
                column,
                F.col(column).cast(
                    DoubleType()
                )
            )

        # ----------------------------------------------------
        # Rejection rules
        # ----------------------------------------------------

        rejection_condition = (
            F.col("timestamp").isNull()
            |
            F.col("grid_id").isNull()
            |
            (F.col("sms_in") < 0)
            |
            (F.col("sms_out") < 0)
            |
            (F.col("call_in") < 0)
            |
            (F.col("call_out") < 0)
            |
            (F.col("internet_activity") < 0)
        )

        self.rejected_rows = (
            df
            .filter(
                rejection_condition
            )
            .count()
        )

        logger.info(
            "REJECTED_ROWS=%d",
            self.rejected_rows
        )

        df = (
            df
            .filter(
                ~rejection_condition
            )
        )

        # ----------------------------------------------------
        # Count null activity values
        # ----------------------------------------------------

        null_expression = sum(
            F.when(
                F.col(column).isNull(),
                1
            )
            .otherwise(0)
            for column in activity_columns
        )

        null_row = (
            df
            .select(
                null_expression.alias(
                    "null_count"
                )
            )
            .collect()[0]
        )

        self.nulls_handled = int(
            null_row["null_count"]
        )

        # ----------------------------------------------------
        # Replace null activity with zero
        # ----------------------------------------------------

        for column in activity_columns:

            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.lit(0.0)
                )
            )

        # ----------------------------------------------------
        # Date / hour
        # ----------------------------------------------------

        df = (
            df
            .withColumn(
                "date",
                F.to_date(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "hour",
                F.hour(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(
                    F.col("timestamp")
                )
        )
        )
        # ----------------------------------------------------
        # Derived activity fields
        # ----------------------------------------------------

        df = (
            df
            .withColumn(
                "total_sms",
                F.col("sms_in")
                +
                F.col("sms_out")
            )
            .withColumn(
                "total_calls",
                F.col("call_in")
                +
                F.col("call_out")
            )
            .withColumn(
                "total_activity",
                F.col("sms_in")
                +
                F.col("sms_out")
                +
                F.col("call_in")
                +
                F.col("call_out")
                +
                F.col("internet_activity")
            )
        )

        # ----------------------------------------------------
        # Select stable canonical column order
        # ----------------------------------------------------

        self.clean_df = df.select(
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
            "date",
            "hour",
            "day_of_week"
        )

        logger.info(
            "NULLS_HANDLED=%d",
            self.nulls_handled
        )

        logger.info(
            "Cleaning stage completed."
        )

        return self.clean_df

    # ========================================================
    # AGGREGATE
    # ========================================================

    def aggregate(self):

        logger.info(
            "Starting country-code aggregation."
        )

        # ----------------------------------------------------
        # Country code aggregation happens BEFORE
        # grid/hour operational analytics.
        # ----------------------------------------------------

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        aggregations = [
            F.sum(
                column
            ).alias(
                column
            )
            for column in activity_columns
        ]

        df = (
            self.clean_df
            .groupBy(
                "timestamp",
                "grid_id"
            )
            .agg(
                *aggregations
            )
        )

        # ----------------------------------------------------
        # Derived fields after aggregation
        # ----------------------------------------------------

        df = (
            df
            .withColumn(
                "total_sms",
                F.col("sms_in")
                +
                F.col("sms_out")
            )
            .withColumn(
                "total_calls",
                F.col("call_in")
                +
                F.col("call_out")
            )
            .withColumn(
                "total_activity",
                F.col("sms_in")
                +
                F.col("sms_out")
                +
                F.col("call_in")
                +
                F.col("call_out")
                +
                F.col("internet_activity")
            )
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
            .withColumn(
                "date",
                F.to_date(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "hour",
                F.hour(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(
                    F.col("timestamp")
                )
        )
        )
        # ----------------------------------------------------
        # Stable schema order
        # ----------------------------------------------------

        self.hourly_grid_summary = df.select(
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "total_sms",
            "total_calls",
            "internet_activity",
            "total_activity",
            "internet_share",
            "hour",
            "day_of_week",
            "date"
        )
        

        # ----------------------------------------------------
        # Duplicate validation
        # ----------------------------------------------------

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

        if duplicate_count != 0:

            raise AssertionError(
                "Duplicate grid/hour records found: "
                f"{duplicate_count}"
            )

        logger.info(
            "Hourly grid grain validation: PASS"
        )

        hourly_count = (
            self.hourly_grid_summary.count()
        )

        logger.info(
            "Hourly grid summary rows: %d",
            hourly_count
        )

        if hourly_count >= self.input_rows:

            raise AssertionError(
                "Aggregation did not reduce the input grain."
            )

        # ----------------------------------------------------
        # Cache because the same DataFrame is reused.
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            self.hourly_grid_summary
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        self.hourly_grid_summary.count()

        return self.hourly_grid_summary

    # ========================================================
    # LOAD GEOJSON
    # ========================================================

    def load_geojson_lookup(self):

        logger.info(
            "Starting geospatial enrichment."
        )

        logger.info(
            "Reading Milan GeoJSON:"
        )

        logger.info(
            "%s",
            self.geojson_path
        )

        if not self.geojson_path.exists():

            raise FileNotFoundError(
                "Milan GeoJSON not found:\n"
                f"{self.geojson_path}"
            )

        with open(
            self.geojson_path,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        if geojson.get(
            "type"
        ) != "FeatureCollection":

            raise ValueError(
                "GeoJSON top-level type must be FeatureCollection."
            )

        features = geojson.get(
            "features",
            []
        )

        logger.info(
            "GeoJSON feature count: %d",
            len(features)
        )

        records = []

        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            if "cellId" not in properties:

                raise ValueError(
                    "GeoJSON feature missing properties.cellId."
                )

            grid_id = int(
                properties["cellId"]
            )

            geometry = feature.get(
                "geometry"
            )

            geometry_json = json.dumps(
                geometry,
                separators=(",", ":")
            )

            records.append(
                (
                    grid_id,
                    geometry_json
                )
            )

        schema = StructType([
            StructField(
                "grid_id",
                IntegerType(),
                False
            ),
            StructField(
                "geometry",
                StringType(),
                False
            )
        ])

        lookup_df = (
            self.spark
            .createDataFrame(
                records,
                schema
            )
        )

        duplicate_count = (
            lookup_df
            .groupBy("grid_id")
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        if duplicate_count != 0:

            raise AssertionError(
                "Duplicate grid IDs found in GeoJSON."
            )

        lookup_count = (
            lookup_df.count()
        )

        logger.info(
            "Grid lookup rows: %d",
            lookup_count
        )

        return lookup_df

    # ========================================================
    # ENRICH
    # ========================================================

    def enrich(self):

        lookup_df = (
            self.load_geojson_lookup()
        )

        logger.info(
            "Broadcasting static grid lookup."
        )

        # ----------------------------------------------------
        # Standard join plan for SP5 evidence
        # ----------------------------------------------------

        logger.info(
            "========== STANDARD JOIN PLAN =========="
        )

        standard_join = (
            self.hourly_grid_summary
            .join(
                lookup_df,
                on="grid_id",
                how="left"
            )
        )

        standard_join.explain(
            mode="formatted"
        )

        # ----------------------------------------------------
        # Broadcast join
        # ----------------------------------------------------

        logger.info(
            "========== BROADCAST JOIN PLAN =========="
        )

        self.enriched_df = (
            self.hourly_grid_summary
            .join(
                broadcast(
                    lookup_df
                ),
                on="grid_id",
                how="left"
            )
        )

        before_count = (
            self.hourly_grid_summary.count()
        )

        after_count = (
            self.enriched_df.count()
        )

        if before_count != after_count:

            raise AssertionError(
                "Geospatial enrichment changed row count."
            )

        logger.info(
            "Geospatial row-count validation: PASS"
        )

        # ----------------------------------------------------
        # Coverage
        # ----------------------------------------------------

        total_grids = (
            self.enriched_df
            .select("grid_id")
            .distinct()
            .count()
        )

        matched_grids = (
            self.enriched_df
            .filter(
                F.col("geometry").isNotNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        unmatched_count = (
            self.enriched_df
            .filter(
                F.col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        coverage = (
            matched_grids
            /
            total_grids
            *
            100
            if total_grids > 0
            else 0
        )

        logger.info(
            "Distinct activity grids: %d",
            total_grids
        )

        logger.info(
            "Matched grids: %d",
            matched_grids
        )

        logger.info(
            "Unmatched grids: %d",
            unmatched_count
        )

        logger.info(
            "Enrichment coverage: %.2f%%",
            coverage
        )

        if unmatched_count != 0:

            raise AssertionError(
                "Some activity grid IDs were not found "
                "in the GeoJSON reference."
            )

        logger.info(
            "Geospatial enrichment coverage: PASS"
        )

        return self.enriched_df

    # ========================================================
    # WRITE CLEAN ACTIVITY
    # ========================================================

    def write_clean_activity(self):

        output_path = (
            self.output_path
            / "activity"
        )

        logger.info(
            "Writing cleaned activity Parquet:"
        )

        logger.info(
            "%s",
            output_path
        )

        # ----------------------------------------------------
        # Partition by date.
        #
        # Date is deliberately the partition column.
        # ----------------------------------------------------

        write_df = (
            self.clean_df
            .repartition(
                "date"
            )
        )

        (
            write_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .option(
                "compression",
                "snappy"
            )
            .parquet(
                str(output_path)
            )
        )

        logger.info(
            "Clean activity Parquet written."
        )

        return output_path

    # ========================================================
    # WRITE HOURLY SUMMARY
    # ========================================================

    def write_hourly_summary(self):

        output_path = (
            self.output_path
            / "hourly_grid_summary"
        )

        logger.info(
            "Writing hourly_grid_summary Parquet:"
        )

        logger.info(
            "%s",
            output_path
        )

        # ----------------------------------------------------
        # Geometry is NOT selected.
        # Geometry stays in GeoJSON reference data.
        # ----------------------------------------------------

        columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "total_sms",
            "total_calls",
            "internet_activity",
            "total_activity",
            "internet_share",
            "hour",
            "day_of_week",
            "date"
        ]

        analytics_df = (
            self.hourly_grid_summary
            .select(columns)
        )

        # ----------------------------------------------------
        # Controlled number of output partitions.
        # ----------------------------------------------------

        analytics_df = (
            analytics_df
            .repartition(
                8,
                "date"
            )
        )

        (
            analytics_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .option(
                "compression",
                "snappy"
            )
            .parquet(
                str(output_path)
            )
        )

        logger.info(
            "Hourly analytics Parquet written."
        )

        return output_path

    # ========================================================
    # DASHBOARD SUMMARY
    # ========================================================

    def write_dashboard_summary(self):

        output_file = (
            self.output_path
            / "dashboard_summary.csv"
        )

        logger.info(
            "Creating dashboard summary."
        )

        dashboard_df = (
            self.hourly_grid_summary
            .groupBy("date")
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
                F.avg(
                    "internet_share"
                ).alias(
                    "average_internet_share"
                )
            )
            .orderBy("date")
        )

        temporary_path = (
            self.output_path
            / "_dashboard_csv"
        )

        (
            dashboard_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(temporary_path)
            )
        )

        csv_files = list(
            temporary_path.glob(
                "part-*.csv"
            )
        )

        if not csv_files:

            raise FileNotFoundError(
                "Dashboard CSV was not created."
            )

        if output_file.exists():

            output_file.unlink()

        shutil.copyfile(
            csv_files[0],
            output_file
        )

        shutil.rmtree(
            temporary_path,
            ignore_errors=True
        )

        logger.info(
            "Dashboard summary written: %s",
            output_file
        )

        return output_file

    # ========================================================
    # ROUND-TRIP VALIDATION
    # ========================================================

    def validate_round_trip(self):

        logger.info(
            "Starting Parquet round-trip validation."
        )

        # ====================================================
        # ACTIVITY
        # ====================================================

        activity_path = (
            self.output_path
            / "activity"
        )

        activity_read = (
            self.spark.read
            .parquet(
                str(activity_path)
            )
        )

        original_activity_count = (
            self.clean_df.count()
        )

        roundtrip_activity_count = (
            activity_read.count()
        )

        if (
            original_activity_count
            !=
            roundtrip_activity_count
        ):

            raise AssertionError(
                "Activity Parquet row count mismatch."
            )

        logger.info(
            "Activity Parquet row count: PASS"
        )

        # ====================================================
        # HOURLY SUMMARY
        # ====================================================

        hourly_path = (
            self.output_path
            / "hourly_grid_summary"
        )

        hourly_read = (
            self.spark.read
            .parquet(
                str(hourly_path)
            )
        )

        original_hourly_count = (
            self.hourly_grid_summary.count()
        )

        roundtrip_hourly_count = (
            hourly_read.count()
        )

        if (
            original_hourly_count
            !=
            roundtrip_hourly_count
        ):

            raise AssertionError(
                "Hourly Parquet row count mismatch."
            )

        logger.info(
            "Hourly Parquet row count: PASS"
        )

        # ====================================================
        # GEOMETRY CHECK
        # ====================================================

        if "geometry" in hourly_read.columns:

            raise AssertionError(
                "Geometry exists in hourly_grid_summary."
            )

        logger.info(
            "Hourly geometry exclusion: PASS"
        )

        # ====================================================
        # DUPLICATE CHECK
        # ====================================================

        duplicate_count = (
            hourly_read
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

        if duplicate_count != 0:

            raise AssertionError(
                "Duplicate grid/hour rows found "
                "after Parquet round trip."
            )

        logger.info(
            "Round-trip duplicate validation: PASS"
        )

        # ====================================================
        # SCHEMA VALIDATION
        # ====================================================
        #
        # IMPORTANT:
        #
        # Spark places partition columns at the end when
        # reading partitioned Parquet.
        #
        # Therefore:
        #
        # Original:
        #
        # timestamp
        # grid_id
        # ...
        # hour
        # day_of_week
        # date
        #
        # Read-back:
        #
        # timestamp
        # grid_id
        # ...
        # hour
        # day_of_week
        # date
        #
        # The logical schema is identical.
        #
        # We compare:
        #   1. same column names
        #   2. same data types
        #
        # without treating column order as a failure.
        # ====================================================

        expected_fields = {
            field.name: field.dataType.simpleString()
            for field
            in self.hourly_grid_summary.schema.fields
        }

        actual_fields = {
            field.name: field.dataType.simpleString()
            for field
            in hourly_read.schema.fields
        }

        if expected_fields != actual_fields:

            logger.error(
                "Expected schema fields:"
            )

            logger.error(
                "%s",
                expected_fields
            )

            logger.error(
                "Actual schema fields:"
            )

            logger.error(
                "%s",
                actual_fields
            )

            raise AssertionError(
                "Hourly Parquet schema fields or "
                "data types do not match."
            )

        logger.info(
            "Hourly Parquet schema fields/types: PASS"
        )

        # ====================================================
        # COLUMN SET VALIDATION
        # ====================================================

        expected_columns = set(
            self.hourly_grid_summary.columns
        )

        actual_columns = set(
            hourly_read.columns
        )

        if expected_columns != actual_columns:

            raise AssertionError(
                "Hourly Parquet column set does not match."
            )

        logger.info(
            "Hourly Parquet column set: PASS"
        )

        # ====================================================
        # ROUND-TRIP VALIDATION COMPLETE
        # ====================================================

        logger.info(
            "Parquet round-trip validation: PASS"
        )

    # ========================================================
    # OUTPUT ROW COUNT
    # ========================================================

    def calculate_output_rows(self):

        self.output_rows = (
            self.hourly_grid_summary.count()
        )

        logger.info(
            "OUTPUT_ROWS=%d",
            self.output_rows
        )

    # ========================================================
    # RUN
    # ========================================================

    def run(self):

        self.start_time = datetime.now()

        logger.info(
            "============================================================"
        )

        logger.info(
            "SP7 TELECOM ETL JOB STARTED"
        )

        logger.info(
            "============================================================"
        )

        logger.info(
            "Project root: %s",
            PROJECT_ROOT
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
            "Input path: %s",
            self.input_path
        )

        logger.info(
            "Output path: %s",
            self.output_path
        )

        logger.info(
            "Reference path: %s",
            self.reference_path
        )

        try:

            # ------------------------------------------------
            # ETL
            # ------------------------------------------------

            self.read_raw()

            self.clean()

            self.aggregate()

            self.enrich()

            # ------------------------------------------------
            # OUTPUTS
            # ------------------------------------------------

            self.write_clean_activity()

            self.write_hourly_summary()

            self.write_dashboard_summary()

            # ------------------------------------------------
            # VALIDATION
            # ------------------------------------------------

            self.validate_round_trip()

            self.calculate_output_rows()

            self.end_time = datetime.now()

            duration = (
                self.end_time
                -
                self.start_time
            ).total_seconds()

            # ------------------------------------------------
            # FINAL STRUCTURED LOG FIELDS
            # ------------------------------------------------

            logger.info(
                "INPUT_ROWS=%d",
                self.input_rows
            )

            logger.info(
                "REJECTED_ROWS=%d",
                self.rejected_rows
            )

            logger.info(
                "NULLS_HANDLED=%d",
                self.nulls_handled
            )

            logger.info(
                "OUTPUT_ROWS=%d",
                self.output_rows
            )

            logger.info(
                "JOB_DURATION_SECONDS=%.2f",
                duration
            )

            logger.info(
                "STATUS=SUCCESS"
            )

            logger.info(
                "============================================================"
            )

            logger.info(
                "SP7 TELECOM ETL JOB COMPLETED SUCCESSFULLY"
            )

            logger.info(
                "============================================================"
            )

            return self.hourly_grid_summary

        except Exception:

            self.end_time = datetime.now()

            logger.exception(
                "STATUS=FAILED"
            )

            raise

        finally:

            if self.hourly_grid_summary is not None:

                try:

                    self.hourly_grid_summary.unpersist()

                except Exception:

                    pass


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession
        .builder
        .appName(
            "SP7_Telecom_ETL"
        )
        .master(
            "local[*]"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "14"
        )
        .config(
            "spark.default.parallelism",
            "14"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config(
            "spark.python.worker.reuse",
            "true"
        )
        .config(
            "spark.network.timeout",
            "600s"
        )
        .config(
            "spark.executor.heartbeatInterval",
            "30s"
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    logger.info(
        "SparkSession created successfully."
    )

    logger.info(
        "Spark version: %s",
        spark.version
    )

    return spark


# ============================================================
# ARGUMENTS
# ============================================================

def parse_arguments():

    input_path = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else DEFAULT_INPUT_PATH
    )

    output_path = (
        Path(sys.argv[2])
        if len(sys.argv) > 2
        else DEFAULT_OUTPUT_PATH
    )

    reference_path = (
        Path(sys.argv[3])
        if len(sys.argv) > 3
        else DEFAULT_REFERENCE_PATH
    )

    return (
        input_path,
        output_path,
        reference_path
    )


# ============================================================
# MAIN
# ============================================================

def main():

    spark = None

    try:

        configure_hadoop()

        (
            input_path,
            output_path,
            reference_path
        ) = parse_arguments()

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        reference_path.mkdir(
            parents=True,
            exist_ok=True
        )

        spark = create_spark_session()

        pipeline = TelecomSparkETL(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            reference_path=reference_path
        )

        pipeline.run()

        return 0

    except Exception as exc:

        logger.error(
            "SP7 JOB FAILED."
        )

        logger.error(
            "%s",
            exc
        )

        return 1

    finally:

        if spark is not None:

            try:

                spark.stop()

                logger.info(
                    "SparkSession stopped."
                )

            except Exception:

                pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    exit_code = main()

    sys.exit(
        exit_code
    )
