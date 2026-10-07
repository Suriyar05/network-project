
import logging
import shutil
from pathlib import Path

from pyspark.sql import functions as F


# ============================================================
# PROJECT PATHS
# ============================================================

PHASE2_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PHASE2_ROOT / "data"
OUTPUT_DIR = PHASE2_ROOT / "output"
LOG_DIR = PHASE2_ROOT / "logs"

# ============================================================
# INPUT
# ============================================================

SP3_INPUT = (
    OUTPUT_DIR
    / "sp3"
    / "hourly_grid_summary"
)

# ============================================================
# OUTPUTS
# ============================================================

PROCESSED_ACTIVITY_DIR = (
    DATA_DIR
    / "processed"
    / "activity"
)

ANALYTICS_DIR = (
    DATA_DIR
    / "analytics"
    / "hourly_grid_summary"
)

REFERENCE_DIR = (
    DATA_DIR
    / "reference"
)

REFERENCE_GEOJSON = (
    REFERENCE_DIR
    / "milano-grid.geojson"
)

DASHBOARD_SUMMARY = (
    OUTPUT_DIR
    / "dashboard_summary.csv"
)

SP6_REPORT = (
    OUTPUT_DIR
    / "sp6_storage_report.txt"
)


# ============================================================
# DIRECTORIES
# ============================================================

PROCESSED_ACTIVITY_DIR.mkdir(
    parents=True,
    exist_ok=True
)

ANALYTICS_DIR.mkdir(
    parents=True,
    exist_ok=True
)

REFERENCE_DIR.mkdir(
    parents=True,
    exist_ok=True
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

LOG_FILE = LOG_DIR / "sp6.log"

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

logger = logging.getLogger(__name__)


# ============================================================
# SP6 STORAGE PIPELINE
# ============================================================

class SparkStoragePipeline:

    """
    SP6 — Write Processed & Analytics Data.

    Input:
        SP3 hourly_grid_summary

    Outputs:

        data/processed/activity/
            Parquet partitioned by date

        data/analytics/hourly_grid_summary/
            Parquet
            No geometry
            No country_code

        output/dashboard_summary.csv

        data/reference/milano-grid.geojson

    Validation:

        - row count
        - logical schema
        - date partitioning
        - zero duplicate grid_id + timestamp
        - no geometry in analytics
        - no country_code in analytics
        - Parquet round-trip
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(self, spark):

        self.spark = spark

        self.activity_df = None

        self.processed_df = None

        self.hourly_grid_summary = None

        self.dashboard_df = None

        self.original_row_count = 0

        self.processed_row_count = 0

        self.analytics_row_count = 0

        self.results_file_sizes = {}

    # ========================================================
    # 1. LOAD SP3
    # ========================================================

    def load_sp3_data(self):

        logger.info("=" * 70)
        logger.info("LOADING SP3 HOURLY GRID SUMMARY")
        logger.info("=" * 70)

        if not SP3_INPUT.exists():

            raise FileNotFoundError(
                "\nSP3 output not found.\n\n"
                f"Expected:\n{SP3_INPUT}\n\n"
                "Run SP3 first."
            )

        logger.info(
            "SP3 input directory: %s",
            SP3_INPUT
        )

        # ----------------------------------------------------
        # Your SP3 currently writes CSV files.
        #
        # If the directory contains Parquet instead,
        # automatically detect it.
        # ----------------------------------------------------

        parquet_files = list(
            SP3_INPUT.glob("*.parquet")
        )

        csv_files = list(
            SP3_INPUT.glob("*.csv")
        )

        if parquet_files:

            logger.info(
                "Detected Parquet SP3 output."
            )

            self.activity_df = (
                self.spark.read
                .parquet(
                    str(SP3_INPUT)
                )
            )

        elif csv_files:

            logger.info(
                "Detected CSV SP3 output."
            )

            self.activity_df = (
                self.spark.read
                .option(
                    "header",
                    True
                )
                .option(
                    "inferSchema",
                    True
                )
                .csv(
                    str(SP3_INPUT)
                )
            )

        else:

            # Spark output may contain nested directories.
            # Try Parquet first, then CSV.

            try:

                self.activity_df = (
                    self.spark.read
                    .parquet(
                        str(SP3_INPUT)
                    )
                )

                logger.info(
                    "SP3 loaded as Parquet."
                )

            except Exception:

                self.activity_df = (
                    self.spark.read
                    .option(
                        "header",
                        True
                    )
                    .option(
                        "inferSchema",
                        True
                    )
                    .csv(
                        str(SP3_INPUT)
                    )
                )

                logger.info(
                    "SP3 loaded as CSV."
                )

        self.original_row_count = (
            self.activity_df.count()
        )

        logger.info(
            "SP3 row count: %d",
            self.original_row_count
        )

        logger.info(
            "SP3 columns: %s",
            self.activity_df.columns
        )

        return self.activity_df

    # ========================================================
    # 2. VALIDATE INPUT
    # ========================================================

    def validate_input(self):

        logger.info("=" * 70)
        logger.info("VALIDATING SP3 INPUT")
        logger.info("=" * 70)

        required_columns = [
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
            for column in required_columns
            if column not in self.activity_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "SP3 input is missing required columns: "
                f"{missing_columns}"
            )

        # ----------------------------------------------------
        # Normalize timestamp.
        # ----------------------------------------------------

        self.activity_df = (
            self.activity_df
            .withColumn(
                "timestamp",
                F.to_timestamp(
                    F.col("timestamp")
                )
            )
        )

        # ----------------------------------------------------
        # Normalize grid_id.
        # ----------------------------------------------------

        self.activity_df = (
            self.activity_df
            .withColumn(
                "grid_id",
                F.col("grid_id").cast("int")
            )
        )

        # ----------------------------------------------------
        # Normalize date.
        #
        # IMPORTANT:
        # This is also used as the Parquet partition column.
        # ----------------------------------------------------

        self.activity_df = (
            self.activity_df
            .withColumn(
                "date",
                F.to_date(
                    F.col("date")
                )
            )
        )

        # ----------------------------------------------------
        # Null timestamp.
        # ----------------------------------------------------

        null_timestamp = (
            self.activity_df
            .filter(
                F.col("timestamp").isNull()
            )
            .count()
        )

        if null_timestamp > 0:

            raise AssertionError(
                f"Found {null_timestamp} rows with "
                "null timestamp."
            )

        # ----------------------------------------------------
        # Null grid.
        # ----------------------------------------------------

        null_grid = (
            self.activity_df
            .filter(
                F.col("grid_id").isNull()
            )
            .count()
        )

        if null_grid > 0:

            raise AssertionError(
                f"Found {null_grid} rows with "
                "null grid_id."
            )

        logger.info(
            "Required columns found."
        )

        logger.info(
            "Input validation: PASS"
        )

    # ========================================================
    # 3. VALIDATE GRAIN
    # ========================================================

    def validate_grain(self):

        logger.info(
            "Checking grid_id + timestamp grain..."
        )

        duplicate_count = (
            self.activity_df
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

        if duplicate_count > 0:

            raise AssertionError(
                "Duplicate grid_id + timestamp records: "
                f"{duplicate_count}"
            )

        logger.info(
            "ZERO duplicates on "
            "(grid_id, timestamp): PASS"
        )

    # ========================================================
    # 4. CREATE PROCESSED DATAFRAME
    # ========================================================

    def create_processed_dataframe(self):

        logger.info(
            "Preparing processed activity DataFrame..."
        )

        columns = [
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

        self.processed_df = (
            self.activity_df
            .select(columns)
        )

        return self.processed_df

    # ========================================================
    # 5. WRITE PROCESSED ACTIVITY
    # ========================================================

    def write_processed_activity(self):

        logger.info("=" * 70)
        logger.info("WRITING PROCESSED ACTIVITY")
        logger.info("=" * 70)

        if self.processed_df is None:

            self.create_processed_dataframe()

        (
            self.processed_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(
                str(PROCESSED_ACTIVITY_DIR)
            )
        )

        logger.info(
            "Processed activity written to:"
        )

        logger.info(
            "%s",
            PROCESSED_ACTIVITY_DIR
        )

        return self.processed_df

    # ========================================================
    # 6. CREATE HOURLY GRID SUMMARY
    # ========================================================

    def create_hourly_grid_summary(self):

        logger.info("=" * 70)
        logger.info("CREATING HOURLY GRID SUMMARY")
        logger.info("=" * 70)

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
            "internet_share"
        ]

        self.hourly_grid_summary = (
            self.activity_df
            .select(columns)
        )

        # ----------------------------------------------------
        # Geometry must NOT exist.
        # ----------------------------------------------------

        assert (
            "geometry"
            not in
            self.hourly_grid_summary.columns
        ), (
            "Geometry must not exist in "
            "hourly_grid_summary."
        )

        # ----------------------------------------------------
        # country_code must NOT exist.
        # ----------------------------------------------------

        assert (
            "country_code"
            not in
            self.hourly_grid_summary.columns
        ), (
            "country_code must not exist in "
            "hourly_grid_summary."
        )

        # ----------------------------------------------------
        # Required grain assertion.
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

        assert (
            duplicate_count == 0
        ), (
            "hourly_grid_summary contains "
            "duplicate grid_id + timestamp records."
        )

        logger.info(
            "Hourly grid grain: PASS"
        )

        return self.hourly_grid_summary

    # ========================================================
    # 7. WRITE HOURLY GRID SUMMARY
    # ========================================================

    def write_hourly_grid_summary(self):

        logger.info("=" * 70)
        logger.info("WRITING HOURLY GRID SUMMARY")
        logger.info("=" * 70)

        if self.hourly_grid_summary is None:

            self.create_hourly_grid_summary()

        (
            self.hourly_grid_summary
            .write
            .mode("overwrite")
            .parquet(
                str(ANALYTICS_DIR)
            )
        )

        logger.info(
            "Hourly grid summary written to:"
        )

        logger.info(
            "%s",
            ANALYTICS_DIR
        )

        return self.hourly_grid_summary

    # ========================================================
    # 8. CREATE DASHBOARD SUMMARY
    # ========================================================

    def create_dashboard_summary(self):

        logger.info("=" * 70)
        logger.info("CREATING DASHBOARD SUMMARY")
        logger.info("=" * 70)

        self.dashboard_df = (
            self.activity_df
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
                ),

                F.countDistinct(
                    "grid_id"
                ).alias(
                    "active_grids"
                )
            )
            .orderBy(
                "date"
            )
        )

        self.dashboard_df.show(
            truncate=False
        )

        return self.dashboard_df

    # ========================================================
    # 9. WRITE DASHBOARD CSV
    # ========================================================

    def write_dashboard_summary(self):

        logger.info(
            "Writing dashboard summary CSV..."
        )

        if self.dashboard_df is None:

            self.create_dashboard_summary()

        temp_dir = (
            OUTPUT_DIR
            / "dashboard_summary_tmp"
        )

        if temp_dir.exists():

            shutil.rmtree(
                temp_dir
            )

        (
            self.dashboard_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(temp_dir)
            )
        )

        csv_files = list(
            temp_dir.glob(
                "part-*.csv"
            )
        )

        if not csv_files:

            raise FileNotFoundError(
                "Spark did not generate dashboard CSV."
            )

        source = csv_files[0]

        if DASHBOARD_SUMMARY.exists():

            DASHBOARD_SUMMARY.unlink()

        shutil.copy2(
            source,
            DASHBOARD_SUMMARY
        )

        shutil.rmtree(
            temp_dir,
            ignore_errors=True
        )

        logger.info(
            "Dashboard CSV created: %s",
            DASHBOARD_SUMMARY
        )

        return DASHBOARD_SUMMARY

    # ========================================================
    # 10. COPY GEOJSON
    # ========================================================

    def copy_reference_geojson(self):

        logger.info("=" * 70)
        logger.info("COPYING STATIC GEOJSON REFERENCE")
        logger.info("=" * 70)

        source = (
            DATA_DIR
            / "milano-grid.geojson"
        )

        if not source.exists():

            raise FileNotFoundError(
                "Milan GeoJSON not found:\n"
                f"{source}"
            )

        shutil.copy2(
            source,
            REFERENCE_GEOJSON
        )

        logger.info(
            "GeoJSON retained separately:"
        )

        logger.info(
            "%s",
            REFERENCE_GEOJSON
        )

        return REFERENCE_GEOJSON

    # ========================================================
    # 11. COMPARE SCHEMA SAFELY
    # ========================================================

    @staticmethod
    def schemas_match(
        expected_df,
        actual_df,
        label
    ):

        expected = {
            field.name: field.dataType.simpleString()
            for field in expected_df.schema.fields
        }

        actual = {
            field.name: field.dataType.simpleString()
            for field in actual_df.schema.fields
        }

        expected_names = set(
            expected.keys()
        )

        actual_names = set(
            actual.keys()
        )

        # ----------------------------------------------------
        # Missing columns.
        # ----------------------------------------------------

        missing = (
            expected_names
            -
            actual_names
        )

        # ----------------------------------------------------
        # Extra columns.
        # ----------------------------------------------------

        extra = (
            actual_names
            -
            expected_names
        )

        # ----------------------------------------------------
        # Type differences.
        # ----------------------------------------------------

        type_differences = {}

        for column in (
            expected_names
            &
            actual_names
        ):

            if (
                expected[column]
                !=
                actual[column]
            ):

                type_differences[column] = {
                    "expected":
                        expected[column],

                    "actual":
                        actual[column]
                }

        # ----------------------------------------------------
        # Report differences.
        # ----------------------------------------------------

        if missing:

            logger.error(
                "%s missing columns: %s",
                label,
                sorted(missing)
            )

        if extra:

            logger.error(
                "%s extra columns: %s",
                label,
                sorted(extra)
            )

        if type_differences:

            logger.error(
                "%s type differences: %s",
                label,
                type_differences
            )

        if (
            missing
            or
            extra
            or
            type_differences
        ):

            raise AssertionError(
                f"{label} schema validation failed."
            )

        logger.info(
            "%s logical schema: PASS",
            label
        )

    # ========================================================
    # 12. PROCESSED ROUND TRIP
    # ========================================================

    def validate_processed_round_trip(self):

        logger.info("=" * 70)
        logger.info(
            "ROUND-TRIP VALIDATION - PROCESSED PARQUET"
        )
        logger.info("=" * 70)

        read_back = (
            self.spark.read
            .parquet(
                str(PROCESSED_ACTIVITY_DIR)
            )
        )

        read_back_count = (
            read_back.count()
        )

        logger.info(
            "Original row count: %d",
            self.original_row_count
        )

        logger.info(
            "Read-back row count: %d",
            read_back_count
        )

        # ----------------------------------------------------
        # ROW COUNT
        # ----------------------------------------------------

        if (
            read_back_count
            !=
            self.original_row_count
        ):

            raise AssertionError(
                "Processed Parquet row count mismatch."
            )

        logger.info(
            "Processed row count: PASS"
        )

        # ----------------------------------------------------
        # SCHEMA
        #
        # Do NOT compare StructType directly.
        #
        # Partitioned Parquet can change column ordering
        # and partition-column representation.
        #
        # Compare column names + logical data types instead.
        # ----------------------------------------------------

        self.schemas_match(
            self.processed_df,
            read_back,
            "Processed Parquet"
        )

        # ----------------------------------------------------
        # DATE PARTITION COLUMN
        # ----------------------------------------------------

        if "date" not in read_back.columns:

            raise AssertionError(
                "date partition column was not "
                "recovered after Parquet read."
            )

        logger.info(
            "date partition column: PASS"
        )

    # ========================================================
    # 13. ANALYTICS ROUND TRIP
    # ========================================================

    def validate_analytics_round_trip(self):

        logger.info("=" * 70)
        logger.info(
            "ROUND-TRIP VALIDATION - ANALYTICS PARQUET"
        )
        logger.info("=" * 70)

        read_back = (
            self.spark.read
            .parquet(
                str(ANALYTICS_DIR)
            )
        )

        self.analytics_row_count = (
            read_back.count()
        )

        logger.info(
            "Original analytics rows: %d",
            self.original_row_count
        )

        logger.info(
            "Read-back analytics rows: %d",
            self.analytics_row_count
        )

        # ----------------------------------------------------
        # ROW COUNT
        # ----------------------------------------------------

        if (
            self.analytics_row_count
            !=
            self.original_row_count
        ):

            raise AssertionError(
                "Analytics Parquet row count mismatch."
            )

        logger.info(
            "Analytics row count: PASS"
        )

        # ----------------------------------------------------
        # SCHEMA
        # ----------------------------------------------------

        self.schemas_match(
            self.hourly_grid_summary,
            read_back,
            "Analytics Parquet"
        )

        # ----------------------------------------------------
        # NO GEOMETRY
        # ----------------------------------------------------

        if "geometry" in read_back.columns:

            raise AssertionError(
                "Geometry exists in analytics output."
            )

        logger.info(
            "No geometry: PASS"
        )

        # ----------------------------------------------------
        # NO COUNTRY CODE
        # ----------------------------------------------------

        if "country_code" in read_back.columns:

            raise AssertionError(
                "country_code exists in analytics output."
            )

        logger.info(
            "No country_code: PASS"
        )

        # ----------------------------------------------------
        # DUPLICATES
        # ----------------------------------------------------

        duplicate_count = (
            read_back
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
                "Duplicates found after analytics "
                "Parquet round-trip."
            )

        logger.info(
            "Zero duplicates after round trip: PASS"
        )

    # ========================================================
    # 14. PARTITION VALIDATION
    # ========================================================

    def validate_partition_layout(self):

        logger.info("=" * 70)
        logger.info(
            "VALIDATING DATE PARTITIONS"
        )
        logger.info("=" * 70)

        partitions = []

        if PROCESSED_ACTIVITY_DIR.exists():

            for path in (
                PROCESSED_ACTIVITY_DIR.iterdir()
            ):

                if (
                    path.is_dir()
                    and
                    path.name.startswith(
                        "date="
                    )
                ):

                    partitions.append(
                        path.name
                    )

        logger.info(
            "Date partitions found: %d",
            len(partitions)
        )

        for partition in sorted(
            partitions
        ):

            logger.info(
                "  %s",
                partition
            )

        if not partitions:

            raise AssertionError(
                "No date=... partitions found."
            )

        logger.info(
            "Date partition validation: PASS"
        )

        return partitions

    # ========================================================
    # 15. DIRECTORY SIZE
    # ========================================================

    @staticmethod
    def directory_size(path):

        path = Path(path)

        if not path.exists():

            return 0

        if path.is_file():

            return path.stat().st_size

        total = 0

        for file in path.rglob("*"):

            if file.is_file():

                total += (
                    file.stat().st_size
                )

        return total

    # ========================================================
    # 16. FILE SIZE COMPARISON
    # ========================================================

    def compare_file_sizes(self):

        logger.info("=" * 70)
        logger.info(
            "FILE SIZE COMPARISON"
        )
        logger.info("=" * 70)

        processed_parquet_size = (
            self.directory_size(
                PROCESSED_ACTIVITY_DIR
            )
        )

        analytics_parquet_size = (
            self.directory_size(
                ANALYTICS_DIR
            )
        )

        dashboard_csv_size = (
            self.directory_size(
                DASHBOARD_SUMMARY
            )
        )

        # ----------------------------------------------------
        # Create temporary CSV version of analytics.
        # ----------------------------------------------------

        comparison_dir = (
            OUTPUT_DIR
            / "sp6_csv_comparison"
        )

        if comparison_dir.exists():

            shutil.rmtree(
                comparison_dir
            )

        (
            self.hourly_grid_summary
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(comparison_dir)
            )
        )

        analytics_csv_size = (
            self.directory_size(
                comparison_dir
            )
        )

        self.results_file_sizes = {
            "processed_parquet":
                processed_parquet_size,

            "analytics_parquet":
                analytics_parquet_size,

            "analytics_csv":
                analytics_csv_size,

            "dashboard_csv":
                dashboard_csv_size
        }

        logger.info(
            "Processed Parquet: %d bytes",
            processed_parquet_size
        )

        logger.info(
            "Analytics Parquet: %d bytes",
            analytics_parquet_size
        )

        logger.info(
            "Analytics CSV: %d bytes",
            analytics_csv_size
        )

        logger.info(
            "Dashboard CSV: %d bytes",
            dashboard_csv_size
        )

        if analytics_parquet_size > 0:

            ratio = (
                analytics_csv_size
                /
                analytics_parquet_size
            )

        else:

            ratio = 0

        self.results_file_sizes[
            "csv_parquet_ratio"
        ] = ratio

        logger.info(
            "Analytics CSV / Parquet ratio: %.2f",
            ratio
        )

        shutil.rmtree(
            comparison_dir,
            ignore_errors=True
        )

    # ========================================================
    # 17. WRITE REPORT
    # ========================================================

    def write_report(
        self,
        partitions
    ):

        logger.info(
            "Writing SP6 report..."
        )

        sizes = (
            self.results_file_sizes
        )

        with open(
            SP6_REPORT,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP6 — WRITE PROCESSED & ANALYTICS DATA\n"
            )

            file.write(
                "=" * 70
                + "\n\n"
            )

            file.write(
                "INPUT\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                f"SP3 input:\n"
                f"{SP3_INPUT}\n"
            )

            file.write(
                f"Input rows: "
                f"{self.original_row_count}\n\n"
            )

            file.write(
                "OUTPUTS\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                f"Processed activity:\n"
                f"{PROCESSED_ACTIVITY_DIR}\n\n"
            )

            file.write(
                f"Hourly grid summary:\n"
                f"{ANALYTICS_DIR}\n\n"
            )

            file.write(
                f"Dashboard summary:\n"
                f"{DASHBOARD_SUMMARY}\n\n"
            )

            file.write(
                f"Static GeoJSON:\n"
                f"{REFERENCE_GEOJSON}\n\n"
            )

            file.write(
                "PARTITIONING\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "Processed activity is partitioned by date.\n"
            )

            file.write(
                f"Number of partitions: "
                f"{len(partitions)}\n"
            )

            for partition in sorted(
                partitions
            ):

                file.write(
                    f"  {partition}\n"
                )

            file.write(
                "\n"
            )

            file.write(
                "ROUND-TRIP VALIDATION\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "Processed row count: PASS\n"
            )

            file.write(
                "Processed logical schema: PASS\n"
            )

            file.write(
                "Analytics row count: PASS\n"
            )

            file.write(
                "Analytics logical schema: PASS\n"
            )

            file.write(
                "Zero duplicates on grid_id + timestamp: PASS\n"
            )

            file.write(
                "No geometry in analytics: PASS\n"
            )

            file.write(
                "No country_code in analytics: PASS\n\n"
            )

            file.write(
                "FILE SIZE COMPARISON\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                f"Processed Parquet: "
                f"{sizes.get('processed_parquet', 0)} bytes\n"
            )

            file.write(
                f"Analytics Parquet: "
                f"{sizes.get('analytics_parquet', 0)} bytes\n"
            )

            file.write(
                f"Analytics CSV: "
                f"{sizes.get('analytics_csv', 0)} bytes\n"
            )

            file.write(
                f"Dashboard CSV: "
                f"{sizes.get('dashboard_csv', 0)} bytes\n"
            )

            file.write(
                f"CSV / Parquet ratio: "
                f"{sizes.get('csv_parquet_ratio', 0):.2f}\n\n"
            )

            file.write(
                "STORAGE DECISIONS\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "Parquet is used for processed and analytics "
                "data because it is columnar and efficient "
                "for analytical workloads.\n\n"
            )

            file.write(
                "Processed activity is partitioned by date "
                "to support date-based filtering and reduce "
                "unnecessary data reads.\n\n"
            )

            file.write(
                "hourly_grid_summary does not contain geometry. "
                "Full Polygon geometry belongs in the static "
                "Milan grid reference rather than being "
                "duplicated into every hourly fact row.\n\n"
            )

            file.write(
                "The Milan GeoJSON is retained separately under "
                "data/reference/.\n\n"
            )

            file.write(
                "CSV is used for the small dashboard summary "
                "because it is easy to inspect and consume.\n\n"
            )

            file.write(
                "APPEND VS OVERWRITE\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "overwrite is appropriate for this training "
                "pipeline because the supplied dataset is "
                "being rebuilt completely.\n\n"
            )

            file.write(
                "append is appropriate for controlled incremental "
                "loads where new daily partitions are added. "
                "Append requires idempotency controls to avoid "
                "duplicate records.\n"
            )

        logger.info(
            "SP6 report written: %s",
            SP6_REPORT
        )

    # ========================================================
    # 18. RUN
    # ========================================================

    def run(self):

        try:

            logger.info("=" * 70)
            logger.info(
                "SP6 STORAGE PIPELINE STARTED"
            )
            logger.info("=" * 70)

            # ------------------------------------------------
            # LOAD
            # ------------------------------------------------

            self.load_sp3_data()

            # ------------------------------------------------
            # INPUT VALIDATION
            # ------------------------------------------------

            self.validate_input()

            self.validate_grain()

            # ------------------------------------------------
            # PROCESSED DATA
            # ------------------------------------------------

            self.create_processed_dataframe()

            self.write_processed_activity()

            # ------------------------------------------------
            # ANALYTICS
            # ------------------------------------------------

            self.create_hourly_grid_summary()

            self.write_hourly_grid_summary()

            # ------------------------------------------------
            # DASHBOARD
            # ------------------------------------------------

            self.create_dashboard_summary()

            self.write_dashboard_summary()

            # ------------------------------------------------
            # STATIC REFERENCE
            # ------------------------------------------------

            self.copy_reference_geojson()

            # ------------------------------------------------
            # ROUND TRIP
            # ------------------------------------------------

            self.validate_processed_round_trip()

            self.validate_analytics_round_trip()

            # ------------------------------------------------
            # PARTITION VALIDATION
            # ------------------------------------------------

            partitions = (
                self.validate_partition_layout()
            )

            # ------------------------------------------------
            # SIZE COMPARISON
            # ------------------------------------------------

            self.compare_file_sizes()

            # ------------------------------------------------
            # REPORT
            # ------------------------------------------------

            self.write_report(
                partitions
            )

            logger.info("=" * 70)
            logger.info(
                "ALL SP6 ACCEPTANCE CRITERIA PASSED"
            )
            logger.info("=" * 70)

            return {
                "processed_activity":
                    str(PROCESSED_ACTIVITY_DIR),

                "hourly_grid_summary":
                    str(ANALYTICS_DIR),

                "dashboard_summary":
                    str(DASHBOARD_SUMMARY),

                "reference_geojson":
                    str(REFERENCE_GEOJSON),

                "report":
                    str(SP6_REPORT)
            }

        except Exception:

            logger.exception(
                "SP6 processing failed."
            )

            raise
