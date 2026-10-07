from pathlib import Path
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    input_file_name,
    to_timestamp,
    count,
    countDistinct
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "output"
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
# FILE PATTERN
# ============================================================

INPUT_PATTERN = "sms-call-internet-mi-*.csv"


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = (
    LOG_DIR
    / "sp1_ingestion.log"
)

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
# MANUAL SCHEMA
# ============================================================

RAW_SCHEMA = StructType([

    StructField(
        "datetime",
        StringType(),
        True
    ),

    StructField(
        "CellID",
        IntegerType(),
        True
    ),

    StructField(
        "countrycode",
        IntegerType(),
        True
    ),

    StructField(
        "smsin",
        DoubleType(),
        True
    ),

    StructField(
        "smsout",
        DoubleType(),
        True
    ),

    StructField(
        "callin",
        DoubleType(),
        True
    ),

    StructField(
        "callout",
        DoubleType(),
        True
    ),

    StructField(
        "internet",
        DoubleType(),
        True
    )
])


# ============================================================
# SPARK INGESTION
# ============================================================

class SparkNetworkIngestion:

    """
    SP1 — Distributed Ingestion

    Reads all Milan daily telecom files.

    Raw grain:

        datetime + CellID + countrycode

    SP1 intentionally does NOT aggregate
    country-code rows.

    SP1 intentionally does NOT clean or rename
    the raw columns.

    Those operations belong to SP2.
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(self):

        self.spark = None

        self.raw_network_df = None

        self.discovered_files = []

        self.file_count = 0

        self.total_rows = 0

        self.unique_grids = 0

        self.country_code_categories = 0

        self.distinct_timestamps = 0

        self.partition_count = 0

        self.file_report = None

    # ========================================================
    # 1. CREATE SPARK SESSION
    # ========================================================

    def create_spark_session(self):

        logger.info(
            "Creating SparkSession..."
        )

        self.spark = (
            SparkSession
            .builder
            .appName(
                "NetworkOperations-SP1"
            )
            .master("local[*]")
            .config(
                "spark.sql.shuffle.partitions",
                "8"
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
    # 2. DISCOVER FILES
    # ========================================================

    def discover_files(self):

        logger.info(
            "Searching for daily Milan files..."
        )

        logger.info(
            "Raw data directory: %s",
            RAW_DATA_DIR
        )

        logger.info(
            "Required pattern: %s",
            INPUT_PATTERN
        )

        if not RAW_DATA_DIR.exists():

            raise FileNotFoundError(
                "Raw data directory does not exist:\n"
                f"{RAW_DATA_DIR}"
            )

        # ----------------------------------------------------
        # IMPORTANT
        #
        # We use pathlib to discover the files instead
        # of giving Spark a Windows glob path.
        #
        # This avoids:
        #
        # sms-call-internet-mi-\*.csv
        #
        # problems on Windows.
        # ----------------------------------------------------

        self.discovered_files = sorted(
            RAW_DATA_DIR.glob(
                INPUT_PATTERN
            )
        )

        if not self.discovered_files:

            raise FileNotFoundError(
                "No Milan CSV files found.\n"
                f"Directory: {RAW_DATA_DIR}\n"
                f"Pattern: {INPUT_PATTERN}"
            )

        self.file_count = len(
            self.discovered_files
        )

        logger.info(
            "Files discovered: %d",
            self.file_count
        )

        # ----------------------------------------------------
        # VALIDATE FILE NAMES
        # ----------------------------------------------------

        for file_path in self.discovered_files:

            logger.info(
                "Found file: %s",
                file_path.name
            )

            if not file_path.name.startswith(
                "sms-call-internet-mi-"
            ):

                raise ValueError(
                    "Non-Milan file detected: "
                    f"{file_path.name}"
                )

            if not file_path.name.endswith(
                ".csv"
            ):

                raise ValueError(
                    "Non-CSV file detected: "
                    f"{file_path.name}"
                )

        logger.info(
            "File discovery validation: PASS"
        )

    # ========================================================
    # 3. READ DATA
    # ========================================================

    def read_data(self):

        if self.spark is None:

            raise RuntimeError(
                "SparkSession has not been created."
            )

        if not self.discovered_files:

            raise RuntimeError(
                "Run discover_files() first."
            )

        logger.info(
            "Loading discovered files with Spark..."
        )

        # ----------------------------------------------------
        # IMPORTANT
        #
        # Pass the actual discovered file paths.
        #
        # Do NOT use:
        #
        # C:\\...\\sms-call-internet-mi-*.csv
        #
        # because Windows Hadoop glob handling can cause
        # problems.
        # ----------------------------------------------------

        file_paths = [
            str(
                file_path.resolve()
            )
            for file_path
            in self.discovered_files
        ]

        self.raw_network_df = (
            self.spark.read
            .option(
                "header",
                True
            )
            .option(
                "mode",
                "PERMISSIVE"
            )
            .schema(
                RAW_SCHEMA
            )
            .csv(
                file_paths
            )
        )

        # ----------------------------------------------------
        # SOURCE FILE TRACEABILITY
        # ----------------------------------------------------

        self.raw_network_df = (
            self.raw_network_df
            .withColumn(
                "input_file_name",
                input_file_name()
            )
        )

        logger.info(
            "Multiple CSV files loaded successfully."
        )

        logger.info(
            "Manual StructType schema applied."
        )

        logger.info(
            "Raw country-code grain preserved."
        )

    # ========================================================
    # 4. PREPARE TIMESTAMP
    # ========================================================

    def prepare_timestamp(self):

        if self.raw_network_df is None:

            raise RuntimeError(
                "Data has not been loaded."
            )

        self.raw_network_df = (
            self.raw_network_df
            .withColumn(
                "timestamp",
                to_timestamp(
                    col("datetime")
                )
            )
        )

        invalid_timestamp_count = (
            self.raw_network_df
            .filter(
                col("timestamp").isNull()
            )
            .count()
        )

        if invalid_timestamp_count > 0:

            raise ValueError(
                "Invalid timestamp values found: "
                f"{invalid_timestamp_count}"
            )

        logger.info(
            "Timestamp preparation: PASS"
        )

    # ========================================================
    # 5. VALIDATE SCHEMA
    # ========================================================

    def validate_schema(self):

        logger.info(
            "========== VALIDATED SCHEMA =========="
        )

        self.raw_network_df.printSchema()

        required_columns = [

            "datetime",

            "CellID",

            "countrycode",

            "smsin",

            "smsout",

            "callin",

            "callout",

            "internet",

            "input_file_name",

            "timestamp"
        ]

        actual_columns = (
            self.raw_network_df.columns
        )

        missing_columns = [
            column
            for column
            in required_columns
            if column not in actual_columns
        ]

        if missing_columns:

            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )

        logger.info(
            "Schema validation: PASS"
        )

    # ========================================================
    # 6. BASIC STATISTICS
    # ========================================================

    def calculate_basic_statistics(self):

        logger.info(
            "Calculating SP1 statistics..."
        )

        self.total_rows = (
            self.raw_network_df
            .count()
        )

        self.unique_grids = (
            self.raw_network_df
            .select(
                "CellID"
            )
            .distinct()
            .count()
        )

        self.country_code_categories = (
            self.raw_network_df
            .select(
                "countrycode"
            )
            .distinct()
            .count()
        )

        self.distinct_timestamps = (
            self.raw_network_df
            .select(
                "timestamp"
            )
            .distinct()
            .count()
        )

        self.partition_count = (
            self.raw_network_df
            .rdd
            .getNumPartitions()
        )

        logger.info(
            "Total raw rows: %d",
            self.total_rows
        )

        logger.info(
            "Unique grids: %d",
            self.unique_grids
        )

        logger.info(
            "Country-code categories: %d",
            self.country_code_categories
        )

        logger.info(
            "Distinct hourly timestamps: %d",
            self.distinct_timestamps
        )

        logger.info(
            "Spark partition count: %d",
            self.partition_count
        )

    # ========================================================
    # 7. GRID VALIDATION
    # ========================================================

    def validate_grid_range(self):

        invalid_count = (
            self.raw_network_df
            .filter(
                (col("CellID") < 1)
                |
                (col("CellID") > 10000)
                |
                col("CellID").isNull()
            )
            .count()
        )

        if invalid_count > 0:

            raise ValueError(
                "Invalid CellID values detected: "
                f"{invalid_count}"
            )

        logger.info(
            "All CellID values are within 1-10000."
        )

        logger.info(
            "VALIDATION grid range: PASS"
        )

    # ========================================================
    # 8. TIMESTAMP VALIDATION
    # ========================================================

    def validate_timestamp_count(self):

        expected_timestamps = (
            self.file_count * 24
        )

        logger.info(
            "Distinct timestamps: %d",
            self.distinct_timestamps
        )

        logger.info(
            "Expected timestamps: %d",
            expected_timestamps
        )

        if (
            self.distinct_timestamps
            != expected_timestamps
        ):

            raise ValueError(
                "Timestamp validation failed.\n"
                f"Expected: {expected_timestamps}\n"
                f"Actual: {self.distinct_timestamps}"
            )

        logger.info(
            "VALIDATION timestamp count: PASS"
        )

    # ========================================================
    # 9. FILE TRACEABILITY
    # ========================================================

    def validate_file_traceability(self):

        missing_file_names = (
            self.raw_network_df
            .filter(
                col("input_file_name").isNull()
                |
                (
                    col("input_file_name")
                    == ""
                )
            )
            .count()
        )

        if missing_file_names > 0:

            raise ValueError(
                "Rows without input_file_name: "
                f"{missing_file_names}"
            )

        distinct_files = (
            self.raw_network_df
            .select(
                "input_file_name"
            )
            .distinct()
            .count()
        )

        if distinct_files != self.file_count:

            raise ValueError(
                "File count mismatch.\n"
                f"Discovered: {self.file_count}\n"
                f"In DataFrame: {distinct_files}"
            )

        logger.info(
            "Every row contains input_file_name."
        )

        logger.info(
            "VALIDATION file traceability: PASS"
        )

    # ========================================================
    # 10. RAW GRAIN VALIDATION
    # ========================================================

    def validate_raw_grain(self):

        """
        SP1 must preserve:

            timestamp + CellID + countrycode

        We therefore confirm that multiple
        country-code rows exist for the same
        grid/hour combination.
        """

        grid_hour_count = (
            self.raw_network_df
            .select(
                "timestamp",
                "CellID"
            )
            .distinct()
            .count()
        )

        raw_count = self.total_rows

        logger.info(
            "Raw rows: %d",
            raw_count
        )

        logger.info(
            "Distinct timestamp + CellID groups: %d",
            grid_hour_count
        )

        if raw_count <= grid_hour_count:

            raise ValueError(
                "Raw country-code grain does not appear "
                "to be preserved."
            )

        logger.info(
            "Raw grain validation: PASS"
        )

        logger.info(
            "Raw grain = "
            "timestamp + CellID + countrycode"
        )

    # ========================================================
    # 11. FILE LEVEL REPORT
    # ========================================================

    def generate_file_report(self):

        logger.info(
            "========== FILE LEVEL REPORT =========="
        )

        self.file_report = (
            self.raw_network_df
            .groupBy(
                "input_file_name"
            )
            .agg(
                count("*").alias(
                    "row_count"
                ),

                countDistinct(
                    "CellID"
                ).alias(
                    "unique_grids"
                ),

                countDistinct(
                    "timestamp"
                ).alias(
                    "hourly_intervals"
                ),

                countDistinct(
                    "countrycode"
                ).alias(
                    "country_code_categories"
                )
            )
            .orderBy(
                "input_file_name"
            )
        )

        self.file_report.show(
            100,
            truncate=False
        )

        logger.info(
            "File-level report generated."
        )

    # ========================================================
    # 12. WRITE SUMMARY
    # ========================================================

    def write_summary(self):

        summary_file = (
            OUTPUT_DIR
            / "sp1_summary.txt"
        )

        with open(
            summary_file,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP1 DISTRIBUTED INGESTION SUMMARY\n"
            )

            file.write(
                "=" * 60
                + "\n\n"
            )

            file.write(
                f"Raw data directory:\n"
                f"{RAW_DATA_DIR}\n\n"
            )

            file.write(
                f"Input pattern:\n"
                f"{INPUT_PATTERN}\n\n"
            )

            file.write(
                f"Files loaded: "
                f"{self.file_count}\n"
            )

            file.write(
                f"Total raw rows: "
                f"{self.total_rows}\n"
            )

            file.write(
                f"Unique grids: "
                f"{self.unique_grids}\n"
            )

            file.write(
                f"Country-code categories: "
                f"{self.country_code_categories}\n"
            )

            file.write(
                f"Distinct timestamps: "
                f"{self.distinct_timestamps}\n"
            )

            file.write(
                f"Spark partitions: "
                f"{self.partition_count}\n\n"
            )

            file.write(
                "TIMESTAMP VALIDATION\n"
            )

            file.write(
                "-" * 40
                + "\n"
            )

            file.write(
                f"Expected: "
                f"{self.file_count} × 24 = "
                f"{self.file_count * 24}\n"
            )

            file.write(
                f"Actual: "
                f"{self.distinct_timestamps}\n\n"
            )

            file.write(
                "RAW GRAIN\n"
            )

            file.write(
                "-" * 40
                + "\n"
            )

            file.write(
                "timestamp + CellID + countrycode\n\n"
            )

            file.write(
                "TRACEABILITY\n"
            )

            file.write(
                "-" * 40
                + "\n"
            )

            file.write(
                "Every row contains input_file_name.\n\n"
            )

            file.write(
                "SCHEMA\n"
            )

            file.write(
                "-" * 40
                + "\n"
            )

            file.write(
                "Manual StructType schema used.\n"
            )

            file.write(
                "inferSchema was intentionally not used.\n\n"
            )

            file.write(
                "PARTITIONS\n"
            )

            file.write(
                "-" * 40
                + "\n"
            )

            file.write(
                "Spark partitions determine the number "
                "of execution tasks and influence "
                "distributed processing performance.\n"
            )

        logger.info(
            "SP1 summary written to: %s",
            summary_file
        )

    # ========================================================
    # 13. RUN PIPELINE
    # ========================================================

    def run(self):

        try:

            logger.info(
                "=" * 60
            )

            logger.info(
                "SP1 DISTRIBUTED INGESTION STARTED"
            )

            logger.info(
                "Project root: %s",
                PROJECT_ROOT
            )

            logger.info(
                "Raw data directory: %s",
                RAW_DATA_DIR
            )

            logger.info(
                "Output directory: %s",
                OUTPUT_DIR
            )

            logger.info(
                "Log directory: %s",
                LOG_DIR
            )

            logger.info(
                "=" * 60
            )

            # ------------------------------------------------
            # 1. SPARK
            # ------------------------------------------------

            self.create_spark_session()

            # ------------------------------------------------
            # 2. DISCOVER FILES
            # ------------------------------------------------

            self.discover_files()

            # ------------------------------------------------
            # 3. READ FILES
            # ------------------------------------------------

            self.read_data()

            # ------------------------------------------------
            # 4. TIMESTAMP
            # ------------------------------------------------

            self.prepare_timestamp()

            # ------------------------------------------------
            # 5. SCHEMA
            # ------------------------------------------------

            self.validate_schema()

            # ------------------------------------------------
            # 6. STATISTICS
            # ------------------------------------------------

            self.calculate_basic_statistics()

            # ------------------------------------------------
            # 7. GRID VALIDATION
            # ------------------------------------------------

            self.validate_grid_range()

            # ------------------------------------------------
            # 8. TIMESTAMP VALIDATION
            # ------------------------------------------------

            self.validate_timestamp_count()

            # ------------------------------------------------
            # 9. FILE TRACEABILITY
            # ------------------------------------------------

            self.validate_file_traceability()

            # ------------------------------------------------
            # 10. RAW GRAIN
            # ------------------------------------------------

            self.validate_raw_grain()

            # ------------------------------------------------
            # 11. FILE REPORT
            # ------------------------------------------------

            self.generate_file_report()

            # ------------------------------------------------
            # 12. SUMMARY
            # ------------------------------------------------

            self.write_summary()

            logger.info(
                "=" * 60
            )

            logger.info(
                "ALL SP1 ACCEPTANCE CRITERIA PASSED"
            )

            logger.info(
                "SP1 DISTRIBUTED INGESTION COMPLETED"
            )

            logger.info(
                "=" * 60
            )

            return self.raw_network_df

        finally:

            if self.spark is not None:

                self.spark.stop()

                logger.info(
                    "SparkSession stopped."
                )