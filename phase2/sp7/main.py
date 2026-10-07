import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# PROJECT PATHS
# ============================================================

CURRENT_FILE = Path(__file__).resolve()

PROJECT_ROOT = CURRENT_FILE.parents[2]

PHASE2_ROOT = PROJECT_ROOT / "phase 2"

INPUT_PATH = PHASE2_ROOT / "data" / "raw"

OUTPUT_PATH = PHASE2_ROOT / "data" / "processed"

REFERENCE_PATH = PHASE2_ROOT / "data" / "reference"

LOG_PATH = PHASE2_ROOT / "logs"

HADOOP_HOME = (
    PROJECT_ROOT
    / "winutils"
    / "hadoop-win-utils"
)

HADOOP_BIN = HADOOP_HOME / "bin"

WINUTILS_EXE = HADOOP_BIN / "winutils.exe"


# ============================================================
# DIRECTORY SETUP
# ============================================================

OUTPUT_PATH.mkdir(
    parents=True,
    exist_ok=True
)

REFERENCE_PATH.mkdir(
    parents=True,
    exist_ok=True
)

LOG_PATH.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_PATH / "sp7_telecom_pipeline.log"

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
# HADOOP CONFIGURATION
# ============================================================

def configure_hadoop():

    if not HADOOP_HOME.exists():

        raise FileNotFoundError(
            f"HADOOP_HOME directory not found: "
            f"{HADOOP_HOME}"
        )

    if not HADOOP_BIN.exists():

        raise FileNotFoundError(
            f"Hadoop bin directory not found: "
            f"{HADOOP_BIN}"
        )

    if not WINUTILS_EXE.exists():

        raise FileNotFoundError(
            f"winutils.exe not found: "
            f"{WINUTILS_EXE}"
        )

    os.environ["HADOOP_HOME"] = str(
        HADOOP_HOME
    )

    os.environ["hadoop.home.dir"] = str(
        HADOOP_HOME
    )

    current_path = os.environ.get(
        "PATH",
        ""
    )

    hadoop_bin_string = str(
        HADOOP_BIN
    )

    path_entries = current_path.split(
        os.pathsep
    )

    if hadoop_bin_string not in path_entries:

        os.environ["PATH"] = (
            hadoop_bin_string
            + os.pathsep
            + current_path
        )

    python_executable = sys.executable

    os.environ[
        "PYSPARK_PYTHON"
    ] = python_executable

    os.environ[
        "PYSPARK_DRIVER_PYTHON"
    ] = python_executable

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
        "Python executable: %s",
        python_executable
    )

    logger.info(
        "Hadoop configuration: PASS"
    )


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession
        .builder
        .appName(
            "NetworkOperationsPredictiveIntelligence"
        )
        .master("local[*]")
        .config(
            "spark.sql.shuffle.partitions",
            "14"
        )
        .config(
            "spark.driver.host",
            "127.0.0.1"
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )
        .config(
            "spark.python.worker.reuse",
            "true"
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
# TELECOM PIPELINE
# ============================================================

class TelecomPipeline:

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

        self.raw_df = None

        self.clean_network_df = None

        self.hourly_grid_summary = None

        self.grid_activity_geo_df = None

        self.rejected_df = None

        self.input_rows = 0

        self.output_rows = 0

        self.rejected_rows = 0

        self.nulls_handled = 0


    # ========================================================
    # READ RAW
    # ========================================================

    def read_raw(self):

        logger.info(
            "Reading raw network activity data."
        )

        files = sorted(
            self.input_path.glob(
                "sms-call-internet-mi-*.csv"
            )
        )

        if not files:

            raise FileNotFoundError(
                "No daily activity CSV files found in: "
                f"{self.input_path}"
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
            .option(
                "header",
                True
            )
            .option(
                "inferSchema",
                True
            )
            .csv(
                [str(file) for file in files]
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

        rename_map = {
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet_activity"
        }

        for old_name, new_name in rename_map.items():

            if old_name in df.columns:

                df = df.withColumnRenamed(
                    old_name,
                    new_name
                )

        if "datetime" in df.columns:

            df = df.withColumn(
                "timestamp",
                F.to_timestamp(
                    F.col("datetime")
                )
            )

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        for column in activity_columns:

            if column in df.columns:

                df = df.withColumn(
                    column,
                    F.when(
                        F.trim(
                            F.col(column).cast("string")
                        ) == "",
                        None
                    )
                    .otherwise(
                        F.col(column).cast("double")
                    )
                )

        if "grid_id" in df.columns:

            df = df.withColumn(
                "grid_id",
                F.col("grid_id").cast("integer")
            )

        invalid_condition = (
            F.col("grid_id").isNull()
            |
            F.col("timestamp").isNull()
        )

        for column in activity_columns:

            invalid_condition = (
                invalid_condition
                |
                (
                    F.col(column).isNotNull()
                    &
                    (F.col(column) < 0)
                )
            )

        self.rejected_df = (
            df.filter(
                invalid_condition
            )
        )

        self.rejected_rows = (
            self.rejected_df.count()
        )

        logger.info(
            "REJECTED_ROWS=%d",
            self.rejected_rows
        )

        df = df.filter(
            ~invalid_condition
        )

        null_conditions = []

        for column in activity_columns:

            null_conditions.append(
                F.col(column).isNull()
            )

        if null_conditions:

            combined_null_condition = (
                null_conditions[0]
            )

            for condition in null_conditions[1:]:

                combined_null_condition = (
                    combined_null_condition
                    |
                    condition
                )

            self.nulls_handled = (
                df.filter(
                    combined_null_condition
                ).count()
            )

        for column in activity_columns:

            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.lit(0.0)
                )
            )

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
                F.col("total_sms")
                +
                F.col("total_calls")
                +
                F.col("internet_activity")
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

        self.clean_network_df = df

        logger.info(
            "NULLS_HANDLED=%d",
            self.nulls_handled
        )

        logger.info(
            "Cleaning stage completed."
        )

        return self.clean_network_df


    # ========================================================
    # AGGREGATE
    # ========================================================

    def aggregate(self):

        logger.info(
            "Starting country-code aggregation."
        )

        required_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]

        missing = [
            column
            for column in required_columns
            if column not in self.clean_network_df.columns
        ]

        if missing:

            raise ValueError(
                f"Missing columns for aggregation: {missing}"
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
                F.col("total_sms")
                +
                F.col("total_calls")
                +
                F.col("internet_activity")
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
                "hourly_grid_summary contains duplicate "
                "grid_id + timestamp records."
            )

        row_count = (
            self.hourly_grid_summary.count()
        )

        logger.info(
            "Hourly grid summary rows: %d",
            row_count
        )

        logger.info(
            "Hourly grid grain validation: PASS"
        )

        return self.hourly_grid_summary


    # ========================================================
    # ENRICH
    # ========================================================

    def enrich(self):

        logger.info(
            "Starting geospatial enrichment."
        )

        geojson_file = (
            self.reference_path
            / "milano-grid.geojson"
        )

        if not geojson_file.exists():

            geojson_file = (
                PHASE2_ROOT
                / "data"
                / "milano-grid.geojson"
            )

        if not geojson_file.exists():

            raise FileNotFoundError(
                f"GeoJSON reference file not found: "
                f"{geojson_file}"
            )

        with open(
            geojson_file,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        if geojson.get("type") != "FeatureCollection":

            raise ValueError(
                "GeoJSON must be a FeatureCollection."
            )

        lookup_records = []

        for feature in geojson.get(
            "features",
            []
        ):

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

            geometry = json.dumps(
                feature.get(
                    "geometry"
                ),
                separators=(
                    ",",
                    ":"
                )
            )

            lookup_records.append(
                (
                    grid_id,
                    geometry
                )
            )

        grid_lookup_df = (
            self.spark
            .createDataFrame(
                lookup_records,
                [
                    "grid_id",
                    "geometry"
                ]
            )
        )

        duplicate_keys = (
            grid_lookup_df
            .groupBy("grid_id")
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        if duplicate_keys != 0:

            raise AssertionError(
                "GeoJSON lookup contains duplicate grid IDs."
            )

        self.grid_activity_geo_df = (
            self.hourly_grid_summary
            .join(
                F.broadcast(
                    grid_lookup_df
                ),
                on="grid_id",
                how="left"
            )
        )

        missing_geometry = (
            self.grid_activity_geo_df
            .filter(
                F.col("geometry").isNull()
            )
            .select(
                "grid_id"
            )
            .distinct()
            .count()
        )

        distinct_grids = (
            self.hourly_grid_summary
            .select(
                "grid_id"
            )
            .distinct()
            .count()
        )

        if missing_geometry != 0:

            raise AssertionError(
                f"Missing geometry for "
                f"{missing_geometry} grids."
            )

        coverage = 100.0

        if distinct_grids > 0:

            coverage = (
                (
                    distinct_grids
                    -
                    missing_geometry
                )
                /
                distinct_grids
                *
                100
            )

        logger.info(
            "Geographic enrichment coverage: %.2f%%",
            coverage
        )

        logger.info(
            "Geospatial enrichment completed."
        )

        return self.grid_activity_geo_df


    # ========================================================
    # WRITE OUTPUTS
    # ========================================================

    def write_outputs(self):

        logger.info(
            "Writing processed outputs."
        )

        clean_output = (
            self.output_path
            / "activity"
        )

        hourly_output = (
            self.output_path
            / "hourly_grid_summary"
        )

        dashboard_output = (
            self.output_path
            / "dashboard_summary"
        )

        (
            self.clean_network_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(
                str(clean_output)
            )
        )

        logger.info(
            "Clean activity Parquet written."
        )

        hourly_fact_df = (
            self.hourly_grid_summary
            .select(
                "timestamp",
                "grid_id",
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
                "day_of_week",
                "internet_share"
            )
        )

        (
            hourly_fact_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(
                str(hourly_output)
            )
        )

        logger.info(
            "Hourly grid summary Parquet written."
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
                F.countDistinct(
                    "grid_id"
                ).alias(
                    "active_grids"
                )
            )
            .orderBy("date")
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
                str(dashboard_output)
            )
        )

        logger.info(
            "Dashboard CSV written."
        )

        self.output_rows = (
            hourly_fact_df.count()
        )

        logger.info(
            "OUTPUT_ROWS=%d",
            self.output_rows
        )


    # ========================================================
    # ROUND TRIP VALIDATION
    # ========================================================

    def validate_outputs(self):

        hourly_output = (
            self.output_path
            / "hourly_grid_summary"
        )

        read_back = (
            self.spark.read
            .parquet(
                str(hourly_output)
            )
        )

        original_count = (
            self.hourly_grid_summary.count()
        )

        round_trip_count = (
            read_back.count()
        )

        if original_count != round_trip_count:

            raise AssertionError(
                "Round-trip row count mismatch."
            )

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
                "Round-trip output contains duplicate "
                "grid_id + timestamp records."
            )

        if "geometry" in read_back.columns:

            raise AssertionError(
                "Geometry must not exist in hourly "
                "analytics fact output."
            )

        logger.info(
            "ROUND_TRIP_ROWS=%d",
            round_trip_count
        )

        logger.info(
            "Round-trip validation: PASS"
        )


    # ========================================================
    # RUN
    # ========================================================

    def run(self):

        self.read_raw()

        self.clean()

        self.aggregate()

        self.enrich()

        self.write_outputs()

        self.validate_outputs()

        logger.info(
            "ETL pipeline completed successfully."
        )

        return self.hourly_grid_summary


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = datetime.now()

    spark = None

    try:

        logger.info(
            "============================================================"
        )

        logger.info(
            "SP7 TELECOM ETL JOB STARTED"
        )

        logger.info(
            "============================================================"
        )

        configure_hadoop()

        logger.info(
            "Input path: %s",
            INPUT_PATH
        )

        logger.info(
            "Output path: %s",
            OUTPUT_PATH
        )

        logger.info(
            "Reference path: %s",
            REFERENCE_PATH
        )

        spark = create_spark_session()

        pipeline = TelecomPipeline(
            spark=spark,
            input_path=INPUT_PATH,
            output_path=OUTPUT_PATH,
            reference_path=REFERENCE_PATH
        )

        pipeline.run()

        end_time = datetime.now()

        duration = (
            end_time
            -
            start_time
        )

        logger.info(
            "INPUT_ROWS=%d",
            pipeline.input_rows
        )

        logger.info(
            "REJECTED_ROWS=%d",
            pipeline.rejected_rows
        )

        logger.info(
            "NULLS_HANDLED=%d",
            pipeline.nulls_handled
        )

        logger.info(
            "OUTPUT_ROWS=%d",
            pipeline.output_rows
        )

        logger.info(
            "START_TIME=%s",
            start_time
        )

        logger.info(
            "END_TIME=%s",
            end_time
        )

        logger.info(
            "DURATION=%s",
            duration
        )

        logger.info(
            "STATUS=SUCCESS"
        )

        logger.info(
            "============================================================"
        )

        return 0

    except Exception as exc:

        logger.exception(
            "SP7 TELECOM ETL JOB FAILED"
        )

        logger.error(
            "STATUS=FAILED"
        )

        logger.error(
            "ERROR=%s",
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


if __name__ == "__main__":

    exit_code = main()

    sys.exit(
        exit_code
    )