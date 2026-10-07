from pathlib import Path
import logging

import pandas as pd

from sqlalchemy import (
    create_engine,
    text,
    inspect
)
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# PROJECT PATH
# ============================================================

DE6_ROOT = Path(__file__).resolve().parent


# ============================================================
# SP3 SOURCE
# ============================================================

SPARK_OUTPUT = (
    DE6_ROOT.parent.parent
    / "phase 2"
    / "output"
    / "sp3"
    / "hourly_grid_summary"
)


# ============================================================
# MYSQL CONFIGURATION
# ============================================================

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "network_analytics"


DATABASE_URL = (
    f"mysql+pymysql://"
    f"{MYSQL_USER}:"
    f"{MYSQL_PASSWORD}@"
    f"{MYSQL_HOST}:"
    f"{MYSQL_PORT}/"
    f"{MYSQL_DATABASE}"
    "?charset=utf8mb4"
)


# ============================================================
# LOGGING
# ============================================================

LOG_DIR = DE6_ROOT / "logs"

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)


LOG_FILE = LOG_DIR / "de6_validation.log"


logger = logging.getLogger(
    "DE6_VALIDATION"
)

logger.setLevel(
    logging.INFO
)

logger.propagate = False


if not logger.handlers:

    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8"
    )

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(
        formatter
    )

    console_handler.setFormatter(
        formatter
    )

    logger.addHandler(
        file_handler
    )

    logger.addHandler(
        console_handler
    )


# ============================================================
# CREATE MYSQL ENGINE
# ============================================================

def create_database_engine():

    logger.info(
        "Connecting to MySQL warehouse..."
    )

    try:

        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=3600
        )

        with engine.connect() as connection:

            connection.execute(
                text("SELECT 1")
            )

        logger.info(
            "MySQL connection successful."
        )

        return engine

    except SQLAlchemyError as error:

        logger.error(
            "MySQL connection failed: %s",
            error
        )

        raise


# ============================================================
# LOAD SP3 SOURCE
# ============================================================

def load_sp3_source():

    logger.info(
        "Loading SP3 source for reconciliation..."
    )

    if not SPARK_OUTPUT.exists():

        raise FileNotFoundError(
            f"SP3 output not found: {SPARK_OUTPUT}"
        )


    csv_files = sorted(
        SPARK_OUTPUT.glob("*.csv")
    )


    csv_files = [

        file

        for file in csv_files

        if not file.name.startswith("_")

    ]


    if not csv_files:

        raise FileNotFoundError(
            "No SP3 CSV files found."
        )


    frames = []


    for file in csv_files:

        logger.info(
            "Reading source: %s",
            file.name
        )

        frames.append(
            pd.read_csv(file)
        )


    source_df = pd.concat(
        frames,
        ignore_index=True
    )


    source_df["timestamp"] = pd.to_datetime(
        source_df["timestamp"],
        errors="coerce"
    )


    source_df["grid_id"] = pd.to_numeric(
        source_df["grid_id"],
        errors="coerce"
    )


    source_df["total_activity"] = pd.to_numeric(
        source_df["total_activity"],
        errors="coerce"
    )


    if source_df["timestamp"].isna().any():

        raise ValueError(
            "SP3 contains invalid timestamps."
        )


    if source_df["grid_id"].isna().any():

        raise ValueError(
            "SP3 contains invalid grid IDs."
        )


    if source_df["total_activity"].isna().any():

        raise ValueError(
            "SP3 contains invalid total_activity."
        )


    logger.info(
        "SP3 source rows: %d",
        len(source_df)
    )


    return source_df


# ============================================================
# 1. FACT ROW COUNT
# ============================================================

def validate_fact_row_count(
    source_df,
    engine
):

    source_count = len(
        source_df
    )


    query = text(
        """
        SELECT COUNT(*) AS row_count
        FROM fact_network_activity
        """
    )


    warehouse_count = int(

        pd.read_sql(
            query,
            engine
        ).iloc[0]["row_count"]

    )


    logger.info(
        "Source rows: %d",
        source_count
    )


    logger.info(
        "Warehouse fact rows: %d",
        warehouse_count
    )


    assert (

        warehouse_count
        ==
        source_count

    ), (

        "FACT ROW COUNT FAILED"

    )


    logger.info(
        "FACT ROW COUNT: PASS"
    )


# ============================================================
# 2. DIM GRID COUNT
# ============================================================

def validate_dim_grid_count(
    source_df,
    engine
):

    expected_count = int(

        source_df["grid_id"]
        .nunique()

    )


    actual_count = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS row_count
                FROM dim_grid
                """
            ),

            engine

        ).iloc[0]["row_count"]

    )


    logger.info(
        "Expected dim_grid rows: %d",
        expected_count
    )


    logger.info(
        "Actual dim_grid rows: %d",
        actual_count
    )


    assert (

        actual_count
        ==
        expected_count

    ), (

        "DIM GRID COUNT FAILED"

    )


    logger.info(
        "DIM GRID COUNT: PASS"
    )


# ============================================================
# 3. DIM GRID DUPLICATE VALIDATION
# ============================================================

def validate_dim_grid_duplicates(
    engine
):

    result = pd.read_sql(

        text(
            """
            SELECT
                grid_id,
                COUNT(*) AS duplicate_count

            FROM dim_grid

            GROUP BY
                grid_id

            HAVING
                COUNT(*) > 1
            """
        ),

        engine

    )


    assert result.empty, (

        "Duplicate grid IDs found in dim_grid."

    )


    logger.info(
        "DIM GRID DUPLICATE CHECK: PASS"
    )


# ============================================================
# 4. FACT GEOMETRY EXCLUSION
# ============================================================

def validate_fact_geometry(
    engine
):

    inspector = inspect(
        engine
    )


    columns = [

        column["name"]

        for column

        in inspector.get_columns(
            "fact_network_activity"
        )

    ]


    assert (
        "geometry"
        not in columns
    )


    assert (
        "geometry_reference"
        not in columns
    )


    logger.info(
        "FACT GEOMETRY EXCLUSION: PASS"
    )


# ============================================================
# 5. FOREIGN KEY VALIDATION
# ============================================================

def validate_foreign_keys(
    engine
):

    orphan_time = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_network_activity f

                LEFT JOIN dim_time t
                    ON f.time_key = t.time_key

                WHERE t.time_key IS NULL
                """
            ),

            engine

        ).iloc[0]["count"]

    )


    orphan_grid = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_network_activity f

                LEFT JOIN dim_grid g
                    ON f.grid_key = g.grid_key

                WHERE g.grid_key IS NULL
                """
            ),

            engine

        ).iloc[0]["count"]

    )


    assert orphan_time == 0

    assert orphan_grid == 0


    logger.info(
        "FOREIGN KEY VALIDATION: PASS"
    )


# ============================================================
# 6. FACT GRAIN VALIDATION
# ============================================================

def validate_fact_grain(
    engine
):

    duplicate_count = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count

                FROM (

                    SELECT
                        time_key,
                        grid_key

                    FROM fact_network_activity

                    GROUP BY
                        time_key,
                        grid_key

                    HAVING COUNT(*) > 1

                ) duplicates
                """
            ),

            engine

        ).iloc[0]["count"]

    )


    assert duplicate_count == 0


    logger.info(
        "FACT GRAIN VALIDATION: PASS"
    )


# ============================================================
# 7. TOTAL ACTIVITY RECONCILIATION
# ============================================================

def validate_total_activity(
    source_df,
    engine
):

    source_total = float(

        source_df[
            "total_activity"
        ].sum()

    )


    warehouse_total = float(

        pd.read_sql(

            text(
                """
                SELECT
                    SUM(total_activity)
                    AS total_activity

                FROM fact_network_activity
                """
            ),

            engine

        ).iloc[0]["total_activity"]

    )


    difference = abs(

        source_total
        -
        warehouse_total

    )


    tolerance = max(

        abs(source_total)
        * 1e-9,

        0.000001

    )


    logger.info(
        "SP3 total_activity: %.10f",
        source_total
    )


    logger.info(
        "Warehouse total_activity: %.10f",
        warehouse_total
    )


    logger.info(
        "Total activity difference: %.10f",
        difference
    )


    logger.info(
        "Allowed tolerance: %.10f",
        tolerance
    )


    assert difference <= tolerance, (

        "TOTAL ACTIVITY RECONCILIATION FAILED"

    )


    logger.info(
        "TOTAL ACTIVITY VALIDATION: PASS"
    )


# ============================================================
# 8. TOP GRID RECONCILIATION
# ============================================================

def validate_top_grid(
    source_df,
    engine
):

    expected_top_grid = int(

        source_df

        .groupby(
            "grid_id"
        )["total_activity"]

        .sum()

        .idxmax()

    )


    result = pd.read_sql(

        text(
            """
            SELECT
                g.grid_id,

                SUM(
                    f.total_activity
                ) AS total_activity

            FROM fact_network_activity f

            INNER JOIN dim_grid g
                ON f.grid_key = g.grid_key

            GROUP BY
                g.grid_id

            ORDER BY
                total_activity DESC

            LIMIT 1
            """
        ),

        engine

    )


    if result.empty:

        raise AssertionError(
            "Warehouse returned no top grid."
        )


    warehouse_top_grid = int(
        result.iloc[0]["grid_id"]
    )


    logger.info(
        "Expected top grid: %d",
        expected_top_grid
    )


    logger.info(
        "Warehouse top grid: %d",
        warehouse_top_grid
    )


    assert (

        expected_top_grid
        ==
        warehouse_top_grid

    ), (

        "TOP GRID RECONCILIATION FAILED"

    )


    logger.info(
        "TOP GRID RECONCILIATION: PASS"
    )


# ============================================================
# 9. INDEX VALIDATION
# ============================================================

def validate_indexes(
    engine
):

    inspector = inspect(
        engine
    )


    fact_indexes = inspector.get_indexes(
        "fact_network_activity"
    )


    index_names = {

        index["name"]

        for index in fact_indexes

    }


    required_indexes = {

        "idx_fact_grid",

        "idx_fact_time"

    }


    missing = (

        required_indexes
        -
        index_names

    )


    assert not missing, (

        "Missing required indexes: "
        f"{sorted(missing)}"

    )


    logger.info(
        "INDEX VALIDATION: PASS"
    )


# ============================================================
# 10. DIM TIME VALIDATION
# ============================================================

def validate_dim_time(
    source_df,
    engine
):

    expected_count = int(

        source_df[
            "timestamp"
        ]
        .nunique()

    )


    actual_count = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*)
                AS row_count

                FROM dim_time
                """
            ),

            engine

        ).iloc[0]["row_count"]

    )


    logger.info(
        "Expected dim_time rows: %d",
        expected_count
    )


    logger.info(
        "Actual dim_time rows: %d",
        actual_count
    )


    assert (

        expected_count
        ==
        actual_count

    ), (

        "DIM TIME COUNT FAILED"

    )


    duplicate_count = int(

        pd.read_sql(

            text(
                """
                SELECT COUNT(*)
                FROM (

                    SELECT
                        timestamp

                    FROM dim_time

                    GROUP BY timestamp

                    HAVING COUNT(*) > 1

                ) duplicates
                """
            ),

            engine

        ).iloc[0][0]

    )


    assert duplicate_count == 0


    logger.info(
        "DIM TIME VALIDATION: PASS"
    )


# ============================================================
# MAIN VALIDATION
# ============================================================

def run_validation():

    logger.info(
        "============================================================"
    )

    logger.info(
        "DE6 VALIDATION STARTED"
    )

    logger.info(
        "============================================================"
    )


    engine = None


    try:

        # ----------------------------------------------------
        # LOAD SOURCE
        # ----------------------------------------------------

        source_df = load_sp3_source()


        # ----------------------------------------------------
        # CONNECT MYSQL
        # ----------------------------------------------------

        engine = create_database_engine()


        # ----------------------------------------------------
        # VALIDATIONS
        # ----------------------------------------------------

        validate_fact_row_count(

            source_df,

            engine

        )


        validate_dim_grid_count(

            source_df,

            engine

        )


        validate_dim_grid_duplicates(

            engine

        )


        validate_dim_time(

            source_df,

            engine

        )


        validate_fact_geometry(

            engine

        )


        validate_foreign_keys(

            engine

        )


        validate_fact_grain(

            engine

        )


        validate_total_activity(

            source_df,

            engine

        )


        validate_top_grid(

            source_df,

            engine

        )


        validate_indexes(

            engine

        )


        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        logger.info(
            "============================================================"
        )

        logger.info(
            "ALL DE6 VALIDATIONS PASSED"
        )

        logger.info(
            "============================================================"
        )


    except Exception as error:

        logger.error(
            "DE6 VALIDATION FAILED: %s",
            error
        )

        raise


    finally:

        if engine is not None:

            engine.dispose()

            logger.info(
                "MySQL connection pool closed."
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_validation()
