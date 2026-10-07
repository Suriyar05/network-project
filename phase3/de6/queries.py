
from pathlib import Path
import logging

import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# PROJECT PATH
# ============================================================

DE6_ROOT = Path(__file__).resolve().parent


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


LOG_FILE = LOG_DIR / "de6_queries.log"


logger = logging.getLogger("DE6_QUERIES")

logger.setLevel(logging.INFO)

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

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def create_database_engine():

    logger.info(
        "Connecting to MySQL database..."
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
# CHECK REQUIRED TABLES
# ============================================================

def validate_tables(engine):

    logger.info(
        "Checking warehouse tables..."
    )

    required_tables = {
        "dim_time",
        "dim_grid",
        "fact_network_activity"
    }

    query = text(
        """
        SELECT
            table_name
        FROM information_schema.tables
        WHERE table_schema = :database
        """
    )

    with engine.connect() as connection:

        rows = connection.execute(
            query,
            {
                "database": MYSQL_DATABASE
            }
        ).fetchall()

    existing_tables = {
        row[0]
        for row in rows
    }

    missing_tables = (
        required_tables
        - existing_tables
    )

    if missing_tables:

        raise RuntimeError(
            "Missing warehouse tables: "
            f"{sorted(missing_tables)}"
        )

    logger.info(
        "Warehouse table validation: PASS"
    )


# ============================================================
# QUERY 1
# TOP 10 HIGH-ACTIVITY GRIDS
# ============================================================

def query_top_grids(engine):

    logger.info(
        "============================================================"
    )

    logger.info(
        "QUERY 1: TOP 10 HIGH-ACTIVITY GRIDS"
    )

    logger.info(
        "============================================================"
    )

    query = text(
        """
        SELECT
            g.grid_id,
            SUM(
                f.total_activity
            ) AS total_activity,
            SUM(
                f.total_sms
            ) AS total_sms,
            SUM(
                f.total_calls
            ) AS total_calls,
            SUM(
                f.internet_activity
            ) AS internet_activity
        FROM fact_network_activity f

        INNER JOIN dim_grid g
            ON f.grid_key = g.grid_key

        GROUP BY
            g.grid_id

        ORDER BY
            total_activity DESC

        LIMIT 10
        """
    )

    result = pd.read_sql(
        query,
        engine
    )

    if result.empty:

        logger.warning(
            "No top-grid records found."
        )

        return result

    print()
    print("=" * 70)
    print("TOP 10 HIGH-ACTIVITY GRIDS")
    print("=" * 70)
    print()

    print(
        result.to_string(
            index=False
        )
    )

    print()

    logger.info(
        "Top grid query completed successfully."
    )

    logger.info(
        "Highest-activity grid: %s",
        result.iloc[0]["grid_id"]
    )

    return result


# ============================================================
# QUERY 2
# HOURLY NETWORK ACTIVITY TREND
# ============================================================

def query_hourly_trend(engine):

    logger.info(
        "============================================================"
    )

    logger.info(
        "QUERY 2: HOURLY NETWORK ACTIVITY TREND"
    )

    logger.info(
        "============================================================"
    )

    query = text(
        """
        SELECT
            t.timestamp,
            SUM(
                f.total_activity
            ) AS total_activity,
            SUM(
                f.total_sms
            ) AS total_sms,
            SUM(
                f.total_calls
            ) AS total_calls,
            SUM(
                f.internet_activity
            ) AS internet_activity
        FROM fact_network_activity f

        INNER JOIN dim_time t
            ON f.time_key = t.time_key

        GROUP BY
            t.timestamp

        ORDER BY
            t.timestamp
        """
    )

    result = pd.read_sql(
        query,
        engine
    )

    if result.empty:

        logger.warning(
            "No hourly trend records found."
        )

        return result

    print()
    print("=" * 70)
    print("HOURLY NETWORK ACTIVITY TREND")
    print("=" * 70)
    print()

    print(
        result.to_string(
            index=False
        )
    )

    print()

    logger.info(
        "Hourly trend query completed successfully."
    )

    return result


# ============================================================
# QUERY 3
# INTERNET-HEAVY WINDOWS
# ============================================================

def query_internet_heavy_windows(engine):

    logger.info(
        "============================================================"
    )

    logger.info(
        "QUERY 3: INTERNET-HEAVY WINDOWS"
    )

    logger.info(
        "============================================================"
    )

    query = text(
        """
        SELECT
            t.timestamp,

            SUM(
                f.internet_activity
            ) AS internet_activity,

            SUM(
                f.total_activity
            ) AS total_activity,

            CASE
                WHEN SUM(f.total_activity) > 0
                THEN
                    SUM(f.internet_activity)
                    /
                    SUM(f.total_activity)
                ELSE 0
            END AS internet_share

        FROM fact_network_activity f

        INNER JOIN dim_time t
            ON f.time_key = t.time_key

        GROUP BY
            t.timestamp

        HAVING
            SUM(f.total_activity) > 0

        ORDER BY
            internet_share DESC

        LIMIT 20
        """
    )

    result = pd.read_sql(
        query,
        engine
    )

    if result.empty:

        logger.warning(
            "No internet-heavy windows found."
        )

        return result

    print()
    print("=" * 70)
    print("TOP 20 INTERNET-HEAVY WINDOWS")
    print("=" * 70)
    print()

    print(
        result.to_string(
            index=False
        )
    )

    print()

    logger.info(
        "Internet-heavy query completed successfully."
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info(
        "============================================================"
    )

    logger.info(
        "DE6 ANALYTICS QUERIES STARTED"
    )

    logger.info(
        "============================================================"
    )

    engine = None

    try:

        # ----------------------------------------------------
        # CONNECT
        # ----------------------------------------------------

        engine = create_database_engine()

        # ----------------------------------------------------
        # VALIDATE TABLES
        # ----------------------------------------------------

        validate_tables(
            engine
        )

        # ----------------------------------------------------
        # QUERY 1
        # ----------------------------------------------------

        query_top_grids(
            engine
        )

        # ----------------------------------------------------
        # QUERY 2
        # ----------------------------------------------------

        query_hourly_trend(
            engine
        )

        # ----------------------------------------------------
        # QUERY 3
        # ----------------------------------------------------

        query_internet_heavy_windows(
            engine
        )

        logger.info(
            "============================================================"
        )

        logger.info(
            "ALL DE6 ANALYTICS QUERIES COMPLETED"
        )

        logger.info(
            "============================================================"
        )

    except Exception as error:

        logger.error(
            "DE6 queries failed: %s",
            error
        )

        raise

    finally:

        if engine is not None:

            engine.dispose()

            logger.info(
                "MySQL connection pool closed."
            )


if __name__ == "__main__":

    main()
