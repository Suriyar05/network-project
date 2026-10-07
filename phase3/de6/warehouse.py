from pathlib import Path
import logging
import json

import pandas as pd

from sqlalchemy import (
    create_engine,
    text,
    inspect
)
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# PROJECT PATHS
# ============================================================

DE6_ROOT = Path(__file__).resolve().parent

PHASE3_ROOT = DE6_ROOT.parent

PROJECT_ROOT = PHASE3_ROOT.parent


# ============================================================
# PHASE 2 SPARK OUTPUT
# ============================================================

SPARK_OUTPUT = (
    PROJECT_ROOT
    / "phase 2"
    / "output"
    / "sp3"
    / "hourly_grid_summary"
)


# ============================================================
# STATIC MILAN REFERENCE
# ============================================================

GRID_REFERENCE = (
    PROJECT_ROOT
    / "phase 2"
    / "data"
    / "reference"
    / "milano-grid.geojson"
)


# ============================================================
# MYSQL CONFIGURATION
# ============================================================

MYSQL_HOST = "localhost"

MYSQL_PORT = 3306

MYSQL_USER = "root"

MYSQL_PASSWORD = "root"

MYSQL_DATABASE = "network_analytics"


# ============================================================
# LOGGING
# ============================================================

LOG_DIR = (
    DE6_ROOT
    / "logs"
)

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)


LOG_FILE = (
    LOG_DIR
    / "de6.log"
)


logger = logging.getLogger("DE6")

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
# MYSQL CONNECTION URL
# ============================================================

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
# REQUIRED SOURCE COLUMNS
# ============================================================

SOURCE_COLUMNS = [

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


# ============================================================
# WAREHOUSE CLASS
# ============================================================

class NetworkWarehouse:

    """
    DE6 - Warehouse Modelling for Network Analytics.

    Star schema:

            dim_grid
                |
                |
        fact_network_activity
                |
                |
            dim_time

    MySQL is used as the analytics warehouse.

    fact_network_activity contains:

        - time_key
        - grid_key
        - activity measures

    Geometry is NOT stored in the fact table.

    Full geometry is represented through
    geometry_reference in dim_grid.
    """

    def __init__(self):

        self.engine = None

        self.source_df = None

        self.dim_grid_df = None

        self.dim_time_df = None

        self.fact_df = None


    # ========================================================
    # 1. CONNECT TO MYSQL
    # ========================================================

    def connect(self):

        logger.info(
            "Connecting to MySQL warehouse..."
        )

        try:

            self.engine = create_engine(
                DATABASE_URL,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            with self.engine.connect() as connection:

                connection.execute(
                    text("SELECT 1")
                )

            logger.info(
                "MySQL connection successful."
            )

            logger.info(
                "Database: %s",
                MYSQL_DATABASE
            )

            return self.engine

        except SQLAlchemyError as error:

            logger.error(
                "MySQL connection failed: %s",
                error
            )

            raise


    # ========================================================
    # 2. LOAD SP3 SOURCE
    # ========================================================

    def load_source_data(self):

        if not SPARK_OUTPUT.exists():

            raise FileNotFoundError(
                "SP3 hourly_grid_summary was not found:\n"
                f"{SPARK_OUTPUT}"
            )

        logger.info(
            "Loading SP3 hourly_grid_summary..."
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
                "No CSV files found in:\n"
                f"{SPARK_OUTPUT}"
            )

        frames = []

        for file in csv_files:

            logger.info(
                "Reading: %s",
                file.name
            )

            frame = pd.read_csv(
                file
            )

            frames.append(
                frame
            )

        self.source_df = pd.concat(
            frames,
            ignore_index=True
        )

        logger.info(
            "Source rows loaded: %d",
            len(self.source_df)
        )

        return self.source_df


    # ========================================================
    # 3. VALIDATE SOURCE
    # ========================================================

    def validate_source(self):

        if self.source_df is None:

            raise RuntimeError(
                "Load source before validation."
            )

        missing_columns = [

            column

            for column in SOURCE_COLUMNS

            if column not in self.source_df.columns

        ]

        if missing_columns:

            raise ValueError(
                "Missing source columns: "
                f"{missing_columns}"
            )

        logger.info(
            "Source schema validation: PASS"
        )


        # ----------------------------------------------------
        # TIMESTAMP
        # ----------------------------------------------------

        self.source_df["timestamp"] = pd.to_datetime(

            self.source_df["timestamp"],

            errors="coerce"

        )

        if self.source_df["timestamp"].isna().any():

            raise ValueError(
                "Invalid timestamp values found."
            )


        # ----------------------------------------------------
        # NUMERIC COLUMNS
        # ----------------------------------------------------

        numeric_columns = [

            "grid_id",

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


        for column in numeric_columns:

            self.source_df[column] = pd.to_numeric(

                self.source_df[column],

                errors="coerce"

            )


        # ----------------------------------------------------
        # NULL CHECK
        # ----------------------------------------------------

        for column in numeric_columns:

            if self.source_df[column].isna().any():

                raise ValueError(
                    f"Null values found in {column}."
                )


        # ----------------------------------------------------
        # DUPLICATE CHECK
        # ----------------------------------------------------

        duplicate_count = (

            self.source_df

            .duplicated(

                subset=[

                    "grid_id",

                    "timestamp"

                ]

            )

            .sum()

        )


        if duplicate_count > 0:

            raise ValueError(

                "Duplicate grid/hour records found: "

                f"{duplicate_count}"

            )


        logger.info(
            "Source duplicate validation: PASS"
        )


        # ----------------------------------------------------
        # GEOMETRY CHECK
        # ----------------------------------------------------

        if "geometry" in self.source_df.columns:

            raise ValueError(
                "Geometry must not exist in SP3 fact data."
            )


        logger.info(
            "Source validation completed successfully."
        )


    # ========================================================
    # 4. CLEAR EXISTING WAREHOUSE
    # ========================================================

    def clear_existing_tables(self):

        logger.info(
            "Clearing previous MySQL warehouse..."
        )

        statements = [

            """
            DROP TABLE IF EXISTS fact_network_activity
            """,

            """
            DROP TABLE IF EXISTS dim_grid
            """,

            """
            DROP TABLE IF EXISTS dim_time
            """

        ]


        with self.engine.begin() as connection:

            for statement in statements:

                connection.execute(
                    text(statement)
                )


        logger.info(
            "Previous warehouse tables cleared."
        )


    # ========================================================
    # 5. CREATE TABLES
    # ========================================================

    def create_tables(self):

        logger.info(
            "Creating MySQL warehouse tables..."
        )


        with self.engine.begin() as connection:

            # ------------------------------------------------
            # DIM TIME
            # ------------------------------------------------

            connection.execute(
                text(
                    """
                    CREATE TABLE dim_time (

                        time_key INT PRIMARY KEY,

                        timestamp DATETIME NOT NULL UNIQUE,

                        date DATE NOT NULL,

                        hour INT NOT NULL,

                        day_of_week INT NOT NULL

                    )
                    ENGINE=InnoDB
                    """
                )
            )


            # ------------------------------------------------
            # DIM GRID
            # ------------------------------------------------

            connection.execute(
                text(
                    """
                    CREATE TABLE dim_grid (

                        grid_key INT AUTO_INCREMENT PRIMARY KEY,

                        grid_id INT NOT NULL UNIQUE,

                        centroid_latitude DOUBLE NULL,

                        centroid_longitude DOUBLE NULL,

                        geometry_reference VARCHAR(500) NULL

                    )
                    ENGINE=InnoDB
                    """
                )
            )


            # ------------------------------------------------
            # FACT
            # ------------------------------------------------

            connection.execute(
                text(
                    """
                    CREATE TABLE fact_network_activity (

                        time_key INT NOT NULL,

                        grid_key INT NOT NULL,

                        sms_in DOUBLE NOT NULL,

                        sms_out DOUBLE NOT NULL,

                        call_in DOUBLE NOT NULL,

                        call_out DOUBLE NOT NULL,

                        total_sms DOUBLE NOT NULL,

                        total_calls DOUBLE NOT NULL,

                        internet_activity DOUBLE NOT NULL,

                        total_activity DOUBLE NOT NULL,

                        internet_share DOUBLE NOT NULL,

                        PRIMARY KEY (
                            time_key,
                            grid_key
                        ),

                        CONSTRAINT fk_fact_time

                            FOREIGN KEY (
                                time_key
                            )

                            REFERENCES dim_time(
                                time_key
                            ),

                        CONSTRAINT fk_fact_grid

                            FOREIGN KEY (
                                grid_key
                            )

                            REFERENCES dim_grid(
                                grid_key
                            )

                    )
                    ENGINE=InnoDB
                    """
                )
            )


        logger.info(
            "MySQL warehouse tables created successfully."
        )


    # ========================================================
    # 6. CREATE DIM TIME
    # ========================================================

    def create_dim_time_dataframe(self):

        logger.info(
            "Creating dim_time DataFrame..."
        )


        timestamps = (

            pd.to_datetime(

                self.source_df["timestamp"],

                errors="coerce"

            )

            .dropna()

            .drop_duplicates()

            .sort_values()

            .reset_index(drop=True)

        )


        if timestamps.empty:

            raise ValueError(
                "No timestamps available for dim_time."
            )


        self.dim_time_df = pd.DataFrame({

            "time_key": range(
                1,
                len(timestamps) + 1
            ),

            "timestamp": timestamps,

            "date": timestamps.dt.date,

            "hour": timestamps.dt.hour,

            "day_of_week": timestamps.dt.dayofweek + 1

        })


        self.dim_time_df["timestamp"] = pd.to_datetime(

            self.dim_time_df["timestamp"]

        )


        logger.info(
            "dim_time DataFrame created: %d rows",
            len(self.dim_time_df)
        )


        return self.dim_time_df


    # ========================================================
    # 7. LOAD DIM TIME
    # ========================================================

    def load_dim_time(self):

        logger.info(
            "Loading dim_time..."
        )


        if self.dim_time_df is None:

            self.create_dim_time_dataframe()


        load_df = self.dim_time_df.copy()


        load_df["timestamp"] = (

            pd.to_datetime(
                load_df["timestamp"]
            )

        )


        load_df.to_sql(

            "dim_time",

            self.engine,

            if_exists="append",

            index=False,

            chunksize=5000

        )


        logger.info(
            "dim_time loaded successfully: %d rows",
            len(load_df)
        )


    # ========================================================
    # 8. LOAD MILAN GRID REFERENCE
    # ========================================================

    def load_milan_grid_reference(self):

        if not GRID_REFERENCE.exists():

            raise FileNotFoundError(
                "Milan GeoJSON not found:\n"
                f"{GRID_REFERENCE}"
            )


        logger.info(
            "Loading static Milan grid reference..."
        )


        with open(

            GRID_REFERENCE,

            "r",

            encoding="utf-8"

        ) as file:

            geojson = json.load(file)


        features = geojson.get(
            "features",
            []
        )


        if not features:

            raise ValueError(
                "Milan GeoJSON contains no features."
            )


        records = []


        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            geometry = feature.get(
                "geometry",
                {}
            )


            if "cellId" not in properties:

                continue


            grid_id = int(
                properties["cellId"]
            )


            centroid_latitude = None

            centroid_longitude = None


            coordinates = geometry.get(
                "coordinates"
            )


            points = []


            geometry_type = geometry.get(
                "type"
            )


            # ------------------------------------------------
            # POLYGON
            # ------------------------------------------------

            if geometry_type == "Polygon":

                for ring in coordinates or []:

                    for point in ring:

                        if len(point) >= 2:

                            points.append(

                                (

                                    float(point[0]),

                                    float(point[1])

                                )

                            )


            # ------------------------------------------------
            # MULTIPOLYGON
            # ------------------------------------------------

            elif geometry_type == "MultiPolygon":

                for polygon in coordinates or []:

                    for ring in polygon:

                        for point in ring:

                            if len(point) >= 2:

                                points.append(

                                    (

                                        float(point[0]),

                                        float(point[1])

                                    )

                                )


            if points:

                centroid_longitude = (

                    sum(
                        point[0]
                        for point in points
                    )

                    / len(points)

                )


                centroid_latitude = (

                    sum(
                        point[1]
                        for point in points
                    )

                    / len(points)

                )


            geometry_reference = (

                "milano-grid.geojson"
                f"#cellId={grid_id}"

            )


            records.append({

                "grid_id": grid_id,

                "centroid_latitude":
                    centroid_latitude,

                "centroid_longitude":
                    centroid_longitude,

                "geometry_reference":
                    geometry_reference

            })


        reference_df = pd.DataFrame(
            records
        )


        if reference_df.empty:

            raise ValueError(
                "No valid cellId records found."
            )


        reference_df = (

            reference_df

            .drop_duplicates(
                subset=["grid_id"]
            )

            .reset_index(drop=True)

        )


        logger.info(
            "Milan reference grids found: %d",
            len(reference_df)
        )


        return reference_df


    # ========================================================
    # 9. LOAD DIM GRID
    # ========================================================

    def load_dim_grid(self):

        logger.info(
            "Loading dim_grid..."
        )


        reference_df = (

            self.load_milan_grid_reference()

        )


        observed_grid_ids = set(

            self.source_df["grid_id"]

            .astype(int)

            .unique()

        )


        reference_df = (

            reference_df[

                reference_df["grid_id"]

                .isin(
                    observed_grid_ids
                )

            ]

            .copy()

        )


        if reference_df.empty:

            raise ValueError(
                "No observed grid IDs matched Milan reference."
            )


        self.dim_grid_df = reference_df[

            [

                "grid_id",

                "centroid_latitude",

                "centroid_longitude",

                "geometry_reference"

            ]

        ]


        self.dim_grid_df.to_sql(

            "dim_grid",

            self.engine,

            if_exists="append",

            index=False,

            chunksize=5000

        )


        logger.info(
            "dim_grid rows inserted: %d",
            len(self.dim_grid_df)
        )


        return self.dim_grid_df


    # ========================================================
    # 10. LOAD FACT NETWORK ACTIVITY
    # ========================================================

    def load_fact_network_activity(self):

        logger.info(
            "Loading fact_network_activity..."
        )

        if self.source_df is None:
            raise RuntimeError(
                "Source DataFrame has not been loaded."
            )

        if self.dim_time_df is None:
            raise RuntimeError(
                "dim_time DataFrame has not been created."
            )

        if self.dim_grid_df is None:
            raise RuntimeError(
                "dim_grid DataFrame has not been created."
            )

        # ====================================================
        # PREPARE SOURCE
        # ====================================================

        source = self.source_df.copy()

        # ====================================================
        # NORMALIZE TIMESTAMP
        # ====================================================

        source["timestamp"] = pd.to_datetime(
            source["timestamp"],
            errors="coerce"
        )

        if source["timestamp"].isna().any():

            raise ValueError(
                "Source contains invalid timestamps."
            )

        # ----------------------------------------------------
        # Remove timezone WITHOUT changing local clock time
        #
        # Example:
        #
        # 2013-11-01 00:00:00+05:30
        #
        # becomes:
        #
        # 2013-11-01 00:00:00
        # ----------------------------------------------------

        try:

            if source["timestamp"].dt.tz is not None:

                source["timestamp"] = (
                    source["timestamp"]
                    .dt.tz_localize(None)
                )

        except AttributeError:

            pass

        source["timestamp"] = (
            source["timestamp"]
            .dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        # ====================================================
        # PREPARE DIM TIME
        # ====================================================

        dim_time = self.dim_time_df.copy()

        dim_time["timestamp"] = pd.to_datetime(
            dim_time["timestamp"],
            errors="coerce"
        )

        if dim_time["timestamp"].isna().any():

            raise ValueError(
                "dim_time contains invalid timestamps."
            )

        try:

            if dim_time["timestamp"].dt.tz is not None:

                dim_time["timestamp"] = (
                    dim_time["timestamp"]
                    .dt.tz_localize(None)
                )

        except AttributeError:

            pass

        dim_time["timestamp"] = (
            dim_time["timestamp"]
            .dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        # ====================================================
        # VALIDATE DIM TIME
        # ====================================================

        duplicate_time_count = (
            dim_time
            .duplicated(
                subset=["timestamp"]
            )
            .sum()
        )

        if duplicate_time_count > 0:

            raise ValueError(
                "dim_time contains duplicate timestamps: "
                f"{duplicate_time_count}"
            )

        logger.info(
            "dim_time validation: PASS"
        )

        # ====================================================
        # TIMESTAMP OVERLAP
        # ====================================================

        source_timestamps = set(
            source["timestamp"]
            .dropna()
            .unique()
        )

        dim_timestamps = set(
            dim_time["timestamp"]
            .dropna()
            .unique()
        )

        missing_timestamps = (
            source_timestamps
            - dim_timestamps
        )

        if missing_timestamps:

            logger.error(
                "Source timestamps: %d",
                len(source_timestamps)
            )

            logger.error(
                "dim_time timestamps: %d",
                len(dim_timestamps)
            )

            logger.error(
                "Example missing timestamp: %s",
                sorted(missing_timestamps)[0]
            )

            raise ValueError(
                "Source timestamps do not completely "
                "match dim_time."
            )

        logger.info(
            "Timestamp overlap validation: PASS | "
            "%d timestamps matched",
            len(source_timestamps)
        )

        # ====================================================
        # JOIN SOURCE → DIM TIME
        # ====================================================

        logger.info(
            "Joining fact records to dim_time..."
        )

        source = source.merge(

            dim_time[
                [
                    "time_key",
                    "timestamp"
                ]
            ],

            on="timestamp",

            how="left",

            validate="many_to_one"
        )

        missing_time_keys = (
            source["time_key"]
            .isna()
            .sum()
        )

        if missing_time_keys > 0:

            raise ValueError(
                "Some fact records could not match "
                "dim_time. Missing time keys: "
                f"{missing_time_keys}"
            )

        logger.info(
            "dim_time join: PASS"
        )

        # ====================================================
        # LOAD GENERATED GRID KEYS FROM MYSQL
        # ====================================================
        #
        # grid_key is AUTO_INCREMENT in MySQL.
        #
        # It does NOT exist inside self.dim_grid_df.
        #
        # Therefore read the generated keys back from
        # the database.
        #
        # ====================================================
        # ========================================================
# READ GENERATED GRID KEYS FROM MYSQL
        # ========================================================

        logger.info(
            "Reading generated grid_key values from MySQL..."
        )

        dim_grid = pd.read_sql(
            text(
                """
                SELECT
                    grid_key,
                    grid_id
                FROM dim_grid
                """
            ),
            self.engine
        )

        # ====================================================
        # VALIDATE GRID DIMENSION
        # ====================================================

        duplicate_grid_count = (
            dim_grid
            .duplicated(
                subset=["grid_id"]
            )
            .sum()
        )

        if duplicate_grid_count > 0:

            raise ValueError(
                "dim_grid contains duplicate grid IDs: "
                f"{duplicate_grid_count}"
            )

        logger.info(
            "dim_grid database validation: PASS | "
            "%d grids",
            len(dim_grid)
        )

        # ====================================================
        # JOIN SOURCE → DIM GRID
        # ====================================================

        logger.info(
            "Joining fact records to dim_grid..."
        )

        source["grid_id"] = pd.to_numeric(
            source["grid_id"],
            errors="coerce"
        )

        dim_grid["grid_id"] = pd.to_numeric(
            dim_grid["grid_id"],
            errors="coerce"
        )

        source = source.merge(

            dim_grid[
                [
                    "grid_key",
                    "grid_id"
                ]
            ],

            on="grid_id",

            how="left",

            validate="many_to_one"
        )

        # ====================================================
        # VALIDATE GRID JOIN
        # ====================================================

        missing_grid_keys = (
            source["grid_key"]
            .isna()
            .sum()
        )

        if missing_grid_keys > 0:

            raise ValueError(
                "Some fact records could not match "
                "dim_grid. Missing grid keys: "
                f"{missing_grid_keys}"
            )

        logger.info(
            "dim_grid join: PASS"
        )

        # ====================================================
        # CREATE FACT DATAFRAME
        # ====================================================

        fact_columns = [

            "time_key",
            "grid_key",

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

        self.fact_df = (
            source[
                fact_columns
            ]
            .copy()
        )

        # ====================================================
        # FOREIGN KEYS
        # ====================================================

        self.fact_df["time_key"] = (
            self.fact_df["time_key"]
            .astype("int64")
        )

        self.fact_df["grid_key"] = (
            self.fact_df["grid_key"]
            .astype("int64")
        )

        # ====================================================
        # NUMERIC MEASURES
        # ====================================================

        numeric_columns = [

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

        for column in numeric_columns:

            self.fact_df[column] = pd.to_numeric(
                self.fact_df[column],
                errors="coerce"
            )

        # ====================================================
        # NULL VALIDATION
        # ====================================================

        for column in fact_columns:

            if self.fact_df[column].isna().any():

                raise ValueError(
                    "Fact table contains null values "
                    f"in column: {column}"
                )

        logger.info(
            "Fact null validation: PASS"
        )

        # ====================================================
        # ROW COUNT VALIDATION
        # ====================================================

        source_row_count = len(
            self.source_df
        )

        fact_row_count = len(
            self.fact_df
        )

        logger.info(
            "SP3 source rows: %d",
            source_row_count
        )

        logger.info(
            "Fact rows after joins: %d",
            fact_row_count
        )

        if fact_row_count != source_row_count:

            raise ValueError(
                "FACT ROW COUNT FAILED: "
                f"source={source_row_count}, "
                f"fact={fact_row_count}"
            )

        logger.info(
            "FACT ROW COUNT: PASS"
        )

        # ====================================================
        # FACT GRAIN
        # ====================================================

        duplicate_fact_count = (
            self.fact_df
            .duplicated(
                subset=[
                    "time_key",
                    "grid_key"
                ]
            )
            .sum()
        )

        if duplicate_fact_count > 0:

            raise ValueError(
                "Fact table contains duplicate "
                "time_key + grid_key records: "
                f"{duplicate_fact_count}"
            )

        logger.info(
            "FACT GRAIN VALIDATION: PASS"
        )

        # ====================================================
        # GEOMETRY EXCLUSION
        # ====================================================

        if "geometry" in self.fact_df.columns:

            raise ValueError(
                "Geometry must not exist "
                "in fact_network_activity."
            )

        if "geometry_reference" in self.fact_df.columns:

            raise ValueError(
                "geometry_reference must not exist "
                "in fact_network_activity."
            )

        logger.info(
            "FACT GEOMETRY EXCLUSION: PASS"
        )

        # ====================================================
        # INSERT FACT DATA
        # ====================================================

        logger.info(
            "Writing fact_network_activity to MySQL..."
        )

        self.fact_df.to_sql(
            "fact_network_activity",
            self.engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )

        logger.info(
            "fact_network_activity loaded successfully: %d rows",
            len(self.fact_df)
        )

        return self.fact_df

    # ========================================================
    # 11. CREATE INDEXES
    # ========================================================

    def create_indexes(self):

        logger.info(
            "Creating MySQL indexes..."
        )


        statements = [

            """
            CREATE INDEX idx_fact_grid
            ON fact_network_activity(grid_key)
            """,

            """
            CREATE INDEX idx_fact_time
            ON fact_network_activity(time_key)
            """,

            """
            CREATE INDEX idx_fact_total_activity
            ON fact_network_activity(total_activity)
            """,

            """
            CREATE INDEX idx_dim_grid_grid_id
            ON dim_grid(grid_id)
            """,

            """
            CREATE INDEX idx_dim_time_date
            ON dim_time(date)
            """,

            """
            CREATE INDEX idx_dim_time_hour
            ON dim_time(hour)
            """

        ]


        with self.engine.begin() as connection:

            for statement in statements:

                try:

                    connection.execute(
                        text(statement)
                    )

                except SQLAlchemyError:

                    logger.warning(
                        "Index may already exist."
                    )


        logger.info(
            "MySQL indexes created."
        )


    # ========================================================
    # 12. VALIDATE WAREHOUSE
    # ========================================================

    def validate_warehouse(self):

        logger.info(
            "Running DE6 warehouse validation..."
        )


        # ----------------------------------------------------
        # SOURCE COUNT
        # ----------------------------------------------------

        source_count = len(
            self.source_df
        )


        # ----------------------------------------------------
        # FACT COUNT
        # ----------------------------------------------------

        fact_count = pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_network_activity
                """
            ),

            self.engine

        ).iloc[0]["count"]


        logger.info(
            "Source rows: %d",
            source_count
        )


        logger.info(
            "Warehouse fact rows: %d",
            fact_count
        )


        assert int(fact_count) == int(source_count), (

            "FACT ROW COUNT FAILED"

        )


        logger.info(
            "FACT ROW COUNT: PASS"
        )


        # ----------------------------------------------------
        # DIM GRID COUNT
        # ----------------------------------------------------

        source_grid_count = (

            self.source_df["grid_id"]
            .nunique()

        )


        dim_grid_count = pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM dim_grid
                """
            ),

            self.engine

        ).iloc[0]["count"]


        logger.info(
            "Source distinct grids: %d",
            source_grid_count
        )


        logger.info(
            "Warehouse dim_grid rows: %d",
            dim_grid_count
        )


        assert int(dim_grid_count) == int(
            source_grid_count
        )


        logger.info(
            "DIM GRID COUNT: PASS"
        )


        # ----------------------------------------------------
        # DIM GRID DUPLICATES
        # ----------------------------------------------------

        duplicate_grids = pd.read_sql(

            text(
                """
                SELECT
                    grid_id,
                    COUNT(*) AS count
                FROM dim_grid
                GROUP BY grid_id
                HAVING COUNT(*) > 1
                """
            ),

            self.engine

        )


        assert duplicate_grids.empty


        logger.info(
            "DIM GRID DUPLICATE CHECK: PASS"
        )


        # ----------------------------------------------------
        # FACT COLUMNS
        # ----------------------------------------------------

        inspector = inspect(
            self.engine
        )


        fact_columns = [

            column["name"]

            for column in inspector.get_columns(
                "fact_network_activity"
            )

        ]


        assert "geometry" not in fact_columns

        assert "geometry_reference" not in fact_columns


        logger.info(
            "FACT GEOMETRY EXCLUSION: PASS"
        )


        # ----------------------------------------------------
        # FOREIGN KEY CHECK
        # ----------------------------------------------------

        orphan_time = pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_network_activity f
                LEFT JOIN dim_time t
                    ON f.time_key = t.time_key
                WHERE t.time_key IS NULL
                """
            ),

            self.engine

        ).iloc[0]["count"]


        orphan_grid = pd.read_sql(

            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_network_activity f
                LEFT JOIN dim_grid g
                    ON f.grid_key = g.grid_key
                WHERE g.grid_key IS NULL
                """
            ),

            self.engine

        ).iloc[0]["count"]


        assert int(orphan_time) == 0

        assert int(orphan_grid) == 0


        logger.info(
            "FOREIGN KEY VALIDATION: PASS"
        )


        # ----------------------------------------------------
        # FACT GRAIN
        # ----------------------------------------------------

        duplicate_fact_count = pd.read_sql(

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
                ) x
                """
            ),

            self.engine

        ).iloc[0]["count"]


        assert int(duplicate_fact_count) == 0


        logger.info(
            "FACT GRAIN VALIDATION: PASS"
        )


        # ----------------------------------------------------
        # TOTAL ACTIVITY RECONCILIATION
        # ----------------------------------------------------

        source_total = float(

            self.source_df[
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

                self.engine

            ).iloc[0]["total_activity"]

        )


        difference = abs(

            source_total
            -
            warehouse_total

        )


        tolerance = max(

            abs(source_total) * 1e-9,

            0.000001

        )


        logger.info(
            "Source total_activity: %.10f",
            source_total
        )


        logger.info(
            "Warehouse total_activity: %.10f",
            warehouse_total
        )


        logger.info(
            "Difference: %.10f",
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


        # ----------------------------------------------------
        # TOP GRID
        # ----------------------------------------------------

        source_top_grid = (

            self.source_df

            .groupby("grid_id")[
                "total_activity"
            ]

            .sum()

            .idxmax()

        )


        warehouse_top_grid = pd.read_sql(

            text(
                """
                SELECT
                    g.grid_id,
                    SUM(
                        f.total_activity
                    ) AS total_activity
                FROM fact_network_activity f
                JOIN dim_grid g
                    ON f.grid_key = g.grid_key
                GROUP BY g.grid_id
                ORDER BY total_activity DESC
                LIMIT 1
                """
            ),

            self.engine

        ).iloc[0]["grid_id"]


        logger.info(
            "Expected top grid: %s",
            source_top_grid
        )


        logger.info(
            "Warehouse top grid: %s",
            warehouse_top_grid
        )


        assert int(source_top_grid) == int(
            warehouse_top_grid
        )


        logger.info(
            "TOP GRID RECONCILIATION: PASS"
        )


        # ----------------------------------------------------
        # INDEX VALIDATION
        # ----------------------------------------------------

        indexes = inspector.get_indexes(
            "fact_network_activity"
        )


        index_names = {

            index["name"]

            for index in indexes

        }


        assert (
            "idx_fact_grid"
            in index_names
        )


        assert (
            "idx_fact_time"
            in index_names
        )


        logger.info(
            "INDEX VALIDATION: PASS"
        )


        logger.info(
            "============================================================"
        )

        logger.info(
            "ALL DE6 VALIDATIONS PASSED"
        )

        logger.info(
            "============================================================"
        )


    # ========================================================
    # 13. CLOSE
    # ========================================================

    def close(self):

        if self.engine is not None:

            self.engine.dispose()

            logger.info(
                "MySQL warehouse connection pool closed."
            )


    # ========================================================
    # 14. RUN
    # ========================================================

    def run(self):

        try:

            logger.info(
                "============================================================"
            )

            logger.info(
                "DE6 MYSQL WAREHOUSE MODELLING STARTED"
            )

            logger.info(
                "============================================================"
            )


            logger.info(
                "SP3 source: %s",
                SPARK_OUTPUT
            )


            logger.info(
                "Grid reference: %s",
                GRID_REFERENCE
            )


            logger.info(
                "MySQL host: %s",
                MYSQL_HOST
            )


            logger.info(
                "MySQL database: %s",
                MYSQL_DATABASE
            )


            # ------------------------------------------------
            # CONNECT
            # ------------------------------------------------

            self.connect()


            # ------------------------------------------------
            # LOAD SOURCE
            # ------------------------------------------------

            self.load_source_data()

            self.validate_source()


            # ------------------------------------------------
            # REBUILD WAREHOUSE
            # ------------------------------------------------

            self.clear_existing_tables()

            self.create_tables()


            # ------------------------------------------------
            # DIMENSIONS
            # ------------------------------------------------

            self.create_dim_time_dataframe()

            self.load_dim_time()

            self.load_dim_grid()


            # ------------------------------------------------
            # FACT
            # ------------------------------------------------

            self.load_fact_network_activity()


            # ------------------------------------------------
            # INDEXES
            # ------------------------------------------------

            self.create_indexes()


            # ------------------------------------------------
            # VALIDATION
            # ------------------------------------------------

            self.validate_warehouse()


            logger.info(
                "============================================================"
            )

            logger.info(
                "DE6 MYSQL WAREHOUSE BUILD COMPLETED SUCCESSFULLY"
            )

            logger.info(
                "============================================================"
            )


        except Exception as error:

            logger.error(
                "DE6 MYSQL WAREHOUSE BUILD FAILED: %s",
                error
            )

            raise


        finally:

            self.close()


# ============================================================
# DIRECT EXECUTION
# ============================================================

if __name__ == "__main__":

    warehouse = NetworkWarehouse()

    warehouse.run()