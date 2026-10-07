import os
import json
import logging
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


# ============================================================
# PROJECT PATHS
# ============================================================

# File:
# phase 2/sp4/spark_geospatial.py
#
# parents[0] = sp4
# parents[1] = phase 2
#
PHASE2_ROOT = Path(__file__).resolve().parents[1]

PROJECT_ROOT = PHASE2_ROOT.parent

DATA_DIR = PHASE2_ROOT / "data"
OUTPUT_DIR = PHASE2_ROOT / "output"
LOG_DIR = PHASE2_ROOT / "logs"

RAW_DATA_DIR = DATA_DIR / "raw"

GEOJSON_FILE = DATA_DIR / "milano-grid.geojson"


# ============================================================
# SP3 OUTPUT
# ============================================================

SP3_OUTPUT_DIR = (
    OUTPUT_DIR
    / "sp3"
    / "hourly_grid_summary"
)


# ============================================================
# SP4 OUTPUT
# ============================================================

SP4_ENRICHED_OUTPUT = (
    OUTPUT_DIR
    / "sp4_grid_activity_geo"
)

SP4_UNMATCHED_OUTPUT = (
    OUTPUT_DIR
    / "sp4_unmatched_grid_ids"
)

SP4_TOP_GRIDS_OUTPUT = (
    OUTPUT_DIR
    / "sp4_top_activity_grids"
)

SP4_REPORT = (
    OUTPUT_DIR
    / "sp4_grid_enrichment_report.txt"
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

LOG_FILE = LOG_DIR / "sp4.log"

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
# SP4 CLASS
# ============================================================

class SparkGeospatialEnrichment:

    """
    SP4 - Geospatial Enrichment Using Milan Grid.

    Input:
        SP3 hourly_grid_summary

    Activity grain:
        timestamp + grid_id

    GeoJSON:
        data/milano-grid.geojson

    IMPORTANT:
        GeoJSON grid identifier is:

            properties.cellId

        NOT:

            feature.id

    Output:
        timestamp
        grid_id
        sms_in
        sms_out
        call_in
        call_out
        internet_activity
        total_activity
        geometry
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(
        self,
        spark,
        activity_df=None
    ):

        self.spark = spark

        self.activity_df = activity_df

        self.grid_lookup_df = None

        self.grid_activity_geo_df = None

        self.unmatched_grid_ids = None

        self.top_activity_grids = None

        self.row_count_before = 0

        self.row_count_after = 0

        self.distinct_grids_before = 0

        self.distinct_grids_after = 0

        self.enrichment_percentage = 0.0

        self.geojson_feature_count = 0

    # ========================================================
    # 1. LOAD SP3 DATA
    # ========================================================

    def load_activity_data(self):

        if self.activity_df is not None:

            logger.info(
                "Using activity DataFrame supplied directly."
            )

            return self.activity_df

        if not SP3_OUTPUT_DIR.exists():

            raise FileNotFoundError(
                "\nSP3 hourly_grid_summary was not found.\n\n"
                f"Expected directory:\n"
                f"{SP3_OUTPUT_DIR}\n\n"
                "Run SP3 first and make sure it creates:\n"
                "output/sp3/hourly_grid_summary/"
            )

        logger.info(
            "Loading SP3 hourly_grid_summary from:"
        )

        logger.info(
            "%s",
            SP3_OUTPUT_DIR
        )

        self.activity_df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(
                str(SP3_OUTPUT_DIR)
            )
        )

        logger.info(
            "SP3 hourly_grid_summary loaded successfully."
        )

        logger.info(
            "SP3 columns: %s",
            self.activity_df.columns
        )

        return self.activity_df

    # ========================================================
    # 2. VALIDATE SP3 DATA
    # ========================================================

    def validate_activity_data(self):

        required_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_activity"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in self.activity_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "SP3 DataFrame is missing required columns: "
                f"{missing_columns}"
            )

        logger.info(
            "Required SP3 columns found."
        )

        # ----------------------------------------------------
        # Standardize timestamp
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
        # Standardize grid_id
        # ----------------------------------------------------

        self.activity_df = (
            self.activity_df
            .withColumn(
                "grid_id",
                F.col("grid_id").cast(
                    IntegerType()
                )
            )
        )

        # ----------------------------------------------------
        # Validate timestamp
        # ----------------------------------------------------

        null_timestamp_count = (
            self.activity_df
            .filter(
                F.col("timestamp").isNull()
            )
            .count()
        )

        if null_timestamp_count > 0:

            raise ValueError(
                f"Found {null_timestamp_count} rows "
                "with null timestamp."
            )

        # ----------------------------------------------------
        # Validate grid_id
        # ----------------------------------------------------

        null_grid_count = (
            self.activity_df
            .filter(
                F.col("grid_id").isNull()
            )
            .count()
        )

        if null_grid_count > 0:

            raise ValueError(
                f"Found {null_grid_count} rows "
                "with null grid_id."
            )

        # ----------------------------------------------------
        # Validate grid range
        # ----------------------------------------------------

        invalid_grid_count = (
            self.activity_df
            .filter(
                (F.col("grid_id") < 1)
                |
                (F.col("grid_id") > 10000)
            )
            .count()
        )

        if invalid_grid_count > 0:

            raise ValueError(
                "SP3 contains grid_id values outside "
                "the valid 1-10000 range."
            )

        # ----------------------------------------------------
        # Validate SP3 grain
        # ----------------------------------------------------

        duplicate_groups = (
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

        if duplicate_groups > 0:

            raise AssertionError(
                "SP3 hourly_grid_summary contains "
                f"{duplicate_groups} duplicate "
                "grid_id + timestamp groups."
            )

        # ----------------------------------------------------
        # Counts
        # ----------------------------------------------------

        self.row_count_before = (
            self.activity_df.count()
        )

        self.distinct_grids_before = (
            self.activity_df
            .select("grid_id")
            .distinct()
            .count()
        )

        logger.info(
            "Activity rows before join: %d",
            self.row_count_before
        )

        logger.info(
            "Distinct activity grids before join: %d",
            self.distinct_grids_before
        )

        logger.info(
            "VALIDATION activity input: PASS"
        )

    # ========================================================
    # 3. INSPECT GEOJSON
    # ========================================================

    def inspect_geojson(self):

        if not GEOJSON_FILE.exists():

            raise FileNotFoundError(
                "\nMilan GeoJSON was not found.\n\n"
                f"Expected:\n{GEOJSON_FILE}"
            )

        logger.info(
            "Reading Milan GeoJSON:"
        )

        logger.info(
            "%s",
            GEOJSON_FILE
        )

        with open(
            GEOJSON_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        # ----------------------------------------------------
        # Top-level type
        # ----------------------------------------------------

        top_level_type = geojson.get(
            "type"
        )

        logger.info(
            "GeoJSON top-level type: %s",
            top_level_type
        )

        if top_level_type != "FeatureCollection":

            raise ValueError(
                "GeoJSON must be a FeatureCollection."
            )

        features = geojson.get(
            "features",
            []
        )

        if not features:

            raise ValueError(
                "GeoJSON contains no features."
            )

        self.geojson_feature_count = len(
            features
        )

        logger.info(
            "GeoJSON feature count: %d",
            self.geojson_feature_count
        )

        # ----------------------------------------------------
        # Inspect first feature
        # ----------------------------------------------------

        first_feature = features[0]

        logger.info(
            "Example top-level feature id: %s",
            first_feature.get("id")
        )

        properties = first_feature.get(
            "properties",
            {}
        )

        geometry = first_feature.get(
            "geometry",
            {}
        )

        logger.info(
            "Feature properties: %s",
            list(properties.keys())
        )

        logger.info(
            "Example properties.cellId: %s",
            properties.get("cellId")
        )

        logger.info(
            "Example geometry type: %s",
            geometry.get("type")
        )

        logger.info(
            "IMPORTANT: grid_id uses properties.cellId."
        )

        logger.info(
            "IMPORTANT: feature.id is NOT used."
        )

        return geojson

    # ========================================================
    # 4. BUILD GRID LOOKUP
    # ========================================================

    def build_grid_lookup(self):

        geojson = self.inspect_geojson()

        features = geojson["features"]

        lookup_records = []

        python_grid_ids = []

        # ----------------------------------------------------
        # Flatten:
        #
        # features[]
        #       ↓
        # properties.cellId
        #       ↓
        # grid_id
        #       +
        # geometry
        # ----------------------------------------------------

        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            geometry = feature.get(
                "geometry"
            )

            if "cellId" not in properties:

                raise ValueError(
                    "GeoJSON feature is missing "
                    "properties.cellId."
                )

            if geometry is None:

                raise ValueError(
                    "GeoJSON feature is missing geometry."
                )

            # =================================================
            # ACCEPTANCE CRITERION:
            #
            # USE properties.cellId
            #
            # DO NOT USE feature.id
            # =================================================

            grid_id = int(
                properties["cellId"]
            )

            if grid_id < 1 or grid_id > 10000:

                raise ValueError(
                    f"Invalid GeoJSON grid_id: {grid_id}"
                )

            geometry_json = json.dumps(
                geometry,
                separators=(",", ":")
            )

            lookup_records.append(
                (
                    grid_id,
                    geometry_json
                )
            )

            python_grid_ids.append(
                grid_id
            )

        # ----------------------------------------------------
        # Validate feature count
        # ----------------------------------------------------

        if len(lookup_records) != 10000:

            raise AssertionError(
                "Expected 10000 Milan grid cells, "
                f"but found {len(lookup_records)}."
            )

        # ----------------------------------------------------
        # Validate duplicate grid IDs in Python.
        #
        # This avoids unnecessary Spark execution for
        # a small 10,000-row static lookup.
        # ----------------------------------------------------

        if len(python_grid_ids) != len(
            set(python_grid_ids)
        ):

            raise AssertionError(
                "GeoJSON contains duplicate properties.cellId "
                "values."
            )

        logger.info(
            "GeoJSON grid ID uniqueness: PASS"
        )

        # ----------------------------------------------------
        # Create Spark lookup
        # ----------------------------------------------------

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

        self.grid_lookup_df = (
            self.spark
            .createDataFrame(
                lookup_records,
                schema=schema
            )
        )

        # ----------------------------------------------------
        # Cache static lookup
        # ----------------------------------------------------

        self.grid_lookup_df = (
            self.grid_lookup_df
            .cache()
        )

        logger.info(
            "Grid lookup created successfully."
        )

        logger.info(
            "Grid lookup rows: %d",
            len(lookup_records)
        )

        logger.info(
            "Grid lookup created from "
            "properties.cellId."
        )

        return self.grid_lookup_df

    # ========================================================
    # 5. STANDARD JOIN PLAN
    # ========================================================

    def show_standard_join_plan(self):

        if self.grid_lookup_df is None:

            raise RuntimeError(
                "Grid lookup has not been created."
            )

        logger.info(
            "========== STANDARD JOIN PLAN =========="
        )

        standard_join = (
            self.activity_df
            .join(
                self.grid_lookup_df,
                on="grid_id",
                how="left"
            )
        )

        standard_join.explain(
            mode="formatted"
        )

        logger.info(
            "Standard join plan displayed."
        )

    # ========================================================
    # 6. BROADCAST JOIN
    # ========================================================

    def enrich_with_broadcast_join(self):

        if self.grid_lookup_df is None:

            raise RuntimeError(
                "Grid lookup has not been created."
            )

        logger.info(
            "========== BROADCAST JOIN =========="
        )

        # ----------------------------------------------------
        # Grid lookup = 10,000 rows.
        #
        # Activity = millions of rows.
        #
        # Therefore the grid lookup is a broadcast candidate.
        # ----------------------------------------------------

        broadcast_lookup = F.broadcast(
            self.grid_lookup_df
        )

        self.grid_activity_geo_df = (
            self.activity_df
            .join(
                broadcast_lookup,
                on="grid_id",
                how="left"
            )
        )

        logger.info(
            "Broadcast LEFT JOIN created."
        )

        return self.grid_activity_geo_df

    # ========================================================
    # 7. VALIDATE ENRICHMENT
    # ========================================================

    def validate_enrichment(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "Run the broadcast join first."
            )

        # ----------------------------------------------------
        # Row count
        # ----------------------------------------------------

        self.row_count_after = (
            self.grid_activity_geo_df.count()
        )

        logger.info(
            "Rows before join: %d",
            self.row_count_before
        )

        logger.info(
            "Rows after join: %d",
            self.row_count_after
        )

        # ----------------------------------------------------
        # LEFT JOIN MUST PRESERVE ROW COUNT
        # ----------------------------------------------------

        if (
            self.row_count_after
            != self.row_count_before
        ):

            raise AssertionError(
                "LEFT JOIN changed the row count. "
                "The grid lookup probably contains duplicate keys."
            )

        logger.info(
            "VALIDATION row count after LEFT JOIN: PASS"
        )

        # ----------------------------------------------------
        # Distinct grids
        # ----------------------------------------------------

        self.distinct_grids_after = (
            self.grid_activity_geo_df
            .select("grid_id")
            .distinct()
            .count()
        )

        logger.info(
            "Distinct grids after join: %d",
            self.distinct_grids_after
        )

        if (
            self.distinct_grids_after
            != self.distinct_grids_before
        ):

            raise AssertionError(
                "Distinct grid count changed after "
                "LEFT JOIN."
            )

        # ----------------------------------------------------
        # Missing geometry
        # ----------------------------------------------------

        missing_geometry_df = (
            self.grid_activity_geo_df
            .filter(
                F.col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
        )

        missing_count = (
            missing_geometry_df.count()
        )

        logger.info(
            "Grids with missing geometry: %d",
            missing_count
        )

        self.unmatched_grid_ids = (
            missing_geometry_df
            .orderBy("grid_id")
        )

        # ----------------------------------------------------
        # Enrichment coverage
        # ----------------------------------------------------

        enriched_grids = (
            self.grid_activity_geo_df
            .filter(
                F.col("geometry").isNotNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )

        if self.distinct_grids_before > 0:

            self.enrichment_percentage = (
                enriched_grids
                /
                self.distinct_grids_before
                *
                100.0
            )

        else:

            self.enrichment_percentage = 0.0

        logger.info(
            "Geographic enrichment coverage: %.2f%%",
            self.enrichment_percentage
        )

        if missing_count != 0:

            logger.error(
                "Unmatched grid IDs exist."
            )

            raise AssertionError(
                "SP4 requires 100%% enrichment coverage."
            )

        if self.enrichment_percentage != 100.0:

            raise AssertionError(
                "Enrichment coverage is not 100%%."
            )

        logger.info(
            "VALIDATION enrichment coverage: PASS"
        )

        logger.info(
            "VALIDATION unmatched grid IDs: PASS"
        )

    # ========================================================
    # 8. POLYGON CENTROID
    # ========================================================

    @staticmethod
    def _calculate_polygon_centroid(
        geometry
    ):

        geometry_type = geometry.get(
            "type"
        )

        coordinates = geometry.get(
            "coordinates"
        )

        if geometry_type != "Polygon":

            raise ValueError(
                "Expected Polygon geometry."
            )

        if not coordinates:

            raise ValueError(
                "Polygon contains no coordinates."
            )

        ring = coordinates[0]

        if len(ring) < 3:

            raise ValueError(
                "Polygon ring has insufficient coordinates."
            )

        # ----------------------------------------------------
        # Shoelace centroid.
        #
        # GeoJSON coordinate order:
        #
        # [longitude, latitude]
        # ----------------------------------------------------

        area_twice = 0.0
        centroid_x = 0.0
        centroid_y = 0.0

        for index in range(
            len(ring) - 1
        ):

            x1 = ring[index][0]
            y1 = ring[index][1]

            x2 = ring[index + 1][0]
            y2 = ring[index + 1][1]

            cross = (
                x1 * y2
                -
                x2 * y1
            )

            area_twice += cross

            centroid_x += (
                x1 + x2
            ) * cross

            centroid_y += (
                y1 + y2
            ) * cross

        if area_twice == 0:

            # Fallback for degenerate polygons.
            longitude = sum(
                point[0]
                for point in ring
            ) / len(ring)

            latitude = sum(
                point[1]
                for point in ring
            ) / len(ring)

            return (
                longitude,
                latitude
            )

        centroid_x /= (
            3.0 * area_twice
        )

        centroid_y /= (
            3.0 * area_twice
        )

        return (
            centroid_x,
            centroid_y
        )

    # ========================================================
    # 9. GEOGRAPHIC SPOT CHECK
    # ========================================================

    def geographic_spot_check(self):

        logger.info(
            "========== GEOGRAPHIC SPOT CHECK =========="
        )

        sample_ids = [
            1,
            2,
            4821
        ]

        centroids = {}

        for grid_id in sample_ids:

            row = (
                self.grid_lookup_df
                .filter(
                    F.col("grid_id") == grid_id
                )
                .select("geometry")
                .first()
            )

            if row is None:

                raise AssertionError(
                    f"Grid {grid_id} was not found."
                )

            geometry = json.loads(
                row["geometry"]
            )

            centroid = (
                self._calculate_polygon_centroid(
                    geometry
                )
            )

            centroids[grid_id] = centroid

            logger.info(
                "Grid %d centroid: "
                "longitude=%.6f, latitude=%.6f",
                grid_id,
                centroid[0],
                centroid[1]
            )

        # ----------------------------------------------------
        # Grid 1 and grid 2 must not be identical.
        # ----------------------------------------------------

        longitude_1, latitude_1 = (
            centroids[1]
        )

        longitude_2, latitude_2 = (
            centroids[2]
        )

        distance = (
            (
                longitude_1
                -
                longitude_2
            ) ** 2
            +
            (
                latitude_1
                -
                latitude_2
            ) ** 2
        ) ** 0.5

        logger.info(
            "Grid 1 / Grid 2 centroid distance: %.8f",
            distance
        )

        if distance == 0:

            raise AssertionError(
                "Grid 1 and grid 2 have identical "
                "centroids."
            )

        logger.info(
            "Grid 1 and Grid 2 are spatially distinct."
        )

        logger.info(
            "VALIDATION geographic spot-check: PASS"
        )

    # ========================================================
    # 10. CREATE FINAL DATASET
    # ========================================================

    def create_final_output(self):

        if self.grid_activity_geo_df is None:

            raise RuntimeError(
                "No enriched DataFrame available."
            )

        final_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_activity",
            "geometry"
        ]

        missing_columns = [
            column
            for column in final_columns
            if column
            not in self.grid_activity_geo_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Final output is missing columns: "
                f"{missing_columns}"
            )

        self.grid_activity_geo_df = (
            self.grid_activity_geo_df
            .select(final_columns)
        )

        # ----------------------------------------------------
        # Required grain assertion
        # ----------------------------------------------------

        duplicate_count = (
            self.grid_activity_geo_df
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
            "SP4 enriched dataset contains duplicate "
            "grid_id + timestamp records."
        )

        logger.info(
            "VALIDATION final grid/hour grain: PASS"
        )

        # ----------------------------------------------------
        # country_code must not exist
        # ----------------------------------------------------

        if "country_code" in (
            self.grid_activity_geo_df.columns
        ):

            raise AssertionError(
                "country_code must not appear "
                "in SP4 output."
            )

        logger.info(
            "VALIDATION country_code excluded: PASS"
        )

        return self.grid_activity_geo_df

    # ========================================================
    # 11. TOP HIGH-ACTIVITY GRIDS
    # ========================================================

    def identify_top_activity_grids(
        self,
        limit=10
    ):

        logger.info(
            "Calculating top %d high-activity grids...",
            limit
        )

        self.top_activity_grids = (
            self.grid_activity_geo_df
            .groupBy(
                "grid_id",
                "geometry"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),
                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                ),
                F.count(
                    "*"
                ).alias(
                    "active_hours"
                )
            )
            .orderBy(
                F.col(
                    "total_activity"
                ).desc()
            )
            .limit(limit)
        )

        logger.info(
            "Top high-activity grids:"
        )

        self.top_activity_grids.show(
            limit,
            truncate=False
        )

        return self.top_activity_grids

    # ========================================================
    # 12. EXPORT ENRICHED DATA
    # ========================================================

    def export_enriched_data(self):

        logger.info(
            "Writing enriched data to:"
        )

        logger.info(
            "%s",
            SP4_ENRICHED_OUTPUT
        )

        (
            self.grid_activity_geo_df
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(SP4_ENRICHED_OUTPUT)
            )
        )

        logger.info(
            "Enriched dataset exported successfully."
        )

    # ========================================================
    # 13. EXPORT UNMATCHED GRID IDS
    # ========================================================

    def export_unmatched_grids(self):

        if self.unmatched_grid_ids is None:

            raise RuntimeError(
                "Unmatched grid DataFrame has not been created."
            )

        (
            self.unmatched_grid_ids
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(SP4_UNMATCHED_OUTPUT)
            )
        )

        logger.info(
            "Unmatched grid IDs exported to:"
        )

        logger.info(
            "%s",
            SP4_UNMATCHED_OUTPUT
        )

    # ========================================================
    # 14. EXPORT TOP GRIDS
    # ========================================================

    def export_top_activity_grids(self):

        if self.top_activity_grids is None:

            raise RuntimeError(
                "Top activity grids have not been calculated."
            )

        (
            self.top_activity_grids
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(SP4_TOP_GRIDS_OUTPUT)
            )
        )

        logger.info(
            "Top activity grids exported to:"
        )

        logger.info(
            "%s",
            SP4_TOP_GRIDS_OUTPUT
        )

    # ========================================================
    # 15. COVERAGE REPORT
    # ========================================================

    def export_coverage_report(self):

        with open(
            SP4_REPORT,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP4 MILAN GRID GEOSPATIAL ENRICHMENT REPORT\n"
            )

            file.write(
                "=" * 65
                + "\n\n"
            )

            file.write(
                "GeoJSON:\n"
            )

            file.write(
                f"{GEOJSON_FILE}\n\n"
            )

            file.write(
                "SP3 input:\n"
            )

            file.write(
                f"{SP3_OUTPUT_DIR}\n\n"
            )

            file.write(
                "GeoJSON top-level type:\n"
            )

            file.write(
                "FeatureCollection\n\n"
            )

            file.write(
                "GeoJSON feature count:\n"
            )

            file.write(
                f"{self.geojson_feature_count}\n\n"
            )

            file.write(
                "JOIN KEY:\n"
            )

            file.write(
                "GeoJSON properties.cellId -> grid_id\n\n"
            )

            file.write(
                "IMPORTANT:\n"
            )

            file.write(
                "The top-level GeoJSON feature.id was NOT used.\n"
            )

            file.write(
                "properties.cellId is the correct 1-based identifier.\n\n"
            )

            file.write(
                "JOIN TYPE:\n"
            )

            file.write(
                "LEFT JOIN\n\n"
            )

            file.write(
                "JOIN STRATEGY:\n"
            )

            file.write(
                "BROADCAST JOIN\n\n"
            )

            file.write(
                "ACTIVITY ROWS BEFORE JOIN:\n"
            )

            file.write(
                f"{self.row_count_before}\n\n"
            )

            file.write(
                "ACTIVITY ROWS AFTER JOIN:\n"
            )

            file.write(
                f"{self.row_count_after}\n\n"
            )

            file.write(
                "DISTINCT GRIDS BEFORE JOIN:\n"
            )

            file.write(
                f"{self.distinct_grids_before}\n\n"
            )

            file.write(
                "DISTINCT GRIDS AFTER JOIN:\n"
            )

            file.write(
                f"{self.distinct_grids_after}\n\n"
            )

            file.write(
                "ENRICHMENT COVERAGE:\n"
            )

            file.write(
                f"{self.enrichment_percentage:.2f}%\n\n"
            )

            file.write(
                "VALIDATION RESULTS:\n"
            )

            file.write(
                "Row count preserved: PASS\n"
            )

            file.write(
                "Distinct grid count preserved: PASS\n"
            )

            file.write(
                "100% enrichment coverage: PASS\n"
            )

            file.write(
                "No unmatched grids: PASS\n"
            )

            file.write(
                "Grid/hour duplicate check: PASS\n"
            )

            file.write(
                "GeoJSON key = properties.cellId: PASS\n"
            )

            file.write(
                "country_code excluded: PASS\n"
            )

            file.write(
                "\nWHY BROADCAST?\n"
            )

            file.write(
                "The Milan grid lookup contains 10,000 rows, "
                "while the activity dataset contains more than "
                "one million rows. The small static lookup is "
                "therefore suitable for broadcast joining.\n"
            )

        logger.info(
            "Coverage report written to:"
        )

        logger.info(
            "%s",
            SP4_REPORT
        )

    # ========================================================
    # 16. RUN PIPELINE
    # ========================================================

    def run(self):

        try:

            logger.info(
                "=" * 60
            )

            logger.info(
                "SP4 GEOSPATIAL ENRICHMENT STARTED"
            )

            logger.info(
                "=" * 60
            )

            # ------------------------------------------------
            # Step 1
            # ------------------------------------------------

            self.load_activity_data()

            # ------------------------------------------------
            # Step 2
            # ------------------------------------------------

            self.validate_activity_data()

            # ------------------------------------------------
            # Step 3
            # ------------------------------------------------

            self.build_grid_lookup()

            # ------------------------------------------------
            # Step 4
            # ------------------------------------------------

            self.show_standard_join_plan()

            # ------------------------------------------------
            # Step 5
            # ------------------------------------------------

            self.enrich_with_broadcast_join()

            # ------------------------------------------------
            # Step 6
            # ------------------------------------------------

            self.validate_enrichment()

            # ------------------------------------------------
            # Step 7
            # ------------------------------------------------

            self.geographic_spot_check()

            # ------------------------------------------------
            # Step 8
            # ------------------------------------------------

            self.create_final_output()

            # ------------------------------------------------
            # Step 9
            # ------------------------------------------------

            self.identify_top_activity_grids(
                limit=10
            )

            # ------------------------------------------------
            # Step 10
            # ------------------------------------------------

            self.export_enriched_data()

            self.export_unmatched_grids()

            self.export_top_activity_grids()

            self.export_coverage_report()

            logger.info(
                "=" * 60
            )

            logger.info(
                "ALL SP4 ACCEPTANCE CRITERIA PASSED"
            )

            logger.info(
                "SP4 PROCESSING COMPLETED SUCCESSFULLY"
            )

            logger.info(
                "=" * 60
            )

            return self.grid_activity_geo_df

        except Exception:

            logger.exception(
                "SP4 processing failed."
            )

            raise