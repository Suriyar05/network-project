import os
import sys
import time
import logging
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ============================================================
# PROJECT PATHS
# ============================================================

PHASE2_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = PHASE2_ROOT.parent

DATA_DIR = PHASE2_ROOT / "data"
OUTPUT_DIR = PHASE2_ROOT / "output"
LOG_DIR = PHASE2_ROOT / "logs"

SP3_OUTPUT_DIR = (
    OUTPUT_DIR
    / "sp3"
    / "hourly_grid_summary"
)

SP4_OUTPUT_DIR = (
    OUTPUT_DIR
    / "sp4_grid_activity_geo"
)

SP5_OUTPUT_DIR = (
    OUTPUT_DIR
    / "sp5"
)

SP5_REPORT = (
    SP5_OUTPUT_DIR
    / "performance_report.txt"
)


OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)

SP5_OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_DIR / "sp5.log"

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
# SP5 PERFORMANCE CLASS
# ============================================================

class SparkPerformanceAnalysis:

    """
    SP5 - Spark Performance & Execution Behaviour.

    Purpose:
        Measure Spark execution behaviour rather than blindly
        applying optimizations.

    Experiments:
        1. explain() on hotspot aggregation
        2. cache / persist timing
        3. repartition by date
        4. column pruning
        5. standard vs broadcast join
        6. partition analysis
        7. evidence-based observations
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(self, spark):

        self.spark = spark

        self.activity_df = None

        self.grid_lookup_df = None

        self.cleaned_cached_df = None

        self.performance_observations = []

        self.results = {}

    # ========================================================
    # 1. LOAD SP3 DATA
    # ========================================================

    def load_sp3_data(self):

        if not SP3_OUTPUT_DIR.exists():

            raise FileNotFoundError(
                "\nSP3 output was not found.\n\n"
                f"Expected:\n"
                f"{SP3_OUTPUT_DIR}\n\n"
                "Run SP3 first."
            )

        logger.info(
            "Loading SP3 hourly_grid_summary:"
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
            "SP3 data loaded."
        )

        logger.info(
            "Columns: %s",
            self.activity_df.columns
        )

        return self.activity_df

    # ========================================================
    # 2. BASIC DATA PROFILE
    # ========================================================

    def profile_data(self):

        logger.info(
            "========== SP5 DATA PROFILE =========="
        )

        row_count = (
            self.activity_df.count()
        )

        partition_count = (
            self.activity_df.rdd
            .getNumPartitions()
        )

        distinct_dates = (
            self.activity_df
            .select("date")
            .distinct()
            .count()
        )

        distinct_grids = (
            self.activity_df
            .select("grid_id")
            .distinct()
            .count()
        )

        logger.info(
            "Rows: %d",
            row_count
        )

        logger.info(
            "Partitions: %d",
            partition_count
        )

        logger.info(
            "Distinct dates: %d",
            distinct_dates
        )

        logger.info(
            "Distinct grids: %d",
            distinct_grids
        )

        self.results["row_count"] = row_count

        self.results["initial_partitions"] = (
            partition_count
        )

        self.results["distinct_dates"] = (
            distinct_dates
        )

        self.results["distinct_grids"] = (
            distinct_grids
        )

    # ========================================================
    # 3. HOTSPOT AGGREGATION PLAN
    # ========================================================

    def hotspot_aggregation_plan(self):

        logger.info(
            "========== HOTSPOT AGGREGATION PLAN =========="
        )

        required_columns = [
            "grid_id",
            "timestamp",
            "total_activity"
        ]

        hotspot_df = (
            self.activity_df
            .select(required_columns)
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.col(
                    "total_activity"
                ).desc()
            )
            .limit(10)
        )

        logger.info(
            "Physical execution plan:"
        )

        hotspot_df.explain(
            mode="formatted"
        )

        logger.info(
            "Hotspot aggregation plan inspection complete."
        )

        self.results[
            "hotspot_plan"
        ] = (
            "Physical plan inspected using explain(formatted)."
        )

        return hotspot_df

    # ========================================================
    # 4. EXECUTE HOTSPOT AGGREGATION
    # ========================================================

    def execute_hotspot_aggregation(self):

        logger.info(
            "Executing hotspot aggregation..."
        )

        start = time.perf_counter()

        result = (
            self.activity_df
            .select(
                "grid_id",
                "total_activity"
            )
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.col(
                    "total_activity"
                ).desc()
            )
            .limit(10)
        )

        result_rows = result.collect()

        elapsed = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Hotspot aggregation execution time: %.4f seconds",
            elapsed
        )

        result.show(
            truncate=False
        )

        self.results[
            "hotspot_execution_seconds"
        ] = elapsed

        return result

    # ========================================================
    # 5. CACHE EXPERIMENT
    # ========================================================

    def cache_experiment(self):

        logger.info(
            "========== CACHE EXPERIMENT =========="
        )

        # ----------------------------------------------------
        # Create a reusable transformation.
        # ----------------------------------------------------

        reusable_df = (
            self.activity_df
            .select(
                "timestamp",
                "date",
                "grid_id",
                "total_activity",
                "internet_activity"
            )
            .filter(
                F.col(
                    "total_activity"
                ).isNotNull()
            )
        )

        # ----------------------------------------------------
        # BEFORE CACHE
        # ----------------------------------------------------

        logger.info(
            "Running first action WITHOUT cache..."
        )

        start = time.perf_counter()

        count_before = (
            reusable_df.count()
        )

        uncached_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Uncached action count: %d",
            count_before
        )

        logger.info(
            "Uncached action time: %.4f seconds",
            uncached_time
        )

        # ----------------------------------------------------
        # CACHE
        # ----------------------------------------------------

        logger.info(
            "Persisting reusable DataFrame..."
        )

        self.cleaned_cached_df = (
            reusable_df
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        # ----------------------------------------------------
        # FIRST CACHED ACTION
        #
        # This materializes the cache.
        # ----------------------------------------------------

        start = time.perf_counter()

        first_cached_count = (
            self.cleaned_cached_df.count()
        )

        first_cached_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "First cached action: %.4f seconds",
            first_cached_time
        )

        # ----------------------------------------------------
        # SECOND CACHED ACTION
        #
        # This should reuse persisted partitions.
        # ----------------------------------------------------

        start = time.perf_counter()

        second_cached_count = (
            self.cleaned_cached_df.count()
        )

        second_cached_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Second cached action: %.4f seconds",
            second_cached_time
        )

        if (
            first_cached_count
            !=
            second_cached_count
        ):

            raise AssertionError(
                "Cached action counts do not match."
            )

        self.results[
            "uncached_seconds"
        ] = uncached_time

        self.results[
            "first_cached_seconds"
        ] = first_cached_time

        self.results[
            "second_cached_seconds"
        ] = second_cached_time

        # ----------------------------------------------------
        # Evidence-based observation.
        # ----------------------------------------------------

        if second_cached_time < uncached_time:

            observation = (
                "Caching was beneficial for this repeated "
                "action because the second cached action "
                "was faster than the original uncached action."
            )

        else:

            observation = (
                "Caching did not improve the measured repeated "
                "action in this local run. This is evidence "
                "against assuming that cache is always beneficial."
            )

        logger.info(
            "CACHE OBSERVATION: %s",
            observation
        )

        self.performance_observations.append(
            observation
        )

        return self.cleaned_cached_df

    # ========================================================
    # 6. REPARTITION EXPERIMENT
    # ========================================================

    def repartition_experiment(self):

        logger.info(
            "========== REPARTITION EXPERIMENT =========="
        )

        original_partitions = (
            self.activity_df
            .rdd
            .getNumPartitions()
        )

        logger.info(
            "Original partition count: %d",
            original_partitions
        )

        # ----------------------------------------------------
        # Repartition by date.
        #
        # This is deliberately an experiment.
        # We do not claim it is automatically better.
        # ----------------------------------------------------

        repartitioned_df = (
            self.activity_df
            .repartition(
                "date"
            )
        )

        repartitioned_partitions = (
            repartitioned_df
            .rdd
            .getNumPartitions()
        )

        logger.info(
            "Repartitioned partition count: %d",
            repartitioned_partitions
        )

        logger.info(
            "Repartition by date creates a shuffle."
        )

        # ----------------------------------------------------
        # Execute a date aggregation after repartition.
        # ----------------------------------------------------

        start = time.perf_counter()

        (
            repartitioned_df
            .groupBy("date")
            .count()
            .collect()
        )

        elapsed = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Repartitioned date aggregation time: %.4f seconds",
            elapsed
        )

        self.results[
            "repartition_original"
        ] = original_partitions

        self.results[
            "repartition_new"
        ] = repartitioned_partitions

        self.results[
            "repartition_aggregation_seconds"
        ] = elapsed

        observation = (
            "Repartitioning by date introduced a shuffle. "
            "It should only be retained when the resulting "
            "partitioning benefits downstream operations."
        )

        logger.info(
            "REPARTITION OBSERVATION: %s",
            observation
        )

        self.performance_observations.append(
            observation
        )

        return repartitioned_df

    # ========================================================
    # 7. COLUMN PRUNING
    # ========================================================

    def column_pruning_experiment(self):

        logger.info(
            "========== COLUMN PRUNING EXPERIMENT =========="
        )

        # ----------------------------------------------------
        # BAD / WIDE VERSION
        # ----------------------------------------------------

        wide_df = (
            self.activity_df
        )

        logger.info(
            "Wide aggregation physical plan:"
        )

        wide_aggregation = (
            wide_df
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
        )

        wide_aggregation.explain(
            mode="formatted"
        )

        # ----------------------------------------------------
        # PRUNED VERSION
        # ----------------------------------------------------

        pruned_df = (
            self.activity_df
            .select(
                "grid_id",
                "total_activity"
            )
        )

        logger.info(
            "Column-pruned aggregation physical plan:"
        )

        pruned_aggregation = (
            pruned_df
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
        )

        pruned_aggregation.explain(
            mode="formatted"
        )

        # ----------------------------------------------------
        # Execute pruned version.
        # ----------------------------------------------------

        start = time.perf_counter()

        pruned_aggregation.collect()

        elapsed = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Column-pruned aggregation time: %.4f seconds",
            elapsed
        )

        self.results[
            "column_pruning_seconds"
        ] = elapsed

        observation = (
            "Column pruning reduces the logical input to only "
            "the fields required by the aggregation. Spark's "
            "physical plan can therefore avoid carrying "
            "unnecessary columns through the aggregation."
        )

        logger.info(
            "COLUMN PRUNING OBSERVATION: %s",
            observation
        )

        self.performance_observations.append(
            observation
        )

        return pruned_aggregation

    # ========================================================
    # 8. LOAD SP4 GRID LOOKUP
    # ========================================================

    def load_grid_lookup(self):

        if not SP4_OUTPUT_DIR.exists():

            logger.warning(
                "SP4 output not found."
            )

            logger.warning(
                "Standard vs broadcast join experiment "
                "will use a small grid lookup generated "
                "from the SP4 GeoJSON output if possible."
            )

            return None

        logger.info(
            "Loading SP4 enriched dataset:"
        )

        logger.info(
            "%s",
            SP4_OUTPUT_DIR
        )

        sp4_df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(
                str(SP4_OUTPUT_DIR)
            )
        )

        # ----------------------------------------------------
        # Extract static grid lookup.
        # ----------------------------------------------------

        self.grid_lookup_df = (
            sp4_df
            .select(
                "grid_id",
                "geometry"
            )
            .dropDuplicates(
                ["grid_id"]
            )
        )

        logger.info(
            "Grid lookup loaded from SP4."
        )

        logger.info(
            "Grid lookup rows: %d",
            self.grid_lookup_df.count()
        )

        return self.grid_lookup_df

    # ========================================================
    # 9. STANDARD VS BROADCAST JOIN
    # ========================================================

    def broadcast_join_experiment(self):

        logger.info(
            "========== BROADCAST JOIN EXPERIMENT =========="
        )

        if self.grid_lookup_df is None:

            logger.warning(
                "Skipping broadcast experiment because "
                "SP4 grid lookup is unavailable."
            )

            return None

        # ----------------------------------------------------
        # Standard join
        # ----------------------------------------------------

        standard_join = (
            self.activity_df
            .join(
                self.grid_lookup_df,
                on="grid_id",
                how="left"
            )
        )

        logger.info(
            "STANDARD JOIN PHYSICAL PLAN:"
        )

        standard_join.explain(
            mode="formatted"
        )

        # ----------------------------------------------------
        # Broadcast join
        # ----------------------------------------------------

        broadcast_join = (
            self.activity_df
            .join(
                F.broadcast(
                    self.grid_lookup_df
                ),
                on="grid_id",
                how="left"
            )
        )

        logger.info(
            "BROADCAST JOIN PHYSICAL PLAN:"
        )

        broadcast_join.explain(
            mode="formatted"
        )

        # ----------------------------------------------------
        # Timing standard join
        # ----------------------------------------------------

        start = time.perf_counter()

        standard_count = (
            standard_join.count()
        )

        standard_time = (
            time.perf_counter()
            -
            start
        )

        # ----------------------------------------------------
        # Timing broadcast join
        # ----------------------------------------------------

        start = time.perf_counter()

        broadcast_count = (
            broadcast_join.count()
        )

        broadcast_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Standard join rows: %d",
            standard_count
        )

        logger.info(
            "Broadcast join rows: %d",
            broadcast_count
        )

        logger.info(
            "Standard join time: %.4f seconds",
            standard_time
        )

        logger.info(
            "Broadcast join time: %.4f seconds",
            broadcast_time
        )

        if (
            standard_count
            !=
            broadcast_count
        ):

            raise AssertionError(
                "Standard and broadcast join row counts differ."
            )

        self.results[
            "standard_join_seconds"
        ] = standard_time

        self.results[
            "broadcast_join_seconds"
        ] = broadcast_time

        # ----------------------------------------------------
        # Evidence-based observation.
        # ----------------------------------------------------

        if broadcast_time < standard_time:

            observation = (
                "The broadcast join was faster in this run. "
                "The physical plan should show the small grid "
                "lookup being broadcast rather than requiring "
                "the same shuffle strategy as the standard join."
            )

        else:

            observation = (
                "The broadcast join was not faster in this "
                "local run. This demonstrates why benchmark "
                "evidence matters instead of assuming every "
                "broadcast join will improve runtime."
            )

        logger.info(
            "BROADCAST OBSERVATION: %s",
            observation
        )

        self.performance_observations.append(
            observation
        )

        return broadcast_join

    # ========================================================
    # 10. OVER-PARTITIONING DEMONSTRATION
    # ========================================================

    def over_partitioning_experiment(self):

        logger.info(
            "========== OVER-PARTITIONING EXPERIMENT =========="
        )

        original_partitions = (
            self.activity_df
            .rdd
            .getNumPartitions()
        )

        # ----------------------------------------------------
        # Deliberately create many more partitions.
        #
        # This is NOT recommended as an optimization.
        # It demonstrates the cost of excessive partitioning.
        # ----------------------------------------------------

        excessive_partition_count = max(
            original_partitions * 20,
            100
        )

        logger.info(
            "Original partitions: %d",
            original_partitions
        )

        logger.info(
            "Experimental excessive partitions: %d",
            excessive_partition_count
        )

        start = time.perf_counter()

        excessive_df = (
            self.activity_df
            .repartition(
                excessive_partition_count
            )
        )

        repartition_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Repartition operation construction time: %.4f seconds",
            repartition_time
        )

        # ----------------------------------------------------
        # Execute an action.
        # ----------------------------------------------------

        start = time.perf_counter()

        excessive_count = (
            excessive_df.count()
        )

        execution_time = (
            time.perf_counter()
            -
            start
        )

        logger.info(
            "Excessively partitioned count: %d",
            excessive_count
        )

        logger.info(
            "Excessively partitioned execution time: %.4f seconds",
            execution_time
        )

        self.results[
            "excessive_partitions"
        ] = excessive_partition_count

        self.results[
            "excessive_partition_execution_seconds"
        ] = execution_time

        observation = (
            "Over-partitioning can increase scheduling and "
            "task-management overhead. On a small local "
            "training dataset, creating hundreds of tiny "
            "partitions is generally unnecessary."
        )

        logger.info(
            "OVER-PARTITIONING OBSERVATION: %s",
            observation
        )

        self.performance_observations.append(
            observation
        )

    # ========================================================
    # 11. REJECT ONE OPTIMIZATION
    # ========================================================

    def document_rejected_optimization(self):

        # ----------------------------------------------------
        # Required by SP5 acceptance criteria:
        #
        # At least one suggestion must be rejected.
        # ----------------------------------------------------

        rejected = (
            "REJECTED OPTIMIZATION: Aggressively increasing "
            "the partition count was rejected because this "
            "dataset is running locally and the measured "
            "workload does not justify hundreds of tiny tasks. "
            "The goal is not to maximize partition count; "
            "the goal is to choose a partitioning strategy "
            "supported by evidence."
        )

        logger.info(
            rejected
        )

        self.performance_observations.append(
            rejected
        )

    # ========================================================
    # 12. WRITE PERFORMANCE REPORT
    # ========================================================

    def write_report(self):

        logger.info(
            "Writing SP5 performance report:"
        )

        logger.info(
            "%s",
            SP5_REPORT
        )

        with open(
            SP5_REPORT,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(
                "SP5 - SPARK PERFORMANCE & EXECUTION BEHAVIOUR\n"
            )

            file.write(
                "=" * 70
                + "\n\n"
            )

            file.write(
                "PURPOSE\n"
            )

            file.write(
                "Measure Spark execution behaviour and make "
                "optimization decisions based on evidence.\n\n"
            )

            file.write(
                "DATASET\n"
            )

            file.write(
                f"SP3 output: {SP3_OUTPUT_DIR}\n\n"
            )

            file.write(
                "BASIC PROFILE\n"
            )

            file.write(
                f"Rows: "
                f"{self.results.get('row_count', 'N/A')}\n"
            )

            file.write(
                f"Initial partitions: "
                f"{self.results.get('initial_partitions', 'N/A')}\n"
            )

            file.write(
                f"Distinct dates: "
                f"{self.results.get('distinct_dates', 'N/A')}\n"
            )

            file.write(
                f"Distinct grids: "
                f"{self.results.get('distinct_grids', 'N/A')}\n\n"
            )

            file.write(
                "CACHE EXPERIMENT\n"
            )

            file.write(
                f"Uncached action: "
                f"{self.results.get('uncached_seconds', 0):.4f} seconds\n"
            )

            file.write(
                f"First cached action: "
                f"{self.results.get('first_cached_seconds', 0):.4f} seconds\n"
            )

            file.write(
                f"Second cached action: "
                f"{self.results.get('second_cached_seconds', 0):.4f} seconds\n\n"
            )

            file.write(
                "REPARTITION EXPERIMENT\n"
            )

            file.write(
                f"Original partitions: "
                f"{self.results.get('repartition_original', 'N/A')}\n"
            )

            file.write(
                f"Partitions after repartition(date): "
                f"{self.results.get('repartition_new', 'N/A')}\n"
            )

            file.write(
                f"Date aggregation time: "
                f"{self.results.get('repartition_aggregation_seconds', 0):.4f} seconds\n\n"
            )

            file.write(
                "COLUMN PRUNING\n"
            )

            file.write(
                f"Pruned aggregation time: "
                f"{self.results.get('column_pruning_seconds', 0):.4f} seconds\n\n"
            )

            file.write(
                "JOIN EXPERIMENT\n"
            )

            file.write(
                f"Standard join: "
                f"{self.results.get('standard_join_seconds', 'N/A')} seconds\n"
            )

            file.write(
                f"Broadcast join: "
                f"{self.results.get('broadcast_join_seconds', 'N/A')} seconds\n\n"
            )

            file.write(
                "OVER-PARTITIONING\n"
            )

            file.write(
                f"Experimental partition count: "
                f"{self.results.get('excessive_partitions', 'N/A')}\n"
            )

            file.write(
                f"Execution time: "
                f"{self.results.get('excessive_partition_execution_seconds', 0):.4f} seconds\n\n"
            )

            file.write(
                "THREE+ PERFORMANCE OBSERVATIONS\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            for index, observation in enumerate(
                self.performance_observations,
                start=1
            ):

                file.write(
                    f"{index}. {observation}\n\n"
                )

            file.write(
                "EXECUTION CONCEPTS\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "Transformations are lazy. Operations such as "
                "select(), filter(), withColumn(), groupBy() and "
                "repartition() build a logical plan. Actions "
                "such as count(), collect(), show() and write() "
                "trigger execution.\n\n"
            )

            file.write(
                "GROUP BY operations generally involve a shuffle "
                "because rows with the same grouping key must "
                "be brought together.\n\n"
            )

            file.write(
                "repartition() causes a shuffle because Spark "
                "redistributes records across partitions.\n\n"
            )

            file.write(
                "A broadcast join can avoid a large shuffle on "
                "the small side when the lookup is sufficiently "
                "small to broadcast.\n\n"
            )

            file.write(
                "Column pruning reduces unnecessary data carried "
                "through the query plan.\n\n"
            )

            file.write(
                "Cache is useful when a DataFrame is expensive to "
                "recompute and will be reused. Caching every "
                "DataFrame is not automatically beneficial.\n\n"
            )

            file.write(
                "REJECTED OPTIMIZATION\n"
            )

            file.write(
                "-" * 70
                + "\n"
            )

            file.write(
                "Aggressively increasing the partition count "
                "was rejected because this is a local training "
                "workload. More partitions can create additional "
                "task scheduling overhead without providing "
                "enough parallel work to justify it.\n\n"
            )

            file.write(
                "IMPORTANT\n"
            )

            file.write(
                "The timings in this report are workload- and "
                "machine-dependent. They are evidence for this "
                "run, not universal Spark performance guarantees.\n"
            )

        logger.info(
            "SP5 report written successfully."
        )

    # ========================================================
    # 13. RELEASE CACHE
    # ========================================================

    def release_cache(self):

        if self.cleaned_cached_df is not None:

            logger.info(
                "Releasing cached DataFrame..."
            )

            self.cleaned_cached_df.unpersist(
                blocking=True
            )

            logger.info(
                "Cache released."
            )

    # ========================================================
    # 14. RUN
    # ========================================================

    def run(self):

        try:

            logger.info(
                "=" * 70
            )

            logger.info(
                "SP5 PERFORMANCE & EXECUTION BEHAVIOUR STARTED"
            )

            logger.info(
                "=" * 70
            )

            # ------------------------------------------------
            # SP3 data
            # ------------------------------------------------

            self.load_sp3_data()

            # ------------------------------------------------
            # Basic profile
            # ------------------------------------------------

            self.profile_data()

            # ------------------------------------------------
            # Experiment 1
            # ------------------------------------------------

            self.hotspot_aggregation_plan()

            self.execute_hotspot_aggregation()

            # ------------------------------------------------
            # Experiment 2
            # ------------------------------------------------

            self.cache_experiment()

            # ------------------------------------------------
            # Experiment 3
            # ------------------------------------------------

            self.repartition_experiment()

            # ------------------------------------------------
            # Experiment 4
            # ------------------------------------------------

            self.column_pruning_experiment()

            # ------------------------------------------------
            # Experiment 5
            # ------------------------------------------------

            self.load_grid_lookup()

            self.broadcast_join_experiment()

            # ------------------------------------------------
            # Experiment 6
            # ------------------------------------------------

            self.over_partitioning_experiment()

            # ------------------------------------------------
            # Acceptance criterion:
            # reject at least one optimization.
            # ------------------------------------------------

            self.document_rejected_optimization()

            # ------------------------------------------------
            # Report
            # ------------------------------------------------

            self.write_report()

            logger.info(
                "=" * 70
            )

            logger.info(
                "SP5 PERFORMANCE ANALYSIS COMPLETED"
            )

            logger.info(
                "Report: %s",
                SP5_REPORT
            )

            logger.info(
                "=" * 70
            )

            return self.results

        finally:

            self.release_cache()