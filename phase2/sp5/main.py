import os
import sys
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

PHASE2_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

PROJECT_ROOT = (
    PHASE2_ROOT.parent
)


# ============================================================
# HADOOP CONFIGURATION
# ============================================================

HADOOP_HOME = (
    PROJECT_ROOT
    / "winutils"
    / "hadoop-win-utils"
)

HADOOP_BIN = (
    HADOOP_HOME
    / "bin"
)

WINUTILS_EXE = (
    HADOOP_BIN
    / "winutils.exe"
)


# ============================================================
# VALIDATE HADOOP
# ============================================================

if not HADOOP_HOME.exists():

    raise FileNotFoundError(
        "\nHADOOP_HOME directory not found:\n"
        f"{HADOOP_HOME}\n\n"
        "Expected:\n"
        "project root/"
        "winutils/"
        "hadoop-win-utils/"
        "bin/"
        "winutils.exe"
    )


if not WINUTILS_EXE.exists():

    raise FileNotFoundError(
        "\nwinutils.exe was not found:\n"
        f"{WINUTILS_EXE}"
    )


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

os.environ["HADOOP_HOME"] = str(
    HADOOP_HOME
)

os.environ["hadoop.home.dir"] = str(
    HADOOP_HOME
)

os.environ["PATH"] = (
    str(HADOOP_BIN)
    + os.pathsep
    + os.environ.get(
        "PATH",
        ""
    )
)


# ============================================================
# PYTHON WORKER CONFIGURATION
# ============================================================

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = (
    PYTHON_EXECUTABLE
)

os.environ["PYSPARK_DRIVER_PYTHON"] = (
    PYTHON_EXECUTABLE
)


# ============================================================
# IMPORT PYSPARK
# ============================================================

from pyspark.sql import SparkSession

from spark_performance import (
    SparkPerformanceAnalysis
)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=" * 70
    )

    print(
        "SP5 ENVIRONMENT"
    )

    print(
        "=" * 70
    )

    print(
        f"Project root : {PROJECT_ROOT}"
    )

    print(
        f"Phase 2 root: {PHASE2_ROOT}"
    )

    print(
        f"HADOOP_HOME : {HADOOP_HOME}"
    )

    print(
        f"winutils    : {WINUTILS_EXE}"
    )

    print(
        f"Python      : {PYTHON_EXECUTABLE}"
    )

    print(
        "=" * 70
    )

    # ========================================================
    # CREATE SPARK SESSION
    # ========================================================

    spark = (
        SparkSession
        .builder
        .appName(
            "SP5_Performance_Execution_Behaviour"
        )
        .master(
            "local[2]"
        )

        # ----------------------------------------------------
        # Windows Python worker stability
        # ----------------------------------------------------

        .config(
            "spark.python.worker.connect.timeout",
            "120s"
        )

        .config(
            "spark.network.timeout",
            "300s"
        )

        # ----------------------------------------------------
        # Local training workload.
        # ----------------------------------------------------

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        # ----------------------------------------------------
        # Driver memory
        # ----------------------------------------------------

        .config(
            "spark.driver.memory",
            "4g"
        )

        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false"
        )

        .getOrCreate()
    )

    try:

        spark.sparkContext.setLogLevel(
            "WARN"
        )

        print(
            f"Spark version: {spark.version}"
        )

        print(
            "=" * 70
        )

        # ====================================================
        # RUN SP5
        # ====================================================

        performance = (
            SparkPerformanceAnalysis(
                spark=spark
            )
        )

        performance.run()

        print(
            "=" * 70
        )

        print(
            "SP5 COMPLETED SUCCESSFULLY"
        )

        print(
            "Performance report created in:"
        )

        print(
            PHASE2_ROOT
            / "output"
            / "sp5"
            / "performance_report.txt"
        )

        print(
            "=" * 70
        )

    finally:

        spark.stop()

        print(
            "SparkSession stopped."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()