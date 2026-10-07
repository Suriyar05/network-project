import os
import sys
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

# main.py is located at:
#
# phase 2/sp4/main.py
#
# parents[0] = sp4
# parents[1] = phase 2
# parents[2] = project root
#

PHASE2_ROOT = Path(
    __file__
).resolve().parents[1]

PROJECT_ROOT = PHASE2_ROOT.parent


# ============================================================
# HADOOP / WINUTILS CONFIGURATION
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


# ------------------------------------------------------------
# Validate Hadoop installation
# ------------------------------------------------------------

if not HADOOP_HOME.exists():

    raise FileNotFoundError(
        "\nHADOOP_HOME directory not found:\n"
        f"{HADOOP_HOME}\n\n"
        "Expected structure:\n"
        "project root/\n"
        "  winutils/\n"
        "    hadoop-win-utils/\n"
        "      bin/\n"
        "        winutils.exe\n"
    )


if not WINUTILS_EXE.exists():

    raise FileNotFoundError(
        "\nwinutils.exe was not found:\n"
        f"{WINUTILS_EXE}\n\n"
        "Check your Hadoop installation."
    )


# ============================================================
# SET HADOOP ENVIRONMENT VARIABLES
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
# PYSPARK PYTHON CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# Force Spark driver and Python workers to use the same
# Python executable.
#
# This is especially important on Windows.
# ------------------------------------------------------------

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = (
    PYTHON_EXECUTABLE
)

os.environ["PYSPARK_DRIVER_PYTHON"] = (
    PYTHON_EXECUTABLE
)


# ============================================================
# DISPLAY CONFIGURATION
# ============================================================

print(
    "=" * 60
)

print(
    "SP4 ENVIRONMENT"
)

print(
    "=" * 60
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
    "=" * 60
)


# ============================================================
# IMPORT SPARK
# ============================================================

from pyspark.sql import SparkSession

from spark_geospatial import (
    SparkGeospatialEnrichment
)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=" * 60
    )

    print(
        "STARTING SP4 MAIN"
    )

    print(
        "=" * 60
    )

    # --------------------------------------------------------
    # Create Spark session
    # --------------------------------------------------------

    spark = (
        SparkSession
        .builder
        .appName(
            "SP4_Geospatial_Enrichment"
        )
        .master(
            "local[2]"
        )

        # ----------------------------------------------------
        # Windows / Python worker stability
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
        # Smaller number of shuffle partitions for local
        # development.
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

        # ----------------------------------------------------
        # Arrow disabled because SP4 does not need Arrow.
        # ----------------------------------------------------

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
            "=" * 60
        )

        # ----------------------------------------------------
        # Create SP4 processor
        # ----------------------------------------------------

        enricher = (
            SparkGeospatialEnrichment(
                spark=spark
            )
        )

        # ----------------------------------------------------
        # Run complete SP4 pipeline
        # ----------------------------------------------------

        enricher.run()

        print(
            "=" * 60
        )

        print(
            "SP4 COMPLETED SUCCESSFULLY"
        )

        print(
            "=" * 60
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