import sys
from pathlib import Path

PHASE2_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(PHASE2_ROOT)
)

from sp1.spark_ingestion import SparkNetworkIngestion

def main():

    ingestion = SparkNetworkIngestion()

    ingestion.run()


if __name__ == "__main__":
    main()