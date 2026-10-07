from spark_ingestion import SparkNetworkIngestion


def main():

    ingestion = SparkNetworkIngestion()

    try:

        ingestion.run()

    except Exception:

        raise

    finally:

        ingestion.stop()


if __name__ == "__main__":
    main()