from spark_aggregation import SparkNetworkAggregation


def main():

    aggregator = SparkNetworkAggregation()

    aggregator.run()


if __name__ == "__main__":
    main()