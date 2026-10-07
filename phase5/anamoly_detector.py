import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if "ml4" not in config:
        raise KeyError("Missing 'ml4' configuration section")
    return config["ml4"]


def setup_logging(log_path):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)], force=True)
    return logging.getLogger("ML4")


def read_data(config, logger):
    source = pd.read_parquet(config["input_path"])
    alerts = pd.read_csv(config["np3_alert_path"])
    predictions = pd.read_parquet(config["ml3_prediction_path"])

    logger.info("Input rows: %d", len(source))
    logger.info("NP3 alert rows: %d", len(alerts))
    logger.info("ML3 prediction rows: %d", len(predictions))

    return source, alerts, predictions


def validate_data(source, alerts, predictions, config):
    required_source = ["grid_id", "timestamp", "total_activity"]
    required_alerts = ["grid_id", "timestamp"]
    required_predictions = ["grid_id", "feature_timestamp", "model_prediction"]

    missing_source = [column for column in required_source if column not in source.columns]
    missing_alerts = [column for column in required_alerts if column not in alerts.columns]
    missing_predictions = [column for column in required_predictions if column not in predictions.columns]

    if missing_source:
        raise ValueError(f"Missing source columns: {missing_source}")

    if missing_alerts:
        raise ValueError(f"Missing NP3 columns: {missing_alerts}")

    if missing_predictions:
        raise ValueError(f"Missing ML3 columns: {missing_predictions}")


def prepare_data(source, config, logger):
    timestamp_column = config["timestamp_column"]
    activity_column = config["activity_column"]

    source = source.copy()
    source[timestamp_column] = pd.to_datetime(source[timestamp_column])
    source[activity_column] = pd.to_numeric(source[activity_column], errors="coerce")
    source = source.dropna(subset=["grid_id", timestamp_column, activity_column])
    source = source.sort_values(["grid_id", timestamp_column])
    source["hour"] = source[timestamp_column].dt.hour
    source["day"] = source[timestamp_column].dt.date

    logger.info("Prepared %d rows", len(source))

    return source


def calculate_baseline(df, bucket_columns, activity_column="total_activity", baseline_column="baseline"):
    df = df.copy()

    group_columns = list(bucket_columns)

    df[baseline_column] = df.groupby(group_columns)[activity_column].transform("median")

    return df


def calculate_hourly_baseline(df, config, logger):
    df = calculate_baseline(
        df,
        ["grid_id", "hour"],
        config["activity_column"],
        "hour_of_day_baseline"
    )

    bucket_counts = df.groupby(["grid_id", "hour"]).size()

    logger.info("Hour-of-day baseline calculated")
    logger.info("Minimum observations per grid-hour bucket: %d", bucket_counts.min())
    logger.info("Maximum observations per grid-hour bucket: %d", bucket_counts.max())
    logger.info("Grid-hour buckets using multiple days: %d", int((bucket_counts > 1).sum()))

    return df


def calculate_anomaly_score(df, config, logger):
    activity_column = config["activity_column"]
    baseline_column = "hour_of_day_baseline"
    threshold = config["anomaly"]["percentage_threshold"]

    df = df.copy()

    df["deviation"] = df[activity_column] - df[baseline_column]

    df["anomaly_score"] = np.where(
        df[baseline_column].abs() > 0,
        (df["deviation"] / df[baseline_column].abs()) * 100,
        np.nan
    )

    df["direction"] = np.select(
        [
            df["anomaly_score"] >= threshold,
            df["anomaly_score"] <= -threshold
        ],
        [
            "HIGH",
            "LOW"
        ],
        default="NORMAL"
    )

    df["anomaly_flag"] = (df["direction"] != "NORMAL").astype(int)

    df["reason"] = np.select(
        [
            df["direction"] == "HIGH",
            df["direction"] == "LOW"
        ],
        [
            "Current activity is %.2f%% above the historical hour-of-day baseline",
            "Current activity is %.2f%% below the historical hour-of-day baseline"
        ],
        default="Activity is within the historical hour-of-day baseline range"
    )

    df.loc[df["direction"] == "HIGH", "reason"] = df.loc[df["direction"] == "HIGH", "anomaly_score"].map(
        lambda x: f"Current activity is {x:.2f}% above the historical hour-of-day baseline"
    )

    df.loc[df["direction"] == "LOW", "reason"] = df.loc[df["direction"] == "LOW", "anomaly_score"].map(
        lambda x: f"Current activity is {abs(x):.2f}% below the historical hour-of-day baseline"
    )

    logger.info("Anomaly scoring completed")
    logger.info("HIGH anomalies: %d", int((df["direction"] == "HIGH").sum()))
    logger.info("LOW anomalies: %d", int((df["direction"] == "LOW").sum()))
    logger.info("NORMAL observations: %d", int((df["direction"] == "NORMAL").sum()))

    return df


def build_anomaly_output(df):
    columns = [
        "grid_id",
        "timestamp",
        "hour",
        "total_activity",
        "hour_of_day_baseline",
        "deviation",
        "anomaly_score",
        "direction",
        "anomaly_flag",
        "reason"
    ]

    return df[columns].copy()


def prepare_np3_alerts(alerts):
    alerts = alerts.copy()
    alerts["timestamp"] = pd.to_datetime(alerts["timestamp"])

    np3 = alerts[["grid_id", "timestamp"]].drop_duplicates()
    np3["np3_alert"] = 1

    return np3


def prepare_ml3_predictions(predictions):
    predictions = predictions.copy()
    predictions["feature_timestamp"] = pd.to_datetime(predictions["feature_timestamp"])

    return predictions[["grid_id", "feature_timestamp", "model_prediction"]].drop_duplicates(
        ["grid_id", "feature_timestamp"]
    )


def compare_three_way(anomalies, alerts, predictions, logger):
    np3 = prepare_np3_alerts(alerts)
    ml3 = prepare_ml3_predictions(predictions)

    comparison = anomalies.merge(
        np3,
        on=["grid_id", "timestamp"],
        how="left"
    )

    comparison = comparison.merge(
        ml3,
        left_on=["grid_id", "timestamp"],
        right_on=["grid_id", "feature_timestamp"],
        how="left"
    )

    comparison["np3_alert"] = comparison["np3_alert"].fillna(0).astype(int)
    comparison["model_prediction"] = comparison["model_prediction"].fillna(0).astype(int)

    comparison["anomaly_alert"] = comparison["anomaly_flag"]

    comparison["all_agree"] = (
        (comparison["anomaly_alert"] == comparison["np3_alert"])
        & (comparison["anomaly_alert"] == comparison["model_prediction"])
    )

    comparison["disagreement_type"] = np.select(
        [
            (comparison["anomaly_alert"] == 1) & (comparison["np3_alert"] == 0) & (comparison["model_prediction"] == 0),
            (comparison["anomaly_alert"] == 0) & (comparison["np3_alert"] == 1) & (comparison["model_prediction"] == 0),
            (comparison["anomaly_alert"] == 0) & (comparison["np3_alert"] == 0) & (comparison["model_prediction"] == 1),
            (comparison["anomaly_alert"] == 1) & (comparison["np3_alert"] == 1) & (comparison["model_prediction"] == 0),
            (comparison["anomaly_alert"] == 1) & (comparison["np3_alert"] == 0) & (comparison["model_prediction"] == 1),
            (comparison["anomaly_alert"] == 0) & (comparison["np3_alert"] == 1) & (comparison["model_prediction"] == 1)
        ],
        [
            "ANOMALY_ONLY",
            "NP3_ONLY",
            "ML3_ONLY",
            "ANOMALY_NP3_ONLY",
            "ANOMALY_ML3_ONLY",
            "NP3_ML3_ONLY"
        ],
        default="ALL_AGREE"
    )

    logger.info("Three-way comparison completed")
    logger.info("All mechanisms agree: %d", int(comparison["all_agree"].sum()))
    logger.info("Any disagreement: %d", int((~comparison["all_agree"]).sum()))

    print("\nThree-Way Comparison")
    print(f"Total observations : {len(comparison)}")
    print(f"All agree          : {comparison['all_agree'].sum()}")
    print(f"Any disagreement   : {(~comparison['all_agree']).sum()}")

    print("\nDisagreement Summary")
    print(comparison["disagreement_type"].value_counts().to_string())

    return comparison


def inspect_cases(anomalies, logger):
    high = anomalies[anomalies["direction"] == "HIGH"].sort_values("anomaly_score", ascending=False).head(2)
    low = anomalies[anomalies["direction"] == "LOW"].sort_values("anomaly_score").head(2)

    print("\nHigh Anomaly Cases")
    print(high[["grid_id", "timestamp", "total_activity", "hour_of_day_baseline", "anomaly_score", "reason"]].to_string(index=False))

    print("\nLow Anomaly Cases")
    print(low[["grid_id", "timestamp", "total_activity", "hour_of_day_baseline", "anomaly_score", "reason"]].to_string(index=False))

    logger.info("Displayed two high and two low anomaly cases")


def write_output(anomalies, comparison, config, logger):
    output_path = Path(config["output_path"])
    output_path.mkdir(parents=True, exist_ok=True)

    anomalies.to_parquet(output_path / "network_anomaly_scores.parquet", index=False)
    comparison.to_parquet(output_path / "three_way_comparison.parquet", index=False)

    logger.info("Anomaly scores written to %s", output_path / "network_anomaly_scores.parquet")
    logger.info("Three-way comparison written to %s", output_path / "three_way_comparison.parquet")


def pipeline(config_path):
    config = load_config(config_path)
    logger = setup_logging(config["log_path"])

    try:
        logger.info("ML4 pipeline started")

        source, alerts, predictions = read_data(config, logger)
        validate_data(source, alerts, predictions, config)

        source = prepare_data(source, config, logger)
        source = calculate_hourly_baseline(source, config, logger)
        source = calculate_anomaly_score(source, config, logger)

        anomalies = build_anomaly_output(source)
        comparison = compare_three_way(anomalies, alerts, predictions, logger)

        inspect_cases(anomalies, logger)
        write_output(anomalies, comparison, config, logger)

        logger.info("ML4 pipeline completed successfully")

    except Exception:
        logger.exception("ML4 pipeline failed")
        raise


if __name__ == "__main__":
    pipeline("./config.yaml")