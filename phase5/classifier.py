import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, classification_report
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import joblib

def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if "ml3" not in config:
        raise KeyError("Missing 'ml3' configuration section")
    return config["ml3"]


def setup_logging(log_path):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)], force=True)
    return logging.getLogger("ML3")


def read_data(config, logger):
    feature_path = config["feature_path"]
    label_path = config["label_source_path"]
    alert_path = config["np3_alert_path"]

    logger.info("Reading feature table: %s", feature_path)
    features = pd.read_parquet(feature_path)

    logger.info("Reading label source: %s", label_path)
    source = pd.read_parquet(label_path)

    logger.info("Reading NP3 alerts: %s", alert_path)
    alerts = pd.read_csv(alert_path)

    logger.info("Feature rows: %d", len(features))
    logger.info("Label source rows: %d", len(source))
    logger.info("NP3 alert rows: %d", len(alerts))

    return features, source, alerts


def validate_data(features, source, alerts, config):
    feature_columns = config["features"]
    label = config["label"]

    required_features = feature_columns + ["grid_id", "feature_timestamp"]
    required_source = [label["grid_id_column"], label["timestamp_column"], label["activity_column"]]
    required_alerts = ["grid_id", "timestamp"]

    missing_features = [column for column in required_features if column not in features.columns]
    missing_source = [column for column in required_source if column not in source.columns]
    missing_alerts = [column for column in required_alerts if column not in alerts.columns]

    if missing_features:
        raise ValueError(f"Missing feature columns: {missing_features}")

    if missing_source:
        raise ValueError(f"Missing label source columns: {missing_source}")

    if missing_alerts:
        raise ValueError(f"Missing alert columns: {missing_alerts}")

    return features, source, alerts


def create_labels(features, source, config, logger):
    label = config["label"]
    grid_id = label["grid_id_column"]
    timestamp = label["timestamp_column"]
    activity = label["activity_column"]
    horizon = label["horizon_hours"]

    source = source[[grid_id, timestamp, activity]].copy()
    source[timestamp] = pd.to_datetime(source[timestamp])
    source[activity] = pd.to_numeric(source[activity], errors="coerce")
    source = source.dropna(subset=[grid_id, timestamp, activity])
    source = source.drop_duplicates([grid_id, timestamp])
    source = source.sort_values([grid_id, timestamp])

    source["_future_timestamp"] = source[timestamp] - pd.Timedelta(hours=horizon)

    labels = source[[grid_id, "_future_timestamp", activity]].copy()
    labels["feature_timestamp"] = labels["_future_timestamp"]
    labels["future_activity"] = labels[activity]

    labels = labels[["grid_id", "feature_timestamp", "future_activity"]]

    result = features.merge(labels, on=["grid_id", "feature_timestamp"], how="inner")

    if result.empty:
        raise ValueError("No rows matched between feature table and future activity labels")

    logger.info("Labeled rows: %d", len(result))

    return result


def chronological_split(df, config, logger):
    ratio = config["split"]["train_ratio"]

    df = df.sort_values("feature_timestamp").reset_index(drop=True)
    timestamps = sorted(df["feature_timestamp"].dropna().unique())

    if len(timestamps) < 2:
        raise ValueError("Not enough timestamps for chronological split")

    split_index = max(1, int(len(timestamps) * ratio))

    if split_index >= len(timestamps):
        split_index = len(timestamps) - 1

    split_timestamp = timestamps[split_index]

    train = df[df["feature_timestamp"] < split_timestamp].copy()
    test = df[df["feature_timestamp"] >= split_timestamp].copy()

    if train.empty or test.empty:
        raise ValueError("Chronological split produced an empty train or test set")

    logger.info("Split timestamp: %s", split_timestamp)
    logger.info("Train rows: %d", len(train))
    logger.info("Test rows: %d", len(test))
    logger.info("Train earliest: %s", train["feature_timestamp"].min())
    logger.info("Train latest: %s", train["feature_timestamp"].max())
    logger.info("Test earliest: %s", test["feature_timestamp"].min())
    logger.info("Test latest: %s", test["feature_timestamp"].max())

    return train, test


def apply_training_threshold(train, test, config, logger):
    percentile = config["label"]["positive_percentile"]

    thresholds = train.groupby("grid_id")["future_activity"].quantile(percentile / 100)
    global_threshold = train["future_activity"].quantile(percentile / 100)

    train = train.copy()
    test = test.copy()

    train["_threshold"] = train["grid_id"].map(thresholds).fillna(global_threshold)
    test["_threshold"] = test["grid_id"].map(thresholds).fillna(global_threshold)

    train["risk_label"] = (train["future_activity"] >= train["_threshold"]).astype(int)
    test["risk_label"] = (test["future_activity"] >= test["_threshold"]).astype(int)

    logger.info("Training threshold percentile: P%s", percentile)
    logger.info("Train positive labels: %d", int(train["risk_label"].sum()))
    logger.info("Train negative labels: %d", int((train["risk_label"] == 0).sum()))
    logger.info("Test positive labels: %d", int(test["risk_label"].sum()))
    logger.info("Test negative labels: %d", int((test["risk_label"] == 0).sum()))

    return train, test


def train_model(train, config, logger):
    feature_columns = config["features"]

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("classifier", LogisticRegression(max_iter=config["model"]["max_iter"]))
    ])

    x_train = train[feature_columns].replace([np.inf, -np.inf], np.nan)
    y_train = train["risk_label"]

    if y_train.nunique() < 2:
        raise ValueError("Training data contains only one risk class")

    model.fit(x_train, y_train)

    logger.info("Logistic Regression training completed")

    return model


def evaluate_model(model, train, test, config, logger):
    feature_columns = config["features"]

    x_train = train[feature_columns].replace([np.inf, -np.inf], np.nan)
    x_test = test[feature_columns].replace([np.inf, -np.inf], np.nan)

    y_train = train["risk_label"]
    y_test = test["risk_label"]

    train_predictions = model.predict(x_train)
    test_predictions = model.predict(x_test)

    train_base_rate = y_train.mean()
    test_base_rate = y_test.mean()

    accuracy = accuracy_score(y_test, test_predictions)
    precision = precision_score(y_test, test_predictions, zero_division=0)
    recall = recall_score(y_test, test_predictions, zero_division=0)

    logger.info("Train base rate: %.4f", train_base_rate)
    logger.info("Test base rate: %.4f", test_base_rate)
    logger.info("Test accuracy: %.4f", accuracy)
    logger.info("Test precision: %.4f", precision)
    logger.info("Test recall: %.4f", recall)

    print("\nML3 Evaluation")
    print(f"Train base rate : {train_base_rate:.4f}")
    print(f"Test base rate  : {test_base_rate:.4f}")
    print(f"Accuracy        : {accuracy:.4f}")
    print(f"Precision       : {precision:.4f}")
    print(f"Recall          : {recall:.4f}")

    print("\nClass Balance")
    print("Train:")
    print(y_train.value_counts().sort_index())
    print("\nTest:")
    print(y_test.value_counts().sort_index())

    print("\nClassification Report")
    print(classification_report(y_test, test_predictions, zero_division=0))

    test = test.copy()
    test["model_prediction"] = test_predictions
    test["model_probability"] = model.predict_proba(x_test)[:, 1]

    return test


def inspect_model(model, config, logger):
    feature_columns = config["features"]
    classifier = model.named_steps["classifier"]

    coefficients = pd.DataFrame({
        "feature": feature_columns,
        "coefficient": classifier.coef_[0]
    })

    coefficients["absolute_coefficient"] = coefficients["coefficient"].abs()
    coefficients = coefficients.sort_values("absolute_coefficient", ascending=False)

    logger.info("Model coefficients:")

    for _, row in coefficients.iterrows():
        logger.info("%s: %.6f", row["feature"], row["coefficient"])

    print("\nModel Coefficients")
    print(coefficients[["feature", "coefficient"]].to_string(index=False))

    return coefficients


def compare_np3(predictions, alerts, logger):
    if alerts is None or alerts.empty:
        logger.warning("NP3 alerts unavailable. Comparison skipped.")
        return None

    predictions = predictions.copy()
    alerts = alerts.copy()

    predictions["feature_timestamp"] = pd.to_datetime(predictions["feature_timestamp"])
    alerts["timestamp"] = pd.to_datetime(alerts["timestamp"])

    np3 = alerts[["grid_id", "timestamp"]].drop_duplicates()
    np3["np3_alert"] = 1

    comparison = predictions.merge(
        np3,
        left_on=["grid_id", "feature_timestamp"],
        right_on=["grid_id", "timestamp"],
        how="left"
    )

    comparison["np3_alert"] = comparison["np3_alert"].fillna(0).astype(int)

    comparison["agreement"] = comparison["model_prediction"] == comparison["np3_alert"]

    model_alert_np3_no = ((comparison["model_prediction"] == 1) & (comparison["np3_alert"] == 0)).sum()
    model_no_np3_alert = ((comparison["model_prediction"] == 0) & (comparison["np3_alert"] == 1)).sum()
    both_alert = ((comparison["model_prediction"] == 1) & (comparison["np3_alert"] == 1)).sum()
    both_no_alert = ((comparison["model_prediction"] == 0) & (comparison["np3_alert"] == 0)).sum()

    agreement = comparison["agreement"].sum()
    agreement_rate = comparison["agreement"].mean()

    logger.info("NP3/model agreement: %d", agreement)
    logger.info("NP3/model agreement rate: %.4f", agreement_rate)
    logger.info("Both alert: %d", both_alert)
    logger.info("Both no alert: %d", both_no_alert)
    logger.info("Model alert / NP3 no alert: %d", model_alert_np3_no)
    logger.info("Model no alert / NP3 alert: %d", model_no_np3_alert)

    print("\nModel vs NP3 Comparison")
    print(f"Agreement                 : {agreement}")
    print(f"Agreement rate            : {agreement_rate:.4f}")
    print(f"Both alert                : {both_alert}")
    print(f"Both no alert             : {both_no_alert}")
    print(f"Model alert / NP3 no alert: {model_alert_np3_no}")
    print(f"Model no alert / NP3 alert: {model_no_np3_alert}")

    return comparison


def write_output(test, coefficients, comparison, model, config, logger):
    path = Path(config["output_path"])
    path.mkdir(parents=True, exist_ok=True)

    test.to_parquet(path / "predictions.parquet", index=False)
    coefficients.to_parquet(path / "model_coefficients.parquet", index=False)
    joblib.dump(model, path / "risk_classifier.joblib")

    if comparison is not None:
        comparison.to_parquet(path / "np3_comparison.parquet", index=False)

    logger.info("Predictions written: %s", path / "predictions.parquet")
    logger.info("Coefficients written: %s", path / "model_coefficients.parquet")
    logger.info("Model written: %s", path / "risk_classifier.joblib")

    print(f"\nModel saved: {path / 'risk_classifier.joblib'}")

def pipeline(config_path):
    config = load_config(config_path)
    logger = setup_logging(config["log_path"])

    try:
        logger.info("ML3 pipeline started")

        features, source, alerts = read_data(config, logger)
        features, source, alerts = validate_data(features, source, alerts, config)
        data = create_labels(features, source, config, logger)
        train, test = chronological_split(data, config, logger)
        train, test = apply_training_threshold(train, test, config, logger)
        model = train_model(train, config, logger)
        test = evaluate_model(model, train, test, config, logger)
        coefficients = inspect_model(model, config, logger)
        comparison = compare_np3(test, alerts, logger)
        write_output(test, coefficients, comparison, model, config, logger)

        logger.info("ML3 pipeline completed successfully")

    except Exception:
        logger.exception("ML3 pipeline failed")
        raise


if __name__ == "__main__":
    pipeline("./config.yaml")