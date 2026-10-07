
from pathlib import Path
import logging
import joblib
import pandas as pd
import yaml

from pathlib import Path
import yaml

CONFIG_PATH = Path("config.yaml")

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["ml6"]


CONFIG = load_config()

FEATURE_PATH = Path(CONFIG["feature_path"])
MODEL_PATH = Path(CONFIG["model_path"])
OUTPUT_PATH = Path(CONFIG["output_path"])
REPORT_PATH = Path(CONFIG["report_path"])
LOG_PATH = Path(CONFIG["log_path"])

FEATURE_COLUMNS = CONFIG["features"]

GRID_ID_COLUMN = CONFIG["columns"]["grid_id"]
TIMESTAMP_COLUMN = CONFIG["columns"]["timestamp"]
RISK_SCORE_COLUMN = CONFIG["columns"]["risk_score"]
RISK_LEVEL_COLUMN = CONFIG["columns"]["risk_level"]
MODEL_VERSION_COLUMN = CONFIG["columns"]["model_version"]

MODEL_VERSION = CONFIG["model"]["version"]
RISK_THRESHOLD = CONFIG["scoring"]["threshold"]
TOP_N = CONFIG["report"]["top_n"]

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def load_features():
    logger.info("Loading ML2 feature table")

    if not FEATURE_PATH.exists():
        raise FileNotFoundError(
            f"Feature table not found: {FEATURE_PATH}"
        )

    df = pd.read_parquet(FEATURE_PATH)

    if df.empty:
        raise ValueError("ML2 feature table is empty")

    required_columns = [
        GRID_ID_COLUMN,
        TIMESTAMP_COLUMN,
        *FEATURE_COLUMNS
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"ML2 feature schema mismatch. Missing columns: {missing_columns}"
        )

    df[TIMESTAMP_COLUMN] = pd.to_datetime(
        df[TIMESTAMP_COLUMN],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            GRID_ID_COLUMN,
            TIMESTAMP_COLUMN
        ]
    )

    logger.info(
        "Loaded %d feature records",
        len(df)
    )

    return df


def load_model():
    logger.info("Loading ML5 trained model")

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"ML5 model artifact not found: {MODEL_PATH}"
        )

    model = joblib.load(MODEL_PATH)

    logger.info(
        "Model loaded successfully: %s",
        MODEL_VERSION
    )

    return model


def validate_features(df):
    logger.info("Validating ML2 feature schema")

    null_counts = df[FEATURE_COLUMNS].isnull().sum()

    null_features = null_counts[
        null_counts > 0
    ].to_dict()

    if null_features:
        raise ValueError(
            f"Null values found in ML2 features: {null_features}"
        )

    logger.info(
        "Feature validation successful"
    )

    return df


def score_features(df, model):
    logger.info("Starting batch inference")

    X = df[FEATURE_COLUMNS]

    predictions = model.predict(X)

    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X)

        if probabilities.shape[1] == 2:
            risk_scores = probabilities[:, 1]
        else:
            risk_scores = probabilities.max(axis=1)
    else:
        risk_scores = predictions.astype(float)

    scores = pd.DataFrame({
        GRID_ID_COLUMN: df[GRID_ID_COLUMN].values,
        TIMESTAMP_COLUMN: df[TIMESTAMP_COLUMN].values,
        RISK_SCORE_COLUMN: risk_scores,
        RISK_LEVEL_COLUMN: [
            "high" if score >= RISK_THRESHOLD else "low"
            for score in risk_scores
        ],
        MODEL_VERSION_COLUMN: MODEL_VERSION
    })

    scores[RISK_SCORE_COLUMN] = scores[
        RISK_SCORE_COLUMN
    ].round(6)

    logger.info(
        "Scored %d grid/time records",
        len(scores)
    )

    return scores


def write_risk_scores(scores):
    OUTPUT_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_PATH /
        "network_risk_scores.parquet"
    )

    scores.to_parquet(
        output_file,
        index=False
    )

    logger.info(
        "Network risk scores written to %s",
        output_file
    )


def create_top_20_report(scores):
    report = (
        scores
        .sort_values(
            RISK_SCORE_COLUMN,
            ascending=False
        )
        .head(TOP_N)
        .reset_index(drop=True)
    )

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    report.to_parquet(
        REPORT_PATH,
        index=False
    )

    logger.info(
        "Top-%d operational attention report written to %s",
        TOP_N,
        REPORT_PATH
    )

    return report


def run():
    logger.info("ML6 batch scoring started")

    features = load_features()

    features = validate_features(
        features
    )

    model = load_model()

    scores = score_features(
        features,
        model
    )

    write_risk_scores(
        scores
    )

    report = create_top_20_report(
        scores
    )

    logger.info(
        "ML6 batch scoring completed successfully"
    )

    logger.info(
        "Total records: %d",
        len(scores)
    )

    logger.info(
        "High-risk records: %d",
        (scores[RISK_LEVEL_COLUMN] == "high").sum()
    )

    logger.info(
        "Top risk score: %.6f",
        scores[RISK_SCORE_COLUMN].max()
    )

    return scores, report


if __name__ == "__main__":
    run()