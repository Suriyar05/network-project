import logging
import sys
from pathlib import Path

import pandas as pd
import yaml
from tqdm import tqdm


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "ml2" not in config:
        raise KeyError("Missing 'ml2' configuration section")

    return config["ml2"]


def setup_logging(log_path):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )

    return logging.getLogger("ML2")


def read_input(config, logger):
    path = config["input_path"]

    logger.info("Reading input: %s", path)

    with tqdm(
        total=1,
        desc="Reading input",
        unit="file"
    ) as progress:
        df = pd.read_parquet(path)
        progress.update(1)

    logger.info("Input rows: %d", len(df))

    return df


def validate_input(df, config):
    columns = config["columns"]

    required = [
        columns["timestamp"],
        columns["grid_id"],
        columns["total_activity"],
        columns["internet_activity"]
    ]

    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df


def build_features(df, config, logger):
    columns = config["columns"]

    timestamp = columns["timestamp"]
    grid_id = columns["grid_id"]
    activity = columns["total_activity"]
    internet = columns["internet_activity"]

    recent_hours = config["feature"]["recent_window_hours"]
    baseline_hours = config["feature"]["baseline_window_hours"]
    minimum_history = config["feature"]["minimum_history_hours"]

    if recent_hours <= 0 or baseline_hours <= 0:
        raise ValueError("Window sizes must be greater than zero")

    if minimum_history < recent_hours + baseline_hours:
        raise ValueError("minimum_history_hours must cover both windows")

    df = df[
        [grid_id, timestamp, activity, internet]
    ].copy()

    df[timestamp] = pd.to_datetime(df[timestamp])

    df[activity] = pd.to_numeric(
        df[activity],
        errors="coerce"
    ).fillna(0.0)

    df[internet] = pd.to_numeric(
        df[internet],
        errors="coerce"
    ).fillna(0.0)

    df = (
        df
        .drop_duplicates([grid_id, timestamp])
        .sort_values([grid_id, timestamp])
    )

    groups = list(df.groupby(grid_id, sort=False))

    logger.info(
        "Processing %d grids",
        len(groups)
    )

    results = []

    with tqdm(
        total=len(groups),
        desc="Engineering features",
        unit="grid"
    ) as progress:

        for current_grid_id, group in groups:

            group = group.set_index(timestamp).sort_index()

            recent = group[activity].rolling(
                f"{recent_hours}h",
                closed="both"
            )

            baseline = group[activity].shift(
                recent_hours
            ).rolling(
                f"{baseline_hours}h",
                closed="both"
            )

            recent_avg = recent.mean()
            baseline_avg = baseline.mean()
            recent_peak = recent.max()
            recent_std = recent.std()

            active_hours = recent.apply(
                lambda x: (x > 0).sum(),
                raw=True
            )

            internet_sum = group[internet].rolling(
                f"{recent_hours}h",
                closed="both"
            ).sum()

            activity_sum = group[activity].rolling(
                f"{recent_hours}h",
                closed="both"
            ).sum()

            history_count = group[activity].rolling(
                f"{minimum_history}h",
                closed="both"
            ).count()

            result = pd.DataFrame(
                {
                    grid_id: current_grid_id,
                    "feature_timestamp": group.index,
                    "avg_activity": recent_avg.values,
                    "activity_growth": (
                        (recent_avg - baseline_avg) / baseline_avg
                    ).where(
                        baseline_avg != 0,
                        0.0
                    ).values,
                    "active_hours": active_hours.values.astype("int64"),
                    "peak_ratio": (
                        recent_peak / recent_avg
                    ).where(
                        recent_avg != 0,
                        0.0
                    ).values,
                    "variability": recent_std.fillna(0.0).values,
                    "internet_share": (
                        internet_sum / activity_sum
                    ).where(
                        activity_sum != 0,
                        0.0
                    ).values,
                    "_history_count": history_count.values
                }
            )

            result = result[
                result["_history_count"] >= minimum_history
            ]

            result = result.drop(
                columns=["_history_count"]
            )

            results.append(result)

            progress.update(1)

    if not results:
        raise ValueError("No features were generated")

    features = pd.concat(
        results,
        ignore_index=True
    )

    logger.info(
        "Feature engineering completed: %d rows",
        len(features)
    )

    return features


def write_output(df, config, logger):
    path = Path(config["output_path"])

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    logger.info(
        "Writing feature table: %s",
        path
    )

    output_format = config["output"]["format"]

    if output_format == "parquet":

        with tqdm(
            total=1,
            desc="Writing Parquet",
            unit="file"
        ) as progress:

            df.to_parquet(
                path,
                index=False
            )

            progress.update(1)

    else:
        raise ValueError(
            f"Unsupported output format: {output_format}"
        )

    logger.info(
        "Feature table written successfully: %d rows",
        len(df)
    )


def pipeline(config_path):
    config = load_config(config_path)

    logger = setup_logging(
        config["log_path"]
    )

    try:
        logger.info(
            "ML2 pipeline started"
        )

        with tqdm(
            total=4,
            desc="ML2 Pipeline",
            unit="stage"
        ) as pipeline_progress:

            df = read_input(
                config,
                logger
            )

            pipeline_progress.update(1)

            df = validate_input(
                df,
                config
            )

            pipeline_progress.update(1)

            features = build_features(
                df,
                config,
                logger
            )

            pipeline_progress.update(1)

            write_output(
                features,
                config,
                logger
            )

            pipeline_progress.update(1)

        logger.info(
            "ML2 pipeline completed successfully"
        )

    except Exception:
        logger.exception(
            "ML2 pipeline failed"
        )
        raise


if __name__ == "__main__":
    pipeline("./config.yaml")