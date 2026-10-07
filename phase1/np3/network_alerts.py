
import logging
import json
from pathlib import Path

import pandas as pd

from usage_processor import UsageProcessor

import sys
from pathlib import Path

PHASE1_ROOT = Path(__file__).resolve().parents[1]

if str(PHASE1_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE1_ROOT))

# ============================================================
# PROJECT PATHS
# ============================================================

# network_alerts.py is located at:
# project_root / phase 1 / np3 / network_alerts.py
#
# Therefore:
# parents[0] = np3
# parents[1] = phase 1
# parents[2] = project root
# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "landing"
    / "sms-call-internet-mi-2013-11-01.csv"
)

GRID_FILE = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "milano-grid.geojson"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "outputs"
    / "np3"
)

LOG_DIR = (
    PROJECT_ROOT
    / "logs"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)

# ============================================================
# LOGGING
# ============================================================

LOG_FILE = LOG_DIR / "np3.log"

logger = logging.getLogger("np3")
logger.setLevel(logging.INFO)

logger.propagate = False

if not logger.handlers:

    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8"
    )

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ============================================================
# NETWORK ALERT GENERATOR
# ============================================================

class NetworkAlertGenerator:

    # --------------------------------------------------------
    # Alert thresholds
    # --------------------------------------------------------

    HIGH_ACTIVITY_MULTIPLIER = 1.5
    SPIKE_MULTIPLIER = 1.5
    DROP_MULTIPLIER = 0.5

    def __init__(self, analytics_df):

        self.data = analytics_df.copy()

        self.alerts = None
        self.activity_floor = None

        self.input_rows = len(self.data)

    # ========================================================
    # 1. VALIDATE INPUT
    # ========================================================

    def validate_input(self):

        required_columns = [
            "timestamp",
            "grid_id",
            "date",
            "hour",
            "day_of_week",
            "total_activity"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in self.data.columns
        ]

        if missing_columns:

            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # ----------------------------------------------------
        # country_code must not exist after aggregation
        # ----------------------------------------------------

        if "country_code" in self.data.columns:

            raise ValueError(
                "country_code must not exist in "
                "the grid/hour analytics layer."
            )

        # ----------------------------------------------------
        # Check duplicate grid/hour records
        # ----------------------------------------------------

        duplicate_count = (
            self.data
            .duplicated(
                subset=["grid_id", "timestamp"]
            )
            .sum()
        )

        if duplicate_count > 0:

            raise ValueError(
                f"Found {duplicate_count} duplicate "
                "grid/hour records."
            )

        # ----------------------------------------------------
        # total_activity cannot be null
        # ----------------------------------------------------

        if self.data["total_activity"].isna().any():

            raise ValueError(
                "total_activity contains null values."
            )

        # ----------------------------------------------------
        # total_activity cannot be negative
        # ----------------------------------------------------

        if (self.data["total_activity"] < 0).any():

            raise ValueError(
                "Negative total_activity values detected."
            )

        # ----------------------------------------------------
        # Convert timestamp
        # ----------------------------------------------------

        self.data["timestamp"] = pd.to_datetime(
            self.data["timestamp"]
        )

        # ----------------------------------------------------
        # Check each grid has 24 hours
        # ----------------------------------------------------

        grid_hour_counts = (
            self.data
            .groupby("grid_id")["timestamp"]
            .nunique()
        )

        invalid_grids = grid_hour_counts[
            grid_hour_counts != 24
        ]

        if len(invalid_grids) > 0:

            raise ValueError(
                "Some grids do not contain exactly "
                "24 hourly observations."
            )

        logger.info(
            "Input validation: PASS | %d grid/hour records",
            len(self.data)
        )

        return self.data

    # ========================================================
    # 2. CALCULATE ACTIVITY FLOOR
    # ========================================================

    def calculate_activity_floor(self):

        daily_grid_totals = (
            self.data
            .groupby("grid_id")["total_activity"]
            .sum()
        )

        self.activity_floor = (
            daily_grid_totals.quantile(0.25)
        )

        logger.info(
            "Activity floor: 25th percentile = %.4f",
            self.activity_floor
        )

        return self.activity_floor

    # ========================================================
    # 3. BUILD WITHIN-DAY LEAVE-ONE-OUT BASELINE
    # ========================================================

    def build_baseline(self):

        self.data = (
            self.data
            .sort_values(
                ["grid_id", "timestamp"]
            )
            .reset_index(drop=True)
        )

        # ----------------------------------------------------
        # Previous hour activity
        # ----------------------------------------------------

        self.data["previous_hour_activity"] = (
            self.data
            .groupby("grid_id")["total_activity"]
            .shift(1)
        )

        # ----------------------------------------------------
        # Calculate leave-one-out baseline
        # ----------------------------------------------------

        baseline_values = []

        for grid_id, group in self.data.groupby(
            "grid_id",
            sort=False
        ):

            activities = (
                group["total_activity"]
                .tolist()
            )

            for position in range(
                len(activities)
            ):

                other_values = (
                    activities[:position]
                    + activities[position + 1:]
                )

                baseline = (
                    pd.Series(other_values)
                    .median()
                )

                baseline_values.append(
                    (
                        group.index[position],
                        baseline
                    )
                )

        # ----------------------------------------------------
        # Attach baseline
        # ----------------------------------------------------

        baseline_series = pd.Series(
            {
                index: baseline
                for index, baseline
                in baseline_values
            }
        )

        self.data["baseline_activity"] = (
            baseline_series
            .reindex(self.data.index)
        )

        logger.info(
            "Within-day leave-one-out baseline created "
            "using the other 23 hours."
        )

        return self.data

    # ========================================================
    # 4. VALIDATE BASELINE
    # ========================================================

    def validate_baseline(self):

        example_grid = (
            self.data["grid_id"]
            .iloc[0]
        )

        example_rows = (
            self.data[
                self.data["grid_id"] == example_grid
            ]
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        example_position = 0

        current_value = (
            example_rows
            .loc[
                example_position,
                "total_activity"
            ]
        )

        other_values = (
            example_rows["total_activity"]
            .drop(index=example_position)
        )

        calculated_baseline = (
            other_values.median()
        )

        stored_baseline = (
            example_rows
            .loc[
                example_position,
                "baseline_activity"
            ]
        )

        assert len(other_values) == 23

        assert (
            abs(
                calculated_baseline
                - stored_baseline
            ) < 0.000001
        )

        logger.info(
            "Baseline validation: PASS | "
            "Grid=%s | Timestamp=%s | "
            "Current=%.4f | Baseline=%.4f",
            example_grid,
            example_rows.loc[
                example_position,
                "timestamp"
            ],
            current_value,
            stored_baseline
        )

    # ========================================================
    # 5. GENERATE ALERTS
    # ========================================================

    def generate_alerts(self):

        alert_records = []

        # ----------------------------------------------------
        # Daily total for every grid
        # ----------------------------------------------------

        daily_totals = (
            self.data
            .groupby("grid_id")["total_activity"]
            .sum()
        )

        # ----------------------------------------------------
        # Evaluate every grid/hour
        # ----------------------------------------------------

        for _, row in self.data.iterrows():

            grid_id = row["grid_id"]

            timestamp = row["timestamp"]

            current_activity = (
                row["total_activity"]
            )

            baseline_activity = (
                row["baseline_activity"]
            )

            previous_activity = (
                row["previous_hour_activity"]
            )

            daily_total = (
                daily_totals.loc[grid_id]
            )

            # ------------------------------------------------
            # Activity floor
            # ------------------------------------------------

            if daily_total < self.activity_floor:

                continue

            # ------------------------------------------------
            # Cannot evaluate without baseline
            # ------------------------------------------------

            if pd.isna(baseline_activity):

                continue

            # =================================================
            # RULE 1: HIGH_ACTIVITY
            # =================================================

            if (
                baseline_activity > 0
                and current_activity
                >= self.HIGH_ACTIVITY_MULTIPLIER
                * baseline_activity
            ):

                alert_records.append({

                    "grid_id": grid_id,

                    "timestamp": timestamp,

                    "alert_type": "HIGH_ACTIVITY",

                    "current_activity":
                        current_activity,

                    "baseline_activity":
                        baseline_activity,

                    "reason": (
                        f"HIGH_ACTIVITY: current activity "
                        f"{current_activity:.4f} is at least "
                        f"50% above the within-day baseline "
                        f"{baseline_activity:.4f}."
                    )
                })

            # =================================================
            # RULE 2: ACTIVITY_DROP
            # =================================================

            if (
                baseline_activity > 0
                and current_activity
                <= self.DROP_MULTIPLIER
                * baseline_activity
            ):

                alert_records.append({

                    "grid_id": grid_id,

                    "timestamp": timestamp,

                    "alert_type": "ACTIVITY_DROP",

                    "current_activity":
                        current_activity,

                    "baseline_activity":
                        baseline_activity,

                    "reason": (
                        f"ACTIVITY_DROP: current activity "
                        f"{current_activity:.4f} is at or below "
                        f"50% of the within-day baseline "
                        f"{baseline_activity:.4f}."
                    )
                })

            # =================================================
            # RULE 3: ACTIVITY_SPIKE
            # =================================================

            if (
                pd.notna(previous_activity)
                and previous_activity > 0
                and current_activity
                >= self.SPIKE_MULTIPLIER
                * previous_activity
            ):

                alert_records.append({

                    "grid_id": grid_id,

                    "timestamp": timestamp,

                    "alert_type": "ACTIVITY_SPIKE",

                    "current_activity":
                        current_activity,

                    "baseline_activity":
                        baseline_activity,

                    "reason": (
                        f"ACTIVITY_SPIKE: current activity "
                        f"{current_activity:.4f} increased by "
                        f"at least 50% from the preceding hour; "
                        f"within-day baseline is "
                        f"{baseline_activity:.4f}."
                    )
                })

        # ----------------------------------------------------
        # Create DataFrame
        # ----------------------------------------------------

        self.alerts = pd.DataFrame(
            alert_records,
            columns=[
                "grid_id",
                "timestamp",
                "alert_type",
                "current_activity",
                "baseline_activity",
                "reason"
            ]
        )

        logger.info(
            "Alerts generated: %d",
            len(self.alerts)
        )

        return self.alerts

    # ========================================================
    # 6. VALIDATE ALERT REASONS
    # ========================================================

    def validate_reasons(self):

        if self.alerts.empty:

            logger.info(
                "Alert reason validation: PASS "
                "(no alerts generated)"
            )

            return

        assert (
            self.alerts["reason"]
            .notna()
            .all()
        )

        for _, row in self.alerts.iterrows():

            reason = row["reason"]

            assert (
                row["alert_type"]
                in reason
            )

            assert (
                f"{row['current_activity']:.4f}"
                in reason
            )

            assert (
                f"{row['baseline_activity']:.4f}"
                in reason
            )

        logger.info(
            "Alert reason validation: PASS"
        )

    # ========================================================
    # 7. VALIDATE ALERT VOLUME
    # ========================================================

    def validate_alert_volume(self):

        total_grid_hours = len(
            self.data
        )

        alerted_grid_hours = (
            self.alerts[
                [
                    "grid_id",
                    "timestamp"
                ]
            ]
            .drop_duplicates()
            .shape[0]
        )

        proportion = (
            alerted_grid_hours
            / total_grid_hours
        )

        logger.info(
            "Alert volume: %d / %d grid-hours "
            "(%.4f%%)",
            alerted_grid_hours,
            total_grid_hours,
            proportion * 100
        )

        assert proportion < 0.25, (
            "Alert volume exceeds 25%; "
            "thresholds require review."
        )

        logger.info(
            "Alert volume validation: PASS"
        )

    # ========================================================
    # 8. VALIDATE MILAN GRID IDs
    # ========================================================

    def validate_milan_grid_ids(self):

        if not GRID_FILE.exists():

            raise FileNotFoundError(
                f"Milan grid file not found: "
                f"{GRID_FILE}"
            )

        with open(
            GRID_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            geojson = json.load(file)

        features = geojson.get(
            "features",
            []
        )

        if not features:

            raise ValueError(
                "Milan GeoJSON contains no features."
            )

        geojson_grid_ids = set()

        for feature in features:

            properties = feature.get(
                "properties",
                {}
            )

            if "cellId" in properties:

                geojson_grid_ids.add(
                    int(properties["cellId"])
                )

        if not geojson_grid_ids:

            raise ValueError(
                "Could not find cellId values "
                "in milano-grid.geojson."
            )

        alert_grid_ids = set(
            self.alerts["grid_id"]
            .astype(int)
            .unique()
        )

        invalid_grid_ids = (
            alert_grid_ids
            - geojson_grid_ids
        )

        if invalid_grid_ids:

            raise ValueError(
                "Alert grid IDs not found in "
                "Milan grid: "
                f"{sorted(invalid_grid_ids)}"
            )

        logger.info(
            "Milan grid validation: PASS | "
            "%d alert grid IDs verified",
            len(alert_grid_ids)
        )

    # ========================================================
    # 9. EXPORT ALERTS
    # ========================================================

    def export_alerts(self):

        output_file = (
            OUTPUT_DIR
            / "network_alerts.csv"
        )

        self.alerts.to_csv(
            output_file,
            index=False
        )

        reloaded = pd.read_csv(
            output_file
        )

        assert (
            len(reloaded)
            == len(self.alerts)
        )

        assert (
            list(reloaded.columns)
            == list(self.alerts.columns)
        )

        logger.info(
            "Alert file validation: PASS | %s",
            output_file
        )

        return output_file

    # ========================================================
    # 10. OPERATIONAL SUMMARY
    # ========================================================

    def operational_summary(self):

        logger.info(
            "========== OPERATIONAL SUMMARY =========="
        )

        alerts_by_type = (
            self.alerts["alert_type"]
            .value_counts()
        )

        logger.info(
            "Alerts by type:\n%s",
            alerts_by_type.to_string()
        )

        top_grids = (
            self.alerts
            .groupby("grid_id")
            .size()
            .sort_values(
                ascending=False
            )
            .head(10)
        )

        logger.info(
            "Top 10 grids by alert count:\n%s",
            top_grids.to_string()
        )

        total_grid_hours = len(
            self.data
        )

        alerted_grid_hours = (
            self.alerts[
                [
                    "grid_id",
                    "timestamp"
                ]
            ]
            .drop_duplicates()
            .shape[0]
        )

        proportion = (
            alerted_grid_hours
            / total_grid_hours
        )

        logger.info(
            "Alerted grid-hours: %d / %d "
            "(%.4f%%)",
            alerted_grid_hours,
            total_grid_hours,
            proportion * 100
        )

        logger.info(
            "Alerts represent anomalies for "
            "investigation, not confirmed "
            "network congestion."
        )

    # ========================================================
    # 11. LIMITATION STATEMENT
    # ========================================================

    def write_limitation_statement(self):

        limitation_file = (
            OUTPUT_DIR
            / "np3_limitations.txt"
        )

        statement = """
NP3 RULE-BASED ALERT LIMITATIONS

The NP3 alert layer identifies unusual activity relative to
each grid's own within-day activity pattern. An alert is a
request for investigation and is not a diagnosis of network
congestion.

The baseline uses only one day of data. Therefore, it cannot
distinguish a genuine network problem from a normal recurring
pattern that happens to occur at a particular hour.

HOUR-OF-DAY BLIND SPOT

Because only one day of data is available, the system cannot
learn the normal activity level for a particular hour of day
across multiple days.

For example, it cannot determine whether 18:00 is normally a
high-activity period for a particular grid because there are
no historical 18:00 observations from other days.

The within-day baseline also cannot distinguish whether an
unusual activity level is caused by network congestion,
legitimate user behaviour, a public event, maintenance,
measurement behaviour, or another operational cause.

The activity floor removes very-low-activity grids from alert
evaluation. This reduces noisy ratio-based alerts but may also
hide genuine anomalies in those low-activity grids.

The HIGH_ACTIVITY, ACTIVITY_DROP and ACTIVITY_SPIKE rules are
transparent business rules rather than learned statistical or
machine-learning models.

Before an alert could be described as network congestion,
additional evidence would be required, including multiple
days or weeks of historical grid/hour activity, network
performance indicators such as latency, packet loss,
throughput and utilisation, service-impact indicators,
outage and maintenance information, and preferably independent
operational evidence showing that users or network services
were affected.
""".strip()

        with open(
            limitation_file,
            "w",
            encoding="utf-8"
        ) as file:

            file.write(statement)

        logger.info(
            "Limitation statement written: %s",
            limitation_file
        )

        return limitation_file


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    logger.info(
        "========== NP3 STARTED =========="
    )

    # ========================================================
    # RUN NP2
    # ========================================================

    processor = UsageProcessor(
       str(INPUT_FILE)
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()

    analytics_df = (
        processor.analytics_df
    )

    logger.info(
        "NP2 grid/hour output received: %d records",
        len(analytics_df)
    )

    # ========================================================
    # CREATE ALERT GENERATOR
    # ========================================================

    generator = NetworkAlertGenerator(
        analytics_df
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    generator.validate_input()

    # ========================================================
    # ACTIVITY FLOOR
    # ========================================================

    generator.calculate_activity_floor()

    # ========================================================
    # BASELINE
    # ========================================================

    generator.build_baseline()

    generator.validate_baseline()

    # ========================================================
    # ALERTS
    # ========================================================

    generator.generate_alerts()

    # ========================================================
    # VALIDATIONS
    # ========================================================

    generator.validate_reasons()

    generator.validate_alert_volume()

    generator.validate_milan_grid_ids()

    # ========================================================
    # EXPORT
    # ========================================================

    generator.export_alerts()

    # ========================================================
    # SUMMARY
    # ========================================================

    generator.operational_summary()

    # ========================================================
    # LIMITATIONS
    # ========================================================

    generator.write_limitation_statement()

    logger.info(
        "========== NP3 COMPLETED =========="
    )
