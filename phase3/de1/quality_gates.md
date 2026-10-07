# DE1 — Quality Gates

## Gate 1 — Landing Validation

An incoming daily CSV must pass the landing validation gate before being accepted into the raw zone.

### Required checks

* File exists.
* File can be opened.
* File contains the expected columns.
* Timestamp values are valid.
* Grid identifiers are valid.
* Country codes are valid where required.
* Activity measures have valid types.
* Duplicate records are detected.
* File contains expected daily data.

### Result

```text
PASS → Raw Zone
FAIL → Rejected / Quarantine
```

---

## Gate 2 — Raw Acceptance

The raw layer should contain only files that passed the landing validation.

The original source data should remain traceable.

Required metadata should identify:

* source file
* ingestion time
* processing run
* validation result

---

## Gate 3 — Spark Processing

Before publication to processed storage:

* cleaning completes successfully
* required columns exist
* rejected rows are counted
* null handling is recorded
* transformations complete
* aggregation completes
* expected grain is validated

---

## Gate 4 — Analytics Publication

Analytics can be published only when:

* processed data exists
* hourly grid records contain the expected grain
* `(grid_id, timestamp)` contains no duplicates
* expected dates are present
* required analytics columns exist
* country-code grain has been consolidated
* geometry is not duplicated into hourly fact-shaped records
* warehouse loading succeeds

---

## Gate 5 — Serve Readiness

FastAPI should expose analytics only when the relevant pipeline run has successfully completed.

The API should be able to identify:

* latest successful run
* latest failed run
* processing status
* processed input date
* row counts
* validation status

---

## Failure Principle

A failed quality gate must not be silently treated as success.

The pipeline should record the failure and prevent invalid data from being published downstream.
