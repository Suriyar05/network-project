# DE1 — Tool Responsibility Mapping

| Tool / Component  | Single Responsibility                                                     |
| ----------------- | ------------------------------------------------------------------------- |
| Landing Zone      | Receive incoming daily network activity files without transforming them   |
| Validation        | Determine whether incoming data satisfies the ingestion contract          |
| Raw Zone          | Preserve accepted source data in its original logical form                |
| Reference Zone    | Store static geographic reference data such as milano-grid.geojson        |
| Spark             | Perform cleaning, transformation, aggregation and geospatial enrichment   |
| Processed Parquet | Store reusable cleaned and transformed datasets                           |
| SQL Warehouse     | Store governed analytical tables for downstream querying                  |
| Airflow           | Orchestrate dependencies, schedules, retries and pipeline execution       |
| FastAPI           | Expose governed data and operational status through APIs                  |
| React             | Display analytics and operational information to users                    |
| ML                | Generate predictive scores or risk estimates from approved features       |
| Claude            | Explain available evidence and help users investigate operational signals |

## Separation of Responsibilities

### Spark does not orchestrate

Spark calculates and transforms data.

Airflow determines when Spark jobs run and what must happen before and after them.

### Airflow does not transform

Airflow should not contain the business transformation logic.

It calls the appropriate processing components.

### FastAPI does not perform ETL

FastAPI reads governed outputs and exposes them to consumers.

### React does not calculate analytics

React presents information returned by APIs.

### ML does not replace deterministic analytics

ML generates predictions using approved features.

Existing analytical rules such as hotspot calculations and alert rules remain separate.

### Claude does not become the source of truth

Claude interprets available evidence.

It does not silently recalculate the pipeline's underlying analytics.

## Three Architecture Questions

### Which component calculates data?

**Spark, SQL and ML**, each within its defined responsibility.

### Which component orchestrates the pipeline?

**Airflow.**

### Which component explains the available evidence?

**Claude.**
