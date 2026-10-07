# DE1 — Telecom Data Architecture

## Purpose

This design defines the production architecture for the Network Operations and Predictive Intelligence pipeline.

The architecture follows:

**Landing → Validate → Raw → Spark → Processed → Analytics Warehouse → Serve → Monitor**

The static Milan grid GeoJSON is maintained separately as reference data.

## Data Sources

### Daily network activity

Format:

* CSV
* One daily file at a time
* Historical files accumulate after successful processing

Example:

```text
sms-call-internet-mi-2013-11-01.csv
sms-call-internet-mi-2013-11-02.csv
...
```

### Static geographic reference

Format:

```text
milano-grid.geojson
```

This contains the geographic grid/polygon information used to enrich network analytics.

It is **reference data**, not a daily activity input.

## Main Pipeline

```text
Daily CSV
   ↓
Landing
   ↓
Validation
   ↓
Raw
   ↓
Spark
   ↓
Processed Parquet
   ↓
Analytics Warehouse
   ↓
FastAPI
   ↓
React

Static GeoJSON
   ↓
Reference Zone
   ↓
Spark / SQL enrichment
```

## Core Analytics

The pipeline produces:

* hourly_grid_summary
* daily_grid_summary
* hotspots
* network alerts
* risk tables

## Component Responsibilities

| Component     | Responsibility                                                  |
| ------------- | --------------------------------------------------------------- |
| Landing       | Receive incoming daily files                                    |
| Validation    | Check whether an incoming file satisfies the ingestion contract |
| Raw           | Preserve accepted source data                                   |
| Spark         | Clean, transform, aggregate and enrich data                     |
| Processed     | Store reusable cleaned data in Parquet                          |
| SQL Warehouse | Store governed analytical tables                                |
| Airflow       | Orchestrate pipeline execution and dependencies                 |
| FastAPI       | Serve governed analytics through APIs                           |
| React         | Present operational information to users                        |
| ML            | Produce predictive risk scores                                  |
| Claude        | Explain available evidence and assist investigation             |

## Architecture Principles

1. Raw data is preserved before transformation.
2. Reference data is separated from daily activity data.
3. Spark performs large-scale data transformation.
4. SQL provides governed analytical access.
5. Airflow orchestrates the pipeline.
6. FastAPI serves data to applications.
7. React is responsible for presentation.
8. ML produces predictions rather than replacing deterministic analytics.
9. Claude explains available evidence rather than calculating the source analytics.
10. Pipeline health is recorded independently from business analytics.

## Assumptions

* Daily activity files arrive one at a time.
* Historical processed data already exists.
* The training workload is batch-oriented.
* Milan grid geometry is relatively static.
* Grid IDs can be used to associate activity with geographic reference data.
* Spark is the primary transformation engine.
* SQL is the governed analytics layer.

## Non-Goals

The following are explicitly outside the current design:

* We do not have capacity, throughput or utilization data.
* We do not claim that activity alone proves network congestion.
* We do not treat GeoJSON as daily source data.
* We do not build a real-time streaming architecture.
* We do not make Claude the primary analytics engine.
* We do not make React responsible for data processing.
* We do not make Airflow responsible for business transformations.
* We do not make FastAPI responsible for ETL.
* We do not duplicate polygon geometry into every fact-shaped analytics record.

## Quality Gates

Before raw acceptance:

* File exists.
* File is readable.
* Required columns exist.
* Required data types are valid.
* Timestamp values are valid.
* Duplicate records are checked.
* Required identifiers are valid.
* Invalid rows are rejected or quarantined according to the ingestion contract.

Before analytics publication:

* Spark processing completed successfully.
* Required output tables exist.
* Expected schemas are present.
* Row counts are reasonable.
* Duplicate grid/hour records are absent.
* Required dates are present.
* No unwanted country-code grain remains in grid/hour analytics.
* Geometry is not duplicated into fact-shaped hourly analytics.
* Warehouse loads completed successfully.
* Pipeline health status is recorded.

## Pipeline Health

Pipeline health is recorded in a dedicated operational metadata area.

At minimum it records:

* pipeline run ID
* pipeline name
* start time
* end time
* status
* input file
* input row count
* rejected row count
* output row count
* validation status
* error message
* processing duration

This operational information is later exposed through the API layer.

## Key Design Question

**Which component calculates data?**

Spark, SQL and ML calculate their respective outputs.

**Which component orchestrates it?**

Airflow.

**Which component explains it?**

Claude explains evidence produced by the pipeline and other approved data sources.
