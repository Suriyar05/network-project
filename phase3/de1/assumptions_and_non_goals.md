# DE1 — Assumptions and Non-Goals

## Assumptions

1. Network activity arrives as daily CSV files.
2. Files may arrive one at a time.
3. Previously processed history remains available.
4. The system is primarily batch-oriented.
5. The Milan grid reference changes less frequently than daily activity.
6. Grid identifiers provide the relationship between activity and geographic reference data.
7. Spark is available for distributed-style transformation even though the training environment is local.
8. SQL provides the governed analytical access layer.
9. Airflow controls workflow execution.
10. FastAPI provides application-facing access.
11. React consumes APIs rather than reading warehouse files directly.
12. ML operates on approved analytical features.
13. Claude receives evidence from governed sources rather than directly manipulating raw data.

## Non-Goals

### Capacity metrics

**We do not have capacity, throughput or utilization data.**

Therefore the current pipeline cannot directly determine:

* network capacity utilization
* bandwidth utilization
* CPU utilization
* network throughput capacity
* saturation percentage

### Real-time streaming

The current design does not require Kafka or another real-time streaming platform.

Daily files are sufficient for the current business scenario.

### Automated congestion diagnosis

High activity does not automatically mean network congestion.

An alert identifies something worth investigating.

### Geometry duplication

Polygon geometry is not copied into every hourly fact record.

The static GeoJSON remains separately governed reference data.

### AI as an analytics engine

Claude is not responsible for calculating the authoritative analytical tables.

It explains evidence produced by the pipeline.

### UI as a processing engine

React does not perform ETL or warehouse calculations.

### Orchestrator as transformation engine

Airflow schedules and coordinates jobs but does not contain the core Spark transformation rules.
