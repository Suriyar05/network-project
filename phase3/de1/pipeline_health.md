# DE1 — Pipeline Health Design

## Purpose

Pipeline health records whether the data pipeline successfully processed an input batch.

It is separate from business analytics.

A successful business query does not necessarily mean the latest pipeline run succeeded, so operational health must be recorded explicitly.

## Health Record

Each pipeline run should produce one operational record containing:

| Field             | Purpose                             |
| ----------------- | ----------------------------------- |
| run_id            | Unique identifier for the execution |
| pipeline_name     | Name of the pipeline                |
| start_time        | Processing start                    |
| end_time          | Processing completion               |
| status            | SUCCESS / FAILED                    |
| input_file        | File processed                      |
| input_rows        | Number of input rows                |
| rejected_rows     | Number of rejected records          |
| output_rows       | Number of published records         |
| validation_status | Quality gate result                 |
| error_message     | Failure details                     |
| duration_seconds  | Processing duration                 |

## Example

```text
run_id: RUN-2026-08-30-001
pipeline_name: telecom_network_pipeline
input_file: sms-call-internet-mi-2013-11-08.csv
input_rows: 2199540
rejected_rows: 0
output_rows: 23999
validation_status: PASS
status: SUCCESS
duration_seconds: 142
```

## Failure Example

```text
run_id: RUN-2026-08-30-002
pipeline_name: telecom_network_pipeline
input_file: sms-call-internet-mi-2013-11-09.csv
input_rows: 0
rejected_rows: 0
output_rows: 0
validation_status: FAIL
status: FAILED
error_message: Required column timestamp is missing
```

## Where It Lives

The pipeline health record should be stored in an operational metadata table or equivalent durable store.

It should not be mixed into the hourly network activity fact table.

## API6 Connection

The later API layer can expose endpoints such as:

```text
GET /health
GET /pipeline/status
GET /pipeline/runs
GET /pipeline/runs/{run_id}
```

The API can therefore tell React and operational users whether the data they are viewing came from a successful pipeline run.

## Design Principle

Business data answers:

> What happened in the network?

Pipeline health answers:

> Did our pipeline successfully produce trustworthy data?

These are different questions and should remain separate.
