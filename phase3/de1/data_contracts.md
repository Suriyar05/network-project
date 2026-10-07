# DE1 — Data Contracts

## 1. Daily Activity Contract

### Input

```text
CSV
```

### Logical fields

* timestamp
* grid_id / CellID
* country_code
* sms activity
* call activity
* internet activity

### Expected behaviour

One daily source file represents one day's network activity.

---

# 2. Static Reference Contract

### Input

```text
milano-grid.geojson
```

### Purpose

Provides geographic grid information for enrichment and map rendering.

### Important rule

The GeoJSON is static reference data.

It must not be treated as a daily network activity file.

---

# 3. Processed Activity Contract

### Format

```text
Parquet
```

### Purpose

Reusable cleaned activity data.

### Characteristics

* columnar storage
* typed columns
* reusable by downstream Spark/SQL processes
* partitioning may be based on date

---

# 4. Hourly Grid Summary Contract

### Grain

```text
one row per grid_id + timestamp
```

### Expected fields

* timestamp
* grid_id
* date
* hour
* day_of_week
* sms_in
* sms_out
* call_in
* call_out
* total_sms
* total_calls
* internet_activity
* total_activity
* internet_share

### Important rule

`country_code` must not remain in the final grid/hour analytics grain.

### Geometry rule

Full polygon geometry must not be duplicated into every hourly analytics row.

The geometry remains in the static reference layer.

---

# 5. Daily Grid Summary

### Grain

```text
one row per grid_id + date
```

### Purpose

Daily operational traffic analysis.

---

# 6. Hotspot Table

### Grain

A ranked analytical representation of high-activity grids.

### Purpose

Identify grids with unusually high or significant network activity.

---

# 7. Alert Table

### Purpose

Store deterministic operational alerts generated from approved activity rules.

An alert is an investigation signal, not proof of network congestion.

---

# 8. Risk Table

### Purpose

Store ML-generated risk predictions and supporting model information.

Example fields may include:

* grid_id
* timestamp
* risk_score
* risk_level
* model_version
* prediction_time

---

# 9. Pipeline Health Contract

Pipeline health is operational metadata rather than business analytics.

### Required fields

```text
run_id
pipeline_name
start_time
end_time
status
input_file
input_rows
rejected_rows
output_rows
validation_status
error_message
duration_seconds
```

This data becomes available to API consumers later.
