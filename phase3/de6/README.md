# DE6 — Warehouse Modelling for Network Analytics

## Purpose

DE6 converts the Spark analytics output into a relational warehouse
designed for repeatable network analytics.

The warehouse uses a minimal star schema.

---

## Architecture

SP3 Spark analytics

        ↓

hourly_grid_summary

        ↓

DE6 Warehouse

        ├── dim_time
        │
        ├── dim_grid
        │
        └── fact_network_activity

        ↓

SQL analytics

        ↓

FastAPI API1

---

## Star Schema

### fact_network_activity

The fact table contains:

- time_key
- grid_key
- sms_in
- sms_out
- call_in
- call_out
- total_sms
- total_calls
- internet_activity
- total_activity
- internet_share

The fact table contains NO geometry.

---

## dim_time

Contains:

- time_key
- timestamp
- date
- hour
- day_of_week

---

## dim_grid

Contains:

- grid_key
- grid_id
- centroid_latitude
- centroid_longitude
- geometry_reference

The full Polygon geometry is not stored in the fact table.

The geometry reference points back to:

milano-grid.geojson

---

## Data Sources

SP3 source:

phase 2/output/sp3/hourly_grid_summary/

Static grid reference:

phase 2/data/reference/milano-grid.geojson

---

## Warehouse

SQLite database:

warehouse/network_analytics.db

---

## Validation

DE6 validates:

1. Fact row count equals SP3 row count.
2. dim_grid count equals distinct observed grid IDs.
3. dim_grid has no duplicate grid IDs.
4. fact_network_activity has no geometry.
5. Fact grain is time_key + grid_key.
6. Foreign keys are valid.
7. Spark total_activity equals SQL total_activity.
8. Top grid agrees with the Spark equivalent.

---

## Analytics Queries

The warehouse supports:

- top grids
- hourly trends
- internet-heavy windows
- grid drill-down
- ML feature extraction

---

## Indexes

Indexes are created on:

- fact_network_activity.grid_key
- fact_network_activity.time_key
- dim_grid.grid_id
- dim_time.date
- dim_time.hour

These support common joins and filtering operations.