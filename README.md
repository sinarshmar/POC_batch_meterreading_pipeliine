# POC Summary
The pipeline reads JSON files representing smart meter data, joins them with a SQLite database of static entities, and outputs final tables suitable for reporting and dashboarding. Although automation and orchestration are not implemented, the solution is built to be easily schedulable using tools like Airflow or cron.

---
# Design Philosophy

- **Simplicity first**: Built in modular Python files for clarity and reusability.
- **Delta-friendly design**: Optimised for incremental writes, deduplication, and MERGE support.
- **Ready for orchestration**: Though currently batch-triggered, it's DAG-ready with clear boundaries for task separation.

---

# Pipeline Structure

The pipeline follows a **three-layer architecture** to ensure scalability, reusability, and maintainability:

- **Raw Layer**:  
  - Loads raw JSON smart meter readings and SQLite tables without transformation.
- **Staging Layer**:  
  - Cleans and enriches data: timestamps standardised, joins with agreements and products.
- **Presentation Layer**:  
  - Final tables written in Delta format, suitable for analysis and reporting.

Modular code files are organised into:
- `pipeline/load_data.py` → ingestion
- `pipeline/transform_staging.py` → cleaning & joins
- `pipeline/generate_outputs.py` → final aggregations
- - `pipeline/utils.py` → helpers

---
# How to Run

### Prerequisites
- Python 3.8+
- Apache Spark with Delta Lake
- `pyspark`, `delta-spark`, `sqlite3`
- Java installed (for SQLite JDBC)

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run the Pipeline
```bash
spark-submit --jars "$(pwd)/Kraken_assignment/jars/sqlite-jdbc-3.50.2.0.jar" main.py
```

### Input Assumptions
- JSON files: `data/readings/*.json`
- SQLite DB: `data/case_study.db`
- All the fresh datafor the day is available by 8 am.
- All smart meter readings are stored as individual JSON files under a folder like `data/readings/`.
- Each file follows a custom format with `"columns"` and `"data"` keys.
- 🚨🚨🚨 **In cloud deployment**, the pipeline will be modified to ingest **only the most recent files** (e.g., from the last 24 hours or with specific naming logic) to improve efficiency.


---

# Outputs

All outputs are saved in Delta format under `data_lake/presentation/`:

| Table Name                               | Description |
|------------------------------------------|-------------|
| `active_agreements`                      | Agreements active as of Jan 1, 2021 |
| `total_consumption_half_hour`            | Total kWh and meterpoint count per half hour |
| `consumption_per_product_per_day`        | Total kWh and meterpoint count per product per day |
| `gold_aggregate_per_half_hour`           | Same as above (used in dashboards) |
| `gold_avg_half_hour_by_product`          | Average kWh per product per half hour |

---

# Robust Pipeline Design Features

### Deduplication

Smart meter data may contain duplicate readings for the same meterpoint and interval due to retries or ingestion overlaps. This pipeline automatically removes such duplicates using:
```python
.dropDuplicates(["interval_start", "meterpoint_id"])
```
This ensures that downstream metrics (e.g., total kWh) are accurate and not inflated.

---

### Safe Re-runs

The pipeline is **idempotent**, meaning you can run it multiple times without corrupting or duplicating data. This is achieved by:
- Deduplicating input data
- Using Delta Lake’s MERGE operations to update only necessary rows
- Tracking which input files have already been processed
- Writing data in partitioned form (to avoid full table overwrites)

---

### Partitioning for Scale

Large datasets are written using Delta partitioning:
- Aggregated tables (e.g., by day or half-hour) are partitioned by `interval_start` or `date`
- This improves read/write performance and allows targeted refresh of only relevant partitions

Partitioning allows the pipeline to scale from thousands to millions of records without a drop in performance.

---

### Delta MERGE Logic

The pipeline uses **Delta Lake’s `MERGE INTO`** feature to support **incremental refresh** in presentation tables. Instead of overwriting full tables:
- New records are inserted
- Existing records (based on `date` and `product_display_name`) are updated in place


---

### Logging of Ingested Files

To ensure incremental ingestion, each processed file is logged into a Delta-based ingestion log table (`raw_ingestion_log`). Before any file is processed, the pipeline checks this log to prevent reprocessing.

Logged metadata includes:
- File name
- Ingestion timestamp

This approach ensures data lineage, traceability, and protection from accidental duplicate ingestion.

---

# Future Improvements

- Automate pipeline using Airflow or Cloud Composer (triggered daily after 8am)
- Add data quality checks (null handling, schema validation, anomaly detection)
- Store metadata about file ingestion status (for incremental loads)
- Migrate to GCP Dataproc for Cloud deployment
- Add unit tests for all transformation logic (e.g., with PyTest)
- Implement detailed logging and error handling with structured logs
- Implement schema enforcement using pyspark.sql.types.StructType or schema registry.
- Enable alerting and observability by emitting metrics (e.g., file counts, ingestion latency) to Prometheus/Grafana or Cloud Monitoring.

# Acknowledgements

Parts of this README and minor code optimisations were improved with the help of ChatGPT, used as a coding assistant to refine explanations, clarify logic, and enhance structure for clarity and efficiency.


# Confidential Submission

This project is submitted solely for assessment by Octopus Energy.  
It must not be redistributed, reused, or made public in any form, including GitHub or online portfolios.
