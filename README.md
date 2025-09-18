# POC Summary
This is a **proof of concept (POC)** data pipeline that demonstrates processing smart meter readings. It reads JSON files, joins them with SQLite reference data, and outputs aggregated tables. This POC focuses on demonstrating the core data flow and transformations rather than production-grade features.

---
# Design Approach

- **Basic implementation**: Simple Python modules to demonstrate the data flow
- **Delta Lake exploration**: Testing Delta features for potential production use
- **Modular structure**: Separated into logical components for clarity

---

# Pipeline Structure

The POC implements a three-layer architecture to demonstrate data processing stages:

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
spark-submit --jars "$(pwd)/energy_pipeline/jars/sqlite-jdbc-3.50.2.0.jar" main.py
```

### Input Assumptions
- JSON files: `data/readings/*.json`
- SQLite DB: `data/case_study.db`
- Sample data covers 3 days (Jan 1-3, 2021)
- Each JSON file contains readings for a single meter point
- **Note**: This POC processes all files in the directory - production version would need date filtering


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

# POC Features Demonstrated

### Basic Deduplication
The POC includes simple deduplication logic:
```python
.dropDuplicates(["interval_start", "meterpoint_id"])
```
Note: This is a basic implementation that doesn't handle edge cases like different consumption values for duplicates.

### Delta Lake Testing
The POC experiments with Delta Lake features:
- Basic MERGE operations for one table
- Partitioned writes for performance testing
- Simple file logging mechanism

**Limitations**: No optimization, Z-ordering, or VACUUM implemented yet.

### Current Implementation Notes
- **File Processing**: Currently loads ALL files serially (not scalable)
- **Memory Usage**: Contains `.collect()` operations that would fail with large datasets
- **Error Handling**: Basic try-catch with print statements (not production-ready)
- **Configuration**: Hardcoded dates and paths throughout

---

# Known Limitations & Future Improvements

## Current POC Limitations
- **Not production-ready**: This is a demonstration of data flow concepts only
- **Performance issues**: Serial file loading and `.collect()` operations won't scale
- **No error recovery**: Failed files are silently skipped
- **Missing validation**: No schema enforcement or data quality checks
- **Basic logging**: Uses print statements instead of proper logging framework

## Future Improvements for Production

## Performance Optimizations
- **Parallelize file ingestion**: Replace serial JSON loading with `spark.read.json()` for parallel processing
- **Eliminate .collect() bottleneck**: Replace driver-side collection with distributed operations
- **Implement broadcast joins**: Use broadcast hints for small dimension tables (product, meterpoint)
- **Add caching strategy**: Cache frequently accessed dimension tables
- **Delta optimization**: Implement Z-ordering, OPTIMIZE, and VACUUM commands
- **Partition pruning**: Add partition filters to reduce data scanning

## Production Readiness
- **Robust error handling**: Replace generic exceptions with specific error types and recovery strategies
- **Implement retry logic**: Add exponential backoff for transient failures
- **Structured logging**: Replace print statements with proper logging framework (e.g., structlog)
- **Schema validation**: Define and enforce schemas using StructType before processing
- **Data quality checks**: Add null checks, range validations, and anomaly detection
- **Monitoring & alerting**: Integrate with Datadog/Prometheus for metrics and alerts
- **Circuit breaker pattern**: Prevent cascade failures in pipeline steps

## Scalability & Architecture
- **Configuration management**: Externalize hardcoded values to config files/environment variables
- **Checkpoint & recovery**: Implement Spark checkpointing for failure recovery
- **Incremental processing**: Modify to process only new/changed files since last run
- **Streaming architecture**: Consider Spark Structured Streaming for real-time processing
- **Data lineage**: Implement Apache Atlas or DataHub for tracking data flow
- **Multi-tenancy support**: Add customer/region-based data isolation

## Testing & Quality
- **Unit tests**: Add comprehensive tests for all transformation logic
- **Integration tests**: Test end-to-end pipeline with sample data
- **Data validation tests**: Verify output correctness and completeness
- **Performance tests**: Benchmark with production-scale data volumes
- **CI/CD pipeline**: Automate testing and deployment

## Orchestration & Operations
- **Workflow orchestration**: Integrate with Airflow/Prefect for scheduling and dependencies
- **Idempotency verification**: Ensure all operations are truly idempotent
- **Backfill capabilities**: Add support for reprocessing historical data
- **SLA monitoring**: Track and alert on pipeline completion times
- **Cost optimization**: Implement spot instances and auto-scaling for cloud deployment

# Acknowledgements

Parts of this README and minor code optimisations were improved with the help of ChatGPT, used as a coding assistant to refine explanations, clarify logic, and enhance structure for clarity and efficiency.
