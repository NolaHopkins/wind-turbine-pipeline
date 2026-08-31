# Wind Turbine Data Pipeline

## Overview

This project implements a proof-of-concept PySpark data pipeline for processing wind turbine sensor data.

The source data contains hourly measurements for 15 wind turbines across multiple CSV files. The pipeline ingests and cleans the source data, calculates daily power output statistics, identifies anomalous turbine readings, and stores the processed outputs for further analysis.

## Approach

The pipeline follows the below process:

`CSV -> PySpark ingestion -> cleaning -> daily summary -> anomaly detection -> Parquet -> DuckDB`

**Ingestion:** The CSV files are read using an explicit schema and metadata columns are added to identify the source file and ingestion time.

**Cleaning:** Records with missing or invalid required values are separated from the cleaned dataset and retained with a rejection reason. Duplicate turbine/timestamp measurements are also removed.

**Daily summary:** Minimum, maximum and average power output are calculated for each turbine per calendar day. A measurement count is also included to help identify days with missing hourly readings.

**Anomaly detection:** Each turbine's power output is compared with the fleet mean at the same timestamp. Readings more than two standard deviations from the mean are identified as anomalies.

**Storage:** Spark outputs are written to Parquet and then loaded into a local DuckDB database. DuckDB provides a lightweight database suitable for this proof of concept.

## Assumptions

* Measurements are expected hourly.
* A 24-hour period is treated as a calendar day.
* Missing or invalid measurements are removed rather than imputed.
* Negative wind speed and power output are considered invalid for this POC.
* Wind direction is expected to be between 0 and 359 degrees.
* Anomalies are readings more than two standard deviations from the fleet mean at the same timestamp.
* No maximum power output threshold is applied because turbine capacity metadata was not supplied.

## Running the project

Install the required dependencies:

```bash
pip install -r requirements.txt
pip install -e .
```

Run the pipeline:

```bash
python main.py
```

The processed data is written to `data/output/` and loaded into `data/wind_turbine.duckdb`.

## Tests

Tests are written using pytest and use small, deterministic datasets to validate the cleaning, daily summary and anomaly detection logic.

This allows invalid and anomalous values to be tested without modifying the supplied source files.

Run the tests with:

```bash
pytest -v
```

## Production considerations

For a production implementation, I would consider incremental processing rather than rebuilding the full dataset on each run, cloud object storage for source and processed data, orchestration and scheduling of the daily pipeline, and monitoring for pipeline failures and data quality issues.

The anomaly detection could also be improved by considering factors such as wind conditions, turbine characteristics and historical performance rather than relying only on the simple statistical rule used for this POC.