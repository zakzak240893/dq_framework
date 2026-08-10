# Data Quality Framework

A PySpark-based local data quality framework for comparing datasets and finding primary keys. Supports both local and distributed (Docker) Spark clusters.

## Features

- **Schema Comparison**: Compare column names and data types between two datasets
- **Data Comparison**: Row-by-row comparison with optional column exclusion, showing up to 3 mismatch examples per column
- **Primary Key Detection**: Find primary keys using a cardinality-based algorithm on categorical columns
- **Streaming Logging**: Real-time logging to file
- **Docker Support**: Scalable Spark cluster with configurable workers

## Installation

Using uv (recommended):

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

## Quick Start

### Recommended Run Mode

Use Docker mode for production-like execution. It avoids host Java compatibility issues by running driver and executors in containers.

### 1. Configure Inputs

Edit `dq_setup.json` in the project root.

Example:

```json
{
  "run": {
    "remote": false,
    "master": "spark://localhost:7077",
    "schema_file": "data/schema.json"
  },
  "data": {
    "file1": "data/1.csv",
    "file2": "data/2.csv",
    "file1_type": "csv",
    "file2_type": "csv"
  },
  "pk": {
    "file": "data/1.csv",
    "file_type": "csv"
  },
  "compare": {
    "exclude_columns": []
  }
}
```

Notes:

1. Relative paths are resolved relative to the setup file location.
2. `schema_file` is optional.
3. Schema comparison always uses inferred schemas.
4. If `schema_file` is provided, it is applied only to row-level comparison and PK inference.

### 2. Start Spark Services

```bash
docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-worker-3
```

### 3. Run Data Quality Job

Default config file:

```bash
docker compose --profile dq run --rm dq-runner
```

Custom config file:

```bash
docker compose --profile dq run --rm dq-runner /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /app/main.py --config /app/path/to/custom_setup.json --remote --master spark://spark-master:7077
```

### 4. Check Results

1. Console output shows schema status, mismatch count, and PK results.
2. Logs are written to `dq.log`.

### 5. Stop Spark Services

```bash
docker compose down
```

### Optional: Run From Host CLI

Default config file:

```bash
python main.py
```

Custom config file:

```bash
python main.py --config path/to/custom_setup.json
```

CLI flags can override setup file values (for example `--file1`, `--file2`, `--schema-file`, `--exclude-columns`).

## Docker Configuration

The `docker-compose.yml` file creates:
- 1 Spark Master node (ports 7077, 8081, 4040)
- 3 Spark Worker nodes (each with 2GB memory, 1 core)
- 1 optional DQ runner service (`dq-runner`) that submits `main.py` from inside Docker

### Scaling Workers

To add more workers, duplicate a worker service in `docker-compose.yml`:

```yaml
spark-worker-4:
  image: apache/spark:3.5.1
  container_name: spark-worker-4
  command:
    - /opt/spark/bin/spark-class
    - org.apache.spark.deploy.worker.Worker
    - --cores
    - "1"
    - --memory
    - 2g
    - spark://spark-master:7077
  depends_on:
    spark-master:
      condition: service_healthy
  volumes:
    - ./data:/app/data
  networks:
    - spark-network
```

Then restart:

```bash
docker compose up -d
```

## API Reference

### `DataQualityFramework`

Main orchestrator class.

**Constructor:**
- `log_file` (str, optional): Path to log file
- `spark_session` (SparkSession, optional): Existing SparkSession
- `remote_mode` (bool, optional): Connect to remote Spark cluster
- `master_url` (str, optional): Spark master URL

**Methods:**

- `compare_files(file1, file2, file1_type='csv', file2_type='csv', exclude_columns=None, schema_file_path=None)`
  - Compares two local CSV/Parquet files
  - Compares inferred schemas first; optional external schema is only used for row-level comparison
  - Returns up to 3 mismatch examples per column with row identifiers (`row_identifier`) and compared values (`df1_value`, `df2_value`)
  - Returns: `(bool, dict)` - (success, result)

- `find_primary_keys(file_path, file_type='csv', schema_file_path=None)`
  - Detects primary keys in a dataset
  - Optional external schema can be applied before PK detection
  - Returns: `(bool, dict)` - (success, result)

### `DataConnector`

Loads CSV/Parquet files into DataFrames. Supports local and remote Spark clusters.

### `SchemaComparator`

Compares schemas (columns and data types).

### `DataComparator`

Compares data row-by-row.

### `PrimaryKeyFinder`

Finds primary keys using cardinality-based algorithm.

### `StreamingLogger`

Streams log messages to file in real-time.

## Algorithm Details

### Primary Key Detection

1. Filter to categorical (non-numeric) columns only
2. Sort by cardinality (distinct values count) from highest to lowest
3. Iteratively add columns to candidate PK list
4. Check uniqueness after each addition
5. Return first combination that guarantees uniqueness

**Note:** Null values are treated as distinct values in PK detection.

## Project Structure

```
dq_framework/
├── src/
│   ├── __init__.py
│   ├── data_connector.py
│   ├── schema_comparator.py
│   ├── data_comparator.py
│   ├── primary_key_finder.py
│   ├── logger_config.py
│   └── data_quality_framework.py
├── data/
│   ├── sample1.csv
│   └── sample2.csv
├── main.py
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Dependencies

- Python >= 3.12
- PySpark >= 4.2.0
- PyArrow >= 25.0.0

## License

MIT