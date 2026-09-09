# Data Quality Framework

A PySpark-based local data quality framework for comparing datasets and finding primary keys. Supports both local and distributed (Docker) Spark clusters.

## Features

- **Schema Comparison**: Compare column names and data types between two datasets
- **Data Comparison**: Row-by-row comparison via a null-safe full-outer join on key columns, with optional column exclusion, showing up to 3 mismatch examples per column and a nested JSON export of every mismatched row
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
docker compose --profile dq run --service-ports --rm dq-runner
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

Current Spark container tag: `apache/spark:4.0.1`

If you change the Spark image tag, pull and recreate containers:

```bash
docker compose pull
docker compose up -d --force-recreate spark-master spark-worker-1 spark-worker-2 spark-worker-3 dq-runner
```

### Scaling Workers

To add more workers, duplicate a worker service in `docker-compose.yml`:

```yaml
spark-worker-4:
  image: apache/spark:4.0.1
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

Then start the new worker service:

```bash
docker compose up -d spark-worker-4
```

### Tuning Partition Count

Row-level comparison (`DataQualityFramework.compare_files`) repartitions both loaded DataFrames before primary-key inference and joining. This exists because Spark's file scan splits large Parquet files into partitions based on **compressed bytes on disk**, not row count — since columnar compression density varies across a file (e.g. some column value ranges compress far smaller per row than others), byte-balanced partitions can end up wildly imbalanced in actual row count, causing a few tasks to take 100x longer than the rest ("data skew").

The partition count is derived automatically as:

```
target_partitions = spark.sparkContext.defaultParallelism * REPARTITION_CORE_MULTIPLIER
```

`defaultParallelism` reflects the total cores currently registered across all Spark workers, so this scales automatically if you add/remove workers or change `--cores` in `docker-compose.yml`. `REPARTITION_CORE_MULTIPLIER` (default `4`) controls how many partitions each core processes, and can be overridden via `compare.repartition_core_multiplier` in `dq_setup.json` or the `--repartition-core-multiplier` CLI flag:

- Lower it (e.g. `1`) for the fewest possible shuffles/scheduling overhead — appropriate if your data has little skew and every partition costs roughly the same to process.
- Raise it (e.g. `8`+) for very large datasets or unreliable/slow workers — smaller partitions reduce the wall-clock cost of any single retried/lost task and further average out residual skew.

#### Choosing a value for your machine

Important: this multiplier only controls how the *existing* total work is sliced up — it does not reduce the total CPU-seconds of work, which is bounded by your cluster's total cores (`defaultParallelism`). With `N` partitions and `C` total cores, tasks run in `ceil(N / C)` sequential waves, so total wall time for the repartitioned stage is roughly `waves * time_per_partition`. Going from `N = C` (1 wave) to `N = 4*C` (4 waves) does **not** make the stage faster — it only shrinks each individual partition (which helps if a task gets retried/lost, and further evens out any residual skew).

Pick a value based on how many cores you've actually configured across `docker-compose.yml` workers (`--cores` per worker × number of workers):

- **Few cores (2-6 total, e.g. the default 3-worker/2-core-each setup)**: use `2-4`. Keeping partitions reasonably large avoids excessive scheduling overhead when there aren't many cores to spread work across anyway.
- **Moderate core count (8-32 total)**: `4` (the default) is a good starting point — enough granularity for retry resilience without too many tiny tasks.
- **Many cores (32+ total, e.g. several beefy workers)**: raise to `6-8` so there are enough partitions to keep all cores busy simultaneously and avoid a few large partitions straggling behind while other cores sit idle.
- If wall-clock time for the repartitioned stages is too slow regardless of this setting, the fix is to add more Spark worker cores (more workers, or a higher `--cores` value per worker in `docker-compose.yml`), not to keep raising the multiplier.

#### Lower-RAM machines

If your machine has less RAM than the workers/executors are configured for in `docker-compose.yml` and `src/data_connector.py`, **raise** `repartition_core_multiplier` (e.g. `6-8+`) rather than lower it. More, smaller partitions mean each task holds less data in memory at once, reducing per-task/executor memory pressure — at the cost of more (but individually cheaper) shuffle tasks, which is a reasonable trade-off when memory, not CPU, is the constraint.

Also lower `spark.executor.memory` / `spark.executor.memoryOverhead` / `spark.driver.memory` in `src/data_connector.py` and the `--memory` value per worker in `docker-compose.yml` to fit your available RAM — check total memory available to Docker with `docker info --format '{{.MemTotal}}'` first, and keep total demand (all executors + driver + a few GB overhead for the master/worker daemons and OS) comfortably under that value to avoid the engine's OOM killer terminating an executor (visible in logs as `Command exited with code 137` / `Lost executor N`).

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
  - Writes mismatched rows under `mismatch_output_dir/<file1 stem>/` (default base dir `data/mismatches`), split into three JSON files, one record per key value: `{<key_columns...>, "mismatches": [{"column_name", "df1_value", "df2_value"}, ...]}`
    - `mismatches_missing_in_df1.json`: rows whose key exists only in df2 (`column_name` is `__missing_in_df1__`)
    - `mismatches_missing_in_df2.json`: rows whose key exists only in df1 (`column_name` is `__missing_in_df2__`)
    - `mismatches_data.json`: rows present on both sides with column-level value discrepancies
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