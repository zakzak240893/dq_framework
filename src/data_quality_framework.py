"""
DataQualityFramework module - main orchestrator class.
"""

from typing import Optional, Tuple, Dict
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from .data_connector import DataConnector
from .schema_comparator import SchemaComparator
from .data_comparator import DataComparator
from .primary_key_finder import PrimaryKeyFinder
from .logger_config import StreamingLogger


class DataQualityFramework:
    """
    Main orchestrator class for data quality operations.
    Combines all components to provide a high-level API.
    Follows Dependency Inversion and Interface Segregation principles.
    """

    # Multiplier applied to the cluster's default parallelism to size row-level repartitioning;
    # see README "Tuning partition count" section before changing this.
    REPARTITION_CORE_MULTIPLIER = 4

    def __init__(
        self,
        log_file: Optional[str] = None,
        spark_session: Optional[SparkSession] = None,
        remote_mode: bool = False,
        master_url: Optional[str] = None,
    ):
        """
        Initialize the Data Quality Framework.

        Args:
            log_file: Path to the log file. If None, no file logging.
            spark_session: Optional existing SparkSession.
            remote_mode: Whether to connect to a remote Spark cluster (Docker).
            master_url: Spark master URL (e.g., 'spark://localhost:7077').
        """
        self.logger = StreamingLogger(log_file) if log_file else None
        self.connector = DataConnector(spark_session, remote_mode=remote_mode, master_url=master_url)
        self.schema_comparator = SchemaComparator()
        self.data_comparator = DataComparator()
        self.primary_key_finder = PrimaryKeyFinder()

    def _log(self, message: str, level: str = "info"):
        """Internal logging helper."""
        if self.logger:
            if level == "info":
                self.logger.log_info(message)
            elif level == "warning":
                self.logger.log_warning(message)
            elif level == "error":
                self.logger.log_error(message)

    def _target_partition_count(self, repartition_core_multiplier: Optional[int] = None) -> int:
        """Derive a repartition target from the cluster's total registered cores."""
        multiplier = repartition_core_multiplier if repartition_core_multiplier is not None else self.REPARTITION_CORE_MULTIPLIER
        default_parallelism = self.connector.spark.sparkContext.defaultParallelism
        return max(default_parallelism * multiplier, 1)

    def _load_dataframe(
        self,
        file_path: str,
        file_type: str,
        schema_file_path: Optional[str] = None,
        infer_schema: bool = True,
    ) -> DataFrame:
        """Internal helper to load files with optional external schema support."""
        if file_type.lower() == "csv":
            return self.connector.load_csv(
                file_path,
                schema_file_path=schema_file_path,
                infer_schema=infer_schema,
            )
        if file_type.lower() == "parquet":
            return self.connector.load_parquet(file_path, schema_file_path=schema_file_path)
        raise ValueError(f"Unsupported file type: {file_type}")

    def compare_files(
        self,
        file1: str,
        file2: str,
        file1_type: str = "csv",
        file2_type: str = "csv",
        exclude_columns: Optional[list] = None,
        schema_file_path: Optional[str] = None,
        key_columns: Optional[list] = None,
        mismatch_output_dir: str = "./data/mismatches",
        repartition_core_multiplier: Optional[int] = None,
    ) -> Tuple[bool, Dict]:
        """
        Compare two local files (CSV/Parquet).

        Args:
            file1: Path to first file.
            file2: Path to second file.
            file1_type: Type of first file ('csv' or 'parquet').
            file2_type: Type of second file ('csv' or 'parquet').
            exclude_columns: List of columns to exclude from data comparison.
            schema_file_path: Optional Spark JSON schema file applied only for row-level data comparison.
            key_columns: Optional explicit key columns for row alignment.
            mismatch_output_dir: Base directory under which a `<file1 stem>/` subfolder is created,
                containing `mismatches_missing_in_df1.json` (rows whose key exists only in df2),
                `mismatches_missing_in_df2.json` (rows whose key exists only in df1), and
                `mismatches_data.json` (rows present on both sides with column-level discrepancies).
            repartition_core_multiplier: Overrides REPARTITION_CORE_MULTIPLIER for this call (see README "Tuning Partition Count").

        Returns:
            Tuple of (is_successful, result_dict) where:
            - is_successful: Boolean indicating if comparison completed successfully
            - result_dict: Dictionary containing comparison results
        """
        try:
            self._log(f"Starting comparison: {file1} vs {file2}")

            # Always use inferred schemas for schema comparison.
            df1_inferred = self._load_dataframe(file1, file1_type, infer_schema=True)
            df2_inferred = self._load_dataframe(file2, file2_type, infer_schema=True)

            self._log(
                f"Loaded inferred DataFrames: df1 ({df1_inferred.count()} rows), df2 ({df2_inferred.count()} rows)"
            )

            # Compare schemas
            self._log("Comparing inferred schemas...")
            schemas_match, schema_diff = self.schema_comparator.compare_schemas(df1_inferred, df2_inferred, exclude_columns)

            if not schemas_match:
                self._log("Schema mismatch detected!", "warning")
                self._log(f"Schema differences: {schema_diff}", "warning")
                return False, {"schemas_match": False, "schema_differences": schema_diff}

            if schema_file_path:
                self._log(f"Schemas match. Applying external schema from {schema_file_path} for row-level comparison...")
                df1_compare = self._load_dataframe(
                    file1,
                    file1_type,
                    schema_file_path=schema_file_path,
                    infer_schema=False,
                )
                df2_compare = self._load_dataframe(
                    file2,
                    file2_type,
                    schema_file_path=schema_file_path,
                    infer_schema=False,
                )
            else:
                self._log("Schemas match. Proceeding to row-level data comparison...")
                df1_compare = df1_inferred
                df2_compare = df2_inferred

            # Rebalance by row count (not compressed bytes) to avoid skewed partitions from uneven parquet encoding density.
            # Cached so later actions (exceptAll counts, PK inference, join) reuse the balanced partitions instead of
            # re-scanning the source files, which would reintroduce compression-density skew from a fresh byte-based scan.
            target_partitions = self._target_partition_count(repartition_core_multiplier)
            self._log(f"Repartitioning row-level DataFrames into {target_partitions} partitions for balanced parallelism...")
            df1_compare = df1_compare.repartition(target_partitions).cache()
            df2_compare = df2_compare.repartition(target_partitions).cache()

            try:
                pk_checked_columns = self.primary_key_finder._get_categorical_columns(df1_compare)
                datetime_columns = self.primary_key_finder._get_datetime_columns(df1_compare)
                numeric_columns = self.primary_key_finder._get_numeric_columns(df1_compare)
                self._log(f"Metadata | files_compared: file1={file1}, file2={file2}")
                self._log(f"Metadata | pk_columns_checked: {pk_checked_columns}")
                self._log(f"Metadata | datetime_columns_checked: {datetime_columns}")
                self._log(f"Metadata | numeric_columns: {numeric_columns}")

                key_columns_for_compare = None
                if key_columns:
                    missing_columns = [
                        c for c in key_columns if c not in df1_compare.columns or c not in df2_compare.columns
                    ]
                    if missing_columns:
                        self._log(
                            f"Provided key columns not found in both datasets: {missing_columns}. Falling back to PK inference.",
                            "warning",
                        )
                    else:
                        key_columns_for_compare = key_columns
                        self._log(
                            f"Using user-provided key columns for mismatch row identification: {key_columns_for_compare}"
                        )

                # If no explicit keys are available, use inferred PK columns.
                if not key_columns_for_compare:
                    is_pk_found_for_compare, compare_pk_columns = self.primary_key_finder.find_primary_keys(df1_compare, exclude_columns)
                    key_columns_for_compare = compare_pk_columns if is_pk_found_for_compare else None

                if key_columns_for_compare and not key_columns:
                    self._log(f"Using inferred PK columns for mismatch row identification: {key_columns_for_compare}")
                else:
                    if not key_columns_for_compare:
                        error_message = "No PK found for mismatch row identification. Row-number fallback is disabled."
                        self._log(error_message, "error")
                        return False, {"error": error_message}

                # Compare data
                object_name = Path(file1).stem
                object_mismatch_dir = Path(mismatch_output_dir) / object_name
                missing_in_df1_output_path = str(object_mismatch_dir / "mismatches_missing_in_df1.json")
                missing_in_df2_output_path = str(object_mismatch_dir / "mismatches_missing_in_df2.json")
                data_output_path = str(object_mismatch_dir / "mismatches_data.json")

                total_mismatches, mismatch_samples, row_counts = self.data_comparator.compare_data(
                    df1_compare,
                    df2_compare,
                    exclude_columns,
                    key_columns=key_columns_for_compare,
                    missing_in_df1_output_path=missing_in_df1_output_path,
                    missing_in_df2_output_path=missing_in_df2_output_path,
                    data_output_path=data_output_path,
                )

                self._log(f"Data comparison completed. Total mismatches: {total_mismatches}")

                discrepancy_columns = list(mismatch_samples.keys())
                columns_with_issues = discrepancy_columns + [
                    label
                    for label, count in (
                        ("missing_in_df1", row_counts["missing_in_df1"]),
                        ("missing_in_df2", row_counts["missing_in_df2"]),
                    )
                    if count > 0
                ]
                self._log(f"Columns/rows with discrepancies: {columns_with_issues}")

                if total_mismatches > 0 and mismatch_samples:
                    self._log(f"Mismatch row identifier columns: {key_columns_for_compare or ['row_num']}")
                    for col_name, samples in mismatch_samples.items():
                        self._log(f"Column '{col_name}' mismatch examples (up to 3):")
                        for idx, sample in enumerate(samples, 1):
                            self._log(
                                f"  Example {idx}: row={sample.get('row_identifier')}, "
                                f"df1={sample.get('df1_value')}, df2={sample.get('df2_value')}"
                            )

                return True, {
                    "schemas_match": True,
                    "total_mismatches": total_mismatches,
                    "mismatch_samples": mismatch_samples,
                    "columns_with_discrepancies": columns_with_issues,
                    "row_counts": row_counts,
                    "row_identifier_columns": key_columns_for_compare,
                    "missing_in_df1_file": missing_in_df1_output_path,
                    "missing_in_df2_file": missing_in_df2_output_path,
                    "data_mismatches_file": data_output_path,
                }
            finally:
                df1_compare.unpersist()
                df2_compare.unpersist()

        except Exception as e:
            self._log(f"Error during comparison: {str(e)}", "error")
            return False, {"error": str(e)}

    def find_primary_keys(
        self,
        file_path: str,
        file_type: str = "csv",
        schema_file_path: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """
        Find primary keys in a dataset.

        Args:
            file_path: Path to the file.
            file_type: Type of file ('csv' or 'parquet').
            schema_file_path: Optional Spark JSON schema file applied before PK inference.

        Returns:
            Tuple of (is_successful, result_dict) where:
            - is_successful: Boolean indicating if PK detection completed successfully
            - result_dict: Dictionary containing PK results
        """
        try:
            self._log(f"Starting primary key detection for {file_path}")

            # Load file
            df = self._load_dataframe(
                file_path,
                file_type,
                schema_file_path=schema_file_path,
                infer_schema=not bool(schema_file_path),
            )

            self._log(f"Loaded DataFrame with {df.count()} rows and {len(df.columns)} columns")

            # Find primary keys
            is_pk_found, pk_columns = self.primary_key_finder.find_primary_keys(df)

            if is_pk_found:
                self._log(f"Primary key found: {pk_columns}")
                return True, {"pk_found": True, "pk_columns": pk_columns}
            else:
                self._log("No valid primary key found.", "warning")
                return True, {"pk_found": False, "pk_columns": []}

        except Exception as e:
            self._log(f"Error during PK detection: {str(e)}", "error")
            return False, {"error": str(e)}

    def close(self):
        """Close the framework and release resources."""
        self.connector.close()
        self._log("Data Quality Framework closed.")