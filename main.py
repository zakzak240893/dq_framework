"""
Main entry point for the Data Quality Framework.
Provides example usage of the framework.
"""

import os
import sys
import json
from datetime import datetime

# CRITICAL: Set environment variable BEFORE any Spark imports
# This fixes Windows JDK 25+ compatibility issue with Hadoop security
os.environ["HADOOP_USER_NAME"] = "root"

from pathlib import Path
import argparse
from typing import Optional

# Import from the src package
from src.data_quality_framework import DataQualityFramework


DEFAULT_CONFIG_FILE = "dq_setup.json"
VALID_FILE_TYPES = {"csv", "parquet"}


def _load_setup_config(config_path: Path) -> dict:
    """Load setup parameters from a JSON file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid config format in {config_path}")
    return config


def _parse_exclude_columns(value):
    """Normalize exclude columns from CLI string or config list."""
    if value is None:
        return None

    if isinstance(value, str):
        cols = [c.strip() for c in value.split(",") if c.strip()]
        return cols or None

    if isinstance(value, list):
        cols = [str(c).strip() for c in value if str(c).strip()]
        return cols or None

    raise ValueError("exclude_columns must be a comma-separated string or a list")


def _parse_key_columns(value):
    """Normalize key columns from CLI string or config list."""
    if value is None:
        return None

    if isinstance(value, str):
        cols = [c.strip() for c in value.split(",") if c.strip()]
        return cols or None

    if isinstance(value, list):
        cols = [str(c).strip() for c in value if str(c).strip()]
        return cols or None

    raise ValueError("key_columns must be a comma-separated string or a list")


def _validate_file_type(file_type: str, field_name: str) -> str:
    normalized = file_type.lower()
    if normalized not in VALID_FILE_TYPES:
        raise ValueError(f"Invalid {field_name}: {file_type}. Expected one of: {sorted(VALID_FILE_TYPES)}")
    return normalized


def _resolve_path(path_value: Optional[str], base_dir: Path) -> Optional[str]:
    """Resolve relative paths against config directory."""
    if path_value is None:
        return None

    path = Path(path_value)
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def main():
    """Example usage of the Data Quality Framework."""
    
    parser = argparse.ArgumentParser(description="Data Quality Framework")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path(__file__).parent / DEFAULT_CONFIG_FILE),
        help="Path to setup JSON file with CLI defaults"
    )
    parser.add_argument(
        "--remote",
        dest="remote",
        action="store_true",
        help="Connect to remote Spark cluster (Docker)"
    )
    parser.add_argument(
        "--no-remote",
        dest="remote",
        action="store_false",
        help="Disable remote Spark cluster mode"
    )
    parser.set_defaults(remote=None)
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL"
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        default=None,
        help="Optional Spark StructType JSON schema file path"
    )
    parser.add_argument(
        "--file1",
        type=str,
        default=None,
        help="Path to first input file"
    )
    parser.add_argument(
        "--file2",
        type=str,
        default=None,
        help="Path to second input file"
    )
    parser.add_argument(
        "--file1-type",
        type=str,
        default=None,
        choices=["csv", "parquet"],
        help="Type of first input file"
    )
    parser.add_argument(
        "--file2-type",
        type=str,
        default=None,
        choices=["csv", "parquet"],
        help="Type of second input file"
    )
    parser.add_argument(
        "--pk-file",
        type=str,
        default=None,
        help="Path to file for primary key inference (defaults to --file1)"
    )
    parser.add_argument(
        "--pk-file-type",
        type=str,
        default=None,
        choices=["csv", "parquet"],
        help="Type of PK file (defaults to --file1-type)"
    )
    parser.add_argument(
        "--exclude-columns",
        type=str,
        default=None,
        help="Comma-separated list of columns to exclude from row comparison"
    )
    parser.add_argument(
        "--key-columns",
        type=str,
        default=None,
        help="Comma-separated list of key columns used for row alignment"
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config_dir = config_path.parent
    config = _load_setup_config(config_path)

    run_cfg = config.get("run", {})
    data_cfg = config.get("data", {})
    pk_cfg = config.get("pk", {})
    compare_cfg = config.get("compare", {})

    remote_mode = args.remote if args.remote is not None else bool(run_cfg.get("remote", False))
    master_url = args.master or run_cfg.get("master", "spark://localhost:7077")
    schema_file = _resolve_path(args.schema_file or run_cfg.get("schema_file"), config_dir)
    mismatch_output_path = _resolve_path(run_cfg.get("mismatch_output_path", "data/mismatches.csv"), config_dir)

    file1 = _resolve_path(args.file1 or data_cfg.get("file1", "data/sample1.csv"), config_dir)
    file2 = _resolve_path(args.file2 or data_cfg.get("file2", "data/sample2.csv"), config_dir)
    file1_type = _validate_file_type(args.file1_type or data_cfg.get("file1_type", "csv"), "file1_type")
    file2_type = _validate_file_type(args.file2_type or data_cfg.get("file2_type", "csv"), "file2_type")

    pk_file = _resolve_path(args.pk_file or pk_cfg.get("file") or file1, config_dir)
    pk_file_type = _validate_file_type(
        args.pk_file_type or pk_cfg.get("file_type") or file1_type,
        "pk_file_type",
    )

    exclude_columns = _parse_exclude_columns(
        args.exclude_columns if args.exclude_columns is not None else compare_cfg.get("exclude_columns")
    )
    key_columns = _parse_key_columns(
        args.key_columns if args.key_columns is not None else compare_cfg.get("key_columns")
    )

    # Initialize framework with a new log file for each run
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"dq_{run_timestamp}.log"

    print(f"Log file: {log_path}")
    
    framework = DataQualityFramework(
        log_file=str(log_path),
        remote_mode=remote_mode,
        master_url=master_url
    )

    try:
        # Example 1: Compare two CSV files
        print("\n=== Example 1: Compare two files ===")

        success, result = framework.compare_files(
            file1=file1,
            file2=file2,
            file1_type=file1_type,
            file2_type=file2_type,
            exclude_columns=exclude_columns,
            schema_file_path=schema_file,
            key_columns=key_columns,
            mismatch_output_path=mismatch_output_path,
        )

        if success:
            print(f"Schemas match: {result['schemas_match']}")
            print(f"Total mismatches: {result['total_mismatches']}")
            if result.get("row_identifier_columns"):
                print(f"Row identifier columns: {result['row_identifier_columns']}")
            if result.get("missing_rows_file"):
                print(f"Missing rows file: {result['missing_rows_file']}")
            if result['mismatch_samples']:
                print("\nMismatch samples (up to 3 per column):")
                for col_name, samples in result['mismatch_samples'].items():
                    print(f"\nColumn '{col_name}':")
                    for i, sample in enumerate(samples, 1):
                        print(
                            f"  Example {i}: row={sample.get('row_identifier')}, "
                            f"df1={sample['df1_value']}, df2={sample['df2_value']}"
                        )
        else:
            if result.get("schemas_match") is False and "schema_differences" in result:
                print("Comparison failed: Schema mismatch")
                print(f"Schema differences: {result['schema_differences']}")
            else:
                print(f"Comparison failed: {result.get('error', 'Unknown error')}")

        # Example 2: Find primary keys in a dataset
        print("\n=== Example 2: Find primary keys ===")
        pk_success, pk_result = framework.find_primary_keys(
            file_path=pk_file,
            file_type=pk_file_type,
            schema_file_path=schema_file,
        )

        if pk_success:
            if pk_result['pk_found']:
                print(f"Primary key columns: {pk_result['pk_columns']}")
            else:
                print("No valid primary key found.")
        else:
            print(f"PK detection failed: {pk_result.get('error', 'Unknown error')}")

    finally:
        framework.close()


if __name__ == "__main__":
    main()
