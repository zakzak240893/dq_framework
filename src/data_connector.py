"""
DataConnector module for loading local CSV and Parquet files.
"""

import json
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType


class DataConnector:
    """
    A connector class for loading local CSV and Parquet files into PySpark DataFrames.
    Supports both local and remote Spark clusters (Docker-based).
    Follows Single Responsibility Principle.
    """

    def __init__(self, spark_session: Optional[SparkSession] = None, remote_mode: bool = False, master_url: Optional[str] = None):
        """
        Initialize DataConnector with a SparkSession.

        Args:
            spark_session: Optional existing SparkSession. If not provided, creates a new one.
            remote_mode: Whether to connect to a remote Spark cluster.
            master_url: Spark master URL (e.g., 'spark://localhost:7077'). Default is local.
        """
        if spark_session is None:
            if remote_mode:
                self._spark = (
                    SparkSession.builder
                    .appName("DataQualityFramework")
                    .master(master_url or "spark://localhost:7077")
                    .config("spark.sql.shuffle.partitions", "200")
                    .config("spark.ui.enabled", "true")
                    .config("spark.ui.port", "4040")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.executor.memory", "18g")
                    .config("spark.executor.memoryOverhead", "1g")
                    .config("spark.driver.memory", "6g")
                    .config("spark.driver.extraJavaOptions", "-DHADOOP_USER_NAME=root")
                    .config("spark.executor.extraJavaOptions", "-DHADOOP_USER_NAME=root")
                    .getOrCreate()
                )
            else:
                self._spark = (
                    SparkSession.builder.appName("DataQualityFramework")
                    .config("spark.sql.shuffle.partitions", "200")
                    .config("spark.ui.enabled", "true")
                    .config("spark.ui.port", "4040")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.sql.warehouse.dir", "tmp/spark-warehouse")
                    .config("spark.driver.extraJavaOptions", 
                        "-Djava.security.auth.kerberos.dns.lookup=false "
                        "-Djava.security.krb5.disableReplay=true "
                        "-Dhadoop.user.name=root")
                    .config("spark.executor.extraJavaOptions", 
                        "-Djava.security.auth.kerberos.dns.lookup=false "
                        "-Djava.security.krb5.disableReplay=true "
                        "-Dhadoop.user.name=root")
                    .getOrCreate()
                )
            self._is_external_session = False
        else:
            self._spark = spark_session
            self._is_external_session = True

    @property
    def spark(self) -> SparkSession:
        """Expose the underlying SparkSession (e.g. to inspect cluster parallelism)."""
        return self._spark

    def _load_struct_schema(self, schema_file_path: str) -> StructType:
        """
        Load a Spark StructType schema from a JSON file.

        Args:
            schema_file_path: Path to JSON schema file generated from StructType.json().

        Returns:
            Parsed StructType instance.
        """
        path = Path(schema_file_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file_path}")

        try:
            with path.open("r", encoding="utf-8") as f:
                schema_json = json.load(f)
            return StructType.fromJson(schema_json)
        except Exception as e:
            raise ValueError(f"Invalid Spark schema JSON file '{schema_file_path}': {str(e)}") from e

    def _normalize_literal_null_strings(self, df: DataFrame) -> DataFrame:
        """
        Convert case-insensitive literal 'null' strings to actual null values.

        This keeps downstream comparisons consistent when some files encode missing
        values as text while others have true nulls.
        """
        string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
        if not string_columns:
            return df

        transformed_columns = []
        string_column_set = set(string_columns)
        for column_name in df.columns:
            if column_name in string_column_set:
                transformed_columns.append(
                    when(lower(trim(col(column_name))) == "null", None)
                    .otherwise(col(column_name))
                    .alias(column_name)
                )
            else:
                transformed_columns.append(col(column_name))

        return df.select(*transformed_columns)

    def load_csv(
        self,
        file_path: str,
        has_header: bool = True,
        schema_file_path: Optional[str] = None,
        infer_schema: bool = True,
    ) -> DataFrame:
        """
        Load a CSV file into a DataFrame.

        Args:
            file_path: Path to the CSV file.
            has_header: Whether the CSV has a header row.
            schema_file_path: Optional Spark JSON schema file path.
            infer_schema: Whether to infer schema when no schema file is provided.

        Returns:
            PySpark DataFrame.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        reader = self._spark.read.option("header", has_header).option("nullValue", "NULL")

        if schema_file_path:
            reader = reader.schema(self._load_struct_schema(schema_file_path)).option("enforceSchema", "true")
        elif infer_schema:
            reader = reader.option("inferSchema", "true")

        df = reader.csv(str(path))
        return self._normalize_literal_null_strings(df)

    def load_parquet(self, file_path: str, schema_file_path: Optional[str] = None) -> DataFrame:
        """
        Load a Parquet file into a DataFrame.

        Args:
            file_path: Path to the Parquet file.
            schema_file_path: Optional Spark JSON schema file path.

        Returns:
            PySpark DataFrame.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")

        reader = self._spark.read
        if schema_file_path:
            reader = reader.schema(self._load_struct_schema(schema_file_path))

        df = reader.parquet(str(path))
        return self._normalize_literal_null_strings(df)

    def close(self):
        """
        Close the SparkSession if it was created internally.
        """
        if not self._is_external_session and self._spark is not None:
            self._spark.stop()