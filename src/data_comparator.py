"""
DataComparator module for row-by-row data comparison.
"""

from typing import List, Dict, Optional, Tuple
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    array,
    col,
    coalesce,
    explode,
    filter as array_filter,
    lit,
    monotonically_increasing_id,
    row_number,
    size,
    struct,
    when,
)

MISSING_IN_DF1_MARKER = "__missing_in_df1__"
MISSING_IN_DF2_MARKER = "__missing_in_df2__"


class DataComparator:
    """
    A class to compare data row by row between two DataFrames.
    Follows Single Responsibility Principle.
    """

    def __init__(self):
        self._logger = logging.getLogger("DataQualityFramework")

    def _log(self, message: str, level: str = "info") -> None:
        if level == "warning":
            self._logger.warning(message)
        elif level == "error":
            self._logger.error(message)
        else:
            self._logger.info(message)

    def _build_eq_null_safe_condition(
        self, left_alias: str, right_alias: str, columns: List[str]
    ):
        join_condition = None
        for column_name in columns:
            predicate = col(f"{left_alias}.{column_name}").eqNullSafe(col(f"{right_alias}.{column_name}"))
            join_condition = predicate if join_condition is None else (join_condition & predicate)
        return join_condition

    def _write_single_json(self, df: DataFrame, output_json_path: str) -> None:
        output_path = Path(output_json_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_dir = output_path.parent / f"{output_path.stem}_tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        if output_path.exists():
            output_path.unlink()

        df.coalesce(1).write.mode("overwrite").json(str(temp_dir))

        part_files = list(temp_dir.glob("part-*.json"))
        if part_files:
            shutil.move(str(part_files[0]), str(output_path))
        else:
            output_path.write_text("", encoding="utf-8")

        shutil.rmtree(temp_dir, ignore_errors=True)

    def compare_data(
        self,
        df1: DataFrame,
        df2: DataFrame,
        exclude_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        mismatch_output_path: str = "./data/mismatches.json",
    ) -> Tuple[int, Dict[str, List[Dict]]]:
        """
        Compare data row by row between two DataFrames using a single null-safe full-outer join.

        Args:
            df1: First DataFrame.
            df2: Second DataFrame.
            exclude_columns: List of columns to exclude from comparison.
            key_columns: Required key columns used to align rows across DataFrames.
            mismatch_output_path: Path to write the nested mismatches JSON file.

        Returns:
            Tuple of (total_mismatches, mismatch_samples) where:
            - total_mismatches: Total number of rows with at least one mismatch
            - mismatch_samples: Dictionary mapping column names to up to 3 example mismatches
              including row identifiers and mismatched values.
        """
        if exclude_columns is None:
            exclude_columns = []

        key_columns = key_columns or []

        # Required pre-check: directional row delta from df1 to df2.
        total_mismatches = df1.exceptAll(df2).count()
        self._log(f"Total mismatches from df1.exceptAll(df2): {total_mismatches}")
        reverse_total_mismatches = df2.exceptAll(df1).count()
        self._log(f"Total mismatches from df2.exceptAll(df1): {reverse_total_mismatches}")

        common_columns = [c for c in df1.columns if c in df2.columns]
        if not common_columns:
            self._log("No common columns found. Skipped missing-row export.", "warning")
            return total_mismatches, {}

        missing_keys = [k for k in key_columns if k not in df1.columns or k not in df2.columns]
        if not key_columns or missing_keys:
            raise ValueError("Key columns are required for comparison. Row-number fallback is disabled.")

        non_key_cols = sorted(c for c in common_columns if c not in exclude_columns and c not in key_columns)
        if not non_key_cols:
            self._log("No comparable non-key columns found. Skipped missing-row export.", "warning")
            return total_mismatches, {}

        df1_marked = df1.withColumn("_dq_in_df1", lit(True))
        df2_marked = df2.withColumn("_dq_in_df2", lit(True))
        join_condition = self._build_eq_null_safe_condition("df1", "df2", key_columns)

        joined = (
            df1_marked.alias("df1")
            .join(df2_marked.alias("df2"), on=join_condition, how="full_outer")
            .select(
                *[coalesce(col(f"df1.{k}"), col(f"df2.{k}")).alias(k) for k in key_columns],
                *[col(f"df1.{c}").alias(f"{c}__df1") for c in non_key_cols],
                *[col(f"df2.{c}").alias(f"{c}__df2") for c in non_key_cols],
                col("df1._dq_in_df1").alias("_dq_in_df1"),
                col("df2._dq_in_df2").alias("_dq_in_df2"),
            )
        )

        per_column_struct_exprs = []
        for column_name in non_key_cols:
            df1_value = col(f"{column_name}__df1")
            df2_value = col(f"{column_name}__df2")
            differs = ~df1_value.eqNullSafe(df2_value)
            per_column_struct_exprs.append(
                when(
                    differs,
                    struct(
                        lit(column_name).alias("column_name"),
                        df1_value.cast("string").alias("df1_value"),
                        df2_value.cast("string").alias("df2_value"),
                    ),
                )
            )
        both_sides_mismatches = array_filter(array(*per_column_struct_exprs), lambda x: x.isNotNull())

        def _missing_array(marker: str):
            return array(
                struct(
                    lit(marker).alias("column_name"),
                    lit(None).cast("string").alias("df1_value"),
                    lit(None).cast("string").alias("df2_value"),
                )
            )

        mismatches_col = (
            when(col("_dq_in_df1").isNull(), _missing_array(MISSING_IN_DF1_MARKER))
            .when(col("_dq_in_df2").isNull(), _missing_array(MISSING_IN_DF2_MARKER))
            .otherwise(both_sides_mismatches)
        )

        # Cache: the joined mismatches are reused across the count, JSON write, and sample query below.
        result = (
            joined.withColumn("mismatches", mismatches_col)
            .select(*key_columns, "mismatches")
            .filter(size("mismatches") > 0)
            .cache()
        )

        try:
            detailed_mismatch_count = result.count()
            self._log(f"Detailed mismatch rows (keyed full-outer comparison): {detailed_mismatch_count}")

            self._write_single_json(result, mismatch_output_path)
            self._log(f"Saved detailed mismatches to {mismatch_output_path}")

            if detailed_mismatch_count == 0:
                return total_mismatches, {}

            exploded = (
                result.select(*key_columns, explode(col("mismatches")).alias("mismatch"))
                .select(
                    *key_columns,
                    col("mismatch.column_name").alias("column_name"),
                    col("mismatch.df1_value").alias("df1_value"),
                    col("mismatch.df2_value").alias("df2_value"),
                )
                .filter(~col("column_name").isin(MISSING_IN_DF1_MARKER, MISSING_IN_DF2_MARKER))
            )

            sample_window = Window.partitionBy("column_name").orderBy(monotonically_increasing_id())
            sample_rows = (
                exploded.withColumn("_rn", row_number().over(sample_window))
                .filter(col("_rn") <= 3)
                .orderBy("column_name", "_rn")
                .collect()
            )
        finally:
            result.unpersist()

        mismatch_samples: Dict[str, List[Dict]] = {}
        for row in sample_rows:
            row_identifier = {k: row[k] for k in key_columns}
            mismatch_samples.setdefault(row["column_name"], []).append(
                {
                    "row_identifier": row_identifier,
                    "df1_value": row["df1_value"],
                    "df2_value": row["df2_value"],
                }
            )

        return total_mismatches, mismatch_samples
