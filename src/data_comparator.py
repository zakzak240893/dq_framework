"""
DataComparator module for row-by-row data comparison.
"""

from typing import List, Dict, Optional, Tuple
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, sum as spark_sum
from pyspark.sql.types import NumericType


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

    def _write_single_csv(self, df: DataFrame, output_csv_path: str) -> None:
        output_path = Path(output_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_dir = output_path.parent / f"{output_path.stem}_tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        if output_path.exists():
            output_path.unlink()

        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

        part_files = list(temp_dir.glob("part-*.csv"))
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
        mismatch_output_path: str = "./data/mismatches.csv",
    ) -> Tuple[int, Dict[str, List[Dict]]]:
        """
        Compare data row by row between two DataFrames.

        Args:
            df1: First DataFrame.
            df2: Second DataFrame.
            exclude_columns: List of columns to exclude from comparison.
            key_columns: Optional key columns used to align rows across DataFrames.
            mismatch_output_path: Path to write missing-row mismatches CSV.

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

        # Compare deduplicated data only.
        df1_compare = df1.dropDuplicates()
        df2_compare = df2.dropDuplicates()

        self._log(
            f"Deduplicated datasets for comparison: df1={df1_compare.count()} rows, df2={df2_compare.count()} rows"
        )

        common_columns = [c for c in df1_compare.columns if c in df2_compare.columns]
        if not common_columns:
            self._log("No common columns found. Skipped missing-row export.", "warning")
            return total_mismatches, {}

        # Compare only categorical (non-numeric) columns.
        df1_schema_types = {field.name: field.dataType for field in df1_compare.schema}
        compare_columns = [
            c
            for c in df1_compare.columns
            if c not in exclude_columns and c in df2_compare.columns and not isinstance(df1_schema_types[c], NumericType)
        ]

        # Export should capture mismatches across all comparable columns.
        export_columns = [c for c in common_columns if c not in exclude_columns]
        if not export_columns:
            return total_mismatches, {}

        can_use_keys = bool(key_columns) and all(k in df1_compare.columns and k in df2_compare.columns for k in key_columns)
        both_sides_present = None

        if not can_use_keys:
            raise ValueError("Key columns are required for comparison. Row-number fallback is disabled.")

        if can_use_keys:
            df1_marked = df1_compare.withColumn("_dq_in_df1", lit(1))
            df2_marked = df2_compare.withColumn("_dq_in_df2", lit(1))
            join_condition = self._build_eq_null_safe_condition("df1", "df2", key_columns)
            value_columns = sorted(set(common_columns + key_columns))

            left_joined = (
                df1_marked.alias("df1")
                .join(df2_marked.alias("df2"), on=join_condition, how="left")
                .select(
                    *[coalesce(col(f"df1.{k}"), col(f"df2.{k}")).alias(f"pk_{k}") for k in key_columns],
                    *[col(f"df1.{c}").alias(f"df1__{c}") for c in value_columns],
                    *[col(f"df2.{c}").alias(f"df2__{c}") for c in value_columns],
                    col("df1._dq_in_df1").alias("_dq_in_df1"),
                    col("df2._dq_in_df2").alias("_dq_in_df2"),
                )
            )
            right_joined = (
                df1_marked.alias("df1")
                .join(df2_marked.alias("df2"), on=join_condition, how="right")
                .select(
                    *[coalesce(col(f"df1.{k}"), col(f"df2.{k}")).alias(f"pk_{k}") for k in key_columns],
                    *[col(f"df1.{c}").alias(f"df1__{c}") for c in value_columns],
                    *[col(f"df2.{c}").alias(f"df2__{c}") for c in value_columns],
                    col("df1._dq_in_df1").alias("_dq_in_df1"),
                    col("df2._dq_in_df2").alias("_dq_in_df2"),
                )
            )

            joined = left_joined.unionByName(right_joined).dropDuplicates()
            identifier_select = [col(f"pk_{k}") for k in key_columns]
            both_sides_present = col("_dq_in_df1").isNotNull() & col("_dq_in_df2").isNotNull()

        comparison_expr = lit(True)
        for col_name in compare_columns:
            if can_use_keys:
                df1_col = col(f"df1__{col_name}")
                df2_col = col(f"df2__{col_name}")
            else:
                df1_col = col(f"df1.{col_name}")
                df2_col = col(f"df2.{col_name}")

            col_match = df1_col.eqNullSafe(df2_col)
            comparison_expr = comparison_expr & col_match

        export_match_expr = lit(True)
        for col_name in export_columns:
            if can_use_keys:
                left_col = col(f"df1__{col_name}")
                right_col = col(f"df2__{col_name}")
            else:
                left_col = col(f"df1.{col_name}")
                right_col = col(f"df2.{col_name}")

            export_match_expr = export_match_expr & left_col.eqNullSafe(right_col)

        if can_use_keys:
            mismatch_row_condition = (~both_sides_present) | (~comparison_expr)
            export_row_condition = (~both_sides_present) | (~export_match_expr)
        else:
            mismatch_row_condition = ~comparison_expr
            export_row_condition = ~export_match_expr

        mismatched_rows = joined.filter(mismatch_row_condition)
        detailed_mismatch_count = mismatched_rows.count()
        self._log(f"Detailed mismatch rows after dedup and full key coverage: {detailed_mismatch_count}")

        mismatches_for_export = joined.filter(export_row_condition)

        mismatch_name_exprs = []
        mismatch_indicator_exprs = []
        for col_name in export_columns:
            if can_use_keys:
                col_mismatch = both_sides_present & (~col(f"df1__{col_name}").eqNullSafe(col(f"df2__{col_name}")))
            else:
                col_mismatch = ~col(f"df1.{col_name}").eqNullSafe(col(f"df2.{col_name}"))
            mismatch_name_exprs.append(when(col_mismatch, lit(col_name)))
            mismatch_indicator_exprs.append(
                spark_sum(when(col_mismatch, lit(1)).otherwise(lit(0))).alias(col_name)
            )

        mismatch_counts = mismatches_for_export.select(*mismatch_indicator_exprs).first()
        distinct_mismatch_columns = [
            col_name for col_name in export_columns if mismatch_counts[col_name] and mismatch_counts[col_name] > 0
        ]

        if can_use_keys:
            mismatch_name_exprs.append(when(col("_dq_in_df1").isNull(), lit("__missing_in_df1__")))
            mismatch_name_exprs.append(when(col("_dq_in_df2").isNull(), lit("__missing_in_df2__")))
            missing_in_df1_count = mismatches_for_export.filter(col("_dq_in_df1").isNull()).count()
            missing_in_df2_count = mismatches_for_export.filter(col("_dq_in_df2").isNull()).count()
            if missing_in_df1_count > 0:
                distinct_mismatch_columns.append("__missing_in_df1__")
            if missing_in_df2_count > 0:
                distinct_mismatch_columns.append("__missing_in_df2__")

        self._log(f"Distinct mismatch columns: {distinct_mismatch_columns}")

        mismatched_columns_expr = concat_ws("||", *mismatch_name_exprs).alias("mismatched_columns")

        if can_use_keys:
            pk_identifier_expr = concat_ws(
                "||",
                *[coalesce(col(f"df1__{k}"), col(f"df2__{k}")).cast("string") for k in key_columns],
            ).alias("pk_identifier")

            df1_export_rows = (
                mismatches_for_export
                .filter(col("_dq_in_df1").isNotNull())
                .select(
                    *[col(f"df1__{c}").alias(c) for c in common_columns],
                    pk_identifier_expr,
                    mismatched_columns_expr,
                    lit("df1").alias("row_source"),
                )
            )
            df2_export_rows = (
                mismatches_for_export
                .filter(col("_dq_in_df2").isNotNull())
                .select(
                    *[col(f"df2__{c}").alias(c) for c in common_columns],
                    pk_identifier_expr,
                    mismatched_columns_expr,
                    lit("df2").alias("row_source"),
                )
            )
        else:
            pk_identifier_expr = col("row_num").cast("string").alias("pk_identifier")

            df1_export_rows = mismatches_for_export.select(
                *[col(f"df1.{c}").alias(c) for c in common_columns],
                pk_identifier_expr,
                mismatched_columns_expr,
                lit("df1").alias("row_source"),
            )
            df2_export_rows = mismatches_for_export.select(
                *[col(f"df2.{c}").alias(c) for c in common_columns],
                pk_identifier_expr,
                mismatched_columns_expr,
                lit("df2").alias("row_source"),
            )

        mismatch_export_df = df1_export_rows.unionByName(df2_export_rows)
        self._write_single_csv(mismatch_export_df.orderBy(col("pk_identifier")), mismatch_output_path)
        self._log(f"Saved detailed mismatches to {mismatch_output_path}")

        if detailed_mismatch_count == 0:
            return total_mismatches, {}

        mismatch_samples: Dict[str, List[Dict]] = {}

        for col_name in compare_columns:
            if can_use_keys:
                column_mismatch_condition = both_sides_present & (
                    ~col(f"df1__{col_name}").eqNullSafe(col(f"df2__{col_name}"))
                )
                df1_value_col = col(f"df1__{col_name}")
                df2_value_col = col(f"df2__{col_name}")
            else:
                column_mismatch_condition = ~col(f"df1.{col_name}").eqNullSafe(col(f"df2.{col_name}"))
                df1_value_col = col(f"df1.{col_name}")
                df2_value_col = col(f"df2.{col_name}")

            samples = (
                joined.filter(column_mismatch_condition)
                .select(
                    *identifier_select,
                    df1_value_col.alias("df1_value"),
                    df2_value_col.alias("df2_value"),
                )
                .limit(3)
                .collect()
            )

            if not samples:
                continue

            formatted_samples: List[Dict] = []
            for row in samples:
                if can_use_keys:
                    row_identifier = {k: row[f"pk_{k}"] for k in key_columns}
                else:
                    row_identifier = {"row_num": row["row_num"]}

                formatted_samples.append(
                    {
                        "row_identifier": row_identifier,
                        "df1_value": row["df1_value"],
                        "df2_value": row["df2_value"],
                    }
                )

            mismatch_samples[col_name] = formatted_samples

        return total_mismatches, mismatch_samples
