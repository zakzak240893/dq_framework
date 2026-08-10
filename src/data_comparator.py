"""
DataComparator module for row-by-row data comparison.
"""

from typing import List, Dict, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window


class DataComparator:
    """
    A class to compare data row by row between two DataFrames.
    Follows Single Responsibility Principle.
    """

    def compare_data(
        self,
        df1: DataFrame,
        df2: DataFrame,
        exclude_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
    ) -> Tuple[int, Dict[str, List[Dict]]]:
        """
        Compare data row by row between two DataFrames.

        Args:
            df1: First DataFrame.
            df2: Second DataFrame.
            exclude_columns: List of columns to exclude from comparison.
            key_columns: Optional key columns used to align rows across DataFrames.

        Returns:
            Tuple of (total_mismatches, mismatch_samples) where:
            - total_mismatches: Total number of rows with at least one mismatch
            - mismatch_samples: Dictionary mapping column names to up to 3 example mismatches
              including row identifiers and mismatched values.
        """
        if exclude_columns is None:
            exclude_columns = []

        key_columns = key_columns or []

        # Columns to compare
        compare_columns = [c for c in df1.columns if c not in exclude_columns]

        if not compare_columns:
            return 0, {}

        can_use_keys = bool(key_columns) and all(k in df1.columns and k in df2.columns for k in key_columns)

        if can_use_keys:
            join_condition = None
            for key_col in key_columns:
                key_match = col(f"df1.{key_col}").eqNullSafe(col(f"df2.{key_col}"))
                join_condition = key_match if join_condition is None else (join_condition & key_match)
            joined = df1.alias("df1").join(df2.alias("df2"), on=join_condition, how="inner")
            identifier_select = [col(f"df1.{k}").alias(f"pk_{k}") for k in key_columns]
        else:
            # Fallback alignment when no reliable key columns are available.
            window_spec = Window.orderBy(lit(1))
            df1_with_row = df1.withColumn("row_num", row_number().over(window_spec))
            df2_with_row = df2.withColumn("row_num", row_number().over(window_spec))
            joined = df1_with_row.alias("df1").join(df2_with_row.alias("df2"), on="row_num", how="inner")
            identifier_select = [col("row_num")]

        comparison_expr = None
        for col_name in compare_columns:
            df1_col = col(f"df1.{col_name}")
            df2_col = col(f"df2.{col_name}")
            col_match = df1_col.eqNullSafe(df2_col)
            if comparison_expr is None:
                comparison_expr = col_match
            else:
                comparison_expr = comparison_expr & col_match

        mismatched_rows = joined.filter(~comparison_expr)
        total_mismatches = mismatched_rows.count()

        if total_mismatches == 0:
            return 0, {}

        mismatch_samples: Dict[str, List[Dict]] = {}

        for col_name in compare_columns:
            column_mismatch_condition = ~col(f"df1.{col_name}").eqNullSafe(col(f"df2.{col_name}"))

            samples = (
                joined.filter(column_mismatch_condition)
                .select(
                    *identifier_select,
                    col(f"df1.{col_name}").alias("df1_value"),
                    col(f"df2.{col_name}").alias("df2_value"),
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
