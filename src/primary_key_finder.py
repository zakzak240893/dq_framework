"""
PrimaryKeyFinder module for detecting primary keys in a DataFrame.
"""

from typing import List, Tuple
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import NumericType


class PrimaryKeyFinder:
    """
    A class to find primary keys in a DataFrame using cardinality-based algorithm.
    Follows Single Responsibility Principle.
    """

    def __init__(self):
        self._logger = logging.getLogger("DataQualityFramework")

    def _log(self, message: str, level: str = "info") -> None:
        """Write finder messages to framework logger when available."""
        if level == "warning":
            self._logger.warning(message)
        elif level == "error":
            self._logger.error(message)
        else:
            self._logger.info(message)

    def find_primary_keys(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Find primary key columns in a DataFrame.

        Algorithm:
        1. Compare row count and distinct row count.
        2. If duplicates exist, log it, drop duplicate rows, and continue.
        3. Use categorical (non-numeric) columns first.
        4. Sort candidate columns by cardinality (high to low).
        5. Iteratively build a PK list and test uniqueness.
        6. If categorical columns are insufficient, continue by adding numeric columns.

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (is_pk_found, pk_columns) where:
            - is_pk_found: Boolean indicating if a valid PK was found
            - pk_columns: List of column names that form the primary key
        """
        total_rows = df.count()
        distinct_row_count = df.distinct().count()

        working_df = df
        if distinct_row_count < total_rows:
            duplicate_count = total_rows - distinct_row_count
            self._log(
                f"Detected {duplicate_count} duplicate rows before PK inference. "
                f"Proceeding with deduplicated dataset of {distinct_row_count} rows.",
                "warning",
            )
            working_df = df.dropDuplicates()
            total_rows = distinct_row_count

        categorical_cols = self._get_categorical_columns(working_df)
        numeric_cols = self._get_numeric_columns(working_df)

        candidate_cols = categorical_cols + numeric_cols
        if not candidate_cols:
            return False, []

        # OPTIMIZATION: Calculate all cardinalities in a SINGLE pass
        from pyspark.sql.functions import count_distinct as cd
        cardinality_row = working_df.select([cd(col(c)).alias(c) for c in candidate_cols]).first()
        cardinality_map = {col_name: cardinality_row[col_name] for col_name in candidate_cols}

        sorted_categorical_cols = sorted(
            categorical_cols, key=lambda x: cardinality_map[x], reverse=True
        )
        sorted_numeric_cols = sorted(
            numeric_cols, key=lambda x: cardinality_map[x]
        )

        # Build PK candidates with categorical columns first.
        pk_candidates: List[str] = []
        for col_name in sorted_categorical_cols:
            pk_candidates.append(col_name)
            if self._check_uniqueness(working_df, pk_candidates, total_rows):
                return True, pk_candidates

        # If needed, continue by adding numeric columns from lowest cardinality first.
        for col_name in sorted_numeric_cols:
            pk_candidates.append(col_name)
            if self._check_uniqueness(working_df, pk_candidates, total_rows):
                return True, pk_candidates

        return False, []

    def _get_categorical_columns(self, df: DataFrame) -> List[str]:
        """
        Get list of categorical (non-numeric) columns.

        Args:
            df: Input DataFrame.

        Returns:
            List of categorical column names.
        """
        categorical_cols = []
        for field in df.schema:
            if not isinstance(field.dataType, NumericType):
                categorical_cols.append(field.name)

        return categorical_cols

    def _get_numeric_columns(self, df: DataFrame) -> List[str]:
        """Get list of numeric column names."""
        numeric_cols = []
        for field in df.schema:
            if isinstance(field.dataType, NumericType):
                numeric_cols.append(field.name)

        return numeric_cols

    def _check_uniqueness(
        self, df: DataFrame, columns: List[str], expected_count: int
    ) -> bool:
        """
        Check if the given columns guarantee uniqueness (considering nulls).

        Args:
            df: Input DataFrame.
            columns: List of column names to check.
            expected_count: Expected number of rows.

        Returns:
            Boolean indicating if uniqueness is guaranteed.
        """
        if not columns:
            return False

        # Nulls are allowed in PK columns for this framework.
        # Distinct tuples over selected columns preserve null-bearing combinations.
        distinct_count = df.select(*[col(col_name) for col_name in columns]).distinct().count()

        return distinct_count == expected_count
