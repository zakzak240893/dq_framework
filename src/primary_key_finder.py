"""
PrimaryKeyFinder module for detecting primary keys in a DataFrame.
"""

from typing import List, Tuple, Optional
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import NumericType, DateType, TimestampType, TimestampNTZType
from pyspark.sql.functions import count_distinct as cd


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

    def find_primary_keys(
            self, 
            df: DataFrame,
            skip_columns:Optional[list[str]] = []
        ) -> Tuple[bool, List[str]]:
        """
        Find primary key columns in a DataFrame.

        Algorithm:
        1. Compare row count and distinct row count.
        2. If duplicates exist, log it, drop duplicate rows, and continue.
        3. Use categorical columns first, then datetime, then numeric columns.
        4. Sort categorical columns by cardinality (high to low); sort datetime and
           numeric columns by cardinality (low to high) to defer likely-incidental
           near-unique columns.
        5. Iteratively build a PK list and test uniqueness.

        Args:
            df: Input DataFrame.

        Returns:
            Tuple of (is_pk_found, pk_columns) where:
            - is_pk_found: Boolean indicating if a valid PK was found
            - pk_columns: List of column names that form the primary key
        """
        df = df.cache()
        working_df = df
        try:
            total_rows = df.count()
            distinct_row_count = df.distinct().count()

            if distinct_row_count < total_rows:
                duplicate_count = total_rows - distinct_row_count
                self._log(
                    f"Detected {duplicate_count} duplicate rows before PK inference. "
                    f"Proceeding with deduplicated dataset of {distinct_row_count} rows.",
                    "warning",
                )
                working_df = df.dropDuplicates().cache()
                total_rows = distinct_row_count

            categorical_cols = self._get_categorical_columns(working_df)
            datetime_cols = self._get_datetime_columns(working_df)
            numeric_cols = self._get_numeric_columns(working_df)

            if skip_columns:
                categorical_cols = list(set(categorical_cols) - set(skip_columns))
                datetime_cols = list(set(datetime_cols) - set(skip_columns))
                numeric_cols = list(set(numeric_cols) - set(skip_columns))
            self._log(f"skip_columns: {skip_columns}")

            if (not categorical_cols) and (not datetime_cols) and (not numeric_cols):
                return False, []

            # Single pass: cardinalities for all candidate columns computed together.
            candidate_cols = categorical_cols + datetime_cols + numeric_cols
            cardinality_row = working_df.select([cd(col(c)).alias(c) for c in candidate_cols]).first()
            cardinality_map = {col_name: cardinality_row[col_name] for col_name in candidate_cols}

            sorted_categorical_cols = sorted(
                categorical_cols, key=lambda x: cardinality_map[x], reverse=True
            )
            sorted_dt_cols = sorted(
                datetime_cols, key=lambda x: cardinality_map[x], reverse=False
            )
            sorted_numeric_cols = sorted(
                numeric_cols, key=lambda x: cardinality_map[x], reverse=False
            )
            sorted_cols = sorted_categorical_cols + sorted_dt_cols + sorted_numeric_cols

            # Cardinality of a column prefix only grows (or stays the same) as columns are
            # appended, so once a prefix is unique every longer prefix stays unique.
            best_prefix = self._find_unique_prefix(working_df, sorted_cols, total_rows)

            if best_prefix is not None:
                return True, self._minimize_pk(working_df, best_prefix, total_rows)

            return False, []
        finally:
            working_df.unpersist()
            if working_df is not df:
                df.unpersist()

    def _get_categorical_columns(self, df: DataFrame) -> List[str]:
        """
        Get list of categorical (non-numeric) columns.

        Args:
            df: Input DataFrame.

        Returns:
            List of categorical column names.
        """
        # Numeric columns are handled separately and sorted ascending; see find_primary_keys.
        categorical_cols = []
        for field in df.schema:
            if not isinstance(field.dataType, (DateType, TimestampType, TimestampNTZType,NumericType)):
                categorical_cols.append(field.name)

        return categorical_cols

    def _get_datetime_columns(self, df: DataFrame) -> List[str]:
            """
            Get list of datetime (non-numeric) columns.
    
            Args:
                df: Input DataFrame.
    
            Returns:
                List of datetime column names.
            """
            categorical_cols = []
            for field in df.schema:
                if isinstance(field.dataType, (DateType, TimestampType, TimestampNTZType)):
                    categorical_cols.append(field.name)
    
            return categorical_cols

    def _get_numeric_columns(self, df: DataFrame) -> List[str]:
        """Get list of numeric column names."""
        numeric_cols = []
        for field in df.schema:
            if isinstance(field.dataType, NumericType):
                numeric_cols.append(field.name)

        return numeric_cols

    def _find_unique_prefix(
        self, df: DataFrame, sorted_cols: List[str], total_rows: int
    ) -> Optional[List[str]]:
        """
        Find the shortest prefix of sorted_cols that uniquely identifies every row.

        Cardinality of a growing prefix only increases, so once a prefix is unique every
        longer prefix stays unique too. Binary search the minimal length instead of
        scanning linearly, cutting exact distinct().count() shuffles from O(n) to O(log n).
        """
        if not sorted_cols:
            return None

        lo, hi = 1, len(sorted_cols)
        best_prefix: Optional[List[str]] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            prefix = sorted_cols[:mid]
            if self._check_uniqueness(df, prefix, total_rows):
                best_prefix = prefix
                hi = mid - 1
            else:
                lo = mid + 1

        return best_prefix

    def _check_uniqueness(
        self, df: DataFrame, columns: List[str], expected_count: int
    ) -> bool:
        """
        Check if the given columns uniquely identify every row. Rows with identical
        values (including matching nulls) collapse together and fail the check.

        Args:
            df: Input DataFrame.
            columns: List of column names to check.
            expected_count: Expected number of rows.

        Returns:
            Boolean indicating if uniqueness is guaranteed.
        """
        if not columns:
            return False

        # count_distinct() drops rows with any null column, unlike distinct(); use
        # distinct() here so null handling matches the total_rows/dedup calculation.
        distinct_count = df.select(*[col(col_name) for col_name in columns]).distinct().count()

        return distinct_count == expected_count

    def _minimize_pk(
        self, df: DataFrame, columns: List[str], expected_count: int
    ) -> List[str]:
        """
        Drop columns that aren't needed for uniqueness, trying the least-selective
        (last-added) columns first so the result is an irreducible key.
        """
        minimal = list(columns)
        for col_name in reversed(columns):
            candidate = [c for c in minimal if c != col_name]
            if self._check_uniqueness(df, candidate, expected_count):
                minimal = candidate
        return minimal
