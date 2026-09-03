"""
SchemaComparator module for comparing DataFrame schemas.
"""

from typing import List, Tuple, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DataType


class SchemaComparator:
    """
    A class to compare schemas of two DataFrames.
    Follows Single Responsibility Principle.
    """

    def compare_schemas(
        self, 
        df1: DataFrame, 
        df2: DataFrame, 
        exclude_columns: Optional[list[str]] = None
    ) -> Tuple[bool, Dict[str, any]]:
        """
        Compare the schemas of two DataFrames.

        Args:
            df1: First DataFrame.
            df2: Second DataFrame.

        Returns:
            Tuple of (is_match, differences) where:
            - is_match: Boolean indicating if schemas match
            - differences: Dictionary containing details about mismatches
        """

        def exclude_cols( df:DataFrame, exclude_columns:list[str]):
            for column_to_exclude in exclude_columns:
                if column_to_exclude in df.columns:
                    df = df.drop(column_to_exclude)
            return df

        if exclude_columns:
            df1 = exclude_cols(df1, exclude_columns)
            df2 = exclude_cols(df2, exclude_columns)
        

        schema1 = df1.schema
        schema2 = df2.schema

        columns1 = set(df1.columns)
        columns2 = set(df2.columns)
        
        differences = {
            "missing_in_df2": list(columns1 - columns2),
            "missing_in_df1": list(columns2 - columns1),
            "type_mismatches": [],
        }

        # Check for type mismatches in common columns
        for column in df1.columns:
            if column in df2.columns:
                type1 = df1.schema[column].dataType
                type2 = df2.schema[column].dataType
                if type1 != type2:
                    differences["type_mismatches"].append(
                        {
                            "column": column,
                            "df1_type": str(type1),
                            "df2_type": str(type2),
                        }
                    )

        is_match = (
            len(differences["missing_in_df2"]) == 0
            and len(differences["missing_in_df1"]) == 0
            #and len(differences["type_mismatches"]) == 0
        )

        return is_match, differences