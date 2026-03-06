"""
Type Casting Utilities for STG_XTC_LD_TEST_JOB

This module provides utilities to eliminate duplicated type conversion code.
Instead of repeating 13+ selectExpr blocks with CAST statements, all schemas
are defined once in JobSchemas and applied via TypeCaster.

Benefits:
- Single source of truth for DataFrame schemas
- Modify type once, applies everywhere
- Easy to test and validate
- Self-documenting code

Usage:
    from utils.type_casting import TypeCaster, JobSchemas

    # Cast DataFrame using predefined schema
    df = TypeCaster.cast_dataframe(df, JobSchemas.ACCOUNT_DETAIL)

    # Or cast with custom schema
    df = TypeCaster.cast_dataframe(df, {
        'ACCT_NBR': 'STRING',
        'AMOUNT': 'DECIMAL(17,3)'
    })
"""

from pyspark.sql import DataFrame
from typing import Dict, List


class JobSchemas:
    """
    Schema definitions for all DataFrames in STG_XTC_LD_TEST_JOB.

    Each constant defines the column names and their target data types.
    This eliminates the need to repeat CAST statements throughout the code.
    """

    # V138S0: Account Detail (Driving DataFrame)
    ACCOUNT_DETAIL = {
        'ACCT_NBR': 'STRING',
        'NEW_CAPN_AMT': 'DECIMAL(17,3)',
        'OLD_CAPN_AMT': 'DECIMAL(17,3)',
        'ACTIVITY_CODE': 'STRING',
        'SBSD_SCHEME_CD': 'STRING',
        'IND_CODE': 'STRING',
        'SUB_IND_CODE': 'STRING',
        'SUB_SUB_IND_CODE': 'STRING',
        'SPONSOR_ID': 'STRING',
        'OLD_BRANCH': 'STRING'
    }

    # V0S108: PSL Lookup
    PSL_LOOKUP = {
        'ACCT_NBR': 'STRING',
        'CUST_NBR': 'STRING',
        'PSL_CODE': 'STRING'
    }

    # V0S126: Advance Detail (Before Aggregation)
    ADVANCE_DETAIL_RAW = {
        'ACCT_NBR': 'STRING',
        'FIRST_AMT_ADV': 'DECIMAL(25,5)',
        'FIRST_ADV_DATE': 'INTEGER'
    }

    # V199S0: Advance Detail (After Aggregation)
    ADVANCE_DETAIL_AGG = {
        'ACCT_NBR': 'STRING',
        'FIRST_ADV_DATE': 'INTEGER',
        'FIRST_AMT_ADV': 'DECIMAL(25,5)'
    }

    # V0S132: Customer Total Balance
    CUSTOMER_BALANCE = {
        'CUST_NBR': 'STRING',
        'CUST_TOT_BAL': 'DECIMAL(25,5)'
    }

    # V154S0: Branch Master Dimension
    BRANCH_MASTER = {
        'BRNCH_NBR': 'STRING',
        'CRCL_CD': 'SHORT',
        'BRNCH_NBR_SKEY': 'LONG'
    }

    # V155S0: Customer Branch (Copy of Branch Master)
    CUSTOMER_BRANCH = {
        'BRNCH_NBR': 'STRING',
        'CRCL_CD': 'SHORT',
        'BRNCH_NBR_SKEY': 'LONG'
    }

    # V155S0: Old Branch (Copy of Branch Master)
    OLD_BRANCH = {
        'BRNCH_NBR': 'STRING',
        'CRCL_CD': 'SHORT',
        'BRNCH_NBR_SKEY': 'LONG'
    }

    # V0S2: First Join Result (Account + PSL + Advance)
    JOIN_STAGE_1 = {
        'ACCT_NBR': 'STRING',
        'NEW_CAPN_AMT': 'DECIMAL(17,3)',
        'OLD_CAPN_AMT': 'DECIMAL(17,3)',
        'ACTIVITY_CODE': 'STRING',
        'SBSD_SCHEME_CD': 'STRING',
        'IND_CODE': 'STRING',
        'SUB_IND_CODE': 'STRING',
        'SUB_SUB_IND_CODE': 'STRING',
        'SPONSOR_ID': 'STRING',
        'OLD_BRANCH': 'STRING',
        'CUST_NBR': 'STRING',
        'PSL_CODE': 'STRING',
        'FIRST_ADV_DATE': 'INTEGER',
        'FIRST_AMT_ADV': 'DECIMAL(25,5)'
    }

    # V0S29: Second Join Result (+ Customer Balance)
    JOIN_STAGE_2 = {
        'ACCT_NBR': 'STRING',
        'NEW_CAPN_AMT': 'DECIMAL(17,3)',
        'OLD_CAPN_AMT': 'DECIMAL(17,3)',
        'ACTIVITY_CODE': 'STRING',
        'SBSD_SCHEME_CD': 'STRING',
        'IND_CODE': 'STRING',
        'SUB_IND_CODE': 'STRING',
        'SUB_SUB_IND_CODE': 'STRING',
        'SPONSOR_ID': 'STRING',
        'OLD_BRANCH': 'STRING',
        'CUST_NBR': 'STRING',
        'PSL_CODE': 'STRING',
        'FIRST_ADV_DATE': 'INTEGER',
        'FIRST_AMT_ADV': 'DECIMAL(25,5)',
        'CUST_TOT_BAL': 'DECIMAL(25,5)'
    }

    # Final Join Result (+ Branch Keys)
    JOIN_FINAL = {
        'OLD_ACCT_BRNCH_CD_SKEY': 'LONG',
        'CUST_HOME_BR_SKEY': 'LONG',
        'ACCT_NBR': 'STRING',
        'NEW_CAPN_AMT': 'DECIMAL(17,3)',
        'OLD_CAPN_AMT': 'DECIMAL(17,3)',
        'ACTIVITY_CODE': 'STRING',
        'SBSD_SCHEME_CD': 'STRING',
        'IND_CODE': 'STRING',
        'SUB_IND_CODE': 'STRING',
        'SUB_SUB_IND_CODE': 'STRING',
        'SPONSOR_ID': 'STRING',
        'OLD_BRANCH': 'STRING',
        'CUST_NBR': 'STRING',
        'PSL_CODE': 'STRING',
        'FIRST_ADV_DATE': 'INTEGER',
        'FIRST_AMT_ADV': 'DECIMAL(25,5)',
        'CUST_TOT_BAL': 'DECIMAL(25,5)'
    }

    # V157S0: Output to Fact Table
    FACT_TABLE = JOIN_FINAL  # Same as final join

    # V157S0: Reject Output
    REJECT_OUTPUT = JOIN_FINAL  # Same as final join


class TypeCaster:
    """
    Utility class for applying type conversions to DataFrames.

    This replaces repetitive selectExpr(CAST...) blocks with a single
    method call using predefined schemas.
    """

    @staticmethod
    def cast_dataframe(df: DataFrame, schema: Dict[str, str]) -> DataFrame:
        """
        Cast DataFrame columns according to schema definition.

        Args:
            df: Input DataFrame
            schema: Dictionary mapping column names to SQL data types
                   Example: {'ACCT_NBR': 'STRING', 'AMOUNT': 'DECIMAL(17,3)'}

        Returns:
            DataFrame with columns cast to specified types

        Example:
            # Old way (repetitive):
            df = df.selectExpr(
                "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
                "CAST(AMOUNT AS DECIMAL(17,3)) AS AMOUNT"
            )

            # New way (concise):
            df = TypeCaster.cast_dataframe(df, {
                'ACCT_NBR': 'STRING',
                'AMOUNT': 'DECIMAL(17,3)'
            })
        """
        cast_exprs = [
            f"CAST({col} AS {dtype}) AS {col}"
            for col, dtype in schema.items()
        ]
        return df.selectExpr(*cast_exprs)

    @staticmethod
    def cast_columns(
        df: DataFrame,
        columns: Dict[str, str],
        keep_other_columns: bool = True
    ) -> DataFrame:
        """
        Cast specific columns while optionally keeping other columns.

        Args:
            df: Input DataFrame
            columns: Dictionary mapping column names to types to cast
            keep_other_columns: If True, keeps columns not in the cast dict

        Returns:
            DataFrame with specified columns cast

        Example:
            # Cast only AMOUNT column, keep others unchanged
            df = TypeCaster.cast_columns(
                df,
                {'AMOUNT': 'DECIMAL(17,3)'},
                keep_other_columns=True
            )
        """
        if keep_other_columns:
            # Build selectExpr list with casts and pass-through columns
            select_exprs = []
            for col in df.columns:
                if col in columns:
                    select_exprs.append(f"CAST({col} AS {columns[col]}) AS {col}")
                else:
                    select_exprs.append(col)
            return df.selectExpr(*select_exprs)
        else:
            # Only cast specified columns
            return TypeCaster.cast_dataframe(df, columns)

    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: Dict[str, str]) -> List[str]:
        """
        Validate that DataFrame has expected columns.

        Args:
            df: Input DataFrame
            expected_schema: Dictionary with expected column names and types

        Returns:
            List of validation error messages (empty if valid)

        Example:
            errors = TypeCaster.validate_schema(df, JobSchemas.ACCOUNT_DETAIL)
            if errors:
                raise ValueError(f"Schema validation failed: {errors}")
        """
        errors = []
        df_columns = set(df.columns)
        expected_columns = set(expected_schema.keys())

        # Check for missing columns
        missing = expected_columns - df_columns
        if missing:
            errors.append(f"Missing columns: {sorted(missing)}")

        # Check for extra columns
        extra = df_columns - expected_columns
        if extra:
            errors.append(f"Extra columns: {sorted(extra)}")

        return errors

    @staticmethod
    def get_schema_info(schema_name: str) -> Dict[str, str]:
        """
        Get schema definition by name.

        Args:
            schema_name: Name of schema constant in JobSchemas

        Returns:
            Schema dictionary

        Example:
            schema = TypeCaster.get_schema_info("ACCOUNT_DETAIL")
        """
        if not hasattr(JobSchemas, schema_name):
            raise ValueError(f"Unknown schema: {schema_name}")
        return getattr(JobSchemas, schema_name)

    @staticmethod
    def list_available_schemas() -> List[str]:
        """
        List all available schema names.

        Returns:
            List of schema constant names

        Example:
            schemas = TypeCaster.list_available_schemas()
            print(f"Available schemas: {schemas}")
        """
        return [
            name for name in dir(JobSchemas)
            if not name.startswith('_') and name.isupper()
        ]


# Convenience function
def cast_df(df: DataFrame, schema_name_or_dict) -> DataFrame:
    """
    Convenience function to cast DataFrame.

    Args:
        df: Input DataFrame
        schema_name_or_dict: Either a schema name (string) or schema dict

    Returns:
        Cast DataFrame

    Example:
        # Using schema name
        df = cast_df(df, "ACCOUNT_DETAIL")

        # Using schema dict
        df = cast_df(df, {'ACCT_NBR': 'STRING'})
    """
    if isinstance(schema_name_or_dict, str):
        schema = TypeCaster.get_schema_info(schema_name_or_dict)
    else:
        schema = schema_name_or_dict

    return TypeCaster.cast_dataframe(df, schema)
