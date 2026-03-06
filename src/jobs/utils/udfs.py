"""
User-Defined Functions (UDFs) for STG_XTC_LD_TEST_JOB

This module provides UDF registration and definitions.
All UDFs are registered centrally to ensure they're available
across all SQL queries.

Usage:
    from utils.udfs import register_udfs

    spark = SparkSession.builder.getOrCreate()
    register_udfs(spark)

    # Now UDFs are available in SQL
    df = spark.sql("SELECT udf_to_number('20241231') as date_num")
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType
from typing import Optional


def udf_to_number_impl(value: Optional[str]) -> Optional[int]:
    """
    Convert string to number, handling nulls and invalid values.

    This UDF is used in the original job to convert date strings
    (in YYYYMMDD format) to integers.

    Args:
        value: String value to convert

    Returns:
        Integer value or None if conversion fails

    Examples:
        udf_to_number("20241231") -> 20241231
        udf_to_number("abc") -> None
        udf_to_number(None) -> None
    """
    if value is None:
        return None

    try:
        # Remove whitespace
        value = value.strip()

        # Try to convert to int
        return int(value)
    except (ValueError, AttributeError):
        # Return None for invalid conversions
        return None


def register_udfs(spark: SparkSession):
    """
    Register all UDFs with the Spark session.

    This function should be called once after creating the Spark session
    and before executing any SQL queries that use these UDFs.

    Args:
        spark: SparkSession instance

    Example:
        spark = SparkSession.builder.getOrCreate()
        register_udfs(spark)
    """
    # Register udf_to_number
    spark.udf.register(
        "udf_to_number",
        udf_to_number_impl,
        IntegerType()
    )

    # Add more UDFs here as needed
    # Example:
    # spark.udf.register("udf_clean_string", clean_string_impl, StringType())


def get_udf_functions():
    """
    Get dictionary of all UDF functions for testing.

    Returns:
        Dictionary mapping UDF names to their implementation functions

    Example:
        udfs = get_udf_functions()
        result = udfs['udf_to_number']("20241231")
        assert result == 20241231
    """
    return {
        'udf_to_number': udf_to_number_impl,
    }


# Additional UDF implementations can be added below
# ===================================================

def clean_string_impl(value: Optional[str]) -> Optional[str]:
    """
    Clean string value (example UDF).

    Args:
        value: String to clean

    Returns:
        Cleaned string or None
    """
    if value is None:
        return None

    # Remove leading/trailing whitespace
    value = value.strip()

    # Return None for empty strings
    return value if value else None


def safe_divide_impl(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """
    Safely divide two numbers, handling division by zero.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Division result or None if denominator is zero or inputs are None
    """
    if numerator is None or denominator is None:
        return None

    if denominator == 0:
        return None

    return numerator / denominator


# Note: Additional UDFs can be registered by adding them to register_udfs()
# and implementing them above.
