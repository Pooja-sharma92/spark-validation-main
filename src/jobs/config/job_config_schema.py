"""
Pydantic Configuration Schema for STG_XTC_LD_TEST_JOB

This module defines the configuration data models with type validation
using Pydantic. It ensures configuration files are correct and provides
IDE autocomplete support.

Usage:
    from config.job_config_schema import JobConfig
    config = JobConfig(**yaml_data)
"""

from pydantic import BaseModel, Field, field_validator
from typing import Dict, List, Optional, Any


class CatalogConfig(BaseModel):
    """Iceberg catalog configuration"""
    type: str = Field(..., description="Catalog type: hive or hadoop")
    uri: Optional[str] = Field(None, description="Metastore URI (for hive catalog)")
    warehouse: str = Field(..., description="Warehouse path")
    cache_enabled: bool = Field(False, description="Enable metadata cache")
    default_catalog: bool = Field(True, description="Set as default catalog")
    write_format: str = Field("parquet", description="Default write format")
    write_target_file_size_bytes: int = Field(134217728, description="Target file size for writes")
    write_metadata_compression_codec: str = Field("gzip", description="Metadata compression codec")


class PerformanceConfig(BaseModel):
    """Spark performance tuning configuration"""
    # Adaptive Query Execution
    adaptive_enabled: bool = Field(True, description="Enable AQE")
    adaptive_shuffle_target_post_shuffle_input_size: str = Field("64MB")
    adaptive_coalesce_partitions_enabled: bool = Field(True)
    adaptive_skew_join_enabled: bool = Field(True)

    # Shuffle partitioning
    shuffle_partitions: int = Field(200, description="Number of shuffle partitions")
    files_max_partition_bytes: int = Field(134217728, description="Max partition bytes (128MB)")
    files_open_cost_in_bytes: int = Field(4194304, description="File open cost (4MB)")

    # SQL Settings
    case_sensitive: bool = Field(False, description="SQL case sensitivity")
    sources_partition_overwrite_mode: str = Field("dynamic", description="Partition overwrite mode")
    iceberg_merge_schema: bool = Field(True, description="Enable schema merge for Iceberg")


class SparkConfig(BaseModel):
    """Spark session configuration"""
    app_name: str = Field(..., description="Spark application name")
    master: str = Field("yarn", description="Spark master URL")
    catalogs: Dict[str, CatalogConfig] = Field(..., description="Catalog configurations")
    performance: PerformanceConfig = Field(..., description="Performance tuning settings")


class DatabasesConfig(BaseModel):
    """Database and schema configuration"""
    source_schema: str = Field(..., description="Source database schema")
    dim_schema: str = Field(..., description="Dimension database schema")
    target_schema: str = Field(..., description="Target database schema")


class SourceTablesConfig(BaseModel):
    """Source table names configuration"""
    loan_master: str = Field(..., description="Main loan account table")
    psl_lookup: str = Field(..., description="PSL code lookup table")
    advance_detail: str = Field(..., description="Advance detail table")
    loan_account_detail: str = Field(..., description="Loan account detail table")
    customer_balance: str = Field(..., description="Customer total balance table")
    loan_customer_balance: str = Field(..., description="Loan customer balance table")
    branch_master: str = Field(..., description="Branch master dimension table")
    date_dimension: str = Field(..., description="Date dimension table")


class TargetTablesConfig(BaseModel):
    """Target table names configuration"""
    fact_table: str = Field(..., description="Target fact table name")


class TablesConfig(BaseModel):
    """Table configuration"""
    sources: SourceTablesConfig = Field(..., description="Source table names")
    target: TargetTablesConfig = Field(..., description="Target table names")


class FiltersConfig(BaseModel):
    """Filter and business logic configuration"""
    status_codes: List[str] = Field(..., description="Status codes for loan filtering")
    min_date: str = Field("1899-12-31", description="Historical missing date default")
    max_date: str = Field("2999-12-31", description="Future date default")
    latest_flag: str = Field("Y", description="Latest record flag")
    customer_balance_table_id: int = Field(2, description="Customer balance table ID")

    @field_validator('status_codes')
    @classmethod
    def validate_status_codes(cls, v):
        if not v:
            raise ValueError("status_codes cannot be empty")
        return v


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = Field("INFO", description="Log level: DEBUG, INFO, WARN, ERROR")
    format: str = Field("json", description="Log format: json or text")
    log_schemas: bool = Field(True, description="Log DataFrame schemas")
    log_row_counts: bool = Field(True, description="Log row counts")
    log_execution_time: bool = Field(True, description="Log execution time")

    @field_validator('level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()


class OutputConfig(BaseModel):
    """Output configuration"""
    reject_file_path: str = Field(..., description="Base path for reject files")
    fact_file_pattern: str = Field(
        "GDM_LON_AGMNT_WKLY_FT_{business_date}.parquet",
        description="Fact file name pattern"
    )
    transform_file_pattern: str = Field(
        "Trf_GDM_TFR_LON_AGMNT_WKLY_FT_{business_date}.parquet",
        description="Transform file name pattern"
    )
    write_mode: str = Field("overwrite", description="Write mode: overwrite, append")


class JobConfig(BaseModel):
    """
    Complete job configuration model.

    This is the root configuration object that contains all
    settings for the STG_XTC_LD_TEST_JOB.

    Usage:
        config = JobConfig(**yaml_data)
        print(config.databases.source_schema)
        print(config.tables.sources.loan_master)
    """
    metadata: Dict[str, Any] = Field(..., description="Job metadata")
    spark: SparkConfig = Field(..., description="Spark configuration")
    databases: DatabasesConfig = Field(..., description="Database configuration")
    tables: TablesConfig = Field(..., description="Table configuration")
    filters: FiltersConfig = Field(..., description="Filter configuration")
    logging: LoggingConfig = Field(..., description="Logging configuration")
    output: OutputConfig = Field(..., description="Output configuration")

    class Config:
        extra = "forbid"  # Raise error if unknown fields are present
        validate_assignment = True  # Validate on assignment
        str_strip_whitespace = True  # Strip whitespace from strings

    def get_table_fqn(self, table_type: str, table_name: str) -> str:
        """
        Get fully qualified table name.

        Args:
            table_type: "source" or "target"
            table_name: Name of the table (e.g., "loan_master")

        Returns:
            Fully qualified table name (e.g., "IBM_TEST.LOAN_MASTER")
        """
        if table_type == "source":
            schema = self.databases.source_schema
            table = getattr(self.tables.sources, table_name)
            return f"{schema}.{table}"
        elif table_type == "target":
            schema = self.databases.target_schema
            table = self.tables.target.fact_table
            return f"{schema}.{table}"
        else:
            raise ValueError(f"Invalid table_type: {table_type}")

    def get_output_path(self, pattern_name: str, business_date: str) -> str:
        """
        Get output file path with business date substituted.

        Args:
            pattern_name: "fact_file" or "transform_file"
            business_date: Business date string (e.g., "2024-12-31")

        Returns:
            Complete output path
        """
        if pattern_name == "fact_file":
            pattern = self.output.fact_file_pattern
        elif pattern_name == "transform_file":
            pattern = self.output.transform_file_pattern
        else:
            raise ValueError(f"Invalid pattern_name: {pattern_name}")

        filename = pattern.format(business_date=business_date)
        return f"{self.output.reject_file_path}/GDM/{filename}"
