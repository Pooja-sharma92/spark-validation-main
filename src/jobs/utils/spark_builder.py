"""
Spark Session Builder for STG_XTC_LD_TEST_JOB

This module provides a configuration-driven Spark session factory.
All Spark configuration is read from JobConfig, eliminating hardcoded values.

Usage:
    from utils.config_loader import load_config
    from utils.spark_builder import SparkBuilder

    config = load_config(environment="prod")
    spark = SparkBuilder.build(config)
"""

from pyspark.sql import SparkSession
from typing import Optional
import sys
from pathlib import Path

# Add parent directory to import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.job_config_schema import JobConfig


class SparkBuilder:
    """
    Spark session factory that builds sessions from JobConfig.

    All Spark configurations are driven by the YAML config files,
    making it easy to switch between environments.
    """

    @staticmethod
    def build(config: JobConfig, enable_hive: bool = True) -> SparkSession:
        """
        Build Spark session from configuration.

        Args:
            config: JobConfig instance with all settings
            enable_hive: Whether to enable Hive support (default: True)

        Returns:
            Configured SparkSession instance

        Example:
            config = load_config(environment="prod")
            spark = SparkBuilder.build(config)
        """
        # Start with app name and master
        builder = SparkSession.builder \
            .appName(config.spark.app_name) \
            .master(config.spark.master)

        # Configure Iceberg catalogs
        builder = SparkBuilder._configure_catalogs(builder, config)

        # Configure performance settings
        builder = SparkBuilder._configure_performance(builder, config)

        # Enable Hive support if requested
        if enable_hive:
            builder = builder.enableHiveSupport()

        # Create and return session
        return builder.getOrCreate()

    @staticmethod
    def _configure_catalogs(
        builder: SparkSession.Builder,
        config: JobConfig
    ) -> SparkSession.Builder:
        """
        Configure Iceberg catalogs.

        Args:
            builder: Spark session builder
            config: JobConfig instance

        Returns:
            Updated builder
        """
        for catalog_name, catalog_config in config.spark.catalogs.items():
            # Base Iceberg SparkCatalog binding
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog"
            )

            cat_type = str(getattr(catalog_config, "type", "") or "").strip().lower()

            # ✅ IMPORTANT:
            # If using REST catalog, DO NOT set spark.sql.catalog.<cat>.type, because it conflicts
            # with the catalog-impl based configuration and triggers:
            #   "both type and catalog-impl are set"
            if cat_type == "rest":
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.catalog-impl",
                    "org.apache.iceberg.rest.RESTCatalog"
                )
                if getattr(catalog_config, "uri", None):
                    builder = builder.config(
                        f"spark.sql.catalog.{catalog_name}.uri",
                        catalog_config.uri
                    )

                # Optional S3/MinIO IO layer (env defaults are safe for dev)
                s3_endpoint = os.environ.get("ICEBERG_S3_ENDPOINT", "http://minio:9000")
                builder = builder.config(
                    f"spark.sql.catalog.{catalog_name}.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO"
                )
                builder = builder.config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", s3_endpoint)
                builder = builder.config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")

                # Spark S3A (needed by Spark read/write too)
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
                builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            else:
                # Default behavior for non-REST catalogs (hive/hadoop/etc)
                if cat_type:
                    builder = builder.config(
                        f"spark.sql.catalog.{catalog_name}.type",
                        cat_type
                    )
                if getattr(catalog_config, "uri", None):
                    builder = builder.config(
                        f"spark.sql.catalog.{catalog_name}.uri",
                        catalog_config.uri
                    )
            # Warehouse path
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                catalog_config.warehouse
            )

            # Cache enabled
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.cache-enabled",
                str(catalog_config.cache_enabled).lower()
            )

            # Write format
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.write-format.default",
                catalog_config.write_format
            )

            # Write target file size
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.write.target-file-size-bytes",
                str(catalog_config.write_target_file_size_bytes)
            )

            # Write metadata compression
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.write.metadata.compression-codec",
                catalog_config.write_metadata_compression_codec
            )

            # Set as default catalog if configured
            if catalog_config.default_catalog:
                builder = builder.config("spark.sql.defaultCatalog", catalog_name)

        # Iceberg Spark extensions
        builder = builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        return builder

    @staticmethod
    def _configure_performance(
        builder: SparkSession.Builder,
        config: JobConfig
    ) -> SparkSession.Builder:
        """
        Configure Spark performance settings.

        Args:
            builder: Spark session builder
            config: JobConfig instance

        Returns:
            Updated builder
        """
        perf = config.spark.performance

        # Adaptive Query Execution (AQE)
        builder = builder.config(
            "spark.sql.adaptive.enabled",
            str(perf.adaptive_enabled).lower()
        )
        builder = builder.config(
            "spark.sql.adaptive.shuffle.targetPostShuffleInputSize",
            perf.adaptive_shuffle_target_post_shuffle_input_size
        )
        builder = builder.config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            str(perf.adaptive_coalesce_partitions_enabled).lower()
        )
        builder = builder.config(
            "spark.sql.adaptive.skewJoin.enabled",
            str(perf.adaptive_skew_join_enabled).lower()
        )

        # Shuffle partitioning
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(perf.shuffle_partitions)
        )
        builder = builder.config(
            "spark.sql.files.maxPartitionBytes",
            str(perf.files_max_partition_bytes)
        )
        builder = builder.config(
            "spark.sql.files.openCostInBytes",
            str(perf.files_open_cost_in_bytes)
        )

        # SQL Settings
        builder = builder.config(
            "spark.sql.caseSensitive",
            str(perf.case_sensitive).lower()
        )
        builder = builder.config(
            "spark.sql.sources.partitionOverwriteMode",
            perf.sources_partition_overwrite_mode
        )
        builder = builder.config(
            "spark.sql.iceberg.merge-schema",
            str(perf.iceberg_merge_schema).lower()
        )

        return builder

    @staticmethod
    def get_or_create(config: JobConfig) -> SparkSession:
        """
        Get existing Spark session or create a new one.

        This is a convenience method that always tries to get
        an existing session first.

        Args:
            config: JobConfig instance

        Returns:
            SparkSession instance
        """
        try:
            # Try to get existing session
            return SparkSession.getActiveSession()
        except Exception:
            # No active session, create new one
            return SparkBuilder.build(config)

    @staticmethod
    def stop_session():
        """
        Stop the active Spark session if one exists.

        This is useful for cleanup in tests and scripts.
        """
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()


# Convenience function
def build_spark(config: JobConfig, enable_hive: bool = True) -> SparkSession:
    """
    Convenience function to build Spark session.

    Args:
        config: JobConfig instance
        enable_hive: Whether to enable Hive support

    Returns:
        SparkSession instance

    Example:
        from utils.config_loader import load_config
        from utils.spark_builder import build_spark

        config = load_config(environment="prod")
        spark = build_spark(config)
    """
    return SparkBuilder.build(config, enable_hive=enable_hive)
