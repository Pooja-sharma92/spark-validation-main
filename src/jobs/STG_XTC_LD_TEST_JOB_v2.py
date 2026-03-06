#!/usr/bin/env python3
"""
STG_XTC_LD_TEST_JOB - Version 2.0 (Refactored)

Loan Agreement Weekly Fact ETL Job

Improvements from v1:
- ✅ Configuration externalized to YAML files
- ✅ Eliminated 13+ duplicate type conversions
- ✅ Added structured logging for observability
- ✅ Parameterized SQL queries
- ✅ Backward compatible with legacy JSON parameters
- ✅ Error handling and try-except blocks
- ✅ Easy to test with different environments

Usage:
    # New way (recommended):
    python STG_XTC_LD_TEST_JOB_v2.py \\
        --environment prod \\
        --business-date 2024-12-31 \\
        --reject-file-path /data/rejects

    # Old way (backward compatible):
    python STG_XTC_LD_TEST_JOB_v2.py \\
        --GDM_ParameterSet /path/to/gdm_params.json \\
        --PCS_ParameterSet /path/to/pcs_params.json

    # Mixed way (new params override old):
    python STG_XTC_LD_TEST_JOB_v2.py \\
        --GDM_ParameterSet /path/to/gdm_params.json \\
        --PCS_ParameterSet /path/to/pcs_params.json \\
        --environment prod
"""

import argparse
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.config_loader import ConfigLoader, RuntimeParams
from utils.spark_builder import SparkBuilder
from utils.type_casting import TypeCaster, JobSchemas
from utils.sql_templates import SQLTemplates
from utils.logger import create_logger
from utils.udfs import register_udfs
from pyspark.sql.functions import col, max as spark_max, sum as spark_sum


def parse_args():
    """
    Parse command line arguments.

    Supports both new YAML-based configuration and legacy JSON parameters
    for backward compatibility.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="STG_XTC_LD_TEST_JOB - Loan Agreement Weekly Fact ETL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # New configuration approach
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["dev", "test", "prod"],
        help="Environment to run in (dev/test/prod)"
    )
    parser.add_argument(
        "--business-date",
        type=str,
        help="Business date (YYYY-MM-DD format)"
    )
    parser.add_argument(
        "--reject-file-path",
        type=str,
        help="Base path for reject files"
    )

    # Legacy JSON parameter files (backward compatibility)
    parser.add_argument(
        "--GDM_ParameterSet",
        type=str,
        default="(As pre-defined)",
        help="GDM parameter set JSON file path (legacy)"
    )
    parser.add_argument(
        "--PCS_ParameterSet",
        type=str,
        default="(As pre-defined)",
        help="PCS parameter set JSON file path (legacy)"
    )

    return parser.parse_args()


def main():
    """Main job execution function."""

    # ===================================================================
    # SETUP: Parse arguments and load configuration
    # ===================================================================

    args = parse_args()

    # Load configuration (supports both new YAML and legacy JSON)
    config_loader = ConfigLoader()
    legacy_paths = None

    # Check if legacy JSON files are provided
    if (args.GDM_ParameterSet != "(As pre-defined)" or
        args.PCS_ParameterSet != "(As pre-defined)"):
        legacy_paths = {
            'gdm': args.GDM_ParameterSet if args.GDM_ParameterSet != "(As pre-defined)" else None,
            'pcs': args.PCS_ParameterSet if args.PCS_ParameterSet != "(As pre-defined)" else None
        }

    config = config_loader.load(
        environment=args.environment,
        legacy_json_paths=legacy_paths
    )

    # Get business date (from args or legacy params)
    if args.business_date:
        business_date = args.business_date
    else:
        # Try to get from legacy PCS parameters
        runtime_params = RuntimeParams(config)
        business_date = runtime_params.get_pcs_param("JpBusinessDate")
        if not business_date:
            raise ValueError(
                "Business date not provided. Use --business-date or provide PCS_ParameterSet"
            )

    # Get reject file path (from args or legacy params)
    if args.reject_file_path:
        reject_file_path = args.reject_file_path
    else:
        # Try to get from legacy GDM parameters
        runtime_params = RuntimeParams(config)
        reject_file_path = runtime_params.get_gdm_param("SSRejectFilePath")
        if not reject_file_path:
            # Fall back to config
            reject_file_path = config.output.reject_file_path

    # ===================================================================
    # INITIALIZATION: Spark, Logger, SQL Templates, UDFs
    # ===================================================================

    # Create Spark session from configuration
    spark = SparkBuilder.build(config)

    # Register UDFs
    register_udfs(spark)

    # Create logger
    logger = create_logger(config, "STG_XTC_LD_TEST_JOB")
    logger.log_stage("JOB", "start", business_date=business_date, environment=args.environment)

    # Initialize SQL template generator
    sql_templates = SQLTemplates(config, business_date)

    try:
        # ===================================================================
        # STAGE 1: V138S0 - Account Detail (Driving DataFrame)
        # ===================================================================

        logger.log_stage("V138S0", "start")

        sql = sql_templates.get_account_detail_query()
        logger.log_sql("V138S0", sql)

        Acct_dtal_driving_df = spark.sql(sql)
        Acct_dtal_driving_df = TypeCaster.cast_dataframe(
            Acct_dtal_driving_df,
            JobSchemas.ACCOUNT_DETAIL
        )

        logger.log_dataframe(Acct_dtal_driving_df, "Acct_dtal_driving_df", "V138S0")
        logger.log_stage("V138S0", "complete")

        # ===================================================================
        # STAGE 2: V0S108 - PSL Lookup
        # ===================================================================

        logger.log_stage("V0S108", "start")

        sql = sql_templates.get_psl_lookup_query()
        logger.log_sql("V0S108", sql)

        Psl_df = spark.sql(sql)
        Psl_df = TypeCaster.cast_dataframe(Psl_df, JobSchemas.PSL_LOOKUP)

        logger.log_dataframe(Psl_df, "Psl_df", "V0S108")
        logger.log_stage("V0S108", "complete")

        # ===================================================================
        # STAGE 3: V0S126 - Advance Detail
        # ===================================================================

        logger.log_stage("V0S126", "start")

        sql = sql_templates.get_advance_detail_query()
        logger.log_sql("V0S126", sql)

        Lk_BLDVAA_df = spark.sql(sql)
        Lk_BLDVAA_df = TypeCaster.cast_dataframe(Lk_BLDVAA_df, JobSchemas.ADVANCE_DETAIL_RAW)

        logger.log_dataframe(Lk_BLDVAA_df, "Lk_BLDVAA_df", "V0S126")
        logger.log_stage("V0S126", "complete")

        # ===================================================================
        # STAGE 4: V199S0 - Aggregate Advance Detail
        # ===================================================================

        logger.log_stage("V199S0", "start")

        Agg_To_Joi_df = Lk_BLDVAA_df
        Agg_To_Joi_df = Agg_To_Joi_df.groupBy(col('ACCT_NBR')).agg(
            spark_max(col('FIRST_ADV_DATE')).alias('FIRST_ADV_DATE'),
            spark_sum(col('FIRST_AMT_ADV')).alias('FIRST_AMT_ADV')
        )
        Agg_To_Joi_df = TypeCaster.cast_dataframe(
            Agg_To_Joi_df,
            JobSchemas.ADVANCE_DETAIL_AGG
        )

        logger.log_dataframe(Agg_To_Joi_df, "Agg_To_Joi_df", "V199S0")
        logger.log_stage("V199S0", "complete")

        # ===================================================================
        # STAGE 5: V0S132 - Customer Total Balance
        # ===================================================================

        logger.log_stage("V0S132", "start")

        sql = sql_templates.get_customer_balance_query()
        logger.log_sql("V0S132", sql)

        Cust_Tot_df = spark.sql(sql)
        Cust_Tot_df = TypeCaster.cast_dataframe(Cust_Tot_df, JobSchemas.CUSTOMER_BALANCE)

        logger.log_dataframe(Cust_Tot_df, "Cust_Tot_df", "V0S132")
        logger.log_stage("V0S132", "complete")

        # ===================================================================
        # STAGE 6: V154S0 - Branch Master Dimension
        # ===================================================================

        logger.log_stage("V154S0", "start")

        sql = sql_templates.get_branch_master_query()
        logger.log_sql("V154S0", sql)

        Lk_INTR_ORG_DIM_df = spark.sql(sql)
        Lk_INTR_ORG_DIM_df = TypeCaster.cast_dataframe(
            Lk_INTR_ORG_DIM_df,
            JobSchemas.BRANCH_MASTER
        )

        logger.log_dataframe(Lk_INTR_ORG_DIM_df, "Lk_INTR_ORG_DIM_df", "V154S0")
        logger.log_stage("V154S0", "complete")

        # ===================================================================
        # STAGE 7: V155S0 - Create Branch Copies (for different join purposes)
        # ===================================================================

        logger.log_stage("V155S0", "start")

        # Customer Branch (same as branch master)
        Cust_Brnch_df = Lk_INTR_ORG_DIM_df.select("BRNCH_NBR", "CRCL_CD", "BRNCH_NBR_SKEY")
        Cust_Brnch_df = TypeCaster.cast_dataframe(Cust_Brnch_df, JobSchemas.CUSTOMER_BRANCH)

        # Old Branch (same as branch master)
        Old_brnch_df = Lk_INTR_ORG_DIM_df.select("BRNCH_NBR", "CRCL_CD", "BRNCH_NBR_SKEY")
        Old_brnch_df = TypeCaster.cast_dataframe(Old_brnch_df, JobSchemas.OLD_BRANCH)

        logger.log_stage("V155S0", "complete")

        # ===================================================================
        # STAGE 8: V0S2 - First Join (Account + PSL + Advance)
        # ===================================================================

        logger.log_stage("V0S2", "start")

        # Chained multi-way join
        Join_To_Join_df = (
            Acct_dtal_driving_df.alias('Acct_dtal_driving')
            .join(Psl_df.alias('Psl'), on='ACCT_NBR', how='leftouter')
            .join(Agg_To_Joi_df.alias('Agg_To_Joi'), on='ACCT_NBR', how='leftouter')
            .select(
                col('Acct_dtal_driving.ACCT_NBR').alias('ACCT_NBR'),
                col('Acct_dtal_driving.NEW_CAPN_AMT').alias('NEW_CAPN_AMT'),
                col('Acct_dtal_driving.OLD_CAPN_AMT').alias('OLD_CAPN_AMT'),
                col('Acct_dtal_driving.ACTIVITY_CODE').alias('ACTIVITY_CODE'),
                col('Acct_dtal_driving.SBSD_SCHEME_CD').alias('SBSD_SCHEME_CD'),
                col('Acct_dtal_driving.IND_CODE').alias('IND_CODE'),
                col('Acct_dtal_driving.SUB_IND_CODE').alias('SUB_IND_CODE'),
                col('Acct_dtal_driving.SUB_SUB_IND_CODE').alias('SUB_SUB_IND_CODE'),
                col('Acct_dtal_driving.SPONSOR_ID').alias('SPONSOR_ID'),
                col('Acct_dtal_driving.OLD_BRANCH').alias('OLD_BRANCH'),
                col('Psl.CUST_NBR').alias('CUST_NBR'),
                col('Psl.PSL_CODE').alias('PSL_CODE'),
                col('Agg_To_Joi.FIRST_ADV_DATE').alias('FIRST_ADV_DATE'),
                col('Agg_To_Joi.FIRST_AMT_ADV').alias('FIRST_AMT_ADV')
            )
        )

        Join_To_Join_df = TypeCaster.cast_dataframe(Join_To_Join_df, JobSchemas.JOIN_STAGE_1)

        logger.log_dataframe(Join_To_Join_df, "Join_To_Join_df", "V0S2")
        logger.log_stage("V0S2", "complete")

        # ===================================================================
        # STAGE 9: V0S29 - Second Join (+ Customer Balance)
        # ===================================================================

        logger.log_stage("V0S29", "start")

        Lk_Src_Skey_df = (
            Join_To_Join_df.alias('Join_To_Join')
            .join(Cust_Tot_df.alias('Cust_Tot'), on='CUST_NBR', how='leftouter')
            .select(
                col('Join_To_Join.ACCT_NBR').alias('ACCT_NBR'),
                col('Join_To_Join.NEW_CAPN_AMT').alias('NEW_CAPN_AMT'),
                col('Join_To_Join.OLD_CAPN_AMT').alias('OLD_CAPN_AMT'),
                col('Join_To_Join.ACTIVITY_CODE').alias('ACTIVITY_CODE'),
                col('Join_To_Join.SBSD_SCHEME_CD').alias('SBSD_SCHEME_CD'),
                col('Join_To_Join.IND_CODE').alias('IND_CODE'),
                col('Join_To_Join.SUB_IND_CODE').alias('SUB_IND_CODE'),
                col('Join_To_Join.SUB_SUB_IND_CODE').alias('SUB_SUB_IND_CODE'),
                col('Join_To_Join.SPONSOR_ID').alias('SPONSOR_ID'),
                col('Join_To_Join.OLD_BRANCH').alias('OLD_BRANCH'),
                col('Join_To_Join.CUST_NBR').alias('CUST_NBR'),
                col('Join_To_Join.PSL_CODE').alias('PSL_CODE'),
                col('Join_To_Join.FIRST_ADV_DATE').alias('FIRST_ADV_DATE'),
                col('Join_To_Join.FIRST_AMT_ADV').alias('FIRST_AMT_ADV'),
                col('Cust_Tot.CUST_TOT_BAL').alias('CUST_TOT_BAL')
            )
        )

        Lk_Src_Skey_df = TypeCaster.cast_dataframe(Lk_Src_Skey_df, JobSchemas.JOIN_STAGE_2)

        logger.log_dataframe(Lk_Src_Skey_df, "Lk_Src_Skey_df", "V0S29")
        logger.log_stage("V0S29", "complete")

        # ===================================================================
        # STAGE 10: Final Join (+ Branch Keys)
        # ===================================================================

        logger.log_stage("FINAL_JOIN", "start")

        # Join with Old Branch
        Lk_Src_Skey_alias = Lk_Src_Skey_df.alias("Lk_Src_Skey")
        temp_output_df = Lk_Src_Skey_alias

        Old_brnch_alias = Old_brnch_df.alias("Old_brnch")
        Old_brnch_alias = Old_brnch_alias.withColumnRenamed("BRNCH_NBR_SKEY", "OLD_ACCT_BRNCH_CD_SKEY")
        Old_brnch_alias = Old_brnch_alias.withColumnRenamed("BRNCH_NBR", "BRNCH_NBR_r")
        temp_output_df = temp_output_df.join(
            Old_brnch_alias,
            (Old_brnch_alias["BRNCH_NBR_r"] == temp_output_df["OLD_BRANCH"]),
            "left"
        )
        temp_output_df = temp_output_df.drop("BRNCH_NBR_r")

        # Join with Customer Branch
        Cust_Brnch_alias = Cust_Brnch_df.alias("Cust_Brnch")
        Cust_Brnch_alias = Cust_Brnch_alias.withColumnRenamed("BRNCH_NBR_SKEY", "CUST_HOME_BR_SKEY")
        Cust_Brnch_alias = Cust_Brnch_alias.withColumnRenamed("BRNCH_NBR", "BRNCH_NBR_r")
        temp_output_df = temp_output_df.join(
            Cust_Brnch_alias,
            (Cust_Brnch_alias["BRNCH_NBR_r"] == temp_output_df["OLD_BRANCH"]),
            "left"
        )
        temp_output_df = temp_output_df.drop("BRNCH_NBR_r")

        Lk_Src_Tfr_df = temp_output_df
        Lk_Src_Tfr_df = TypeCaster.cast_dataframe(Lk_Src_Tfr_df, JobSchemas.JOIN_FINAL)

        logger.log_dataframe(Lk_Src_Tfr_df, "Lk_Src_Tfr_df", "FINAL_JOIN")
        logger.log_stage("FINAL_JOIN", "complete")

        # ===================================================================
        # STAGE 11: V157S0 - Transformer (Split into Fact and Reject)
        # ===================================================================

        logger.log_stage("V157S0", "start")

        # Fact table output (pass through)
        LkTo_Lon_Agmnt_Ft_df = Lk_Src_Tfr_df
        LkTo_Lon_Agmnt_Ft_df = TypeCaster.cast_dataframe(LkTo_Lon_Agmnt_Ft_df, JobSchemas.FACT_TABLE)

        # Reject output (same data)
        Tfr_Rej_df = Lk_Src_Tfr_df
        Tfr_Rej_df = TypeCaster.cast_dataframe(Tfr_Rej_df, JobSchemas.REJECT_OUTPUT)

        logger.log_stage("V157S0", "complete")

        # ===================================================================
        # STAGE 12: V0S74 - Write Outputs
        # ===================================================================

        logger.log_stage("OUTPUT", "start")

        # Write to Iceberg fact table
        target_table = config.get_table_fqn("target", "fact_table")
        logger.info(f"Writing to target table: {target_table}")

        LkTo_Lon_Agmnt_Ft_df.writeTo(target_table).using("iceberg").createOrReplace()

        # Write reject files
        fact_output_path = f"{reject_file_path}/GDM/{config.output.fact_file_pattern.format(business_date=business_date)}"
        tfr_output_path = f"{reject_file_path}/GDM/{config.output.transform_file_pattern.format(business_date=business_date)}"

        logger.info(f"Writing fact reject file: {fact_output_path}")
        LkTo_Lon_Agmnt_Ft_df.write.mode(config.output.write_mode).parquet(fact_output_path)

        logger.info(f"Writing transform reject file: {tfr_output_path}")
        Tfr_Rej_df.write.mode(config.output.write_mode).parquet(tfr_output_path)

        logger.log_metrics("OUTPUT", {
            "target_table": target_table,
            "fact_output_path": fact_output_path,
            "tfr_output_path": tfr_output_path,
            "final_row_count": LkTo_Lon_Agmnt_Ft_df.count()
        })

        logger.log_stage("OUTPUT", "complete")

        # ===================================================================
        # JOB COMPLETE
        # ===================================================================

        logger.log_stage("JOB", "success")
        print(f"\n✅ Job completed successfully!")
        print(f"   Business Date: {business_date}")
        print(f"   Target Table: {target_table}")
        print(f"   Environment: {args.environment}")

    except Exception as e:
        logger.log_error("JOB", e)
        logger.log_stage("JOB", "failed", error=str(e))
        print(f"\n❌ Job failed with error: {e}")
        raise

    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
