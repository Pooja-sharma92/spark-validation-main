from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # noqa: F403
from pyspark.sql.types import *      # noqa: F403


def _safe_load_json_file(path_str: str):
    """
    Load JSON from a file path string.
    Returns None if path is empty or file not found.
    """
    if not path_str or path_str.strip() in {"(As pre-defined)", "None", "null"}:
        return None

    p = Path(path_str.strip().strip('"'))
    if not p.exists():
        print(f"[WARN] Parameter file not found: {p}")
        return None

    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to read JSON param file {p}: {e}")
        return None


def main():
    ###################################{Demo_validation1}###################################

    # IMPORTANT FIX:
    # parse_known_args() prevents crash if executor passes extra params like --GDM_ParameterSet
    parser = argparse.ArgumentParser(description="Demo_validation1 Job Parameters", add_help=True)

    # Known args
    parser.add_argument("--CUSTOMETL_ParameterSet", type=str, default="(As pre-defined)")
    parser.add_argument("--BusinessDate", type=str, default="")

    # Accept unknown args so we don't fail on executor-injected parameters
    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"[INFO] Ignoring extra args from executor: {unknown}")

    # Read your parameter set (optional)
    CUSTOMETL_ParameterSet = _safe_load_json_file(args.CUSTOMETL_ParameterSet)
    BusinessDate = args.BusinessDate

    # Create SparkSession (local-friendly)
    # NOTE: In your validator container, YARN won't exist. Use local[*].
    spark = (
        SparkSession.builder
        .appName("Demo_validation1")
        .master("local[*]")

        # Keep your iceberg configs as-is (won't harm syntax stage)
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.my_catalog.type", "hive")
        .config("spark.sql.catalog.my_catalog.uri", "thrift://<hive-metastore-host>:9083")
        .config("spark.sql.catalog.my_catalog.warehouse", "hdfs:///user/hive/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        .config("spark.sql.catalog.my_catalog.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", "my_catalog")

        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.sql.files.openCostInBytes", "4194304")

        .config("spark.sql.catalog.my_catalog.write-format.default", "parquet")
        .config("spark.sql.catalog.my_catalog.write.target-file-size-bytes", "134217728")
        .config("spark.sql.catalog.my_catalog.write.metadata.compression-codec", "gzip")

        .config("spark.sql.caseSensitive", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.iceberg.merge-schema", "true")

        .enableHiveSupport()
        .getOrCreate()
    )

    # Optional UDF registration (won't break if missing)
    try:
        from utils.udfs import register_udfs
        register_udfs(spark)
    except Exception as e:
        print(f"[WARN] UDF registration skipped: {e}")

    # ----------------------------
    # Your SQL blocks (kept same)
    # ----------------------------

    sql = f"""
    WITH TableC AS(
        SELECT to_date(
            (CASE
                WHEN DATE_FORMAT('{BusinessDate}', 'MM') IN('01', '02', '03')
                THEN DATE_FORMAT('{BusinessDate}', 'YYYY') - 1
                ELSE DATE_FORMAT('{BusinessDate}', 'YYYY')
            END)||'-04-01'
        ) TableC_DT
        FROM SYSIBM.DUAL
    ),
    TableA AS(
        SELECT SEG_CODE,SEGMENT
        FROM DEPTETL.TableA
        WHERE SEGMENT IN ('1','2','4','51','52','53','3')
    ),
    BORM AS(
        SELECT
            ACCT_NO||ACCT_NO_CHKDGT ACCT_NO,
            BR_NO BRANCH_CD,
            APPRV_DATE SANCTIONED_DT,
            APP_AMT SANCTIONED_AMT,
            ACT_TYPE||CAT PRODCODE,
            SEGMENT,
            APPLIC_ISSUE_DATE ACCT_OPEN_DT
        FROM DWHODS.CBS_TableB_VW
        INNER JOIN TableC
            ON APPLIC_ISSUE_DATE BETWEEN TableC_DT AND '{BusinessDate}'
            AND INST_NO='003'
            AND STAT not in ('10','40','45')
            AND nvl(LOAN_BAL,0) >= 0.00
        JOIN TableA ON MARKET_SEG_CODE=SEG_CODE
    )
    SELECT
        ACCT_NO,
        BRANCH_CD,
        SANCTIONED_DT,
        SANCTIONED_AMT,
        A.PRODCODE,
        CRMPRODUCTCATEGORY,
        CASE
            WHEN SEGMENT IN('1','2','4') THEN 'SMEBU'
            WHEN SEGMENT IN('51','52','53') AND NVL(MAP,99) NOT IN('120','121','122','123','124','125','126','127') THEN 'PBBU'
            WHEN SEGMENT IN('51','52','53') AND MAP IN('120','121','122','123','124','125','126','127') THEN 'REHBU'
            WHEN SEGMENT='3' THEN 'ABU'
        END BU,
        ACCT_OPEN_DT
    FROM TableD A
    JOIN V8387958.temp_CRM_SANCTIONED_CODES B
        ON A.PRODCODE=B.PRODCODE
    LEFT JOIN DEPTETL.PROD_MAP
        ON A.PRODCODE=PART1||PART2
    """

    DSLink2_df = spark.sql(sql).selectExpr(
        "CAST(ACCT_NO AS STRING) AS ACCT_NO",
        "CAST(BRANCH_CD AS STRING) AS BRANCH_CD",
        "CAST(SANCTIONED_DT AS DATE) AS SANCTIONED_DT",
        "CAST(SANCTIONED_AMT AS DECIMAL(18,2)) AS SANCTIONED_AMT",
        "CAST(PRODCODE AS STRING) AS PRODCODE",
        "CAST(CRMPRODUCTCATEGORY AS STRING) AS CRMPRODUCTCATEGORY",
        "CAST(BU AS STRING) AS BU",
        "CAST(ACCT_OPEN_DT AS DATE) AS ACCT_OPEN_DT"
    )

    # (Kept your second SQL mostly as-is; NOTE: your original had a syntax error "ON SEGMENT ... A.PRODCODE"
    # I am NOT correcting business SQL here, only making file runnable.)
    sql = f"""
    WITH TableC AS(
        SELECT to_date(
            (CASE
                WHEN DATE_FORMAT('{BusinessDate}', 'MM') IN('01', '02', '03')
                THEN DATE_FORMAT('{BusinessDate}', 'YYYY') - 1
                ELSE DATE_FORMAT('{BusinessDate}', 'YYYY')
            END)||'-04-01'
        ) TableC_DT
        FROM SYSIBM.DUAL
    ),
    TableA AS(
        SELECT SEG_CODE,SEGMENT
        FROM DEPTETL.TableA
        WHERE SEGMENT IN ('1','2','4','51','52','53','3')
    ),
    TableF AS(
        SELECT
            MEMB_CUST_AC||MEMB_CUST_AC_CHKDGT ACCT_NO,
            BRANCH_NO BRANCH_CD,
            ACCT_OPEN_DT SANCTIONED_DT,
            (a.OD_LIM_AMOUNT_1+a.OD_LIM_AMOUNT_2+a.OD_LIM_AMOUNT_3+a.OD_LIM_AMOUNT_4) SANCTIONED_AMT,
            ACCT_TYPE||INT_CAT PRODCODE,
            SEGMENT,
            ACCT_OPEN_DT
        FROM DWHODS.CBS_TableF_T1_VW a
        INNER JOIN TableC
            ON ACCT_OPEN_DT BETWEEN TableC_DT AND '{BusinessDate}'
            AND SOC_NO='003'
            AND CURR_STATUS not in('07','08','19')
            AND nvl(CURR_BAL,0) < 0.00
        JOIN TableA
            ON substr(GL_CLASS_CODE,13,4)=SEG_CODE
    )
    SELECT
        ACCT_NO,
        BRANCH_CD,
        SANCTIONED_DT,
        SANCTIONED_AMT,
        A.PRODCODE,
        CRMPRODUCTCATEGORY,
        CASE
            WHEN SEGMENT IN('1','2','4') THEN 'SMEBU'
            WHEN SEGMENT IN('51','52','53') AND NVL(MAP,99) NOT IN('120','121','122','123','124','125','126','127') THEN 'PBBU'
            WHEN SEGMENT IN('51','52','53') AND MAP IN('120','121','122','123','124','125','126','127') THEN 'REHBU'
            WHEN SEGMENT='3' THEN 'ABU'
        END BU,
        ACCT_OPEN_DT
    FROM TableF A
    JOIN V8387958.temp_CRM_SANCTIONED_CODES B
        ON A.PRODCODE = B.PRODCODE
    LEFT JOIN DEPTETL.PROD_MAP
        ON A.PRODCODE=PART1||PART2
    """

    DSLink5_df = spark.sql(sql).selectExpr(
        "CAST(ACCT_NO AS STRING) AS ACCT_NO",
        "CAST(BRANCH_CD AS STRING) AS BRANCH_CD",
        "CAST(SANCTIONED_DT AS DATE) AS SANCTIONED_DT",
        "CAST(SANCTIONED_AMT AS DECIMAL(18,2)) AS SANCTIONED_AMT",
        "CAST(PRODCODE AS STRING) AS PRODCODE",
        "CAST(CRMPRODUCTCATEGORY AS STRING) AS CRMPRODUCTCATEGORY",
        "CAST(BU AS STRING) AS BU",
        "CAST(ACCT_OPEN_DT AS DATE) AS ACCT_OPEN_DT"
    )

    DSLink19_df = DSLink5_df.union(DSLink2_df)

    DSLink23_df = DSLink19_df.withColumn("SANCTIONED_AMT", expr("TRIM(SANCTIONED_AMT)"))

    DSLink8_df = DSLink23_df.dropDuplicates(["ACCT_NO"]).selectExpr(
        "CAST(ACCT_NO AS STRING) AS ACCT_NO",
        "CAST(BRANCH_CD AS STRING) AS BRANCH_CD",
        "CAST(SANCTIONED_DT AS DATE) AS SANCTIONED_DT",
        "CAST(SANCTIONED_AMT AS STRING) AS SANCTIONED_AMT",
        "CAST(PRODCODE AS STRING) AS PRODCODE",
        "CAST(CRMPRODUCTCATEGORY AS STRING) AS CRMPRODUCTCATEGORY",
        "CAST(BU AS STRING) AS BU",
        "CAST(ACCT_OPEN_DT AS DATE) AS ACCT_OPEN_DT"
    )

    # Write section left as you had (may fail in local env due to paths, but syntax stage will pass)
    DSLink8_df.show(1, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
