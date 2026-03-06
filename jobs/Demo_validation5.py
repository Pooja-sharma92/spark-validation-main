from pyspark.sql import SparkSession
#from udfs import register_udfs
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import json
def main():
    ###################################{Demo_validation5}###################################

    import argparse
    parser = argparse.ArgumentParser(description="DataStage Job Parameters")
    parser.add_argument("--CRM_PARAMETER_SET", type=str, default= "(As pre-defined)")
    parser.add_argument("--CUSTOMETL_ParameterSet", type=str, default= "(As pre-defined)")
    args = parser.parse_args()
    with open(args.CRM_PARAMETER_SET, 'r') as f:
                            real_value = json.load(f)
        CRM_PARAMETER_SET = real_value
    with open(args.CUSTOMETL_ParameterSet, 'r') as f:
                            real_value = json.load(f)
        CUSTOMETL_ParameterSet = real_value
    #Parameters loaded successfully 



    # Create a SparkSession for a cloud environment
    spark = (SparkSession.builder 
            .appName("Demo_validation5") 
            .master("yarn")
            # Replace 'yarn' with the appropriate cluster manager for the cloud setup 
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.my_catalog.type", "hive") 
            # or 'hadoop' if you're using FS-based catalog
            .config("spark.sql.catalog.my_catalog.uri", "thrift://<hive-metastore-host>:9083")
            .config("spark.sql.catalog.my_catalog.warehouse", "hdfs:///user/hive/warehouse")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

            # Iceberg Optional Tuning
            .config("spark.sql.catalog.my_catalog.cache-enabled", "false")  # Disable cache if debugging
            .config("spark.sql.defaultCatalog", "my_catalog")  # Optional shortcut

            # Adaptive Query Execution (AQE)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")

            # Shuffle partitioning
            .config("spark.sql.shuffle.partitions", "200")  
            # Safe default; AQE may override
            .config("spark.sql.files.maxPartitionBytes", "134217728")  
            # 128MB
            .config("spark.sql.files.openCostInBytes", "4194304")  
            # 4MB

            # Iceberg Spark Write Behavior
            .config("spark.sql.catalog.my_catalog.write-format.default", "parquet")
            .config("spark.sql.catalog.my_catalog.write.target-file-size-bytes", "134217728") 
              # 128MB
            .config("spark.sql.catalog.my_catalog.write.metadata.compression-codec", "gzip")

            # Optional Spark SQL tuning
            .config("spark.sql.caseSensitive", "false")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") 
            .config("spark.sql.iceberg.merge-schema", "true") 
            # For INSERT OVERWRITE on partitioned tables

            .enableHiveSupport()  
            # Required if Hive catalog
            .getOrCreate())
    from utils.udfs import register_udfs
    register_udfs(spark)



    # DB2 Connector Activity: V1S4

    sql = f"""select * from(select CUSTOMER_NO,
    Account_No,
    POST_DATE,
    Scheduled_Payment_Date_01,
    Scheduled_Payment_Date_02,
    Scheduled_Payment_Date_03,
    Scheduled_Payment_Date_04,
    Scheduled_Payment_Date_05,
    Scheduled_Payment_Date_06,
    Scheduled_Payment_Date_07,
    Scheduled_Payment_Date_08,
    Scheduled_Payment_Date_09,
    Scheduled_Payment_Date_10,
    Scheduled_Payment_Date_11,
    Scheduled_Payment_Date_12,
    Scheduled_Payment_Date_13,
    Scheduled_Payment_Date_14,
    Scheduled_Payment_Date_15,
    Scheduled_Amount_01,
    Scheduled_Amount_02,
    Scheduled_Amount_03,
    Scheduled_Amount_04,
    Scheduled_Amount_05,
    Scheduled_Amount_06,
    Scheduled_Amount_07,
    Scheduled_Amount_08,
    Scheduled_Amount_09,
    Scheduled_Amount_10,
    Scheduled_Amount_11,
    Scheduled_Amount_12,
    Scheduled_Amount_13,
    Scheduled_Amount_14,
    Scheduled_Amount_15,
    RANK over(partition by CUSTOMER_NO order by POST_DATE desc ) Rnk from(select
    C.CUST_NBR as CUSTOMER_NO,
    A.acct_no as Account_No,
    '' as Type_of_Payment,
    A.POST_DATE as POST_DATE,
    A.START_DATE_01 as Scheduled_Payment_Date_01,
    A.START_DATE_02 as Scheduled_Payment_Date_02,
    A.START_DATE_03 as Scheduled_Payment_Date_03,
    A.START_DATE_04 as Scheduled_Payment_Date_04,
    A.START_DATE_05 as Scheduled_Payment_Date_05,
    A.START_DATE_06 as Scheduled_Payment_Date_06,
    A.START_DATE_07 as Scheduled_Payment_Date_07,
    A.START_DATE_08 as Scheduled_Payment_Date_08,
    A.START_DATE_09 as Scheduled_Payment_Date_09,
    A.START_DATE_10 as Scheduled_Payment_Date_10,
    A.START_DATE_11 as Scheduled_Payment_Date_11,
    A.START_DATE_12 as Scheduled_Payment_Date_12,
    A.START_DATE_13 as Scheduled_Payment_Date_13,
    A.START_DATE_14 as Scheduled_Payment_Date_14,
    A.START_DATE_15 as Scheduled_Payment_Date_15,
    A.PRINC_DUE_01+PROJ_INT_01 as Scheduled_Amount_01,
    A.PRINC_DUE_02+PROJ_INT_02 as Scheduled_Amount_02,
    A.PRINC_DUE_03+PROJ_INT_03 as Scheduled_Amount_03,
    A.PRINC_DUE_04+PROJ_INT_04 as Scheduled_Amount_04,
    A.PRINC_DUE_05+PROJ_INT_05 as Scheduled_Amount_05,
    A.PRINC_DUE_06+PROJ_INT_06 as Scheduled_Amount_06,
    A.PRINC_DUE_07+PROJ_INT_07 as Scheduled_Amount_07,
    A.PRINC_DUE_08+PROJ_INT_08 as Scheduled_Amount_08,
    A.PRINC_DUE_09+PROJ_INT_09 as Scheduled_Amount_09,
    A.PRINC_DUE_10+PROJ_INT_10 as Scheduled_Amount_10,
    A.PRINC_DUE_11+PROJ_INT_11 as Scheduled_Amount_11,
    A.PRINC_DUE_12+PROJ_INT_12 as Scheduled_Amount_12,
    A.PRINC_DUE_13+PROJ_INT_13 as Scheduled_Amount_13,
    A.PRINC_DUE_14+PROJ_INT_14 as Scheduled_Amount_14,
    A.PRINC_DUE_15+PROJ_INT_15 as Scheduled_Amount_15
    from 
    schema2.TableA A 
    inner join
    schema2.TableR C 
    on
    (A.ACCT_NO=C.ACCT_NBR and A.POST_DATE <=  to_date('{CUSTOM_PARAMETER_SET["BusinessDate"]}') 
    and ACCT_TYPE_IND='OWN' ) 
    )) where Rnk=1"""
    DSLink2_df = spark.sql(sql)
    DSLink2_df = DSLink2_df.selectExpr(
        "CAST(Account_No AS STRING) AS Account_No",
        "CAST(Scheduled_Payment_Date_01 AS DATE) AS Scheduled_Payment_Date_01",
        "CAST(Scheduled_Payment_Date_02 AS DATE) AS Scheduled_Payment_Date_02",
        "CAST(Scheduled_Payment_Date_03 AS DATE) AS Scheduled_Payment_Date_03",
        "CAST(Scheduled_Payment_Date_04 AS DATE) AS Scheduled_Payment_Date_04",
        "CAST(Scheduled_Payment_Date_05 AS DATE) AS Scheduled_Payment_Date_05",
        "CAST(Scheduled_Payment_Date_06 AS DATE) AS Scheduled_Payment_Date_06",
        "CAST(Scheduled_Payment_Date_07 AS DATE) AS Scheduled_Payment_Date_07",
        "CAST(Scheduled_Payment_Date_08 AS DATE) AS Scheduled_Payment_Date_08",
        "CAST(Scheduled_Payment_Date_09 AS DATE) AS Scheduled_Payment_Date_09",
        "CAST(Scheduled_Payment_Date_10 AS DATE) AS Scheduled_Payment_Date_10",
        "CAST(Scheduled_Payment_Date_11 AS DATE) AS Scheduled_Payment_Date_11",
        "CAST(Scheduled_Payment_Date_12 AS DATE) AS Scheduled_Payment_Date_12",
        "CAST(Scheduled_Payment_Date_13 AS DATE) AS Scheduled_Payment_Date_13",
        "CAST(Scheduled_Payment_Date_14 AS DATE) AS Scheduled_Payment_Date_14",
        "CAST(Scheduled_Payment_Date_15 AS DATE) AS Scheduled_Payment_Date_15",
        "CAST(CUSTOMER_NO AS STRING) AS CUSTOMER_NO",
        "CAST(Scheduled_Amount_01 AS DECIMAL(20,5)) AS Scheduled_Amount_01",
        "CAST(Scheduled_Amount_02 AS DECIMAL(20,5)) AS Scheduled_Amount_02",
        "CAST(Scheduled_Amount_03 AS DECIMAL(20,5)) AS Scheduled_Amount_03",
        "CAST(Scheduled_Amount_04 AS DECIMAL(20,5)) AS Scheduled_Amount_04",
        "CAST(Scheduled_Amount_05 AS DECIMAL(20,5)) AS Scheduled_Amount_05",
        "CAST(Scheduled_Amount_06 AS DECIMAL(20,5)) AS Scheduled_Amount_06",
        "CAST(Scheduled_Amount_07 AS DECIMAL(20,5)) AS Scheduled_Amount_07",
        "CAST(Scheduled_Amount_08 AS DECIMAL(20,5)) AS Scheduled_Amount_08",
        "CAST(Scheduled_Amount_09 AS DECIMAL(20,5)) AS Scheduled_Amount_09",
        "CAST(Scheduled_Amount_10 AS DECIMAL(20,5)) AS Scheduled_Amount_10",
        "CAST(Scheduled_Amount_11 AS DECIMAL(20,5)) AS Scheduled_Amount_11",
        "CAST(Scheduled_Amount_12 AS DECIMAL(20,5)) AS Scheduled_Amount_12",
        "CAST(Scheduled_Amount_13 AS DECIMAL(20,5)) AS Scheduled_Amount_13",
        "CAST(Scheduled_Amount_14 AS DECIMAL(20,5)) AS Scheduled_Amount_14",
        "CAST(Scheduled_Amount_15 AS DECIMAL(20,5)) AS Scheduled_Amount_15",
        "CAST(POST_DATE AS DATE) AS POST_DATE"
    )

    DSLink29_df = DSLink2_df
    DSLink29_df = DSLink2_df
    DSLink29_df = DSLink29_df.selectExpr("Account_No, POST_DATE, CUSTOMER_NO", "stack(15, Scheduled_Payment_Date_01, Scheduled_Amount_01, Scheduled_Payment_Date_02, Scheduled_Amount_02, Scheduled_Payment_Date_03, Scheduled_Amount_03, Scheduled_Payment_Date_04, Scheduled_Amount_04, Scheduled_Payment_Date_05, Scheduled_Amount_05, Scheduled_Payment_Date_06, Scheduled_Amount_06, Scheduled_Payment_Date_07, Scheduled_Amount_07, Scheduled_Payment_Date_08, Scheduled_Amount_08, Scheduled_Payment_Date_09, Scheduled_Amount_09, Scheduled_Payment_Date_10, Scheduled_Amount_10, Scheduled_Payment_Date_11, Scheduled_Amount_11, Scheduled_Payment_Date_12, Scheduled_Amount_12, Scheduled_Payment_Date_13, Scheduled_Amount_13, Scheduled_Payment_Date_14, Scheduled_Amount_14, Scheduled_Payment_Date_15, Scheduled_Amount_15) as (Scheduled_Payment_date, Scheduled_Amount)")
    DSLink29_df = DSLink29_df.selectExpr(
        "CAST(Account_No AS STRING) AS Account_No",
        "CAST(CUSTOMER_NO AS STRING) AS CUSTOMER_NO",
        "CAST(Scheduled_Payment_date AS DATE) AS Scheduled_Payment_date",
        "CAST(Scheduled_Amount AS DECIMAL(20,5)) AS Scheduled_Amount"
    )

    from pyspark.sql.functions import *

    # Generated for activity: V1S2 by Transformer function
    # Processing input pin: V1S2P1 for activity V1S2

    # Processing output pin: V1S2P2 for activity V1S2
    DSLink17_df = DSLink29_df
    # Column Operations
    DSLink17_df = DSLink17_df.withColumn('CIF_NUMBER', expr("""CUSTOMER_NO || CheckDigit ( CUSTOMER_NO )"""))
    DSLink17_df = DSLink17_df.withColumn('ACCOUNT_NUMBER', expr("""Account_No || CheckDigit ( Account_No )"""))
    DSLink17_df = DSLink17_df.withColumn('Type_of_Payment', expr("""''"""))
    DSLink17_df = DSLink17_df.withColumn('Scheduled_Amount', expr("""UDF_DECIMALTOSTRING( Scheduled_Amount,"suppress_zero" )"""))
    # Directly passed through columns: Scheduled_Payment_Date
    # DSLink17_df = DSLink17_df.select('ACCOUNT_NUMBER', 'CIF_NUMBER', 'Scheduled_Amount', 'Scheduled_Payment_Date', 'Type_of_Payment')

    DSLink17_df = DSLink17_df.selectExpr(
        "CAST(CIF_NUMBER AS STRING) AS CIF_NUMBER",
        "CAST(ACCOUNT_NUMBER AS STRING) AS ACCOUNT_NUMBER",
        "CAST(Type_of_Payment AS STRING) AS Type_of_Payment",
        "CAST(Scheduled_Payment_Date AS DATE) AS Scheduled_Payment_Date",
        "CAST(Scheduled_Amount AS DECIMAL(20,5)) AS Scheduled_Amount"
    )


    # Original path: /dsworking/TempFileDir/CUSTOMUSER/CUSTOM/out/sme/cbg_repayment_master_sme_{CUSTOM_PARAMETER_SET["BusinessDate"]}.txt, converted to Parquet: /dsworking/TempFileDir/CUSTOMUSER/CUSTOM/out/sme/cbg_repayment_master_sme_{CUSTOM_PARAMETER_SET["BusinessDate"]}.parquet
    DSLink17_df.write.mode("overwrite").parquet(f"""/dsworking/TempFileDir/CUSTOMUSER/CUSTOM/out/sme/cbg_repayment_master_sme_{CUSTOM_PARAMETER_SET["BusinessDate"]}.parquet""")

if __name__ == "__main__":
    main()
