from pyspark.sql import SparkSession
#from udfs import register_udfs
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import json
def main():
    ###################################{STG_XTC_LD_TEST_JOB}###################################

    import argparse
    parser = argparse.ArgumentParser(description="DataStage Job Parameters")
    parser.add_argument("--GDM_ParameterSet", type=str, default= "(As pre-defined)")
    parser.add_argument("--PCS_ParameterSet", type=str, default= "(As pre-defined)")
    args = parser.parse_args()
    with open(args.GDM_ParameterSet, 'r') as f:
                            real_value = json.load(f)
        GDM_ParameterSet = real_value
    with open(args.PCS_ParameterSet, 'r') as f:
                            real_value = json.load(f)
        PCS_ParameterSet = real_value
    #Parameters loaded successfully 



    # Create a SparkSession for a cloud environment
    spark = (SparkSession.builder 
            .appName("STG_XTC_LD_TEST_JOB") 
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



    # DB2 Connector Activity: V138S0

    sql = f"""SELECT
        ACCT_NBR,
        NEW_CAPN_AMT,
        OLD_CAPN_AMT,
        ACTIVITY_CODE,
        SBSD_SCHEME_CD,
        IND_CODE,
        SUB_IND_CODE,
        SUB_SUB_IND_CODE,
        SPONSOR_ID,
    OLD_BRANCH
    FROM
       IBM_TEST.LOAN_MASTER A INNER JOIN GDMSBI.DT_DIM D
    ON
        to_date('{PCS_ParameterSet["JpBusinessDate"]}')=D.DT AND(CASE WHEN STAT IN ('10','40','45') THEN nvl(LST_FIN_DATE,'1899-12-31')    ELSE '2999-12-31'  END) >= FIN_YR_STRT_DT"""
    Acct_dtal_driving_df = spark.sql(sql)
    Acct_dtal_driving_df = Acct_dtal_driving_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH"
    )



    # DB2 Connector Activity: V0S108

    sql = f"""SELECT
        ACCT_NBR,
        CUST_NBR,
        PSL_CODE
    FROM
       IBM_TEST.CCOD_LON_PSL WHERE  = CURRENT DBPARTITIONNUM"""
    Psl_df = spark.sql(sql)
    Psl_df = Psl_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE"
    )



    # DB2 Connector Activity: V0S126

    sql = f"""SELECT
        ACCT_NO AS ACCT_NBR,
        (CASE WHEN POST_DATE>=MTH_STRT_DT AND POST_DATE<='{PCS_ParameterSet["JpBusinessDate"]}' THEN FIRST_AMT_ADV ELSE 0 END) FIRST_AMT_ADV,
        udf_to_number(DATE_FORMAT(POST_DATE, 'YYYYMMDD')) AS FIRST_ADV_DATE
    FROM
        IBM_TEST.BLDVAA_UAT A,
        IBM_TEST.LON_ACCT_DTL_TBL B,
        IBM_TEST.DT_DIM C
    WHERE
        A.ACCT_NO=B.ACCT_NBR
    AND C.DT='{PCS_ParameterSet["JpBusinessDate"]}' AND(CASE WHEN STAT IN ('10','40','45') THEN nvl(LST_FIN_DATE,'1899-12-31') ELSE '2999-12-31' END) >= FIN_YR_STRT_DT"""
    Lk_BLDVAA_df = spark.sql(sql)
    Lk_BLDVAA_df = Lk_BLDVAA_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE"
    )


    # Generated for activity: V199S0 by Aggregator function
    Agg_To_Joi_df = Lk_BLDVAA_df
    #Agg_To_Joi_df = Agg_To_Joi_df.sort(col('ACCT_NBR'))
    #Agg_To_Joi_df = Agg_To_Joi_df.select(col('ACCT_NBR'), col('FIRST_ADV_DATE').cast(DecimalType(8,0)), col('FIRST_AMT_ADV').cast(DecimalType(25,5)))
    Agg_To_Joi_df = Agg_To_Joi_df.groupBy(col('ACCT_NBR')).agg(max(col('FIRST_ADV_DATE')).alias('FIRST_ADV_DATE'), sum(col('FIRST_AMT_ADV')).alias('FIRST_AMT_ADV'))
    Agg_To_Joi_df = Agg_To_Joi_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV"
    )




    # DB2 Connector Activity: V0S132

    sql = f"""SELECT
        A.CUST_NBR,
        A.CUST_TOT_BAL
    FROM
        IBM_TEST.CUST_TOT_BALANCE A,
       IBM_TEST.LON_CUST_TOT_BALANCE  B
    WHERE
        A.CUST_NBR=B.CUST_NBR
    AND TBL_ID=2"""
    Cust_Tot_df = spark.sql(sql)
    Cust_Tot_df = Cust_Tot_df.selectExpr(
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(CUST_TOT_BAL AS DECIMAL(25,5)) AS CUST_TOT_BAL"
    )



    # DB2 Connector Activity: V154S0

    sql = f"""SELECT
    	BRNCH_NBR,
    	CRCL_CD,
    	BRNCH_NBR_SKEY
    FROM
    	IBM_TEST.BRANCH_MASTER_DIM
    WHERE
    	LATEST_FLG='Y'"""
    Lk_INTR_ORG_DIM_df = spark.sql(sql)
    Lk_INTR_ORG_DIM_df = Lk_INTR_ORG_DIM_df.selectExpr(
        "CAST(BRNCH_NBR AS STRING) AS BRNCH_NBR",
        "CAST(CRCL_CD AS SHORT) AS CRCL_CD",
        "CAST(BRNCH_NBR_SKEY AS LONG) AS BRNCH_NBR_SKEY"
    )

    # Generated for activity: V155S0 by copy function
    from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
    Cust_Brnch_schema = StructType([StructField('BRNCH_NBR', StringType(), False), StructField('CRCL_CD', ShortType(), False), StructField('BRNCH_NBR_SKEY', LongType(), False)])
    Cust_Brnch_df = spark.createDataFrame([], Cust_Brnch_schema)
    Cust_Brnch_df = Lk_INTR_ORG_DIM_df.select("BRNCH_NBR", "CRCL_CD", "BRNCH_NBR_SKEY")

    Cust_Brnch_df = Cust_Brnch_df.selectExpr(
        "CAST(BRNCH_NBR AS STRING) AS BRNCH_NBR",
        "CAST(CRCL_CD AS SHORT) AS CRCL_CD",
        "CAST(BRNCH_NBR_SKEY AS LONG) AS BRNCH_NBR_SKEY"
    )

    from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
    Old_brnch_schema = StructType([StructField('BRNCH_NBR', StringType(), False), StructField('CRCL_CD', ShortType(), False), StructField('BRNCH_NBR_SKEY', LongType(), False)])
    Old_brnch_df = spark.createDataFrame([], Old_brnch_schema)
    Old_brnch_df = Lk_INTR_ORG_DIM_df.select("BRNCH_NBR", "CRCL_CD", "BRNCH_NBR_SKEY")

    Old_brnch_df = Old_brnch_df.selectExpr(
        "CAST(BRNCH_NBR AS STRING) AS BRNCH_NBR",
        "CAST(CRCL_CD AS SHORT) AS CRCL_CD",
        "CAST(BRNCH_NBR_SKEY AS LONG) AS BRNCH_NBR_SKEY"
    )

    from pyspark.sql.functions import col

    # Generated for activity: V0S2 by Join function
    # Chained multi-way join
    Join_To_Join_df = Acct_dtal_driving_df.alias('Acct_dtal_driving').join(Psl_df.alias('Psl'), on='ACCT_NBR', how='leftouter').join(Agg_To_Joi_df.alias('Agg_To_Joi'), on='ACCT_NBR', how='leftouter').select(col('Acct_dtal_driving.ACCT_NBR').alias('ACCT_NBR'), col('Acct_dtal_driving.NEW_CAPN_AMT').alias('NEW_CAPN_AMT'), col('Acct_dtal_driving.OLD_CAPN_AMT').alias('OLD_CAPN_AMT'), col('Acct_dtal_driving.ACTIVITY_CODE').alias('ACTIVITY_CODE'), col('Acct_dtal_driving.SBSD_SCHEME_CD').alias('SBSD_SCHEME_CD'), col('Acct_dtal_driving.IND_CODE').alias('IND_CODE'), col('Acct_dtal_driving.SUB_IND_CODE').alias('SUB_IND_CODE'), col('Acct_dtal_driving.SUB_SUB_IND_CODE').alias('SUB_SUB_IND_CODE'), col('Acct_dtal_driving.SPONSOR_ID').alias('SPONSOR_ID'), col('Acct_dtal_driving.OLD_BRANCH').alias('OLD_BRANCH'), col('Psl.CUST_NBR').alias('CUST_NBR'), col('Psl.PSL_CODE').alias('PSL_CODE'), col('Agg_To_Joi.FIRST_ADV_DATE').alias('FIRST_ADV_DATE'), col('Agg_To_Joi.FIRST_AMT_ADV').alias('FIRST_AMT_ADV'))

    Join_To_Join_df = Join_To_Join_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV"
    )


    # Generated for activity: V0S29 by Join function
    Lk_Src_Skey_df = Join_To_Join_df.alias('Join_To_Join').join(Cust_Tot_df.alias('Cust_Tot'), on='CUST_NBR', how='leftouter').select(col('Join_To_Join.ACCT_NBR').alias('ACCT_NBR'), col('Join_To_Join.NEW_CAPN_AMT').alias('NEW_CAPN_AMT'), col('Join_To_Join.OLD_CAPN_AMT').alias('OLD_CAPN_AMT'), col('Join_To_Join.ACTIVITY_CODE').alias('ACTIVITY_CODE'), col('Join_To_Join.SBSD_SCHEME_CD').alias('SBSD_SCHEME_CD'), col('Join_To_Join.IND_CODE').alias('IND_CODE'), col('Join_To_Join.SUB_IND_CODE').alias('SUB_IND_CODE'), col('Join_To_Join.SUB_SUB_IND_CODE').alias('SUB_SUB_IND_CODE'), col('Join_To_Join.SPONSOR_ID').alias('SPONSOR_ID'), col('Join_To_Join.OLD_BRANCH').alias('OLD_BRANCH'), col('Join_To_Join.CUST_NBR').alias('CUST_NBR'), col('Join_To_Join.PSL_CODE').alias('PSL_CODE'), col('Join_To_Join.FIRST_ADV_DATE').alias('FIRST_ADV_DATE'), col('Join_To_Join.FIRST_AMT_ADV').alias('FIRST_AMT_ADV'), col('Cust_Tot.CUST_TOT_BAL').alias('CUST_TOT_BAL'))

    Lk_Src_Skey_df = Lk_Src_Skey_df.selectExpr(
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV",
        "CAST(CUST_TOT_BAL AS DECIMAL(25,5)) AS CUST_TOT_BAL"
    )

    Lk_Src_Skey_alias = Lk_Src_Skey_df.alias("Lk_Src_Skey")
    temp_output_df = Lk_Src_Skey_alias
    Old_brnch_alias = Old_brnch_df.alias("Old_brnch")
    Old_brnch_alias = Old_brnch_alias.withColumnRenamed("BRNCH_NBR_SKEY", "OLD_ACCT_BRNCH_CD_SKEY")
    Old_brnch_alias = Old_brnch_alias.withColumnRenamed("BRNCH_NBR", "BRNCH_NBR_r")
    temp_output_df = temp_output_df.join(Old_brnch_alias, (Old_brnch_alias["BRNCH_NBR_r"] == temp_output_df["OLD_BRANCH"]), "left")
    temp_output_df = temp_output_df.drop("BRNCH_NBR_r")
    Cust_Brnch_alias = Cust_Brnch_df.alias("Cust_Brnch")
    Cust_Brnch_alias = Cust_Brnch_alias.withColumnRenamed("BRNCH_NBR_SKEY", "CUST_HOME_BR_SKEY")
    Cust_Brnch_alias = Cust_Brnch_alias.withColumnRenamed("BRNCH_NBR", "BRNCH_NBR_r")
    temp_output_df = temp_output_df.join(Cust_Brnch_alias, (Cust_Brnch_alias["BRNCH_NBR_r"] == temp_output_df["OLD_BRANCH"]), "left")
    temp_output_df = temp_output_df.drop("BRNCH_NBR_r")
    Lk_Src_Tfr_df = temp_output_df
    Lk_Src_Tfr_df = Lk_Src_Tfr_df.selectExpr(
        "CAST(OLD_ACCT_BRNCH_CD_SKEY AS LONG) AS OLD_ACCT_BRNCH_CD_SKEY",
        "CAST(CUST_HOME_BR_SKEY AS LONG) AS CUST_HOME_BR_SKEY",
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV",
        "CAST(CUST_TOT_BAL AS DECIMAL(25,5)) AS CUST_TOT_BAL"
    )

    from pyspark.sql.functions import *

    # Generated for activity: V157S0 by Transformer function
    # Processing input pin: V157S0P3 for activity V157S0

    # Processing output pin: V157S0P4 for activity V157S0
    LkTo_Lon_Agmnt_Ft_df = Lk_Src_Tfr_df
    # Column Operations
    # Directly passed through columns: ACCT_NBR, ACTIVITY_CODE, CUST_HOME_BR_SKEY, CUST_NBR, CUST_TOT_BAL, FIRST_ADV_DATE, FIRST_AMT_ADV, IND_CODE, NEW_CAPN_AMT, OLD_ACCT_BRNCH_CD_SKEY, OLD_BRANCH, OLD_CAPN_AMT, PSL_CODE, SBSD_SCHEME_CD, SPONSOR_ID, SUB_IND_CODE, SUB_SUB_IND_CODE
    # LkTo_Lon_Agmnt_Ft_df = LkTo_Lon_Agmnt_Ft_df.select('ACCT_NBR', 'ACTIVITY_CODE', 'CUST_HOME_BR_SKEY', 'CUST_NBR', 'CUST_TOT_BAL', 'FIRST_ADV_DATE', 'FIRST_AMT_ADV', 'IND_CODE', 'NEW_CAPN_AMT', 'OLD_ACCT_BRNCH_CD_SKEY', 'OLD_BRANCH', 'OLD_CAPN_AMT', 'PSL_CODE', 'SBSD_SCHEME_CD', 'SPONSOR_ID', 'SUB_IND_CODE', 'SUB_SUB_IND_CODE')


    # Processing output pin: V157S0P5 for activity V157S0
    Tfr_Rej_df = Lk_Src_Tfr_df
    # Column Operations
    # Directly passed through columns: ACCT_NBR, ACTIVITY_CODE, CUST_HOME_BR_SKEY, CUST_NBR, CUST_TOT_BAL, FIRST_ADV_DATE, FIRST_AMT_ADV, IND_CODE, NEW_CAPN_AMT, OLD_ACCT_BRNCH_CD_SKEY, OLD_BRANCH, OLD_CAPN_AMT, PSL_CODE, SBSD_SCHEME_CD, SPONSOR_ID, SUB_IND_CODE, SUB_SUB_IND_CODE
    # Tfr_Rej_df = Tfr_Rej_df.select('ACCT_NBR', 'ACTIVITY_CODE', 'CUST_HOME_BR_SKEY', 'CUST_NBR', 'CUST_TOT_BAL', 'FIRST_ADV_DATE', 'FIRST_AMT_ADV', 'IND_CODE', 'NEW_CAPN_AMT', 'OLD_ACCT_BRNCH_CD_SKEY', 'OLD_BRANCH', 'OLD_CAPN_AMT', 'PSL_CODE', 'SBSD_SCHEME_CD', 'SPONSOR_ID', 'SUB_IND_CODE', 'SUB_SUB_IND_CODE')

    LkTo_Lon_Agmnt_Ft_df = LkTo_Lon_Agmnt_Ft_df.selectExpr(
        "CAST(OLD_ACCT_BRNCH_CD_SKEY AS LONG) AS OLD_ACCT_BRNCH_CD_SKEY",
        "CAST(CUST_HOME_BR_SKEY AS LONG) AS CUST_HOME_BR_SKEY",
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV",
        "CAST(CUST_TOT_BAL AS DECIMAL(25,5)) AS CUST_TOT_BAL"
    )

    Tfr_Rej_df = Tfr_Rej_df.selectExpr(
        "CAST(OLD_ACCT_BRNCH_CD_SKEY AS LONG) AS OLD_ACCT_BRNCH_CD_SKEY",
        "CAST(CUST_HOME_BR_SKEY AS LONG) AS CUST_HOME_BR_SKEY",
        "CAST(ACCT_NBR AS STRING) AS ACCT_NBR",
        "CAST(NEW_CAPN_AMT AS DECIMAL(17,3)) AS NEW_CAPN_AMT",
        "CAST(OLD_CAPN_AMT AS DECIMAL(17,3)) AS OLD_CAPN_AMT",
        "CAST(ACTIVITY_CODE AS STRING) AS ACTIVITY_CODE",
        "CAST(SBSD_SCHEME_CD AS STRING) AS SBSD_SCHEME_CD",
        "CAST(IND_CODE AS STRING) AS IND_CODE",
        "CAST(SUB_IND_CODE AS STRING) AS SUB_IND_CODE",
        "CAST(SUB_SUB_IND_CODE AS STRING) AS SUB_SUB_IND_CODE",
        "CAST(SPONSOR_ID AS STRING) AS SPONSOR_ID",
        "CAST(OLD_BRANCH AS STRING) AS OLD_BRANCH",
        "CAST(CUST_NBR AS STRING) AS CUST_NBR",
        "CAST(PSL_CODE AS STRING) AS PSL_CODE",
        "CAST(FIRST_ADV_DATE AS INTEGER) AS FIRST_ADV_DATE",
        "CAST(FIRST_AMT_ADV AS DECIMAL(25,5)) AS FIRST_AMT_ADV",
        "CAST(CUST_TOT_BAL AS DECIMAL(25,5)) AS CUST_TOT_BAL"
    )




    # DB2 Connector Activity: V0S74

    LkTo_Lon_Agmnt_Ft_df.createOrReplaceTempView("LkTo_Lon_Agmnt_Ft_df_vw")
    db2_sql = f"""INSERT INTO VIBMAVPNE0.LON_AGREEMENT_WEEKLY_FACT SELECT * FROM LkTo_Lon_Agmnt_Ft_df_vw"""
    spark.sql(db2_sql)
    LkTo_Lon_Agmnt_Ft_df.writeTo("VIBMAVPNE0.LON_AGREEMENT_WEEKLY_FACT").using("iceberg").createOrReplace()
    Rej_Lnk_df = LkTo_Lon_Agmnt_Ft_df# Original path: {GDM_ParameterSet["SSRejectFilePath"]}/GDM/GDM_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.txt, converted to Parquet: {GDM_ParameterSet["SSRejectFilePath"]}/GDM/GDM_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.parquet
    Rej_Lnk_df.write.mode("overwrite").parquet(f"""{GDM_ParameterSet["SSRejectFilePath"]}/GDM/GDM_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.parquet""")
    # Original path: {GDM_ParameterSet["SSRejectFilePath"]}/GDM/Trf_GDM_TFR_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.txt, converted to Parquet: {GDM_ParameterSet["SSRejectFilePath"]}/GDM/Trf_GDM_TFR_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.parquet
    Tfr_Rej_df.write.mode("overwrite").parquet(f"""{GDM_ParameterSet["SSRejectFilePath"]}/GDM/Trf_GDM_TFR_LON_AGMNT_WKLY_FT_{PCS_ParameterSet["JpBusinessDate"]}.parquet""")

if __name__ == "__main__":
    main()
