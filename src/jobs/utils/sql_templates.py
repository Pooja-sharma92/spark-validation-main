"""
SQL Query Templates for STG_XTC_LD_TEST_JOB

This module provides parameterized SQL query templates that use configuration
values instead of hardcoded table names, schemas, and filter conditions.

Benefits:
- All SQL logic in one place
- Easy to test with different configurations
- Can switch table names for testing
- Safer than string concatenation (reduces SQL injection risk)

Usage:
    from utils.config_loader import load_config
    from utils.sql_templates import SQLTemplates

    config = load_config(environment="test")
    sql_gen = SQLTemplates(config, business_date="2024-12-31")

    # Generate SQL queries
    sql = sql_gen.get_account_detail_query()
    df = spark.sql(sql)
"""

import sys
from pathlib import Path
from typing import Optional

# Add parent directory to import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.job_config_schema import JobConfig


class SQLTemplates:
    """
    SQL query template generator using JobConfig.

    All queries are parameterized with table names, schemas,
    and filter conditions from configuration.
    """

    def __init__(self, config: JobConfig, business_date: str):
        """
        Initialize SQL template generator.

        Args:
            config: JobConfig instance with all settings
            business_date: Business date for queries (YYYY-MM-DD format)
        """
        self.config = config
        self.business_date = business_date

    def get_account_detail_query(self) -> str:
        """
        Generate V138S0 query: Account Detail (Driving DataFrame).

        This query joins LOAN_MASTER with DT_DIM to get account details
        for the business date, filtering by status codes and financial year.

        Returns:
            SQL query string
        """
        # Format status codes for SQL IN clause
        status_codes = ','.join(f"'{code}'" for code in self.config.filters.status_codes)

        # Build fully qualified table names
        loan_master = self.config.get_table_fqn("source", "loan_master")
        # Note: date_dimension is in dim_schema
        date_dim = f"{self.config.databases.dim_schema}.{self.config.tables.sources.date_dimension}"

        return f"""
        SELECT
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
            {loan_master} A
            INNER JOIN {date_dim} D
        ON
            to_date('{self.business_date}') = D.DT
            AND (
                CASE
                    WHEN STAT IN ({status_codes})
                    THEN nvl(LST_FIN_DATE, '{self.config.filters.min_date}')
                    ELSE '{self.config.filters.max_date}'
                END
            ) >= FIN_YR_STRT_DT
        """.strip()

    def get_psl_lookup_query(self) -> str:
        """
        Generate V0S108 query: PSL Lookup.

        Note: The original query has a syntax error:
        "WHERE  = CURRENT DBPARTITIONNUM"
        This is DB2-specific syntax that doesn't work in Spark.

        For now, we'll omit the WHERE clause. If DB partitioning
        is needed, it should be configured differently.

        Returns:
            SQL query string
        """
        psl_lookup = self.config.get_table_fqn("source", "psl_lookup")

        # TODO: Clarify the DBPARTITIONNUM requirement with business users
        # Original: WHERE  = CURRENT DBPARTITIONNUM (syntax error)
        return f"""
        SELECT
            ACCT_NBR,
            CUST_NBR,
            PSL_CODE
        FROM
            {psl_lookup}
        """.strip()
        # Note: WHERE clause removed due to syntax error in original code

    def get_advance_detail_query(self) -> str:
        """
        Generate V0S126 query: Advance Detail.

        This query joins advance detail with loan account detail and date dimension
        to calculate first advance amounts and dates.

        Returns:
            SQL query string
        """
        # Format status codes for SQL IN clause
        status_codes = ','.join(f"'{code}'" for code in self.config.filters.status_codes)

        # Build fully qualified table names
        advance_detail = f"{self.config.databases.source_schema}.{self.config.tables.sources.advance_detail}"
        loan_acct_detail = f"{self.config.databases.source_schema}.{self.config.tables.sources.loan_account_detail}"
        date_dim = f"{self.config.databases.source_schema}.{self.config.tables.sources.date_dimension}"

        return f"""
        SELECT
            ACCT_NO AS ACCT_NBR,
            (CASE
                WHEN POST_DATE >= MTH_STRT_DT AND POST_DATE <= '{self.business_date}'
                THEN FIRST_AMT_ADV
                ELSE 0
            END) FIRST_AMT_ADV,
            udf_to_number(DATE_FORMAT(POST_DATE, 'YYYYMMDD')) AS FIRST_ADV_DATE
        FROM
            {advance_detail} A,
            {loan_acct_detail} B,
            {date_dim} C
        WHERE
            A.ACCT_NO = B.ACCT_NBR
            AND C.DT = '{self.business_date}'
            AND (
                CASE
                    WHEN STAT IN ({status_codes})
                    THEN nvl(LST_FIN_DATE, '{self.config.filters.min_date}')
                    ELSE '{self.config.filters.max_date}'
                END
            ) >= FIN_YR_STRT_DT
        """.strip()

    def get_customer_balance_query(self) -> str:
        """
        Generate V0S132 query: Customer Total Balance.

        This query joins customer total balance tables with a table ID filter.

        Returns:
            SQL query string
        """
        cust_balance = f"{self.config.databases.source_schema}.{self.config.tables.sources.customer_balance}"
        loan_cust_balance = f"{self.config.databases.source_schema}.{self.config.tables.sources.loan_customer_balance}"

        return f"""
        SELECT
            A.CUST_NBR,
            A.CUST_TOT_BAL
        FROM
            {cust_balance} A,
            {loan_cust_balance} B
        WHERE
            A.CUST_NBR = B.CUST_NBR
            AND TBL_ID = {self.config.filters.customer_balance_table_id}
        """.strip()

    def get_branch_master_query(self) -> str:
        """
        Generate V154S0 query: Branch Master Dimension.

        This query gets branch information with the latest flag filter.

        Returns:
            SQL query string
        """
        branch_master = f"{self.config.databases.source_schema}.{self.config.tables.sources.branch_master}"

        return f"""
        SELECT
            BRNCH_NBR,
            CRCL_CD,
            BRNCH_NBR_SKEY
        FROM
            {branch_master}
        WHERE
            LATEST_FLG = '{self.config.filters.latest_flag}'
        """.strip()

    def get_insert_sql(self, temp_view_name: str) -> str:
        """
        Generate INSERT INTO SQL for writing to target table.

        Note: This SQL is for compatibility but the actual write
        uses DataFrame.writeTo() with Iceberg.

        Args:
            temp_view_name: Name of temporary view containing data

        Returns:
            SQL query string
        """
        target_table = self.config.get_table_fqn("target", "fact_table")

        return f"""
        INSERT INTO {target_table}
        SELECT * FROM {temp_view_name}
        """.strip()

    def get_query_by_stage(self, stage_name: str) -> str:
        """
        Get SQL query by stage name.

        Args:
            stage_name: Stage identifier (V138S0, V0S108, etc.)

        Returns:
            SQL query string

        Raises:
            ValueError: If stage name is unknown
        """
        stage_map = {
            'V138S0': self.get_account_detail_query,
            'V0S108': self.get_psl_lookup_query,
            'V0S126': self.get_advance_detail_query,
            'V0S132': self.get_customer_balance_query,
            'V154S0': self.get_branch_master_query,
        }

        if stage_name not in stage_map:
            raise ValueError(
                f"Unknown stage: {stage_name}. "
                f"Available stages: {list(stage_map.keys())}"
            )

        return stage_map[stage_name]()

    def get_all_queries(self) -> dict:
        """
        Get all SQL queries as a dictionary.

        Returns:
            Dictionary mapping stage names to SQL queries

        Example:
            queries = sql_gen.get_all_queries()
            for stage, sql in queries.items():
                print(f"{stage}:\n{sql}\n")
        """
        return {
            'V138S0_account_detail': self.get_account_detail_query(),
            'V0S108_psl_lookup': self.get_psl_lookup_query(),
            'V0S126_advance_detail': self.get_advance_detail_query(),
            'V0S132_customer_balance': self.get_customer_balance_query(),
            'V154S0_branch_master': self.get_branch_master_query(),
        }


# Convenience function
def generate_query(
    config: JobConfig,
    business_date: str,
    stage_name: str
) -> str:
    """
    Convenience function to generate a single query.

    Args:
        config: JobConfig instance
        business_date: Business date string
        stage_name: Stage identifier

    Returns:
        SQL query string

    Example:
        from utils.config_loader import load_config
        from utils.sql_templates import generate_query

        config = load_config(environment="test")
        sql = generate_query(config, "2024-12-31", "V138S0")
    """
    sql_gen = SQLTemplates(config, business_date)
    return sql_gen.get_query_by_stage(stage_name)
