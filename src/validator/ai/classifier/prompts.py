"""
AI Prompts for Job Classification

Contains specialized prompts for classifying Spark/PySpark jobs
by Domain, Module, Job Group, and Complexity.
"""

from typing import Dict, Any, List, Optional


class ClassificationPrompts:
    """Prompts for AI-based job classification"""

    SYSTEM_PROMPT = """You are an expert data engineer specializing in analyzing Spark/PySpark ETL jobs.
Your task is to classify jobs based on their code, identifying:
1. Business Domain (e.g., Finance, Customer, Risk, Marketing)
2. Functional Module (e.g., Loan Processing, Account Management)
3. Job Group type (e.g., ETL, Analytics, Reporting, Data Quality)
4. Complexity level (low/medium/high)

You understand DataStage-to-Spark migrations and can identify patterns from:
- SQL queries and table names
- DataFrame transformations
- Join patterns and aggregations
- Business logic in transformers

Always respond in valid JSON format."""

    @staticmethod
    def classification_prompt(
        code: str,
        file_path: str,
        existing_categories: Optional[Dict[str, List[str]]] = None,
        metrics: Optional[Dict[str, int]] = None
    ) -> str:
        """
        Generate the main classification prompt.

        Args:
            code: The Python/PySpark code to classify
            file_path: Path to the job file
            existing_categories: Available categories to match against
            metrics: Pre-computed code metrics
        """
        # Format existing categories for the prompt
        categories_context = ""
        if existing_categories:
            categories_context = """
## Existing Categories (prefer matching to these when appropriate)

### Domains:
{}

### Modules:
{}

### Job Groups:
{}
""".format(
                "\n".join(f"- {d}" for d in existing_categories.get("domains", [])),
                "\n".join(f"- {m}" for m in existing_categories.get("modules", [])),
                "\n".join(f"- {jg}" for jg in existing_categories.get("job_groups", []))
            )

        # Format metrics if available
        metrics_context = ""
        if metrics:
            metrics_context = f"""
## Pre-computed Metrics:
- Lines of Code: {metrics.get('lines_of_code', 'N/A')}
- SQL Queries: {metrics.get('sql_queries', 'N/A')}
- Joins: {metrics.get('joins', 'N/A')}
- Aggregations: {metrics.get('aggregations', 'N/A')}
- DataFrames: {metrics.get('dataframes', 'N/A')}
- Source Tables: {metrics.get('source_tables', 'N/A')}
"""

        return f"""Analyze the following Spark/PySpark job and classify it.

## File Path:
{file_path}
{categories_context}
{metrics_context}
## Code:
```python
{code[:8000]}  # Truncated if too long
```

## Classification Task:

Analyze the code and provide a classification in the following JSON format:

```json
{{
    "domain": {{
        "name": "Domain name (e.g., Finance, Customer, Risk)",
        "confidence": 0.0-1.0,
        "reasoning": "Brief explanation of why this domain"
    }},
    "module": {{
        "name": "Module name (e.g., Loan Processing, Account Management)",
        "parent_domain": "Parent domain name",
        "confidence": 0.0-1.0,
        "reasoning": "Brief explanation"
    }},
    "job_group": {{
        "name": "Job type (e.g., ETL, Analytics, Reporting, Data Quality, Dimension Load, Fact Load)",
        "confidence": 0.0-1.0,
        "reasoning": "Brief explanation"
    }},
    "complexity": {{
        "level": "low|medium|high",
        "score": 0-100,
        "reasoning": "Explanation of complexity factors",
        "factors": ["List of complexity drivers"]
    }},
    "summary": "One-sentence description of what this job does"
}}
```

## Classification Guidelines:

### Domain Detection:
- Look at table names, schemas, column names for business context
- Example: `LOAN_MASTER`, `CCOD_LON_PSL` → Finance/Lending domain
- Example: `CUSTOMER_DIM`, `CUST_SEGMENT` → Customer domain

### Module Detection:
- Identify specific business function within the domain
- Example: Loan tables + agreement data → "Loan Agreement Processing"
- Example: Risk scores + compliance → "Risk Assessment"

### Job Group Detection:
- ETL: Extract-Transform-Load pattern with source reads and target writes
- Analytics: Heavy aggregations, calculations, derived metrics
- Reporting: Formatted outputs, summary tables
- Data Quality: Validation, cleansing, reject handling
- Dimension Load: Loading dimension/lookup tables
- Fact Load: Loading fact tables with foreign keys to dimensions

### Complexity Assessment:
- LOW (score < 30): Simple transformations, 1-2 sources, minimal joins
- MEDIUM (30-70): Multiple sources, several joins, some aggregations
- HIGH (> 70): Many sources, complex joins, multiple aggregations, UDFs

Consider these factors:
- Number of source tables and SQL queries
- Number and types of joins (especially multi-way joins)
- Aggregation complexity
- Transformation logic complexity
- DataStage stage count (V###S# markers)
- Type casting complexity
- Error handling (reject file patterns)

Respond ONLY with the JSON object, no additional text."""

    @staticmethod
    def metrics_extraction_prompt(code: str) -> str:
        """
        Prompt to extract code metrics for complexity calculation.
        Used as a fallback when AST-based extraction is not available.
        """
        return f"""Analyze the following Spark/PySpark code and extract metrics.

```python
{code[:6000]}
```

Provide the metrics in JSON format:

```json
{{
    "lines_of_code": <total lines>,
    "sql_queries": <count of spark.sql() calls>,
    "joins": <count of .join() operations>,
    "aggregations": <count of .groupBy().agg() or .agg() operations>,
    "transformations": <count of .select(), .withColumn(), .filter() operations>,
    "dataframes": <count of distinct DataFrame variables>,
    "source_tables": <count of distinct source tables in SQL or reads>,
    "target_tables": <count of distinct target tables in writes>,
    "udfs": <count of UDF registrations or usages>,
    "datastage_stages": <count of V###S# stage markers in comments>
}}
```

Respond ONLY with the JSON object."""

    @staticmethod
    def category_suggestion_prompt(
        category_type: str,
        name: str,
        context: str,
        similar_categories: List[str]
    ) -> str:
        """
        Prompt to validate and describe a suggested new category.
        """
        similar_list = "\n".join(f"- {c}" for c in similar_categories) if similar_categories else "None"

        return f"""A new {category_type} category "{name}" has been suggested based on job classification.

## Context from job code:
{context[:2000]}

## Similar existing categories:
{similar_list}

Evaluate this suggestion and provide:

```json
{{
    "should_create": true|false,
    "recommended_name": "Standardized category name",
    "description": "Clear description of what this category represents",
    "merge_with": "Existing category name if this should be merged, or null",
    "confidence": 0.0-1.0,
    "reasoning": "Explanation of the recommendation"
}}
```

Guidelines:
- Prefer merging with existing categories if semantically similar
- Standardize naming (Title Case, clear terminology)
- Ensure the category is specific enough to be useful
- Consider whether this represents a genuine new business concept

Respond ONLY with the JSON object."""

    @staticmethod
    def batch_classification_prompt(
        jobs: List[Dict[str, str]],
        existing_categories: Dict[str, List[str]]
    ) -> str:
        """
        Prompt for classifying multiple jobs in a single request.
        More efficient for batch processing.
        """
        job_list = ""
        for i, job in enumerate(jobs, 1):
            job_list += f"""
### Job {i}: {job['file_path']}
```python
{job['code'][:3000]}
```
"""

        categories_context = ""
        if existing_categories:
            categories_context = f"""
## Available Categories:
- Domains: {', '.join(existing_categories.get('domains', []))}
- Modules: {', '.join(existing_categories.get('modules', []))}
- Job Groups: {', '.join(existing_categories.get('job_groups', []))}
"""

        return f"""Classify the following {len(jobs)} Spark/PySpark jobs.
{categories_context}
## Jobs to Classify:
{job_list}

Provide classifications in JSON format:

```json
{{
    "classifications": [
        {{
            "file_path": "path/to/job1.py",
            "domain": "Domain name",
            "module": "Module name",
            "job_group": "Job Group",
            "complexity": "low|medium|high",
            "complexity_score": 0-100,
            "confidence": 0.0-1.0,
            "summary": "Brief description"
        }},
        ...
    ]
}}
```

Respond ONLY with the JSON object."""

    @staticmethod
    def reclassification_prompt(
        code: str,
        file_path: str,
        previous_classification: Dict[str, Any],
        feedback: Optional[str] = None
    ) -> str:
        """
        Prompt for reclassifying a job with context from previous classification.
        """
        feedback_section = ""
        if feedback:
            feedback_section = f"""
## User Feedback on Previous Classification:
{feedback}
"""

        return f"""Reclassify the following job, considering the previous classification.

## File Path:
{file_path}

## Previous Classification:
- Domain: {previous_classification.get('domain', 'N/A')}
- Module: {previous_classification.get('module', 'N/A')}
- Job Group: {previous_classification.get('job_group', 'N/A')}
- Complexity: {previous_classification.get('complexity', 'N/A')}
{feedback_section}

## Current Code:
```python
{code[:8000]}
```

Provide an updated classification, considering:
1. Changes in the code since last classification
2. Any feedback provided
3. Whether the previous classification was accurate

Respond with the same JSON format as the standard classification prompt."""


# Singleton instance for convenience
prompts = ClassificationPrompts()
