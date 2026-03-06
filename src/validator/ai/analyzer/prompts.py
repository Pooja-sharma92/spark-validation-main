"""
Analysis Prompts for LLM
Contains carefully crafted prompts for different analysis aspects
"""


class AnalysisPrompts:
    """Collection of analysis prompts"""

    def business_logic_prompt(self, code: str, context: dict) -> str:
        """Prompt for business logic analysis"""
        return f"""
Analyze the following code and explain the business logic in 2-3 paragraphs:

File: {context.get('file_path', 'unknown')}
Lines of Code: {context.get('lines_of_code', 0)}

Code:
```python
{code[:3000]}  # Truncate if too long
```

Focus on:
1. What is the primary purpose of this code?
2. What business problem does it solve?
3. What are the key business rules and logic?
4. What data entities are involved?

Provide a clear, concise explanation that a business analyst could understand.
"""

    def data_flow_prompt(self, code: str, context: dict) -> str:
        """Prompt for data flow analysis"""
        return f"""
Analyze the data flow in this code and provide a detailed description:

Code:
```python
{code[:4000]}
```

Please describe:
1. Data Sources: What tables/sources are being read?
2. Transformations: What transformations are applied (joins, aggregations, filters)?
3. Data Flow: How does data flow from source to output?
4. Outputs: Where is the final data written?

Format your response as a structured data flow description, like:
- Source 1: [table name] → [columns read] → [transformations]
- Join: [table1] ⋈ [table2] on [key]
- Output: [destination] ([format])
"""

    def performance_prompt(self, code: str, context: dict) -> str:
        """Prompt for performance analysis"""
        return f"""
Analyze this Spark code for performance issues and opportunities:

Code:
```python
{code[:3000]}
```

Context:
- Total DataFrames: {context.get('metrics', {}).get('dataframes', 'unknown')}
- Total Joins: {context.get('metrics', {}).get('joins', 'unknown')}
- SQL Queries: {context.get('metrics', {}).get('sql_queries', 'unknown')}

Identify:
1. Performance bottlenecks (e.g., unnecessary shuffles, missing caching)
2. Inefficient operations (e.g., collect() on large data)
3. Missing optimizations (e.g., broadcast joins, partition pruning)
4. Repeated computations that could be cached

Provide specific, actionable recommendations. Format as a numbered list.
"""

    def security_prompt(self, code: str, context: dict) -> str:
        """Prompt for security analysis"""
        return f"""
Analyze this code for security vulnerabilities:

Code:
```python
{code[:3000]}
```

Check for:
1. SQL injection vulnerabilities
2. Hardcoded credentials or secrets
3. Insecure data access patterns
4. Missing input validation
5. Exposure of sensitive data

List only actual security concerns found. Format as bullet points.
If no significant security issues found, say "No critical security issues detected."
"""

    def refactoring_prompt(self, code: str, context: dict) -> str:
        """Prompt for refactoring suggestions"""
        return f"""
Suggest refactoring improvements for this code:

Code:
```python
{code[:3000]}
```

Metrics:
{context.get('metrics', {})}

Suggest improvements for:
1. Code duplication (DRY principle)
2. Function extraction opportunities
3. Variable naming improvements
4. Code organization and modularity
5. Removing dead code or unused variables

Provide specific, actionable suggestions. Format as numbered list.
Focus on the most impactful improvements first.
"""

    def comprehensive_analysis_prompt(self, code: str, context: dict, static_issues: list) -> str:
        """Comprehensive analysis prompt (used for full reports)"""
        issues_summary = "\n".join([
            f"- [{issue.severity}] Line {issue.line}: {issue.message}"
            for issue in static_issues[:10]
        ]) if static_issues else "No static analysis issues detected."

        return f"""
Perform a comprehensive analysis of this Spark/Python code:

File: {context.get('file_path')}
Lines of Code: {context.get('lines_of_code')}

Static Analysis Issues:
{issues_summary}

Code:
```python
{code[:5000]}
```

Provide a detailed analysis covering:

## 1. Executive Summary
- Brief overview of what the code does
- Critical findings (2-3 bullet points)

## 2. Code Structure & Design
- Overall architecture
- Design patterns used
- Identified anti-patterns

## 3. Data Flow Analysis
- Visual representation of data flow (text-based)
- Source → Transform → Sink mapping

## 4. Critical Issues (P0/P1)
- Bugs that prevent execution
- Logic errors
- Data quality risks

## 5. Performance Analysis
- Bottlenecks
- Optimization opportunities
- Estimated impact

## 6. Code Quality Assessment
- Maintainability score (0-10)
- Readability issues
- Technical debt

## 7. Top 5 Recommendations
- Prioritized list of improvements
- Expected benefits

Keep your analysis focused, specific, and actionable.
"""

    def quick_summary_prompt(self, code: str) -> str:
        """Quick summary for dashboard/overview"""
        return f"""
Provide a 3-sentence summary of this code:

```python
{code[:2000]}
```

Format:
1. What it does (purpose)
2. Main data sources and outputs
3. Key transformation logic
"""
