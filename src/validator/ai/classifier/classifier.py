"""
LLM-based Job Classifier.

Classifies Spark jobs using AI with provider fallback support.
Uses the unified AI core for provider management.
"""

import re
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

from ..core import UnifiedLLMClient, AIError
from .models import (
    ClassificationResult,
    Complexity,
    ComplexityMetrics,
)
from .prompts import ClassificationPrompts

logger = logging.getLogger(__name__)


class ClassificationError(AIError):
    """Error during job classification."""
    pass


class LLMClassifier:
    """
    AI-powered job classifier using LLMs.

    Uses the unified LLM client for provider management with
    automatic fallback between Ollama and Azure OpenAI.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the classifier.

        Args:
            config: Classification configuration from framework.yaml
                   Should contain 'ai' or 'ai_providers' section
        """
        self.config = config

        # Initialize unified LLM client
        ai_config = config.get("ai", config.get("ai_providers", config))
        self.client = UnifiedLLMClient.from_config(ai_config)

        # Complexity scoring config
        self.complexity_weights = config.get("complexity_weights", {
            "lines_of_code": 0.1,
            "sql_queries": 5,
            "joins": 10,
            "aggregations": 8,
            "transformations": 3,
            "dataframes": 2,
            "source_tables": 5,
            "udfs": 7,
        })

        self.complexity_thresholds = config.get("complexity_thresholds", {
            "low": 30,
            "medium": 70,
        })

        self.prompts = ClassificationPrompts()

    def classify_job(
        self,
        code: str,
        file_path: str,
        existing_categories: Optional[Dict[str, List[str]]] = None,
    ) -> Tuple[ClassificationResult, str]:
        """
        Classify a job using AI.

        Args:
            code: The Python/PySpark code
            file_path: Path to the job file
            existing_categories: Available categories to match against

        Returns:
            Tuple of (ClassificationResult, provider_used)

        Raises:
            ClassificationError: If classification fails
        """
        # Extract metrics from code first
        metrics = self._extract_metrics(code)

        # Generate prompt
        prompt = self.prompts.classification_prompt(
            code=code,
            file_path=file_path,
            existing_categories=existing_categories,
            metrics=metrics.to_dict()
        )

        try:
            # Call LLM via unified client
            response = self.client.call(
                system_prompt=self.prompts.SYSTEM_PROMPT,
                user_prompt=prompt,
                temperature=0.3,
                max_tokens=4000,
            )

            # Parse response
            result = self._parse_classification_response(
                response.content,
                metrics,
                existing_categories
            )
            result.raw_response = {
                "provider": response.provider,
                "model": response.model,
                "response": response.content,
                "tokens_used": response.total_tokens,
            }

            return result, response.provider

        except AIError as e:
            raise ClassificationError(
                f"Classification failed: {e}",
                details={"file_path": file_path}
            )

    def _extract_metrics(self, code: str) -> ComplexityMetrics:
        """Extract code metrics using regex patterns."""
        metrics = ComplexityMetrics()

        # Lines of code
        lines = code.split('\n')
        metrics.lines_of_code = len([l for l in lines if l.strip() and not l.strip().startswith('#')])

        # SQL queries (spark.sql())
        metrics.sql_queries = len(re.findall(r'spark\.sql\s*\(', code))

        # Joins (.join())
        metrics.joins = len(re.findall(r'\.join\s*\(', code))

        # Aggregations (groupBy, agg)
        metrics.aggregations = len(re.findall(r'\.(groupBy|agg)\s*\(', code))

        # Transformations (select, withColumn, filter, where)
        metrics.transformations = len(re.findall(r'\.(select|withColumn|filter|where)\s*\(', code))

        # DataFrames (variable assignments ending in _df or containing DataFrame)
        df_pattern = r'\b\w+_df\b|\bDataFrame\b'
        metrics.dataframes = len(set(re.findall(df_pattern, code)))

        # Source tables (FROM clauses, table names in SQL)
        from_pattern = r'FROM\s+([A-Za-z_][A-Za-z0-9_\.]*)'
        tables = set(re.findall(from_pattern, code, re.IGNORECASE))
        metrics.source_tables = len(tables)

        # UDFs
        metrics.udfs = len(re.findall(r'register_udfs|udf\s*\(|@udf', code))

        return metrics

    def _calculate_complexity_score(self, metrics: ComplexityMetrics) -> int:
        """Calculate complexity score based on metrics and weights."""
        score = 0.0

        score += (metrics.lines_of_code / 100) * self.complexity_weights.get("lines_of_code", 0.1)
        score += metrics.sql_queries * self.complexity_weights.get("sql_queries", 5)
        score += metrics.joins * self.complexity_weights.get("joins", 10)
        score += metrics.aggregations * self.complexity_weights.get("aggregations", 8)
        score += metrics.transformations * self.complexity_weights.get("transformations", 3)
        score += metrics.dataframes * self.complexity_weights.get("dataframes", 2)
        score += metrics.source_tables * self.complexity_weights.get("source_tables", 5)
        score += metrics.udfs * self.complexity_weights.get("udfs", 7)

        return min(100, max(0, int(score)))

    def _determine_complexity(self, score: int) -> Complexity:
        """Determine complexity level from score."""
        low_threshold = self.complexity_thresholds.get("low", 30)
        medium_threshold = self.complexity_thresholds.get("medium", 70)

        if score < low_threshold:
            return Complexity.LOW
        elif score < medium_threshold:
            return Complexity.MEDIUM
        else:
            return Complexity.HIGH

    def _parse_classification_response(
        self,
        response: str,
        metrics: ComplexityMetrics,
        existing_categories: Optional[Dict[str, List[str]]]
    ) -> ClassificationResult:
        """Parse LLM response into ClassificationResult."""
        # Extract JSON from response
        json_str = self._extract_json(response)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            # Fall back to regex extraction
            data = self._fallback_parse(response)

        # Extract classification data
        domain_data = data.get("domain", {})
        module_data = data.get("module", {})
        job_group_data = data.get("job_group", {})
        complexity_data = data.get("complexity", {})

        domain = domain_data.get("name", "Unknown") if isinstance(domain_data, dict) else str(domain_data)
        module = module_data.get("name", "Unknown") if isinstance(module_data, dict) else str(module_data)
        job_group = job_group_data.get("name", "ETL") if isinstance(job_group_data, dict) else str(job_group_data)

        # Get complexity from AI or calculate
        complexity_level = complexity_data.get("level", "medium") if isinstance(complexity_data, dict) else "medium"
        ai_complexity_score = complexity_data.get("score") if isinstance(complexity_data, dict) else None

        # Calculate our own complexity score
        calculated_score = self._calculate_complexity_score(metrics)

        # Use AI score if provided and reasonable, otherwise use calculated
        complexity_score = ai_complexity_score if ai_complexity_score is not None else calculated_score

        # Determine complexity level
        if complexity_level.lower() in ["low", "medium", "high"]:
            complexity = Complexity(complexity_level.lower())
        else:
            complexity = self._determine_complexity(complexity_score)

        # Get reasoning
        complexity_reasoning = ""
        if isinstance(complexity_data, dict):
            complexity_reasoning = complexity_data.get("reasoning", "")
            factors = complexity_data.get("factors", [])
            if factors:
                complexity_reasoning += f" Factors: {', '.join(factors)}"

        # Calculate confidence
        confidences = []
        if isinstance(domain_data, dict) and "confidence" in domain_data:
            confidences.append(domain_data["confidence"])
        if isinstance(module_data, dict) and "confidence" in module_data:
            confidences.append(module_data["confidence"])
        if isinstance(job_group_data, dict) and "confidence" in job_group_data:
            confidences.append(job_group_data["confidence"])

        confidence = sum(confidences) / len(confidences) if confidences else 0.7

        # Check if categories exist
        domain_exists = True
        module_exists = True
        job_group_exists = True

        if existing_categories:
            domains = [d.lower() for d in existing_categories.get("domains", [])]
            modules = [m.lower() for m in existing_categories.get("modules", [])]
            job_groups = [jg.lower() for jg in existing_categories.get("job_groups", [])]

            domain_exists = domain.lower() in domains
            module_exists = module.lower() in modules
            job_group_exists = job_group.lower() in job_groups

        return ClassificationResult(
            domain=domain,
            module=module,
            job_group=job_group,
            complexity=complexity,
            complexity_score=complexity_score,
            complexity_reasoning=complexity_reasoning,
            confidence_score=confidence,
            metrics=metrics,
            domain_exists=domain_exists,
            module_exists=module_exists,
            job_group_exists=job_group_exists,
        )

    def _extract_json(self, response: str) -> str:
        """Extract JSON object from response text."""
        # Try to find JSON block
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            return json_match.group(1)

        # Try to find raw JSON object
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
        if json_match:
            return json_match.group(0)

        # Return full response and hope for the best
        return response

    def _fallback_parse(self, response: str) -> Dict[str, Any]:
        """Fallback parsing when JSON fails."""
        result = {
            "domain": {"name": "Unknown", "confidence": 0.5},
            "module": {"name": "Unknown", "confidence": 0.5},
            "job_group": {"name": "ETL", "confidence": 0.5},
            "complexity": {"level": "medium", "score": 50},
        }

        # Try to extract domain
        domain_match = re.search(r'domain["\s:]+([A-Za-z\s]+)', response, re.IGNORECASE)
        if domain_match:
            result["domain"]["name"] = domain_match.group(1).strip()

        # Try to extract module
        module_match = re.search(r'module["\s:]+([A-Za-z\s]+)', response, re.IGNORECASE)
        if module_match:
            result["module"]["name"] = module_match.group(1).strip()

        # Try to extract job group
        job_group_match = re.search(r'job.?group["\s:]+([A-Za-z\s]+)', response, re.IGNORECASE)
        if job_group_match:
            result["job_group"]["name"] = job_group_match.group(1).strip()

        # Try to extract complexity
        complexity_match = re.search(r'complexity["\s:]+["\s]*(low|medium|high)', response, re.IGNORECASE)
        if complexity_match:
            result["complexity"]["level"] = complexity_match.group(1).lower()

        return result


def create_classifier(config: Dict[str, Any]) -> LLMClassifier:
    """Factory function to create a classifier."""
    classifier_config = config.get("ai", config.get("classification", config))
    return LLMClassifier(classifier_config)
