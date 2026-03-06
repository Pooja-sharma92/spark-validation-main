"""
AI-Powered Deep Code Analysis
Uses LLMs to understand business logic, data flow, and complex patterns
"""

import os
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from .prompts import AnalysisPrompts


@dataclass
class AIAnalysisResult:
    """Result from AI analysis"""
    business_logic: str
    data_flow: str
    performance_insights: List[str]
    security_concerns: List[str]
    refactoring_suggestions: List[str]
    code_quality_score: Dict[str, int]
    summary: str


class LLMAnalyzer:
    """AI-powered code analyzer using LLMs"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.provider = config.get('ai_provider', {}).get('type', 'openai')
        self.model = config.get('ai_provider', {}).get('model', 'gpt-4')
        self.client = self._initialize_client()
        self.prompts = AnalysisPrompts()

    def _initialize_client(self):
        """Initialize LLM client based on provider"""
        if self.provider == 'openai':
            try:
                import openai
                api_key = os.getenv('OPENAI_API_KEY') or self.config.get('ai_provider', {}).get('api_key')
                if not api_key:
                    raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
                openai.api_key = api_key
                return openai
            except ImportError:
                raise ImportError("openai package not installed. Run: pip install openai")

        elif self.provider == 'azure-openai' or self.provider == 'azure_openai':
            try:
                import openai

                # Azure OpenAI requires specific configuration
                endpoint = os.getenv('AZURE_OPENAI_ENDPOINT') or self.config.get('ai_provider', {}).get('endpoint')
                api_key = os.getenv('AZURE_OPENAI_KEY') or self.config.get('ai_provider', {}).get('api_key')
                api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')
                deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT') or self.model

                if not endpoint or not api_key:
                    raise ValueError(
                        "Azure OpenAI configuration incomplete. "
                        "Set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY environment variables."
                    )

                # Configure for Azure
                openai.api_type = "azure"
                openai.api_base = endpoint
                openai.api_key = api_key
                openai.api_version = api_version

                # Store deployment name for later use
                self.azure_deployment = deployment

                return openai
            except ImportError:
                raise ImportError("openai package not installed. Run: pip install openai")

        elif self.provider == 'anthropic':
            try:
                from anthropic import Anthropic
                api_key = os.getenv('ANTHROPIC_API_KEY') or self.config.get('ai_provider', {}).get('api_key')
                if not api_key:
                    raise ValueError("Anthropic API key not found. Set ANTHROPIC_API_KEY environment variable.")
                return Anthropic(api_key=api_key)
            except ImportError:
                raise ImportError("anthropic package not installed. Run: pip install anthropic")

        elif self.provider == 'ollama':
            try:
                import requests
                base_url = self.config.get('ai_provider', {}).get('base_url', 'http://localhost:11434')
                return {'base_url': base_url, 'requests': requests}
            except ImportError:
                raise ImportError("requests package not installed. Run: pip install requests")

        else:
            raise ValueError(f"Unsupported AI provider: {self.provider}")

    def analyze_code(self, code: str, file_path: str, static_results: Any = None) -> AIAnalysisResult:
        """Perform deep AI analysis of code"""

        # Prepare context from static analysis
        context = self._prepare_context(code, file_path, static_results)

        # Run different analysis aspects
        business_logic = self._analyze_business_logic(code, context)
        data_flow = self._analyze_data_flow(code, context)
        performance = self._analyze_performance(code, context)
        security = self._analyze_security(code, context)
        refactoring = self._analyze_refactoring_opportunities(code, context)
        quality_score = self._calculate_quality_score(code, static_results)
        summary = self._generate_summary(code, {
            'business_logic': business_logic,
            'data_flow': data_flow,
            'performance': performance,
            'security': security
        })

        return AIAnalysisResult(
            business_logic=business_logic,
            data_flow=data_flow,
            performance_insights=performance,
            security_concerns=security,
            refactoring_suggestions=refactoring,
            code_quality_score=quality_score,
            summary=summary
        )

    def _prepare_context(self, code: str, file_path: str, static_results: Any) -> Dict[str, Any]:
        """Prepare context for AI analysis"""
        context = {
            'file_path': file_path,
            'lines_of_code': len(code.split('\n')),
        }

        if static_results:
            context['static_issues'] = len(static_results.issues)
            context['critical_issues'] = static_results.summary.get('critical', 0)
            context['metrics'] = static_results.metrics

        return context

    def _call_llm(self, prompt: str, max_tokens: int = 2000) -> str:
        """Call LLM with prompt"""
        try:
            if self.provider == 'openai':
                response = self.client.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert code analyst specializing in Spark and ETL pipelines."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.3
                )
                return response.choices[0].message.content

            elif self.provider == 'azure-openai' or self.provider == 'azure_openai':
                # Azure OpenAI uses engine (deployment name) instead of model
                response = self.client.ChatCompletion.create(
                    engine=self.azure_deployment,  # Use deployment name for Azure
                    messages=[
                        {"role": "system", "content": "You are an expert code analyst specializing in Spark and ETL pipelines."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.3
                )
                return response.choices[0].message.content

            elif self.provider == 'anthropic':
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    messages=[
                        {"role": "user", "content": prompt}
                    ],
                    system="You are an expert code analyst specializing in Spark and ETL pipelines."
                )
                return message.content[0].text

            elif self.provider == 'ollama':
                import requests
                response = requests.post(
                    f"{self.client['base_url']}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False
                    }
                )
                return response.json().get('response', '')

        except Exception as e:
            return f"Error calling LLM: {str(e)}"

    def _analyze_business_logic(self, code: str, context: Dict) -> str:
        """Analyze business logic and purpose"""
        prompt = self.prompts.business_logic_prompt(code, context)
        return self._call_llm(prompt, max_tokens=1500)

    def _analyze_data_flow(self, code: str, context: Dict) -> str:
        """Analyze data flow and transformations"""
        prompt = self.prompts.data_flow_prompt(code, context)
        return self._call_llm(prompt, max_tokens=2000)

    def _analyze_performance(self, code: str, context: Dict) -> List[str]:
        """Analyze performance issues and opportunities"""
        prompt = self.prompts.performance_prompt(code, context)
        response = self._call_llm(prompt, max_tokens=1500)

        # Parse response into list
        insights = []
        for line in response.split('\n'):
            line = line.strip()
            if line and (line.startswith('-') or line.startswith('•') or line[0].isdigit()):
                insights.append(line.lstrip('-•0123456789. '))

        return insights if insights else [response]

    def _analyze_security(self, code: str, context: Dict) -> List[str]:
        """Analyze security concerns"""
        prompt = self.prompts.security_prompt(code, context)
        response = self._call_llm(prompt, max_tokens=1000)

        concerns = []
        for line in response.split('\n'):
            line = line.strip()
            if line and (line.startswith('-') or line.startswith('•') or line[0].isdigit()):
                concerns.append(line.lstrip('-•0123456789. '))

        return concerns if concerns else [response]

    def _analyze_refactoring_opportunities(self, code: str, context: Dict) -> List[str]:
        """Identify refactoring opportunities"""
        prompt = self.prompts.refactoring_prompt(code, context)
        response = self._call_llm(prompt, max_tokens=1500)

        suggestions = []
        for line in response.split('\n'):
            line = line.strip()
            if line and (line.startswith('-') or line.startswith('•') or line[0].isdigit()):
                suggestions.append(line.lstrip('-•0123456789. '))

        return suggestions if suggestions else [response]

    def _calculate_quality_score(self, code: str, static_results: Any) -> Dict[str, int]:
        """Calculate code quality scores (0-10)"""
        scores = {
            'functionality': 10,
            'performance': 10,
            'maintainability': 10,
            'reliability': 10,
            'security': 10
        }

        if not static_results:
            return scores

        # Deduct points based on issues
        critical_count = static_results.summary.get('critical', 0)
        warning_count = static_results.summary.get('warning', 0)

        scores['functionality'] = max(0, 10 - critical_count * 3)
        scores['reliability'] = max(0, 10 - critical_count * 2 - warning_count * 0.5)
        scores['security'] = max(0, 10 - len([i for i in static_results.issues if i.category == 'security']) * 2)

        # Performance score based on anti-patterns
        perf_issues = len([i for i in static_results.issues if i.category == 'performance'])
        scores['performance'] = max(0, 10 - perf_issues * 1)

        # Maintainability based on metrics
        metrics = static_results.metrics
        if metrics.get('total_lines', 0) > 500:
            scores['maintainability'] -= 2
        if metrics.get('dataframes', 0) > 10:
            scores['maintainability'] -= 1

        scores['maintainability'] = max(0, scores['maintainability'])

        return scores

    def _generate_summary(self, code: str, analyses: Dict) -> str:
        """Generate executive summary"""
        summary_prompt = f"""
Based on the following code analysis results, provide a concise executive summary (3-5 bullet points):

Business Logic: {analyses['business_logic'][:500]}
Data Flow: {analyses['data_flow'][:500]}
Performance: {str(analyses['performance'][:3])}
Security: {str(analyses['security'][:3])}

Format the summary as bullet points highlighting the most critical findings.
"""
        return self._call_llm(summary_prompt, max_tokens=500)


if __name__ == "__main__":
    # Test the AI analyzer
    test_code = """
    # Sample Spark code for testing
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.sql("SELECT * FROM users WHERE id = '{}'".format(user_id))
    df.collect()  # Potential performance issue
    """

    config = {
        'ai_provider': {
            'type': 'openai',
            'model': 'gpt-4'
        }
    }

    analyzer = LLMAnalyzer(config)
    result = analyzer.analyze_code(test_code, "test.py")

    print("=== AI Analysis Result ===")
    print(f"\nSummary:\n{result.summary}")
    print(f"\nQuality Scores: {result.code_quality_score}")
