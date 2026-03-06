"""
Intelligent Analysis Planner (Claude Code Style)
AI decides what to analyze and how deeply
"""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum


class AnalysisDepth(Enum):
    """How deeply to analyze the code"""
    QUICK_SCAN = "quick_scan"  # Just static analysis + AI summary
    MODERATE = "moderate"  # Static + AI on priority areas
    DEEP = "deep"  # Full static + full AI analysis


@dataclass
class IntelligentAnalysisPlan:
    """AI-generated analysis plan with depth decision"""

    # AI's decision
    recommended_depth: AnalysisDepth
    reasoning: str  # Why this depth?

    # Code assessment
    code_type: str  # "ETL job", "API server", etc.
    complexity_level: str  # "simple", "moderate", "complex"
    risk_level: str  # "low", "medium", "high", "critical"

    # What to analyze (if not quick scan)
    priority_areas: List[str]  # Specific areas needing attention
    skip_areas: List[str]  # Areas that are fine

    # Expected findings
    expected_issues: List[str]
    estimated_issue_count: Dict[str, int]  # {"critical": 2, "warning": 5}

    # Cost estimate
    estimated_cost: float  # In USD
    estimated_time: int  # In seconds


class IntelligentPlanner:
    """
    AI planner that decides analysis strategy (Claude Code style)

    Philosophy:
    1. AI reads code quickly
    2. AI decides: Is this code risky/complex enough for deep analysis?
    3. If yes: AI identifies what specific areas need attention
    4. If no: Quick static scan is sufficient
    """

    def __init__(self, ai_provider: str = "openai", model: str = "gpt-4"):
        self.provider = ai_provider
        self.model = model
        self.client = self._initialize_client()

    def _initialize_client(self):
        """Initialize LLM client"""
        if self.provider == 'openai':
            try:
                import openai
                api_key = os.getenv('OPENAI_API_KEY')
                if not api_key:
                    raise ValueError("OPENAI_API_KEY not found")
                openai.api_key = api_key
                return openai
            except ImportError:
                raise ImportError("openai package not installed")

        elif self.provider == 'azure-openai' or self.provider == 'azure_openai':
            try:
                import openai
                endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
                api_key = os.getenv('AZURE_OPENAI_KEY')
                api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-15-preview')
                deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT') or self.model

                if not endpoint or not api_key:
                    raise ValueError("Azure OpenAI configuration incomplete")

                openai.api_type = "azure"
                openai.api_base = endpoint
                openai.api_key = api_key
                openai.api_version = api_version
                self.azure_deployment = deployment

                return openai
            except ImportError:
                raise ImportError("openai package not installed")

        elif self.provider == 'anthropic':
            try:
                from anthropic import Anthropic
                api_key = os.getenv('ANTHROPIC_API_KEY')
                if not api_key:
                    raise ValueError("ANTHROPIC_API_KEY not found")
                return Anthropic(api_key=api_key)
            except ImportError:
                raise ImportError("anthropic package not installed")

        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def assess_and_plan(self, code: str, file_path: str) -> IntelligentAnalysisPlan:
        """
        Stage 1: AI Quick Assessment

        AI decides:
        1. Does this code need deep analysis?
        2. What's the risk level?
        3. Where should we focus?
        """

        # Quick scan (first 5000 chars to save cost)
        code_sample = code[:5000]

        prompt = self._create_assessment_prompt(code_sample, file_path, len(code))
        response = self._call_llm(prompt, max_tokens=1500)

        # Parse AI's decision
        plan = self._parse_assessment(response, code)

        return plan

    def _create_assessment_prompt(self, code_sample: str, file_path: str, total_length: int) -> str:
        """Create prompt for AI assessment"""
        return f"""You are an intelligent code analyzer deciding analysis strategy.

File: {file_path}
Total lines: {len(code_sample.split(chr(10)))} (showing first 5000 chars of {total_length} total)

Code Sample:
```python
{code_sample}
```

Your task: Decide the analysis strategy.

## Decision Framework

### QUICK_SCAN (cheapest, fastest)
Use when:
- Simple, straightforward code
- No obvious red flags
- Standard patterns, well-written
- Low complexity
Example: Simple utility functions, config files

### MODERATE (balanced)
Use when:
- Some complexity or risk
- A few areas need attention
- Mix of good and concerning patterns
- Medium complexity
Example: Standard ETL jobs, typical APIs

### DEEP (thorough, expensive)
Use when:
- High complexity or critical bugs visible
- Security concerns
- Performance-critical code
- Auto-generated code needing validation
- Multiple red flags
Example: Complex data pipelines, security-sensitive code

## Your Analysis

1. **Code Type**: What is this? (1-2 words)

2. **Complexity**: simple | moderate | complex

3. **Risk Level**: low | medium | high | critical
   Risk indicators:
   - SQL injection patterns
   - Missing error handling
   - Performance anti-patterns (collect(), missing cache)
   - Auto-generated code
   - Critical bugs

4. **Recommended Depth**: QUICK_SCAN | MODERATE | DEEP

5. **Reasoning**: Why this depth? (1-2 sentences)

6. **Priority Areas** (if MODERATE or DEEP):
   List 2-5 specific areas to analyze deeply:
   - Area 1: [description, line range if visible]
   - Area 2: ...

7. **Expected Issues**:
   Quick estimate:
   - Critical: X
   - Warning: Y
   - Info: Z

8. **Skip Areas** (if MODERATE):
   Parts that look fine, can skip detailed analysis

Format your response as:
CODE_TYPE: [type]
COMPLEXITY: [level]
RISK: [level]
DEPTH: [QUICK_SCAN|MODERATE|DEEP]
REASONING: [why]
PRIORITY_AREAS:
- [area 1]
- [area 2]
SKIP_AREAS:
- [area 1]
EXPECTED_ISSUES: critical=X, warning=Y, info=Z
"""

    def _call_llm(self, prompt: str, max_tokens: int = 1500) -> str:
        """Call LLM for assessment"""
        try:
            if self.provider == 'openai':
                response = self.client.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert code analyst making strategic decisions about analysis depth."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.2  # Lower for more consistent decisions
                )
                return response.choices[0].message.content

            elif self.provider == 'azure-openai' or self.provider == 'azure_openai':
                response = self.client.ChatCompletion.create(
                    engine=self.azure_deployment,
                    messages=[
                        {"role": "system", "content": "You are an expert code analyst making strategic decisions about analysis depth."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.2
                )
                return response.choices[0].message.content

            elif self.provider == 'anthropic':
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                    system="You are an expert code analyst making strategic decisions about analysis depth."
                )
                return message.content[0].text

        except Exception as e:
            return f"Error: {str(e)}"

    def _parse_assessment(self, response: str, full_code: str) -> IntelligentAnalysisPlan:
        """Parse AI's assessment into structured plan"""

        lines = response.split('\n')

        # Extract fields
        code_type = "unknown"
        complexity = "moderate"
        risk = "medium"
        depth_str = "MODERATE"
        reasoning = ""
        priority_areas = []
        skip_areas = []
        expected_critical = 0
        expected_warning = 0
        expected_info = 0

        current_section = None

        for line in lines:
            line = line.strip()

            if line.startswith('CODE_TYPE:'):
                code_type = line.split(':', 1)[1].strip()
            elif line.startswith('COMPLEXITY:'):
                complexity = line.split(':', 1)[1].strip().lower()
            elif line.startswith('RISK:'):
                risk = line.split(':', 1)[1].strip().lower()
            elif line.startswith('DEPTH:'):
                depth_str = line.split(':', 1)[1].strip().upper()
            elif line.startswith('REASONING:'):
                reasoning = line.split(':', 1)[1].strip()
            elif line.startswith('PRIORITY_AREAS:'):
                current_section = 'priority'
            elif line.startswith('SKIP_AREAS:'):
                current_section = 'skip'
            elif line.startswith('EXPECTED_ISSUES:'):
                # Parse: critical=2, warning=5, info=3
                issues_str = line.split(':', 1)[1].strip()
                import re
                critical_match = re.search(r'critical=(\d+)', issues_str)
                warning_match = re.search(r'warning=(\d+)', issues_str)
                info_match = re.search(r'info=(\d+)', issues_str)

                if critical_match:
                    expected_critical = int(critical_match.group(1))
                if warning_match:
                    expected_warning = int(warning_match.group(1))
                if info_match:
                    expected_info = int(info_match.group(1))
            elif line.startswith('-') and current_section:
                content = line.lstrip('- ')
                if current_section == 'priority':
                    priority_areas.append(content)
                elif current_section == 'skip':
                    skip_areas.append(content)

        # Map string to enum
        depth_map = {
            'QUICK_SCAN': AnalysisDepth.QUICK_SCAN,
            'MODERATE': AnalysisDepth.MODERATE,
            'DEEP': AnalysisDepth.DEEP
        }
        depth = depth_map.get(depth_str, AnalysisDepth.MODERATE)

        # Estimate cost and time
        estimated_cost = self._estimate_cost(depth, len(full_code))
        estimated_time = self._estimate_time(depth, len(full_code))

        return IntelligentAnalysisPlan(
            recommended_depth=depth,
            reasoning=reasoning,
            code_type=code_type,
            complexity_level=complexity,
            risk_level=risk,
            priority_areas=priority_areas,
            skip_areas=skip_areas,
            expected_issues=[
                f"Critical issues: ~{expected_critical}",
                f"Warnings: ~{expected_warning}",
                f"Info: ~{expected_info}"
            ],
            estimated_issue_count={
                'critical': expected_critical,
                'warning': expected_warning,
                'info': expected_info
            },
            estimated_cost=estimated_cost,
            estimated_time=estimated_time
        )

    def _estimate_cost(self, depth: AnalysisDepth, code_length: int) -> float:
        """Estimate analysis cost in USD"""
        # Rough estimates for GPT-4
        if depth == AnalysisDepth.QUICK_SCAN:
            return 0.01  # Just planning
        elif depth == AnalysisDepth.MODERATE:
            return 0.05  # Planning + targeted analysis
        else:  # DEEP
            return 0.15  # Full analysis

    def _estimate_time(self, depth: AnalysisDepth, code_length: int) -> int:
        """Estimate analysis time in seconds"""
        if depth == AnalysisDepth.QUICK_SCAN:
            return 5
        elif depth == AnalysisDepth.MODERATE:
            return 20
        else:  # DEEP
            return 45


if __name__ == "__main__":
    # Test the intelligent planner
    test_code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

# SQL injection risk
user_input = "123"
df = spark.sql(f"SELECT * FROM users WHERE id = {user_input}")

# Missing cache
df2 = df.filter("age > 18")
df2.count()
df2.show()  # Recomputed!

# Collect anti-pattern
all_data = df.collect()  # Dangerous!
"""

    planner = IntelligentPlanner(ai_provider="openai")
    plan = planner.assess_and_plan(test_code, "test.py")

    print("=== Intelligent Analysis Plan ===")
    print(f"Recommended Depth: {plan.recommended_depth.value}")
    print(f"Reasoning: {plan.reasoning}")
    print(f"Code Type: {plan.code_type}")
    print(f"Risk Level: {plan.risk_level}")
    print(f"Expected Cost: ${plan.estimated_cost:.2f}")
    print(f"Expected Time: {plan.estimated_time}s")
    print(f"\nPriority Areas:")
    for area in plan.priority_areas:
        print(f"  - {area}")
