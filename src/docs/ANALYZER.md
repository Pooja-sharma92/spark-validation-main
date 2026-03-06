# Spark Code Analyzer

A comprehensive code analysis tool for PySpark and Python ETL pipelines. Combines static analysis with AI-powered deep insights.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run analysis (static only - free)
python -m src.validator.analyzer.main analyze src/jobs/example.py

# With AI analysis (requires API key)
export OPENAI_API_KEY="your-key"
python -m src.validator.analyzer.main analyze src/jobs/example.py -v
```

## Analysis Modes

### 1. Static Analysis (Free)

Fast, rule-based analysis detecting:
- Syntax errors (Python + SQL)
- Spark anti-patterns (missing cache, collect() on large data)
- Performance issues (inefficient operations)
- Security vulnerabilities (SQL injection, hardcoded credentials)
- Code quality metrics

```bash
python -m src.validator.analyzer.main analyze job.py
```

**Cost**: Free | **Speed**: 1-2 seconds

### 2. AI Deep Analysis

Adds AI-powered insights:
- Business logic understanding
- Data flow visualization
- Quality scoring (0-10 scale)
- Refactoring suggestions

```bash
python -m src.validator.analyzer.main analyze job.py -v
```

**Cost**: ~$0.05-0.10/file | **Speed**: 10-20 seconds

### 3. Intelligent Analysis (Recommended)

AI-driven analysis that automatically selects depth based on code complexity:

```bash
python -m src.validator.analyzer.intelligent_analyzer analyze job.py -v
```

| Code Type | Depth | Cost | Speed |
|-----------|-------|------|-------|
| Simple (<200 LOC) | Quick scan | $0.01 | 5s |
| Medium (200-500 LOC) | Moderate | $0.05 | 20s |
| Complex (>500 LOC) | Deep | $0.15 | 45s |

**Average savings**: 67% cost, 2.2x faster vs full analysis.

## Configuration

Create `.env` file in project root:

```bash
# AI Provider (choose one)
OPENAI_API_KEY=sk-your-key
# or
ANTHROPIC_API_KEY=sk-ant-your-key
# or (local, free)
AI_PROVIDER=ollama
AI_MODEL=codellama:13b

# Optional settings
ENABLE_AI_ANALYSIS=true
REPORT_FORMAT=markdown  # markdown, html, json
```

## Python API

```python
from validator.analyzer.main import CodeAnalyzer

analyzer = CodeAnalyzer()
result = analyzer.analyze_file('src/jobs/my_job.py', verbose=True)

print(f"Issues: {len(result['static_results'].issues)}")
print(f"Quality Score: {result['ai_results'].code_quality_score}")
```

## Use Case Recommendations

| Scenario | Recommended Mode |
|----------|------------------|
| Daily development | Intelligent Analyzer |
| Batch scanning (100+ files) | Intelligent Analyzer |
| Code review | Intelligent Analyzer |
| CI/CD pipeline | Static Analysis |
| Critical audit | Full AI Analysis |
| Pre-production check | Full AI Analysis |

## CI/CD Integration

```yaml
# .github/workflows/code-quality.yml
- name: Run Analysis
  run: |
    python -m src.validator.analyzer.main analyze src/jobs/ --format json > analysis.json

- name: Check Quality Gates
  run: |
    critical=$(jq '.summary.critical // 0' analysis.json)
    [ "$critical" -gt 0 ] && exit 1
```

## Detection Capabilities

### Static Analysis
- Python syntax errors
- SQL syntax errors in spark.sql() calls
- Missing DataFrame cache/persist
- Collect on large datasets
- Duplicate write operations
- Hardcoded credentials
- SQL injection patterns
- Magic numbers
- Code duplication

### AI Analysis
- Business logic explanation
- Data lineage mapping
- Architecture assessment
- Performance bottleneck identification
- Refactoring recommendations
- Technical debt scoring
