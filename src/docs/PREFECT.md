# Prefect Validation Flow Guide

## Flow Structure

The validation framework uses Prefect to orchestrate 4 validation stages:

```
┌─────────────────────────────────────────────────────────┐
│         Prefect Flow: validate_spark_job                 │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Stage 1 & 2: Parallel Execution     │
        │  ├─ validate_syntax (Task)           │
        │  └─ validate_logic (Task)            │
        └─────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Stage 3: Sequential Execution       │
        │  └─ validate_data_sources (Task)     │
        └─────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Stage 4: Sequential Execution       │
        │  └─ validate_with_great_expectations │
        └─────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Report Generation                   │
        │  └─ generate_validation_report       │
        └─────────────────────────────────────┘
```

## Viewing Results

### Method 1: Console Output (Real-time)

```bash
python -m src.validator.api.main --verbose
```

**Output Example**:
```
11:44:32 | INFO | prefect - Starting temporary server
11:44:34 | INFO | Flow run 'solemn-turaco' - Beginning flow run
11:44:34 | INFO | Flow run - Starting validation
11:44:34 | INFO | Task run 'Validate Logic' - Finished
11:44:34 | INFO | Task run 'Validate Syntax' - Finished
11:44:35 | INFO | Flow run - VALIDATION PASSED
11:44:35 | INFO | Flow run - Duration: 1.1s
```

### Method 2: Prefect UI (Recommended)

```bash
# Start Prefect server
prefect server start

# Open browser: http://localhost:4200
```

**UI Features**:
- Flow run history with status (pass/fail)
- Task execution timeline (visualize parallel execution)
- Artifacts (view formatted Markdown reports)
- Detailed logs per task

### Method 3: JSON Output

```bash
python -m src.validator.api.main job.py --json-output result.json

# Parse results
cat result.json | jq '.passed'
cat result.json | jq '.validation_results[0].issues'
```

**JSON Structure**:
```json
{
  "job": "src/jobs/transform_job.py",
  "passed": true,
  "total_issues": 2,
  "critical_count": 0,
  "warning_count": 2,
  "duration_seconds": 1.1,
  "validation_results": [
    {
      "stage": "syntax",
      "passed": true,
      "issues": [...],
      "total_issues": 1
    }
  ]
}
```

## Key Prefect Features

### 1. Parallel Execution

Independent tasks run concurrently:

```python
syntax_result = validate_syntax.submit(...)  # Async submit
logic_result = validate_logic.submit(...)    # Runs in parallel

syntax_result = syntax_result.result()  # Wait for results
logic_result = logic_result.result()
```

**Result**: ~15% time savings for syntax + logic validation.

### 2. Automatic Retry

Network-related tasks retry automatically:

```python
@task(name="Validate Data Sources", retries=2, retry_delay_seconds=10)
def validate_data_sources(...):
    ...
```

### 3. Artifacts (Report Archive)

Reports are automatically saved and viewable in UI:

```python
create_markdown_artifact(
    key="validation-report",
    markdown=report_content,
    description=f"Validation report for {job_name}"
)
```

## CI/CD Integration

```bash
python -m src.validator.api.main job.py \
  --json-output result.json \
  --fail-on-warnings

if [ $? -eq 0 ]; then
  echo "Validation passed"
else
  echo "Validation failed"
  cat result.json | jq '.validation_results[].issues[]'
fi
```

## Quick Start

```bash
# 1. Run validation
python -m src.validator.api.main src/jobs/example.py --verbose

# 2. Start Prefect UI (separate terminal)
prefect server start

# 3. Open browser
open http://localhost:4200
```
