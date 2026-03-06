# GVS / Spark Validator: Capabilities & Checkpoints (v1)

This document describes **what the current validator can do today**, in a clear, numbered list of checkpoints.

The validator is designed to be useful even when it is not yet "perfect". The goal is:
- predictable checkpoints
- clear pass/fail and warnings
- a repeatable path from **Dry-run** to **Execution + Golden comparison**

---

## 1) Checkpoints (what we validate)

### 1. Job discovery & configuration
**What it checks**
- Job file path is valid
- Repo root / runtime overlay / DS export paths can be resolved (best-effort)
- Framework config is readable

**Output**
- Errors if required metadata/config is missing

---

### 2. Syntax validation
**What it checks**
- Python syntax / AST parse errors
- Indentation issues, invalid tokens

**Output**
- ERROR with file and line number

---

### 3. Imports & dependency hygiene
**What it checks**
- Star imports and risky import patterns
- Deprecated modules (best-effort)
- Obvious missing import issues (best-effort)

**Output**
- WARNING / INFO with lines and suggestions

---

### 4. SQL parsing & semantic checks (dry-run)
**What it checks**
- SQL parse errors (best-effort)
- Ambiguous columns, risky joins, unsupported patterns (best-effort)

**Output**
- ERROR/WARNING with location and suggestion

---

### 5. Logic heuristics (dry-run)
**What it checks**
- Common migration anti-patterns (collect without limit, bare except, hardcoded credentials, etc.)
- Print statements / logging guidance

**Output**
- INFO/WARNING (and blocking if configured)

---

### 6. Pre-execution metadata validation (optional)
*(Stage: `pre_execution` — currently toggled via config)*

**What it does**
- Uses DS export XML to generate metadata/YAML artifacts
- Validates generated assets exist and are consistent

**Output**
- Pass/fail with details and artifact paths

---

### 7. Spark execution (optional)
*(Stage: `execution` — can be disabled for dry-only runs)*

**What it does**
- Executes the Spark job with discovered/declared test data
- Captures runtime errors and a small output preview

**Output**
- Pass/fail with runtime error details (if any)

---

### 8. Golden dataset comparison (Phase-2 / planned)
**What it will do**
- Compare Spark output vs Golden output (DataStage expected output)
- Uses business key (if available) and tolerance rules (numeric/time/audit columns)

**Status**
- Not fully integrated as a pipeline stage yet
- We provide a standalone script to run comparison now (see below)

---

## 2) Validation phases (recommended usage)

### Phase-1: Dry-run (current focus)
Enable:
- `syntax`, `imports`, `sql`, `logic` (and optionally `pre_execution`)
Disable:
- `execution`

Purpose:
- fast feedback
- scalable across many jobs
- catches code/SQL/logic issues early

### Phase-2: Execution + Golden compare
Enable:
- `execution`
Run:
- golden dataset comparison (standalone script today; stage integration later)

Purpose:
- highest confidence (≈90%+) when output matches golden within tolerance

---

## 3) Compatibility & assumptions

### Supported
- PySpark jobs that run with local/VM Spark submit
- Jobs where input and output locations are known
- Deterministic transformations or tolerances agreed

### Known limitations (current)
- If no **business key** is available, row-level diff is hard; we fallback to aggregates.
- Non-deterministic columns (timestamps, random, audit fields) must be ignored or normalized.
- Complex dynamic SQL generation reduces parse accuracy.

---

## 4) How to run (quick guidance)

### Dry-run only
Update `src/config/framework.yaml` to disable execution:

```yaml
validation:
  stages:
    - name: "execution"
      enabled: false
```

Then run the executor/poller as usual.

### Golden comparison (standalone)
Use `scripts/compare_datasets.py` to compare Spark output vs golden output.

Example:

```bash
python scripts/compare_datasets.py \
  --left parquet:/path/to/spark_output \
  --right parquet:/path/to/golden_output \
  --keys account_id,txn_date \
  --ignore_cols load_ts,batch_id \
  --numeric_tol 0.01
```

---

## 5) What "Pass" means

- Dry-run pass means: code + SQL + logic checks are clean enough to proceed to execution.
- Golden match means: output is functionally equivalent (high confidence conversion success).
