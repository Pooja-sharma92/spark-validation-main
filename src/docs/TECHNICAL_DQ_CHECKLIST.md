# Technical Data Quality Checklist (Draft v1)

Purpose: a shared, team-wide **standard checklist** for *technical* data quality checks.

> Note: This is **Phase-2/Phase-3** work. For the current phase we can focus only on **Golden output comparison**.

---

## 1) Basic structural checks (fast)
- **Schema match**: same column names and compatible data types
- **Column count**: expected number of columns
- **Row count**: count should match (or be within agreed tolerance if sampling)

## 2) Key integrity checks
- **Business key defined** (single or composite)
- **Key uniqueness**: no duplicates on business key
- **Key null check**: no nulls in key columns

## 3) Nullability checks
- Null % per critical columns
- Columns that must be NOT NULL

## 4) Value sanity checks
- Allowed ranges for numeric columns (min/max)
- Allowed value set for categorical columns
- Date validity (no future dates if not expected)

## 5) Distribution checks (optional)
- Distinct count per key columns
- Top-N frequency drift checks
- Percentiles for key numeric metrics

## 6) Reconciliation checks (optional)
- Sum checks for measures (amounts, balances)
- Group-by totals match (by date/region/product)

## 7) Audit/system column handling
- Identify audit columns (load_ts, batch_id, created_ts, etc.)
- Decide: ignore vs normalize

---

## What we need from business/SME (when we add business rules)
- Business rules (domain-specific)
- Expected reconciliation logic
- Critical metrics and thresholds

---

## Suggested default output report fields
- dataset_name
- row_count_left / row_count_right
- schema_diff
- key_duplicate_count
- null_rate_summary
- top_mismatch_columns
