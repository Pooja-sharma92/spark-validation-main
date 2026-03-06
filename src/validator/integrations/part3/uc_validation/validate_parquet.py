from __future__ import annotations
import pandas as pd
from pathlib import Path
import yaml

def _read_yaml(yaml_path: str):
    return yaml.safe_load(Path(yaml_path).read_text())

def validate_schema_only(parquet_path: str, yaml_path: str) -> pd.DataFrame:
    """
    Start simple: Schema validation only (columns present, types roughly match if possible).
    This avoids needing DB/table access.
    """
    import pyarrow.parquet as pq

    yml = _read_yaml(yaml_path)
    expected_cols = [c["name"] for c in yml["columns"]]

    table = pq.read_table(parquet_path)
    actual_cols = table.column_names

    missing = [c for c in expected_cols if c not in actual_cols]
    extra = [c for c in actual_cols if c not in expected_cols]

    rows = []
    rows.append({"check": "missing_columns", "status": "FAIL" if missing else "PASS", "details": ",".join(missing)})
    rows.append({"check": "extra_columns", "status": "WARN" if extra else "PASS", "details": ",".join(extra)})

    return pd.DataFrame(rows)

def write_report(df: pd.DataFrame, report_csv: str):
    out = Path(report_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
