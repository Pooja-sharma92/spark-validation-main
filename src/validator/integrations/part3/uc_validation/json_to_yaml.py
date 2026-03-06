from __future__ import annotations
from pathlib import Path
import json
import yaml

def _normalize_type(ds_sqltype: str | None, precision: str | None, scale: str | None) -> str:
    # Best-effort mapping (enough for validation rules + schema checks)
    # SqlType examples in DSExport: 12=string, 9=date, 3=decimal :contentReference[oaicite:4]{index=4}
    if ds_sqltype == "12":
        return "string"
    if ds_sqltype == "9":
        return "date"
    if ds_sqltype == "3":
        p = precision or "38"
        s = scale or "0"
        return f"decimal({p},{s})"
    return "unknown"

def metadata_json_to_yaml(json_path: str, yaml_path: str):
    meta = json.loads(Path(json_path).read_text())

    cols_yaml = []
    for c in meta["columns"]:
        dtype = _normalize_type(c.get("ds_sqltype"), c.get("precision"), c.get("scale"))
        cols_yaml.append({
            "name": c["name"],
            "type": dtype,
            "nullable": bool(c.get("nullable", True)),
            "rules": []   # keep empty first; we can add rules later
        })

    yml = {
        "version": 1,
        "dataset": meta["job_name"],
        "columns": cols_yaml
    }

    out = Path(yaml_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(yml, sort_keys=False))
    return yml
