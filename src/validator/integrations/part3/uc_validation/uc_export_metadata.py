from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import xml.etree.ElementTree as ET


# ----------------------------
# helpers
# ----------------------------

_DS_ESC = r"\(\d+\)"  # matches DataStage escaped markers like \(2)\(2)0...

def _clean_ds_value(v: str) -> str:
    if v is None:
        return ""
    v = re.sub(_DS_ESC, "", v)
    return v.strip()

def _extract_job_name(root: ET.Element) -> str:
    job = root.find(".//Job")
    if job is None:
        return "unknown_job"
    return job.attrib.get("Identifier", "unknown_job")

def _extract_parameters(root: ET.Element) -> List[Dict[str, str]]:
    params: List[Dict[str, str]] = []
    for sr in root.findall(".//Collection[@Name='Parameters']/SubRecord"):
        name = sr.findtext("./Property[@Name='Name']", default="").strip()
        default = sr.findtext("./Property[@Name='Default']", default="").strip()
        prompt = sr.findtext("./Property[@Name='Prompt']", default="").strip()
        if name:
            params.append({"name": name, "default": default, "prompt": prompt})
    return params

def _extract_sequential_outputs(root: ET.Element) -> List[Dict[str, str]]:
    """
    Looks for:
      <Record Type="CustomStage"> with StageType PxSequentialFile
      then the matching CustomInput record has Collection Properties -> SubRecord Name="file" Value="...."
    """
    outputs: List[Dict[str, str]] = []
    for stage in root.findall(".//Record[@Type='CustomStage']"):
        stype = stage.findtext("./Property[@Name='StageType']", default="").strip()
        if stype != "PxSequentialFile":
            continue

        stage_name = stage.findtext("./Property[@Name='Name']", default="").strip()

        # Find corresponding CustomInput that belongs to this stage:
        pin_id = stage.findtext("./Property[@Name='InputPins']", default="").strip()
        if not pin_id:
            continue

        pin = root.find(f".//Record[@Identifier='{pin_id}']")
        if pin is None:
            continue

        file_val = None
        for sr in pin.findall(".//Collection[@Name='Properties']/SubRecord"):
            n = sr.findtext("./Property[@Name='Name']", default="").strip()
            v = sr.findtext("./Property[@Name='Value']", default="")
            if n == "file":
                file_val = _clean_ds_value(v)

        schema = None
        for msr in pin.findall(".//Collection[@Name='MetaBag']/SubRecord"):
            owner = msr.findtext("./Property[@Name='Owner']", default="").strip()
            name = msr.findtext("./Property[@Name='Name']", default="").strip()
            value = msr.findtext("./Property[@Name='Value']", default="")
            if owner == "APT" and name == "Schema":
                schema = value.strip()

        outputs.append(
            {
                "stage_name": stage_name,
                "file_template": file_val or "",
                "apt_schema_record": schema or "",
                "pin_id": pin_id,
            }
        )

    return outputs


def _yaml_dump(d: Dict[str, Any]) -> str:
    """
    Tiny YAML writer (no external libs).
    Good enough for our configs.
    """
    def esc(s: Any) -> str:
        s = str(s)
        if any(c in s for c in [":", "{", "}", "[", "]", "#", "\n"]):
            return json.dumps(s)  # quote safely
        return s

    lines: List[str] = []

    def write(key: str, val: Any, indent: int = 0) -> None:
        pad = "  " * indent
        if isinstance(val, dict):
            lines.append(f"{pad}{key}:")
            for k, v in val.items():
                write(k, v, indent + 1)
        elif isinstance(val, list):
            lines.append(f"{pad}{key}:")
            for item in val:
                if isinstance(item, dict):
                    lines.append(f"{pad}  -")
                    for k, v in item.items():
                        write(k, v, indent + 2)
                else:
                    lines.append(f"{pad}  - {esc(item)}")
        else:
            lines.append(f"{pad}{key}: {esc(val)}")

    for k, v in d.items():
        write(k, v, 0)
    return "\n".join(lines) + "\n"


# ----------------------------
# Robust XML loading (NO lxml)
# ----------------------------

_XML_DECL_RE = re.compile(r"<\?xml[^>]*\?>", re.IGNORECASE)

def _sanitize_xml_text(raw: str) -> str:
    """
    Make DS export files parseable even if:
    - browser banner exists before <DSExport
    - extra <?xml ...?> declarations appear mid-file
    - <none> appears as raw text
    - raw '&' appears in SQL/text
    - raw '<' appears in expressions like '<>' inside Property text (INVALID XML)
    - control chars exist
    """
    raw = raw.lstrip("\ufeff")

    # Keep only DSExport payload
    start = raw.find("<DSExport")
    if start == -1:
        start = raw.lower().find("<dsexport")
    if start != -1:
        end = raw.lower().rfind("</dsexport>")
        if end != -1:
            raw = raw[start : end + len("</DSExport>")]
        else:
            raw = raw[start:]

    # Remove ALL XML declarations (common issue: appears mid-file)
    raw = _XML_DECL_RE.sub("", raw)

    # Replace common placeholder that breaks XML
    raw = raw.replace("<none>", "&lt;none&gt;")

    # Escape bare ampersands
    raw = re.sub(r"&(?!amp;|lt;|gt;|apos;|quot;)", "&amp;", raw)

    # Remove invalid XML 1.0 control chars (except tab/newline/carriage return)
    raw = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", raw)

    # Escape stray '<' that are NOT tag starters.
    # This fixes tokens like "<>" (as in constraints) inside Property text.
    # Tag start is usually "<" followed by letter, "/", "!", or "?".
    raw = re.sub(r"<(?=[^A-Za-z!/\\?])", "&lt;", raw)

    return raw


def _load_xml_root(xml_path: Path) -> ET.Element:
    raw = xml_path.read_text(encoding="utf-8", errors="ignore")
    cleaned = _sanitize_xml_text(raw)
    try:
        return ET.fromstring(cleaned)
    except ET.ParseError as e:
        raise ET.ParseError(f"{e} (while parsing {xml_path})")


# ----------------------------
# main API used by run_all.py
# ----------------------------

def run_export_and_generate(
    *,
    project_root: str,
    xml_dir: str = "inputs/xml",
    out_dir: str = "metadata/out",
    gen_dir: str = "metadata/generated",
) -> Dict[str, Any]:
    rootp = Path(project_root)
    xmld = rootp / xml_dir
    outd = rootp / out_dir
    gend = rootp / gen_dir
    outd.mkdir(parents=True, exist_ok=True)
    gend.mkdir(parents=True, exist_ok=True)

    xml_files = sorted([p for p in xmld.glob("*") if p.is_file()])
    if not xml_files:
        raise FileNotFoundError(f"No XML files found in {xmld}")

    summary: Dict[str, Any] = {"xml_dir": str(xmld), "jobs": []}

    for xml_path in xml_files:
        try:
            xroot = _load_xml_root(xml_path)
        except Exception as e:
            summary["jobs"].append(
                {"xml": str(xml_path), "status": "FAILED_PARSE", "error": str(e)}
            )
            continue

        job_name = _extract_job_name(xroot)
        params = _extract_parameters(xroot)
        outputs = _extract_sequential_outputs(xroot)

        metadata = {
            "job_name": job_name,
            "source_xml": str(xml_path),
            "parameters": params,
            "sequential_outputs": outputs,
        }

        meta_json = outd / f"{job_name}_metadata.json"
        meta_json.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        yaml_doc = {
            "job_name": job_name,
            "source_xml": str(xml_path),
            "parameters": params,
            "outputs": outputs,
            "validation": {
                "checks": [
                    "output_file_exists",
                    "schema_matches_expected_if_parquet_or_dataframe",
                    "row_count_nonzero_if_expected",
                ]
            },
        }
        yaml_path = gend / f"{job_name}.yaml"
        yaml_path.write_text(_yaml_dump(yaml_doc), encoding="utf-8")

        summary["jobs"].append(
            {
                "job_name": job_name,
                "metadata_json": str(meta_json),
                "validation_yaml": str(yaml_path),
                "status": "OK",
            }
        )

    return summary
