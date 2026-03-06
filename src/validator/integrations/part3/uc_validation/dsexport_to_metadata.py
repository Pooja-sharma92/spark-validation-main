from __future__ import annotations

from pathlib import Path
import json
import re
import html
import xml.etree.ElementTree as ET


_CTRL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_BAD_AMP = re.compile(r"&(?!(amp|lt|gt|apos|quot|#\d+|#x[0-9a-fA-F]+);)")


def _clean_xml_text(raw: str) -> str:
    """
    Make DSExport XML more parseable:
    - remove BOM/control chars
    - ensure XML declaration is only at start (drop later ones)
    - escape stray '&'
    - trim junk before first '<'
    """
    if not raw:
        return raw

    # remove BOM if present
    raw = raw.lstrip("\ufeff")

    # drop any junk before first tag
    first_lt = raw.find("<")
    if first_lt > 0:
        raw = raw[first_lt:]

    # remove control characters that break XML parsing
    raw = _CTRL_CHARS.sub("", raw)

    # if multiple XML declarations exist, keep only the first one
    # (common issue: xml declaration appears mid-file)
    decl = "<?xml"
    idx = raw.find(decl)
    if idx > 0:
        raw = raw[idx:]  # keep from first declaration only
    # remove any subsequent declarations
    parts = raw.split(decl)
    if len(parts) > 2:
        raw = decl.join([parts[0], parts[1]])  # keep first decl only

    # escape stray '&' which causes "invalid token"
    raw = _BAD_AMP.sub("&amp;", raw)

    return raw.strip()


def _get_prop_text(block: str, prop_name: str) -> str | None:
    """
    Extract <Property Name="X">value</Property> from a text block (regex-based fallback).
    """
    m = re.search(
        rf"<Property[^>]*\bName=['\"]{re.escape(prop_name)}['\"][^>]*>(.*?)</Property>",
        block,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    val = m.group(1).strip()
    # unescape XML entities if any
    return html.unescape(val)


def _regex_extract_columns(xml_text: str) -> list[dict]:
    """
    Fallback extractor: works even if XML is not well-formed.
    We only care about OutputColumn subrecords.
    """
    cols: list[dict] = []

    # DSExport commonly has SubRecord Name="OutputColumn"
    for sub in re.finditer(
        r"<SubRecord[^>]*\bName=['\"]OutputColumn['\"][^>]*>(.*?)</SubRecord>",
        xml_text,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        block = sub.group(1)

        name = _get_prop_text(block, "Name")
        if not name:
            continue

        col = {
            "name": name,
            "sql_type": _get_prop_text(block, "SqlType"),
            "precision": _get_prop_text(block, "Precision"),
            "scale": _get_prop_text(block, "Scale"),
            "nullable": _get_prop_text(block, "Nullable"),
        }
        cols.append(col)

    return cols


def _etree_extract_columns(root: ET.Element) -> list[dict]:
    cols: list[dict] = []
    for sub in root.findall(".//SubRecord[@Name='OutputColumn']"):
        def get(name: str) -> str | None:
            p = sub.find(f"./Property[@Name='{name}']")
            return p.text.strip() if p is not None and p.text else None

        name = get("Name")
        if not name:
            continue

        cols.append(
            {
                "name": name,
                "sql_type": get("SqlType"),
                "precision": get("Precision"),
                "scale": get("Scale"),
                "nullable": get("Nullable"),
            }
        )
    return cols


def dsexport_to_metadata(dsexport_path: str, out_json_path: str, job_name: str | None = None) -> dict:
    """
    Convert DSExport XML -> metadata JSON (tolerant).
    """
    src = Path(dsexport_path)
    raw = src.read_text(encoding="utf-8", errors="replace")
    cleaned = _clean_xml_text(raw)

    if job_name is None:
        job_name = src.stem  # works even if extension is .json

    cols: list[dict] = []

    # 1) Try strict XML parse
    try:
        root = ET.fromstring(cleaned)
        cols = _etree_extract_columns(root)
    except Exception:
        cols = []

    # 2) Fallback: regex-based extraction (handles broken XML)
    if not cols:
        cols = _regex_extract_columns(cleaned)

    if not cols:
        raise RuntimeError(
            f"No OutputColumn columns found in DSExport for {src} "
            "(file may be truncated/corrupt)."
        )

    meta = {
        "job_name": job_name,
        "source": "dsexport",
        "columns": cols,
    }

    out = Path(out_json_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta
