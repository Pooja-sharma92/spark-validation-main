"""Dataset comparison utility (Golden vs Spark output).

This is intentionally **standalone** (not wired into the pipeline yet).
It helps validate Phase-2: "Execution + Golden dataset comparison".

Supports:
- schema comparison
- row count comparison
- key-based row hash diff (if keys provided)
- aggregate-only comparison (if no keys)

Examples:
  python scripts/compare_datasets.py \
    --left parquet:/path/to/spark_out \
    --right parquet:/path/to/golden_out \
    --keys account_id,txn_date \
    --ignore_cols load_ts,batch_id \
    --numeric_tol 0.01
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Tuple


def _split_csv(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _parse_uri(uri: str) -> Tuple[str, str]:
    # uri format: "parquet:/path" or "csv:/path" or "delta:/path" (best-effort)
    if ":" not in uri:
        raise ValueError("Dataset URI must be like 'parquet:/path' or 'csv:/path'")
    fmt, path = uri.split(":", 1)
    return fmt.lower(), path


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two datasets (Spark output vs golden)")
    parser.add_argument("--left", required=True, help="Left dataset URI, e.g. parquet:/path")
    parser.add_argument("--right", required=True, help="Right dataset URI, e.g. parquet:/path")
    parser.add_argument("--keys", default="", help="Comma-separated business key columns")
    parser.add_argument("--ignore_cols", default="", help="Comma-separated columns to ignore")
    parser.add_argument("--numeric_tol", type=float, default=0.0, help="Numeric tolerance (absolute), e.g. 0.01")
    parser.add_argument("--sample_mismatches", type=int, default=20, help="How many mismatch rows to show")
    parser.add_argument("--output_json", default="", help="Write summary JSON to this path")
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
    except Exception as e:
        print("[ERROR] pyspark is required to run this script.")
        print(f"Details: {e}")
        return 2

    keys = _split_csv(args.keys)
    ignore = set(_split_csv(args.ignore_cols))

    lfmt, lpath = _parse_uri(args.left)
    rfmt, rpath = _parse_uri(args.right)

    spark = (
        SparkSession.builder.appName("compare_datasets")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    def load(fmt: str, path: str):
        if fmt == "parquet":
            return spark.read.parquet(path)
        if fmt == "csv":
            return spark.read.option("header", True).option("inferSchema", True).csv(path)
        if fmt == "delta":
            return spark.read.format("delta").load(path)
        raise ValueError(f"Unsupported format: {fmt}")

    left = load(lfmt, lpath)
    right = load(rfmt, rpath)

    # Normalize columns: ignore audit columns, align common columns
    lcols = [c for c in left.columns if c not in ignore]
    rcols = [c for c in right.columns if c not in ignore]
    common = [c for c in lcols if c in rcols]

    summary = {
        "left": {"format": lfmt, "path": lpath},
        "right": {"format": rfmt, "path": rpath},
        "keys": keys,
        "ignore_cols": sorted(ignore),
        "numeric_tol": args.numeric_tol,
        "schema": {},
        "counts": {},
        "diff": {},
    }

    # Schema compare (common columns only)
    lschema = {f.name: f.dataType.simpleString() for f in left.schema.fields if f.name in common}
    rschema = {f.name: f.dataType.simpleString() for f in right.schema.fields if f.name in common}
    schema_mismatches = {c: {"left": lschema.get(c), "right": rschema.get(c)} for c in common if lschema.get(c) != rschema.get(c)}

    summary["schema"] = {
        "left_cols": len(lcols),
        "right_cols": len(rcols),
        "common_cols": len(common),
        "only_in_left": sorted([c for c in lcols if c not in rcols]),
        "only_in_right": sorted([c for c in rcols if c not in lcols]),
        "type_mismatches": schema_mismatches,
    }

    # Counts
    lcount = left.count()
    rcount = right.count()
    summary["counts"] = {"left": lcount, "right": rcount, "match": lcount == rcount}

    # If no keys: do aggregate-only check
    if not keys:
        # Compare aggregates for numeric columns in common
        num_cols = []
        for f in left.schema.fields:
            if f.name in common and isinstance(f.dataType, (T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType, T.ShortType)):
                num_cols.append(f.name)

        aggs_left = left.select([F.sum(F.col(c)).alias(c) for c in num_cols]).collect()[0].asDict() if num_cols else {}
        aggs_right = right.select([F.sum(F.col(c)).alias(c) for c in num_cols]).collect()[0].asDict() if num_cols else {}

        diffs = {}
        for c in num_cols:
            lv = aggs_left.get(c)
            rv = aggs_right.get(c)
            if lv is None and rv is None:
                continue
            try:
                delta = None if lv is None or rv is None else float(lv) - float(rv)
                ok = delta is None or abs(delta) <= args.numeric_tol
            except Exception:
                delta = None
                ok = lv == rv
            if not ok:
                diffs[c] = {"left_sum": lv, "right_sum": rv, "delta": delta}

        summary["diff"] = {
            "mode": "aggregate_only",
            "numeric_columns": num_cols,
            "sum_mismatches": diffs,
        }

        print(json.dumps(summary, indent=2, default=str))
        if args.output_json:
            with open(args.output_json, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, default=str)
        return 0 if not diffs and summary["schema"]["type_mismatches"] == {} else 1

    # Key-based diff
    missing_keys = [k for k in keys if k not in common and k not in left.columns]
    if missing_keys:
        print(f"[ERROR] Keys not found in dataset columns: {missing_keys}")
        return 2

    # Align to common columns (including keys)
    sel_cols = sorted(set(common).union(keys))
    ldf = left.select([F.col(c) for c in sel_cols])
    rdf = right.select([F.col(c) for c in sel_cols])

    # Normalize numeric tolerance by rounding numeric columns if tolerance provided
    if args.numeric_tol > 0:
        # Convert tolerance into a number of decimal places for rounding (best-effort)
        # e.g., 0.01 -> 2 decimals
        import math

        decimals = max(0, int(round(-math.log10(args.numeric_tol))) if args.numeric_tol < 1 else 0)
        for c in sel_cols:
            dt = dict(ldf.dtypes).get(c)
            if dt in ("double", "float"):
                ldf = ldf.withColumn(c, F.round(F.col(c), decimals))
                rdf = rdf.withColumn(c, F.round(F.col(c), decimals))

    # Row hash over non-key columns
    non_key = [c for c in sel_cols if c not in keys]
    def row_hash(df):
        # cast everything to string safely, keep null marker
        parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in non_key]
        return df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *parts), 256))

    lhash = row_hash(ldf).select(*keys, "__row_hash")
    rhash = row_hash(rdf).select(*keys, "__row_hash")

    # Missing / extra keys
    left_only = lhash.join(rhash, on=keys, how="left_anti")
    right_only = rhash.join(lhash, on=keys, how="left_anti")

    # Mismatched rows by hash
    joined = lhash.alias("l").join(rhash.alias("r"), on=keys, how="inner")
    mism = joined.where(F.col("l.__row_hash") != F.col("r.__row_hash"))

    left_only_cnt = left_only.count()
    right_only_cnt = right_only.count()
    mism_cnt = mism.count()

    summary["diff"] = {
        "mode": "key_hash",
        "left_only_keys": left_only_cnt,
        "right_only_keys": right_only_cnt,
        "mismatched_rows": mism_cnt,
        "sample_mismatches": [],
    }

    if mism_cnt > 0 and args.sample_mismatches > 0:
        # Show some keys that mismatch
        rows = mism.select(*keys).limit(args.sample_mismatches).collect()
        summary["diff"]["sample_mismatches"] = [r.asDict() for r in rows]

    print(json.dumps(summary, indent=2, default=str))
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

    ok = (
        summary["schema"]["type_mismatches"] == {}
        and summary["schema"]["only_in_left"] == []
        and summary["schema"]["only_in_right"] == []
        and left_only_cnt == 0
        and right_only_cnt == 0
        and mism_cnt == 0
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
