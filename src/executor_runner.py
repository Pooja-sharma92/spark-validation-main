#!/usr/bin/env python3
"""
Spark Validation Executor - Polls queue and executes validation jobs.

What this version fixes (in one place):
1) Full per-stage logs (no more only "✗ FAILED" with silence)
2) Prints real exception + traceback when a stage fails with 0 issues
3) Forces Iceberg REST + MinIO and blocks any job code injecting:
      spark.sql.catalog.<catalog>.type = hive
   (prevents: "both type and catalog-impl are set")
4) Ensures validator.config uses container Redis/Postgres (no localhost fallback)
5) Output capture without changing job code:
   duplicates df.write.save/csv/parquet/saveAsTable to /app/reports/outputs/<job>/<job_id>/
6) Writes executor log file under /app/reports/executor_logs/ as well.
7) Fixes your crash:
      AnalysisException: Cannot modify the value of a static config: spark.sql.extensions
   by NEVER setting spark.sql.extensions via spark.conf.set() after SparkSession exists.
"""

import asyncio
import argparse
import contextvars
import copy
import json
import os
import re

def _expand_env_vars(value: str) -> str:
    """
    Expand ${VAR:default} or ${VAR} placeholders in strings.
    Used for framework.yaml values like:
      ${ICEBERG_REST_URI:http://iceberg-rest:8181}
    """
    if value is None:
        return value
    if not isinstance(value, str):
        return str(value)
    pattern = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")
    def repl(m):
        var = m.group(1)
        default = m.group(2) if m.group(2) is not None else ""
        return os.environ.get(var, default)
    return pattern.sub(repl, value)

import signal
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# -----------------------------------------------------------------------------
# Environment defaults for PySpark inside container
# -----------------------------------------------------------------------------
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# -----------------------------------------------------------------------------
# Resolve SRC_DIR so "validator" package imports work
# -----------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if (_THIS_DIR / "validator").exists():
    SRC_DIR = _THIS_DIR  # executor_runner.py is inside src/
else:
    SRC_DIR = _THIS_DIR / "src"  # executor_runner.py is in repo root

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# -----------------------------------------------------------------------------
# Helpers: sys.path
# -----------------------------------------------------------------------------
def _ensure_sys_path(paths: List[str]) -> None:
    for p in paths:
        if not p:
            continue
        if p not in sys.path:
            sys.path.insert(0, p)

# -----------------------------------------------------------------------------
# Helpers: env + config override for validator.config
# -----------------------------------------------------------------------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    v = str(v).strip()
    return default if v == "" else v

def _build_validator_config_override_from_env() -> Dict[str, Any]:
    override: Dict[str, Any] = {}

    redis_url = _env("REDIS_URL")
    if redis_url:
        override.setdefault("redis", {})["url"] = redis_url

    pg: Dict[str, Any] = {}
    pg_host = _env("POSTGRES_HOST")
    pg_port = _env("POSTGRES_PORT")
    pg_db = _env("POSTGRES_DB")
    pg_user = _env("POSTGRES_USER")
    pg_password = os.environ.get("POSTGRES_PASSWORD")  # allow empty string

    if pg_host:
        pg["host"] = pg_host
    if pg_port:
        try:
            pg["port"] = int(pg_port)
        except Exception:
            pass
    if pg_db:
        pg["database"] = pg_db
    if pg_user:
        pg["user"] = pg_user
    if pg_password is not None:
        pg["password"] = pg_password

    if pg:
        override["postgres"] = pg

    return override

# -----------------------------------------------------------------------------
# Import pipeline components
# -----------------------------------------------------------------------------
from validator.pipeline import ValidationContext, ValidationPipeline  # noqa: E402
from validator.stages import get_stages_from_config  # noqa: E402

from validator.config import load_config as _load_framework_config  # noqa: E402
from validator.config import set_config as _set_framework_config  # noqa: E402
from validator.config import get_config as _get_framework_config  # noqa: E402

try:
    from validator.results.storage import create_storage  # noqa: E402
    STORAGE_AVAILABLE = True
except Exception:
    STORAGE_AVAILABLE = False

try:
    from pyspark.sql import SparkSession  # noqa: E402
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False
    SparkSession = None

# -----------------------------------------------------------------------------
# Executor log file (persisted in /app volume)
# -----------------------------------------------------------------------------
def _log_dir() -> Path:
    d = Path("/app/reports/executor_logs")
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return d

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    print(line, flush=True)
    try:
        with (_log_dir() / "executor.log").open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Output capture (no job code change)
# -----------------------------------------------------------------------------
_CAPTURE_CTX = contextvars.ContextVar("spark_validator_capture_ctx", default=None)

def _get_capture_cfg(config: dict) -> dict:
    return (
        (((config.get("pipeline") or {}).get("stage_settings") or {}).get("execution") or {})
        .get("output_capture")
        or {}
    )

def _safe_name(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "output"
    s = s.replace("\\", "/")
    s = s.split("/")[-1]
    s = re.sub(r"[^a-zA-Z0-9_.-]+", "_", s)
    return s[:120] or "output"

def _compute_capture_base(config: dict, job_path: str, request_id: str) -> str:
    cap = _get_capture_cfg(config)
    output_root = (cap.get("output_root") or "/app/reports/outputs").strip()
    layout = (cap.get("output_layout") or "job_request").strip().lower()

    job_stem = Path(str(job_path)).stem if job_path else "job"
    rid = str(request_id or "run")

    if layout == "job_request":
        return str(Path(output_root) / job_stem / rid)
    if layout == "job_only":
        return str(Path(output_root) / job_stem)
    return str(Path(output_root))

def install_output_capture_patch(config: dict) -> None:
    """
    Monkeypatch DataFrameWriter so any df.write...save/csv/parquet/saveAsTable
    ALSO writes a copy to the per-job capture folder computed at runtime.

    Safe: capture failures never break the job.
    """
    try:
        from pyspark.sql.readwriter import DataFrameWriter
    except Exception:
        return

    cfg = _get_capture_cfg(config)
    enabled = cfg.get("enabled", True)
    if str(enabled).strip().lower() in {"false", "0", "no"}:
        return

    capture_format = (cfg.get("format") or "parquet").strip().lower()
    capture_mode = (cfg.get("mode") or "overwrite").strip().lower()

    if getattr(DataFrameWriter, "_spark_validator_patched", False):
        return

    orig_save = DataFrameWriter.save
    orig_csv = getattr(DataFrameWriter, "csv", None)
    orig_parquet = getattr(DataFrameWriter, "parquet", None)
    orig_save_as_table = getattr(DataFrameWriter, "saveAsTable", None)

    def _capture_df(writer_self, name_hint: str) -> None:
        ctx = _CAPTURE_CTX.get()
        if not ctx:
            return
        df = getattr(writer_self, "_df", None)
        if df is None:
            return
        capture_base = ctx.get("capture_base")
        if not capture_base:
            return

        capture_name = _safe_name(name_hint)
        out_base = Path(str(capture_base))
        out_path = str(out_base / capture_name)

        try:
            out_base.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        try:
            if capture_format == "csv":
                df.write.mode(capture_mode).option("header", "true").csv(out_path)
            else:
                df.write.mode(capture_mode).parquet(out_path)
        except Exception:
            return

    def patched_save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        res = orig_save(self, path=path, format=format, mode=mode, partitionBy=partitionBy, **options)
        _capture_df(self, str(path or "save"))
        return res

    DataFrameWriter.save = patched_save

    if orig_csv:
        def patched_csv(self, path, mode=None, header=None, **options):
            res = orig_csv(self, path, mode=mode, header=header, **options)
            _capture_df(self, str(path))
            return res
        DataFrameWriter.csv = patched_csv

    if orig_parquet:
        def patched_parquet(self, path, mode=None, **options):
            res = orig_parquet(self, path, mode=mode, **options)
            _capture_df(self, str(path))
            return res
        DataFrameWriter.parquet = patched_parquet

    if orig_save_as_table:
        def patched_save_as_table(self, name, format=None, mode=None, **options):
            res = orig_save_as_table(self, name, format=format, mode=mode, **options)
            _capture_df(self, f"table__{name}")
            return res
        DataFrameWriter.saveAsTable = patched_save_as_table

    DataFrameWriter._spark_validator_patched = True

# -----------------------------------------------------------------------------
# YAML load with ${ENV:default} interpolation
# -----------------------------------------------------------------------------
def _smart_cast(value: str):
    if not isinstance(value, str):
        return value
    v = value.strip()
    if v.lower() in {"null", "none"}:
        return None
    if v.lower() in {"true", "false"}:
        return v.lower() == "true"
    if v.isdigit():
        try:
            return int(v)
        except Exception:
            return value
    try:
        if "." in v and all(c.isdigit() or c == "." for c in v) and v.count(".") == 1:
            return float(v)
    except Exception:
        pass
    return value

def load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        config_path = str(SRC_DIR / "config" / "framework.yaml")

    config_path = str(Path(config_path).resolve())
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"

    def interpolate(obj):
        if isinstance(obj, str):
            def replace(match):
                var_name = match.group(1)
                default = match.group(2) if match.group(2) is not None else ""
                return os.environ.get(var_name, default)
            replaced = re.sub(pattern, replace, obj)
            return _smart_cast(replaced)
        if isinstance(obj, dict):
            return {k: interpolate(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [interpolate(v) for v in obj]
        return obj

    return interpolate(config)

# -----------------------------------------------------------------------------
# Path helpers
# -----------------------------------------------------------------------------
def to_container_path(job_path: Optional[str]) -> Optional[str]:
    if not job_path:
        return job_path
    s = str(job_path).strip().strip('"').replace("\\", "/")
    if s.startswith("/app/"):
        return s
    if not s.startswith("/"):
        return f"/app/{s.lstrip('/')}"
    marker = "/spark-validation-main/"
    if marker in s:
        return "/app/" + s.split(marker, 1)[1].lstrip("/")
    return s

def read_code_from_path(job_path: Optional[str]) -> str:
    if not job_path:
        return ""
    try:
        fp = Path(job_path)
        if fp.exists() and fp.is_file():
            return fp.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return ""

# -----------------------------------------------------------------------------
# Reject file
# -----------------------------------------------------------------------------
def _extract_default_params(config: dict) -> Dict[str, Any]:
    params = (config or {}).get("params", {}) or {}
    defaults = params.get("default_params", {}) or {}
    return copy.deepcopy(defaults)

def ensure_default_reject_file(config: dict) -> None:
    try:
        defaults = _extract_default_params(config)
        prm_path = defaults.get("Prm_Rej_ParamSet") or "/tmp/spark-validator/rejects/prm_rej.json"
        rej_path = Path(str(prm_path))
        rej_path.parent.mkdir(parents=True, exist_ok=True)
        if not rej_path.exists():
            rej_path.write_text('{"rejects":[]}\n', encoding="utf-8")
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Iceberg REST + MinIO: enforce and block catalog.type injections
# -----------------------------------------------------------------------------
def _iceberg_cfg(config: dict) -> dict:
    if isinstance((config or {}).get("iceberg"), dict):
        return (config or {}).get("iceberg") or {}

    spark_cfg = (config or {}).get("spark", {}) or {}
    cats = spark_cfg.get("catalogs")
    if isinstance(cats, dict) and isinstance(cats.get("my_catalog"), dict):
        c = cats.get("my_catalog") or {}
        return {"enabled": True, "catalog_name": str(c.get("name") or "my_catalog")}
    return {"enabled": False, "catalog_name": "my_catalog"}

def _iceberg_enabled(config: dict) -> bool:
    ice = _iceberg_cfg(config)
    return str(ice.get("enabled", False)).strip().lower() in {"1", "true", "yes", "y"}

def _iceberg_catalog_name(config: dict) -> str:
    ice = _iceberg_cfg(config)
    return str(ice.get("catalog_name") or "my_catalog").strip() or "my_catalog"

def _apply_iceberg_rest_to_builder(builder, config: dict):
    if not _iceberg_enabled(config):
        return builder

    cat = _iceberg_catalog_name(config)
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.environ.get("ICEBERG_S3_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")

    iceberg_version = os.environ.get("ICEBERG_VERSION", "1.10.1")
    hadoop_aws_version = os.environ.get("HADOOP_AWS_VERSION", "3.3.4")
    aws_sdk_bundle_version = os.environ.get("AWS_SDK_BUNDLE_VERSION", "1.12.262")

    pkgs = [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version}",
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_bundle_version}",
    ]

    builder = builder.config("spark.jars.packages", ",".join(pkgs))

    # STATIC config: must be set BEFORE SparkSession exists (builder only)
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # IMPORTANT: DO NOT set spark.sql.catalog.<cat>.type at all
    builder = builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    builder = builder.config(f"spark.sql.catalog.{cat}.uri", rest_uri)

    builder = builder.config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    builder = builder.config(f"spark.sql.catalog.{cat}.warehouse", warehouse)
    builder = builder.config(f"spark.sql.catalog.{cat}.s3.endpoint", s3_endpoint)
    builder = builder.config(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")

    builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_key)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    builder = builder.config("spark.sql.defaultCatalog", cat)

    _log(f"  ✓ Iceberg REST forced: {cat} -> {rest_uri} (warehouse={warehouse}, s3={s3_endpoint})")
    return builder

def _enforce_iceberg_rest_runtime(spark, config: dict) -> None:
    """
    Runtime enforcement must NOT set any static Spark conf keys.
    We only:
      - unset injected spark.sql.catalog.<cat>.type (conflicts with REST)
      - set non-static REST catalog keys (safe)
    """
    if spark is None or not _iceberg_enabled(config):
        return

    cat = _iceberg_catalog_name(config)
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.environ.get("ICEBERG_S3_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")

    # remove "type" if injected
    for k in (f"spark.sql.catalog.{cat}.type",):
        try:
            spark.conf.unset(k)
        except Exception:
            try:
                spark.conf.set(k, "")
            except Exception:
                pass

    # DO NOT set spark.sql.extensions here (static!)
    spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    spark.conf.set(f"spark.sql.catalog.{cat}.uri", rest_uri)

    spark.conf.set(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", warehouse)
    spark.conf.set(f"spark.sql.catalog.{cat}.s3.endpoint", s3_endpoint)
    spark.conf.set(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")

    spark.conf.set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        spark.conf.set("spark.sql.defaultCatalog", cat)
    except Exception:
        pass

def _install_iceberg_builder_guards(config: dict) -> None:
    """
    Prevent jobs from injecting spark.sql.catalog.<cat>.type via builder.config().
    Many converted jobs do: builder.config("spark.sql.catalog.my_catalog.type","hive")
    and that breaks REST.
    """
    if not _iceberg_enabled(config):
        return

    try:
        from pyspark.sql.session import SparkSession as _SS
    except Exception:
        return

    Builder = _SS.Builder
    if getattr(Builder, "_spark_validator_iceberg_guard", False):
        return

    cat = _iceberg_catalog_name(config)
    blocked = f"spark.sql.catalog.{cat}.type"

    orig_config = Builder.config
    orig_get = Builder.getOrCreate

    holder = {"cfg": config}

    def patched_config(self, key=None, value=None, *args, **kwargs):
        try:
            if isinstance(key, str) and key.strip() == blocked:
                return self
        except Exception:
            pass
        return orig_config(self, key, value, *args, **kwargs)

    def patched_get_or_create(self):
        spark = orig_get(self)
        try:
            _enforce_iceberg_rest_runtime(spark, holder["cfg"])
        except Exception:
            pass
        return spark

    Builder.config = patched_config
    Builder.getOrCreate = patched_get_or_create
    Builder._spark_validator_iceberg_guard = True

# -----------------------------------------------------------------------------
# Stage printing helpers
# -----------------------------------------------------------------------------
def _stage_display_name(stage_obj: Any) -> str:
    for attr in ("stage", "name"):
        try:
            v = getattr(stage_obj, attr, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        except Exception:
            pass
    return stage_obj.__class__.__name__

def _print_stage_exception_if_any(stage_obj: Any) -> None:
    """
    If stage failed but has 0 issues, print details/error/traceback if present.
    """
    try:
        if getattr(stage_obj, "passed", True):
            return
        issues = getattr(stage_obj, "issues", None) or []
        if len(issues) > 0:
            return

        details = getattr(stage_obj, "details", None) or {}
        err = details.get("error") or details.get("exception")
        tb = details.get("traceback")

        if err:
            _log(f"      [EXCEPTION] {str(err)[:500]}")
        if tb:
            lines = str(tb).splitlines()
            tail = lines[-20:] if len(lines) > 20 else lines
            for l in tail:
                _log(f"        {l[:240]}")
    except Exception:
        return

# -----------------------------------------------------------------------------
# Executor
# -----------------------------------------------------------------------------
class ExecutorRunner:
    def __init__(self, config: dict, worker_id: str = "worker-1", dry_run: bool = False):
        self.config = config
        self.worker_id = worker_id
        self.dry_run = dry_run

        self._redis = None
        self._running = False
        self._spark = None
        self._storage = None
        self.pipeline: Optional[ValidationPipeline] = None

        self.poll_interval = 2.0
        self.idle_sleep = 5.0

    async def initialize(self):
        import redis.asyncio as redis

        redis_url = (
            (self.config.get("queue", {}) or {}).get("redis_url")
            or (self.config.get("redis", {}) or {}).get("url")
            or os.environ.get("REDIS_URL")
            or "redis://localhost:6379/0"
        )

        self._redis = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        _log(f"✓ Connected to Redis: {redis_url}")

        ensure_default_reject_file(self.config)

        # Sync validator.config with env overrides so Postgres never falls back to localhost
        try:
            cfg_path = self.config.get("_config_path") or str(Path("config/framework.yaml").resolve())
            override = _build_validator_config_override_from_env()
            fw_cfg = _load_framework_config(config_path=cfg_path, config_dict=override if override else None)
            _set_framework_config(fw_cfg)

            resolved = _get_framework_config()
            _log(f"[validator.config] Redis URL: {resolved.redis.url}")
            _log(
                f"[validator.config] Postgres: {resolved.postgres.host}:{resolved.postgres.port} "
                f"db={resolved.postgres.database} user={resolved.postgres.user}"
            )
        except Exception as e:
            _log(f"[validator.config] WARNING: failed to set global config: {e}")

        # Spark session
        if SPARK_AVAILABLE and not self.dry_run:
            _log("  Initializing Spark session...")
            _install_iceberg_builder_guards(self.config)

            spark_cfg = self.config.get("spark", {}) or {}
            spark_local = spark_cfg.get("local", {}) or {}
            spark_conf = (spark_cfg.get("conf", {}) or {}).copy()

            master = spark_local.get("master") or spark_cfg.get("master") or "local[*]"
            app_name = spark_local.get("app_name") or spark_cfg.get("app_name") or f"ValidationExecutor-{self.worker_id}"

            builder = SparkSession.builder.appName(app_name).master(master)

            for k, v in spark_conf.items():
                if v is None:
                    continue
                sv = str(v).strip()
                if sv == "":
                    continue
                builder = builder.config(str(k), sv)

            builder = _apply_iceberg_rest_to_builder(builder, self.config)

            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")
            _log("  ✓ Spark session created")

            # Enforce again (runtime-safe keys only)
            _enforce_iceberg_rest_runtime(self._spark, self.config)

            try:
                cat = _iceberg_catalog_name(self.config)
                _log(f"  [iceberg] spark.sql.catalog.{cat} = {self._spark.conf.get(f'spark.sql.catalog.{cat}')}")
                _log(f"  [iceberg] catalog-impl = {self._spark.conf.get(f'spark.sql.catalog.{cat}.catalog-impl', '')}")
                _log(f"  [iceberg] type = '{self._spark.conf.get(f'spark.sql.catalog.{cat}.type', '')}'")
                _log(f"  [iceberg] warehouse = {self._spark.conf.get(f'spark.sql.catalog.{cat}.warehouse', '')}")
            except Exception:
                pass

            install_output_capture_patch(self.config)
            _log("  ✓ Output capture enabled (DataFrameWriter patched)")
        else:
            if self.dry_run:
                _log("  ○ DRY RUN enabled - Spark execution will be skipped")
            else:
                _log("  ⚠ PySpark not available - execution stage will be skipped")

        # PostgreSQL storage (optional)
        if STORAGE_AVAILABLE:
            try:
                self._storage = create_storage(use_postgres=True)
                await self._storage.initialize()
                _log("  ✓ PostgreSQL storage connected")
            except Exception as e:
                _log(f"  ⚠ PostgreSQL storage unavailable: {e}")
                self._storage = None

        stages = get_stages_from_config(self.config)
        self.pipeline = ValidationPipeline(stages=stages, config=self.config)

        try:
            stage_names = [getattr(s, "stage", getattr(s, "name", s.__class__.__name__)) for s in stages]
            _log(f"  ✓ Pipeline stages: {' → '.join(stage_names)}")
        except Exception:
            pass

        _log(f"✓ Executor ready: {self.worker_id}" + (" [DRY RUN MODE]" if self.dry_run else ""))

    def get_repo_config(self, file_path: str) -> Tuple[Optional[Path], Optional[dict]]:
        base_path = (self.config.get("jobs", {}) or {}).get("base_path", "/app")
        repos = (((self.config.get("triggers", {}) or {}).get("git_poller", {}) or {}).get("repositories", []) or [])

        try:
            file_path_resolved = Path(file_path).resolve()
        except Exception:
            file_path_resolved = Path(file_path)

        for repo in repos:
            repo_name = repo.get("name", "")
            repo_root = Path(base_path) / repo_name
            try:
                file_path_resolved.relative_to(repo_root)
                return repo_root.resolve(), repo
            except Exception:
                continue

        return None, None

    def get_python_paths(self, file_path: str) -> List[str]:
        paths: List[str] = []
        for p in ("/app", "/app/src", "/app/jobs"):
            if p not in paths:
                paths.append(p)

        try:
            job_dir = str(Path(file_path).resolve().parent)
            if job_dir not in paths:
                paths.append(job_dir)
        except Exception:
            pass

        repo_root, repo_config = self.get_repo_config(file_path)
        if repo_root and repo_config:
            for p in repo_config.get("python_path", ["."]):
                full_path = (repo_root / p).resolve()
                if full_path.exists():
                    sp = str(full_path)
                    if sp not in paths:
                        paths.append(sp)

        return paths

    def create_context(self, job: Dict[str, Any]) -> ValidationContext:
        raw_path = job.get("job_path") or ""
        file_path = to_container_path(raw_path) or raw_path

        code = job.get("code") or job.get("code_content") or ""
        if not code:
            code = read_code_from_path(file_path)

        python_paths = self.get_python_paths(file_path)
        _ensure_sys_path(python_paths)

        defaults = _extract_default_params(self.config)
        overrides = job.get("job_params") or job.get("params") or {}
        if isinstance(overrides, dict) and overrides:
            defaults.update(overrides)

        metadata = {
            "job_id": job.get("id"),
            "branch": job.get("branch"),
            "commit_sha": job.get("commit_sha"),
            "trigger_source": job.get("trigger_source"),
            "triggered_by": job.get("triggered_by"),
        }

        ctx = ValidationContext.from_job(
            file_path=file_path,
            spark=self._spark,
            config=self.config,
            job_params=defaults,
            python_paths=python_paths,
            metadata=metadata,
            dry_run=self.dry_run,
        )

        try:
            ctx.code = code
        except Exception:
            pass

        return ctx

    async def dequeue_job(self) -> Optional[Dict[str, Any]]:
        if not self._redis:
            return None

        for priority in range(4):
            queue_key = f"validation:queue:priority:{priority}"
            result = await self._redis.zpopmin(queue_key, count=1)
            if not result:
                continue

            job_id, _score = result[0]
            job_key = f"validation:job:{job_id}"
            job_data = await self._redis.hgetall(job_key)
            if not job_data:
                continue

            now = datetime.now(timezone.utc).isoformat()
            await self._redis.hset(
                job_key,
                mapping={
                    "status": "RUNNING",
                    "started_at": now,
                    "worker_id": self.worker_id,
                },
            )

            return {"id": job_id, **job_data}

        return None

    async def complete_job(self, job_id: str, result: Dict[str, Any]):
        job_key = f"validation:job:{job_id}"
        status = "COMPLETED" if result.get("passed") else "FAILED"
        now = datetime.now(timezone.utc).isoformat()

        await self._redis.hset(
            job_key,
            mapping={
                "status": status,
                "completed_at": now,
                "result": json.dumps(result),
            },
        )

    async def fail_job(self, job_id: str, error: str):
        job_key = f"validation:job:{job_id}"
        now = datetime.now(timezone.utc).isoformat()
        await self._redis.hset(
            job_key,
            mapping={
                "status": "ERROR",
                "completed_at": now,
                "error": error,
            },
        )

    async def get_queue_stats(self) -> dict:
        stats = {"pending": {}, "total_pending": 0}
        for priority in range(4):
            queue_key = f"validation:queue:priority:{priority}"
            count = await self._redis.zcard(queue_key)
            stats["pending"][f"P{priority}"] = count
            stats["total_pending"] += count
        return stats

    async def run(self):
        await self.initialize()
        self._running = True
        jobs_processed = 0

        _log("Starting executor loop (Ctrl+C to stop)")

        while self._running:
            try:
                job = await self.dequeue_job()

                if job is None:
                    stats = await self.get_queue_stats()
                    now = datetime.now().strftime("%H:%M:%S")
                    _log(f"[{now}] Queue empty. Waiting... {stats}")
                    await asyncio.sleep(self.idle_sleep)
                    continue

                jobs_processed += 1
                job_id = job["id"]

                _log("=" * 70)
                _log(f"[Job #{jobs_processed}] {str(job_id)[:8]}")
                _log(f"  Path: {job.get('job_path')}")
                _log(f"  Branch: {job.get('branch', 'N/A')}")

                context = self.create_context(job)

                # Output capture folder
                capture_base = ""
                try:
                    capture_base = _compute_capture_base(self.config, str(context.file_path), str(job_id))
                    Path(capture_base).mkdir(parents=True, exist_ok=True)
                    _log(f"  Output capture folder: {capture_base}")
                    try:
                        context.set_shared("output_path", capture_base)
                    except Exception:
                        pass
                except Exception as e:
                    _log(f"  ⚠ capture folder init failed: {e}")

                # Set capture context for DFWriter patch
                token = None
                try:
                    token = _CAPTURE_CTX.set(
                        {"capture_base": capture_base, "job_id": str(job_id), "job_stem": Path(str(context.file_path)).stem}
                    )
                except Exception:
                    token = None

                try:
                    # enforce Iceberg again before running job (runtime-safe)
                    try:
                        _enforce_iceberg_rest_runtime(self._spark, self.config)
                    except Exception:
                        pass

                    result = self.pipeline.run(context)

                except Exception as e:
                    tb = traceback.format_exc()
                    _log(f"  ✗ PIPELINE CRASH: {e}")
                    for line in tb.splitlines()[-30:]:
                        _log(f"    {line}")
                    await self.fail_job(job_id, f"{e}")
                    continue

                finally:
                    if token is not None:
                        try:
                            _CAPTURE_CTX.reset(token)
                        except Exception:
                            pass

                # Print full stage results
                _log(f"  {'✓ PASSED' if result.passed else '✗ FAILED'}")

                try:
                    for stage in result.stages:
                        status = "✓" if stage.passed else "✗"
                        if getattr(stage, "skipped", False):
                            status = "○"

                        issues = getattr(stage, "issues", []) or []
                        dur = float(getattr(stage, "duration_seconds", 0.0) or 0.0)
                        name = _stage_display_name(stage)

                        _log(f"    {status} {name}: {len(issues)} issues ({dur:.2f}s)")

                        for issue in issues[:6]:
                            line = getattr(issue, "line", None)
                            line_info = f"L{line}" if line else ""
                            sev = getattr(issue.severity, "value", issue.severity)
                            msg = (issue.message or "")[:180]
                            _log(f"      [{sev}] {line_info} {msg}")

                        _print_stage_exception_if_any(stage)
                except Exception:
                    pass

                await self.complete_job(job_id, result.to_dict())

            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(f"Executor loop error: {e}")
                await asyncio.sleep(self.poll_interval)

        _log(f"Executor stopped. Processed {jobs_processed} jobs.")

    def stop(self):
        self._running = False
        _log("Stopping executor...")

    async def cleanup(self):
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                try:
                    await self._redis.close()
                except Exception:
                    pass

        if self._storage:
            try:
                await self._storage.close()
                _log("PostgreSQL storage closed")
            except Exception:
                pass

        if self._spark:
            try:
                self._spark.stop()
                _log("Spark session stopped")
            except Exception:
                pass

async def main():
    parser = argparse.ArgumentParser(description="Spark Validation Executor")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--worker-id", default="worker-1", help="Worker ID")
    parser.add_argument("--dry-run", action="store_true", help="Skip Spark execution")
    args = parser.parse_args()

    config = load_config(args.config)
    try:
        config["_config_path"] = str(Path(args.config or "config/framework.yaml").resolve())
    except Exception:
        pass

    runner = ExecutorRunner(config, worker_id=args.worker_id, dry_run=args.dry_run)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.stop)
        except NotImplementedError:
            pass

    try:
        await runner.run()
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
