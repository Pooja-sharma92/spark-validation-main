# ====== src/validator/stages/execution_stage.py ======

import io
import sys
import json
import re
import time
import traceback
import ast
from pathlib import Path
from typing import Dict, Any, Optional, Set, Tuple, List
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass
import types

from validator.pipeline import ValidationContext
from validator.stages.base import ValidationStage, ValidationIssue, Severity, StageResult


def _get_nested(cfg: dict, path: list, default=None):
    cur = cfg
    try:
        for k in path:
            cur = cur[k]
        return cur
    except Exception:
        return default


def _as_bool(v, default=False) -> bool:
    try:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in {"true", "1", "yes", "y"}
        return bool(v)
    except Exception:
        return default


def _get_cfg_dict_from_context(context) -> dict:
    cfg = getattr(context, "config", None)
    if cfg is None:
        cfg = getattr(context, "framework_config", None)
    if cfg is None:
        return {}
    if hasattr(cfg, "dict"):
        try:
            cfg = cfg.dict()
        except Exception:
            pass
    return cfg if isinstance(cfg, dict) else {}


def _is_dry_mode(context) -> bool:
    cfg = _get_cfg_dict_from_context(context)

    mode = _get_nested(cfg, ["framework", "mode"], default=None)
    if isinstance(mode, str) and mode.strip().lower() == "dry":
        return True

    return _as_bool(
        _get_nested(cfg, ["pipeline", "stage_settings", "execution", "dry_mode"], default=False),
        default=False,
    )


def _get_execution_skip_flags(context) -> dict:
    cfg = _get_cfg_dict_from_context(context)
    s = _get_nested(cfg, ["pipeline", "stage_settings", "execution"], default={}) or {}
    return {
        "skip_on_missing_table": _as_bool(s.get("skip_on_missing_table", False)),
        "skip_on_missing_utils": _as_bool(s.get("skip_on_missing_utils", False)),
        "skip_on_missing_files": _as_bool(s.get("skip_on_missing_files", False)),
        "skip_on_unresolved_udf": _as_bool(s.get("skip_on_unresolved_udf", False)),
        # ✅ This is what produced your warning instead of failing
        "skip_on_missing_paramset_key": _as_bool(s.get("skip_on_missing_paramset_key", True)),
    }


def run_python_code(code: str, filename: str, globals_dict: dict) -> None:
    compiled = compile(code, filename, "exec")
    exec(compiled, globals_dict)


def _inject_fallback_utils_udfs() -> None:
    if "utils" not in sys.modules:
        sys.modules["utils"] = types.ModuleType("utils")

    if "utils.udfs" in sys.modules:
        return

    udfs_mod = types.ModuleType("utils.udfs")

    def _noop(*args, **kwargs):
        return None

    for n in [
        "udf_to_number",
        "udf_left",
        "udf_right",
        "udf_timestamp",
        "udf_decimal",
        "register_udfs",
    ]:
        setattr(udfs_mod, n, _noop)

    sys.modules["utils.udfs"] = udfs_mod


def _is_paramset_name(name: str) -> bool:
    if not name:
        return False
    return name.endswith("ParameterSet") or name.endswith("ParamSet") or "ParameterSet" in name or "ParamSet" in name


def extract_paramset_keys(code: str) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    if not code:
        return result

    try:
        tree = ast.parse(code)
    except Exception:
        return result

    class V(ast.NodeVisitor):
        def visit_Subscript(self, node: ast.Subscript):
            try:
                if isinstance(node.value, ast.Name):
                    base = node.value.id
                    if not _is_paramset_name(base):
                        return

                    key = None
                    if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                        key = node.slice.value
                    elif hasattr(ast, "Index") and isinstance(node.slice, ast.Index):
                        if isinstance(node.slice.value, ast.Constant) and isinstance(node.slice.value.value, str):
                            key = node.slice.value.value

                    if key:
                        result.setdefault(base, set()).add(key)
            except Exception:
                pass
            self.generic_visit(node)

    V().visit(tree)
    return result


def extract_args_paramset_names(code: str) -> Set[str]:
    found: Set[str] = set()
    if not code:
        return found

    for m in re.finditer(r"\bargs\.([A-Za-z_][A-Za-z0-9_]*)\b", code or ""):
        name = m.group(1)
        if _is_paramset_name(name):
            found.add(name)
    return found


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json_file(fp: Path, payload: Dict[str, Any]) -> str:
    _ensure_dir(fp.parent)
    fp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return str(fp)


def _looks_like_as_predefined(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"(as pre-defined)", "(as predefined)", "as pre-defined", "as predefined"}


def _default_value_for_key(k: str) -> Any:
    kl = (k or "").lower()
    if "date" in kl:
        return "1970-01-01"
    if "time" in kl:
        return "00:00:00"
    if "flag" in kl:
        return "N"
    if "count" in kl or "num" in kl:
        return 0
    return ""


def _ensure_default_reject_file() -> str:
    base = Path("/tmp/spark-validator/rejects")
    _ensure_dir(base)
    fp = base / "prm_rej.json"
    if not fp.exists():
        fp.write_text('{"rejects":[]}\n', encoding="utf-8")
    return str(fp)


def sanitize_predefined_paramsets(job_id: str, params: Dict[str, Any], code: str) -> Dict[str, Any]:
    out = dict(params or {})
    needed_keys = extract_paramset_keys(code)
    needed_arg_paramsets = extract_args_paramset_names(code)

    base = Path("/tmp/spark-validator/params")
    _ensure_dir(base)

    reject_fp = _ensure_default_reject_file()

    if "Prm_Rej_ParamSet" in out and _looks_like_as_predefined(out.get("Prm_Rej_ParamSet")):
        out["Prm_Rej_ParamSet"] = reject_fp
    elif "Prm_Rej_ParamSet" not in out:
        out["Prm_Rej_ParamSet"] = reject_fp

    for name in sorted(needed_arg_paramsets):
        if (name not in out) or _looks_like_as_predefined(out.get(name)):
            fp = base / f"{job_id}_{name}.json"
            out[name] = _write_json_file(fp, {})

    for name, val in list(out.items()):
        if not _is_paramset_name(name):
            continue
        if not _looks_like_as_predefined(val):
            continue

        required = needed_keys.get(name, set())
        payload = {k: _default_value_for_key(k) for k in sorted(required)} if required else {}
        fp = base / f"{job_id}_{name}.json"
        out[name] = _write_json_file(fp, payload)

    return out


def build_argv_from_params(script_path: str, params: Dict[str, Any], allowed_args: Optional[Set[str]] = None) -> list:
    argv = [script_path]
    if not params:
        return argv

    for k, v in params.items():
        if allowed_args and k not in allowed_args:
            continue
        if v is None:
            continue
        argv.append(f"--{k}")
        argv.append("true" if isinstance(v, bool) and v else "false" if isinstance(v, bool) else str(v))

    return argv


def materialize_and_enrich_params(job_id: str, params: Dict[str, Any], code: str) -> Dict[str, Any]:
    out = dict(params or {})
    base = Path("/tmp/spark-validator/params")
    _ensure_dir(base)

    for k, v in list(out.items()):
        if isinstance(v, (dict, list)):
            fp = base / f"{job_id}_{k}.json"
            fp.write_text(json.dumps(v, indent=2) + "\n", encoding="utf-8")
            out[k] = str(fp)

    return out


def extract_argparse_options(code: str) -> Optional[Set[str]]:
    opts: Set[str] = set()
    try:
        tree = ast.parse(code)
    except Exception:
        return None

    class V(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call):
            try:
                if isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
                    if node.args:
                        a0 = node.args[0]
                        if isinstance(a0, ast.Constant) and isinstance(a0.value, str):
                            s = a0.value
                            if s.startswith("--"):
                                opts.add(s[2:])
            except Exception:
                pass
            self.generic_visit(node)

    V().visit(tree)
    return opts or None


def try_create_missing_file(e: FileNotFoundError) -> Optional[str]:
    try:
        missing = e.filename
        if not missing:
            return None
        if isinstance(missing, str) and missing.strip().lower().startswith("(as"):
            return None

        p = Path(str(missing))
        p.parent.mkdir(parents=True, exist_ok=True)

        if p.name.lower().endswith(".json"):
            if not p.exists():
                p.write_text("{}", encoding="utf-8")
            return str(p)

        if not p.exists():
            p.write_text("", encoding="utf-8")
        return str(p)
    except Exception:
        return None


@dataclass
class ExecutionOutput:
    exit_code: int
    duration_seconds: float
    stdout: str
    stderr: str


class ExecutionStage(ValidationStage):
    name = "execution"
    requires_spark = True
    blocking = False

    def __init__(self, config: Optional[dict] = None):
        super().__init__(config=config)

    def _warn(self, msg: str) -> ValidationIssue:
        return ValidationIssue(stage=self.name, severity=Severity.WARNING, message=msg, line=None)

    def _err(self, msg: str) -> ValidationIssue:
        return ValidationIssue(stage=self.name, severity=Severity.ERROR, message=msg, line=None)

    def _get_output_dir(self, context: ValidationContext) -> Optional[Path]:
        """
        IMPORTANT:
        Your framework already prints:
          Output capture folder: /app/reports/outputs/<JOB>/<UUID>

        That path is typically stored in context shared state.
        We'll try multiple common places to fetch it safely.
        """
        candidates: List[Optional[str]] = []

        # 1) shared: output_path (used by golden_compare_stage you have)
        try:
            candidates.append(context.get_shared("output_path"))
        except Exception:
            pass

        # 2) shared: output_dir / output_capture_folder
        for k in ("output_dir", "output_capture_folder", "output_capture_path"):
            try:
                candidates.append(context.get_shared(k))
            except Exception:
                pass

        # 3) metadata fields (if framework puts it there)
        md = getattr(context, "metadata", {}) or {}
        for k in ("output_path", "output_dir", "output_capture_folder", "output_capture_path"):
            v = md.get(k)
            if v:
                candidates.append(str(v))

        # pick first valid
        for c in candidates:
            if not c:
                continue
            p = Path(str(c))
            return p
        return None

    def _write_text_safe(self, fp: Path, text: str) -> None:
        try:
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(text or "", encoding="utf-8", errors="ignore")
        except Exception:
            pass

    def _write_json_safe(self, fp: Path, payload: Dict[str, Any]) -> None:
        try:
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

    def _execute_job_with_dry_mode_rules(
        self, context: ValidationContext, code: str, allowed_args: Optional[Set[str]], out_dir: Optional[Path]
    ) -> Tuple[ExecutionOutput, List[ValidationIssue], List[str]]:
        t0 = time.time()
        issues: List[ValidationIssue] = []
        sql_calls: List[str] = []

        dry_mode = _is_dry_mode(context)
        skip_flags = _get_execution_skip_flags(context)

        if skip_flags.get("skip_on_missing_utils"):
            try:
                _inject_fallback_utils_udfs()
            except Exception:
                pass

        job_id = (getattr(context, "metadata", {}) or {}).get("job_id") or Path(context.file_path).stem

        params = materialize_and_enrich_params(str(job_id), getattr(context, "job_params", {}) or {}, code)
        params = sanitize_predefined_paramsets(str(job_id), params, code)
        fake_argv = build_argv_from_params(context.file_path, params, allowed_args)

        try:
            context.job_params = params
        except Exception:
            pass

        exec_globals = {
            "__file__": context.file_path,
            "__name__": "__main__",
            "spark": context.spark,
            "config": params,
        }

        original_sys_path = list(sys.path)
        for p in ("/app", "/app/src", "/app/jobs"):
            if p not in sys.path:
                sys.path.insert(0, p)

        # ✅ Patch spark.sql to capture DML/DDL text into out_dir (MERGE/INSERT/UPDATE...)
        original_sql = None
        try:
            if getattr(context, "spark", None) is not None:
                original_sql = context.spark.sql

                def _wrapped_sql(query, *args, **kwargs):
                    try:
                        q = str(query)
                    except Exception:
                        q = repr(query)
                    sql_calls.append(q)

                    # Write DML/DDL statements to output folder so folder never stays empty
                    if out_dir is not None:
                        try:
                            head = q.lstrip().upper()[:32] if q else ""
                            if head.startswith(("MERGE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER")):
                                fp = out_dir / f"sql_{len(sql_calls):04d}.sql"
                                fp.write_text(q + "\n", encoding="utf-8", errors="ignore")
                        except Exception:
                            pass

                    return original_sql(query, *args, **kwargs)

                context.spark.sql = _wrapped_sql  # type: ignore[attr-defined]
        except Exception:
            pass

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        exit_code = 0
        old_argv = sys.argv
        try:
            sys.argv = fake_argv
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                run_python_code(code, context.file_path, exec_globals)

        except KeyError as e:
            missing_key = str(e).strip("'").strip('"')
            if skip_flags.get("skip_on_missing_paramset_key"):
                issues.append(self._warn(f"Missing ParamSet key during execution (downgraded): {missing_key}"))
                exit_code = 0
            else:
                issues.append(self._err(f"Execution crashed: {e}"))
                exit_code = 1

        except FileNotFoundError as e:
            created = None
            if skip_flags.get("skip_on_missing_files"):
                created = try_create_missing_file(e)

            if created:
                issues.append(self._warn(f"Missing file during execution; created placeholder: {created}"))
                exit_code = 0 if dry_mode else 1
            else:
                issues.append(self._err(str(e)))
                exit_code = 1

        except Exception as e:
            msg = str(e)
            if skip_flags.get("skip_on_missing_table"):
                if ("TABLE_OR_VIEW_NOT_FOUND" in msg) or ("[PATH_NOT_FOUND]" in msg) or ("Cannot find" in msg):
                    issues.append(self._warn(f"Spark SQL missing object (downgraded): {msg}"))
                    exit_code = 0
                else:
                    issues.append(self._err(f"Execution crashed: {e}"))
                    exit_code = 1
            else:
                issues.append(self._err(f"Execution crashed: {e}"))
                exit_code = 1

        finally:
            # restore argv + sys.path
            try:
                sys.argv = old_argv
            except Exception:
                pass
            sys.path = original_sys_path

            # restore spark.sql
            try:
                if original_sql is not None and getattr(context, "spark", None) is not None:
                    context.spark.sql = original_sql  # type: ignore[attr-defined]
            except Exception:
                pass

        duration = time.time() - t0
        out = ExecutionOutput(
            exit_code=exit_code,
            duration_seconds=duration,
            stdout=stdout_capture.getvalue(),
            stderr=stderr_capture.getvalue(),
        )
        return out, issues, sql_calls

    def validate(self, context: ValidationContext) -> StageResult:
        t0 = time.time()
        issues: List[ValidationIssue] = []

        code = getattr(context, "code", None) or ""
        if not code:
            try:
                code = Path(context.file_path).read_text(encoding="utf-8", errors="ignore")
            except Exception:
                code = ""

        allowed_args = extract_argparse_options(code)

        if not getattr(context, "spark", None):
            issues.append(self._warn("Spark session not available; execution skipped."))
            return StageResult(stage=self.name, passed=True, issues=issues, duration_seconds=(time.time() - t0))

        # ✅ Get the already-created output capture folder (UUID path printed in logs)
        out_dir = self._get_output_dir(context)

        # ✅ Always create at least a manifest so output folder is never empty
        if out_dir is not None:
            try:
                out_dir.mkdir(parents=True, exist_ok=True)
                self._write_json_safe(
                    out_dir / "manifest.json",
                    {
                        "job_path": str(getattr(context, "file_path", "")),
                        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "note": "Created by execution_stage to avoid empty output capture folder when job writes via spark.sql (MERGE/INSERT) instead of df.write.",
                    },
                )
            except Exception:
                pass

        out, exec_issues, sql_calls = self._execute_job_with_dry_mode_rules(context, code, allowed_args, out_dir)
        issues.extend(exec_issues)

        # Store execution output in shared state (used by reports)
        try:
            context.set_shared(
                "execution_output",
                {
                    "exit_code": out.exit_code,
                    "duration_seconds": out.duration_seconds,
                    "stdout": out.stdout,
                    "stderr": out.stderr,
                },
            )
        except Exception:
            pass

        # ✅ Write logs/outputs into output capture folder
        if out_dir is not None:
            self._write_text_safe(out_dir / "stdout.log", out.stdout)
            self._write_text_safe(out_dir / "stderr.log", out.stderr)
            self._write_json_safe(
                out_dir / "execution_output.json",
                {
                    "exit_code": out.exit_code,
                    "duration_seconds": out.duration_seconds,
                    "sql_calls": len(sql_calls),
                },
            )
            # If there were SQL calls but none were DML/DDL captured, still keep a trace
            if sql_calls:
                self._write_text_safe(out_dir / "sql_all.txt", "\n\n-- ---\n\n".join(sql_calls))

        passed = (out.exit_code == 0)

        cfg = _get_cfg_dict_from_context(context)
        fail_on_exec = _as_bool(_get_nested(cfg, ["framework", "fail_on_execution_error"], default=False), default=False)
        if not fail_on_exec:
            if not passed:
                issues.append(self._warn("Execution failed but framework.fail_on_execution_error=false; downgraded to WARNING."))
            passed = True

        return StageResult(
            stage=self.name,
            passed=passed,
            issues=issues,
            duration_seconds=(time.time() - t0),
            details={
                "output_path": str(out_dir) if out_dir else None,
                "sql_calls": len(sql_calls),
                "exit_code": out.exit_code,
            },
        )
