"""
Syntax validation stage.

Validates Python syntax using AST parsing.
This is typically the first stage and blocks subsequent stages on failure.
"""

import ast
import time
from pathlib import Path
from typing import Optional

from .base import ValidationStage, StageResult, ValidationIssue, Severity
from ..pipeline.context import ValidationContext


class SyntaxStage(ValidationStage):
    """
    Python syntax validation using AST.

    This stage parses the code using Python's ast module to detect
    syntax errors. It's a blocking stage - if syntax is invalid,
    no further validation can proceed.
    """

    name = "syntax"
    requires_spark = False
    blocking = True

    def _load_code_if_missing(self, context: ValidationContext) -> Optional[str]:
        """
        If context.code is empty, try to load the file from context.file_path.
        This avoids silent failures and provides better diagnostics.
        """
        if context.code and context.code.strip():
            return context.code

        fp = getattr(context, "file_path", None)
        if not fp:
            return None

        try:
            p = Path(fp)
            if not p.exists():
                # Store diagnostic info in shared for debugging
                context.set_shared("file_read_error", f"File not found: {fp}")
                return None

            # Read using utf-8 but tolerate bad chars
            text = p.read_text(encoding="utf-8", errors="replace")
            if text is None:
                context.set_shared("file_read_error", f"Read returned None: {fp}")
                return None

            # Update context.code so downstream stages use it
            context.code = text
            return text

        except Exception as e:
            context.set_shared("file_read_error", f"Failed reading {fp}: {e}")
            return None

    def validate(self, context: ValidationContext) -> StageResult:
        """
        Validate Python syntax by parsing AST.
        """
        start = time.time()

        # Ensure code is loaded
        code = self._load_code_if_missing(context)

        if not code or not code.strip():
            fp = getattr(context, "file_path", None)
            p = Path(fp) if fp else None

            details = {"file": fp}
            if p:
                details.update({
                    "exists": p.exists(),
                    "is_file": p.is_file() if p.exists() else False,
                    "size_bytes": p.stat().st_size if p.exists() and p.is_file() else None,
                })

            read_err = context.get_shared("file_read_error")
            if read_err:
                details["file_read_error"] = read_err

            return self.create_result(
                passed=False,
                issues=[self.create_issue(
                    Severity.ERROR,
                    "No code content to validate (file could not be loaded).",
                    suggestion="Ensure the job path is valid inside the executor container and the repo is mounted to /app.",
                    rule="code_load_failed",
                )],
                duration=time.time() - start,
                details=details,
            )

        try:
            tree = ast.parse(code)
            context.set_shared("ast", tree)

            return self.create_result(
                passed=True,
                issues=[],
                duration=time.time() - start,
                details={
                    "file": getattr(context, "file_path", None),
                    "lines": len(code.splitlines()),
                },
            )

        except SyntaxError as e:
            return self.create_result(
                passed=False,
                issues=[self.create_issue(
                    severity=Severity.ERROR,
                    message=f"Syntax error: {e.msg}",
                    line=e.lineno,
                    column=e.offset,
                    suggestion="Fix the syntax error before proceeding",
                    rule="python_syntax",
                )],
                duration=time.time() - start,
                details={
                    "file": getattr(context, "file_path", None),
                    "error_type": "SyntaxError",
                },
            )
