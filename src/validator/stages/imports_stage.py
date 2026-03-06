"""
Import validation stage.

Analyzes import statements for potential issues like star imports
and deprecated modules.
"""

import ast
import time
from typing import Dict, Any, List, Set

from .base import ValidationStage, StageResult, ValidationIssue, Severity
from ..pipeline.context import ValidationContext


class ImportsStage(ValidationStage):
    """
    Import statement analysis.

    Checks for:
    - Star imports (from X import *)
    - Deprecated modules (e.g., pyspark.mllib)
    - Potentially problematic imports

    This is a non-blocking stage - warnings don't stop validation.
    """

    name = "imports"
    requires_spark = False
    blocking = False  # Warnings only, don't block

    # Deprecated modules and their replacements
    DEPRECATED_MODULES = {
        "pyspark.mllib": "Use pyspark.ml instead of pyspark.mllib",
        "pandas.core.common": "This module is deprecated",
    }

    def validate(self, context: ValidationContext) -> StageResult:
        """
        Analyze import statements for issues.

        Args:
            context: ValidationContext with code/AST

        Returns:
            StageResult with any import warnings
        """
        start = time.time()
        issues = []

        # Try to use cached AST from syntax stage
        tree = context.get_shared("ast")
        if tree is None:
            try:
                tree = ast.parse(context.code)
            except SyntaxError:
                return self.create_result(
                    passed=False,
                    issues=[self.create_issue(
                        Severity.ERROR,
                        "Cannot parse imports due to syntax error",
                    )],
                    duration=time.time() - start,
                )

        imports_found: List[str] = []
        star_imports: List[str] = []

        for node in ast.walk(tree):
            # Check ImportFrom statements
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                imports_found.append(module)

                # Check for star imports
                for alias in node.names:
                    if alias.name == "*":
                        star_imports.append(module)
                        issues.append(self.create_issue(
                            severity=Severity.WARNING,
                            message=f"Star import from '{module}' - avoid in production",
                            line=node.lineno,
                            suggestion=f"Import specific names from {module}",
                            rule="star_import",
                        ))

                # Check for deprecated modules
                for deprecated, suggestion in self.DEPRECATED_MODULES.items():
                    if module and deprecated in module:
                        issues.append(self.create_issue(
                            severity=Severity.WARNING,
                            message=f"Deprecated module: {module}",
                            line=node.lineno,
                            suggestion=suggestion,
                            rule="deprecated_module",
                        ))

            # Check regular Import statements
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports_found.append(alias.name)

                    # Check for deprecated modules
                    for deprecated, suggestion in self.DEPRECATED_MODULES.items():
                        if deprecated in alias.name:
                            issues.append(self.create_issue(
                                severity=Severity.WARNING,
                                message=f"Deprecated module: {alias.name}",
                                line=node.lineno,
                                suggestion=suggestion,
                                rule="deprecated_module",
                            ))

        # Store import info in shared context
        context.set_shared("imports", imports_found)
        context.set_shared("star_imports", star_imports)

        # Pass if no ERROR-level issues (warnings are acceptable)
        error_count = len([i for i in issues if i.severity == Severity.ERROR])

        return self.create_result(
            passed=error_count == 0,
            issues=issues,
            duration=time.time() - start,
            details={
                "imports_count": len(imports_found),
                "star_imports_count": len(star_imports),
            },
        )
