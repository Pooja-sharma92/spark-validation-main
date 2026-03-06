"""
Validation Report Generator.

Generates reports in multiple formats:
- Markdown: Human-readable, good for PRs and docs
- JSON: Machine-readable, for integrations
- HTML: Rich formatting for web display
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum
import json

from validator.models import (
    ValidationResult,
    ValidationStatus,
    Severity,
    StageResult,
    ValidationIssue,
)


class ReportFormat(Enum):
    """Supported report formats."""
    MARKDOWN = "markdown"
    JSON = "json"
    HTML = "html"
    TEXT = "text"


@dataclass
class ReportConfig:
    """Configuration for report generation."""
    # Include detailed stage information
    include_stages: bool = True

    # Include individual issues
    include_issues: bool = True

    # Maximum issues to include per severity
    max_issues_per_severity: int = 10

    # Include timing information
    include_timing: bool = True

    # Include metadata
    include_metadata: bool = True

    # For batch reports: include individual job details
    include_job_details: bool = True

    # Output directory for file reports
    output_dir: Optional[str] = None


class ValidationReporter:
    """
    Generate validation reports in various formats.

    Supports single job reports and batch summary reports.
    """

    def __init__(self, config: Optional[ReportConfig] = None):
        self.config = config or ReportConfig()

    def generate(
        self,
        result: ValidationResult,
        format: ReportFormat = ReportFormat.MARKDOWN,
    ) -> str:
        """Generate a report for a single validation result."""
        if format == ReportFormat.MARKDOWN:
            return self._generate_markdown(result)
        elif format == ReportFormat.JSON:
            return self._generate_json(result)
        elif format == ReportFormat.HTML:
            return self._generate_html(result)
        elif format == ReportFormat.TEXT:
            return self._generate_text(result)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def generate_batch_report(
        self,
        results: List[ValidationResult],
        batch_id: str,
        format: ReportFormat = ReportFormat.MARKDOWN,
    ) -> str:
        """Generate a summary report for a batch of validations."""
        if format == ReportFormat.MARKDOWN:
            return self._generate_batch_markdown(results, batch_id)
        elif format == ReportFormat.JSON:
            return self._generate_batch_json(results, batch_id)
        elif format == ReportFormat.HTML:
            return self._generate_batch_html(results, batch_id)
        else:
            return self._generate_batch_markdown(results, batch_id)

    def save_report(
        self,
        content: str,
        filename: str,
        format: ReportFormat,
    ) -> Path:
        """Save report to file."""
        output_dir = Path(self.config.output_dir or ".")
        output_dir.mkdir(parents=True, exist_ok=True)

        extension = {
            ReportFormat.MARKDOWN: ".md",
            ReportFormat.JSON: ".json",
            ReportFormat.HTML: ".html",
            ReportFormat.TEXT: ".txt",
        }.get(format, ".txt")

        filepath = output_dir / f"{filename}{extension}"
        filepath.write_text(content, encoding="utf-8")
        return filepath

    # ─────────────────────────────────────────────────────────────────
    # Markdown Generation
    # ─────────────────────────────────────────────────────────────────

    def _generate_markdown(self, result: ValidationResult) -> str:
        """Generate Markdown report for single result."""
        lines = []

        # Header
        status_emoji = self._get_status_emoji(result.status)
        lines.append(f"# {status_emoji} Validation Report: {result.job_path}")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"| Field | Value |")
        lines.append(f"|-------|-------|")
        lines.append(f"| **Status** | {result.status.value} |")
        lines.append(f"| **Job ID** | `{result.job_id}` |")
        lines.append(f"| **Timestamp** | {result.timestamp.isoformat()} |")

        if self.config.include_timing and result.duration_seconds:
            lines.append(f"| **Duration** | {result.duration_seconds:.2f}s |")

        if result.commit_sha:
            lines.append(f"| **Commit** | `{result.commit_sha[:8]}` |")

        if result.branch:
            lines.append(f"| **Branch** | {result.branch} |")

        lines.append("")

        # Issue Summary
        if result.issues:
            lines.append("## Issues Summary")
            lines.append("")

            issue_counts = self._count_issues_by_severity(result.issues)
            for severity in Severity:
                count = issue_counts.get(severity, 0)
                if count > 0:
                    emoji = self._get_severity_emoji(severity)
                    lines.append(f"- {emoji} **{severity.value}**: {count}")
            lines.append("")

        # Stage Details
        if self.config.include_stages and result.stage_results:
            lines.append("## Stage Results")
            lines.append("")

            for stage in result.stage_results:
                stage_emoji = "✓" if stage.passed else "✗"
                lines.append(f"### {stage_emoji} {stage.stage_name.title()}")
                lines.append("")
                lines.append(f"- **Status**: {'Passed' if stage.passed else 'Failed'}")

                if stage.duration_seconds:
                    lines.append(f"- **Duration**: {stage.duration_seconds:.2f}s")

                if stage.error_message:
                    lines.append(f"- **Error**: {stage.error_message}")

                # Stage-specific issues
                stage_issues = [i for i in result.issues if i.stage == stage.stage_name]
                if stage_issues and self.config.include_issues:
                    lines.append("")
                    lines.append("**Issues:**")
                    for issue in stage_issues[:self.config.max_issues_per_severity]:
                        emoji = self._get_severity_emoji(issue.severity)
                        lines.append(f"- {emoji} `{issue.code}`: {issue.message}")
                        if issue.location:
                            lines.append(f"  - Location: {issue.location}")

                lines.append("")

        # All Issues (if not shown per stage)
        if self.config.include_issues and result.issues and not self.config.include_stages:
            lines.append("## Issues")
            lines.append("")

            for severity in Severity:
                severity_issues = [i for i in result.issues if i.severity == severity]
                if severity_issues:
                    emoji = self._get_severity_emoji(severity)
                    lines.append(f"### {emoji} {severity.value}")
                    lines.append("")

                    for issue in severity_issues[:self.config.max_issues_per_severity]:
                        lines.append(f"- `{issue.code}`: {issue.message}")
                        if issue.location:
                            lines.append(f"  - Location: {issue.location}")
                        if issue.suggestion:
                            lines.append(f"  - Suggestion: {issue.suggestion}")

                    if len(severity_issues) > self.config.max_issues_per_severity:
                        remaining = len(severity_issues) - self.config.max_issues_per_severity
                        lines.append(f"- ... and {remaining} more")

                    lines.append("")

        # Metadata
        if self.config.include_metadata and result.metadata:
            lines.append("## Metadata")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(result.metadata, indent=2))
            lines.append("```")
            lines.append("")

        # Footer
        lines.append("---")
        lines.append(f"*Generated at {datetime.now().isoformat()}*")

        return "\n".join(lines)

    def _generate_batch_markdown(
        self,
        results: List[ValidationResult],
        batch_id: str,
    ) -> str:
        """Generate Markdown batch summary report."""
        lines = []

        # Header
        lines.append(f"# Batch Validation Report: {batch_id}")
        lines.append("")

        # Overall Summary
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        warnings = sum(1 for r in results if r.status == ValidationStatus.PASSED_WITH_WARNINGS)

        lines.append("## Overall Summary")
        lines.append("")
        lines.append(f"| Metric | Count |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Total Jobs | {total} |")
        lines.append(f"| ✓ Passed | {passed} |")
        lines.append(f"| ⚠ Warnings | {warnings} |")
        lines.append(f"| ✗ Failed | {failed} |")
        lines.append(f"| Success Rate | {(passed + warnings) / total * 100:.1f}% |")
        lines.append("")

        # Results table
        lines.append("## Results by Job")
        lines.append("")
        lines.append("| Job | Status | Issues | Duration |")
        lines.append("|-----|--------|--------|----------|")

        for result in results:
            status_emoji = self._get_status_emoji(result.status)
            issue_count = len(result.issues)
            duration = f"{result.duration_seconds:.1f}s" if result.duration_seconds else "-"
            job_name = Path(result.job_path).name
            lines.append(f"| {job_name} | {status_emoji} {result.status.value} | {issue_count} | {duration} |")

        lines.append("")

        # Failed jobs detail
        failed_results = [r for r in results if r.status == ValidationStatus.FAILED]
        if failed_results and self.config.include_job_details:
            lines.append("## Failed Jobs Details")
            lines.append("")

            for result in failed_results:
                lines.append(f"### {result.job_path}")
                lines.append("")

                critical_issues = [i for i in result.issues if i.severity == Severity.ERROR]
                for issue in critical_issues[:5]:
                    lines.append(f"- `{issue.code}`: {issue.message}")

                lines.append("")

        # Footer
        lines.append("---")
        lines.append(f"*Batch ID: {batch_id}*")
        lines.append(f"*Generated at {datetime.now().isoformat()}*")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # JSON Generation
    # ─────────────────────────────────────────────────────────────────

    def _generate_json(self, result: ValidationResult) -> str:
        """Generate JSON report for single result."""
        report = {
            "job_id": result.job_id,
            "job_path": result.job_path,
            "status": result.status.value,
            "timestamp": result.timestamp.isoformat(),
        }

        if self.config.include_timing:
            report["duration_seconds"] = result.duration_seconds

        if result.commit_sha:
            report["commit_sha"] = result.commit_sha

        if result.branch:
            report["branch"] = result.branch

        # Issue summary
        issue_counts = self._count_issues_by_severity(result.issues)
        report["issue_summary"] = {
            s.value: issue_counts.get(s, 0) for s in Severity
        }

        if self.config.include_stages:
            report["stages"] = [
                {
                    "name": s.stage_name,
                    "passed": s.passed,
                    "duration_seconds": s.duration_seconds,
                    "error_message": s.error_message,
                }
                for s in result.stage_results
            ]

        if self.config.include_issues:
            report["issues"] = [
                {
                    "code": i.code,
                    "severity": i.severity.value,
                    "message": i.message,
                    "stage": i.stage,
                    "location": i.location,
                    "suggestion": i.suggestion,
                }
                for i in result.issues
            ]

        if self.config.include_metadata:
            report["metadata"] = result.metadata

        report["generated_at"] = datetime.now().isoformat()

        return json.dumps(report, indent=2)

    def _generate_batch_json(
        self,
        results: List[ValidationResult],
        batch_id: str,
    ) -> str:
        """Generate JSON batch summary report."""
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        warnings = sum(1 for r in results if r.status == ValidationStatus.PASSED_WITH_WARNINGS)

        report = {
            "batch_id": batch_id,
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "warnings": warnings,
                "success_rate": (passed + warnings) / total if total > 0 else 0,
            },
            "results": [
                {
                    "job_id": r.job_id,
                    "job_path": r.job_path,
                    "status": r.status.value,
                    "issue_count": len(r.issues),
                    "duration_seconds": r.duration_seconds,
                }
                for r in results
            ],
            "generated_at": datetime.now().isoformat(),
        }

        if self.config.include_job_details:
            report["failed_details"] = [
                {
                    "job_path": r.job_path,
                    "issues": [
                        {"code": i.code, "message": i.message}
                        for i in r.issues
                        if i.severity == Severity.ERROR
                    ][:5]
                }
                for r in results
                if r.status == ValidationStatus.FAILED
            ]

        return json.dumps(report, indent=2)

    # ─────────────────────────────────────────────────────────────────
    # HTML Generation
    # ─────────────────────────────────────────────────────────────────

    def _generate_html(self, result: ValidationResult) -> str:
        """Generate HTML report for single result."""
        status_class = {
            ValidationStatus.PASSED: "success",
            ValidationStatus.FAILED: "danger",
            ValidationStatus.PASSED_WITH_WARNINGS: "warning",
            ValidationStatus.RUNNING: "info",
            ValidationStatus.PENDING: "secondary",
        }.get(result.status, "secondary")

        issues_html = ""
        if self.config.include_issues and result.issues:
            issues_html = "<h3>Issues</h3><ul>"
            for issue in result.issues[:20]:
                severity_class = {
                    Severity.ERROR: "danger",
                    Severity.WARNING: "warning",
                    Severity.INFO: "info",
                }.get(issue.severity, "secondary")
                issues_html += f'<li class="issue-{severity_class}"><code>{issue.code}</code>: {issue.message}</li>'
            issues_html += "</ul>"

        stages_html = ""
        if self.config.include_stages and result.stage_results:
            stages_html = "<h3>Stages</h3><table><tr><th>Stage</th><th>Status</th><th>Duration</th></tr>"
            for stage in result.stage_results:
                stage_class = "success" if stage.passed else "danger"
                duration = f"{stage.duration_seconds:.2f}s" if stage.duration_seconds else "-"
                stages_html += f'<tr class="{stage_class}"><td>{stage.stage_name}</td><td>{"✓" if stage.passed else "✗"}</td><td>{duration}</td></tr>'
            stages_html += "</table>"

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Validation Report: {result.job_path}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; }}
        .header {{ border-bottom: 2px solid #eee; padding-bottom: 20px; margin-bottom: 20px; }}
        .status-badge {{ padding: 5px 15px; border-radius: 4px; color: white; font-weight: bold; }}
        .success {{ background-color: #28a745; }}
        .danger {{ background-color: #dc3545; }}
        .warning {{ background-color: #ffc107; color: black; }}
        .info {{ background-color: #17a2b8; }}
        .secondary {{ background-color: #6c757d; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background-color: #f5f5f5; }}
        .issue-danger {{ color: #dc3545; }}
        .issue-warning {{ color: #856404; }}
        .issue-info {{ color: #0c5460; }}
        code {{ background-color: #f4f4f4; padding: 2px 6px; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Validation Report</h1>
        <p><strong>Job:</strong> {result.job_path}</p>
        <p><span class="status-badge {status_class}">{result.status.value}</span></p>
    </div>

    <h3>Summary</h3>
    <table>
        <tr><th>Field</th><th>Value</th></tr>
        <tr><td>Job ID</td><td><code>{result.job_id}</code></td></tr>
        <tr><td>Timestamp</td><td>{result.timestamp.isoformat()}</td></tr>
        <tr><td>Duration</td><td>{f'{result.duration_seconds:.2f}s' if result.duration_seconds else '-'}</td></tr>
        <tr><td>Issues</td><td>{len(result.issues)}</td></tr>
    </table>

    {stages_html}
    {issues_html}

    <footer style="margin-top: 40px; color: #666; font-size: 12px;">
        Generated at {datetime.now().isoformat()}
    </footer>
</body>
</html>"""
        return html

    def _generate_batch_html(
        self,
        results: List[ValidationResult],
        batch_id: str,
    ) -> str:
        """Generate HTML batch summary report."""
        total = len(results)
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)

        rows_html = ""
        for result in results:
            status_class = "success" if result.status == ValidationStatus.PASSED else "danger"
            job_name = Path(result.job_path).name
            rows_html += f'<tr class="{status_class}"><td>{job_name}</td><td>{result.status.value}</td><td>{len(result.issues)}</td></tr>'

        return f"""<!DOCTYPE html>
<html>
<head>
    <title>Batch Report: {batch_id}</title>
    <style>
        body {{ font-family: sans-serif; margin: 40px; }}
        .success td {{ background-color: #d4edda; }}
        .danger td {{ background-color: #f8d7da; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; }}
    </style>
</head>
<body>
    <h1>Batch Validation Report</h1>
    <p><strong>Batch ID:</strong> {batch_id}</p>
    <p><strong>Total:</strong> {total} | <strong>Passed:</strong> {passed} | <strong>Failed:</strong> {failed}</p>

    <table>
        <tr><th>Job</th><th>Status</th><th>Issues</th></tr>
        {rows_html}
    </table>
</body>
</html>"""

    # ─────────────────────────────────────────────────────────────────
    # Text Generation
    # ─────────────────────────────────────────────────────────────────

    def _generate_text(self, result: ValidationResult) -> str:
        """Generate plain text report."""
        lines = []

        lines.append("=" * 60)
        lines.append(f"VALIDATION REPORT: {result.job_path}")
        lines.append("=" * 60)
        lines.append("")
        lines.append(f"Status:    {result.status.value}")
        lines.append(f"Job ID:    {result.job_id}")
        lines.append(f"Timestamp: {result.timestamp.isoformat()}")

        if result.duration_seconds:
            lines.append(f"Duration:  {result.duration_seconds:.2f}s")

        lines.append("")
        lines.append("-" * 60)
        lines.append("ISSUES SUMMARY")
        lines.append("-" * 60)

        issue_counts = self._count_issues_by_severity(result.issues)
        for severity in Severity:
            count = issue_counts.get(severity, 0)
            lines.append(f"  {severity.value}: {count}")

        if self.config.include_stages and result.stage_results:
            lines.append("")
            lines.append("-" * 60)
            lines.append("STAGE RESULTS")
            lines.append("-" * 60)

            for stage in result.stage_results:
                status = "PASS" if stage.passed else "FAIL"
                duration = f"{stage.duration_seconds:.2f}s" if stage.duration_seconds else "-"
                lines.append(f"  [{status}] {stage.stage_name}: {duration}")

        if self.config.include_issues and result.issues:
            lines.append("")
            lines.append("-" * 60)
            lines.append("ISSUES")
            lines.append("-" * 60)

            for issue in result.issues[:20]:
                lines.append(f"  [{issue.severity.value}] {issue.code}: {issue.message}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    def _get_status_emoji(self, status: ValidationStatus) -> str:
        """Get emoji for validation status."""
        return {
            ValidationStatus.PASSED: "✅",
            ValidationStatus.FAILED: "❌",
            ValidationStatus.PASSED_WITH_WARNINGS: "⚠️",
            ValidationStatus.RUNNING: "🔄",
            ValidationStatus.PENDING: "⏳",
            ValidationStatus.CANCELLED: "🚫",
        }.get(status, "❓")

    def _get_severity_emoji(self, severity: Severity) -> str:
        """Get emoji for issue severity."""
        return {
            Severity.ERROR: "🔴",
            Severity.WARNING: "🟡",
            Severity.INFO: "🔵",
        }.get(severity, "⚪")

    def _count_issues_by_severity(
        self,
        issues: List[ValidationIssue],
    ) -> Dict[Severity, int]:
        """Count issues by severity level."""
        counts: Dict[Severity, int] = {}
        for issue in issues:
            counts[issue.severity] = counts.get(issue.severity, 0) + 1
        return counts


def create_reporter(config: Optional[ReportConfig] = None) -> ValidationReporter:
    """Factory function to create a reporter."""
    return ValidationReporter(config)
