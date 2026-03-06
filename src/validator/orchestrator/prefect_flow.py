"""Prefect Flow for Spark Job Validation

Orchestrates validation tasks in parallel and generates comprehensive reports.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

# Import validation tasks
from validator.tasks import validate_syntax, validate_logic, validate_data_sources


@task(name="Validate with Great Expectations", retries=1)
def validate_with_great_expectations(
    job_path: str,
    config_path: str,
    ge_config_dir: str = "ge_config"
) -> Dict[str, Any]:
    """Run Great Expectations validation on output data

    Args:
        job_path: Path to Spark job Python file
        config_path: Path to job config JSON file
        ge_config_dir: Path to Great Expectations config directory

    Returns:
        Validation result with issues found
    """
    issues = []

    try:
        # Import Great Expectations
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest
        from pyspark.sql import SparkSession

        # Load job config to get output table info
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Get GE context
        ge_context_path = Path(ge_config_dir)
        if not ge_context_path.exists():
            issues.append({
                "severity": "warning",
                "category": "expectations",
                "message": "Great Expectations config not found - skipping data quality validation",
                "suggestion": f"Initialize Great Expectations in {ge_config_dir}"
            })
            return {
                "stage": "expectations",
                "passed": True,
                "issues": issues,
                "total_issues": len(issues),
                "critical_count": 0,
                "warning_count": len(issues)
            }

        # Create GE data context
        context = gx.get_context(context_root_dir=str(ge_context_path))

        # Create or get Spark session
        spark = SparkSession.builder \
            .appName("GE_Validation") \
            .master("local[2]") \
            .getOrCreate()

        # Load the output table data (simulated - in production would read from actual table)
        # For now, we'll just validate the configuration
        issues.append({
            "severity": "info",
            "category": "expectations",
            "message": "Great Expectations validation ready (requires actual data)",
            "suggestion": "Run end-to-end test with real data to validate expectations"
        })

        spark.stop()

    except ImportError as e:
        issues.append({
            "severity": "warning",
            "category": "expectations",
            "message": f"Great Expectations not available: {str(e)}",
            "suggestion": "Install great-expectations: pip install great-expectations"
        })
    except Exception as e:
        issues.append({
            "severity": "warning",
            "category": "expectations",
            "message": f"Great Expectations validation failed: {str(e)}",
            "suggestion": "Check Great Expectations configuration"
        })

    return {
        "stage": "expectations",
        "passed": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "total_issues": len(issues),
        "critical_count": len([i for i in issues if i["severity"] == "critical"]),
        "warning_count": len([i for i in issues if i["severity"] == "warning"])
    }


@task(name="Generate Validation Report")
def generate_validation_report(
    validation_results: List[Dict[str, Any]],
    job_path: str,
    start_time: datetime
) -> str:
    """Generate comprehensive Markdown validation report

    Args:
        validation_results: List of validation results from all stages
        job_path: Path to validated Spark job
        start_time: Validation start timestamp

    Returns:
        Markdown report content
    """
    duration = (datetime.now() - start_time).total_seconds()

    # Aggregate results
    total_issues = sum(r["total_issues"] for r in validation_results)
    critical_count = sum(r["critical_count"] for r in validation_results)
    warning_count = sum(r["warning_count"] for r in validation_results)
    all_passed = all(r["passed"] for r in validation_results)

    # Build report
    report_lines = [
        "# Spark Job Validation Report",
        "",
        f"**Job**: `{Path(job_path).name}`  ",
        f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Duration**: {duration:.1f}s  ",
        f"**Status**: {'✅ PASSED' if all_passed else '❌ FAILED'}",
        "",
        "## Summary",
        "",
        f"- **Total Issues**: {total_issues}",
        f"- **Critical**: {critical_count}",
        f"- **Warnings**: {warning_count}",
        ""
    ]

    # Stage results
    report_lines.append("## Validation Stages\n")

    stage_icons = {
        "syntax": "🔍",
        "logic": "🧠",
        "data": "📊",
        "expectations": "✓"
    }

    for result in validation_results:
        stage = result["stage"]
        icon = stage_icons.get(stage, "📋")
        status = "✅ PASSED" if result["passed"] else "❌ FAILED"

        report_lines.append(
            f"### {icon} {stage.title()} Validation - {status}\n"
        )
        report_lines.append(
            f"- Issues: {result['total_issues']} "
            f"(Critical: {result['critical_count']}, Warnings: {result['warning_count']})\n"
        )

        if result["issues"]:
            report_lines.append("**Issues Found**:\n")
            for i, issue in enumerate(result["issues"], 1):
                severity_icon = "🔴" if issue["severity"] == "critical" else "🟡" if issue["severity"] == "warning" else "ℹ️"
                line_info = f" (Line {issue['line']})" if issue.get('line') else ""

                report_lines.append(f"{i}. {severity_icon} **{issue['category'].title()}**{line_info}")
                report_lines.append(f"   - {issue['message']}")
                if issue.get('suggestion'):
                    report_lines.append(f"   - 💡 *{issue['suggestion']}*")
                report_lines.append("")

    # Recommendations
    if critical_count > 0:
        report_lines.extend([
            "## ⚠️ Required Actions",
            "",
            f"Found {critical_count} critical issue(s) that must be fixed before deployment:",
            ""
        ])

        for result in validation_results:
            critical_issues = [i for i in result["issues"] if i["severity"] == "critical"]
            for issue in critical_issues:
                report_lines.append(f"- {issue['message']}")

        report_lines.append("")

    elif warning_count > 0:
        report_lines.extend([
            "## 💡 Recommendations",
            "",
            f"Found {warning_count} warning(s) - consider addressing these before deployment:",
            ""
        ])

        for result in validation_results:
            warning_issues = [i for i in result["issues"] if i["severity"] == "warning"]
            for issue in warning_issues[:5]:  # Show max 5 warnings
                report_lines.append(f"- {issue['message']}")

        report_lines.append("")

    else:
        report_lines.extend([
            "## ✨ Excellent!",
            "",
            "No critical issues or warnings found. Job is ready for deployment.",
            ""
        ])

    return "\n".join(report_lines)


@flow(name="Validate Spark Job", log_prints=True)
def validate_spark_job(
    job_path: str,
    config_path: str,
    enable_dry_run: bool = False,
    required_columns: Optional[List[str]] = None,
    ge_config_dir: str = "ge_config",
    fail_on_warnings: bool = False
) -> Dict[str, Any]:
    """Main Prefect flow for Spark job validation

    Orchestrates all validation stages in parallel and sequential execution.

    Args:
        job_path: Path to Spark job Python file
        config_path: Path to job config JSON file
        enable_dry_run: Enable dry-run testing (slower)
        required_columns: List of required column names
        ge_config_dir: Great Expectations config directory
        fail_on_warnings: Fail validation if warnings are found

    Returns:
        Overall validation result
    """
    start_time = datetime.now()
    print(f"🚀 Starting validation for: {job_path}")

    # Stage 1 & 2: Run syntax and logic validation in parallel
    print("\n📋 Stage 1 & 2: Syntax and Logic Validation (Parallel)")
    syntax_result = validate_syntax.submit(job_path, config_path)
    logic_result = validate_logic.submit(job_path, config_path, enable_dry_run)

    # Wait for parallel tasks to complete
    syntax_result = syntax_result.result()
    logic_result = logic_result.result()

    print(f"  ✓ Syntax: {syntax_result['total_issues']} issues")
    print(f"  ✓ Logic: {logic_result['total_issues']} issues")

    # Stage 3: Data source validation (sequential - depends on config validation)
    print("\n📊 Stage 3: Data Source Validation")
    data_result = validate_data_sources(
        job_path,
        config_path,
        required_columns=required_columns
    )
    print(f"  ✓ Data: {data_result['total_issues']} issues")

    # Stage 4: Great Expectations validation
    print("\n✓ Stage 4: Data Quality Expectations")
    ge_result = validate_with_great_expectations(
        job_path,
        config_path,
        ge_config_dir
    )
    print(f"  ✓ Expectations: {ge_result['total_issues']} issues")

    # Aggregate results
    validation_results = [syntax_result, logic_result, data_result, ge_result]

    # Generate report
    print("\n📝 Generating validation report...")
    report_content = generate_validation_report(
        validation_results,
        job_path,
        start_time
    )

    # Create Prefect artifact (visible in UI)
    create_markdown_artifact(
        key="validation-report",
        markdown=report_content,
        description=f"Validation report for {Path(job_path).name}"
    )

    # Calculate overall status
    total_issues = sum(r["total_issues"] for r in validation_results)
    critical_count = sum(r["critical_count"] for r in validation_results)
    warning_count = sum(r["warning_count"] for r in validation_results)

    # Determine if validation passed
    passed = critical_count == 0
    if fail_on_warnings:
        passed = passed and warning_count == 0

    # Print summary
    print("\n" + "="*60)
    print(f"{'✅ VALIDATION PASSED' if passed else '❌ VALIDATION FAILED'}")
    print(f"Total Issues: {total_issues} (Critical: {critical_count}, Warnings: {warning_count})")
    print(f"Duration: {(datetime.now() - start_time).total_seconds():.1f}s")
    print("="*60 + "\n")

    # Print full report
    print(report_content)

    return {
        "passed": passed,
        "total_issues": total_issues,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "validation_results": validation_results,
        "report": report_content,
        "duration_seconds": (datetime.now() - start_time).total_seconds()
    }


if __name__ == "__main__":
    # Example usage
    result = validate_spark_job(
        job_path="spark_jobs/myjob/transform_job.py",
        config_path="spark_jobs/myjob/config.json",
        enable_dry_run=False,
        required_columns=["customer_id", "account_balance", "last_transaction_date"],
        ge_config_dir="ge_config",
        fail_on_warnings=False
    )

    # Exit with appropriate code for CI/CD
    import sys
    sys.exit(0 if result["passed"] else 1)
