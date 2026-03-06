"""
Report Generator
Generates comprehensive analysis reports in various formats
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path
import json


class ReportGenerator:
    """Generate structured analysis reports"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.output_path = Path(config.get('reporting', {}).get('output_path', './analysis_reports'))
        self.output_path.mkdir(parents=True, exist_ok=True)

    def generate_report(
        self,
        file_path: str,
        static_results: Any,
        ai_results: Any = None
    ) -> str:
        """Generate comprehensive analysis report"""

        output_format = self.config.get('reporting', {}).get('output_format', 'markdown')

        if output_format == 'markdown':
            return self._generate_markdown_report(file_path, static_results, ai_results)
        elif output_format == 'html':
            return self._generate_html_report(file_path, static_results, ai_results)
        elif output_format == 'json':
            return self._generate_json_report(file_path, static_results, ai_results)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    def _generate_markdown_report(
        self,
        file_path: str,
        static_results: Any,
        ai_results: Any = None
    ) -> str:
        """Generate Markdown report"""

        report = []

        # Header
        report.append(f"# Code Analysis Report")
        report.append(f"\n**File:** `{file_path}`")
        report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("\n---\n")

        # Executive Summary (if AI results available)
        if ai_results:
            report.append("## 📊 Executive Summary\n")
            report.append(ai_results.summary)
            report.append("\n")

            # Quality Score Card
            report.append("### Quality Score Card\n")
            report.append("| Dimension | Score | Rating |")
            report.append("|-----------|-------|--------|")
            for dimension, score in ai_results.code_quality_score.items():
                rating = self._get_rating_emoji(score)
                report.append(f"| {dimension.title()} | {score}/10 | {rating} |")
            report.append("\n")

        # Metrics Overview
        report.append("## 📈 Code Metrics\n")
        if static_results.metrics:
            report.append("| Metric | Value |")
            report.append("|--------|-------|")
            for key, value in static_results.metrics.items():
                report.append(f"| {key.replace('_', ' ').title()} | {value} |")
        report.append("\n")

        # Critical Issues
        critical_issues = [i for i in static_results.issues if i.severity == 'critical']
        if critical_issues:
            report.append("## 🚨 Critical Issues (Must Fix)\n")
            for idx, issue in enumerate(critical_issues, 1):
                report.append(f"### {idx}. {issue.message}\n")
                report.append(f"**Location:** Line {issue.line}, Column {issue.column}")
                report.append(f"**Category:** {issue.category}")
                if issue.code_snippet:
                    report.append(f"\n```python\n{issue.code_snippet}\n```\n")
                report.append(f"**💡 Suggestion:** {issue.suggestion}\n")
            report.append("\n")

        # Data Flow Analysis (if AI results available)
        if ai_results:
            report.append("## 🔄 Data Flow Analysis\n")
            report.append(ai_results.data_flow)
            report.append("\n")

            # Business Logic
            report.append("## 💼 Business Logic\n")
            report.append(ai_results.business_logic)
            report.append("\n")

        # Performance Analysis
        report.append("## ⚡ Performance Analysis\n")

        warning_issues = [i for i in static_results.issues if i.severity == 'warning' and i.category == 'performance']
        if warning_issues:
            report.append("### Issues Detected:\n")
            for issue in warning_issues:
                report.append(f"- **Line {issue.line}:** {issue.message}")
                if issue.suggestion:
                    report.append(f"  - 💡 {issue.suggestion}")
            report.append("\n")

        if ai_results and ai_results.performance_insights:
            report.append("### AI Insights:\n")
            for insight in ai_results.performance_insights:
                report.append(f"- {insight}")
            report.append("\n")

        # Security Analysis
        security_issues = [i for i in static_results.issues if i.category == 'security']
        if security_issues or (ai_results and ai_results.security_concerns):
            report.append("## 🔒 Security Analysis\n")

            if security_issues:
                report.append("### Detected Vulnerabilities:\n")
                for issue in security_issues:
                    report.append(f"- **[{issue.severity.upper()}] Line {issue.line}:** {issue.message}")
                    if issue.suggestion:
                        report.append(f"  - 🛡️ {issue.suggestion}")
                report.append("\n")

            if ai_results and ai_results.security_concerns:
                report.append("### AI Security Assessment:\n")
                for concern in ai_results.security_concerns:
                    report.append(f"- {concern}")
                report.append("\n")

        # Code Quality Issues
        quality_issues = [i for i in static_results.issues if i.category == 'quality']
        if quality_issues:
            report.append("## 🎨 Code Quality Issues\n")
            for issue in quality_issues:
                severity_emoji = {'critical': '🔴', 'warning': '🟡', 'info': '🔵'}.get(issue.severity, '⚪')
                report.append(f"- {severity_emoji} **Line {issue.line}:** {issue.message}")
                if issue.suggestion:
                    report.append(f"  - {issue.suggestion}")
            report.append("\n")

        # Refactoring Suggestions
        if ai_results and ai_results.refactoring_suggestions:
            report.append("## 🔧 Refactoring Opportunities\n")
            for idx, suggestion in enumerate(ai_results.refactoring_suggestions, 1):
                report.append(f"{idx}. {suggestion}")
            report.append("\n")

        # Summary Statistics
        report.append("## 📊 Issue Summary\n")
        report.append("| Severity | Count |")
        report.append("|----------|-------|")
        for severity, count in static_results.summary.items():
            report.append(f"| {severity.title()} | {count} |")
        report.append(f"| **Total** | **{len(static_results.issues)}** |")
        report.append("\n")

        # Action Plan
        report.append("## 🎯 Recommended Action Plan\n")
        report.append(self._generate_action_plan(static_results, ai_results))
        report.append("\n")

        # Footer
        report.append("---\n")
        report.append("*Generated by Spark Code Analyzer*")

        return "\n".join(report)

    def _generate_html_report(self, file_path: str, static_results: Any, ai_results: Any = None) -> str:
        """Generate HTML report"""
        # Convert markdown to HTML (simplified)
        md_report = self._generate_markdown_report(file_path, static_results, ai_results)

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Code Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .critical {{ color: #d32f2f; font-weight: bold; }}
        .warning {{ color: #f57c00; }}
        .info {{ color: #0288d1; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; }}
        pre {{ background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="container">
        <pre>{md_report}</pre>
    </div>
</body>
</html>
"""
        return html

    def _generate_json_report(self, file_path: str, static_results: Any, ai_results: Any = None) -> str:
        """Generate JSON report"""
        report = {
            'file_path': file_path,
            'generated_at': datetime.now().isoformat(),
            'metrics': static_results.metrics,
            'summary': static_results.summary,
            'issues': [
                {
                    'severity': issue.severity,
                    'category': issue.category,
                    'line': issue.line,
                    'column': issue.column,
                    'message': issue.message,
                    'suggestion': issue.suggestion,
                    'code_snippet': issue.code_snippet
                }
                for issue in static_results.issues
            ]
        }

        if ai_results:
            report['ai_analysis'] = {
                'business_logic': ai_results.business_logic,
                'data_flow': ai_results.data_flow,
                'performance_insights': ai_results.performance_insights,
                'security_concerns': ai_results.security_concerns,
                'refactoring_suggestions': ai_results.refactoring_suggestions,
                'quality_scores': ai_results.code_quality_score,
                'summary': ai_results.summary
            }

        return json.dumps(report, indent=2)

    def _get_rating_emoji(self, score: int) -> str:
        """Get emoji rating for score"""
        if score >= 8:
            return "🟢 Excellent"
        elif score >= 6:
            return "🟡 Good"
        elif score >= 4:
            return "🟠 Fair"
        else:
            return "🔴 Poor"

    def _generate_action_plan(self, static_results: Any, ai_results: Any = None) -> str:
        """Generate prioritized action plan"""
        actions = []

        # P0: Critical issues
        critical_count = static_results.summary.get('critical', 0)
        if critical_count > 0:
            actions.append(f"### Priority 0 (Immediate)\n")
            actions.append(f"- Fix {critical_count} critical issue(s) that prevent execution\n")

        # P1: Security issues
        security_count = len([i for i in static_results.issues if i.category == 'security'])
        if security_count > 0:
            actions.append(f"### Priority 1 (This Week)\n")
            actions.append(f"- Address {security_count} security vulnerability(ies)\n")

        # P2: Performance issues
        perf_count = len([i for i in static_results.issues if i.category == 'performance'])
        if perf_count > 0:
            actions.append(f"### Priority 2 (This Month)\n")
            actions.append(f"- Optimize {perf_count} performance issue(s)\n")
            if ai_results and ai_results.performance_insights:
                actions.append(f"- Focus on: {ai_results.performance_insights[0][:100]}...\n")

        # P3: Code quality
        quality_count = len([i for i in static_results.issues if i.category == 'quality'])
        if quality_count > 0:
            actions.append(f"### Priority 3 (Next Quarter)\n")
            actions.append(f"- Improve code quality: {quality_count} issue(s)\n")
            if ai_results and ai_results.refactoring_suggestions:
                actions.append(f"- Start with: {ai_results.refactoring_suggestions[0][:100]}...\n")

        return "\n".join(actions) if actions else "No major issues detected. Code is in good shape! 🎉"

    def save_report(self, report_content: str, file_path: str) -> str:
        """Save report to file"""
        output_format = self.config.get('reporting', {}).get('output_format', 'markdown')
        ext = {'markdown': 'md', 'html': 'html', 'json': 'json'}.get(output_format, 'txt')

        # Generate output filename
        file_stem = Path(file_path).stem
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = self.output_path / f"analysis_{file_stem}_{timestamp}.{ext}"

        # Save report
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)

        return str(output_file)


if __name__ == "__main__":
    # Test report generator
    from ..core.static_analyzer import StaticAnalyzer, AnalysisResult, Issue

    # Create test data
    test_result = AnalysisResult(file_path="test.py")
    test_result.add_issue(Issue(
        severity="critical",
        category="syntax",
        line=10,
        column=5,
        message="SQL syntax error",
        suggestion="Fix the WHERE clause"
    ))
    test_result.metrics = {'total_lines': 100, 'dataframes': 5}

    config = {'reporting': {'output_format': 'markdown', 'output_path': './reports'}}
    generator = ReportGenerator(config)

    report = generator.generate_report("test.py", test_result)
    print(report)
