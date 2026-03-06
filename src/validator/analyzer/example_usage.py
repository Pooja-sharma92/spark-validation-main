#!/usr/bin/env python3
"""
Example Usage of Spark Code Analyzer
Demonstrates different ways to use the analyzer
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from main import CodeAnalyzer
from core.static_analyzer import StaticAnalyzer
from reporting.report_generator import ReportGenerator


def example_1_basic_static_analysis():
    """Example 1: Basic static analysis without AI"""
    print("\n" + "="*60)
    print("Example 1: Basic Static Analysis (No AI)")
    print("="*60)

    # Create analyzer with minimal config
    config = {
        'analysis': {'modules': {'ai_deep_analysis': False}},
        'reporting': {'output_format': 'markdown', 'output_path': './reports'}
    }

    analyzer = CodeAnalyzer()
    analyzer.config = config
    analyzer.ai_enabled = False

    # Analyze the test job
    result = analyzer.analyze_file('../jobs/STG_XTC_LD_TEST_JOB.py', verbose=True)

    # Print key findings
    static = result['static_results']
    print(f"\n📊 Analysis Summary:")
    print(f"  Total Issues: {len(static.issues)}")
    print(f"  Critical: {static.summary.get('critical', 0)}")
    print(f"  Warnings: {static.summary.get('warning', 0)}")

    # Show first 3 critical issues
    critical_issues = [i for i in static.issues if i.severity == 'critical']
    if critical_issues:
        print(f"\n🚨 Critical Issues Found:")
        for idx, issue in enumerate(critical_issues[:3], 1):
            print(f"\n  {idx}. Line {issue.line}: {issue.message}")
            print(f"     💡 {issue.suggestion}")

    return result


def example_2_ai_analysis():
    """Example 2: Full analysis with AI (requires API key)"""
    print("\n" + "="*60)
    print("Example 2: AI-Powered Deep Analysis")
    print("="*60)

    try:
        # Create analyzer with AI enabled
        analyzer = CodeAnalyzer(config_path='config.yaml')

        if not analyzer.ai_enabled:
            print("⚠️  AI analysis not available (check API key configuration)")
            return None

        # Analyze with AI
        result = analyzer.analyze_file('../jobs/STG_XTC_LD_TEST_JOB.py', verbose=True)

        # Print AI insights
        if result.get('ai_results'):
            ai = result['ai_results']

            print(f"\n🤖 AI Analysis Summary:")
            print(ai.summary)

            print(f"\n📊 Quality Scores:")
            for dimension, score in ai.code_quality_score.items():
                emoji = "🟢" if score >= 7 else "🟡" if score >= 5 else "🔴"
                print(f"  {emoji} {dimension.title()}: {score}/10")

            print(f"\n⚡ Top Performance Insights:")
            for idx, insight in enumerate(ai.performance_insights[:3], 1):
                print(f"  {idx}. {insight}")

        return result

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None


def example_3_custom_config():
    """Example 3: Using custom configuration"""
    print("\n" + "="*60)
    print("Example 3: Custom Configuration")
    print("="*60)

    # Create custom config
    custom_config = {
        'analysis': {
            'modules': {
                'static_analysis': True,
                'pattern_detection': True,
                'ai_deep_analysis': False  # Disable AI for faster analysis
            },
            'thresholds': {
                'critical_complexity': 10,  # More strict
                'max_function_lines': 50
            }
        },
        'reporting': {
            'output_format': 'json',  # Generate JSON instead of Markdown
            'output_path': './custom_reports',
            'include_code_snippets': True
        }
    }

    # Use static analyzer directly
    static_analyzer = StaticAnalyzer(custom_config)
    result = static_analyzer.analyze_file('../jobs/STG_XTC_LD_TEST_JOB.py')

    print(f"✓ Analysis complete")
    print(f"  Issues: {len(result.issues)}")
    print(f"  Metrics: {result.metrics}")

    # Generate JSON report
    report_gen = ReportGenerator(custom_config)
    report = report_gen.generate_report('../jobs/STG_XTC_LD_TEST_JOB.py', result)

    print(f"\n📄 JSON Report Preview:")
    print(report[:500] + "...")

    return result


def example_4_programmatic_filtering():
    """Example 4: Programmatically filter and analyze results"""
    print("\n" + "="*60)
    print("Example 4: Programmatic Result Filtering")
    print("="*60)

    analyzer = CodeAnalyzer()
    analyzer.ai_enabled = False

    result = analyzer.analyze_file('../jobs/STG_XTC_LD_TEST_JOB.py', verbose=False)
    static = result['static_results']

    # Filter issues by category
    security_issues = [i for i in static.issues if i.category == 'security']
    performance_issues = [i for i in static.issues if i.category == 'performance']
    quality_issues = [i for i in static.issues if i.category == 'quality']

    print(f"\n📊 Issues by Category:")
    print(f"  🔒 Security: {len(security_issues)}")
    print(f"  ⚡ Performance: {len(performance_issues)}")
    print(f"  🎨 Quality: {len(quality_issues)}")

    # Find issues in specific line ranges
    print(f"\n📍 Issues in lines 100-200:")
    for issue in static.issues:
        if 100 <= issue.line <= 200:
            print(f"  Line {issue.line}: {issue.message[:60]}...")

    # Calculate custom metrics
    print(f"\n📈 Custom Metrics:")
    print(f"  Issue density: {len(static.issues) / static.metrics['total_lines']:.2%}")
    print(f"  Comment ratio: {static.metrics['comment_lines'] / static.metrics['code_lines']:.2%}")

    return result


def example_5_batch_analysis():
    """Example 5: Batch analyze multiple files"""
    print("\n" + "="*60)
    print("Example 5: Batch Analysis")
    print("="*60)

    analyzer = CodeAnalyzer()
    analyzer.ai_enabled = False  # Disable AI for faster batch processing

    # Analyze all Python files in jobs directory
    jobs_dir = Path('../jobs')
    results = {}

    for py_file in jobs_dir.glob('*.py'):
        print(f"\nAnalyzing {py_file.name}...")
        try:
            result = analyzer.analyze_file(str(py_file), verbose=False)
            results[py_file.name] = result['static_results']
            print(f"  ✓ Found {len(result['static_results'].issues)} issues")
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")

    # Aggregate statistics
    total_issues = sum(len(r.issues) for r in results.values())
    total_critical = sum(r.summary.get('critical', 0) for r in results.values())

    print(f"\n📊 Batch Analysis Summary:")
    print(f"  Files analyzed: {len(results)}")
    print(f"  Total issues: {total_issues}")
    print(f"  Critical issues: {total_critical}")

    # Find file with most issues
    if results:
        worst_file = max(results.items(), key=lambda x: len(x[1].issues))
        print(f"  File with most issues: {worst_file[0]} ({len(worst_file[1].issues)} issues)")

    return results


def example_6_integration_with_ci():
    """Example 6: CI/CD integration pattern"""
    print("\n" + "="*60)
    print("Example 6: CI/CD Integration Pattern")
    print("="*60)

    analyzer = CodeAnalyzer()
    analyzer.ai_enabled = False

    result = analyzer.analyze_file('../jobs/STG_XTC_LD_TEST_JOB.py', verbose=False)
    static = result['static_results']

    # Define quality gates
    CRITICAL_THRESHOLD = 0  # No critical issues allowed
    WARNING_THRESHOLD = 10  # Max 10 warnings

    critical_count = static.summary.get('critical', 0)
    warning_count = static.summary.get('warning', 0)

    print(f"\n🚦 Quality Gates:")
    print(f"  Critical issues: {critical_count}/{CRITICAL_THRESHOLD}")
    print(f"  Warnings: {warning_count}/{WARNING_THRESHOLD}")

    # Determine pass/fail
    if critical_count > CRITICAL_THRESHOLD:
        print(f"\n❌ FAILED: {critical_count} critical issues found")
        sys.exit(1)
    elif warning_count > WARNING_THRESHOLD:
        print(f"\n⚠️  WARNING: {warning_count} warnings (threshold: {WARNING_THRESHOLD})")
        sys.exit(0)  # Pass but with warning
    else:
        print(f"\n✅ PASSED: Code meets quality standards")
        sys.exit(0)


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("  Spark Code Analyzer - Usage Examples")
    print("="*70)

    examples = [
        ("Basic Static Analysis", example_1_basic_static_analysis),
        ("AI-Powered Analysis", example_2_ai_analysis),
        ("Custom Configuration", example_3_custom_config),
        ("Result Filtering", example_4_programmatic_filtering),
        ("Batch Analysis", example_5_batch_analysis),
        # Note: Example 6 calls sys.exit, so comment it out for demo
        # ("CI/CD Integration", example_6_integration_with_ci),
    ]

    for name, func in examples:
        try:
            func()
            input(f"\n➡️  Press Enter to continue to next example...")
        except KeyboardInterrupt:
            print("\n\n👋 Examples interrupted by user")
            break
        except Exception as e:
            print(f"\n❌ Error in {name}: {str(e)}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*70)
    print("  All examples completed!")
    print("="*70)


if __name__ == "__main__":
    main()
