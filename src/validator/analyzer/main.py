#!/usr/bin/env python3
"""
Spark Code Analyzer - Main Entry Point
Orchestrates static analysis, AI analysis, and report generation
"""

import argparse
import sys
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load from project root .env
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)
    print(f"✓ Loaded environment from: {env_path}")
except ImportError:
    print("⚠️  python-dotenv not installed. Using system environment variables only.")
    print("   Install: pip install python-dotenv")

from core.static_analyzer import StaticAnalyzer
from validator.ai.llm_analyzer import LLMAnalyzer
from validator.ai.analysis_planner import AnalysisPlanner
from reporting.report_generator import ReportGenerator


class CodeAnalyzer:
    """Main orchestrator for code analysis"""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize analyzer with configuration"""
        self.config = self._load_config(config_path)
        self.static_analyzer = StaticAnalyzer(self.config)

        # Initialize AI analyzer if enabled
        if self.config.get('analysis', {}).get('modules', {}).get('ai_deep_analysis', False):
            try:
                self.ai_analyzer = LLMAnalyzer(self.config)
                self.ai_enabled = True
            except Exception as e:
                print(f"Warning: AI analysis disabled - {str(e)}")
                self.ai_enabled = False
        else:
            self.ai_enabled = False

        self.report_generator = ReportGenerator(self.config)

    def _load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if config_path is None:
            # Try to find config in current directory or analyzer directory
            config_path = Path(__file__).parent / 'config.yaml'

        if not Path(config_path).exists():
            print(f"Warning: Config file not found at {config_path}, using defaults")
            return self._get_default_config()

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Expand environment variables in config
        self._expand_env_vars(config)

        return config

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'analysis': {
                'modules': {
                    'static_analysis': True,
                    'ai_deep_analysis': False
                }
            },
            'reporting': {
                'output_format': 'markdown',
                'output_path': './analysis_reports'
            }
        }

    def _expand_env_vars(self, config: Dict[str, Any]):
        """Recursively expand environment variables in config"""
        import os
        import re

        def expand_value(value):
            if isinstance(value, str):
                # Replace ${VAR_NAME} with environment variable
                pattern = r'\$\{(\w+)\}'
                matches = re.findall(pattern, value)
                for var in matches:
                    env_value = os.getenv(var, '')
                    value = value.replace(f'${{{var}}}', env_value)
            elif isinstance(value, dict):
                for k, v in value.items():
                    value[k] = expand_value(v)
            elif isinstance(value, list):
                value = [expand_value(item) for item in value]
            return value

        for key, value in config.items():
            config[key] = expand_value(value)

    def analyze_file(self, file_path: str, verbose: bool = False, use_ai_planning: bool = False) -> Dict[str, Any]:
        """
        Analyze a single Python file

        Args:
            file_path: Path to file to analyze
            verbose: Print progress
            use_ai_planning: Use AI to create analysis plan first (2-phase analysis)
        """
        if verbose:
            print(f"\n{'='*60}")
            print(f"Analyzing: {file_path}")
            print(f"{'='*60}\n")

        # Read code once
        with open(file_path, 'r') as f:
            code = f.read()

        analysis_plan = None

        # === PHASE 1: AI Planning (Optional) ===
        if use_ai_planning and self.ai_enabled:
            if verbose:
                print("🤖 Phase 1: AI creating analysis plan...")

            try:
                planner = AnalysisPlanner(
                    ai_provider=self.config.get('ai_provider', {}).get('type', 'openai'),
                    model=self.config.get('ai_provider', {}).get('model', 'gpt-4')
                )
                analysis_plan = planner.create_analysis_plan(code, file_path)

                if verbose:
                    print(f"  ✓ Analysis plan created")
                    print(f"  📋 Priority areas: {len(analysis_plan.priority_areas)}")
                    print(f"  🎯 Expected issues: {len(analysis_plan.expected_issues)}")
                    print(f"\n  Priority Areas:")
                    for i, area in enumerate(analysis_plan.priority_areas[:3], 1):
                        print(f"    {i}. {area[:80]}...")

            except Exception as e:
                print(f"  ⚠️  AI planning failed: {str(e)}")
                print(f"  ↳  Continuing with standard analysis...")

        # === PHASE 2: Detailed Analysis ===
        if verbose:
            if use_ai_planning:
                print(f"\n🔍 Phase 2: Executing detailed analysis...")
            else:
                print("🔍 Running analysis...")

        # Step 1: Static Analysis
        if verbose:
            print("  → Static analysis...")

        static_results = self.static_analyzer.analyze_file(file_path)

        if verbose:
            print(f"    ✓ Found {len(static_results.issues)} issues")
            print(f"    ✓ Calculated {len(static_results.metrics)} metrics")

        # Step 2: AI Deep Analysis (if enabled)
        ai_results = None
        if self.ai_enabled:
            if verbose:
                print("  → AI deep analysis...")

            try:
                ai_results = self.ai_analyzer.analyze_code(code, file_path, static_results)

                if verbose:
                    print("    ✓ AI analysis complete")

            except Exception as e:
                print(f"    ✗ AI analysis failed: {str(e)}")

        # Step 3: Generate Report
        if verbose:
            print("\n📄 Generating report...")

        report_content = self.report_generator.generate_report(
            file_path,
            static_results,
            ai_results
        )

        # Add analysis plan to report if available
        if analysis_plan:
            report_content = self._enhance_report_with_plan(report_content, analysis_plan)

        # Save report
        output_file = self.report_generator.save_report(report_content, file_path)

        if verbose:
            print(f"  ✓ Report saved to: {output_file}")

        return {
            'file_path': file_path,
            'analysis_plan': analysis_plan,
            'static_results': static_results,
            'ai_results': ai_results,
            'report_file': output_file,
            'report_content': report_content
        }

    def _enhance_report_with_plan(self, report: str, plan) -> str:
        """Add AI analysis plan to report"""
        plan_section = f"""

## 🧠 AI Analysis Plan

### Code Characteristics
This code was identified as having the following characteristics based on initial AI analysis.

### Priority Analysis Areas
{chr(10).join(f'{i}. {area}' for i, area in enumerate(plan.priority_areas, 1))}

### Analysis Strategies Applied
{chr(10).join(f'- {strategy}' for strategy in plan.analysis_strategies)}

### Expected Issues (AI Prediction)
{chr(10).join(f'- {issue}' for issue in plan.expected_issues)}

---

"""
        # Insert after title
        parts = report.split('\n---\n', 1)
        if len(parts) == 2:
            return parts[0] + '\n---\n' + plan_section + parts[1]
        return plan_section + report

    def analyze_directory(self, directory: str, pattern: str = "*.py", verbose: bool = False) -> Dict[str, Any]:
        """Analyze all Python files in a directory"""
        dir_path = Path(directory)
        py_files = list(dir_path.glob(pattern))

        if not py_files:
            print(f"No Python files found in {directory}")
            return {}

        if verbose:
            print(f"\nFound {len(py_files)} Python file(s) to analyze")

        results = {}
        for py_file in py_files:
            try:
                result = self.analyze_file(str(py_file), verbose=verbose)
                results[str(py_file)] = result
            except Exception as e:
                print(f"Error analyzing {py_file}: {str(e)}")
                results[str(py_file)] = {'error': str(e)}

        return results

    def print_summary(self, results: Dict[str, Any]):
        """Print summary of analysis results"""
        if isinstance(results, dict) and 'static_results' in results:
            # Single file result
            static = results['static_results']
            print("\n" + "="*60)
            print("ANALYSIS SUMMARY")
            print("="*60)
            print(f"File: {results['file_path']}")
            print(f"Total Issues: {len(static.issues)}")
            print(f"  - Critical: {static.summary.get('critical', 0)}")
            print(f"  - Warning: {static.summary.get('warning', 0)}")
            print(f"  - Info: {static.summary.get('info', 0)}")
            print(f"\nMetrics:")
            for key, value in static.metrics.items():
                print(f"  - {key}: {value}")

            if results.get('ai_results'):
                print(f"\nQuality Scores:")
                for dim, score in results['ai_results'].code_quality_score.items():
                    print(f"  - {dim.title()}: {score}/10")

            print(f"\nReport: {results['report_file']}")
        else:
            # Directory results
            total_files = len(results)
            total_issues = sum(
                len(r.get('static_results', {}).issues or [])
                for r in results.values()
                if not r.get('error')
            )
            print("\n" + "="*60)
            print("ANALYSIS SUMMARY")
            print("="*60)
            print(f"Files Analyzed: {total_files}")
            print(f"Total Issues: {total_issues}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Analyze Spark/Python code for bugs, performance, and quality issues",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a single file
  python main.py analyze jobs/STG_XTC_LD_TEST_JOB.py

  # Analyze all Python files in a directory
  python main.py analyze jobs/ --recursive

  # Use custom config
  python main.py analyze jobs/job.py --config my_config.yaml

  # Enable verbose output
  python main.py analyze jobs/job.py -v

  # Generate JSON report
  python main.py analyze jobs/job.py --format json
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze code file(s)')
    analyze_parser.add_argument('path', help='File or directory to analyze')
    analyze_parser.add_argument('-r', '--recursive', action='store_true', help='Recursively analyze directory')
    analyze_parser.add_argument('-c', '--config', help='Path to config file')
    analyze_parser.add_argument('-f', '--format', choices=['markdown', 'html', 'json'], help='Output format')
    analyze_parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    analyze_parser.add_argument('--no-ai', action='store_true', help='Disable AI analysis')
    analyze_parser.add_argument('--ai-plan', action='store_true', help='Use AI to create analysis plan first (2-phase analysis)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Initialize analyzer
    analyzer = CodeAnalyzer(config_path=args.config)

    # Override config from command line
    if args.format:
        analyzer.config['reporting']['output_format'] = args.format

    if args.no_ai:
        analyzer.ai_enabled = False

    # Execute command
    if args.command == 'analyze':
        path = Path(args.path)

        if path.is_file():
            results = analyzer.analyze_file(
                str(path),
                verbose=args.verbose,
                use_ai_planning=args.ai_plan
            )
        elif path.is_dir():
            pattern = "**/*.py" if args.recursive else "*.py"
            results = analyzer.analyze_directory(str(path), pattern=pattern, verbose=args.verbose)
        else:
            print(f"Error: {args.path} is not a valid file or directory")
            sys.exit(1)

        # Print summary
        analyzer.print_summary(results)


if __name__ == "__main__":
    main()
