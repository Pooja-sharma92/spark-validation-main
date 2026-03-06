#!/usr/bin/env python3
"""
Intelligent Code Analyzer (Claude Code Style)

AI-driven analysis that decides what and how deeply to analyze.
Philosophy: Let AI make strategic decisions to optimize cost and accuracy.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from core.static_analyzer import StaticAnalyzer
from validator.ai.intelligent_planner import IntelligentPlanner, AnalysisDepth, IntelligentAnalysisPlan
from validator.ai.llm_analyzer import LLMAnalyzer
from reporting.report_generator import ReportGenerator


@dataclass
class AnalysisResult:
    """Complete analysis result"""
    file_path: str
    plan: IntelligentAnalysisPlan
    static_results: Any
    ai_results: Optional[Any]
    report_file: str
    actual_cost: float
    actual_time: int


class IntelligentAnalyzer:
    """
    AI-driven code analyzer (Claude Code style)

    Workflow:
    1. AI Quick Assessment (always) - "Does this need deep analysis?"
    2. Selective Analysis (conditional) - Based on AI's decision
    3. Report Generation - Tailored to depth
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.static_analyzer = StaticAnalyzer(config)

        # Initialize AI components
        ai_provider_config = config.get('ai_provider', {})
        provider = ai_provider_config.get('type', 'openai')
        model = ai_provider_config.get('model', 'gpt-4')

        self.intelligent_planner = IntelligentPlanner(
            ai_provider=provider,
            model=model
        )

        try:
            self.ai_analyzer = LLMAnalyzer(config)
            self.ai_available = True
        except Exception as e:
            print(f"⚠️  AI deep analyzer unavailable: {str(e)}")
            self.ai_available = False

        self.report_generator = ReportGenerator(config)

    def analyze(self, file_path: str, verbose: bool = True) -> AnalysisResult:
        """
        Intelligent analysis workflow

        Stage 1: AI Assessment (always)
        Stage 2: Targeted Analysis (conditional)
        Stage 3: Report Generation
        """

        if verbose:
            print(f"\n{'='*70}")
            print(f"🤖 Intelligent Code Analysis")
            print(f"{'='*70}")
            print(f"File: {file_path}\n")

        # Read code
        with open(file_path, 'r') as f:
            code = f.read()

        import time
        start_time = time.time()

        # ===== STAGE 1: AI ASSESSMENT =====
        if verbose:
            print("📋 Stage 1: AI Assessment")
            print("  ↳ AI analyzing code to determine strategy...")

        plan = self.intelligent_planner.assess_and_plan(code, file_path)

        if verbose:
            print(f"  ✓ Assessment complete\n")
            self._print_plan(plan)

        # ===== STAGE 2: TARGETED ANALYSIS =====
        static_results = None
        ai_results = None

        if plan.recommended_depth == AnalysisDepth.QUICK_SCAN:
            if verbose:
                print("\n🔍 Stage 2: Quick Scan")
                print("  ↳ Running static analysis only (AI determined deep analysis not needed)...")

            static_results = self.static_analyzer.analyze_file(file_path)

            if verbose:
                print(f"  ✓ Found {len(static_results.issues)} issues")
                print(f"  💰 Saved ~$0.10 by skipping deep analysis\n")

        elif plan.recommended_depth == AnalysisDepth.MODERATE:
            if verbose:
                print("\n🔍 Stage 2: Moderate Analysis")
                print("  ↳ Static analysis + AI on priority areas...")

            # Static analysis
            static_results = self.static_analyzer.analyze_file(file_path)

            if verbose:
                print(f"    ✓ Static: {len(static_results.issues)} issues found")

            # AI analysis on priority areas only
            if self.ai_available and plan.priority_areas:
                if verbose:
                    print(f"    ↳ AI analyzing {len(plan.priority_areas)} priority areas...")

                ai_results = self.ai_analyzer.analyze_code(code, file_path, static_results)

                if verbose:
                    print(f"    ✓ AI analysis complete\n")

        else:  # DEEP
            if verbose:
                print("\n🔍 Stage 2: Deep Analysis")
                print("  ↳ Full static + AI analysis (AI determined this code needs thorough review)...")

            # Full static analysis
            static_results = self.static_analyzer.analyze_file(file_path)

            if verbose:
                print(f"    ✓ Static: {len(static_results.issues)} issues found")

            # Full AI analysis
            if self.ai_available:
                if verbose:
                    print(f"    ↳ AI performing deep analysis...")

                ai_results = self.ai_analyzer.analyze_code(code, file_path, static_results)

                if verbose:
                    print(f"    ✓ AI analysis complete\n")

        # ===== STAGE 3: REPORT GENERATION =====
        if verbose:
            print("📄 Stage 3: Report Generation")
            print("  ↳ Generating analysis report...")

        report_content = self._generate_intelligent_report(
            file_path,
            plan,
            static_results,
            ai_results
        )

        # Save report
        output_file = self.report_generator.save_report(report_content, file_path)

        end_time = time.time()
        actual_time = int(end_time - start_time)

        if verbose:
            print(f"  ✓ Report saved: {output_file}\n")
            print(f"{'='*70}")
            print(f"✅ Analysis Complete")
            print(f"{'='*70}")
            print(f"Time: {actual_time}s (estimated: {plan.estimated_time}s)")
            print(f"Depth: {plan.recommended_depth.value}")
            print(f"Report: {output_file}\n")

        return AnalysisResult(
            file_path=file_path,
            plan=plan,
            static_results=static_results,
            ai_results=ai_results,
            report_file=output_file,
            actual_cost=plan.estimated_cost,
            actual_time=actual_time
        )

    def _print_plan(self, plan: IntelligentAnalysisPlan):
        """Print AI's analysis plan (Claude Code style)"""

        # Color codes for terminal
        BOLD = '\033[1m'
        BLUE = '\033[94m'
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        RESET = '\033[0m'

        # Risk color
        risk_color = {
            'low': GREEN,
            'medium': YELLOW,
            'high': RED,
            'critical': RED + BOLD
        }.get(plan.risk_level, RESET)

        print(f"{BOLD}AI Decision:{RESET}")
        print(f"  Depth: {BLUE}{plan.recommended_depth.value.upper()}{RESET}")
        print(f"  Risk: {risk_color}{plan.risk_level.upper()}{RESET}")
        print(f"  Type: {plan.code_type}")
        print(f"  Complexity: {plan.complexity_level}")
        print(f"  Reasoning: {plan.reasoning}")

        if plan.priority_areas:
            print(f"\n{BOLD}Priority Areas:{RESET}")
            for i, area in enumerate(plan.priority_areas[:5], 1):
                print(f"  {i}. {area[:80]}...")

        if plan.skip_areas:
            print(f"\n{GREEN}Skipping:{RESET}")
            for area in plan.skip_areas[:3]:
                print(f"  • {area[:60]}...")

        print(f"\n{BOLD}Estimate:{RESET}")
        print(f"  Cost: ${plan.estimated_cost:.2f}")
        print(f"  Time: ~{plan.estimated_time}s")
        print(f"  Expected: {plan.estimated_issue_count.get('critical', 0)} critical, "
              f"{plan.estimated_issue_count.get('warning', 0)} warnings")

    def _generate_intelligent_report(
        self,
        file_path: str,
        plan: IntelligentAnalysisPlan,
        static_results: Any,
        ai_results: Optional[Any]
    ) -> str:
        """Generate report with AI decision context"""

        # Start with standard report
        report = self.report_generator.generate_report(
            file_path,
            static_results,
            ai_results
        )

        # Add AI decision section at the top
        ai_section = f"""
## 🤖 AI Analysis Strategy

**Analysis Depth**: `{plan.recommended_depth.value.upper()}`

**AI's Assessment**:
- **Code Type**: {plan.code_type}
- **Complexity**: {plan.complexity_level}
- **Risk Level**: {plan.risk_level.upper()}
- **Decision Reasoning**: {plan.reasoning}

**Cost & Time**:
- Estimated Cost: ${plan.estimated_cost:.2f}
- Analysis Time: ~{plan.estimated_time}s

"""

        if plan.recommended_depth == AnalysisDepth.QUICK_SCAN:
            ai_section += """
**Why Quick Scan?**
AI determined this code is straightforward and low-risk. A quick static analysis
is sufficient. This saved ~$0.10 in analysis costs while still catching issues.

"""
        elif plan.recommended_depth == AnalysisDepth.MODERATE:
            ai_section += f"""
**Priority Areas** (AI-selected for deep analysis):
"""
            for i, area in enumerate(plan.priority_areas, 1):
                ai_section += f"{i}. {area}\n"

            if plan.skip_areas:
                ai_section += f"""
**Skipped Areas** (AI determined these are acceptable):
"""
                for area in plan.skip_areas:
                    ai_section += f"- {area}\n"

        else:  # DEEP
            ai_section += """
**Why Deep Analysis?**
AI identified significant complexity, risk factors, or potential issues that warrant
thorough analysis. This code received full static + AI review.

"""

        # Insert AI section after title
        parts = report.split('\n---\n', 1)
        if len(parts) == 2:
            return parts[0] + '\n---\n' + ai_section + '\n---\n' + parts[1]

        return ai_section + '\n---\n' + report


def main():
    """CLI entry point"""
    import argparse
    import yaml

    parser = argparse.ArgumentParser(
        description="Intelligent Code Analyzer (AI-driven)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # AI decides analysis strategy
  python intelligent_analyzer.py analyze job.py

  # Verbose output
  python intelligent_analyzer.py analyze job.py -v

  # Use custom config
  python intelligent_analyzer.py analyze job.py -c config.yaml

The analyzer uses AI to determine:
- Does this code need deep analysis?
- Which parts need attention?
- What's the optimal cost/accuracy trade-off?
"""
    )

    subparsers = parser.add_subparsers(dest='command')

    analyze_parser = subparsers.add_parser('analyze', help='Analyze code file')
    analyze_parser.add_argument('file', help='File to analyze')
    analyze_parser.add_argument('-c', '--config', help='Config file')
    analyze_parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Load config
    if args.config and Path(args.config).exists():
        with open(args.config) as f:
            config = yaml.safe_load(f)
    else:
        config_path = Path(__file__).parent / 'config.yaml'
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f)
        else:
            # Minimal default config
            config = {
                'ai_provider': {
                    'type': os.getenv('AI_PROVIDER', 'openai'),
                    'model': os.getenv('AI_MODEL', 'gpt-4')
                },
                'reporting': {
                    'output_format': 'markdown',
                    'output_path': './analysis_reports'
                }
            }

    # Expand env vars in config
    def expand_env_vars(obj):
        if isinstance(obj, str):
            import re
            pattern = r'\$\{(\w+)\}'
            matches = re.findall(pattern, obj)
            for var in matches:
                obj = obj.replace(f'${{{var}}}', os.getenv(var, ''))
        elif isinstance(obj, dict):
            for k, v in obj.items():
                obj[k] = expand_env_vars(v)
        elif isinstance(obj, list):
            obj = [expand_env_vars(item) for item in obj]
        return obj

    config = expand_env_vars(config)

    # Run analysis
    analyzer = IntelligentAnalyzer(config)
    result = analyzer.analyze(args.file, verbose=args.verbose)

    # Print summary
    if not args.verbose:
        print(f"\n✅ Analysis complete: {result.report_file}")
        print(f"Depth: {result.plan.recommended_depth.value}")
        print(f"Issues: {len(result.static_results.issues)}")


if __name__ == "__main__":
    main()
