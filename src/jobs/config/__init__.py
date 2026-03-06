"""
Configuration package for STG_XTC_LD_TEST_JOB

This package contains:
- job_config_schema: Pydantic models for configuration validation
- YAML configuration files (base.yaml, dev.yaml, test.yaml, prod.yaml)
"""

from .job_config_schema import JobConfig

__all__ = ['JobConfig']
