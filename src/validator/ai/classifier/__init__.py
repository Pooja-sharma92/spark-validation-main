"""
AI Classifier Module.

Provides AI-powered job classification for Spark jobs:
- Domain classification (Finance, Customer, Risk, etc.)
- Module classification (Loan Processing, Account Management, etc.)
- Job Group classification (ETL, Analytics, Reporting, etc.)
- Complexity assessment (Low, Medium, High)
"""

from .models import (
    Category,
    CategoryType,
    Complexity,
    ComplexityMetrics,
    JobClassification,
    ClassificationResult,
    ClassificationBatch,
    BatchStatus,
    SuggestedCategory,
    SuggestionStatus,
)
from .classifier import LLMClassifier, ClassificationError
from .prompts import ClassificationPrompts
from .category_manager import CategoryManager
from .batch_processor import BatchProcessor

__all__ = [
    # Models
    'Category',
    'CategoryType',
    'Complexity',
    'ComplexityMetrics',
    'JobClassification',
    'ClassificationResult',
    'ClassificationBatch',
    'BatchStatus',
    'SuggestedCategory',
    'SuggestionStatus',
    # Core
    'LLMClassifier',
    'ClassificationError',
    'ClassificationPrompts',
    'CategoryManager',
    'BatchProcessor',
]
