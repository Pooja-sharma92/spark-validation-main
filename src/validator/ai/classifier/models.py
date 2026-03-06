"""
Data models for AI Job Classification
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4


class CategoryType(str, Enum):
    """Types of classification categories"""
    DOMAIN = "domain"
    MODULE = "module"
    JOB_GROUP = "job_group"


class Complexity(str, Enum):
    """Job complexity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class BatchStatus(str, Enum):
    """Classification batch status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SuggestionStatus(str, Enum):
    """Status of suggested categories"""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    MERGED = "merged"


@dataclass
class Category:
    """A classification category (domain, module, or job_group)"""
    id: UUID
    type: CategoryType
    name: str
    description: Optional[str] = None
    parent_id: Optional[UUID] = None
    ai_discovered: bool = False
    approved: bool = True
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "parent_id": str(self.parent_id) if self.parent_id else None,
            "ai_discovered": self.ai_discovered,
            "approved": self.approved,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Category":
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            type=CategoryType(data["type"]),
            name=data["name"],
            description=data.get("description"),
            parent_id=UUID(data["parent_id"]) if data.get("parent_id") else None,
            ai_discovered=data.get("ai_discovered", False),
            approved=data.get("approved", True),
            approved_by=data.get("approved_by"),
            approved_at=datetime.fromisoformat(data["approved_at"]) if data.get("approved_at") else None,
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else datetime.now(),
        )


@dataclass
class ComplexityMetrics:
    """Metrics used to determine job complexity"""
    lines_of_code: int = 0
    sql_queries: int = 0
    joins: int = 0
    aggregations: int = 0
    transformations: int = 0
    dataframes: int = 0
    source_tables: int = 0
    target_tables: int = 0
    udfs: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "lines_of_code": self.lines_of_code,
            "sql_queries": self.sql_queries,
            "joins": self.joins,
            "aggregations": self.aggregations,
            "transformations": self.transformations,
            "dataframes": self.dataframes,
            "source_tables": self.source_tables,
            "target_tables": self.target_tables,
            "udfs": self.udfs,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ComplexityMetrics":
        return cls(
            lines_of_code=data.get("lines_of_code", 0),
            sql_queries=data.get("sql_queries", 0),
            joins=data.get("joins", 0),
            aggregations=data.get("aggregations", 0),
            transformations=data.get("transformations", 0),
            dataframes=data.get("dataframes", 0),
            source_tables=data.get("source_tables", 0),
            target_tables=data.get("target_tables", 0),
            udfs=data.get("udfs", 0),
        )


@dataclass
class ClassificationResult:
    """Result from AI classification of a single job"""
    # Classification outputs
    domain: str
    module: str
    job_group: str
    complexity: Complexity
    complexity_score: int  # 0-100
    complexity_reasoning: str

    # Confidence
    confidence_score: float  # 0.0-1.0

    # Metrics used for classification
    metrics: ComplexityMetrics

    # Whether categories were found or are suggestions
    domain_exists: bool = True
    module_exists: bool = True
    job_group_exists: bool = True

    # Raw response for debugging
    raw_response: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "module": self.module,
            "job_group": self.job_group,
            "complexity": self.complexity.value,
            "complexity_score": self.complexity_score,
            "complexity_reasoning": self.complexity_reasoning,
            "confidence_score": self.confidence_score,
            "metrics": self.metrics.to_dict(),
            "domain_exists": self.domain_exists,
            "module_exists": self.module_exists,
            "job_group_exists": self.job_group_exists,
            "raw_response": self.raw_response,
        }


@dataclass
class JobClassification:
    """Stored classification for a job"""
    id: UUID
    job_path: str
    job_name: str

    # Category references (IDs if matched, None if suggested)
    domain_id: Optional[UUID] = None
    module_id: Optional[UUID] = None
    job_group_id: Optional[UUID] = None

    # Category names (for display)
    domain_name: Optional[str] = None
    module_name: Optional[str] = None
    job_group_name: Optional[str] = None

    # Complexity
    complexity: Optional[Complexity] = None
    complexity_score: Optional[int] = None
    complexity_reasoning: Optional[str] = None

    # Metrics
    metrics: Optional[ComplexityMetrics] = None

    # Confidence
    confidence_score: Optional[float] = None

    # Suggestions (when no existing category matches)
    suggested_domain: Optional[str] = None
    suggested_module: Optional[str] = None
    suggested_job_group: Optional[str] = None

    # AI info
    ai_provider: Optional[str] = None
    ai_model: Optional[str] = None
    batch_id: Optional[UUID] = None

    # Timestamps
    classified_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    # Raw response
    raw_response: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "job_path": self.job_path,
            "job_name": self.job_name,
            "domain_id": str(self.domain_id) if self.domain_id else None,
            "module_id": str(self.module_id) if self.module_id else None,
            "job_group_id": str(self.job_group_id) if self.job_group_id else None,
            "domain_name": self.domain_name,
            "module_name": self.module_name,
            "job_group_name": self.job_group_name,
            "complexity": self.complexity.value if self.complexity else None,
            "complexity_score": self.complexity_score,
            "complexity_reasoning": self.complexity_reasoning,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "confidence_score": self.confidence_score,
            "suggested_domain": self.suggested_domain,
            "suggested_module": self.suggested_module,
            "suggested_job_group": self.suggested_job_group,
            "ai_provider": self.ai_provider,
            "ai_model": self.ai_model,
            "batch_id": str(self.batch_id) if self.batch_id else None,
            "classified_at": self.classified_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @property
    def has_suggestions(self) -> bool:
        """Check if any category is a suggestion (not matched)"""
        return bool(self.suggested_domain or self.suggested_module or self.suggested_job_group)


@dataclass
class ClassificationBatch:
    """A batch classification run"""
    id: UUID
    name: Optional[str] = None
    status: BatchStatus = BatchStatus.PENDING

    # Scope
    directories: List[str] = field(default_factory=list)
    file_patterns: List[str] = field(default_factory=lambda: ["*.py"])
    exclude_patterns: List[str] = field(default_factory=list)

    # Progress
    total_jobs: int = 0
    processed_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0
    skipped_jobs: int = 0

    # AI provider
    ai_provider: Optional[str] = None
    ai_model: Optional[str] = None

    # Timing
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Error
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

    # Audit
    triggered_by: Optional[str] = None

    # Options
    options: Dict[str, Any] = field(default_factory=dict)

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage"""
        if self.total_jobs == 0:
            return 0.0
        return (self.processed_jobs / self.total_jobs) * 100

    @property
    def is_running(self) -> bool:
        return self.status == BatchStatus.RUNNING

    @property
    def is_complete(self) -> bool:
        return self.status in (BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.CANCELLED)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "status": self.status.value,
            "directories": self.directories,
            "file_patterns": self.file_patterns,
            "exclude_patterns": self.exclude_patterns,
            "total_jobs": self.total_jobs,
            "processed_jobs": self.processed_jobs,
            "successful_jobs": self.successful_jobs,
            "failed_jobs": self.failed_jobs,
            "skipped_jobs": self.skipped_jobs,
            "progress_percent": round(self.progress_percent, 1),
            "ai_provider": self.ai_provider,
            "ai_model": self.ai_model,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "triggered_by": self.triggered_by,
            "options": self.options,
        }


@dataclass
class SuggestedCategory:
    """An AI-suggested category pending human approval"""
    id: UUID
    type: CategoryType
    name: str
    description: Optional[str] = None
    parent_name: Optional[str] = None  # For context

    # Jobs that suggested this
    suggested_by_jobs: List[str] = field(default_factory=list)
    occurrence_count: int = 1

    # Timestamps
    first_suggested_at: datetime = field(default_factory=datetime.now)
    last_suggested_at: datetime = field(default_factory=datetime.now)

    # Review
    status: SuggestionStatus = SuggestionStatus.PENDING
    merged_into_id: Optional[UUID] = None
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    review_notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "parent_name": self.parent_name,
            "suggested_by_jobs": self.suggested_by_jobs,
            "occurrence_count": self.occurrence_count,
            "first_suggested_at": self.first_suggested_at.isoformat(),
            "last_suggested_at": self.last_suggested_at.isoformat(),
            "status": self.status.value,
            "merged_into_id": str(self.merged_into_id) if self.merged_into_id else None,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "review_notes": self.review_notes,
        }


@dataclass
class ClassificationRequest:
    """Request to classify a job"""
    job_path: str
    code: str
    batch_id: Optional[UUID] = None
    force_reclassify: bool = False


@dataclass
class BatchRequest:
    """Request to start a batch classification"""
    directories: List[str]
    name: Optional[str] = None
    file_patterns: List[str] = field(default_factory=lambda: ["*.py"])
    exclude_patterns: List[str] = field(default_factory=list)
    ai_provider: Optional[str] = None  # Use config default if None
    force_reclassify: bool = False
    triggered_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "directories": self.directories,
            "name": self.name,
            "file_patterns": self.file_patterns,
            "exclude_patterns": self.exclude_patterns,
            "ai_provider": self.ai_provider,
            "force_reclassify": self.force_reclassify,
            "triggered_by": self.triggered_by,
        }
