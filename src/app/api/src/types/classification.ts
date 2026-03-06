/**
 * TypeScript types for Job Classification API
 */

// ============================================================================
// Enums
// ============================================================================

export type CategoryType = 'domain' | 'module' | 'job_group';
export type Complexity = 'low' | 'medium' | 'high';
export type BatchStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
export type SuggestionStatus = 'pending' | 'approved' | 'rejected' | 'merged';

// ============================================================================
// Category Types
// ============================================================================

export interface Category {
  id: string;
  type: CategoryType;
  name: string;
  description?: string;
  parentId?: string;
  aiDiscovered: boolean;
  approved: boolean;
  approvedBy?: string;
  approvedAt?: string;
  metadata?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CategoryTreeNode extends Category {
  children: CategoryTreeNode[];
  level: number;
  path: string;
}

export interface CreateCategoryRequest {
  type: CategoryType;
  name: string;
  description?: string;
  parentId?: string;
}

export interface UpdateCategoryRequest {
  name?: string;
  description?: string;
  parentId?: string;
}

// ============================================================================
// Classification Types
// ============================================================================

export interface ComplexityMetrics {
  linesOfCode: number;
  sqlQueries: number;
  joins: number;
  aggregations: number;
  transformations: number;
  dataframes: number;
  sourceTables: number;
  targetTables: number;
  udfs: number;
}

export interface JobClassification {
  id: string;
  jobPath: string;
  jobName: string;

  // Category references
  domainId?: string;
  moduleId?: string;
  jobGroupId?: string;

  // Category names (for display)
  domainName?: string;
  moduleName?: string;
  jobGroupName?: string;

  // Complexity
  complexity?: Complexity;
  complexityScore?: number;
  complexityReasoning?: string;

  // Metrics
  metrics?: ComplexityMetrics;

  // Confidence
  confidenceScore?: number;

  // Suggestions (when no existing category matches)
  suggestedDomain?: string;
  suggestedModule?: string;
  suggestedJobGroup?: string;

  // AI info
  aiProvider?: string;
  aiModel?: string;
  batchId?: string;

  // Timestamps
  classifiedAt: string;
  updatedAt: string;

  // Flags
  hasSuggestions: boolean;
}

export interface ClassificationFilters {
  domainId?: string;
  moduleId?: string;
  jobGroupId?: string;
  complexity?: Complexity;
  batchId?: string;
  hasSuggestions?: boolean;
  search?: string;
}

export interface ClassificationListResponse {
  data: JobClassification[];
  pagination: Pagination;
}

// ============================================================================
// Batch Types
// ============================================================================

export interface ClassificationBatch {
  id: string;
  name?: string;
  status: BatchStatus;

  // Scope
  directories: string[];
  filePatterns: string[];
  excludePatterns: string[];

  // Progress
  totalJobs: number;
  processedJobs: number;
  successfulJobs: number;
  failedJobs: number;
  skippedJobs: number;
  progressPercent: number;

  // AI provider
  aiProvider?: string;
  aiModel?: string;

  // Timing
  createdAt: string;
  startedAt?: string;
  completedAt?: string;

  // Error
  errorMessage?: string;

  // Audit
  triggeredBy?: string;

  // Runtime flags
  isRunning?: boolean;
  isCancelled?: boolean;
}

export interface StartBatchRequest {
  directories: string[];
  name?: string;
  filePatterns?: string[];
  excludePatterns?: string[];
  aiProvider?: 'ollama' | 'azure-openai';
  forceReclassify?: boolean;
}

export interface BatchStatusResponse {
  id: string;
  name?: string;
  status: BatchStatus;
  totalJobs: number;
  processedJobs: number;
  successfulJobs: number;
  failedJobs: number;
  skippedJobs: number;
  progressPercent: number;
  startedAt?: string;
  estimatedTimeRemaining?: number;
  isRunning: boolean;
  isCancelled: boolean;
}

// ============================================================================
// Suggested Category Types
// ============================================================================

export interface SuggestedCategory {
  id: string;
  type: CategoryType;
  name: string;
  description?: string;
  parentName?: string;

  // Jobs that suggested this
  suggestedByJobs: string[];
  occurrenceCount: number;

  // Timestamps
  firstSuggestedAt: string;
  lastSuggestedAt: string;

  // Review status
  status: SuggestionStatus;
  mergedIntoId?: string;
  reviewedBy?: string;
  reviewedAt?: string;
  reviewNotes?: string;
}

export interface ApproveSuggestionRequest {
  reviewedBy: string;
}

export interface RejectSuggestionRequest {
  reviewedBy: string;
  notes?: string;
}

export interface MergeSuggestionRequest {
  reviewedBy: string;
  targetCategoryId: string;
}

// ============================================================================
// Statistics Types
// ============================================================================

export interface ClassificationStats {
  totalClassifications: number;
  byComplexity: Record<Complexity, number>;
  byDomain: Record<string, number>;
  pendingSuggestions: number;
  recentBatch?: ClassificationBatch;
}

export interface CategoryStats {
  totalCategories: number;
  byType: Record<CategoryType, number>;
  pendingSuggestions: number;
}

// ============================================================================
// Tree View Types
// ============================================================================

export type TreeNodeType = 'domain' | 'module' | 'job-group' | 'job';

export interface TreeNodeMetadata {
  totalJobs: number;
  completedJobs: number;
  runningJobs: number;
  failedJobs: number;
}

export interface TreeNode {
  id: string;
  name: string;
  type: TreeNodeType;
  description?: string;
  children?: TreeNode[];
  metadata?: TreeNodeMetadata;

  // For job nodes
  jobId?: string;
  jobPath?: string;
  complexity?: Complexity;
  complexityScore?: number;
  confidenceScore?: number;
}

// ============================================================================
// Common Types
// ============================================================================

export interface Pagination {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// ============================================================================
// Request/Response Helpers
// ============================================================================

export interface PaginationQuery {
  page?: number;
  limit?: number;
}

export interface ClassificationQuery extends PaginationQuery, ClassificationFilters {}

// ============================================================================
// Type Guards
// ============================================================================

export function isValidCategoryType(type: string): type is CategoryType {
  return ['domain', 'module', 'job_group'].includes(type);
}

export function isValidComplexity(complexity: string): complexity is Complexity {
  return ['low', 'medium', 'high'].includes(complexity);
}

export function isValidBatchStatus(status: string): status is BatchStatus {
  return ['pending', 'running', 'completed', 'failed', 'cancelled'].includes(status);
}
