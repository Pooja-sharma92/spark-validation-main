export type TreeNodeType = 'domain' | 'module' | 'job-group' | 'job';
export type Complexity = 'low' | 'medium' | 'high';

export interface TreeNodeMetadata {
  totalJobs: number;
  completedJobs: number;
  runningJobs: number;
  failedJobs: number;
  pendingJobs?: number;
  totalRecords?: number;
  migratedRecords?: number;
}

export interface TreeNode {
  id: string;
  name: string;
  type: TreeNodeType;
  children?: TreeNode[];
  description?: string;
  metadata?: TreeNodeMetadata;

  // For job nodes (from classification)
  jobId?: string;
  jobPath?: string;
  complexity?: Complexity;
  complexityScore?: number;
  confidenceScore?: number;
}

export interface TreeViewState {
  expandedNodes: Set<string>;
  selectedNode: string | null;
  searchQuery: string;
}
