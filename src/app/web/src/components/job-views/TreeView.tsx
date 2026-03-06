import { useState, useEffect } from 'react';
import { TreeNode } from '../../types/jobTree';

// API base URL - use relative path for production, explicit URL for development
const API_BASE = (window as any).__VITE_API_URL__ || 'http://localhost:3801';

// Simple search function for tree
function searchTree(nodes: TreeNode[], query: string): TreeNode[] {
  const q = query.toLowerCase();
  return nodes
    .map(node => {
      if (node.type === 'job' && node.name.toLowerCase().includes(q)) {
        return node;
      }
      if (node.children) {
        const filteredChildren = searchTree(node.children, query);
        if (filteredChildren.length > 0) {
          return { ...node, children: filteredChildren };
        }
      }
      if (node.name.toLowerCase().includes(q)) {
        return node;
      }
      return null;
    })
    .filter(Boolean) as TreeNode[];
}
import {
  ChevronRight,
  ChevronDown,
  Folder,
  FolderOpen,
  Package,
  Cog,
  Search,
  CheckCircle2,
  XCircle,
  Loader2,
  Play,
  MoreVertical,
  TrendingUp,
  Filter,
  AlertTriangle,
  ChevronsLeft,
  ChevronsRight,
} from 'lucide-react';

const MAX_VISIBLE_JOBS_WARNING = 1000;
const JOBS_PER_PAGE_IN_GROUP = 100;

interface TreeViewProps {
  onJobClick?: (job: TreeNode) => void;
}

export function TreeView({ onJobClick }: TreeViewProps) {
  // API data state
  const [jobTree, setJobTree] = useState<TreeNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [complexityFilter, setComplexityFilter] = useState<string[]>([]);
  const [domainFilter, setDomainFilter] = useState<string[]>([]);
  const [showFilters, setShowFilters] = useState(false);
  const [expandLevel, setExpandLevel] = useState<number>(1);
  const [groupPagination, setGroupPagination] = useState<Map<string, number>>(new Map());
  const [showPerformanceWarning, setShowPerformanceWarning] = useState(false);
  const [currentDomainPage, setCurrentDomainPage] = useState(1);
  const [domainsPerPage] = useState(5);

  // Fetch job tree from API
  useEffect(() => {
    async function fetchJobTree() {
      try {
        setLoading(true);
        const response = await fetch(`${API_BASE}/api/classify/tree`);
        if (!response.ok) {
          throw new Error('Failed to fetch job tree');
        }
        const data = await response.json();
        if (data.success) {
          setJobTree(data.data);
          // Auto-expand first domain
          if (data.data.length > 0) {
            setExpandedNodes(new Set([data.data[0].id]));
          }
        } else {
          throw new Error(data.error || 'Unknown error');
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load job tree');
      } finally {
        setLoading(false);
      }
    }
    fetchJobTree();
  }, []);

  const toggleNode = (nodeId: string) => {
    const newExpanded = new Set(expandedNodes);
    if (newExpanded.has(nodeId)) {
      newExpanded.delete(nodeId);
      const newPagination = new Map(groupPagination);
      newPagination.delete(nodeId);
      setGroupPagination(newPagination);
    } else {
      newExpanded.add(nodeId);
      if (!groupPagination.has(nodeId)) {
        const newPagination = new Map(groupPagination);
        newPagination.set(nodeId, 1);
        setGroupPagination(newPagination);
      }
    }
    setExpandedNodes(newExpanded);
  };

  const calculateVisibleJobs = (nodes: TreeNode[], expanded: Set<string>): number => {
    let count = 0;
    const traverse = (node: TreeNode) => {
      if (node.type === 'job') count++;
      if (node.children && expanded.has(node.id)) {
        node.children.forEach(traverse);
      }
    };
    nodes.forEach(traverse);
    return count;
  };

  useEffect(() => {
    const visibleJobCount = calculateVisibleJobs(jobTree, expandedNodes);
    setShowPerformanceWarning(visibleJobCount > MAX_VISIBLE_JOBS_WARNING);
  }, [expandedNodes, jobTree]);

  const getNodeIcon = (node: TreeNode, isExpanded: boolean) => {
    const iconClass = 'w-4 h-4';
    if (node.type !== 'job') {
      const color = node.type === 'domain' ? 'text-blue-600' :
                    node.type === 'module' ? 'text-purple-600' :
                    'text-green-600';
      return isExpanded ? (
        <FolderOpen className={`${iconClass} ${color}`} />
      ) : (
        <Folder className={`${iconClass} ${color}`} />
      );
    }
    // Job node - color by complexity
    const color = node.complexity === 'high' ? 'text-red-600' :
                  node.complexity === 'medium' ? 'text-yellow-600' :
                  'text-green-600';
    return <Cog className={`${iconClass} ${color}`} />;
  };

  const getComplexityBadge = (node: TreeNode) => {
    if (node.type !== 'job' || !node.complexity) return null;
    const colors = {
      low: 'bg-green-100 text-green-700',
      medium: 'bg-yellow-100 text-yellow-700',
      high: 'bg-red-100 text-red-700',
    };
    return (
      <span className={`px-2 py-0.5 rounded text-xs ${colors[node.complexity as keyof typeof colors]}`}>
        {node.complexity.toUpperCase()} ({node.complexityScore})
      </span>
    );
  };

  const getFilteredTree = (nodes: TreeNode[]): TreeNode[] => {
    return nodes.map(node => {
      if (node.type === 'domain' && domainFilter.length > 0 && !domainFilter.includes(node.id)) {
        return null;
      }
      if (node.type === 'job' && complexityFilter.length > 0 && node.complexity && !complexityFilter.includes(node.complexity)) {
        return null;
      }
      if (node.children) {
        const filteredChildren = getFilteredTree(node.children).filter(Boolean) as TreeNode[];
        if (filteredChildren.length === 0 && node.type !== 'job') return null;
        return { ...node, children: filteredChildren };
      }
      return node;
    }).filter(Boolean) as TreeNode[];
  };

  const renderNodeMetadata = (node: TreeNode) => {
    if (!node.metadata) return null;
    const { totalJobs, completedJobs, runningJobs, failedJobs } = node.metadata;
    const completionRate = totalJobs > 0 ? (completedJobs / totalJobs) * 100 : 0;

    return (
      <div className="flex items-center gap-3 text-xs">
        <span className="text-gray-600">{totalJobs} jobs</span>
        {completedJobs > 0 && (
          <span className="text-green-700 flex items-center gap-1">
            <CheckCircle2 className="w-3 h-3" />{completedJobs}
          </span>
        )}
        {runningJobs > 0 && (
          <span className="text-blue-700 flex items-center gap-1">
            <Loader2 className="w-3 h-3" />{runningJobs}
          </span>
        )}
        {failedJobs > 0 && (
          <span className="text-red-700 flex items-center gap-1">
            <XCircle className="w-3 h-3" />{failedJobs}
          </span>
        )}
        <div className="ml-2 flex items-center gap-1">
          <div className="w-16 bg-gray-200 rounded-full h-1.5">
            <div className="bg-green-600 h-1.5 rounded-full" style={{ width: `${completionRate}%` }}></div>
          </div>
          <span className="text-gray-600">{completionRate.toFixed(0)}%</span>
        </div>
      </div>
    );
  };

  const renderTreeNode = (node: TreeNode, level: number = 0) => {
    const isExpanded = expandedNodes.has(node.id);
    const isSelected = selectedNode === node.id;
    const hasChildren = node.children && node.children.length > 0;
    const paddingLeft = level * 24;

    const handleNodeClick = (e: React.MouseEvent) => {
      e.stopPropagation();
      if (hasChildren) toggleNode(node.id);
      setSelectedNode(node.id);
      if (node.type === 'job' && onJobClick) {
        onJobClick(node);
      }
    };

    let childrenToRender = node.children || [];
    let totalChildren = childrenToRender.length;
    let currentPage = groupPagination.get(node.id) || 1;
    let totalPages = 1;
    let showPagination = false;

    if (hasChildren && isExpanded && node.type === 'job-group') {
      const jobChildren = childrenToRender.filter(child => child.type === 'job');
      if (jobChildren.length > JOBS_PER_PAGE_IN_GROUP) {
        showPagination = true;
        totalPages = Math.ceil(jobChildren.length / JOBS_PER_PAGE_IN_GROUP);
        const startIdx = (currentPage - 1) * JOBS_PER_PAGE_IN_GROUP;
        const endIdx = startIdx + JOBS_PER_PAGE_IN_GROUP;
        const nonJobChildren = childrenToRender.filter(child => child.type !== 'job');
        const paginatedJobChildren = jobChildren.slice(startIdx, endIdx);
        childrenToRender = [...nonJobChildren, ...paginatedJobChildren];
      }
    }

    const handlePageChange = (newPage: number) => {
      const newPagination = new Map(groupPagination);
      newPagination.set(node.id, newPage);
      setGroupPagination(newPagination);
    };

    return (
      <div key={node.id}>
        <div
          className={`group flex items-center gap-2 px-3 py-2 hover:bg-gray-50 cursor-pointer transition-colors ${
            isSelected ? 'bg-blue-50 border-l-4 border-blue-600' : 'border-l-4 border-transparent'
          }`}
          style={{ paddingLeft: `${paddingLeft + 12}px` }}
          onClick={handleNodeClick}
        >
          <div className="w-5 flex items-center justify-center">
            {hasChildren && (
              <button onClick={(e) => { e.stopPropagation(); toggleNode(node.id); }}>
                {isExpanded ? <ChevronDown className="w-4 h-4 text-gray-600" /> : <ChevronRight className="w-4 h-4 text-gray-600" />}
              </button>
            )}
          </div>
          {getNodeIcon(node, isExpanded)}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-gray-900">{node.name}</span>
              {getComplexityBadge(node)}
              {showPagination && (
                <span className="text-xs text-gray-500 bg-gray-100 px-2 py-0.5 rounded">
                  Page {currentPage}/{totalPages} ({totalChildren} jobs)
                </span>
              )}
            </div>
            {node.description && node.type !== 'job' && (
              <div className="text-xs text-gray-600 mt-0.5">{node.description}</div>
            )}
            {node.type === 'job' && node.jobPath && (
              <div className="text-xs text-gray-500 mt-0.5 truncate" title={node.jobPath}>
                {node.jobPath.split('/').slice(-2).join('/')}
              </div>
            )}
            {node.type !== 'job' && node.metadata && (
              <div className="mt-1">{renderNodeMetadata(node)}</div>
            )}
          </div>
          <div className="opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-1">
            {node.type !== 'job' && (
              <button className="p-1.5 hover:bg-blue-100 rounded transition-colors" title="Start all jobs">
                <Play className="w-4 h-4 text-blue-600" />
              </button>
            )}
            <button className="p-1.5 hover:bg-gray-100 rounded transition-colors" title="More actions">
              <MoreVertical className="w-4 h-4 text-gray-600" />
            </button>
          </div>
          {node.type === 'job' && node.confidenceScore !== undefined && (
            <div className="w-20">
              <div className="text-xs text-gray-600 mb-1">{Math.round(node.confidenceScore * 100)}%</div>
              <div className="w-full bg-gray-200 rounded-full h-1.5">
                <div
                  className="h-1.5 rounded-full bg-blue-600"
                  style={{ width: `${node.confidenceScore * 100}%` }}
                ></div>
              </div>
            </div>
          )}
        </div>

        {showPagination && isExpanded && (
          <div
            className="flex items-center justify-center gap-2 py-2 bg-gray-50 border-t border-b border-gray-200"
            style={{ paddingLeft: `${paddingLeft + 12}px` }}
          >
            <button
              onClick={(e) => { e.stopPropagation(); handlePageChange(Math.max(1, currentPage - 1)); }}
              disabled={currentPage === 1}
              className="px-3 py-1 text-xs border border-gray-300 rounded hover:bg-white transition-colors disabled:opacity-50"
            >
              Previous
            </button>
            <span className="text-xs text-gray-600">Page {currentPage} of {totalPages}</span>
            <button
              onClick={(e) => { e.stopPropagation(); handlePageChange(Math.min(totalPages, currentPage + 1)); }}
              disabled={currentPage === totalPages}
              className="px-3 py-1 text-xs border border-gray-300 rounded hover:bg-white transition-colors disabled:opacity-50"
            >
              Next
            </button>
          </div>
        )}

        {hasChildren && isExpanded && (
          <div>{childrenToRender.map(child => renderTreeNode(child, level + 1))}</div>
        )}
      </div>
    );
  };

  let processedTree = jobTree;
  if (complexityFilter.length > 0 || domainFilter.length > 0) {
    processedTree = getFilteredTree(processedTree);
  }
  let filteredTree = searchQuery.trim() ? searchTree(processedTree, searchQuery) : processedTree;

  const totalDomains = filteredTree.length;
  const totalDomainPages = Math.ceil(totalDomains / domainsPerPage);
  const shouldPaginateDomains = !searchQuery.trim() && domainFilter.length === 0 && totalDomains > domainsPerPage;

  if (shouldPaginateDomains) {
    const startIdx = (currentDomainPage - 1) * domainsPerPage;
    const endIdx = startIdx + domainsPerPage;
    filteredTree = filteredTree.slice(startIdx, endIdx);
  }

  const expandToLevel = (level: number) => {
    const nodesToExpand = new Set<string>();
    const traverseTree = (nodes: TreeNode[], currentLevel: number) => {
      nodes.forEach(node => {
        if (currentLevel < level && node.children && node.children.length > 0) {
          nodesToExpand.add(node.id);
          traverseTree(node.children, currentLevel + 1);
        }
      });
    };
    traverseTree(jobTree, 0);
    setExpandedNodes(nodesToExpand);
    setExpandLevel(level);
    setGroupPagination(new Map());
  };

  const getTotalStats = () => {
    const stats = {
      totalDomains: jobTree.length,
      totalModules: 0,
      totalGroups: 0,
      totalJobs: 0,
      lowComplexity: 0,
      mediumComplexity: 0,
      highComplexity: 0,
    };
    const count = (nodes: TreeNode[]) => {
      nodes.forEach(node => {
        if (node.type === 'module') stats.totalModules++;
        if (node.type === 'job-group') stats.totalGroups++;
        if (node.type === 'job') {
          stats.totalJobs++;
          if (node.complexity === 'low') stats.lowComplexity++;
          else if (node.complexity === 'medium') stats.mediumComplexity++;
          else if (node.complexity === 'high') stats.highComplexity++;
        }
        if (node.children) count(node.children);
      });
    };
    count(jobTree);
    return stats;
  };

  const stats = getTotalStats();
  const visibleJobCount = calculateVisibleJobs(filteredTree, expandedNodes);

  // Loading state
  if (loading) {
    return (
      <div className="flex items-center justify-center p-12">
        <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
        <span className="ml-3 text-gray-600">Loading job tree...</span>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
        <XCircle className="w-12 h-12 text-red-500 mx-auto mb-3" />
        <p className="text-red-700 mb-2">Failed to load job tree</p>
        <p className="text-sm text-red-600">{error}</p>
        <button
          onClick={() => window.location.reload()}
          className="mt-4 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {showPerformanceWarning && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-yellow-600 mt-0.5" />
            <div className="flex-1">
              <div className="text-sm text-yellow-900 mb-1">Performance Warning</div>
              <p className="text-sm text-yellow-800">
                You are viewing <strong>{visibleJobCount.toLocaleString()} jobs</strong>. Consider using filters or collapsing branches.
              </p>
            </div>
            <button
              onClick={() => { expandToLevel(2); setShowPerformanceWarning(false); }}
              className="px-3 py-1 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700"
            >
              Auto-Optimize
            </button>
          </div>
        </div>
      )}

      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center gap-4 mb-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search in tree..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`px-4 py-2 border rounded-lg transition-colors flex items-center gap-2 ${
              showFilters ? 'bg-blue-50 border-blue-300' : 'border-gray-300 hover:bg-gray-50'
            }`}
          >
            <Filter className="w-4 h-4" />
            Filters
          </button>
        </div>

        {showFilters && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4 p-4 bg-gray-50 rounded-lg border border-gray-200">
            <div>
              <label className="text-sm text-gray-700 mb-2 block">Complexity</label>
              <div className="space-y-1.5">
                {['low', 'medium', 'high'].map(complexity => (
                  <label key={complexity} className="flex items-center gap-2 cursor-pointer hover:bg-white px-2 py-1 rounded">
                    <input
                      type="checkbox"
                      checked={complexityFilter.includes(complexity)}
                      onChange={(e) => {
                        if (e.target.checked) setComplexityFilter([...complexityFilter, complexity]);
                        else setComplexityFilter(complexityFilter.filter(c => c !== complexity));
                      }}
                      className="rounded border-gray-300 text-blue-600"
                    />
                    <span className={`text-sm px-2 py-0.5 rounded ${
                      complexity === 'high' ? 'bg-red-100 text-red-700' :
                      complexity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-green-100 text-green-700'
                    }`}>
                      {complexity.charAt(0).toUpperCase() + complexity.slice(1)}
                    </span>
                  </label>
                ))}
              </div>
            </div>
            <div>
              <label className="text-sm text-gray-700 mb-2 block">Domain</label>
              <div className="space-y-1.5 max-h-32 overflow-y-auto">
                {jobTree.map(domain => (
                  <label key={domain.id} className="flex items-center gap-2 cursor-pointer hover:bg-white px-2 py-1 rounded">
                    <input
                      type="checkbox"
                      checked={domainFilter.includes(domain.id)}
                      onChange={(e) => {
                        if (e.target.checked) setDomainFilter([...domainFilter, domain.id]);
                        else setDomainFilter(domainFilter.filter(d => d !== domain.id));
                      }}
                      className="rounded border-gray-300 text-blue-600"
                    />
                    <span className="text-sm text-gray-700">{domain.name}</span>
                  </label>
                ))}
              </div>
            </div>
          </div>
        )}

        <div className="flex items-center gap-2 mb-4">
          <span className="text-sm text-gray-600">Expand:</span>
          {[0, 1, 2, 3].map(level => (
            <button
              key={level}
              onClick={() => expandToLevel(level)}
              className={`px-3 py-1 rounded text-sm ${expandLevel === level ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
            >
              {level === 0 ? 'None' : `Level ${level}`}
            </button>
          ))}
          <span className="text-xs text-gray-500 ml-2">({visibleJobCount.toLocaleString()} jobs visible)</span>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-7 gap-3">
          <div className="p-3 bg-blue-50 rounded-lg"><div className="text-xs text-blue-700 mb-1">Domains</div><div className="text-blue-900">{stats.totalDomains}</div></div>
          <div className="p-3 bg-purple-50 rounded-lg"><div className="text-xs text-purple-700 mb-1">Modules</div><div className="text-purple-900">{stats.totalModules}</div></div>
          <div className="p-3 bg-teal-50 rounded-lg"><div className="text-xs text-teal-700 mb-1">Groups</div><div className="text-teal-900">{stats.totalGroups}</div></div>
          <div className="p-3 bg-gray-50 rounded-lg"><div className="text-xs text-gray-700 mb-1">Total Jobs</div><div className="text-gray-900">{stats.totalJobs.toLocaleString()}</div></div>
          <div className="p-3 bg-green-50 rounded-lg"><div className="text-xs text-green-700 mb-1">Low</div><div className="text-green-900">{stats.lowComplexity}</div></div>
          <div className="p-3 bg-yellow-50 rounded-lg"><div className="text-xs text-yellow-700 mb-1">Medium</div><div className="text-yellow-900">{stats.mediumComplexity}</div></div>
          <div className="p-3 bg-red-50 rounded-lg"><div className="text-xs text-red-700 mb-1">High</div><div className="text-red-900">{stats.highComplexity}</div></div>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-gray-900">Job Hierarchy Tree</h3>
              <p className="text-sm text-gray-600 mt-1">Domain &gt; Module &gt; Job Group &gt; Job</p>
            </div>
            <div className="flex items-center gap-2 text-xs text-gray-600">
              <div className="flex items-center gap-1"><Folder className="w-4 h-4 text-blue-600" />Domain</div>
              <div className="flex items-center gap-1"><Folder className="w-4 h-4 text-purple-600" />Module</div>
              <div className="flex items-center gap-1"><Package className="w-4 h-4 text-green-600" />Group</div>
              <div className="flex items-center gap-1"><Cog className="w-4 h-4 text-gray-600" />Job</div>
            </div>
          </div>
        </div>

        <div className="max-h-[600px] overflow-y-auto">
          {filteredTree.length > 0 ? (
            filteredTree.map(node => renderTreeNode(node, 0))
          ) : jobTree.length === 0 ? (
            <div className="p-12 text-center text-gray-500">
              <Package className="w-12 h-12 mx-auto mb-3 text-gray-400" />
              <p className="mb-2">No classified jobs yet</p>
              <p className="text-sm">Run the classification pipeline to categorize your jobs.</p>
            </div>
          ) : (
            <div className="p-12 text-center text-gray-500">
              <Search className="w-12 h-12 mx-auto mb-3 text-gray-400" />
              <p className="mb-2">No results found</p>
              <p className="text-sm">Try adjusting your search or filters.</p>
            </div>
          )}
        </div>

        {shouldPaginateDomains && (
          <div className="p-4 border-t border-gray-200 bg-gray-50">
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-600">
                Showing domains {((currentDomainPage - 1) * domainsPerPage) + 1}-{Math.min(currentDomainPage * domainsPerPage, totalDomains)} of {totalDomains}
              </div>
              <div className="flex items-center gap-2">
                <button onClick={() => setCurrentDomainPage(1)} disabled={currentDomainPage === 1} className="p-2 border border-gray-300 rounded hover:bg-white disabled:opacity-50"><ChevronsLeft className="w-4 h-4" /></button>
                <button onClick={() => setCurrentDomainPage(Math.max(1, currentDomainPage - 1))} disabled={currentDomainPage === 1} className="px-3 py-1.5 border border-gray-300 rounded hover:bg-white disabled:opacity-50 text-sm">Previous</button>
                <span className="text-sm text-gray-700">Page {currentDomainPage} of {totalDomainPages}</span>
                <button onClick={() => setCurrentDomainPage(Math.min(totalDomainPages, currentDomainPage + 1))} disabled={currentDomainPage === totalDomainPages} className="px-3 py-1.5 border border-gray-300 rounded hover:bg-white disabled:opacity-50 text-sm">Next</button>
                <button onClick={() => setCurrentDomainPage(totalDomainPages)} disabled={currentDomainPage === totalDomainPages} className="p-2 border border-gray-300 rounded hover:bg-white disabled:opacity-50"><ChevronsRight className="w-4 h-4" /></button>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <TrendingUp className="w-5 h-5 text-blue-600 mt-0.5" />
          <div>
            <div className="text-sm text-blue-900 mb-1">Optimized for Large Datasets</div>
            <ul className="text-sm text-blue-800 space-y-1">
              <li>• Automatic pagination for job groups with 100+ jobs</li>
              <li>• Performance warnings when viewing 1000+ jobs</li>
              <li>• Smart expansion levels for best performance</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
