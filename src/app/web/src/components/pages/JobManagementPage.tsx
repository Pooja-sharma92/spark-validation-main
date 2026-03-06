import { useState, useEffect, useCallback } from 'react';
import { mockJobs } from '../../data/migrationMockData';
import { MigrationJob } from '../../types/migration';
import { JobFilter } from '../../types/jobManagement';
import { TreeNode } from '../../types/jobTree';
import { TagView } from '../job-views/TagView';
import { DependencyView } from '../job-views/DependencyView';
import { TreeView } from '../job-views/TreeView';
import { JobDetailsPanel } from '../job-details/JobDetailsPanel';
import { apiClient, QueueJob } from '../../services/api';
import { transformQueueJobToMigrationJob } from '../../services/transformers';
import {
  Play,
  Pause,
  RotateCcw,
  Search,
  Filter,
  Tag,
  CheckSquare,
  Square,
  ChevronDown,
  Download,
  Upload,
  Settings,
  Trash2,
  Copy,
  ChevronsLeft,
  ChevronsRight,
  X,
  Star,
  AlertCircle,
} from 'lucide-react';

export function JobManagementPage() {
  const [filters, setFilters] = useState<JobFilter>({
    search: '',
    status: [],
    priority: [],
    tags: [],
    dateRange: {},
    hasIssues: null,
  });

  const [selectedJobs, setSelectedJobs] = useState<Set<string>>(new Set());
  const [showFilters, setShowFilters] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [viewMode, setViewMode] = useState<'tree' | 'list' | 'tags' | 'dependencies'>('tree');
  const [itemsPerPage, setItemsPerPage] = useState(50);
  const [quickFilter, setQuickFilter] = useState<string | null>(null);
  const [selectedJobForDetails, setSelectedJobForDetails] = useState<MigrationJob | null>(null);
  const [loadingJobDetails, setLoadingJobDetails] = useState(false);
  const [validationHistory, setValidationHistory] = useState<QueueJob[]>([]);

  // Fetch validation history on mount
  useEffect(() => {
    async function fetchHistory() {
      try {
        const history = await apiClient.getValidationHistory(100);
        setValidationHistory(history);
      } catch (err) {
        console.error('Failed to fetch validation history:', err);
      }
    }
    fetchHistory();
  }, []);

  // Handler for tree node clicks
  // Fetches real validation data from API when a job is clicked
  const handleTreeNodeClick = useCallback(async (node: TreeNode) => {
    if (node.type === 'job') {
      setLoadingJobDetails(true);

      // Try to find matching job in validation history by job path
      const jobPath = node.jobPath || '';
      const matchingQueueJob = validationHistory.find(j =>
        j.jobPath === jobPath ||
        j.jobPath.endsWith(node.name) ||
        j.jobPath.includes(node.name.replace('.py', ''))
      );

      if (matchingQueueJob) {
        // Found a real validation result - transform and display
        try {
          // Fetch fresh data with full result included
          const freshJob = await apiClient.getJobById(matchingQueueJob.id);
          if (freshJob) {
            const migrationJob = transformQueueJobToMigrationJob(freshJob);
            // Merge with tree node metadata
            migrationJob.metadata = {
              ...migrationJob.metadata,
              jobPath: node.jobPath,
              complexityScore: node.complexityScore,
              confidenceScore: node.confidenceScore,
            };
            setSelectedJobForDetails(migrationJob);
          } else {
            setSelectedJobForDetails(transformQueueJobToMigrationJob(matchingQueueJob));
          }
        } catch (err) {
          console.error('Failed to fetch job details:', err);
          setSelectedJobForDetails(transformQueueJobToMigrationJob(matchingQueueJob));
        }
      } else {
        // No validation result yet - create a placeholder from tree node
        const jobFromTree: MigrationJob = {
          id: node.jobId || node.id,
          name: node.name,
          sourceTable: 'N/A',
          targetTable: 'N/A',
          status: 'pending',
          progress: 0,
          sourceRowCount: 0,
          targetRowCount: 0,
          validatedRowCount: 0,
          failedRowCount: 0,
          mismatchCount: 0,
          priority: node.complexity === 'high' ? 'high' : node.complexity === 'medium' ? 'medium' : 'low',
          domain: 'Unknown',
          complexity: node.complexity === 'high' ? 'complex' : node.complexity === 'medium' ? 'medium' : 'simple',
          validationSteps: [],
          metadata: {
            jobPath: node.jobPath,
            complexityScore: node.complexityScore,
            confidenceScore: node.confidenceScore,
          },
        };
        setSelectedJobForDetails(jobFromTree);
      }

      setLoadingJobDetails(false);
    }
  }, [validationHistory]);

  const availableTags = [
    { id: 'critical', name: 'Critical', color: 'red' },
    { id: 'user-data', name: 'User Data', color: 'blue' },
    { id: 'financial', name: 'Financial', color: 'green' },
    { id: 'large-table', name: 'Large Table', color: 'purple' },
  ];

  const quickFilters = [
    { id: 'all', label: 'All Jobs', icon: null },
    { id: 'issues', label: 'Has Issues', icon: AlertCircle, filter: { hasIssues: true } },
    { id: 'running', label: 'Running', icon: Play, filter: { status: ['running'] } },
    { id: 'failed', label: 'Failed', icon: X, filter: { status: ['failed'] } },
    { id: 'high-priority', label: 'High Priority', icon: Star, filter: { priority: ['high'] } },
  ];

  const activeFilters = quickFilter && quickFilter !== 'all'
    ? { ...filters, ...quickFilters.find(f => f.id === quickFilter)?.filter }
    : filters;

  const filteredJobs = mockJobs.filter(job => {
    const matchesSearch =
      job.name.toLowerCase().includes(activeFilters.search.toLowerCase()) ||
      job.sourceTable.toLowerCase().includes(activeFilters.search.toLowerCase()) ||
      job.targetTable.toLowerCase().includes(activeFilters.search.toLowerCase());

    const matchesStatus = activeFilters.status.length === 0 || activeFilters.status.includes(job.status);
    const matchesPriority = activeFilters.priority.length === 0 || activeFilters.priority.includes(job.priority);
    const matchesIssues = activeFilters.hasIssues === null ||
      (activeFilters.hasIssues && (job.mismatchCount > 0 || job.failedRowCount > 0));

    return matchesSearch && matchesStatus && matchesPriority && matchesIssues;
  });

  const totalPages = Math.ceil(filteredJobs.length / itemsPerPage);
  const paginatedJobs = filteredJobs.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage
  );

  const toggleJobSelection = (jobId: string) => {
    const newSelection = new Set(selectedJobs);
    if (newSelection.has(jobId)) {
      newSelection.delete(jobId);
    } else {
      newSelection.add(jobId);
    }
    setSelectedJobs(newSelection);
  };

  const toggleSelectAll = () => {
    if (selectedJobs.size === paginatedJobs.length) {
      setSelectedJobs(new Set());
    } else {
      setSelectedJobs(new Set(paginatedJobs.map(j => j.id)));
    }
  };

  const handleBulkAction = (action: string) => {
    if (selectedJobs.size === 0) {
      alert('Please select at least one job');
      return;
    }
    alert(`Performing ${action} on ${selectedJobs.size} jobs`);
  };

  const handlePageJump = (page: number) => {
    const validPage = Math.max(1, Math.min(totalPages, page));
    setCurrentPage(validPage);
  };

  const getPageNumbers = () => {
    const pages: (number | string)[] = [];
    const maxVisible = 7;

    if (totalPages <= maxVisible) {
      for (let i = 1; i <= totalPages; i++) {
        pages.push(i);
      }
    } else {
      if (currentPage <= 4) {
        for (let i = 1; i <= 5; i++) pages.push(i);
        pages.push('...');
        pages.push(totalPages);
      } else if (currentPage >= totalPages - 3) {
        pages.push(1);
        pages.push('...');
        for (let i = totalPages - 4; i <= totalPages; i++) pages.push(i);
      } else {
        pages.push(1);
        pages.push('...');
        for (let i = currentPage - 1; i <= currentPage + 1; i++) pages.push(i);
        pages.push('...');
        pages.push(totalPages);
      }
    }

    return pages;
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-gray-900 mb-2">Job Management</h1>
            <p className="text-gray-600">
              Manage {mockJobs.length.toLocaleString()} migration jobs
              {filteredJobs.length !== mockJobs.length && (
                <span className="text-blue-600"> • {filteredJobs.length.toLocaleString()} filtered</span>
              )}
            </p>
          </div>
          <div className="flex gap-2">
            <button className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors flex items-center gap-2">
              <Upload className="w-4 h-4" />
              Import Jobs
            </button>
            <button className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors flex items-center gap-2">
              <Download className="w-4 h-4" />
              Export
            </button>
          </div>
        </div>
      </div>

      {/* Quick Filters Bar */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-4">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm text-gray-600">Quick Filters:</span>
          {quickFilters.map((filter) => {
            const Icon = filter.icon;
            return (
              <button
                key={filter.id}
                onClick={() => {
                  setQuickFilter(filter.id);
                  setCurrentPage(1);
                }}
                className={`px-3 py-1.5 rounded-lg text-sm transition-colors flex items-center gap-1.5 ${
                  quickFilter === filter.id
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {Icon && <Icon className="w-4 h-4" />}
                {filter.label}
              </button>
            );
          })}
          {quickFilter && quickFilter !== 'all' && (
            <button
              onClick={() => setQuickFilter('all')}
              className="px-2 py-1.5 text-sm text-gray-600 hover:text-gray-900"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      {/* View Mode Tabs */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-4">
        <div className="flex border-b border-gray-200">
          <button
            onClick={() => setViewMode('tree')}
            className={`px-6 py-3 text-sm transition-colors ${
              viewMode === 'tree'
                ? 'border-b-2 border-blue-600 text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            Tree View
          </button>
          <button
            onClick={() => setViewMode('list')}
            className={`px-6 py-3 text-sm transition-colors ${
              viewMode === 'list'
                ? 'border-b-2 border-blue-600 text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            List View
          </button>
          <button
            onClick={() => setViewMode('tags')}
            className={`px-6 py-3 text-sm transition-colors ${
              viewMode === 'tags'
                ? 'border-b-2 border-blue-600 text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            Tags
          </button>
          <button
            onClick={() => setViewMode('dependencies')}
            className={`px-6 py-3 text-sm transition-colors ${
              viewMode === 'dependencies'
                ? 'border-b-2 border-blue-600 text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            Dependencies
          </button>
        </div>
      </div>

      {/* Render Different Views */}
      {viewMode === 'tags' && <TagView />}
      {viewMode === 'dependencies' && <DependencyView />}
      {viewMode === 'tree' && <TreeView onJobClick={handleTreeNodeClick} />}

      {/* List View */}
      {viewMode === 'list' && (
        <>
          {/* Search and Filters */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-4">
            <div className="flex gap-4 mb-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search by job name, source table, or target table..."
                  value={filters.search}
                  onChange={(e) => {
                    setFilters({ ...filters, search: e.target.value });
                    setCurrentPage(1);
                  }}
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
                Advanced Filters
                <ChevronDown className={`w-4 h-4 transition-transform ${showFilters ? 'rotate-180' : ''}`} />
              </button>
            </div>

            {showFilters && (
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4 pt-4 border-t border-gray-200">
                <div>
                  <label className="block text-sm text-gray-700 mb-2">Status</label>
                  <div className="space-y-2">
                    {['pending', 'running', 'validating', 'completed', 'failed'].map(status => (
                      <label key={status} className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={filters.status.includes(status)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setFilters({ ...filters, status: [...filters.status, status] });
                            } else {
                              setFilters({ ...filters, status: filters.status.filter(s => s !== status) });
                            }
                            setCurrentPage(1);
                          }}
                          className="rounded border-gray-300"
                        />
                        <span className="text-sm text-gray-700 capitalize">{status}</span>
                      </label>
                    ))}
                  </div>
                </div>

                <div>
                  <label className="block text-sm text-gray-700 mb-2">Priority</label>
                  <div className="space-y-2">
                    {['high', 'medium', 'low'].map(priority => (
                      <label key={priority} className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={filters.priority.includes(priority)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setFilters({ ...filters, priority: [...filters.priority, priority] });
                            } else {
                              setFilters({ ...filters, priority: filters.priority.filter(p => p !== priority) });
                            }
                            setCurrentPage(1);
                          }}
                          className="rounded border-gray-300"
                        />
                        <span className="text-sm text-gray-700 capitalize">{priority}</span>
                      </label>
                    ))}
                  </div>
                </div>

                <div>
                  <label className="block text-sm text-gray-700 mb-2">Issues</label>
                  <label className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={filters.hasIssues === true}
                      onChange={(e) => {
                        setFilters({ ...filters, hasIssues: e.target.checked ? true : null });
                        setCurrentPage(1);
                      }}
                      className="rounded border-gray-300"
                    />
                    <span className="text-sm text-gray-700">Show only jobs with issues</span>
                  </label>
                </div>

                <div>
                  <label className="block text-sm text-gray-700 mb-2">Tags</label>
                  <div className="space-y-2">
                    {availableTags.map(tag => (
                      <label key={tag.id} className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={filters.tags.includes(tag.id)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setFilters({ ...filters, tags: [...filters.tags, tag.id] });
                            } else {
                              setFilters({ ...filters, tags: filters.tags.filter(t => t !== tag.id) });
                            }
                            setCurrentPage(1);
                          }}
                          className="rounded border-gray-300"
                        />
                        <span className={`text-sm px-2 py-0.5 rounded bg-${tag.color}-100 text-${tag.color}-700`}>
                          {tag.name}
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              </div>
            )}

            <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-200">
              <div className="flex items-center gap-4">
                <div className="text-sm text-gray-600">
                  Showing {((currentPage - 1) * itemsPerPage) + 1}-{Math.min(currentPage * itemsPerPage, filteredJobs.length)} of {filteredJobs.length.toLocaleString()} jobs
                  {selectedJobs.size > 0 && ` (${selectedJobs.size} selected)`}
                </div>
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-600">Per page:</label>
                  <select
                    value={itemsPerPage}
                    onChange={(e) => {
                      setItemsPerPage(Number(e.target.value));
                      setCurrentPage(1);
                    }}
                    className="px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value={20}>20</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                    <option value={200}>200</option>
                    <option value={500}>500</option>
                  </select>
                </div>
              </div>
              <button
                onClick={() => {
                  setFilters({
                    search: '',
                    status: [],
                    priority: [],
                    tags: [],
                    dateRange: {},
                    hasIssues: null,
                  });
                  setQuickFilter('all');
                  setCurrentPage(1);
                }}
                className="text-sm text-blue-600 hover:text-blue-700"
              >
                Clear All Filters
              </button>
            </div>
          </div>

          {/* Bulk Actions */}
          {selectedJobs.size > 0 && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
              <div className="flex items-center justify-between">
                <div className="text-sm text-blue-900">
                  {selectedJobs.size} job{selectedJobs.size > 1 ? 's' : ''} selected
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => handleBulkAction('start')}
                    className="px-3 py-1.5 bg-green-600 text-white rounded hover:bg-green-700 transition-colors text-sm flex items-center gap-1"
                  >
                    <Play className="w-4 h-4" />
                    Start
                  </button>
                  <button
                    onClick={() => handleBulkAction('pause')}
                    className="px-3 py-1.5 bg-yellow-600 text-white rounded hover:bg-yellow-700 transition-colors text-sm flex items-center gap-1"
                  >
                    <Pause className="w-4 h-4" />
                    Pause
                  </button>
                  <button
                    onClick={() => handleBulkAction('retry')}
                    className="px-3 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors text-sm flex items-center gap-1"
                  >
                    <RotateCcw className="w-4 h-4" />
                    Retry
                  </button>
                  <button
                    onClick={() => handleBulkAction('add-tag')}
                    className="px-3 py-1.5 bg-purple-600 text-white rounded hover:bg-purple-700 transition-colors text-sm flex items-center gap-1"
                  >
                    <Tag className="w-4 h-4" />
                    Tag
                  </button>
                  <button
                    onClick={() => handleBulkAction('delete')}
                    className="px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-700 transition-colors text-sm flex items-center gap-1"
                  >
                    <Trash2 className="w-4 h-4" />
                    Delete
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Jobs Table */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden mb-4">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left">
                      <button onClick={toggleSelectAll}>
                        {selectedJobs.size === paginatedJobs.length && paginatedJobs.length > 0 ? (
                          <CheckSquare className="w-5 h-5 text-blue-600" />
                        ) : (
                          <Square className="w-5 h-5 text-gray-400" />
                        )}
                      </button>
                    </th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Job Name</th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Tables</th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Progress</th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Records</th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Status</th>
                    <th className="px-6 py-3 text-left text-sm text-gray-700">Priority</th>
                    <th className="px-6 py-3 text-right text-sm text-gray-700">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {paginatedJobs.map((job) => (
                    <tr
                      key={job.id}
                      className={`hover:bg-gray-50 transition-colors ${
                        selectedJobs.has(job.id) ? 'bg-blue-50' : ''
                      }`}
                    >
                      <td className="px-6 py-4">
                        <button onClick={() => toggleJobSelection(job.id)}>
                          {selectedJobs.has(job.id) ? (
                            <CheckSquare className="w-5 h-5 text-blue-600" />
                          ) : (
                            <Square className="w-5 h-5 text-gray-400" />
                          )}
                        </button>
                      </td>
                      <td className="px-6 py-4">
                        <div className="text-gray-900">{job.name}</div>
                        <div className="text-xs text-gray-500">{job.id}</div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="text-sm">
                          <div className="text-gray-600">{job.sourceTable}</div>
                          <div className="text-gray-400">↓ {job.targetTable}</div>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="w-24">
                          <div className="text-sm text-gray-700 mb-1">{job.progress}%</div>
                          <div className="w-full bg-gray-200 rounded-full h-1.5">
                            <div
                              className="bg-blue-600 h-1.5 rounded-full"
                              style={{ width: `${job.progress}%` }}
                            ></div>
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="text-sm text-gray-900">
                          {job.targetRowCount.toLocaleString()} / {job.sourceRowCount.toLocaleString()}
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-1 rounded text-xs ${
                          job.status === 'completed' ? 'bg-green-100 text-green-700' :
                          job.status === 'running' ? 'bg-blue-100 text-blue-700' :
                          job.status === 'failed' ? 'bg-red-100 text-red-700' :
                          job.status === 'validating' ? 'bg-purple-100 text-purple-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {job.status}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-1 rounded text-xs ${
                          job.priority === 'high' ? 'bg-red-100 text-red-700' :
                          job.priority === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {job.priority}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center justify-end gap-2">
                          <button className="p-1.5 hover:bg-gray-100 rounded" title="Settings">
                            <Settings className="w-4 h-4 text-gray-600" />
                          </button>
                          <button className="p-1.5 hover:bg-gray-100 rounded" title="Clone">
                            <Copy className="w-4 h-4 text-gray-600" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Empty State */}
            {filteredJobs.length === 0 && (
              <div className="p-12 text-center text-gray-500">
                <Search className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                <p className="mb-2">No jobs found</p>
                <p className="text-sm">Try adjusting your filters or search terms</p>
              </div>
            )}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-600">
                Page {currentPage} of {totalPages.toLocaleString()} • {filteredJobs.length.toLocaleString()} total jobs
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => handlePageJump(1)}
                  disabled={currentPage === 1}
                  className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  title="First page"
                >
                  <ChevronsLeft className="w-4 h-4" />
                </button>
                <button
                  onClick={() => handlePageJump(currentPage - 1)}
                  disabled={currentPage === 1}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Previous
                </button>

                <div className="flex items-center gap-1">
                  {getPageNumbers().map((page, index) => (
                    page === '...' ? (
                      <span key={`ellipsis-${index}`} className="px-2 text-gray-500">...</span>
                    ) : (
                      <button
                        key={page}
                        onClick={() => handlePageJump(Number(page))}
                        className={`px-3 py-2 rounded-lg text-sm transition-colors ${
                          currentPage === page
                            ? 'bg-blue-600 text-white'
                            : 'hover:bg-gray-100 text-gray-700'
                        }`}
                      >
                        {page}
                      </button>
                    )
                  ))}
                </div>

                <button
                  onClick={() => handlePageJump(currentPage + 1)}
                  disabled={currentPage === totalPages}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
                <button
                  onClick={() => handlePageJump(totalPages)}
                  disabled={currentPage === totalPages}
                  className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Last page"
                >
                  <ChevronsRight className="w-4 h-4" />
                </button>

                <div className="ml-2 flex items-center gap-2">
                  <span className="text-sm text-gray-600">Go to:</span>
                  <input
                    type="number"
                    min={1}
                    max={totalPages}
                    placeholder="Page"
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        const value = parseInt(e.currentTarget.value);
                        if (value >= 1 && value <= totalPages) {
                          handlePageJump(value);
                          e.currentTarget.value = '';
                        }
                      }
                    }}
                    className="w-20 px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>
            </div>
          )}
        </>
      )}

      {/* Job Details Panel */}
      {loadingJobDetails && (
        <div className="fixed inset-0 bg-black/30 z-50 flex items-center justify-center">
          <div className="bg-white rounded-lg p-6 shadow-xl flex items-center gap-3">
            <div className="w-6 h-6 border-2 border-blue-600 border-t-transparent rounded-full animate-spin" />
            <span className="text-gray-700">Loading job details...</span>
          </div>
        </div>
      )}
      {selectedJobForDetails && !loadingJobDetails && (
        <JobDetailsPanel
          job={selectedJobForDetails}
          onClose={() => setSelectedJobForDetails(null)}
        />
      )}
    </div>
  );
}
