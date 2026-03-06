import { useState, useMemo } from 'react';
import { QueueJob, JobStatus, Priority, TriggerSource } from '../../types/queue';
import {
  Search,
  Filter,
  GitBranch,
  FileCode,
  User,
  Calendar,
  GitCommit,
  Clock,
  ChevronDown,
  X,
} from 'lucide-react';

interface AllJobsTabProps {
  jobs: QueueJob[];
  onJobClick: (job: QueueJob) => void;
}

interface FilterState {
  status: JobStatus | 'all';
  priority: Priority | 'all';
  source: TriggerSource | 'all';
  search: string;
}

export function AllJobsTab({ jobs, onJobClick }: AllJobsTabProps) {
  const [filters, setFilters] = useState<FilterState>({
    status: 'all',
    priority: 'all',
    source: 'all',
    search: '',
  });
  const [showFilters, setShowFilters] = useState(false);

  const filteredJobs = useMemo(() => {
    return jobs.filter((job) => {
      if (filters.status !== 'all' && job.status !== filters.status) return false;
      if (filters.priority !== 'all' && job.priority !== filters.priority) return false;
      if (filters.source !== 'all' && job.triggerSource !== filters.source) return false;
      if (filters.search) {
        const searchLower = filters.search.toLowerCase();
        return (
          job.jobPath.toLowerCase().includes(searchLower) ||
          job.id.toLowerCase().includes(searchLower) ||
          job.branch?.toLowerCase().includes(searchLower) ||
          job.commitSha?.toLowerCase().includes(searchLower)
        );
      }
      return true;
    });
  }, [jobs, filters]);

  const activeFilterCount = [
    filters.status !== 'all',
    filters.priority !== 'all',
    filters.source !== 'all',
  ].filter(Boolean).length;

  const clearFilters = () => {
    setFilters({
      status: 'all',
      priority: 'all',
      source: 'all',
      search: '',
    });
  };

  const getTriggerIcon = (source: TriggerSource) => {
    const iconClass = 'w-4 h-4';
    switch (source) {
      case 'webhook':
        return <GitBranch className={iconClass} />;
      case 'file':
        return <FileCode className={iconClass} />;
      case 'manual':
        return <User className={iconClass} />;
      case 'scheduled':
        return <Calendar className={iconClass} />;
      case 'ci_cd':
        return <GitCommit className={iconClass} />;
    }
  };

  const getStatusBadge = (status: JobStatus) => {
    const baseClass = 'px-2 py-1 rounded text-xs font-medium';
    switch (status) {
      case 'pending':
        return <span className={`${baseClass} bg-gray-100 text-gray-700`}>Pending</span>;
      case 'running':
        return <span className={`${baseClass} bg-blue-100 text-blue-700`}>Running</span>;
      case 'completed':
        return <span className={`${baseClass} bg-green-100 text-green-700`}>Completed</span>;
      case 'failed':
        return <span className={`${baseClass} bg-red-100 text-red-700`}>Failed</span>;
      case 'error':
        return <span className={`${baseClass} bg-red-200 text-red-800`}>Error</span>;
    }
  };

  const getPriorityBadge = (priority: Priority) => {
    const baseClass = 'px-2 py-1 rounded text-xs font-medium text-white';
    switch (priority) {
      case 'P0':
        return <span className={`${baseClass} bg-red-600`}>P0</span>;
      case 'P1':
        return <span className={`${baseClass} bg-orange-600`}>P1</span>;
      case 'P2':
        return <span className={`${baseClass} bg-blue-600`}>P2</span>;
      case 'P3':
        return <span className={`${baseClass} bg-gray-600`}>P3</span>;
    }
  };

  const formatDuration = (ms?: number) => {
    if (!ms) return '-';
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${minutes}m ${secs}s`;
  };

  return (
    <div className="space-y-4">
      {/* Search and Filter Bar */}
      <div className="flex items-center gap-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search jobs by path, ID, branch, or commit..."
            value={filters.search}
            onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
            className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        <button
          onClick={() => setShowFilters(!showFilters)}
          className={`flex items-center gap-2 px-4 py-2 border rounded-lg transition-colors ${
            showFilters || activeFilterCount > 0
              ? 'border-blue-500 text-blue-600 bg-blue-50'
              : 'border-gray-200 text-gray-600 hover:bg-gray-50'
          }`}
        >
          <Filter className="w-4 h-4" />
          <span>Filters</span>
          {activeFilterCount > 0 && (
            <span className="px-1.5 py-0.5 bg-blue-500 text-white text-xs rounded-full">
              {activeFilterCount}
            </span>
          )}
          <ChevronDown className={`w-4 h-4 transition-transform ${showFilters ? 'rotate-180' : ''}`} />
        </button>
      </div>

      {/* Filter Dropdowns */}
      {showFilters && (
        <div className="flex items-center gap-4 p-4 bg-gray-50 rounded-lg">
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Status:</label>
            <select
              value={filters.status}
              onChange={(e) => setFilters((f) => ({ ...f, status: e.target.value as JobStatus | 'all' }))}
              className="px-3 py-1.5 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All</option>
              <option value="pending">Pending</option>
              <option value="running">Running</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
              <option value="error">Error</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Priority:</label>
            <select
              value={filters.priority}
              onChange={(e) => setFilters((f) => ({ ...f, priority: e.target.value as Priority | 'all' }))}
              className="px-3 py-1.5 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All</option>
              <option value="P0">P0 - Critical</option>
              <option value="P1">P1 - Manual</option>
              <option value="P2">P2 - CI/CD</option>
              <option value="P3">P3 - Batch</option>
            </select>
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Source:</label>
            <select
              value={filters.source}
              onChange={(e) => setFilters((f) => ({ ...f, source: e.target.value as TriggerSource | 'all' }))}
              className="px-3 py-1.5 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All</option>
              <option value="webhook">Webhook</option>
              <option value="manual">Manual</option>
              <option value="ci_cd">CI/CD</option>
              <option value="scheduled">Scheduled</option>
              <option value="file">File</option>
            </select>
          </div>
          {activeFilterCount > 0 && (
            <button
              onClick={clearFilters}
              className="flex items-center gap-1 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
            >
              <X className="w-4 h-4" />
              Clear all
            </button>
          )}
        </div>
      )}

      {/* Results Count */}
      <div className="text-sm text-gray-600">
        Showing {filteredJobs.length} of {jobs.length} jobs
      </div>

      {/* Jobs Table */}
      <div className="border border-gray-200 rounded-lg overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Job Path
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Priority
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Source
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Branch
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Duration
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Created
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {filteredJobs.map((job) => (
              <tr
                key={job.id}
                onClick={() => onJobClick(job)}
                className="hover:bg-gray-50 cursor-pointer transition-colors"
              >
                <td className="px-4 py-3">
                  <div className="font-mono text-sm text-gray-900 truncate max-w-xs" title={job.jobPath}>
                    {job.jobPath}
                  </div>
                  <div className="text-xs text-gray-500">{job.id}</div>
                </td>
                <td className="px-4 py-3">{getStatusBadge(job.status)}</td>
                <td className="px-4 py-3">{getPriorityBadge(job.priority)}</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2 text-gray-600">
                    {getTriggerIcon(job.triggerSource)}
                    <span className="text-sm capitalize">{job.triggerSource.replace('_', ' ')}</span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <GitBranch className="w-4 h-4 text-gray-400" />
                    <span className="text-sm text-gray-700 truncate max-w-[120px]">{job.branch || '-'}</span>
                    {job.commitSha && (
                      <code className="text-xs bg-gray-100 px-1.5 py-0.5 rounded">{job.commitSha}</code>
                    )}
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1 text-sm text-gray-600">
                    <Clock className="w-4 h-4" />
                    {formatDuration(job.duration)}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-600">{job.createdAt}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {filteredJobs.length === 0 && (
          <div className="p-8 text-center text-gray-500">
            <p>No jobs match your filters</p>
          </div>
        )}
      </div>
    </div>
  );
}
