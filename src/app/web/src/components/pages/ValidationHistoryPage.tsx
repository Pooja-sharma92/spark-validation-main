import { useState, useEffect, useCallback } from 'react';
import { ValidationJob } from '../../types/queue';
import { ValidationJobDetailModal } from '../validation/ValidationJobDetailModal';
import {
  History,
  Search,
  RefreshCw,
  Loader2,
  WifiOff,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Clock,
  GitBranch,
  User,
  FileCode,
  ChevronRight,
} from 'lucide-react';

const API_BASE = '/api/queue';

export function ValidationHistoryPage() {
  const [jobs, setJobs] = useState<ValidationJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [selectedJob, setSelectedJob] = useState<ValidationJob | null>(null);

  const fetchData = useCallback(async (showRefreshing = false) => {
    if (showRefreshing) setRefreshing(true);
    else setLoading(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/history?limit=100`);
      if (!res.ok) {
        throw new Error('Failed to fetch validation history');
      }
      const data = await res.json();
      setJobs(data);
    } catch (err) {
      console.error('Failed to fetch validation history:', err);
      setError(err instanceof Error ? err.message : 'Failed to connect to API');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Auto-refresh every 30 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      fetchData(true);
    }, 30000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const filteredJobs = jobs.filter((job) => {
    const matchesSearch =
      job.jobPath.toLowerCase().includes(searchTerm.toLowerCase()) ||
      job.id.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (job.branch && job.branch.toLowerCase().includes(searchTerm.toLowerCase()));

    const matchesStatus =
      statusFilter === 'all' ||
      (statusFilter === 'passed' && job.result?.passed) ||
      (statusFilter === 'failed' && job.result && !job.result.passed) ||
      (statusFilter === 'error' && job.status.toUpperCase() === 'ERROR');

    return matchesSearch && matchesStatus;
  });

  const getStatusIcon = (job: ValidationJob) => {
    if (job.status.toUpperCase() === 'ERROR') {
      return <AlertTriangle className="w-5 h-5 text-orange-500" />;
    }
    if (job.result?.passed) {
      return <CheckCircle className="w-5 h-5 text-green-500" />;
    }
    return <XCircle className="w-5 h-5 text-red-500" />;
  };

  const getStatusBadge = (job: ValidationJob) => {
    if (job.status.toUpperCase() === 'ERROR') {
      return (
        <span className="px-2 py-1 text-xs rounded-full bg-orange-100 text-orange-700">
          Error
        </span>
      );
    }
    if (job.result?.passed) {
      return (
        <span className="px-2 py-1 text-xs rounded-full bg-green-100 text-green-700">
          Passed
        </span>
      );
    }
    return (
      <span className="px-2 py-1 text-xs rounded-full bg-red-100 text-red-700">
        Failed
      </span>
    );
  };

  const formatDuration = (job: ValidationJob) => {
    if (!job.startedAt || !job.completedAt) return '-';
    const start = new Date(job.startedAt).getTime();
    const end = new Date(job.completedAt).getTime();
    const ms = end - start;
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleString();
  };

  const getFileName = (path: string) => {
    const parts = path.split('/');
    return parts[parts.length - 1];
  };

  const getIssueCount = (job: ValidationJob) => {
    if (!job.result?.stages) return 0;
    return job.result.stages.reduce((sum, stage) => sum + (stage.issues?.length || 0), 0);
  };

  // Error state
  if (error && jobs.length === 0) {
    return (
      <div className="p-6">
        <div className="flex flex-col items-center justify-center p-12 bg-white rounded-lg border border-gray-200">
          <WifiOff className="w-12 h-12 text-gray-400 mb-4" />
          <h2 className="text-lg font-medium text-gray-900 mb-2">Unable to Connect</h2>
          <p className="text-sm text-gray-500 mb-4">{error}</p>
          <button
            onClick={() => fetchData()}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900 flex items-center gap-2">
            <History className="w-6 h-6" />
            Validation History
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            View executed validation jobs with detailed results
            {error && (
              <span className="text-orange-500 ml-2">(Connection issue - showing cached data)</span>
            )}
          </p>
        </div>
        <button
          onClick={() => fetchData(true)}
          disabled={refreshing}
          className="flex items-center gap-2 px-4 py-2 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex gap-4 items-center">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search by file path, job ID, or branch..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="all">All Status</option>
            <option value="passed">Passed</option>
            <option value="failed">Failed</option>
            <option value="error">Error</option>
          </select>
        </div>
      </div>

      {/* Stats Summary */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-2xl font-bold text-gray-900">{jobs.length}</div>
          <div className="text-sm text-gray-500">Total Jobs</div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-2xl font-bold text-green-600">
            {jobs.filter((j) => j.result?.passed).length}
          </div>
          <div className="text-sm text-gray-500">Passed</div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-2xl font-bold text-red-600">
            {jobs.filter((j) => j.result && !j.result.passed).length}
          </div>
          <div className="text-sm text-gray-500">Failed</div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-2xl font-bold text-orange-600">
            {jobs.filter((j) => j.status.toUpperCase() === 'ERROR').length}
          </div>
          <div className="text-sm text-gray-500">Errors</div>
        </div>
      </div>

      {/* Jobs List */}
      {loading ? (
        <div className="flex items-center justify-center p-12">
          <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
        </div>
      ) : filteredJobs.length === 0 ? (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-12 text-center">
          <History className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500">No validation jobs found</p>
          <p className="text-sm text-gray-400 mt-1">
            Run the executor to process validation jobs
          </p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
          <div className="divide-y divide-gray-200">
            {filteredJobs.map((job) => (
              <div
                key={job.id}
                onClick={() => setSelectedJob(job)}
                className="p-4 hover:bg-gray-50 cursor-pointer transition-colors"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    {getStatusIcon(job)}
                    <div>
                      <div className="flex items-center gap-2">
                        <FileCode className="w-4 h-4 text-gray-400" />
                        <span className="font-medium text-gray-900">
                          {getFileName(job.jobPath)}
                        </span>
                        {getStatusBadge(job)}
                        {getIssueCount(job) > 0 && (
                          <span className="px-2 py-0.5 text-xs rounded-full bg-yellow-100 text-yellow-700">
                            {getIssueCount(job)} issue{getIssueCount(job) > 1 ? 's' : ''}
                          </span>
                        )}
                      </div>
                      <div className="text-sm text-gray-500 mt-1 flex items-center gap-4">
                        <span className="font-mono text-xs">{job.id.substring(0, 8)}</span>
                        {job.branch && (
                          <span className="flex items-center gap-1">
                            <GitBranch className="w-3 h-3" />
                            {job.branch}
                          </span>
                        )}
                        {job.triggeredBy && (
                          <span className="flex items-center gap-1">
                            <User className="w-3 h-3" />
                            {job.triggeredBy}
                          </span>
                        )}
                        <span className="flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {formatDuration(job)}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <div className="text-right">
                      <div className="text-sm text-gray-600">
                        {job.completedAt && formatDate(job.completedAt)}
                      </div>
                      <div className="text-xs text-gray-400">
                        {job.priority} · {job.triggerSource}
                      </div>
                    </div>
                    <ChevronRight className="w-5 h-5 text-gray-400" />
                  </div>
                </div>

                {/* Stages Preview */}
                {job.result?.stages && (
                  <div className="mt-3 flex gap-2">
                    {job.result.stages.map((stage) => (
                      <div
                        key={stage.stage}
                        className={`px-2 py-1 text-xs rounded ${
                          stage.passed
                            ? 'bg-green-50 text-green-700 border border-green-200'
                            : 'bg-red-50 text-red-700 border border-red-200'
                        }`}
                      >
                        {stage.passed ? '✓' : '✗'} {stage.stage}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Job Detail Modal */}
      {selectedJob && (
        <ValidationJobDetailModal
          job={selectedJob}
          onClose={() => setSelectedJob(null)}
        />
      )}
    </div>
  );
}
