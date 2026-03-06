import { QueueJob } from '../../types/queue';
import {
  AlertTriangle,
  Clock,
  GitBranch,
  RotateCcw,
  Trash2,
  Eye,
  AlertCircle,
} from 'lucide-react';

interface DeadLetterTabProps {
  jobs: QueueJob[];
  onJobClick: (job: QueueJob) => void;
  onRetry: (job: QueueJob) => void;
  onDelete: (job: QueueJob) => void;
}

export function DeadLetterTab({ jobs, onJobClick, onRetry, onDelete }: DeadLetterTabProps) {
  const failedJobs = jobs.filter((j) => j.status === 'failed' || j.status === 'error');

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'P0':
        return 'bg-red-600';
      case 'P1':
        return 'bg-orange-600';
      case 'P2':
        return 'bg-blue-600';
      default:
        return 'bg-gray-600';
    }
  };

  const getStatusIcon = (status: string) => {
    if (status === 'error') {
      return <AlertCircle className="w-5 h-5 text-red-700" />;
    }
    return <AlertTriangle className="w-5 h-5 text-red-600" />;
  };

  const formatTime = (dateStr?: string) => {
    if (!dateStr) return '-';
    return dateStr;
  };

  if (failedJobs.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center p-12 text-gray-500">
        <div className="p-4 bg-green-100 rounded-full mb-4">
          <AlertTriangle className="w-8 h-8 text-green-600" />
        </div>
        <h3 className="text-lg font-medium text-gray-900 mb-2">No Failed Jobs</h3>
        <p className="text-sm">All jobs are processing normally. The dead letter queue is empty.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Header Stats */}
      <div className="flex items-center justify-between p-4 bg-red-50 border border-red-200 rounded-lg">
        <div className="flex items-center gap-3">
          <AlertTriangle className="w-6 h-6 text-red-600" />
          <div>
            <h3 className="font-medium text-red-900">Dead Letter Queue</h3>
            <p className="text-sm text-red-700">
              {failedJobs.length} job{failedJobs.length !== 1 ? 's' : ''} require attention
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-red-300 text-red-700 rounded-lg hover:bg-red-50 transition-colors">
            <RotateCcw className="w-4 h-4" />
            Retry All
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors">
            <Trash2 className="w-4 h-4" />
            Clear All
          </button>
        </div>
      </div>

      {/* Failed Jobs List */}
      <div className="space-y-3">
        {failedJobs.map((job) => (
          <div
            key={job.id}
            className="border border-gray-200 rounded-lg overflow-hidden hover:border-red-300 transition-colors"
          >
            {/* Job Header */}
            <div className="flex items-center justify-between p-4 bg-white border-b border-gray-100">
              <div className="flex items-center gap-3">
                {getStatusIcon(job.status)}
                <div>
                  <div className="flex items-center gap-2">
                    <span className={`px-2 py-0.5 text-xs text-white rounded ${getPriorityColor(job.priority)}`}>
                      {job.priority}
                    </span>
                    <span className={`px-2 py-0.5 text-xs rounded ${
                      job.status === 'error' ? 'bg-red-200 text-red-800' : 'bg-red-100 text-red-700'
                    }`}>
                      {job.status === 'error' ? 'System Error' : 'Validation Failed'}
                    </span>
                  </div>
                  <div className="font-mono text-sm text-gray-900 mt-1">{job.jobPath}</div>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onJobClick(job);
                  }}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded transition-colors"
                >
                  <Eye className="w-4 h-4" />
                  View
                </button>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onRetry(job);
                  }}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded transition-colors"
                >
                  <RotateCcw className="w-4 h-4" />
                  Retry
                </button>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onDelete(job);
                  }}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm text-red-600 hover:text-red-800 hover:bg-red-50 rounded transition-colors"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete
                </button>
              </div>
            </div>

            {/* Error Details */}
            <div className="p-4 bg-gray-50">
              {/* Error Message */}
              {job.errorMessage && (
                <div className="mb-3 p-3 bg-red-50 border border-red-200 rounded text-sm text-red-800 font-mono">
                  {job.errorMessage}
                </div>
              )}

              {/* Metadata */}
              <div className="flex items-center gap-6 text-sm text-gray-600">
                <div className="flex items-center gap-1">
                  <GitBranch className="w-4 h-4" />
                  <span>{job.branch || 'main'}</span>
                </div>
                {job.commitSha && (
                  <code className="text-xs bg-gray-200 px-1.5 py-0.5 rounded">{job.commitSha}</code>
                )}
                <div className="flex items-center gap-1">
                  <Clock className="w-4 h-4" />
                  <span>Failed: {formatTime(job.completedAt)}</span>
                </div>
                <span className="text-gray-400">ID: {job.id}</span>
              </div>

              {/* Validation Results Summary (if available) */}
              {job.validationResults && job.validationResults.length > 0 && (
                <div className="mt-3 flex items-center gap-2">
                  <span className="text-xs text-gray-500">Validation issues:</span>
                  {job.validationResults.map((result) => (
                    <span
                      key={result.stage}
                      className={`px-2 py-0.5 text-xs rounded ${
                        result.status === 'failed'
                          ? 'bg-red-100 text-red-700'
                          : result.status === 'warning'
                          ? 'bg-yellow-100 text-yellow-700'
                          : result.status === 'passed'
                          ? 'bg-green-100 text-green-700'
                          : 'bg-gray-100 text-gray-600'
                      }`}
                    >
                      {result.stage}: {result.issueCount} issues
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
