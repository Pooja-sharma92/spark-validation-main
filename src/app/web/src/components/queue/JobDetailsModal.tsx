import { QueueJob } from '../../types/queue';
import {
  X,
  GitBranch,
  Clock,
  User,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Info,
  Terminal,
  FileCode,
  RotateCcw,
  Trash2,
} from 'lucide-react';

interface JobDetailsModalProps {
  job: QueueJob | null;
  onClose: () => void;
  onRetry?: (job: QueueJob) => void;
  onCancel?: (job: QueueJob) => void;
}

export function JobDetailsModal({ job, onClose, onRetry, onCancel }: JobDetailsModalProps) {
  if (!job) return null;

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'pending':
        return 'bg-gray-100 text-gray-700';
      case 'running':
        return 'bg-blue-100 text-blue-700';
      case 'completed':
        return 'bg-green-100 text-green-700';
      case 'failed':
        return 'bg-red-100 text-red-700';
      case 'error':
        return 'bg-red-200 text-red-800';
      default:
        return 'bg-gray-100 text-gray-700';
    }
  };

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

  const getValidationIcon = (status: string) => {
    switch (status) {
      case 'passed':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'failed':
        return <XCircle className="w-4 h-4 text-red-500" />;
      case 'warning':
        return <AlertTriangle className="w-4 h-4 text-yellow-500" />;
      case 'skipped':
        return <Info className="w-4 h-4 text-gray-400" />;
      default:
        return null;
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

  const getLogLevelColor = (level: string) => {
    switch (level) {
      case 'error':
        return 'text-red-500';
      case 'warn':
        return 'text-yellow-500';
      case 'info':
        return 'text-blue-500';
      case 'debug':
        return 'text-gray-400';
      default:
        return 'text-gray-600';
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-3xl mx-4 max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-gray-200">
          <div className="flex items-center gap-3">
            <FileCode className="w-6 h-6 text-gray-600" />
            <div>
              <h2 className="text-lg font-medium text-gray-900">Job Details</h2>
              <p className="text-sm text-gray-500 font-mono">{job.id}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 space-y-6">
          {/* Job Info */}
          <div className="flex items-start justify-between">
            <div>
              <div className="font-mono text-lg text-gray-900">{job.jobPath}</div>
              <div className="flex items-center gap-3 mt-2">
                <span className={`px-2 py-1 text-xs text-white rounded ${getPriorityColor(job.priority)}`}>
                  {job.priority}
                </span>
                <span className={`px-2 py-1 text-xs rounded ${getStatusColor(job.status)}`}>
                  {job.status}
                </span>
                <span className="text-sm text-gray-500 capitalize">
                  {job.triggerSource.replace('_', ' ')}
                </span>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {(job.status === 'failed' || job.status === 'error') && onRetry && (
                <button
                  onClick={() => onRetry(job)}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                >
                  <RotateCcw className="w-4 h-4" />
                  Retry
                </button>
              )}
              {(job.status === 'pending' || job.status === 'running') && onCancel && (
                <button
                  onClick={() => onCancel(job)}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                >
                  <Trash2 className="w-4 h-4" />
                  Cancel
                </button>
              )}
            </div>
          </div>

          {/* Metadata Grid */}
          <div className="grid grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg">
            <div className="flex items-center gap-2">
              <GitBranch className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Branch</div>
                <div className="text-sm text-gray-900">{job.branch || 'main'}</div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Duration</div>
                <div className="text-sm text-gray-900">{formatDuration(job.duration)}</div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <User className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Triggered By</div>
                <div className="text-sm text-gray-900">{job.triggeredBy || '-'}</div>
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500">Commit SHA</div>
              <code className="text-sm bg-gray-200 px-1.5 py-0.5 rounded">{job.commitSha || '-'}</code>
            </div>
          </div>

          {/* Error Message */}
          {job.errorMessage && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <XCircle className="w-5 h-5 text-red-600" />
                <span className="font-medium text-red-900">Error</span>
              </div>
              <p className="text-sm text-red-800 font-mono">{job.errorMessage}</p>
            </div>
          )}

          {/* Validation Results */}
          {job.validationResults && job.validationResults.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-900 mb-3">Validation Results</h3>
              <div className="space-y-3">
                {job.validationResults.map((result) => (
                  <div key={result.stage} className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="flex items-center justify-between p-3 bg-gray-50">
                      <div className="flex items-center gap-2">
                        {getValidationIcon(result.status)}
                        <span className="font-medium text-gray-900 capitalize">{result.stage.replace('_', ' ')}</span>
                      </div>
                      <div className="flex items-center gap-4 text-sm text-gray-600">
                        <span>{formatDuration(result.duration)}</span>
                        <span
                          className={`px-2 py-0.5 rounded ${
                            result.status === 'passed'
                              ? 'bg-green-100 text-green-700'
                              : result.status === 'failed'
                              ? 'bg-red-100 text-red-700'
                              : result.status === 'warning'
                              ? 'bg-yellow-100 text-yellow-700'
                              : 'bg-gray-100 text-gray-600'
                          }`}
                        >
                          {result.issueCount} issues
                        </span>
                      </div>
                    </div>
                    {result.issues.length > 0 && (
                      <div className="p-3 space-y-2">
                        {result.issues.map((issue) => (
                          <div
                            key={issue.id}
                            className={`p-2 rounded text-sm ${
                              issue.severity === 'critical'
                                ? 'bg-red-50 border border-red-200'
                                : issue.severity === 'error'
                                ? 'bg-red-50 border border-red-100'
                                : issue.severity === 'warning'
                                ? 'bg-yellow-50 border border-yellow-200'
                                : 'bg-blue-50 border border-blue-200'
                            }`}
                          >
                            <div className="flex items-start gap-2">
                              <span
                                className={`px-1.5 py-0.5 rounded text-xs uppercase ${
                                  issue.severity === 'critical' || issue.severity === 'error'
                                    ? 'bg-red-100 text-red-700'
                                    : issue.severity === 'warning'
                                    ? 'bg-yellow-100 text-yellow-700'
                                    : 'bg-blue-100 text-blue-700'
                                }`}
                              >
                                {issue.severity}
                              </span>
                              <div className="flex-1">
                                <p className="text-gray-900">{issue.message}</p>
                                {issue.fileLocation && (
                                  <p className="text-gray-500 text-xs mt-1 font-mono">
                                    {issue.fileLocation}
                                    {issue.lineNumber && `:${issue.lineNumber}`}
                                  </p>
                                )}
                                {issue.suggestedFix && (
                                  <p className="text-gray-600 text-xs mt-1">
                                    Suggestion: {issue.suggestedFix}
                                  </p>
                                )}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Logs */}
          {job.logs && job.logs.length > 0 && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Terminal className="w-4 h-4 text-gray-500" />
                <h3 className="text-sm font-medium text-gray-900">Execution Logs</h3>
              </div>
              <div className="bg-gray-900 rounded-lg p-4 max-h-64 overflow-y-auto">
                <div className="font-mono text-xs space-y-1">
                  {job.logs.map((log, idx) => (
                    <div key={idx} className="flex gap-3">
                      <span className="text-gray-500 flex-shrink-0">{log.timestamp}</span>
                      <span className={`flex-shrink-0 uppercase ${getLogLevelColor(log.level)}`}>
                        [{log.level}]
                      </span>
                      <span className="text-gray-300">{log.message}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Timeline */}
          <div>
            <h3 className="text-sm font-medium text-gray-900 mb-3">Timeline</h3>
            <div className="space-y-2 text-sm">
              <div className="flex items-center gap-3">
                <div className="w-2 h-2 bg-gray-400 rounded-full" />
                <span className="text-gray-600">Created:</span>
                <span className="text-gray-900">{job.createdAt}</span>
              </div>
              {job.queuedAt && (
                <div className="flex items-center gap-3">
                  <div className="w-2 h-2 bg-blue-400 rounded-full" />
                  <span className="text-gray-600">Queued:</span>
                  <span className="text-gray-900">{job.queuedAt}</span>
                </div>
              )}
              {job.startedAt && (
                <div className="flex items-center gap-3">
                  <div className="w-2 h-2 bg-green-400 rounded-full" />
                  <span className="text-gray-600">Started:</span>
                  <span className="text-gray-900">{job.startedAt}</span>
                  {job.workerId && (
                    <span className="text-gray-500">on {job.workerId}</span>
                  )}
                </div>
              )}
              {job.completedAt && (
                <div className="flex items-center gap-3">
                  <div
                    className={`w-2 h-2 rounded-full ${
                      job.status === 'completed' ? 'bg-green-500' : 'bg-red-500'
                    }`}
                  />
                  <span className="text-gray-600">
                    {job.status === 'completed' ? 'Completed:' : 'Failed:'}
                  </span>
                  <span className="text-gray-900">{job.completedAt}</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-gray-200 bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
