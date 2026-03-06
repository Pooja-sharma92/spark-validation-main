import { useState } from 'react';
import { ValidationJob, ExecutorValidationStage, ExecutorValidationIssue } from '../../types/queue';
import {
  X,
  FileCode,
  GitBranch,
  User,
  Clock,
  Server,
  Tag,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Info,
  ChevronDown,
  ChevronRight,
  Copy,
  Check,
} from 'lucide-react';

interface Props {
  job: ValidationJob;
  onClose: () => void;
}

type TabType = 'input' | 'output' | 'logs';

export function ValidationJobDetailModal({ job, onClose }: Props) {
  const [activeTab, setActiveTab] = useState<TabType>('output');
  const [expandedStages, setExpandedStages] = useState<Set<string>>(new Set(['syntax', 'imports', 'sql', 'logic']));
  const [copied, setCopied] = useState(false);

  const toggleStage = (stage: string) => {
    const newExpanded = new Set(expandedStages);
    if (newExpanded.has(stage)) {
      newExpanded.delete(stage);
    } else {
      newExpanded.add(stage);
    }
    setExpandedStages(newExpanded);
  };

  const copyJobId = () => {
    navigator.clipboard.writeText(job.id);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const formatDate = (dateStr?: string) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString();
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 0.001) return '<0.001s';
    if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
    return `${seconds.toFixed(3)}s`;
  };

  const getSeverityIcon = (severity: string) => {
    switch (severity.toUpperCase()) {
      case 'ERROR':
        return <XCircle className="w-4 h-4 text-red-500" />;
      case 'WARNING':
        return <AlertTriangle className="w-4 h-4 text-yellow-500" />;
      case 'INFO':
        return <Info className="w-4 h-4 text-blue-500" />;
      default:
        return <Info className="w-4 h-4 text-gray-500" />;
    }
  };

  const getSeverityBadge = (severity: string) => {
    switch (severity.toUpperCase()) {
      case 'ERROR':
        return 'bg-red-100 text-red-700 border-red-200';
      case 'WARNING':
        return 'bg-yellow-100 text-yellow-700 border-yellow-200';
      case 'INFO':
        return 'bg-blue-100 text-blue-700 border-blue-200';
      default:
        return 'bg-gray-100 text-gray-700 border-gray-200';
    }
  };

  const tabs: { id: TabType; label: string }[] = [
    { id: 'input', label: 'Input' },
    { id: 'output', label: 'Output' },
    { id: 'logs', label: 'Logs' },
  ];

  const renderInputTab = () => (
    <div className="space-y-6">
      {/* Job Metadata */}
      <div>
        <h3 className="text-sm font-medium text-gray-700 mb-3">Job Information</h3>
        <div className="bg-gray-50 rounded-lg p-4 space-y-3">
          <div className="flex items-center gap-3">
            <FileCode className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Job Path</div>
              <div className="text-sm font-mono text-gray-900 break-all">{job.jobPath}</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Tag className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Job ID</div>
              <div className="text-sm font-mono text-gray-900 flex items-center gap-2">
                {job.id}
                <button
                  onClick={copyJobId}
                  className="p-1 hover:bg-gray-200 rounded transition-colors"
                  title="Copy Job ID"
                >
                  {copied ? (
                    <Check className="w-3 h-3 text-green-500" />
                  ) : (
                    <Copy className="w-3 h-3 text-gray-400" />
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Trigger Information */}
      <div>
        <h3 className="text-sm font-medium text-gray-700 mb-3">Trigger Information</h3>
        <div className="bg-gray-50 rounded-lg p-4 grid grid-cols-2 gap-4">
          <div className="flex items-center gap-3">
            <Server className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Trigger Source</div>
              <div className="text-sm text-gray-900 capitalize">{job.triggerSource}</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Tag className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Priority</div>
              <div className="text-sm text-gray-900">{job.priority}</div>
            </div>
          </div>
          {job.branch && (
            <div className="flex items-center gap-3">
              <GitBranch className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Branch</div>
                <div className="text-sm text-gray-900">{job.branch}</div>
              </div>
            </div>
          )}
          {job.commitSha && (
            <div className="flex items-center gap-3">
              <GitBranch className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Commit SHA</div>
                <div className="text-sm font-mono text-gray-900">{job.commitSha}</div>
              </div>
            </div>
          )}
          {job.triggeredBy && (
            <div className="flex items-center gap-3">
              <User className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Triggered By</div>
                <div className="text-sm text-gray-900">{job.triggeredBy}</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Timing Information */}
      <div>
        <h3 className="text-sm font-medium text-gray-700 mb-3">Execution Timeline</h3>
        <div className="bg-gray-50 rounded-lg p-4 space-y-3">
          <div className="flex items-center gap-3">
            <Clock className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Created At</div>
              <div className="text-sm text-gray-900">{formatDate(job.createdAt)}</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Clock className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Started At</div>
              <div className="text-sm text-gray-900">{formatDate(job.startedAt)}</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Clock className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-500">Completed At</div>
              <div className="text-sm text-gray-900">{formatDate(job.completedAt)}</div>
            </div>
          </div>
          {job.workerId && (
            <div className="flex items-center gap-3">
              <Server className="w-4 h-4 text-gray-400" />
              <div>
                <div className="text-xs text-gray-500">Worker ID</div>
                <div className="text-sm font-mono text-gray-900">{job.workerId}</div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const renderOutputTab = () => (
    <div className="space-y-6">
      {/* Overall Result */}
      <div className={`rounded-lg p-4 border ${
        job.result?.passed
          ? 'bg-green-50 border-green-200'
          : 'bg-red-50 border-red-200'
      }`}>
        <div className="flex items-center gap-3">
          {job.result?.passed ? (
            <CheckCircle className="w-6 h-6 text-green-600" />
          ) : (
            <XCircle className="w-6 h-6 text-red-600" />
          )}
          <div>
            <div className={`font-medium ${job.result?.passed ? 'text-green-800' : 'text-red-800'}`}>
              Validation {job.result?.passed ? 'Passed' : 'Failed'}
            </div>
            <div className={`text-sm ${job.result?.passed ? 'text-green-600' : 'text-red-600'}`}>
              {job.result?.stages?.length || 0} stages executed
            </div>
          </div>
        </div>
      </div>

      {/* Error Message */}
      {job.errorMessage && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
            <div>
              <div className="font-medium text-red-800">Error</div>
              <div className="text-sm text-red-700 mt-1">{job.errorMessage}</div>
            </div>
          </div>
        </div>
      )}

      {/* Stages */}
      {job.result?.stages && (
        <div>
          <h3 className="text-sm font-medium text-gray-700 mb-3">Validation Stages</h3>
          <div className="space-y-2">
            {job.result.stages.map((stage: ExecutorValidationStage) => (
              <div
                key={stage.stage}
                className="border border-gray-200 rounded-lg overflow-hidden"
              >
                <button
                  onClick={() => toggleStage(stage.stage)}
                  className={`w-full px-4 py-3 flex items-center justify-between transition-colors ${
                    stage.passed ? 'bg-white hover:bg-gray-50' : 'bg-red-50 hover:bg-red-100'
                  }`}
                >
                  <div className="flex items-center gap-3">
                    {stage.passed ? (
                      <CheckCircle className="w-5 h-5 text-green-500" />
                    ) : (
                      <XCircle className="w-5 h-5 text-red-500" />
                    )}
                    <span className="font-medium text-gray-900 capitalize">{stage.stage}</span>
                    {stage.issues && stage.issues.length > 0 && (
                      <span className={`px-2 py-0.5 text-xs rounded-full ${
                        stage.passed
                          ? 'bg-yellow-100 text-yellow-700'
                          : 'bg-red-100 text-red-700'
                      }`}>
                        {stage.issues.length} issue{stage.issues.length > 1 ? 's' : ''}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-sm text-gray-500">
                      {formatDuration(stage.duration_seconds)}
                    </span>
                    {expandedStages.has(stage.stage) ? (
                      <ChevronDown className="w-4 h-4 text-gray-400" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    )}
                  </div>
                </button>

                {expandedStages.has(stage.stage) && (
                  <div className="px-4 py-3 border-t border-gray-200 bg-gray-50">
                    {/* Stage Details */}
                    {stage.details && Object.keys(stage.details).length > 0 && (
                      <div className="mb-3">
                        <div className="text-xs text-gray-500 mb-1">Details</div>
                        <div className="text-sm font-mono bg-white rounded p-2 border border-gray-200">
                          {Object.entries(stage.details).map(([key, value]) => (
                            <div key={key}>
                              <span className="text-gray-600">{key}:</span>{' '}
                              <span className="text-gray-900">{String(value)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Issues */}
                    {stage.issues && stage.issues.length > 0 ? (
                      <div className="space-y-2">
                        {stage.issues.map((issue: ExecutorValidationIssue, idx: number) => (
                          <div
                            key={idx}
                            className={`p-3 rounded-lg border ${getSeverityBadge(issue.severity)}`}
                          >
                            <div className="flex items-start gap-2">
                              {getSeverityIcon(issue.severity)}
                              <div className="flex-1">
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-medium uppercase">
                                    {issue.severity}
                                  </span>
                                  {issue.line && (
                                    <span className="text-xs text-gray-500">
                                      Line {issue.line}
                                      {issue.column && `:${issue.column}`}
                                    </span>
                                  )}
                                </div>
                                <div className="text-sm mt-1">{issue.message}</div>
                                {issue.suggestion && (
                                  <div className="text-xs text-gray-600 mt-1 italic">
                                    Suggestion: {issue.suggestion}
                                  </div>
                                )}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-gray-500 italic">No issues found</div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );

  const renderLogsTab = () => {
    // Generate log entries from the validation result
    const logs: { timestamp: string; level: string; message: string }[] = [];

    if (job.createdAt) {
      logs.push({
        timestamp: job.createdAt,
        level: 'INFO',
        message: `Job created: ${job.jobPath}`,
      });
    }

    if (job.startedAt) {
      logs.push({
        timestamp: job.startedAt,
        level: 'INFO',
        message: `Validation started by worker: ${job.workerId || 'unknown'}`,
      });
    }

    if (job.result?.stages) {
      job.result.stages.forEach((stage) => {
        const stageTime = job.result?.started_at || job.startedAt || '';
        logs.push({
          timestamp: stageTime,
          level: stage.passed ? 'INFO' : 'ERROR',
          message: `Stage [${stage.stage}]: ${stage.passed ? 'PASSED' : 'FAILED'} (${formatDuration(stage.duration_seconds)})`,
        });

        stage.issues?.forEach((issue) => {
          logs.push({
            timestamp: stageTime,
            level: issue.severity,
            message: `  [${stage.stage}] ${issue.message}${issue.line ? ` (line ${issue.line})` : ''}`,
          });
        });
      });
    }

    if (job.completedAt) {
      logs.push({
        timestamp: job.completedAt,
        level: job.result?.passed ? 'INFO' : 'ERROR',
        message: `Validation ${job.result?.passed ? 'completed successfully' : 'failed'}`,
      });
    }

    if (job.errorMessage) {
      logs.push({
        timestamp: job.completedAt || job.createdAt,
        level: 'ERROR',
        message: `Error: ${job.errorMessage}`,
      });
    }

    return (
      <div className="bg-gray-900 rounded-lg p-4 font-mono text-sm overflow-x-auto">
        {logs.length === 0 ? (
          <div className="text-gray-500 italic">No logs available</div>
        ) : (
          <div className="space-y-1">
            {logs.map((log, idx) => (
              <div key={idx} className="flex items-start gap-3">
                <span className="text-gray-500 whitespace-nowrap">
                  {new Date(log.timestamp).toLocaleTimeString()}
                </span>
                <span
                  className={`w-16 ${
                    log.level === 'ERROR'
                      ? 'text-red-400'
                      : log.level === 'WARNING'
                      ? 'text-yellow-400'
                      : 'text-green-400'
                  }`}
                >
                  [{log.level}]
                </span>
                <span className="text-gray-300 whitespace-pre-wrap">{log.message}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-3">
            {job.result?.passed ? (
              <CheckCircle className="w-6 h-6 text-green-500" />
            ) : (
              <XCircle className="w-6 h-6 text-red-500" />
            )}
            <div>
              <h2 className="text-lg font-semibold text-gray-900">
                {job.jobPath.split('/').pop()}
              </h2>
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

        {/* Tabs */}
        <div className="border-b border-gray-200">
          <div className="flex px-6">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {activeTab === 'input' && renderInputTab()}
          {activeTab === 'output' && renderOutputTab()}
          {activeTab === 'logs' && renderLogsTab()}
        </div>
      </div>
    </div>
  );
}
