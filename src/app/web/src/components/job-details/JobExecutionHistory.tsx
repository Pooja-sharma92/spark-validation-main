import { useState } from 'react';
import { MigrationJob, JobExecutionRecord } from '../../types/migration';
import {
  Clock,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Loader2,
  ChevronDown,
  ChevronRight,
  BarChart3,
  TrendingUp,
  TrendingDown,
  User,
  Calendar,
  Timer,
  Database,
  GitCommit,
  RefreshCw,
  Eye,
} from 'lucide-react';

interface JobExecutionHistoryProps {
  job: MigrationJob;
}

export function JobExecutionHistory({ job }: JobExecutionHistoryProps) {
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set());
  const [selectedForComparison, setSelectedForComparison] = useState<string[]>([]);
  const [viewMode, setViewMode] = useState<'list' | 'comparison'>('list');

  const toggleRun = (runId: string) => {
    const newExpanded = new Set(expandedRuns);
    if (newExpanded.has(runId)) {
      newExpanded.delete(runId);
    } else {
      newExpanded.add(runId);
    }
    setExpandedRuns(newExpanded);
  };

  const toggleComparison = (runId: string) => {
    if (selectedForComparison.includes(runId)) {
      setSelectedForComparison(selectedForComparison.filter(id => id !== runId));
    } else if (selectedForComparison.length < 2) {
      setSelectedForComparison([...selectedForComparison, runId]);
    }
  };

  const getStatusIcon = (status: string) => {
    const iconClass = 'w-5 h-5';
    switch (status) {
      case 'completed':
        return <CheckCircle2 className={`${iconClass} text-green-600`} />;
      case 'failed':
        return <XCircle className={`${iconClass} text-red-600`} />;
      case 'running':
        return <Loader2 className={`${iconClass} text-blue-600 animate-spin`} />;
      case 'warning':
        return <AlertTriangle className={`${iconClass} text-yellow-600`} />;
      default:
        return <Clock className={`${iconClass} text-gray-400`} />;
    }
  };

  const getStatusBadge = (status: string) => {
    const baseClass = 'px-2 py-0.5 rounded text-xs';
    switch (status) {
      case 'completed':
        return <span className={`${baseClass} bg-green-100 text-green-700`}>Completed</span>;
      case 'failed':
        return <span className={`${baseClass} bg-red-100 text-red-700`}>Failed</span>;
      case 'running':
        return <span className={`${baseClass} bg-blue-100 text-blue-700`}>Running</span>;
      case 'warning':
        return <span className={`${baseClass} bg-yellow-100 text-yellow-700`}>Warning</span>;
      default:
        return <span className={`${baseClass} bg-gray-100 text-gray-600`}>{status}</span>;
    }
  };

  const formatDuration = (ms?: number) => {
    if (!ms) return '—';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    if (ms < 3600000) return `${(ms / 60000).toFixed(1)}min`;
    return `${(ms / 3600000).toFixed(1)}h`;
  };

  const formatDateTime = (dateStr?: string) => {
    if (!dateStr) return '—';
    const date = new Date(dateStr);
    return date.toLocaleString();
  };

  const getTrendIndicator = (current: number, previous?: number) => {
    if (!previous) return null;
    const diff = ((current - previous) / previous) * 100;
    if (Math.abs(diff) < 1) return null;
    if (diff > 0) {
      return (
        <span className="flex items-center gap-0.5 text-red-600 text-xs">
          <TrendingUp className="w-3 h-3" />
          +{diff.toFixed(1)}%
        </span>
      );
    }
    return (
      <span className="flex items-center gap-0.5 text-green-600 text-xs">
        <TrendingDown className="w-3 h-3" />
        {diff.toFixed(1)}%
      </span>
    );
  };

  const executionHistory = job.executionHistory || [];
  const sortedHistory = [...executionHistory].sort(
    (a, b) => new Date(b.startTime).getTime() - new Date(a.startTime).getTime()
  );

  const comparisonRuns = selectedForComparison
    .map(id => sortedHistory.find(r => r.id === id))
    .filter(Boolean) as JobExecutionRecord[];

  const getAverageDuration = () => {
    const completedRuns = sortedHistory.filter(r => r.status === 'completed' && r.duration);
    if (completedRuns.length === 0) return null;
    const avg = completedRuns.reduce((sum, r) => sum + (r.duration || 0), 0) / completedRuns.length;
    return avg;
  };

  const getSuccessRate = () => {
    if (sortedHistory.length === 0) return 0;
    const completed = sortedHistory.filter(r => r.status === 'completed').length;
    return (completed / sortedHistory.length) * 100;
  };

  return (
    <div className="space-y-6">
      {/* Summary Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <RefreshCw className="w-4 h-4 text-gray-600" />
            <span className="text-sm text-gray-600">Total Runs</span>
          </div>
          <div className="text-2xl text-gray-900">{sortedHistory.length}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle2 className="w-4 h-4 text-green-600" />
            <span className="text-sm text-gray-600">Success Rate</span>
          </div>
          <div className="text-2xl text-gray-900">{getSuccessRate().toFixed(1)}%</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Timer className="w-4 h-4 text-blue-600" />
            <span className="text-sm text-gray-600">Avg Duration</span>
          </div>
          <div className="text-2xl text-gray-900">{formatDuration(getAverageDuration() || undefined)}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Database className="w-4 h-4 text-purple-600" />
            <span className="text-sm text-gray-600">Last Run Rows</span>
          </div>
          <div className="text-2xl text-gray-900">
            {sortedHistory[0]?.rowsProcessed?.toLocaleString() || '—'}
          </div>
        </div>
      </div>

      {/* View Mode Toggle */}
      <div className="flex items-center justify-between">
        <h3 className="text-gray-900">Execution History</h3>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setViewMode('list')}
            className={`px-3 py-1.5 rounded text-sm transition-colors ${
              viewMode === 'list'
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            List View
          </button>
          <button
            onClick={() => setViewMode('comparison')}
            disabled={selectedForComparison.length < 2}
            className={`px-3 py-1.5 rounded text-sm transition-colors ${
              viewMode === 'comparison'
                ? 'bg-blue-600 text-white'
                : selectedForComparison.length < 2
                ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            <div className="flex items-center gap-1.5">
              <BarChart3 className="w-4 h-4" />
              Compare ({selectedForComparison.length}/2)
            </div>
          </button>
        </div>
      </div>

      {/* Comparison View */}
      {viewMode === 'comparison' && comparisonRuns.length === 2 && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="bg-gray-50 px-4 py-3 border-b border-gray-200">
            <h4 className="text-gray-900">Run Comparison</h4>
          </div>
          <div className="divide-y divide-gray-200">
            <div className="grid grid-cols-3 px-4 py-2 bg-gray-50 text-sm text-gray-600">
              <div>Metric</div>
              <div className="text-center">Run {comparisonRuns[0].id.slice(-6)}</div>
              <div className="text-center">Run {comparisonRuns[1].id.slice(-6)}</div>
            </div>
            <div className="grid grid-cols-3 px-4 py-3 text-sm">
              <div className="text-gray-600">Status</div>
              <div className="text-center">{getStatusBadge(comparisonRuns[0].status)}</div>
              <div className="text-center">{getStatusBadge(comparisonRuns[1].status)}</div>
            </div>
            <div className="grid grid-cols-3 px-4 py-3 text-sm">
              <div className="text-gray-600">Duration</div>
              <div className="text-center text-gray-900">{formatDuration(comparisonRuns[0].duration)}</div>
              <div className="text-center text-gray-900">
                {formatDuration(comparisonRuns[1].duration)}
                {getTrendIndicator(comparisonRuns[1].duration || 0, comparisonRuns[0].duration)}
              </div>
            </div>
            <div className="grid grid-cols-3 px-4 py-3 text-sm">
              <div className="text-gray-600">Rows Processed</div>
              <div className="text-center text-gray-900">{comparisonRuns[0].rowsProcessed?.toLocaleString() || '—'}</div>
              <div className="text-center text-gray-900">{comparisonRuns[1].rowsProcessed?.toLocaleString() || '—'}</div>
            </div>
            <div className="grid grid-cols-3 px-4 py-3 text-sm">
              <div className="text-gray-600">Rows Rejected</div>
              <div className="text-center text-gray-900">{comparisonRuns[0].rowsRejected?.toLocaleString() || '—'}</div>
              <div className="text-center text-gray-900">{comparisonRuns[1].rowsRejected?.toLocaleString() || '—'}</div>
            </div>
            {comparisonRuns[0].performanceMetrics && comparisonRuns[1].performanceMetrics && (
              <>
                <div className="grid grid-cols-3 px-4 py-3 text-sm">
                  <div className="text-gray-600">CPU Usage</div>
                  <div className="text-center text-gray-900">{comparisonRuns[0].performanceMetrics.cpuUsage}%</div>
                  <div className="text-center text-gray-900">
                    {comparisonRuns[1].performanceMetrics.cpuUsage}%
                    {getTrendIndicator(
                      comparisonRuns[1].performanceMetrics.cpuUsage,
                      comparisonRuns[0].performanceMetrics.cpuUsage
                    )}
                  </div>
                </div>
                <div className="grid grid-cols-3 px-4 py-3 text-sm">
                  <div className="text-gray-600">Memory Usage</div>
                  <div className="text-center text-gray-900">{comparisonRuns[0].performanceMetrics.memoryUsage}%</div>
                  <div className="text-center text-gray-900">
                    {comparisonRuns[1].performanceMetrics.memoryUsage}%
                    {getTrendIndicator(
                      comparisonRuns[1].performanceMetrics.memoryUsage,
                      comparisonRuns[0].performanceMetrics.memoryUsage
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Run List */}
      {viewMode === 'list' && (
        <div className="space-y-3">
          {sortedHistory.length === 0 ? (
            <div className="bg-gray-50 rounded-lg p-8 text-center text-gray-500">
              No execution history available
            </div>
          ) : (
            sortedHistory.map((run, index) => {
              const isExpanded = expandedRuns.has(run.id);
              const isSelected = selectedForComparison.includes(run.id);
              const previousRun = sortedHistory[index + 1];

              return (
                <div
                  key={run.id}
                  className={`border rounded-lg overflow-hidden transition-all ${
                    isSelected ? 'border-blue-400 bg-blue-50' : 'border-gray-200 bg-white'
                  }`}
                >
                  <div className="p-4">
                    <div className="flex items-center gap-4">
                      {/* Comparison Checkbox */}
                      <button
                        onClick={() => toggleComparison(run.id)}
                        className={`w-5 h-5 rounded border-2 flex items-center justify-center transition-colors ${
                          isSelected
                            ? 'bg-blue-600 border-blue-600 text-white'
                            : 'border-gray-300 hover:border-blue-400'
                        }`}
                      >
                        {isSelected && <CheckCircle2 className="w-3 h-3" />}
                      </button>

                      {/* Status Icon */}
                      {getStatusIcon(run.status)}

                      {/* Run Info */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-3 mb-1">
                          <span className="text-gray-900 font-mono text-sm">
                            #{run.id.slice(-8)}
                          </span>
                          {getStatusBadge(run.status)}
                          {index === 0 && (
                            <span className="px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs">
                              Latest
                            </span>
                          )}
                        </div>
                        <div className="flex items-center gap-4 text-sm text-gray-600">
                          <span className="flex items-center gap-1">
                            <Calendar className="w-3.5 h-3.5" />
                            {formatDateTime(run.startTime)}
                          </span>
                          <span className="flex items-center gap-1">
                            <Timer className="w-3.5 h-3.5" />
                            {formatDuration(run.duration)}
                            {previousRun && getTrendIndicator(run.duration || 0, previousRun.duration)}
                          </span>
                          {run.triggeredBy && (
                            <span className="flex items-center gap-1">
                              <User className="w-3.5 h-3.5" />
                              {run.triggeredBy}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Metrics Summary */}
                      <div className="flex items-center gap-6 text-sm">
                        <div className="text-center">
                          <div className="text-gray-500 text-xs">Processed</div>
                          <div className="text-gray-900">{run.rowsProcessed?.toLocaleString() || '—'}</div>
                        </div>
                        <div className="text-center">
                          <div className="text-gray-500 text-xs">Rejected</div>
                          <div className={run.rowsRejected && run.rowsRejected > 0 ? 'text-red-600' : 'text-gray-900'}>
                            {run.rowsRejected?.toLocaleString() || '0'}
                          </div>
                        </div>
                      </div>

                      {/* Expand Button */}
                      <button
                        onClick={() => toggleRun(run.id)}
                        className="p-1.5 hover:bg-gray-100 rounded transition-colors"
                      >
                        {isExpanded ? (
                          <ChevronDown className="w-5 h-5 text-gray-400" />
                        ) : (
                          <ChevronRight className="w-5 h-5 text-gray-400" />
                        )}
                      </button>
                    </div>
                  </div>

                  {/* Expanded Details */}
                  {isExpanded && (
                    <div className="border-t border-gray-200 bg-gray-50 p-4 space-y-4">
                      {/* Time Details */}
                      <div className="grid grid-cols-3 gap-4">
                        <div>
                          <div className="text-xs text-gray-500 mb-1">Start Time</div>
                          <div className="text-sm text-gray-900 font-mono">{formatDateTime(run.startTime)}</div>
                        </div>
                        <div>
                          <div className="text-xs text-gray-500 mb-1">End Time</div>
                          <div className="text-sm text-gray-900 font-mono">{formatDateTime(run.endTime)}</div>
                        </div>
                        <div>
                          <div className="text-xs text-gray-500 mb-1">Version</div>
                          <div className="text-sm text-gray-900 flex items-center gap-1">
                            <GitCommit className="w-3.5 h-3.5" />
                            {run.version || '—'}
                          </div>
                        </div>
                      </div>

                      {/* Performance Metrics */}
                      {run.performanceMetrics && (
                        <div>
                          <div className="text-sm text-gray-700 mb-2 flex items-center gap-1">
                            <BarChart3 className="w-4 h-4" />
                            Performance Metrics
                          </div>
                          <div className="grid grid-cols-4 gap-4">
                            <div className="bg-white rounded p-3 border border-gray-200">
                              <div className="text-xs text-gray-500 mb-1">CPU Usage</div>
                              <div className="text-lg text-gray-900">{run.performanceMetrics.cpuUsage}%</div>
                              <div className="w-full bg-gray-200 rounded-full h-1.5 mt-2">
                                <div
                                  className="bg-blue-600 h-1.5 rounded-full"
                                  style={{ width: `${run.performanceMetrics.cpuUsage}%` }}
                                />
                              </div>
                            </div>
                            <div className="bg-white rounded p-3 border border-gray-200">
                              <div className="text-xs text-gray-500 mb-1">Memory Usage</div>
                              <div className="text-lg text-gray-900">{run.performanceMetrics.memoryUsage}%</div>
                              <div className="w-full bg-gray-200 rounded-full h-1.5 mt-2">
                                <div
                                  className="bg-green-600 h-1.5 rounded-full"
                                  style={{ width: `${run.performanceMetrics.memoryUsage}%` }}
                                />
                              </div>
                            </div>
                            <div className="bg-white rounded p-3 border border-gray-200">
                              <div className="text-xs text-gray-500 mb-1">Disk I/O</div>
                              <div className="text-lg text-gray-900">{run.performanceMetrics.diskIO} MB/s</div>
                            </div>
                            <div className="bg-white rounded p-3 border border-gray-200">
                              <div className="text-xs text-gray-500 mb-1">Network</div>
                              <div className="text-lg text-gray-900">{run.performanceMetrics.networkIO} MB/s</div>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Error Message */}
                      {run.errorMessage && (
                        <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                          <div className="flex items-start gap-2">
                            <XCircle className="w-4 h-4 text-red-600 mt-0.5 flex-shrink-0" />
                            <div>
                              <div className="text-sm text-red-900 mb-1">Error Message</div>
                              <div className="text-sm text-red-700 font-mono">{run.errorMessage}</div>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Actions */}
                      <div className="flex items-center gap-2 pt-2">
                        <button className="px-3 py-1.5 bg-white border border-gray-200 rounded text-sm text-gray-700 hover:bg-gray-50 transition-colors flex items-center gap-1.5">
                          <Eye className="w-4 h-4" />
                          View Full Logs
                        </button>
                        <button className="px-3 py-1.5 bg-white border border-gray-200 rounded text-sm text-gray-700 hover:bg-gray-50 transition-colors flex items-center gap-1.5">
                          <RefreshCw className="w-4 h-4" />
                          Rerun Job
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}
