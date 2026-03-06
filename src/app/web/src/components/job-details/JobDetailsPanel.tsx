import { MigrationJob } from '../../types/migration';
import { JobValidationSteps } from './JobValidationSteps';
import { JobExecutionHistory } from './JobExecutionHistory';
import { JobSourceCodeViewer } from './JobSourceCodeViewer';
import { useState } from 'react';
import {
  X,
  Activity,
  Database,
  Clock,
  Zap,
  Tag,
  GitBranch,
  Server,
  Hash,
  Cpu,
  HardDrive,
  Timer,
  AlertCircle,
  CheckCircle2,
  BarChart3,
  Code,
  History,
} from 'lucide-react';

interface JobDetailsPanelProps {
  job: MigrationJob;
  onClose: () => void;
}

type TabType = 'validation' | 'history' | 'code' | 'overview';

export function JobDetailsPanel({ job, onClose }: JobDetailsPanelProps) {
  const [activeTab, setActiveTab] = useState<TabType>('validation');

  const calculateEstimateAccuracy = () => {
    if (!job.estimatedDuration || !job.actualDuration) return null;
    const diff = ((job.actualDuration - job.estimatedDuration) / job.estimatedDuration) * 100;
    return {
      percentage: Math.abs(diff),
      isAccurate: Math.abs(diff) <= 10,
      isFaster: diff < 0,
    };
  };

  const estimateAccuracy = calculateEstimateAccuracy();

  return (
    <div className="fixed inset-y-0 right-0 w-full md:w-3/4 lg:w-2/3 xl:w-1/2 bg-white shadow-2xl z-50 overflow-hidden flex flex-col">
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 to-blue-700 text-white p-6 flex-shrink-0">
        <div className="flex items-start justify-between mb-4">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-2">
              <span className="px-2 py-1 bg-white/20 rounded text-xs">
                {job.sparkJobId || job.id}
              </span>
              <span className={`px-2 py-1 rounded text-xs ${
                job.complexity === 'critical' ? 'bg-red-500' :
                job.complexity === 'complex' ? 'bg-orange-500' :
                job.complexity === 'medium' ? 'bg-yellow-500' :
                'bg-green-500'
              }`}>
                {job.complexity?.toUpperCase()}
              </span>
              <span className={`px-2 py-1 rounded text-xs ${
                job.priority === 'high' ? 'bg-red-500' :
                job.priority === 'medium' ? 'bg-yellow-500' :
                'bg-gray-500'
              }`}>
                {job.priority?.toUpperCase()}
              </span>
            </div>
            <h2 className="text-2xl mb-2">{job.name}</h2>
            <p className="text-blue-100 text-sm">
              <Database className="w-4 h-4 inline mr-1" />
              {job.sourceTable} → {job.targetTable}
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-white/10 rounded transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Quick Stats */}
        <div className="grid grid-cols-4 gap-4 mt-4">
          <div>
            <div className="text-blue-200 text-xs mb-1">Domain</div>
            <div className="text-sm">{job.domain}</div>
          </div>
          <div>
            <div className="text-blue-200 text-xs mb-1">Owner</div>
            <div className="text-sm">{job.owner || '—'}</div>
          </div>
          <div>
            <div className="text-blue-200 text-xs mb-1">Progress</div>
            <div className="text-sm">{job.progress}%</div>
          </div>
          <div>
            <div className="text-blue-200 text-xs mb-1">Status</div>
            <div className={`text-sm px-2 py-0.5 rounded inline-block ${
              job.status === 'completed' ? 'bg-green-500' :
              job.status === 'failed' ? 'bg-red-500' :
              job.status === 'running' ? 'bg-blue-500' :
              'bg-gray-500'
            }`}>
              {job.status}
            </div>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Tab Navigation */}
        <div className="flex items-center gap-4 mb-4">
          <button
            className={`px-3 py-1 rounded ${
              activeTab === 'overview' ? 'bg-blue-500 text-white' : 'bg-gray-200 text-gray-700'
            }`}
            onClick={() => setActiveTab('overview')}
          >
            <Database className="w-4 h-4 inline mr-1" />
            Overview
          </button>
          <button
            className={`px-3 py-1 rounded ${
              activeTab === 'validation' ? 'bg-blue-500 text-white' : 'bg-gray-200 text-gray-700'
            }`}
            onClick={() => setActiveTab('validation')}
          >
            <CheckCircle2 className="w-4 h-4 inline mr-1" />
            Validation
          </button>
          <button
            className={`px-3 py-1 rounded ${
              activeTab === 'history' ? 'bg-blue-500 text-white' : 'bg-gray-200 text-gray-700'
            }`}
            onClick={() => setActiveTab('history')}
          >
            <History className="w-4 h-4 inline mr-1" />
            History
          </button>
          <button
            className={`px-3 py-1 rounded ${
              activeTab === 'code' ? 'bg-blue-500 text-white' : 'bg-gray-200 text-gray-700'
            }`}
            onClick={() => setActiveTab('code')}
          >
            <Code className="w-4 h-4 inline mr-1" />
            Code
          </button>
        </div>

        {/* Overview Tab */}
        {activeTab === 'overview' && (
          <div className="space-y-6">
            {/* Performance Metrics */}
            {job.performance && (
              <div className="bg-white border border-gray-200 rounded-lg p-6">
                <div className="flex items-center gap-2 mb-4">
                  <Activity className="w-5 h-5 text-blue-600" />
                  <h3 className="text-gray-900">Performance Metrics</h3>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="p-4 bg-blue-50 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <Zap className="w-4 h-4 text-blue-600" />
                      <div className="text-xs text-blue-700">Throughput</div>
                    </div>
                    <div className="text-2xl text-blue-900">
                      {job.performance.rowsPerSecond?.toLocaleString()}
                    </div>
                    <div className="text-xs text-blue-700 mt-1">rows/sec</div>
                  </div>
                  <div className="p-4 bg-purple-50 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <Cpu className="w-4 h-4 text-purple-600" />
                      <div className="text-xs text-purple-700">CPU Usage</div>
                    </div>
                    <div className="text-2xl text-purple-900">{job.performance.cpuUsage}%</div>
                    <div className="w-full bg-purple-200 rounded-full h-1.5 mt-2">
                      <div className="bg-purple-600 h-1.5 rounded-full" style={{ width: `${job.performance.cpuUsage}%` }} />
                    </div>
                  </div>
                  <div className="p-4 bg-green-50 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <HardDrive className="w-4 h-4 text-green-600" />
                      <div className="text-xs text-green-700">Memory</div>
                    </div>
                    <div className="text-2xl text-green-900">{job.performance.memoryUsage?.toFixed(1)}</div>
                    <div className="text-xs text-green-700 mt-1">GB</div>
                  </div>
                  <div className="p-4 bg-orange-50 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <Timer className="w-4 h-4 text-orange-600" />
                      <div className="text-xs text-orange-700">I/O Wait</div>
                    </div>
                    <div className="text-2xl text-orange-900">{job.performance.ioWaitTime}</div>
                    <div className="text-xs text-orange-700 mt-1">ms</div>
                  </div>
                </div>
              </div>
            )}

            {/* Execution Details */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <div className="flex items-center gap-2 mb-4">
                <Clock className="w-5 h-5 text-gray-600" />
                <h3 className="text-gray-900">Execution Details</h3>
              </div>
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <div className="text-gray-600 mb-1">Start Time</div>
                  <div className="text-gray-900 font-mono">{job.startTime || '—'}</div>
                </div>
                <div>
                  <div className="text-gray-600 mb-1">End Time</div>
                  <div className="text-gray-900 font-mono">{job.endTime || '—'}</div>
                </div>
                <div>
                  <div className="text-gray-600 mb-1">Estimated Duration</div>
                  <div className="text-gray-900">{job.estimatedDuration ? `${job.estimatedDuration} min` : '—'}</div>
                </div>
                <div>
                  <div className="text-gray-600 mb-1">Actual Duration</div>
                  <div className="flex items-center gap-2">
                    <span className="text-gray-900">{job.actualDuration ? `${job.actualDuration} min` : '—'}</span>
                    {estimateAccuracy && (
                      <span className={`text-xs px-2 py-0.5 rounded ${
                        estimateAccuracy.isAccurate ? 'bg-green-100 text-green-700' :
                        estimateAccuracy.isFaster ? 'bg-blue-100 text-blue-700' :
                        'bg-orange-100 text-orange-700'
                      }`}>
                        {estimateAccuracy.isFaster ? '-' : '+'}{estimateAccuracy.percentage.toFixed(0)}%
                      </span>
                    )}
                  </div>
                </div>
                <div>
                  <div className="text-gray-600 mb-1">Execution Node</div>
                  <div className="flex items-center gap-1 text-gray-900">
                    <Server className="w-4 h-4 text-gray-600" />
                    <span className="font-mono text-xs">{job.metadata?.executionNode || '—'}</span>
                  </div>
                </div>
                <div>
                  <div className="text-gray-600 mb-1">Retry Count</div>
                  <div className="text-gray-900">{job.metadata?.retryCount || 0}</div>
                </div>
              </div>
            </div>

            {/* Data Statistics */}
            <div className="bg-white border border-gray-200 rounded-lg p-6">
              <div className="flex items-center gap-2 mb-4">
                <BarChart3 className="w-5 h-5 text-green-600" />
                <h3 className="text-gray-900">Data Statistics</h3>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
                <div className="p-3 bg-gray-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Source Rows</div>
                  <div className="text-xl text-gray-900">{job.sourceRowCount.toLocaleString()}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Target Rows</div>
                  <div className="text-xl text-gray-900">{job.targetRowCount.toLocaleString()}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Validated</div>
                  <div className="text-xl text-green-700">{job.validatedRowCount.toLocaleString()}</div>
                </div>
                <div className="p-3 bg-red-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Failed Rows</div>
                  <div className="text-xl text-red-700">{job.failedRowCount.toLocaleString()}</div>
                </div>
                <div className="p-3 bg-yellow-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Mismatches</div>
                  <div className="text-xl text-yellow-700">{job.mismatchCount.toLocaleString()}</div>
                </div>
                <div className="p-3 bg-blue-50 rounded">
                  <div className="text-gray-600 text-xs mb-1">Success Rate</div>
                  <div className="text-xl text-blue-700">
                    {job.sourceRowCount > 0
                      ? ((job.targetRowCount / job.sourceRowCount) * 100).toFixed(1)
                      : 0}%
                  </div>
                </div>
              </div>
            </div>

            {/* Dependencies */}
            {job.dependencies && job.dependencies.length > 0 && (
              <div className="bg-white border border-gray-200 rounded-lg p-6">
                <div className="flex items-center gap-2 mb-4">
                  <GitBranch className="w-5 h-5 text-purple-600" />
                  <h3 className="text-gray-900">Dependencies</h3>
                </div>
                <div className="space-y-2">
                  {job.dependencies.map(depId => (
                    <div key={depId} className="p-3 bg-purple-50 rounded border border-purple-200 text-sm">
                      <div className="flex items-center gap-2">
                        <AlertCircle className="w-4 h-4 text-purple-600" />
                        <span className="text-purple-900 font-mono">{depId}</span>
                        <span className="text-purple-700">must complete first</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Tags */}
            {job.tags && job.tags.length > 0 && (
              <div className="bg-white border border-gray-200 rounded-lg p-6">
                <div className="flex items-center gap-2 mb-4">
                  <Tag className="w-5 h-5 text-gray-600" />
                  <h3 className="text-gray-900">Tags</h3>
                </div>
                <div className="flex flex-wrap gap-2">
                  {job.tags.map(tag => (
                    <span key={tag} className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm border border-gray-300">
                      {tag}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Debug Information */}
            <div className="bg-gray-900 border border-gray-700 rounded-lg p-6">
              <div className="flex items-center gap-2 mb-4">
                <Hash className="w-5 h-5 text-green-400" />
                <h3 className="text-green-400">Debug Information</h3>
              </div>
              <div className="font-mono text-xs text-green-400 space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-500">Job ID:</span>
                  <span>{job.id}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Spark Job ID:</span>
                  <span>{job.sparkJobId}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Config Hash:</span>
                  <span>{job.metadata?.configHash}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Last Modified:</span>
                  <span>{job.metadata?.lastModified}</span>
                </div>
                {job.error && (
                  <div className="mt-4 p-3 bg-red-900/20 border border-red-700 rounded">
                    <div className="text-red-400 mb-1">Error Message:</div>
                    <div className="text-red-300">{job.error}</div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Validation Tab */}
        {activeTab === 'validation' && (
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
            <JobValidationSteps job={job} />
          </div>
        )}

        {/* History Tab */}
        {activeTab === 'history' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <JobExecutionHistory job={job} />
          </div>
        )}

        {/* Code Tab */}
        {activeTab === 'code' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <JobSourceCodeViewer job={job} />
          </div>
        )}
      </div>
    </div>
  );
}
