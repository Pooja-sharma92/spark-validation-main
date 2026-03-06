import { useState } from 'react';
import { mockJobs } from '../../data/migrationMockData';
import { mockJobDependencies } from '../../data/jobGroupsData';
import {
  GitBranch,
  ArrowRight,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  AlertTriangle,
  Eye,
  Info,
} from 'lucide-react';

export function DependencyView() {
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'graph' | 'list'>('graph');

  const getJobById = (jobId: string) => mockJobs.find(j => j.id === jobId);

  const getDependencyInfo = (jobId: string) => {
    return mockJobDependencies.find(d => d.jobId === jobId);
  };

  const getStatusIcon = (status: string, size = 'w-5 h-5') => {
    switch (status) {
      case 'completed': return <CheckCircle2 className={`${size} text-green-600`} />;
      case 'running': return <Loader2 className={`${size} text-blue-600 animate-spin`} />;
      case 'failed': return <XCircle className={`${size} text-red-600`} />;
      case 'validating': return <Eye className={`${size} text-purple-600`} />;
      case 'pending': return <Clock className={`${size} text-gray-600`} />;
      default: return null;
    }
  };

  const canJobStart = (jobId: string) => {
    const depInfo = getDependencyInfo(jobId);
    if (!depInfo || depInfo.dependsOn.length === 0) return true;
    return depInfo.dependsOn.every(depId => {
      const job = getJobById(depId);
      return job?.status === 'completed';
    });
  };

  const isJobBlocked = (jobId: string) => {
    const job = getJobById(jobId);
    if (!job || job.status === 'completed') return false;
    return !canJobStart(jobId);
  };

  const getExecutionLayers = () => {
    const layers: string[][] = [];
    const processed = new Set<string>();
    const allJobIds = mockJobs.map(j => j.id);

    while (processed.size < allJobIds.length) {
      const currentLayer: string[] = [];

      for (const jobId of allJobIds) {
        if (processed.has(jobId)) continue;
        const depInfo = getDependencyInfo(jobId);
        const dependencies = depInfo?.dependsOn || [];
        if (dependencies.every(depId => processed.has(depId))) {
          currentLayer.push(jobId);
        }
      }

      if (currentLayer.length === 0 && processed.size < allJobIds.length) {
        const remaining = allJobIds.filter(id => !processed.has(id));
        currentLayer.push(...remaining);
      }

      layers.push(currentLayer);
      currentLayer.forEach(id => processed.add(id));
    }

    return layers;
  };

  const executionLayers = getExecutionLayers();

  return (
    <div className="space-y-6">
      {/* View Mode Toggle */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <GitBranch className="w-5 h-5 text-gray-600" />
            <h3 className="text-gray-900">Dependency Visualization</h3>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setViewMode('graph')}
              className={`px-4 py-2 rounded-lg transition-colors ${
                viewMode === 'graph' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Execution Flow
            </button>
            <button
              onClick={() => setViewMode('list')}
              className={`px-4 py-2 rounded-lg transition-colors ${
                viewMode === 'list' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Dependency List
            </button>
          </div>
        </div>

        {/* Legend */}
        <div className="mt-4 pt-4 border-t border-gray-200 flex flex-wrap gap-4 text-sm">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4 text-green-600" />
            <span className="text-gray-600">Completed</span>
          </div>
          <div className="flex items-center gap-2">
            <Loader2 className="w-4 h-4 text-blue-600" />
            <span className="text-gray-600">Running</span>
          </div>
          <div className="flex items-center gap-2">
            <Clock className="w-4 h-4 text-gray-600" />
            <span className="text-gray-600">Ready to Start</span>
          </div>
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-4 h-4 text-yellow-600" />
            <span className="text-gray-600">Blocked</span>
          </div>
          <div className="flex items-center gap-2">
            <XCircle className="w-4 h-4 text-red-600" />
            <span className="text-gray-600">Failed</span>
          </div>
        </div>
      </div>

      {/* Graph View - Execution Flow */}
      {viewMode === 'graph' && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="mb-4">
            <h3 className="text-gray-900 mb-2">Execution Order (Layer by Layer)</h3>
            <p className="text-sm text-gray-600">
              Jobs in the same layer can run in parallel. Each layer waits for the previous layer to complete.
            </p>
          </div>

          <div className="space-y-6">
            {executionLayers.map((layer, layerIndex) => (
              <div key={layerIndex}>
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 text-blue-700">
                    {layerIndex + 1}
                  </div>
                  <span className="text-sm text-gray-600">
                    Layer {layerIndex + 1} ({layer.length} job{layer.length > 1 ? 's' : ''})
                  </span>
                </div>

                <div className="ml-11 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                  {layer.map(jobId => {
                    const job = getJobById(jobId);
                    const depInfo = getDependencyInfo(jobId);
                    const blocked = isJobBlocked(jobId);

                    if (!job) return null;

                    return (
                      <div
                        key={jobId}
                        className={`p-3 border-2 rounded-lg cursor-pointer transition-all ${
                          selectedJobId === jobId ? 'border-blue-500 bg-blue-50' :
                          blocked ? 'border-yellow-300 bg-yellow-50' :
                          'border-gray-200 hover:border-gray-300'
                        }`}
                        onClick={() => setSelectedJobId(jobId)}
                      >
                        <div className="flex items-start gap-2 mb-2">
                          {blocked ? (
                            <AlertTriangle className="w-5 h-5 text-yellow-600 mt-0.5" />
                          ) : (
                            getStatusIcon(job.status)
                          )}
                          <div className="flex-1 min-w-0">
                            <div className="text-sm text-gray-900 truncate">{job.name}</div>
                            <div className="text-xs text-gray-600 truncate">{job.sourceTable}</div>
                          </div>
                        </div>

                        {depInfo && (
                          <div className="text-xs text-gray-600 mt-2 pt-2 border-t border-gray-200">
                            {depInfo.dependsOn.length > 0 && <div>Depends on: {depInfo.dependsOn.length}</div>}
                            {depInfo.blockingJobs.length > 0 && <div>Blocks: {depInfo.blockingJobs.length}</div>}
                          </div>
                        )}

                        {blocked && (
                          <div className="mt-2 text-xs text-yellow-700 bg-yellow-100 px-2 py-1 rounded">
                            Waiting for dependencies
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

                {layerIndex < executionLayers.length - 1 && (
                  <div className="flex items-center justify-center my-4">
                    <ArrowRight className="w-6 h-6 text-gray-400" />
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* List View - Detailed Dependencies */}
      {viewMode === 'list' && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="divide-y divide-gray-200">
            {mockJobs.map(job => {
              const depInfo = getDependencyInfo(job.id);
              const blocked = isJobBlocked(job.id);

              return (
                <div
                  key={job.id}
                  className={`p-4 hover:bg-gray-50 transition-colors ${
                    selectedJobId === job.id ? 'bg-blue-50' : ''
                  }`}
                  onClick={() => setSelectedJobId(job.id === selectedJobId ? null : job.id)}
                >
                  <div className="flex items-start gap-4">
                    {blocked ? (
                      <AlertTriangle className="w-5 h-5 text-yellow-600 mt-1" />
                    ) : (
                      getStatusIcon(job.status)
                    )}

                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-gray-900">{job.name}</span>
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          job.status === 'completed' ? 'bg-green-100 text-green-700' :
                          job.status === 'running' ? 'bg-blue-100 text-blue-700' :
                          job.status === 'failed' ? 'bg-red-100 text-red-700' :
                          'bg-gray-100 text-gray-700'
                        }`}>
                          {job.status}
                        </span>
                        {blocked && (
                          <span className="px-2 py-0.5 rounded text-xs bg-yellow-100 text-yellow-700">
                            Blocked
                          </span>
                        )}
                      </div>
                      <div className="text-sm text-gray-600">
                        {job.sourceTable} → {job.targetTable}
                      </div>

                      {/* Dependency Details */}
                      {depInfo && selectedJobId === job.id && (
                        <div className="mt-3 p-3 bg-gray-50 rounded-lg space-y-3">
                          {depInfo.dependsOn.length > 0 && (
                            <div>
                              <div className="text-sm text-gray-700 mb-2 flex items-center gap-1">
                                <Info className="w-4 h-4" />
                                This job depends on:
                              </div>
                              <div className="space-y-1.5">
                                {depInfo.dependsOn.map(depId => {
                                  const depJob = getJobById(depId);
                                  if (!depJob) return null;
                                  return (
                                    <div key={depId} className="flex items-center gap-2 text-sm">
                                      {getStatusIcon(depJob.status, 'w-4 h-4')}
                                      <span className="text-gray-700">{depJob.name}</span>
                                      <span className={`text-xs ${
                                        depJob.status === 'completed' ? 'text-green-600' : 'text-gray-600'
                                      }`}>
                                        ({depJob.status})
                                      </span>
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          )}

                          {depInfo.blockingJobs.length > 0 && (
                            <div>
                              <div className="text-sm text-gray-700 mb-2 flex items-center gap-1">
                                <AlertTriangle className="w-4 h-4" />
                                Blocking {depInfo.blockingJobs.length} job(s):
                              </div>
                              <div className="space-y-1.5">
                                {depInfo.blockingJobs.map(blockId => {
                                  const blockJob = getJobById(blockId);
                                  if (!blockJob) return null;
                                  return (
                                    <div key={blockId} className="flex items-center gap-2 text-sm">
                                      {getStatusIcon(blockJob.status, 'w-4 h-4')}
                                      <span className="text-gray-700">{blockJob.name}</span>
                                      <span className="text-xs text-gray-600">({blockJob.status})</span>
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          )}

                          {depInfo.dependsOn.length === 0 && depInfo.blockingJobs.length === 0 && (
                            <div className="text-sm text-gray-600">
                              No dependencies - this job can run independently
                            </div>
                          )}
                        </div>
                      )}
                    </div>

                    {depInfo && (
                      <div className="text-right text-sm">
                        <div className="text-gray-600">Dependencies</div>
                        <div className="text-gray-900">
                          {depInfo.dependsOn.length} → {depInfo.blockingJobs.length}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Dependency Analysis Summary */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-sm text-gray-600 mb-1">Total Jobs</div>
          <div className="text-gray-900">{mockJobs.length}</div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-sm text-gray-600 mb-1">Execution Layers</div>
          <div className="text-gray-900">{executionLayers.length}</div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-sm text-gray-600 mb-1">Independent Jobs</div>
          <div className="text-gray-900">
            {mockJobDependencies.filter(d => d.dependsOn.length === 0).length}
          </div>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="text-sm text-gray-600 mb-1">Blocked Jobs</div>
          <div className="text-yellow-700">
            {mockJobs.filter(j => isJobBlocked(j.id)).length}
          </div>
        </div>
      </div>
    </div>
  );
}
