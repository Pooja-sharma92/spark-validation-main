import { useState } from 'react';
import { mockJobs } from '../../data/migrationMockData';
import { mockJobTags, jobTagsMapping } from '../../data/jobGroupsData';
import { Tag, X, CheckCircle2, XCircle, Loader2, Clock, Eye } from 'lucide-react';

export function TagView() {
  const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set());

  const toggleTag = (tagId: string) => {
    const newSelection = new Set(selectedTags);
    if (newSelection.has(tagId)) {
      newSelection.delete(tagId);
    } else {
      newSelection.add(tagId);
    }
    setSelectedTags(newSelection);
  };

  const getJobsForTag = (tagId: string) => {
    const jobIds = Object.entries(jobTagsMapping)
      .filter(([_, tags]) => tags.includes(tagId))
      .map(([jobId]) => jobId);
    return mockJobs.filter(j => jobIds.includes(j.id));
  };

  const getTagStats = (tagId: string) => {
    const jobs = getJobsForTag(tagId);
    return {
      total: jobs.length,
      completed: jobs.filter(j => j.status === 'completed').length,
      running: jobs.filter(j => j.status === 'running').length,
      failed: jobs.filter(j => j.status === 'failed').length,
      pending: jobs.filter(j => j.status === 'pending').length,
    };
  };

  const filteredJobs = selectedTags.size === 0
    ? mockJobs
    : mockJobs.filter(job => {
        const jobTags = jobTagsMapping[job.id] || [];
        return Array.from(selectedTags).every(tag => jobTags.includes(tag));
      });

  const getJobTags = (jobId: string) => {
    const tagIds = jobTagsMapping[jobId] || [];
    return mockJobTags.filter(t => tagIds.includes(t.id));
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed': return <CheckCircle2 className="w-4 h-4 text-green-600" />;
      case 'running': return <Loader2 className="w-4 h-4 text-blue-600 animate-spin" />;
      case 'failed': return <XCircle className="w-4 h-4 text-red-600" />;
      case 'validating': return <Eye className="w-4 h-4 text-purple-600" />;
      case 'pending': return <Clock className="w-4 h-4 text-gray-600" />;
      default: return null;
    }
  };

  const getTagColorClasses = (color: string) => {
    const colorMap: Record<string, { bg: string; text: string; border: string }> = {
      red: { bg: 'bg-red-100', text: 'text-red-700', border: 'border-red-400' },
      orange: { bg: 'bg-orange-100', text: 'text-orange-700', border: 'border-orange-400' },
      yellow: { bg: 'bg-yellow-100', text: 'text-yellow-700', border: 'border-yellow-400' },
      green: { bg: 'bg-green-100', text: 'text-green-700', border: 'border-green-400' },
      blue: { bg: 'bg-blue-100', text: 'text-blue-700', border: 'border-blue-400' },
      purple: { bg: 'bg-purple-100', text: 'text-purple-700', border: 'border-purple-400' },
      gray: { bg: 'bg-gray-100', text: 'text-gray-700', border: 'border-gray-400' },
      brown: { bg: 'bg-amber-100', text: 'text-amber-700', border: 'border-amber-400' },
    };
    return colorMap[color] || colorMap.gray;
  };

  return (
    <div className="space-y-6">
      {/* Tag Filter Bar */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center gap-2 mb-3">
          <Tag className="w-5 h-5 text-gray-600" />
          <h3 className="text-gray-900">Filter by Tags</h3>
          {selectedTags.size > 0 && (
            <span className="text-sm text-gray-600">({selectedTags.size} selected)</span>
          )}
        </div>

        <div className="flex flex-wrap gap-2">
          {mockJobTags.map(tag => {
            const stats = getTagStats(tag.id);
            const isSelected = selectedTags.has(tag.id);
            const colors = getTagColorClasses(tag.color);

            return (
              <button
                key={tag.id}
                onClick={() => toggleTag(tag.id)}
                className={`px-3 py-2 rounded-lg border-2 transition-all ${
                  isSelected ? `${colors.bg} ${colors.border}` : 'bg-white border-gray-200 hover:border-gray-300'
                }`}
                title={tag.description}
              >
                <div className="flex items-center gap-2">
                  <span className={colors.text}>{tag.name}</span>
                  <span className="text-xs px-1.5 py-0.5 bg-white rounded text-gray-600">
                    {stats.total}
                  </span>
                  {isSelected && <X className="w-3 h-3 text-gray-600" />}
                </div>
              </button>
            );
          })}
        </div>

        {selectedTags.size > 0 && (
          <div className="mt-3 pt-3 border-t border-gray-200">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-600">
                Showing {filteredJobs.length} jobs with selected tags
              </span>
              <button
                onClick={() => setSelectedTags(new Set())}
                className="text-sm text-blue-600 hover:text-blue-700"
              >
                Clear All
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Tag Statistics Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {mockJobTags.map(tag => {
          const stats = getTagStats(tag.id);
          const completionRate = stats.total > 0 ? (stats.completed / stats.total) * 100 : 0;
          const colors = getTagColorClasses(tag.color);

          return (
            <div
              key={tag.id}
              className={`bg-white rounded-lg shadow-sm border-2 p-4 transition-all cursor-pointer ${
                selectedTags.has(tag.id) ? colors.border : 'border-gray-200 hover:border-gray-300'
              }`}
              onClick={() => toggleTag(tag.id)}
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div className={`p-2 rounded-lg ${colors.bg}`}>
                    <Tag className={`w-4 h-4 ${colors.text}`} />
                  </div>
                  <div>
                    <div className={colors.text}>{tag.name}</div>
                    <div className="text-xs text-gray-600">{stats.total} jobs</div>
                  </div>
                </div>
              </div>

              <div className="space-y-2 mb-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-gray-600">Progress</span>
                  <span className="text-gray-900">{completionRate.toFixed(0)}%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-1.5">
                  <div
                    className={`h-1.5 rounded-full transition-all ${colors.bg.replace('100', '600')}`}
                    style={{ width: `${completionRate}%` }}
                  ></div>
                </div>
              </div>

              <div className="flex items-center gap-3 text-xs">
                {stats.completed > 0 && <span className="text-green-700">✓ {stats.completed}</span>}
                {stats.running > 0 && <span className="text-blue-700">▶ {stats.running}</span>}
                {stats.failed > 0 && <span className="text-red-700">✗ {stats.failed}</span>}
                {stats.pending > 0 && <span className="text-gray-700">◷ {stats.pending}</span>}
              </div>
            </div>
          );
        })}
      </div>

      {/* Jobs List with Tags */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="p-4 border-b border-gray-200">
          <h3 className="text-gray-900">
            {selectedTags.size === 0 ? 'All Jobs' : 'Filtered Jobs'}
          </h3>
        </div>

        <div className="divide-y divide-gray-100">
          {filteredJobs.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              No jobs match the selected tags
            </div>
          ) : (
            filteredJobs.map(job => {
              const jobTags = getJobTags(job.id);

              return (
                <div key={job.id} className="p-4 hover:bg-gray-50 transition-colors">
                  <div className="flex items-start gap-4">
                    {getStatusIcon(job.status)}

                    <div className="flex-1 min-w-0">
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
                      </div>

                      <div className="text-sm text-gray-600 mb-2">
                        {job.sourceTable} → {job.targetTable}
                      </div>

                      {jobTags.length > 0 && (
                        <div className="flex flex-wrap gap-1.5">
                          {jobTags.map(tag => {
                            const colors = getTagColorClasses(tag.color);
                            return (
                              <span
                                key={tag.id}
                                className={`px-2 py-0.5 rounded text-xs ${colors.bg} ${colors.text}`}
                              >
                                {tag.name}
                              </span>
                            );
                          })}
                        </div>
                      )}
                    </div>

                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <div className="text-sm text-gray-900">
                          {job.targetRowCount.toLocaleString()}
                        </div>
                        <div className="text-xs text-gray-600">
                          / {job.sourceRowCount.toLocaleString()}
                        </div>
                      </div>

                      <div className="w-32">
                        <div className="text-sm text-gray-700 mb-1">{job.progress}%</div>
                        <div className="w-full bg-gray-200 rounded-full h-1.5">
                          <div
                            className={`h-1.5 rounded-full ${
                              job.status === 'completed' ? 'bg-green-600' :
                              job.status === 'failed' ? 'bg-red-600' :
                              'bg-blue-600'
                            }`}
                            style={{ width: `${job.progress}%` }}
                          ></div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
