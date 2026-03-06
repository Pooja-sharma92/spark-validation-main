import { useState } from 'react';
import { Priority, TriggerSource } from '../../types/queue';
import { X, Upload, GitBranch, AlertCircle } from 'lucide-react';

interface SubmitJobModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSubmit: (jobData: SubmitJobData) => void;
}

export interface SubmitJobData {
  jobPath: string;
  priority: Priority;
  branch: string;
  commitSha: string;
  triggerSource: TriggerSource;
  metadata?: Record<string, string>;
}

export function SubmitJobModal({ isOpen, onClose, onSubmit }: SubmitJobModalProps) {
  const [formData, setFormData] = useState<SubmitJobData>({
    jobPath: '',
    priority: 'P2',
    branch: 'main',
    commitSha: '',
    triggerSource: 'manual',
  });
  const [errors, setErrors] = useState<Partial<Record<keyof SubmitJobData, string>>>({});

  const validate = (): boolean => {
    const newErrors: Partial<Record<keyof SubmitJobData, string>> = {};

    if (!formData.jobPath.trim()) {
      newErrors.jobPath = 'Job path is required';
    } else if (!formData.jobPath.endsWith('.sql') && !formData.jobPath.endsWith('.py')) {
      newErrors.jobPath = 'Job path must end with .sql or .py';
    }

    if (!formData.branch.trim()) {
      newErrors.branch = 'Branch is required';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validate()) {
      onSubmit(formData);
      onClose();
      // Reset form
      setFormData({
        jobPath: '',
        priority: 'P2',
        branch: 'main',
        commitSha: '',
        triggerSource: 'manual',
      });
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-lg mx-4">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-gray-200">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-100 rounded-lg">
              <Upload className="w-5 h-5 text-blue-600" />
            </div>
            <h2 className="text-lg font-medium text-gray-900">Submit New Job</h2>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-4 space-y-4">
          {/* Job Path */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Job Path <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={formData.jobPath}
              onChange={(e) => setFormData((f) => ({ ...f, jobPath: e.target.value }))}
              placeholder="src/migration/example_job.sql"
              className={`w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 ${
                errors.jobPath ? 'border-red-300' : 'border-gray-200'
              }`}
            />
            {errors.jobPath && (
              <p className="mt-1 text-sm text-red-600 flex items-center gap-1">
                <AlertCircle className="w-4 h-4" />
                {errors.jobPath}
              </p>
            )}
          </div>

          {/* Priority */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Priority</label>
            <div className="grid grid-cols-4 gap-2">
              {(['P0', 'P1', 'P2', 'P3'] as Priority[]).map((p) => (
                <button
                  key={p}
                  type="button"
                  onClick={() => setFormData((f) => ({ ...f, priority: p }))}
                  className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                    formData.priority === p
                      ? p === 'P0'
                        ? 'bg-red-600 text-white'
                        : p === 'P1'
                        ? 'bg-orange-600 text-white'
                        : p === 'P2'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-600 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  {p}
                </button>
              ))}
            </div>
            <p className="mt-1 text-xs text-gray-500">
              {formData.priority === 'P0' && 'Critical - Hotfix/Production issues'}
              {formData.priority === 'P1' && 'Manual - PR Reviews and manual submissions'}
              {formData.priority === 'P2' && 'CI/CD - Pipeline and feature branch validation'}
              {formData.priority === 'P3' && 'Batch - Scheduled and bulk scans'}
            </p>
          </div>

          {/* Branch & Commit */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Branch <span className="text-red-500">*</span>
              </label>
              <div className="relative">
                <GitBranch className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  value={formData.branch}
                  onChange={(e) => setFormData((f) => ({ ...f, branch: e.target.value }))}
                  placeholder="main"
                  className={`w-full pl-10 pr-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 ${
                    errors.branch ? 'border-red-300' : 'border-gray-200'
                  }`}
                />
              </div>
              {errors.branch && (
                <p className="mt-1 text-sm text-red-600 flex items-center gap-1">
                  <AlertCircle className="w-4 h-4" />
                  {errors.branch}
                </p>
              )}
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Commit SHA <span className="text-gray-400">(optional)</span>
              </label>
              <input
                type="text"
                value={formData.commitSha}
                onChange={(e) => setFormData((f) => ({ ...f, commitSha: e.target.value }))}
                placeholder="a1b2c3d"
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          {/* Trigger Source */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Trigger Source</label>
            <select
              value={formData.triggerSource}
              onChange={(e) =>
                setFormData((f) => ({ ...f, triggerSource: e.target.value as TriggerSource }))
              }
              className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="manual">Manual</option>
              <option value="webhook">Webhook</option>
              <option value="ci_cd">CI/CD</option>
              <option value="scheduled">Scheduled</option>
              <option value="file">File Upload</option>
            </select>
          </div>

          {/* Info Box */}
          <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-800">
            The job will be added to the <strong>{formData.priority}</strong> queue and processed
            according to its priority level.
          </div>
        </form>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-gray-200 bg-gray-50">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 text-gray-600 hover:text-gray-800 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Submit Job
          </button>
        </div>
      </div>
    </div>
  );
}
