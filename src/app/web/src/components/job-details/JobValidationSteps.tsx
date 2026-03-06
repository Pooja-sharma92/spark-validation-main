import { MigrationJob, ValidationStep, ValidationError, ValidationWarning } from '../../types/migration';
import {
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Clock,
  Loader2,
  ChevronRight,
  ChevronDown,
  AlertCircle,
  Info,
  Terminal,
  Bug,
} from 'lucide-react';
import { useState } from 'react';

interface JobValidationStepsProps {
  job: MigrationJob;
}

export function JobValidationSteps({ job }: JobValidationStepsProps) {
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set());

  const toggleStep = (stepId: string) => {
    const newExpanded = new Set(expandedSteps);
    if (newExpanded.has(stepId)) {
      newExpanded.delete(stepId);
    } else {
      newExpanded.add(stepId);
    }
    setExpandedSteps(newExpanded);
  };

  const getStepIcon = (status: string) => {
    const iconClass = 'w-5 h-5';
    switch (status) {
      case 'passed': return <CheckCircle2 className={`${iconClass} text-green-600`} />;
      case 'failed': return <XCircle className={`${iconClass} text-red-600`} />;
      case 'warning': return <AlertTriangle className={`${iconClass} text-yellow-600`} />;
      case 'running': return <Loader2 className={`${iconClass} text-blue-600 animate-spin`} />;
      case 'pending': return <Clock className={`${iconClass} text-gray-400`} />;
      case 'skipped': return <ChevronRight className={`${iconClass} text-gray-400`} />;
      default: return <Clock className={`${iconClass} text-gray-400`} />;
    }
  };

  const getStepStatusBadge = (status: string) => {
    const baseClass = 'px-2.5 py-1 rounded text-xs';
    switch (status) {
      case 'passed': return <span className={`${baseClass} bg-green-100 text-green-700`}>✓ Passed</span>;
      case 'failed': return <span className={`${baseClass} bg-red-100 text-red-700`}>✗ Failed</span>;
      case 'warning': return <span className={`${baseClass} bg-yellow-100 text-yellow-700`}>⚠ Warning</span>;
      case 'running': return <span className={`${baseClass} bg-blue-100 text-blue-700`}>⟳ Running</span>;
      case 'pending': return <span className={`${baseClass} bg-gray-100 text-gray-600`}>○ Pending</span>;
      case 'skipped': return <span className={`${baseClass} bg-gray-100 text-gray-500`}>— Skipped</span>;
      default: return <span className={`${baseClass} bg-gray-100 text-gray-600`}>{status}</span>;
    }
  };

  const formatDuration = (ms?: number) => {
    if (!ms) return '—';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
  };

  const renderError = (error: ValidationError) => {
    const severityColors: Record<string, { bg: string; border: string; text: string }> = {
      critical: { bg: 'bg-red-50', border: 'border-red-200', text: 'text-red-600' },
      error: { bg: 'bg-orange-50', border: 'border-orange-200', text: 'text-orange-600' },
      warning: { bg: 'bg-yellow-50', border: 'border-yellow-200', text: 'text-yellow-600' },
    };
    const colors = severityColors[error.severity] || severityColors.warning;

    return (
      <div key={error.id} className={`p-4 ${colors.bg} border ${colors.border} rounded-lg`}>
        <div className="flex items-start gap-3">
          <AlertCircle className={`w-5 h-5 ${colors.text} mt-0.5 flex-shrink-0`} />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-2">
              <span className={`px-2 py-0.5 rounded text-xs ${colors.bg} ${colors.text.replace('600', '800')}`}>
                {error.severity.toUpperCase()}
              </span>
              <span className="text-xs text-gray-600 font-mono">{error.code}</span>
            </div>
            <div className="text-sm text-gray-900 mb-2"><strong>{error.message}</strong></div>
            {error.details && <div className="text-sm text-gray-700 mb-2">{error.details}</div>}
            {error.location && (
              <div className="text-xs text-gray-600 mb-2 font-mono bg-white px-2 py-1 rounded inline-block">
                📍 {error.location}
              </div>
            )}
            {error.suggestion && (
              <div className={`mt-3 p-3 ${colors.bg} rounded border-l-4 ${colors.border.replace('200', '400')}`}>
                <div className="flex items-start gap-2">
                  <Info className="w-4 h-4 text-blue-600 mt-0.5 flex-shrink-0" />
                  <div>
                    <div className="text-xs text-blue-900 mb-1">💡 Suggested Fix:</div>
                    <div className="text-sm text-gray-800">{error.suggestion}</div>
                  </div>
                </div>
              </div>
            )}
            <div className="mt-2 text-xs text-gray-500">Detected at: {error.timestamp}</div>
          </div>
        </div>
      </div>
    );
  };

  const renderWarning = (warning: ValidationWarning) => {
    return (
      <div key={warning.id} className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
        <div className="flex items-start gap-3">
          <AlertTriangle className="w-4 h-4 text-yellow-600 mt-0.5 flex-shrink-0" />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-xs text-gray-600 font-mono">{warning.code}</span>
            </div>
            <div className="text-sm text-gray-900 mb-1">{warning.message}</div>
            {warning.details && <div className="text-xs text-gray-700 mb-2">{warning.details}</div>}
            {warning.suggestion && (
              <div className="mt-2 text-xs text-blue-700 bg-blue-50 px-2 py-1 rounded">
                💡 {warning.suggestion}
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  const renderValidationStep = (step: ValidationStep, index: number, total: number) => {
    const isExpanded = expandedSteps.has(step.id);
    const hasIssues = (step.errors && step.errors.length > 0) || (step.warnings && step.warnings.length > 0);

    return (
      <div key={step.id} className="relative">
        {index < total - 1 && (
          <div className="absolute left-6 top-12 bottom-0 w-0.5 bg-gray-200" />
        )}

        <div className={`bg-white border rounded-lg overflow-hidden transition-all ${
          step.status === 'failed' ? 'border-red-300' :
          step.status === 'warning' ? 'border-yellow-300' :
          step.status === 'passed' ? 'border-green-300' :
          step.status === 'running' ? 'border-blue-300' :
          'border-gray-200'
        }`}>
          <button
            onClick={() => toggleStep(step.id)}
            className="w-full p-4 flex items-center gap-4 hover:bg-gray-50 transition-colors"
          >
            <div className="flex items-center gap-3">
              <div className={`flex-shrink-0 w-12 h-12 rounded-full flex items-center justify-center border-2 ${
                step.status === 'passed' ? 'bg-green-50 border-green-300' :
                step.status === 'failed' ? 'bg-red-50 border-red-300' :
                step.status === 'warning' ? 'bg-yellow-50 border-yellow-300' :
                step.status === 'running' ? 'bg-blue-50 border-blue-300' :
                'bg-gray-50 border-gray-300'
              }`}>
                {getStepIcon(step.status)}
              </div>
            </div>

            <div className="flex-1 text-left min-w-0">
              <div className="flex items-center gap-3 mb-1">
                <h4 className="text-gray-900">{step.name}</h4>
                {getStepStatusBadge(step.status)}
                {hasIssues && (
                  <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded text-xs">
                    {(step.errors?.length || 0) + (step.warnings?.length || 0)} issue(s)
                  </span>
                )}
              </div>
              <p className="text-sm text-gray-600">{step.description}</p>
            </div>

            <div className="flex items-center gap-6 text-sm">
              {step.progress !== undefined && step.status === 'running' && (
                <div className="text-center">
                  <div className="text-gray-600 text-xs mb-1">Progress</div>
                  <div className="text-blue-700">{step.progress}%</div>
                </div>
              )}
              {step.duration !== undefined && (
                <div className="text-center">
                  <div className="text-gray-600 text-xs mb-1">Duration</div>
                  <div className="text-gray-900">{formatDuration(step.duration)}</div>
                </div>
              )}
              {step.metadata?.recordsChecked !== undefined && (
                <div className="text-center">
                  <div className="text-gray-600 text-xs mb-1">Checked</div>
                  <div className="text-gray-900">{step.metadata.recordsChecked.toLocaleString()}</div>
                </div>
              )}
            </div>

            <div className="flex-shrink-0">
              {isExpanded ? <ChevronDown className="w-5 h-5 text-gray-400" /> : <ChevronRight className="w-5 h-5 text-gray-400" />}
            </div>
          </button>

          {isExpanded && (
            <div className="border-t border-gray-200 bg-gray-50">
              <div className="p-4 space-y-4">
                {(step.startTime || step.endTime) && (
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    {step.startTime && (
                      <div>
                        <div className="text-gray-600 text-xs mb-1">Start Time</div>
                        <div className="text-gray-900 font-mono text-xs">{step.startTime}</div>
                      </div>
                    )}
                    {step.endTime && (
                      <div>
                        <div className="text-gray-600 text-xs mb-1">End Time</div>
                        <div className="text-gray-900 font-mono text-xs">{step.endTime}</div>
                      </div>
                    )}
                  </div>
                )}

                {step.status === 'running' && step.progress !== undefined && (
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm text-gray-700">Validation Progress</span>
                      <span className="text-sm text-blue-700">{step.progress}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div className="bg-blue-600 h-2 rounded-full transition-all duration-500" style={{ width: `${step.progress}%` }} />
                    </div>
                  </div>
                )}

                {step.errors && step.errors.length > 0 && (
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <Bug className="w-4 h-4 text-red-600" />
                      <h5 className="text-sm text-red-900">Errors ({step.errors.length})</h5>
                    </div>
                    <div className="space-y-3">
                      {step.errors.map(error => renderError(error))}
                    </div>
                  </div>
                )}

                {step.warnings && step.warnings.length > 0 && (
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <AlertTriangle className="w-4 h-4 text-yellow-600" />
                      <h5 className="text-sm text-yellow-900">Warnings ({step.warnings.length})</h5>
                    </div>
                    <div className="space-y-2">
                      {step.warnings.map(warning => renderWarning(warning))}
                    </div>
                  </div>
                )}

                {step.metadata && Object.keys(step.metadata).length > 0 && (
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      <Terminal className="w-4 h-4 text-gray-600" />
                      <h5 className="text-sm text-gray-700">Metadata</h5>
                    </div>
                    <div className="bg-gray-900 rounded p-3 font-mono text-xs text-green-400 overflow-x-auto">
                      <pre>{JSON.stringify(step.metadata, null, 2)}</pre>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-gray-900 mb-1">Validation Pipeline</h3>
          <p className="text-sm text-gray-600">
            {job.validationSteps.length} validation step(s) • Click to expand details
          </p>
        </div>
        <div className="flex items-center gap-2 text-sm">
          <div className="flex items-center gap-1.5">
            <CheckCircle2 className="w-4 h-4 text-green-600" />
            <span className="text-gray-700">{job.validationSteps.filter(s => s.status === 'passed').length} Passed</span>
          </div>
          <div className="flex items-center gap-1.5">
            <XCircle className="w-4 h-4 text-red-600" />
            <span className="text-gray-700">{job.validationSteps.filter(s => s.status === 'failed').length} Failed</span>
          </div>
          <div className="flex items-center gap-1.5">
            <AlertTriangle className="w-4 h-4 text-yellow-600" />
            <span className="text-gray-700">{job.validationSteps.filter(s => s.status === 'warning').length} Warning</span>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        {job.validationSteps.map((step, index) =>
          renderValidationStep(step, index, job.validationSteps.length)
        )}
      </div>

      {((job.errors && job.errors.length > 0) || (job.warnings && job.warnings.length > 0)) && (
        <div className="mt-6 pt-6 border-t border-gray-200">
          <h4 className="text-gray-900 mb-4 flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-orange-600" />
            Overall Job Issues
          </h4>
          <div className="space-y-3">
            {job.errors && job.errors.map(error => renderError(error))}
            {job.warnings && job.warnings.map(warning => renderWarning(warning))}
          </div>
        </div>
      )}
    </div>
  );
}
