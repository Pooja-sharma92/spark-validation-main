import { useState } from 'react';
import { mockJobs } from '../../data/migrationMockData';
import { ValidationStrategy, ValidationRule, ValidationRuleType } from '../../types/validation';
import {
  Settings,
  Play,
  Plus,
  Trash2,
  Code,
  Database,
  FileText,
  Eye,
  EyeOff,
} from 'lucide-react';

export function DataValidationPage() {
  const [selectedJobId, setSelectedJobId] = useState(mockJobs[0].id);
  const [validationStrategy, setValidationStrategy] = useState<ValidationStrategy>('full');
  const [sampleSize, setSampleSize] = useState(1000);
  const [keyFields, setKeyFields] = useState<string[]>(['id']);
  const [rules, setRules] = useState<ValidationRule[]>([
    {
      id: 'rule-1',
      fieldName: 'email',
      ruleType: 'format',
      description: 'Validate email format',
      enabled: true,
      config: { pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' },
    },
    {
      id: 'rule-2',
      fieldName: 'created_at',
      ruleType: 'exact-match',
      description: 'Timestamp must match exactly',
      enabled: true,
    },
    {
      id: 'rule-3',
      fieldName: 'price',
      ruleType: 'range',
      description: 'Price tolerance +/-0.01',
      enabled: true,
      config: { tolerance: 0.01 },
    },
  ]);

  const [showAddRule, setShowAddRule] = useState(false);
  const [newRule, setNewRule] = useState<Partial<ValidationRule>>({
    fieldName: '',
    ruleType: 'exact-match',
    description: '',
    enabled: true,
    config: {},
  });

  const selectedJob = mockJobs.find(j => j.id === selectedJobId);

  const addRule = () => {
    if (!newRule.fieldName || !newRule.ruleType) {
      alert('Please fill in required fields');
      return;
    }

    const rule: ValidationRule = {
      id: `rule-${Date.now()}`,
      fieldName: newRule.fieldName!,
      ruleType: newRule.ruleType!,
      description: newRule.description || '',
      enabled: true,
      config: newRule.config,
    };

    setRules([...rules, rule]);
    setNewRule({
      fieldName: '',
      ruleType: 'exact-match',
      description: '',
      enabled: true,
      config: {},
    });
    setShowAddRule(false);
  };

  const toggleRule = (ruleId: string) => {
    setRules(rules.map(r => r.id === ruleId ? { ...r, enabled: !r.enabled } : r));
  };

  const deleteRule = (ruleId: string) => {
    if (confirm('Are you sure you want to delete this rule?')) {
      setRules(rules.filter(r => r.id !== ruleId));
    }
  };

  const startValidation = () => {
    const config = {
      jobId: selectedJobId,
      strategy: validationStrategy,
      sampleSize: validationStrategy === 'sampling' ? sampleSize : undefined,
      keyFields,
      rules: rules.filter(r => r.enabled),
    };

    console.log('Starting validation with config:', config);
    alert(`Starting ${validationStrategy} validation for ${selectedJob?.name}\n\n${rules.filter(r => r.enabled).length} rules will be applied.`);
  };

  const getRuleTypeColor = (type: ValidationRuleType) => {
    const colors: Record<ValidationRuleType, string> = {
      'exact-match': 'blue',
      'range': 'green',
      'format': 'purple',
      'custom': 'orange',
      'null-check': 'yellow',
      'type-check': 'pink',
    };
    return colors[type] || 'gray';
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-gray-900 mb-2">Data Validation Configuration</h1>
        <p className="text-gray-600">Configure validation strategies and rules for data verification</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Panel - Configuration */}
        <div className="lg:col-span-1 space-y-6">
          {/* Job Selection */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center gap-2 mb-4">
              <Database className="w-5 h-5 text-gray-600" />
              <h2 className="text-gray-900">Select Job</h2>
            </div>
            <select
              value={selectedJobId}
              onChange={(e) => setSelectedJobId(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {mockJobs.map(job => (
                <option key={job.id} value={job.id}>
                  {job.name}
                </option>
              ))}
            </select>
            {selectedJob && (
              <div className="mt-4 p-3 bg-gray-50 rounded-lg text-sm">
                <div className="text-gray-600 mb-1">Source → Target</div>
                <div className="text-gray-900">{selectedJob.sourceTable}</div>
                <div className="text-gray-400">↓</div>
                <div className="text-gray-900">{selectedJob.targetTable}</div>
              </div>
            )}
          </div>

          {/* Validation Strategy */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center gap-2 mb-4">
              <Settings className="w-5 h-5 text-gray-600" />
              <h2 className="text-gray-900">Validation Strategy</h2>
            </div>

            <div className="space-y-3">
              <label className="flex items-start gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                <input
                  type="radio"
                  name="strategy"
                  value="full"
                  checked={validationStrategy === 'full'}
                  onChange={(e) => setValidationStrategy(e.target.value as ValidationStrategy)}
                  className="mt-1"
                />
                <div>
                  <div className="text-gray-900">Full Validation</div>
                  <div className="text-sm text-gray-600">Compare all records row by row</div>
                </div>
              </label>

              <label className="flex items-start gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                <input
                  type="radio"
                  name="strategy"
                  value="sampling"
                  checked={validationStrategy === 'sampling'}
                  onChange={(e) => setValidationStrategy(e.target.value as ValidationStrategy)}
                  className="mt-1"
                />
                <div className="flex-1">
                  <div className="text-gray-900">Sampling Validation</div>
                  <div className="text-sm text-gray-600 mb-2">Validate random sample</div>
                  {validationStrategy === 'sampling' && (
                    <input
                      type="number"
                      value={sampleSize}
                      onChange={(e) => setSampleSize(Number(e.target.value))}
                      placeholder="Sample size"
                      className="w-full px-3 py-2 border border-gray-300 rounded text-sm"
                    />
                  )}
                </div>
              </label>

              <label className="flex items-start gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                <input
                  type="radio"
                  name="strategy"
                  value="key-fields"
                  checked={validationStrategy === 'key-fields'}
                  onChange={(e) => setValidationStrategy(e.target.value as ValidationStrategy)}
                  className="mt-1"
                />
                <div>
                  <div className="text-gray-900">Key Fields Only</div>
                  <div className="text-sm text-gray-600">Validate critical fields only</div>
                </div>
              </label>

              <label className="flex items-start gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                <input
                  type="radio"
                  name="strategy"
                  value="incremental"
                  checked={validationStrategy === 'incremental'}
                  onChange={(e) => setValidationStrategy(e.target.value as ValidationStrategy)}
                  className="mt-1"
                />
                <div>
                  <div className="text-gray-900">Incremental</div>
                  <div className="text-sm text-gray-600">Only validate new/changed records</div>
                </div>
              </label>
            </div>
          </div>

          {/* Key Fields */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-gray-900 mb-3">Primary Key Fields</h3>
            <input
              type="text"
              value={keyFields.join(', ')}
              onChange={(e) => setKeyFields(e.target.value.split(',').map(s => s.trim()))}
              placeholder="e.g., id, user_id"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="text-sm text-gray-600 mt-2">
              Comma-separated field names used to match records
            </p>
          </div>

          {/* Start Validation Button */}
          <button
            onClick={startValidation}
            className="w-full px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center justify-center gap-2"
          >
            <Play className="w-5 h-5" />
            Start Validation
          </button>
        </div>

        {/* Right Panel - Validation Rules */}
        <div className="lg:col-span-2">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2">
                <FileText className="w-5 h-5 text-gray-600" />
                <h2 className="text-gray-900">Validation Rules</h2>
                <span className="text-sm text-gray-600">
                  ({rules.filter(r => r.enabled).length} active)
                </span>
              </div>
              <button
                onClick={() => setShowAddRule(true)}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              >
                <Plus className="w-4 h-4" />
                Add Rule
              </button>
            </div>

            {/* Add Rule Form */}
            {showAddRule && (
              <div className="mb-6 p-4 border-2 border-blue-200 rounded-lg bg-blue-50">
                <h3 className="text-gray-900 mb-4">New Validation Rule</h3>
                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div>
                    <label className="block text-sm text-gray-700 mb-2">Field Name</label>
                    <input
                      type="text"
                      value={newRule.fieldName || ''}
                      onChange={(e) => setNewRule({ ...newRule, fieldName: e.target.value })}
                      placeholder="e.g., email, price"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm text-gray-700 mb-2">Rule Type</label>
                    <select
                      value={newRule.ruleType || 'exact-match'}
                      onChange={(e) => setNewRule({ ...newRule, ruleType: e.target.value as ValidationRuleType })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    >
                      <option value="exact-match">Exact Match</option>
                      <option value="range">Range Tolerance</option>
                      <option value="format">Format Pattern</option>
                      <option value="null-check">Null Check</option>
                      <option value="type-check">Type Check</option>
                      <option value="custom">Custom Script</option>
                    </select>
                  </div>
                </div>

                <div className="mb-4">
                  <label className="block text-sm text-gray-700 mb-2">Description</label>
                  <input
                    type="text"
                    value={newRule.description || ''}
                    onChange={(e) => setNewRule({ ...newRule, description: e.target.value })}
                    placeholder="Describe what this rule validates"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                  />
                </div>

                {/* Rule-specific config */}
                {newRule.ruleType === 'range' && (
                  <div className="mb-4">
                    <label className="block text-sm text-gray-700 mb-2">Tolerance</label>
                    <input
                      type="number"
                      step="0.01"
                      value={newRule.config?.tolerance || 0}
                      onChange={(e) => setNewRule({
                        ...newRule,
                        config: { ...newRule.config, tolerance: Number(e.target.value) }
                      })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    />
                  </div>
                )}

                {newRule.ruleType === 'format' && (
                  <div className="mb-4">
                    <label className="block text-sm text-gray-700 mb-2">Regex Pattern</label>
                    <input
                      type="text"
                      value={newRule.config?.pattern || ''}
                      onChange={(e) => setNewRule({
                        ...newRule,
                        config: { ...newRule.config, pattern: e.target.value }
                      })}
                      placeholder="^[a-zA-Z0-9]+$"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg font-mono text-sm"
                    />
                  </div>
                )}

                {newRule.ruleType === 'custom' && (
                  <div className="mb-4">
                    <label className="block text-sm text-gray-700 mb-2">Custom Script</label>
                    <textarea
                      value={newRule.config?.customScript || ''}
                      onChange={(e) => setNewRule({
                        ...newRule,
                        config: { ...newRule.config, customScript: e.target.value }
                      })}
                      placeholder="// JavaScript validation function"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg font-mono text-sm h-24"
                    />
                  </div>
                )}

                <div className="flex gap-2">
                  <button
                    onClick={addRule}
                    className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                  >
                    Add Rule
                  </button>
                  <button
                    onClick={() => setShowAddRule(false)}
                    className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            {/* Rules List */}
            <div className="space-y-3">
              {rules.map(rule => {
                const color = getRuleTypeColor(rule.ruleType);
                return (
                  <div
                    key={rule.id}
                    className={`p-4 border rounded-lg transition-all ${
                      rule.enabled
                        ? 'border-gray-200 bg-white'
                        : 'border-gray-200 bg-gray-50 opacity-60'
                    }`}
                  >
                    <div className="flex items-start justify-between mb-2">
                      <div className="flex items-start gap-3 flex-1">
                        <button
                          onClick={() => toggleRule(rule.id)}
                          className="mt-1"
                        >
                          {rule.enabled ? (
                            <Eye className="w-5 h-5 text-blue-600" />
                          ) : (
                            <EyeOff className="w-5 h-5 text-gray-400" />
                          )}
                        </button>
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-gray-900">{rule.fieldName}</span>
                            <span className={`px-2 py-0.5 rounded text-xs bg-${color}-100 text-${color}-700`}>
                              {rule.ruleType}
                            </span>
                          </div>
                          <p className="text-sm text-gray-600">{rule.description}</p>

                          {/* Rule Config Display */}
                          {rule.config && Object.keys(rule.config).length > 0 && (
                            <div className="mt-2 p-2 bg-gray-50 rounded text-xs font-mono">
                              {rule.config.tolerance !== undefined && (
                                <div>Tolerance: +/-{rule.config.tolerance}</div>
                              )}
                              {rule.config.pattern && (
                                <div className="text-purple-700">Pattern: {rule.config.pattern}</div>
                              )}
                              {rule.config.customScript && (
                                <div className="flex items-center gap-1 text-orange-700">
                                  <Code className="w-3 h-3" />
                                  Custom validation function
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={() => deleteRule(rule.id)}
                        className="p-1.5 hover:bg-red-50 rounded transition-colors"
                      >
                        <Trash2 className="w-4 h-4 text-red-600" />
                      </button>
                    </div>
                  </div>
                );
              })}

              {rules.length === 0 && (
                <div className="text-center py-12 text-gray-500">
                  No validation rules configured. Click "Add Rule" to create one.
                </div>
              )}
            </div>

            {/* Preset Templates */}
            <div className="mt-6 pt-6 border-t border-gray-200">
              <h3 className="text-gray-900 mb-3">Rule Templates</h3>
              <div className="grid grid-cols-2 gap-3">
                <button className="p-3 border border-gray-200 rounded-lg hover:bg-gray-50 text-left transition-colors">
                  <div className="text-sm text-gray-900 mb-1">Standard Data Types</div>
                  <div className="text-xs text-gray-600">Email, phone, date validation</div>
                </button>
                <button className="p-3 border border-gray-200 rounded-lg hover:bg-gray-50 text-left transition-colors">
                  <div className="text-sm text-gray-900 mb-1">Financial Data</div>
                  <div className="text-xs text-gray-600">Price, amount, currency rules</div>
                </button>
                <button className="p-3 border border-gray-200 rounded-lg hover:bg-gray-50 text-left transition-colors">
                  <div className="text-sm text-gray-900 mb-1">User Data</div>
                  <div className="text-xs text-gray-600">Username, email, profile fields</div>
                </button>
                <button className="p-3 border border-gray-200 rounded-lg hover:bg-gray-50 text-left transition-colors">
                  <div className="text-sm text-gray-900 mb-1">Export Config</div>
                  <div className="text-xs text-gray-600">Save rules as template</div>
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
