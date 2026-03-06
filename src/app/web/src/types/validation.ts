export type ValidationStrategy = 'full' | 'sampling' | 'incremental' | 'key-fields';
export type ValidationRuleType = 'exact-match' | 'range' | 'format' | 'custom' | 'null-check' | 'type-check';

export interface ValidationRule {
  id: string;
  fieldName: string;
  ruleType: ValidationRuleType;
  description: string;
  enabled: boolean;
  config?: {
    tolerance?: number;
    pattern?: string;
    customScript?: string;
    allowNull?: boolean;
  };
}

export interface ValidationConfig {
  jobId: string;
  strategy: ValidationStrategy;
  sampleSize?: number;
  keyFields: string[];
  rules: ValidationRule[];
  autoValidate: boolean;
  validationBatchSize: number;
}

export interface ValidationResult {
  jobId: string;
  executionTime: string;
  strategy: ValidationStrategy;
  totalRows: number;
  validatedRows: number;
  passedRows: number;
  failedRows: number;
  errorsByType: {
    valueMismatch: number;
    formatError: number;
    missingRow: number;
    extraRow: number;
    nullViolation: number;
    typeError: number;
  };
  detailedErrors: ValidationErrorDetail[];
}

export interface ValidationErrorDetail {
  rowKey: string;
  fieldName: string;
  errorType: string;
  sourceValue: unknown;
  targetValue: unknown;
  expectedValue?: unknown;
  rule?: string;
  severity: 'critical' | 'warning' | 'info';
}
