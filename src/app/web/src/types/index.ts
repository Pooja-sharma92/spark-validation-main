export interface FieldInfo {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
}

export interface TableInfo {
  name: string;
  fields: FieldInfo[];
  rowCount?: number;
}

export interface DatabaseInfo {
  name: string;
  tables: TableInfo[];
}

export interface ScanData {
  id: string;
  timestamp: string;
  databases: DatabaseInfo[];
}

export type ChangeType = 'added' | 'removed' | 'modified' | 'unchanged';

export interface FieldComparison {
  name: string;
  changeType: ChangeType;
  field1?: FieldInfo;
  field2?: FieldInfo;
  changes?: string[];
}

export interface TableComparison {
  name: string;
  changeType: ChangeType;
  table1?: TableInfo;
  table2?: TableInfo;
  fieldComparisons?: FieldComparison[];
}

export interface DatabaseComparison {
  name: string;
  changeType: ChangeType;
  database1?: DatabaseInfo;
  database2?: DatabaseInfo;
  tableComparisons?: TableComparison[];
}
