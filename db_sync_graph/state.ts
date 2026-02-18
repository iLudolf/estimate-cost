import { Annotation } from "@langchain/langgraph";

export type RunStatus = "running" | "success" | "partial_success" | "failed";

export type TableMode = "full" | "skip";

export type TableResultStatus = "reindexed" | "skipped" | "failed";

export type TextColumnsMode = "auto" | "all";

export type ColumnInfo = {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  columnDefault: string | null;
  ordinalPosition: number;
  pkPosition: number | null;
};

export type TableInfo = {
  schema: string;
  table: string;
  columns: ColumnInfo[];
  pkColumns: string[];
  updatedAtColumn: string | null;
};

export type TableSnapshot = {
  schema: string;
  table: string;
  rowCount: number;
  maxUpdatedAt: string | null;
  maxPkLexicographic: string | null;
  schemaHash: string;
  tableHash: string;
};

export type TablePlan = {
  schema: string;
  table: string;
  mode: TableMode;
  reason: string;
};

export type TableRunResult = {
  schema: string;
  table: string;
  mode: TableMode;
  status: TableResultStatus;
  rowsUpserted: number;
  error: string | null;
};

export type RunError = {
  tableKey: string;
  message: string;
};

export type RunSummary = {
  tablesTotal: number;
  tablesReindexed: number;
  tablesSkipped: number;
  rowsUpserted: number;
  errors: RunError[];
};

export type CatalogByTable = Record<string, ControlCatalogRecord | null>;

export type ControlCatalogRecord = {
  schema: string;
  table: string;
  schema_hash: string;
  table_hash: string;
  row_count: number;
  max_updated_at: string | null;
  last_success_run_id: string | null;
  last_success_at: string | null;
  last_mode: TableMode | null;
  last_error: string | null;
};

export function tableKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}

export function defaultRunSummary(): RunSummary {
  return {
    tablesTotal: 0,
    tablesReindexed: 0,
    tablesSkipped: 0,
    rowsUpserted: 0,
    errors: [],
  };
}

export const DbSyncStateAnnotation = Annotation.Root({
  runId: Annotation<string | null>({
    reducer: (_existing: string | null, incoming: string | null) => incoming,
    default: () => null,
  }),
  startedAt: Annotation<string | null>({
    reducer: (_existing: string | null, incoming: string | null) => incoming,
    default: () => null,
  }),
  finishedAt: Annotation<string | null>({
    reducer: (_existing: string | null, incoming: string | null) => incoming,
    default: () => null,
  }),
  fatalError: Annotation<string | null>({
    default: () => null,
    reducer: (existing: string | null, incoming: string | null) =>
      incoming ?? existing,
  }),
  status: Annotation<RunStatus>({
    reducer: (_existing: RunStatus, incoming: RunStatus) => incoming,
    default: () => "running",
  }),
  tableInfos: Annotation<TableInfo[]>({
    reducer: (_existing: TableInfo[], incoming: TableInfo[]) => incoming,
    default: () => [],
  }),
  tableSnapshots: Annotation<TableSnapshot[]>({
    reducer: (_existing: TableSnapshot[], incoming: TableSnapshot[]) => incoming,
    default: () => [],
  }),
  tablePlans: Annotation<TablePlan[]>({
    reducer: (_existing: TablePlan[], incoming: TablePlan[]) => incoming,
    default: () => [],
  }),
  tableResults: Annotation<TableRunResult[]>({
    reducer: (_existing: TableRunResult[], incoming: TableRunResult[]) =>
      incoming,
    default: () => [],
  }),
  catalogByTable: Annotation<CatalogByTable>({
    reducer: (_existing: CatalogByTable, incoming: CatalogByTable) => incoming,
    default: () => ({}),
  }),
  summary: Annotation<RunSummary>({
    reducer: (_existing: RunSummary, incoming: RunSummary) => incoming,
    default: defaultRunSummary,
  }),
});

export type DbSyncState = typeof DbSyncStateAnnotation.State;
