import { Document } from "@langchain/core/documents";
import { computeRowHash, sha256Hex, stableStringify } from "./hashing.js";
import { TableInfo, TextColumnsMode } from "./state.js";

const TEXTUAL_DATA_TYPES = [
  "text",
  "character varying",
  "varchar",
  "character",
  "char",
  "citext",
  "json",
  "jsonb",
  "uuid",
];

function isTextualColumn(dataType: string): boolean {
  const normalizedDataType = dataType.toLowerCase();
  return TEXTUAL_DATA_TYPES.some((candidate) =>
    normalizedDataType.includes(candidate),
  );
}

function normalizeValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "object") {
    return stableStringify(value);
  }

  return String(value);
}

function selectPageContentColumns(params: {
  tableInfo: TableInfo;
  textColumnsMode: TextColumnsMode;
  excludedColumns: Set<string>;
}): string[] {
  const { tableInfo, textColumnsMode, excludedColumns } = params;

  return tableInfo.columns
    .filter((column) => !excludedColumns.has(column.columnName.toLowerCase()))
    .filter((column) =>
      textColumnsMode === "all" ? true : isTextualColumn(column.dataType),
    )
    .map((column) => column.columnName);
}

function createPageContent(params: {
  row: Record<string, unknown>;
  tableInfo: TableInfo;
  textColumnsMode: TextColumnsMode;
  excludedColumns: Set<string>;
}): string {
  const { row, tableInfo, textColumnsMode, excludedColumns } = params;

  const selectedColumns = selectPageContentColumns({
    tableInfo,
    textColumnsMode,
    excludedColumns,
  });

  const lines = selectedColumns
    .map((columnName) => {
      const normalized = normalizeValue(row[columnName]);
      return normalized.length > 0 ? `${columnName}: ${normalized}` : "";
    })
    .filter((line) => line.length > 0);

  if (lines.length > 0) {
    return lines.join("\n");
  }

  return `[${tableInfo.schema}.${tableInfo.table}] ${stableStringify(row)}`;
}

function extractPrimaryKey(
  row: Record<string, unknown>,
  tableInfo: TableInfo,
): Record<string, unknown> {
  const primaryKey: Record<string, unknown> = {};

  for (const primaryKeyColumn of tableInfo.pkColumns) {
    if (!(primaryKeyColumn in row)) {
      throw new Error(
        `Missing PK column ${primaryKeyColumn} in ${tableInfo.schema}.${tableInfo.table}`,
      );
    }
    primaryKey[primaryKeyColumn] = row[primaryKeyColumn];
  }

  return primaryKey;
}

export function buildDocId(params: {
  schema: string;
  table: string;
  primaryKey: Record<string, unknown>;
}): string {
  return sha256Hex(
    `${params.schema}.${params.table}|${stableStringify(params.primaryKey)}`,
  );
}

export type DbSyncDocument = {
  docId: string;
  document: Document;
};

export function transformRowToDocument(params: {
  row: Record<string, unknown>;
  tableInfo: TableInfo;
  runId: string;
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
}): DbSyncDocument {
  const excludedColumns = new Set(
    params.excludedColumns.map((column) => column.toLowerCase()),
  );

  const primaryKey = extractPrimaryKey(params.row, params.tableInfo);
  const primaryKeyHash = sha256Hex(stableStringify(primaryKey));
  const rowHash = computeRowHash(params.row);
  const pageContent = createPageContent({
    row: params.row,
    tableInfo: params.tableInfo,
    textColumnsMode: params.textColumnsMode,
    excludedColumns,
  });
  const updatedAt = params.tableInfo.updatedAtColumn
    ? normalizeValue(params.row[params.tableInfo.updatedAtColumn]) || null
    : null;

  const docId = buildDocId({
    schema: params.tableInfo.schema,
    table: params.tableInfo.table,
    primaryKey,
  });

  return {
    docId,
    document: new Document({
      pageContent,
      metadata: {
        schema: params.tableInfo.schema,
        table: params.tableInfo.table,
        pk: primaryKey,
        pk_hash: primaryKeyHash,
        updated_at: updatedAt,
        row_hash: rowHash,
        run_id: params.runId,
      },
    }),
  };
}
