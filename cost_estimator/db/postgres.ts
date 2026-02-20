import { Pool } from "pg";
import { ColumnInfo, TableInfo } from "./types.js";

type TableNameRow = {
  table_name: string;
};

type ColumnRow = {
  column_name: string;
  data_type: string;
  is_nullable: "YES" | "NO";
  column_default: string | null;
  ordinal_position: number;
};

type PrimaryKeyRow = {
  column_name: string;
  pk_position: number;
};

type SnapshotRow = {
  row_count: string;
  max_updated_at: Date | string | null;
  max_pk_lexicographic: string | null;
};

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export function createPostgresPool(sourceDbUrl: string): Pool {
  return new Pool({
    connectionString: sourceDbUrl,
  });
}

export async function closePostgresPool(pool: Pool): Promise<void> {
  await pool.end();
}

export async function discoverTables(params: {
  pool: Pool;
  sourceSchema: string;
  tableAllowlist: string[];
  tableBlocklist?: string[];
  updatedAtCandidates: string[];
}): Promise<TableInfo[]> {
  const { pool, sourceSchema, tableAllowlist, tableBlocklist, updatedAtCandidates } = params;

  const discoveredTables = await pool.query<TableNameRow>(
    `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = $1
        AND table_type = 'BASE TABLE'
      ORDER BY table_name;
    `,
    [sourceSchema],
  );

  const includeAll = tableAllowlist.includes("*");
  const allowlist = new Set(tableAllowlist.map((name) => name.toLowerCase()));
  const blocklist = new Set((tableBlocklist ?? []).map((name) => name.toLowerCase()));
  const tableInfos: TableInfo[] = [];

  for (const tableRow of discoveredTables.rows) {
    const tableName = tableRow.table_name.toLowerCase();
    if (!includeAll && !allowlist.has(tableName)) {
      continue;
    }
    if (blocklist.has(tableName)) {
      continue;
    }

    const columnsResult = await pool.query<ColumnRow>(
      `
        SELECT
          column_name,
          data_type,
          is_nullable,
          column_default,
          ordinal_position
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position;
      `,
      [sourceSchema, tableRow.table_name],
    );

    const primaryKeyResult = await pool.query<PrimaryKeyRow>(
      `
        SELECT
          kcu.column_name,
          kcu.ordinal_position AS pk_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        ORDER BY kcu.ordinal_position;
      `,
      [sourceSchema, tableRow.table_name],
    );

    const pkPositions = new Map<string, number>();
    for (const pk of primaryKeyResult.rows) {
      pkPositions.set(pk.column_name, pk.pk_position);
    }

    const columns: ColumnInfo[] = columnsResult.rows.map((columnRow) => ({
      columnName: columnRow.column_name,
      dataType: columnRow.data_type,
      isNullable: columnRow.is_nullable === "YES",
      columnDefault: columnRow.column_default,
      ordinalPosition: Number(columnRow.ordinal_position),
      pkPosition: pkPositions.get(columnRow.column_name) ?? null,
    }));

    const updatedAtColumn = findUpdatedAtColumn(columns, updatedAtCandidates);

    tableInfos.push({
      schema: sourceSchema,
      table: tableRow.table_name,
      columns,
      pkColumns: primaryKeyResult.rows.map((row) => row.column_name),
      updatedAtColumn,
    });
  }

  return tableInfos;
}

function findUpdatedAtColumn(
  columns: ColumnInfo[],
  updatedAtCandidates: string[],
): string | null {
  const byLowerName = new Map(
    columns.map((column) => [column.columnName.toLowerCase(), column.columnName]),
  );

  for (const candidate of updatedAtCandidates) {
    const resolved = byLowerName.get(candidate.toLowerCase());
    if (resolved) {
      return resolved;
    }
  }

  return null;
}

function buildMaxPkExpression(pkColumns: string[]): string {
  if (pkColumns.length === 0) {
    return "NULL";
  }

  const serializedColumns = pkColumns.map(
    (column) => `COALESCE(${quoteIdentifier(column)}::text, '')`,
  );

  if (serializedColumns.length === 1) {
    return serializedColumns[0];
  }

  return `concat_ws('|', ${serializedColumns.join(", ")})`;
}

export async function fetchTableSnapshot(params: {
  pool: Pool;
  tableInfo: TableInfo;
}): Promise<{
  rowCount: number;
  maxUpdatedAt: string | null;
  maxPkLexicographic: string | null;
}> {
  const { pool, tableInfo } = params;

  const qualifiedTable = `${quoteIdentifier(tableInfo.schema)}.${quoteIdentifier(
    tableInfo.table,
  )}`;

  const updatedAtExpression = tableInfo.updatedAtColumn
    ? `MAX(${quoteIdentifier(tableInfo.updatedAtColumn)})`
    : "NULL";

  const maxPkExpression = buildMaxPkExpression(tableInfo.pkColumns);

  const snapshotResult = await pool.query<SnapshotRow>(
    `
      SELECT
        COUNT(*)::bigint AS row_count,
        ${updatedAtExpression} AS max_updated_at,
        MAX(${maxPkExpression}) AS max_pk_lexicographic
      FROM ${qualifiedTable};
    `,
  );

  const row = snapshotResult.rows[0];
  const rowCount = Number.parseInt(row.row_count, 10);
  const parsedMaxUpdatedAt =
    row.max_updated_at == null ? null : new Date(row.max_updated_at).toISOString();

  return {
    rowCount: Number.isFinite(rowCount) ? rowCount : 0,
    maxUpdatedAt: parsedMaxUpdatedAt,
    maxPkLexicographic: row.max_pk_lexicographic,
  };
}

export async function fetchTableRows(params: {
  pool: Pool;
  tableInfo: TableInfo;
  limit: number;
  offset: number;
}): Promise<Record<string, unknown>[]> {
  const { pool, tableInfo, limit, offset } = params;

  const qualifiedTable = `${quoteIdentifier(tableInfo.schema)}.${quoteIdentifier(
    tableInfo.table,
  )}`;
  const columns = tableInfo.columns.map((column) => quoteIdentifier(column.columnName));
  const orderBy =
    tableInfo.pkColumns.length > 0
      ? tableInfo.pkColumns.map((column) => quoteIdentifier(column)).join(", ")
      : columns.join(", ");

  const rowsResult = await pool.query<Record<string, unknown>>(
    `
      SELECT ${columns.join(", ")}
      FROM ${qualifiedTable}
      ORDER BY ${orderBy}
      LIMIT $1
      OFFSET $2;
    `,
    [limit, offset],
  );

  return rowsResult.rows;
}
