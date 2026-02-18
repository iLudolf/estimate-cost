import { parentPort } from "node:worker_threads";
import { Pool } from "pg";
import { encodingForModel } from "js-tiktoken";
import type { TiktokenModel } from "js-tiktoken";
import { fetchTableRows } from "../db_sync_graph/postgres.js";
import { transformRowToDocument } from "../db_sync_graph/transform.js";
import type { TableInfo, TextColumnsMode } from "../db_sync_graph/state.js";

// ---------------------------------------------------------------------------
// Types – messages exchanged between main thread and worker
// ---------------------------------------------------------------------------

type ProcessPayload = {
  tableInfo: TableInfo;
  dbUrl: string;
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
  batchSize: number;
};

export type WorkerRequest =
  | { type: "process"; payload: ProcessPayload }
  | {
      type: "process-chunk";
      payload: ProcessPayload & {
        chunkId: string;
        offsetStart: number;
        rowLimit: number;
      };
    }
  | { type: "shutdown" };

export type WorkerResponse =
  | { type: "ready" }
  | {
      type: "result";
      data: { schema: string; table: string; rowCount: number; tokenCount: number };
    }
  | {
      type: "chunk-result";
      data: {
        schema: string;
        table: string;
        chunkId: string;
        rowCount: number;
        tokenCount: number;
      };
    }
  | { type: "error"; schema: string; table: string; error: string };

// ---------------------------------------------------------------------------
// Internal state – one pg Pool per worker, lazily created
// ---------------------------------------------------------------------------

let pool: Pool | null = null;
let currentDbUrl: string | null = null;

function getPool(dbUrl: string): Pool {
  if (!pool || currentDbUrl !== dbUrl) {
    pool = new Pool({ connectionString: dbUrl, max: 2 });
    currentDbUrl = dbUrl;
  }
  return pool;
}

// ---------------------------------------------------------------------------
// Token counting (CPU-intensive – main reason for worker threads)
// ---------------------------------------------------------------------------

let cachedEncoder: ReturnType<typeof encodingForModel> | null = null;

function countTokens(texts: string[]): number {
  if (!cachedEncoder) {
    cachedEncoder = encodingForModel("text-embedding-3-small" as TiktokenModel);
  }
  let total = 0;
  for (const text of texts) {
    total += cachedEncoder.encode(text).length;
  }
  return total;
}

// ---------------------------------------------------------------------------
// Fetch rows, transform, and collect page contents
// ---------------------------------------------------------------------------

async function fetchAndTransform(params: {
  pgPool: Pool;
  tableInfo: TableInfo;
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
  batchSize: number;
  offsetStart: number;
  rowLimit: number | null; // null = fetch all rows
}): Promise<string[]> {
  const { pgPool, tableInfo, textColumnsMode, excludedColumns, batchSize, offsetStart, rowLimit } =
    params;

  const pageContents: string[] = [];
  let offset = offsetStart;
  let fetched = 0;

  while (true) {
    const limit =
      rowLimit !== null ? Math.min(batchSize, rowLimit - fetched) : batchSize;

    if (limit <= 0) break;

    const rows = await fetchTableRows({
      pool: pgPool,
      tableInfo,
      limit,
      offset,
    });

    if (rows.length === 0) break;

    for (const row of rows) {
      const { document } = transformRowToDocument({
        row,
        tableInfo,
        runId: "cost-estimate",
        textColumnsMode,
        excludedColumns,
      });
      pageContents.push(document.pageContent);
    }

    fetched += rows.length;
    offset += rows.length;

    if (rowLimit !== null && fetched >= rowLimit) break;
  }

  return pageContents;
}

// ---------------------------------------------------------------------------
// Process a full table (small tables)
// ---------------------------------------------------------------------------

async function processTable(
  payload: ProcessPayload,
): Promise<{ schema: string; table: string; rowCount: number; tokenCount: number }> {
  const { tableInfo, dbUrl, textColumnsMode, excludedColumns, batchSize } = payload;
  const pgPool = getPool(dbUrl);

  const pageContents = await fetchAndTransform({
    pgPool,
    tableInfo,
    textColumnsMode,
    excludedColumns,
    batchSize,
    offsetStart: 0,
    rowLimit: null,
  });

  const tokenCount = countTokens(pageContents);

  return {
    schema: tableInfo.schema,
    table: tableInfo.table,
    rowCount: pageContents.length,
    tokenCount,
  };
}

// ---------------------------------------------------------------------------
// Process a chunk of a large table
// ---------------------------------------------------------------------------

async function processChunk(
  payload: ProcessPayload & { chunkId: string; offsetStart: number; rowLimit: number },
): Promise<{
  schema: string;
  table: string;
  chunkId: string;
  rowCount: number;
  tokenCount: number;
}> {
  const { tableInfo, dbUrl, textColumnsMode, excludedColumns, batchSize, chunkId, offsetStart, rowLimit } =
    payload;
  const pgPool = getPool(dbUrl);

  const pageContents = await fetchAndTransform({
    pgPool,
    tableInfo,
    textColumnsMode,
    excludedColumns,
    batchSize,
    offsetStart,
    rowLimit,
  });

  const tokenCount = countTokens(pageContents);

  return {
    schema: tableInfo.schema,
    table: tableInfo.table,
    chunkId,
    rowCount: pageContents.length,
    tokenCount,
  };
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

const port = parentPort!;

port.on("message", async (msg: WorkerRequest) => {
  if (msg.type === "process") {
    try {
      const result = await processTable(msg.payload);
      port.postMessage({ type: "result", data: result } satisfies WorkerResponse);
    } catch (err) {
      port.postMessage({
        type: "error",
        schema: msg.payload.tableInfo.schema,
        table: msg.payload.tableInfo.table,
        error: err instanceof Error ? err.message : String(err),
      } satisfies WorkerResponse);
    }
  } else if (msg.type === "process-chunk") {
    try {
      const result = await processChunk(msg.payload);
      port.postMessage({ type: "chunk-result", data: result } satisfies WorkerResponse);
    } catch (err) {
      port.postMessage({
        type: "error",
        schema: msg.payload.tableInfo.schema,
        table: msg.payload.tableInfo.table,
        error: err instanceof Error ? err.message : String(err),
      } satisfies WorkerResponse);
    }
  } else if (msg.type === "shutdown") {
    if (pool) {
      await pool.end();
      pool = null;
    }
    process.exit(0);
  }
});

// Signal that this worker is ready to receive tasks
port.postMessage({ type: "ready" } satisfies WorkerResponse);
