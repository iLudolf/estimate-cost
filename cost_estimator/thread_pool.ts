import { Worker } from "node:worker_threads";
import { cpus } from "node:os";
import type { TableInfo, TextColumnsMode } from "./db/types.js";
import type { WorkerRequest, WorkerResponse } from "./estimate_worker.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type TableTokenEstimate = {
  schema: string;
  table: string;
  rowCount: number;
  tokenCount: number;
};

/** A queue item is either a whole table or a chunk of a large table. */
type QueueItem =
  | { kind: "table"; tableInfo: TableInfo }
  | {
      kind: "chunk";
      tableInfo: TableInfo;
      chunkId: string;
      offsetStart: number;
      rowLimit: number;
      tableKey: string;
    };

/** Aggregates partial chunk results until all chunks of a table are done. */
type ChunkAggregator = {
  schema: string;
  table: string;
  totalChunks: number;
  received: Map<string, { rowCount: number; tokenCount: number }>;
};

export type PoolOptions = {
  /** Max concurrent worker threads. Defaults to (CPU cores − 1, min 1). */
  maxThreads?: number;
  /**
   * How many items to assign per batch to each thread.
   * When a thread finishes its batch it receives more items from the queue.
   * Defaults to 3.
   */
  tablesPerBatch?: number;
  dbUrl: string;
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
  batchSize: number;
  /** Row count per table (from fetchTableSnapshot). Key: "schema.table" */
  rowCounts?: Map<string, number>;
  /** Tables with more rows than this are split into chunks. Default: 50000 */
  largeTableThreshold?: number;
  /** Rows per chunk for large tables. Default: 10000 */
  chunkSize?: number;
  /** Called when a table (or chunk) begins processing on a worker. */
  onTableStart?: (schema: string, table: string, chunkLabel?: string) => void;
  /** Called every time a single table finishes (for progressive output). */
  onTableComplete: (result: TableTokenEstimate) => void;
  /** Called when a table fails. */
  onTableError: (schema: string, table: string, error: string) => void;
  /** Called when a chunk of a large table completes (for progressive UI). */
  onChunkComplete?: (
    schema: string,
    table: string,
    completedChunks: number,
    totalChunks: number,
  ) => void;
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const WORKER_PATH = new URL("./estimate_worker.ts", import.meta.url).toString();

function createWorkerThread(): Worker {
  const bootstrapCode = [
    `import { register } from 'tsx/esm/api';`,
    `register();`,
    `await import(${JSON.stringify(WORKER_PATH)});`,
  ].join('\n');
  return new Worker(bootstrapCode, { eval: true });
}

function tableKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}

// ---------------------------------------------------------------------------
// Build the work queue – split large tables into chunks
// ---------------------------------------------------------------------------

function buildQueue(
  tables: TableInfo[],
  rowCounts: Map<string, number>,
  largeTableThreshold: number,
  chunkSize: number,
): { queue: QueueItem[]; chunkCounts: Map<string, number> } {
  const queue: QueueItem[] = [];
  const chunkCounts = new Map<string, number>();

  for (const tableInfo of tables) {
    const key = tableKey(tableInfo.schema, tableInfo.table);
    const rowCount = rowCounts.get(key) ?? 0;

    if (rowCount >= largeTableThreshold && chunkSize > 0) {
      const numChunks = Math.ceil(rowCount / chunkSize);
      chunkCounts.set(key, numChunks);

      for (let i = 0; i < numChunks; i++) {
        queue.push({
          kind: "chunk",
          tableInfo,
          chunkId: `${key}#${i}`,
          offsetStart: i * chunkSize,
          rowLimit: chunkSize,
          tableKey: key,
        });
      }
    } else {
      queue.push({ kind: "table", tableInfo });
    }
  }

  return { queue, chunkCounts };
}

// ---------------------------------------------------------------------------
// Thread pool – processes a queue of tables/chunks across N workers
// ---------------------------------------------------------------------------

export function processTablesInParallel(
  tables: TableInfo[],
  options: PoolOptions,
): Promise<TableTokenEstimate[]> {
  const {
    maxThreads = Math.max(1, cpus().length - 1),
    tablesPerBatch = 3,
    dbUrl,
    textColumnsMode,
    excludedColumns,
    batchSize,
    rowCounts = new Map(),
    largeTableThreshold = 50_000,
    chunkSize = 10_000,
    onTableStart,
    onTableComplete,
    onTableError,
    onChunkComplete,
  } = options;

  if (tables.length === 0) {
    return Promise.resolve([]);
  }

  const { queue, chunkCounts } = buildQueue(tables, rowCounts, largeTableThreshold, chunkSize);
  const workerCount = Math.min(maxThreads, Math.ceil(queue.length / tablesPerBatch));
  const results: TableTokenEstimate[] = [];

  // Chunk aggregation
  const aggregators = new Map<string, ChunkAggregator>();

  return new Promise<TableTokenEstimate[]>((resolve, reject) => {
    let aliveWorkers = 0;
    const workerErrors: Error[] = [];

    // -- dispatch next batch to an idle worker --------------------------------
    function dispatchBatch(worker: Worker): void {
      const batch: QueueItem[] = [];
      for (let i = 0; i < tablesPerBatch && queue.length > 0; i++) {
        batch.push(queue.shift()!);
      }

      if (batch.length === 0) {
        worker.postMessage({ type: "shutdown" } satisfies WorkerRequest);
        return;
      }

      (worker as any).__batch = batch;
      sendNextFromBatch(worker);
    }

    // -- send the next item from the worker's current batch -------------------
    function sendNextFromBatch(worker: Worker): void {
      const batch: QueueItem[] | undefined = (worker as any).__batch;
      if (!batch || batch.length === 0) {
        dispatchBatch(worker);
        return;
      }

      const item = batch.shift()!;

      if (item.kind === "table") {
        onTableStart?.(item.tableInfo.schema, item.tableInfo.table);
        const msg: WorkerRequest = {
          type: "process",
          payload: { tableInfo: item.tableInfo, dbUrl, textColumnsMode, excludedColumns, batchSize },
        };
        worker.postMessage(msg);
      } else {
        const key = item.tableKey;
        const totalChunks = chunkCounts.get(key) ?? 1;
        const chunkIndex = parseInt(item.chunkId.split("#")[1], 10) + 1;
        onTableStart?.(
          item.tableInfo.schema,
          item.tableInfo.table,
          `chunk ${chunkIndex}/${totalChunks}`,
        );
        const msg: WorkerRequest = {
          type: "process-chunk",
          payload: {
            tableInfo: item.tableInfo,
            dbUrl,
            textColumnsMode,
            excludedColumns,
            batchSize,
            chunkId: item.chunkId,
            offsetStart: item.offsetStart,
            rowLimit: item.rowLimit,
          },
        };
        worker.postMessage(msg);
      }
    }

    // -- handle a chunk result: aggregate until all chunks arrive --------------
    function handleChunkResult(data: {
      schema: string;
      table: string;
      chunkId: string;
      rowCount: number;
      tokenCount: number;
    }): void {
      const key = tableKey(data.schema, data.table);
      const totalChunks = chunkCounts.get(key) ?? 1;

      let agg = aggregators.get(key);
      if (!agg) {
        agg = { schema: data.schema, table: data.table, totalChunks, received: new Map() };
        aggregators.set(key, agg);
      }

      agg.received.set(data.chunkId, { rowCount: data.rowCount, tokenCount: data.tokenCount });
      onChunkComplete?.(data.schema, data.table, agg.received.size, totalChunks);

      // All chunks collected → aggregate and emit
      if (agg.received.size === agg.totalChunks) {
        let totalRows = 0;
        let totalTokens = 0;
        for (const partial of agg.received.values()) {
          totalRows += partial.rowCount;
          totalTokens += partial.tokenCount;
        }

        const aggregated: TableTokenEstimate = {
          schema: agg.schema,
          table: agg.table,
          rowCount: totalRows,
          tokenCount: totalTokens,
        };
        results.push(aggregated);
        onTableComplete(aggregated);
        aggregators.delete(key);
      }
    }

    // -- called when a worker is gone -----------------------------------------
    function onWorkerDone(): void {
      aliveWorkers--;
      if (aliveWorkers === 0) {
        if (results.length === 0 && workerErrors.length > 0) {
          reject(new AggregateError(workerErrors, "All workers failed"));
        } else {
          resolve(results);
        }
      }
    }

    // -- create a worker and wire up its event handlers -----------------------
    function spawnWorker(): void {
      const worker = createWorkerThread();
      aliveWorkers++;

      worker.on("message", (msg: WorkerResponse) => {
        switch (msg.type) {
          case "ready":
            dispatchBatch(worker);
            break;

          case "result":
            results.push(msg.data);
            onTableComplete(msg.data);
            sendNextFromBatch(worker);
            break;

          case "chunk-result":
            handleChunkResult(msg.data);
            sendNextFromBatch(worker);
            break;

          case "error":
            onTableError(msg.schema, msg.table, msg.error);
            sendNextFromBatch(worker);
            break;
        }
      });

      worker.on("error", (err) => {
        const error = err instanceof Error ? err : new Error(String(err));
        console.error("[thread-pool] Worker crashed:", error.message);
        workerErrors.push(error);
        onWorkerDone();
      });

      worker.on("exit", () => {
        onWorkerDone();
      });
    }

    // -- spin up the pool -----------------------------------------------------
    for (let i = 0; i < workerCount; i++) {
      spawnWorker();
    }
  });
}
