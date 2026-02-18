import "dotenv/config";
import { cpus } from "node:os";
import { fileURLToPath } from "node:url";
import {
  createPostgresPool,
  closePostgresPool,
  discoverTables,
  fetchTableSnapshot,
} from "../db_sync_graph/postgres.js";
import { processTablesInParallel } from "./thread_pool.js";
import type { TableTokenEstimate } from "./thread_pool.js";
import type { TextColumnsMode } from "../db_sync_graph/state.js";
import { TerminalUI } from "./terminal_ui.js";
import { ProgressFileWriter } from "./progress_file.js";

// ---------------------------------------------------------------------------
// Pricing
// ---------------------------------------------------------------------------

/** Price per 1 million tokens for each supported embedding model. */
export const EMBEDDING_MODEL_PRICING: Record<string, number> = {
  "text-embedding-3-small": 0.02,
  "text-embedding-3-large": 0.13,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type { TableTokenEstimate };

export type CostEstimationResult = {
  tables: TableTokenEstimate[];
  totalTokens: number;
  costByModel: Record<string, number>;
};

// ---------------------------------------------------------------------------
// Main estimation (parallel)
// ---------------------------------------------------------------------------

export async function estimateCost(params: {
  sourceDbUrl: string;
  sourceSchema: string;
  tableAllowlist: string[];
  tableBlocklist?: string[];
  updatedAtCandidates: string[];
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
  batchSize: number;
  maxThreads?: number;
  tablesPerBatch?: number;
  largeTableThreshold?: number;
  chunkSize?: number;
  ui?: TerminalUI;
  progressWriter?: ProgressFileWriter;
}): Promise<CostEstimationResult> {
  const pool = createPostgresPool(params.sourceDbUrl);
  const ui = params.ui;
  const progressWriter = params.progressWriter;

  try {
    // Discovery still happens on the main thread (lightweight I/O)
    const tableInfos = await discoverTables({
      pool,
      sourceSchema: params.sourceSchema,
      tableAllowlist: params.tableAllowlist,
      tableBlocklist: params.tableBlocklist,
      updatedAtCandidates: params.updatedAtCandidates,
    });

    // Fetch row counts for all tables (to detect large ones)
    const rowCounts = new Map<string, number>();
    const snapshots = await Promise.all(
      tableInfos.map(async (tableInfo) => {
        const snapshot = await fetchTableSnapshot({ pool, tableInfo });
        return { key: `${tableInfo.schema}.${tableInfo.table}`, rowCount: snapshot.rowCount };
      }),
    );
    for (const { key, rowCount } of snapshots) {
      rowCounts.set(key, rowCount);
    }

    const maxThreads = params.maxThreads ?? Math.max(1, cpus().length - 1);
    const tablesPerBatch = params.tablesPerBatch ?? 3;
    const largeTableThreshold = params.largeTableThreshold ?? 50_000;
    const chunkSize = params.chunkSize ?? 10_000;

    const largeTables = [...rowCounts.entries()].filter(
      ([, count]) => count >= largeTableThreshold,
    );

    const workerCount = Math.min(maxThreads, tableInfos.length);
    let discoveryMsg =
      `Found ${tableInfos.length} table(s). Using ${workerCount} thread(s) ` +
      `(max ${maxThreads}, ${cpus().length} CPUs available)`;
    if (largeTables.length > 0) {
      discoveryMsg += `. ${largeTables.length} large table(s) will be split into chunks`;
    }

    if (progressWriter) {
      progressWriter.setTotalTables(tableInfos.length);
      await progressWriter.initialize();
    }

    if (ui) {
      ui.stopSpinner(discoveryMsg);
      ui.setTotalTables(tableInfos.length);
      ui.startProgress();
    } else {
      console.log(`  ${discoveryMsg}\n`);
      const sep = "  " + "-".repeat(60);
      console.log("  Progressive results:");
      console.log(sep);
      console.log(
        "  " + "Table".padEnd(32) + "Rows".padStart(10) + "Tokens".padStart(15),
      );
      console.log(sep);
    }

    let completedCount = 0;

    // Process all tables in parallel via worker threads
    const tables = await processTablesInParallel(tableInfos, {
      maxThreads,
      tablesPerBatch,
      dbUrl: params.sourceDbUrl,
      textColumnsMode: params.textColumnsMode,
      excludedColumns: params.excludedColumns,
      batchSize: params.batchSize,
      rowCounts,
      largeTableThreshold,
      chunkSize,
      onTableStart: ui
        ? (schema, table, chunkLabel) => ui.onTableStart(schema, table, chunkLabel)
        : undefined,
      onTableComplete: (result) => {
        completedCount++;
        if (ui) {
          ui.onTableComplete(result);
        } else {
          const name = `${result.schema}.${result.table}`;
          console.log(
            `  ${name.padEnd(32)}${result.rowCount.toLocaleString().padStart(10)}${result.tokenCount.toLocaleString().padStart(15)}   [${completedCount}/${tableInfos.length}]`,
          );
        }
        progressWriter?.addCompletedTable(result);
      },
      onTableError: (schema, table, error) => {
        completedCount++;
        if (ui) {
          ui.onTableError(schema, table, error);
        } else {
          console.error(
            `  [ERROR] ${schema}.${table}: ${error}   [${completedCount}/${tableInfos.length}]`,
          );
        }
        progressWriter?.addErrorTable(schema, table, error);
      },
      onChunkComplete: ui
        ? (schema, table, completedChunks, totalChunks) =>
            ui.onChunkComplete(schema, table, completedChunks, totalChunks)
        : undefined,
    });

    if (ui) {
      ui.finishProgress();
    } else {
      console.log("  " + "-".repeat(60));
    }

    if (progressWriter) {
      await progressWriter.finalize();
    }

    const totalTokens = tables.reduce((sum, t) => sum + t.tokenCount, 0);

    const costByModel: Record<string, number> = {};
    for (const [model, pricePerMillion] of Object.entries(EMBEDDING_MODEL_PRICING)) {
      costByModel[model] = (totalTokens / 1_000_000) * pricePerMillion;
    }

    return { tables, totalTokens, costByModel };
  } finally {
    await closePostgresPool(pool);
  }
}

// ---------------------------------------------------------------------------
// Report formatting
// ---------------------------------------------------------------------------

function formatReport(result: CostEstimationResult): string {
  const lines: string[] = [];
  const separator = "\u2500".repeat(74);

  lines.push("");
  lines.push(separator);
  lines.push("  EMBEDDING COST ESTIMATION REPORT");
  lines.push(separator);
  lines.push("");

  // Per-table breakdown
  lines.push("  Table Breakdown:");
  lines.push("  " + "-".repeat(70));
  lines.push(
    "  " +
      "Table".padEnd(32) +
      "Rows".padStart(10) +
      "Tokens".padStart(15),
  );
  lines.push("  " + "-".repeat(70));

  for (const table of result.tables) {
    const name = `${table.schema}.${table.table}`;
    lines.push(
      "  " +
        name.padEnd(32) +
        table.rowCount.toLocaleString().padStart(10) +
        table.tokenCount.toLocaleString().padStart(15),
    );
  }

  const totalRows = result.tables.reduce((sum, t) => sum + t.rowCount, 0);

  lines.push("  " + "-".repeat(70));
  lines.push(
    "  " +
      "TOTAL".padEnd(32) +
      totalRows.toLocaleString().padStart(10) +
      result.totalTokens.toLocaleString().padStart(15),
  );

  lines.push("");
  lines.push("  Embedding Cost per Model:");
  lines.push("  " + "-".repeat(70));

  for (const [model, cost] of Object.entries(result.costByModel)) {
    const pricePerM = EMBEDDING_MODEL_PRICING[model];
    lines.push(
      `  ${model.padEnd(30)}  $${pricePerM}/1M tokens  =>  $${cost.toFixed(6)}`,
    );
  }

  lines.push(separator);
  lines.push("");

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

function buildDbUrlFromEnv(): string | undefined {
  const host = process.env.DB_HOST;
  const port = process.env.DB_PORT || "5432";
  const dbName = stripQuotes(process.env.DB_NAME);
  const username = stripQuotes(process.env.DB_USERNAME);
  const password = stripQuotes(process.env.DB_PASSWORD);

  if (!host || !dbName || !username || !password) {
    return undefined;
  }

  return `postgres://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${encodeURIComponent(dbName)}`;
}

function stripQuotes(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1).trim();
  }
  return trimmed;
}

async function main(): Promise<void> {
  const sourceDbUrl = buildDbUrlFromEnv() || process.env.SOURCE_DB_URL;
  if (!sourceDbUrl) {
    console.error(
      "ERROR: Database connection required. Set SOURCE_DB_URL or DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD.",
    );
    process.exit(1);
  }

  const sourceSchema =
    stripQuotes(process.env.SOURCE_SCHEMA) ||
    stripQuotes(process.env.DB_SCHEMA) ||
    "public";

  const tableAllowlistRaw = process.env.SOURCE_TABLE_ALLOWLIST || "";
  const tableAllowlist = tableAllowlistRaw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  if (tableAllowlist.length === 0) {
    console.error("ERROR: SOURCE_TABLE_ALLOWLIST environment variable is required.");
    process.exit(1);
  }

  const tableBlocklistRaw = process.env.SOURCE_TABLE_BLOCKLIST || "";
  const tableBlocklist = tableBlocklistRaw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const updatedAtCandidatesRaw =
    process.env.SOURCE_UPDATED_AT_CANDIDATES || "updated_at,modified_at,updatedon";
  const updatedAtCandidates = updatedAtCandidatesRaw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const textColumnsMode: TextColumnsMode =
    process.env.TEXT_COLUMNS_MODE === "all" ? "all" : "auto";

  const excludedColumnsRaw = process.env.EXCLUDED_COLUMNS || "";
  const excludedColumns = excludedColumnsRaw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const batchSize = parseInt(process.env.SOURCE_BATCH_SIZE || "1000", 10);
  const maxThreads = parseInt(
    process.env.MAX_THREADS || String(Math.max(1, cpus().length - 1)),
    10,
  );
  const tablesPerBatch = parseInt(process.env.TABLES_PER_BATCH || "3", 10);
  const largeTableThreshold = parseInt(process.env.LARGE_TABLE_THRESHOLD || "50000", 10);
  const chunkSize = parseInt(process.env.CHUNK_SIZE || "10000", 10);

  const ui = new TerminalUI({ totalTables: 0 });

  const progressFilePath = process.env.COST_PROGRESS_FILE || "./cost_estimation_progress.json";
  const progressWriter = new ProgressFileWriter({
    filePath: progressFilePath,
    totalTables: 0,
    modelPricing: EMBEDDING_MODEL_PRICING,
  });

  try {
    ui.showSpinner("Connecting to PostgreSQL and estimating embedding costs...");

    const result = await estimateCost({
      sourceDbUrl,
      sourceSchema,
      tableAllowlist,
      tableBlocklist,
      updatedAtCandidates,
      textColumnsMode,
      excludedColumns,
      batchSize,
      maxThreads,
      tablesPerBatch,
      largeTableThreshold,
      chunkSize,
      ui,
      progressWriter,
    });

    console.log(formatReport(result));
  } finally {
    ui.destroy();
  }
}

// Only run as CLI entry point, not when imported as a module
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}
