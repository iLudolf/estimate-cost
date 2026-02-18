import { RunnableConfig } from "@langchain/core/runnables";
import { END, START, StateGraph } from "@langchain/langgraph";
import { v4 as uuidv4 } from "uuid";
import {
  buildTableIndexName,
  createElasticClient,
  createTableVectorStore,
  getCatalogRecord,
  makeTextEmbeddings,
  putCatalogRecord,
  putRunRecord,
  RunRecord,
  type ElasticRetrieverProvider,
} from "./elastic_control.js";
import { computeSchemaHash, computeTableHash } from "./hashing.js";
import { closePostgresPool, createPostgresPool, discoverTables, fetchTableRows, fetchTableSnapshot } from "./postgres.js";
import { transformRowToDocument } from "./transform.js";
import {
  CatalogByTable,
  ControlCatalogRecord,
  DbSyncStateAnnotation,
  defaultRunSummary,
  RunError,
  RunStatus,
  TableMode,
  TablePlan,
  TableRunResult,
  tableKey,
} from "./state.js";
import {
  DbSyncConfigurationAnnotation,
  ensureDbSyncConfiguration,
} from "./configuration.js";

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function getTableModeReason(params: {
  previousCatalog: ControlCatalogRecord | null;
  schemaHash: string;
  tableHash: string;
}): { mode: TableMode; reason: string } {
  const { previousCatalog, schemaHash, tableHash } = params;

  if (!previousCatalog) {
    return { mode: "full", reason: "no_previous_catalog" };
  }

  if (previousCatalog.schema_hash !== schemaHash) {
    return { mode: "full", reason: "schema_hash_changed" };
  }

  if (previousCatalog.table_hash !== tableHash) {
    return { mode: "full", reason: "table_hash_changed" };
  }

  return { mode: "skip", reason: "table_hash_unchanged" };
}

export function decideTableMode(params: {
  previousCatalog: ControlCatalogRecord | null;
  schemaHash: string;
  tableHash: string;
  hasPrimaryKey: boolean;
}): { mode: TableMode; reason: string } {
  if (!params.hasPrimaryKey) {
    return { mode: "skip", reason: "skipped_no_pk" };
  }

  return getTableModeReason({
    previousCatalog: params.previousCatalog,
    schemaHash: params.schemaHash,
    tableHash: params.tableHash,
  });
}

export function buildTablePlans(params: {
  tableInfos: typeof DbSyncStateAnnotation.State.tableInfos;
  tableSnapshots: typeof DbSyncStateAnnotation.State.tableSnapshots;
  catalogByTable: CatalogByTable;
}): TablePlan[] {
  const snapshotByTable = new Map(
    params.tableSnapshots.map((snapshot) => [tableKey(snapshot.schema, snapshot.table), snapshot]),
  );

  return params.tableInfos.map((tableInfo) => {
    const key = tableKey(tableInfo.schema, tableInfo.table);
    const snapshot = snapshotByTable.get(key);

    if (!snapshot) {
      return {
        schema: tableInfo.schema,
        table: tableInfo.table,
        mode: "skip",
        reason: "snapshot_missing",
      };
    }

    const decision = decideTableMode({
      previousCatalog: params.catalogByTable[key] ?? null,
      schemaHash: snapshot.schemaHash,
      tableHash: snapshot.tableHash,
      hasPrimaryKey: tableInfo.pkColumns.length > 0,
    });

    return {
      schema: tableInfo.schema,
      table: tableInfo.table,
      mode: decision.mode,
      reason: decision.reason,
    };
  });
}

export function computeRunStatus(params: {
  fatalError: string | null;
  tableResults: TableRunResult[];
}): RunStatus {
  if (params.fatalError) {
    return "failed";
  }

  if (params.tableResults.some((result) => result.status === "failed")) {
    return "partial_success";
  }

  return "success";
}

export function summarizeRun(params: {
  tablePlans: TablePlan[];
  tableResults: TableRunResult[];
}): typeof DbSyncStateAnnotation.State.summary {
  const summary = defaultRunSummary();
  summary.tablesTotal = params.tablePlans.length;

  for (const result of params.tableResults) {
    if (result.status === "reindexed") {
      summary.tablesReindexed += 1;
      summary.rowsUpserted += result.rowsUpserted;
    } else if (result.status === "skipped") {
      summary.tablesSkipped += 1;
    } else if (result.status === "failed") {
      const key = tableKey(result.schema, result.table);
      summary.errors.push({
        tableKey: key,
        message: result.error || "unknown_table_error",
      });
    }
  }

  return summary;
}

export async function executePlansWithHandler(params: {
  tablePlans: TablePlan[];
  onFullTable: (tablePlan: TablePlan) => Promise<number>;
}): Promise<TableRunResult[]> {
  const tableResults: TableRunResult[] = [];

  for (const tablePlan of params.tablePlans) {
    if (tablePlan.mode === "skip") {
      tableResults.push({
        schema: tablePlan.schema,
        table: tablePlan.table,
        mode: tablePlan.mode,
        status: "skipped",
        rowsUpserted: 0,
        error: null,
      });
      continue;
    }

    try {
      const rowsUpserted = await params.onFullTable(tablePlan);
      tableResults.push({
        schema: tablePlan.schema,
        table: tablePlan.table,
        mode: tablePlan.mode,
        status: "reindexed",
        rowsUpserted,
        error: null,
      });
    } catch (error) {
      tableResults.push({
        schema: tablePlan.schema,
        table: tablePlan.table,
        mode: tablePlan.mode,
        status: "failed",
        rowsUpserted: 0,
        error: errorMessage(error),
      });
    }
  }

  return tableResults;
}

async function startRun(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  const runId = state.runId ?? uuidv4();
  const startedAt = new Date().toISOString();

  try {
    const configuration = ensureDbSyncConfiguration(config);
    const client = createElasticClient(configuration.retrieverProvider as ElasticRetrieverProvider);

    const runRecord: RunRecord = {
      run_id: runId,
      started_at: startedAt,
      finished_at: null,
      status: "running",
      tables_total: 0,
      tables_reindexed: 0,
      tables_skipped: 0,
      rows_upserted: 0,
      errors: [],
    };

    await putRunRecord({
      client,
      indexName: configuration.controlRunsIndex,
      runId,
      record: runRecord,
    });

    return {
      runId,
      startedAt,
      fatalError: null,
      status: "running",
      summary: defaultRunSummary(),
      tableResults: [],
    };
  } catch (error) {
    return {
      runId,
      startedAt,
      fatalError: errorMessage(error),
      status: "failed",
      summary: defaultRunSummary(),
      tableResults: [],
    };
  }
}

async function discoverSourceTables(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  if (state.fatalError) {
    return {};
  }

  let pool;
  try {
    const configuration = ensureDbSyncConfiguration(config);
    pool = createPostgresPool(configuration.sourceDbUrl);

    const tableInfos = await discoverTables({
      pool,
      sourceSchema: configuration.sourceSchema,
      tableAllowlist: configuration.tableAllowlist,
      updatedAtCandidates: configuration.updatedAtCandidates,
    });

    return { tableInfos };
  } catch (error) {
    return {
      fatalError: errorMessage(error),
      status: "failed",
    };
  } finally {
    if (pool) {
      await closePostgresPool(pool);
    }
  }
}

async function snapshotSourceTables(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  if (state.fatalError) {
    return {};
  }

  let pool;
  try {
    const configuration = ensureDbSyncConfiguration(config);
    pool = createPostgresPool(configuration.sourceDbUrl);

    const tableSnapshots: typeof DbSyncStateAnnotation.State.tableSnapshots = [];

    for (const tableInfo of state.tableInfos) {
      const snapshot = await fetchTableSnapshot({
        pool,
        tableInfo,
      });

      const schemaHash = computeSchemaHash(tableInfo.columns);
      const tableHash = computeTableHash({
        schemaHash,
        rowCount: snapshot.rowCount,
        maxUpdatedAt: snapshot.maxUpdatedAt,
        maxPkLexicographic: snapshot.maxPkLexicographic,
      });

      tableSnapshots.push({
        schema: tableInfo.schema,
        table: tableInfo.table,
        rowCount: snapshot.rowCount,
        maxUpdatedAt: snapshot.maxUpdatedAt,
        maxPkLexicographic: snapshot.maxPkLexicographic,
        schemaHash,
        tableHash,
      });
    }

    return { tableSnapshots };
  } catch (error) {
    return {
      fatalError: errorMessage(error),
      status: "failed",
    };
  } finally {
    if (pool) {
      await closePostgresPool(pool);
    }
  }
}

async function planTableExecution(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  if (state.fatalError) {
    return {};
  }

  try {
    const configuration = ensureDbSyncConfiguration(config);
    const client = createElasticClient(configuration.retrieverProvider as ElasticRetrieverProvider);
    const catalogByTable: CatalogByTable = {};

    for (const tableInfo of state.tableInfos) {
      const key = tableKey(tableInfo.schema, tableInfo.table);
      catalogByTable[key] = await getCatalogRecord({
        client,
        indexName: configuration.controlCatalogIndex,
        schema: tableInfo.schema,
        table: tableInfo.table,
      });
    }

    const tablePlans = buildTablePlans({
      tableInfos: state.tableInfos,
      tableSnapshots: state.tableSnapshots,
      catalogByTable,
    });

    return {
      catalogByTable,
      tablePlans,
    };
  } catch (error) {
    return {
      fatalError: errorMessage(error),
      status: "failed",
    };
  }
}

async function executeTablePlans(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  if (state.fatalError) {
    return {
      tableResults: [],
      summary: summarizeRun({
        tablePlans: state.tablePlans,
        tableResults: [],
      }),
    };
  }

  const configuration = ensureDbSyncConfiguration(config);

  let pool;
  let client;
  let embeddingModel;

  try {
    pool = createPostgresPool(configuration.sourceDbUrl);
    client = createElasticClient(configuration.retrieverProvider as ElasticRetrieverProvider);
    embeddingModel = makeTextEmbeddings(configuration.embeddingModel);
  } catch (error) {
    return {
      fatalError: errorMessage(error),
      status: "failed",
    };
  }

  const tableInfosByKey = new Map(
    state.tableInfos.map((tableInfo) => [tableKey(tableInfo.schema, tableInfo.table), tableInfo]),
  );

  try {
    const tableResults = await executePlansWithHandler({
      tablePlans: state.tablePlans,
      onFullTable: async (tablePlan) => {
        const key = tableKey(tablePlan.schema, tablePlan.table);
        const tableInfo = tableInfosByKey.get(key);

        if (!tableInfo) {
          throw new Error(`Missing tableInfo for ${key}`);
        }

        const indexName = buildTableIndexName({
          targetIndexPrefix: configuration.targetIndexPrefix,
          schema: tableInfo.schema,
          table: tableInfo.table,
        });

        const vectorStore = createTableVectorStore({
          client,
          embeddingModel,
          indexName,
        });

        await vectorStore.deleteIfExists();

        let rowsUpserted = 0;
        let offset = 0;

        while (true) {
          const rows = await fetchTableRows({
            pool,
            tableInfo,
            limit: configuration.batchSize,
            offset,
          });

          if (rows.length === 0) {
            break;
          }

          const docs = rows.map((row) =>
            transformRowToDocument({
              row,
              tableInfo,
              runId: state.runId as string,
              textColumnsMode: configuration.textColumnsMode,
              excludedColumns: configuration.excludedColumns,
            }),
          );

          await vectorStore.addDocuments(
            docs.map((item) => item.document),
            { ids: docs.map((item) => item.docId) },
          );

          rowsUpserted += docs.length;
          offset += rows.length;
        }

        return rowsUpserted;
      },
    });

    const summary = summarizeRun({
      tablePlans: state.tablePlans,
      tableResults,
    });

    return {
      tableResults,
      summary,
    };
  } finally {
    if (pool) {
      await closePostgresPool(pool);
    }
  }
}

function buildCatalogRecord(params: {
  snapshot: typeof DbSyncStateAnnotation.State.tableSnapshots[number];
  result: TableRunResult | undefined;
  existing: ControlCatalogRecord | null;
  runId: string;
  finishedAt: string;
  fatalError: string | null;
}): ControlCatalogRecord {
  const { snapshot, result, existing, runId, finishedAt, fatalError } = params;

  const failedResult = result?.status === "failed";
  const tableSucceeded = !failedResult && !fatalError && Boolean(result);

  return {
    schema: snapshot.schema,
    table: snapshot.table,
    schema_hash: snapshot.schemaHash,
    table_hash: snapshot.tableHash,
    row_count: snapshot.rowCount,
    max_updated_at: snapshot.maxUpdatedAt,
    last_success_run_id: tableSucceeded
      ? runId
      : existing?.last_success_run_id || null,
    last_success_at: tableSucceeded ? finishedAt : existing?.last_success_at || null,
    last_mode: result?.mode ?? existing?.last_mode ?? null,
    last_error: failedResult ? result.error : fatalError,
  };
}

async function finalizeRun(
  state: typeof DbSyncStateAnnotation.State,
  config: RunnableConfig,
): Promise<typeof DbSyncStateAnnotation.Update> {
  const finishedAt = new Date().toISOString();

  try {
    const configuration = ensureDbSyncConfiguration(config);
    const client = createElasticClient(configuration.retrieverProvider as ElasticRetrieverProvider);

    const resultsByTable = new Map(
      state.tableResults.map((result) => [tableKey(result.schema, result.table), result]),
    );

    for (const snapshot of state.tableSnapshots) {
      const key = tableKey(snapshot.schema, snapshot.table);
      const result = resultsByTable.get(key);
      const existing = state.catalogByTable[key] ?? null;
      const record = buildCatalogRecord({
        snapshot,
        result,
        existing,
        runId: state.runId as string,
        finishedAt,
        fatalError: state.fatalError,
      });

      await putCatalogRecord({
        client,
        indexName: configuration.controlCatalogIndex,
        record,
      });
    }

    const summary = summarizeRun({
      tablePlans: state.tablePlans,
      tableResults: state.tableResults,
    });

    const runErrors: RunError[] = [...summary.errors];
    if (state.fatalError) {
      runErrors.push({ tableKey: "bootstrap", message: state.fatalError });
    }

    const status = computeRunStatus({
      fatalError: state.fatalError,
      tableResults: state.tableResults,
    });

    const runRecord: RunRecord = {
      run_id: state.runId as string,
      started_at: (state.startedAt as string) || finishedAt,
      finished_at: finishedAt,
      status,
      tables_total: summary.tablesTotal,
      tables_reindexed: summary.tablesReindexed,
      tables_skipped: summary.tablesSkipped,
      rows_upserted: summary.rowsUpserted,
      errors: runErrors.map((error) => ({
        table: error.tableKey,
        message: error.message,
      })),
    };

    await putRunRecord({
      client,
      indexName: configuration.controlRunsIndex,
      runId: state.runId as string,
      record: runRecord,
    });

    return {
      status,
      finishedAt,
      summary: {
        ...summary,
        errors: runErrors,
      },
    };
  } catch (error) {
    return {
      status: "failed",
      finishedAt,
      fatalError: state.fatalError ?? errorMessage(error),
    };
  }
}

const builder = new StateGraph(
  DbSyncStateAnnotation,
  DbSyncConfigurationAnnotation,
)
  .addNode("startRun", startRun)
  .addNode("discoverTables", discoverSourceTables)
  .addNode("snapshotTables", snapshotSourceTables)
  .addNode("planTables", planTableExecution)
  .addNode("executeTables", executeTablePlans)
  .addNode("finalizeRun", finalizeRun)
  .addEdge(START, "startRun")
  .addEdge("startRun", "discoverTables")
  .addEdge("discoverTables", "snapshotTables")
  .addEdge("snapshotTables", "planTables")
  .addEdge("planTables", "executeTables")
  .addEdge("executeTables", "finalizeRun")
  .addEdge("finalizeRun", END);

export const graph = builder.compile().withConfig({ runName: "DbSyncGraph" });
