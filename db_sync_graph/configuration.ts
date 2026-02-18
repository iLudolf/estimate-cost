import { RunnableConfig } from "@langchain/core/runnables";
import { Annotation } from "@langchain/langgraph";
import {
  BaseConfigurationAnnotation,
  ensureBaseConfiguration,
} from "../shared/configuration.js";
import { TextColumnsMode } from "./state.js";

const DEFAULT_SOURCE_SCHEMA = "public";
const DEFAULT_UPDATED_AT_CANDIDATES = [
  "updated_at",
  "modified_at",
  "updatedon",
];
const DEFAULT_BATCH_SIZE = 1000;
const DEFAULT_TARGET_INDEX_PREFIX = "db_table_";
const DEFAULT_CONTROL_CATALOG_INDEX = "db_sync_catalog";
const DEFAULT_CONTROL_RUNS_INDEX = "db_sync_runs";

export const DbSyncConfigurationAnnotation = Annotation.Root({
  ...BaseConfigurationAnnotation.spec,
  sourceDbUrl: Annotation<string>,
  sourceSchema: Annotation<string>,
  tableAllowlist: Annotation<string[]>,
  updatedAtCandidates: Annotation<string[]>,
  batchSize: Annotation<number>,
  targetIndexPrefix: Annotation<string>,
  controlCatalogIndex: Annotation<string>,
  controlRunsIndex: Annotation<string>,
  textColumnsMode: Annotation<TextColumnsMode>,
  excludedColumns: Annotation<string[]>,
});

function parseStringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => `${item}`.trim())
      .filter((item) => item.length > 0);
  }

  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }

  return [];
}

function parsePositiveInt(value: unknown, fallback: number): number {
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    return Math.floor(numeric);
  }
  return fallback;
}

function parseTextColumnsMode(value: unknown): TextColumnsMode {
  if (value === "all") {
    return "all";
  }
  return "auto";
}

function firstDefinedString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      const normalized = value.trim();
      if (
        (normalized.startsWith("\"") && normalized.endsWith("\"")) ||
        (normalized.startsWith("'") && normalized.endsWith("'"))
      ) {
        return normalized.slice(1, -1).trim();
      }
      return normalized;
    }
  }
  return undefined;
}

function buildDbUrlFromLegacyEnv(
  rawConfig: Record<string, unknown>,
): string | undefined {
  const host = firstDefinedString(rawConfig.dbHost, process.env.DB_HOST);
  const port = firstDefinedString(rawConfig.dbPort, process.env.DB_PORT) || "5432";
  const dbName = firstDefinedString(rawConfig.dbName, process.env.DB_NAME);
  const username = firstDefinedString(
    rawConfig.dbUsername,
    process.env.DB_USERNAME,
  );
  const password = firstDefinedString(
    rawConfig.dbPassword,
    process.env.DB_PASSWORD,
  );

  if (!host || !dbName || !username || !password) {
    return undefined;
  }

  return `postgres://${encodeURIComponent(username)}:${encodeURIComponent(
    password,
  )}@${host}:${port}/${encodeURIComponent(dbName)}`;
}

/**
 * Build DB sync configuration from RunnableConfig with env fallback.
 */
export function ensureDbSyncConfiguration(
  config: RunnableConfig,
): typeof DbSyncConfigurationAnnotation.State {
  const rawConfig = (config?.configurable || {}) as Record<string, unknown>;
  const baseConfig = ensureBaseConfiguration(config);

  if (
    baseConfig.retrieverProvider !== "elastic" &&
    baseConfig.retrieverProvider !== "elastic-local"
  ) {
    throw new Error(
      `db_sync_graph supports only elastic retrievers. Received: ${baseConfig.retrieverProvider}`,
    );
  }

  const explicitSourceDbUrl = firstDefinedString(rawConfig.sourceDbUrl);
  const legacyDbUrl = buildDbUrlFromLegacyEnv(rawConfig);
  const envSourceDbUrl = firstDefinedString(process.env.SOURCE_DB_URL);

  const sourceDbUrl = explicitSourceDbUrl || legacyDbUrl || envSourceDbUrl;

  if (!sourceDbUrl) {
    throw new Error(
      "Missing sourceDbUrl. Set configurable.sourceDbUrl or SOURCE_DB_URL, or provide DB_HOST/DB_PORT/DB_NAME/DB_USERNAME/DB_PASSWORD.",
    );
  }

  const sourceSchema =
    firstDefinedString(
      rawConfig.sourceSchema,
      process.env.SOURCE_SCHEMA,
      rawConfig.dbSchema,
      process.env.DB_SCHEMA,
    ) ||
    DEFAULT_SOURCE_SCHEMA;

  const tableAllowlist = parseStringList(
    rawConfig.tableAllowlist ?? process.env.SOURCE_TABLE_ALLOWLIST,
  );

  if (tableAllowlist.length === 0) {
    throw new Error(
      "tableAllowlist is required. Set configurable.tableAllowlist or SOURCE_TABLE_ALLOWLIST.",
    );
  }

  const updatedAtCandidates =
    parseStringList(
      rawConfig.updatedAtCandidates ?? process.env.SOURCE_UPDATED_AT_CANDIDATES,
    ) || [];

  const batchSize = parsePositiveInt(
    rawConfig.batchSize ?? process.env.SOURCE_BATCH_SIZE,
    DEFAULT_BATCH_SIZE,
  );

  const targetIndexPrefix =
    firstDefinedString(rawConfig.targetIndexPrefix, process.env.TARGET_INDEX_PREFIX) ||
    DEFAULT_TARGET_INDEX_PREFIX;

  const controlCatalogIndex =
    firstDefinedString(
      rawConfig.controlCatalogIndex,
      process.env.CONTROL_CATALOG_INDEX,
    ) || DEFAULT_CONTROL_CATALOG_INDEX;

  const controlRunsIndex =
    firstDefinedString(rawConfig.controlRunsIndex, process.env.CONTROL_RUNS_INDEX) ||
    DEFAULT_CONTROL_RUNS_INDEX;

  const textColumnsMode = parseTextColumnsMode(
    rawConfig.textColumnsMode ?? process.env.TEXT_COLUMNS_MODE,
  );

  const excludedColumns = parseStringList(
    rawConfig.excludedColumns ?? process.env.EXCLUDED_COLUMNS,
  );

  return {
    ...baseConfig,
    sourceDbUrl,
    sourceSchema,
    tableAllowlist,
    updatedAtCandidates:
      updatedAtCandidates.length > 0
        ? updatedAtCandidates
        : DEFAULT_UPDATED_AT_CANDIDATES,
    batchSize,
    targetIndexPrefix,
    controlCatalogIndex,
    controlRunsIndex,
    textColumnsMode,
    excludedColumns,
  };
}
