import { confirm, select, text } from "@clack/prompts";
import { checkCancel } from "./helpers.js";
import type {
  EmbeddingUserAnswers,
  EmbeddingOperation,
  CommonEmbeddingParams,
} from "./embedding-types.js";
import type { TextColumnsMode } from "./cost_estimator/db/types.js";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function gatherEmbeddingResponses(): Promise<EmbeddingUserAnswers> {
  // ── Step 1: Operation ────────────────────────────────────────────────────
  const op: EmbeddingOperation = "estimate";

  // ── Step 2: Database connection ──────────────────────────────────────────
  const sourceDbUrl = await resolveDbUrl();

  // ── Step 3: Schema ───────────────────────────────────────────────────────
  const envSchema =
    process.env.DB_SCHEMA?.trim() || process.env.SOURCE_SCHEMA?.trim() || "";
  let sourceSchema: string;

  if (envSchema) {
    sourceSchema = envSchema;
  } else {
    const schemaInput = await text({
      message: "Schema de origem?",
      initialValue: "public",
    });
    checkCancel(schemaInput);
    sourceSchema = ((schemaInput as string) || "public").trim() || "public";
  }

  // ── Step 4: Table allowlist ──────────────────────────────────────────────
  const envAllowlist = parseCommaSeparated(process.env.SOURCE_TABLE_ALLOWLIST);
  let tableAllowlist: string[];

  if (envAllowlist.length > 0) {
    tableAllowlist = envAllowlist;
  } else {
    const allowlistInput = await text({
      message: "Tabelas a incluir (separadas por vírgula)?",
      placeholder: "users, orders, products  ou  *  para todas",
      validate: (v) =>
        !v?.trim() ? "Informe ao menos uma tabela" : undefined,
    });
    checkCancel(allowlistInput);
    tableAllowlist = parseCommaSeparated(allowlistInput as string);
  }

  // ── Step 5: Advanced options ─────────────────────────────────────────────
  const tableBlocklist = parseCommaSeparated(process.env.SOURCE_TABLE_BLOCKLIST);
  const defaultTextMode: TextColumnsMode =
    process.env.TEXT_COLUMNS_MODE === "all" ? "all" : "auto";
  const defaultExcludedColumns = parseCommaSeparated(process.env.EXCLUDED_COLUMNS);
  const defaultBatchSize = parseInt(process.env.SOURCE_BATCH_SIZE || "1000", 10);
  const updatedAtCandidates = parseCommaSeparated(
    process.env.SOURCE_UPDATED_AT_CANDIDATES ||
      "updated_at,modified_at,updatedon"
  );

  const customizeAdvanced = await confirm({
    message: "Personalizar opções avançadas (modo de colunas, exclusões, batch size)?",
    initialValue: false,
  });
  checkCancel(customizeAdvanced);

  let textColumnsMode = defaultTextMode;
  let excludedColumns = defaultExcludedColumns;
  let batchSize = defaultBatchSize;

  if (customizeAdvanced) {
    const modeInput = await select({
      message: "Modo de colunas de texto?",
      options: [
        {
          label: "Auto (somente varchar/text/json/uuid)",
          value: "auto",
          hint: "Recomendado",
        },
        {
          label: "Todas as colunas",
          value: "all",
        },
      ],
      initialValue: defaultTextMode,
    });
    checkCancel(modeInput);
    textColumnsMode = modeInput as TextColumnsMode;

    const excludedInput = await text({
      message: "Colunas a excluir (separadas por vírgula, deixe em branco para nenhuma)?",
      initialValue: defaultExcludedColumns.join(", "),
    });
    checkCancel(excludedInput);
    excludedColumns = parseCommaSeparated(excludedInput as string);

    const batchSizeInput = await text({
      message: "Batch size (linhas por busca no DB)?",
      initialValue: String(defaultBatchSize),
      validate: (v) =>
        isNaN(parseInt(v || "")) ? "Deve ser um número" : undefined,
    });
    checkCancel(batchSizeInput);
    batchSize = parseInt(batchSizeInput as string, 10);
  }

  const common: CommonEmbeddingParams = {
    sourceDbUrl,
    sourceSchema,
    tableAllowlist,
    tableBlocklist,
    textColumnsMode,
    excludedColumns,
    batchSize,
    updatedAtCandidates,
  };

  return { operation: op, common };
}

// ---------------------------------------------------------------------------
// DB URL resolution
// ---------------------------------------------------------------------------

async function resolveDbUrl(): Promise<string> {
  // Try explicit URL first
  const envUrl =
    process.env.SOURCE_DB_URL?.trim() || buildDbUrlFromEnv() || "";
  if (envUrl) {
    return envUrl;
  }

  // Must ask
  const inputMode = await select({
    message: "Como deseja informar as credenciais do banco?",
    options: [
      {
        label: "URL de conexão completa",
        value: "url",
        hint: "postgres://user:pass@host/db",
      },
      {
        label: "Campos individuais",
        value: "fields",
      },
    ],
    initialValue: "url" as "url" | "fields",
  });
  checkCancel(inputMode);

  if (inputMode === "url") {
    const rawUrl = await text({
      message: "URL de conexão PostgreSQL?",
      placeholder: "postgres://user:password@host:5432/database",
      validate: (v) => (!v?.trim() ? "A URL é obrigatória" : undefined),
    });
    checkCancel(rawUrl);
    return (rawUrl as string).trim();
  }

  // Fields
  const dbHost = await text({
    message: "Host do banco?",
    placeholder: "localhost",
    validate: required,
  });
  checkCancel(dbHost);

  const dbPort = await text({
    message: "Porta?",
    initialValue: "5432",
  });
  checkCancel(dbPort);

  const dbName = await text({
    message: "Nome do banco?",
    validate: required,
  });
  checkCancel(dbName);

  const dbUser = await text({
    message: "Usuário?",
    validate: required,
  });
  checkCancel(dbUser);

  const dbPass = await text({
    message: "Senha?",
    validate: required,
  });
  checkCancel(dbPass);

  return buildDbUrlFromParts(
    (dbHost as string).trim(),
    (dbPort as string).trim() || "5432",
    (dbName as string).trim(),
    (dbUser as string).trim(),
    (dbPass as string).trim()
  );
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function required(value: string | undefined): string | undefined {
  return !value?.trim() ? "Este campo é obrigatório" : undefined;
}

function parseCommaSeparated(input: string | undefined): string[] {
  if (!input) return [];
  return input
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

function buildDbUrlFromEnv(): string | undefined {
  const host = stripQuotes(process.env.DB_HOST);
  const port = stripQuotes(process.env.DB_PORT) || "5432";
  const dbName = stripQuotes(process.env.DB_NAME);
  const username = stripQuotes(process.env.DB_USERNAME);
  const password = stripQuotes(process.env.DB_PASSWORD);
  if (!host || !dbName || !username || !password) return undefined;
  return buildDbUrlFromParts(host, port, dbName, username, password);
}

function buildDbUrlFromParts(
  host: string,
  port: string,
  dbName: string,
  username: string,
  password: string
): string {
  return `postgres://${encodeURIComponent(username)}:${encodeURIComponent(
    password
  )}@${host}:${port}/${encodeURIComponent(dbName)}`;
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
