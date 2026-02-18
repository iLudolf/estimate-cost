import "dotenv/config";
import { confirm, intro, note, outro, spinner } from "@clack/prompts";
import pc from "picocolors";

import { checkCancel, checkNodeVersion } from "./helpers.js";
import { log } from "./logger.js";
import { REQUIRED_NODE_VERSION } from "./constants.js";
import { gatherEmbeddingResponses } from "./gather-embedding-responses.js";
import type { EmbeddingUserAnswers } from "./embedding-types.js";
import { estimateCost, EMBEDDING_MODEL_PRICING } from "../cost_estimator/estimate.js";
import type { CostEstimationResult } from "../cost_estimator/estimate.js";
import { graph as dbSyncGraph } from "../db_sync_graph/graph.js";
import type { RunSummary, RunStatus } from "../db_sync_graph/state.js";

checkNodeVersion(REQUIRED_NODE_VERSION);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  intro(
    pc.blue(pc.bold("Embedding CLI")) +
      pc.dim("  —  Estime custos e sincronize tabelas para o Elasticsearch")
  );

  const answers = await gatherEmbeddingResponses();

  if (answers.operation === "estimate" || answers.operation === "both") {
    await runEstimate(answers);
  }

  if (answers.operation === "sync" || answers.operation === "both") {
    if (answers.operation === "both") {
      const proceed = await confirm({
        message: "Prosseguir com a sincronização para o Elasticsearch?",
        initialValue: true,
      });
      checkCancel(proceed);
      if (!proceed) {
        outro(pc.yellow("Sincronização cancelada. Resultados da estimativa exibidos acima."));
        return;
      }
    }
    await runSync(answers);
  }

  outro(pc.green("Concluído!"));
}

// ---------------------------------------------------------------------------
// Estimate runner
// ---------------------------------------------------------------------------

async function runEstimate(answers: EmbeddingUserAnswers): Promise<void> {
  const { common } = answers;

  log("", { newline: "before" });
  log(pc.bold(pc.cyan("Estimativa de custo")));
  log(pc.dim("Conectando ao banco e escaneando tabelas..."), { newline: "after" });

  let result: CostEstimationResult;

  result = await estimateCost({
    sourceDbUrl: common.sourceDbUrl,
    sourceSchema: common.sourceSchema,
    tableAllowlist: common.tableAllowlist,
    tableBlocklist: common.tableBlocklist,
    updatedAtCandidates: common.updatedAtCandidates,
    textColumnsMode: common.textColumnsMode,
    excludedColumns: common.excludedColumns,
    batchSize: common.batchSize,
  });

  displayCostResults(result);
}

// ---------------------------------------------------------------------------
// Sync runner
// ---------------------------------------------------------------------------

async function runSync(answers: EmbeddingUserAnswers): Promise<void> {
  const { common, sync } = answers;
  if (!sync) throw new Error("Parâmetros de sync ausentes");

  log("", { newline: "before" });
  log(pc.bold(pc.cyan("Sincronização para o Elasticsearch")));

  // Inject credentials into process.env — elastic_control.ts reads them directly
  process.env.ELASTICSEARCH_URL = sync.elasticsearchUrl;
  if (sync.elasticAuthMode === "cloud" && sync.elasticsearchApiKey) {
    process.env.ELASTICSEARCH_API_KEY = sync.elasticsearchApiKey;
  }
  if (sync.elasticAuthMode === "local") {
    if (sync.elasticsearchUser)
      process.env.ELASTICSEARCH_USER = sync.elasticsearchUser;
    if (sync.elasticsearchPassword)
      process.env.ELASTICSEARCH_PASSWORD = sync.elasticsearchPassword;
  }
  if (sync.openaiApiKey) {
    process.env.OPENAI_API_KEY = sync.openaiApiKey;
  }

  const retrieverProvider =
    sync.elasticAuthMode === "local" ? "elastic-local" : "elastic";

  const s = spinner();
  s.start("Iniciando workflow de sincronização...");

  try {
    const finalState = await dbSyncGraph.invoke(
      {},
      {
        configurable: {
          retrieverProvider,
          embeddingModel: sync.embeddingModel,
          sourceDbUrl: common.sourceDbUrl,
          sourceSchema: common.sourceSchema,
          tableAllowlist: common.tableAllowlist,
          updatedAtCandidates: common.updatedAtCandidates,
          batchSize: common.batchSize,
          targetIndexPrefix: sync.targetIndexPrefix,
          textColumnsMode: common.textColumnsMode,
          excludedColumns: common.excludedColumns,
        } as Record<string, unknown>,
      }
    );

    s.stop(pc.green("Workflow de sincronização concluído"));
    displaySyncResults(
      finalState.summary as RunSummary,
      finalState.status as RunStatus,
      finalState.fatalError as string | null
    );
  } catch (error: unknown) {
    s.stop(pc.red("Sync falhou"));
    const msg = error instanceof Error ? error.message : String(error);
    log(pc.red(msg));
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Results display
// ---------------------------------------------------------------------------

function displayCostResults(result: CostEstimationResult): void {
  const sep = pc.dim("─".repeat(60));
  const totalRows = result.tables.reduce((sum, t) => sum + t.rowCount, 0);

  const lines: string[] = [
    "",
    sep,
    pc.bold("  RELATÓRIO DE CUSTO DE EMBEDDING"),
    sep,
    "",
    pc.bold("  Tabelas:"),
    pc.dim("  " + "─".repeat(56)),
    pc.dim(
      "  " +
        "Tabela".padEnd(32) +
        "Linhas".padStart(10) +
        "Tokens".padStart(15)
    ),
    pc.dim("  " + "─".repeat(56)),
  ];

  for (const t of result.tables) {
    const name = `${t.schema}.${t.table}`;
    lines.push(
      "  " +
        pc.cyan(name.padEnd(32)) +
        pc.white(t.rowCount.toLocaleString("pt-BR").padStart(10)) +
        pc.white(t.tokenCount.toLocaleString("pt-BR").padStart(15))
    );
  }

  lines.push(pc.dim("  " + "─".repeat(56)));
  lines.push(
    "  " +
      pc.bold("TOTAL".padEnd(32)) +
      pc.bold(totalRows.toLocaleString("pt-BR").padStart(10)) +
      pc.bold(result.totalTokens.toLocaleString("pt-BR").padStart(15))
  );
  lines.push("");
  lines.push(pc.bold("  Custo estimado por modelo:"));
  lines.push(pc.dim("  " + "─".repeat(56)));

  for (const [model, cost] of Object.entries(result.costByModel)) {
    const pricePerM = EMBEDDING_MODEL_PRICING[model];
    const costStr = `$${cost.toFixed(4)}`;
    lines.push(
      "  " +
        pc.yellow(model.padEnd(34)) +
        pc.dim(`  $${pricePerM}/1M tokens`) +
        "  " +
        pc.green(pc.bold(costStr))
    );
  }

  lines.push(sep);
  lines.push("");

  note(lines.join("\n"), pc.green("Estimativa concluída"));
}

function displaySyncResults(
  summary: RunSummary,
  status: RunStatus,
  fatalError: string | null
): void {
  const sep = pc.dim("─".repeat(60));
  const statusColor =
    status === "success"
      ? pc.green
      : status === "partial_success"
        ? pc.yellow
        : pc.red;

  const lines: string[] = [
    "",
    sep,
    pc.bold("  RESULTADO DA SINCRONIZAÇÃO"),
    sep,
    "",
    `  Status:                ${statusColor(pc.bold(status.toUpperCase()))}`,
    `  Tabelas processadas:   ${pc.white(String(summary.tablesTotal))}`,
    `  Tabelas reindexadas:   ${pc.green(String(summary.tablesReindexed))}`,
    `  Tabelas ignoradas:     ${pc.dim(String(summary.tablesSkipped))}`,
    `  Linhas inseridas:      ${pc.cyan(summary.rowsUpserted.toLocaleString("pt-BR"))}`,
  ];

  if (fatalError) {
    lines.push("");
    lines.push(`  ${pc.red(pc.bold("Erro fatal:"))} ${pc.red(fatalError)}`);
  }

  if (summary.errors.length > 0) {
    lines.push("");
    lines.push(
      `  ${pc.red(pc.bold(`Erros por tabela (${summary.errors.length}):`))} `
    );
    for (const err of summary.errors) {
      lines.push(`    ${pc.red("✗")} ${err.tableKey}: ${pc.dim(err.message)}`);
    }
  }

  lines.push(sep);
  lines.push("");

  const noteTitle =
    status === "success"
      ? pc.green("Sync concluído")
      : status === "partial_success"
        ? pc.yellow("Sync parcialmente concluído")
        : pc.red("Sync falhou");

  note(lines.join("\n"), noteTitle);
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((error: unknown) => {
  const msg = error instanceof Error ? error.message : String(error);
  log(pc.red("\nErro fatal: " + msg));
  process.exit(1);
});
