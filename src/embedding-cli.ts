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

checkNodeVersion(REQUIRED_NODE_VERSION);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  intro(
    pc.blue(pc.bold("Embedding CLI")) +
      pc.dim("  —  Estimativa de custo de embedding")
  );

  const answers = await gatherEmbeddingResponses();

  await runEstimate(answers);

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

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((error: unknown) => {
  const msg = error instanceof Error ? error.message : String(error);
  log(pc.red("\nErro fatal: " + msg));
  process.exit(1);
});
