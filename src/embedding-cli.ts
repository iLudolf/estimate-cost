import "dotenv/config";
import { confirm, intro, note, outro, spinner } from "@clack/prompts";
import pc from "picocolors";

import { checkCancel, checkNodeVersion } from "./helpers.js";
import { log } from "./logger.js";
import { REQUIRED_NODE_VERSION } from "./constants.js";
import { gatherEmbeddingResponses } from "./gather-embedding-responses.js";
import type { EmbeddingUserAnswers } from "./embedding-types.js";
import { estimateCost } from "./cost_estimator/estimate.js";
import type { CostEstimationResult } from "./cost_estimator/estimate.js";
import { ProgressFileWriter } from "./cost_estimator/progress_file.js";
import { startProgressDashboard } from "./cost_estimator/progress_dashboard.js";
import type { ProgressDashboardHandle } from "./cost_estimator/progress_dashboard.js";
import { getPricing, toPricingMap } from "./cost_estimator/pricing.js";
import type { ModelPricingEntry } from "./cost_estimator/pricing.js";

checkNodeVersion(REQUIRED_NODE_VERSION);

const DEFAULT_PROGRESS_FILE = "./cost_estimation_progress.json";
const DEFAULT_DASHBOARD_HOST = "127.0.0.1";
const DEFAULT_DASHBOARD_PORT = 4173;

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
  const progressFilePath = process.env.COST_PROGRESS_FILE || DEFAULT_PROGRESS_FILE;
  const dashboardHost = process.env.COST_DASHBOARD_HOST || DEFAULT_DASHBOARD_HOST;
  const dashboardPort = parsePositiveInt(
    process.env.COST_DASHBOARD_PORT,
    DEFAULT_DASHBOARD_PORT,
  );
  const dashboardUrl = `http://${dashboardHost}:${dashboardPort}`;

  let dashboardHandle: ProgressDashboardHandle | null = null;

  try {
    dashboardHandle = await startProgressDashboard({
      progressFilePath,
      host: dashboardHost,
      port: dashboardPort,
      suppressLogs: true,
    });
  } catch (error: unknown) {
    if (isAddressInUseError(error)) {
      log(
        pc.yellow(
          `[progress-dashboard] Porta ${dashboardPort} já está em uso. Usando dashboard existente em ${dashboardUrl}.`,
        ),
      );
    } else {
      const message = error instanceof Error ? error.message : String(error);
      log(
        pc.yellow(
          `[progress-dashboard] Não foi possível iniciar automaticamente: ${message}`,
        ),
      );
    }
  }

  try {
    log("", { newline: "before" });
    log(pc.bold(pc.cyan("Estimativa de custo")));
    log(`Acesse o dashboard: ${dashboardUrl}`);
    log(pc.dim("Conectando ao banco e escaneando tabelas..."), { newline: "after" });

    // Fetch pricing (uses cache if fresh, otherwise fetches from internet)
    const pricingEntries = await getPricing();
    const progressWriter = new ProgressFileWriter({
      filePath: progressFilePath,
      totalTables: 0,
      modelPricing: toPricingMap(pricingEntries),
    });

    const result = await estimateCost({
      sourceDbUrl: common.sourceDbUrl,
      sourceSchema: common.sourceSchema,
      tableAllowlist: common.tableAllowlist,
      tableBlocklist: common.tableBlocklist,
      updatedAtCandidates: common.updatedAtCandidates,
      textColumnsMode: common.textColumnsMode,
      excludedColumns: common.excludedColumns,
      batchSize: common.batchSize,
      pricingEntries,
      progressWriter,
    });

    displayCostResults(result);
  } finally {
    if (dashboardHandle) {
      await dashboardHandle.close();
    }
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

  // Group entries by provider
  const byProvider = new Map<string, ModelPricingEntry[]>();
  for (const entry of result.pricingEntries) {
    const list = byProvider.get(entry.provider) ?? [];
    list.push(entry);
    byProvider.set(entry.provider, list);
  }

  for (const [provider, entries] of byProvider) {
    lines.push("  " + pc.bold(pc.dim(provider.toUpperCase())));
    for (const entry of entries) {
      const cost = result.costByModel[entry.model] ?? 0;
      const costStr = `$${cost.toFixed(4)}`;
      const priceLabel =
        entry.pricePerMillion === 0
          ? pc.dim("  free/local")
          : pc.dim(`  $${entry.pricePerMillion}/1M tokens`);
      lines.push(
        "  " +
          pc.yellow(("  " + entry.model).padEnd(36)) +
          priceLabel +
          "  " +
          pc.green(pc.bold(costStr))
      );
    }
    lines.push("");
  }

  lines.push(sep);
  lines.push("");

  note(lines.join("\n"), pc.green("Estimativa concluída"));
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function isAddressInUseError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  return "code" in error && (error as NodeJS.ErrnoException).code === "EADDRINUSE";
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((error: unknown) => {
  const msg = error instanceof Error ? error.message : String(error);
  log(pc.red("\nErro fatal: " + msg));
  process.exit(1);
});
