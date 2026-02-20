import { writeFile } from "node:fs/promises";

// Re-use pricing from estimate module would create a circular dependency,
// so we accept it as a parameter instead.

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type TableProgress = {
  schema: string;
  table: string;
  rowCount: number;
  tokenCount: number;
  status: "completed" | "error";
  errorMessage?: string;
  completedAt: string;
};

type ProgressData = {
  startTime: string;
  lastUpdate: string;
  status: "in_progress" | "completed";
  progress: {
    completed: number;
    total: number;
    errors: number;
    percentComplete: number;
  };
  tables: TableProgress[];
  totals: {
    totalTokens: number;
    totalRows: number;
    costByModel: Record<string, number>;
  };
  elapsedSeconds: number;
};

export type ProgressFileConfig = {
  filePath: string;
  totalTables: number;
  /** Price per 1 million tokens for each model. */
  modelPricing: Record<string, number>;
};

// ---------------------------------------------------------------------------
// ProgressFileWriter
// ---------------------------------------------------------------------------

export class ProgressFileWriter {
  private readonly filePath: string;
  private totalTables: number;
  private readonly modelPricing: Record<string, number>;
  private readonly startTime: Date;
  private tables: TableProgress[] = [];
  private errorCount = 0;
  private writeQueue: Promise<void> = Promise.resolve();

  constructor(config: ProgressFileConfig) {
    this.filePath = config.filePath;
    this.totalTables = config.totalTables;
    this.modelPricing = config.modelPricing;
    this.startTime = new Date();
  }

  setTotalTables(total: number): void {
    this.totalTables = total;
  }

  /** Create / overwrite the JSON file with the initial empty structure. */
  async initialize(): Promise<void> {
    this.tables = [];
    this.errorCount = 0;
    await this.writeToFile();
  }

  /** Record a successfully completed table and flush to disk. */
  addCompletedTable(result: {
    schema: string;
    table: string;
    rowCount: number;
    tokenCount: number;
  }): void {
    this.tables.push({
      schema: result.schema,
      table: result.table,
      rowCount: result.rowCount,
      tokenCount: result.tokenCount,
      status: "completed",
      completedAt: new Date().toISOString(),
    });
    this.enqueueWrite();
  }

  /** Record a table that failed and flush to disk. */
  addErrorTable(schema: string, table: string, error: string): void {
    this.errorCount++;
    this.tables.push({
      schema,
      table,
      rowCount: 0,
      tokenCount: 0,
      status: "error",
      errorMessage: error,
      completedAt: new Date().toISOString(),
    });
    this.enqueueWrite();
  }

  /** Mark the process as completed and flush a final snapshot. */
  async finalize(): Promise<void> {
    await this.writeToFile("completed");
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private enqueueWrite(): void {
    this.writeQueue = this.writeQueue
      .then(() => this.writeToFile())
      .catch((err) => {
        console.error("[progress-file] Failed to write:", (err as Error).message);
      });
  }

  private async writeToFile(status: "in_progress" | "completed" = "in_progress"): Promise<void> {
    const now = new Date();
    const elapsedSeconds = Math.floor((now.getTime() - this.startTime.getTime()) / 1000);

    const totalTokens = this.tables.reduce((sum, t) => sum + t.tokenCount, 0);
    const totalRows = this.tables.reduce((sum, t) => sum + t.rowCount, 0);

    const costByModel: Record<string, number> = {};
    for (const [model, pricePerMillion] of Object.entries(this.modelPricing)) {
      costByModel[model] = (totalTokens / 1_000_000) * pricePerMillion;
    }

    const completedCount = this.tables.length;

    const data: ProgressData = {
      startTime: this.startTime.toISOString(),
      lastUpdate: now.toISOString(),
      status,
      progress: {
        completed: completedCount,
        total: this.totalTables,
        errors: this.errorCount,
        percentComplete: this.totalTables > 0 ? Math.round((completedCount / this.totalTables) * 100) : 0,
      },
      tables: this.tables,
      totals: {
        totalTokens,
        totalRows,
        costByModel,
      },
      elapsedSeconds,
    };

    await writeFile(this.filePath, JSON.stringify(data, null, 2), "utf-8");
  }
}
