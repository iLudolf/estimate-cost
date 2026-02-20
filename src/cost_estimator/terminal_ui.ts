// ---------------------------------------------------------------------------
// ANSI escape codes
// ---------------------------------------------------------------------------

const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";
const DIM = "\x1b[2m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const RED = "\x1b[31m";
const CYAN = "\x1b[36m";

const CLEAR_LINE = "\x1b[2K";
const CURSOR_UP = (n: number) => `\x1b[${n}A`;
const CURSOR_TO_COL0 = "\x1b[G";
const HIDE_CURSOR = "\x1b[?25l";
const SHOW_CURSOR = "\x1b[?25h";

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const BAR_FILLED = "█";
const BAR_EMPTY = "░";

const TICK_INTERVAL = 80;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type CompletedEntry = {
  name: string;
  rowCount: number;
  tokenCount: number;
  index: number;
  isError: boolean;
  errorMessage?: string;
};

export type TerminalUIConfig = {
  totalTables: number;
};

// ---------------------------------------------------------------------------
// TerminalUI
// ---------------------------------------------------------------------------

export class TerminalUI {
  private totalTables: number;
  private readonly isTTY: boolean;

  private activeTables = new Map<string, string>(); // key → display label
  private chunkProgress = new Map<string, string>(); // "schema.table" → "3/10"
  private completedEntries: CompletedEntry[] = [];
  private completedCount = 0;
  private errorCount = 0;
  private spinnerIndex = 0;
  private startTime = 0;
  private tickTimer: ReturnType<typeof setInterval> | null = null;

  private dynamicLineCount = 0;
  private lastPrintedCompletedIndex = 0;

  constructor(config: TerminalUIConfig) {
    this.totalTables = config.totalTables;
    this.isTTY = process.stdout.isTTY === true;

    // Ensure cursor is always restored
    const cleanup = () => {
      this.stopTick();
      if (this.isTTY) process.stdout.write(SHOW_CURSOR);
    };
    process.on("exit", cleanup);
    process.on("SIGINT", () => {
      cleanup();
      process.exit(130);
    });
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  setTotalTables(total: number): void {
    this.totalTables = total;
  }

  showSpinner(message: string): void {
    if (!this.isTTY) {
      process.stdout.write(message + "\n");
      return;
    }
    process.stdout.write(HIDE_CURSOR);
    this.stopTick();
    this.tickTimer = setInterval(() => {
      const frame = SPINNER_FRAMES[this.spinnerIndex % SPINNER_FRAMES.length];
      this.spinnerIndex++;
      process.stdout.write(
        CURSOR_TO_COL0 + CLEAR_LINE + `  ${CYAN}${frame}${RESET} ${message}`,
      );
    }, TICK_INTERVAL);
  }

  stopSpinner(finalMessage?: string): void {
    this.stopTick();
    if (!this.isTTY) {
      if (finalMessage) process.stdout.write(finalMessage + "\n");
      return;
    }
    process.stdout.write(CURSOR_TO_COL0 + CLEAR_LINE);
    if (finalMessage) {
      process.stdout.write(`  ${GREEN}✓${RESET} ${finalMessage}\n`);
    }
  }

  startProgress(): void {
    this.startTime = Date.now();
    this.spinnerIndex = 0;
    this.dynamicLineCount = 0;
    this.lastPrintedCompletedIndex = 0;

    const separator = "  " + "-".repeat(60);
    const header =
      "  " +
      "Table".padEnd(32) +
      "Rows".padStart(10) +
      "Tokens".padStart(15);

    process.stdout.write(
      `\n  Progressive results:\n${separator}\n${header}\n${separator}\n`,
    );

    if (this.isTTY) {
      process.stdout.write(HIDE_CURSOR);
      this.tickTimer = setInterval(() => {
        this.spinnerIndex++;
        this.render();
      }, TICK_INTERVAL);
      this.render();
    }
  }

  onTableStart(schema: string, table: string, chunkLabel?: string): void {
    const name = `${schema}.${table}`;
    const label = chunkLabel ? `${name} (${chunkLabel})` : name;
    this.activeTables.set(`${name}:${chunkLabel ?? ""}`, label);
  }

  onChunkComplete(
    schema: string,
    table: string,
    completedChunks: number,
    totalChunks: number,
  ): void {
    const name = `${schema}.${table}`;
    this.chunkProgress.set(name, `${completedChunks}/${totalChunks}`);
  }

  onTableComplete(result: {
    schema: string;
    table: string;
    rowCount: number;
    tokenCount: number;
  }): void {
    const name = `${result.schema}.${result.table}`;
    // Remove all active entries for this table (including chunk-specific ones)
    for (const key of this.activeTables.keys()) {
      if (key.startsWith(name)) {
        this.activeTables.delete(key);
      }
    }
    this.chunkProgress.delete(name);
    this.completedCount++;
    this.completedEntries.push({
      name,
      rowCount: result.rowCount,
      tokenCount: result.tokenCount,
      index: this.completedCount,
      isError: false,
    });

    if (!this.isTTY) {
      process.stdout.write(
        `  ${name.padEnd(32)}${result.rowCount.toLocaleString().padStart(10)}${result.tokenCount.toLocaleString().padStart(15)}   [${this.completedCount}/${this.totalTables}]\n`,
      );
    }
  }

  onTableError(schema: string, table: string, error: string): void {
    const name = `${schema}.${table}`;
    // Remove all active entries for this table
    for (const key of this.activeTables.keys()) {
      if (key.startsWith(name)) {
        this.activeTables.delete(key);
      }
    }
    this.chunkProgress.delete(name);
    this.completedCount++;
    this.errorCount++;
    this.completedEntries.push({
      name,
      rowCount: 0,
      tokenCount: 0,
      index: this.completedCount,
      isError: true,
      errorMessage: error,
    });

    if (!this.isTTY) {
      process.stdout.write(
        `  [ERROR] ${name}: ${error}   [${this.completedCount}/${this.totalTables}]\n`,
      );
    }
  }

  finishProgress(): void {
    this.stopTick();

    if (this.isTTY) {
      this.clearDynamicRegion();
      this.flushCompletedEntries();

      const elapsed = this.formatElapsed();
      const separator = "  " + "-".repeat(60);
      process.stdout.write(`${separator}\n`);
      process.stdout.write(
        `  ${GREEN}✓${RESET} Completed ${BOLD}${this.completedCount}${RESET} table(s) in ${BOLD}${elapsed}${RESET}`,
      );
      if (this.errorCount > 0) {
        process.stdout.write(` (${RED}${this.errorCount} error(s)${RESET})`);
      }
      process.stdout.write("\n");
      process.stdout.write(SHOW_CURSOR);
    } else {
      const separator = "  " + "-".repeat(60);
      const elapsed = this.formatElapsed();
      process.stdout.write(`${separator}\n`);
      process.stdout.write(
        `  Completed ${this.completedCount} table(s) in ${elapsed}`,
      );
      if (this.errorCount > 0) {
        process.stdout.write(` (${this.errorCount} error(s))`);
      }
      process.stdout.write("\n");
    }
  }

  destroy(): void {
    this.stopTick();
    if (this.isTTY) {
      process.stdout.write(SHOW_CURSOR);
    }
  }

  // -------------------------------------------------------------------------
  // Rendering (private)
  // -------------------------------------------------------------------------

  private render(): void {
    // Build entire frame in a single buffer to avoid flicker
    let buf = "";

    // 1. Clear previous dynamic region
    if (this.dynamicLineCount > 0) {
      buf += CURSOR_UP(this.dynamicLineCount);
      for (let i = 0; i < this.dynamicLineCount; i++) {
        buf += CLEAR_LINE + (i < this.dynamicLineCount - 1 ? "\n" : "");
      }
      if (this.dynamicLineCount > 1) {
        buf += CURSOR_UP(this.dynamicLineCount - 1);
      }
      buf += CURSOR_TO_COL0;
    }

    // 2. Flush completed entries (permanent lines above dynamic region)
    while (this.lastPrintedCompletedIndex < this.completedEntries.length) {
      const entry = this.completedEntries[this.lastPrintedCompletedIndex];
      this.lastPrintedCompletedIndex++;
      if (entry.isError) {
        buf += `  ${RED}[ERROR] ${entry.name}: ${entry.errorMessage}${RESET}   [${entry.index}/${this.totalTables}]\n`;
      } else {
        buf += `  ${GREEN}${entry.name.padEnd(32)}${entry.rowCount.toLocaleString().padStart(10)}${entry.tokenCount.toLocaleString().padStart(15)}${RESET}   [${entry.index}/${this.totalTables}]\n`;
      }
    }

    // 3. Build dynamic lines
    const lines: string[] = [];
    lines.push(this.buildProgressBar());

    const frame = SPINNER_FRAMES[this.spinnerIndex % SPINNER_FRAMES.length];
    for (const [, label] of this.activeTables) {
      lines.push(
        `  ${YELLOW}${frame}${RESET} Processing: ${YELLOW}${label}${RESET}`,
      );
    }

    const eta = this.calculateETA();
    const etaSuffix = eta ? `  ETA: ~${eta}` : "";
    lines.push(`  ${DIM}Elapsed: ${this.formatElapsed()}${etaSuffix}${RESET}`);

    buf += lines.join("\n") + "\n";

    // 4. Single atomic write — no intermediate blank frames
    process.stdout.write(buf);
    this.dynamicLineCount = lines.length;
  }

  private clearDynamicRegion(): void {
    if (this.dynamicLineCount > 0) {
      process.stdout.write(CURSOR_UP(this.dynamicLineCount));
      for (let i = 0; i < this.dynamicLineCount; i++) {
        process.stdout.write(CLEAR_LINE + (i < this.dynamicLineCount - 1 ? "\n" : ""));
      }
      if (this.dynamicLineCount > 1) {
        process.stdout.write(CURSOR_UP(this.dynamicLineCount - 1));
      }
      process.stdout.write(CURSOR_TO_COL0);
    }
  }

  private flushCompletedEntries(): void {
    while (this.lastPrintedCompletedIndex < this.completedEntries.length) {
      const entry = this.completedEntries[this.lastPrintedCompletedIndex];
      this.lastPrintedCompletedIndex++;

      if (entry.isError) {
        process.stdout.write(
          `  ${RED}[ERROR] ${entry.name}: ${entry.errorMessage}${RESET}   [${entry.index}/${this.totalTables}]\n`,
        );
      } else {
        process.stdout.write(
          `  ${GREEN}${entry.name.padEnd(32)}${entry.rowCount.toLocaleString().padStart(10)}${entry.tokenCount.toLocaleString().padStart(15)}${RESET}   [${entry.index}/${this.totalTables}]\n`,
        );
      }
    }
  }

  private buildProgressBar(): string {
    const total = this.totalTables || 1;
    const percent = Math.round((this.completedCount / total) * 100);
    const barWidth = 30;
    const filled = Math.round((this.completedCount / total) * barWidth);
    const empty = barWidth - filled;

    const bar =
      GREEN +
      BAR_FILLED.repeat(filled) +
      RESET +
      DIM +
      BAR_EMPTY.repeat(empty) +
      RESET;

    const eta = this.calculateETA();
    const etaPart = eta ? `  ${DIM}ETA: ~${eta}${RESET}` : "";
    return `  [${bar}] ${BOLD}${percent}%${RESET} (${this.completedCount}/${this.totalTables})${etaPart}`;
  }

  private formatElapsed(): string {
    if (this.startTime === 0) return "00:00";
    const totalSeconds = Math.floor((Date.now() - this.startTime) / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  private calculateETA(): string | null {
    if (this.completedCount === 0 || this.startTime === 0) return null;
    const remaining = this.totalTables - this.completedCount;
    if (remaining <= 0) return null;

    const elapsedMs = Date.now() - this.startTime;
    const etaSeconds = Math.ceil((elapsedMs / this.completedCount) * remaining / 1000);
    const minutes = Math.floor(etaSeconds / 60);
    const seconds = etaSeconds % 60;
    return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  private stopTick(): void {
    if (this.tickTimer !== null) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
  }
}
