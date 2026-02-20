import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { readFile } from "node:fs/promises";
import { dirname as pathDirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type CliOptions = {
  progressFilePath: string;
  host: string;
  port: number;
};

export type ProgressDashboardStartOptions = {
  progressFilePath?: string;
  host?: string;
  port?: number;
  suppressLogs?: boolean;
};

export type ProgressDashboardHandle = {
  host: string;
  port: number;
  url: string;
  progressFilePath: string;
  close: () => Promise<void>;
};

type ProgressPayload = {
  meta: {
    fetchedAt: string;
    progressFilePath: string;
  };
  progress: unknown;
};

type ProgressSnapshot = {
  signature: string;
  payload: ProgressPayload;
};

type StreamState = {
  clients: Map<number, ServerResponse>;
  nextClientId: number;
  pollTimer: ReturnType<typeof setInterval> | null;
  lastSignature: string | null;
  lastErrorSignature: string | null;
};

const DEFAULT_PROGRESS_FILE = "./cost_estimation_progress.json";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 4173;
const STREAM_POLL_INTERVAL_MS = 800;
const STREAM_HEARTBEAT_MS = 15000;

const streamState: StreamState = {
  clients: new Map(),
  nextClientId: 1,
  pollTimer: null,
  lastSignature: null,
  lastErrorSignature: null,
};

function parseArgValue(argv: string[], key: string): string | undefined {
  const flag = `--${key}`;
  const index = argv.findIndex((arg) => arg === flag);
  if (index === -1) {
    return undefined;
  }

  const value = argv[index + 1];
  if (!value || value.startsWith("--")) {
    return undefined;
  }

  return value;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseCliOptions(argv: string[]): CliOptions {
  const progressFileArg = parseArgValue(argv, "file");
  const hostArg = parseArgValue(argv, "host");
  const portArg = parseArgValue(argv, "port");

  const progressFilePath =
    progressFileArg ||
    process.env.COST_PROGRESS_FILE ||
    DEFAULT_PROGRESS_FILE;

  const host = hostArg || process.env.COST_DASHBOARD_HOST || DEFAULT_HOST;
  const port = parsePositiveInt(
    portArg || process.env.COST_DASHBOARD_PORT,
    DEFAULT_PORT,
  );

  return {
    progressFilePath,
    host,
    port,
  };
}

function resolveRuntimeOptions(
  startOptions: ProgressDashboardStartOptions = {},
): CliOptions {
  const defaults = parseCliOptions([]);
  const port =
    typeof startOptions.port === "number" &&
    Number.isFinite(startOptions.port) &&
    startOptions.port > 0
      ? Math.floor(startOptions.port)
      : defaults.port;

  return {
    progressFilePath: startOptions.progressFilePath ?? defaults.progressFilePath,
    host: startOptions.host ?? defaults.host,
    port,
  };
}

function sendJson(
  response: ServerResponse,
  statusCode: number,
  data: unknown,
): void {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(data, null, 2));
}

function sendText(
  response: ServerResponse,
  statusCode: number,
  body: string,
  contentType: string,
): void {
  response.writeHead(statusCode, {
    "Content-Type": contentType,
    "Cache-Control": "no-store",
  });
  response.end(body);
}

function isNodeError(value: unknown): value is NodeJS.ErrnoException {
  return value instanceof Error;
}

function resolveProgressFilePath(progressFilePath: string): string {
  return resolve(process.cwd(), progressFilePath);
}

function buildProgressPayload(progressFilePath: string, progress: unknown): ProgressPayload {
  return {
    meta: {
      fetchedAt: new Date().toISOString(),
      progressFilePath: resolveProgressFilePath(progressFilePath),
    },
    progress,
  };
}

async function readProgressSnapshot(
  progressFilePath: string,
): Promise<ProgressSnapshot> {
  const raw = await readFile(progressFilePath, "utf-8");
  const progress = JSON.parse(raw) as unknown;
  return {
    signature: raw,
    payload: buildProgressPayload(progressFilePath, progress),
  };
}

function mapProgressReadError(
  error: unknown,
  progressFilePath: string,
): {
  error: string;
  message: string;
  progressFilePath?: string;
} {
  if (isNodeError(error) && error.code === "ENOENT") {
    return {
      error: "Progress file not found",
      message:
        "The progress file does not exist yet. Start the estimator first, then refresh this page.",
      progressFilePath: resolveProgressFilePath(progressFilePath),
    };
  }

  if (error instanceof SyntaxError) {
    return {
      error: "Invalid progress file format",
      message:
        "Could not parse the progress JSON file. Check whether the file is being written correctly.",
      progressFilePath: resolveProgressFilePath(progressFilePath),
    };
  }

  const message = isNodeError(error) ? error.message : "Unknown error";
  return {
    error: "Failed to read progress file",
    message,
    progressFilePath: resolveProgressFilePath(progressFilePath),
  };
}

function writeSseEvent(
  response: ServerResponse,
  eventName: string,
  payload: unknown,
): void {
  response.write(`event: ${eventName}\n`);
  response.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcastSseEvent(eventName: string, payload: unknown): void {
  for (const [clientId, response] of streamState.clients.entries()) {
    try {
      writeSseEvent(response, eventName, payload);
    } catch {
      streamState.clients.delete(clientId);
    }
  }
}

async function pollProgressAndBroadcast(progressFilePath: string): Promise<void> {
  if (streamState.clients.size === 0) {
    return;
  }

  try {
    const snapshot = await readProgressSnapshot(progressFilePath);
    if (snapshot.signature === streamState.lastSignature) {
      return;
    }

    streamState.lastSignature = snapshot.signature;
    streamState.lastErrorSignature = null;
    broadcastSseEvent("progress", snapshot.payload);
  } catch (error: unknown) {
    const errorPayload = mapProgressReadError(error, progressFilePath);
    const errorSignature = JSON.stringify(errorPayload);

    if (errorSignature === streamState.lastErrorSignature) {
      return;
    }

    streamState.lastErrorSignature = errorSignature;
    streamState.lastSignature = null;
    broadcastSseEvent("progress_error", errorPayload);
  }
}

function startProgressPolling(progressFilePath: string): void {
  if (streamState.pollTimer !== null) {
    return;
  }

  streamState.pollTimer = setInterval(() => {
    void pollProgressAndBroadcast(progressFilePath);
  }, STREAM_POLL_INTERVAL_MS);

  void pollProgressAndBroadcast(progressFilePath);
}

function stopProgressPollingWhenIdle(): void {
  if (streamState.clients.size > 0) {
    return;
  }

  if (streamState.pollTimer !== null) {
    clearInterval(streamState.pollTimer);
    streamState.pollTimer = null;
  }

  streamState.lastSignature = null;
  streamState.lastErrorSignature = null;
}

async function sendInitialSnapshot(
  response: ServerResponse,
  progressFilePath: string,
): Promise<void> {
  try {
    const snapshot = await readProgressSnapshot(progressFilePath);
    writeSseEvent(response, "progress", snapshot.payload);
  } catch (error: unknown) {
    const payload = mapProgressReadError(error, progressFilePath);
    writeSseEvent(response, "progress_error", payload);
  }
}

async function handleDashboardPage(
  response: ServerResponse,
  dashboardFilePath: string,
): Promise<void> {
  try {
    const html = await readFile(dashboardFilePath, "utf-8");
    sendText(response, 200, html, "text/html; charset=utf-8");
  } catch (error: unknown) {
    const message = isNodeError(error) ? error.message : "Unknown error";
    sendText(
      response,
      500,
      `Dashboard page unavailable: ${message}`,
      "text/plain; charset=utf-8",
    );
  }
}

async function handleProgressApi(
  response: ServerResponse,
  progressFilePath: string,
): Promise<void> {
  try {
    const snapshot = await readProgressSnapshot(progressFilePath);
    sendJson(response, 200, snapshot.payload);
  } catch (error: unknown) {
    const payload = mapProgressReadError(error, progressFilePath);
    const statusCode = payload.error === "Progress file not found" ? 404 : 500;
    sendJson(response, statusCode, payload);
  }
}

async function handleProgressStream(
  request: IncomingMessage,
  response: ServerResponse,
  progressFilePath: string,
): Promise<void> {
  response.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  response.write(": connected\n\n");

  const clientId = streamState.nextClientId;
  streamState.nextClientId++;
  streamState.clients.set(clientId, response);

  const heartbeat = setInterval(() => {
    response.write(": heartbeat\n\n");
  }, STREAM_HEARTBEAT_MS);

  let isClosed = false;
  const closeClient = () => {
    if (isClosed) {
      return;
    }
    isClosed = true;
    clearInterval(heartbeat);
    streamState.clients.delete(clientId);
    stopProgressPollingWhenIdle();
  };

  request.on("close", closeClient);
  response.on("close", closeClient);
  response.on("error", closeClient);

  await sendInitialSnapshot(response, progressFilePath);
  startProgressPolling(progressFilePath);
}

function handleNotFound(response: ServerResponse): void {
  sendJson(response, 404, {
    error: "Not found",
  });
}

async function handleRequest(
  request: IncomingMessage,
  response: ServerResponse,
  options: CliOptions,
  dashboardFilePath: string,
): Promise<void> {
  const method = request.method ?? "GET";
  if (method !== "GET" && method !== "HEAD") {
    response.writeHead(405, {
      Allow: "GET, HEAD",
    });
    response.end();
    return;
  }

  const url = new URL(
    request.url ?? "/",
    `http://${request.headers.host ?? "localhost"}`,
  );

  if (url.pathname === "/" || url.pathname === "/index.html") {
    await handleDashboardPage(response, dashboardFilePath);
    return;
  }

  if (url.pathname === "/api/progress") {
    await handleProgressApi(response, options.progressFilePath);
    return;
  }

  if (url.pathname === "/api/stream") {
    if (method !== "GET") {
      response.writeHead(405, { Allow: "GET" });
      response.end();
      return;
    }
    await handleProgressStream(request, response, options.progressFilePath);
    return;
  }

  if (url.pathname === "/health") {
    sendJson(response, 200, {
      status: "ok",
      now: new Date().toISOString(),
    });
    return;
  }

  if (url.pathname === "/favicon.ico") {
    response.writeHead(204);
    response.end();
    return;
  }

  handleNotFound(response);
}

function getDashboardFilePath(): string {
  const currentFile = fileURLToPath(import.meta.url);
  const currentDir = pathDirname(currentFile);
  return resolve(
    currentDir,
    "dashboard",
    "progress_dashboard.html",
  );
}

function resetStreamState(): void {
  for (const response of streamState.clients.values()) {
    response.end();
  }
  streamState.clients.clear();
  stopProgressPollingWhenIdle();
}

export async function startProgressDashboard(
  startOptions: ProgressDashboardStartOptions = {},
): Promise<ProgressDashboardHandle> {
  const options = resolveRuntimeOptions(startOptions);
  const dashboardFilePath = getDashboardFilePath();

  const server = createServer((request, response) => {
    void handleRequest(request, response, options, dashboardFilePath);
  });

  await new Promise<void>((resolveServer, rejectServer) => {
    const onError = (error: unknown) => {
      rejectServer(error);
    };

    server.once("error", onError);
    server.listen(options.port, options.host, () => {
      server.off("error", onError);
      if (!startOptions.suppressLogs) {
        console.log(
          `[progress-dashboard] Running at http://${options.host}:${options.port}`,
        );
        console.log(
          `[progress-dashboard] Reading progress file: ${resolve(
            process.cwd(),
            options.progressFilePath,
          )}`,
        );
        console.log(
          `[progress-dashboard] Real-time stream: http://${options.host}:${options.port}/api/stream`,
        );
        console.log(
          "[progress-dashboard] Press Ctrl+C to stop.",
        );
      }
      resolveServer();
    });
  });

  let closed = false;

  return {
    host: options.host,
    port: options.port,
    url: `http://${options.host}:${options.port}`,
    progressFilePath: resolve(process.cwd(), options.progressFilePath),
    close: async () => {
      if (closed) {
        return;
      }
      closed = true;
      resetStreamState();
      await new Promise<void>((resolveClose) => {
        server.close(() => resolveClose());
      });
    },
  };
}

async function main(): Promise<void> {
  const cliOptions = parseCliOptions(process.argv.slice(2));
  await startProgressDashboard({
    progressFilePath: cliOptions.progressFilePath,
    host: cliOptions.host,
    port: cliOptions.port,
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    const message = isNodeError(error) ? error.message : "Unknown error";
    console.error(`[progress-dashboard] Fatal error: ${message}`);
    process.exit(1);
  });
}
