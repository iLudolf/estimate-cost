import { cp, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const thisDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(thisDir, "..");
const sourceDashboardPath = resolve(
  rootDir,
  "src/cost_estimator/dashboard/progress_dashboard.html",
);
const targetDashboardPath = resolve(
  rootDir,
  "dist/cost_estimator/dashboard/progress_dashboard.html",
);

await mkdir(dirname(targetDashboardPath), { recursive: true });
await cp(sourceDashboardPath, targetDashboardPath);
