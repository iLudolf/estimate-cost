import { describe, expect, it } from "@jest/globals";
import {
  buildTablePlans,
  computeRunStatus,
  executePlansWithHandler,
  summarizeRun,
} from "../graph.js";
import { CatalogByTable, TableInfo, TableSnapshot } from "../state.js";

function makeTableInfo(table: string): TableInfo {
  return {
    schema: "public",
    table,
    columns: [],
    pkColumns: ["id"],
    updatedAtColumn: "updated_at",
  };
}

function makeSnapshot(table: string, hash: string): TableSnapshot {
  return {
    schema: "public",
    table,
    rowCount: 10,
    maxUpdatedAt: "2026-02-10T10:00:00.000Z",
    maxPkLexicographic: "10",
    schemaHash: `schema_${table}`,
    tableHash: hash,
  };
}

describe("integration with mocks", () => {
  it("plans full on first run and skip on second run", () => {
    const tableInfos = [makeTableInfo("users")];
    const tableSnapshots = [makeSnapshot("users", "hash_a")];

    const firstPlans = buildTablePlans({
      tableInfos,
      tableSnapshots,
      catalogByTable: {},
    });

    expect(firstPlans[0].mode).toBe("full");

    const catalogByTable: CatalogByTable = {
      "public.users": {
        schema: "public",
        table: "users",
        schema_hash: "schema_users",
        table_hash: "hash_a",
        row_count: 10,
        max_updated_at: "2026-02-10T10:00:00.000Z",
        last_success_run_id: "run_1",
        last_success_at: "2026-02-10T10:10:00.000Z",
        last_mode: "full",
        last_error: null,
      },
    };

    const secondPlans = buildTablePlans({
      tableInfos,
      tableSnapshots,
      catalogByTable,
    });

    expect(secondPlans[0].mode).toBe("skip");
  });

  it("keeps running when one table fails and ends partial_success", async () => {
    const tablePlans = [
      {
        schema: "public",
        table: "users",
        mode: "full" as const,
        reason: "no_previous_catalog",
      },
      {
        schema: "public",
        table: "orders",
        mode: "full" as const,
        reason: "no_previous_catalog",
      },
    ];

    const tableResults = await executePlansWithHandler({
      tablePlans,
      onFullTable: async (tablePlan) => {
        if (tablePlan.table === "users") {
          return 5;
        }

        throw new Error("table failed");
      },
    });

    const status = computeRunStatus({
      fatalError: null,
      tableResults,
    });

    const summary = summarizeRun({
      tablePlans,
      tableResults,
    });

    expect(tableResults).toHaveLength(2);
    expect(tableResults[0].status).toBe("reindexed");
    expect(tableResults[1].status).toBe("failed");
    expect(summary.tablesReindexed).toBe(1);
    expect(summary.errors).toHaveLength(1);
    expect(status).toBe("partial_success");
  });
});
