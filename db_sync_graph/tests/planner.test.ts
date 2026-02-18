import { describe, expect, it } from "@jest/globals";
import { buildTablePlans, decideTableMode } from "../graph.js";
import { CatalogByTable, TableInfo, TableSnapshot } from "../state.js";

function makeTableInfo(): TableInfo {
  return {
    schema: "public",
    table: "users",
    columns: [],
    pkColumns: ["id"],
    updatedAtColumn: "updated_at",
  };
}

function makeSnapshot(): TableSnapshot {
  return {
    schema: "public",
    table: "users",
    rowCount: 10,
    maxUpdatedAt: "2026-02-10T10:00:00.000Z",
    maxPkLexicographic: "10",
    schemaHash: "schema_hash_a",
    tableHash: "table_hash_a",
  };
}

describe("planner", () => {
  it("returns full for table with no previous catalog", () => {
    const decision = decideTableMode({
      previousCatalog: null,
      schemaHash: "schema",
      tableHash: "table",
      hasPrimaryKey: true,
    });

    expect(decision.mode).toBe("full");
  });

  it("returns skip for unchanged table hash", () => {
    const tableInfo = makeTableInfo();
    const snapshot = makeSnapshot();
    const catalogByTable: CatalogByTable = {
      "public.users": {
        schema: "public",
        table: "users",
        schema_hash: "schema_hash_a",
        table_hash: "table_hash_a",
        row_count: 10,
        max_updated_at: snapshot.maxUpdatedAt,
        last_success_run_id: "run_1",
        last_success_at: "2026-02-10T09:00:00.000Z",
        last_mode: "full",
        last_error: null,
      },
    };

    const plans = buildTablePlans({
      tableInfos: [tableInfo],
      tableSnapshots: [snapshot],
      catalogByTable,
    });

    expect(plans[0].mode).toBe("skip");
  });

  it("returns full when schema hash changes", () => {
    const tableInfo = makeTableInfo();
    const snapshot = makeSnapshot();
    const catalogByTable: CatalogByTable = {
      "public.users": {
        schema: "public",
        table: "users",
        schema_hash: "other_schema_hash",
        table_hash: "table_hash_a",
        row_count: 10,
        max_updated_at: snapshot.maxUpdatedAt,
        last_success_run_id: "run_1",
        last_success_at: "2026-02-10T09:00:00.000Z",
        last_mode: "full",
        last_error: null,
      },
    };

    const plans = buildTablePlans({
      tableInfos: [tableInfo],
      tableSnapshots: [snapshot],
      catalogByTable,
    });

    expect(plans[0].mode).toBe("full");
    expect(plans[0].reason).toBe("schema_hash_changed");
  });
});
