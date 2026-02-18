import { describe, expect, it } from "@jest/globals";
import { buildDocId, transformRowToDocument } from "../transform.js";
import { TableInfo } from "../state.js";

const tableInfo: TableInfo = {
  schema: "public",
  table: "users",
  columns: [
    {
      columnName: "id",
      dataType: "integer",
      isNullable: false,
      columnDefault: null,
      ordinalPosition: 1,
      pkPosition: 1,
    },
    {
      columnName: "name",
      dataType: "character varying",
      isNullable: false,
      columnDefault: null,
      ordinalPosition: 2,
      pkPosition: null,
    },
    {
      columnName: "secret",
      dataType: "text",
      isNullable: true,
      columnDefault: null,
      ordinalPosition: 3,
      pkPosition: null,
    },
  ],
  pkColumns: ["id"],
  updatedAtColumn: null,
};

describe("transform", () => {
  it("creates deterministic document id", () => {
    const primaryKey = { id: 10 };
    const first = buildDocId({ schema: "public", table: "users", primaryKey });
    const second = buildDocId({ schema: "public", table: "users", primaryKey });

    expect(first).toBe(second);
  });

  it("excludes configured columns from page content", () => {
    const transformed = transformRowToDocument({
      row: {
        id: 1,
        name: "Alice",
        secret: "token",
      },
      tableInfo,
      runId: "run_1",
      textColumnsMode: "auto",
      excludedColumns: ["secret"],
    });

    expect(transformed.document.pageContent).toContain("name: Alice");
    expect(transformed.document.pageContent).not.toContain("token");
  });

  it("produces non-empty pageContent when textual columns exist", () => {
    const transformed = transformRowToDocument({
      row: {
        id: 2,
        name: "Bob",
        secret: null,
      },
      tableInfo,
      runId: "run_1",
      textColumnsMode: "auto",
      excludedColumns: [],
    });

    expect(transformed.document.pageContent.length).toBeGreaterThan(0);
  });
});
