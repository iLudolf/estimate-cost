import { describe, expect, it } from "@jest/globals";
import { computeSchemaHash } from "../hashing.js";
import { ColumnInfo } from "../state.js";

function baseColumns(): ColumnInfo[] {
  return [
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
  ];
}

describe("hashing", () => {
  it("produces deterministic schema hash", () => {
    const columns = baseColumns();

    const firstHash = computeSchemaHash(columns);
    const secondHash = computeSchemaHash(columns);

    expect(firstHash).toBe(secondHash);
  });

  it("changes schema hash when column definition changes", () => {
    const columns = baseColumns();
    const originalHash = computeSchemaHash(columns);

    const changedColumns = baseColumns();
    changedColumns[1] = {
      ...changedColumns[1],
      dataType: "text",
    };

    const changedHash = computeSchemaHash(changedColumns);

    expect(changedHash).not.toBe(originalHash);
  });
});
