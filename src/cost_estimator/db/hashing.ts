import crypto from "crypto";
import { ColumnInfo } from "./types.js";

type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

function sortJsonValue(value: unknown): JsonValue {
  if (value == null) {
    return null;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((item) => sortJsonValue(item));
  }

  if (typeof value === "object") {
    const objectValue = value as Record<string, unknown>;
    const sortedEntries = Object.keys(objectValue)
      .sort()
      .map((key) => [key, sortJsonValue(objectValue[key])] as const);

    return Object.fromEntries(sortedEntries);
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  return value as string | number | boolean;
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(sortJsonValue(value));
}

export function sha256Hex(input: string): string {
  return crypto.createHash("sha256").update(input, "utf8").digest("hex");
}

export function computeRowHash(row: Record<string, unknown>): string {
  return sha256Hex(stableStringify(row));
}
