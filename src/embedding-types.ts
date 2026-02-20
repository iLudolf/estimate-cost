import type { TextColumnsMode } from "./cost_estimator/db/types.js";

export type EmbeddingOperation = "estimate";

export interface CommonEmbeddingParams {
  sourceDbUrl: string;
  sourceSchema: string;
  tableAllowlist: string[];
  tableBlocklist: string[];
  textColumnsMode: TextColumnsMode;
  excludedColumns: string[];
  batchSize: number;
  updatedAtCandidates: string[];
}

export interface EmbeddingUserAnswers {
  operation: EmbeddingOperation;
  common: CommonEmbeddingParams;
}
