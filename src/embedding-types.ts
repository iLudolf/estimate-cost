import type { TextColumnsMode } from "../db_sync_graph/state.js";

export type EmbeddingOperation = "estimate" | "sync" | "both";

export type ElasticAuthMode = "cloud" | "local";

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

export interface SyncOnlyParams {
  targetIndexPrefix: string;
  embeddingModel: string;
  elasticsearchUrl: string;
  elasticAuthMode: ElasticAuthMode;
  elasticsearchApiKey?: string;
  elasticsearchUser?: string;
  elasticsearchPassword?: string;
  openaiApiKey?: string;
}

export interface EmbeddingUserAnswers {
  operation: EmbeddingOperation;
  common: CommonEmbeddingParams;
  sync?: SyncOnlyParams;
}
