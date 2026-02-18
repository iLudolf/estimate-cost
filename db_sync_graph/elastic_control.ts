import { Client } from "@elastic/elasticsearch";
import { ElasticVectorSearch } from "@langchain/community/vectorstores/elasticsearch";
import { CohereEmbeddings } from "@langchain/cohere";
import { Embeddings } from "@langchain/core/embeddings";
import { OpenAIEmbeddings } from "@langchain/openai";
import { ControlCatalogRecord } from "./state.js";

export type ElasticRetrieverProvider =
  | "elastic"
  | "elastic-local"
  | "pinecone"
  | "mongodb";

type RunRecordStatus = "running" | "success" | "partial_success" | "failed";

export type RunRecord = {
  run_id: string;
  started_at: string;
  finished_at: string | null;
  status: RunRecordStatus;
  tables_total: number;
  tables_reindexed: number;
  tables_skipped: number;
  rows_upserted: number;
  errors: Array<{ table: string; message: string }>;
};

export function createElasticClient(
  retrieverProvider: ElasticRetrieverProvider,
): Client {
  if (retrieverProvider !== "elastic" && retrieverProvider !== "elastic-local") {
    throw new Error(
      `db_sync_graph supports only elastic retrievers. Received: ${retrieverProvider}`,
    );
  }

  const elasticUrl = process.env.ELASTICSEARCH_URL;
  if (!elasticUrl) {
    throw new Error("ELASTICSEARCH_URL environment variable is not defined");
  }

  if (retrieverProvider === "elastic-local") {
    const username = process.env.ELASTICSEARCH_USER;
    const password = process.env.ELASTICSEARCH_PASSWORD;

    if (!username || !password) {
      throw new Error(
        "ELASTICSEARCH_USER and ELASTICSEARCH_PASSWORD are required for elastic-local provider",
      );
    }

    return new Client({
      node: elasticUrl,
      auth: { username, password },
    });
  }

  const apiKey = process.env.ELASTICSEARCH_API_KEY;
  if (!apiKey) {
    throw new Error(
      "ELASTICSEARCH_API_KEY environment variable is required for elastic provider",
    );
  }

  return new Client({
    node: elasticUrl,
    auth: { apiKey },
  });
}

export function makeTextEmbeddings(modelName: string): Embeddings {
  const splitIndex = modelName.indexOf("/");
  const provider = splitIndex >= 0 ? modelName.slice(0, splitIndex) : "openai";
  const model = splitIndex >= 0 ? modelName.slice(splitIndex + 1) : modelName;

  switch (provider) {
    case "openai":
      return new OpenAIEmbeddings({ model });
    case "cohere":
      return new CohereEmbeddings({ model });
    default:
      throw new Error(`Unsupported embedding provider: ${provider}`);
  }
}

export function createTableVectorStore(params: {
  client: Client;
  embeddingModel: Embeddings;
  indexName: string;
}): ElasticVectorSearch {
  return new ElasticVectorSearch(params.embeddingModel, {
    client: params.client,
    indexName: params.indexName,
  });
}

export function buildTableIndexName(params: {
  targetIndexPrefix: string;
  schema: string;
  table: string;
}): string {
  return `${params.targetIndexPrefix}${params.schema}_${params.table}`.toLowerCase();
}

export function catalogDocumentId(schema: string, table: string): string {
  return `${schema}.${table}`;
}

export async function getCatalogRecord(params: {
  client: Client;
  indexName: string;
  schema: string;
  table: string;
}): Promise<ControlCatalogRecord | null> {
  const id = catalogDocumentId(params.schema, params.table);

  try {
    const response = await params.client.get<ControlCatalogRecord>({
      index: params.indexName,
      id,
    });

    return response._source ?? null;
  } catch (error) {
    if (isNotFound(error)) {
      return null;
    }
    throw error;
  }
}

export async function putCatalogRecord(params: {
  client: Client;
  indexName: string;
  record: ControlCatalogRecord;
}): Promise<void> {
  await params.client.index({
    index: params.indexName,
    id: catalogDocumentId(params.record.schema, params.record.table),
    document: params.record,
    refresh: true,
  });
}

export async function putRunRecord(params: {
  client: Client;
  indexName: string;
  runId: string;
  record: RunRecord;
}): Promise<void> {
  await params.client.index({
    index: params.indexName,
    id: params.runId,
    document: params.record,
    refresh: true,
  });
}

function isNotFound(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const statusCode =
    "statusCode" in error ? Number((error as { statusCode?: number }).statusCode) : NaN;

  return statusCode === 404;
}
