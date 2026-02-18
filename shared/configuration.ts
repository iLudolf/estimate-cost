import { Annotation } from "@langchain/langgraph";
import { RunnableConfig } from "@langchain/core/runnables";

export const BaseConfigurationAnnotation = Annotation.Root({
  retrieverProvider: Annotation<string>,
  embeddingModel: Annotation<string>,
});

export function ensureBaseConfiguration(config: RunnableConfig) {
  const raw = (config?.configurable || {}) as Record<string, unknown>;
  return {
    retrieverProvider:
      (raw.retrieverProvider as string) ??
      process.env.RETRIEVER_PROVIDER ??
      "elastic",
    embeddingModel:
      (raw.embeddingModel as string) ??
      process.env.EMBEDDING_MODEL ??
      "openai/text-embedding-3-small",
  };
}
