import { homedir } from "node:os";
import { join } from "node:path";
import { readFile, writeFile } from "node:fs/promises";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ModelPricingEntry = {
  provider: string;
  model: string;
  pricePerMillion: number;
};

type PricingCache = {
  fetchedAt: string;
  entries: ModelPricingEntry[];
};

// ---------------------------------------------------------------------------
// Fallback pricing (all embedding providers)
// ---------------------------------------------------------------------------

export const FALLBACK_PRICING: ModelPricingEntry[] = [
  // OpenAI
  { provider: "openai", model: "text-embedding-3-small", pricePerMillion: 0.02 },
  { provider: "openai", model: "text-embedding-3-large", pricePerMillion: 0.13 },
  { provider: "openai", model: "text-embedding-ada-002", pricePerMillion: 0.1 },
  // Cohere
  { provider: "cohere", model: "embed-english-v3.0", pricePerMillion: 0.1 },
  { provider: "cohere", model: "embed-multilingual-v3.0", pricePerMillion: 0.1 },
  // Voyage AI
  { provider: "voyage", model: "voyage-3-large", pricePerMillion: 0.18 },
  { provider: "voyage", model: "voyage-3", pricePerMillion: 0.06 },
  { provider: "voyage", model: "voyage-3-lite", pricePerMillion: 0.02 },
  // Ollama (local, free)
  { provider: "ollama", model: "nomic-embed-text", pricePerMillion: 0 },
  { provider: "ollama", model: "mxbai-embed-large", pricePerMillion: 0 },
  { provider: "ollama", model: "all-minilm", pricePerMillion: 0 },
];

// ---------------------------------------------------------------------------
// Cache management
// ---------------------------------------------------------------------------

const CACHE_PATH = join(homedir(), ".embedding-cli-pricing-cache.json");
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

async function readCache(): Promise<ModelPricingEntry[] | null> {
  try {
    const content = await readFile(CACHE_PATH, "utf-8");
    const cache: PricingCache = JSON.parse(content);

    // Check if cache is still fresh
    const fetchedAt = new Date(cache.fetchedAt);
    const age = Date.now() - fetchedAt.getTime();
    if (age < CACHE_TTL_MS && Array.isArray(cache.entries)) {
      return cache.entries;
    }
  } catch {
    // Cache file missing, invalid, or stale — continue to fetch
  }
  return null;
}

async function writeCache(entries: ModelPricingEntry[]): Promise<void> {
  try {
    const cache: PricingCache = {
      fetchedAt: new Date().toISOString(),
      entries,
    };
    await writeFile(CACHE_PATH, JSON.stringify(cache, null, 2), "utf-8");
  } catch {
    // Silently swallow write errors — caching is best-effort
  }
}

// ---------------------------------------------------------------------------
// LiteLLM fetch
// ---------------------------------------------------------------------------

function inferProvider(
  modelKey: string,
  rawEntry: Record<string, unknown>
): string {
  // Prefer litellm_provider from JSON if available
  if (typeof rawEntry.litellm_provider === "string") {
    return rawEntry.litellm_provider.toLowerCase();
  }

  // Fallback to key prefix
  const slashIdx = modelKey.indexOf("/");
  if (slashIdx > 0) {
    return modelKey.slice(0, slashIdx).toLowerCase();
  }

  // Recognize well-known OpenAI model patterns
  if (
    modelKey.startsWith("text-embedding") ||
    modelKey.startsWith("ada") ||
    modelKey.startsWith("embedding")
  ) {
    return "openai";
  }

  return "other";
}

function extractModelName(modelKey: string): string {
  const slashIdx = modelKey.indexOf("/");
  if (slashIdx > 0) {
    return modelKey.slice(slashIdx + 1);
  }
  return modelKey;
}

async function fetchFromLiteLLM(): Promise<ModelPricingEntry[]> {
  const url =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 5000); // 5s timeout

  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json() as Record<string, unknown>;
    const entries: ModelPricingEntry[] = [];

    for (const [modelKey, rawEntry] of Object.entries(data)) {
      const entry = rawEntry as Record<string, unknown>;

      // Filter: embedding mode only, with valid price
      if (entry.mode !== "embedding") continue;
      if (typeof entry.input_cost_per_token !== "number") continue;

      const pricePerMillion = entry.input_cost_per_token * 1_000_000;
      const provider = inferProvider(modelKey, entry);
      const model = extractModelName(modelKey);

      entries.push({
        provider,
        model,
        pricePerMillion,
      });
    }

    return entries.length > 0 ? entries : FALLBACK_PRICING;
  } catch (error) {
    // Network error, timeout, parse error, etc. — fetch failed
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Fetch pricing entries with fallback and caching.
 *
 * Resolution order:
 *   1. Fresh disk cache (< 24h old)
 *   2. Live fetch from LiteLLM + write to cache
 *   3. Fallback to FALLBACK_PRICING on any error
 *
 * This function never rejects — it always returns a pricing array.
 */
export async function getPricing(): Promise<ModelPricingEntry[]> {
  // Try cache first
  const cached = await readCache();
  if (cached !== null) {
    return cached;
  }

  // Try fetch
  try {
    const fetched = await fetchFromLiteLLM();
    // Write to cache in background (fire-and-forget)
    writeCache(fetched).catch(() => {
      // Silently ignore cache write errors
    });
    return fetched;
  } catch {
    // Any error during fetch → fall back to hardcoded pricing
    return FALLBACK_PRICING;
  }
}

/**
 * Convert ModelPricingEntry[] to Record<model, pricePerMillion> for backward compatibility.
 */
export function toPricingMap(
  entries: ModelPricingEntry[]
): Record<string, number> {
  const map: Record<string, number> = {};
  for (const entry of entries) {
    map[entry.model] = entry.pricePerMillion;
  }
  return map;
}
