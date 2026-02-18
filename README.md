# @iludolf/embedding-cli

An interactive CLI for **estimating embedding costs** and **syncing PostgreSQL tables to Elasticsearch** with vector search support.

Built on top of [LangChain](https://www.langchain.com/) and [LangGraph](https://langchain-ai.github.io/langgraphjs/), it provides an interactive terminal experience powered by [`@clack/prompts`](https://github.com/bombshell-dev/clack).

---

## Overview

This tool solves two problems that typically appear before or during a RAG (Retrieval-Augmented Generation) pipeline setup:

1. **Cost estimation** — Before committing to embedding an entire database, how much will it cost? This tool scans your PostgreSQL tables, counts tokens using the same tokenizer as OpenAI, and calculates the estimated cost for each supported embedding model.

2. **DB → Elasticsearch sync** — Once you've decided to proceed, the sync workflow reads your tables, generates embeddings, and upserts documents into Elasticsearch indices. It detects schema and data changes so that subsequent runs only reindex what has actually changed.

---

## Features

- **Interactive menu** — `@clack/prompts` guides you step-by-step; reads from `.env` first and only asks for what's missing
- **Parallel token counting** — uses Node.js worker threads to tokenize rows across multiple tables simultaneously
- **Incremental sync** — skips tables whose schema and row data haven't changed since the last run
- **Audit trail** — every sync run is recorded in a `db_sync_runs` Elasticsearch index
- **Catalog tracking** — per-table metadata is stored in `db_sync_catalog` for change detection
- **Pluggable embeddings** — OpenAI (`text-embedding-3-small`, `text-embedding-3-large`) and Cohere out of the box; custom models supported

---

## Requirements

- **Node.js** >= 20.0.0
- **PostgreSQL** database
- **Elasticsearch** 8.x (cloud or self-hosted)
- An **OpenAI API key** (or Cohere) for embedding generation

---

## Installation

```bash
npm install -g @iludolf/embedding-cli
# or
npx @iludolf/embedding-cli
```

---

## Usage

### Interactive mode

Run without arguments to start the guided interactive flow:

```bash
embedding-cli
```

You will be prompted to:

1. Choose an operation:
   - **Estimate embedding cost** — scan tables and calculate token costs
   - **Sync DB → Elasticsearch** — generate embeddings and index rows
   - **Both** — estimate first, then optionally proceed with sync

2. Provide database credentials (or set them in `.env`)
3. Select tables to process
4. Configure Elasticsearch and embedding model *(sync only)*

### Via npm scripts (development)

```bash
# Interactive embedding CLI
yarn embedding-cli

# Run the cost estimator directly (non-interactive, reads from env)
yarn estimate-cost

# Type-check without compiling
yarn typecheck
```

---

## Environment Variables

Create a `.env` file at the project root. The CLI reads these automatically and only prompts for what is missing.

```env
# ── PostgreSQL ────────────────────────────────────────────────────────────────
# Option A: full URL
SOURCE_DB_URL=postgres://user:password@host:5432/database

# Option B: individual fields
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USERNAME=myuser
DB_PASSWORD=mypassword

# ── Table selection ───────────────────────────────────────────────────────────
SOURCE_TABLE_ALLOWLIST=users,orders,products   # comma-separated, or * for all
SOURCE_TABLE_BLOCKLIST=audit_logs,migrations   # optional
DB_SCHEMA=public                               # optional, default: public

# ── Advanced (optional) ───────────────────────────────────────────────────────
TEXT_COLUMNS_MODE=auto          # auto (text/varchar/json/uuid only) | all
EXCLUDED_COLUMNS=internal_id    # comma-separated columns to skip
SOURCE_BATCH_SIZE=1000          # rows per DB fetch
SOURCE_UPDATED_AT_CANDIDATES=updated_at,modified_at,updatedon

# ── Elasticsearch ─────────────────────────────────────────────────────────────
ELASTICSEARCH_URL=https://my-deployment.es.us-east-1.aws.found.io

# Cloud (API key auth):
ELASTICSEARCH_API_KEY=your-api-key-here

# Local / self-hosted (basic auth):
ELASTICSEARCH_USER=elastic
ELASTICSEARCH_PASSWORD=changeme

# ── Embedding ─────────────────────────────────────────────────────────────────
OPENAI_API_KEY=sk-...
TARGET_INDEX_PREFIX=db_table_   # default index name prefix
```

---

## How it works

### Cost estimation

```
PostgreSQL
    │
    ├─ Discover tables (schema + allowlist filter)
    ├─ Fetch row counts
    └─ Worker threads (parallel)
           ├─ Fetch rows in batches
           ├─ Transform rows → text documents
           ├─ Count tokens with js-tiktoken
           └─ Report back → aggregate totals

Output: table-by-table breakdown + cost per embedding model
```

**Supported models and pricing:**

| Model | Price |
|---|---|
| `text-embedding-3-small` | $0.02 / 1M tokens |
| `text-embedding-3-large` | $0.13 / 1M tokens |

### DB → Elasticsearch sync (LangGraph workflow)

The sync is orchestrated as a 6-node LangGraph state machine:

```
startRun → discoverTables → snapshotTables → planTables → executeTables → finalizeRun
```

| Node | What it does |
|---|---|
| `startRun` | Creates a run record in `db_sync_runs` |
| `discoverTables` | Queries PostgreSQL information_schema with allowlist/blocklist |
| `snapshotTables` | Computes schema hash and data hash (row count + max updated_at) |
| `planTables` | Compares hashes to previous catalog; marks tables as `full` reindex or `skip` |
| `executeTables` | Fetches rows, transforms to documents, generates embeddings, upserts to Elasticsearch |
| `finalizeRun` | Updates catalog records and run status (success / partial_success / failed) |

**Change detection:** a table is reindexed only when its schema hash (column types/names) or data hash (row count / max updated_at) differs from the last successful run. This makes subsequent runs significantly faster.

---

## Project structure

```
├── bin/
│   └── embedding-cli.js          # npm bin entry point (spawns ts-node/esm)
├── src/
│   ├── embedding-cli.ts          # main orchestrator
│   ├── gather-embedding-responses.ts  # interactive prompt flow
│   ├── embedding-types.ts        # TypeScript types
│   ├── helpers.ts                # checkCancel, checkNodeVersion
│   ├── logger.ts                 # simple log level utility
│   └── constants.ts              # REQUIRED_NODE_VERSION
├── cost_estimator/
│   ├── estimate.ts               # estimateCost() — main entry
│   ├── estimate_worker.ts        # worker thread: tokenize rows
│   ├── thread_pool.ts            # parallel worker management
│   ├── terminal_ui.ts            # progress display (TTY)
│   └── progress_file.ts          # optional JSON progress output
├── db_sync_graph/
│   ├── graph.ts                  # LangGraph state machine
│   ├── state.ts                  # state annotations and types
│   ├── configuration.ts          # config resolution (env + configurable)
│   ├── postgres.ts               # schema discovery, row fetching
│   ├── elastic_control.ts        # Elasticsearch client, catalog, runs
│   ├── transform.ts              # row → LangChain Document
│   └── hashing.ts                # schema hash, table hash
├── shared/
│   └── configuration.ts          # BaseConfigurationAnnotation
├── tsconfig.json                 # base TypeScript config
└── tsconfig.embedding.json       # ESM config for the CLI
```

---

## License

MIT
