# @iludolf/embedding-cli

CLI for estimating embedding costs from PostgreSQL tables.

It scans selected tables, converts rows to text, counts tokens with `js-tiktoken`, and calculates estimated cost across multiple embedding models.

## Dashboard preview

![Embedding Estimation Progress Dashboard](./.github/progress-dashboard.png)

## What this project does

- Interactive CLI flow with `@clack/prompts`
- Parallel token counting with Node.js worker threads
- Large-table chunking for better throughput
- Real-time progress JSON output (`cost_estimation_progress.json` by default)
- Built-in web dashboard for live progress and cost tracking
- Pricing fetched from LiteLLM model pricing data, with local cache and fallback prices

## Requirements

- Node.js `>=20.0.0`
- PostgreSQL access
- Optional internet access for live pricing fetch (falls back to bundled pricing if unavailable)

## Setup

```bash
yarn install
```

## Usage

### Interactive CLI

```bash
yarn embedding-cli
```

Notes:
- Reads `.env` first and asks only for missing values.
- Prompt messages are in Portuguese.
- Starts the progress dashboard automatically (default `http://127.0.0.1:4173`).

### Non-interactive estimator

```bash
yarn estimate-cost
```

This mode reads configuration from environment variables only.

### Dashboard only

```bash
yarn progress-dashboard
```

Optional CLI flags:

```bash
yarn progress-dashboard --host 127.0.0.1 --port 4173 --file ./cost_estimation_progress.json
```

### Type checking

```bash
yarn typecheck
```

## Quick start

Create a `.env` file in the project root:

```env
# Database (choose one approach)
SOURCE_DB_URL=postgres://user:password@host:5432/database
# or use individual fields:
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=mydb
# DB_USERNAME=myuser
# DB_PASSWORD=mypassword

# Required for non-interactive mode (use "*" for all tables)
SOURCE_TABLE_ALLOWLIST=users,orders,products

# Optional
DB_SCHEMA=public
SOURCE_TABLE_BLOCKLIST=audit_logs,migrations
SOURCE_UPDATED_AT_CANDIDATES=updated_at,modified_at,updatedon
TEXT_COLUMNS_MODE=auto
EXCLUDED_COLUMNS=internal_id
SOURCE_BATCH_SIZE=1000

# Progress output / dashboard
COST_PROGRESS_FILE=./cost_estimation_progress.json
COST_DASHBOARD_HOST=127.0.0.1
COST_DASHBOARD_PORT=4173
```

Then run:

```bash
yarn embedding-cli
```

## Environment variables

### Database connection

- `SOURCE_DB_URL`: Full PostgreSQL URL.
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`: Alternative to `SOURCE_DB_URL`.

### Table selection

- `SOURCE_TABLE_ALLOWLIST`: Comma-separated tables or `*`.
- `SOURCE_TABLE_BLOCKLIST`: Optional comma-separated skip list.
- `SOURCE_SCHEMA` or `DB_SCHEMA`: Source schema (`public` default).
- `SOURCE_UPDATED_AT_CANDIDATES`: Candidate timestamp columns for snapshot logic. Default: `updated_at,modified_at,updatedon`.

### Row-to-text conversion

- `TEXT_COLUMNS_MODE`: `auto` (default) or `all`.
- `EXCLUDED_COLUMNS`: Comma-separated columns to exclude.
- `SOURCE_BATCH_SIZE`: Rows fetched per batch. Default: `1000`.

### Performance tuning

- `MAX_THREADS`: Worker thread limit. Default: `CPU cores - 1` (minimum 1).
- `TABLES_PER_BATCH`: Work items assigned per worker turn. Default: `3`.
- `LARGE_TABLE_THRESHOLD`: Table row threshold to split into chunks. Default: `50000`.
- `CHUNK_SIZE`: Rows per chunk for large tables. Default: `10000`.

### Progress and dashboard

- `COST_PROGRESS_FILE`: Progress JSON file path. Default: `./cost_estimation_progress.json`.
- `COST_DASHBOARD_HOST`: Dashboard host. Default: `127.0.0.1`.
- `COST_DASHBOARD_PORT`: Dashboard port. Default: `4173`.

## How it works

1. Discover tables from `information_schema`.
2. Apply allowlist/blocklist filters.
3. Fetch row counts and split large tables into chunks.
4. Process tables/chunks in parallel workers:
   - Fetch rows in batches.
   - Convert rows to text payloads.
   - Count tokens with `js-tiktoken` (`text-embedding-3-small` tokenizer).
5. Aggregate total tokens and calculate estimated cost per pricing entry.
6. Write progress snapshots to JSON for terminal/dashboard visualization.

## Pricing source and cache

- Runtime pricing source: LiteLLM `model_prices_and_context_window.json`
- Local cache file: `~/.embedding-cli-pricing-cache.json`
- Cache TTL: 24 hours
- Fallback: bundled static pricing entries (OpenAI, Cohere, Voyage, Ollama)

## Dashboard endpoints

- `/`: HTML dashboard
- `/api/progress`: Current progress snapshot JSON
- `/api/stream`: Server-Sent Events stream for live updates
- `/health`: Health check

## Project structure

```text
.
├── bin/
│   └── embedding-cli.js
├── src/
│   ├── embedding-cli.ts
│   ├── gather-embedding-responses.ts
│   ├── embedding-types.ts
│   ├── constants.ts
│   ├── helpers.ts
│   ├── logger.ts
│   └── cost_estimator/
│       ├── estimate.ts
│       ├── estimate_worker.ts
│       ├── thread_pool.ts
│       ├── terminal_ui.ts
│       ├── pricing.ts
│       ├── progress_file.ts
│       ├── progress_dashboard.ts
│       ├── dashboard/progress_dashboard.html
│       └── db/
│           ├── postgres.ts
│           ├── transform.ts
│           ├── hashing.ts
│           └── types.ts
├── tsconfig.json
├── tsconfig.embedding.json
└── package.json
```

## License

MIT
