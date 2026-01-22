# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PB1 is a Node.js/Express API server with a single-page frontend for an executive dashboard displaying store reviews and metrics. Backend connects to Supabase and integrates with Apify for review ingestion.

## Commands

- `npm start` or `npm run dev` — runs `node server.js` (both identical)
- `node check-env.js` — utility to verify environment variables are loading
- No tests, lint, or build scripts exist

## Architecture

**Backend**: Single file `server.js` (ESM module, ~1660 lines)
**Frontend**: Single file `frontend/index.html` (inline CSS/JS, ~2900 lines)

### Supabase Tables
Default names (configurable via env vars):
- `stores` — store locations
- `reviews` — individual reviews with `external_id` as unique constraint
- `exec_weekly` — weekly aggregated metrics per store
- `ingest_events` — raw webhook payloads

### API Endpoints

**Public read endpoints:**
- `GET /api/exec-weekly` — weekly metrics (supports week, store, state, city, source filters; has fallback computation from reviews table)
- `GET /api/reviews` — reviews list (supports store_id, state, city, source, limit)
- `GET /api/stores` — store list with market filters
- `GET /api/store/:store_id` — single store lookup (checks both `id` and `store_id` columns)
- `GET /api/stores-apify` — store list formatted for frontend (maps store_id → id)
- `GET /api/meta` — available weeks list
- `GET /api/metrics` — aggregate counts
- `GET /api/stores/summary` — stores reporting count
- `GET /api/debug` — debug info with column introspection
- `GET /health`, `GET /api/health` — health checks

**Ingest endpoints (protected by INGEST_TOKEN via header `X-Ingest-Token` or query `?token=`):**
- `POST /api/ingest/reviews` — bulk upsert reviews
- `POST /api/ingest/apify` — webhook receiver, stores raw payload
- `POST /api/ingest/apify/pull` — pulls from APIFY_DATASET_ID, upserts to `stores_apify` table

**Ingest endpoints (NO auth required):**
- `POST /api/ingest/apify-reviews` — pulls from APIFY_REVIEWS_DATASET_ID, upserts to reviews
- `POST /api/admin/purge-bad-apify` — deletes malformed apify reviews

### Key Patterns

- All responses include `signature`, `build`, `last_updated` fields
- Source filtering defaults to "apify" on most endpoints (uses `ilike("source", "%apify%")`)
- Store ID lookups try both `id` and `store_id` columns with fallback resolution
- `/api/exec-weekly` computes rollups from `reviews` if `exec_weekly` has no data for requested week
- Rate limiting: 60 req/min per IP on protected ingest endpoints

## Environment Variables

**Required:**
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

**Required for ingest features:**
- `APIFY_TOKEN` — Apify API token
- `INGEST_TOKEN` — auth token for protected ingest endpoints
- `APIFY_REVIEWS_DATASET_ID` — dataset ID for reviews ingest (no default, required for `/api/ingest/apify-reviews`)

**Optional with defaults:**
- `PORT` (default: 10000)
- `EXEC_WEEKLY_TABLE` (default: "exec_weekly")
- `STORES_TABLE` (default: "stores")
- `REVIEWS_TABLE` (default: "reviews")
- `INGEST_EVENTS_TABLE` (default: "ingest_events")
- `RPC_REFRESH_ROLLUPS` (default: "refresh_rollups")

**Optional read API auth (fail-open by default for local dev):**
- `READ_API_TOKEN` — token for read API authentication. If not set and `REQUIRE_READ_AUTH=false`, all reads are allowed (local dev mode)
- `REQUIRE_READ_AUTH` — set to `"true"` in production to require read auth. When true, `READ_API_TOKEN` must be set

**Required only for `/api/ingest/apify/pull`:**
- `APIFY_DATASET_ID` — separate dataset for store locations (no default)
