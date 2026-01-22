// server.js (PB1) — COPY/PASTE ENTIRE FILE
// ESM module (requires: "type":"module" in package.json)

import "dotenv/config";
import express from "express";
import cors from "cors";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


const app = express();
const PORT = process.env.PORT || 10000;

// ----- Fingerprints (so you always know what's running) -----
const BUILD = process.env.BUILD || "PB1_SERVER_BUILD__2026-01-17__APIFY_LOCK";

console.log("✅ SERVER FILE:", new URL(import.meta.url).pathname);
console.log("✅ SERVER BUILD:", BUILD);
console.log("✅ SERVER START:", new Date().toISOString());

// ----- Config -----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const APIFY_TOKEN = process.env.APIFY_TOKEN || "";
const APIFY_REVIEWS_DATASET_ID = (process.env.APIFY_REVIEWS_DATASET_ID || "").trim();

// required for ingest protection
const INGEST_TOKEN = (process.env.INGEST_TOKEN || "").trim();
console.log("🔐 INGEST_TOKEN loaded?", INGEST_TOKEN ? "YES" : "NO", "len=", INGEST_TOKEN.length);

// optional read API auth (fail-open by default for local dev)
const READ_API_TOKEN = (process.env.READ_API_TOKEN || "").trim();
const REQUIRE_READ_AUTH = process.env.REQUIRE_READ_AUTH === "true";
console.log("🔐 READ_API_TOKEN loaded?", READ_API_TOKEN ? "YES" : "NO", "REQUIRE_READ_AUTH=", REQUIRE_READ_AUTH);

// Table/View names (match your Supabase schema)
const CFG = {
  EXEC_WEEKLY: process.env.EXEC_WEEKLY_TABLE || "exec_weekly",
  STORES: process.env.STORES_TABLE || "stores",
  REVIEWS: process.env.REVIEWS_TABLE || "reviews",
  INGEST_EVENTS: process.env.INGEST_EVENTS_TABLE || "ingest_events",
  // optional RPC to refresh rollups (only runs if present and exists)
  RPC_REFRESH_ROLLUPS: process.env.RPC_REFRESH_ROLLUPS || "refresh_rollups",
};

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    : null;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("⚠️ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
}

// ----- Middleware -----
app.set("etag", false);
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "X-Ingest-Token", "X-Read-Token"],
    maxAge: 86400,
  })
);
app.use(express.json({ limit: "2mb" }));

app.use(express.static(path.join(__dirname, "frontend")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "frontend", "index.html"));
});


// Prevent caching on API routes (avoid 304/body issues)
app.use("/api", function (_req, res, next) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

// ----- Helpers -----
const ALLOWED_SOURCES = ["apify"];

let LAST_APIFY_REVIEWS_INGEST_AT = null;

function norm(x) {
  return (x ?? "").toString().trim().toLowerCase();
}
function isApifySource(s) {
  return norm(s).includes("apify");
}
function nowIso() {
  return new Date().toISOString();
}
function respondError(res, signature, error, status = 500, extra = {}) {
  res.status(status).json({
    ok: false,
    signature,
    build: BUILD,
    last_updated: nowIso(),
    error,
    ...extra,
  });
}
function applyApifySourceFilter(q) {
  return q.ilike("source", "%apify%");
}

/**
 * Apply source filter to a Supabase query
 * @param {object} q - Supabase query builder
 * @param {string|null} source - Source to filter by (null = no filter)
 * @param {boolean} hasSourceColumn - Whether the table has a source column
 * @returns {object} Modified query builder
 */
function applySourceFilter(q, source, hasSourceColumn) {
  if (!source || !hasSourceColumn) return q;
  if (source.toLowerCase() === "apify") return q.ilike("source", "%apify%");
  return q.eq("source", source);
}

function isWeekString(v) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(v || ""));
}
function normalizeState(input) {
  const s = String(input || "").trim();
  if (!s) return { raw: null, code: null, name: null };
  const up = s.toUpperCase();
  const map = {
    AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California", CO: "Colorado", CT: "Connecticut",
    DE: "Delaware", FL: "Florida", GA: "Georgia", HI: "Hawaii", ID: "Idaho", IL: "Illinois", IN: "Indiana", IA: "Iowa",
    KS: "Kansas", KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland", MA: "Massachusetts", MI: "Michigan",
    MN: "Minnesota", MS: "Mississippi", MO: "Missouri", MT: "Montana", NE: "Nebraska", NV: "Nevada", NH: "New Hampshire",
    NJ: "New Jersey", NM: "New Mexico", NY: "New York", NC: "North Carolina", ND: "North Dakota", OH: "Ohio", OK: "Oklahoma",
    OR: "Oregon", PA: "Pennsylvania", RI: "Rhode Island", SC: "South Carolina", SD: "South Dakota", TN: "Tennessee",
    TX: "Texas", UT: "Utah", VT: "Vermont", VA: "Virginia", WA: "Washington", WV: "West Virginia", WI: "Wisconsin", WY: "Wyoming"
  };
  if (up.length === 2 && map[up]) return { raw: s, code: up, name: map[up] };
  const codeFromName = Object.keys(map).find((code) => map[code].toUpperCase() === up);
  if (codeFromName) return { raw: s, code: codeFromName, name: map[codeFromName] };
  // if user typed full name already or something else
  return { raw: s, code: null, name: s };
}

/**
 * Parse state and city filters from request query parameters
 * @param {object} req - Express request object
 * @returns {{ state: {raw: string, code: string, name: string}|null, city: string|null }} Parsed market filters
 */
function parseMarketFilters(req) {
  const stateRaw = (req.query.state || "").toString().trim();
  const cityRaw  = (req.query.city  || "").toString().trim();

  return {
    state: stateRaw ? normalizeState(stateRaw) : null,
    city: cityRaw || null
  };
}

/**
 * Apply state and city filters to a Supabase query builder
 * @param {object} q - Supabase query builder
 * @param {{ state: {raw: string, code: string, name: string}|null, city: string|null }} filters - Market filters
 * @returns {object} Modified query builder
 */
function applyMarketFilters(q, { state, city } = {}) {
  let out = q;
  const name = state?.name || null;
  const code = state?.code || null;
  if (name && code) {
    out = out.or(`state.ilike.%${name}%,state.ilike.%${code}%`);
  } else if (name) {
    out = out.ilike("state", `%${name}%`);
  }
  if (city) out = out.ilike("city", city);
  return out;
}
function parseLimit(raw, fallback, max) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}
function isUuid(v) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    String(v || "")
  );
}
async function storeIdExists(id) {
  if (!supabase) return false;
  const res = await supabase.from(CFG.STORES).select("id").eq("id", id).limit(1);
  if (res.error) return false;
  return (res.data || []).length > 0;
}

// Column cache for inferColumns() - avoids repeated DB queries for schema info
const columnCache = new Map();
const COLUMN_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function inferColumns(tableName) {
  // Check cache first
  const cached = columnCache.get(tableName);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.columns;
  }

  try {
    const result = await supabase.from(tableName).select("*").limit(1);
    if (result.error) return { error: result.error.message || String(result.error) };
    const row = (result.data || [])[0];
    const columns = row ? Object.keys(row) : [];

    // Cache the result
    columnCache.set(tableName, { columns, expiresAt: Date.now() + COLUMN_CACHE_TTL });
    return columns;
  } catch (e) {
    return { error: e?.message || String(e) };
  }
}

/**
 * Middleware check: ensure Supabase client is configured
 * @param {object} res - Express response object
 * @param {string} signature - API signature for error response
 * @returns {boolean} true if Supabase is configured, false otherwise (response already sent)
 */
function requireSupabase(res, signature) {
  if (!supabase) {
    respondError(res, signature, "supabase_not_configured", 500);
    return false;
  }
  return true;
}

/**
 * Middleware check: validate ingest token from header (X-Ingest-Token) or query (?token=)
 * Fails closed if INGEST_TOKEN env var is not set
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 * @param {string} signature - API signature for error response
 * @returns {boolean} true if token is valid, false otherwise (response already sent)
 */
function requireIngestToken(req, res, signature) {
  if (!INGEST_TOKEN) {
    respondError(res, signature, "ingest_token_not_configured", 403);
    return false;
  }

  const headerToken = (req.get("x-ingest-token") || "").trim();
  const queryToken = (req.query?.token || "").toString().trim();
  const token = headerToken || queryToken;

  if (!token || token !== INGEST_TOKEN) {
    respondError(res, signature, "invalid_ingest_token", 403);
    return false;
  }
  return true;
}

/**
 * Middleware check: validate read token from header (X-Read-Token) or query (?token=)
 * Fail-open: no token configured + auth not required = allow (local dev)
 * Fail-closed: auth required but no token configured = error
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 * @param {string} signature - API signature for error response
 * @returns {boolean} true if access allowed, false otherwise (response already sent)
 */
function requireReadToken(req, res, signature) {
  // Fail-open: no token configured + auth not required = allow (local dev)
  if (!READ_API_TOKEN && !REQUIRE_READ_AUTH) return true;

  // Fail-closed: auth required but no token configured = error
  if (REQUIRE_READ_AUTH && !READ_API_TOKEN) {
    respondError(res, signature, "read_token_not_configured", 500);
    return false;
  }

  // Validate token from header or query param
  const headerToken = (req.get("x-read-token") || "").trim();
  const queryToken = (req.query?.token || "").toString().trim();
  const token = headerToken || queryToken;

  if (!token || token !== READ_API_TOKEN) {
    respondError(res, signature, "invalid_read_token", 401);
    return false;
  }
  return true;
}
async function refreshRollupsSafe() {
  try {
    if (!supabase) return;
    const rpcName = (CFG.RPC_REFRESH_ROLLUPS || "").trim();
    if (!rpcName) return;

    const r = await supabase.rpc(rpcName);
    if (r.error) {
      console.warn("⚠️ rollup refresh warning:", r.error.message || r.error);
    }
  } catch (e) {
    console.warn("⚠️ rollup refresh exception:", e?.message || e);
  }
}

// Very light in-memory limiter for ingest endpoints
const ingestWindowMs = 60_000;
const ingestMax = 60;
const ingestHits = new Map();
function createIngestLimiter(signature) {
  return function ingestLimiter(req, res, next) {
    const key = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown").toString();
    const now = Date.now();
    const rec = ingestHits.get(key) || { n: 0, t: now };

    if (now - rec.t > ingestWindowMs) {
      rec.n = 0;
      rec.t = now;
    }

    rec.n += 1;
    ingestHits.set(key, rec);

    if (rec.n > ingestMax) {
      return respondError(res, signature, "rate_limited", 429);
    }

    next();
  };
}

/**
 * Send formatted exec-weekly response with computed KPIs
 * @param {object} res - Express response object
 * @param {Array} rowsRaw - Array of weekly rollup rows (from exec_weekly or computed fallback)
 * @param {{ week: string|null, store: string|null, state: string|null, city: string|null, sourceRequested: string|null, fallbackUsed: boolean }} filters - Applied filters for metadata
 */
function sendExecWeekly(res, rowsRaw, filters) {
  const rows = rowsRaw ?? [];
  const week_ending = filters.week || (rows[0]?.week_ending ?? null);

  let totalReviews = 0;
  let weightedSum = 0;
  const storeSet = new Set();

  for (const r of rows) {
    const tr = Number(r.total_reviews ?? r.total ?? 0) || 0;
    const ar = Number(r.avg_rating ?? r.avg_stars ?? r.avg ?? 0) || 0;

    totalReviews += tr;
    weightedSum += ar * tr;

    const sid = r.store_id ?? "";
    if (sid) storeSet.add(sid);
  }

  const storesReporting = storeSet.size;

  const avgRatingOverall =
    totalReviews > 0
      ? weightedSum / totalReviews
      : rows.length
        ? rows.reduce((acc, r) => acc + (Number(r.avg_rating ?? r.avg_stars ?? r.avg ?? 0) || 0), 0) /
          rows.length
        : 0;

  res.json({
    ok: true,
    signature: "EXEC_WEEKLY_V1",
    build: BUILD,
    last_updated: nowIso(),
    records: rows.length,
    week_ending,
    kpis: {
      total_reviews: totalReviews,
      stores_reporting: storesReporting,
      avg_stars_overall: Number(avgRatingOverall.toFixed(2)),
    },
    meta: {
      filtersApplied: {
        week: filters.week ?? null,
        store: filters.store ?? null,
        state: filters.state ?? null,
        city: filters.city ?? null,
        sourceRequested: filters.sourceRequested ?? null,
      },
      fallbackUsed: Boolean(filters.fallbackUsed),
      counts: {
        weeklyRows: rows.length,
      },
    },
    rows,
  });
}

// ----- Basic health -----
app.get("/health", (_req, res) => res.send("ok"));
app.get("/api/health", (_req, res) =>
  res.json({ ok: true, signature: "HEALTH_V1", build: BUILD, last_updated: nowIso() })
);

// ----- Debug -----
app.get("/api/debug", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "DEBUG_V1")) return;
    if (!requireSupabase(res, "DEBUG_V1")) return;

    const filters = parseMarketFilters(req);
    const normalizedState = filters.state?.name || filters.state?.code ? filters.state : null;
    const reviewsColumns = await inferColumns(CFG.REVIEWS);
    const execWeeklyColumns = await inferColumns(CFG.EXEC_WEEKLY);

    let storesQuery = supabase.from(CFG.STORES).select("store_id", { count: "exact", head: true });
    storesQuery = applyMarketFilters(storesQuery, filters);
    const stores = await storesQuery;
    if (stores.error) throw stores.error;

    const storeIds = [];
    if (filters.state || filters.city) {
      const storeIdQuery = applyMarketFilters(
        supabase.from(CFG.STORES).select("store_id").limit(5000),
        filters
      );
      const storeIdResult = await storeIdQuery;
      if (storeIdResult.error) throw storeIdResult.error;
      storeIds.push(...(storeIdResult.data || []).map((r) => r.store_id).filter(Boolean));
    }

    let reviewsQuery = supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true });
    if (storeIds.length) reviewsQuery = reviewsQuery.in("store_id", storeIds);
    const reviews = await reviewsQuery;
    if (reviews.error) throw reviews.error;

    let execWeeklyQuery = supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending")
      .order("week_ending", { ascending: false })
      .limit(5000);
    execWeeklyQuery = applyMarketFilters(execWeeklyQuery, filters);
    const execWeekly = await execWeeklyQuery;
    if (execWeekly.error) throw execWeekly.error;

    const ingestEvents = await supabase
      .from(CFG.INGEST_EVENTS)
      .select("id", { count: "exact", head: true });

    const execRows = execWeekly.data ?? [];

    const weeks = [...new Set(execRows.map((r) => r.week_ending).filter(Boolean))];

    res.json({
      ok: true,
      signature: "DEBUG_V1",
      build: BUILD,
      last_updated: nowIso(),
      storesCount: stores.count ?? null,
      execWeeklyRowsCount: execRows.length,
      reviewsCount: reviews.count ?? null,
      ingestEventsCount: ingestEvents.count ?? null,
      apifyReviewsDatasetId: APIFY_REVIEWS_DATASET_ID,
      apifyReviewsLastIngestAt: LAST_APIFY_REVIEWS_INGEST_AT,
      apifyTokenLoaded: !!APIFY_TOKEN,
      liveOnlyDefaults: { reviewsDefaultSource: "apify", metricsDefaultSource: "apify" },
      weeks,
      inferredColumns: {
        reviews: reviewsColumns,
        exec_weekly: execWeeklyColumns,
      },
      meta: {
        filtersApplied: {
          state: filters.state?.raw ?? null,
          city: filters.city ?? null,
          normalizedState,
        },
        counts: {
          stores: stores.count ?? null,
          weeklyRows: execRows.length,
          reviews: reviews.count ?? null,
        },
        apifyReviews: {
          datasetId: APIFY_REVIEWS_DATASET_ID,
          lastIngestAt: LAST_APIFY_REVIEWS_INGEST_AT,
        },
      },
    });
  } catch (e) {
    console.error("❌ /api/debug error:", e);
    res.status(500).json({
      ok: false,
      signature: "DEBUG_V1",
      build: BUILD,
      last_updated: new Date().toISOString(),
      error: "server_error",
    });
  }
});

// ---------- /api/reviews ----------
app.get("/api/reviews", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "REVIEWS_V1")) return;
    if (!requireSupabase(res, "REVIEWS_V1")) return;

    const limitRaw = req.query.limit;
    if (limitRaw != null && String(limitRaw).trim() !== "") {
      const limitNum = Number(limitRaw);
      if (!Number.isFinite(limitNum) || limitNum <= 0) {
        return respondError(res, "REVIEWS_V1", "invalid_limit", 400);
      }
    }
    const limit = parseLimit(req.query.limit ?? 60, 60, 200);

    // accept BOTH store_id and store
    const store_id = (req.query.store_id || req.query.store || "").toString().trim() || null;

    if (store_id && !isUuid(store_id)) {
      return respondError(res, "REVIEWS_V1", "invalid_store_id", 400);
    }

    const sourceRaw = (req.query.source ?? "").toString().trim();
    const sourceNorm = sourceRaw.toLowerCase();
    const reviewsColumns = await inferColumns(CFG.REVIEWS);
    const hasSourceColumn = Array.isArray(reviewsColumns) && reviewsColumns.includes("source");
    const hasSourceFilterParam = Boolean(sourceRaw) && sourceNorm !== "all";
    const effectiveSource = hasSourceFilterParam ? sourceRaw : (sourceNorm === "all" ? null : "apify");
    const wantsSourceFilter = Boolean(effectiveSource) && hasSourceColumn;
    const cityFilter = (req.query.city || "").toString().trim();
    const stateFilter = (req.query.state || "").toString().trim();

    const baseFields = ["store_id", "reviewer_name", "rating", "review_text", "review_date", "url"];
    if (hasSourceColumn) baseFields.splice(1, 0, "source");

    const runQuery = async (storeIdOverride) => {
      let q = supabase.from(CFG.REVIEWS).select(baseFields.join(", ")).order("review_date", {
        ascending: false,
      });
      if (storeIdOverride) q = q.eq("store_id", storeIdOverride);
      if (cityFilter) q = q.ilike("city", `%${cityFilter}%`);
      if (stateFilter) q = q.ilike("state", `%${stateFilter}%`);
      q = applySourceFilter(q, effectiveSource, hasSourceColumn);
      return q.limit(limit);
    };

    let data = [];
    let storeIdResolvedTo = null;

    if (store_id) {
      const first = await runQuery(store_id);
      if (first.error) throw first.error;
      data = first.data ?? [];

      if (!data.length && isUuid(store_id)) {
        const byExternal = await supabase
          .from(CFG.STORES)
          .select("id")
          .eq("store_id", store_id)
          .limit(1);
        if (byExternal.error) throw byExternal.error;
        const resolvedExternalId = (byExternal.data || [])[0]?.id || null;
        if (resolvedExternalId) {
          const second = await runQuery(resolvedExternalId);
          if (second.error) throw second.error;
          data = second.data ?? [];
          if (data.length) storeIdResolvedTo = resolvedExternalId;
        }
      }

      if (data.length && !storeIdResolvedTo) storeIdResolvedTo = store_id;
    } else {
      const first = await runQuery(null);
      if (first.error) throw first.error;
      data = first.data ?? [];
    }

    res.json({
      ok: true,
      signature: "REVIEWS_V1",
      build: BUILD,
      last_updated: nowIso(),
      records: (data ?? []).length,
      filters: {
        limit,
        store_id: store_id ?? null,
        source: effectiveSource ?? null,
        state: stateFilter || null,
        city: cityFilter || null,
        store_id_resolved_to: storeIdResolvedTo,
      },
      rows: data ?? [],
    });
  } catch (e) {
    console.error("❌ /api/reviews error:", e);
    respondError(res, "REVIEWS_V1", "server_error", 500);
  }
});

// ---------- /api/exec-weekly helpers ----------

/**
 * Parse and validate query parameters for /api/exec-weekly
 * @returns {{ weekRaw, store, stateRaw, normalizedState, cityFilter, sourceRequested, wantsSourceFilter, limit } | { error, errorCode, status }}
 */
function parseExecWeeklyParams(req) {
  const weekRaw = (req.query.week ?? "").toString().trim() || null;
  const store = (req.query.store || req.query.store_id || "").toString().trim() || null;
  const stateRaw = (req.query.state ?? "").toString().trim() || null;
  const normalizedState = stateRaw ? normalizeState(stateRaw) : null;
  const cityFilter = (req.query.city ?? "").toString().trim() || null;
  const sourceRaw = (req.query.source ?? "").toString().trim();
  const sourceNorm = sourceRaw.toLowerCase();
  const hasSourceFilterParam = Boolean(sourceRaw) && sourceNorm !== "all";
  const effectiveSource = hasSourceFilterParam ? sourceRaw : (sourceNorm === "all" ? null : "apify");
  const wantsSourceFilter = Boolean(effectiveSource);
  const sourceRequested = wantsSourceFilter ? effectiveSource : null;

  if (weekRaw && !isWeekString(weekRaw)) {
    return { error: "invalid_week_format", errorCode: "invalid_week_format", status: 400 };
  }
  if (store && !isUuid(store)) {
    return { error: "invalid_store_id", errorCode: "invalid_store_id", status: 400 };
  }

  const limitRaw = req.query.limit;
  if (limitRaw != null && String(limitRaw).trim() !== "") {
    const limitNum = Number(limitRaw);
    if (!Number.isFinite(limitNum) || limitNum <= 0) {
      return { error: "invalid_limit", errorCode: "invalid_limit", status: 400 };
    }
  }
  const limit = parseLimit(req.query.limit ?? 5000, 5000, 5000);

  return { weekRaw, store, stateRaw, normalizedState, cityFilter, sourceRequested, wantsSourceFilter, limit };
}

/**
 * Compute week ending date from a review_date string (returns Monday of that week)
 */
function computeWeekEnding(dateStr) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  const day = d.getUTCDay(); // 0=Sun,1=Mon
  const offset = (1 - day + 7) % 7;
  d.setUTCDate(d.getUTCDate() + offset);
  return d.toISOString().slice(0, 10);
}

/**
 * Resolve the week to query for exec-weekly (latest week from exec_weekly or reviews)
 */
async function resolveWeekForExecWeekly({ store, normalizedState, storeIds, sourceRequested, hasSourceColumn, execWeeklyHasSource }) {
  let execWeekLatest = null;
  let reviewWeekLatest = null;

  // Try exec_weekly first
  let wq = supabase.from(CFG.EXEC_WEEKLY).select("week_ending");
  if (store) wq = wq.eq("store_id", store);
  wq = applyMarketFilters(wq, { state: normalizedState, city: null });
  wq = applySourceFilter(wq, sourceRequested, execWeeklyHasSource);
  wq = wq.order("week_ending", { ascending: false });
  const wres = await wq.limit(1);
  if (wres.error) throw wres.error;
  execWeekLatest = wres.data?.[0]?.week_ending ?? null;

  // Try reviews if we have store context
  if (store || (Array.isArray(storeIds) && storeIds.length)) {
    let rq = supabase
      .from(CFG.REVIEWS)
      .select("review_date")
      .not("review_date", "is", null)
      .order("review_date", { ascending: false });
    if (store) rq = rq.eq("store_id", store);
    else if (storeIds.length) rq = rq.in("store_id", storeIds);
    rq = applySourceFilter(rq, sourceRequested, hasSourceColumn);
    const rres = await rq.limit(1);
    if (rres.error) throw rres.error;
    const latestDate = rres.data?.[0]?.review_date ?? null;
    reviewWeekLatest = latestDate ? computeWeekEnding(latestDate) : null;
  }

  return execWeekLatest || reviewWeekLatest || null;
}

/**
 * Query exec_weekly table with filters
 */
async function queryExecWeeklyTable({ week, store, normalizedState, sourceRequested, execWeeklyHasSource, limit }) {
  let q = supabase
    .from(CFG.EXEC_WEEKLY)
    .select("week_ending,store_id,store_name,state,avg_rating,total_reviews,low_reviews,high_reviews")
    .eq("week_ending", week);
  if (store) q = q.eq("store_id", store);
  q = applyMarketFilters(q, { state: normalizedState, city: null });
  q = applySourceFilter(q, sourceRequested, execWeeklyHasSource);
  const execRes = await q.limit(limit);
  if (execRes.error) throw execRes.error;
  return execRes.data ?? [];
}

/**
 * Compute fallback rollups from reviews table when exec_weekly has no data
 */
async function computeFallbackFromReviews({ weekStart, weekEnd, store, storeIds, sourceRequested, hasSourceColumn, storeMap }) {
  // Query reviews in date range
  let rq = supabase.from(CFG.REVIEWS).select("store_id,rating,review_date");
  rq = rq.gte("review_date", weekStart).lte("review_date", weekEnd);
  if (store) rq = rq.eq("store_id", store);
  else if (Array.isArray(storeIds) && storeIds.length) rq = rq.in("store_id", storeIds);
  rq = applySourceFilter(rq, sourceRequested, hasSourceColumn);
  const reviewsRes = await rq;
  if (reviewsRes.error) throw reviewsRes.error;
  const reviewsRows = reviewsRes.data ?? [];

  // Fetch store info if needed
  const localStoreMap = new Map(storeMap);
  if (!localStoreMap.size && reviewsRows.length) {
    const reviewStoreIds = [...new Set(reviewsRows.map((r) => r.store_id).filter(Boolean))];
    if (reviewStoreIds.length) {
      const sres = await supabase
        .from(CFG.STORES)
        .select("id,name,city,state")
        .in("id", reviewStoreIds);
      if (sres.error) throw sres.error;
      for (const s of sres.data || []) {
        if (s.id) localStoreMap.set(s.id, s);
      }
    }
  }

  // Compute rollups
  const rollups = new Map();
  for (const r of reviewsRows) {
    if (!r.store_id) continue;
    if (!rollups.has(r.store_id)) {
      const storeInfo = localStoreMap.get(r.store_id) || {};
      rollups.set(r.store_id, {
        week_ending: weekEnd,
        store_id: r.store_id,
        store_name: storeInfo.name || null,
        state: storeInfo.state || null,
        avg_rating: 0,
        total_reviews: 0,
        low_reviews: 0,
        high_reviews: 0,
        _rating_sum: 0,
        _rating_count: 0,
      });
    }
    const rec = rollups.get(r.store_id);
    rec.total_reviews += 1;
    const ratingNum = Number(r.rating);
    if (Number.isFinite(ratingNum)) {
      rec._rating_sum += ratingNum;
      rec._rating_count += 1;
      if (ratingNum <= 2) rec.low_reviews += 1;
      if (ratingNum >= 4) rec.high_reviews += 1;
    }
  }

  // Finalize rollups
  const fallbackRows = [];
  for (const rec of rollups.values()) {
    rec.avg_rating = rec._rating_count > 0 ? Number((rec._rating_sum / rec._rating_count).toFixed(2)) : 0;
    delete rec._rating_sum;
    delete rec._rating_count;
    fallbackRows.push(rec);
  }
  fallbackRows.sort((a, b) => {
    if (b.total_reviews !== a.total_reviews) return b.total_reviews - a.total_reviews;
    return b.avg_rating - a.avg_rating;
  });

  return fallbackRows;
}

// ---------- /api/exec-weekly ----------
app.get("/api/exec-weekly", async (req, res) => {
  const respondExecWeeklyError = (where, err) => {
    console.error("❌ /api/exec-weekly error:", where, err);
    respondError(res, "EXEC_WEEKLY_V1", "server_error", 500);
  };

  try {
    if (!requireReadToken(req, res, "EXEC_WEEKLY_V1")) return;
    if (!requireSupabase(res, "EXEC_WEEKLY_V1")) return;

    // Parse and validate parameters
    const params = parseExecWeeklyParams(req);
    if (params.error) {
      return respondError(res, "EXEC_WEEKLY_V1", params.errorCode, params.status);
    }
    const { weekRaw, store, stateRaw, normalizedState, cityFilter, sourceRequested, limit } = params;

    // Infer source column availability
    let hasSourceColumn = false;
    let execWeeklyHasSource = false;
    try {
      const reviewsColumns = await inferColumns(CFG.REVIEWS);
      hasSourceColumn = Array.isArray(reviewsColumns) && reviewsColumns.includes("source");
      const execWeeklyColumns = await inferColumns(CFG.EXEC_WEEKLY);
      execWeeklyHasSource = Array.isArray(execWeeklyColumns) && execWeeklyColumns.includes("source");
    } catch (e) {
      return respondExecWeeklyError("execWeekly:columns", e);
    }

    // Resolve store IDs for state filtering
    let storeIds = null;
    const storeMap = new Map();
    if (store) {
      storeIds = [store];
    } else if (normalizedState) {
      try {
        let sq = supabase.from(CFG.STORES).select("id,name,city,state");
        sq = applyMarketFilters(sq, { state: normalizedState, city: null });
        const sres = await sq;
        if (sres.error) throw sres.error;
        const rows = sres.data ?? [];
        storeIds = rows.map((r) => r.id).filter(Boolean);
        for (const s of rows) {
          if (s.id) storeMap.set(s.id, s);
        }
      } catch (e) {
        return respondExecWeeklyError("execWeekly:stores", e);
      }
    }

    // Logging helper
    const logExecWeekly = ({ usedFallback, records, week }) => {
      console.info("exec-weekly", { week: week ?? null, state: stateRaw ?? null, store: store ?? null, usedFallback, records });
    };

    // Resolve week if not provided
    let resolvedWeek = weekRaw;
    if (!resolvedWeek && Array.isArray(storeIds)) {
      try {
        resolvedWeek = await resolveWeekForExecWeekly({ store, normalizedState, storeIds, sourceRequested, hasSourceColumn, execWeeklyHasSource });
      } catch (e) {
        return respondExecWeeklyError("execWeekly:resolveWeek", e);
      }
    }

    // Global week lookup if no store/state filter
    if (!resolvedWeek && storeIds === null) {
      try {
        let wq = supabase.from(CFG.EXEC_WEEKLY).select("week_ending").order("week_ending", { ascending: false }).limit(1);
        wq = applyMarketFilters(wq, { state: normalizedState, city: null });
        wq = applySourceFilter(wq, sourceRequested, execWeeklyHasSource);
        const wres = await wq;
        if (wres.error) throw wres.error;
        resolvedWeek = wres.data?.[0]?.week_ending ?? null;
      } catch (e) {
        return respondExecWeeklyError("execWeekly:resolveWeekGlobal", e);
      }
    }

    const responseFilters = { week: resolvedWeek, store, state: stateRaw, city: cityFilter, sourceRequested };

    // No week resolved - return empty
    if (!resolvedWeek) {
      logExecWeekly({ usedFallback: false, records: 0, week: null });
      return sendExecWeekly(res, [], { ...responseFilters, week: null, fallbackUsed: false });
    }

    // Query exec_weekly table
    let execRows = [];
    try {
      execRows = await queryExecWeeklyTable({ week: resolvedWeek, store, normalizedState, sourceRequested, execWeeklyHasSource, limit });
    } catch (e) {
      return respondExecWeeklyError("execWeekly:execWeeklyQuery", e);
    }

    if (execRows.length) {
      logExecWeekly({ usedFallback: false, records: execRows.length, week: resolvedWeek });
      return sendExecWeekly(res, execRows, { ...responseFilters, fallbackUsed: false });
    }

    // No stores in filtered state - return empty
    if (normalizedState && Array.isArray(storeIds) && storeIds.length === 0) {
      logExecWeekly({ usedFallback: false, records: 0, week: resolvedWeek });
      return sendExecWeekly(res, [], { ...responseFilters, fallbackUsed: false });
    }

    // Compute fallback from reviews
    const endDate = new Date(`${resolvedWeek}T00:00:00Z`);
    const startDate = new Date(endDate.getTime());
    startDate.setUTCDate(startDate.getUTCDate() - 6);
    const weekStart = startDate.toISOString().slice(0, 10);

    let fallbackRows = [];
    try {
      fallbackRows = await computeFallbackFromReviews({ weekStart, weekEnd: resolvedWeek, store, storeIds, sourceRequested, hasSourceColumn, storeMap });
    } catch (e) {
      return respondExecWeeklyError("execWeekly:fallback", e);
    }

    logExecWeekly({ usedFallback: true, records: fallbackRows.length, week: resolvedWeek });
    return sendExecWeekly(res, fallbackRows, { ...responseFilters, fallbackUsed: true });
  } catch (e) {
    return respondExecWeeklyError("execWeekly:unknown", e);
  }
});

// ---------- /api/meta ----------
app.get("/api/meta", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "META_V1")) return;
    if (!requireSupabase(res, "META_V1")) return;

    let data = null;

    const attempt = await supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, source")
      .ilike("source", "%apify%")
      .order("week_ending", { ascending: false })
      .limit(5000);

    if (attempt.error && (attempt.error.message || "").toLowerCase().includes("source")) {
      const retry = await supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending")
        .order("week_ending", { ascending: false })
        .limit(5000);

      if (retry.error) throw retry.error;
      data = retry.data ?? [];
    } else if (attempt.error) {
      throw attempt.error;
    } else {
      data = attempt.data ?? [];
    }

    const weeks = [...new Set((data ?? []).map((r) => r.week_ending).filter(Boolean))];

    res.json({
      ok: true,
      signature: "META_V1",
      build: BUILD,
      last_updated: nowIso(),
      weeks,
      sources: ["apify"],
    });
  } catch (e) {
    console.error("❌ /api/meta error:", e);
    respondError(res, "META_V1", "server_error", 500);
  }
});

// ---------- /api/stores ----------
app.get("/api/stores", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "STORES_V1")) return;
    if (!requireSupabase(res, "STORES_V1")) return;

    const limitRaw = req.query.limit;
    if (limitRaw != null && String(limitRaw).trim() !== "") {
      const limitNum = Number(limitRaw);
      if (!Number.isFinite(limitNum) || limitNum <= 0) {
        return respondError(res, "STORES_V1", "invalid_limit", 400);
      }
    }
    const limit = parseLimit(req.query.limit ?? 500, 500, 2000);
    const filters = parseMarketFilters(req);

    let q = supabase
      .from(CFG.STORES)
      .select("store_id,id,name,address,city,state,created_at", { count: "exact" })
      .order("created_at", { ascending: false })
      .limit(limit);
    q = applyMarketFilters(q, filters);

    const { data, error, count } = await q;
    if (error) throw error;

    const rows = (data ?? []).map((r) => ({
      ...r,
      id: r.store_id || r.id || null,
    }));

    res.json({
      ok: true,
      signature: "STORES_V1",
      build: BUILD,
      last_updated: nowIso(),
      records: count ?? rows.length,
      rows,
      meta: {
        filtersApplied: {
          state: filters.state?.raw ?? null,
          city: filters.city ?? null,
        },
        counts: {
          stores: count ?? rows.length,
        },
      },
    });
  } catch (e) {
    console.error("❌ /api/stores error:", e);
    respondError(res, "STORES_V1", "server_error", 500);
  }
});

// ---------- /api/store/:store_id ----------
app.get("/api/store/:store_id", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "STORE_V1_SHAPE")) return;
    if (!requireSupabase(res, "STORE_V1_SHAPE")) return;

    const store_id = (req.params.store_id || "").toString().trim();
    if (!isUuid(store_id)) {
      return respondError(res, "STORE_V1_SHAPE", "invalid_store_id", 400);
    }

    // PB1_FIX_STORE_ID_V1
    const { data, error } = await supabase
      .from(CFG.STORES)
      .select("*")
      .or(`id.eq.${store_id},store_id.eq.${store_id}`)
      .limit(1);
    if (error) throw error;
    const row = (data || [])[0] || null;

    res.json({
      ok: true,
      signature: "STORE_V1_SHAPE",
      build: BUILD,
      last_updated: nowIso(),
      records: row ? 1 : 0,
              row,
             });

      return;
  } catch (e) {
    console.error("❌ /api/store/:store_id error:", e);
    respondError(res, "STORE_V1_SHAPE", "server_error", 500);
  }
});

// ---------- /api/stores/summary ----------
app.get("/api/stores/summary", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "STORES_SUMMARY_V1")) return;
    if (!requireSupabase(res, "STORES_SUMMARY_V1")) return;

    const attempt = await supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, store_id, avg_rating, total_reviews, state, source")
      .ilike("source", "%apify%")
      .order("week_ending", { ascending: false })
      .limit(5000);

    let rows = null;

    if (attempt.error && (attempt.error.message || "").toLowerCase().includes("source")) {
      const retry = await supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending, store_id, avg_rating, total_reviews, state")
        .order("week_ending", { ascending: false })
        .limit(5000);
      if (retry.error) throw retry.error;
      rows = retry.data ?? [];
    } else if (attempt.error) {
      throw attempt.error;
    } else {
      rows = attempt.data ?? [];
    }

    const latestWeek = rows[0]?.week_ending ?? null;
    const latest = latestWeek ? rows.filter((r) => r.week_ending === latestWeek) : rows;
    const stores = new Set(latest.map((r) => r.store_id).filter(Boolean));

    res.json({
      ok: true,
      signature: "STORES_SUMMARY_V1",
      build: BUILD,
      last_updated: nowIso(),
      latest_week: latestWeek,
      stores_reporting: stores.size,
      records: latest.length,
    });
  } catch (e) {
    console.error("❌ /api/stores/summary error:", e);
    respondError(res, "STORES_SUMMARY_V1", "server_error", 500);
  }
});

// ---------- /api/metrics ----------
app.get("/api/metrics", async (req, res) => {
  const respondMetricsError = (where, err) => {
    console.error("❌ /api/metrics error:", where, err);
    respondError(res, "METRICS_V1", "server_error", 500);
  };

  try {
    if (!requireReadToken(req, res, "METRICS_V1")) return;
    if (!requireSupabase(res, "METRICS_V1")) return;

    const stateRaw = (req.query.state ?? "").toString().trim();
    const cityRaw = (req.query.city ?? "").toString().trim();
    const stateFilter = stateRaw || null;
    const cityFilter = cityRaw || null;
    const hasFilters = Boolean(stateFilter || cityFilter);
    const reviewsColumns = await inferColumns(CFG.REVIEWS);
    const hasSourceColumn = Array.isArray(reviewsColumns) && reviewsColumns.includes("source");
    const sourceRaw = (req.query.source ?? "").toString().trim();
    const sourceNorm = sourceRaw.toLowerCase();
    const hasSourceFilterParam = Boolean(sourceRaw) && sourceNorm !== "all";
    const effectiveSource = hasSourceFilterParam ? sourceRaw : (sourceNorm === "all" ? null : "apify");
    const wantsSourceFilter = Boolean(effectiveSource) && hasSourceColumn;
    const filtersApplied = {
      state: stateFilter,
      city: cityFilter,
      source: wantsSourceFilter ? effectiveSource : null,
    };
    const execWeeklyColumns = await inferColumns(CFG.EXEC_WEEKLY);

    let storesCount = null;
    let storeIds = [];
    try {
      if (hasFilters) {
        let storesQuery = supabase
          .from(CFG.STORES)
          .select("id,state,city", { count: "exact" });
        if (stateFilter) storesQuery = storesQuery.ilike("state", `%${stateFilter}%`);
        if (cityFilter) storesQuery = storesQuery.ilike("city", `%${cityFilter}%`);
        const storesRes = await storesQuery;
        if (storesRes.error) throw storesRes.error;
        storesCount = storesRes.count ?? null;
        storeIds = (storesRes.data ?? []).map((r) => r.id).filter(Boolean);
      } else {
        let storeCountQuery = supabase.from(CFG.STORES).select("store_id", { count: "exact", head: true });
        const storesRes = await storeCountQuery;
        if (storesRes.error) throw storesRes.error;
        storesCount = storesRes.count ?? null;
      }
    } catch (e) {
      return respondMetricsError("metrics:stores", e);
    }

    if (hasFilters && storeIds.length === 0) {
      return res.json({
        ok: true,
        signature: "METRICS_V1",
        build: BUILD,
        last_updated: nowIso(),
        storesCount: 0,
        reviewsCount: 0,
        execWeeklyRowsCount: 0,
        stores: 0,
        reviews: 0,
        exec_weekly_rows: 0,
        meta: {
          filtersApplied,
          storeIdsCount: 0,
          storeIdSample: [],
          inferredColumns: {
            reviews: reviewsColumns,
            exec_weekly: execWeeklyColumns,
          },
          counts: {
            stores: 0,
            reviews: 0,
            weeklyRows: 0,
          },
        },
      });
    }

    let reviewsCount = null;
    try {
      let reviewsQuery = supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true });
      if (storeIds.length) reviewsQuery = reviewsQuery.in("store_id", storeIds);
      reviewsQuery = applySourceFilter(reviewsQuery, effectiveSource, hasSourceColumn);
      const reviews = await reviewsQuery;
      if (reviews.error) throw reviews.error;
      reviewsCount = reviews.count ?? null;
    } catch (e) {
      return respondMetricsError("metrics:reviewsCount", e);
    }

    let execWeeklyRowsCount = null;
    try {
      let execWeeklyQuery = supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending", { count: "exact", head: true });
      if (storeIds.length) execWeeklyQuery = execWeeklyQuery.in("store_id", storeIds);
      const weeks = await execWeeklyQuery;
      if (weeks.error) throw weeks.error;
      execWeeklyRowsCount = weeks.count ?? null;
    } catch (e) {
      return respondMetricsError("metrics:execWeeklyCount", e);
    }

    res.json({
      ok: true,
      signature: "METRICS_V1",
      build: BUILD,
      last_updated: nowIso(),
      storesCount,
      reviewsCount,
      execWeeklyRowsCount,
      stores: storesCount,
      reviews: reviewsCount,
      exec_weekly_rows: execWeeklyRowsCount,
      meta: {
        filtersApplied,
        storeIdsCount: storeIds.length,
        storeIdSample: hasFilters ? storeIds.slice(0, 3) : [],
        inferredColumns: {
          reviews: reviewsColumns,
          exec_weekly: execWeeklyColumns,
        },
        counts: {
          stores: storesCount,
          reviews: reviewsCount,
          weeklyRows: execWeeklyRowsCount,
        },
      },
    });
  } catch (e) {
    return respondMetricsError("metrics:unknown", e);
  }
});

// ---------- /api/ingest/apify-reviews ----------
app.post("/api/ingest/apify-reviews", async (req, res) => {
  try {
    if (!requireSupabase(res, "INGEST_APIFY_REVIEWS_V1")) return;
    if (!APIFY_TOKEN) {
      return respondError(res, "INGEST_APIFY_REVIEWS_V1", "apify_token_not_configured", 500);
    }
    if (!APIFY_REVIEWS_DATASET_ID) {
      return respondError(res, "INGEST_APIFY_REVIEWS_V1", "apify_reviews_dataset_id_not_configured", 500, {
        hint: "Set APIFY_REVIEWS_DATASET_ID environment variable to the dataset containing reviews"
      });
    }

    const datasetId = APIFY_REVIEWS_DATASET_ID;
    const limitRaw = req.body?.limit;
    const limitNum = Number(limitRaw ?? 2000);
    const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.min(Math.floor(limitNum), 2000) : 2000;
    const source = "apify_google_maps";

    const url =
      `https://api.apify.com/v2/datasets/${encodeURIComponent(datasetId)}/items` +
      `?clean=true&format=json&limit=${limit}&token=${encodeURIComponent(APIFY_TOKEN)}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      return respondError(res, "INGEST_APIFY_REVIEWS_V1", "apify_fetch_failed", 502, {
        status: resp.status,
        status_text: resp.statusText,
      });
    }
    const items = await resp.json();
    const rowsRaw = Array.isArray(items) ? items : [];

    const firstItem = rowsRaw[0] || {};
    const firstKeys = Object.keys(firstItem || {});
    const hasPlaceKeys = ["title", "categoryName", "price", "neighborhood", "postalCode"].some((k) =>
      Object.prototype.hasOwnProperty.call(firstItem, k)
    );
    const hasReviewKeys = [
      "rating",
      "stars",
      "starRating",
      "reviewer_name",
      "reviewerName",
      "author",
      "userName",
      "name",
      "text",
      "textTranslated",
      "reviewText",
      "comment",
      "content",
      "date",
      "publishedAt",
      "published_at",
      "publishedAtDate",
      "review_date",
      "review_text",
    ].some((k) => Object.prototype.hasOwnProperty.call(firstItem, k));
    if (hasPlaceKeys && !hasReviewKeys) {
      return res.status(400).json({
        ok: false,
        signature: "INGEST_APIFY_REVIEWS_V1",
        error: "dataset_not_reviews",
        datasetId,
        hint:
          "This dataset looks like locations/places. Run a Google Reviews scraper actor and provide the datasetId containing individual reviews (rating/text/date/author).",
        firstKeys: firstKeys.slice(0, 40),
      });
    }

    const reviewRows = [];
    const sampleMapped = [];
    let skipped = 0;

    const extractReviewDate = (v) => {
      const str = String(v || "");
      const m = str.match(/\d{4}-\d{2}-\d{2}/);
      if (m) return m[0];
      const d = new Date(str);
      if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
      return null;
    };

    for (const item of rowsRaw) {
      const placeId =
        item?.place_id ||
        item?.placeId ||
        item?.location?.place_id ||
        item?.location?.placeId ||
        item?.place?.placeId ||
        null;
      const reviewerName =
        item?.name ||
        item?.reviewerName ||
        item?.reviewer_name ||
        item?.author ||
        item?.userName ||
        null;
      const rating = Number(item?.stars ?? item?.rating ?? item?.starRating ?? null);
      const reviewText =
        item?.text ||
        item?.textTranslated ||
        item?.reviewText ||
        item?.review_text ||
        item?.comment ||
        item?.content ||
        null;
      const reviewDate = extractReviewDate(
        item?.publishedAtDate ||
          item?.publishedAt ||
          item?.published_at ||
          item?.date ||
          item?.publishAt ||
          item?.scrapedAt ||
          null
      );
      const reviewUrl = item?.reviewUrl || item?.review_url || item?.url || item?.link || null;
      const placeName =
        item?.title || item?.place_name || item?.placeName || item?.locationName || null;

      const storeId = placeId
        ? String(placeId)
        : sha1([placeName || "", reviewUrl || ""].join("|"));
      const externalId =
        reviewUrl ||
        item?.reviewId ||
        item?.external_id ||
        item?.id ||
        sha1(
          [
            reviewerName || "",
            Number.isFinite(rating) ? String(rating) : "",
            reviewText || "",
            reviewDate || "",
            storeId || "",
          ].join("|")
        );

      const row = {
        store_id: storeId,
        source,
        reviewer_name: reviewerName,
        rating: Number.isFinite(rating) ? rating : null,
        review_text: reviewText,
        review_date: reviewDate,
        url: reviewUrl,
        external_id: reviewUrl ? sha1(String(reviewUrl)) : String(externalId),
        city: item?.city || item?.location?.city || null,
        state: item?.state || item?.location?.state || null,
      };
      reviewRows.push(row);
      if (sampleMapped.length < 3) {
        sampleMapped.push({
          store_id: storeId,
          placeId: placeId || null,
          city: row.city,
          state: row.state,
          reviewer_name: reviewerName,
          rating: Number.isFinite(rating) ? rating : null,
          review_date: reviewDate,
          url: reviewUrl,
          external_id: row.external_id,
          review_text_preview: (reviewText || "").slice(0, 80),
        });
      }
    }

    skipped = Math.max(rowsRaw.length - reviewRows.length, 0);
    const storeIdSample = [...new Set(reviewRows.map((r) => r.store_id).filter(Boolean))].slice(0, 3);

    if (!reviewRows.length) {
      return res.json({
        ok: true,
        signature: "INGEST_APIFY_REVIEWS_V1",
        datasetId,
        fetched: rowsRaw.length,
        matched: reviewRows.length,
        ingested: 0,
        inserted: 0,
        updated: 0,
        skipped: rowsRaw.length,
        errors: 0,
        store_id_sample: [],
        last_updated: nowIso(),
      });
    }

    console.log("INGEST_MAP_V2 sample:", sampleMapped[0]);

    const externalIds = [...new Set(reviewRows.map((r) => r.external_id).filter(Boolean))];
    let existingCount = 0;
    if (externalIds.length) {
      const existingRes = await supabase.from(CFG.REVIEWS).select("external_id").in("external_id", externalIds);
      if (existingRes.error) throw existingRes.error;
      existingCount = (existingRes.data || []).length;
    }

    const upsertRes = await supabase.from(CFG.REVIEWS).upsert(reviewRows, { onConflict: "external_id" });
    if (upsertRes.error) throw upsertRes.error;
    LAST_APIFY_REVIEWS_INGEST_AT = nowIso();

    return res.json({
      ok: true,
      signature: "INGEST_APIFY_REVIEWS_V1",
      mapping_version: "APIFY_REVIEW_MAP_V2__2026-01-19",
      sample_first_keys: firstKeys.slice(0, 30),
      sample_mapped: sampleMapped,
      datasetId,
      fetched: rowsRaw.length,
      matched: reviewRows.length,
      ingested: reviewRows.length,
      inserted: Math.max(reviewRows.length - existingCount, 0),
      updated: existingCount,
      skipped,
      errors: 0,
      store_id_sample: storeIdSample,
      last_updated: nowIso(),
    });
  } catch (e) {
    console.error("❌ /api/ingest/apify-reviews error:", e);
    return respondError(res, "INGEST_APIFY_REVIEWS_V1", "server_error", 500);
  }
});

// ---------- /api/admin/purge-bad-apify ----------
app.post("/api/admin/purge-bad-apify", async (req, res) => {
  try {
    if (!requireSupabase(res, "ADMIN_PURGE_BAD_APIFY_V1")) return;

    const del = await supabase
      .from(CFG.REVIEWS)
      .delete({ count: "exact" })
      .eq("source", "apify")
      .is("reviewer_name", null)
      .is("review_date", null)
      .ilike("url", "%query_place_id=%");
    if (del.error) throw del.error;

    res.json({
      ok: true,
      signature: "ADMIN_PURGE_BAD_APIFY_V1",
      deleted: del.count ?? 0,
    });
  } catch (e) {
    console.error("❌ /api/admin/purge-bad-apify error:", e);
    respondError(res, "ADMIN_PURGE_BAD_APIFY_V1", "server_error", 500);
  }
});

/*
Manual tests:
- Re-ingest: POST /api/ingest/apify-reviews datasetId AYqfTL4InXM8rHHQh
- Verify reviewer_name + review_date: GET /api/reviews?state=Illinois&city=Chicago&limit=5
- Verify external store_id works: GET /api/reviews?store_id=e3c2ef9a-736a-1b20-4edf-7798d6d56f59&limit=5
- Verify weekly: GET /api/exec-weekly?state=Illinois&city=Chicago&limit=50
*/

// ===============================
// INGEST (LOCKED) ENDPOINTS
// ===============================

// ---------- INGEST (LOCKED): /api/ingest/reviews ----------
app.post("/api/ingest/reviews", createIngestLimiter("INGEST_REVIEWS_V1"), async (req, res) => {
  try {
    if (!requireSupabase(res, "INGEST_REVIEWS_V1")) return;
    if (!requireIngestToken(req, res, "INGEST_REVIEWS_V1")) return;

    const reviews = req.body?.reviews || [];
    if (!Array.isArray(reviews) || !reviews.length) {
      return respondError(res, "INGEST_REVIEWS_V1", "missing_reviews_array", 400);
    }

    const rows = reviews
      .map((r) => {
        const store_id = r.store_id || r.storeId || null;

        // Guard: prevent Postgres uuid errors
        if (!store_id || !isUuid(store_id)) return null;

        const reviewer_name = r.reviewer_name || r.reviewerName || null;
        const review_text = r.review_text || r.reviewText || null;
        const review_date = r.review_date || r.reviewDate || null;

        return {
          store_id,
          source: r.source || "unknown",
          reviewer_name,
          rating: Number(r.rating ?? r.stars ?? 0) || null,
          review_text,
          review_date,
          url: r.url || r.reviewUrl || null,
          external_id:
            r.external_id ||
            r.externalId ||
            sha1(`${store_id}|${review_date}|${reviewer_name}|${review_text}`),
        };
      })
      .filter(Boolean);

    if (!rows.length) {
      return respondError(res, "INGEST_REVIEWS_V1", "no_valid_rows_store_id_must_be_uuid", 400);
    }

    const up = await supabase.from(CFG.REVIEWS).upsert(rows, { onConflict: "external_id" });
    if (up.error) throw up.error;

    await refreshRollupsSafe();

    res.json({
      ok: true,
      signature: "INGEST_REVIEWS_V1",
      build: BUILD,
      last_updated: nowIso(),
      ingested: rows.length,
    });
  } catch (e) {
    console.error("❌ /api/ingest/reviews error:", e);
    respondError(res, "INGEST_REVIEWS_V1", "server_error", 500);
  }
});

// ---------- INGEST (LOCKED): /api/ingest/apify ----------
// Webhook receiver (stores raw payload in ingest_events)
app.post("/api/ingest/apify", createIngestLimiter("INGEST_APIFY_V1"), async (req, res) => {
  try {
    if (!requireSupabase(res, "INGEST_APIFY_V1")) return;
    if (!requireIngestToken(req, res, "INGEST_APIFY_V1")) return;

    const payload = req.body ?? {};
    if (!payload || typeof payload !== "object") {
      return respondError(res, "INGEST_APIFY_V1", "invalid_payload", 400);
    }

    const received_at = new Date().toISOString();

    const ins = await supabase
      .from(CFG.INGEST_EVENTS)
      .insert([{ source: "apify", received_at, payload }])
      .select("id, received_at")
      .single();

    if (ins.error) throw ins.error;

    res.json({
      ok: true,
      signature: "INGEST_APIFY_V1",
      build: BUILD,
      last_updated: received_at,
      ingest_event: ins.data,
    });
  } catch (e) {
    console.error("❌ /api/ingest/apify error:", e);
    respondError(res, "INGEST_APIFY_V1", "server_error", 500);
  }
});

// ---------- INGEST (LOCKED): /api/ingest/apify/pull ----------
// Pull Apify DATASET and upsert into stores_apify (or whatever table you choose)
app.post("/api/ingest/apify/pull", createIngestLimiter("INGEST_APIFY_PULL_V1"), async (req, res) => {
  try {
    if (!requireSupabase(res, "INGEST_APIFY_PULL_V1")) return;
    if (!requireIngestToken(req, res, "INGEST_APIFY_PULL_V1")) return;

    const token = process.env.APIFY_TOKEN;
    const datasetId = process.env.APIFY_DATASET_ID;

    if (!token) return res.status(400).json({ ok: false, error: "Missing APIFY_TOKEN" });
    if (!datasetId) return res.status(400).json({ ok: false, error: "Missing APIFY_DATASET_ID" });

    const limit = Number(req.query.limit || 500);
    const url = `https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json&limit=${limit}&token=${token}`;

    const r = await fetch(url);
    const items = await r.json();

    if (!r.ok) {
      return res.status(r.status).json({ ok: false, error: "Apify fetch failed", status: r.status, body: items });
    }
    if (!Array.isArray(items)) {
      return res.status(500).json({ ok: false, error: "Apify returned non-array", body: items });
    }

    // NOTE: This mapping assumes your Apify items use the key names shown in your earlier curl output:
    // place_id, name, address, street, city, state, postalCode/postal_code, countryCode/country_code, lat/lng, total_score, reviews_count, scraped_at
    const rows = items.map((x) => ({
      source: "apify_google_maps",
      place_id: x.place_id || x.placeId || null,
      name: x.name || x.title || null,
      address: x.address || null,
      street: x.street || null,
      city: x.city || null,
      state: x.state || null,
      postal_code: x.postal_code || x.postalCode || null,
      country_code: x.country_code || x.countryCode || null,
      lat: x.lat ?? x.location?.lat ?? null,
      lng: x.lng ?? x.location?.lng ?? null,
      total_score: x.total_score ?? x.totalScore ?? null,
      reviews_count: x.reviews_count ?? x.reviewsCount ?? null,
      scraped_at: x.scraped_at
        ? new Date(x.scraped_at).toISOString()
        : x.scrapedAt
          ? new Date(x.scrapedAt).toISOString()
          : null,
      raw: x, // remove if your table doesn't have a jsonb column named "raw"
    }));

    const targetTable = "stores_apify"; // change if needed

    const { data, error } = await supabase
      .from(targetTable)
      .upsert(rows, { onConflict: "place_id" })
      .select("place_id");

    if (error) {
      return res.status(500).json({ ok: false, error: error.message, hint: error.hint });
    }

    return res.json({ ok: true, ingested: data?.length || 0, table: targetTable });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// ---------- /api/stores-apify ----------
// MVP: serve directly from "stores" (the table you just populated from Apify)
// Frontend expects rows[].id to exist, so we set id = store_id.
app.get("/api/stores-apify", async (req, res) => {
  try {
    if (!requireReadToken(req, res, "STORES_APIFY_V1")) return;
    if (!requireSupabase(res, "STORES_APIFY_V1")) return;

    const limit = parseLimit(req.query.limit ?? 200, 200, 2000);
    const qtxt = (req.query.q || "").toString().trim();
    const filters = parseMarketFilters(req);

    let q = supabase
      .from(CFG.STORES)
      .select("store_id, name, address, city, state, created_at")
      .order("created_at", { ascending: false })
      .limit(limit);

    q = applyMarketFilters(q, filters);

    if (qtxt) {
      const esc = qtxt.replace(/,/g, " "); // basic safety for .or string
      q = q.or(`name.ilike.%${esc}%,city.ilike.%${esc}%,state.ilike.%${esc}%,address.ilike.%${esc}%`);
    }

    const { data, error } = await q;
    if (error) throw error;

    // Frontend expects `id` field. Provide stable id = store_id.
    const rows = (data ?? []).map((r) => ({
      id: r.store_id,
      place_id: r.store_id,        // keep compatibility if UI expects place_id
      name: r.name,
      address: r.address,
      street: null,                // not available in this table yet
      city: r.city,
      state: r.state,
      postal_code: null,
      country_code: "US",
      lat: null,
      lng: null,
      total_score: null,
      reviews_count: null,
      scraped_at: r.created_at
    }));

    res.json({
      ok: true,
      signature: "STORES_APIFY_V1",
      build: BUILD,
      last_updated: nowIso(),
      records: rows.length,
      rows,
    });
  } catch (e) {
    console.error("❌ /api/stores-apify error:", e);
    respondError(res, "STORES_APIFY_V1", "server_error", 500);
  }
});

// ---------- start ----------
app.listen(PORT, () => {
  console.log(`pb1 backend running on http://localhost:${PORT}`);
});

// VERIFY (local):
// curl -s "http://localhost:10000/api/reviews?limit=3" | python3 -m json.tool
// curl -s "http://localhost:10000/api/metrics?state=Illinois&city=Chicago" | python3 -m json.tool | head -n 60
// curl -s "http://localhost:10000/api/reviews?limit=3&source=seed" | python3 -m json.tool
// curl -s "http://localhost:10000/api/debug" | python3 -m json.tool | head -n 80
