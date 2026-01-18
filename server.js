// server.js (PB1) — COPY/PASTE ENTIRE FILE
// ESM module (requires: "type":"module" in package.json)

import "dotenv/config";
import express from "express";
import cors from "cors";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

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

// required for ingest protection
const INGEST_TOKEN = (process.env.INGEST_TOKEN || "").trim();
console.log("🔐 INGEST_TOKEN loaded?", INGEST_TOKEN ? "YES" : "NO", "len=", INGEST_TOKEN.length);

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
    allowedHeaders: ["Content-Type", "X-Ingest-Token"],
    maxAge: 86400,
  })
);
app.use(express.json({ limit: "2mb" }));

// Prevent caching on API routes (avoid 304/body issues)
app.use("/api", function (_req, res, next) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

// ----- Helpers -----
const ALLOWED_SOURCES = ["apify"];

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
function enforceApifySourceParam(req, res, signature) {
  const raw = (req.query.source ?? "").toString().trim();
  const normalized = norm(raw);
  if (raw && normalized !== "apify") {
    respondError(res, signature, "source_not_allowed", 400, { allowed: ALLOWED_SOURCES });
    return null;
  }
  return "apify";
}
function applyApifySourceFilter(q) {
  return q.ilike("source", "%apify%");
}
function isWeekString(v) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(v || ""));
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

function requireSupabase(res, signature) {
  if (!supabase) {
    respondError(res, signature, "supabase_not_configured", 500);
    return false;
  }
  return true;
}
function requireIngestToken(req, res, signature) {
  // Fail closed if you forgot to set it (safer)
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
    rows,
  });
}

// ----- Basic health -----
app.get("/health", (_req, res) => res.send("ok"));
app.get("/api/health", (_req, res) =>
  res.json({ ok: true, signature: "HEALTH_V1", build: BUILD, last_updated: nowIso() })
);

// ----- Debug -----
app.get("/api/debug", async (_req, res) => {
  try {
    if (!requireSupabase(res, "DEBUG_V1")) return;

    const [stores, reviews, ingestEvents, execWeekly] = await Promise.all([
      supabase.from(CFG.STORES).select("id", { count: "exact", head: true }),
      supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true }),
      supabase.from(CFG.INGEST_EVENTS).select("id", { count: "exact", head: true }),
      supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending, source")
        .order("week_ending", { ascending: false })
        .limit(5000),
    ]);

    let execRows = execWeekly.data ?? [];
    if (execWeekly.error && (execWeekly.error.message || "").toLowerCase().includes("source")) {
      const retry = await supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending")
        .order("week_ending", { ascending: false })
        .limit(5000);
      if (retry.error) throw retry.error;
      execRows = retry.data ?? [];
    }

    const weeks = [...new Set(execRows.map((r) => r.week_ending).filter(Boolean))];
    const sources = [...new Set(execRows.map((r) => r.source).filter(Boolean))].filter(isApifySource);

    res.json({
      ok: true,
      signature: "DEBUG_V1",
      build: BUILD,
      last_updated: nowIso(),
      storesCount: stores.count ?? null,
      execWeeklyRowsCount: execRows.length,
      reviewsCount: reviews.count ?? null,
      ingestEventsCount: ingestEvents.count ?? null,
      sources: sources.length ? sources : ["apify"],
      weeks,
    });
  } catch (e) {
    console.error("❌ /api/debug error:", e);
    respondError(res, "DEBUG_V1", "server_error", 500);
  }
});

// ---------- /api/reviews ----------
app.get("/api/reviews", async (req, res) => {
  try {
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

    if (!enforceApifySourceParam(req, res, "REVIEWS_V1")) return;

    let q = supabase
      .from(CFG.REVIEWS)
      .select("store_id, source, reviewer_name, rating, review_text, review_date, url")
      .order("review_date", { ascending: false });

    if (store_id) q = q.eq("store_id", store_id);
    q = applyApifySourceFilter(q);

    let { data, error } = await q.limit(limit);
    if (error && (error.message || "").toLowerCase().includes("source")) {
      let retry = supabase
        .from(CFG.REVIEWS)
        .select("store_id, reviewer_name, rating, review_text, review_date, url")
        .order("review_date", { ascending: false });
      if (store_id) retry = retry.eq("store_id", store_id);
      const retryResult = await retry.limit(limit);
      if (retryResult.error) throw retryResult.error;
      data = retryResult.data ?? [];
    } else if (error) {
      throw error;
    }

    res.json({
      ok: true,
      signature: "REVIEWS_V1",
      build: BUILD,
      last_updated: nowIso(),
      records: (data ?? []).length,
      filters: { limit, store_id: store_id ?? null, source: "apify" },
      rows: data ?? [],
    });
  } catch (e) {
    console.error("❌ /api/reviews error:", e);
    respondError(res, "REVIEWS_V1", "server_error", 500);
  }
});

// ---------- /api/exec-weekly ----------
app.get("/api/exec-weekly", async (req, res) => {
  try {
    if (!requireSupabase(res, "EXEC_WEEKLY_V1")) return;

    const week = norm(req.query.week) || null;
    const store = (req.query.store || req.query.store_id || "").toString().trim() || null;
    const region = norm(req.query.region) || null;

    if (week && !isWeekString(week)) {
      return respondError(res, "EXEC_WEEKLY_V1", "invalid_week_format", 400);
    }
    if (store && !isUuid(store)) {
      return respondError(res, "EXEC_WEEKLY_V1", "invalid_store_id", 400);
    }

    if (!enforceApifySourceParam(req, res, "EXEC_WEEKLY_V1")) return;

    const limitRaw = req.query.limit;
    if (limitRaw != null && String(limitRaw).trim() !== "") {
      const limitNum = Number(limitRaw);
      if (!Number.isFinite(limitNum) || limitNum <= 0) {
        return respondError(res, "EXEC_WEEKLY_V1", "invalid_limit", 400);
      }
    }
    const limit = parseLimit(req.query.limit ?? 5000, 5000, 5000);

    const MIN_STORES = Number(process.env.MIN_STORES_FOR_LATEST_WEEK || 2);

    const buildWeekQuery = (w) => {
      let q = supabase.from(CFG.EXEC_WEEKLY).select("*").eq("week_ending", w);
      if (store) q = q.eq("store_id", store);
      if (region) q = q.eq("state", region.toUpperCase());
      q = applyApifySourceFilter(q);
      return q.order("week_ending", { ascending: false });
    };

    // If user explicitly asked for a week, fetch it (no fallback)
    if (week) {
      const { data, error } = await buildWeekQuery(week).limit(limit);
      if (error && (error.message || "").toLowerCase().includes("source")) {
        let q2 = supabase.from(CFG.EXEC_WEEKLY).select("*").eq("week_ending", week);
        if (store) q2 = q2.eq("store_id", store);
        if (region) q2 = q2.eq("state", region.toUpperCase());
        const retry = await q2.order("week_ending", { ascending: false }).limit(limit);
        if (retry.error) throw retry.error;
        return sendExecWeekly(res, retry.data ?? [], { week, store, region, source: "apify" });
      }
      if (error) throw error;
      return sendExecWeekly(res, data ?? [], { week, store, region, source: "apify" });
    }

    // No week -> find candidate weeks newest->oldest
    let weeks = [];
    let attemptWeeks = supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, source")
      .order("week_ending", { ascending: false })
      .limit(5000);
    attemptWeeks = applyApifySourceFilter(attemptWeeks);
    const attemptWeeksResult = await attemptWeeks;

    if (attemptWeeksResult.error && (attemptWeeksResult.error.message || "").toLowerCase().includes("source")) {
      const retryWeeks = await supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending")
        .order("week_ending", { ascending: false })
        .limit(5000);
      if (retryWeeks.error) throw retryWeeks.error;
      weeks = [...new Set((retryWeeks.data ?? []).map((r) => r.week_ending).filter(Boolean))];
    } else if (attemptWeeksResult.error) {
      throw attemptWeeksResult.error;
    } else {
      weeks = [...new Set((attemptWeeksResult.data ?? []).map((r) => r.week_ending).filter(Boolean))];
    }

    if (!weeks.length) {
      return sendExecWeekly(res, [], { week: null, store, region, source: "apify" });
    }

    // Choose effective week: first week with >= MIN_STORES rows (after filters)
    let effectiveWeek = null;
    for (const w of weeks) {
      const { data, error } = await buildWeekQuery(w).limit(limit);
      if (error && (error.message || "").toLowerCase().includes("source")) {
        let q2 = supabase.from(CFG.EXEC_WEEKLY).select("*").eq("week_ending", w);
        if (store) q2 = q2.eq("store_id", store);
        if (region) q2 = q2.eq("state", region.toUpperCase());
        const retry = await q2.order("week_ending", { ascending: false }).limit(limit);
        if (retry.error) throw retry.error;
        const rows = retry.data ?? [];
        if (rows.length >= MIN_STORES) {
          effectiveWeek = w;
          break;
        }
        continue;
      }
      if (error) throw error;
      const rows = data ?? [];
      if (rows.length >= MIN_STORES) {
        effectiveWeek = w;
        break;
      }
    }
    if (!effectiveWeek) effectiveWeek = weeks[0] || null;

    if (!effectiveWeek) {
      return sendExecWeekly(res, [], { week: null, store, region, source: "apify" });
    }

    const { data: finalRows, error: finalErr } = await buildWeekQuery(effectiveWeek).limit(limit);
    if (finalErr && (finalErr.message || "").toLowerCase().includes("source")) {
      let q2 = supabase.from(CFG.EXEC_WEEKLY).select("*").eq("week_ending", effectiveWeek);
      if (store) q2 = q2.eq("store_id", store);
      if (region) q2 = q2.eq("state", region.toUpperCase());
      const retry = await q2.order("week_ending", { ascending: false }).limit(limit);
      if (retry.error) throw retry.error;
      return sendExecWeekly(res, retry.data ?? [], { week: effectiveWeek, store, region, source: "apify" });
    }
    if (finalErr) throw finalErr;

    return sendExecWeekly(res, finalRows ?? [], { week: effectiveWeek, store, region, source: "apify" });
  } catch (e) {
    console.error("❌ /api/exec-weekly error FULL:", e);
    respondError(res, "EXEC_WEEKLY_V1", "server_error", 500);
  }
});

// ---------- /api/meta ----------
app.get("/api/meta", async (_req, res) => {
  try {
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
    if (!requireSupabase(res, "STORES_V1")) return;

    const limitRaw = req.query.limit;
    if (limitRaw != null && String(limitRaw).trim() !== "") {
      const limitNum = Number(limitRaw);
      if (!Number.isFinite(limitNum) || limitNum <= 0) {
        return respondError(res, "STORES_V1", "invalid_limit", 400);
      }
    }
    const limit = parseLimit(req.query.limit ?? 500, 500, 2000);
    const week = norm(req.query.week) || null;

    if (week && !isWeekString(week)) {
      return respondError(res, "STORES_V1", "invalid_week_format", 400);
    }

    if (!enforceApifySourceParam(req, res, "STORES_V1")) return;
        // --- MVP FAST PATH: return stores directly (Apify Chicago locations) ---
    // Use: /api/stores?mode=stores or /api/stores?mode=stores&city=Chicago
    if (norm(req.query.mode) === "stores") {
      const city = norm(req.query.city) || "Chicago";
      const state = norm(req.query.state) || null;
      const limit = parseLimit(req.query.limit ?? 500, 500, 2000);

      let q = supabase
        .from(CFG.STORES) // ensure CFG.STORES points to your "stores" table
        .select("store_id,id,name,address,city,state,created_at")
        .order("name", { ascending: true })
        .limit(limit);

      if (city) q = q.ilike("city", city);
      if (state) q = q.ilike("state", state);

      // Filter out demo/test rows just in case
      q = q.not("name", "ilike", "%demo%").not("name", "ilike", "%test%");

      const { data, error } = await q;
      if (error) throw error;

      return res.json({
        ok: true,
        signature: "STORES_V1",
        build: BUILD,
        last_updated: nowIso(),
        mode: "stores",
        records: (data || []).length,
        rows: data || [],
      });
    }


    const MIN_STORES = Number(process.env.MIN_STORES_FOR_LATEST_WEEK || 2);
    let effectiveWeek = week;
    let weeks = [];

    let apifyStoreIds = null;

    let weekQuery = supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, source")
      .order("week_ending", { ascending: false })
      .limit(5000);
    weekQuery = applyApifySourceFilter(weekQuery);
    const weekResult = await weekQuery;

    if (weekResult.error && (weekResult.error.message || "").toLowerCase().includes("source")) {
      const retry = await supabase
        .from(CFG.EXEC_WEEKLY)
        .select("week_ending")
        .order("week_ending", { ascending: false })
        .limit(5000);
      if (retry.error) throw retry.error;
      weeks = [...new Set((retry.data ?? []).map((r) => r.week_ending).filter(Boolean))];
    } else if (weekResult.error) {
      throw weekResult.error;
    } else {
      weeks = [...new Set((weekResult.data ?? []).map((r) => r.week_ending).filter(Boolean))];
    }

    if (!weeks.length) {
      return res.json({
        ok: true,
        signature: "STORES_V1",
        build: BUILD,
        last_updated: nowIso(),
        records: 0,
        rows: [],
      });
    }

    if (!effectiveWeek) {
      for (const w of weeks) {
        let q = supabase
          .from(CFG.EXEC_WEEKLY)
          .select("store_id, source")
          .eq("week_ending", w)
          .limit(5000);
        q = applyApifySourceFilter(q);
        const { data, error } = await q;

        if (error && (error.message || "").toLowerCase().includes("source")) {
          const retry = await supabase
            .from(CFG.EXEC_WEEKLY)
            .select("store_id")
            .eq("week_ending", w)
            .limit(5000);
          if (retry.error) throw retry.error;
          if ((retry.data ?? []).length >= MIN_STORES) {
            effectiveWeek = w;
            break;
          }
        } else if (error) {
          throw error;
        } else if ((data ?? []).length >= MIN_STORES) {
          effectiveWeek = w;
          break;
        }
      }
      if (!effectiveWeek) effectiveWeek = weeks[0] || null;
    }

    let apifyStores = supabase
      .from(CFG.EXEC_WEEKLY)
      .select("store_id, source, week_ending")
      .order("week_ending", { ascending: false })
      .limit(5000);

    if (effectiveWeek) apifyStores = apifyStores.eq("week_ending", effectiveWeek);
    apifyStores = applyApifySourceFilter(apifyStores);

    const apifyResult = await apifyStores;

    if (apifyResult.error && (apifyResult.error.message || "").toLowerCase().includes("source")) {
      let fallbackStores = supabase
        .from(CFG.EXEC_WEEKLY)
        .select("store_id, week_ending")
        .order("week_ending", { ascending: false })
        .limit(5000);
      if (effectiveWeek) fallbackStores = fallbackStores.eq("week_ending", effectiveWeek);
      const fallbackResult = await fallbackStores;
      if (fallbackResult.error) throw fallbackResult.error;
      apifyStoreIds = [...new Set((fallbackResult.data || []).map((r) => r.store_id).filter(Boolean))];
    } else if (apifyResult.error) {
      throw apifyResult.error;
    } else {
      apifyStoreIds = [...new Set((apifyResult.data || []).map((r) => r.store_id).filter(Boolean))];
    }

    if (Array.isArray(apifyStoreIds) && apifyStoreIds.length === 0) {
      return res.json({
        ok: true,
        signature: "STORES_V1",
        build: BUILD,
        last_updated: nowIso(),
        records: 0,
        rows: [],
      });
    }

    let q = supabase
      .from(CFG.STORES)
      .select("*", { count: "exact" })
      .order("created_at", { ascending: false })
      .limit(limit);

    if (Array.isArray(apifyStoreIds) && apifyStoreIds.length > 0) {
      q = q.in("store_id", apifyStoreIds);
    }

    const { data, error, count } = await q;
    if (error) throw error;

    res.json({
      ok: true,
      signature: "STORES_V1",
      build: BUILD,
      last_updated: nowIso(),
      records: count ?? (data?.length ?? 0),
      rows: data ?? [],
    });
  } catch (e) {
    console.error("❌ /api/stores error:", e);
    respondError(res, "STORES_V1", "server_error", 500);
  }
});

// ---------- /api/store/:store_id ----------
app.get("/api/store/:store_id", async (req, res) => {
  try {
    if (!requireSupabase(res, "STORE_V1_SHAPE")) return;

    const store_id = (req.params.store_id || "").toString().trim();
    if (!isUuid(store_id)) {
      return respondError(res, "STORE_V1_SHAPE", "invalid_store_id", 400);
    }

   const { data, error } = await supabase.from(CFG.STORES).select("*").eq("store_id", store_id).single();


    res.json({
      ok: true,
      signature: "STORE_V1_SHAPE",
      build: BUILD,
      last_updated: nowIso(),
      records: data ? 1 : 0,
      row: data,
    });
  } catch (e) {
    console.error("❌ /api/store/:store_id error:", e);
    respondError(res, "STORE_V1_SHAPE", "server_error", 500);
  }
});

// ---------- /api/stores/summary ----------
app.get("/api/stores/summary", async (_req, res) => {
  try {
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
app.get("/api/metrics", async (_req, res) => {
  try {
    if (!requireSupabase(res, "METRICS_V1")) return;

    const [stores, reviews, weeks] = await Promise.all([
      supabase.from(CFG.STORES).select("id", { count: "exact", head: true }),
      supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true }),
      supabase.from(CFG.EXEC_WEEKLY).select("week_ending", { count: "exact", head: true }),
    ]);

    res.json({
      ok: true,
      signature: "METRICS_V1",
      build: BUILD,
      last_updated: nowIso(),
      stores: stores.count ?? null,
      reviews: reviews.count ?? null,
      exec_weekly_rows: weeks.count ?? null,
    });
  } catch (e) {
    console.error("❌ /api/metrics error:", e);
    respondError(res, "METRICS_V1", "server_error", 500);
  }
});

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
    if (!requireSupabase(res, "STORES_APIFY_V1")) return;

    const limit = parseLimit(req.query.limit ?? 200, 200, 2000);
    const qtxt = (req.query.q || "").toString().trim();
    const city = (req.query.city || "Chicago").toString().trim(); // MVP default
    const state = (req.query.state || "").toString().trim();

    let q = supabase
      .from(CFG.STORES)
      .select("store_id, name, address, city, state, created_at")
      .order("created_at", { ascending: false })
      .limit(limit);

    // MVP: lock to Chicago unless caller overrides with city=""
    if (city) q = q.ilike("city", city);
    if (state) q = q.ilike("state", state);

    if (qtxt) {
      const esc = qtxt.replace(/,/g, " "); // basic safety for .or string
      q = q.or(`name.ilike.%${esc}%,city.ilike.%${esc}%,state.ilike.%${esc}%,address.ilike.%${esc}%`);
    }

    // Filter demo/test just in case
    q = q.not("name", "ilike", "%demo%").not("name", "ilike", "%test%");

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
