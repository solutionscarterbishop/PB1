// server.js (ESM)
// PB1 backend: Supabase-powered API for stores, reviews, exec-weekly rollups, and ingest.

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;

// CHANGE THIS STRING ANY TIME YOU WANT TO CONFIRM YOU'RE RUNNING THE NEW FILE
const BUILD = "PB1_SERVER_BUILD__2026-01-16__EXEC_WEEKLY_V3";


app.set("etag", false);
app.use(cors());
app.use(express.json({ limit: "2mb" }));

// Prevent caching on API routes (avoids 304/body weirdness)
app.use("/api", function (_req, res, next) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

// ---------- Supabase ----------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
        auth: { persistSession: false },
      })
    : null;

// ---------- Config ----------
const CFG = {
  STORES_UNIQUE: "stores_unique",
  REVIEWS: "reviews",
  EXEC_WEEKLY: "exec_weekly",

  RPC_REFRESH: "refresh_exec_weekly_rollup",
  INGEST_EVENTS: "ingest_events",
};

function requireSupabase(res) {
  if (!supabase) {
    res.status(500).json({
      ok: false,
      error: "Supabase client not initialized (missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY)",
    });
    return false;
  }
  return true;
}

function norm(v) {
  return (v ?? "").toString().trim();
}
function toInt(v, def = 0) {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}
function isUuid(s) {
  return (
    typeof s === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s)
  );
}

function normalizeStoreRow(r) {
  const store_name =
    r.store_name ??
    r.place_name ??
    r.name ??
    r.location_name ??
    r.store ??
    r.title ??
    "Unknown Store";

  const store_id = r.store_id ?? r.id ?? r.uuid ?? r.store_uuid ?? null;

  return {
    ...r,
    store_id,
    store_name,
    city: r.city ?? null,
    state: r.state ?? null,
    address: r.address ?? null,
  };
}

function normalizeReviewRow(r) {
  return {
    id: r.id ?? null,
    store_id: r.store_id ?? null,
    source: r.source ?? null,
    reviewer_name: r.reviewer_name ?? null,
    rating: r.rating ?? null,
    review_text: r.review_text ?? null,
    review_date: r.review_date ?? null,
    url: r.url ?? null,
    created_at: r.created_at ?? null,
    external_id: r.external_id ?? null,
  };
}

async function refreshRollupsSafe() {
  if (!supabase) throw new Error("Supabase client not initialized");
  const r = await supabase.rpc(CFG.RPC_REFRESH);
  if (!r.error) return true;
  throw r.error;
}

// ---------- Security helper ----------
function requireIngestToken(req, res) {
  const expected = process.env.INGEST_TOKEN;
  if (!expected) {
    res.status(500).json({
      ok: false,
      error: "Server missing INGEST_TOKEN in env (set it locally + on Render).",
    });
    return false;
  }
  const got = req.headers["x-ingest-token"];
  if (!got || got !== expected) {
    res.status(401).json({ ok: false, error: "Unauthorized (bad ingest token)" });
    return false;
  }
  return true;
}

// ---------- simple in-memory rate limiter ----------
function makeRateLimiter({ windowMs, max, keyFn }) {
  const hits = new Map(); // key -> { count, resetAt }
  return function rateLimit(req, res, next) {
    const now = Date.now();
    const key = (keyFn ? keyFn(req) : req.ip) || "unknown";
    const cur = hits.get(key);

    if (!cur || now > cur.resetAt) {
      hits.set(key, { count: 1, resetAt: now + windowMs });
      res.setHeader("X-RateLimit-Limit", String(max));
      res.setHeader("X-RateLimit-Remaining", String(max - 1));
      res.setHeader("X-RateLimit-Reset", String(now + windowMs));
      return next();
    }

    cur.count += 1;
    res.setHeader("X-RateLimit-Limit", String(max));
    res.setHeader("X-RateLimit-Remaining", String(Math.max(0, max - cur.count)));
    res.setHeader("X-RateLimit-Reset", String(cur.resetAt));

    if (cur.count > max) {
      return res.status(429).json({
        ok: false,
        error: "Rate limit exceeded",
        retry_after_ms: Math.max(0, cur.resetAt - now),
      });
    }

    next();
  };
}

// Limit per token (best for Zapier/Apify) — not per IP
const ingestLimiter = makeRateLimiter({
  windowMs: 60_000, // 1 minute window
  max: 30,          // 30 requests/minute per token
  keyFn: (req) => req.headers["x-ingest-token"] || req.ip,
});

// ---------- health ----------
app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/api/health", (_req, res) => res.json({ ok: true }));

// ---------- build stamp (so we can verify we're running the right file) ----------
app.get("/api/build", (_req, res) => {
  res.json({ ok: true, build: BUILD, now: new Date().toISOString() });
});

// ---------- debug ----------
app.get("/api/debug", async (_req, res) => {
  try {
    if (!supabase) {
      return res.json({ ok: false, error: "missing env vars", now: new Date().toISOString(), build: BUILD });
    }

    const s = await supabase.from("stores").select("id", { count: "exact", head: true });
    const r = await supabase.from(CFG.REVIEWS).select("id", { count: "exact", head: true });
    const ie = await supabase.from(CFG.INGEST_EVENTS).select("id", { count: "exact", head: true });

    res.json({
      ok: true,
      build: BUILD,
      storesCount: s.error ? null : s.count ?? null,
      reviewsCount: r.error ? null : r.count ?? null,
      ingestEventsCount: ie.error ? null : ie.count ?? null,
      now: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "debug_error", build: BUILD });
  }
});

// ---------- /api/stores ----------
app.get("/api/stores", async (_req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const { data, error } = await supabase.from(CFG.STORES_UNIQUE).select("*").limit(10000);
    if (error) throw error;

    const rows = (data ?? []).map(normalizeStoreRow);

    res.json({
      ok: true,
      signature: "STORES_V1_SHAPE",
      build: BUILD,
      last_updated: new Date().toISOString(),
      records: rows.length,
      rows,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "stores_error", build: BUILD });
  }
});

// ---------- /api/reviews ----------
app.get("/api/reviews", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const store_id = norm(req.query.store_id);
    const source = norm(req.query.source) || "all";
    const limit = clamp(toInt(req.query.limit, 50), 1, 200);

    let q = supabase.from(CFG.REVIEWS).select("*").order("review_date", { ascending: false }).limit(limit);
    if (store_id) q = q.eq("store_id", store_id);
    if (source !== "all") q = q.eq("source", source);

    const { data, error } = await q;
    if (error) throw error;

    res.json({
      ok: true,
      signature: "REVIEWS_V1_SHAPE",
      build: BUILD,
      last_updated: new Date().toISOString(),
      records: data?.length ?? 0,
      filters: { store_id: store_id || null, source, limit },
      rows: (data ?? []).map(normalizeReviewRow),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "reviews_error", build: BUILD });
  }
});

// ---------- /api/exec-weekly ----------
app.get("/api/exec-weekly", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const week = norm(req.query.week) || null;
    const store = norm(req.query.store) || null;
    const source = norm(req.query.source) || null;

    let q = supabase.from(CFG.EXEC_WEEKLY).select("*").order("week_ending", { ascending: false });

    if (week) q = q.eq("week_ending", week);
    if (store) q = q.eq("store_id", store);
    if (source) q = q.eq("source", source);

    const { data, error } = await q.limit(2000);

    if (error && source && (error.message || "").toLowerCase().includes("source")) {
      let q2 = supabase.from(CFG.EXEC_WEEKLY).select("*").order("week_ending", { ascending: false });
      if (week) q2 = q2.eq("week_ending", week);
      if (store) q2 = q2.eq("store_id", store);

      const retry = await q2.limit(2000);
      if (retry.error) throw retry.error;

      return sendExecWeekly(res, retry.data ?? [], { week, store, source: null });
    }

    if (error) throw error;

    sendExecWeekly(res, data ?? [], { week, store, source });
  } catch (e) {
    console.error("❌ /api/exec-weekly error:", e?.message || e);
    res.status(500).json({ ok: false, error: e?.message || "exec_weekly_error", build: BUILD });
  }
});

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
  const avgRatingOverall = totalReviews > 0 ? weightedSum / totalReviews : 0;

  res.json({
    ok: true,
    signature: "EXEC_WEEKLY_V2_SHAPE",
    build: BUILD,
    last_updated: new Date().toISOString(),
    records: rows.length,
    week_ending,
    filters: {
      week: filters.week ?? null,
      store: filters.store ?? null,
      source: filters.source ?? null,
    },
    kpis: {
      total_reviews: totalReviews,
      stores_reporting: storesReporting,
      avg_stars_overall: Number(avgRatingOverall.toFixed(2)),
    },
    rows,
  });
}

// ---------- /api/meta ----------
app.get("/api/meta", async (_req, res) => {
  try {
    if (!requireSupabase(res)) return;

    let data = null;

    const attempt = await supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, source")
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
    const sources = [...new Set((data ?? []).map((r) => r.source).filter(Boolean))];

    res.json({
      ok: true,
      signature: "META_V1_SHAPE",
      build: BUILD,
      last_updated: new Date().toISOString(),
      weeks,
      sources: sources.length ? sources : ["google"],
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "meta_error", build: BUILD });
  }
});

// ---------- INGEST (LOCKED): /api/ingest/reviews ----------
app.post("/api/ingest/reviews", ingestLimiter, async (req, res) => {

  try {
    if (!requireSupabase(res)) return;
    if (!requireIngestToken(req, res)) return;

    const reviews = req.body?.reviews || [];
    if (!Array.isArray(reviews) || !reviews.length) {
      return res.status(400).json({ ok: false, error: "Missing reviews[]", build: BUILD });
    }

    const rows = reviews
      .map((r) => {
        const store_id = r.store_id || r.storeId || null;

        // Guard: prevent Postgres uuid errors
        if (!store_id || !isUuid(store_id)) return null;

        return {
          store_id,
          source: r.source || "unknown",
          reviewer_name: r.reviewer_name || r.reviewerName || null,
          rating: Number(r.rating ?? r.stars ?? 0) || null,
          review_text: r.review_text || r.reviewText || null,
          review_date: r.review_date || r.reviewDate || null,
          url: r.url || r.reviewUrl || null,
          external_id:
            r.external_id ||
            r.externalId ||
            sha1(
              `${store_id}|${r.review_date || r.reviewDate}|${r.reviewer_name || r.reviewerName}|${r.review_text || r.reviewText}`
            ),
        };
      })
      .filter(Boolean);

    if (!rows.length) {
      return res.status(400).json({
        ok: false,
        error: "No valid rows to ingest (store_id must be a UUID).",
        build: BUILD,
      });
    }

    const up = await supabase.from(CFG.REVIEWS).upsert(rows, { onConflict: "external_id" });
    if (up.error) throw up.error;

    await refreshRollupsSafe();

    res.json({
      ok: true,
      signature: "INGEST_REVIEWS_V1",
      build: BUILD,
      last_updated: new Date().toISOString(),
      ingested: rows.length,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "ingest_error", build: BUILD });
  }
});

// ---------- INGEST (LOCKED): /api/ingest/apify ----------
app.post("/api/ingest/apify", ingestLimiter, async (req, res) => {

  try {
    if (!requireSupabase(res)) return;
    if (!requireIngestToken(req, res)) return;

    const payload = req.body ?? {};
    if (!payload || typeof payload !== "object") {
      return res.status(400).json({ ok: false, error: "Invalid payload", build: BUILD });
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
    console.error("INGEST_APIFY_ERROR:", e?.message || e);
    res.status(500).json({ ok: false, error: e?.message || "ingest_apify_error", build: BUILD });
  }
});

// ---------- start ----------
const server = app.listen(PORT, () => {
  console.log(`pb1 backend running on http://localhost:${PORT}`);
  console.log("✅ SERVER BUILD:", BUILD);
});

process.on("SIGINT", () => {
  console.log("\nShutting down...");
  server.close(() => process.exit(0));
});
