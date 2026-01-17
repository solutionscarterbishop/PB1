// server.js (PB1) — copy/paste entire file

import "dotenv/config";
import express from "express";
import cors from "cors";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

const app = express();
const PORT = process.env.PORT || 10000;

// ----- Fingerprints (so you always know what's running) -----
const BUILD =
  process.env.BUILD ||
  "PB1_SERVER_BUILD__2026-01-16__HOTFIX_EXEC_WEEKLY"; // you can override via env anytime

console.log("✅ SERVER FILE:", new URL(import.meta.url).pathname);
console.log("✅ SERVER BUILD:", BUILD);
console.log("✅ SERVER START:", new Date().toISOString());

// ----- Config -----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// optional, but recommended for ingest protection
const INGEST_TOKEN = process.env.INGEST_TOKEN || "";

// Table/View names (match your Supabase schema)
const CFG = {
  EXEC_WEEKLY: process.env.EXEC_WEEKLY_TABLE || "exec_weekly",
  STORES: process.env.STORES_TABLE || "stores",
  REVIEWS: process.env.REVIEWS_TABLE || "reviews",
  INGEST_EVENTS: process.env.INGEST_EVENTS_TABLE || "ingest_events",
  // optional RPC to refresh rollups (only runs if present)
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
app.use(cors());
app.use(express.json({ limit: "2mb" }));

// Prevent caching on API routes (avoid 304/body issues)
app.use("/api", function (_req, res, next) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

// ----- Helpers -----
function norm(x) {
  return (x ?? "").toString().trim().toLowerCase();
}

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function isUuid(v) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(v || "")
  );
}

function requireSupabase(res) {
  if (!supabase) {
    res.status(500).json({ ok: false, error: "supabase_not_configured", build: BUILD });
    return false;
  }
  return true;
}

function requireIngestToken(req, res) {
  if (!INGEST_TOKEN) {
    // If you forget to set it, we fail closed (safer)
    res.status(403).json({ ok: false, error: "ingest_token_not_configured", build: BUILD });
    return false;
  }
  const token =
    (req.headers["x-ingest-token"] || req.headers["x-ingest-token".toLowerCase()] || "").toString().trim() ||
    (req.query?.token || "").toString().trim();

  if (!token || token !== INGEST_TOKEN) {
    res.status(403).json({ ok: false, error: "invalid_ingest_token", build: BUILD });
    return false;
  }
  return true;
}

async function refreshRollupsSafe() {
  // Optional: only works if you have an RPC named refresh_rollups (or set RPC_REFRESH_ROLLUPS)
  try {
    if (!supabase) return;
    const rpcName = CFG.RPC_REFRESH_ROLLUPS;
    if (!rpcName) return;
    const r = await supabase.rpc(rpcName);
    if (r.error) {
      // Don't fail ingest on rollup refresh errors; just log
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
function ingestLimiter(req, res, next) {
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
    return res.status(429).json({ ok: false, error: "rate_limited", build: BUILD });
  }
  next();
}

// ----- Basic health -----
app.get("/health", (_req, res) => res.send("ok"));
app.get("/api/health", (_req, res) =>
  res.json({ ok: true, signature: "HEALTH_V1", build: BUILD, now: new Date().toISOString() })
);

app.get("/api/debug", async (_req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const [stores, reviews, ingestEvents] = await Promise.all([
      supabase.from(CFG.STORES).select("id", { count: "exact", head: true }),
      supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true }),
      supabase.from(CFG.INGEST_EVENTS).select("id", { count: "exact", head: true }),
    ]);

    res.json({
      ok: true,
      build: BUILD,
      now: new Date().toISOString(),
      storesCount: stores.count ?? null,
      reviewsCount: reviews.count ?? null,
      ingestEventsCount: ingestEvents.count ?? null,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "debug_error", build: BUILD });
  }
});

// ---------- /api/exec-weekly ----------
app.get("/api/exec-weekly", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const week = norm(req.query.week) || null;

    // DO NOT norm() UUIDs or region codes; keep raw (trimmed) so matches don't break.
    const store = (req.query.store || "").toString().trim() || null;
    const region = (req.query.region || "").toString().trim() || null;

    // keep source behavior as-is
    const source = norm(req.query.source) || null;

    let q = supabase.from(CFG.EXEC_WEEKLY).select("*").order("week_ending", { ascending: false });

    if (week) q = q.eq("week_ending", week);

    // Executive scope precedence: store > region > all
    if (store) {
      q = q.eq("store_id", store);
    } else if (region) {
      // Case-insensitive match because your data has "FL" and "Fl"
      q = q.ilike("state", region);
    }

    if (source) q = q.eq("source", source);

    const { data, error } = await q.limit(2000);

    // Backward-compatible fallback if "source" column isn't present
    if (error && source && (error.message || "").toLowerCase().includes("source")) {
      let q2 = supabase.from(CFG.EXEC_WEEKLY).select("*").order("week_ending", { ascending: false });

      if (week) q2 = q2.eq("week_ending", week);

      if (store) q2 = q2.eq("store_id", store);
      else if (region) q2 = q2.ilike("state", region);

      const retry = await q2.limit(2000);
      if (retry.error) throw retry.error;

      return sendExecWeekly(res, retry.data ?? [], { week, store, region, source: null });
    }

    if (error) throw error;

    sendExecWeekly(res, data ?? [], { week, store, region, source });
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
      region: filters.region ?? null,
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

// ---------- /api/stores ----------
app.get("/api/stores", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const limit = Math.min(Number(req.query.limit ?? 500) || 500, 2000);

    const { data, error, count } = await supabase
      .from(CFG.STORES)
      .select("*", { count: "exact" })
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;

    res.json({
      ok: true,
      signature: "STORES_V1_SHAPE",
      build: BUILD,
      last_updated: new Date().toISOString(),
      records: count ?? (data?.length ?? 0),
      rows: data ?? [],
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "stores_error", build: BUILD });
  }
});

// ---------- /api/store/:store_id ----------
app.get("/api/store/:store_id", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const store_id = (req.params.store_id || "").toString().trim();
    if (!isUuid(store_id)) {
      return res.status(400).json({ ok: false, error: "invalid_store_id", build: BUILD });
    }

    const { data, error } = await supabase.from(CFG.STORES).select("*").eq("id", store_id).single();
    if (error) throw error;

    res.json({ ok: true, signature: "STORE_V1_SHAPE", build: BUILD, row: data });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "store_error", build: BUILD });
  }
});

// ---------- /api/stores/summary ----------
app.get("/api/stores/summary", async (_req, res) => {
  try {
    if (!requireSupabase(res)) return;

    // Uses exec_weekly to compute high-level summary quickly
    const { data, error } = await supabase
      .from(CFG.EXEC_WEEKLY)
      .select("week_ending, store_id, avg_rating, total_reviews, state")
      .order("week_ending", { ascending: false })
      .limit(5000);

    if (error) throw error;

    const rows = data ?? [];
    const latestWeek = rows[0]?.week_ending ?? null;

    const latest = latestWeek ? rows.filter((r) => r.week_ending === latestWeek) : rows;
    const stores = new Set(latest.map((r) => r.store_id).filter(Boolean));

    res.json({
      ok: true,
      signature: "STORES_SUMMARY_V1",
      build: BUILD,
      last_updated: new Date().toISOString(),
      latest_week: latestWeek,
      stores_reporting: stores.size,
      records: latest.length,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "stores_summary_error", build: BUILD });
  }
});

// ---------- /api/metrics ----------
app.get("/api/metrics", async (_req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const [stores, reviews, weeks] = await Promise.all([
      supabase.from(CFG.STORES).select("id", { count: "exact", head: true }),
      supabase.from(CFG.REVIEWS).select("external_id", { count: "exact", head: true }),
      supabase.from(CFG.EXEC_WEEKLY).select("week_ending", { count: "exact", head: true }),
    ]);

    res.json({
      ok: true,
      signature: "METRICS_V1",
      build: BUILD,
      last_updated: new Date().toISOString(),
      stores: stores.count ?? null,
      reviews: reviews.count ?? null,
      exec_weekly_rows: weeks.count ?? null,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "metrics_error", build: BUILD });
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
              `${store_id}|${r.review_date || r.reviewDate}|${r.reviewer_name || r.reviewerName}|${
                r.review_text || r.reviewText
              }`
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
app.listen(PORT, () => {
  console.log(`pb1 backend running on http://localhost:${PORT}`);
});
