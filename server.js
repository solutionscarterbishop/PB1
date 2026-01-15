// server.js (ESM)
// PB1 backend: Supabase-powered API for stores, reviews, exec-weekly rollups, and ingest.
//
// Run locally:
//   npm install
//   npm start
//
// Env required:
//   SUPABASE_URL=...
//   SUPABASE_SERVICE_ROLE_KEY=...

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;

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
  // NOTE: stores_unique is NOT required anymore for /api/stores (we derive from exec_weekly)
  STORES_UNIQUE: "stores_unique",
  REVIEWS: "reviews",
  EXEC_WEEKLY: "exec_weekly",
  RPC_REFRESH: "refresh_exec_weekly_rollup",
};

// ---------- Helpers ----------
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

// ---------- health ----------
app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/api/health", (_req, res) => res.json({ ok: true }));

// ---------- debug (safe) ----------
app.get("/api/debug/env", (_req, res) => {
  res.json({
    ok: true,
    has_SUPABASE_URL: !!process.env.SUPABASE_URL,
    has_SUPABASE_SERVICE_ROLE_KEY: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
    node_env: process.env.NODE_ENV || null,
    now: new Date().toISOString(),
  });
});

// Optional: lightweight connectivity check against known tables/views
app.get("/api/debug", async (_req, res) => {
  try {
    if (!supabase) {
      return res.json({ ok: false, error: "missing env vars", now: new Date().toISOString() });
    }

    const ew = await supabase.from(CFG.EXEC_WEEKLY).select("store_id", { count: "exact", head: true });
    const rv = await supabase.from(CFG.REVIEWS).select("id", { count: "exact", head: true });

    res.json({
      ok: true,
      exec_weekly_count: ew.error ? null : ew.count ?? null,
      reviews_count: rv.error ? null : rv.count ?? null,
      exec_weekly_error: ew.error ? ew.error.message : null,
      reviews_error: rv.error ? rv.error.message : null,
      now: new Date().toISOString(),
      server_build: "EXEC_WEEKLY_SHAPE_V2",
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "debug_error" });
  }
});

// ---------- /api/stores ----------
// FIX: derive stores from exec_weekly so we don't depend on stores_unique existing.
app.get("/api/stores", async (_req, res) => {
  try {
    if (!supabase) {
      return res.status(500).json({ ok: false, error: "SUPABASE_NOT_CONFIGURED" });
    }

    const limit = 6000;
    const { data, error } = await supabase
      .from(CFG.EXEC_WEEKLY)
      .select("store_id, place_name, city, state")
      .limit(limit);

    let rowsSource = data ?? [];
    let nameField = "place_name";

    if (error) {
      const fallback = await supabase.from(CFG.EXEC_WEEKLY).select("*").limit(limit);
      if (fallback.error) {
        return res.status(500).json({
          ok: false,
          error: "stores_query_failed",
          details: {
            primary: error.message,
            fallback: fallback.error.message,
          },
        });
      }

      rowsSource = fallback.data ?? [];
      nameField = pickStoreNameField(rowsSource);
    }

    const map = new Map();
    for (const r of rowsSource) {
      if (!r?.store_id) continue;
      if (!map.has(r.store_id)) {
        const name = (r?.[nameField] ?? "").toString().trim() || "Unknown";
        const city = r.city || "";
        const state = r.state || "";
        const label = `${name} — ${city}, ${state}`.replace(" — , ", " — ").replace(/,\s*$/, "");

        map.set(r.store_id, {
          id: r.store_id,
          name,
          city,
          state,
          label,
        });
      }
    }

    const rows = Array.from(map.values()).sort((a, b) => a.label.localeCompare(b.label));

    return res.json({
      ok: true,
      signature: "STORES_FROM_EXEC_WEEKLY_V1",
      records: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
});

function pickStoreNameField(rows) {
  const candidates = ["place_name", "store_name", "name", "location_name"];
  for (const r of rows || []) {
    if (!r || typeof r !== "object") continue;
    for (const c of candidates) {
      const v = r?.[c];
      if (v !== null && v !== undefined && String(v).trim()) return c;
    }
  }
  return "place_name";
}

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
      last_updated: new Date().toISOString(),
      records: data?.length ?? 0,
      filters: { store_id: store_id || null, source, limit },
      rows: (data ?? []).map(normalizeReviewRow),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "reviews_error" });
  }
});

// ---------- /api/exec-weekly (RICH SHAPE) ----------
app.get("/api/exec-weekly", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const week = norm(req.query.week) || null; // YYYY-MM-DD
    const store = norm(req.query.store) || null; // store_id
    const source = norm(req.query.source) || null;

    let q = supabase.from(CFG.EXEC_WEEKLY).select("*").order("week_ending", { ascending: false });

    if (week) q = q.eq("week_ending", week);
    if (store) q = q.eq("store_id", store);
    if (source) q = q.eq("source", source);

    const { data, error } = await q.limit(2000);

    // If "source" column doesn't exist in exec_weekly, retry without source filter
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
    res.status(500).json({ ok: false, error: e?.message || "exec_weekly_error" });
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
      last_updated: new Date().toISOString(),
      weeks,
      sources: sources.length ? sources : ["google"],
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "meta_error" });
  }
});

// ---------- INGEST ----------
app.post("/api/ingest/reviews", async (req, res) => {
  try {
    if (!requireSupabase(res)) return;

    const reviews = req.body?.reviews || [];
    if (!Array.isArray(reviews) || !reviews.length) {
      return res.status(400).json({ ok: false, error: "Missing reviews[]" });
    }

    const rows = reviews
      .map((r) => ({
        store_id: r.store_id || r.storeId || null,
        source: r.source || "unknown",
        reviewer_name: r.reviewer_name || r.reviewerName || null,
        rating: Number(r.rating ?? r.stars ?? 0) || null,
        review_text: r.review_text || r.reviewText || null,
        review_date: r.review_date || r.reviewDate || null,
        url: r.url || r.reviewUrl || null,
        external_id:
          r.external_id ||
          r.externalId ||
          sha1(`${r.store_id || r.storeId}|${r.review_date || r.reviewDate}|${r.reviewer_name || r.reviewerName}|${r.review_text || r.reviewText}`),
      }))
      .filter((r) => r.external_id);

    const up = await supabase.from(CFG.REVIEWS).upsert(rows, { onConflict: "external_id" });
    if (up.error) throw up.error;

    await refreshRollupsSafe();

    res.json({
      ok: true,
      signature: "INGEST_REVIEWS_V1",
      last_updated: new Date().toISOString(),
      ingested: rows.length,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "ingest_error" });
  }
});

// ---------- start ----------
const server = app.listen(PORT, () => {
  console.log(`pb1 backend running on http://localhost:${PORT}`);
  console.log("✅ SERVER BUILD: EXEC_WEEKLY_SHAPE_V2");
});

// Make CTRL+C safer / avoid orphan server
process.on("SIGINT", () => {
  console.log("\nShutting down...");
  server.close(() => process.exit(0));
});
