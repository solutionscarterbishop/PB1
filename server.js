console.log("✅ SERVER FILE:", new URL(import.meta.url).pathname);
console.log("✅ PID:", process.pid);

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;

app.set("etag", false);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("⚠️ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
}

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    : null;

app.use(cors());
app.use(express.json());

// Prevent caching on API routes (avoid 304/body issues)
app.use("/api", function (_req, res, next) {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate"
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

// Health endpoints
app.get("/health", function (_req, res) {
  res.json({ ok: true });
});
app.get("/api/health", function (_req, res) {
  res.json({ ok: true });
});

function requireSupabase(res) {
  if (!supabase) {
    res.status(500).json({
      error:
        "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env",
    });
    return false;
  }
  return true;
}

/**
 * /api/exec-weekly
 * MUST match frontend expectations:
 * {
 *   summary: {...},
 *   stores: [...],
 *   weekly: [...]
 * }
 */
app.get("/api/exec-weekly", async function (_req, res) {
  try {
    if (!requireSupabase(res)) return;

    // Pull weekly rollup rows from your view
    const weeklyQ = await supabase
      .from("exec_weekly")
      .select("*")
      .order("week_ending", { ascending: false });

    if (weeklyQ.error) return res.status(500).json({ error: weeklyQ.error.message });

    const rows = weeklyQ.data || [];

    // Build stores list from rows (guarantees Stores table populates)
    const storeMap = new Map();
    for (const r of rows) {
      const k = String(r.store_id);
      if (!storeMap.has(k)) {
        storeMap.set(k, {
          store_id: r.store_id,
          place_name: r.place_name,
          address: "",
          city: "",
          state: r.state || "",
        });
      }
    }
    const stores = Array.from(storeMap.values());

    // Compute total_reviews_delta per store (week-over-week)
    const byStore = new Map();
    for (const r of rows) {
      const sid = String(r.store_id);
      if (!byStore.has(sid)) byStore.set(sid, []);
      byStore.get(sid).push(r);
    }
    for (const arr of byStore.values()) {
      arr.sort(function (a, b) {
        return String(a.week_ending).localeCompare(String(b.week_ending));
      });
    }

    // Transform rows -> weekly with EXACT keys frontend expects
    const weekly = [];
    for (const entry of byStore.entries()) {
      const arr = entry[1];
      let prevTotal = null;

      for (const r of arr) {
        const total = Number(r.total_reviews || 0);
        const totalDelta = prevTotal === null ? null : total - prevTotal;
        prevTotal = total;

        weekly.push({
          week_ending: r.week_ending,
          store_id: r.store_id,
          place_name: r.place_name,
          city: "", // not available yet
          state: r.state || "",

          total_reviews: r.total_reviews == null ? null : r.total_reviews,
          avg_stars: r.avg_rating == null ? null : r.avg_rating,

          positive_reviews: null,
          negative_reviews: null,

          avg_stars_delta: r.rating_change == null ? null : r.rating_change,
          total_reviews_delta: totalDelta,
        });
      }
    }

    // newest first
    weekly.sort(function (a, b) {
      return String(b.week_ending).localeCompare(String(a.week_ending));
    });

    // Summary KPIs from latest week only
    const latestWeekEnding = weekly.length ? weekly[0].week_ending : null;
    const latestWeekRows = latestWeekEnding
      ? weekly.filter(function (r) {
          return r.week_ending === latestWeekEnding;
        })
      : [];

    const storeSet = new Set();
    for (const r of latestWeekRows) storeSet.add(String(r.store_id));

    let total_reviews = 0;
    for (const r of latestWeekRows) total_reviews += Number(r.total_reviews) || 0;

    let avg_stars_overall = null;
    if (latestWeekRows.length) {
      let sum = 0;
      let count = 0;
      for (const r of latestWeekRows) {
        const x = Number(r.avg_stars);
        if (Number.isFinite(x)) {
          sum += x;
          count += 1;
        }
      }
      avg_stars_overall = count ? sum / count : null;
    }

    const summary = {
      week_ending: latestWeekEnding,
      stores_reporting: storeSet.size,
      total_reviews: total_reviews,
      avg_stars_overall: avg_stars_overall,
      positive_reviews: null,
      negative_reviews: null,
    };

    return res.json({
      __signature: "EXEC_WEEKLY_V2_SHAPE",
      summary: summary,
      stores: stores,
      weekly: weekly,
      rows: rows, // debug / safe to keep for now
    });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

/**
 * Stores list (optional helper endpoint)
 */
app.get("/api/stores", async function (_req, res) {
  try {
    if (!requireSupabase(res)) return;

    const q = await supabase
      .from("stores")
      .select("store_id, place_name, address, city, state")
      .order("place_name", { ascending: true });

    if (q.error) return res.status(500).json({ error: q.error.message });

    const rows = q.data || [];
    return res.json({ rows: rows, stores: rows });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

/**
 * Store detail + last 100 reviews (optional helper endpoint)
 */
app.get("/api/store/:store_id", async function (req, res) {
  try {
    if (!requireSupabase(res)) return;

    const store_id = req.params.store_id;

    const storeQ = await supabase
      .from("stores")
      .select("*")
      .eq("store_id", store_id)
      .maybeSingle();

    if (storeQ.error) return res.status(500).json({ error: storeQ.error.message });

    const reviewsQ = await supabase
      .from("reviews")
      .select(
        "review_date, rating, stars, review_name, review_text, review_url, likes, complaint_match, positive_match"
      )
      .eq("store_id", store_id)
      .order("review_date", { ascending: false })
      .limit(100);

    if (reviewsQ.error) return res.status(500).json({ error: reviewsQ.error.message });

    return res.json({
      store: storeQ.data || null,
      reviews: reviewsQ.data || [],
    });
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

// -----------------------------
// UI COMPAT ROUTES (PATCH)
// Adds /api/metrics and /api/stores/summary expected by frontend
// Works by computing from your existing /api/exec-weekly payload
// -----------------------------

function qStr(req, key, def) {
  const v = req.query?.[key];
  if (v === undefined || v === null || v === "") return def;
  return String(v);
}
function qNum(req, key, def) {
  const v = Number(req.query?.[key]);
  return Number.isFinite(v) ? v : def;
}
function safePct(n, d) {
  if (!Number.isFinite(n) || !Number.isFinite(d) || d <= 0) return 0;
  return (n / d) * 100;
}

function computeFromExecWeekly(execWeekly, { window = "30d", rule = "standard", minReviews = 8 } = {}) {
  const rows = Array.isArray(execWeekly?.rows) ? execWeekly.rows : [];
  const stores = Array.isArray(execWeekly?.stores) ? execWeekly.stores : [];

  const weekSet = [...new Set(rows.map(r => r.week_ending).filter(Boolean))].sort();
  const latestWeek = weekSet[weekSet.length - 1] || null;

  const latestRows = latestWeek ? rows.filter(r => r.week_ending === latestWeek) : [];

  const thresholds =
    rule === "conservative" ? { rating: 3.4, negPct: 30, delta: -0.15 } :
    rule === "aggressive"   ? { rating: 3.8, negPct: 20, delta: -0.10 } :
                              { rating: 3.6, negPct: 25, delta: -0.15 };

  const avgStars = execWeekly?.summary?.avg_stars_overall;
  const totalReviews = execWeekly?.summary?.total_reviews;

  const positive = execWeekly?.summary?.positive_reviews;
  const negative = execWeekly?.summary?.negative_reviews;

  const negPct = (negative != null && totalReviews != null)
    ? safePct(Number(negative), Number(totalReviews))
    : 0;

  const summaryRows = latestRows.map(r => {
    const vol30d = Number(r.total_reviews ?? 0);
    const avg30d = (r.avg_rating ?? r.avg_stars ?? null);
    const avg7d = avg30d; // until you have real 7d rollups
    const deltaWk = r.rating_change ?? r.avg_stars_delta ?? null;

    const negPct30d = (r.negative_reviews != null && vol30d)
      ? safePct(Number(r.negative_reviews), vol30d)
      : 0;

    const status =
      (avg30d != null && avg30d < thresholds.rating && vol30d >= minReviews) ||
      (negPct30d >= thresholds.negPct && vol30d >= minReviews) ||
      (deltaWk != null && deltaWk <= thresholds.delta && vol30d >= minReviews)
        ? "At Risk"
        : "Stable";

    const s = stores.find(x => String(x.store_id) === String(r.store_id));

    return {
      store_id: String(r.store_id),
      place_name: r.place_name || s?.place_name || "Unknown",
      address: r.address || s?.address || "",
      city: r.city || s?.city || "",
      state: r.state || s?.state || "",
      market: r.market || r.state || s?.state || "Unknown",

      avg30d,
      avg7d,
      deltaWk,
      negPct30d,
      vol30d,

      topIssue: r.top_issue || "—",
      topPraise: r.top_praise || "—",
      confidence: vol30d >= minReviews ? "High" : "Low",
      reason: vol30d >= minReviews ? "Sufficient volume" : "Insufficient volume",
      status
    };
  });

  const storesAtRiskCount = summaryRows.filter(x => x.status === "At Risk").length;

  const drivers = {
    topComplaintTag: "—",
    topPraiseTag: "—",
    worstMoverCount: summaryRows.filter(r => (r.deltaWk ?? 0) < 0).length
  };

  const metrics = {
    window,
    rule,
    minReviews,
    lastRefreshed: new Date().toISOString(),
    brandAvg: (avgStars != null ? Number(avgStars) : null),
    negativePct: negPct,
    reviewVolume7d: totalReviews ?? null,
    storesAtRiskCount,
    drivers,
    definitions: {
      brandAvg: "Average star rating across stores in scope.",
      deltaWk: "Week-over-week rating change."
    }
  };

  return { metrics, summary: { rows: summaryRows } };
}

// Fetch exec-weekly using node's built-in fetch (Node 18+ / Node 22 ok)
async function getExecWeeklyLocal(PORT) {
  const res = await fetch(`http://localhost:${PORT}/api/exec-weekly`, {
    headers: { "Accept": "application/json" }
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`exec-weekly ${res.status}: ${text}`);
  try { return JSON.parse(text); } catch { throw new Error(`exec-weekly returned non-JSON: ${text.slice(0,200)}`); }
}

app.get("/api/metrics", async (req, res) => {
  try {
    const window = qStr(req, "window", "30d");
    const rule = qStr(req, "rule", "standard");
    const minReviews = qNum(req, "minReviews", 8);

    const execWeekly = await getExecWeeklyLocal(PORT);
    const { metrics } = computeFromExecWeekly(execWeekly, { window, rule, minReviews });

    res.json(metrics);
  } catch (e) {
    console.error("metrics error:", e);
    res.status(500).json({ error: "metrics_failed", detail: String(e?.message || e) });
  }
});

app.get("/api/stores/summary", async (req, res) => {
  try {
    const window = qStr(req, "window", "30d");
    const rule = qStr(req, "rule", "standard");
    const minReviews = qNum(req, "minReviews", 8);

    const execWeekly = await getExecWeeklyLocal(PORT);
    const { summary } = computeFromExecWeekly(execWeekly, { window, rule, minReviews });

    res.json(summary);
  } catch (e) {
    console.error("summary error:", e);
    res.status(500).json({ error: "summary_failed", detail: String(e?.message || e) });
  }
});

setTimeout(() => {
  console.log("✅ ROUTES REGISTERED:");
  app._router.stack
    .filter(r => r.route)
    .forEach(r =>
      console.log(
        Object.keys(r.route.methods).join(",").toUpperCase(),
        r.route.path
      )
    );
}, 200);

app.get("/api/exec-weekly", async (_req, res) => {
  try {
    if (!supabase) return res.status(500).json({ error: "Supabase client not initialized (missing env vars)" });

    const { data, error } = await supabase
      .from("exec_weekly")
      .select("*");

    if (error) return res.status(500).json({ error: error.message });
    return res.json(data ?? []);
  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});


app.listen(PORT, function () {
  console.log("pb1 backend running on http://localhost:" + PORT);
});
