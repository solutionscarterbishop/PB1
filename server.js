import dotenv from "dotenv";
dotenv.config();

import express from "express";
import cors from "cors";
import rateLimit from "express-rate-limit";
import NodeCache from "node-cache";
import PDFDocument from "pdfkit";
import { createClient } from "@supabase/supabase-js";

// =====================
// ENV + CONFIG
// =====================
const PORT = Number(process.env.PORT || 10000);
const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE_KEY = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
const EXEC_WEEKLY_TABLE = (process.env.EXEC_WEEKLY_TABLE || "weekly_rollups").trim();

const SCHEMA_VERSION = 1;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
}

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
    : null;

const cache = new NodeCache({ stdTTL: 300 }); // 5 min

// =====================
// APP 560

// =====================
const app = express();

app.set("trust proxy", 1);
// dev-friendly CORS (stop blocking yourself)
app.use(cors({
  origin: true,
  credentials: false
}));

app.use(express.json({ limit: "2mb" }));

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 300
});
app.use("/api/", limiter);

// =====================
// HELPERS
// =====================
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

function parseRule(rule) {
  if (rule === "conservative") return { avgMin: 3.4, negMax: 30, deltaMin: -0.15 };
  if (rule === "aggressive") return { avgMin: 3.8, negMax: 20, deltaMin: -0.10 };
  return { avgMin: 3.6, negMax: 25, deltaMin: -0.15 }; // standard
}

function toPct(x) {
  if (!Number.isFinite(x)) return null;
  return x * 100;
}

function confFromVol(vol30d) {
  if (!Number.isFinite(vol30d)) return "Low";
  if (vol30d >= 50) return "High";
  if (vol30d >= 15) return "Medium";
  return "Low";
}

function statusFrom(row, ruleCfg) {
  const avg = row.avg30d;
  const neg = row.negPct30d;
  const delta = row.deltaWk;

  const reasons = [];
  if (avg != null && avg < ruleCfg.avgMin) reasons.push(`avg<${ruleCfg.avgMin}`);
  if (neg != null && neg >= ruleCfg.negMax) reasons.push(`neg%≥${ruleCfg.negMax}`);
  if (delta != null && delta <= ruleCfg.deltaMin) reasons.push(`Δwk≤${ruleCfg.deltaMin}`);

  if (reasons.length >= 2) return { status: "At Risk", reason: reasons.join(" + ") };
  if (reasons.length === 1) return { status: "Watch", reason: reasons[0] };
  return { status: "Stable", reason: "within thresholds" };
}

// Pull last N weeks of weekly_rollups
async function getWeeklyRows({ limit = 10000 } = {}) {
  if (!supabase) throw new Error("Supabase not configured.");

  const cacheKey = `weekly:${limit}`;
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const { data, error } = await supabase
    .from(EXEC_WEEKLY_TABLE)
    .select("*")
    .order("week_ending", { ascending: false })
    .limit(limit);

  if (error) throw new Error(`Supabase select failed: ${error.message || JSON.stringify(error)}`);

  const rows = data || [];
  cache.set(cacheKey, rows);
  return rows;
}

function lastNUniqueWeeks(rows, n) {
  const weeks = [];
  for (const r of rows) {
    if (r.week_ending && !weeks.includes(r.week_ending)) weeks.push(r.week_ending);
    if (weeks.length >= n) break;
  }
  return weeks;
}

function groupBy(rows, keyFn) {
  const m = new Map();
  for (const r of rows) {
    const k = keyFn(r);
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(r);
  }
  return m;
}

function normalizeStoreId(r) {
  // prefer store_id; fallback to store_name|state
  const sid = r.store_id || r.storeid || r.store || null;
  if (sid) return String(sid);
  const name = (r.store_name || r.store || r.location || "unknown").toString().trim();
  const st = (r.state || "").toString().trim();
  return `${name}__${st}`.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function pickName(r) {
  return (r.store_name || r.store || r.location || r.name || "Unknown Store").toString();
}

function pickMarket(r) {
  return (r.market || r.region || r.city || "Unknown").toString();
}

// Build “store summary row” used by your frontend
function buildStoreSummary({ store_id, name, market, state, rowsForStore, windowWeeks, ruleCfg }) {
  // rowsForStore are sorted desc by week_ending (because global fetch is desc)
  // Compute over windowWeeks (N weeks)
  const byWeek = groupBy(rowsForStore, (r) => r.week_ending);
  const weeks = windowWeeks.filter((w) => byWeek.has(w));

  const windowRows = weeks.map((w) => byWeek.get(w)[0]).filter(Boolean); // 1 row per week
  const latest = windowRows[0] || null;
  const prev = windowRows[1] || null;

  // avg30d = avg of last 4 weeks; avg7d = latest week avg
  const avg7d = latest ? Number(latest.avg_rating ?? latest.avg ?? latest.rating ?? null) : null;
  const avg30d =
    windowRows.length
      ? windowRows.slice(0, 4).reduce((s, r) => s + Number(r.avg_rating ?? r.avg ?? r.rating ?? 0), 0) /
        clamp(Math.min(4, windowRows.length), 1, 999)
      : null;

  const deltaWk =
    latest && prev
      ? Number(avg7d) - Number(prev.avg_rating ?? prev.avg ?? prev.rating ?? 0)
      : null;

  const totalLatest = latest ? Number(latest.total_reviews ?? latest.total ?? 0) : 0;
  const lowLatest = latest ? Number(latest.low_reviews ?? latest.low ?? 0) : 0;

  const vol30d = windowRows.slice(0, 4).reduce((s, r) => s + Number(r.total_reviews ?? r.total ?? 0), 0);
  const negPct30d = vol30d > 0
    ? toPct(windowRows.slice(0, 4).reduce((s, r) => s + Number(r.low_reviews ?? r.low ?? 0), 0) / vol30d)
    : null;

  const flag = statusFrom({ avg30d, negPct30d, deltaWk }, ruleCfg);
  const confidence = confFromVol(vol30d);

  return {
    store_id,
    place_name: name,
    address: "",
    city: "",
    state: state || "",
    market: market || "Unknown",
    avg30d: Number.isFinite(avg30d) ? Number(avg30d.toFixed(2)) : null,
    avg7d: Number.isFinite(avg7d) ? Number(avg7d.toFixed(2)) : null,
    deltaWk: Number.isFinite(deltaWk) ? Number(deltaWk.toFixed(2)) : null,
    negPct30d: Number.isFinite(negPct30d) ? Number(negPct30d.toFixed(1)) : 0,
    vol30d: vol30d || 0,
    topIssue: "—",
    topPraise: "—",
    status: flag.status,
    confidence,
    reason: flag.reason
  };
}

async function computeSummaryAndMetrics({ window = "30d", rule = "standard", minReviews = 8, market, state, city, q, sort }) {
  const ruleCfg = parseRule(rule);

  const all = await getWeeklyRows({ limit: 10000 });

  // interpret window as weeks (7d=1, 14d=2, 30d=4, 90d=12)
  const windowWeeksN =
    window === "7d" ? 1 : window === "14d" ? 2 : window === "90d" ? 12 : 4;

  const uniqWeeks = lastNUniqueWeeks(all, Math.max(windowWeeksN, 12));
  const storeGroups = groupBy(all, (r) => normalizeStoreId(r));

  // build store rows
  let rows = [];
  for (const [store_id, storeRows] of storeGroups.entries()) {
    const r0 = storeRows[0];
    const name = pickName(r0);
    const mk = pickMarket(r0);
    const st = (r0.state || "").toString();

    const sumRow = buildStoreSummary({
      store_id,
      name,
      market: mk,
      state: st,
      rowsForStore: storeRows,
      windowWeeks: uniqWeeks.slice(0, windowWeeksN),
      ruleCfg
    });

    // enforce minReviews based on vol30d (proxy)
    if ((sumRow.vol30d || 0) < Number(minReviews || 0)) continue;

    rows.push(sumRow);
  }

  // filtering
  if (market) rows = rows.filter(r => r.market === market);
  if (state) rows = rows.filter(r => r.state === state);
  if (city) rows = rows.filter(r => (r.city || "") === city);
  if (q) {
    const qq = String(q).toLowerCase();
    rows = rows.filter(r =>
      (r.place_name || "").toLowerCase().includes(qq) ||
      (r.market || "").toLowerCase().includes(qq) ||
      (r.state || "").toLowerCase().includes(qq)
    );
  }

  // sorting
  if (sort === "rating_low") rows.sort((a,b) => (a.avg30d ?? 9) - (b.avg30d ?? 9));
  else if (sort === "rating_high") rows.sort((a,b) => (b.avg30d ?? 0) - (a.avg30d ?? 0));
  else if (sort === "delta_down") rows.sort((a,b) => (a.deltaWk ?? 9) - (b.deltaWk ?? 9));
  else if (sort === "neg_high") rows.sort((a,b) => (b.negPct30d ?? 0) - (a.negPct30d ?? 0));
  else if (sort === "vol_high") rows.sort((a,b) => (b.vol30d ?? 0) - (a.vol30d ?? 0));
  else {
    // atrisk default: At Risk first, then worst avg
    const rank = (s) => (s === "At Risk" ? 0 : s === "Watch" ? 1 : 2);
    rows.sort((a,b) => (rank(a.status)-rank(b.status)) || ((a.avg30d ?? 9) - (b.avg30d ?? 9)));
  }

  // metrics (weighted brand avg by vol30d)
  const totalVol = rows.reduce((s, r) => s + (r.vol30d || 0), 0);
  const brandAvg =
    totalVol > 0
      ? rows.reduce((s, r) => s + (r.avg30d != null ? r.avg30d * (r.vol30d || 0) : 0), 0) / totalVol
      : null;

  const negativePct =
    totalVol > 0
      ? rows.reduce((s, r) => s + ((r.negPct30d || 0) / 100) * (r.vol30d || 0), 0) / totalVol * 100
      : 0;

  const storesAtRiskCount = rows.filter(r => r.status === "At Risk").length;
  const worstMoverCount = rows.filter(r => (r.deltaWk ?? 0) < 0).length;

  const now = new Date().toISOString();

  return {
    summary: { rows },
    metrics: {
      schemaVersion: SCHEMA_VERSION,
      lastRefreshed: now,
      window,
      rule,
      minReviews: Number(minReviews),
      brandAvg: brandAvg != null ? Number(brandAvg.toFixed(2)) : null,
      negativePct: Number(negativePct.toFixed(1)),
      reviewVolume7d: rows.reduce((s, r) => s + (r.vol30d || 0), 0), // proxy
      storesAtRiskCount,
      drivers: {
        topComplaintTag: "—",
        topPraiseTag: "—",
        worstMoverCount
      },
      definitions: {
        brandAvg: "Weighted average rating across stores in scope (proxy from weekly rollups).",
        deltaWk: "Δwk = latest week avg rating minus prior week."
      }
    }
  };
}

// =====================
// ROUTES
// =====================
app.get("/", (req, res) => res.send("pb1 backend running"));

app.get("/api/health", async (req, res) => {
  try {
    res.json({
      ok: true,
      now: new Date().toISOString(),
      schemaVersion: SCHEMA_VERSION
    });
  } catch (e) {
    res.status(500).json({ ok: false, schemaVersion: SCHEMA_VERSION, error: String(e?.message || e) });
  }
});

// Frontend expects /api/stores => ARRAY of stores
app.get("/api/stores", async (req, res) => {
  try {
    const { summary } = await computeSummaryAndMetrics({
      window: "30d",
      rule: "standard",
      minReviews: 0
    });

    // Your frontend wants store objects with fields like market/state/city/place_name/store_id
    const stores = (summary.rows || []).map(r => ({
      store_id: r.store_id,
      place_name: r.place_name,
      address: r.address,
      city: r.city,
      state: r.state,
      market: r.market
    }));

    res.json(stores);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get("/api/metrics", async (req, res) => {
  try {
    const out = await computeSummaryAndMetrics({
      window: (req.query.window || "30d").toString(),
      rule: (req.query.rule || "standard").toString(),
      minReviews: Number(req.query.minReviews || 8),
      market: (req.query.market || "").toString() || undefined,
      state: (req.query.state || "").toString() || undefined,
      city: (req.query.city || "").toString() || undefined,
      q: (req.query.q || "").toString() || undefined,
      sort: (req.query.sort || "").toString() || undefined
    });

    res.json(out.metrics);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get("/api/stores/summary", async (req, res) => {
  try {
    const out = await computeSummaryAndMetrics({
      window: (req.query.window || "30d").toString(),
      rule: (req.query.rule || "standard").toString(),
      minReviews: Number(req.query.minReviews || 8),
      market: (req.query.market || "").toString() || undefined,
      state: (req.query.state || "").toString() || undefined,
      city: (req.query.city || "").toString() || undefined,
      q: (req.query.q || "").toString() || undefined,
      sort: (req.query.sort || "").toString() || undefined
    });

    res.json(out.summary);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Store detail used by your modal: /api/store/:id
app.get("/api/store/:id", async (req, res) => {
  try {
    const store_id = req.params.id;
    const window = (req.query.window || "30d").toString();
    const rule = (req.query.rule || "standard").toString();
    const minReviews = Number(req.query.minReviews || 8);
    const ruleCfg = parseRule(rule);

    const all = await getWeeklyRows({ limit: 10000 });
    const storeRows = all.filter(r => normalizeStoreId(r) === store_id);

    if (!storeRows.length) {
      return res.status(404).json({ ok: false, error: "Store not found in weekly rollups." });
    }

    const windowWeeksN = window === "7d" ? 1 : window === "14d" ? 2 : window === "90d" ? 12 : 4;
    const uniqWeeks = lastNUniqueWeeks(all, Math.max(windowWeeksN, 12));
    const weeks = uniqWeeks.slice(0, 12);

    // build trends arrays
    const byWeek = groupBy(storeRows, (r) => r.week_ending);
    const avgRatingByWeek = weeks.map(w => {
      const row = byWeek.get(w)?.[0];
      const v = row ? Number(row.avg_rating ?? row.avg ?? row.rating ?? null) : null;
      return Number.isFinite(v) ? Number(v.toFixed(2)) : null;
    });

    // compute summary kpis
    const summaryRow = buildStoreSummary({
      store_id,
      name: pickName(storeRows[0]),
      market: pickMarket(storeRows[0]),
      state: (storeRows[0].state || "").toString(),
      rowsForStore: storeRows,
      windowWeeks: uniqWeeks.slice(0, windowWeeksN),
      ruleCfg
    });

    if ((summaryRow.vol30d || 0) < minReviews) {
      // still return, but note confidence
      summaryRow.confidence = "Low";
    }

    const flag = statusFrom(summaryRow, ruleCfg);

    res.json({
      ok: true,
      lastRefreshed: new Date().toISOString(),
      window,
      rule,
      minReviews,
      confidence: summaryRow.confidence,
      store: {
        store_id,
        place_name: summaryRow.place_name,
        market: summaryRow.market,
        state: summaryRow.state
      },
      kpis: {
        avg7d: summaryRow.avg7d,
        avg30d: summaryRow.avg30d,
        deltaWk: summaryRow.deltaWk,
        neg7d: null,
        neg30d: summaryRow.negPct30d,
        vol7d: null,
        vol30d: summaryRow.vol30d
      },
      trends: {
        weeks,
        avgRatingByWeek
      },
      themes: {
        complaints: [],
        praise: [],
        newThisWeek: []
      },
      actionChecklist: [
        { title: "Validate excerpts", rationale: `Status=${flag.status}. Confirm drivers before action.` },
        { title: "Ops check", rationale: "Spot-check speed/CA/cleanliness on next manager visit." },
        { title: "Close the loop", rationale: "Track next week’s avg + volume to confirm rebound." }
      ],
      recentNegative: [],
      recentPositive: [],
      rawLinks: {
        googleSearchUrl: `https://www.google.com/search?q=${encodeURIComponent(summaryRow.place_name + " " + (summaryRow.state || ""))}`
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Compare endpoint: your frontend calls /api/compare?ids=...
app.get("/api/compare", async (req, res) => {
  try {
    const ids = (req.query.ids || "").toString().split(",").map(s => s.trim()).filter(Boolean);
    const window = (req.query.window || "30d").toString();
    const rule = (req.query.rule || "standard").toString();
    const minReviews = Number(req.query.minReviews || 8);

    if (ids.length < 2) return res.status(400).json({ ok: false, error: "Need at least 2 ids." });

    // build store objects by calling store detail builder quickly
    const all = await getWeeklyRows({ limit: 10000 });
    const windowWeeksN = window === "7d" ? 1 : window === "14d" ? 2 : window === "90d" ? 12 : 4;
    const uniqWeeks = lastNUniqueWeeks(all, Math.max(windowWeeksN, 12));
    const ruleCfg = parseRule(rule);

    const stores = [];
    for (const store_id of ids.slice(0, 6)) {
      const storeRows = all.filter(r => normalizeStoreId(r) === store_id);
      if (!storeRows.length) continue;

      const summaryRow = buildStoreSummary({
        store_id,
        name: pickName(storeRows[0]),
        market: pickMarket(storeRows[0]),
        state: (storeRows[0].state || "").toString(),
        rowsForStore: storeRows,
        windowWeeks: uniqWeeks.slice(0, windowWeeksN),
        ruleCfg
      });

      if ((summaryRow.vol30d || 0) < minReviews) summaryRow.confidence = "Low";

      // trends
      const byWeek = groupBy(storeRows, (r) => r.week_ending);
      const weeks = uniqWeeks.slice(0, 12);
      const avgRatingByWeek = weeks.map(w => {
        const row = byWeek.get(w)?.[0];
        const v = row ? Number(row.avg_rating ?? row.avg ?? row.rating ?? null) : null;
        return Number.isFinite(v) ? Number(v.toFixed(2)) : null;
      });

      stores.push({
        ...summaryRow,
        trends: { weeks, avgRatingByWeek }
      });
    }

    res.json({
      ok: true,
      stores,
      themeList: [],
      drilldown: { byTheme: {} }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// PDF endpoints your UI tries to open (basic working placeholder)
app.get("/api/pdf/compare", async (req, res) => {
  try {
    res.setHeader("Content-Type", "application/pdf");
    const doc = new PDFDocument({ margin: 48 });
    doc.pipe(res);

    doc.fontSize(18).text("Executive Brief (MVP)", { underline: true });
    doc.moveDown();
    doc.fontSize(11).text(`Generated: ${new Date().toLocaleString()}`);
    doc.moveDown();
    doc.text("PDF export is wired. Next step: populate with real executive summary + store tables.");

    doc.end();
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// START
app.listen(PORT, () => {
  console.log(`pb1 backend running on http://localhost:${PORT}`);

