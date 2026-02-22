import 'dotenv/config';
import express from 'express';
import path from 'path';
import fs from 'fs';
import { getIcpDb, listIcps, saveIcps, createIcp, initDb } from './db';
import type { RunFilters } from './types';

const app = express();
const PORT = process.env.DASHBOARD_PORT || 4242;

app.use(express.json());

// ── ICP helpers ───────────────────────────────────────────────────────────────

function getDb(slug = 'source-angel') {
  const dbPath = path.join(process.cwd(), 'data', `${slug}.db`);
  if (!fs.existsSync(dbPath)) return null;
  return getIcpDb(slug);
}

function icpSlug(req: express.Request): string {
  return (req.query.icp as string) || (req.body?.icp as string) || 'source-angel';
}

// ── ICP API ───────────────────────────────────────────────────────────────────

app.get('/api/icps', (_, res) => {
  try {
    res.json(listIcps());
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/icps', (req, res) => {
  const { slug, name, description, keywords, target_filters } = req.body || {};
  if (!slug || !name) return res.status(400).json({ error: 'slug and name required' });
  try {
    const icp = createIcp({ slug, name, description, keywords: keywords ?? [], target_filters });
    initDb(slug);
    res.json(icp);
  } catch (err: any) {
    res.status(409).json({ error: err.message });
  }
});

app.delete('/api/icps/:slug', (req, res) => {
  const { slug } = req.params;
  if (slug === 'source-angel') return res.status(400).json({ error: 'Cannot delete the default ICP' });
  try {
    const icps = listIcps();
    const filtered = icps.filter(i => i.slug !== slug);
    if (filtered.length === icps.length) return res.status(404).json({ error: 'ICP not found' });
    saveIcps(filtered);
    // Remove DB file if it exists
    const dbPath = path.join(process.cwd(), 'data', `${slug}.db`);
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    res.json({ ok: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/icps/:slug', (req, res) => {
  const { slug } = req.params;
  const { name, description, keywords, target_filters } = req.body || {};
  try {
    const icps = listIcps();
    const idx = icps.findIndex(i => i.slug === slug);
    if (idx < 0) return res.status(404).json({ error: 'ICP not found' });
    if (name !== undefined) icps[idx].name = name;
    if (description !== undefined) icps[idx].description = description;
    if (keywords !== undefined) icps[idx].keywords = keywords;
    if (target_filters !== undefined) icps[idx].target_filters = target_filters;
    saveIcps(icps);
    res.json(icps[idx]);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ── Duplicate detection ───────────────────────────────────────────────────────

function normalizeCo(name: string): string {
  return (name || '')
    .toLowerCase()
    .replace(/\b(a\/s|aps|as|ab|gmbh|ltd|inc|llc|co\.?|ag|nv|bv|plc|denmark|dk|group|holding|solutions|technology|technologies|consulting|software|digital|services|systems)\b/g, '')
    .replace(/[|&\-_.,'"()]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function jaccardSim(a: string, b: string): number {
  const tokA = new Set(a.split(/\s+/).filter(Boolean));
  const tokB = new Set(b.split(/\s+/).filter(Boolean));
  if (!tokA.size || !tokB.size) return 0;
  const inter = [...tokA].filter(t => tokB.has(t)).length;
  const union = new Set([...tokA, ...tokB]).size;
  return inter / union;
}

function longestCommonSubstr(a: string, b: string): number {
  if (!a || !b) return 0;
  let max = 0;
  for (let i = 0; i < a.length; i++) {
    for (let j = 0; j < b.length; j++) {
      let len = 0;
      while (i + len < a.length && j + len < b.length && a[i + len] === b[j + len]) len++;
      if (len > max) max = len;
    }
  }
  return max / Math.max(a.length, b.length);
}

function companyConfidence(nameA: string, nameB: string): { score: number; reason: string } {
  const normA = normalizeCo(nameA);
  const normB = normalizeCo(nameB);
  if (normA === normB) return { score: 1.0, reason: 'exact_normalized' };
  const jacc = jaccardSim(normA, normB);
  const lcs  = longestCommonSubstr(normA, normB);
  const combined = jacc * 0.6 + lcs * 0.4;
  const reason = jacc >= 0.8 ? 'high_token_overlap' : lcs >= 0.7 ? 'substring_match' : 'partial_match';
  return { score: parseFloat(combined.toFixed(3)), reason };
}

function detectDuplicates(companies: { company: string; count: number; score: number; ids: string }[]): any[] {
  const groups: any[] = [];
  const used = new Set<number>();

  for (let i = 0; i < companies.length; i++) {
    if (used.has(i)) continue;
    const group: any = { leads: [companies[i]], confidence: 1, reason: '' };
    for (let j = i + 1; j < companies.length; j++) {
      if (used.has(j)) continue;
      const { score, reason } = companyConfidence(companies[i].company, companies[j].company);
      if (score >= 0.65) {
        group.leads.push({ ...companies[j], matchScore: score, matchReason: reason });
        group.confidence = Math.max(group.confidence, score);
        group.reason = reason;
        used.add(j);
      }
    }
    if (group.leads.length > 1) {
      group.leads.sort((a: any, b: any) => (b.count - a.count) || (b.score - a.score));
      group.suggestedCanonical = group.leads[0].company;
      groups.push(group);
    }
  }
  return groups.sort((a, b) => b.confidence - a.confidence);
}

// ── ICP filter merging ────────────────────────────────────────────────────────

function mergeIcpFilters(filters: RunFilters): RunFilters {
  const icps = listIcps();
  const icp = icps.find(i => i.slug === filters.icp);
  if (!icp?.target_filters) return filters;
  const tf = icp.target_filters;
  return {
    minScore: tf.min_score ?? 70,
    eeRisk: tf.ee_risk ?? 'LOW,MEDIUM',
    temperature: tf.temperature ?? 'HOT,WARM',
    limit: tf.limit ?? 100,
    minEmployees: tf.min_employees ?? null,
    maxEmployees: tf.max_employees ?? null,
    ...filters, // explicit request body overrides ICP defaults
  };
}

// ── Run state + SSE log streaming ─────────────────────────────────────────────

interface RunState {
  phase: number | null;
  status: 'idle' | 'running' | 'done' | 'error';
  log: string[];
  startedAt: string | null;
  filters: RunFilters;
  progress: { current: number; total: number } | null;
}

let runState: RunState = {
  phase: null, status: 'idle', log: [], startedAt: null, filters: {}, progress: null,
};

const sseClients = new Set<express.Response>();

function emit(msg: string) {
  const line = `[${new Date().toTimeString().slice(0, 8)}] ${msg}`;
  runState.log.push(line);
  if (runState.log.length > 1000) runState.log = runState.log.slice(-1000);
  const payload = `data: ${JSON.stringify(line)}\n\n`;
  for (const client of sseClients) {
    try { client.write(payload); } catch {}
  }
}

app.get('/api/run/log', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.flushHeaders();

  for (const line of runState.log) {
    res.write(`data: ${JSON.stringify(line)}\n\n`);
  }
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/run/status', (_, res) => {
  res.json(runState);
});

// ── Run phase 1 ───────────────────────────────────────────────────────────────

app.post('/api/run/phase1', async (req, res) => {
  if (runState.status === 'running') return res.status(409).json({ error: 'A run is already in progress' });
  const filters: RunFilters = req.body || {};
  if (!filters.icp) filters.icp = 'source-angel';
  runState = { phase: 1, status: 'running', log: [], startedAt: new Date().toISOString(), filters, progress: null };
  res.json({ ok: true, message: 'Phase 1 started' });

  try {
    const { runPhase1 } = await import('./phase1-scrape');
    await runPhase1(filters, emit);
    runState.status = 'done';
    emit('Phase 1 complete');
  } catch (err: any) {
    runState.status = 'error';
    emit(`Phase 1 failed: ${err.message}`);
  }
});

// ── Run phase 2 ───────────────────────────────────────────────────────────────

app.post('/api/run/phase2', async (req, res) => {
  if (runState.status === 'running') return res.status(409).json({ error: 'A run is already in progress' });
  const filters: RunFilters = req.body || {};
  if (!filters.icp) filters.icp = 'source-angel';
  runState = { phase: 2, status: 'running', log: [], startedAt: new Date().toISOString(), filters, progress: null };
  res.json({ ok: true, message: 'Phase 2 started' });

  try {
    const { runPhase2 } = await import('./phase2-intel');
    await runPhase2(filters, emit);
    runState.status = 'done';
    emit('Phase 2 complete');
  } catch (err: any) {
    runState.status = 'error';
    emit(`Phase 2 failed: ${err.message}`);
  }
});

// ── Run phase 2B ──────────────────────────────────────────────────────────────

app.post('/api/run/phase2b', async (req, res) => {
  if (runState.status === 'running') return res.status(409).json({ error: 'A run is already in progress' });
  let filters: RunFilters = req.body || {};
  if (!filters.icp) filters.icp = 'source-angel';
  filters = mergeIcpFilters(filters);
  runState = { phase: 22, status: 'running', log: [], startedAt: new Date().toISOString(), filters, progress: null };
  res.json({ ok: true, message: 'Phase 2B started' });

  try {
    const { runPhase2b } = await import('./phase2b-people');
    await runPhase2b(filters, emit);
    runState.status = 'done';
    emit('Phase 2B complete');
  } catch (err: any) {
    runState.status = 'error';
    emit(`Phase 2B failed: ${err.message}`);
  }
});

// ── Run phase 3 ───────────────────────────────────────────────────────────────

app.post('/api/run/phase3', async (req, res) => {
  if (runState.status === 'running') return res.status(409).json({ error: 'A run is already in progress' });
  let filters: RunFilters = req.body || {};
  if (!filters.icp) filters.icp = 'source-angel';
  filters = mergeIcpFilters(filters);
  runState = { phase: 3, status: 'running', log: [], startedAt: new Date().toISOString(), filters, progress: null };
  res.json({ ok: true, message: 'Phase 3 started' });

  try {
    const { runPhase3 } = await import('./phase3-outreach');
    await runPhase3(filters, emit);
    runState.status = 'done';
    emit('Phase 3 complete');
  } catch (err: any) {
    runState.status = 'error';
    emit(`Phase 3 failed: ${err.message}`);
  }
});

// ── Run all phases sequentially ───────────────────────────────────────────────

app.post('/api/run/all', async (req, res) => {
  if (runState.status === 'running') return res.status(409).json({ error: 'A run is already in progress' });
  let filters: RunFilters = req.body || {};
  if (!filters.icp) filters.icp = 'source-angel';
  filters = mergeIcpFilters(filters);
  runState = { phase: 0, status: 'running', log: [], startedAt: new Date().toISOString(), filters, progress: null };
  res.json({ ok: true, message: 'Full pipeline started' });

  try {
    const { runPhase1 } = await import('./phase1-scrape');
    const { runPhase2 } = await import('./phase2-intel');
    const { runPhase2b } = await import('./phase2b-people');
    const { runPhase3 } = await import('./phase3-outreach');

    runState.phase = 1;
    emit('Starting Phase 1: Scrape jobs');
    await runPhase1(filters, emit);
    emit('Phase 1 done');

    runState.phase = 2;
    emit('Starting Phase 2: Company intelligence');
    await runPhase2(filters, emit);
    emit('Phase 2 done');

    runState.phase = 22;
    emit('Starting Phase 2B: People mapping');
    await runPhase2b(filters, emit);
    emit('Phase 2B done');

    runState.phase = 3;
    emit('Starting Phase 3: Outreach drafts');
    await runPhase3(filters, emit);
    emit('Phase 3 done');

    runState.status = 'done';
    emit('Full pipeline complete!');
  } catch (err: any) {
    runState.status = 'error';
    emit(`Pipeline failed at phase ${runState.phase}: ${err.message}`);
  }
});

// ── Data API ──────────────────────────────────────────────────────────────────

app.get('/api/stats', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json({ error: 'No database yet — run Phase 1 first' });
  try {
    const totalLeads    = (db.prepare('SELECT COUNT(*) as c FROM leads').get() as any)?.c ?? 0;
    const highLeads     = (db.prepare("SELECT COUNT(*) as c FROM leads WHERE priority='HIGH'").get() as any)?.c ?? 0;
    const hotCompanies  = (db.prepare("SELECT COUNT(*) as c FROM companies WHERE hiring_temperature='HOT'").get() as any)?.c ?? 0;
    const warmCompanies = (db.prepare("SELECT COUNT(*) as c FROM companies WHERE hiring_temperature='WARM'").get() as any)?.c ?? 0;
    const totalDrafts   = (db.prepare('SELECT COUNT(*) as c FROM drafts').get() as any)?.c ?? 0;
    const warmIntros    = (db.prepare("SELECT COUNT(*) as c FROM targets WHERE connection_found=1").get() as any)?.c ?? 0;
    const totalCompanies= (db.prepare('SELECT COUNT(*) as c FROM companies').get() as any)?.c ?? 0;
    const lastRun       = (db.prepare('SELECT MAX(scraped_at) as t FROM leads').get() as any)?.t ?? null;
    res.json({ totalLeads, highLeads, hotCompanies, warmCompanies, totalDrafts, warmIntros, totalCompanies, lastRun });
  } catch { res.json({ error: 'Database not initialized' }); }
});

app.get('/api/companies', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json([]);
  try {
    const rows = db.prepare(`
      SELECT c.*,
        (SELECT COUNT(*) FROM drafts d WHERE d.company_name = c.company_name) as draft_count,
        (SELECT target_name FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as target_name,
        (SELECT outreach_strategy FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as outreach_strategy,
        (SELECT connection_type FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as connection_type,
        (SELECT bridge_name FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as bridge_name,
        (SELECT bridge_position FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as bridge_position,
        (SELECT bridge_connected_on FROM targets t WHERE t.company_name = c.company_name ORDER BY connection_score DESC LIMIT 1) as bridge_connected_on
      FROM companies c
      ORDER BY CASE c.hiring_temperature WHEN 'HOT' THEN 0 WHEN 'WARM' THEN 1 ELSE 2 END, c.priority_score DESC
      LIMIT 100
    `).all();
    res.json(rows);
  } catch { res.json([]); }
});

app.get('/api/leads', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json([]);
  try {
    const rows = db.prepare(`SELECT * FROM leads ORDER BY priority_score DESC LIMIT 500`).all();
    res.json(rows);
  } catch { res.json([]); }
});

app.get('/api/drafts', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json([]);
  try {
    const rows = db.prepare(`
      SELECT d.*, c.linkedin_url as company_linkedin_url, c.hiring_temperature
      FROM drafts d LEFT JOIN companies c ON c.company_name = d.company_name
      ORDER BY d.created_at DESC LIMIT 200
    `).all();
    res.json(rows);
  } catch { res.json([]); }
});

app.get('/api/targets', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json([]);
  try {
    const rows = db.prepare(`SELECT * FROM targets ORDER BY connection_score DESC, created_at DESC LIMIT 100`).all();
    res.json(rows);
  } catch { res.json([]); }
});

// ── Duplicate detection API ───────────────────────────────────────────────────

app.get('/api/duplicates', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.json([]);
  try {
    const rows = db.prepare(`
      SELECT company,
             COUNT(*) as count,
             MAX(priority_score) as score,
             GROUP_CONCAT(id) as ids
      FROM leads
      GROUP BY company
      ORDER BY company
    `).all() as any[];

    const groups = detectDuplicates(rows);
    res.json(groups);
  } catch (err: any) { res.json({ error: err.message }); }
});

// Merge: rename all leads from `from` company to `to` company
app.post('/api/leads/merge', (req, res) => {
  const { from: fromName, to: toName } = req.body || {};
  if (!fromName || !toName) return res.status(400).json({ error: 'from and to required' });
  const db = getDb(icpSlug(req));
  if (!db) return res.status(503).json({ error: 'No database' });
  try {
    db.prepare('UPDATE leads SET company = ? WHERE company = ?').run(toName, fromName);
    db.prepare('DELETE FROM leads WHERE company = ? AND canonical_url IN (SELECT canonical_url FROM leads GROUP BY canonical_url HAVING COUNT(*) > 1)').run(toName);
    emit(`MERGE: "${fromName}" -> "${toName}"`);
    res.json({ ok: true, merged: fromName, into: toName });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

// Delete all leads for a company
app.delete('/api/leads/company', (req, res) => {
  const name = req.query.name as string;
  if (!name) return res.status(400).json({ error: 'name query param required' });
  const db = getDb(icpSlug(req));
  if (!db) return res.status(503).json({ error: 'No database' });
  try {
    const count = (db.prepare('SELECT COUNT(*) as c FROM leads WHERE company = ?').get(name) as any)?.c ?? 0;
    db.prepare('DELETE FROM leads WHERE company = ?').run(name);
    emit(`DELETED: "${name}" (${count} leads)`);
    res.json({ ok: true, deleted: count, company: name });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

// Delete a single lead by id
app.delete('/api/leads/:id', (req, res) => {
  const db = getDb(icpSlug(req));
  if (!db) return res.status(503).json({ error: 'No database' });
  try {
    db.prepare('DELETE FROM leads WHERE id = ?').run(parseInt(req.params.id, 10));
    res.json({ ok: true });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

// ── Serve static dashboard ────────────────────────────────────────────────────

app.use(express.static(path.join(process.cwd(), 'public')));

app.listen(PORT, () => {
  console.log(`\nLead Agent Dashboard running at http://localhost:${PORT}\n`);
});
