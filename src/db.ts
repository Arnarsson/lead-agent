import { DatabaseSync } from 'node:sqlite';
import path from 'path';
import fs from 'fs';

const DATA_DIR = path.join(process.cwd(), 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ── ICP registry ──────────────────────────────────────────────────────────────

export interface ICP {
  slug: string;
  name: string;
  description?: string;
  keywords?: string[];
  target_filters?: {
    min_employees?: number | null;
    max_employees?: number | null;
    min_score?: number;
    ee_risk?: string;
    temperature?: string;
    limit?: number;
  };
  created_at: string;
}

const ICPS_PATH = path.join(DATA_DIR, 'icps.json');

export function listIcps(): ICP[] {
  if (!fs.existsSync(ICPS_PATH)) return [];
  return JSON.parse(fs.readFileSync(ICPS_PATH, 'utf-8'));
}

export function saveIcps(icps: ICP[]) {
  fs.writeFileSync(ICPS_PATH, JSON.stringify(icps, null, 2));
}

export function createIcp(icp: Omit<ICP, 'created_at'>): ICP {
  const icps = listIcps();
  if (icps.find(i => i.slug === icp.slug)) throw new Error(`ICP '${icp.slug}' already exists`);
  const newIcp: ICP = { ...icp, created_at: new Date().toISOString().slice(0, 10) };
  icps.push(newIcp);
  saveIcps(icps);
  return newIcp;
}

// ── Per-ICP DB factory ────────────────────────────────────────────────────────

const _dbCache = new Map<string, DatabaseSync>();

export function getIcpDb(slug: string): DatabaseSync {
  if (!_dbCache.has(slug)) {
    const dbPath = path.join(DATA_DIR, `${slug}.db`);
    _dbCache.set(slug, new DatabaseSync(dbPath));
  }
  return _dbCache.get(slug)!;
}

export function getConnectionsPath(slug: string): string {
  return path.join(DATA_DIR, `${slug}-connections.csv`);
}

// Keep legacy export for backward compat during transition
export const db = getIcpDb('source-angel');

export function initDb(slug = 'source-angel') {
  const d = getIcpDb(slug);
  d.exec(`
    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      canonical_url TEXT UNIQUE,
      company TEXT,
      title TEXT,
      location TEXT,
      source TEXT,
      priority TEXT,
      priority_score INTEGER,
      scoring_factors TEXT,
      scraped_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS companies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_name TEXT UNIQUE,
      linkedin_url TEXT,
      linkedin_slug TEXT,
      search_confidence TEXT,
      employees_count INTEGER,
      company_size TEXT,
      countries TEXT,
      ee_countries TEXT,
      non_ee_countries TEXT,
      ee_risk TEXT,
      ee_action TEXT,
      ee_note TEXT,
      ee_is_known_outsourcer INTEGER DEFAULT 0,
      has_non_we_offices INTEGER DEFAULT 0,
      countries_outside_we TEXT,
      evidence_summary TEXT,
      hiring_signals TEXT,
      hiring_temperature TEXT,
      hiring_signal_snippets TEXT,
      outreach_angles TEXT,
      posts_scanned INTEGER DEFAULT 0,
      ceo_name TEXT,
      ceo_title TEXT,
      cto_name TEXT,
      cto_title TEXT,
      website_url TEXT,
      job_urls TEXT,
      job_titles TEXT,
      priority TEXT,
      priority_score INTEGER,
      lead_count INTEGER DEFAULT 1,
      enriched_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      query_key TEXT UNIQUE,
      snapshot_id TEXT,
      status TEXT DEFAULT 'pending',
      result_json TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS targets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_name TEXT,
      target_name TEXT,
      target_role TEXT,
      target_title TEXT,
      target_linkedin_url TEXT,
      connection_found INTEGER DEFAULT 0,
      connection_type TEXT,
      connection_score INTEGER DEFAULT 0,
      outreach_strategy TEXT,
      all_companies TEXT,
      search_query TEXT,
      bridge_name TEXT,
      bridge_position TEXT,
      bridge_linkedin_url TEXT,
      bridge_connected_on TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS drafts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_name TEXT,
      target_name TEXT,
      target_linkedin_url TEXT,
      hiring_temperature TEXT,
      draft_index INTEGER,
      angle_used TEXT,
      subject TEXT,
      body TEXT,
      cta TEXT,
      language TEXT DEFAULT 'da',
      created_at TEXT DEFAULT (datetime('now'))
    );
  `);
  // Migrate existing targets table
  for (const col of [
    'bridge_name TEXT',
    'bridge_position TEXT',
    'bridge_linkedin_url TEXT',
    'bridge_connected_on TEXT',
  ]) {
    try { d.exec(`ALTER TABLE targets ADD COLUMN ${col}`); } catch {}
  }

  console.log(`Database initialized [${slug}]`);
}

// ── Query helpers (take explicit db) ─────────────────────────────────────────

export function getOne<T = any>(d: DatabaseSync, sql: string, ...params: any[]): T | undefined {
  return d.prepare(sql).get(...params) as T | undefined;
}

export function getAll<T = any>(d: DatabaseSync, sql: string, ...params: any[]): T[] {
  return d.prepare(sql).all(...params) as T[];
}

export function run(d: DatabaseSync, sql: string, ...params: any[]): void {
  d.prepare(sql).run(...params);
}

export function getSnapshot(d: DatabaseSync, queryKey: string): any {
  return getOne(d, 'SELECT * FROM snapshots WHERE query_key = ?', queryKey);
}

export function upsertSnapshot(d: DatabaseSync, queryKey: string, snapshotId: string, status: string, resultJson?: string) {
  d.exec(`
    INSERT INTO snapshots (query_key, snapshot_id, status, result_json, updated_at)
    VALUES ('${queryKey.replace(/'/g, "''")}', '${snapshotId}', '${status}', ${resultJson ? `'${resultJson.replace(/'/g, "''")}'` : 'NULL'}, datetime('now'))
    ON CONFLICT(query_key) DO UPDATE SET
      snapshot_id = excluded.snapshot_id,
      status = excluded.status,
      result_json = COALESCE(excluded.result_json, result_json),
      updated_at = excluded.updated_at
  `);
}

export function upsertCompany(d: DatabaseSync, data: Record<string, any>) {
  const sanitized: Record<string, any> = {};
  for (const [k, v] of Object.entries(data)) {
    sanitized[k] = v === undefined ? null : v;
  }
  const cols = Object.keys(sanitized);
  const placeholders = cols.map(() => '?').join(', ');
  const updates = cols.filter(k => k !== 'company_name').map(k => `${k} = excluded.${k}`).join(', ');
  const values = cols.map(k => sanitized[k]);
  d.prepare(`
    INSERT INTO companies (${cols.join(', ')}) VALUES (${placeholders})
    ON CONFLICT(company_name) DO UPDATE SET ${updates}
  `).run(...values);
}
