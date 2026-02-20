import { DatabaseSync } from 'node:sqlite';
import path from 'path';
import fs from 'fs';

const DATA_DIR = path.join(process.cwd(), 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

export const db = new DatabaseSync(path.join(DATA_DIR, 'leads.db'));

export function initDb() {
  db.exec(`
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
  console.log('Database initialized');
}

// ── Query helpers ─────────────────────────────────────────────────────────────

export function getOne<T = any>(sql: string, ...params: any[]): T | undefined {
  return db.prepare(sql).get(...params) as T | undefined;
}

export function getAll<T = any>(sql: string, ...params: any[]): T[] {
  return db.prepare(sql).all(...params) as T[];
}

export function run(sql: string, ...params: any[]): void {
  db.prepare(sql).run(...params);
}

// ── Snapshot helpers ──────────────────────────────────────────────────────────

export function getSnapshot(queryKey: string): any {
  return getOne('SELECT * FROM snapshots WHERE query_key = ?', queryKey);
}

export function upsertSnapshot(queryKey: string, snapshotId: string, status: string, resultJson?: string) {
  db.exec(`
    INSERT INTO snapshots (query_key, snapshot_id, status, result_json, updated_at)
    VALUES ('${queryKey.replace(/'/g, "''")}', '${snapshotId}', '${status}', ${resultJson ? `'${resultJson.replace(/'/g, "''")}'` : 'NULL'}, datetime('now'))
    ON CONFLICT(query_key) DO UPDATE SET
      snapshot_id = excluded.snapshot_id,
      status = excluded.status,
      result_json = COALESCE(excluded.result_json, result_json),
      updated_at = excluded.updated_at
  `);
}

// ── Company helpers ───────────────────────────────────────────────────────────

export function getCompany(companyName: string): any {
  return getOne('SELECT * FROM companies WHERE company_name = ?', companyName);
}

export function upsertCompany(data: Record<string, any>) {
  const sanitized: Record<string, any> = {};
  for (const [k, v] of Object.entries(data)) {
    sanitized[k] = v === undefined ? null : v;
  }

  const cols = Object.keys(sanitized);
  const placeholders = cols.map(() => '?').join(', ');
  const updates = cols.filter(k => k !== 'company_name').map(k => `${k} = excluded.${k}`).join(', ');
  const values = cols.map(k => sanitized[k]);

  db.prepare(`
    INSERT INTO companies (${cols.join(', ')}) VALUES (${placeholders})
    ON CONFLICT(company_name) DO UPDATE SET ${updates}
  `).run(...values);
}
