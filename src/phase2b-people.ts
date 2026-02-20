import axios from 'axios';
import fs from 'fs';
import path from 'path';
import { db, getSnapshot, upsertSnapshot, getAll, getOne } from './db';
import { sleep, cleanText } from './utils';
import type { RunFilters, LogFn } from './types';

const BRIGHTDATA_KEY = process.env.BRIGHTDATA_API_KEY!;
const FIRECRAWL_KEY = process.env.FIRECRAWL_API_KEY!;

interface Connection {
  firstName: string;
  lastName: string;
  company: string;
  linkedinUrl: string;
}

// ── Load connections from CSV ─────────────────────────────────────────────────

function loadConnections(): Connection[] {
  const csvPath = path.join(process.cwd(), 'data', 'connections.csv');
  if (!fs.existsSync(csvPath)) {
    console.warn('  No connections.csv found at data/connections.csv — skipping connection matching');
    console.warn('  Export from LinkedIn: Settings → Data privacy → Get a copy of your data → Connections');
    return [];
  }

  const raw = fs.readFileSync(csvPath, 'utf-8');
  const lines = raw.split('\n').filter(Boolean);
  const connections: Connection[] = [];

  // Skip header lines (LinkedIn CSV has some preamble)
  let headerIdx = lines.findIndex(l => l.toLowerCase().includes('first name'));
  if (headerIdx < 0) headerIdx = 0;

  const headers = lines[headerIdx].toLowerCase().split(',').map(h => h.replace(/"/g, '').trim());
  const firstNameIdx = headers.findIndex(h => h.includes('first name'));
  const lastNameIdx = headers.findIndex(h => h.includes('last name'));
  const companyIdx = headers.findIndex(h => h.includes('company'));
  const urlIdx = headers.findIndex(h => h.includes('url') || h.includes('profile'));

  for (const line of lines.slice(headerIdx + 1)) {
    const cols = line.split(',').map(c => c.replace(/"/g, '').trim());
    const firstName = cols[firstNameIdx] || '';
    const lastName = cols[lastNameIdx] || '';
    const company = cols[companyIdx] || '';
    const url = cols[urlIdx] || '';
    if (firstName || lastName) {
      connections.push({ firstName, lastName, company, linkedinUrl: url });
    }
  }

  console.log(`  Loaded ${connections.length} connections from CSV`);
  return connections;
}

// ── Connection matching ───────────────────────────────────────────────────────

function normalizeForMatch(s: string): string {
  return (s || '')
    .toLowerCase()
    .replace(/\s*(a\/s|aps|as|ab|gmbh|ltd|inc|llc)\s*$/i, '')
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractSlug(url: string): string {
  const m = (url || '').match(/(?:[a-z]{2,3}\.)?linkedin\.com\/in\/([^/?#\s]+)/i);
  return m ? m[1].toLowerCase() : '';
}

function matchConnections(
  targetName: string,
  targetLinkedinUrl: string,
  allCompanies: string[],
  connections: Connection[]
): { found: boolean; type: string; score: number; matchedRow: Connection | null } {
  const targetSlug = extractSlug(targetLinkedinUrl);
  const targetNameNorm = normalizeForMatch(targetName);
  const allCompaniesNorm = allCompanies.map(normalizeForMatch);

  // 1. DIRECT_LINKEDIN_MATCH
  if (targetSlug) {
    const direct = connections.find(c => extractSlug(c.linkedinUrl) === targetSlug);
    if (direct) return { found: true, type: 'DIRECT_LINKEDIN_MATCH', score: 100, matchedRow: direct };
  }

  // 2. NAME_COMPANY_MATCH
  for (const conn of connections) {
    const connName = normalizeForMatch(`${conn.firstName} ${conn.lastName}`);
    const connCompany = normalizeForMatch(conn.company);
    if (connName === targetNameNorm && allCompaniesNorm.some(c => c && connCompany && (c.includes(connCompany) || connCompany.includes(c)))) {
      return { found: true, type: 'NAME_COMPANY_MATCH', score: 90, matchedRow: conn };
    }
  }

  // 3. NAME_ONLY_MATCH
  for (const conn of connections) {
    const connName = normalizeForMatch(`${conn.firstName} ${conn.lastName}`);
    if (connName === targetNameNorm && targetNameNorm.length > 3) {
      return { found: true, type: 'NAME_ONLY_MATCH', score: 80, matchedRow: conn };
    }
  }

  // 4. COMPANY_COLLEAGUE_MATCH
  for (const conn of connections) {
    const connCompany = normalizeForMatch(conn.company);
    if (allCompaniesNorm.some(c => c && connCompany && (c.includes(connCompany) || connCompany.includes(c)))) {
      return { found: true, type: 'COMPANY_COLLEAGUE_MATCH', score: 70, matchedRow: conn };
    }
  }

  return { found: false, type: 'NO_MATCH', score: 0, matchedRow: null };
}

// ── BrightData SERP: personal LinkedIn URL ───────────────────────────────────

async function triggerPersonSerpSnapshot(keyword: string): Promise<string> {
  const res = await axios.post(
    'https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_mfz5x93lmsjjjylob&include_errors=true',
    [{ url: 'https://www.google.com/', keyword, language: 'da-DK', country: 'DK', start_page: 1, end_page: 1 }],
    { headers: { Authorization: `Bearer ${BRIGHTDATA_KEY}`, 'Content-Type': 'application/json' } }
  );
  return res.data?.snapshot_id || res.data?.id || '';
}

async function pollReady(snapshotId: string, maxAttempts = 60): Promise<boolean> {
  for (let i = 0; i < maxAttempts; i++) {
    await sleep(5000);
    const res = await axios.get(
      `https://api.brightdata.com/datasets/v3/progress/${snapshotId}`,
      { headers: { Authorization: `Bearer ${BRIGHTDATA_KEY}` } }
    );
    if (res.data?.status === 'ready') return true;
    if (res.data?.status === 'failed') return false;
  }
  return false;
}

async function downloadSnapshot(snapshotId: string): Promise<any[]> {
  const res = await axios.get(
    `https://api.brightdata.com/datasets/v3/snapshot/${snapshotId}?format=json`,
    { headers: { Authorization: `Bearer ${BRIGHTDATA_KEY}` } }
  );
  return Array.isArray(res.data) ? res.data : [res.data];
}

function extractPersonLinkedInUrl(organic: any[], originalName: string, companyName: string): { url: string; slug: string; confidence: string } {
  const slugFromUrl = (url: string): string => {
    const m = (url || '').match(/(?:[a-z]{2,3}\.)?linkedin\.com\/in\/([^/?#\s]+)/i);
    return m ? m[1].toLowerCase() : '';
  };

  for (let i = 0; i < organic.length; i++) {
    const link = organic[i]?.link || organic[i]?.url || '';
    const slug = slugFromUrl(link);
    if (slug) {
      return {
        url: `https://www.linkedin.com/in/${slug}`,
        slug,
        confidence: i === 0 ? 'HIGH' : 'MEDIUM',
      };
    }
  }

  // Fallback: guess from name
  const guessedSlug = originalName
    .toLowerCase().trim()
    .replace(/&/g, 'and').replace(/['".,()]/g, '').replace(/\s+/g, '-').replace(/-+/g, '-');
  return { url: `https://www.linkedin.com/in/${guessedSlug}`, slug: guessedSlug, confidence: 'GUESSED' };
}

async function findPersonLinkedInUrl(name: string, companyName: string): Promise<{ url: string; slug: string; confidence: string }> {
  const queryKey = `person:${name.toLowerCase()}:${companyName.toLowerCase()}`;
  const cached = getSnapshot(queryKey);

  if (cached?.status === 'ready' && cached.result_json) {
    return JSON.parse(cached.result_json);
  }

  const keyword = `site:linkedin.com/in "${name}" "${companyName}"`;

  try {
    const snapshotId = await triggerPersonSerpSnapshot(keyword);
    upsertSnapshot(queryKey, snapshotId, 'pending');

    const ready = await pollReady(snapshotId);
    if (!ready) throw new Error('Snapshot timed out');

    const results = await downloadSnapshot(snapshotId);
    const organic = results[0]?.organic || [];
    const result = extractPersonLinkedInUrl(organic, name, companyName);

    upsertSnapshot(queryKey, snapshotId, 'ready', JSON.stringify(result));
    return result;
  } catch (err: any) {
    const guessedSlug = name.toLowerCase().trim().replace(/\s+/g, '-');
    return { url: `https://www.linkedin.com/in/${guessedSlug}`, slug: guessedSlug, confidence: 'GUESSED' };
  }
}

// ── Firecrawl: extract work history ──────────────────────────────────────────

async function extractWorkHistory(name: string, company: string, linkedinUrl: string): Promise<{ allCompanies: string[]; currentTitle: string | null; currentCompany: string | null }> {
  if (!FIRECRAWL_KEY || linkedinUrl.includes('GUESSED')) {
    return { allCompanies: [company], currentTitle: null, currentCompany: company };
  }

  try {
    const res = await axios.post('https://api.firecrawl.dev/v1/agent', {
      prompt: `Extract the work experience of ${name} from their LinkedIn profile. They work at ${company}.`,
      urls: [linkedinUrl],
      model: 'spark-1-mini',
    }, {
      headers: { Authorization: `Bearer ${FIRECRAWL_KEY}`, 'Content-Type': 'application/json' },
      timeout: 60000,
    });

    const data = res.data?.data || res.data || {};
    const experience = data.work_experience || [];
    const allCompanies = [...new Set([
      company,
      ...experience.map((e: any) => e.organization_name || e.company || '').filter(Boolean),
    ])] as string[];

    return {
      allCompanies,
      currentTitle: data.current_title || null,
      currentCompany: data.current_company || company,
    };
  } catch (err: any) {
    console.warn(`    Firecrawl failed for ${name}: ${err.message}`);
    return { allCompanies: [company], currentTitle: null, currentCompany: company };
  }
}

// ── Main phase 2B ─────────────────────────────────────────────────────────────

export async function runPhase2b(filters: RunFilters = {}, logFn: LogFn = console.log): Promise<void> {
  const eeRisks   = (filters.eeRisk ?? 'LOW,MEDIUM').split(',').map(s => s.trim()).filter(Boolean);
  const limit     = filters.limit ?? 100;
  const forceRefresh = filters.forceRefresh ?? false;

  logFn(`=== Phase 2B: People mapping ===`);
  logFn(`Filters: eeRisk=${eeRisks.join(',')} limit=${limit}`);

  const connections = loadConnections();
  logFn(`Loaded ${connections.length} connections`);

  const eeRiskList = eeRisks.map(r => `'${r}'`).join(',');

  const companies = getAll(`
    SELECT * FROM companies
    WHERE ee_risk IN (${eeRiskList})
    AND (ceo_name IS NOT NULL OR cto_name IS NOT NULL)
    ORDER BY priority_score DESC
    LIMIT ${limit}
  `);

  logFn(`Processing ${companies.length} companies for people mapping`);

  const insertTarget = db.prepare(`
    INSERT INTO targets (company_name, target_name, target_role, target_title, target_linkedin_url,
      connection_found, connection_type, connection_score, outreach_strategy, all_companies, search_query)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  for (const company of companies) {
    const targets: Array<{ name: string; role: string; title: string | null }> = [];

    // Prefer CTO for tech outreach, fallback to CEO
    if (company.cto_name) targets.push({ name: company.cto_name, role: 'CTO', title: company.cto_title });
    if (company.ceo_name) targets.push({ name: company.ceo_name, role: 'CEO', title: company.ceo_title });

    for (const target of targets) {
      // Check if already processed
      if (!forceRefresh) {
        const existing = getOne('SELECT id FROM targets WHERE company_name = ? AND target_name = ?', company.company_name, target.name);
        if (existing) continue;
      }

      logFn(`  → ${company.company_name} / ${target.name} (${target.role})`);

      try {
        // Find personal LinkedIn URL
        const { url: linkedinUrl, confidence } = await findPersonLinkedInUrl(target.name, company.company_name);

        // Extract work history via Firecrawl
        const { allCompanies } = await extractWorkHistory(target.name, company.company_name, linkedinUrl);

        // Connection matching
        const match = matchConnections(target.name, linkedinUrl, allCompanies, connections);

        const outreachStrategy = match.score >= 90 ? 'WARM_INTRO'
          : match.score >= 70 ? 'WARM_REFERENCE'
          : 'COLD_PERSONALIZED';

        insertTarget.run(
          company.company_name,
          target.name,
          target.role,
          target.title || target.role,
          linkedinUrl,
          match.found ? 1 : 0,
          match.type,
          match.score,
          outreachStrategy,
          JSON.stringify(allCompanies),
          `site:linkedin.com/in "${target.name}" "${company.company_name}"`,
        );

        logFn(`    ✓ ${match.type} (score:${match.score}) → ${outreachStrategy}`);
        await sleep(1000);
      } catch (err: any) {
        logFn(`    ✗ ${target.name}: ${err.message}`);
      }
    }
  }

  const total = getOne('SELECT COUNT(*) as c FROM targets') as any;
  const warm  = getOne("SELECT COUNT(*) as c FROM targets WHERE connection_found = 1") as any;
  logFn(`Phase 2B done: ${total?.c ?? 0} targets mapped (${warm?.c ?? 0} with connections)`);
}
