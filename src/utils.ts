// ── Staffing / agency blacklist ───────────────────────────────────────────────

export const STAFFING_AGENCIES = new Set([
  // International
  'randstad', 'adecco', 'manpower', 'manpowergroup', 'michaelpage', 'michael page',
  'robert half', 'hays', 'harvey nash', 'reed', 'experis', 'insight global',
  'kelly services', 'staffmark', 'spherion', 'allegis', 'aerotek', 'teksystems',
  'tek systems', 'ciber', 'kforce', 'volt', 'sungard',
  // Danish / Nordic
  'moment vikar', 'dancare', 'student consulting', 'academic work', 'team relation',
  'søndergaard', 'stegvad', 'komp', 'ama vikar', 'vikarhuset', 'propeople',
  'dfm consulting', 'people & performance', 'technation', 'scalepoint',
  'hr manager', 'hr-manager', 'jobmatch', 'talentsoft', 'recman', 'emply',
  'blue recruitment', 'nordic staffing', 'first agenda',
]);

// Agency keyword signals — company names containing these are likely agencies
const AGENCY_KEYWORDS = ['vikar', 'vikarbur', 'rekrutter', 'rekruttering', 'staffing',
  'personaleservice', 'personalekonsulent', 'headhunter', 'executive search', 'talent acquisition',
  'outplacement', 'hr bureau', 'hr-bureau', 'bureau', 'employment agency'];

// Role category keywords for diversity signal
const ROLE_CATEGORIES: Record<string, string[]> = {
  backend:   ['backend', 'back-end', 'back end', 'api', 'server', 'microservice', 'java', 'python', 'golang', 'rust', 'php', 'ruby', '.net', 'c#'],
  frontend:  ['frontend', 'front-end', 'front end', 'react', 'vue', 'angular', 'svelte', 'css', 'html', 'ui developer', 'javascript developer'],
  fullstack: ['fullstack', 'full-stack', 'full stack'],
  mobile:    ['ios', 'android', 'mobile', 'swift', 'kotlin', 'react native', 'flutter'],
  devops:    ['devops', 'dev-ops', 'sre', 'platform', 'infrastructure', 'cloud', 'kubernetes', 'docker', 'terraform', 'aws engineer', 'azure engineer'],
  ml:        ['machine learning', 'deep learning', 'nlp', 'computer vision', 'ai engineer', 'ml engineer', 'data scientist', 'llm'],
  data:      ['data engineer', 'data analyst', 'analytics', 'bi developer', 'business intelligence', 'dbt', 'spark', 'hadoop'],
  product:   ['product manager', 'product owner', 'product lead', 'cpo'],
  design:    ['designer', 'ux', 'ui/ux', 'design lead', 'figma'],
  qa:        ['qa', 'test', 'quality assurance', 'automation engineer', 'selenium', 'cypress'],
  security:  ['security', 'pen test', 'devsecops', 'infosec', 'cissp', 'soc analyst'],
};

export function detectRoleCategory(title: string): string | null {
  const t = (title || '').toLowerCase();
  for (const [cat, kws] of Object.entries(ROLE_CATEGORIES)) {
    if (kws.some(kw => t.includes(kw))) return cat;
  }
  return null;
}

// ── Company name normalization ─────────────────────────────────────────────────

export function normalizeCompanyName(name: string): string {
  return (name || '')
    .replace(/\b(a\/s|aps|as|ab|gmbh|ltd|inc|llc|corp|i\/s|nv|bv|plc|ag|se|oy|kb|ehf)\b/gi, '')
    .replace(/\b(holding|group|solutions?|technologies?|technology|consulting|consultancy|software|digital|services?|systems?|denmark|dk|nordic|scandinavia|international|global)\b/gi, '')
    .replace(/[|&\-_.,'"()[\]/\\]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Legacy alias (keep for backward compat)
export function normalizeCompany(name: string): string {
  return normalizeCompanyName(name).toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
}

export function isStaffingAgency(companyName: string): boolean {
  const norm = companyName.toLowerCase().replace(/[^a-z0-9\s]/g, '').trim();
  for (const agency of STAFFING_AGENCIES) {
    if (norm === agency || norm.includes(agency)) return true;
  }
  return AGENCY_KEYWORDS.some(kw => norm.includes(kw));
}

// ── Scoring ────────────────────────────────────────────────────────────────────

const HIGH_VALUE_TITLES = ['lead', 'senior', 'staff', 'principal', 'architect', 'head', 'director', 'vp', 'cto', 'manager'];
const TECH_HIGH = ['react', 'python', 'typescript', 'node', 'aws', 'kubernetes', 'docker', 'ai', 'ml', 'devops', 'cloud', 'golang', 'rust'];
const TECH_MED  = ['javascript', 'java', 'c#', '.net', 'angular', 'vue', 'sql', 'postgresql', 'mongodb', 'backend', 'frontend', 'fullstack'];
const TECH_LOW  = ['wordpress', 'php', 'jquery', 'legacy', 'cobol', 'mainframe'];

const GEO_TIER1 = ['denmark', 'danmark', 'copenhagen', 'aarhus', 'odense', 'aalborg', 'sweden', 'stockholm', 'norway', 'oslo', 'nordic', 'scandinavia'];
const GEO_TIER2 = ['iceland', 'reykjavik'];
const GEO_TIER3 = ['germany', 'deutschland', 'berlin', 'munich', 'hamburg'];
const GEO_TIER4 = ['netherlands', 'holland', 'amsterdam', 'belgium', 'brussels', 'benelux'];
const GEO_TIER5 = ['uk', 'london', 'manchester', 'edinburgh'];
const GEO_TIER6 = ['usa', 'new york', 'san francisco', 'boston'];
const GEO_TIER7 = ['france', 'paris'];

const HAS_DEV_TEAM = ['team', 'developers', 'engineers', 'dev team', 'engineering team', 'growing team', 'join our'];
const FUNDING_HIGH = ['seed', 'pre-seed', 'preseed', 'series a', 'startup', 'early stage', 'funded', 'venture'];
const FUNDING_MED  = ['series b', 'growth stage', 'scale-up'];

const SOURCE_BONUS: Record<string, number> = { LinkedIn: 5, TheHub: 8, Jobindex: 10, 'IT-Jobbank': 10 };

interface ScoreInput {
  title: string;
  company: string;
  location: string;
  source: string;
  scraped_at?: string;
  // Optional enrichments from company-level analysis
  multiSourceBonus?: number;
  roleDiversityBonus?: number;
}

export function scoreLead(job: ScoreInput): { score: number; factors: string[] } {
  let score = 40;
  const factors: string[] = [];

  const title    = (job.title    || '').toLowerCase();
  const company  = (job.company  || '').toLowerCase();
  const location = (job.location || '').toLowerCase();
  const fullText = `${title} ${company} ${location}`;

  // ── 1. Staffing agency / blacklist (early exit) ─────────────────────────────
  if (isStaffingAgency(job.company)) {
    return { score: 5, factors: ['staffing_agency'] };
  }

  // ── 2. Agency keyword detection ────────────────────────────────────────────
  const agencyKw = AGENCY_KEYWORDS.find(kw => company.includes(kw));
  if (agencyKw) { score -= 30; factors.push('agency_keyword'); }

  // ── 3. Recency weighting ───────────────────────────────────────────────────
  if (job.scraped_at) {
    const ageMs = Date.now() - new Date(job.scraped_at).getTime();
    const ageDays = ageMs / (1000 * 60 * 60 * 24);
    if (ageDays <= 7)  { score += 10; factors.push('recent'); }
    else if (ageDays <= 30) { score += 5;  factors.push('active'); }
    else if (ageDays > 60)  { score -= 10; factors.push('stale'); }
  }

  // ── 4. Geographic priority ─────────────────────────────────────────────────
  const geoTiers = [
    { tier: GEO_TIER1, pts: 25 }, { tier: GEO_TIER2, pts: 22 }, { tier: GEO_TIER3, pts: 18 },
    { tier: GEO_TIER4, pts: 15 }, { tier: GEO_TIER5, pts: 12 }, { tier: GEO_TIER6, pts: 10 }, { tier: GEO_TIER7, pts: 8 },
  ];
  for (const { tier, pts } of geoTiers) {
    const match = tier.find(g => location.includes(g) || company.includes(g));
    if (match) { score += pts; factors.push(`geo:${match}`); break; }
  }

  // ── 5. Dev team signals ────────────────────────────────────────────────────
  if (HAS_DEV_TEAM.some(kw => fullText.includes(kw))) { score += 10; factors.push('has_dev_team'); }

  // ── 6. Funding ─────────────────────────────────────────────────────────────
  const fh = FUNDING_HIGH.find(kw => fullText.includes(kw));
  if (fh) { score += 15; factors.push(`funding:${fh}`); }
  else {
    const fm = FUNDING_MED.find(kw => fullText.includes(kw));
    if (fm) { score += 8; factors.push(`funding:${fm}`); }
  }

  // ── 7. Seniority ───────────────────────────────────────────────────────────
  const sen = HIGH_VALUE_TITLES.find(kw => title.includes(kw));
  if (sen) { score += 15; factors.push(`seniority:${sen}`); }

  // ── 8. Tech stack ──────────────────────────────────────────────────────────
  for (const t of TECH_HIGH) { if (fullText.includes(t)) { score += 10; factors.push(`tech:${t}`); break; } }
  for (const t of TECH_MED)  { if (fullText.includes(t)) { score += 5;  factors.push(`tech:${t}`); break; } }
  for (const t of TECH_LOW)  { if (fullText.includes(t)) { score -= 10; factors.push(`tech_low:${t}`); break; } }

  // ── 9. Source bonus ────────────────────────────────────────────────────────
  score += SOURCE_BONUS[job.source] || 0;

  // ── 10. Company-level signals (injected by phase1 after grouping) ──────────
  if (job.multiSourceBonus) {
    score += job.multiSourceBonus;
    factors.push(job.multiSourceBonus >= 30 ? 'multi_source_strong' : 'multi_source');
  }
  if (job.roleDiversityBonus) {
    score += job.roleDiversityBonus;
    factors.push('role_diversity');
  }

  return { score: Math.min(100, Math.max(0, score)), factors };
}

// ── Company-level signal enrichment (called after all jobs collected) ─────────

export function enrichWithCompanySignals(
  jobs: Array<{ company: string; title: string; source: string; scraped_at?: string }>
): Map<string, { multiSourceBonus: number; roleDiversityBonus: number }> {

  const byCompany = new Map<string, { sources: Set<string>; categories: Set<string> }>();

  for (const job of jobs) {
    const key = normalizeCompany(job.company);
    if (!byCompany.has(key)) byCompany.set(key, { sources: new Set(), categories: new Set() });
    const entry = byCompany.get(key)!;
    if (job.source) entry.sources.add(job.source);
    const cat = detectRoleCategory(job.title);
    if (cat) entry.categories.add(cat);
  }

  const result = new Map<string, { multiSourceBonus: number; roleDiversityBonus: number }>();

  for (const [key, { sources, categories }] of byCompany) {
    const multiSourceBonus = sources.size >= 3 ? 30 : sources.size === 2 ? 20 : 0;
    const roleDiversityBonus = categories.size >= 3 ? 15 : 0;
    if (multiSourceBonus > 0 || roleDiversityBonus > 0) {
      result.set(key, { multiSourceBonus, roleDiversityBonus });
    }
  }

  return result;
}

// ── URL canonicalization ──────────────────────────────────────────────────────

export function canonicalizeUrl(url: string, source: string): string {
  if (!url) return '';
  let abs = url;
  if (!abs.startsWith('http')) {
    const src = (source || '').toLowerCase();
    if (src.includes('thehub'))      abs = `https://thehub.io${url}`;
    else if (src.includes('jobindex')) abs = `https://www.jobindex.dk${url}`;
    else if (src.includes('it-jobbank')) abs = `https://www.it-jobbank.dk${url}`;
    else abs = url;
  }
  try {
    const u = new URL(abs);
    const host = u.hostname.toLowerCase();
    const path = u.pathname.replace(/\/$/, '');
    if (host.includes('linkedin.com') && path.includes('/jobs/view/')) {
      const m = path.match(/\/jobs\/view\/.*?(\d+)\b/);
      return m ? `https://www.linkedin.com/jobs/view/${m[1]}` : `https://${host}${path}`;
    }
    if (host.includes('thehub.io') && path.startsWith('/jobs/')) return `https://thehub.io${path}`;
    if ((host.includes('jobindex.dk') || host.includes('it-jobbank.dk')) && path === '/c') {
      const t = u.searchParams.get('t');
      return t ? `https://${host}/c?t=${t}` : `https://${host}${path}`;
    }
    return `https://${host}${path}`;
  } catch { return abs; }
}

// ── Deduplication ─────────────────────────────────────────────────────────────

export function deduplicateJobs<T extends { company: string; title: string; canonical_url?: string }>(jobs: T[]): T[] {
  const seen = new Set<string>();
  return jobs.filter(j => {
    const key = `${normalizeCompany(j.company)}|${(j.title || '').toLowerCase().trim()}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// ── Sleep ─────────────────────────────────────────────────────────────────────

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Text cleaning ─────────────────────────────────────────────────────────────

export function cleanText(s: string, maxLen = 800): string {
  if (!s) return s;
  return String(s)
    .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '')
    .replace(/[\u0000-\u001F\u007F]/g, '')
    .slice(0, maxLen);
}

// ── LinkedIn slug helpers ─────────────────────────────────────────────────────

export function extractLinkedInCompanySlug(url: string): string {
  const m = (url || '').match(/linkedin\.com\/company\/([^/?#\s]+)/i);
  return m ? m[1].toLowerCase() : '';
}

export function extractLinkedInPersonSlug(url: string): string {
  const m = (url || '').match(/(?:[a-z]{2,3}\.)?linkedin\.com\/in\/([^/?#\s]+)/i);
  return m ? m[1].toLowerCase() : '';
}

export function guessLinkedInCompanySlug(name: string): string {
  return (name || '')
    .toLowerCase().trim()
    .replace(/&/g, 'and')
    .replace(/['".,()\/\\]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-');
}
