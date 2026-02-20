// ── Scoring ──────────────────────────────────────────────────────────────────

const HIGH_VALUE_TITLES = ['lead', 'senior', 'staff', 'principal', 'architect', 'head', 'director', 'vp', 'cto', 'manager'];
const TECH_HIGH = ['react', 'python', 'typescript', 'node', 'aws', 'kubernetes', 'docker', 'ai', 'ml', 'devops', 'cloud', 'golang', 'rust'];
const TECH_MED = ['javascript', 'java', 'c#', '.net', 'angular', 'vue', 'sql', 'postgresql', 'mongodb', 'backend', 'frontend', 'fullstack'];
const TECH_LOW = ['wordpress', 'php', 'jquery', 'legacy'];

const GEO_TIER1 = ['denmark', 'danmark', 'copenhagen', 'aarhus', 'odense', 'aalborg', 'sweden', 'stockholm', 'norway', 'oslo', 'nordic', 'scandinavia'];
const GEO_TIER2 = ['iceland', 'reykjavik'];
const GEO_TIER3 = ['germany', 'deutschland', 'berlin', 'munich', 'hamburg'];
const GEO_TIER4 = ['netherlands', 'holland', 'amsterdam', 'belgium', 'brussels', 'benelux'];
const GEO_TIER5 = ['uk', 'london', 'manchester', 'edinburgh'];
const GEO_TIER6 = ['usa', 'new york', 'san francisco', 'boston'];
const GEO_TIER7 = ['france', 'paris'];

const HAS_DEV_TEAM = ['team', 'developers', 'engineers', 'dev team', 'engineering team', 'growing team', 'join our'];
const FUNDING_HIGH = ['seed', 'pre-seed', 'preseed', 'series a', 'startup', 'early stage', 'funded', 'venture'];
const FUNDING_MED = ['series b', 'growth stage', 'scale-up'];

const SOURCE_BONUS: Record<string, number> = { LinkedIn: 5, TheHub: 8, Jobindex: 10, 'IT-Jobbank': 10 };

export function scoreLead(job: { title: string; company: string; location: string; source: string }): { score: number; factors: string[] } {
  let score = 40;
  const factors: string[] = [];

  const title = (job.title || '').toLowerCase();
  const company = (job.company || '').toLowerCase();
  const location = (job.location || '').toLowerCase();
  const fullText = `${title} ${company} ${location}`;

  // Geographic priority
  const geoTiers = [
    { tier: GEO_TIER1, pts: 25 }, { tier: GEO_TIER2, pts: 22 }, { tier: GEO_TIER3, pts: 18 },
    { tier: GEO_TIER4, pts: 15 }, { tier: GEO_TIER5, pts: 12 }, { tier: GEO_TIER6, pts: 10 }, { tier: GEO_TIER7, pts: 8 },
  ];
  for (const { tier, pts } of geoTiers) {
    const match = tier.find(g => location.includes(g) || company.includes(g));
    if (match) { score += pts; factors.push(`geo:${match}`); break; }
  }

  // Dev team
  if (HAS_DEV_TEAM.some(kw => fullText.includes(kw))) { score += 10; factors.push('has_dev_team'); }

  // Funding
  const fh = FUNDING_HIGH.find(kw => fullText.includes(kw));
  if (fh) { score += 15; factors.push(`funding:${fh}`); }
  else {
    const fm = FUNDING_MED.find(kw => fullText.includes(kw));
    if (fm) { score += 8; factors.push(`funding:${fm}`); }
  }

  // Seniority
  const sen = HIGH_VALUE_TITLES.find(kw => title.includes(kw));
  if (sen) { score += 15; factors.push(`seniority:${sen}`); }

  // Tech stack
  for (const t of TECH_HIGH) { if (fullText.includes(t)) { score += 10; factors.push(`tech:${t}`); } }
  for (const t of TECH_MED) { if (fullText.includes(t)) { score += 5; factors.push(`tech:${t}`); } }
  for (const t of TECH_LOW) { if (fullText.includes(t)) { score -= 10; factors.push(`tech_low:${t}`); } }

  // Source bonus
  score += SOURCE_BONUS[job.source] || 0;

  return { score: Math.min(100, Math.max(0, score)), factors };
}

// ── Company name normalization ────────────────────────────────────────────────

export function normalizeCompany(name: string): string {
  return (name || '')
    .toLowerCase()
    .replace(/\s*(a\/s|aps|as|ab|gmbh|ltd|inc|llc|corp|i\/s)\s*$/i, '')
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// ── URL canonicalization ──────────────────────────────────────────────────────

export function canonicalizeUrl(url: string, source: string): string {
  if (!url) return '';

  // Make absolute
  let abs = url;
  if (!abs.startsWith('http')) {
    const src = (source || '').toLowerCase();
    if (src.includes('thehub')) abs = `https://thehub.io${url}`;
    else if (src.includes('jobindex')) abs = `https://www.jobindex.dk${url}`;
    else if (src.includes('it-jobbank')) abs = `https://www.it-jobbank.dk${url}`;
    else abs = url;
  }

  try {
    const u = new URL(abs);
    const host = u.hostname.toLowerCase();
    const path = u.pathname.replace(/\/$/, '');

    // LinkedIn job
    if (host.includes('linkedin.com') && path.includes('/jobs/view/')) {
      const m = path.match(/\/jobs\/view\/.*?(\d+)\b/);
      return m ? `https://www.linkedin.com/jobs/view/${m[1]}` : `https://${host}${path}`;
    }
    // TheHub
    if (host.includes('thehub.io') && path.startsWith('/jobs/')) {
      return `https://thehub.io${path}`;
    }
    // Jobindex / IT-Jobbank
    if ((host.includes('jobindex.dk') || host.includes('it-jobbank.dk')) && path === '/c') {
      const t = u.searchParams.get('t');
      return t ? `https://${host}/c?t=${t}` : `https://${host}${path}`;
    }
    return `https://${host}${path}`;
  } catch {
    return abs;
  }
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
    .toLowerCase()
    .trim()
    .replace(/&/g, 'and')
    .replace(/['".,()\/\\]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-');
}
