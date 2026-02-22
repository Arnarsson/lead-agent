import axios from 'axios';
import { getIcpDb, getSnapshot, upsertSnapshot, upsertCompany, getAll, getOne } from './db';
import { sleep, extractLinkedInCompanySlug, guessLinkedInCompanySlug, cleanText } from './utils';
import type { RunFilters, LogFn } from './types';

const BRIGHTDATA_KEY = process.env.BRIGHTDATA_API_KEY!;
const OPENROUTER_KEY = process.env.OPENROUTER_API_KEY!;

// ── BrightData SERP polling ───────────────────────────────────────────────────

async function triggerSerpSnapshot(keyword: string): Promise<string> {
  const res = await axios.post(
    'https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_mfz5x93lmsjjjylob&include_errors=true',
    [{ url: 'https://www.google.com/', keyword, language: 'da-DK', country: 'DK', start_page: 1, end_page: 1 }],
    { headers: { Authorization: `Bearer ${BRIGHTDATA_KEY}`, 'Content-Type': 'application/json' } }
  );
  return res.data?.snapshot_id || res.data?.id || '';
}

async function pollSnapshotReady(snapshotId: string, maxAttempts = 60): Promise<boolean> {
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

// ── SERP → LinkedIn company URL ───────────────────────────────────────────────

async function findLinkedInCompanyUrl(d: ReturnType<typeof getIcpDb>, companyName: string): Promise<{ slug: string; url: string; confidence: string }> {
  const queryKey = `company:${companyName.toLowerCase()}`;
  const cached = getSnapshot(d, queryKey);

  if (cached?.status === 'ready' && cached.result_json) {
    const result = JSON.parse(cached.result_json);
    return result;
  }

  const keyword = `${companyName} site:linkedin.com/company`;
  let snapshotId = cached?.snapshot_id;

  if (!snapshotId) {
    try {
      snapshotId = await triggerSerpSnapshot(keyword);
      upsertSnapshot(d, queryKey, snapshotId, 'pending');
    } catch (err: any) {
      console.warn(`SERP trigger failed for ${companyName}: ${err.message}`);
      const slug = guessLinkedInCompanySlug(companyName);
      return { slug, url: `https://www.linkedin.com/company/${slug}`, confidence: 'GUESSED' };
    }
  }

  const ready = await pollSnapshotReady(snapshotId);
  if (!ready) {
    const slug = guessLinkedInCompanySlug(companyName);
    return { slug, url: `https://www.linkedin.com/company/${slug}`, confidence: 'GUESSED' };
  }

  const results = await downloadSnapshot(snapshotId);
  const organic = results[0]?.organic || [];

  let slug = '';
  let confidence = 'FAILED';
  for (let i = 0; i < organic.length; i++) {
    const link = organic[i]?.link || organic[i]?.url || '';
    slug = extractLinkedInCompanySlug(link);
    if (slug) { confidence = i === 0 ? 'HIGH' : 'MEDIUM'; break; }
  }

  if (!slug) {
    slug = guessLinkedInCompanySlug(companyName);
    confidence = 'GUESSED';
  }

  const result = { slug, url: `https://www.linkedin.com/company/${slug}`, confidence };
  upsertSnapshot(d, queryKey, snapshotId, 'ready', JSON.stringify(result));
  return result;
}

// ── BrightData LinkedIn company scrape ───────────────────────────────────────

async function scrapeLinkedInCompany(linkedinUrl: string): Promise<any> {
  const res = await axios.post(
    'https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_l1vikfnt1wgvvqz95w&include_errors=true',
    [{ url: linkedinUrl }],
    { headers: { Authorization: `Bearer ${BRIGHTDATA_KEY}`, 'Content-Type': 'application/json' } }
  );
  const snapshotId = res.data?.snapshot_id || res.data?.id || '';
  if (!snapshotId) return null;

  await sleep(2000);
  const ready = await pollSnapshotReady(snapshotId, 30);
  if (!ready) return null;

  const results = await downloadSnapshot(snapshotId);
  return results[0] || null;
}

// ── Hiring signals detection ──────────────────────────────────────────────────

const SIGNAL_KEYWORDS: Record<string, { en: string[]; da: string[] }> = {
  urgent: {
    en: ['asap', 'immediately', 'urgent', 'start asap', 'urgently hiring', 'immediate start', 'right away'],
    da: ['hurtigst muligt', 'omgående', 'akut', 'straks', 'snarest'],
  },
  growth: {
    en: ["we're growing", 'expanding team', 'scaling', 'new team', 'hiring', 'recruiting', 'join our team',
      'we are hiring', 'looking for', 'seeking', 'open position', 'new role', 'growth mode',
      'expanding', 'building our team', 'come join', 'opportunity'],
    da: ['vi udvider', 'vokser', 'ansætter', 'søger', 'nye medarbejdere', 'udvider teamet',
      'vækst', 'nye kolleger', 'er du vores', 'kom og vær', 'ledig stilling'],
  },
  multiple_roles: {
    en: ['multiple positions', 'several roles', 'many openings', 'various positions', 'multiple openings',
      'numerous roles', 'hiring spree', 'mass hiring', 'bulk hiring', 'team expansion'],
    da: ['flere stillinger', 'mange stillinger', 'forskellige roller', 'flere positioner'],
  },
  leadership: {
    en: ['head of', 'director', 'vp ', 'vice president', 'chief', 'lead ', 'principal', 'staff ',
      'senior ', 'manager', 'team lead', 'engineering lead', 'tech lead'],
    da: ['chef for', 'leder', 'direktør', 'ansvarlig', 'teamleder', 'afdelingsleder'],
  },
};

function detectHiringSignals(posts: any[]): { signals: string[]; snippets: string[]; temperature: string } {
  const allSignals: string[] = [];
  const allSnippets: string[] = [];

  for (const post of posts) {
    const text = (post.text || '').toLowerCase();
    for (const [category, kws] of Object.entries(SIGNAL_KEYWORDS)) {
      const allKws = [...kws.en, ...kws.da];
      for (const kw of allKws) {
        if (text.includes(kw.toLowerCase())) {
          if (!allSignals.includes(category)) allSignals.push(category);
          const idx = text.indexOf(kw.toLowerCase());
          const start = Math.max(0, idx - 50);
          const end = Math.min(text.length, idx + kw.length + 50);
          const snippet = post.text.substring(start, end).trim();
          if (allSnippets.length < 3 && !allSnippets.some(s => s.includes(kw))) {
            allSnippets.push(`...${snippet}...`);
          }
          break;
        }
      }
    }
  }

  const hasUrgent = allSignals.includes('urgent');
  const growthCount = allSignals.filter(s => s === 'growth').length;
  const hasMultiple = allSignals.includes('multiple_roles');
  const hasLeadership = allSignals.includes('leadership');

  let temperature = 'COLD';
  if (hasUrgent || growthCount >= 2 || hasMultiple) temperature = 'HOT';
  else if (growthCount >= 1 || allSignals.length >= 2 || hasLeadership) temperature = 'WARM';

  return { signals: [...new Set(allSignals)], snippets: allSnippets.slice(0, 3), temperature };
}

// ── Outreach angle generation ─────────────────────────────────────────────────

const ANGLE_TEMPLATES: Record<string, string[]> = {
  urgent: [
    'Jeg så jeres opslag om at I søger folk med det samme — hvad er den største udfordring i at finde de rette kandidater hurtigt?',
    'I nævnte at I ansætter akut — har I overvejet hvordan en rekrutteringspartner kunne speede processen op?',
  ],
  growth: [
    'Tillykke med væksten — så jeres post om at udvide teamet. Hvordan prioriterer I de næste hires?',
    'Jeg lagde mærke til jeres vækst — hvad er den største flaskehals i rekrutteringen lige nu?',
    'Så I er i gang med at skalere teamet — er der bestemte profiler der er særligt svære at finde?',
  ],
  multiple_roles: [
    'Så I nævnte flere åbne stillinger — hvad er strategien for at tiltrække de rette kandidater til dem alle?',
    'Med så mange roller åbne, hvordan sikrer I at kvaliteten ikke lider under mængden?',
  ],
  leadership: [
    'Jeg så I søger en lederstilling — disse roller kræver ofte en anden tilgang. Hvordan griber I det an?',
    'En chef-stilling er kritisk at ramme rigtigt — hvad er jeres vigtigste kriterier?',
  ],
  default: [
    'Jeg så jeres aktivitet på LinkedIn og tænkte vi måske kunne hjælpe med jeres rekruttering.',
    'Er I tilfredse med jeres nuværende rekrutteringsproces, eller er der plads til forbedring?',
  ],
};

function generateOutreachAngles(signals: string[], temperature: string): string[] {
  const angles: string[] = [];
  const used = new Set<string>();
  const priority = temperature === 'HOT'
    ? ['urgent', 'growth', 'multiple_roles', 'leadership']
    : ['growth', 'leadership', 'multiple_roles', 'urgent'];

  for (const cat of priority) {
    if (signals.includes(cat) && !used.has(cat)) {
      const templates = ANGLE_TEMPLATES[cat];
      angles.push(templates[Math.floor(Math.random() * templates.length)]);
      used.add(cat);
      if (angles.length >= 2) break;
    }
  }

  if (signals.length > 0 && angles.length < 2) {
    const extras = ANGLE_TEMPLATES.growth.filter(a => !angles.includes(a));
    if (extras.length > 0) angles.push(extras[0]);
  }
  if (angles.length === 0) angles.push(ANGLE_TEMPLATES.default[0]);
  return angles.slice(0, 3);
}

// ── Location / EE detection ───────────────────────────────────────────────────

const EE_COUNTRIES = new Set([
  'Poland', 'Ukraine', 'Romania', 'Bulgaria', 'Czech Republic', 'Hungary', 'Slovakia',
  'Croatia', 'Serbia', 'Estonia', 'Latvia', 'Lithuania', 'Greece',
]);

const EE_COMPANY_NAMES = [
  // Known EE/offshore-heavy consultancies
  'netcompany', 'epam', 'luxoft', 'softserve', 'intellias', 'ciklum', 'dataart',
  'eleks', 'devoteam', 'itransition', 'altkom', 'comarch', 'globallogic',
  // Indian IT / offshore majors
  'infosys', 'wipro', 'tcs', 'hcl', 'cognizant', 'mphasis', 'hexaware', 'virtusa',
  'niit', 'zensar', 'persistent', 'mindtree', 'mastech', 'igate',
  'l&t infotech', 'ltimindtree', 'tech mahindra',
  // Eastern European / global SI
  'atos', 'capgemini', 'sopra steria', 'cgi', 'gft', 'nnit', 'tietoevry',
  'unison', 'rackspace', 'dxc', 'ntt', 'fujitsu', 'logica',
  // Nordic outsourcers
  'dfind', 'k-profile', 'itera', 'bouvet', 'bekk',
];

const CITY_TO_COUNTRY: Record<string, string> = {
  copenhagen: 'Denmark', aarhus: 'Denmark', odense: 'Denmark', aalborg: 'Denmark',
  stockholm: 'Sweden', gothenburg: 'Sweden', oslo: 'Norway', helsinki: 'Finland',
  berlin: 'Germany', munich: 'Germany', amsterdam: 'Netherlands', brussels: 'Belgium',
  paris: 'France', london: 'UK', dublin: 'Ireland', zurich: 'Switzerland',
  madrid: 'Spain', lisbon: 'Portugal', milan: 'Italy',
  warsaw: 'Poland', krakow: 'Poland', kyiv: 'Ukraine', bucharest: 'Romania',
  sofia: 'Bulgaria', prague: 'Czech Republic', budapest: 'Hungary',
  bratislava: 'Slovakia', zagreb: 'Croatia', belgrade: 'Serbia',
  tallinn: 'Estonia', riga: 'Latvia', vilnius: 'Lithuania', athens: 'Greece',
  bangalore: 'India', mumbai: 'India', 'new york': 'USA', 'san francisco': 'USA',
};

function parseLocations(data: any): string[] {
  const locs = Array.isArray(data.locations) ? data.locations : [];
  const hq = data.headquarters ? [data.headquarters] : [];
  const about = data.about || data.description || '';
  const blob = [...locs, ...hq].join(' | ').toLowerCase() + ' ' + about.toLowerCase();

  const found = new Set<string>();

  for (const [city, country] of Object.entries(CITY_TO_COUNTRY)) {
    if (blob.includes(city)) found.add(country);
  }

  // Direct country names
  const countries = ['denmark', 'sweden', 'norway', 'finland', 'germany', 'netherlands',
    'belgium', 'france', 'uk', 'ireland', 'switzerland', 'austria', 'spain', 'portugal',
    'italy', 'poland', 'ukraine', 'romania', 'bulgaria', 'czech republic', 'hungary',
    'slovakia', 'croatia', 'serbia', 'estonia', 'latvia', 'lithuania', 'greece',
    'usa', 'canada', 'india', 'china'];
  for (const c of countries) {
    if (blob.includes(c)) {
      found.add(c.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '));
    }
  }

  return [...found];
}

function assessEERisk(companyName: string, countries: string[]): { ee_risk: string; ee_action: string; ee_note: string; ee_is_known_outsourcer: boolean } {
  const nameLower = companyName.toLowerCase();
  const isKnown = EE_COMPANY_NAMES.some(n => nameLower.includes(n));
  const eeFound = countries.filter(c => EE_COUNTRIES.has(c));

  if (isKnown) return { ee_risk: 'HIGH', ee_action: 'SKIP', ee_note: 'Known EE consultancy', ee_is_known_outsourcer: true };
  if (eeFound.length >= 2) return { ee_risk: 'HIGH', ee_action: 'SKIP', ee_note: `Multiple EE offices: ${eeFound.join(', ')}`, ee_is_known_outsourcer: false };
  if (eeFound.length === 1) return { ee_risk: 'MEDIUM', ee_action: 'REVIEW', ee_note: `EE country detected: ${eeFound[0]}`, ee_is_known_outsourcer: false };
  return { ee_risk: 'LOW', ee_action: 'PROCEED', ee_note: 'No EE offices found', ee_is_known_outsourcer: false };
}

// ── Perplexity Sonar helpers ──────────────────────────────────────────────────

async function perplexityQuery(prompt: string, model = 'perplexity/sonar-pro'): Promise<string> {
  const res = await axios.post('https://openrouter.ai/api/v1/chat/completions', {
    model,
    messages: [{ role: 'user', content: prompt }],
  }, {
    headers: { Authorization: `Bearer ${OPENROUTER_KEY}`, 'Content-Type': 'application/json' },
    timeout: 60000,
  });
  return res.data?.choices?.[0]?.message?.content || '';
}

function extractName(content: string, label: string): string | null {
  const nameMatch = content.match(/name:\s*([^\n\[]+)/i);
  if (nameMatch) {
    const name = nameMatch[1].replace(/\[[\d]+\]/g, '').replace(/\*+/g, '').trim();
    if (!name.toUpperCase().includes('NOT_FOUND') && name.length > 2) return name;
  }
  const boldMatch = content.match(/\*\*([A-ZÆØÅ][a-zæøåA-ZÆØÅ\s\-.]+?)\*\*\s*(?:as|is the|,)?\s*(?:CEO|CTO|Chief Executive|Chief Technology|Administrerende)/i);
  if (boldMatch) return boldMatch[1].trim();
  return null;
}

function extractTitle(content: string): string | null {
  const titleMatch = content.match(/title:\s*([^\n\[]+)/i);
  if (titleMatch) {
    const title = titleMatch[1].replace(/\[[\d]+\]/g, '').replace(/\*+/g, '').trim();
    if (!title.toUpperCase().includes('NOT_FOUND')) return title;
  }
  return null;
}

async function findWebsite(companyName: string): Promise<string | null> {
  const content = await perplexityQuery(
    `What is the official website URL for ${companyName}? This is a Danish company. Return ONLY the URL (include https://). If not found, return NOT_FOUND.`
  );
  if (content.includes('NOT_FOUND')) return null;
  const urlMatch = content.match(/https?:\/\/[^\s\n"'<>\[\]]+/);
  return urlMatch ? urlMatch[0].replace(/[.,;:!?)\*]+$/, '') : null;
}

async function findExec(companyName: string, role: 'CEO' | 'CTO', websiteUrl: string | null, ceoName?: string | null): Promise<{ name: string | null; title: string | null }> {
  const prompt = role === 'CEO'
    ? `Who is the CURRENT (2026) CEO/Chief Executive Officer of ${companyName}? Company website: ${websiteUrl || 'unknown'}. Return EXACTLY:\nname: [full name]\ntitle: [exact title]\nIf not found: name: NOT_FOUND\ntitle: NOT_FOUND`
    : `Who is the CURRENT (2026) CTO/Chief Technology Officer of ${companyName}? Company website: ${websiteUrl || 'unknown'}. CEO (hint): ${ceoName || 'unknown'}. Return EXACTLY:\nname: [full name]\ntitle: [exact title]\nIf not found: name: NOT_FOUND\ntitle: NOT_FOUND`;

  const content = await perplexityQuery(prompt);
  return { name: extractName(content, role), title: extractTitle(content) };
}

// ── Main phase 2 ──────────────────────────────────────────────────────────────

export async function runPhase2(filters: RunFilters = {}, logFn: LogFn = console.log): Promise<void> {
  const icp        = filters.icp        ?? 'source-angel';
  const d          = getIcpDb(icp);
  const minScore   = filters.minScore   ?? 70;
  const priority   = filters.priority   ?? 'HIGH';
  const source     = filters.source     ?? 'ALL';
  const limit      = filters.limit      ?? 100;
  const forceRefresh = filters.forceRefresh ?? false;

  logFn(`=== Phase 2: Company intelligence ===`);
  logFn(`Filters: priority=${priority} minScore=${minScore} source=${source} limit=${limit} forceRefresh=${forceRefresh}`);

  // Build WHERE clause dynamically
  const conditions: string[] = [`priority_score >= ${minScore}`];
  if (priority !== 'ALL') conditions.push(`priority = '${priority}'`);
  if (source && source !== 'ALL') conditions.push(`source = '${source}'`);
  const whereClause = 'WHERE ' + conditions.join(' AND ');

  const highLeads = getAll(d, `
    SELECT company, MAX(priority_score) as priority_score, COUNT(*) as lead_count,
           GROUP_CONCAT(DISTINCT canonical_url) as job_urls,
           GROUP_CONCAT(DISTINCT title) as job_titles,
           MAX(source) as source
    FROM leads
    ${whereClause}
    GROUP BY company
    ORDER BY priority_score DESC
    LIMIT ${limit}
  `);

  logFn(`Found ${highLeads.length} companies to process`);

  for (const lead of highLeads) {
    const companyName = lead.company;

    // Skip if already enriched recently (within 7 days)
    if (!forceRefresh) {
      const existing = getOne(d, `
        SELECT enriched_at FROM companies WHERE company_name = ? AND enriched_at > datetime('now', '-7 days')
      `, companyName);
      if (existing) {
        logFn(`  Skipping ${companyName} (cached)`);
        continue;
      }
    }

    logFn(`  -> Processing: ${companyName}`);

    try {
      // Step 1: Find LinkedIn company URL
      const { slug, url: linkedinUrl, confidence } = await findLinkedInCompanyUrl(d, companyName);

      // Step 2: Scrape LinkedIn company page
      let companyData: any = null;
      try {
        companyData = await scrapeLinkedInCompany(linkedinUrl);
      } catch (err: any) {
        console.warn(`    LinkedIn scrape failed: ${err.message}`);
      }

      const posts = companyData
        ? (Array.isArray(companyData.updates) ? companyData.updates : [])
            .map((u: any) => ({ text: u.text || '' }))
            .filter((p: any) => p.text)
        : [];

      // Step 3: Hiring signals
      const { signals, snippets, temperature } = detectHiringSignals(posts);

      // Step 4: Outreach angles
      const angles = generateOutreachAngles(signals, temperature);

      // Step 5: Location / EE detection
      const countries = companyData ? parseLocations(companyData) : [];
      const eeAssessment = assessEERisk(companyName, countries);

      const eeCountries = countries.filter(c => EE_COUNTRIES.has(c));
      const nonEeCountries = countries.filter(c => !EE_COUNTRIES.has(c));

      // Step 6: Website + Exec research (only for LOW/MEDIUM EE risk)
      let websiteUrl: string | null = null;
      let ceoName: string | null = null;
      let ceoTitle: string | null = null;
      let ctoName: string | null = null;
      let ctoTitle: string | null = null;

      if (eeAssessment.ee_risk !== 'HIGH') {
        try {
          websiteUrl = await findWebsite(companyName);
          const ceo = await findExec(companyName, 'CEO', websiteUrl);
          ceoName = ceo.name; ceoTitle = ceo.title;
          if (ceoName) {
            const cto = await findExec(companyName, 'CTO', websiteUrl, ceoName);
            ctoName = cto.name; ctoTitle = cto.title;
          }
        } catch (err: any) {
          console.warn(`    Exec research failed: ${err.message}`);
        }
      }

      // Save to companies table
      upsertCompany(d, {
        company_name: companyName,
        linkedin_url: linkedinUrl,
        linkedin_slug: slug,
        search_confidence: confidence,
        employees_count: companyData?.employees_in_linkedin ?? null,
        company_size: companyData?.company_size ?? null,
        countries: JSON.stringify(countries),
        ee_countries: JSON.stringify(eeCountries),
        non_ee_countries: JSON.stringify(nonEeCountries),
        ee_risk: eeAssessment.ee_risk,
        ee_action: eeAssessment.ee_action,
        ee_note: eeAssessment.ee_note,
        ee_is_known_outsourcer: eeAssessment.ee_is_known_outsourcer ? 1 : 0,
        hiring_signals: JSON.stringify(signals),
        hiring_temperature: temperature,
        hiring_signal_snippets: JSON.stringify(snippets.map(s => cleanText(s, 800))),
        outreach_angles: JSON.stringify(angles.map(a => cleanText(a, 800))),
        posts_scanned: posts.length,
        ceo_name: ceoName,
        ceo_title: ceoTitle,
        cto_name: ctoName,
        cto_title: ctoTitle,
        website_url: websiteUrl,
        job_urls: lead.job_urls,
        job_titles: lead.job_titles,
        priority: 'HIGH',
        priority_score: lead.priority_score,
        lead_count: lead.lead_count,
        enriched_at: new Date().toISOString(),
      });

      logFn(`    OK ${companyName} — EE:${eeAssessment.ee_risk} signals:[${signals.join(',')}] temp:${temperature}`);
      await sleep(1000);
    } catch (err: any) {
      logFn(`    FAIL ${companyName}: ${err.message}`);
    }
  }

  const stats = getAll(d, `SELECT hiring_temperature, COUNT(*) as c FROM companies GROUP BY hiring_temperature`);
  logFn('Phase 2 done:');
  for (const s of stats) logFn(`  ${s.hiring_temperature || 'NONE'}: ${s.c} companies`);
}
