import axios from 'axios';
import * as cheerio from 'cheerio';
import { db, getAll } from './db';
import { scoreLead, canonicalizeUrl, deduplicateJobs, sleep, normalizeCompany, normalizeCompanyName, enrichWithCompanySignals } from './utils';

const BRIGHTDATA_KEY = process.env.BRIGHTDATA_API_KEY!;

const CONFIG = {
  thehub_base_url: 'https://thehub.io/jobs?positionTypes=5b8e46b3853f039706b6ea70&positionTypes=5b8e46b3853f039706b6ea71&positionTypes=5b8e46b3853f039706b6ea75&roles=backenddeveloper&roles=devops&roles=fullstackdeveloper&roles=frontenddeveloper&countryCode=DK&sorting=mostPopular',
  thehub_max_pages: 5,
  linkedin_search_url: 'https://www.linkedin.com/jobs/search/?keywords=software%20developer&location=Denmark&f_TPR=r2592000',
  itjobbank_url: 'https://www.it-jobbank.dk/api/jobsearch/v3/',
  jobindex_url: 'https://www.jobindex.dk/api/jobsearch/v3/',
  max_jobs: 200,
};

interface RawJob {
  source: string;
  title: string;
  company: string;
  location: string;
  url: string;
  canonical_url?: string;
  scraped_at?: string;
}

async function scrapeViaBrightData(url: string): Promise<string> {
  const res = await axios.post('https://api.brightdata.com/request', {
    zone: 'mcp_unlocker',
    url,
    format: 'raw',
  }, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${BRIGHTDATA_KEY}`,
    },
    timeout: 60000,
  });
  return res.data?.data || res.data || '';
}

async function scrapeTheHub(): Promise<RawJob[]> {
  const jobs: RawJob[] = [];
  for (let page = 1; page <= CONFIG.thehub_max_pages; page++) {
    try {
      const html = await scrapeViaBrightData(`${CONFIG.thehub_base_url}&page=${page}`);
      const $ = cheerio.load(html);

      const titles: string[] = [];
      const companies: string[] = [];
      const locations: string[] = [];
      const urls: string[] = [];
      const jobTypes: string[] = [];

      $('.card-job-find-list__position').each((_, el) => { titles.push($(el).text().trim()); });
      $('.card-job-find-list__title .bullet-inline-list span:nth-child(1)').each((_, el) => { companies.push($(el).text().trim()); });
      $('.card-job-find-list__title .bullet-inline-list span:nth-child(2)').each((_, el) => { locations.push($(el).text().trim()); });
      $('.card-job-find-list__title .bullet-inline-list span:nth-child(3)').each((_, el) => { jobTypes.push($(el).text().trim()); });
      $('.card-job-find-list__link').each((_, el) => {
        const href = $(el).attr('href') || '';
        urls.push(href.startsWith('http') ? href : `https://thehub.io${href}`);
      });

      const maxLen = Math.max(titles.length, companies.length, locations.length);
      for (let i = 0; i < maxLen; i++) {
        if (!titles[i] || !companies[i]) continue;
        jobs.push({
          source: 'TheHub',
          title: titles[i] || '',
          company: companies[i] || '',
          location: locations[i] || '',
          url: urls[i] || '',
          scraped_at: new Date().toISOString(),
        });
      }

      await sleep(500);
    } catch (err: any) {
      console.warn(`TheHub page ${page} failed: ${err.message}`);
    }
  }
  console.log(`TheHub: ${jobs.length} jobs`);
  return jobs;
}

async function scrapeLinkedIn(): Promise<RawJob[]> {
  const jobs: RawJob[] = [];
  try {
    const html = await scrapeViaBrightData(CONFIG.linkedin_search_url);
    const $ = cheerio.load(html);

    const titles: string[] = [];
    const companies: string[] = [];
    const locations: string[] = [];
    const urls: string[] = [];

    $('h3.base-search-card__title').each((_, el) => { titles.push($(el).text().trim()); });
    $('h4.base-search-card__subtitle > a.hidden-nested-link').each((_, el) => { companies.push($(el).text().trim()); });
    $('.job-search-card__location').each((_, el) => { locations.push($(el).text().trim()); });
    $('a.base-card__full-link').each((_, el) => { urls.push($(el).attr('href') || ''); });

    for (let i = 0; i < titles.length; i++) {
      if (!titles[i] || !companies[i]) continue;
      jobs.push({
        source: 'LinkedIn',
        title: titles[i],
        company: companies[i],
        location: locations[i] || '',
        url: urls[i] || '',
        scraped_at: new Date().toISOString(),
      });
    }
  } catch (err: any) {
    console.warn(`LinkedIn scrape failed: ${err.message}`);
  }
  console.log(`LinkedIn: ${jobs.length} jobs`);
  return jobs;
}

async function scrapeItJobbank(): Promise<RawJob[]> {
  const jobs: RawJob[] = [];
  try {
    const res = await axios.get(CONFIG.itjobbank_url, {
      params: { q: '', page: 1, sort: 'score', jobage: 30 },
      timeout: 30000,
    });
    const results = res.data?.results || [];
    for (const j of results) {
      let url = j.url || j.share_url || '';
      if (url && !url.startsWith('http')) url = `https://www.it-jobbank.dk${url}`;

      let company = '';
      if (typeof j.company === 'string') company = j.company.trim();
      else if (j.company?.name) company = j.company.name.trim();
      if (!company) company = (j.companytext || '').trim();

      jobs.push({
        source: 'IT-Jobbank',
        title: j.headline || '',
        company,
        location: j.area || 'Denmark',
        url,
        scraped_at: new Date().toISOString(),
      });
    }
  } catch (err: any) {
    console.warn(`IT-Jobbank failed: ${err.message}`);
  }
  console.log(`IT-Jobbank: ${jobs.length} jobs`);
  return jobs;
}

async function scrapeJobindex(): Promise<RawJob[]> {
  const jobs: RawJob[] = [];
  try {
    const res = await axios.get(CONFIG.jobindex_url, {
      params: {
        q: 'developer', page: 1, sort: 'score', jobage: 30,
        employment_place: [3, 2, 4],
        employment_type: [1, 11, 2],
        workinghours_type: [1, 2],
        supid: 1, subid: 1,
      },
      timeout: 30000,
    });
    const results = res.data?.results || res.data?.items || [];
    for (const j of results) {
      let url = j.url || j.share_url || '';
      if (url && !url.startsWith('http')) url = `https://www.jobindex.dk${url}`;

      let company = '';
      if (typeof j.company === 'string') company = j.company;
      else if (j.company?.name) company = j.company.name;
      else if (j.company_name) company = j.company_name;

      jobs.push({
        source: 'Jobindex',
        title: j.headline || j.title || '',
        company: company || 'Unknown',
        location: j.simple_string || j.area || j.city || '',
        url,
        scraped_at: new Date().toISOString(),
      });
    }
  } catch (err: any) {
    console.warn(`Jobindex failed: ${err.message}`);
  }
  console.log(`Jobindex: ${jobs.length} jobs`);
  return jobs;
}

export async function runPhase1(logFn: (msg: string) => void = console.log): Promise<void> {
  logFn('=== Phase 1: Scraping job boards ===');

  const [thehub, linkedin, itjobbank, jobindex] = await Promise.all([
    scrapeTheHub(),
    scrapeLinkedIn(),
    scrapeItJobbank(),
    scrapeJobindex(),
  ]);

  const all: RawJob[] = [...thehub, ...linkedin, ...itjobbank, ...jobindex];
  logFn(`Collected ${all.length} raw jobs`);

  const filtered = all.filter(j =>
    j.title && !j.title.includes('*') &&
    j.company && !j.company.includes('*') && j.company !== 'Unknown Company' &&
    j.company !== 'Unknown'
  );

  const normalized = filtered.map(j => ({
    ...j,
    canonical_url: canonicalizeUrl(j.url || '', j.source),
  })).filter(j => j.canonical_url);

  // Normalize company names before dedup (prevents "Bankdata A/S" vs "Bankdata" duplicates)
  const normalizedCo = normalized.map(j => ({
    ...j,
    company: normalizeCompanyName(j.company) || j.company,
  }));

  const unique = deduplicateJobs(normalizedCo);
  logFn(`After dedup: ${unique.length} jobs`);

  // Build company-level signals (multi-source boost, role diversity)
  const companySignals = enrichWithCompanySignals(unique);

  const insert = db.prepare(`
    INSERT OR IGNORE INTO leads (canonical_url, company, title, location, source, priority, priority_score, scoring_factors, scraped_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  for (const job of unique.slice(0, CONFIG.max_jobs)) {
    const signals = companySignals.get(normalizeCompany(job.company)) || {};
    const { score, factors } = scoreLead({ ...job, ...signals });
    const priority = score >= 70 ? 'HIGH' : score >= 40 ? 'MEDIUM' : 'LOW';
    insert.run(
      job.canonical_url, job.company, job.title, job.location, job.source,
      priority, score, factors.join(', '), job.scraped_at || new Date().toISOString(),
    );
    inserted++;
  }

  const total = db.prepare('SELECT COUNT(*) as c FROM leads').get() as any;
  logFn(`Phase 1 done: ${inserted} new leads inserted (${total?.c ?? 0} total)`);

  const stats = getAll('SELECT priority, COUNT(*) as c FROM leads GROUP BY priority');
  for (const s of stats) logFn(`  ${s.priority}: ${s.c}`);
}
