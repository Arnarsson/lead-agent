import OpenAI from 'openai';
import { db, getAll, getOne } from './db';
import { sleep, cleanText } from './utils';
import type { RunFilters, LogFn } from './types';

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

interface OutreachDraft {
  angle_used: string;
  subject: string;
  body: string;
  cta: string;
}

interface OutreachResult {
  company_name: string;
  linkedin_url: string;
  language: string;
  hiring_temperature: string;
  drafts: OutreachDraft[];
  meta: { draft_count: number; used_angles: string[] };
}

async function generateDrafts(params: {
  company_name: string;
  linkedin_url: string;
  countries: string;
  employees_count: number | null;
  hiring_signals: string[];
  hiring_temperature: string;
  hiring_signal_snippets: string[];
  outreach_angles: string[];
  outreach_strategy: string;
}): Promise<OutreachResult> {

  const prompt = `You are an AI-powered outreach assistant specializing in generating highly personalized and effective outreach drafts. Your goal is to convert qualified leads by crafting compelling messages based on provided research briefs.

For each lead, you will receive a structured research brief:

company_name: ${params.company_name}
linkedin_url: ${params.linkedin_url}
countries: ${params.countries}
employees_count: ${params.employees_count ?? 'Unknown'}
hiring_signals: ${params.hiring_signals.join(', ')}
hiring_temperature: ${params.hiring_temperature}
hiring_signal_snippets: ${params.hiring_signal_snippets.map((s, i) => `${i + 1}. ${s}`).join('\n')}
outreach_angles: ${params.outreach_angles.map((a, i) => `${i + 1}. ${a}`).join('\n')}
outreach_strategy: ${params.outreach_strategy}

Your task:
Generate 2–3 distinct outreach drafts. Each draft MUST:
- Be highly personalized: explicitly reference concrete details from hiring_signal_snippets and/or outreach_angles
- Use at least one outreach angle: each draft must clearly use one of the outreach_angles
- Be concise, friendly, and professional (in Danish - language: da)
- Include a clear CTA
- Vary in approach (one direct, one inquisitive, one consultative)

Respond with ONLY valid JSON in this exact format:
{
  "company_name": "${params.company_name}",
  "linkedin_url": "${params.linkedin_url}",
  "language": "da",
  "hiring_temperature": "${params.hiring_temperature}",
  "drafts": [
    {
      "angle_used": "<exact angle used>",
      "subject": "<short subject line>",
      "body": "<personalized message body>",
      "cta": "<one-sentence call to action>"
    }
  ],
  "meta": {
    "draft_count": 2,
    "used_angles": ["<angle 1>", "<angle 2>"]
  }
}`;

  const response = await openai.chat.completions.create({
    model: 'gpt-4.1-mini',
    messages: [{ role: 'user', content: prompt }],
    temperature: 0.7,
    response_format: { type: 'json_object' },
  });

  const content = response.choices[0]?.message?.content || '{}';
  const parsed = JSON.parse(content);
  return parsed as OutreachResult;
}

export async function runPhase3(filters: RunFilters = {}, logFn: LogFn = console.log): Promise<void> {
  const temps         = (filters.temperature ?? 'HOT,WARM').split(',').map(s => s.trim()).filter(Boolean);
  const eeRisks       = (filters.eeRisk ?? 'LOW,MEDIUM').split(',').map(s => s.trim()).filter(Boolean);
  const limit         = filters.limit ?? 200;
  const forceRegen    = filters.forceRefresh ?? false;

  logFn(`=== Phase 3: Outreach draft generation ===`);
  logFn(`Filters: temperature=${temps.join(',')} eeRisk=${eeRisks.join(',')} limit=${limit} forceRegen=${forceRegen}`);

  const tempList   = temps.map(t => `'${t}'`).join(',');
  const eeRiskList = eeRisks.map(r => `'${r}'`).join(',');

  // Support 'ALL' temperature
  const tempWhere = filters.temperature === 'ALL'
    ? ''
    : `AND c.hiring_temperature IN (${tempList})`;

  const companies = getAll(`
    SELECT c.*, t.target_name, t.target_linkedin_url, t.outreach_strategy
    FROM companies c
    LEFT JOIN targets t ON t.company_name = c.company_name
    WHERE c.ee_risk IN (${eeRiskList})
    ${tempWhere}
    AND (c.hiring_signals IS NOT NULL AND c.hiring_signals != '[]')
    ORDER BY
      CASE c.hiring_temperature WHEN 'HOT' THEN 0 WHEN 'WARM' THEN 1 ELSE 2 END,
      c.priority_score DESC
    LIMIT ${limit}
  `);

  logFn(`Generating drafts for ${companies.length} company/target pairs`);

  let totalDrafts = 0;

  for (const row of companies) {
    // Check if drafts already exist
    if (!forceRegen) {
      const existing = getOne(`SELECT id FROM drafts WHERE company_name = ? AND target_name IS ?`, row.company_name, row.target_name || null);
      if (existing) continue;
    }

    logFn(`  → ${row.company_name} / ${row.target_name || 'generic'}`);

    try {
      let hiringSignals: string[] = [];
      let hiringSnippets: string[] = [];
      let outreachAngles: string[] = [];
      let countries = '';

      try { hiringSignals = JSON.parse(row.hiring_signals || '[]'); } catch {}
      try { hiringSnippets = JSON.parse(row.hiring_signal_snippets || '[]'); } catch {}
      try { outreachAngles = JSON.parse(row.outreach_angles || '[]'); } catch {}
      try { countries = JSON.parse(row.countries || '[]').join(', '); } catch {}

      const result = await generateDrafts({
        company_name: row.company_name,
        linkedin_url: row.linkedin_url || '',
        countries,
        employees_count: row.employees_count,
        hiring_signals: hiringSignals,
        hiring_temperature: row.hiring_temperature || 'WARM',
        hiring_signal_snippets: hiringSnippets.map((s: string) => cleanText(s, 300)),
        outreach_angles: outreachAngles,
        outreach_strategy: row.outreach_strategy || 'COLD_PERSONALIZED',
      });

      const insertDraft = db.prepare(`
        INSERT INTO drafts (company_name, target_name, target_linkedin_url, hiring_temperature,
          draft_index, angle_used, subject, body, cta, language)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (let i = 0; i < (result.drafts || []).length; i++) {
        const draft = result.drafts[i];
        insertDraft.run(
          row.company_name,
          row.target_name || null,
          row.target_linkedin_url || null,
          result.hiring_temperature,
          i,
          draft.angle_used,
          draft.subject,
          draft.body,
          draft.cta,
          result.language || 'da',
        );
        totalDrafts++;
      }

      logFn(`    ✓ ${result.drafts?.length ?? 0} drafts generated for ${row.company_name}`);
      await sleep(500);
    } catch (err: any) {
      logFn(`    ✗ ${row.company_name}: ${err.message}`);
    }
  }

  logFn(`Phase 3 done: ${totalDrafts} drafts generated`);

  // Export summary to JSON
  const allDrafts = getAll(`
    SELECT d.*, c.linkedin_url as company_linkedin_url, c.hiring_temperature,
           c.hiring_signal_snippets, c.outreach_angles
    FROM drafts d
    LEFT JOIN companies c ON c.company_name = d.company_name
    ORDER BY d.company_name, d.draft_index
  `);

  const outputPath = `data/drafts-${new Date().toISOString().split('T')[0]}.json`;
  const fs = await import('fs');
  fs.writeFileSync(outputPath, JSON.stringify(allDrafts, null, 2));
  console.log(`Exported ${allDrafts.length} drafts to ${outputPath}`);
}
