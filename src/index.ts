import 'dotenv/config';
import cron from 'node-cron';
import { initDb } from './db';
import { runPhase1 } from './phase1-scrape';
import { runPhase2 } from './phase2-intel';
import { runPhase2b } from './phase2b-people';
import { runPhase3 } from './phase3-outreach';
import type { RunFilters } from './types';

async function runPipeline(phases?: number[], filters: RunFilters = {}) {
  const start = Date.now();
  initDb(filters.icp ?? 'source-angel');

  const run = (n: number) => !phases || phases.includes(n);

  try {
    if (run(1)) await runPhase1(filters);
    if (run(2)) await runPhase2(filters);
    if (run(3)) await runPhase2b(filters);
    if (run(4)) await runPhase3(filters);

    const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);
    console.log(`\nPipeline complete in ${elapsed} minutes`);
  } catch (err) {
    console.error('Pipeline failed:', err);
    process.exit(1);
  }
}

// ── CLI ───────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

if (args.includes('--cron')) {
  initDb('source-angel');
  console.log('Cron scheduler started — runs nightly at 3:00 AM (Europe/Copenhagen)');
  cron.schedule('0 3 * * *', () => {
    console.log(`\n[${new Date().toISOString()}] Starting nightly pipeline run`);
    runPipeline(undefined, { icp: 'source-angel' });
  }, { timezone: 'Europe/Copenhagen' });
} else {
  const phaseIdx = args.indexOf('--phase');
  const phases = phaseIdx >= 0 ? [parseInt(args[phaseIdx + 1])] : undefined;

  const icpIdx = args.indexOf('--icp');
  const icp = icpIdx >= 0 ? args[icpIdx + 1] : 'source-angel';

  const filters: RunFilters = { icp };

  if (phases) {
    console.log(`Running phase ${phases[0]} only [icp: ${icp}]`);
  } else {
    console.log(`Running full pipeline (all phases) [icp: ${icp}]`);
  }

  runPipeline(phases, filters);
}
