import 'dotenv/config';
import cron from 'node-cron';
import { initDb } from './db';
import { runPhase1 } from './phase1-scrape';
import { runPhase2 } from './phase2-intel';
import { runPhase2b } from './phase2b-people';
import { runPhase3 } from './phase3-outreach';

async function runPipeline(phases?: number[]) {
  const start = Date.now();
  initDb();

  const run = (n: number) => !phases || phases.includes(n);

  try {
    if (run(1)) await runPhase1();
    if (run(2)) await runPhase2();
    if (run(3)) await runPhase2b();
    if (run(4)) await runPhase3();

    const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);
    console.log(`\n✅ Pipeline complete in ${elapsed} minutes`);
  } catch (err) {
    console.error('Pipeline failed:', err);
    process.exit(1);
  }
}

// ── CLI ───────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

if (args.includes('--cron')) {
  initDb();
  console.log('🕒 Cron scheduler started — runs nightly at 3:00 AM (Europe/Copenhagen)');
  cron.schedule('0 3 * * *', () => {
    console.log(`\n[${new Date().toISOString()}] Starting nightly pipeline run`);
    runPipeline();
  }, { timezone: 'Europe/Copenhagen' });
} else {
  const phaseIdx = args.indexOf('--phase');
  const phases = phaseIdx >= 0 ? [parseInt(args[phaseIdx + 1])] : undefined;

  if (phases) {
    console.log(`Running phase ${phases[0]} only`);
  } else {
    console.log('Running full pipeline (all phases)');
  }

  runPipeline(phases);
}
