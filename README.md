# Lead Agent — Local Pipeline

Replaces the n8n Lead Agent workflows. No n8n required. Runs locally with SQLite.

## Architecture

```
Phase 1 → Scrape 4 job boards → dedupe → score → SQLite
Phase 2 → LinkedIn enrichment → EE detection → C-level research → hiring signals
Phase 2B → Personal LinkedIn URLs → work history (Firecrawl) → connection matching
Phase 3 → OpenAI outreach draft generation (2-3 drafts per lead)
```

## Setup

```bash
cp .env.example .env
# Fill in API keys in .env
npm install
```

## API Keys needed

| Key | Where to get |
|-----|-------------|
| `BRIGHTDATA_API_KEY` | BrightData dashboard (zone: mcp_unlocker) |
| `OPENROUTER_API_KEY` | openrouter.ai |
| `OPENAI_API_KEY` | platform.openai.com |
| `FIRECRAWL_API_KEY` | firecrawl.dev (optional) |

## LinkedIn Connections (for warm intro detection)

Export from LinkedIn:
1. LinkedIn → Settings → Data privacy → Get a copy of your data
2. Select "Connections"
3. Save as `data/connections.csv`

(If missing, connection matching is skipped — everything runs as COLD_PERSONALIZED)

## Run

```bash
# Full pipeline
npm start
# or
npx tsx src/index.ts

# Single phase
npx tsx src/index.ts --phase 1   # Scrape job boards
npx tsx src/index.ts --phase 2   # Company intelligence
npx tsx src/index.ts --phase 3   # People mapping
npx tsx src/index.ts --phase 4   # Generate outreach drafts

# Nightly cron (3 AM Copenhagen time)
npm run cron
```

## Output

All data stored in `data/leads.db` (SQLite). Tables:

| Table | Contents |
|-------|---------|
| `leads` | All scraped job listings with scores |
| `companies` | Enriched company data (LinkedIn, EE risk, hiring signals) |
| `snapshots` | BrightData SERP cache (avoids re-running expensive calls) |
| `targets` | CEO/CTO personal profiles + connection matching |
| `drafts` | Generated outreach drafts |

Drafts also exported to `data/drafts-YYYY-MM-DD.json` after Phase 3.

## Query examples

```bash
# View HIGH priority companies
sqlite3 data/leads.db "SELECT company, priority_score FROM leads WHERE priority='HIGH' ORDER BY priority_score DESC LIMIT 20"

# View HOT companies with hiring signals
sqlite3 data/leads.db "SELECT company_name, hiring_temperature, hiring_signals FROM companies WHERE hiring_temperature='HOT'"

# View generated drafts
sqlite3 data/leads.db "SELECT company_name, target_name, subject FROM drafts LIMIT 10"
```
