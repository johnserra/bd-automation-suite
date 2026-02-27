# BD Automation Suite

[![GitHub](https://img.shields.io/badge/GitHub-johnserra%2Fbd--automation--suite-blue?logo=github)](https://github.com/johnserra/bd-automation-suite)

A modular Python automation suite for multi-stream business development pipelines. Odoo CRM is the central data hub; all business logic lives in YAML configs. Nothing is ever auto-sent — outreach drafts are created for human review.

Designed to be adapted to any BD context: swap in your own stream configs, enrichment sources, and scoring criteria without touching Python.

---

## Architecture

```
bd-automation/
├── config/              # YAML configs — all business logic lives here
│   ├── stream_a.yaml        # BD stream: location/asset acquisition
│   ├── stream_b.yaml        # BD stream: existing account growth
│   ├── stream_c.yaml        # BD stream: supplier/vendor sourcing
│   ├── enrichment.yaml      # Per-stream enrichment adapter config
│   ├── scoring.yaml         # Scoring rules and qualified thresholds
│   ├── contact_discovery.yaml
│   ├── followup_rules.yaml
│   ├── outreach.yaml
│   └── reporting.yaml
├── shared/              # Shared libraries (Odoo, LLM, config, logging)
├── modules/             # One directory per BD module
│   ├── prospect_research/   # Module 1: Find new prospects from external sources
│   ├── contact_discovery/   # Module 2: Find decision-maker contacts
│   ├── lead_enrichment/     # Module 3: Enrich leads with contextual data
│   ├── lead_scoring/        # Module 4: YAML-driven weighted scoring
│   ├── outreach_drafter/    # Module 5: AI-draft outreach emails (human sends)
│   ├── followup_scheduler/  # Module 6: Rule-based activity creation in Odoo
│   └── pipeline_reporter/   # Module 7: Markdown pipeline reports
└── scripts/
    ├── setup_odoo_fields.py  # One-time Odoo custom field setup
    ├── run_daily.sh          # Cron: follow-up + scoring (7:45am daily)
    └── run_weekly.sh         # Cron: prospect research + report (Sunday 11pm)
```

### Key design decisions

- **YAML-driven logic** — scoring weights, enrichment sources, follow-up thresholds, and outreach tone are all config, not code
- **Adapter pattern** — each external data source (Google Maps, trade data, company website, news) is a self-contained adapter that never raises; it returns a result with `success=False` on failure
- **Idempotent** — all modules are safe to re-run; deduplication prevents double-creating leads or activities
- **Human in the loop** — outreach drafts written to `x_outreach_draft` field, never auto-sent
- **Haiku vs Sonnet** — Haiku for batch enrichment summarization (cheap); Sonnet for outreach drafts (quality)

---

## Quick Start

### 1. Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) — `pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Odoo Community instance (running and accessible)

### 2. Install dependencies

```bash
uv sync
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials:
#   ODOO_URL, ODOO_DB, ODOO_USER, ODOO_API_KEY
#   ANTHROPIC_API_KEY
#   GOOGLE_MAPS_API_KEY (for location-based streams)
#   HUNTER_IO_API_KEY (for contact discovery)
#   SMTP_* vars (for email notifications)
```

### 4. Set up Odoo custom fields and pipeline stages

```bash
# Preview what will be created (no changes)
uv run python scripts/setup_odoo_fields.py --dry-run

# Create fields and stages in Odoo
uv run python scripts/setup_odoo_fields.py
```

### 5. Verify imports

```bash
uv run python -c "from shared import OdooClient, LLMClient, get_logger, load_config; print('OK')"
```

---

## Running Modules

All modules support `--dry-run` (no writes to Odoo), `--stream STREAM`, and `--limit N`.

```bash
# Follow-up scheduler (run daily)
uv run python -m modules.followup_scheduler.main [--dry-run] [--stream stream_a]

# Prospect research for a specific stream
uv run python -m modules.prospect_research.main --stream stream_c [--dry-run] [--limit 50]

# Lead enrichment
uv run python -m modules.lead_enrichment.main [--dry-run] [--stream stream_c]

# Lead scoring
uv run python -m modules.lead_scoring.main [--dry-run]

# Outreach drafting (generates drafts via LLM, stores for human review)
uv run python -m modules.outreach_drafter.main [--dry-run] [--stream stream_c] [--limit 5]

# Pipeline report (weekly or monthly)
uv run python -m modules.pipeline_reporter.main [--type weekly] [--stream stream_c] [--output ~/reports/report.md]
```

## Running Tests

```bash
# All tests
uv run pytest

# Single module
uv run pytest modules/lead_enrichment/tests/

# Single test class or function
uv run pytest modules/lead_scoring/tests/test_scorer.py::TestScorerEdgeCases
```

---

## Adapting to Your Use Case

1. **Define your streams** — copy and rename `config/stream_a.yaml` for each BD stream you run
2. **Configure enrichment** — edit `config/enrichment.yaml` to list which adapters and fields to populate per stream
3. **Set scoring rules** — edit `config/scoring.yaml` with your criteria, point values, and qualified thresholds
4. **Customize follow-up rules** — edit `config/followup_rules.yaml` with your pipeline stages and time thresholds
5. **Add adapters** — implement `BaseEnrichmentAdapter` or `BaseAdapter` for any new data sources

The `trade_data` adapter (`modules/prospect_research/adapters/trade_data.py`) is a template for scraping a trade data service by HS code — update `BASE_URL` and CSS selectors to match your chosen service.
