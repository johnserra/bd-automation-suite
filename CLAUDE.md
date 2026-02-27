# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (uses uv + Python 3.12 venv at .venv)
uv sync

# Run all tests
uv run pytest

# Run a single module's tests
uv run pytest modules/lead_enrichment/tests/
uv run pytest modules/lead_scoring/tests/test_scorer.py::TestScorerEdgeCases

# Run a specific module (all support --dry-run, --stream STREAM, --limit N)
uv run python -m modules.followup_scheduler.main [--dry-run] [--stream stream_a]
uv run python -m modules.prospect_research.main --stream stream_c [--dry-run]
uv run python -m modules.lead_scoring.main [--dry-run]
uv run python -m modules.lead_enrichment.main [--dry-run]
uv run python -m modules.outreach_drafter.main [--dry-run] [--stream stream_c] [--limit N]
uv run python -m modules.pipeline_reporter.main [--type weekly|monthly] [--stream stream_c] [--output PATH]

# One-time Odoo setup
uv run python scripts/setup_odoo_fields.py --dry-run
uv run python scripts/setup_odoo_fields.py

# Verify shared infrastructure
uv run python -c "from shared import OdooClient, LLMClient, get_logger, load_config; print('OK')"
```

## Architecture

**Central principle:** All business logic lives in `config/*.yaml`. Modules read their config at runtime — adjust scoring weights, enrichment sources, or follow-up thresholds without touching Python.

**Data flow:** External sources → modules → Odoo CRM (`crm.lead`) → human review → outreach

### Shared Infrastructure (`shared/`)

Everything in `shared/` is imported by all modules via `from shared import ...`:

- **`OdooClient`** — XML-RPC client for Odoo. All reads/writes go through `search_leads()`, `get_lead()`, `create_lead()`, `update_lead()`, `create_activity()`. Authenticates lazily; re-authenticates on session expiry. Fuzzy dedup via `search_duplicate()` (threshold 85, `thefuzz` library).
- **`LLMClient`** — Anthropic SDK wrapper with per-session cost tracking. Use `HAIKU` constant for batch tasks, `SONNET` for outreach drafts. Both constants are defined in `shared/llm_client.py`.
- **`load_config(path)`** / **`get_stream_config(stream)`** — Load YAML files. `get_stream_config("stream_c")` loads `config/stream_c.yaml`.
- **`get_logger(name)`** — Structured logger.

### Module Pattern

Each module under `modules/` follows this structure:
- `main.py` — CLI entry point (`argparse`), calls `load_dotenv()`, instantiates `OdooClient.from_env()`, orchestrates the run, exits with code 1 on errors
- `tests/test_*.py` — Unit tests using `pytest-mock`; pure logic functions are tested without Odoo

### Adapter Pattern (Enrichment & Prospect Research)

`modules/lead_enrichment/adapters/` and `modules/prospect_research/adapters/` both use a base ABC:
- Each adapter implements `enrich(lead, fields_to_update, config) → EnrichmentResult` (enrichment) or `fetch(stream_config) → list[ProspectRecord]` (research)
- Adapters **never raise** — they return a result with `success=False` and `error=str`
- The `ADAPTER_REGISTRY` dict in `main.py` maps source names (matching YAML keys) to adapter instances
- Unknown source names in YAML are skipped gracefully

### Lead Scoring (`modules/lead_scoring/scorer.py`)

Pure functions, no Odoo dependency. `score_lead(lead, criteria, state_code_cache)` evaluates YAML condition strings against lead fields. Scoring condition syntax:

```
is not empty | is empty | == true | == false | == 'string' | in ['a','b'] | >= N
```

`state_id` Odoo many2one `[42, "New York"]` is resolved to `"NY"` via a pre-built cache. Integer `0` is treated as non-empty (valid numeric value). Leads auto-advance to Qualified when score ≥ threshold (never demoted).

### Follow-up Scheduler (`modules/followup_scheduler/rule_engine.py`)

Pure functions. `evaluate_rule(lead, rule, today)` checks stage match + days-since-date threshold. `activity_is_duplicate()` enforces idempotency — checks existing open activities before creating.

### Config Files

| File | Used by |
|------|---------|
| `config/stream_a.yaml`, `stream_b.yaml`, `stream_c.yaml` | Prospect Research — search queries, filters per stream |
| `config/enrichment.yaml` | Lead Enrichment — per-stream adapter list + `fields_to_update` |
| `config/scoring.yaml` | Lead Scoring — criteria, points, qualified thresholds |
| `config/followup_rules.yaml` | Follow-up Scheduler — stage/days-since rules |
| `config/outreach.yaml` | Outreach Drafter — templates, tone |
| `config/reporting.yaml` | Pipeline Reporter — metrics, output format |

### Odoo Custom Fields

All `x_*` fields are non-standard and must be created via `scripts/setup_odoo_fields.py` before any module runs. Key fields: `x_bd_stream`, `x_lead_score`, `x_score_breakdown`, `x_enrichment_status`, `x_enrichment_date`, `x_outreach_draft`, and stream-specific fields (`x_already_importing`, `x_current_operator`, `x_estimated_spaces`, etc.).

### BD Streams

Three example streams — rename and reconfigure for your own BD context:
- `stream_a` — location/asset acquisition (Google Maps primary source)
- `stream_b` — existing account growth (proximity-based Google Maps search)
- `stream_c` — supplier/vendor sourcing (trade data + HS code primary source)

## Environment Variables

Required in `.env` (copy from `.env.example`):
- `ODOO_URL`, `ODOO_DB`, `ODOO_USER`, `ODOO_API_KEY`
- `ANTHROPIC_API_KEY`
- `GOOGLE_MAPS_API_KEY` (location-based streams)
- `HUNTER_IO_API_KEY` (contact discovery)
- `SMTP_*` vars (email notifications)
