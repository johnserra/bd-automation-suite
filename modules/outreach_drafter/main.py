"""Outreach Drafter — Module 5.

For each qualified lead with enrichment data and contact info, generates a
personalized outreach email draft using Claude Sonnet and stores it in
x_outreach_draft for human review. Never auto-sends.

Usage:
    uv run python -m modules.outreach_drafter.main
    uv run python -m modules.outreach_drafter.main --dry-run
    uv run python -m modules.outreach_drafter.main --stream stream_c
    uv run python -m modules.outreach_drafter.main --limit 5
"""

import argparse
import sys
from datetime import date
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.llm_client import LLMClient, SONNET
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.outreach_drafter.drafter import (
    draft_outreach,
    select_template,
    STAGE_TEMPLATE_MAP,
)

load_dotenv()

logger = get_logger("outreach_drafter")

TERMINAL_STAGES = {"Won", "Lost"}

# Stages eligible for outreach drafting
OUTREACH_STAGES = set(STAGE_TEMPLATE_MAP.keys())

LEAD_FIELDS = [
    "id", "name", "partner_name", "contact_name", "email_from",
    "phone", "city", "state_id", "street", "stage_id",
    "x_bd_stream", "x_business_type", "x_current_supplier",
    "x_import_source_country", "x_estimated_spaces", "x_current_operator",
    "x_property_type", "x_company_size", "x_lead_score",
    "x_decision_maker_title", "x_outreach_draft", "description",
    "x_enrichment_status",
]


def fetch_leads_needing_outreach(
    odoo: OdooClient,
    stream_filter: Optional[str],
    limit: Optional[int],
) -> list[dict]:
    """Fetch leads eligible for outreach drafting.

    Criteria: x_outreach_draft is empty, stage in OUTREACH_STAGES, not terminal.
    """
    domain = [
        ["x_outreach_draft", "in", [False, ""]],
        ["stage_id.name", "in", list(OUTREACH_STAGES)],
        ["stage_id.name", "not in", list(TERMINAL_STAGES)],
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=LEAD_FIELDS, limit=limit)
    logger.info("Fetched %d lead(s) needing outreach drafts", len(leads))
    return leads


def run(
    dry_run: bool = False,
    stream_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict:
    """Draft outreach emails for all eligible leads.

    Returns:
        Summary dict: {drafted, skipped, errors, total}
    """
    config = load_config("config/outreach.yaml")
    templates: dict = config.get("templates", {})
    llm_config: dict = config.get("llm", {})
    system_prompt = llm_config.get("system_prompt", "")
    llm_model = llm_config.get("model", SONNET)
    max_tokens = llm_config.get("max_tokens", 512)

    odoo = OdooClient.from_env()
    llm = LLMClient.from_env()
    leads = fetch_leads_needing_outreach(odoo, stream_filter, limit)

    if not leads:
        logger.info("No leads need outreach drafts.")
        return {"drafted": 0, "skipped": 0, "errors": 0, "total": 0}

    drafted = 0
    skipped = 0
    errors = 0
    today = date.today()

    for lead in leads:
        stream = lead.get("x_bd_stream") or ""
        company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"
        stage_id = lead.get("stage_id")
        stage_name = stage_id[1] if isinstance(stage_id, (list, tuple)) and len(stage_id) == 2 else ""

        # Select the right template
        result = select_template(stream, stage_name, templates)
        if result is None:
            logger.info(
                "No template for '%s' (stream=%s, stage=%s) — skipping",
                company, stream or "?", stage_name or "?",
            )
            skipped += 1
            continue

        template_key, template = result

        logger.info(
            "Drafting outreach for '%s' (stream=%s, template=%s)",
            company, stream, template_key,
        )

        try:
            draft = draft_outreach(
                lead=lead,
                template_key=template_key,
                template=template,
                llm=llm,
                system_prompt=system_prompt,
                llm_model=llm_model,
                max_tokens=max_tokens,
            )
        except Exception as exc:
            logger.error(
                "LLM draft failed for '%s': %s", company, exc,
            )
            errors += 1
            continue

        if dry_run:
            # Show preview but don't write to Odoo
            preview = draft[:200] + "..." if len(draft) > 200 else draft
            logger.info(
                "[DRY RUN] Draft preview for '%s':\n%s", company, preview,
            )
        else:
            try:
                odoo.update_lead(lead["id"], {"x_outreach_draft": draft})
                odoo.create_activity(
                    lead_id=lead["id"],
                    summary="Review and send outreach draft",
                    date_deadline=today,
                )
            except Exception as exc:
                logger.error(
                    "Failed to save draft for '%s': %s", company, exc,
                )
                errors += 1
                continue

        drafted += 1

    # Summary output
    total = len(leads)
    cost = llm.get_cost_summary()

    print()
    print("=== Outreach Drafter Summary ===")
    print(
        f"Processed: {total}  |  Drafted: {drafted}  |  "
        f"Skipped: {skipped}  |  Errors: {errors}"
    )
    print(
        f"LLM calls: {cost['calls']}  |  "
        f"Tokens: {cost['input_tokens']} in / {cost['output_tokens']} out  |  "
        f"Est. cost: ${cost['cost_usd']:.4f}"
    )
    print()

    logger.info(
        "Outreach drafting complete | total=%d | drafted=%d | skipped=%d | errors=%d",
        total, drafted, skipped, errors,
    )
    return {
        "total": total,
        "drafted": drafted,
        "skipped": skipped,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Outreach Drafter — generate personalized email drafts for Odoo leads."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate drafts (calls LLM) but don't write to Odoo.")
    parser.add_argument("--stream", default=None, metavar="STREAM",
                        help="Limit to one BD stream (e.g. stream_c).")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap the number of leads processed.")
    args = parser.parse_args()

    try:
        summary = run(
            dry_run=args.dry_run,
            stream_filter=args.stream,
            limit=args.limit,
        )
    except EnvironmentError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)

    sys.exit(1 if summary.get("errors", 0) > 0 else 0)


if __name__ == "__main__":
    main()
