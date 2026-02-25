"""Contact Discovery — Module 2.

For each lead missing a contact, runs automated finders (website scraper +
Hunter.io), ranks candidates by title priority, and writes the best contact
back to Odoo.  Leads with no auto-found contact are added to a LinkedIn
manual-work queue file.

Usage:
    uv run python -m modules.contact_discovery.main
    uv run python -m modules.contact_discovery.main --dry-run
    uv run python -m modules.contact_discovery.main --stream stream_c
    uv run python -m modules.contact_discovery.main --limit 20
    uv run python -m modules.contact_discovery.main --queue-file ~/bd-linkedin-queue.csv
"""

import argparse
import sys
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.contact_discovery.finders.hunter import HunterFinder
from modules.contact_discovery.finders.website import WebsiteContactFinder
from modules.contact_discovery.linkedin_queue import (
    DEFAULT_QUEUE_FILE,
    format_queue_summary,
    write_queue_file,
)
from modules.contact_discovery.ranker import best_candidate

load_dotenv()

logger = get_logger("contact_discovery")

# Leads to process: missing contact_name, not terminal stages
TERMINAL_STAGES = {"Won", "Lost"}

LEAD_FIELDS = [
    "id", "name", "partner_name", "contact_name", "email_from",
    "phone", "website", "city", "state_id", "stage_id",
    "x_bd_stream", "x_decision_maker_title", "x_linkedin_url",
    "x_enrichment_status",
]

# Instantiate finders (shared across leads in a run)
FINDERS = [
    WebsiteContactFinder(),
    HunterFinder(),
]


def fetch_leads_needing_contacts(
    odoo: OdooClient,
    stream_filter: Optional[str],
    limit: Optional[int],
) -> list[dict]:
    """Fetch active leads where contact_name is empty."""
    domain = [
        ["contact_name", "in", [False, ""]],
        ["stage_id.name", "not in", list(TERMINAL_STAGES)],
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=LEAD_FIELDS, limit=limit)
    logger.info("Fetched %d lead(s) needing contact discovery", len(leads))
    return leads


def run(
    dry_run: bool = False,
    stream_filter: Optional[str] = None,
    limit: Optional[int] = None,
    queue_file: str = DEFAULT_QUEUE_FILE,
) -> dict:
    """Discover contacts for all eligible leads.

    Returns:
        Summary dict: {auto_found, queued_linkedin, skipped, errors, total}
    """
    config = load_config("config/contact_discovery.yaml")
    target_titles: dict = config.get("target_titles", {})
    search_methods: dict = config.get("search_methods", {})

    odoo = OdooClient.from_env()
    leads = fetch_leads_needing_contacts(odoo, stream_filter, limit)

    if not leads:
        logger.info("No leads need contact discovery.")
        return {"auto_found": 0, "queued_linkedin": 0, "skipped": 0, "errors": 0, "total": 0}

    auto_found = 0
    needs_linkedin: list[dict] = []
    skipped = 0
    errors = 0

    for lead in leads:
        stream = lead.get("x_bd_stream") or ""
        company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"

        priority_titles: list[str] = (
            target_titles.get(stream, {}).get("priority_order", [])
        )
        if not priority_titles:
            logger.debug("No priority titles for stream '%s' — skipping '%s'", stream, company)
            skipped += 1
            continue

        logger.info("Processing: '%s' (stream=%s)", company, stream or "?")

        # --- Run enabled finders ---
        all_candidates = []
        for finder in FINDERS:
            if not finder.is_enabled(search_methods):
                continue
            finder_cfg = finder.get_config(search_methods)
            try:
                found = finder.find(lead, priority_titles, finder_cfg)
                all_candidates.extend(found)
                logger.debug(
                    "  %s: %d candidate(s)", finder.name, len(found)
                )
            except Exception as exc:
                logger.error(
                    "  Finder '%s' failed for '%s': %s", finder.name, company, exc
                )
                errors += 1

        # --- Rank and select best candidate ---
        contact = best_candidate(all_candidates, priority_titles)

        if contact and contact.is_actionable():
            logger.info(
                "  FOUND: %s (%s) via %s%s",
                contact.name,
                contact.title or "?",
                contact.source,
                f" — {contact.email}" if contact.email else "",
            )
            if not dry_run:
                _write_contact_to_odoo(odoo, lead["id"], contact)
            auto_found += 1
        else:
            logger.info("  No contact found automatically — queued for LinkedIn")
            needs_linkedin.append(lead)

    # --- Generate LinkedIn queue ---
    priority_titles_by_stream = {
        stream: titles.get("priority_order", [])
        for stream, titles in target_titles.items()
    }

    if needs_linkedin and not dry_run:
        written = write_queue_file(
            needs_linkedin, priority_titles_by_stream, queue_file=queue_file
        )
        queued_linkedin = written
    elif needs_linkedin and dry_run:
        logger.info(
            "[DRY RUN] Would queue %d lead(s) for LinkedIn lookup", len(needs_linkedin)
        )
        queued_linkedin = len(needs_linkedin)
    else:
        queued_linkedin = 0

    # --- Summary ---
    total = len(leads)
    print()
    print("=== Contact Discovery Summary ===")
    print(
        f"Processed: {total}  |  Auto-found: {auto_found}  |  "
        f"LinkedIn queue: {queued_linkedin}  |  Skipped: {skipped}  |  Errors: {errors}"
    )
    if not dry_run:
        print()
        print(format_queue_summary(queue_file))
    print()

    logger.info(
        "Discovery complete | total=%d | auto_found=%d | queued=%d | skipped=%d | errors=%d",
        total, auto_found, queued_linkedin, skipped, errors,
    )
    return {
        "total": total,
        "auto_found": auto_found,
        "queued_linkedin": queued_linkedin,
        "skipped": skipped,
        "errors": errors,
    }


def _write_contact_to_odoo(
    odoo: OdooClient, lead_id: int, contact
) -> None:
    """Write discovered contact fields back to Odoo."""
    values: dict = {"contact_name": contact.name}
    if contact.title:
        values["x_decision_maker_title"] = contact.title
    if contact.email:
        values["email_from"] = contact.email
    if contact.linkedin_url:
        values["x_linkedin_url"] = contact.linkedin_url
    if contact.phone:
        values["phone"] = contact.phone
    try:
        odoo.update_lead(lead_id, values)
    except Exception as exc:
        logger.error("Failed to update lead #%s: %s", lead_id, exc)


def main():
    parser = argparse.ArgumentParser(
        description="Contact Discovery — find decision-maker contacts for Odoo leads."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Run finders but don't write to Odoo or queue file.")
    parser.add_argument("--stream", default=None, metavar="STREAM",
                        help="Limit to one BD stream (e.g. stream_c).")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Cap the number of leads processed.")
    parser.add_argument("--queue-file", default=DEFAULT_QUEUE_FILE, metavar="PATH",
                        help=f"Path for the LinkedIn queue CSV (default: {DEFAULT_QUEUE_FILE}).")
    args = parser.parse_args()

    try:
        summary = run(
            dry_run=args.dry_run,
            stream_filter=args.stream,
            limit=args.limit,
            queue_file=args.queue_file,
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
