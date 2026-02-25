"""Lead Enrichment — Module 3.

For each lead with pending or partial enrichment status, runs configured
enrichment adapters and writes the gathered data back to Odoo.

Usage:
    uv run python -m modules.lead_enrichment.main
    uv run python -m modules.lead_enrichment.main --dry-run
    uv run python -m modules.lead_enrichment.main --stream stream_c
    uv run python -m modules.lead_enrichment.main --limit 10
"""

import argparse
import sys
from datetime import date
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.lead_enrichment.adapters.base import EnrichmentResult
from modules.lead_enrichment.adapters.company_website import CompanyWebsiteEnrichmentAdapter
from modules.lead_enrichment.adapters.market_presence_check import MarketPresenceCheckAdapter
from modules.lead_enrichment.adapters.google_maps_detail import GoogleMapsDetailAdapter
from modules.lead_enrichment.adapters.trade_data_detail import TradeDataDetailAdapter
from modules.lead_enrichment.adapters.news_search import NewsSearchAdapter

load_dotenv()

logger = get_logger("lead_enrichment")

TERMINAL_STAGES = {"Won", "Lost"}

LEAD_FIELDS = [
    "id", "name", "partner_name", "website", "city", "state_id", "street",
    "stage_id", "x_bd_stream", "x_enrichment_status", "description",
    "x_data_source",
]

# Adapter registry — keyed by source name in enrichment.yaml.
# linkedin_company and county_assessor are manual / not automated; they are
# skipped gracefully when encountered.
ADAPTER_REGISTRY = {
    "company_website": CompanyWebsiteEnrichmentAdapter(),
    "trade_data_detail": TradeDataDetailAdapter(),
    "news_search": NewsSearchAdapter(),
    "google_maps_detail": GoogleMapsDetailAdapter(),
    "market_presence_check": MarketPresenceCheckAdapter(),
}


def fetch_leads_to_enrich(
    odoo: OdooClient,
    stream_filter: Optional[str],
    limit: Optional[int],
) -> list[dict]:
    """Fetch leads whose enrichment status is pending, partial, or unset."""
    domain = [
        ["x_enrichment_status", "in", ["pending", "partial", False]],
        ["stage_id.name", "not in", list(TERMINAL_STAGES)],
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=LEAD_FIELDS, limit=limit)
    logger.info("Fetched %d lead(s) needing enrichment", len(leads))
    return leads


def run(
    dry_run: bool = False,
    stream_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict:
    """Enrich all eligible leads.

    Returns:
        Summary dict: {enriched, partial, skipped, errors, total}
    """
    config = load_config("config/enrichment.yaml")
    enrichment_sources: dict = config.get("enrichment_sources", {})

    odoo = OdooClient.from_env()
    leads = fetch_leads_to_enrich(odoo, stream_filter, limit)

    if not leads:
        logger.info("No leads need enrichment.")
        return {"enriched": 0, "partial": 0, "skipped": 0, "errors": 0, "total": 0}

    enriched = 0
    partial = 0
    skipped = 0
    errors = 0
    today_str = date.today().isoformat()

    for lead in leads:
        stream = lead.get("x_bd_stream") or ""
        company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"
        source_list: list[dict] = enrichment_sources.get(stream, [])

        if not source_list:
            logger.debug(
                "No enrichment sources configured for stream '%s' — skipping '%s'",
                stream, company,
            )
            skipped += 1
            continue

        logger.info("Enriching: '%s' (stream=%s)", company, stream or "?")

        results: list[EnrichmentResult] = []
        for source_cfg in source_list:
            source_name = source_cfg.get("source", "")
            fields_to_update: list[str] = source_cfg.get("fields_to_update", [])
            adapter = ADAPTER_REGISTRY.get(source_name)

            if adapter is None:
                logger.debug(
                    "  Source '%s': no adapter (manual or not implemented) — skipping",
                    source_name,
                )
                continue

            try:
                result = adapter.enrich(lead, fields_to_update, config)
                results.append(result)
                if result.success:
                    logger.debug("  %s: OK — updated %s", source_name, list(result.fields_updated))
                else:
                    logger.debug("  %s: not enriched — %s", source_name, result.error)
            except Exception as exc:
                logger.error(
                    "  Adapter '%s' raised for '%s': %s", source_name, company, exc
                )
                results.append(
                    EnrichmentResult(source=source_name, success=False, error=str(exc))
                )
                errors += 1

        # Collect all fields and description notes from successful results
        all_odoo_fields: dict = {}
        description_notes: list[str] = []
        any_success = False
        any_failure = False

        for r in results:
            if r.success:
                any_success = True
                all_odoo_fields.update(r.fields_updated)
                if r.description_note:
                    description_notes.append(r.description_note)
            else:
                any_failure = True

        # Determine enrichment status
        if any_success and not any_failure:
            status = "complete"
            enriched += 1
        else:
            # partial: either mixed success/failure or all skipped/failed
            status = "partial"
            partial += 1

        # Append enrichment notes to existing description (do not overwrite)
        if description_notes:
            existing_desc = (lead.get("description") or "").strip()
            separator = "\n\n" if existing_desc else ""
            all_odoo_fields["description"] = existing_desc + separator + "\n".join(
                description_notes
            )

        all_odoo_fields["x_enrichment_status"] = status
        all_odoo_fields["x_enrichment_date"] = today_str

        if dry_run:
            logger.info(
                "  [DRY RUN] Would update %d field(s): %s | status=%s",
                len(all_odoo_fields),
                sorted(all_odoo_fields.keys()),
                status,
            )
        else:
            try:
                odoo.update_lead(lead["id"], all_odoo_fields)
            except Exception as exc:
                logger.error("  Failed to update lead #%s: %s", lead["id"], exc)
                errors += 1

    # Summary output
    total = len(leads)
    print()
    print("=== Lead Enrichment Summary ===")
    print(
        f"Processed: {total}  |  Complete: {enriched}  |  "
        f"Partial: {partial}  |  Skipped: {skipped}  |  Errors: {errors}"
    )
    print()

    logger.info(
        "Enrichment complete | total=%d | enriched=%d | partial=%d | skipped=%d | errors=%d",
        total, enriched, partial, skipped, errors,
    )
    return {
        "total": total,
        "enriched": enriched,
        "partial": partial,
        "skipped": skipped,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Lead Enrichment — add contextual data to Odoo leads."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Run adapters but don't write to Odoo.")
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
