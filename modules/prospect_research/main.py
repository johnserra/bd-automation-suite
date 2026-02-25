"""Prospect Research — Module 1.

Finds new companies matching a stream's target profile, deduplicates against
Odoo, and creates Research-stage leads for each genuinely new prospect.

Usage:
    uv run python -m modules.prospect_research.main --stream stream_c
    uv run python -m modules.prospect_research.main --stream stream_a
    uv run python -m modules.prospect_research.main --stream stream_c --dry-run
    uv run python -m modules.prospect_research.main --stream stream_c --limit 50
"""

import argparse
import sys
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import get_stream_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.prospect_research.adapters.google_maps import GoogleMapsAdapter
from modules.prospect_research.adapters.trade_data import TradeDataAdapter
from modules.prospect_research.deduplicator import split_new_and_duplicate
from modules.prospect_research.normalizer import ProspectRecord

load_dotenv()

logger = get_logger("prospect_research")

# Registry of all available adapters
ADAPTERS = [
    TradeDataAdapter(),
    GoogleMapsAdapter(),
]


def _get_or_warn(odoo: OdooClient, getter, label: str) -> Optional[int]:
    """Call getter(), return result or log a warning and return None."""
    try:
        result = getter()
        if result is None:
            logger.warning(
                "'%s' not found in Odoo. Run setup_odoo_fields.py first.", label
            )
        return result
    except Exception as exc:
        logger.error("Failed to look up '%s': %s", label, exc)
        return None


def _resolve_state_ids(
    odoo: OdooClient,
    records: list[ProspectRecord],
) -> dict[str, Optional[int]]:
    """Build a cache of state_code → Odoo state ID for all unique state codes."""
    state_codes = {r.state_code for r in records if r.state_code}
    cache: dict[str, Optional[int]] = {}
    for code in state_codes:
        cache[code] = odoo.get_state_id(code, "US")
        if cache[code] is None:
            logger.warning("State code '%s' not found in Odoo", code)
    return cache


def _get_country_id(odoo: OdooClient) -> Optional[int]:
    """Look up the US country ID in Odoo."""
    try:
        ids = odoo._execute(
            "res.country", "search", [["code", "=", "US"]]
        )
        return ids[0] if ids else None
    except Exception as exc:
        logger.warning("Could not look up US country ID: %s", exc)
        return None


def run(
    stream: str,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> dict:
    """Main execution: fetch → dedup → create leads.

    Args:
        stream:   BD stream name, e.g. 'stream_c'.
        dry_run:  If True, log what would be created but don't write to Odoo.
        limit:    Cap total new leads created (safety valve for first runs).

    Returns:
        Summary dict: {created, skipped_dedup, skipped_limit, errors, total_fetched}
    """
    config = get_stream_config(stream)
    profile: dict = config.get("target_profile", {})
    data_sources: dict = config.get("data_sources", {})
    dedup_config: dict = config.get("dedup", {})
    match_on: list[str] = dedup_config.get("match_on", ["partner_name", "city"])

    logger.info(
        "Prospect Research | stream=%s | dry_run=%s | limit=%s",
        stream,
        dry_run,
        limit,
    )

    # --- Connect to Odoo and resolve IDs ---
    odoo = OdooClient.from_env()
    research_stage_id = _get_or_warn(
        odoo, lambda: odoo.get_stage_id("Research"), "Research stage"
    )
    us_country_id = _get_country_id(odoo)

    if research_stage_id is None and not dry_run:
        logger.error(
            "Cannot create leads without 'Research' stage. "
            "Run: uv run python scripts/setup_odoo_fields.py"
        )
        return {"created": 0, "errors": 1}

    # --- Run enabled adapters ---
    all_records: list[ProspectRecord] = []

    for adapter in ADAPTERS:
        if not adapter.is_enabled(data_sources):
            logger.debug("Adapter '%s' disabled — skipping", adapter.name)
            continue

        adapter_cfg = adapter.get_adapter_config(data_sources)
        logger.info("Running adapter: %s", adapter.name)
        try:
            records = adapter.fetch(adapter_cfg, stream, profile)
            logger.info("Adapter '%s' returned %d record(s)", adapter.name, len(records))
            all_records.extend(records)
        except Exception as exc:
            logger.error(
                "Adapter '%s' failed unexpectedly: %s — continuing with other adapters",
                adapter.name,
                exc,
            )

    if not all_records:
        logger.info("No records fetched from any adapter.")
        return {"created": 0, "skipped_dedup": 0, "skipped_limit": 0, "errors": 0, "total_fetched": 0}

    logger.info("Total fetched: %d record(s) across all adapters", len(all_records))

    # --- Deduplicate against Odoo ---
    new_records, duplicate_records = split_new_and_duplicate(
        all_records, odoo, match_on
    )
    logger.info(
        "After dedup: %d new, %d duplicate(s) skipped",
        len(new_records),
        len(duplicate_records),
    )

    # --- Resolve state IDs in bulk ---
    state_id_cache = _resolve_state_ids(odoo, new_records)

    # --- Create leads ---
    created = 0
    skipped_limit = 0
    errors = 0

    for rec in new_records:
        if limit is not None and created >= limit:
            skipped_limit += 1
            continue

        state_id = state_id_cache.get(rec.state_code) if rec.state_code else None
        values = rec.to_odoo_values(
            stream=stream,
            stage_id=research_stage_id or 0,
            state_id=state_id,
            country_id=us_country_id,
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would create lead: '%s' in %s, %s",
                rec.partner_name,
                rec.city or "?",
                rec.state_code or "?",
            )
            created += 1
            continue

        try:
            lead_id = odoo.create_lead(values)
            logger.info(
                "Created lead #%d: '%s' (%s)",
                lead_id,
                rec.partner_name,
                rec.city or "?",
            )
            created += 1
        except Exception as exc:
            logger.error(
                "Failed to create lead for '%s': %s",
                rec.partner_name,
                exc,
            )
            errors += 1

    summary = {
        "total_fetched": len(all_records),
        "created": created,
        "skipped_dedup": len(duplicate_records),
        "skipped_limit": skipped_limit,
        "errors": errors,
    }

    logger.info(
        "Run complete | fetched=%d | created=%d | dedup_skip=%d | "
        "limit_skip=%d | errors=%d",
        summary["total_fetched"],
        summary["created"],
        summary["skipped_dedup"],
        summary["skipped_limit"],
        summary["errors"],
    )
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Prospect Research — find and create new Odoo leads from external sources."
    )
    parser.add_argument(
        "--stream",
        required=True,
        help="BD stream to run research for (must match a config/<stream>.yaml file).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and dedup, but don't write to Odoo.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of new leads created (useful for first runs).",
    )
    args = parser.parse_args()

    try:
        summary = run(stream=args.stream, dry_run=args.dry_run, limit=args.limit)
    except EnvironmentError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)

    sys.exit(1 if summary.get("errors", 0) > 0 else 0)


if __name__ == "__main__":
    main()
