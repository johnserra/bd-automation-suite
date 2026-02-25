"""Lead Scoring & Prioritization — Module 4.

Scores every active lead based on YAML-configured weighted criteria, writes
x_lead_score and x_score_breakdown back to Odoo, and auto-advances
Research-stage leads that meet the qualified threshold.

Usage:
    uv run python -m modules.lead_scoring.main
    uv run python -m modules.lead_scoring.main --dry-run
    uv run python -m modules.lead_scoring.main --stream stream_c
    uv run python -m modules.lead_scoring.main --min-score 40
"""

import argparse
import sys
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.lead_scoring.scorer import (
    breakdown_to_json,
    format_score_distribution,
    format_top_leads,
    score_lead,
)

load_dotenv()

logger = get_logger("lead_scoring")

# Stages where we score leads (all active pipeline stages)
TERMINAL_STAGES = {"Won", "Lost"}

# Only auto-advance to Qualified from this stage
ADVANCE_FROM_STAGE = "Research"
ADVANCE_TO_STAGE = "Qualified"

# Fields needed to evaluate every criterion across all streams
SCORE_FIELDS = [
    "id", "name", "partner_name", "city", "stage_id",
    "contact_name", "email_from",
    "x_bd_stream", "x_lead_score",
    # Packaging
    "x_already_importing", "x_import_source_country",
    "x_company_size", "x_enrichment_status",
    # Parking acquisition
    "x_current_operator", "x_estimated_spaces",
    "x_property_type",
    # Parking growth
    "x_business_type", "x_product_interest",
    # Address
    "state_id",
]


# ---------------------------------------------------------------------------
# Startup helpers
# ---------------------------------------------------------------------------

def build_state_code_cache(odoo: OdooClient) -> dict[int, str]:
    """Fetch all US states from Odoo and return {state_id: code}."""
    try:
        states = odoo._execute(
            "res.country.state",
            "search_read",
            [[["country_id.code", "=", "US"]]],
            fields=["id", "code"],
        )
        cache = {s["id"]: s["code"] for s in states}
        logger.debug("Built state_code_cache with %d entries", len(cache))
        return cache
    except Exception as exc:
        logger.warning("Could not build state_code_cache: %s — state scoring disabled", exc)
        return {}


def fetch_scoreable_leads(
    odoo: OdooClient,
    stream_filter: Optional[str],
) -> list[dict]:
    """Fetch all non-terminal leads (optionally filtered by stream)."""
    domain = [
        ["stage_id.name", "not in", list(TERMINAL_STAGES)],
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=SCORE_FIELDS)
    logger.info("Fetched %d lead(s) to score", len(leads))
    return leads


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run(
    dry_run: bool = False,
    stream_filter: Optional[str] = None,
    min_score: Optional[int] = None,
) -> dict:
    """Score all leads, write results to Odoo, print summary.

    Args:
        dry_run:       Log what would change without writing to Odoo.
        stream_filter: Limit scoring to one BD stream.
        min_score:     Only log/report leads at or above this score threshold.

    Returns:
        Summary dict with counts.
    """
    config = load_config("config/scoring.yaml")
    scoring_rules: dict = config.get("scoring_rules", {})

    if not scoring_rules:
        logger.warning("No scoring_rules found in config/scoring.yaml")
        return {}

    odoo = OdooClient.from_env()
    state_code_cache = build_state_code_cache(odoo)

    # Resolve the Qualified stage ID once
    qualified_stage_id: Optional[int] = None
    if not dry_run:
        qualified_stage_id = odoo.get_stage_id(ADVANCE_TO_STAGE)
        if qualified_stage_id is None:
            logger.warning(
                "Stage '%s' not found in Odoo — stage advancement disabled. "
                "Run setup_odoo_fields.py first.",
                ADVANCE_TO_STAGE,
            )

    leads = fetch_scoreable_leads(odoo, stream_filter)

    # ── Score every lead ────────────────────────────────────────────────────
    scored: list[tuple[dict, int, dict]] = []   # (lead, score, breakdown)
    scored_count = 0
    skipped_count = 0
    advanced_count = 0
    error_count = 0

    for lead in leads:
        stream = lead.get("x_bd_stream") or ""
        if not stream:
            logger.debug("Lead #%s has no x_bd_stream — skipping", lead["id"])
            skipped_count += 1
            continue

        stream_rules = scoring_rules.get(stream)
        if stream_rules is None:
            logger.debug(
                "No scoring rules for stream '%s' (lead #%s) — skipping",
                stream, lead["id"],
            )
            skipped_count += 1
            continue

        criteria = stream_rules.get("criteria", [])
        threshold = stream_rules.get("thresholds", {}).get("qualified")

        try:
            total, breakdown = score_lead(lead, criteria, state_code_cache)
        except Exception as exc:
            logger.error("Error scoring lead #%s: %s", lead["id"], exc)
            error_count += 1
            continue

        scored.append((lead, total, breakdown))
        scored_count += 1

        company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"
        stage_raw = lead.get("stage_id")
        current_stage = (
            stage_raw[1]
            if isinstance(stage_raw, (list, tuple)) and len(stage_raw) == 2
            else ""
        )

        should_advance = (
            threshold is not None
            and total >= threshold
            and current_stage == ADVANCE_FROM_STAGE
        )

        if dry_run:
            advance_note = f" → would advance to '{ADVANCE_TO_STAGE}'" if should_advance else ""
            logger.info(
                "[DRY RUN] %s  score=%d%s  breakdown=%s",
                company, total, advance_note, breakdown,
            )
            if should_advance:
                advanced_count += 1
            continue

        # Write score fields
        update_values = {
            "x_lead_score": total,
            "x_score_breakdown": breakdown_to_json(breakdown),
        }

        # Advance stage if threshold met and currently in Research
        if should_advance and qualified_stage_id:
            update_values["stage_id"] = qualified_stage_id
            advanced_count += 1
            logger.info(
                "QUALIFIED: %s  score=%d (threshold=%d) → '%s'",
                company, total, threshold, ADVANCE_TO_STAGE,
            )
        else:
            logger.debug("Scored: %s  score=%d", company, total)

        try:
            odoo.update_lead(lead["id"], update_values)
        except Exception as exc:
            logger.error("Failed to update lead #%s: %s", lead["id"], exc)
            error_count += 1

    # ── Print summary ────────────────────────────────────────────────────────
    scored.sort(key=lambda x: x[1], reverse=True)

    all_scores = [s for _, s, _ in scored]
    filtered = [
        (lead, score, bd)
        for lead, score, bd in scored
        if min_score is None or score >= min_score
    ]

    print()
    print(f"=== Lead Scoring {'[DRY RUN] ' if dry_run else ''}Summary ===")
    print(
        f"Scored: {scored_count}  |  Skipped (no stream/rules): {skipped_count}  "
        f"|  Newly qualified: {advanced_count}  |  Errors: {error_count}"
    )
    print()

    if filtered:
        print(format_top_leads(filtered))
        print()

    if all_scores:
        print(format_score_distribution(all_scores))
        print()

    summary = {
        "scored": scored_count,
        "skipped": skipped_count,
        "advanced": advanced_count,
        "errors": error_count,
    }
    logger.info(
        "Scoring complete | scored=%d | skipped=%d | advanced=%d | errors=%d",
        scored_count, skipped_count, advanced_count, error_count,
    )
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Lead Scoring — score Odoo leads and advance qualified ones."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute scores and log results, but don't write to Odoo.",
    )
    parser.add_argument(
        "--stream",
        default=None,
        metavar="STREAM",
        help="Limit scoring to one BD stream (e.g. stream_c).",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        metavar="N",
        help="Only show leads with score >= N in the summary (all are still scored).",
    )
    args = parser.parse_args()

    try:
        summary = run(
            dry_run=args.dry_run,
            stream_filter=args.stream,
            min_score=args.min_score,
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
