"""Follow-up Scheduler — Module 6.

Evaluates YAML-configured rules against all active Odoo leads and creates
activity reminders for triggered conditions.  Sends a morning digest via
email or Slack.

Usage:
    uv run python -m modules.followup_scheduler.main
    uv run python -m modules.followup_scheduler.main --dry-run
    uv run python -m modules.followup_scheduler.main --stream stream_c
"""

import argparse
import sys
from datetime import date, timedelta

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.followup_scheduler.rule_engine import activity_is_duplicate, evaluate_rule
from modules.followup_scheduler.notifier import send_digest

load_dotenv()

logger = get_logger("followup_scheduler")

# Fields fetched for every lead — covers all possible rule conditions
LEAD_FIELDS = [
    "id",
    "name",
    "partner_name",
    "contact_name",
    "email_from",
    "city",
    "stage_id",
    "x_bd_stream",
    "x_lead_score",
    "x_last_personal_contact",
    "x_sample_sent_date",
    "x_enrichment_status",
    "write_date",
]

# Stages that are terminal — we don't create follow-ups for these
INACTIVE_STAGES = {"Won", "Lost"}


def fetch_active_leads(odoo: OdooClient, stream_filter: str = None) -> list[dict]:
    """Fetch all non-terminal leads from Odoo.

    Includes 'Not Now' leads because the re-engage rule targets them.
    Uses active in [True, False] to catch any archived records.
    """
    domain = [
        ["stage_id.name", "not in", list(INACTIVE_STAGES)],
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=LEAD_FIELDS)
    logger.info("Fetched %d active lead(s) from Odoo", len(leads))
    return leads


def _resolve_stage_id(odoo: OdooClient, stage_name: str) -> int | None:
    """Look up a stage ID, with a warning if not found."""
    stage_id = odoo.get_stage_id(stage_name)
    if stage_id is None:
        logger.warning(
            "Stage '%s' not found in Odoo — run setup_odoo_fields.py first",
            stage_name,
        )
    return stage_id


def run(dry_run: bool = False, stream_filter: str = None) -> list[dict]:
    """Main execution: evaluate all rules, create activities, send digest.

    Args:
        dry_run:       If True, log what would happen but don't write to Odoo.
        stream_filter: Optional BD stream to limit leads (e.g. 'stream_c').

    Returns:
        List of triggered result dicts (for testing / inspection).
    """
    config = load_config("config/followup_rules.yaml")
    rules: list[dict] = config.get("rules", [])
    digest_config: dict = config.get("digest", {"enabled": True, "channel": "email"})

    if not rules:
        logger.warning("No rules found in followup_rules.yaml — nothing to do")
        return []

    logger.info(
        "Starting follow-up scheduler | %d rules | dry_run=%s | stream=%s",
        len(rules),
        dry_run,
        stream_filter or "all",
    )

    odoo = OdooClient.from_env()
    leads = fetch_active_leads(odoo, stream_filter)
    today = date.today()

    triggered: list[dict] = []
    created_count = skipped_count = error_count = 0

    for lead in leads:
        # Fetch existing open activities once per lead (for idempotency check)
        try:
            open_activities = (
                [] if dry_run else odoo.get_open_activities(lead["id"])
            )
        except Exception as exc:
            logger.error(
                "Could not fetch activities for lead %s: %s", lead["id"], exc
            )
            open_activities = []

        for rule in rules:
            if not evaluate_rule(lead, rule, today):
                continue

            action = rule.get("action", {})
            summary = action.get("create_activity", "")
            priority = action.get("priority", "medium")
            move_to = action.get("move_to_stage")

            company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"

            # --- Idempotency: skip if identical open activity already exists ---
            if not dry_run and activity_is_duplicate(open_activities, summary):
                logger.debug(
                    "SKIP (duplicate) lead=%s rule='%s'", company, rule["name"]
                )
                skipped_count += 1
                continue

            logger.info(
                "TRIGGERED rule='%s' | lead=%s | priority=%s%s",
                rule["name"],
                company,
                priority,
                " [DRY RUN]" if dry_run else "",
            )

            action_taken = []

            if not dry_run:
                # --- Move to stage (optional) ---
                if move_to:
                    stage_id = _resolve_stage_id(odoo, move_to)
                    if stage_id:
                        try:
                            odoo.update_lead(lead["id"], {"stage_id": stage_id})
                            action_taken.append(f"moved to '{move_to}'")
                            logger.info(
                                "  Moved lead %s to stage '%s'", company, move_to
                            )
                        except Exception as exc:
                            logger.error(
                                "  Failed to move lead %s to '%s': %s",
                                company,
                                move_to,
                                exc,
                            )
                            error_count += 1

                # --- Create activity ---
                if summary:
                    # Due today; Odoo will show it as overdue if not completed
                    deadline = today
                    try:
                        odoo.create_activity(lead["id"], summary, deadline, priority)
                        action_taken.append("activity created")
                        created_count += 1
                    except Exception as exc:
                        logger.error(
                            "  Failed to create activity for lead %s: %s",
                            company,
                            exc,
                        )
                        error_count += 1
                        continue
            else:
                # Dry run — count as if we'd create it
                created_count += 1
                action_taken.append("would create activity")

            triggered.append({
                "lead": lead,
                "rule": rule,
                "priority": priority,
                "action_taken": ", ".join(action_taken),
            })

    # --- Summary ---
    logger.info(
        "Run complete | triggered=%d | created=%d | skipped=%d | errors=%d",
        len(triggered),
        created_count,
        skipped_count,
        error_count,
    )

    # --- Morning digest ---
    send_digest(triggered, digest_config, today)

    return triggered


def main():
    parser = argparse.ArgumentParser(
        description="Follow-up Scheduler — create Odoo activities for overdue leads."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate rules and print digest, but don't write to Odoo.",
    )
    parser.add_argument(
        "--stream",
        metavar="STREAM",
        default=None,
        help="Limit to a specific BD stream (e.g. stream_a, stream_c).",
    )
    args = parser.parse_args()

    try:
        triggered = run(dry_run=args.dry_run, stream_filter=args.stream)
    except EnvironmentError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
