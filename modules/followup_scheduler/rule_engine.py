"""Rule evaluation engine for the Follow-up Scheduler.

All functions here are pure (no Odoo dependency) so they can be unit-tested
without mocking a live connection.

A rule has this YAML shape:
    condition:
      stage: "Outreach"            # str or list of str
      days_since: x_last_personal_contact   # Odoo field name
      threshold: 5                 # fire when days_elapsed >= threshold
    action:
      create_activity: "Follow up on initial outreach"
      priority: medium             # low | medium | high
      move_to_stage: "Research"    # optional
      send_notification: true      # optional

Odoo date/datetime fields come back as strings ("2026-01-15" or
"2026-01-15 10:30:00") or as False when empty.  write_date is always a
datetime string when present.
"""

from datetime import date, datetime
from typing import Union


def parse_odoo_date(value) -> Union[date, None]:
    """Parse an Odoo date or datetime string into a date object.

    Accepts:
      - "2026-01-15"            (date field)
      - "2026-01-15 10:30:00"   (datetime field, e.g. write_date)
      - False / None / ""       → returns None
    """
    if not value:
        return None
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if len(s) >= 10:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _stage_matches(lead_stage_name: str, rule_stage) -> bool:
    """Return True if the lead's stage matches the rule's stage condition.

    rule_stage may be a single string or a list of strings.
    """
    if isinstance(rule_stage, list):
        return lead_stage_name in rule_stage
    return lead_stage_name == rule_stage


def evaluate_rule(lead: dict, rule: dict, today: date = None) -> bool:
    """Evaluate whether a single rule fires for the given lead.

    Args:
        lead:  Odoo lead dict.  stage_id is expected as [id, name] (Odoo
               many2one format) or a plain string.
        rule:  Rule dict loaded from followup_rules.yaml.
        today: The reference date; defaults to date.today().

    Returns:
        True if ALL conditions in the rule are satisfied.
    """
    if today is None:
        today = date.today()

    condition = rule.get("condition", {})

    # --- Stage match ---
    rule_stage = condition.get("stage")
    if rule_stage is not None:
        stage_id_field = lead.get("stage_id")
        # Odoo many2one: [id, "Name"] or False
        if isinstance(stage_id_field, (list, tuple)) and len(stage_id_field) == 2:
            lead_stage_name = stage_id_field[1]
        elif isinstance(stage_id_field, str):
            lead_stage_name = stage_id_field
        else:
            return False  # No stage on lead

        if not _stage_matches(lead_stage_name, rule_stage):
            return False

    # --- Days-since threshold ---
    days_field = condition.get("days_since")
    threshold = condition.get("threshold")

    if days_field is not None and threshold is not None:
        raw_value = lead.get(days_field)
        field_date = parse_odoo_date(raw_value)
        if field_date is None:
            # Date field is empty — rule cannot fire
            return False
        days_elapsed = (today - field_date).days
        if days_elapsed < threshold:
            return False

    return True


def activity_is_duplicate(
    existing_activities: list[dict], summary: str
) -> bool:
    """Return True if an open activity with the same summary already exists.

    Used to enforce idempotency: if we already created this reminder today
    (or it hasn't been completed yet), don't create another one.

    Args:
        existing_activities: list returned by OdooClient.get_open_activities().
        summary: The activity summary string we're about to create.
    """
    summary_lower = summary.strip().lower()
    for activity in existing_activities:
        existing_summary = (activity.get("summary") or "").strip().lower()
        if existing_summary == summary_lower:
            return True
    return False
