"""Unit tests for modules/followup_scheduler/rule_engine.py.

All tests are pure Python — no Odoo connection required.
"""

from datetime import date, timedelta

import pytest

from modules.followup_scheduler.rule_engine import (
    activity_is_duplicate,
    evaluate_rule,
    parse_odoo_date,
)

TODAY = date(2026, 2, 24)


# ---------------------------------------------------------------------------
# Fixtures — reusable lead and rule templates
# ---------------------------------------------------------------------------


def make_lead(**overrides) -> dict:
    """Base lead dict that satisfies most rules by default."""
    lead = {
        "id": 1,
        "name": "Test Lead",
        "partner_name": "Acme Corp",
        "stage_id": [10, "Outreach"],
        "x_last_personal_contact": (TODAY - timedelta(days=6)).isoformat(),
        "x_sample_sent_date": False,
        "write_date": (TODAY - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S"),
        "city": "Syracuse",
        "x_bd_stream": "stream_c",
    }
    lead.update(overrides)
    return lead


def make_rule(stage="Outreach", days_field="x_last_personal_contact", threshold=5, **action_overrides) -> dict:
    action = {
        "create_activity": "Follow up on initial outreach — no response yet",
        "priority": "medium",
    }
    action.update(action_overrides)
    return {
        "name": "Outreach follow-up",
        "condition": {
            "stage": stage,
            "days_since": days_field,
            "threshold": threshold,
        },
        "action": action,
    }


# ---------------------------------------------------------------------------
# parse_odoo_date
# ---------------------------------------------------------------------------


class TestParseOdooDate:
    def test_date_string(self):
        assert parse_odoo_date("2026-01-15") == date(2026, 1, 15)

    def test_datetime_string(self):
        assert parse_odoo_date("2026-01-15 10:30:00") == date(2026, 1, 15)

    def test_false(self):
        assert parse_odoo_date(False) is None

    def test_none(self):
        assert parse_odoo_date(None) is None

    def test_empty_string(self):
        assert parse_odoo_date("") is None

    def test_date_object_passthrough(self):
        d = date(2026, 3, 1)
        assert parse_odoo_date(d) == d


# ---------------------------------------------------------------------------
# evaluate_rule — stage matching
# ---------------------------------------------------------------------------


class TestStageMatching:
    def test_stage_string_match(self):
        lead = make_lead(stage_id=[10, "Outreach"])
        rule = make_rule(stage="Outreach", threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_stage_string_no_match(self):
        lead = make_lead(stage_id=[10, "Research"])
        rule = make_rule(stage="Outreach", threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_stage_list_match_first(self):
        lead = make_lead(stage_id=[10, "Engaged"])
        rule = make_rule(stage=["Engaged", "Negotiating"], days_field="x_last_personal_contact", threshold=14)
        # 6 days elapsed < 14 threshold → False (stage matches but threshold not met)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_stage_list_match_second(self):
        lead = make_lead(
            stage_id=[10, "Negotiating"],
            x_last_personal_contact=(TODAY - timedelta(days=15)).isoformat(),
        )
        rule = make_rule(stage=["Engaged", "Negotiating"], days_field="x_last_personal_contact", threshold=14)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_stage_list_no_match(self):
        lead = make_lead(stage_id=[10, "Qualified"])
        rule = make_rule(stage=["Engaged", "Negotiating"], days_field="x_last_personal_contact", threshold=14)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_missing_stage_field(self):
        lead = make_lead(stage_id=False)
        rule = make_rule(stage="Outreach")
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_stage_as_plain_string_in_lead(self):
        """Lead stage_id as plain string (non-standard but tolerated)."""
        lead = make_lead(stage_id="Outreach")
        rule = make_rule(stage="Outreach", threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is True


# ---------------------------------------------------------------------------
# evaluate_rule — days_since threshold
# ---------------------------------------------------------------------------


class TestDaysSinceThreshold:
    def test_exactly_at_threshold(self):
        """Threshold is inclusive: days_elapsed >= threshold → fires."""
        lead = make_lead(
            x_last_personal_contact=(TODAY - timedelta(days=5)).isoformat()
        )
        rule = make_rule(threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_one_day_over_threshold(self):
        lead = make_lead(
            x_last_personal_contact=(TODAY - timedelta(days=6)).isoformat()
        )
        rule = make_rule(threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_one_day_under_threshold(self):
        lead = make_lead(
            x_last_personal_contact=(TODAY - timedelta(days=4)).isoformat()
        )
        rule = make_rule(threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_threshold_zero_fires_immediately(self):
        lead = make_lead(
            x_last_personal_contact=TODAY.isoformat()
        )
        rule = make_rule(threshold=0)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_empty_date_field_does_not_fire(self):
        lead = make_lead(x_last_personal_contact=False)
        rule = make_rule(threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_none_date_field_does_not_fire(self):
        lead = make_lead(x_last_personal_contact=None)
        rule = make_rule(threshold=5)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_write_date_datetime_string(self):
        """write_date comes as a datetime string from Odoo."""
        write_dt = (TODAY - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
        lead = make_lead(stage_id=[10, "Qualified"], write_date=write_dt)
        rule = make_rule(stage="Qualified", days_field="write_date", threshold=3)
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_write_date_below_threshold(self):
        write_dt = (TODAY - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        lead = make_lead(stage_id=[10, "Qualified"], write_date=write_dt)
        rule = make_rule(stage="Qualified", days_field="write_date", threshold=3)
        assert evaluate_rule(lead, rule, TODAY) is False

    def test_sample_sent_date(self):
        lead = make_lead(
            stage_id=[10, "Samples Sent"],
            x_sample_sent_date=(TODAY - timedelta(days=8)).isoformat(),
        )
        rule = make_rule(
            stage="Samples Sent",
            days_field="x_sample_sent_date",
            threshold=7,
            **{"create_activity": "Check on samples", "priority": "high"},
        )
        assert evaluate_rule(lead, rule, TODAY) is True


# ---------------------------------------------------------------------------
# evaluate_rule — rule without stage condition
# ---------------------------------------------------------------------------


class TestRuleWithoutStage:
    def test_no_stage_condition_checks_only_days(self):
        """A rule with no stage condition fires based on days_since alone."""
        lead = make_lead(
            x_last_personal_contact=(TODAY - timedelta(days=10)).isoformat()
        )
        rule = {
            "name": "Days-only rule",
            "condition": {
                "days_since": "x_last_personal_contact",
                "threshold": 5,
            },
            "action": {"create_activity": "Follow up", "priority": "medium"},
        }
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_no_condition_at_all_always_fires(self):
        """A rule with empty condition should fire for any lead."""
        lead = make_lead()
        rule = {
            "name": "Always-fire rule",
            "condition": {},
            "action": {"create_activity": "Check in", "priority": "low"},
        }
        assert evaluate_rule(lead, rule, TODAY) is True


# ---------------------------------------------------------------------------
# Six-month re-engage rule (the dormant rule)
# ---------------------------------------------------------------------------


class TestDormantRule:
    def test_dormant_fires_after_180_days(self):
        write_dt = (TODAY - timedelta(days=181)).strftime("%Y-%m-%d %H:%M:%S")
        lead = make_lead(stage_id=[20, "Not Now"], write_date=write_dt)
        rule = make_rule(
            stage="Not Now",
            days_field="write_date",
            threshold=180,
            **{"create_activity": "6-month re-engagement", "priority": "low", "move_to_stage": "Research"},
        )
        assert evaluate_rule(lead, rule, TODAY) is True

    def test_dormant_does_not_fire_at_90_days(self):
        write_dt = (TODAY - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
        lead = make_lead(stage_id=[20, "Not Now"], write_date=write_dt)
        rule = make_rule(
            stage="Not Now",
            days_field="write_date",
            threshold=180,
        )
        assert evaluate_rule(lead, rule, TODAY) is False


# ---------------------------------------------------------------------------
# activity_is_duplicate
# ---------------------------------------------------------------------------


class TestActivityIsDuplicate:
    def test_exact_match(self):
        existing = [{"id": 1, "summary": "Follow up on initial outreach — no response yet"}]
        assert activity_is_duplicate(existing, "Follow up on initial outreach — no response yet") is True

    def test_case_insensitive(self):
        existing = [{"id": 1, "summary": "Follow Up On Initial Outreach"}]
        assert activity_is_duplicate(existing, "follow up on initial outreach") is True

    def test_whitespace_stripped(self):
        existing = [{"id": 1, "summary": "  Check on samples  "}]
        assert activity_is_duplicate(existing, "Check on samples") is True

    def test_different_summary(self):
        existing = [{"id": 1, "summary": "Follow up on proposal"}]
        assert activity_is_duplicate(existing, "Follow up on initial outreach") is False

    def test_empty_list(self):
        assert activity_is_duplicate([], "Any activity") is False

    def test_none_summary_in_existing(self):
        existing = [{"id": 1, "summary": None}]
        assert activity_is_duplicate(existing, "Something") is False

    def test_multiple_existing_activities(self):
        existing = [
            {"id": 1, "summary": "Review proposal"},
            {"id": 2, "summary": "Check on samples"},
        ]
        assert activity_is_duplicate(existing, "check on samples") is True
        assert activity_is_duplicate(existing, "call decision maker") is False
