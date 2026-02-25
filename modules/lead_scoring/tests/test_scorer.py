"""Unit tests for modules/lead_scoring/scorer.py.

All pure logic — no Odoo connection required.
"""

import json
from typing import Any

import pytest

from modules.lead_scoring.scorer import (
    _is_not_empty,
    _numeric_compare,
    breakdown_to_json,
    evaluate_condition,
    format_score_distribution,
    format_top_leads,
    resolve_field_value,
    score_lead,
)

# Minimal state cache used across tests
STATE_CACHE = {
    42: "NY",
    43: "PA",
    44: "NJ",
    45: "CT",
    46: "MA",
    47: "OH",
    99: "TX",
}


# ---------------------------------------------------------------------------
# _is_not_empty
# ---------------------------------------------------------------------------

class TestIsNotEmpty:
    def test_string_with_content(self):
        assert _is_not_empty("hello") is True

    def test_empty_string(self):
        assert _is_not_empty("") is False

    def test_whitespace_only(self):
        assert _is_not_empty("   ") is False

    def test_none(self):
        assert _is_not_empty(None) is False

    def test_false(self):
        assert _is_not_empty(False) is False

    def test_true(self):
        assert _is_not_empty(True) is True

    def test_zero_integer(self):
        # 0 is a valid integer value, considered non-empty
        assert _is_not_empty(0) is True

    def test_positive_integer(self):
        assert _is_not_empty(42) is True

    def test_negative_integer(self):
        assert _is_not_empty(-5) is True

    def test_nonempty_list(self):
        assert _is_not_empty(["a"]) is True

    def test_empty_list(self):
        assert _is_not_empty([]) is False


# ---------------------------------------------------------------------------
# resolve_field_value
# ---------------------------------------------------------------------------

class TestResolveFieldValue:
    def test_false_returns_none(self):
        assert resolve_field_value("any_field", False, STATE_CACHE) is None

    def test_none_returns_none(self):
        assert resolve_field_value("any_field", None, STATE_CACHE) is None

    def test_string_passthrough(self):
        assert resolve_field_value("city", "Syracuse", STATE_CACHE) == "Syracuse"

    def test_integer_passthrough(self):
        assert resolve_field_value("x_estimated_spaces", 100, STATE_CACHE) == 100

    def test_boolean_true_passthrough(self):
        assert resolve_field_value("x_already_importing", True, STATE_CACHE) is True

    def test_many2one_returns_name(self):
        # stage_id → extract the name
        assert resolve_field_value("stage_id", [10, "Research"], STATE_CACHE) == "Research"

    def test_state_id_returns_code(self):
        # state_id [42, "New York"] → "NY" via cache
        assert resolve_field_value("state_id", [42, "New York"], STATE_CACHE) == "NY"

    def test_state_id_unknown_id_falls_back_to_name(self):
        # ID not in cache → fall back to the Odoo name string
        assert resolve_field_value("state_id", [999, "Vermont"], STATE_CACHE) == "Vermont"

    def test_state_id_false_returns_none(self):
        assert resolve_field_value("state_id", False, STATE_CACHE) is None

    def test_non_state_many2one_returns_name(self):
        assert resolve_field_value("x_company_size", [5, "medium"], STATE_CACHE) == "medium"


# ---------------------------------------------------------------------------
# evaluate_condition
# ---------------------------------------------------------------------------

class TestEvaluateConditionIsEmpty:
    def test_false_is_empty(self):
        assert evaluate_condition("is empty", None) is True

    def test_empty_string_is_empty(self):
        assert evaluate_condition("is empty", "") is True

    def test_value_is_not_empty(self):
        assert evaluate_condition("is empty", "Operator C") is False

    def test_none_is_empty(self):
        assert evaluate_condition("is empty", None) is True


class TestEvaluateConditionIsNotEmpty:
    def test_string_is_not_empty(self):
        assert evaluate_condition("is not empty", "John Smith") is True

    def test_none_fails_is_not_empty(self):
        assert evaluate_condition("is not empty", None) is False

    def test_false_fails_is_not_empty(self):
        assert evaluate_condition("is not empty", None) is False

    def test_whitespace_fails_is_not_empty(self):
        assert evaluate_condition("is not empty", "   ") is False


class TestEvaluateConditionBooleans:
    def test_true_matches_true(self):
        assert evaluate_condition("== true", True) is True

    def test_false_does_not_match_true(self):
        assert evaluate_condition("== true", None) is False

    def test_none_matches_false(self):
        assert evaluate_condition("== false", None) is True

    def test_true_does_not_match_false(self):
        assert evaluate_condition("== false", True) is False

    def test_integer_one_matches_true(self):
        assert evaluate_condition("== true", 1) is True


class TestEvaluateConditionStringEquality:
    def test_match(self):
        assert evaluate_condition("== 'complete'", "complete") is True

    def test_no_match(self):
        assert evaluate_condition("== 'complete'", "pending") is False

    def test_none_no_match(self):
        assert evaluate_condition("== 'complete'", None) is False

    def test_empty_string_match(self):
        assert evaluate_condition("== ''", "") is True

    def test_case_sensitive(self):
        assert evaluate_condition("== 'complete'", "Complete") is False


class TestEvaluateConditionMembership:
    def test_value_in_list(self):
        assert evaluate_condition("in ['CN', 'TW', 'TH', 'VN']", "CN") is True

    def test_value_not_in_list(self):
        assert evaluate_condition("in ['CN', 'TW', 'TH', 'VN']", "DE") is False

    def test_none_not_in_list(self):
        assert evaluate_condition("in ['CN', 'TW']", None) is False

    def test_state_code_membership(self):
        assert evaluate_condition("in ['NY', 'PA', 'NJ', 'CT', 'MA']", "NY") is True

    def test_out_of_state(self):
        assert evaluate_condition("in ['NY', 'PA', 'NJ', 'CT', 'MA']", "TX") is False

    def test_city_membership(self):
        assert evaluate_condition("in ['Syracuse', 'Rochester']", "Syracuse") is True

    def test_city_not_in_list(self):
        assert evaluate_condition("in ['Syracuse', 'Rochester']", "Buffalo") is False

    def test_selection_membership(self):
        assert evaluate_condition("in ['garage', 'mixed_use']", "garage") is True

    def test_integer_in_list(self):
        assert evaluate_condition("in [1, 2, 3]", 2) is True


class TestEvaluateConditionNumeric:
    def test_gte_satisfied(self):
        assert evaluate_condition(">= 50", 100) is True

    def test_gte_exact(self):
        assert evaluate_condition(">= 50", 50) is True

    def test_gte_not_satisfied(self):
        assert evaluate_condition(">= 50", 49) is False

    def test_lte_satisfied(self):
        assert evaluate_condition("<= 100", 50) is True

    def test_gt_satisfied(self):
        assert evaluate_condition("> 0", 1) is True

    def test_gt_not_satisfied(self):
        assert evaluate_condition("> 0", 0) is False

    def test_lt_satisfied(self):
        assert evaluate_condition("< 10", 5) is True

    def test_negative_threshold(self):
        assert evaluate_condition(">= -10", -5) is True

    def test_none_value_fails(self):
        assert evaluate_condition(">= 50", None) is False

    def test_string_numeric_value(self):
        # Odoo may return "100" as a string for some fields
        assert evaluate_condition(">= 50", "100") is True

    def test_zero_value(self):
        assert evaluate_condition(">= 50", 0) is False


class TestEvaluateConditionEdgeCases:
    def test_unknown_condition_returns_false(self):
        assert evaluate_condition("this is not valid", "anything") is False

    def test_empty_condition_string(self):
        assert evaluate_condition("", "value") is False


# ---------------------------------------------------------------------------
# score_lead
# ---------------------------------------------------------------------------

PACKAGING_CRITERIA = [
    {"field": "x_already_importing", "condition": "== true",              "points": 25, "label": "Already imports from overseas"},
    {"field": "x_import_source_country", "condition": "in ['CN', 'TW', 'TH', 'VN']", "points": 15, "label": "Imports from China/SE Asia (switchable)"},
    {"field": "state_id",               "condition": "in ['NY', 'PA', 'NJ', 'CT', 'MA']", "points": 20, "label": "Northeast US (logistics advantage)"},
    {"field": "x_company_size",         "condition": "in ['medium', 'large']",  "points": 15, "label": "Right size ($5M-$500M)"},
    {"field": "contact_name",           "condition": "is not empty",             "points": 10, "label": "Decision maker identified"},
    {"field": "email_from",             "condition": "is not empty",             "points": 10, "label": "Email available"},
    {"field": "x_enrichment_status",    "condition": "== 'complete'",            "points":  5, "label": "Fully enriched"},
]

PARKING_CRITERIA = [
    {"field": "x_current_operator", "condition": "is empty",                 "points": 25, "label": "No current operator (easier conversion)"},
    {"field": "x_estimated_spaces", "condition": ">= 50",                    "points": 20, "label": "50+ spaces (meaningful revenue)"},
    {"field": "city",               "condition": "in ['Syracuse', 'Rochester']", "points": 20, "label": "Priority market"},
    {"field": "x_property_type",    "condition": "in ['garage', 'mixed_use']", "points": 15, "label": "Garage or mixed-use (higher revenue)"},
    {"field": "contact_name",       "condition": "is not empty",              "points": 10, "label": "Owner/manager identified"},
    {"field": "email_from",         "condition": "is not empty",              "points": 10, "label": "Contact info available"},
]


class TestScoreLead:
    def test_perfect_packaging_lead(self):
        lead = {
            "x_already_importing": True,
            "x_import_source_country": "CN",
            "state_id": [42, "New York"],
            "x_company_size": "medium",
            "contact_name": "Jane Doe",
            "email_from": "jane@bakery.com",
            "x_enrichment_status": "complete",
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert score == 100
        assert breakdown["total"] == 100
        assert breakdown["Already imports from overseas"] == 25
        assert breakdown["Northeast US (logistics advantage)"] == 20

    def test_zero_score_lead(self):
        """Lead with none of the criteria met."""
        lead = {
            "x_already_importing": False,
            "x_import_source_country": False,
            "state_id": [99, "Texas"],
            "x_company_size": "small",
            "contact_name": False,
            "email_from": False,
            "x_enrichment_status": "pending",
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert score == 0
        assert breakdown["total"] == 0
        # Only "total" key should be in breakdown
        assert list(breakdown.keys()) == ["total"]

    def test_partial_packaging_lead(self):
        """Lead qualifying for some but not all criteria."""
        lead = {
            "x_already_importing": True,          # +25
            "x_import_source_country": "DE",       # not in list — 0
            "state_id": [42, "New York"],          # +20
            "x_company_size": "small",             # not in list — 0
            "contact_name": False,                 # empty — 0
            "email_from": False,                   # empty — 0
            "x_enrichment_status": "pending",      # not complete — 0
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert score == 45
        assert "Already imports from overseas" in breakdown
        assert "Northeast US (logistics advantage)" in breakdown
        assert "Decision maker identified" not in breakdown

    def test_parking_no_operator_score(self):
        lead = {
            "x_current_operator": False,   # empty → +25
            "x_estimated_spaces": 120,     # >= 50 → +20
            "city": "Syracuse",            # in list → +20
            "x_property_type": "garage",   # in list → +15
            "contact_name": "Bob Smith",   # not empty → +10
            "email_from": False,           # empty → 0
        }
        score, breakdown = score_lead(lead, PARKING_CRITERIA, STATE_CACHE)
        assert score == 90
        assert "No current operator (easier conversion)" in breakdown

    def test_parking_with_existing_operator_loses_points(self):
        lead = {
            "x_current_operator": "Operator C",  # NOT empty → 0 (not "is empty")
            "x_estimated_spaces": 30,              # < 50 → 0
            "city": "Albany",                      # not in priority list → 0
            "x_property_type": "surface_lot",      # not in list → 0
            "contact_name": False,
            "email_from": False,
        }
        score, breakdown = score_lead(lead, PARKING_CRITERIA, STATE_CACHE)
        assert score == 0

    def test_negative_points_supported(self):
        """A criterion with negative points reduces the total."""
        criteria = [
            {"field": "x_already_importing", "condition": "== true", "points": 25, "label": "Imports"},
            {"field": "city", "condition": "in ['Detroit', 'Cleveland']", "points": -10, "label": "Difficult market"},
        ]
        lead = {"x_already_importing": True, "city": "Detroit"}
        score, breakdown = score_lead(lead, criteria, STATE_CACHE)
        assert score == 15
        assert breakdown["Difficult market"] == -10

    def test_breakdown_always_has_total(self):
        lead = {}
        _, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert "total" in breakdown

    def test_breakdown_matches_total(self):
        lead = {
            "x_already_importing": True,
            "state_id": [42, "New York"],
            "contact_name": "Jane Doe",
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert breakdown["total"] == score
        expected = sum(v for k, v in breakdown.items() if k != "total")
        assert score == expected

    def test_missing_field_scores_zero_for_that_criterion(self):
        """A field missing from the lead dict should not raise."""
        lead = {
            "x_already_importing": True,   # +25
            # state_id, x_company_size etc. intentionally missing
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert score == 25

    def test_empty_criteria_list(self):
        lead = {"x_already_importing": True}
        score, breakdown = score_lead(lead, [], STATE_CACHE)
        assert score == 0
        assert breakdown == {"total": 0}

    def test_state_id_false_scores_zero(self):
        lead = {
            "x_already_importing": True,
            "state_id": False,            # unset
        }
        score, breakdown = score_lead(lead, PACKAGING_CRITERIA, STATE_CACHE)
        assert "Northeast US (logistics advantage)" not in breakdown


# ---------------------------------------------------------------------------
# breakdown_to_json
# ---------------------------------------------------------------------------

class TestBreakdownToJson:
    def test_round_trips_correctly(self):
        bd = {"Already imports": 25, "Northeast US": 20, "total": 45}
        result = breakdown_to_json(bd)
        parsed = json.loads(result)
        assert parsed == bd

    def test_empty_breakdown(self):
        assert breakdown_to_json({"total": 0}) == '{"total": 0}'


# ---------------------------------------------------------------------------
# format helpers (smoke tests — just verify they return strings)
# ---------------------------------------------------------------------------

class TestFormatHelpers:
    def _make_scored(self, n=5) -> list:
        leads = [
            {"id": i, "partner_name": f"Company {i}", "city": "Syracuse",
             "x_bd_stream": "stream_c", "stage_id": [1, "Research"]}
            for i in range(1, n + 1)
        ]
        breakdowns = [{"Criterion A": 25, "total": 25 * i} for i in range(1, n + 1)]
        return [(lead, 25 * (i + 1), bd) for i, (lead, bd) in enumerate(zip(leads, breakdowns))]

    def test_format_top_leads_returns_string(self):
        scored = self._make_scored(5)
        result = format_top_leads(scored, n=3)
        assert isinstance(result, str)
        assert "Company" in result

    def test_format_top_leads_respects_n(self):
        scored = self._make_scored(10)
        result = format_top_leads(scored, n=3)
        # Should show at most 3 leads
        assert result.count("Company") <= 3

    def test_format_score_distribution_returns_string(self):
        scores = [10, 25, 55, 70, 85, 90, 45, 30, 60]
        result = format_score_distribution(scores)
        assert isinstance(result, str)
        assert "80" in result
        assert "60" in result

    def test_format_score_distribution_empty(self):
        result = format_score_distribution([])
        assert isinstance(result, str)
