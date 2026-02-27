"""Tests for Pipeline Reporter — Module 7.

All tests are pure unit tests (no live network or Odoo calls).
reporter.py and main.py are tested via mocking.
"""

import os
import tempfile
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from modules.pipeline_reporter.reporter import (
    DEFAULT_ATTENTION_SCORE,
    DEFAULT_STALE_DAYS,
    PIPELINE_STAGES,
    TERMINAL_STAGES,
    _parse_date,
    _resolve_many2one,
    _stage_name,
    build_monthly_report,
    build_weekly_report,
    conversion_funnel,
    format_conversion_funnel,
    format_lead_list,
    format_pipeline_summary_table,
    format_score_distribution,
    format_source_effectiveness,
    leads_needing_attention,
    new_leads_this_week,
    pipeline_summary_by_stream,
    score_distribution,
    source_effectiveness,
    stale_leads,
    top_leads,
)


# ===========================================================================
# Fixtures
# ===========================================================================

TODAY = date(2026, 2, 27)


def make_lead(**kwargs):
    defaults = {
        "id": 1,
        "name": "Test Lead",
        "partner_name": "Acme Corp",
        "contact_name": "Jane Doe",
        "email_from": "jane@acme.com",
        "city": "Metropolis",
        "state_id": [42, "New York"],
        "stage_id": [3, "Qualified"],
        "x_bd_stream": "stream_c",
        "x_lead_score": 75,
        "x_data_source": "trade_data",
        "x_enrichment_status": "complete",
        "x_last_personal_contact": False,
        "create_date": "2026-02-20 10:00:00",
        "write_date": "2026-02-25 14:00:00",
    }
    defaults.update(kwargs)
    return defaults


# ===========================================================================
# 1. Helper functions
# ===========================================================================

class TestResolveMany2one:
    def test_list_many2one(self):
        assert _resolve_many2one([3, "Qualified"]) == "Qualified"

    def test_tuple_many2one(self):
        assert _resolve_many2one((3, "Qualified")) == "Qualified"

    def test_plain_string(self):
        assert _resolve_many2one("Qualified") == "Qualified"

    def test_false(self):
        assert _resolve_many2one(False) is False

    def test_none(self):
        assert _resolve_many2one(None) is None


class TestStageName:
    def test_many2one(self):
        assert _stage_name({"stage_id": [3, "Qualified"]}) == "Qualified"

    def test_missing(self):
        assert _stage_name({}) == "Unknown"

    def test_false(self):
        assert _stage_name({"stage_id": False}) == "Unknown"


class TestParseDate:
    def test_iso_string(self):
        assert _parse_date("2026-02-20") == date(2026, 2, 20)

    def test_datetime_string(self):
        assert _parse_date("2026-02-20 10:00:00") == date(2026, 2, 20)

    def test_date_object(self):
        d = date(2026, 1, 15)
        assert _parse_date(d) == d

    def test_false(self):
        assert _parse_date(False) is None

    def test_none(self):
        assert _parse_date(None) is None

    def test_invalid(self):
        assert _parse_date("not-a-date") is None

    def test_empty_string(self):
        assert _parse_date("") is None


# ===========================================================================
# 2. pipeline_summary_by_stream
# ===========================================================================

class TestPipelineSummaryByStream:
    def test_single_stream_single_stage(self):
        leads = [make_lead(x_bd_stream="stream_c", stage_id=[3, "Qualified"])]
        result = pipeline_summary_by_stream(leads)
        assert result == {"stream_c": {"Qualified": 1}}

    def test_multiple_streams(self):
        leads = [
            make_lead(id=1, x_bd_stream="stream_c", stage_id=[3, "Qualified"]),
            make_lead(id=2, x_bd_stream="stream_a", stage_id=[1, "Research"]),
            make_lead(id=3, x_bd_stream="stream_c", stage_id=[1, "Research"]),
        ]
        result = pipeline_summary_by_stream(leads)
        assert result["stream_c"]["Qualified"] == 1
        assert result["stream_c"]["Research"] == 1
        assert result["stream_a"]["Research"] == 1

    def test_empty_leads(self):
        assert pipeline_summary_by_stream([]) == {}

    def test_unassigned_stream(self):
        leads = [make_lead(x_bd_stream=False)]
        result = pipeline_summary_by_stream(leads)
        assert "unassigned" in result

    def test_multiple_same_stage(self):
        leads = [
            make_lead(id=1, x_bd_stream="stream_c", stage_id=[3, "Qualified"]),
            make_lead(id=2, x_bd_stream="stream_c", stage_id=[3, "Qualified"]),
        ]
        result = pipeline_summary_by_stream(leads)
        assert result["stream_c"]["Qualified"] == 2


# ===========================================================================
# 3. new_leads_this_week
# ===========================================================================

class TestNewLeadsThisWeek:
    def test_lead_within_week(self):
        lead = make_lead(create_date="2026-02-22")
        result = new_leads_this_week([lead], reference_date=TODAY)
        assert len(result) == 1

    def test_lead_outside_week(self):
        lead = make_lead(create_date="2026-02-10")
        result = new_leads_this_week([lead], reference_date=TODAY)
        assert len(result) == 0

    def test_lead_on_boundary(self):
        """Lead created exactly 7 days ago should be included."""
        cutoff_date = TODAY - timedelta(days=7)
        lead = make_lead(create_date=cutoff_date.isoformat())
        result = new_leads_this_week([lead], reference_date=TODAY)
        assert len(result) == 1

    def test_lead_one_day_before_boundary(self):
        cutoff_date = TODAY - timedelta(days=8)
        lead = make_lead(create_date=cutoff_date.isoformat())
        result = new_leads_this_week([lead], reference_date=TODAY)
        assert len(result) == 0

    def test_mixed_new_and_old(self):
        leads = [
            make_lead(id=1, create_date="2026-02-26"),  # new
            make_lead(id=2, create_date="2026-01-15"),  # old
            make_lead(id=3, create_date="2026-02-25"),  # new
        ]
        result = new_leads_this_week(leads, reference_date=TODAY)
        assert len(result) == 2

    def test_empty_leads(self):
        assert new_leads_this_week([], reference_date=TODAY) == []

    def test_missing_create_date(self):
        lead = make_lead(create_date=False)
        result = new_leads_this_week([lead], reference_date=TODAY)
        assert len(result) == 0


# ===========================================================================
# 4. stale_leads
# ===========================================================================

class TestStaleLeads:
    def test_stale_lead(self):
        """Lead with write_date 30 days ago is stale."""
        old_date = (TODAY - timedelta(days=30)).isoformat()
        lead = make_lead(write_date=old_date, stage_id=[1, "Research"])
        result = stale_leads([lead], reference_date=TODAY)
        assert len(result) == 1

    def test_fresh_lead(self):
        lead = make_lead(write_date=TODAY.isoformat(), stage_id=[1, "Research"])
        result = stale_leads([lead], reference_date=TODAY)
        assert len(result) == 0

    def test_terminal_stage_excluded(self):
        old_date = (TODAY - timedelta(days=30)).isoformat()
        lead = make_lead(write_date=old_date, stage_id=[10, "Won"])
        result = stale_leads([lead], reference_date=TODAY)
        assert len(result) == 0

    def test_boundary_not_stale(self):
        """Lead with write_date exactly 21 days ago is NOT stale (cutoff is exclusive)."""
        boundary = (TODAY - timedelta(days=DEFAULT_STALE_DAYS)).isoformat()
        lead = make_lead(write_date=boundary, stage_id=[1, "Research"])
        result = stale_leads([lead], reference_date=TODAY)
        assert len(result) == 0

    def test_boundary_plus_one_is_stale(self):
        stale_date = (TODAY - timedelta(days=DEFAULT_STALE_DAYS + 1)).isoformat()
        lead = make_lead(write_date=stale_date, stage_id=[1, "Research"])
        result = stale_leads([lead], reference_date=TODAY)
        assert len(result) == 1

    def test_sorted_oldest_first(self):
        leads = [
            make_lead(id=1, write_date="2026-01-15", stage_id=[1, "Research"]),
            make_lead(id=2, write_date="2026-01-01", stage_id=[1, "Research"]),
            make_lead(id=3, write_date="2026-01-20", stage_id=[1, "Research"]),
        ]
        result = stale_leads(leads, reference_date=TODAY)
        assert result[0]["id"] == 2
        assert result[1]["id"] == 1
        assert result[2]["id"] == 3

    def test_empty_leads(self):
        assert stale_leads([], reference_date=TODAY) == []

    def test_custom_stale_days(self):
        old_date = (TODAY - timedelta(days=10)).isoformat()
        lead = make_lead(write_date=old_date, stage_id=[1, "Research"])
        # 10 days old, stale_days=7 → stale
        assert len(stale_leads([lead], stale_days=7, reference_date=TODAY)) == 1
        # 10 days old, stale_days=14 → not stale
        assert len(stale_leads([lead], stale_days=14, reference_date=TODAY)) == 0


# ===========================================================================
# 5. leads_needing_attention
# ===========================================================================

class TestLeadsNeedingAttention:
    def test_high_score_in_research(self):
        lead = make_lead(stage_id=[1, "Research"], x_lead_score=75)
        result = leads_needing_attention([lead])
        assert len(result) == 1

    def test_low_score_in_research(self):
        lead = make_lead(stage_id=[1, "Research"], x_lead_score=30)
        result = leads_needing_attention([lead])
        assert len(result) == 0

    def test_high_score_not_research(self):
        lead = make_lead(stage_id=[3, "Qualified"], x_lead_score=85)
        result = leads_needing_attention([lead])
        assert len(result) == 0

    def test_boundary_score(self):
        lead = make_lead(stage_id=[1, "Research"], x_lead_score=DEFAULT_ATTENTION_SCORE)
        result = leads_needing_attention([lead])
        assert len(result) == 1

    def test_below_boundary(self):
        lead = make_lead(stage_id=[1, "Research"], x_lead_score=DEFAULT_ATTENTION_SCORE - 1)
        result = leads_needing_attention([lead])
        assert len(result) == 0

    def test_sorted_by_score_descending(self):
        leads = [
            make_lead(id=1, stage_id=[1, "Research"], x_lead_score=65),
            make_lead(id=2, stage_id=[1, "Research"], x_lead_score=90),
            make_lead(id=3, stage_id=[1, "Research"], x_lead_score=75),
        ]
        result = leads_needing_attention(leads)
        assert [r["id"] for r in result] == [2, 3, 1]

    def test_empty(self):
        assert leads_needing_attention([]) == []

    def test_zero_score(self):
        lead = make_lead(stage_id=[1, "Research"], x_lead_score=0)
        result = leads_needing_attention([lead])
        assert len(result) == 0


# ===========================================================================
# 6. top_leads
# ===========================================================================

class TestTopLeads:
    def test_returns_top_n(self):
        leads = [make_lead(id=i, x_lead_score=i * 10, stage_id=[1, "Research"]) for i in range(1, 8)]
        result = top_leads(leads, n=3)
        assert len(result) == 3
        assert result[0]["x_lead_score"] == 70
        assert result[1]["x_lead_score"] == 60
        assert result[2]["x_lead_score"] == 50

    def test_excludes_terminal(self):
        leads = [
            make_lead(id=1, x_lead_score=90, stage_id=[10, "Won"]),
            make_lead(id=2, x_lead_score=80, stage_id=[1, "Research"]),
        ]
        result = top_leads(leads, n=5)
        assert len(result) == 1
        assert result[0]["id"] == 2

    def test_fewer_than_n(self):
        leads = [make_lead(id=1, x_lead_score=50)]
        result = top_leads(leads, n=5)
        assert len(result) == 1

    def test_empty(self):
        assert top_leads([], n=5) == []

    def test_default_n_is_5(self):
        leads = [make_lead(id=i, x_lead_score=i * 10, stage_id=[1, "Research"]) for i in range(1, 10)]
        result = top_leads(leads)
        assert len(result) == 5


# ===========================================================================
# 7. score_distribution
# ===========================================================================

class TestScoreDistribution:
    def test_all_buckets(self):
        leads = [
            make_lead(id=1, x_lead_score=5, stage_id=[1, "Research"]),
            make_lead(id=2, x_lead_score=25, stage_id=[1, "Research"]),
            make_lead(id=3, x_lead_score=45, stage_id=[1, "Research"]),
            make_lead(id=4, x_lead_score=65, stage_id=[1, "Research"]),
            make_lead(id=5, x_lead_score=85, stage_id=[1, "Research"]),
        ]
        result = score_distribution(leads)
        assert result == {"0-19": 1, "20-39": 1, "40-59": 1, "60-79": 1, "80-100": 1}

    def test_zero_score(self):
        leads = [make_lead(x_lead_score=0, stage_id=[1, "Research"])]
        result = score_distribution(leads)
        assert result["0-19"] == 1

    def test_score_100(self):
        leads = [make_lead(x_lead_score=100, stage_id=[1, "Research"])]
        result = score_distribution(leads)
        assert result["80-100"] == 1

    def test_terminal_excluded(self):
        leads = [make_lead(x_lead_score=90, stage_id=[10, "Won"])]
        result = score_distribution(leads)
        assert all(v == 0 for v in result.values())

    def test_empty(self):
        result = score_distribution([])
        assert all(v == 0 for v in result.values())

    def test_boundary_20(self):
        leads = [make_lead(x_lead_score=20, stage_id=[1, "Research"])]
        result = score_distribution(leads)
        assert result["20-39"] == 1
        assert result["0-19"] == 0


# ===========================================================================
# 8. source_effectiveness
# ===========================================================================

class TestSourceEffectiveness:
    def test_single_source(self):
        leads = [
            make_lead(id=1, x_data_source="trade_data", x_lead_score=70),
            make_lead(id=2, x_data_source="trade_data", x_lead_score=80),
        ]
        result = source_effectiveness(leads)
        assert result["trade_data"]["count"] == 2
        assert result["trade_data"]["avg_score"] == 75.0

    def test_multiple_sources(self):
        leads = [
            make_lead(id=1, x_data_source="trade_data", x_lead_score=70),
            make_lead(id=2, x_data_source="google_maps", x_lead_score=40),
        ]
        result = source_effectiveness(leads)
        assert "trade_data" in result
        assert "google_maps" in result
        assert result["trade_data"]["avg_score"] == 70.0
        assert result["google_maps"]["avg_score"] == 40.0

    def test_unknown_source(self):
        leads = [make_lead(x_data_source=False, x_lead_score=50)]
        result = source_effectiveness(leads)
        assert "unknown" in result

    def test_empty(self):
        assert source_effectiveness([]) == {}

    def test_sorted_by_source_name(self):
        leads = [
            make_lead(id=1, x_data_source="z_source", x_lead_score=50),
            make_lead(id=2, x_data_source="a_source", x_lead_score=60),
        ]
        result = source_effectiveness(leads)
        assert list(result.keys()) == ["a_source", "z_source"]


# ===========================================================================
# 9. conversion_funnel
# ===========================================================================

class TestConversionFunnel:
    def test_research_lead_counts_once(self):
        leads = [make_lead(stage_id=[1, "Research"])]
        result = conversion_funnel(leads)
        assert result["Research"] == 1
        assert result["Qualified"] == 0

    def test_qualified_lead_counts_research_and_qualified(self):
        leads = [make_lead(stage_id=[3, "Qualified"])]
        result = conversion_funnel(leads)
        assert result["Research"] == 1
        assert result["Qualified"] == 1
        assert result["Outreach"] == 0

    def test_won_lead_counts_all(self):
        leads = [make_lead(stage_id=[10, "Won"])]
        result = conversion_funnel(leads)
        assert result["Research"] == 1
        assert result["Won"] == 1

    def test_lost_excluded(self):
        leads = [make_lead(stage_id=[11, "Lost"])]
        result = conversion_funnel(leads)
        assert all(v == 0 for v in result.values())

    def test_not_now_excluded(self):
        leads = [make_lead(stage_id=[12, "Not Now"])]
        result = conversion_funnel(leads)
        assert all(v == 0 for v in result.values())

    def test_multiple_leads(self):
        leads = [
            make_lead(id=1, stage_id=[1, "Research"]),
            make_lead(id=2, stage_id=[3, "Qualified"]),
            make_lead(id=3, stage_id=[5, "Outreach"]),
        ]
        result = conversion_funnel(leads)
        assert result["Research"] == 3
        assert result["Qualified"] == 2
        assert result["Outreach"] == 1

    def test_empty(self):
        result = conversion_funnel([])
        assert all(v == 0 for v in result.values())


# ===========================================================================
# 10. Formatting functions
# ===========================================================================

class TestFormatPipelineSummaryTable:
    def test_basic_table(self):
        summary = {"stream_c": {"Research": 2, "Qualified": 1}}
        table = format_pipeline_summary_table(summary)
        assert "| Stage |" in table
        assert "stream_c" in table
        assert "Research" in table
        assert "Qualified" in table
        assert "Total" in table

    def test_empty_summary(self):
        assert "No leads" in format_pipeline_summary_table({})

    def test_multiple_streams(self):
        summary = {
            "stream_c": {"Research": 2},
            "stream_a": {"Qualified": 1},
        }
        table = format_pipeline_summary_table(summary)
        assert "stream_c" in table
        assert "stream_a" in table

    def test_totals_row(self):
        summary = {"stream_a": {"Research": 3, "Qualified": 2}}
        table = format_pipeline_summary_table(summary)
        assert "**Total**" in table
        assert "**5**" in table


class TestFormatLeadList:
    def test_single_lead(self):
        leads = [make_lead()]
        result = format_lead_list(leads)
        assert "**Acme Corp**" in result
        assert "score: 75" in result

    def test_empty(self):
        assert "None" in format_lead_list([])

    def test_missing_partner_name(self):
        lead = make_lead(partner_name=False, name="Fallback Name")
        result = format_lead_list([lead])
        assert "Fallback Name" in result

    def test_includes_stream_and_stage(self):
        leads = [make_lead()]
        result = format_lead_list(leads)
        assert "stream_c" in result
        assert "Qualified" in result


class TestFormatScoreDistribution:
    def test_with_data(self):
        dist = {"0-19": 2, "20-39": 5, "40-59": 3, "60-79": 1, "80-100": 0}
        result = format_score_distribution(dist)
        assert "0-19" in result
        assert "█" in result

    def test_all_zeros(self):
        dist = {"0-19": 0, "20-39": 0, "40-59": 0, "60-79": 0, "80-100": 0}
        result = format_score_distribution(dist)
        assert "No scored leads" in result


class TestFormatSourceEffectiveness:
    def test_with_data(self):
        sources = {"trade_data": {"count": 5, "avg_score": 72.5}}
        result = format_source_effectiveness(sources)
        assert "trade_data" in result
        assert "72.5" in result
        assert "Source" in result

    def test_empty(self):
        assert "No source data" in format_source_effectiveness({})

    def test_sorted_by_avg_score_descending(self):
        sources = {
            "low_source": {"count": 3, "avg_score": 30.0},
            "high_source": {"count": 2, "avg_score": 80.0},
        }
        result = format_source_effectiveness(sources)
        # high_source should appear before low_source
        assert result.index("high_source") < result.index("low_source")


class TestFormatConversionFunnel:
    def test_with_data(self):
        funnel = {"Research": 10, "Qualified": 5, "Outreach": 2, "Won": 1}
        result = format_conversion_funnel(funnel)
        assert "Research" in result
        assert "100%" in result
        assert "█" in result

    def test_all_zeros(self):
        funnel = {"Research": 0, "Qualified": 0}
        result = format_conversion_funnel(funnel)
        assert "No funnel data" in result

    def test_empty(self):
        assert "No funnel data" in format_conversion_funnel({})


# ===========================================================================
# 11. build_weekly_report
# ===========================================================================

class TestBuildWeeklyReport:
    def _config(self):
        return {
            "include": [
                "pipeline_summary_by_stream",
                "new_leads_this_week",
                "leads_needing_attention",
                "stale_leads",
                "top_5_leads",
            ],
            "format": "markdown",
        }

    def test_header(self):
        report = build_weekly_report([], self._config(), reference_date=TODAY)
        assert "Week of February 27, 2026" in report

    def test_includes_all_sections(self):
        leads = [
            make_lead(id=1, stage_id=[1, "Research"], x_lead_score=70,
                      create_date="2026-02-25", write_date="2026-01-01"),
            make_lead(id=2, stage_id=[3, "Qualified"], x_lead_score=85,
                      create_date="2026-02-10", write_date="2026-02-26"),
        ]
        report = build_weekly_report(leads, self._config(), reference_date=TODAY)
        assert "Pipeline by Stream" in report
        assert "New Leads This Week" in report
        assert "Leads Needing Attention" in report
        assert "Stale Leads" in report
        assert "Top 5 Leads" in report

    def test_quick_stats(self):
        leads = [
            make_lead(id=1, stage_id=[1, "Research"]),
            make_lead(id=2, stage_id=[10, "Won"]),
            make_lead(id=3, stage_id=[11, "Lost"]),
        ]
        report = build_weekly_report(leads, self._config(), reference_date=TODAY)
        assert "Active leads:** 1" in report
        assert "Won:** 1" in report
        assert "Lost:** 1" in report
        assert "Total:** 3" in report

    def test_footer(self):
        report = build_weekly_report([], self._config(), reference_date=TODAY)
        assert "Generated by BD Automation Suite" in report

    def test_empty_leads(self):
        report = build_weekly_report([], self._config(), reference_date=TODAY)
        assert "Active leads:** 0" in report

    def test_partial_include(self):
        """Only include specified sections."""
        config = {"include": ["top_5_leads"]}
        report = build_weekly_report([make_lead()], config, reference_date=TODAY)
        assert "Top 5 Leads" in report
        assert "Pipeline by Stream" not in report
        assert "New Leads" not in report

    def test_empty_include(self):
        config = {"include": []}
        report = build_weekly_report([make_lead()], config, reference_date=TODAY)
        # Should still have header and quick stats
        assert "BD Pipeline Report" in report
        assert "Active leads" in report


# ===========================================================================
# 12. build_monthly_report
# ===========================================================================

class TestBuildMonthlyReport:
    def _config(self):
        return {
            "include": [
                "all_weekly_metrics",
                "conversion_rates",
                "score_distribution",
                "source_effectiveness",
            ],
            "format": "markdown",
        }

    def test_header(self):
        report = build_monthly_report([], self._config(), reference_date=TODAY)
        assert "February 2026" in report

    def test_includes_monthly_sections(self):
        leads = [
            make_lead(id=1, stage_id=[1, "Research"], x_lead_score=70,
                      x_data_source="trade_data"),
            make_lead(id=2, stage_id=[3, "Qualified"], x_lead_score=85,
                      x_data_source="google_maps"),
        ]
        report = build_monthly_report(leads, self._config(), reference_date=TODAY)
        assert "Conversion Funnel" in report
        assert "Score Distribution" in report
        assert "Source Effectiveness" in report

    def test_all_weekly_metrics_expands(self):
        leads = [make_lead(stage_id=[1, "Research"], x_lead_score=70,
                           write_date="2026-01-01")]
        report = build_monthly_report(leads, self._config(), reference_date=TODAY)
        assert "Pipeline by Stream" in report
        assert "Top 5 Leads" in report
        assert "Leads Needing Attention" in report
        assert "Stale Leads" in report

    def test_footer(self):
        report = build_monthly_report([], self._config(), reference_date=TODAY)
        assert "Generated by BD Automation Suite" in report

    def test_without_all_weekly_metrics(self):
        config = {"include": ["conversion_rates", "score_distribution"]}
        report = build_monthly_report([make_lead()], config, reference_date=TODAY)
        assert "Conversion Funnel" in report
        assert "Score Distribution" in report
        assert "Pipeline by Stream" not in report


# ===========================================================================
# 13. main.py — fetch_all_leads
# ===========================================================================

class TestFetchAllLeads:
    def test_domain_no_stream(self):
        from modules.pipeline_reporter.main import fetch_all_leads

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        fetch_all_leads(mock_odoo)

        domain = mock_odoo.search_leads.call_args[0][0]
        assert ["active", "in", [True, False]] in domain
        # No stream filter
        stream_conditions = [d for d in domain if d[0] == "x_bd_stream"]
        assert len(stream_conditions) == 0

    def test_domain_with_stream(self):
        from modules.pipeline_reporter.main import fetch_all_leads

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        fetch_all_leads(mock_odoo, stream_filter="stream_b")

        domain = mock_odoo.search_leads.call_args[0][0]
        assert ["x_bd_stream", "=", "stream_b"] in domain

    def test_returns_leads(self):
        from modules.pipeline_reporter.main import fetch_all_leads

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = [make_lead(), make_lead(id=2)]

        result = fetch_all_leads(mock_odoo)
        assert len(result) == 2


# ===========================================================================
# 14. main.py — save_report
# ===========================================================================

class TestSaveReport:
    def test_saves_to_file(self):
        from modules.pipeline_reporter.main import save_report

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test-report.md")
            result = save_report("# Report\nContent here.", path)
            assert os.path.exists(result)
            with open(result) as f:
                assert "# Report" in f.read()

    def test_creates_directories(self):
        from modules.pipeline_reporter.main import save_report

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "subdir", "deep", "report.md")
            result = save_report("Content", path)
            assert os.path.exists(result)

    def test_returns_absolute_path(self):
        from modules.pipeline_reporter.main import save_report

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "report.md")
            result = save_report("Content", path)
            assert os.path.isabs(result)


# ===========================================================================
# 15. main.py — run()
# ===========================================================================

class TestRun:
    def _setup_mocks(self, leads=None):
        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = leads or []
        return mock_odoo

    def _config(self):
        return {
            "weekly_report": {
                "send_to": "user@example.com",
                "include": ["pipeline_summary_by_stream", "top_5_leads"],
                "format": "markdown",
            },
            "monthly_report": {
                "send_to": "user@example.com",
                "include": ["all_weekly_metrics", "conversion_rates", "score_distribution", "source_effectiveness"],
                "format": "markdown",
            },
            "output_dir": "",
        }

    def _run_with_mocks(self, leads=None, report_type="weekly", stream_filter=None, output_path=None):
        from modules.pipeline_reporter.main import run

        mock_odoo = self._setup_mocks(leads)

        with patch("modules.pipeline_reporter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.pipeline_reporter.main.load_config", return_value=self._config()):
            result = run(
                report_type=report_type,
                stream_filter=stream_filter,
                output_path=output_path,
                reference_date=TODAY,
            )

        return result, mock_odoo

    def test_weekly_report(self):
        leads = [make_lead()]
        result, _ = self._run_with_mocks(leads=leads, report_type="weekly")
        assert result["report_type"] == "weekly"
        assert result["lead_count"] == 1

    def test_monthly_report(self):
        leads = [make_lead()]
        result, _ = self._run_with_mocks(leads=leads, report_type="monthly")
        assert result["report_type"] == "monthly"

    def test_no_leads(self):
        result, _ = self._run_with_mocks(leads=[])
        assert result["lead_count"] == 0

    def test_output_to_stdout(self, capsys):
        leads = [make_lead()]
        result, _ = self._run_with_mocks(leads=leads)
        captured = capsys.readouterr()
        assert "BD Pipeline Report" in captured.out
        assert "Pipeline Reporter Summary" in captured.out

    def test_save_to_explicit_path(self):
        from modules.pipeline_reporter.main import run

        leads = [make_lead()]
        mock_odoo = self._setup_mocks(leads)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "my-report.md")

            with patch("modules.pipeline_reporter.main.OdooClient.from_env", return_value=mock_odoo), \
                 patch("modules.pipeline_reporter.main.load_config", return_value=self._config()):
                result = run(output_path=output_path, reference_date=TODAY)

            assert result["saved"] is True
            assert os.path.exists(result["output_path"])

    def test_save_to_config_output_dir(self):
        from modules.pipeline_reporter.main import run

        leads = [make_lead()]
        mock_odoo = self._setup_mocks(leads)

        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._config()
            config["output_dir"] = tmpdir

            with patch("modules.pipeline_reporter.main.OdooClient.from_env", return_value=mock_odoo), \
                 patch("modules.pipeline_reporter.main.load_config", return_value=config):
                result = run(reference_date=TODAY)

            assert result["saved"] is True
            assert "bd-pipeline-weekly-2026-02-27.md" in result["output_path"]
            assert os.path.exists(result["output_path"])

    def test_no_output_dir_stdout_only(self):
        result, _ = self._run_with_mocks(leads=[make_lead()])
        assert result["saved"] is False
        assert result["output_path"] is None

    def test_stream_filter_passed_to_odoo(self):
        result, mock_odoo = self._run_with_mocks(leads=[], stream_filter="stream_b")
        domain = mock_odoo.search_leads.call_args[0][0]
        assert ["x_bd_stream", "=", "stream_b"] in domain

    def test_monthly_output_filename(self):
        from modules.pipeline_reporter.main import run

        leads = [make_lead()]
        mock_odoo = self._setup_mocks(leads)

        with tempfile.TemporaryDirectory() as tmpdir:
            config = self._config()
            config["output_dir"] = tmpdir

            with patch("modules.pipeline_reporter.main.OdooClient.from_env", return_value=mock_odoo), \
                 patch("modules.pipeline_reporter.main.load_config", return_value=config):
                result = run(report_type="monthly", reference_date=TODAY)

            assert "bd-pipeline-monthly-2026-02-27.md" in result["output_path"]
